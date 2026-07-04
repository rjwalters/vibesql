//! Regression tests for issue #5809
//!
//! An *uncorrelated* scalar subquery in WHERE, e.g.
//!
//! ```sql
//! SELECT ... FROM t WHERE run_id = (SELECT MAX(run_id) FROM t)
//! ```
//!
//! was re-evaluated for every candidate row on the parallel filter paths:
//! `apply_where_filter_combined_parallel` (morsel slow path) and
//! `apply_predicates_parallel` construct a fresh thread-local evaluator —
//! with an empty subquery cache — per row, so the subquery's full table scan
//! re-executed n times, an O(n²) blowup (the canonical CLAUDE.md pass-rate
//! query burned 822 CPU-minutes at 21 GB RSS without completing).
//!
//! The fix hoists provably-uncorrelated single-column scalar subqueries out
//! of the predicate before per-row filtering begins: each is evaluated
//! exactly once and replaced by its literal value
//! (`CombinedExpressionEvaluator::hoist_uncorrelated_scalar_subqueries`).
//!
//! These tests cover:
//! - the O(n²) perf regression itself (100k rows under a wall-clock budget)
//! - correctness of the hoisted uncorrelated form (incl. the same-table,
//!   unqualified-column canonical shape)
//! - correlated subqueries, which must still be evaluated per row
//! - NULL-returning / empty-result subqueries
//! - multi-column subqueries in row-value comparisons (must NOT be folded)
//! - scalar subqueries in the SELECT list and HAVING (shared eval path)

use std::time::{Duration, Instant};

use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;
use vibesql_storage::{Database, Row};
use vibesql_types::{DataType, SqlValue};

fn parse_select(sql: &str) -> vibesql_ast::SelectStmt {
    match Parser::parse_sql(sql) {
        Ok(vibesql_ast::Statement::Select(select_stmt)) => *select_stmt,
        _ => panic!("Failed to parse SELECT statement: {}", sql),
    }
}

fn run(db: &Database, sql: &str) -> Vec<Row> {
    let select = parse_select(sql);
    SelectExecutor::new(db).execute(&select).unwrap()
}

fn int(row: &Row, idx: usize) -> i64 {
    match row.get(idx) {
        Some(SqlValue::Integer(n)) => *n,
        Some(SqlValue::Bigint(n)) => *n,
        other => panic!("expected integer at index {idx}, got {other:?}"),
    }
}

/// Small correctness fixture:
///
/// ```sql
/// CREATE TABLE a (id INTEGER, grp INTEGER, val INTEGER);
/// -- (1,1,10),(2,1,20),(3,2,30),(4,2,40),(5,3,NULL),(6,3,60)
/// CREATE TABLE b (grp INTEGER, cap INTEGER);
/// -- (1,15),(2,35),(3,100)
/// ```
fn setup_small_db() -> Database {
    let mut db = Database::new();

    let schema_a = TableSchema::new(
        "A".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, true),
            ColumnSchema::new("grp".to_string(), DataType::Integer, true),
            ColumnSchema::new("val".to_string(), DataType::Integer, true),
        ],
    );
    db.create_table(schema_a).unwrap();

    let rows_a: [(i64, i64, Option<i64>); 6] = [
        (1, 1, Some(10)),
        (2, 1, Some(20)),
        (3, 2, Some(30)),
        (4, 2, Some(40)),
        (5, 3, None),
        (6, 3, Some(60)),
    ];
    for (id, grp, val) in rows_a {
        db.insert_row(
            "A",
            Row::new(vec![
                SqlValue::Integer(id),
                SqlValue::Integer(grp),
                val.map(SqlValue::Integer).unwrap_or(SqlValue::Null),
            ]),
        )
        .unwrap();
    }

    let schema_b = TableSchema::new(
        "B".to_string(),
        vec![
            ColumnSchema::new("grp".to_string(), DataType::Integer, true),
            ColumnSchema::new("cap".to_string(), DataType::Integer, true),
        ],
    );
    db.create_table(schema_b).unwrap();

    for (grp, cap) in [(1i64, 15i64), (2, 35), (3, 100)] {
        db.insert_row("B", Row::new(vec![SqlValue::Integer(grp), SqlValue::Integer(cap)])).unwrap();
    }

    db
}

// ---------------------------------------------------------------------------
// Perf regression: O(n²) blowup
// ---------------------------------------------------------------------------

/// The canonical issue #5809 shape on a table large enough that the parallel
/// filter path is taken (>10k rows) and O(n²) behavior cannot hide:
/// 100k rows × 100k-row subquery scans would be 10^10 row visits (hours even
/// in release; the original report burned 822 CPU-minutes on 413k rows).
/// With the hoist the query is two linear passes. The generous budget keeps
/// the test robust on loaded CI machines while still being orders of
/// magnitude below the O(n²) runtime.
#[test]
fn uncorrelated_scalar_subquery_where_is_not_quadratic() {
    let mut db = Database::new();

    let schema = TableSchema::new(
        "T".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, true),
            ColumnSchema::new("run_id".to_string(), DataType::Integer, true),
        ],
    );
    db.create_table(schema).unwrap();

    const N: i64 = 100_000;
    for i in 1..=N {
        db.insert_row("T", Row::new(vec![SqlValue::Integer(i), SqlValue::Integer(i % 5 + 1)]))
            .unwrap();
    }

    let start = Instant::now();
    let rows = run(
        &db,
        "SELECT run_id, COUNT(*) FROM t \
         WHERE run_id = (SELECT MAX(run_id) FROM t) \
         GROUP BY run_id",
    );
    let elapsed = start.elapsed();

    assert_eq!(rows.len(), 1);
    assert_eq!(int(&rows[0], 0), 5);
    assert_eq!(int(&rows[0], 1), 20_000);

    assert!(
        elapsed < Duration::from_secs(30),
        "uncorrelated scalar subquery in WHERE took {elapsed:?}; \
         re-evaluation per row (O(n²)) suspected (issue #5809)"
    );
}

/// Same guard for the non-aggregate path (no GROUP BY), which filters through
/// the materialized non-aggregate executor.
#[test]
fn uncorrelated_scalar_subquery_where_nonaggregate_is_not_quadratic() {
    let mut db = Database::new();

    let schema = TableSchema::new(
        "T".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, true),
            ColumnSchema::new("run_id".to_string(), DataType::Integer, true),
        ],
    );
    db.create_table(schema).unwrap();

    const N: i64 = 50_000;
    for i in 1..=N {
        db.insert_row("T", Row::new(vec![SqlValue::Integer(i), SqlValue::Integer(i % 5 + 1)]))
            .unwrap();
    }

    let start = Instant::now();
    let rows =
        run(&db, "SELECT id FROM t WHERE run_id = (SELECT MAX(run_id) FROM t) ORDER BY id LIMIT 3");
    let elapsed = start.elapsed();

    assert_eq!(rows.len(), 3);
    // run_id = 5 <=> i % 5 == 4; smallest ids are 4, 9, 14
    assert_eq!(int(&rows[0], 0), 4);
    assert_eq!(int(&rows[1], 0), 9);
    assert_eq!(int(&rows[2], 0), 14);

    assert!(
        elapsed < Duration::from_secs(30),
        "uncorrelated scalar subquery in WHERE (non-aggregate) took {elapsed:?}; \
         re-evaluation per row (O(n²)) suspected (issue #5809)"
    );
}

// ---------------------------------------------------------------------------
// Correctness: uncorrelated forms (hoisted)
// ---------------------------------------------------------------------------

#[test]
fn uncorrelated_max_same_table() {
    let db = setup_small_db();
    // MAX(val) = 60 -> only id 6
    let rows = run(&db, "SELECT id FROM a WHERE val = (SELECT MAX(val) FROM a) ORDER BY id");
    assert_eq!(rows.len(), 1);
    assert_eq!(int(&rows[0], 0), 6);
}

#[test]
fn uncorrelated_subquery_inside_arithmetic() {
    let db = setup_small_db();
    // val >= 60 - 30 = 30 -> ids 3, 4, 6
    let rows = run(&db, "SELECT id FROM a WHERE val >= (SELECT MAX(val) FROM a) - 30 ORDER BY id");
    let ids: Vec<i64> = rows.iter().map(|r| int(r, 0)).collect();
    assert_eq!(ids, vec![3, 4, 6]);
}

#[test]
fn uncorrelated_subqueries_as_between_bounds() {
    let db = setup_small_db();
    // MIN(val)=10, AVG(val)=32 -> val in [10, 32] -> ids 1, 2, 3
    let rows = run(
        &db,
        "SELECT id FROM a \
         WHERE val BETWEEN (SELECT MIN(val) FROM a) AND (SELECT AVG(val) FROM a) \
         ORDER BY id",
    );
    let ids: Vec<i64> = rows.iter().map(|r| int(r, 0)).collect();
    assert_eq!(ids, vec![1, 2, 3]);
}

#[test]
fn uncorrelated_subquery_against_other_table() {
    let db = setup_small_db();
    // MAX(cap) = 100 -> no val reaches it
    let rows = run(&db, "SELECT id FROM a WHERE val > (SELECT MAX(cap) FROM b) ORDER BY id");
    assert!(rows.is_empty());
    // MIN(cap) = 15 -> ids 2, 3, 4, 6
    let rows = run(&db, "SELECT id FROM a WHERE val > (SELECT MIN(cap) FROM b) ORDER BY id");
    let ids: Vec<i64> = rows.iter().map(|r| int(r, 0)).collect();
    assert_eq!(ids, vec![2, 3, 4, 6]);
}

// ---------------------------------------------------------------------------
// Correctness: NULL / empty subquery results
// ---------------------------------------------------------------------------

#[test]
fn null_returning_subquery_filters_all_rows() {
    let db = setup_small_db();
    // Empty result set -> scalar subquery is NULL -> comparison is NULL -> 0 rows
    let rows = run(&db, "SELECT id FROM a WHERE val = (SELECT val FROM a WHERE id = 999)");
    assert!(rows.is_empty());
    // Explicit NULL
    let rows = run(&db, "SELECT id FROM a WHERE val = (SELECT NULL)");
    assert!(rows.is_empty());
    // IS NULL over a NULL-valued subquery must keep all rows
    let rows = run(&db, "SELECT COUNT(*) FROM a WHERE (SELECT NULL) IS NULL");
    assert_eq!(int(&rows[0], 0), 6);
}

// ---------------------------------------------------------------------------
// Correctness: correlated subqueries must still evaluate per row
// ---------------------------------------------------------------------------

#[test]
fn correlated_subquery_still_per_row_two_tables() {
    let db = setup_small_db();
    // Per-row cap from b: grp 1 -> 15 (20 > 15: id 2), grp 2 -> 35 (40 > 35: id 4),
    // grp 3 -> 100 (none). Matches sqlite3.
    let rows = run(
        &db,
        "SELECT id FROM a WHERE val > (SELECT cap FROM b WHERE b.grp = a.grp) ORDER BY id",
    );
    let ids: Vec<i64> = rows.iter().map(|r| int(r, 0)).collect();
    assert_eq!(ids, vec![2, 4]);
}

#[test]
fn correlated_subquery_still_per_row_same_table_alias() {
    let db = setup_small_db();
    // Per-group max: grp 1 -> 20 (id 2), grp 2 -> 40 (id 4), grp 3 -> 60 (id 6).
    // Matches sqlite3.
    let rows = run(
        &db,
        "SELECT id FROM a x \
         WHERE val = (SELECT MAX(val) FROM a y WHERE y.grp = x.grp) \
         ORDER BY id",
    );
    let ids: Vec<i64> = rows.iter().map(|r| int(r, 0)).collect();
    assert_eq!(ids, vec![2, 4, 6]);
}

// ---------------------------------------------------------------------------
// Correctness: multi-column subqueries in row-value comparisons are NOT folded
// ---------------------------------------------------------------------------

#[test]
fn row_value_subquery_comparison_still_works() {
    let db = setup_small_db();
    // (grp, val) of id 4 is (2, 40); subquery returns (2, 40)
    let rows = run(
        &db,
        "SELECT id FROM a \
         WHERE (grp, val) = (SELECT grp, val FROM a WHERE id = 4) \
         ORDER BY id",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(int(&rows[0], 0), 4);
}

// ---------------------------------------------------------------------------
// Correctness: SELECT list and HAVING share the scalar-subquery path
// ---------------------------------------------------------------------------

#[test]
fn uncorrelated_subquery_in_select_list() {
    let db = setup_small_db();
    let rows = run(&db, "SELECT id, (SELECT MIN(val) FROM a) FROM a WHERE id <= 2 ORDER BY id");
    assert_eq!(rows.len(), 2);
    for row in &rows {
        assert_eq!(int(row, 1), 10);
    }
}

#[test]
fn uncorrelated_subquery_in_having() {
    let db = setup_small_db();
    // AVG(val) = 32; group sums: grp1=30, grp2=70, grp3=60 -> grp 2 and 3
    let rows = run(
        &db,
        "SELECT grp, SUM(val) FROM a GROUP BY grp \
         HAVING SUM(val) > (SELECT AVG(val) FROM a) ORDER BY grp",
    );
    let grps: Vec<i64> = rows.iter().map(|r| int(r, 0)).collect();
    assert_eq!(grps, vec![2, 3]);
}

// ---------------------------------------------------------------------------
// Correctness: hoist interacts safely with other WHERE conjuncts
// ---------------------------------------------------------------------------

#[test]
fn hoisted_subquery_anded_with_plain_predicate() {
    let db = setup_small_db();
    // grp = MAX(grp) = 3 AND val IS NOT NULL -> id 6
    let rows = run(
        &db,
        "SELECT id FROM a \
         WHERE grp = (SELECT MAX(grp) FROM a) AND val IS NOT NULL \
         ORDER BY id",
    );
    assert_eq!(rows.len(), 1);
    assert_eq!(int(&rows[0], 0), 6);
}

#[test]
fn hoisted_and_correlated_subqueries_mixed() {
    let db = setup_small_db();
    // Correlated per-group max AND uncorrelated global min:
    // per-group maxes are ids 2 (20), 4 (40), 6 (60); all > MIN(val)=10 except none excluded
    let rows = run(
        &db,
        "SELECT id FROM a x \
         WHERE val = (SELECT MAX(val) FROM a y WHERE y.grp = x.grp) \
           AND val > (SELECT MIN(val) FROM a) \
         ORDER BY id",
    );
    let ids: Vec<i64> = rows.iter().map(|r| int(r, 0)).collect();
    assert_eq!(ids, vec![2, 4, 6]);
}
