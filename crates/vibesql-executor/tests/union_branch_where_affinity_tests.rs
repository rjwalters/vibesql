//! Regression tests for issue #5749:
//! "[executor] WHERE on a UNION ALL branch loses column type affinity".
//!
//! Root cause: `execute_with_iterators`
//! (`select/executor/nonagg/iterator.rs`) ignored `from_result.where_filtered`
//! and UNCONDITIONALLY re-applied the WHERE clause via a `FilterIterator`. The
//! columnar table scan had already applied the predicate WITH numeric coercion
//! (setting `where_filtered = true`), but the second evaluation runs through the
//! expression evaluator's `apply_affinity_for_comparison`, which does NOT coerce
//! `'14'` -> 14 for a NONE-affinity (typeless) column. So `a = '14'` matched in
//! the scan but the re-evaluation dropped the row, yielding an empty branch.
//!
//! The fast path (`fast_path/mod.rs`) and the materialized path
//! (`nonagg/materialized.rs`) already guarded their WHERE re-application behind
//! `where_filtered`, which is why standalone `SELECT a FROM t1 WHERE a='14'`
//! (a simple point query routed through the fast path) returned the row
//! correctly. Only compound (UNION ALL / set-operation / compound-derived-table)
//! queries went through `execute_with_iterators` and lost the row.
//!
//! The fix mirrors those two guards: skip the Stage-2 FilterIterator when
//! `where_filtered == true`. `where_filtered` is only set when the scan FULLY
//! applied the predicate (gated on `extract_full_coverage_predicates` succeeding
//! for all conjuncts and on the absence of complex predicates such as scalar
//! subqueries — see `select/scan/table.rs`), so skipping re-application is safe
//! and never wrongly admits rows. The partial-coverage / complex-predicate tests
//! below confirm re-application still happens when `where_filtered == false`.

use vibesql_executor::{CreateTableExecutor, InsertExecutor};
use vibesql_parser::Parser;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

/// Execute one or more non-SELECT SQL statements separated by ';'.
fn execute_sql(db: &mut Database, sql: &str) {
    for sql_stmt in sql.split(';') {
        let trimmed = sql_stmt.trim();
        if trimmed.is_empty() {
            continue;
        }
        let stmt = Parser::parse_sql(trimmed).expect("Failed to parse SQL");
        match stmt {
            vibesql_ast::Statement::CreateTable(s) => {
                CreateTableExecutor::execute(&s, db).expect("CREATE TABLE failed");
            }
            vibesql_ast::Statement::Insert(s) => {
                InsertExecutor::execute(db, &s).expect("INSERT failed");
            }
            other => panic!("Unsupported statement type: {:?}", other),
        }
    }
}

/// Execute a SELECT and return the first column of each row, preserving order.
fn select_first_col(db: &Database, sql: &str) -> Vec<SqlValue> {
    let stmt = Parser::parse_sql(sql).expect("Failed to parse SELECT");
    let select_stmt = match stmt {
        vibesql_ast::Statement::Select(s) => s,
        other => panic!("Expected SELECT statement, got {:?}", other),
    };
    let executor = vibesql_executor::SelectExecutor::new(db);
    executor
        .execute(&select_stmt)
        .expect("SELECT execution failed")
        .iter()
        .map(|row| row.values[0].clone())
        .collect()
}

/// `CREATE TABLE t1(a, b, c)` — every column is typeless (NONE affinity).
/// `t2(d)` is also typeless and kept empty for the derived-table case.
fn setup_db() -> Database {
    let mut db = Database::new();
    execute_sql(
        &mut db,
        "CREATE TABLE t1(a, b, c); INSERT INTO t1 VALUES(14, 16, 18); CREATE TABLE t2(d)",
    );
    db
}

fn i(n: i64) -> SqlValue {
    SqlValue::Integer(n)
}

/// Repro 1 (left branch). Before the fix this returned `{999}` only.
#[test]
fn test_union_all_left_branch_where_affinity() {
    let db = setup_db();
    let rows = select_first_col(&db, "SELECT a FROM t1 WHERE a = '14' UNION ALL SELECT 999");
    assert_eq!(
        rows,
        vec![i(14), i(999)],
        "left branch `a = '14'` on a typeless column must match the stored integer 14"
    );
}

/// Repro 2 (right branch). The right branch of a set operation executes through
/// the same `execute_with_iterators` path via `execute_set_operations`.
#[test]
fn test_union_all_right_branch_where_affinity() {
    let db = setup_db();
    let rows = select_first_col(&db, "SELECT 999 UNION ALL SELECT a FROM t1 WHERE a = '14'");
    assert_eq!(
        rows,
        vec![i(999), i(14)],
        "right branch `a = '14'` on a typeless column must match the stored integer 14"
    );
}

/// Repro 3 (compound derived table). `t2` is empty, so only the 14 row should
/// surface. Before the fix this returned `{}`.
#[test]
fn test_union_all_compound_derived_table_where_affinity() {
    let db = setup_db();
    let rows = select_first_col(
        &db,
        "SELECT * FROM (SELECT a FROM t1 WHERE a = '14' UNION ALL SELECT d FROM t2) AS v",
    );
    assert_eq!(rows, vec![i(14)], "compound-derived-table branch must keep the matching 14 row");
}

/// Standalone (fast-path) case must continue to return the row — guards against
/// a regression to the path that already worked.
#[test]
fn test_standalone_where_affinity_unchanged() {
    let db = setup_db();
    let rows = select_first_col(&db, "SELECT a FROM t1 WHERE a = '14'");
    assert_eq!(rows, vec![i(14)], "standalone `a = '14'` must still return 14");
}

/// Integer-literal form must keep working in compound queries.
#[test]
fn test_union_all_integer_literal_unchanged() {
    let db = setup_db();
    let rows = select_first_col(&db, "SELECT a FROM t1 WHERE a = 14 UNION ALL SELECT 999");
    assert_eq!(rows, vec![i(14), i(999)]);
}

/// Partial / range predicates: the scan only fully consumes table-local
/// columnar predicates. A `>` predicate over a compound query must still filter
/// correctly (re-application path, where_filtered == true here because `a > 15`
/// is a full-coverage columnar predicate). This asserts no rows are wrongly
/// admitted or dropped.
#[test]
fn test_union_all_range_predicate() {
    let mut db = Database::new();
    execute_sql(
        &mut db,
        "CREATE TABLE t3(a, b, c); \
         INSERT INTO t3 VALUES(14, 16, 18); \
         INSERT INTO t3 VALUES(20, 21, 22); \
         INSERT INTO t3 VALUES(30, 31, 32)",
    );
    let rows = select_first_col(&db, "SELECT a FROM t3 WHERE a > 15 UNION ALL SELECT 999");
    assert_eq!(rows, vec![i(20), i(30), i(999)]);
}

/// Complex predicate (scalar subquery) in a compound branch. This is the
/// INVERSE risk of the fix: such a predicate is routed to `complex_predicates`,
/// so the scan does NOT mark `where_filtered = true`, and the WHERE MUST be
/// re-applied by the iterator. If the guard wrongly trusted a stale flag here,
/// the subquery conjunct would be dropped and extra rows admitted.
#[test]
fn test_union_all_scalar_subquery_predicate_not_dropped() {
    let mut db = Database::new();
    execute_sql(
        &mut db,
        "CREATE TABLE t4(a, b, c); \
         INSERT INTO t4 VALUES(14, 16, 18); \
         INSERT INTO t4 VALUES(20, 21, 22); \
         INSERT INTO t4 VALUES(30, 31, 32)",
    );
    let rows = select_first_col(
        &db,
        "SELECT a FROM t4 WHERE a = (SELECT MAX(a) FROM t4) UNION ALL SELECT 999",
    );
    assert_eq!(
        rows,
        vec![i(30), i(999)],
        "scalar-subquery conjunct must be re-applied (where_filtered stays false)"
    );
}

/// Issue #5749 (regression guard for the columnar precision fix that the
/// `where_filtered` guard exposed): once the iterator path trusts
/// `where_filtered`, the columnar filter must agree with the expression
/// evaluator on `i64::MAX >= (i64::MAX + 1)`. The overflow promotes to the f64
/// 2^63; a lossy `i64::MAX as f64` also rounds to 2^63 and would wrongly match.
/// SQLite returns an empty set here (TCL where-27.1).
#[test]
fn test_integer_max_overflow_comparison_not_matched() {
    let mut db = Database::new();
    execute_sql(
        &mut db,
        "CREATE TABLE big(a INTEGER PRIMARY KEY); INSERT INTO big(a) VALUES(9223372036854775807)",
    );
    let rows = select_first_col(&db, "SELECT 1 FROM big WHERE a >= (9223372036854775807 + 1)");
    assert!(
        rows.is_empty(),
        "i64::MAX must NOT satisfy `a >= i64::MAX + 1` (overflow promotes to 2^63 float); got {:?}",
        rows
    );
}

/// OR predicate across a compound query — exercises a non-trivial columnar
/// predicate shape; results must match exactly.
#[test]
fn test_union_all_or_predicate() {
    let mut db = Database::new();
    execute_sql(
        &mut db,
        "CREATE TABLE t5(a, b, c); \
         INSERT INTO t5 VALUES(14, 16, 18); \
         INSERT INTO t5 VALUES(20, 21, 22); \
         INSERT INTO t5 VALUES(30, 31, 32)",
    );
    let rows =
        select_first_col(&db, "SELECT a FROM t5 WHERE a = 14 OR a = 30 UNION ALL SELECT 999");
    assert_eq!(rows, vec![i(14), i(30), i(999)]);
}
