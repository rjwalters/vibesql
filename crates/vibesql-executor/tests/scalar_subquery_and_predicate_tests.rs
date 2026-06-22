//! Regression tests for issue #5719
//!
//! A scalar-subquery equality predicate was silently dropped when AND'd with
//! another same-table predicate on the columnar analytical fast path.
//!
//! Root cause: `ColumnarPipeline::apply_filter` /
//! `NativeColumnarPipeline::apply_filter` called the lenient
//! `extract_column_predicates`, whose AND branch silently skips conjuncts it
//! cannot fold columnarly. For a WHERE like
//! `status = 'failed' AND run_id = (SELECT MAX(run_id) FROM t)` it returned only
//! `[status = 'failed']` (NOT `None`), so the SIMD path applied just that
//! predicate and the scalar-subquery conjunct was dropped → over-counted rows.
//!
//! The fix routes both pipelines through the strict
//! `extract_full_coverage_predicates`, which returns `Some` only when every
//! conjunct is columnar; otherwise the pipeline falls back to the full
//! expression evaluator (which handles scalar subqueries correctly).
//!
//! The analytical pattern (aggregate + selective projection / arithmetic /
//! GROUP BY) is what selects the StandardColumnar strategy, so the tests use
//! aggregate queries. A control case with `SELECT *` (RowOriented path) and a
//! lone scalar-subquery WHERE (no AND) guard against regressions.

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

/// Build the reproduction table.
///
/// ```sql
/// CREATE TABLE t (run_id INTEGER, status TEXT, amount INTEGER);
/// ```
///
/// The max run_id is 3. Among rows with `run_id = 3` exactly two have
/// `status = 'failed'` (amounts 100 and 40). Many extra rows with smaller
/// run_ids and `status = 'failed'` exist so that dropping the
/// `run_id = (SELECT MAX(run_id) FROM t)` conjunct visibly over-counts.
fn setup_db() -> Database {
    let mut db = Database::new();

    let schema = TableSchema::new(
        "T".to_string(),
        vec![
            ColumnSchema::new("run_id".to_string(), DataType::Integer, true),
            ColumnSchema::new("status".to_string(), DataType::Varchar { max_length: None }, true),
            ColumnSchema::new("amount".to_string(), DataType::Integer, true),
        ],
    );
    db.create_table(schema).unwrap();

    let mut insert = |run_id: i64, status: &str, amount: i64| {
        db.insert_row(
            "T",
            Row::new(vec![
                SqlValue::Integer(run_id),
                SqlValue::Varchar(status.into()),
                SqlValue::Integer(amount),
            ]),
        )
        .unwrap();
    };

    // run_id = 1: 5 failed rows (these must NOT be counted)
    for amount in [1, 2, 3, 4, 5] {
        insert(1, "failed", amount);
    }
    insert(1, "passed", 6);

    // run_id = 2: 3 failed rows (these must NOT be counted)
    for amount in [10, 11, 12] {
        insert(2, "failed", amount);
    }
    insert(2, "passed", 13);

    // run_id = 3 (the MAX): exactly 2 failed rows (amounts 100, 40), 1 passed.
    insert(3, "failed", 100);
    insert(3, "passed", 99);
    insert(3, "failed", 40);

    db
}

/// The exact headline repro: COUNT(*) over a same-table column predicate AND a
/// scalar-subquery equality predicate. Before the fix this returned 10 (every
/// `status = 'failed'` row) instead of 2 (only `run_id = MAX`).
#[test]
fn test_count_col_and_scalar_subquery_not_overcounted() {
    let db = setup_db();

    let sql = "SELECT COUNT(*) FROM t \
               WHERE status = 'failed' AND run_id = (SELECT MAX(run_id) FROM t)";
    let result = run(&db, sql);

    assert_eq!(result.len(), 1);
    assert_eq!(
        result[0].values[0],
        SqlValue::Integer(2),
        "COUNT(*) must be 2 (failed rows at run_id=MAX), not over-counted"
    );
}

/// The scalar-subquery conjunct on the *left* of the AND must be honored too —
/// guards against an order-dependent partial extraction.
#[test]
fn test_count_scalar_subquery_and_col_left_order() {
    let db = setup_db();

    let sql = "SELECT COUNT(*) FROM t \
               WHERE run_id = (SELECT MAX(run_id) FROM t) AND status = 'failed'";
    let result = run(&db, sql);

    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], SqlValue::Integer(2));
}

/// SUM variant: only the two run_id=MAX failed rows (100 + 40) contribute.
#[test]
fn test_sum_col_and_scalar_subquery() {
    let db = setup_db();

    let sql = "SELECT SUM(amount) FROM t \
               WHERE status = 'failed' AND run_id = (SELECT MAX(run_id) FROM t)";
    let result = run(&db, sql);

    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], SqlValue::Integer(140), "SUM(amount) must be 100 + 40 = 140");
}

/// MAX variant: max amount among run_id=MAX failed rows is 100.
#[test]
fn test_max_col_and_scalar_subquery() {
    let db = setup_db();

    let sql = "SELECT MAX(amount) FROM t \
               WHERE status = 'failed' AND run_id = (SELECT MAX(run_id) FROM t)";
    let result = run(&db, sql);

    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], SqlValue::Integer(100));
}

/// Control: lone scalar-subquery predicate (no AND). Must work and count all
/// three run_id=MAX rows (2 failed + 1 passed).
#[test]
fn test_count_scalar_subquery_alone() {
    let db = setup_db();

    let sql = "SELECT COUNT(*) FROM t WHERE run_id = (SELECT MAX(run_id) FROM t)";
    let result = run(&db, sql);

    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], SqlValue::Integer(3));
}

/// Control: `SELECT *` takes the RowOriented path (no analytical pattern) and
/// must already be correct. This asserts the same predicate set returns exactly
/// the 2 matching rows.
#[test]
fn test_select_star_col_and_scalar_subquery() {
    let db = setup_db();

    let sql = "SELECT run_id, status, amount FROM t \
               WHERE status = 'failed' AND run_id = (SELECT MAX(run_id) FROM t) \
               ORDER BY amount";
    let result = run(&db, sql);

    assert_eq!(result.len(), 2, "exactly 2 rows: run_id=3 failed");
    assert_eq!(result[0].values[0], SqlValue::Integer(3));
    assert_eq!(result[0].values[2], SqlValue::Integer(40));
    assert_eq!(result[1].values[0], SqlValue::Integer(3));
    assert_eq!(result[1].values[2], SqlValue::Integer(100));
}

/// Fully-columnar WHERE (two simple column predicates, no subquery) must still
/// take the fast SIMD path and return the correct count — guards against the
/// fix accidentally forcing fallback for fully-covered WHEREs.
#[test]
fn test_count_two_column_predicates_full_coverage() {
    let db = setup_db();

    // run_id = 1 AND status = 'failed' → the 5 seeded failed rows at run_id=1.
    let sql = "SELECT COUNT(*) FROM t WHERE status = 'failed' AND run_id = 1";
    let result = run(&db, sql);

    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], SqlValue::Integer(5));
}
