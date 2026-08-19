//! Test for issue #1863: IN/NOT IN operators return incorrect boolean values
//! when a CASE expression containing aggregates returns NULL.
//!
//! Per SQL three-valued logic, `NULL IN (...)` and `NULL NOT IN (...)` must
//! evaluate to NULL (not TRUE/FALSE). Previously, when the NULL came from a
//! CASE expression wrapping an aggregate (e.g. `CASE -COUNT(*) WHEN -10 THEN 1 END`),
//! the IN / NOT IN comparison incorrectly produced a boolean instead of NULL.
//!
//! Promoted from the manual repro fixture `tests/issue-1863/case_aggregate_in.test`.

use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;
use vibesql_storage::{Database, Row};
use vibesql_types::{DataType, SqlValue};

/// Helper to execute SELECT and return rows
fn select_rows(db: &Database, sql: &str) -> Vec<Row> {
    let stmt = Parser::parse_sql(sql).unwrap();
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let executor = SelectExecutor::new(db);
        executor.execute(&select_stmt).unwrap()
    } else {
        panic!("Expected SELECT statement");
    }
}

/// Assert that a query returns exactly one row with a single NULL value
fn assert_single_null(db: &Database, sql: &str) {
    let rows = select_rows(db, sql);
    assert_eq!(rows.len(), 1, "expected exactly one row for: {sql}");
    assert_eq!(rows[0].values.len(), 1, "expected exactly one column for: {sql}");
    assert_eq!(rows[0].values[0], SqlValue::Null, "expected NULL result for: {sql}");
}

#[test]
fn test_case_aggregate_returns_null_baseline() {
    // Baseline: CASE with aggregate returns NULL when no branch matches
    let db = Database::new();
    assert_single_null(&db, "SELECT (CASE -COUNT(*) WHEN -10 THEN 1 END)");
}

#[test]
fn test_case_aggregate_not_in() {
    // The original bug: NULL (from CASE-with-aggregate) NOT IN (32) must be NULL
    let db = Database::new();
    assert_single_null(&db, "SELECT (CASE -COUNT(*) WHEN -10 THEN 1 END) NOT IN (32)");
}

#[test]
fn test_case_aggregate_in() {
    // The original bug: NULL (from CASE-with-aggregate) IN (32) must be NULL
    let db = Database::new();
    assert_single_null(&db, "SELECT (CASE -COUNT(*) WHEN -10 THEN 1 END) IN (32)");
}

#[test]
fn test_case_aggregate_not_in_subquery_wrapper() {
    // Workaround verification: wrapping the CASE in a scalar subquery also yields NULL
    let db = Database::new();
    assert_single_null(&db, "SELECT (SELECT CASE -COUNT(*) WHEN -10 THEN 1 END) NOT IN (32)");
}

#[test]
fn test_case_aggregate_in_list_containing_null() {
    // Edge case: NULL IN (32, NULL) is still NULL
    let db = Database::new();
    assert_single_null(&db, "SELECT (CASE -COUNT(*) WHEN -10 THEN 1 END) IN (32, NULL)");
}

#[test]
fn test_case_aggregate_not_in_without_negation() {
    // Same bug without the unary minus on the aggregate
    let db = Database::new();
    assert_single_null(&db, "SELECT (CASE COUNT(*) WHEN 10 THEN 1 END) NOT IN (32)");
}

#[test]
fn test_case_sum_aggregate_in_with_table_data() {
    // Other aggregate functions over real table data: SUM(x) = 30, no CASE branch
    // matches 999, so the CASE yields NULL and NULL IN (5) is NULL
    let mut db = Database::new();
    let schema = TableSchema::new(
        "test_table".to_string(),
        vec![ColumnSchema::new("x".to_string(), DataType::Integer, true)],
    );
    db.create_table(schema).unwrap();
    let table = db.get_table_mut("test_table").unwrap();
    table.insert(Row::new(vec![SqlValue::Integer(5)])).unwrap();
    table.insert(Row::new(vec![SqlValue::Integer(10)])).unwrap();
    table.insert(Row::new(vec![SqlValue::Integer(15)])).unwrap();

    assert_single_null(&db, "SELECT (CASE SUM(x) WHEN 999 THEN 1 END) IN (5) FROM test_table");
}

#[test]
fn test_searched_case_aggregate_not_in() {
    // Searched CASE (no operand) with aggregate in the condition
    let db = Database::new();
    assert_single_null(&db, "SELECT (CASE WHEN COUNT(*) = 10 THEN 1 END) NOT IN (32)");
}
