//! Regression tests for issue #5841 (item 5): `x IS TRUE` / `x IS FALSE`
//! must apply SQLite truthiness to REAL / floating-point values.
//!
//! SQLite treats any non-zero, non-NULL numeric value (including REAL) as
//! TRUE, and zero as FALSE:
//!
//! - `0.5 IS TRUE`  -> 1
//! - `0.0 IS TRUE`  -> 0
//! - `0.5 IS FALSE` -> 0
//! - `0.0 IS FALSE` -> 1
//!
//! Expected values below were verified against sqlite3.

use vibesql_executor::SelectExecutor;
use vibesql_types::SqlValue;

fn query_scalar(db: &vibesql_storage::Database, sql: &str) -> SqlValue {
    let stmt = vibesql_parser::Parser::parse_sql(sql).unwrap();
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let executor = SelectExecutor::new(db);
        let rows = executor
            .execute(&select_stmt)
            .unwrap_or_else(|e| panic!("Query failed: {} -- {:?}", sql, e));
        assert_eq!(rows.len(), 1, "Expected exactly one row for: {}", sql);
        assert_eq!(rows[0].values.len(), 1, "Expected exactly one column for: {}", sql);
        match &rows[0].values[0] {
            SqlValue::Boolean(b) => SqlValue::Integer(*b as i64),
            other => other.clone(),
        }
    } else {
        panic!("Expected SELECT statement: {}", sql);
    }
}

fn assert_query(db: &vibesql_storage::Database, sql: &str, expected: SqlValue) {
    let actual = query_scalar(db, sql);
    assert_eq!(actual, expected, "Query: {} -- expected {:?}, got {:?}", sql, expected, actual);
}

#[test]
fn real_nonzero_is_true() {
    let db = vibesql_storage::Database::new();
    assert_query(&db, "SELECT 0.5 IS TRUE", SqlValue::Integer(1));
    assert_query(&db, "SELECT 1.0 IS TRUE", SqlValue::Integer(1));
    assert_query(&db, "SELECT -2.5 IS TRUE", SqlValue::Integer(1));
}

#[test]
fn real_zero_is_not_true() {
    let db = vibesql_storage::Database::new();
    assert_query(&db, "SELECT 0.0 IS TRUE", SqlValue::Integer(0));
}

#[test]
fn real_zero_is_false() {
    let db = vibesql_storage::Database::new();
    assert_query(&db, "SELECT 0.0 IS FALSE", SqlValue::Integer(1));
}

#[test]
fn real_nonzero_is_not_false() {
    let db = vibesql_storage::Database::new();
    assert_query(&db, "SELECT 0.5 IS FALSE", SqlValue::Integer(0));
}

#[test]
fn real_is_not_true_negation() {
    let db = vibesql_storage::Database::new();
    // 0.5 IS NOT TRUE -> 0 ; 0.0 IS NOT TRUE -> 1
    assert_query(&db, "SELECT 0.5 IS NOT TRUE", SqlValue::Integer(0));
    assert_query(&db, "SELECT 0.0 IS NOT TRUE", SqlValue::Integer(1));
}
