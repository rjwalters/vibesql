//! Test COALESCE function implementation
//! Tests from issue #961
//!
//! Note: NULLIF tests are in test_nullif_basic.rs
//! This file focuses on COALESCE and its interaction with NULLIF

use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;
use vibesql_storage::{Database, Row};
use vibesql_types::SqlValue;

fn execute_select(sql: &str) -> Result<Vec<Row>, String> {
    let db = Database::new();
    let stmt = Parser::parse_sql(sql).map_err(|e| format!("Parse error: {:?}", e))?;
    let select_stmt = match stmt {
        vibesql_ast::Statement::Select(s) => s,
        other => return Err(format!("Expected SELECT statement, got {:?}", other)),
    };

    let executor = SelectExecutor::new(&db);
    executor.execute(&select_stmt).map_err(|e| format!("Execution error: {:?}", e))
}

// COALESCE tests
#[test]
fn test_coalesce_first_non_null() {
    let results = execute_select("SELECT COALESCE(NULL, 'hello', 'world')").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].values[0],
        SqlValue::Varchar(std::sync::Arc::from("hello")),
        "COALESCE should return first non-NULL value"
    );
}

#[test]
fn test_coalesce_all_null() {
    let results = execute_select("SELECT COALESCE(NULL, NULL, NULL)").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].values[0],
        SqlValue::Null,
        "COALESCE should return NULL when all arguments are NULL"
    );
}

#[test]
fn test_coalesce_first_not_null() {
    let results = execute_select("SELECT COALESCE(42, NULL, 100)").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].values[0],
        SqlValue::Integer(42),
        "COALESCE should return first non-NULL value"
    );
}

#[test]
fn test_coalesce_two_args() {
    let results = execute_select("SELECT COALESCE(NULL, 'default')").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].values[0],
        SqlValue::Varchar(std::sync::Arc::from("default")),
        "COALESCE with two args should return second if first is NULL"
    );
}

#[test]
fn test_coalesce_with_arithmetic() {
    let results = execute_select("SELECT COALESCE(NULL, 10 + 5)").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Integer(15), "COALESCE should evaluate expressions");
}

// Combined tests with NULLIF
#[test]
fn test_coalesce_with_nullif_equal() {
    let results = execute_select("SELECT COALESCE(NULLIF(5, 5), 10)").unwrap();
    assert_eq!(results.len(), 1);
    // NULLIF(5, 5) returns NULL, so COALESCE returns 10
    assert_eq!(
        results[0].values[0],
        SqlValue::Integer(10),
        "COALESCE(NULLIF(5, 5), 10) should return 10"
    );
}

#[test]
fn test_coalesce_with_nullif_not_equal() {
    let results = execute_select("SELECT COALESCE(NULLIF(5, 10), 99)").unwrap();
    assert_eq!(results.len(), 1);
    // NULLIF(5, 10) returns 5 (not equal), so COALESCE returns 5
    assert_eq!(
        results[0].values[0],
        SqlValue::Integer(5),
        "COALESCE(NULLIF(5, 10), 99) should return 5"
    );
}
