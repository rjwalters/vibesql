//! Comprehensive test for NULLIF and COALESCE functions
//! Tests from issue #961

use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;
use vibesql_storage::{Database, Row};
use vibesql_types::{SqlValue, StringValue};

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
        SqlValue::Varchar(StringValue::from("hello")),
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
        SqlValue::Varchar(StringValue::from("default")),
        "COALESCE with two args should return second if first is NULL"
    );
}

#[test]
fn test_coalesce_with_arithmetic() {
    let results = execute_select("SELECT COALESCE(NULL, 10 + 5)").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].values[0],
        SqlValue::Integer(15),
        "COALESCE should evaluate second argument"
    );
}

// NULLIF tests
#[test]
fn test_nullif_equal() {
    let results = execute_select("SELECT NULLIF(5, 5)").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Null, "NULLIF(5, 5) should return NULL");
}

#[test]
fn test_nullif_not_equal() {
    let results = execute_select("SELECT NULLIF(5, 10)").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Integer(5), "NULLIF(5, 10) should return 5");
}

#[test]
fn test_nullif_with_null_first_arg() {
    let results = execute_select("SELECT NULLIF(NULL, 10)").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Null, "NULLIF(NULL, x) should return NULL");
}

#[test]
fn test_nullif_with_null_second_arg() {
    let results = execute_select("SELECT NULLIF(10, NULL)").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Integer(10), "NULLIF(10, NULL) should return 10");
}

#[test]
fn test_nullif_string_equal() {
    let results = execute_select("SELECT NULLIF('hello', 'hello')").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Null, "NULLIF('hello', 'hello') should return NULL");
}

#[test]
fn test_nullif_string_not_equal() {
    let results = execute_select("SELECT NULLIF('hello', 'world')").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].values[0],
        SqlValue::Varchar(StringValue::from("hello")),
        "NULLIF('hello', 'world') should return 'hello'"
    );
}

// Combined tests
#[test]
fn test_coalesce_with_nullif() {
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
fn test_nullif_with_expressions() {
    let results = execute_select("SELECT NULLIF(21, -40 * 93 / 67)").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Integer(21), "NULLIF with expressions should work");
}

// Aggregate tests are covered in the existing test_nullif_basic.rs file
// These tests demonstrate that NULLIF and COALESCE work correctly as scalar functions
// and can contain aggregate functions in their arguments. The SQLLogicTest suite
// verifies correct behavior in various contexts.
