//! Test NULLIF function implementation

use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;
use vibesql_storage::{Database, Row};
use vibesql_types::{SqlValue, StringValue};

/// Execute a SELECT query end-to-end: parse SQL → execute → return results.
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

#[test]
fn test_nullif_equal_values() {
    // NULLIF(5, 5) should return NULL
    let results = execute_select("SELECT NULLIF(5, 5)").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Null, "NULLIF(5, 5) should return NULL");
}

#[test]
fn test_nullif_unequal_values() {
    // NULLIF(5, 10) should return 5
    let results = execute_select("SELECT NULLIF(5, 10)").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Integer(5), "NULLIF(5, 10) should return 5");
}

#[test]
fn test_nullif_null_first() {
    // NULLIF(NULL, 10) should return NULL
    let results = execute_select("SELECT NULLIF(NULL, 10)").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Null, "NULLIF(NULL, 10) should return NULL");
}

#[test]
fn test_nullif_null_second() {
    // NULLIF(10, NULL) should return 10
    let results = execute_select("SELECT NULLIF(10, NULL)").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Integer(10), "NULLIF(10, NULL) should return 10");
}

#[test]
fn test_nullif_string_comparison() {
    // NULLIF('hello', 'hello') should return NULL
    let results = execute_select("SELECT NULLIF('hello', 'hello')").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Null, "NULLIF('hello', 'hello') should return NULL");
}

#[test]
fn test_nullif_string_unequal() {
    // NULLIF('hello', 'world') should return 'hello'
    let results = execute_select("SELECT NULLIF('hello', 'world')").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].values[0],
        SqlValue::Varchar(StringValue::from("hello")),
        "NULLIF('hello', 'world') should return 'hello'"
    );
}

#[test]
fn test_nullif_with_expressions() {
    // NULLIF(21, -40 * 93 / 67) should return 21
    let results = execute_select("SELECT NULLIF(21, -40 * 93 / 67)").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].values[0],
        SqlValue::Integer(21),
        "NULLIF(21, calculated_value) should return 21"
    );
}

#[test]
fn test_nullif_with_aggregate_in_group_by() {
    // Create a test case from the issue: SELECT + 28 * 33 - NULLIF(- COUNT(*), + -(- -93))
    // This is a complex expression used in the SQLLogicTest
    let results = execute_select("SELECT 28 * 33 - NULLIF(-COUNT(*), 93)").unwrap();
    assert_eq!(results.len(), 1);
    // 28 * 33 = 924, COUNT(*) = 1, -COUNT(*) = -1, NULLIF(-1, 93) = -1 (not equal)
    // so 924 - (-1) depends on numeric type handling
    // Main goal: verify the function doesn't error and aggregates work within scalar functions
    match &results[0].values[0] {
        SqlValue::Integer(_) | SqlValue::Numeric(_) => {
            // Success - function evaluated without error
        }
        val => panic!("Expected numeric result, got {:?}", val),
    }
}
