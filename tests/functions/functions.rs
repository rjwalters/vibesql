//! End-to-end integration tests for SQL functions.
//!
//! Tests COALESCE, NULLIF, and other scalar functions.

use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;
use vibesql_storage::{Database, Row};
use vibesql_types::{DataType, SqlValue, StringValue};

/// Execute a SELECT query end-to-end: parse SQL → execute → return results.
fn execute_select(db: &Database, sql: &str) -> Result<Vec<Row>, String> {
    let stmt = Parser::parse_sql(sql).map_err(|e| format!("Parse error: {:?}", e))?;
    let select_stmt = match stmt {
        vibesql_ast::Statement::Select(s) => s,
        other => return Err(format!("Expected SELECT statement, got {:?}", other)),
    };

    let executor = SelectExecutor::new(db);
    executor.execute(&select_stmt).map_err(|e| format!("Execution error: {:?}", e))
}

// ========================================================================
// COALESCE and NULLIF Tests
// ========================================================================

#[test]
fn test_e2e_coalesce_and_nullif() {
    // Test COALESCE and NULLIF scalar functions
    let schema = TableSchema::new(
        "USERS".to_string(),
        vec![
            ColumnSchema::new("ID".to_string(), DataType::Integer, false),
            ColumnSchema::new(
                "NAME".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
            ColumnSchema::new(
                "NICKNAME".to_string(),
                DataType::Varchar { max_length: Some(50) },
                true, // nullable - to test COALESCE with NULL values
            ),
            ColumnSchema::new("BALANCE".to_string(), DataType::Integer, false),
        ],
    );

    let mut db = Database::new();
    db.create_table(schema).unwrap();

    // Insert test data with some NULL values
    db.insert_row(
        "USERS",
        Row::new(vec![
            SqlValue::Integer(1),
            SqlValue::Varchar(StringValue::from("Alice")),
            SqlValue::Varchar(StringValue::from("Ally")),
            SqlValue::Integer(100),
        ]),
    )
    .unwrap();
    db.insert_row(
        "USERS",
        Row::new(vec![
            SqlValue::Integer(2),
            SqlValue::Varchar(StringValue::from("Bob")),
            SqlValue::Null, // NULL nickname
            SqlValue::Integer(0),
        ]),
    )
    .unwrap();
    db.insert_row(
        "USERS",
        Row::new(vec![
            SqlValue::Integer(3),
            SqlValue::Varchar(StringValue::from("Charlie")),
            SqlValue::Varchar(StringValue::from("Chuck")),
            SqlValue::Integer(200),
        ]),
    )
    .unwrap();

    // Test 1: COALESCE with non-NULL value
    let results =
        execute_select(&db, "SELECT COALESCE(nickname, 'Unknown') FROM users WHERE id = 1")
            .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Varchar(StringValue::from("Ally")));

    // Test 2: COALESCE with NULL value - returns second argument
    let results =
        execute_select(&db, "SELECT COALESCE(nickname, 'Unknown') FROM users WHERE id = 2")
            .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Varchar(StringValue::from("Unknown")));

    // Test 3: COALESCE with multiple arguments
    let results =
        execute_select(&db, "SELECT COALESCE(nickname, name, 'Default') FROM users WHERE id = 2")
            .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].values[0],
        SqlValue::Varchar(StringValue::from("Bob")),
        "Should return name when nickname is NULL"
    );

    // Test 4: COALESCE all NULL - returns NULL
    let results =
        execute_select(&db, "SELECT COALESCE(NULL, NULL, NULL) FROM users WHERE id = 1").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Null);

    // Test 5: NULLIF when values are equal - returns NULL
    let results = execute_select(&db, "SELECT NULLIF(balance, 0) FROM users WHERE id = 2").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].values[0],
        SqlValue::Null,
        "NULLIF should return NULL when values are equal"
    );

    // Test 6: NULLIF when values are not equal - returns first value
    let results = execute_select(&db, "SELECT NULLIF(balance, 0) FROM users WHERE id = 1").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(
        results[0].values[0],
        SqlValue::Integer(100),
        "NULLIF should return first value when not equal"
    );

    // Test 7: NULLIF with NULL input
    let results =
        execute_select(&db, "SELECT NULLIF(nickname, 'test') FROM users WHERE id = 2").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Null, "NULLIF with NULL first arg returns NULL");

    // Test 8: Combined COALESCE and NULLIF
    // Use NULLIF to convert 0 balance to NULL, then COALESCE to provide default
    let results =
        execute_select(&db, "SELECT COALESCE(NULLIF(balance, 0), 999) FROM users").unwrap();
    assert_eq!(results.len(), 3);
    assert_eq!(results[0].values[0], SqlValue::Integer(100)); // Alice: 100 != 0
    assert_eq!(results[1].values[0], SqlValue::Integer(999)); // Bob: 0 becomes NULL, COALESCE to 999
    assert_eq!(results[2].values[0], SqlValue::Integer(200)); // Charlie: 200 != 0

    // Test 9: COALESCE in WHERE clause
    let results =
        execute_select(&db, "SELECT name FROM users WHERE COALESCE(nickname, name) = 'Bob'")
            .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Varchar(StringValue::from("Bob")));

    // Test 10: NULLIF with string comparison
    let results =
        execute_select(&db, "SELECT NULLIF(name, 'Alice') FROM users WHERE id = 1").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Null, "NULLIF('Alice', 'Alice') should return NULL");
}
