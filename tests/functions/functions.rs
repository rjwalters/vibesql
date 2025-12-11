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

// ========================================================================
// SQLite-compatible scalar MIN/MAX Tests (multi-argument form)
// ========================================================================

#[test]
fn test_e2e_scalar_min_max() {
    // Test SQLite-compatible multi-argument MIN/MAX functions
    let schema = TableSchema::new(
        "TEST1".to_string(),
        vec![
            ColumnSchema::new("F1".to_string(), DataType::Integer, false),
            ColumnSchema::new("F2".to_string(), DataType::Integer, false),
        ],
    );

    let mut db = Database::new();
    db.create_table(schema).unwrap();

    db.insert_row("TEST1", Row::new(vec![SqlValue::Integer(11), SqlValue::Integer(22)])).unwrap();
    db.insert_row("TEST1", Row::new(vec![SqlValue::Integer(33), SqlValue::Integer(44)])).unwrap();

    // Test 1: Basic scalar MIN with literals
    let results = execute_select(&db, "SELECT min(11, 22)").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Integer(11), "min(11, 22) should return 11");

    // Test 2: Basic scalar MAX with literals
    let results = execute_select(&db, "SELECT max(11, 22)").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Integer(22), "max(11, 22) should return 22");

    // Test 3: Scalar MIN/MAX with three arguments
    let results = execute_select(&db, "SELECT min(1, 2, 3)").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Integer(1), "min(1, 2, 3) should return 1");

    let results = execute_select(&db, "SELECT max(1, 2, 3)").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Integer(3), "max(1, 2, 3) should return 3");

    // Test 4: Scalar MIN/MAX with floating point values
    // Note: Numeric literals like 1.1 are parsed as Numeric, not Double
    let results = execute_select(&db, "SELECT min(1.1, 2.2)").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Numeric(1.1), "min(1.1, 2.2) should return 1.1");

    let results = execute_select(&db, "SELECT max(1.1, 2.2)").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Numeric(2.2), "max(1.1, 2.2) should return 2.2");

    // Test 5: Scalar MIN/MAX with column references
    let results = execute_select(&db, "SELECT min(f1, f2) FROM test1").unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].values[0], SqlValue::Integer(11), "min(11, 22) should return 11");
    assert_eq!(results[1].values[0], SqlValue::Integer(33), "min(33, 44) should return 33");

    let results = execute_select(&db, "SELECT max(f1, f2) FROM test1").unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].values[0], SqlValue::Integer(22), "max(11, 22) should return 22");
    assert_eq!(results[1].values[0], SqlValue::Integer(44), "max(33, 44) should return 44");

    // Test 6: Combined *, min, max in SELECT
    let results = execute_select(&db, "SELECT *, min(f1, f2), max(f1, f2) FROM test1").unwrap();
    assert_eq!(results.len(), 2);
    // Row 1: f1=11, f2=22, min=11, max=22
    assert_eq!(results[0].values[0], SqlValue::Integer(11));
    assert_eq!(results[0].values[1], SqlValue::Integer(22));
    assert_eq!(results[0].values[2], SqlValue::Integer(11));
    assert_eq!(results[0].values[3], SqlValue::Integer(22));
    // Row 2: f1=33, f2=44, min=33, max=44
    assert_eq!(results[1].values[0], SqlValue::Integer(33));
    assert_eq!(results[1].values[1], SqlValue::Integer(44));
    assert_eq!(results[1].values[2], SqlValue::Integer(33));
    assert_eq!(results[1].values[3], SqlValue::Integer(44));

    // Test 7: Single-argument MIN/MAX should still work as aggregate
    let results = execute_select(&db, "SELECT min(f1) FROM test1").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Integer(11), "min(f1) aggregate should return 11");

    let results = execute_select(&db, "SELECT max(f1) FROM test1").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Integer(33), "max(f1) aggregate should return 33");
}

#[test]
fn test_e2e_scalar_min_max_null_semantics() {
    // Test SQLite NULL semantics: scalar min/max return NULL if ANY argument is NULL
    let schema = TableSchema::new(
        "NULLTEST".to_string(),
        vec![
            ColumnSchema::new("A".to_string(), DataType::Integer, true),
            ColumnSchema::new("B".to_string(), DataType::Integer, true),
        ],
    );

    let mut db = Database::new();
    db.create_table(schema).unwrap();

    db.insert_row("NULLTEST", Row::new(vec![SqlValue::Integer(1), SqlValue::Null])).unwrap();
    db.insert_row("NULLTEST", Row::new(vec![SqlValue::Null, SqlValue::Integer(2)])).unwrap();
    db.insert_row("NULLTEST", Row::new(vec![SqlValue::Integer(3), SqlValue::Integer(4)])).unwrap();

    // Test 1: Scalar MIN with NULL argument returns NULL
    let results = execute_select(&db, "SELECT min(a, b) FROM nulltest WHERE a = 1").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Null, "min(1, NULL) should return NULL");

    let results = execute_select(&db, "SELECT min(a, b) FROM nulltest WHERE b = 2").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Null, "min(NULL, 2) should return NULL");

    // Test 2: Scalar MAX with NULL argument returns NULL
    let results = execute_select(&db, "SELECT max(a, b) FROM nulltest WHERE a = 1").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Null, "max(1, NULL) should return NULL");

    // Test 3: Scalar MIN/MAX without NULLs works normally
    let results = execute_select(&db, "SELECT min(a, b) FROM nulltest WHERE a = 3").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Integer(3), "min(3, 4) should return 3");

    let results = execute_select(&db, "SELECT max(a, b) FROM nulltest WHERE a = 3").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Integer(4), "max(3, 4) should return 4");

    // Test 4: Aggregate MIN/MAX should skip NULLs (different behavior)
    let results = execute_select(&db, "SELECT min(a) FROM nulltest").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Integer(1), "Aggregate min should skip NULLs");

    let results = execute_select(&db, "SELECT max(b) FROM nulltest").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Integer(4), "Aggregate max should skip NULLs");
}
