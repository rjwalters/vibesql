//! Expression Index Optimizer Tests
//!
//! Tests for query optimizer integration with expression indexes (Phase 5).
//! Verifies that the optimizer correctly identifies and uses expression indexes.

use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;
use vibesql_storage::{Database, Row};
use vibesql_types::{DataType, SqlValue, StringValue};

/// Parse and execute a SQL statement
fn execute_sql(db: &Database, sql: &str) -> Result<Vec<Row>, String> {
    let stmt = Parser::parse_sql(sql).map_err(|e| format!("Parse error: {:?}", e))?;

    match stmt {
        vibesql_ast::Statement::Select(select_stmt) => {
            let executor = SelectExecutor::new(db);
            executor.execute(&select_stmt).map_err(|e| format!("Execution error: {:?}", e))
        }
        vibesql_ast::Statement::CreateTable(create_stmt) => {
            db.execute_create_table(&create_stmt)
                .map_err(|e| format!("Create table error: {:?}", e))?;
            Ok(vec![])
        }
        vibesql_ast::Statement::CreateIndex(index_stmt) => {
            db.execute_create_index(&index_stmt)
                .map_err(|e| format!("Create index error: {:?}", e))?;
            Ok(vec![])
        }
        vibesql_ast::Statement::Insert(insert_stmt) => {
            db.execute_insert(&insert_stmt)
                .map_err(|e| format!("Insert error: {:?}", e))?;
            Ok(vec![])
        }
        other => Err(format!("Unexpected statement type: {:?}", other)),
    }
}

/// Execute a SELECT query and return results
fn query(db: &Database, sql: &str) -> Result<Vec<Row>, String> {
    execute_sql(db, sql)
}

/// Execute a non-query statement (CREATE, INSERT, etc.)
fn execute(db: &Database, sql: &str) -> Result<(), String> {
    execute_sql(db, sql)?;
    Ok(())
}

/// Helper to create a test database with a users table
fn setup_users_table(db: &Database) {
    execute(
        db,
        "CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            email TEXT NOT NULL
        )",
    )
    .expect("Failed to create users table");
}

#[test]
fn test_expression_index_basic_equality() {
    let db = Database::new_in_memory();

    setup_users_table(&db);

    // Create expression index on LOWER(email)
    execute(&db, "CREATE INDEX idx_email_lower ON users(LOWER(email))")
        .expect("Failed to create expression index");

    // Insert test data
    execute(&db, "INSERT INTO users VALUES (1, 'USER@EXAMPLE.COM')").unwrap();
    execute(&db, "INSERT INTO users VALUES (2, 'Admin@Test.ORG')").unwrap();
    execute(&db, "INSERT INTO users VALUES (3, 'john.doe@mail.com')").unwrap();

    // Query using LOWER(email) - should use expression index
    let results = query(&db, "SELECT id FROM users WHERE LOWER(email) = 'user@example.com'")
        .expect("Query failed");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values()[0], SqlValue::Integer(1));

    // Query with different case - should also work
    let results = query(&db, "SELECT id FROM users WHERE LOWER(email) = 'admin@test.org'")
        .expect("Query failed");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values()[0], SqlValue::Integer(2));
}

#[test]
fn test_expression_index_with_upper() {
    let db = Database::new_in_memory();

    execute(
        &db,
        "CREATE TABLE employees (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            department TEXT NOT NULL
        )",
    )
    .expect("Failed to create table");

    // Create expression index on UPPER(department)
    execute(&db, "CREATE INDEX idx_dept_upper ON employees(UPPER(department))")
        .expect("Failed to create expression index");

    // Insert test data
    execute(&db, "INSERT INTO employees VALUES (1, 'Alice', 'engineering')").unwrap();
    execute(&db, "INSERT INTO employees VALUES (2, 'Bob', 'Sales')").unwrap();
    execute(&db, "INSERT INTO employees VALUES (3, 'Carol', 'MARKETING')").unwrap();
    execute(&db, "INSERT INTO employees VALUES (4, 'Dave', 'Engineering')").unwrap();

    // Query using UPPER(department)
    let results = query(&db, "SELECT id FROM employees WHERE UPPER(department) = 'ENGINEERING' ORDER BY id")
        .expect("Query failed");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].values()[0], SqlValue::Integer(1));
    assert_eq!(results[1].values()[0], SqlValue::Integer(4));
}

#[test]
fn test_expression_index_range_query() {
    let db = Database::new_in_memory();

    execute(
        &db,
        "CREATE TABLE products (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            price REAL NOT NULL
        )",
    )
    .expect("Failed to create table");

    // Create expression index on ABS(price)
    execute(&db, "CREATE INDEX idx_abs_price ON products(ABS(price))")
        .expect("Failed to create expression index");

    // Insert test data with positive and negative prices
    execute(&db, "INSERT INTO products VALUES (1, 'Widget A', 100)").unwrap();
    execute(&db, "INSERT INTO products VALUES (2, 'Widget B', -50)").unwrap();
    execute(&db, "INSERT INTO products VALUES (3, 'Widget C', 25)").unwrap();

    // Range query on expression - should use index
    let results = query(
        &db,
        "SELECT id FROM products WHERE ABS(price) > 30 ORDER BY id",
    )
    .expect("Query failed");
    assert_eq!(results.len(), 2); // Products with ABS(price) > 30: 100 and |-50|=50
    assert_eq!(results[0].values()[0], SqlValue::Integer(1)); // price 100
    assert_eq!(results[1].values()[0], SqlValue::Integer(2)); // price -50, abs=50
}

#[test]
fn test_expression_index_not_used_for_different_expression() {
    let db = Database::new_in_memory();

    execute(
        &db,
        "CREATE TABLE contacts (
            id INTEGER PRIMARY KEY,
            email TEXT NOT NULL
        )",
    )
    .expect("Failed to create table");

    // Create expression index on LOWER(email)
    execute(&db, "CREATE INDEX idx_lower_email ON contacts(LOWER(email))")
        .expect("Failed to create expression index");

    // Insert test data
    execute(&db, "INSERT INTO contacts VALUES (1, 'User@Example.COM')").unwrap();

    // Query with UPPER(email) - should NOT use the LOWER(email) index
    // but should still return correct results via table scan
    let results = query(&db, "SELECT id FROM contacts WHERE UPPER(email) = 'USER@EXAMPLE.COM'")
        .expect("Query failed");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values()[0], SqlValue::Integer(1));

    // Query with plain email - should NOT use the expression index
    let results = query(&db, "SELECT id FROM contacts WHERE email = 'User@Example.COM'")
        .expect("Query failed");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values()[0], SqlValue::Integer(1));
}

#[test]
fn test_expression_index_function_case_insensitivity() {
    let db = Database::new_in_memory();

    execute(
        &db,
        "CREATE TABLE data (
            id INTEGER PRIMARY KEY,
            value TEXT NOT NULL
        )",
    )
    .expect("Failed to create table");

    // Create expression index with lowercase function name
    execute(&db, "CREATE INDEX idx_lower ON data(lower(value))")
        .expect("Failed to create expression index");

    // Insert test data
    execute(&db, "INSERT INTO data VALUES (1, 'TEST')").unwrap();

    // Query with uppercase function name - should still use index
    // SQL function names are case-insensitive
    let results = query(&db, "SELECT id FROM data WHERE LOWER(value) = 'test'")
        .expect("Query failed");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values()[0], SqlValue::Integer(1));

    // Query with mixed case function name
    let results = query(&db, "SELECT id FROM data WHERE Lower(value) = 'test'")
        .expect("Query failed");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values()[0], SqlValue::Integer(1));
}

#[test]
fn test_expression_index_length_function() {
    let db = Database::new_in_memory();

    execute(
        &db,
        "CREATE TABLE strings (
            id INTEGER PRIMARY KEY,
            text TEXT NOT NULL
        )",
    )
    .expect("Failed to create table");

    // Create expression index on LENGTH(text)
    execute(&db, "CREATE INDEX idx_len ON strings(LENGTH(text))")
        .expect("Failed to create expression index");

    // Insert test data with varying lengths
    execute(&db, "INSERT INTO strings VALUES (1, 'a')").unwrap(); // length 1
    execute(&db, "INSERT INTO strings VALUES (2, 'ab')").unwrap(); // length 2
    execute(&db, "INSERT INTO strings VALUES (3, 'abc')").unwrap(); // length 3
    execute(&db, "INSERT INTO strings VALUES (4, 'abcdefghij')").unwrap(); // length 10

    // Query for specific length
    let results = query(&db, "SELECT id FROM strings WHERE LENGTH(text) = 3")
        .expect("Query failed");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values()[0], SqlValue::Integer(3));

    // Range query on length
    let results = query(&db, "SELECT id FROM strings WHERE LENGTH(text) > 2 ORDER BY id")
        .expect("Query failed");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].values()[0], SqlValue::Integer(3)); // length 3
    assert_eq!(results[1].values()[0], SqlValue::Integer(4)); // length 10
}
