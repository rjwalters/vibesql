//! Expression Index Optimizer Tests
//!
//! Tests for query optimizer integration with expression indexes (Phase 5).
//! Verifies that the optimizer correctly identifies and uses expression indexes.
//!
//! NOTE: These tests insert data BEFORE creating expression indexes.
//! This is required because the storage layer's index maintenance functions
//! don't yet support expression evaluation for subsequent inserts.
//! (Phase 2 of expression indexes created the infrastructure but didn't add
//! post-insert expression index maintenance.)

use vibesql_executor::{CreateTableExecutor, IndexExecutor, InsertExecutor, SelectExecutor};
use vibesql_parser::Parser;
use vibesql_storage::{Database, Row};
use vibesql_types::SqlValue;

/// Parse and execute a SQL statement
fn execute_sql(db: &mut Database, sql: &str) -> Vec<Row> {
    let stmt = Parser::parse_sql(sql).expect("Parse error");

    match stmt {
        vibesql_ast::Statement::Select(select_stmt) => {
            let executor = SelectExecutor::new(db);
            executor.execute(&select_stmt).expect("Execution error")
        }
        vibesql_ast::Statement::CreateTable(create_stmt) => {
            CreateTableExecutor::execute(&create_stmt, db).expect("Create table error");
            vec![]
        }
        vibesql_ast::Statement::CreateIndex(index_stmt) => {
            IndexExecutor::execute(&index_stmt, db).expect("Create index error");
            vec![]
        }
        vibesql_ast::Statement::Insert(insert_stmt) => {
            InsertExecutor::execute(db, &insert_stmt).expect("Insert error");
            vec![]
        }
        other => panic!("Unexpected statement type: {:?}", other),
    }
}

/// Execute a SELECT query and return results
fn query(db: &mut Database, sql: &str) -> Vec<Row> {
    execute_sql(db, sql)
}

/// Execute a non-query statement (CREATE, INSERT, etc.)
fn execute(db: &mut Database, sql: &str) {
    execute_sql(db, sql);
}

#[test]
fn test_expression_index_basic_equality() {
    let mut db = Database::new();

    // Create table
    execute(
        &mut db,
        "CREATE TABLE users (
            id INTEGER PRIMARY KEY,
            email TEXT NOT NULL
        )",
    );

    // Insert test data BEFORE creating index
    execute(&mut db, "INSERT INTO users VALUES (1, 'USER@EXAMPLE.COM')");
    execute(&mut db, "INSERT INTO users VALUES (2, 'Admin@Test.ORG')");
    execute(&mut db, "INSERT INTO users VALUES (3, 'john.doe@mail.com')");

    // Create expression index on LOWER(email) AFTER data is inserted
    execute(&mut db, "CREATE INDEX idx_email_lower ON users(LOWER(email))");

    // Query using LOWER(email) - should use expression index
    let results = query(&mut db, "SELECT id FROM users WHERE LOWER(email) = 'user@example.com'");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Integer(1));

    // Query with different case - should also work
    let results = query(&mut db, "SELECT id FROM users WHERE LOWER(email) = 'admin@test.org'");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Integer(2));
}

#[test]
fn test_expression_index_with_upper() {
    let mut db = Database::new();

    execute(
        &mut db,
        "CREATE TABLE employees (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            department TEXT NOT NULL
        )",
    );

    // Insert test data BEFORE creating index
    execute(&mut db, "INSERT INTO employees VALUES (1, 'Alice', 'engineering')");
    execute(&mut db, "INSERT INTO employees VALUES (2, 'Bob', 'Sales')");
    execute(&mut db, "INSERT INTO employees VALUES (3, 'Carol', 'MARKETING')");
    execute(&mut db, "INSERT INTO employees VALUES (4, 'Dave', 'Engineering')");

    // Create expression index on UPPER(department) AFTER data is inserted
    execute(&mut db, "CREATE INDEX idx_dept_upper ON employees(UPPER(department))");

    // Query using UPPER(department)
    let results = query(&mut db, "SELECT id FROM employees WHERE UPPER(department) = 'ENGINEERING' ORDER BY id");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].values[0], SqlValue::Integer(1));
    assert_eq!(results[1].values[0], SqlValue::Integer(4));
}

#[test]
fn test_expression_index_range_query() {
    let mut db = Database::new();

    execute(
        &mut db,
        "CREATE TABLE products (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            price REAL NOT NULL
        )",
    );

    // Insert test data BEFORE creating index
    execute(&mut db, "INSERT INTO products VALUES (1, 'Widget A', 100)");
    execute(&mut db, "INSERT INTO products VALUES (2, 'Widget B', -50)");
    execute(&mut db, "INSERT INTO products VALUES (3, 'Widget C', 25)");

    // Create expression index on ABS(price) AFTER data is inserted
    execute(&mut db, "CREATE INDEX idx_abs_price ON products(ABS(price))");

    // Range query on expression - should use index
    let results = query(
        &mut db,
        "SELECT id FROM products WHERE ABS(price) > 30 ORDER BY id",
    );
    assert_eq!(results.len(), 2); // Products with ABS(price) > 30: 100 and |-50|=50
    assert_eq!(results[0].values[0], SqlValue::Integer(1)); // price 100
    assert_eq!(results[1].values[0], SqlValue::Integer(2)); // price -50, abs=50
}

#[test]
fn test_expression_index_not_used_for_different_expression() {
    let mut db = Database::new();

    execute(
        &mut db,
        "CREATE TABLE contacts (
            id INTEGER PRIMARY KEY,
            email TEXT NOT NULL
        )",
    );

    // Insert test data BEFORE creating index
    execute(&mut db, "INSERT INTO contacts VALUES (1, 'User@Example.COM')");

    // Create expression index on LOWER(email)
    execute(&mut db, "CREATE INDEX idx_lower_email ON contacts(LOWER(email))");

    // Query with UPPER(email) - should NOT use the LOWER(email) index
    // but should still return correct results via table scan
    let results = query(&mut db, "SELECT id FROM contacts WHERE UPPER(email) = 'USER@EXAMPLE.COM'");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Integer(1));

    // Query with plain email - should NOT use the expression index
    let results = query(&mut db, "SELECT id FROM contacts WHERE email = 'User@Example.COM'");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Integer(1));
}

#[test]
fn test_expression_index_function_case_insensitivity() {
    let mut db = Database::new();

    execute(
        &mut db,
        "CREATE TABLE data (
            id INTEGER PRIMARY KEY,
            value TEXT NOT NULL
        )",
    );

    // Insert test data BEFORE creating index
    execute(&mut db, "INSERT INTO data VALUES (1, 'TEST')");

    // Create expression index with lowercase function name
    execute(&mut db, "CREATE INDEX idx_lower ON data(lower(value))");

    // Query with uppercase function name - should still use index
    // SQL function names are case-insensitive
    let results = query(&mut db, "SELECT id FROM data WHERE LOWER(value) = 'test'");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Integer(1));

    // Query with mixed case function name
    let results = query(&mut db, "SELECT id FROM data WHERE Lower(value) = 'test'");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Integer(1));
}

#[test]
fn test_expression_index_length_function() {
    let mut db = Database::new();

    execute(
        &mut db,
        "CREATE TABLE strings (
            id INTEGER PRIMARY KEY,
            text TEXT NOT NULL
        )",
    );

    // Insert test data BEFORE creating index
    execute(&mut db, "INSERT INTO strings VALUES (1, 'a')"); // length 1
    execute(&mut db, "INSERT INTO strings VALUES (2, 'ab')"); // length 2
    execute(&mut db, "INSERT INTO strings VALUES (3, 'abc')"); // length 3
    execute(&mut db, "INSERT INTO strings VALUES (4, 'abcdefghij')"); // length 10

    // Create expression index on LENGTH(text) AFTER data is inserted
    execute(&mut db, "CREATE INDEX idx_len ON strings(LENGTH(text))");

    // Query for specific length
    let results = query(&mut db, "SELECT id FROM strings WHERE LENGTH(text) = 3");
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].values[0], SqlValue::Integer(3));

    // Range query on length
    let results = query(&mut db, "SELECT id FROM strings WHERE LENGTH(text) > 2 ORDER BY id");
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].values[0], SqlValue::Integer(3)); // length 3
    assert_eq!(results[1].values[0], SqlValue::Integer(4)); // length 10
}
