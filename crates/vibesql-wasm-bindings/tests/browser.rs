//! Browser-based WASM tests
//!
//! Run with: wasm-pack test --headless --firefox crates/vibesql-wasm-bindings

use wasm_bindgen_test::*;
use vibesql_wasm::Database;

wasm_bindgen_test_configure!(run_in_browser);

#[wasm_bindgen_test]
fn test_database_new() {
    let db = Database::new();
    assert_eq!(db.version(), "vibesql-wasm 0.1.0");
}

#[wasm_bindgen_test]
fn test_create_table() {
    let mut db = Database::new();

    // Create a simple table
    let result = db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)".to_string());
    assert!(result.is_ok(), "Failed to create table: {:?}", result);

    // Verify table exists
    let tables = db.list_tables();
    assert!(tables.contains(&"MAIN.USERS".to_string()), "Table not found in: {:?}", tables);
}

#[wasm_bindgen_test]
fn test_insert_and_query() {
    let mut db = Database::new();

    // Create table
    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)".to_string())
        .expect("Failed to create table");

    // Insert data
    db.execute("INSERT INTO users VALUES (1, 'Alice')".to_string())
        .expect("Failed to insert data");

    db.execute("INSERT INTO users VALUES (2, 'Bob')".to_string())
        .expect("Failed to insert data");

    // Query data
    let result = db.query("SELECT * FROM users ORDER BY id".to_string())
        .expect("Failed to query data");

    // Verify result is valid JS value (detailed validation would need JS interop)
    assert!(!result.is_undefined());
    assert!(!result.is_null());
}

#[wasm_bindgen_test]
fn test_describe_table() {
    let mut db = Database::new();

    // Create table with multiple columns
    db.execute("CREATE TABLE products (id INTEGER, name TEXT, price REAL)".to_string())
        .expect("Failed to create table");

    // Describe table
    let result = db.describe_table("products".to_string())
        .expect("Failed to describe table");

    // Verify result is valid
    assert!(!result.is_undefined());
    assert!(!result.is_null());
}

#[wasm_bindgen_test]
fn test_multiple_tables() {
    let mut db = Database::new();

    // Create multiple tables
    db.execute("CREATE TABLE users (id INTEGER PRIMARY KEY)".to_string())
        .expect("Failed to create users table");

    db.execute("CREATE TABLE posts (id INTEGER PRIMARY KEY, user_id INTEGER)".to_string())
        .expect("Failed to create posts table");

    // Verify both tables exist
    let tables = db.list_tables();
    assert!(tables.contains(&"MAIN.USERS".to_string()));
    assert!(tables.contains(&"MAIN.POSTS".to_string()));
    assert_eq!(tables.len(), 2, "Expected 2 tables, found: {:?}", tables);
}

#[wasm_bindgen_test]
fn test_error_handling() {
    let mut db = Database::new();

    // Try to query non-existent table
    let result = db.query("SELECT * FROM nonexistent".to_string());
    assert!(result.is_err(), "Expected error for non-existent table");

    // Try to create duplicate table
    db.execute("CREATE TABLE test (id INTEGER)".to_string())
        .expect("Failed to create table");

    let result = db.execute("CREATE TABLE test (id INTEGER)".to_string());
    assert!(result.is_err(), "Expected error for duplicate table");
}

#[wasm_bindgen_test]
fn test_transactions() {
    let mut db = Database::new();

    // Create table
    db.execute("CREATE TABLE accounts (id INTEGER, balance INTEGER)".to_string())
        .expect("Failed to create table");

    // Insert initial data
    db.execute("INSERT INTO accounts VALUES (1, 100)".to_string())
        .expect("Failed to insert");

    // Start transaction
    db.execute("BEGIN TRANSACTION".to_string())
        .expect("Failed to begin transaction");

    // Update in transaction
    db.execute("UPDATE accounts SET balance = 200 WHERE id = 1".to_string())
        .expect("Failed to update");

    // Commit
    db.execute("COMMIT".to_string())
        .expect("Failed to commit");

    // Verify changes persisted
    let result = db.query("SELECT balance FROM accounts WHERE id = 1".to_string())
        .expect("Failed to query");

    assert!(!result.is_null());
}

#[wasm_bindgen_test]
fn test_example_databases() {
    let mut db = Database::new();

    // Test loading employees example
    let result = db.load_employees();
    assert!(result.is_ok(), "Failed to load employees: {:?}", result);

    // Verify tables were created
    let tables = db.list_tables();
    assert!(!tables.is_empty(), "No tables created by load_employees");

    // Test loading northwind example
    let mut db2 = Database::new();
    let result = db2.load_northwind();
    assert!(result.is_ok(), "Failed to load northwind: {:?}", result);

    // Verify tables were created
    let tables = db2.list_tables();
    assert!(!tables.is_empty(), "No tables created by load_northwind");
}
