//! Integration tests for expression index DML maintenance
//!
//! These tests verify that expression indexes are correctly maintained during
//! INSERT, UPDATE, and DELETE operations.
//!
//! Expression indexes store pre-computed expression values (e.g., LOWER(name))
//! and must be updated whenever the underlying data changes.

use vibesql_parser::Parser;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

use crate::{
    CreateIndexExecutor, CreateTableExecutor, DeleteExecutor, InsertExecutor, UpdateExecutor,
};

/// Create a test database
fn create_test_db() -> Database {
    let mut db = Database::new();
    db.catalog.set_case_sensitive_identifiers(false);
    db
}

/// Helper to execute SQL statement
fn execute_sql(db: &mut Database, sql: &str) -> Result<usize, crate::errors::ExecutorError> {
    let stmt = Parser::parse_sql(sql).map_err(|e| {
        crate::errors::ExecutorError::UnsupportedExpression(format!("Parse error: {:?}", e))
    })?;

    match stmt {
        vibesql_ast::Statement::CreateTable(s) => {
            CreateTableExecutor::execute(&s, db)?;
            Ok(0)
        }
        vibesql_ast::Statement::CreateIndex(s) => {
            CreateIndexExecutor::execute(&s, db)?;
            Ok(0)
        }
        vibesql_ast::Statement::Insert(s) => InsertExecutor::execute(db, &s),
        vibesql_ast::Statement::Update(s) => UpdateExecutor::execute(&s, db),
        vibesql_ast::Statement::Delete(s) => DeleteExecutor::execute(&s, db),
        _ => Err(crate::errors::ExecutorError::UnsupportedExpression(
            "Unsupported statement type".to_string(),
        )),
    }
}

#[test]
fn test_expression_index_insert_maintenance() {
    let mut db = create_test_db();

    // Create users table
    execute_sql(&mut db, "CREATE TABLE users (id INTEGER, name VARCHAR(100), email VARCHAR(100))")
        .unwrap();

    // Create expression index on LOWER(name)
    execute_sql(&mut db, "CREATE INDEX idx_lower_name ON users (LOWER(name))").unwrap();

    // Insert a row
    execute_sql(
        &mut db,
        "INSERT INTO users (id, name, email) VALUES (1, 'Alice', 'alice@example.com')",
    )
    .unwrap();

    // Verify the expression index exists and table has data
    assert!(db.has_expression_indexes("users"), "Table should have expression indexes");
    assert_eq!(db.get_table("users").unwrap().row_count(), 1);

    // Verify the index was populated
    let index_data = db.get_index_data("idx_lower_name");
    assert!(index_data.is_some(), "Expression index data should exist");
}

#[test]
fn test_expression_index_update_maintenance() {
    let mut db = create_test_db();

    // Create table and index
    execute_sql(&mut db, "CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(100))").unwrap();
    execute_sql(&mut db, "CREATE INDEX idx_lower_name ON users (LOWER(name))").unwrap();

    // Insert a row
    execute_sql(&mut db, "INSERT INTO users (id, name) VALUES (1, 'Alice')").unwrap();

    // Verify initial state
    {
        let index_data = db.get_index_data("idx_lower_name").unwrap();
        let alice_key = vec![SqlValue::Varchar(arcstr::ArcStr::from("alice"))];
        assert!(
            index_data.contains_key(&alice_key),
            "Initial 'alice' key should be in expression index"
        );
    }

    // Update the name
    let result = execute_sql(&mut db, "UPDATE users SET name = 'Bob' WHERE id = 1");
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 1);

    // Verify the expression index was updated
    let index_data = db.get_index_data("idx_lower_name").unwrap();

    // Old key "alice" should be removed
    let old_key = vec![SqlValue::Varchar(arcstr::ArcStr::from("alice"))];
    assert!(
        !index_data.contains_key(&old_key),
        "Old key 'alice' should be removed from expression index after UPDATE"
    );

    // New key "bob" should be present
    let new_key = vec![SqlValue::Varchar(arcstr::ArcStr::from("bob"))];
    assert!(
        index_data.contains_key(&new_key),
        "New key 'bob' should be added to expression index after UPDATE"
    );
}

#[test]
fn test_expression_index_delete_maintenance() {
    let mut db = create_test_db();

    // Create table and index
    execute_sql(&mut db, "CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(100))").unwrap();
    execute_sql(&mut db, "CREATE INDEX idx_lower_name ON users (LOWER(name))").unwrap();

    // Insert rows
    execute_sql(&mut db, "INSERT INTO users (id, name) VALUES (1, 'Alice')").unwrap();
    execute_sql(&mut db, "INSERT INTO users (id, name) VALUES (2, 'Bob')").unwrap();

    // Verify both keys exist
    {
        let index_data = db.get_index_data("idx_lower_name").unwrap();
        let alice_key = vec![SqlValue::Varchar(arcstr::ArcStr::from("alice"))];
        let bob_key = vec![SqlValue::Varchar(arcstr::ArcStr::from("bob"))];
        assert!(index_data.contains_key(&alice_key), "Alice key should exist before delete");
        assert!(index_data.contains_key(&bob_key), "Bob key should exist before delete");
    }

    // Debug: check row count before delete
    let row_count_before = db.get_table("users").unwrap().row_count();

    // Delete Alice
    let result = execute_sql(&mut db, "DELETE FROM users WHERE id = 1");
    assert!(result.is_ok(), "DELETE should succeed");
    assert_eq!(result.unwrap(), 1, "Should delete 1 row");

    // Debug: check row count after delete
    let row_count_after = db.get_table("users").unwrap().row_count();
    assert_eq!(row_count_before - 1, row_count_after, "Row count should decrease by 1");

    // Verify the expression index was updated
    let index_data = db.get_index_data("idx_lower_name").unwrap();

    // Alice's key should be removed
    let alice_key = vec![SqlValue::Varchar(arcstr::ArcStr::from("alice"))];
    assert!(
        !index_data.contains_key(&alice_key),
        "Alice's key should be removed from expression index after DELETE"
    );

    // Bob's key should still be present
    let bob_key = vec![SqlValue::Varchar(arcstr::ArcStr::from("bob"))];
    assert!(index_data.contains_key(&bob_key), "Bob's key should still be in expression index");
}

#[test]
fn test_expression_index_batch_insert_maintenance() {
    let mut db = create_test_db();

    // Create table and index
    execute_sql(&mut db, "CREATE TABLE users (id INTEGER, name VARCHAR(100))").unwrap();
    execute_sql(&mut db, "CREATE INDEX idx_lower_name ON users (LOWER(name))").unwrap();

    // Batch insert multiple rows
    let result = execute_sql(
        &mut db,
        "INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob'), (3, 'Charlie')",
    );
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 3);

    // Verify all expression index entries were created
    let index_data = db.get_index_data("idx_lower_name").unwrap();

    let alice_key = vec![SqlValue::Varchar(arcstr::ArcStr::from("alice"))];
    let bob_key = vec![SqlValue::Varchar(arcstr::ArcStr::from("bob"))];
    let charlie_key = vec![SqlValue::Varchar(arcstr::ArcStr::from("charlie"))];

    assert!(index_data.contains_key(&alice_key), "Alice's key should be in index");
    assert!(index_data.contains_key(&bob_key), "Bob's key should be in index");
    assert!(index_data.contains_key(&charlie_key), "Charlie's key should be in index");
}

#[test]
fn test_expression_index_replace_maintenance() {
    let mut db = create_test_db();

    // Create table with PRIMARY KEY for REPLACE testing
    execute_sql(&mut db, "CREATE TABLE products (id INTEGER PRIMARY KEY, name VARCHAR(100))")
        .unwrap();

    // Create expression index on LOWER(name)
    execute_sql(&mut db, "CREATE INDEX idx_lower_product ON products (LOWER(name))").unwrap();

    // Insert a row
    execute_sql(&mut db, "INSERT INTO products (id, name) VALUES (1, 'Widget')").unwrap();

    // Verify initial key
    {
        let index_data = db.get_index_data("idx_lower_product").unwrap();
        let widget_key = vec![SqlValue::Varchar(arcstr::ArcStr::from("widget"))];
        assert!(index_data.contains_key(&widget_key), "Widget key should be in index");
    }

    // REPLACE with same id but different name
    execute_sql(&mut db, "INSERT OR REPLACE INTO products (id, name) VALUES (1, 'Gadget')")
        .unwrap();

    // Verify the expression index was updated
    let index_data = db.get_index_data("idx_lower_product").unwrap();

    // Old key should be removed
    let widget_key = vec![SqlValue::Varchar(arcstr::ArcStr::from("widget"))];
    assert!(
        !index_data.contains_key(&widget_key),
        "Old 'widget' key should be removed after REPLACE"
    );

    // New key should be present
    let gadget_key = vec![SqlValue::Varchar(arcstr::ArcStr::from("gadget"))];
    assert!(index_data.contains_key(&gadget_key), "New 'gadget' key should be added after REPLACE");
}

#[test]
fn test_no_expression_index_fast_path() {
    // Test that tables without expression indexes don't incur overhead
    let mut db = create_test_db();

    // Create table without expression index
    execute_sql(&mut db, "CREATE TABLE users (id INTEGER, name VARCHAR(100))").unwrap();

    // Verify no expression indexes exist
    assert!(!db.has_expression_indexes("users"));

    // Insert a row - should not call expression index maintenance
    execute_sql(&mut db, "INSERT INTO users (id, name) VALUES (1, 'Alice')").unwrap();

    // Verify table has data but no expression indexes
    assert_eq!(db.get_table("users").unwrap().row_count(), 1);
    assert!(!db.has_expression_indexes("users"));
}

// Note: Binary expression indexes (e.g., price * quantity) are not yet supported
// by the parser. This test is commented out until parser support is added.
// See issue #XXXX for expression index parser enhancements.
#[test]
#[ignore = "Parser does not yet support binary expression indexes (price * quantity)"]
fn test_expression_index_with_math_expression() {
    let mut db = create_test_db();

    // Create table with numeric columns
    execute_sql(
        &mut db,
        "CREATE TABLE orders (id INTEGER PRIMARY KEY, price INTEGER, quantity INTEGER)",
    )
    .unwrap();

    // Create expression index on price * quantity
    execute_sql(&mut db, "CREATE INDEX idx_total ON orders (price * quantity)").unwrap();

    // Insert rows
    execute_sql(&mut db, "INSERT INTO orders (id, price, quantity) VALUES (1, 10, 5)").unwrap();
    execute_sql(&mut db, "INSERT INTO orders (id, price, quantity) VALUES (2, 20, 3)").unwrap();

    // Verify expression index was populated
    assert!(db.has_expression_indexes("orders"));
    let index_data = db.get_index_data("idx_total").unwrap();

    // Check for computed values: 10*5=50 and 20*3=60
    let key_50 = vec![SqlValue::Integer(50)];
    let key_60 = vec![SqlValue::Integer(60)];
    assert!(index_data.contains_key(&key_50), "Key 50 (10*5) should be in index");
    assert!(index_data.contains_key(&key_60), "Key 60 (20*3) should be in index");
}
