//! Integration tests for sqlite_schema/sqlite_master virtual table
//!
//! Issue #4577: These tests verify that schema introspection via the
//! sqlite_schema (and sqlite_master alias) virtual table works correctly.
//!
//! The sqlite_schema table provides SQLite-compatible schema introspection:
//! ```sql
//! SELECT * FROM sqlite_schema;
//! SELECT name FROM sqlite_master WHERE type = 'table';
//! ```

use vibesql_executor::{CreateIndexExecutor, CreateTableExecutor, SelectExecutor, ViewExecutor};
use vibesql_parser::Parser;
use vibesql_storage::Database;

/// Helper to execute a CREATE TABLE statement
fn execute_create_table(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).expect("Failed to parse SQL");
    if let vibesql_ast::Statement::CreateTable(create_stmt) = stmt {
        CreateTableExecutor::execute(&create_stmt, db).expect("Failed to execute CREATE TABLE");
    } else {
        panic!("Expected CREATE TABLE statement");
    }
}

/// Helper to execute a CREATE INDEX statement
fn execute_create_index(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).expect("Failed to parse SQL");
    if let vibesql_ast::Statement::CreateIndex(create_stmt) = stmt {
        CreateIndexExecutor::execute(&create_stmt, db).expect("Failed to execute CREATE INDEX");
    } else {
        panic!("Expected CREATE INDEX statement");
    }
}

/// Helper to execute a CREATE VIEW statement
fn execute_create_view(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).expect("Failed to parse SQL");
    if let vibesql_ast::Statement::CreateView(create_stmt) = stmt {
        ViewExecutor::execute_create_view(&create_stmt, db).expect("Failed to execute CREATE VIEW");
    } else {
        panic!("Expected CREATE VIEW statement");
    }
}

/// Helper to execute a SELECT and return rows
fn execute_select(db: &Database, sql: &str) -> (Vec<String>, Vec<vibesql_storage::Row>) {
    let stmt = Parser::parse_sql(sql).expect("Failed to parse SQL");
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let executor = SelectExecutor::new(db);
        let result = executor.execute_with_columns(&select_stmt).expect("Failed to execute SELECT");
        (result.columns, result.rows)
    } else {
        panic!("Expected SELECT statement");
    }
}

fn setup_database_with_table() -> Database {
    let mut db = Database::new();
    execute_create_table(&mut db, "CREATE TABLE users (id INTEGER, name VARCHAR(100))");
    db
}

/// Test basic SELECT * FROM sqlite_schema
#[test]
fn test_sqlite_schema_select_all() {
    let db = setup_database_with_table();

    let (columns, rows) = execute_select(&db, "SELECT * FROM sqlite_schema");

    // Should have 5 columns: type, name, tbl_name, rootpage, sql
    assert_eq!(columns.len(), 5);
    assert_eq!(columns[0], "type");
    assert_eq!(columns[1], "name");
    assert_eq!(columns[2], "tbl_name");
    assert_eq!(columns[3], "rootpage");
    assert_eq!(columns[4], "sql");

    // Should have at least one row for the users table
    assert!(!rows.is_empty(), "Should have at least one row for the users table");
}

/// Test SELECT * FROM sqlite_master (alias for sqlite_schema)
#[test]
fn test_sqlite_master_select_all() {
    let db = setup_database_with_table();

    let (columns, rows) = execute_select(&db, "SELECT * FROM sqlite_master");

    assert_eq!(columns.len(), 5);
    assert!(!rows.is_empty());
}

/// Test SELECT specific columns FROM sqlite_schema
#[test]
fn test_sqlite_schema_select_columns() {
    let db = setup_database_with_table();

    let (columns, rows) = execute_select(&db, "SELECT name, type FROM sqlite_schema");

    assert_eq!(columns.len(), 2);
    assert_eq!(columns[0], "name");
    assert_eq!(columns[1], "type");
    assert!(!rows.is_empty());
}

/// Test SELECT FROM sqlite_schema with WHERE clause
#[test]
fn test_sqlite_schema_where_clause() {
    let db = setup_database_with_table();

    let (columns, rows) =
        execute_select(&db, "SELECT name FROM sqlite_schema WHERE type = 'table'");

    assert_eq!(columns.len(), 1);
    assert_eq!(columns[0], "name");
    // Should find the users table
    assert!(!rows.is_empty());
}

/// Test case-insensitive access to sqlite_schema
#[test]
fn test_sqlite_schema_case_insensitive() {
    let db = setup_database_with_table();

    // Test various case combinations
    let cases = [
        "SELECT * FROM SQLITE_SCHEMA",
        "SELECT * FROM Sqlite_Schema",
        "SELECT * FROM SQLITE_MASTER",
        "SELECT * FROM Sqlite_Master",
    ];

    for sql in cases {
        let result =
            std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| execute_select(&db, sql)));
        assert!(result.is_ok(), "Query '{}' should succeed", sql);
    }
}

/// Test sqlite_schema includes indexes
#[test]
fn test_sqlite_schema_includes_indexes() {
    let mut db = setup_database_with_table();

    // Create an index
    execute_create_index(&mut db, "CREATE INDEX idx_users_name ON users(name)");

    let (_columns, rows) =
        execute_select(&db, "SELECT name, type FROM sqlite_schema WHERE type = 'index'");

    assert!(!rows.is_empty(), "Should have at least one index row");
}

/// Test sqlite_schema includes views
#[test]
fn test_sqlite_schema_includes_views() {
    let mut db = setup_database_with_table();

    // Create a view
    execute_create_view(&mut db, "CREATE VIEW user_names AS SELECT name FROM users");

    let (_columns, rows) =
        execute_select(&db, "SELECT name, type FROM sqlite_schema WHERE type = 'view'");

    assert!(!rows.is_empty(), "Should have at least one view row");
}

/// Test sqlite_schema with aliased table reference
#[test]
fn test_sqlite_schema_with_alias() {
    let db = setup_database_with_table();

    let (columns, rows) = execute_select(&db, "SELECT s.name, s.type FROM sqlite_schema AS s");

    assert_eq!(columns.len(), 2);
    assert!(!rows.is_empty());
}

/// Test sqlite_schema ORDER BY
#[test]
fn test_sqlite_schema_order_by() {
    let mut db = setup_database_with_table();

    // Create additional objects
    execute_create_index(&mut db, "CREATE INDEX idx_users_id ON users(id)");
    execute_create_view(&mut db, "CREATE VIEW v_users AS SELECT * FROM users");

    let (_columns, rows) =
        execute_select(&db, "SELECT type, name FROM sqlite_schema ORDER BY type, name");

    // Should have multiple rows sorted by type, then by name
    assert!(rows.len() >= 3, "Should have table, index, and view");
}

/// Test sqlite_schema with empty database
#[test]
fn test_sqlite_schema_empty_database() {
    let db = Database::new();

    let (columns, rows) = execute_select(&db, "SELECT * FROM sqlite_schema");

    assert_eq!(columns.len(), 5);
    assert!(rows.is_empty(), "Empty database should have no schema rows");
}

/// Test sqlite_schema in subquery (Issue #4577 acceptance criteria)
#[test]
fn test_sqlite_schema_in_subquery() {
    let db = setup_database_with_table();

    let (columns, rows) = execute_select(
        &db,
        "SELECT * FROM (SELECT name FROM sqlite_schema WHERE type = 'table') AS t",
    );

    assert_eq!(columns.len(), 1);
    assert!(!rows.is_empty());
}
