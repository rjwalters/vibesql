//! Tests for SET DEFAULT foreign key constraint actions
//!
//! These tests verify that default value expressions are properly evaluated
//! when foreign key constraints trigger SET DEFAULT actions.

use vibesql_executor::{CreateTableExecutor, DeleteExecutor, InsertExecutor, UpdateExecutor};
use vibesql_parser::Parser;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

/// Helper to execute SQL and return the database
fn execute_sql(sql: &str) -> Database {
    let mut db = Database::new();
    // Split by semicolon and parse/execute each statement
    for sql_stmt in sql.split(';') {
        let trimmed = sql_stmt.trim();
        if trimmed.is_empty() {
            continue;
        }
        let stmt = Parser::parse_sql(trimmed).expect("Failed to parse SQL");
        execute_statement(&stmt, &mut db);
    }
    db
}

/// Execute a single statement
fn execute_statement(stmt: &vibesql_ast::Statement, db: &mut Database) {
    use vibesql_ast::Statement;
    match stmt {
        Statement::CreateTable(create_stmt) => {
            CreateTableExecutor::execute(create_stmt, db).expect("Failed to execute CREATE TABLE");
        }
        Statement::Insert(insert_stmt) => {
            InsertExecutor::execute(db, insert_stmt).expect("Failed to execute INSERT");
        }
        Statement::Delete(delete_stmt) => {
            DeleteExecutor::execute(delete_stmt, db).expect("Failed to execute DELETE");
        }
        Statement::Update(update_stmt) => {
            UpdateExecutor::execute(update_stmt, db).expect("Failed to execute UPDATE");
        }
        _ => panic!("Unsupported statement type"),
    }
}

/// Helper to get all rows from a table
fn get_all_rows(db: &Database, table_name: &str) -> Vec<Vec<SqlValue>> {
    let table = db.get_table(table_name).expect("Table not found");
    table.scan().iter().map(|row| row.values.to_vec()).collect()
}

#[test]
fn test_on_delete_set_default_literal_integer() {
    // Test ON DELETE SET DEFAULT with a literal integer default
    let db = execute_sql(
        r#"
        CREATE TABLE parent (id INTEGER PRIMARY KEY);
        CREATE TABLE child (
            id INTEGER PRIMARY KEY,
            parent_id INTEGER DEFAULT 0,
            FOREIGN KEY (parent_id) REFERENCES parent(id) ON DELETE SET DEFAULT
        );
        INSERT INTO parent VALUES (0);
        INSERT INTO parent VALUES (1);
        INSERT INTO parent VALUES (2);
        INSERT INTO child VALUES (1, 1);
        INSERT INTO child VALUES (2, 2);
        "#,
    );

    // Verify initial state
    let rows = get_all_rows(&db, "child");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], vec![SqlValue::Integer(1), SqlValue::Integer(1)]);
    assert_eq!(rows[1], vec![SqlValue::Integer(2), SqlValue::Integer(2)]);

    // Delete parent row with id=2
    let mut db = db;
    let stmt = Parser::parse_sql("DELETE FROM parent WHERE id = 2").unwrap();
    execute_statement(&stmt, &mut db);

    // Child row should now reference parent_id = 0 (the default)
    let rows = get_all_rows(&db, "child");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], vec![SqlValue::Integer(1), SqlValue::Integer(1)]);
    assert_eq!(rows[1], vec![SqlValue::Integer(2), SqlValue::Integer(0)]);
}

#[test]
fn test_on_delete_set_default_literal_string() {
    // Test ON DELETE SET DEFAULT with a literal string default
    let db = execute_sql(
        r#"
        CREATE TABLE parent (code VARCHAR(10) PRIMARY KEY);
        CREATE TABLE child (
            id INTEGER PRIMARY KEY,
            parent_code VARCHAR(10) DEFAULT 'NONE',
            FOREIGN KEY (parent_code) REFERENCES parent(code) ON DELETE SET DEFAULT
        );
        INSERT INTO parent VALUES ('NONE');
        INSERT INTO parent VALUES ('A');
        INSERT INTO parent VALUES ('B');
        INSERT INTO child VALUES (1, 'A');
        INSERT INTO child VALUES (2, 'B');
        "#,
    );

    // Delete parent row with code='B'
    let mut db = db;
    let stmt = Parser::parse_sql("DELETE FROM parent WHERE code = 'B'").unwrap();
    execute_statement(&stmt, &mut db);

    // Child row should now reference parent_code = 'NONE' (the default)
    let rows = get_all_rows(&db, "child");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("A"))]);
    assert_eq!(rows[1], vec![SqlValue::Integer(2), SqlValue::Varchar(arcstr::ArcStr::from("NONE"))]);
}

#[test]
fn test_on_delete_set_default_null() {
    // Test ON DELETE SET DEFAULT when no default is specified (should use NULL)
    let db = execute_sql(
        r#"
        CREATE TABLE parent (id INTEGER PRIMARY KEY);
        CREATE TABLE child (
            id INTEGER PRIMARY KEY,
            parent_id INTEGER,
            FOREIGN KEY (parent_id) REFERENCES parent(id) ON DELETE SET DEFAULT
        );
        INSERT INTO parent VALUES (1);
        INSERT INTO parent VALUES (2);
        INSERT INTO child VALUES (1, 1);
        INSERT INTO child VALUES (2, 2);
        "#,
    );

    // Delete parent row with id=2
    let mut db = db;
    let stmt = Parser::parse_sql("DELETE FROM parent WHERE id = 2").unwrap();
    execute_statement(&stmt, &mut db);

    // Child row should now have parent_id = NULL (no default specified)
    let rows = get_all_rows(&db, "child");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], vec![SqlValue::Integer(1), SqlValue::Integer(1)]);
    assert_eq!(rows[1], vec![SqlValue::Integer(2), SqlValue::Null]);
}

#[test]
fn test_on_update_set_default_literal() {
    // Test ON UPDATE SET DEFAULT with a literal default
    let db = execute_sql(
        r#"
        CREATE TABLE parent (id INTEGER PRIMARY KEY);
        CREATE TABLE child (
            id INTEGER PRIMARY KEY,
            parent_id INTEGER DEFAULT 99,
            FOREIGN KEY (parent_id) REFERENCES parent(id) ON UPDATE SET DEFAULT
        );
        INSERT INTO parent VALUES (99);
        INSERT INTO parent VALUES (1);
        INSERT INTO parent VALUES (2);
        INSERT INTO child VALUES (1, 1);
        INSERT INTO child VALUES (2, 2);
        "#,
    );

    // Update parent row id from 2 to 3
    let mut db = db;
    let stmt = Parser::parse_sql("UPDATE parent SET id = 3 WHERE id = 2").unwrap();
    execute_statement(&stmt, &mut db);

    // Child row should now reference parent_id = 99 (the default)
    let rows = get_all_rows(&db, "child");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], vec![SqlValue::Integer(1), SqlValue::Integer(1)]);
    assert_eq!(rows[1], vec![SqlValue::Integer(2), SqlValue::Integer(99)]);
}

#[test]
fn test_on_delete_set_default_composite_key() {
    // Test ON DELETE SET DEFAULT with composite foreign key
    let db = execute_sql(
        r#"
        CREATE TABLE parent (id1 INTEGER, id2 INTEGER, PRIMARY KEY (id1, id2));
        CREATE TABLE child (
            id INTEGER PRIMARY KEY,
            parent_id1 INTEGER DEFAULT 0,
            parent_id2 INTEGER DEFAULT 0,
            FOREIGN KEY (parent_id1, parent_id2) REFERENCES parent(id1, id2) ON DELETE SET DEFAULT
        );
        INSERT INTO parent VALUES (0, 0);
        INSERT INTO parent VALUES (1, 1);
        INSERT INTO parent VALUES (2, 2);
        INSERT INTO child VALUES (1, 1, 1);
        INSERT INTO child VALUES (2, 2, 2);
        "#,
    );

    // Delete parent row (2, 2)
    let mut db = db;
    let stmt = Parser::parse_sql("DELETE FROM parent WHERE id1 = 2 AND id2 = 2").unwrap();
    execute_statement(&stmt, &mut db);

    // Child row should now reference (0, 0) - the defaults
    let rows = get_all_rows(&db, "child");
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0], vec![SqlValue::Integer(1), SqlValue::Integer(1), SqlValue::Integer(1)]);
    assert_eq!(rows[1], vec![SqlValue::Integer(2), SqlValue::Integer(0), SqlValue::Integer(0)]);
}

#[test]
fn test_on_delete_set_default_multiple_child_rows() {
    // Test ON DELETE SET DEFAULT affects multiple child rows
    let db = execute_sql(
        r#"
        CREATE TABLE parent (id INTEGER PRIMARY KEY);
        CREATE TABLE child (
            id INTEGER PRIMARY KEY,
            parent_id INTEGER DEFAULT 0,
            FOREIGN KEY (parent_id) REFERENCES parent(id) ON DELETE SET DEFAULT
        );
        INSERT INTO parent VALUES (0);
        INSERT INTO parent VALUES (1);
        INSERT INTO child VALUES (1, 1);
        INSERT INTO child VALUES (2, 1);
        INSERT INTO child VALUES (3, 1);
        "#,
    );

    // Delete parent row with id=1
    let mut db = db;
    let stmt = Parser::parse_sql("DELETE FROM parent WHERE id = 1").unwrap();
    execute_statement(&stmt, &mut db);

    // All child rows should now reference parent_id = 0
    let rows = get_all_rows(&db, "child");
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0], vec![SqlValue::Integer(1), SqlValue::Integer(0)]);
    assert_eq!(rows[1], vec![SqlValue::Integer(2), SqlValue::Integer(0)]);
    assert_eq!(rows[2], vec![SqlValue::Integer(3), SqlValue::Integer(0)]);
}

#[test]
fn test_on_delete_set_default_no_child_rows() {
    // Test ON DELETE SET DEFAULT when no child rows reference the parent
    let db = execute_sql(
        r#"
        CREATE TABLE parent (id INTEGER PRIMARY KEY);
        CREATE TABLE child (
            id INTEGER PRIMARY KEY,
            parent_id INTEGER DEFAULT 0,
            FOREIGN KEY (parent_id) REFERENCES parent(id) ON DELETE SET DEFAULT
        );
        INSERT INTO parent VALUES (0);
        INSERT INTO parent VALUES (1);
        INSERT INTO parent VALUES (2);
        INSERT INTO child VALUES (1, 1);
        "#,
    );

    // Delete parent row with id=2 (no child rows reference it)
    let mut db = db;
    let stmt = Parser::parse_sql("DELETE FROM parent WHERE id = 2").unwrap();
    execute_statement(&stmt, &mut db);

    // Child row should be unchanged
    let rows = get_all_rows(&db, "child");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0], vec![SqlValue::Integer(1), SqlValue::Integer(1)]);
}
