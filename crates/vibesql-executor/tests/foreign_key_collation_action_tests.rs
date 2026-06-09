//! Tests for collation-aware FK action helpers (#5147)
//!
//! Verifies that ON DELETE / ON UPDATE action helpers (CASCADE, SET NULL,
//! SET DEFAULT) honor the parent column's declared collation (NOCASE, RTRIM)
//! when matching child rows. Prior to #5147 these helpers used strict `==`
//! comparisons, which caused FK actions to silently skip rows that the
//! integrity check had already flagged as referring to the deleted/updated
//! parent.

use vibesql_executor::{CreateTableExecutor, DeleteExecutor, InsertExecutor, UpdateExecutor};
use vibesql_parser::Parser;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

fn execute_sql(sql: &str) -> Database {
    let mut db = Database::new();
    db.set_foreign_keys_enabled(true);
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

fn get_all_rows(db: &Database, table_name: &str) -> Vec<Vec<SqlValue>> {
    let table = db.get_table(table_name).expect("Table not found");
    table.scan().iter().map(|row| row.values.to_vec()).collect()
}

// ---------------------------------------------------------------------------
// ON DELETE CASCADE
// ---------------------------------------------------------------------------

#[test]
fn on_delete_cascade_honors_nocase_collation() {
    // Parent stores 'A' with NOCASE; child references via lowercase 'a'.
    // Under strict ==, the cascade helper would not match 'a' to 'A' and
    // the child row would stick around after DELETE FROM parent.
    let mut db = execute_sql(
        r#"
        CREATE TABLE parent (code VARCHAR(10) COLLATE NOCASE PRIMARY KEY);
        CREATE TABLE child (
            id INTEGER PRIMARY KEY,
            parent_code VARCHAR(10),
            FOREIGN KEY (parent_code) REFERENCES parent(code) ON DELETE CASCADE
        );
        INSERT INTO parent VALUES ('A');
        INSERT INTO child VALUES (1, 'a');
        "#,
    );

    let stmt = Parser::parse_sql("DELETE FROM parent WHERE code = 'A'").unwrap();
    execute_statement(&stmt, &mut db);

    // Child row should be cascade-deleted even though 'a' != 'A' under strict equality.
    let rows = get_all_rows(&db, "child");
    assert!(rows.is_empty(), "expected child row to be cascade-deleted, got {:?}", rows);
}

#[test]
fn on_delete_cascade_honors_rtrim_collation() {
    let mut db = execute_sql(
        r#"
        CREATE TABLE parent (code VARCHAR(10) COLLATE RTRIM PRIMARY KEY);
        CREATE TABLE child (
            id INTEGER PRIMARY KEY,
            parent_code VARCHAR(10),
            FOREIGN KEY (parent_code) REFERENCES parent(code) ON DELETE CASCADE
        );
        INSERT INTO parent VALUES ('abc');
        INSERT INTO child VALUES (1, 'abc   ');
        "#,
    );

    let stmt = Parser::parse_sql("DELETE FROM parent WHERE code = 'abc'").unwrap();
    execute_statement(&stmt, &mut db);

    let rows = get_all_rows(&db, "child");
    assert!(rows.is_empty(), "expected RTRIM cascade-delete, got {:?}", rows);
}

// ---------------------------------------------------------------------------
// ON DELETE SET NULL
// ---------------------------------------------------------------------------

#[test]
fn on_delete_set_null_honors_nocase_collation() {
    let mut db = execute_sql(
        r#"
        CREATE TABLE parent (code VARCHAR(10) COLLATE NOCASE PRIMARY KEY);
        CREATE TABLE child (
            id INTEGER PRIMARY KEY,
            parent_code VARCHAR(10),
            FOREIGN KEY (parent_code) REFERENCES parent(code) ON DELETE SET NULL
        );
        INSERT INTO parent VALUES ('A');
        INSERT INTO child VALUES (1, 'a');
        "#,
    );

    let stmt = Parser::parse_sql("DELETE FROM parent WHERE code = 'A'").unwrap();
    execute_statement(&stmt, &mut db);

    let rows = get_all_rows(&db, "child");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0],
        vec![SqlValue::Integer(1), SqlValue::Null],
        "expected NOCASE SET NULL to null out child FK column"
    );
}

// ---------------------------------------------------------------------------
// ON DELETE SET DEFAULT
// ---------------------------------------------------------------------------

#[test]
fn on_delete_set_default_honors_nocase_collation() {
    let mut db = execute_sql(
        r#"
        CREATE TABLE parent (code VARCHAR(10) COLLATE NOCASE PRIMARY KEY);
        CREATE TABLE child (
            id INTEGER PRIMARY KEY,
            parent_code VARCHAR(10) DEFAULT 'NONE',
            FOREIGN KEY (parent_code) REFERENCES parent(code) ON DELETE SET DEFAULT
        );
        INSERT INTO parent VALUES ('A');
        INSERT INTO parent VALUES ('NONE');
        INSERT INTO child VALUES (1, 'a');
        "#,
    );

    let stmt = Parser::parse_sql("DELETE FROM parent WHERE code = 'A'").unwrap();
    execute_statement(&stmt, &mut db);

    let rows = get_all_rows(&db, "child");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0],
        vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("NONE"))],
        "expected NOCASE SET DEFAULT to substitute the literal default"
    );
}

// ---------------------------------------------------------------------------
// ON UPDATE CASCADE (single shared callsite feeding all UPDATE actions)
// ---------------------------------------------------------------------------

#[test]
fn on_update_cascade_honors_nocase_collation() {
    // Parent has NOCASE on the PK column. Child stores lowercase 'a' but
    // the PK in the parent is 'A'. An UPDATE that changes the PK must
    // cascade to the matching child even though strict == would miss.
    let mut db = execute_sql(
        r#"
        CREATE TABLE parent (code VARCHAR(10) COLLATE NOCASE PRIMARY KEY);
        CREATE TABLE child (
            id INTEGER PRIMARY KEY,
            parent_code VARCHAR(10),
            FOREIGN KEY (parent_code) REFERENCES parent(code) ON UPDATE CASCADE
        );
        INSERT INTO parent VALUES ('A');
        INSERT INTO child VALUES (1, 'a');
        "#,
    );

    let stmt = Parser::parse_sql("UPDATE parent SET code = 'Z' WHERE code = 'A'").unwrap();
    execute_statement(&stmt, &mut db);

    let rows = get_all_rows(&db, "child");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0],
        vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Z"))],
        "expected NOCASE ON UPDATE CASCADE to propagate new parent key"
    );
}

#[test]
fn on_update_set_null_honors_nocase_collation() {
    let mut db = execute_sql(
        r#"
        CREATE TABLE parent (code VARCHAR(10) COLLATE NOCASE PRIMARY KEY);
        CREATE TABLE child (
            id INTEGER PRIMARY KEY,
            parent_code VARCHAR(10),
            FOREIGN KEY (parent_code) REFERENCES parent(code) ON UPDATE SET NULL
        );
        INSERT INTO parent VALUES ('A');
        INSERT INTO child VALUES (1, 'a');
        "#,
    );

    let stmt = Parser::parse_sql("UPDATE parent SET code = 'Z' WHERE code = 'A'").unwrap();
    execute_statement(&stmt, &mut db);

    let rows = get_all_rows(&db, "child");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0],
        vec![SqlValue::Integer(1), SqlValue::Null],
        "expected NOCASE ON UPDATE SET NULL to null out child FK column"
    );
}

#[test]
fn on_update_set_default_honors_nocase_collation() {
    let mut db = execute_sql(
        r#"
        CREATE TABLE parent (code VARCHAR(10) COLLATE NOCASE PRIMARY KEY);
        CREATE TABLE child (
            id INTEGER PRIMARY KEY,
            parent_code VARCHAR(10) DEFAULT 'NONE',
            FOREIGN KEY (parent_code) REFERENCES parent(code) ON UPDATE SET DEFAULT
        );
        INSERT INTO parent VALUES ('A');
        INSERT INTO parent VALUES ('NONE');
        INSERT INTO child VALUES (1, 'a');
        "#,
    );

    let stmt = Parser::parse_sql("UPDATE parent SET code = 'Z' WHERE code = 'A'").unwrap();
    execute_statement(&stmt, &mut db);

    let rows = get_all_rows(&db, "child");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0],
        vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("NONE"))],
        "expected NOCASE ON UPDATE SET DEFAULT to substitute the literal default"
    );
}

// ---------------------------------------------------------------------------
// Sanity: binary-collated columns still match exactly (no behavioural drift).
// ---------------------------------------------------------------------------

#[test]
fn cascade_on_binary_collation_still_strict_equality() {
    // No COLLATE clause => default (BINARY). 'A' != 'a' so the cascade
    // helper must NOT match 'a' to 'A' here.
    let mut db = execute_sql(
        r#"
        CREATE TABLE parent (code VARCHAR(10) PRIMARY KEY);
        CREATE TABLE child (
            id INTEGER PRIMARY KEY,
            parent_code VARCHAR(10),
            FOREIGN KEY (parent_code) REFERENCES parent(code) ON DELETE CASCADE
        );
        INSERT INTO parent VALUES ('A');
        INSERT INTO parent VALUES ('a');
        INSERT INTO child VALUES (1, 'a');
        "#,
    );

    let stmt = Parser::parse_sql("DELETE FROM parent WHERE code = 'A'").unwrap();
    execute_statement(&stmt, &mut db);

    // Child rows referencing 'a' must remain — only 'A' was deleted.
    let rows = get_all_rows(&db, "child");
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0],
        vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("a"))],
        "binary-collated FK must preserve strict equality semantics"
    );
}
