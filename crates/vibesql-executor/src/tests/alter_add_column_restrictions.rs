//! Tests for SQLite's `ALTER TABLE ... ADD COLUMN` restrictions (issue #6174,
//! alter3-2.*). SQLite rejects adding a PRIMARY KEY / UNIQUE column, a NOT NULL
//! column without a non-NULL default, a column with a non-constant default, and
//! any column on a view — each with a verbatim message the TCL harness asserts.

use vibesql_ast::Statement;
use vibesql_storage::Database;

fn exec_sql(db: &mut Database, sql: &str) -> Result<String, String> {
    let stmt =
        vibesql_parser::Parser::parse_sql(sql).map_err(|e| format!("Parse error: {:?}", e))?;

    match stmt {
        Statement::CreateTable(s) => {
            crate::CreateTableExecutor::execute(&s, db).map_err(|e| e.to_string())
        }
        Statement::CreateView(s) => crate::advanced_objects::execute_create_view(&s, db)
            .map(|_| "view created".to_string())
            .map_err(|e| e.to_string()),
        Statement::Insert(s) => crate::InsertExecutor::execute(db, &s)
            .map(|count| format!("{} row(s) inserted", count))
            .map_err(|e| e.to_string()),
        Statement::AlterTable(s) => {
            crate::alter::AlterTableExecutor::execute(&s, db).map_err(|e| e.to_string())
        }
        _ => Err("Unsupported statement type".to_string()),
    }
}

#[test]
fn add_primary_key_column_is_rejected() {
    let mut db = Database::new();
    exec_sql(&mut db, "CREATE TABLE t1(a, b)").unwrap();
    exec_sql(&mut db, "INSERT INTO t1 VALUES(1, 2)").unwrap();
    let err = exec_sql(&mut db, "ALTER TABLE t1 ADD c PRIMARY KEY").unwrap_err();
    assert_eq!(err, "Cannot add a PRIMARY KEY column");
    // The rejected ALTER left the table untouched.
    assert!(exec_sql(&mut db, "ALTER TABLE t1 ADD c VARCHAR(10)").is_ok());
}

#[test]
fn add_unique_column_is_rejected() {
    let mut db = Database::new();
    exec_sql(&mut db, "CREATE TABLE t1(a, b)").unwrap();
    let err = exec_sql(&mut db, "ALTER TABLE t1 ADD c UNIQUE").unwrap_err();
    assert_eq!(err, "Cannot add a UNIQUE column");
}

#[test]
fn add_not_null_column_without_default_is_rejected() {
    let mut db = Database::new();
    exec_sql(&mut db, "CREATE TABLE t1(a, b)").unwrap();
    let err = exec_sql(&mut db, "ALTER TABLE t1 ADD c NOT NULL").unwrap_err();
    assert_eq!(err, "Cannot add a NOT NULL column with default value NULL");
}

#[test]
fn add_not_null_column_with_explicit_null_default_is_rejected() {
    let mut db = Database::new();
    exec_sql(&mut db, "CREATE TABLE t1(a, b)").unwrap();
    // DEFAULT NULL counts as "no default" for the NOT NULL check.
    let err = exec_sql(&mut db, "ALTER TABLE t1 ADD c NOT NULL DEFAULT NULL").unwrap_err();
    assert_eq!(err, "Cannot add a NOT NULL column with default value NULL");
}

#[test]
fn add_not_null_column_with_constant_default_is_allowed() {
    let mut db = Database::new();
    exec_sql(&mut db, "CREATE TABLE t1(a, b)").unwrap();
    exec_sql(&mut db, "INSERT INTO t1 VALUES(1, 2)").unwrap();
    // A NOT NULL column with a non-NULL constant default is fine — including
    // when the DEFAULT clause trails the NOT NULL constraint (alter3-2.4).
    assert!(exec_sql(&mut db, "ALTER TABLE t1 ADD c NOT NULL DEFAULT 10").is_ok());
}

#[test]
fn add_column_with_non_constant_default_is_rejected() {
    let mut db = Database::new();
    exec_sql(&mut db, "CREATE TABLE t1(a, b)").unwrap();
    let err = exec_sql(&mut db, "ALTER TABLE t1 ADD d DEFAULT CURRENT_TIME").unwrap_err();
    assert_eq!(err, "Cannot add a column with non-constant default");
}

#[test]
fn add_column_to_view_is_rejected() {
    let mut db = Database::new();
    exec_sql(&mut db, "CREATE TABLE t1(a, b)").unwrap();
    exec_sql(&mut db, "CREATE VIEW v1 AS SELECT * FROM t1").unwrap();
    let err = exec_sql(&mut db, "ALTER TABLE v1 ADD COLUMN d").unwrap_err();
    assert_eq!(err, "Cannot add a column to a view");
}
