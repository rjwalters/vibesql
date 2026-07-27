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

#[test]
fn add_column_check_violated_by_existing_row_is_rejected() {
    // alter3-9.2: `ADD COLUMN c CHECK(a!=1)` must abort because an existing row
    // has a=1. SQLite reports the bare "CHECK constraint failed" for this path.
    let mut db = Database::new();
    exec_sql(&mut db, "CREATE TABLE t1(a, b)").unwrap();
    exec_sql(&mut db, "INSERT INTO t1 VALUES(1, 2), ('null!', NULL), (3, 4)").unwrap();
    let err = exec_sql(&mut db, "ALTER TABLE t1 ADD COLUMN c CHECK(a!=1)").unwrap_err();
    assert_eq!(err, "CHECK constraint failed");
}

#[test]
fn add_column_check_rollback_leaves_table_untouched() {
    // A rejected ADD COLUMN CHECK must be atomic: the column is not left behind,
    // so a subsequent non-violating ADD of the same column name succeeds
    // (alter3-9.2..9.4 depend on this rollback).
    let mut db = Database::new();
    exec_sql(&mut db, "CREATE TABLE t1(a, b)").unwrap();
    exec_sql(&mut db, "INSERT INTO t1 VALUES(1, 2), ('null!', NULL), (3, 4)").unwrap();
    let _ = exec_sql(&mut db, "ALTER TABLE t1 ADD COLUMN c CHECK(a!=1)").unwrap_err();
    // No row has a=2, so this CHECK holds for every existing row.
    assert!(exec_sql(&mut db, "ALTER TABLE t1 ADD COLUMN c CHECK(a!=2)").is_ok());
}

#[test]
fn add_column_check_satisfied_by_all_rows_is_allowed() {
    // alter3-9.4: `ADD COLUMN c CHECK(a!=2)` succeeds because no existing row
    // has a=2.
    let mut db = Database::new();
    exec_sql(&mut db, "CREATE TABLE t1(a, b)").unwrap();
    exec_sql(&mut db, "INSERT INTO t1 VALUES(1, 2), ('null!', NULL), (3, 4)").unwrap();
    assert!(exec_sql(&mut db, "ALTER TABLE t1 ADD COLUMN c CHECK(a!=2)").is_ok());
}

#[test]
fn add_generated_not_null_column_null_value_is_rejected() {
    // alter3-9.5: a generated NOT NULL column is permitted (no static NULL-default
    // rejection), but its computed value is validated per existing row. Row
    // ('null!', NULL) yields b+1 = NULL, violating NOT NULL.
    let mut db = Database::new();
    exec_sql(&mut db, "CREATE TABLE t1(a, b)").unwrap();
    exec_sql(&mut db, "INSERT INTO t1 VALUES(1, 2), ('null!', NULL), (3, 4)").unwrap();
    let err = exec_sql(&mut db, "ALTER TABLE t1 ADD COLUMN d AS (b+1) NOT NULL").unwrap_err();
    assert_eq!(err, "NOT NULL constraint failed");
}

#[test]
fn add_generated_not_null_check_reports_check_first() {
    // alter3-9.6: with both a CHECK and NOT NULL on a generated column, the first
    // existing row (a=1) fails CHECK(a!=1); SQLite reports CHECK even though a
    // later row would also fail NOT NULL. CHECK is evaluated before NOT NULL
    // within a row and rows are scanned in order.
    let mut db = Database::new();
    exec_sql(&mut db, "CREATE TABLE t1(a, b)").unwrap();
    exec_sql(&mut db, "INSERT INTO t1 VALUES(1, 2), ('null!', NULL), (3, 4)").unwrap();
    let err =
        exec_sql(&mut db, "ALTER TABLE t1 ADD COLUMN d AS (b+1) NOT NULL CHECK(a!=1)").unwrap_err();
    assert_eq!(err, "CHECK constraint failed");
}
