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
        Statement::Delete(s) => crate::DeleteExecutor::execute(&s, db)
            .map(|count| format!("{} row(s) deleted", count))
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
fn add_column_check_is_persisted_and_enforced_on_later_insert() {
    // Issue #6241: the CHECK added by `ADD COLUMN c CHECK(...)` must be
    // persisted into the schema, not just validated once against the rows
    // present at ALTER time. A later INSERT that violates it must be rejected,
    // matching CREATE TABLE column-CHECK behavior.
    let mut db = Database::new();
    exec_sql(&mut db, "CREATE TABLE t1(a, b)").unwrap();
    exec_sql(&mut db, "INSERT INTO t1 VALUES(1, 2), (3, 4)").unwrap();
    assert!(exec_sql(&mut db, "ALTER TABLE t1 ADD COLUMN c CHECK(c!=99)").is_ok());
    // Existing rows are unaffected (c defaults to NULL, which passes CHECK).
    assert!(exec_sql(&mut db, "INSERT INTO t1 VALUES(5, 6, 1)").is_ok());
    // A later row violating the added CHECK must now be rejected.
    let err = exec_sql(&mut db, "INSERT INTO t1 VALUES(7, 8, 99)").unwrap_err();
    assert!(err.contains("CHECK constraint failed"), "got: {err}");
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

#[test]
fn add_column_strict_default_type_mismatch_rejected_when_rows_exist() {
    // alter-20.2 (do_catchsql_test): a STRICT table's ADD COLUMN DEFAULT whose
    // value doesn't match the declared strict type must abort the whole ALTER
    // once there is at least one existing row to backfill -- matching
    // sqlite3AlterFinishAddColumn's post-ALTER `pragma_quick_check` pass
    // (alter.c), which maps the resulting type violation to the fixed message
    // `type mismatch on DEFAULT` rather than the ordinary
    // `cannot store ... column ...` runtime message.
    let mut db = Database::new();
    exec_sql(&mut db, "CREATE TABLE t1(a INT) STRICT").unwrap();
    exec_sql(&mut db, "INSERT INTO t1(a) VALUES(45)").unwrap();
    let err =
        exec_sql(&mut db, "ALTER TABLE t1 ADD COLUMN b TEXT DEFAULT x'313233'").unwrap_err();
    assert_eq!(err, "type mismatch on DEFAULT");
    // The rejected ALTER left the table untouched -- no `b` column exists.
    assert!(exec_sql(&mut db, "SELECT b FROM t1").is_err());
}

#[test]
fn add_column_strict_default_type_mismatch_allowed_on_empty_table() {
    // alter-20.2 (do_execsql_test, same ALTER re-run against an empty table):
    // with zero existing rows there is nothing for the post-ALTER scan to
    // find, so the identical ALTER that fails above succeeds here.
    let mut db = Database::new();
    exec_sql(&mut db, "CREATE TABLE t1(a INT) STRICT").unwrap();
    assert!(exec_sql(&mut db, "ALTER TABLE t1 ADD COLUMN b TEXT DEFAULT x'313233'").is_ok());
}

#[test]
fn strict_default_mismatch_surfaces_on_later_insert() {
    // alter-20.3: once the mismatched DEFAULT has been added to a STRICT
    // table (permitted while the table was empty, previous test), a later
    // INSERT that omits `b` and so materializes the BLOB default into the
    // TEXT column must be rejected with the ordinary STRICT runtime message
    // -- the DEFAULT clause itself is not re-validated at ALTER time once
    // already accepted, only at the point a row actually needs the value.
    let mut db = Database::new();
    exec_sql(&mut db, "CREATE TABLE t1(a INT) STRICT").unwrap();
    exec_sql(&mut db, "ALTER TABLE t1 ADD COLUMN b TEXT DEFAULT x'313233'").unwrap();
    let err = exec_sql(&mut db, "INSERT INTO t1(a) VALUES(45)").unwrap_err();
    assert_eq!(err, "cannot store BLOB value in TEXT column t1.b");
}

#[test]
fn add_column_strict_default_matching_type_is_allowed_with_existing_rows() {
    // A STRICT-typed DEFAULT that DOES match the declared column type must
    // still succeed with existing rows to backfill -- only a genuine mismatch
    // triggers `type mismatch on DEFAULT`.
    let mut db = Database::new();
    exec_sql(&mut db, "CREATE TABLE t1(a INT) STRICT").unwrap();
    exec_sql(&mut db, "INSERT INTO t1(a) VALUES(45)").unwrap();
    assert!(exec_sql(&mut db, "ALTER TABLE t1 ADD COLUMN b TEXT DEFAULT 'ok'").is_ok());
}
