//! Name resolution within trigger bodies (SQLite parity, #6176 / triggerB-2.x).
//!
//! Two behaviors verified against sqlite3 3.51.0:
//!
//! 1. An unresolved OLD/NEW column reference in a trigger body reports the pseudo-table qualifier
//!    intact — `no such column: old.c`, not `no such column: c` (triggerB-2.4). VibeSQL renders
//!    `ColumnNotFound` with the qualified `column_name`, so the message carries `old.c` / `new.c`.
//!
//! 2. SQLite resolves a trigger's body when the firing DML is *prepared*, so a body containing an
//!    unresolvable qualified reference (`SELECT wen.x`) errors before the INSERT's own constraint
//!    checks run (triggerB-2.1). Without the prepare-time pass the UNIQUE-constraint error would
//!    win instead.

use vibesql_ast::Statement;
use vibesql_parser::Parser;

use super::super::*;

/// Execute setup SQL that is expected to succeed.
fn exec_ok(db: &mut vibesql_storage::Database, sql: &str) {
    let stmt =
        Parser::parse_sql(sql).unwrap_or_else(|e| panic!("Failed to parse {:?}: {:?}", sql, e));
    match stmt {
        Statement::CreateTable(s) => {
            CreateTableExecutor::execute(&s, db).expect("CREATE TABLE failed");
        }
        Statement::CreateTrigger(s) => {
            crate::advanced_objects::execute_create_trigger(&s, db).expect("CREATE TRIGGER failed");
        }
        Statement::Insert(s) => {
            InsertExecutor::execute(db, &s).expect("INSERT failed");
        }
        other => panic!("Unsupported setup statement: {:?}", other),
    }
}

/// Execute a statement expected to fail, returning the rendered error message.
fn exec_err(db: &mut vibesql_storage::Database, sql: &str) -> String {
    let stmt =
        Parser::parse_sql(sql).unwrap_or_else(|e| panic!("Failed to parse {:?}: {:?}", sql, e));
    let result = match stmt {
        Statement::Insert(s) => InsertExecutor::execute(db, &s).map(|_| ()),
        Statement::Delete(s) => DeleteExecutor::execute(&s, db).map(|_| ()),
        Statement::Update(s) => UpdateExecutor::execute(&s, db).map(|_| ()),
        other => panic!("Unsupported failing statement: {:?}", other),
    };
    result.expect_err("statement was expected to fail").to_string()
}

#[test]
fn missing_old_column_error_keeps_pseudo_qualifier() {
    // triggerB-2.4: body `INSERT INTO changes VALUES(old.a, old.c)` where `c`
    // is not a column of the firing table must report the `old.` qualifier.
    let mut db = vibesql_storage::Database::new();
    exec_ok(&mut db, "CREATE TABLE t2 (a INTEGER PRIMARY KEY, b)");
    exec_ok(&mut db, "INSERT INTO t2 VALUES (1, 2)");
    exec_ok(&mut db, "CREATE TABLE changes (x, y)");
    exec_ok(
        &mut db,
        "CREATE TRIGGER r2t2 AFTER DELETE ON t2 BEGIN \
         INSERT INTO changes VALUES(old.a, old.c); \
         END",
    );

    let msg = exec_err(&mut db, "DELETE FROM t2");
    assert!(msg.contains("old.c"), "message should keep the old. qualifier, got: {msg}");
    assert!(!msg.contains(" c "), "message should not use the bare column name, got: {msg}");
}

#[test]
fn missing_new_column_error_keeps_pseudo_qualifier() {
    // The NEW side of the same rule: `new.c` on an unknown column.
    let mut db = vibesql_storage::Database::new();
    exec_ok(&mut db, "CREATE TABLE t (a, b)");
    exec_ok(&mut db, "CREATE TABLE changes (x, y)");
    exec_ok(
        &mut db,
        "CREATE TRIGGER tr AFTER INSERT ON t BEGIN \
         INSERT INTO changes VALUES(new.a, new.c); \
         END",
    );

    let msg = exec_err(&mut db, "INSERT INTO t VALUES(1, 2)");
    assert!(msg.contains("new.c"), "message should keep the new. qualifier, got: {msg}");
}

#[test]
fn insert_trigger_body_bad_qualifier_beats_unique_constraint() {
    // triggerB-2.1: the AFTER INSERT trigger body `SELECT wen.x` uses an unknown
    // qualifier. SQLite resolves the body at prepare time, so the resolution
    // error surfaces even though the INSERT itself would violate the PRIMARY KEY
    // (x=1 already exists). The prepare-time pass must win over the constraint.
    let mut db = vibesql_storage::Database::new();
    exec_ok(&mut db, "CREATE TABLE x (x INTEGER PRIMARY KEY, y INT NOT NULL)");
    exec_ok(&mut db, "INSERT INTO x VALUES(1, 1)");
    exec_ok(
        &mut db,
        "CREATE TRIGGER ty AFTER INSERT ON x BEGIN \
         SELECT wen.x; \
         END",
    );

    // Inserting x=1 again would fail the PK/UNIQUE check, but the trigger body's
    // bad reference must be reported first.
    let msg = exec_err(&mut db, "INSERT INTO x VALUES(1, 2)");
    assert!(
        msg.contains("wen"),
        "the trigger-body qualifier error should win over the constraint, got: {msg}"
    );
    assert!(
        !msg.to_lowercase().contains("unique") && !msg.to_lowercase().contains("constraint"),
        "the constraint error must not win, got: {msg}"
    );
}

#[test]
fn valid_new_column_reference_in_no_from_select_does_not_error() {
    // The prepare-time pass must not flag a legitimate OLD/NEW reference in a
    // bare `SELECT NEW.col` body — that resolves fine and the INSERT proceeds.
    let mut db = vibesql_storage::Database::new();
    exec_ok(&mut db, "CREATE TABLE t (a, b)");
    exec_ok(
        &mut db,
        "CREATE TRIGGER tr AFTER INSERT ON t BEGIN \
         SELECT NEW.a; \
         END",
    );

    // Should not raise: NEW.a is a valid reference.
    exec_ok(&mut db, "INSERT INTO t VALUES(1, 2)");
    let schema = db.catalog.get_table("t").expect("table exists");
    let idx = schema.columns.iter().position(|c| c.name == "a").expect("column a");
    let rows: Vec<_> = db.get_table("t").expect("table").scan().to_vec();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values[idx], vibesql_types::SqlValue::Integer(1));
}
