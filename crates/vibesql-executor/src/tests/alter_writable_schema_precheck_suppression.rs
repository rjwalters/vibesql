//! Tests that `ALTER TABLE ... RENAME COLUMN` / `DROP COLUMN`'s dependent-object
//! schema re-validation (`alter::drop_column_checks::check_schema_objects`) is
//! suppressed while `PRAGMA writable_schema=ON`, matching SQLite's documented
//! "Do not complain about syntax errors in the schema if in PRAGMA
//! writable_schema=ON mode" behavior (altercol.test group 23, verified against
//! sqlite3 3.51.0). Issue #6174.
//!
//! The gate is keyed on the CURRENT `writable_schema` value at the moment the
//! ALTER statement itself runs — not on whether the schema was ever corrupted
//! under it. A later ALTER that runs after the pragma has been turned back OFF
//! must still raise the normal validation error (altercol.test 13.1.4-13.1.7).

use vibesql_ast::Statement;
use vibesql_storage::Database;

use crate::errors::ExecutorError;

fn exec(db: &mut Database, sql: &str) -> Result<String, ExecutorError> {
    let stmt = vibesql_parser::Parser::parse_sql(sql).expect("parse");
    match stmt {
        Statement::CreateTable(s) => crate::CreateTableExecutor::execute(&s, db),
        Statement::CreateView(s) => crate::ViewExecutor::execute_create_view(&s, db),
        Statement::AlterTable(s) => crate::alter::AlterTableExecutor::execute(&s, db),
        other => panic!("unexpected statement: {:?}", other),
    }
}

#[test]
fn rename_column_tolerates_already_broken_view_under_writable_schema() {
    // altercol.test 23.0/23.1/23.3: a view that references a column that
    // never existed (`xyz`) is pre-existing schema corruption, independent of
    // the rename. With writable_schema OFF, the ALTER must still abort
    // (23.1). With writable_schema ON, it must succeed (23.3).
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t1(a INT, b REAL, c TEXT, d BLOB, e ANY)").unwrap();
    exec(&mut db, "CREATE VIEW t2 AS SELECT a+10, b*5.0, xyz FROM t1").unwrap();

    // writable_schema OFF (default): the pre-existing broken view aborts the ALTER.
    let err = exec(&mut db, "ALTER TABLE t1 RENAME COLUMN e TO eeee").unwrap_err();
    assert_eq!(err.to_string(), "error in view t2: no such column: xyz");

    // writable_schema ON: the same ALTER is tolerated (no abort), and the
    // rename itself still applies to the target table.
    db.set_writable_schema(true);
    exec(&mut db, "ALTER TABLE t1 RENAME COLUMN e TO eeee").unwrap();
    assert!(db.get_table("t1").unwrap().schema.has_column("eeee"));

    // Turning writable_schema back OFF restores normal validation for a
    // later ALTER against the same still-broken view (altercol.test
    // 13.1.4-13.1.7 shape: corruption made under writable_schema=ON is still
    // flagged once it is turned back OFF).
    db.set_writable_schema(false);
    let err = exec(&mut db, "ALTER TABLE t1 RENAME COLUMN d TO ddd").unwrap_err();
    assert_eq!(err.to_string(), "error in view t2: no such column: xyz");
}
