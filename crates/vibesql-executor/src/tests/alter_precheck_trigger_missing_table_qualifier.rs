//! Tests for the schema-qualifier used in the `error in trigger <name>: no
//! such table: <t>` message raised by ALTER TABLE's dependent-object
//! precheck (`alter::drop_column_checks::precheck_schema_objects`), which
//! runs before RENAME COLUMN / DROP COLUMN so an already-broken trigger
//! aborts the ALTER.
//!
//! SQLite qualifies an unqualified missing-table reference inside a
//! main-schema trigger's body with `main.` (`no such table: main.u8`), but
//! leaves the same reference from a TEMP-schema trigger's body bare (`no such
//! table: u8`) — verified against sqlite3 3.51.0 (altercol.test /
//! alter.test 17.1 vs 17.3).

use vibesql_ast::Statement;
use vibesql_storage::Database;

use crate::errors::ExecutorError;

fn exec(db: &mut Database, sql: &str) -> Result<String, ExecutorError> {
    let stmt = vibesql_parser::Parser::parse_sql(sql).expect("parse");
    match stmt {
        Statement::CreateTable(s) => crate::CreateTableExecutor::execute(&s, db),
        Statement::CreateTrigger(s) => {
            crate::TriggerExecutor::create_trigger_with_sql(db, &s, Some(sql))
        }
        Statement::AlterTable(s) => crate::alter::AlterTableExecutor::execute(&s, db),
        other => panic!("unexpected statement: {:?}", other),
    }
}

#[test]
fn main_schema_trigger_missing_table_is_qualified_with_main() {
    // alter.test 17.0/17.1 (main-schema table + trigger).
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE u7(x, y, z)").unwrap();
    exec(
        &mut db,
        "CREATE TRIGGER u7t AFTER INSERT ON u7 BEGIN \
         INSERT INTO u8 VALUES(new.x, new.y, new.z); END",
    )
    .unwrap();

    let err = exec(&mut db, "ALTER TABLE u7 RENAME x TO xxx").unwrap_err();
    assert_eq!(err.to_string(), "error in trigger u7t: no such table: main.u8");
}

#[test]
fn temp_schema_trigger_missing_table_is_left_unqualified() {
    // alter.test 17.2/17.3 (TEMP table + implicitly-TEMP trigger).
    let mut db = Database::new();
    exec(&mut db, "CREATE TEMP TABLE uu7(x, y, z)").unwrap();
    exec(
        &mut db,
        "CREATE TRIGGER uu7t AFTER INSERT ON uu7 BEGIN \
         INSERT INTO u8 VALUES(new.x, new.y, new.z); END",
    )
    .unwrap();

    let err = exec(&mut db, "ALTER TABLE uu7 RENAME x TO xxx").unwrap_err();
    assert_eq!(err.to_string(), "error in trigger uu7t: no such table: u8");
}
