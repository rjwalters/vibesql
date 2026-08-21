//! Tests for `ALTER TABLE ... RENAME TO` preserving triggers whose own
//! `ON <table>` target is the table being renamed, matching sqlite3 3.51.0
//! (`legacy_alter_table=OFF`).
//!
//! `execute_rename_table` implements RENAME TABLE as drop-old + create-new
//! (`crate::alter::table_options::execute_rename_table`). The storage-layer
//! `Catalog::drop_table` cascade-drops every trigger whose `ON <table>` target
//! is the table being dropped — correct for a genuine `DROP TABLE`, but SQLite's
//! RENAME TABLE does *not* drop such triggers: it rewrites their `ON`-target (and
//! any body references) to the new name and keeps them. Without restoring the
//! cascade-dropped triggers before `rewrite_triggers_for_rename` runs, a trigger
//! defined directly on the renamed table is silently and permanently lost
//! (issue #6174, alter.test alter-21.2/alter-21.4).

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
        Statement::DropTrigger(s) => crate::TriggerExecutor::drop_trigger(db, &s),
        Statement::AlterTable(s) => crate::alter::AlterTableExecutor::execute(&s, db),
        other => panic!("unexpected statement: {:?}", other),
    }
}

/// alter.test alter-21.1/21.2: a trigger defined `ON` the table being renamed
/// survives the rename, and its `ON`-target is updated to the new name.
#[test]
fn rename_table_preserves_trigger_defined_on_it() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t1(a,b,c,d)").unwrap();
    exec(&mut db, "CREATE TABLE t2(a,b,c,d,x)").unwrap();
    exec(
        &mut db,
        "CREATE TRIGGER r1 AFTER INSERT ON t2 BEGIN \
           SELECT unknown_function(a ORDER BY (SELECT group_concat(DISTINCT a ORDER BY a) FROM t1)) FROM t1; \
         END",
    )
    .unwrap();

    exec(&mut db, "ALTER TABLE t2 RENAME TO e").unwrap();

    let trigger = db.catalog.get_trigger("r1").expect("trigger r1 must survive the rename");
    assert_eq!(trigger.table_name.to_ascii_lowercase(), "e", "ON-target should be rewritten to e");
}

/// alter.test alter-21.3/21.4: after the first rename, a new trigger defined on
/// the renamed table also survives a *second* rename.
#[test]
fn rename_table_preserves_trigger_across_second_rename() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t1(a,b,c,d)").unwrap();
    exec(&mut db, "CREATE TABLE t2(a,b,c,d,x)").unwrap();
    exec(&mut db, "CREATE TRIGGER r1 AFTER INSERT ON t2 BEGIN SELECT 1; END").unwrap();
    exec(&mut db, "ALTER TABLE t2 RENAME TO e").unwrap();

    exec(&mut db, "DROP TRIGGER r1").unwrap();
    exec(&mut db, "CREATE TRIGGER r2 AFTER INSERT ON e BEGIN SELECT 1; END").unwrap();
    exec(&mut db, "ALTER TABLE e RENAME TO t99").unwrap();

    let trigger = db.catalog.get_trigger("r2").expect("trigger r2 must survive the second rename");
    assert_eq!(trigger.table_name.to_ascii_lowercase(), "t99");
    assert!(db.catalog.get_trigger("r1").is_none(), "r1 was explicitly dropped and stays dropped");
}

/// A trivial trigger body (no table references at all) still must not be
/// dropped by the rename — the cascade-drop-then-restore happens purely by
/// `table_name`, independent of body content.
#[test]
fn rename_table_preserves_trigger_with_trivial_body() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t2(a,b)").unwrap();
    exec(&mut db, "CREATE TRIGGER r1 AFTER INSERT ON t2 BEGIN SELECT 1; END").unwrap();

    exec(&mut db, "ALTER TABLE t2 RENAME TO e").unwrap();

    assert!(db.catalog.get_trigger("r1").is_some());
}
