//! Tests for renaming the same table twice with `ALTER TABLE ... RENAME TO`
//! (issue #6174, altertab3.test 29.7).
//!
//! The first rename rewrites references in dependent triggers and views to
//! the new name, double-quoted (`ON "t2"`, `FROM "t2"`, `"t2".x`), as SQLite
//! does. The table-reference rewriter matched only bare identifiers, so the
//! *second* rename skipped every one of those quoted references. Each trigger
//! and view kept naming a table that no longer existed, even though the
//! trigger's catalog `table_name` moved on. SQLite rewrites them again on
//! every rename. These tests check the stored SQL after a second rename and
//! that the dependent objects still work.

use vibesql_ast::Statement;
use vibesql_storage::Database;

fn exec(db: &mut Database, sql: &str) -> Result<(), String> {
    let stmt = vibesql_parser::Parser::parse_sql(sql).map_err(|e| format!("{e:?}"))?;
    match stmt {
        Statement::CreateTable(s) => {
            crate::CreateTableExecutor::execute(&s, db).map(|_| ()).map_err(|e| e.to_string())
        }
        Statement::CreateView(s) => {
            crate::advanced_objects::execute_create_view(&s, db).map_err(|e| e.to_string())
        }
        Statement::CreateTrigger(s) => {
            crate::TriggerExecutor::create_trigger_with_sql(db, &s, Some(sql))
                .map(|_| ())
                .map_err(|e| e.to_string())
        }
        Statement::Insert(s) => {
            crate::InsertExecutor::execute(db, &s).map(|_| ()).map_err(|e| e.to_string())
        }
        Statement::AlterTable(s) => {
            crate::alter::AlterTableExecutor::execute(&s, db).map(|_| ()).map_err(|e| e.to_string())
        }
        other => panic!("unexpected statement: {other:?}"),
    }
}

fn trigger_sql(db: &Database, name: &str) -> String {
    db.catalog
        .get_trigger(name)
        .unwrap_or_else(|| panic!("trigger {name} must exist"))
        .sql_definition
        .clone()
        .expect("trigger keeps its verbatim SQL")
}

fn view_sql(db: &Database, name: &str) -> String {
    db.catalog
        .get_view(name)
        .unwrap_or_else(|| panic!("view {name} must exist"))
        .sql_definition
        .clone()
        .expect("a renamed-through view carries rewritten SQL")
}

/// altertab3.test 29.1-29.7: a trigger with no timing keyword
/// (`DELETE ON t1`) is rewritten on both renames: header, qualifiers and
/// FROM list.
#[test]
fn second_rename_rewrites_quoted_trigger_references() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t1(x, y)").unwrap();
    exec(
        &mut db,
        "CREATE TRIGGER Trigger1 DELETE ON t1 BEGIN SELECT t1.*, t1.x FROM t1 ORDER BY t1.x; END",
    )
    .unwrap();

    exec(&mut db, "ALTER TABLE t1 RENAME TO t2").unwrap();
    assert_eq!(
        trigger_sql(&db, "Trigger1"),
        "CREATE TRIGGER Trigger1 DELETE ON \"t2\" BEGIN SELECT \"t2\".*, \"t2\".x FROM \"t2\" ORDER BY \"t2\".x; END"
    );

    exec(&mut db, "ALTER TABLE t2 RENAME TO t3").unwrap();
    assert_eq!(
        trigger_sql(&db, "Trigger1"),
        "CREATE TRIGGER Trigger1 DELETE ON \"t3\" BEGIN SELECT \"t3\".*, \"t3\".x FROM \"t3\" ORDER BY \"t3\".x; END"
    );
    let trigger = db.catalog.get_trigger("Trigger1").unwrap();
    assert_eq!(trigger.table_name.to_ascii_lowercase(), "t3");
}

/// A trigger whose body writes to the twice-renamed table keeps working:
/// its stored body must name the table's current name, not a stale one.
#[test]
fn trigger_body_targets_table_after_two_renames() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE src(a)").unwrap();
    exec(&mut db, "CREATE TABLE log(a)").unwrap();
    exec(&mut db, "CREATE TRIGGER tr AFTER INSERT ON src BEGIN INSERT INTO log VALUES(new.a); END")
        .unwrap();

    exec(&mut db, "ALTER TABLE log RENAME TO log2").unwrap();
    exec(&mut db, "ALTER TABLE log2 RENAME TO log3").unwrap();
    assert_eq!(
        trigger_sql(&db, "tr"),
        "CREATE TRIGGER tr AFTER INSERT ON src BEGIN INSERT INTO \"log3\" VALUES(new.a); END"
    );

    exec(&mut db, "INSERT INTO src VALUES(7)").unwrap();
    let rows = db.get_table("log3").expect("log3 exists").row_count();
    assert_eq!(rows, 1, "the trigger must insert into the renamed table");
}

/// Views are rewritten on the second rename too, and a later ALTER still
/// passes the schema re-validation (a stale view would report
/// `no such table`).
#[test]
fn second_rename_rewrites_quoted_view_references() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t1(x, y)").unwrap();
    exec(&mut db, "CREATE VIEW v AS SELECT t1.x FROM t1 WHERE t1.y > 0").unwrap();

    exec(&mut db, "ALTER TABLE t1 RENAME TO t2").unwrap();
    exec(&mut db, "ALTER TABLE t2 RENAME TO t3").unwrap();

    let sql = view_sql(&db, "v");
    assert!(sql.contains("FROM \"t3\""), "view FROM must name t3, got: {sql}");
    assert!(sql.contains("\"t3\".x"), "view qualifier must name t3, got: {sql}");
    assert!(!sql.contains("t2"), "no stale t2 reference may remain, got: {sql}");

    exec(&mut db, "CREATE TABLE other(z)").unwrap();
    exec(&mut db, "ALTER TABLE other RENAME TO other2").unwrap();
}
