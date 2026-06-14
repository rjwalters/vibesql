//! Tests for `ALTER TABLE ... RENAME COLUMN` rewriting column references inside
//! trigger bodies, matching sqlite3 3.51.0 (`legacy_alter_table=OFF`).
//!
//! Covers altertrig.test 2.3-2.7: the renamed column is rewritten in the stored
//! `CREATE TRIGGER` text (unquoted) wherever it resolves to the renamed table's
//! column, while the rest of the text is preserved verbatim. See
//! `crate::trigger_rename`.

use vibesql_ast::{Statement, TriggerAction};
use vibesql_storage::Database;

use crate::errors::ExecutorError;

/// Execute a DDL statement, preserving the original SQL text for any
/// `CREATE TRIGGER` so the stored `sql_definition` can be asserted.
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

/// Read back the stored verbatim `CREATE TRIGGER` text (the `sqlite_schema.sql`
/// column source).
fn trigger_sql(db: &Database, name: &str) -> String {
    db.catalog
        .get_trigger(name)
        .expect("trigger exists")
        .sql_definition
        .clone()
        .expect("sql_definition preserved")
}

/// Read back the raw trigger body action SQL (used at fire time).
fn trigger_body(db: &Database, name: &str) -> String {
    match &db.catalog.get_trigger(name).expect("trigger exists").triggered_action {
        TriggerAction::RawSql(sql) => sql.clone(),
    }
}

fn setup(db: &mut Database) {
    exec(db, "CREATE TABLE t1(a,b)").unwrap();
    exec(db, "CREATE TABLE t2(c,d)").unwrap();
    exec(db, "CREATE TABLE t3(e,f)").unwrap();
    exec(db, "CREATE TABLE t4(e,f)").unwrap();
}

#[test]
fn rename_column_rewrites_unqualified_in_nested_subquery() {
    // altertrig.test 2.3
    let mut db = Database::new();
    setup(&mut db);
    exec(
        &mut db,
        "CREATE TRIGGER r1 INSERT ON t1 BEGIN \
         UPDATE t1 SET a='xyz' FROM t3, (SELECT * FROM (SELECT e FROM t3)); END",
    )
    .unwrap();

    exec(&mut db, "ALTER TABLE t3 RENAME e TO abc").unwrap();

    let sql = trigger_sql(&db, "r1");
    assert!(sql.contains("SELECT abc FROM t3"), "got: {sql}");
    // The table reference t3 must be untouched.
    assert!(sql.contains("FROM t3,"), "got: {sql}");
}

#[test]
fn rename_column_rewrites_unqualified_in_where() {
    // altertrig.test 2.4
    let mut db = Database::new();
    setup(&mut db);
    exec(
        &mut db,
        "CREATE TRIGGER r1 INSERT ON t1 BEGIN \
         UPDATE t1 SET a='xyz' FROM t3, (SELECT 1 FROM t2 WHERE c); END",
    )
    .unwrap();

    exec(&mut db, "ALTER TABLE t2 RENAME c TO abc").unwrap();

    assert!(trigger_sql(&db, "r1").contains("WHERE abc)"), "{}", trigger_sql(&db, "r1"));
}

#[test]
fn rename_column_rewrites_qualified_reference() {
    // altertrig.test 2.5
    let mut db = Database::new();
    setup(&mut db);
    exec(&mut db, "CREATE TRIGGER r1 INSERT ON t1 BEGIN UPDATE t1 SET a=t2.c FROM t2; END")
        .unwrap();

    exec(&mut db, "ALTER TABLE t2 RENAME c TO abc").unwrap();

    let sql = trigger_sql(&db, "r1");
    assert!(sql.contains("a=t2.abc"), "got: {sql}");
    assert!(sql.contains("FROM t2;"), "qualifier table preserved, got: {sql}");
    // The body action is rewritten too so the trigger fires with the new name.
    // The body is re-rendered from tokens, so normalize whitespace before checking.
    let body: String = trigger_body(&db, "r1").split_whitespace().collect::<Vec<_>>().join("");
    assert!(body.contains("a=t2.abc"), "body: {}", trigger_body(&db, "r1"));
}

#[test]
fn rename_column_rewrites_qualified_with_multiple_from() {
    // altertrig.test 2.6
    let mut db = Database::new();
    setup(&mut db);
    exec(&mut db, "CREATE TRIGGER r1 INSERT ON t1 BEGIN UPDATE t1 SET a=t2.c FROM t2, t3; END")
        .unwrap();

    exec(&mut db, "ALTER TABLE t2 RENAME c TO abc").unwrap();

    assert!(trigger_sql(&db, "r1").contains("a=t2.abc"), "{}", trigger_sql(&db, "r1"));
}

#[test]
fn rename_column_rewrites_qualified_in_natural_join() {
    // altertrig.test 2.7
    let mut db = Database::new();
    setup(&mut db);
    exec(
        &mut db,
        "CREATE TRIGGER r1 INSERT ON t1 BEGIN \
         UPDATE t1 SET a=1 FROM t3 NATURAL JOIN t4 WHERE t4.e=a; END",
    )
    .unwrap();

    exec(&mut db, "ALTER TABLE t4 RENAME e TO abc").unwrap();

    assert!(trigger_sql(&db, "r1").contains("t4.abc=a"), "{}", trigger_sql(&db, "r1"));
}

#[test]
fn rename_column_updates_table_schema() {
    let mut db = Database::new();
    setup(&mut db);

    exec(&mut db, "ALTER TABLE t2 RENAME COLUMN c TO abc").unwrap();

    let t2 = db.get_table("t2").expect("t2 exists");
    let cols: Vec<String> = t2.schema.columns.iter().map(|c| c.name.clone()).collect();
    assert!(t2.schema.has_column("abc"), "columns: {cols:?}");
    assert!(!t2.schema.has_column("c"), "columns: {cols:?}");
}

#[test]
fn rename_column_missing_column_errors() {
    let mut db = Database::new();
    setup(&mut db);

    let err = exec(&mut db, "ALTER TABLE t2 RENAME nosuch TO x").unwrap_err();
    assert!(matches!(err, ExecutorError::ColumnNotFound { .. }), "got: {err:?}");
}

#[test]
fn rename_column_onto_existing_errors() {
    let mut db = Database::new();
    setup(&mut db);

    // t2 already has a column `d`.
    let err = exec(&mut db, "ALTER TABLE t2 RENAME c TO d").unwrap_err();
    assert!(matches!(err, ExecutorError::ColumnAlreadyExists(_)), "got: {err:?}");
}
