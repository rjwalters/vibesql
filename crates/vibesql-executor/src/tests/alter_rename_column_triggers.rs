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

#[test]
fn rename_column_ambiguous_in_trigger_aborts_and_leaves_schema_unchanged() {
    // sqlite3 3.51.0 (legacy_alter_table=OFF): an unqualified reference to the
    // renamed column that is ambiguous across multiple in-scope tables aborts
    // the entire ALTER ("SQL logic error" at the shell; "error in trigger r1:
    // ambiguous column name: e" via the C API) and leaves the schema unchanged.
    let mut db = Database::new();
    setup(&mut db);
    // Both t3 and t4 own column `e`; the trigger's WHERE `e` is ambiguous.
    exec(
        &mut db,
        "CREATE TRIGGER r1 INSERT ON t3 BEGIN \
         UPDATE t3 SET e=1 FROM t4 WHERE e=2; END",
    )
    .unwrap();

    let body_before = trigger_body(&db, "r1");
    let sql_before = trigger_sql(&db, "r1");

    let err = exec(&mut db, "ALTER TABLE t3 RENAME e TO abc").unwrap_err();
    match &err {
        ExecutorError::Other(msg) => {
            assert_eq!(msg, "error in trigger r1: ambiguous column name: e", "got: {msg}");
        }
        other => panic!("expected ambiguity Other error, got: {other:?}"),
    }

    // Schema unchanged: t3.e still exists, t3.abc does not.
    let t3 = db.get_table("t3").expect("t3 exists");
    assert!(t3.schema.has_column("e"), "t3.e should be unchanged after aborted ALTER");
    assert!(!t3.schema.has_column("abc"), "t3.abc must not exist after aborted ALTER");

    // Trigger untouched.
    assert_eq!(trigger_body(&db, "r1"), body_before, "trigger body must be unchanged");
    assert_eq!(trigger_sql(&db, "r1"), sql_before, "trigger sql must be unchanged");
}

#[test]
fn rename_column_rewrites_new_old_refs_in_when_clause_and_body() {
    // altercol.test 3.x: a trigger on the renamed table references the renamed
    // column via the NEW/OLD pseudo-tables in both its WHEN clause and body.
    // SQLite rewrites those references (the pseudo-tables alias the subject
    // table). Prior to the fix, `WHEN new.y<0` and `SET x=new.y` were left
    // stale, so the stored `CREATE TRIGGER` text and the runtime WHEN condition
    // both still referenced the old column.
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t4(x, y, z)").unwrap();
    exec(
        &mut db,
        "CREATE TRIGGER ttt AFTER INSERT ON t4 WHEN new.y<0 BEGIN \
         UPDATE t4 SET x=new.y WHERE old.y IS NULL; END",
    )
    .unwrap();

    exec(&mut db, "ALTER TABLE t4 RENAME y TO abc").unwrap();

    // Stored verbatim CREATE TRIGGER: WHEN clause and NEW/OLD body refs rewritten.
    let sql = trigger_sql(&db, "ttt");
    assert!(sql.contains("WHEN new.abc<0"), "WHEN clause not rewritten: {sql}");
    assert!(sql.contains("x=new.abc"), "NEW body ref not rewritten: {sql}");
    assert!(sql.contains("old.abc IS NULL"), "OLD body ref not rewritten: {sql}");
    assert!(!sql.contains(".y"), "stale reference to old column remains: {sql}");

    // Runtime WHEN condition AST (evaluated per row, not re-parsed from text) is
    // rewritten so the trigger fires without a "no such column: new.y" error.
    use vibesql_ast::pretty_print::ToSql;
    let when_sql = db
        .catalog
        .get_trigger("ttt")
        .expect("trigger exists")
        .when_condition
        .as_ref()
        .expect("WHEN condition preserved")
        .to_sql();
    assert!(
        when_sql.to_ascii_lowercase().contains("abc"),
        "runtime WHEN not rewritten: {when_sql}"
    );
    assert!(
        !when_sql.to_ascii_lowercase().contains("new.y"),
        "runtime WHEN still references old column: {when_sql}"
    );
}

#[test]
fn rename_column_leaves_new_old_refs_untouched_for_trigger_on_other_table() {
    // A trigger whose subject table is NOT the renamed table has NEW/OLD
    // pseudo-tables aliasing a different table, so a `new.<name>` reference must
    // not be rewritten even if the name collides with the renamed column.
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t1(a, b)").unwrap();
    exec(&mut db, "CREATE TABLE t2(a, c)").unwrap();
    // Trigger fires on t2; NEW.a here is t2.a, unrelated to t1.a being renamed.
    exec(
        &mut db,
        "CREATE TRIGGER g AFTER INSERT ON t2 WHEN new.a<0 BEGIN \
         INSERT INTO t1 VALUES(new.a, new.c); END",
    )
    .unwrap();

    exec(&mut db, "ALTER TABLE t1 RENAME a TO renamed").unwrap();

    let sql = trigger_sql(&db, "g");
    assert!(sql.contains("WHEN new.a<0"), "NEW.a on unrelated trigger must be untouched: {sql}");
    assert!(sql.contains("new.a, new.c"), "unrelated NEW refs must be untouched: {sql}");
}

#[test]
fn rename_column_unambiguous_still_succeeds_with_other_table_sharing_name() {
    // Guard against over-eager aborting: a qualified `t3.e` is unambiguous even
    // though t4 also owns `e`, so the ALTER must still succeed.
    let mut db = Database::new();
    setup(&mut db);
    exec(
        &mut db,
        "CREATE TRIGGER r1 INSERT ON t3 BEGIN \
         UPDATE t3 SET a=1 FROM t4 WHERE t3.e=2; END",
    )
    .unwrap();

    exec(&mut db, "ALTER TABLE t3 RENAME e TO abc").unwrap();

    let t3 = db.get_table("t3").expect("t3 exists");
    assert!(t3.schema.has_column("abc"));
    assert!(trigger_sql(&db, "r1").contains("t3.abc=2"), "{}", trigger_sql(&db, "r1"));
}
