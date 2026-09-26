//! Views over tables that do not exist (altertab.test 9.0-9.5, 24.2.x; Part
//! of #6174).
//!
//! SQLite never resolves a view's body at `CREATE VIEW` time
//! (`sqlite3CreateView`, build.c), so `CREATE VIEW v1 AS SELECT * FROM t2`
//! succeeds with no `t2`. The missing table surfaces:
//!
//! - when the view is queried (`no such table: main.t2`), and
//! - on the next ALTER-time schema re-parse, which aborts the ALTER with `error in view v1: no such
//!   table: main.t2` (altertab.test 9.1).
//!
//! A trigger that writes to / reads from such a view reports the *view's*
//! missing table, because SQLite expands the view while resolving the trigger
//! body (altertab.test 24.2.1). Views and triggers are re-validated in
//! creation order, so the object reported is the first broken one SQLite's
//! `sqlite_schema` scan would reach.

use vibesql_ast::Statement;
use vibesql_storage::Database;

use crate::errors::ExecutorError;

fn exec(db: &mut Database, sql: &str) -> Result<(), ExecutorError> {
    let stmt = vibesql_parser::Parser::parse_sql(sql).expect("parse");
    match stmt {
        Statement::CreateTable(s) => {
            crate::CreateTableExecutor::execute_with_source(&s, db, Some(sql)).map(|_| ())
        }
        Statement::CreateView(mut s) => {
            s.sql_definition = Some(sql.to_string());
            crate::advanced_objects::execute_create_view(&s, db)
        }
        Statement::DropView(s) => crate::advanced_objects::execute_drop_view(&s, db),
        Statement::CreateTrigger(s) => {
            crate::TriggerExecutor::create_trigger_with_sql(db, &s, Some(sql)).map(|_| ())
        }
        Statement::AlterTable(s) => {
            crate::alter::AlterTableExecutor::execute_with_source(&s, db, Some(sql)).map(|_| ())
        }
        Statement::Select(s) => crate::SelectExecutor::new(db).execute(&s).map(|_| ()),
        other => panic!("unexpected statement: {:?}", other),
    }
}

fn setup(sqls: &[&str]) -> Database {
    let mut db = Database::new();
    for sql in sqls {
        exec(&mut db, sql).unwrap_or_else(|e| panic!("setup `{}` failed: {}", sql, e));
    }
    db
}

#[test]
fn create_view_over_missing_table_succeeds() {
    // altertab.test 9.0
    let db = setup(&["CREATE TABLE t1(a, b, c)", "CREATE VIEW v1 AS SELECT * FROM t2"]);
    assert!(db.catalog.get_view("v1").is_some());
}

#[test]
fn querying_view_over_missing_table_reports_no_such_table() {
    let mut db = setup(&["CREATE VIEW v1 AS SELECT * FROM t2"]);
    let err = exec(&mut db, "SELECT * FROM v1").unwrap_err();
    // Rendered as `no such table: main.t2` in SQLite-compatible mode.
    assert!(
        matches!(&err, ExecutorError::TableNotFound(name) if name == "main.t2"),
        "unexpected error: {err:?}"
    );
}

#[test]
fn view_over_missing_table_resolves_once_table_is_created() {
    let mut db = setup(&["CREATE VIEW v1 AS SELECT * FROM t2", "CREATE TABLE t2(x)"]);
    exec(&mut db, "SELECT * FROM v1").expect("view resolves once its table exists");
}

#[test]
fn rename_table_rejects_view_over_missing_table() {
    // altertab.test 9.1
    let mut db = setup(&["CREATE TABLE t1(a, b, c)", "CREATE VIEW v1 AS SELECT * FROM t2"]);
    let err = exec(&mut db, "ALTER TABLE t1 RENAME TO t3").unwrap_err();
    assert_eq!(err.to_string(), "error in view v1: no such table: main.t2");
    // Atomic: the table keeps its old name.
    assert!(db.get_table("t1").is_some());
    assert!(db.get_table("t3").is_none());
}

#[test]
fn rename_table_after_dropping_broken_view_reports_broken_trigger() {
    // altertab.test 9.2/9.3
    let mut db = setup(&["CREATE TABLE t1(a, b, c)", "CREATE VIEW v1 AS SELECT * FROM t2"]);
    exec(&mut db, "DROP VIEW v1").unwrap();
    exec(&mut db, "CREATE TRIGGER tr AFTER INSERT ON t1 BEGIN INSERT INTO t2 VALUES(new.a); END")
        .unwrap();
    let err = exec(&mut db, "ALTER TABLE t1 RENAME TO t3").unwrap_err();
    assert_eq!(err.to_string(), "error in trigger tr: no such table: main.t2");
}

#[test]
fn rename_column_and_drop_column_also_reject_view_over_missing_table() {
    let mut db =
        setup(&["CREATE TABLE t1(a, b, c)", "CREATE VIEW v1 AS SELECT x FROM t1 JOIN nosuch"]);
    let err = exec(&mut db, "ALTER TABLE t1 RENAME COLUMN a TO z").unwrap_err();
    assert_eq!(err.to_string(), "error in view v1: no such table: main.nosuch");
    let err = exec(&mut db, "ALTER TABLE t1 DROP COLUMN c").unwrap_err();
    assert_eq!(err.to_string(), "error in view v1: no such table: main.nosuch");
}

#[test]
fn missing_table_nested_in_view_subquery_and_cte_is_reported() {
    let mut db = setup(&[
        "CREATE TABLE t1(a)",
        "CREATE VIEW v1 AS WITH c AS (SELECT * FROM gone) SELECT * FROM t1, c",
    ]);
    let err = exec(&mut db, "ALTER TABLE t1 RENAME TO t3").unwrap_err();
    assert_eq!(err.to_string(), "error in view v1: no such table: main.gone");

    let mut db = setup(&[
        "CREATE TABLE t1(a)",
        "CREATE VIEW v1 AS SELECT * FROM t1 WHERE a IN (SELECT x FROM gone)",
    ]);
    let err = exec(&mut db, "ALTER TABLE t1 RENAME TO t3").unwrap_err();
    assert_eq!(err.to_string(), "error in view v1: no such table: main.gone");
}

#[test]
fn trigger_writing_to_broken_view_reports_the_views_missing_table() {
    // altertab.test 24.2.0/24.2.1: the trigger is created before the view, so
    // the creation-order re-parse reaches (and reports) the trigger first, and
    // its reference to `v1` expands to the view's missing `nosuchtable`.
    let mut db = setup(&[
        "CREATE TABLE t1(a, b)",
        "CREATE TRIGGER AFTER INSERT ON t1 BEGIN \
         INSERT INTO v1 VALUES(new.a) ON CONFLICT(a) DO NOTHING; END",
        "CREATE VIEW v1 AS SELECT * FROM nosuchtable",
    ]);
    let err = exec(&mut db, "ALTER TABLE t1 RENAME TO t2").unwrap_err();
    assert_eq!(err.to_string(), "error in trigger AFTER: no such table: main.nosuchtable");
}

#[test]
fn broken_view_created_first_is_reported_before_later_broken_trigger() {
    // Creation order, not name order: `zz_view` precedes `aa_trigger`.
    let mut db = setup(&[
        "CREATE TABLE t1(a)",
        "CREATE VIEW zz_view AS SELECT * FROM gone_v",
        "CREATE TRIGGER aa_trigger AFTER INSERT ON t1 BEGIN INSERT INTO gone_t VALUES(1); END",
    ]);
    let err = exec(&mut db, "ALTER TABLE t1 RENAME TO t2").unwrap_err();
    assert_eq!(err.to_string(), "error in view zz_view: no such table: main.gone_v");
}

#[test]
fn view_chain_ending_in_missing_table_is_reported() {
    let mut db = setup(&[
        "CREATE TABLE t1(a)",
        "CREATE VIEW inner_v AS SELECT * FROM gone",
        "CREATE VIEW outer_v AS SELECT * FROM inner_v",
    ]);
    let err = exec(&mut db, "ALTER TABLE t1 RENAME TO t2").unwrap_err();
    assert_eq!(err.to_string(), "error in view inner_v: no such table: main.gone");
}

#[test]
fn views_over_system_tables_and_valid_views_do_not_block_rename() {
    let mut db = setup(&[
        "CREATE TABLE t1(a)",
        "CREATE TABLE other(x)",
        "CREATE VIEW sys AS SELECT name FROM sqlite_master",
        "CREATE VIEW sys2 AS SELECT name FROM main.sqlite_schema",
        "CREATE VIEW ok1 AS SELECT * FROM other",
        "CREATE VIEW ok2 AS WITH c AS (SELECT 1 AS y) SELECT * FROM c, ok1",
        "CREATE TABLE log(x)",
        "CREATE TRIGGER tr AFTER INSERT ON t1 BEGIN \
         INSERT INTO log SELECT name FROM sqlite_master; END",
    ]);
    exec(&mut db, "ALTER TABLE t1 RENAME TO t2").expect("rename should succeed");
    assert!(db.get_table("t2").is_some());
}
