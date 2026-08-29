//! Tests for the **per-object** dependent-object rewrite fallback on
//! `ALTER TABLE ... RENAME TO` while `PRAGMA writable_schema=ON`.
//!
//! SQLite's `renameTableFunc` (`alter.c`) attempts the trigger/view rewrite for
//! each dependent object individually and, when re-parsing/re-resolving *that
//! one object* fails, ends in:
//!
//! ```c
//! if( rc!=SQLITE_OK ){
//!   if( rc==SQLITE_ERROR && sqlite3WritableSchema(db) ){
//!     sqlite3_result_value(context, argv[3]);   /* input SQL, unchanged */
//!   }else{
//!     sqlite3_result_error_code(context, rc);
//!   }
//! }
//! ```
//!
//! So `writable_schema=ON` does **not** suppress the rewrite pass wholesale: a
//! well-formed dependent view/trigger is still rewritten (including a trigger's
//! own `ON`-target, which `alter.c` renames outside the `isLegacy` gate). Only
//! the specific object that was already broken keeps its stale SQL.
//!
//! Verified against sqlite3 3.51.0; see altercol.test 23.20 and the review on
//! PR #6653 (issue #6174).

use vibesql_ast::Statement;
use vibesql_storage::Database;

use crate::errors::ExecutorError;

fn exec(db: &mut Database, sql: &str) -> Result<String, ExecutorError> {
    let stmt = vibesql_parser::Parser::parse_sql(sql).expect("parse");
    match stmt {
        Statement::CreateTable(s) => crate::CreateTableExecutor::execute(&s, db),
        Statement::CreateView(s) => crate::ViewExecutor::execute_create_view(&s, db),
        Statement::CreateTrigger(s) => {
            crate::TriggerExecutor::create_trigger_with_sql(db, &s, Some(sql))
        }
        Statement::AlterTable(s) => crate::alter::AlterTableExecutor::execute(&s, db),
        other => panic!("unexpected statement: {:?}", other),
    }
}

/// A **healthy** view and trigger are still rewritten with `writable_schema=ON`
/// — matching sqlite3 3.51.0 byte-for-byte on this schema:
///
/// ```text
/// v1  | CREATE VIEW v1 AS SELECT a, b FROM "t1new"
/// tr1 | CREATE TRIGGER tr1 AFTER INSERT ON "t1new" BEGIN INSERT INTO t3 SELECT a FROM "t1new"; END
/// ```
#[test]
fn rename_table_still_rewrites_healthy_dependents_under_writable_schema() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t1(a INT, b INT)").unwrap();
    exec(&mut db, "CREATE TABLE t3(x INT)").unwrap();
    exec(&mut db, "CREATE VIEW v1 AS SELECT a, b FROM t1").unwrap();
    exec(
        &mut db,
        "CREATE TRIGGER tr1 AFTER INSERT ON t1 BEGIN INSERT INTO t3 SELECT a FROM t1; END",
    )
    .unwrap();

    db.set_writable_schema(true);
    exec(&mut db, "ALTER TABLE t1 RENAME TO t1new").unwrap();

    let view = db.catalog.get_view("v1").expect("v1 must survive the rename");
    let view_sql = view.sql_definition.as_deref().expect("v1 keeps its verbatim CREATE VIEW text");
    assert!(
        view_sql.contains("t1new"),
        "healthy view body must still be rewritten under writable_schema=ON, got: {view_sql}"
    );

    let trigger = db.catalog.get_trigger("tr1").expect("tr1 must survive the rename");
    assert_eq!(
        trigger.table_name.to_ascii_lowercase(),
        "t1new",
        "a trigger's own ON-target is renamed unconditionally in SQLite"
    );
    let trigger_sql =
        trigger.sql_definition.as_deref().expect("tr1 keeps its verbatim CREATE TRIGGER text");
    assert!(
        trigger_sql.contains("t1new"),
        "healthy trigger body must still be rewritten under writable_schema=ON, got: {trigger_sql}"
    );
}

/// altercol.test 23.20's shape: an **already-broken** view (`c99` is not a
/// column of `t4`) keeps its stale SQL under `writable_schema=ON` — that is the
/// per-object fallback — while the healthy view alongside it is still rewritten.
#[test]
fn rename_table_leaves_only_the_broken_view_stale_under_writable_schema() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t4(id INTEGER PRIMARY KEY, c1 INT, c2 INT)").unwrap();
    exec(&mut db, "CREATE VIEW t4v1 AS SELECT id, c1, c99 FROM t4").unwrap();
    exec(&mut db, "CREATE VIEW t4v2 AS SELECT id, c1 FROM t4").unwrap();

    // Snapshot the broken view's stored state so "left alone" can be asserted
    // as byte-identical rather than merely "does not mention the new name".
    let broken_before = db.catalog.get_view("t4v1").expect("t4v1 exists").sql_definition.clone();

    db.set_writable_schema(true);
    exec(&mut db, "ALTER TABLE t4 RENAME TO t4new").unwrap();

    let broken = db.catalog.get_view("t4v1").expect("t4v1 must survive");
    assert_eq!(
        broken.sql_definition, broken_before,
        "the already-broken view keeps its hand-edited SQL untouched"
    );
    assert!(
        !view_query_sql(db.catalog.get_view("t4v1").unwrap()).contains("t4new"),
        "the already-broken view's parsed query is not retargeted either"
    );

    let healthy = db.catalog.get_view("t4v2").expect("t4v2 must survive");
    let healthy_sql = healthy.sql_definition.as_deref().expect("verbatim text");
    assert!(
        healthy_sql.contains("t4new"),
        "the healthy view alongside it is still rewritten, got: {healthy_sql}"
    );
}

/// The view's defining query rendered back to SQL — used to assert on views
/// created without captured verbatim `CREATE VIEW` text.
fn view_query_sql(view: &vibesql_catalog::ViewDefinition) -> String {
    use vibesql_ast::pretty_print::ToSql;
    view.query.to_sql()
}

/// With `writable_schema` OFF (the default), an already-broken dependent view
/// aborts the whole `RENAME TO` — matching `precheck_schema_objects` (issue
/// #6174, altertab3.test 4.1.2/4.2.1 shape) and re-verified directly against
/// sqlite3 3.51.0 with `PRAGMA legacy_alter_table=OFF` (the modern, non-legacy
/// semantics the SQLite test suite itself assumes as baseline):
///
/// ```text
/// sqlite> PRAGMA legacy_alter_table=OFF;
/// sqlite> CREATE TABLE t4(id INTEGER PRIMARY KEY, c1 INT, c2 INT);
/// sqlite> CREATE VIEW t4v1 AS SELECT id, c1, c99 FROM t4;
/// sqlite> ALTER TABLE t4 RENAME TO t4new;
/// Error: stepping, error in view t4v1: no such column: c99
/// ```
///
/// (Superseded 2026-08-29: this test previously asserted the *opposite* —
/// that the rename silently succeeds and rewrites the broken view — which
/// only holds under `legacy_alter_table=ON`, the pre-3.25 compatibility mode
/// vibesql does not model as a separate pragma. The bare `sqlite3` CLI on this
/// host happens to default `legacy_alter_table` to `ON`, which is what made
/// the original assertion look verified; with the modern `OFF` semantics the
/// whole test suite (and vibesql) actually targets, real SQLite rejects this
/// exactly like the trigger case below.)
#[test]
fn rename_table_rejects_broken_view_when_writable_schema_is_off() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t4(id INTEGER PRIMARY KEY, c1 INT, c2 INT)").unwrap();
    exec(&mut db, "CREATE VIEW t4v1 AS SELECT id, c1, c99 FROM t4").unwrap();

    let err = exec(&mut db, "ALTER TABLE t4 RENAME TO t4new").unwrap_err();
    assert_eq!(err.to_string(), "error in view t4v1: no such column: c99");

    // The failed ALTER leaves the schema untouched: t4 keeps its name and the
    // view's stored SQL is unmodified.
    assert!(db.get_table("t4").is_some());
    assert!(db.get_table("t4new").is_none());
    let view = db.catalog.get_view("t4v1").expect("t4v1 must survive");
    let sql = view.sql_definition.clone().unwrap_or_else(|| view_query_sql(view));
    assert!(!sql.contains("t4new"), "a rejected ALTER must not rewrite the broken view: {sql}");
}

/// A trigger whose *body* references a table that does not exist is broken in
/// SQLite's re-parse (`error in trigger r3: no such table: main.t3`), so under
/// `writable_schema=ON` it keeps its stale SQL — including its ON-target —
/// while a healthy sibling trigger on the same table is still rewritten.
/// This is the trigger half of altercol.test 23.20.
#[test]
fn rename_table_leaves_only_the_broken_trigger_stale_under_writable_schema() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t1(a INT, b INT)").unwrap();
    exec(&mut db, "CREATE TABLE keep(x INT)").unwrap();
    // `t3` was never created: this trigger does not re-resolve.
    exec(
        &mut db,
        "CREATE TRIGGER broken AFTER INSERT ON t1 BEGIN INSERT INTO t3 SELECT a FROM t1; END",
    )
    .unwrap();
    exec(
        &mut db,
        "CREATE TRIGGER healthy AFTER INSERT ON t1 BEGIN INSERT INTO keep SELECT a FROM t1; END",
    )
    .unwrap();

    db.set_writable_schema(true);
    exec(&mut db, "ALTER TABLE t1 RENAME TO t1new").unwrap();

    let broken = db.catalog.get_trigger("broken").expect("broken trigger must survive");
    assert_eq!(
        broken.table_name.to_ascii_lowercase(),
        "t1",
        "the broken trigger keeps its stale ON-target"
    );
    let broken_sql = broken.sql_definition.as_deref().expect("verbatim text");
    assert!(!broken_sql.contains("t1new"), "the broken trigger keeps its stale SQL: {broken_sql}");

    let healthy = db.catalog.get_trigger("healthy").expect("healthy trigger must survive");
    assert_eq!(
        healthy.table_name.to_ascii_lowercase(),
        "t1new",
        "the healthy trigger is still retargeted"
    );
    let healthy_sql = healthy.sql_definition.as_deref().expect("verbatim text");
    assert!(healthy_sql.contains("t1new"), "the healthy trigger is still rewritten: {healthy_sql}");
}
