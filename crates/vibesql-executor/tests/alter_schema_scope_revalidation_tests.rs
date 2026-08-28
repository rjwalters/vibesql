//! Part of #6174: `ALTER TABLE ... DROP COLUMN` / `RENAME COLUMN` re-parse only
//! the ALTERED TABLE's OWN schema, not the whole catalog.
//!
//! SQLite's `renameReloadSchema` / `sqlite3InitOne` reload the ONE schema
//! (`iDb`) the altered table lives in, so a view or trigger that is already
//! broken but lives in a *different* schema is never re-parsed and must not
//! abort the ALTER. Before this scoping fix,
//! `precheck_schema_objects`/`postcheck_schema_objects` walked every view and
//! trigger in the catalog, so a broken `main`-schema object aborted an ALTER on
//! a `temp` table (and vice versa) — and, when both schemas held a broken
//! object, whichever one the catalog happened to yield first won the race and
//! reported the wrong error.
//!
//! Expectations verified against sqlite3 3.51.0 (altercol.test 17.1 / 17.3
//! shape: a broken main-schema trigger `u7t` must not block an ALTER on the
//! unrelated temp-schema table, while that table's own equally-broken
//! temp-schema trigger DOES block it):
//!
//! ```text
//! CREATE TABLE m1(a, b, c);
//! CREATE TEMP TABLE p1(a, b, c);
//! CREATE TRIGGER mtr AFTER INSERT ON m1 BEGIN INSERT INTO no_main VALUES(new.a); END;
//! ALTER TABLE temp.p1 RENAME COLUMN c TO cc;   -- ok: mtr lives in main, p1 in temp
//! ALTER TABLE main.m1 RENAME COLUMN c TO cc;   -- Error: error in trigger mtr: ...
//! ```
//!
//! The TCL suite cannot currently prove this (the harness demotes
//! `CREATE TEMP TABLE` to a plain persistent table across its process-per-batch
//! re-invocations, losing the temp/main distinction before the trigger is even
//! created), so these Rust-level tests are the only regression guard for the
//! scoping behavior.

use vibesql_executor::{AlterTableExecutor, CreateTableExecutor, TriggerExecutor, ViewExecutor};
use vibesql_parser::Parser;
use vibesql_storage::Database;

fn create_table(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).expect("parse CREATE TABLE");
    let vibesql_ast::Statement::CreateTable(create) = stmt else {
        panic!("expected CREATE TABLE");
    };
    CreateTableExecutor::execute_with_source(&create, db, Some(sql)).expect("CREATE TABLE");
}

fn create_view(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).expect("parse CREATE VIEW");
    let vibesql_ast::Statement::CreateView(view) = stmt else {
        panic!("expected CREATE VIEW");
    };
    ViewExecutor::execute_create_view(&view, db).expect("CREATE VIEW");
}

fn create_trigger(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).expect("parse CREATE TRIGGER");
    let vibesql_ast::Statement::CreateTrigger(trigger) = stmt else {
        panic!("expected CREATE TRIGGER");
    };
    TriggerExecutor::create_trigger(db, &trigger).expect("CREATE TRIGGER");
}

fn try_alter(db: &mut Database, sql: &str) -> Result<String, String> {
    let stmt = Parser::parse_sql(sql).expect("parse ALTER");
    let vibesql_ast::Statement::AlterTable(a) = stmt else {
        panic!("expected ALTER TABLE");
    };
    AlterTableExecutor::execute_with_source(&a, db, Some(sql)).map_err(|e| e.to_string())
}

fn alter_err(db: &mut Database, sql: &str) -> String {
    try_alter(db, sql).expect_err(&format!("expected ALTER to fail: {sql}"))
}

/// `main.m1` + `temp.p1`, each with a *broken* trigger in its own schema.
///
/// Both triggers reference a table that does not exist, so each is fatal to an
/// ALTER of a table in its own schema — and, under correct scoping, invisible
/// to an ALTER of a table in the other schema.
fn db_with_broken_trigger_in_each_schema() -> Database {
    let mut db = Database::new();
    create_table(&mut db, "CREATE TABLE m1(a, b, c)");
    create_table(&mut db, "CREATE TEMP TABLE p1(a, b, c)");
    create_trigger(
        &mut db,
        "CREATE TRIGGER mtr AFTER INSERT ON m1 BEGIN INSERT INTO no_main VALUES(new.a); END",
    );
    create_trigger(
        &mut db,
        "CREATE TEMP TRIGGER ptr AFTER INSERT ON p1 BEGIN INSERT INTO no_temp VALUES(new.a); END",
    );
    db
}

fn has_column(db: &Database, table: &str, column: &str) -> bool {
    db.get_table(table)
        .unwrap_or_else(|| panic!("table {table} should exist"))
        .schema
        .has_column(column)
}

// ---------------------------------------------------------------------------
// RENAME COLUMN
// ---------------------------------------------------------------------------

/// A broken `main`-schema trigger must not abort a RENAME COLUMN on a `temp`
/// table: SQLite only reloads the temp schema, which holds nothing broken.
#[test]
fn rename_on_temp_table_ignores_broken_main_trigger() {
    let mut db = Database::new();
    create_table(&mut db, "CREATE TABLE m1(a, b, c)");
    create_table(&mut db, "CREATE TEMP TABLE p1(a, b, c)");
    create_trigger(
        &mut db,
        "CREATE TRIGGER mtr AFTER INSERT ON m1 BEGIN INSERT INTO no_main VALUES(new.a); END",
    );

    try_alter(&mut db, "ALTER TABLE temp.p1 RENAME COLUMN c TO cc")
        .expect("main-schema trigger must not block a temp-table rename");
    assert!(has_column(&db, "temp.p1", "cc"));
    assert!(!has_column(&db, "temp.p1", "c"));
}

/// The symmetric case: a broken `temp`-schema trigger must not abort a RENAME
/// COLUMN on a `main` table.
#[test]
fn rename_on_main_table_ignores_broken_temp_trigger() {
    let mut db = Database::new();
    create_table(&mut db, "CREATE TABLE m1(a, b, c)");
    create_table(&mut db, "CREATE TEMP TABLE p1(a, b, c)");
    create_trigger(
        &mut db,
        "CREATE TEMP TRIGGER ptr AFTER INSERT ON p1 BEGIN INSERT INTO no_temp VALUES(new.a); END",
    );

    try_alter(&mut db, "ALTER TABLE main.m1 RENAME COLUMN c TO cc")
        .expect("temp-schema trigger must not block a main-table rename");
    assert!(has_column(&db, "main.m1", "cc"));
    assert!(!has_column(&db, "main.m1", "c"));
}

/// With a broken trigger in *both* schemas, a temp-table RENAME reports the
/// TEMP trigger — the one that actually lives in the reloaded schema — never
/// the main-schema one.
#[test]
fn rename_on_temp_table_reports_its_own_temp_trigger() {
    let mut db = db_with_broken_trigger_in_each_schema();

    let msg = alter_err(&mut db, "ALTER TABLE temp.p1 RENAME COLUMN c TO cc");
    assert!(
        msg.contains("trigger ptr"),
        "temp-table rename must report the TEMP trigger, got: {msg}"
    );
    assert!(
        !msg.contains("trigger mtr"),
        "temp-table rename must not report the main-schema trigger, got: {msg}"
    );
    // The failed ALTER leaves both tables untouched.
    assert!(has_column(&db, "temp.p1", "c"));
    assert!(has_column(&db, "main.m1", "c"));
}

/// The symmetric case: with a broken trigger in both schemas, a main-table
/// RENAME reports the MAIN trigger.
#[test]
fn rename_on_main_table_reports_its_own_main_trigger() {
    let mut db = db_with_broken_trigger_in_each_schema();

    let msg = alter_err(&mut db, "ALTER TABLE main.m1 RENAME COLUMN c TO cc");
    assert!(
        msg.contains("trigger mtr"),
        "main-table rename must report the main trigger, got: {msg}"
    );
    assert!(
        !msg.contains("trigger ptr"),
        "main-table rename must not report the temp-schema trigger, got: {msg}"
    );
    assert!(has_column(&db, "main.m1", "c"));
    assert!(has_column(&db, "temp.p1", "c"));
}

// ---------------------------------------------------------------------------
// DROP COLUMN
// ---------------------------------------------------------------------------

/// A broken `main`-schema trigger must not abort a DROP COLUMN on a `temp`
/// table (the pre-drop half of `check_schema_objects`).
#[test]
fn drop_column_on_temp_table_ignores_broken_main_trigger() {
    let mut db = Database::new();
    create_table(&mut db, "CREATE TABLE m1(a, b, c)");
    create_table(&mut db, "CREATE TEMP TABLE p1(a, b, c)");
    create_trigger(
        &mut db,
        "CREATE TRIGGER mtr AFTER INSERT ON m1 BEGIN INSERT INTO no_main VALUES(new.a); END",
    );

    try_alter(&mut db, "ALTER TABLE temp.p1 DROP COLUMN c")
        .expect("main-schema trigger must not block a temp-table drop");
    assert!(!has_column(&db, "temp.p1", "c"));
}

/// The symmetric case for DROP COLUMN: a broken `temp`-schema trigger must not
/// abort a DROP COLUMN on a `main` table.
#[test]
fn drop_column_on_main_table_ignores_broken_temp_trigger() {
    let mut db = Database::new();
    create_table(&mut db, "CREATE TABLE m1(a, b, c)");
    create_table(&mut db, "CREATE TEMP TABLE p1(a, b, c)");
    create_trigger(
        &mut db,
        "CREATE TEMP TRIGGER ptr AFTER INSERT ON p1 BEGIN INSERT INTO no_temp VALUES(new.a); END",
    );

    try_alter(&mut db, "ALTER TABLE main.m1 DROP COLUMN c")
        .expect("temp-schema trigger must not block a main-table drop");
    assert!(!has_column(&db, "main.m1", "c"));
}

/// With a broken trigger in both schemas, a temp-table DROP COLUMN reports the
/// TEMP trigger.
#[test]
fn drop_column_on_temp_table_reports_its_own_temp_trigger() {
    let mut db = db_with_broken_trigger_in_each_schema();

    let msg = alter_err(&mut db, "ALTER TABLE temp.p1 DROP COLUMN c");
    assert!(
        msg.contains("trigger ptr"),
        "temp-table drop must report the TEMP trigger, got: {msg}"
    );
    assert!(
        !msg.contains("trigger mtr"),
        "temp-table drop must not report the main-schema trigger, got: {msg}"
    );
    assert!(has_column(&db, "temp.p1", "c"));
}

/// With a broken trigger in both schemas, a main-table DROP COLUMN reports the
/// MAIN trigger.
#[test]
fn drop_column_on_main_table_reports_its_own_main_trigger() {
    let mut db = db_with_broken_trigger_in_each_schema();

    let msg = alter_err(&mut db, "ALTER TABLE main.m1 DROP COLUMN c");
    assert!(
        msg.contains("trigger mtr"),
        "main-table drop must report the main trigger, got: {msg}"
    );
    assert!(
        !msg.contains("trigger ptr"),
        "main-table drop must not report the temp-schema trigger, got: {msg}"
    );
    assert!(has_column(&db, "main.m1", "c"));
}

// ---------------------------------------------------------------------------
// Views (the other half of the scoped walk)
// ---------------------------------------------------------------------------

/// The view arm of the same walk: a broken `main`-schema VIEW must not abort an
/// ALTER on a `temp` table.
#[test]
fn rename_on_temp_table_ignores_broken_main_view() {
    let mut db = Database::new();
    create_table(&mut db, "CREATE TABLE m1(a, b, c)");
    create_table(&mut db, "CREATE TEMP TABLE p1(a, b, c)");
    create_view(&mut db, "CREATE VIEW mv AS SELECT a, nosuchcol FROM m1");

    try_alter(&mut db, "ALTER TABLE temp.p1 RENAME COLUMN c TO cc")
        .expect("main-schema view must not block a temp-table rename");
    assert!(has_column(&db, "temp.p1", "cc"));
}

/// ...and the broken `main`-schema VIEW still aborts an ALTER on a `main` table
/// (the scoping narrows the walk, it does not disable it).
#[test]
fn rename_on_main_table_still_reports_broken_main_view() {
    let mut db = Database::new();
    create_table(&mut db, "CREATE TABLE m1(a, b, c)");
    create_table(&mut db, "CREATE TEMP TABLE p1(a, b, c)");
    create_view(&mut db, "CREATE VIEW mv AS SELECT a, nosuchcol FROM m1");

    assert_eq!(
        alter_err(&mut db, "ALTER TABLE main.m1 RENAME COLUMN c TO cc"),
        "error in view mv: no such column: nosuchcol"
    );
    assert!(has_column(&db, "main.m1", "c"));
}
