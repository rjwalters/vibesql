//! Integration tests for `DROP VIEW` cascade-dropping INSTEAD OF triggers.
//!
//! Issue #5598: An INSTEAD OF trigger defined ON a view cannot outlive that view.
//! `DROP VIEW v` must remove every INSTEAD OF trigger whose `ON v` target is the
//! dropped view, leaving triggers on a *different* view alone. This matches
//! sqlite3 3.51.0:
//!
//! ```sql
//! CREATE VIEW v AS SELECT a FROM base;
//! CREATE TRIGGER v_ins INSTEAD OF INSERT ON v BEGIN INSERT INTO base VALUES(NEW.a); END;
//! DROP VIEW v;   -- v_ins is gone; recreating v + v_ins succeeds
//! ```
//!
//! This is the view analogue of the table-trigger cascade fixed in #5597.

use vibesql_executor::{CreateTableExecutor, TriggerExecutor, ViewExecutor};
use vibesql_parser::Parser;
use vibesql_storage::Database;

fn exec_create_table(db: &mut Database, sql: &str) {
    match Parser::parse_sql(sql).expect("parse CREATE TABLE") {
        vibesql_ast::Statement::CreateTable(s) => {
            CreateTableExecutor::execute(&s, db).expect("CREATE TABLE")
        }
        other => panic!("expected CREATE TABLE, got {other:?}"),
    };
}

fn exec_create_view(db: &mut Database, sql: &str) {
    match Parser::parse_sql(sql).expect("parse CREATE VIEW") {
        vibesql_ast::Statement::CreateView(s) => {
            ViewExecutor::execute_create_view(&s, db).expect("CREATE VIEW");
        }
        other => panic!("expected CREATE VIEW, got {other:?}"),
    };
}

fn exec_create_trigger(db: &mut Database, sql: &str) {
    match Parser::parse_sql(sql).expect("parse CREATE TRIGGER") {
        vibesql_ast::Statement::CreateTrigger(s) => {
            TriggerExecutor::create_trigger(db, &s).expect("CREATE TRIGGER");
        }
        other => panic!("expected CREATE TRIGGER, got {other:?}"),
    };
}

fn exec_drop_view(db: &mut Database, sql: &str) -> String {
    match Parser::parse_sql(sql).expect("parse DROP VIEW") {
        vibesql_ast::Statement::DropView(s) => {
            ViewExecutor::execute_drop_view(&s, db).expect("DROP VIEW")
        }
        other => panic!("expected DROP VIEW, got {other:?}"),
    }
}

/// DROP VIEW via the live CLI dispatch path
/// (`advanced_objects::execute_drop_view`).
fn exec_drop_view_advanced(db: &mut Database, sql: &str) {
    match Parser::parse_sql(sql).expect("parse DROP VIEW") {
        vibesql_ast::Statement::DropView(s) => {
            vibesql_executor::advanced_objects::execute_drop_view(&s, db).expect("DROP VIEW")
        }
        other => panic!("expected DROP VIEW, got {other:?}"),
    }
}

/// sqlite3 parity: `DROP VIEW v` drops the INSTEAD OF trigger ON v, leaves the
/// INSTEAD OF trigger on a different view, and re-creating v + its trigger
/// succeeds (no orphaned stale trigger).
#[test]
fn drop_view_cascade_drops_instead_of_trigger() {
    let mut db = Database::new();
    exec_create_table(&mut db, "CREATE TABLE base (a, b)");
    exec_create_view(&mut db, "CREATE VIEW v AS SELECT a, b FROM base");
    exec_create_view(&mut db, "CREATE VIEW v2 AS SELECT a FROM base");
    exec_create_trigger(
        &mut db,
        "CREATE TRIGGER v_ins INSTEAD OF INSERT ON v \
         BEGIN INSERT INTO base VALUES (NEW.a, NEW.b); END",
    );
    exec_create_trigger(
        &mut db,
        "CREATE TRIGGER v2_ins INSTEAD OF INSERT ON v2 \
         BEGIN INSERT INTO base (a) VALUES (NEW.a); END",
    );

    assert!(db.catalog.get_trigger("v_ins").is_some());
    assert!(db.catalog.get_trigger("v2_ins").is_some());

    let msg = exec_drop_view(&mut db, "DROP VIEW v");
    assert!(msg.contains("trigger"), "drop message should report the cascade: {msg}");

    // The trigger ON v is gone; the trigger ON v2 survives.
    assert!(db.catalog.get_trigger("v_ins").is_none(), "v_ins should be cascade-dropped");
    assert!(db.catalog.get_trigger("v2_ins").is_some(), "v2_ins must survive");

    // No orphan: re-creating the view and its trigger succeeds (would error with
    // TriggerAlreadyExists if the old one were still in the catalog).
    exec_create_view(&mut db, "CREATE VIEW v AS SELECT a, b FROM base");
    exec_create_trigger(
        &mut db,
        "CREATE TRIGGER v_ins INSTEAD OF INSERT ON v \
         BEGIN INSERT INTO base VALUES (NEW.a, NEW.b); END",
    );
    assert!(db.catalog.get_trigger("v_ins").is_some());
}

/// Dropping a view with no triggers reports the plain message (no cascade).
#[test]
fn drop_view_without_triggers_reports_plain_message() {
    let mut db = Database::new();
    exec_create_table(&mut db, "CREATE TABLE base (a)");
    exec_create_view(&mut db, "CREATE VIEW v AS SELECT a FROM base");

    let msg = exec_drop_view(&mut db, "DROP VIEW v");
    assert!(!msg.contains("trigger"), "no-trigger drop should not mention triggers: {msg}");
}

/// Dropping a `temp` view cascade-drops its temp INSTEAD OF trigger.
///
/// Note: the catalog stores views in a single flat (non-per-schema) map, so a
/// `temp` view and a same-named `main` view cannot coexist (the second CREATE
/// VIEW errors with ViewAlreadyExists — a pre-existing limitation, see #5598).
/// The temp-vs-main schema *isolation* of the cascade is therefore covered at
/// the catalog unit level (`drop_view_triggers_is_schema_aware`); here we verify
/// the executor path wires the temp view's schema tag through to the cascade.
#[test]
fn drop_temp_view_cascade_drops_its_temp_trigger() {
    let mut db = Database::new();
    exec_create_table(&mut db, "CREATE TABLE base (a)");
    exec_create_view(&mut db, "CREATE TEMP VIEW tv AS SELECT a FROM base");
    exec_create_trigger(
        &mut db,
        "CREATE TEMP TRIGGER tv_ins INSTEAD OF INSERT ON tv \
         BEGIN INSERT INTO base VALUES (NEW.a); END",
    );

    assert!(db.catalog.get_trigger("tv_ins").is_some());
    let msg = exec_drop_view(&mut db, "DROP VIEW tv");
    assert!(msg.contains("trigger"), "temp view drop should report the cascade: {msg}");
    assert!(db.catalog.get_trigger("tv_ins").is_none(), "temp trigger dropped with temp view");
}

/// The live CLI dispatch path (`advanced_objects::execute_drop_view`, used by the
/// shell/script runner) cascade-drops the INSTEAD OF trigger ON the view and
/// leaves the trigger on a different view alone — no orphan left behind.
#[test]
fn drop_view_advanced_path_cascade_drops_instead_of_trigger() {
    let mut db = Database::new();
    exec_create_table(&mut db, "CREATE TABLE base (a, b)");
    exec_create_view(&mut db, "CREATE VIEW v AS SELECT a, b FROM base");
    exec_create_view(&mut db, "CREATE VIEW v2 AS SELECT a FROM base");
    exec_create_trigger(
        &mut db,
        "CREATE TRIGGER v_ins INSTEAD OF INSERT ON v \
         BEGIN INSERT INTO base VALUES (NEW.a, NEW.b); END",
    );
    exec_create_trigger(
        &mut db,
        "CREATE TRIGGER v2_ins INSTEAD OF INSERT ON v2 \
         BEGIN INSERT INTO base (a) VALUES (NEW.a); END",
    );

    exec_drop_view_advanced(&mut db, "DROP VIEW v");
    assert!(db.catalog.get_trigger("v_ins").is_none(), "v_ins should be cascade-dropped");
    assert!(db.catalog.get_trigger("v2_ins").is_some(), "v2_ins must survive");

    // No orphan: re-creating the view + trigger succeeds.
    exec_create_view(&mut db, "CREATE VIEW v AS SELECT a, b FROM base");
    exec_create_trigger(
        &mut db,
        "CREATE TRIGGER v_ins INSTEAD OF INSERT ON v \
         BEGIN INSERT INTO base VALUES (NEW.a, NEW.b); END",
    );
    assert!(db.catalog.get_trigger("v_ins").is_some());
}
