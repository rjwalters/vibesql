//! Integration tests for schema-qualified `CREATE VIEW` / `DROP VIEW`
//! (issue #6490).
//!
//! Before this fix, `CREATE VIEW <alias>.<name> AS ...` against an
//! attached-database alias never split the schema qualifier off the view
//! name (unlike `CREATE TABLE`, which already did via
//! `CreateTableExecutor::execute_impl`'s `split_once('.')`). The view landed
//! in the default (`main`) schema under the literal, dot-containing name
//! `"<alias>.<name>"` instead of a view named `<name>` inside `<alias>`.
//!
//! Verified against sqlite3 3.51.0:
//!
//! ```sql
//! ATTACH ':memory:' AS aux;
//! CREATE VIEW aux.v1 AS SELECT 1;  -- ok, homed in aux
//! CREATE VIEW bogus.v1 AS SELECT 1;  -- "unknown database bogus"
//! DROP VIEW aux.v1;  -- ok
//! ```

use vibesql_executor::advanced_objects::{execute_create_view, execute_drop_view};
use vibesql_parser::Parser;
use vibesql_storage::Database;

fn exec_create_table(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).expect("parse CREATE TABLE");
    match stmt {
        vibesql_ast::Statement::CreateTable(s) => {
            vibesql_executor::CreateTableExecutor::execute(&s, db).expect("CREATE TABLE")
        }
        other => panic!("expected CREATE TABLE, got {other:?}"),
    };
}

/// Parse + execute a CREATE VIEW statement via the CLI's actual dispatch path
/// (`vibesql_executor::advanced_objects::execute_create_view`), returning the
/// executor result.
fn exec_create_view(db: &mut Database, sql: &str) -> Result<(), vibesql_executor::ExecutorError> {
    let stmt = Parser::parse_sql(sql).expect("parse CREATE VIEW");
    match stmt {
        vibesql_ast::Statement::CreateView(s) => execute_create_view(&s, db),
        other => panic!("expected CreateView, got {other:?}"),
    }
}

/// Parse + execute a DROP VIEW statement.
fn exec_drop_view(db: &mut Database, sql: &str) -> Result<(), vibesql_executor::ExecutorError> {
    let stmt = Parser::parse_sql(sql).expect("parse DROP VIEW");
    match stmt {
        vibesql_ast::Statement::DropView(s) => execute_drop_view(&s, db),
        other => panic!("expected DropView, got {other:?}"),
    }
}

/// `CREATE VIEW aux.v1 AS ...` against an attached alias creates a view named
/// `v1` homed *inside* the `aux` schema — not a `main`-schema view literally
/// named `"aux.v1"`.
#[test]
fn create_view_attached_schema_qualified_homes_view_in_alias() {
    let mut db = Database::new();
    db.catalog.attach_database("aux", ":memory:").expect("ATTACH DATABASE");
    exec_create_table(&mut db, "CREATE TABLE aux.t(x INTEGER)");

    exec_create_view(&mut db, "CREATE VIEW aux.v1 AS SELECT * FROM aux.t").expect("CREATE VIEW");

    // The view is stored bare ("v1"), tagged to the "aux" schema — not
    // literally named "aux.v1" in the default schema.
    let view = db.catalog.get_view_in_schema("v1", Some("aux")).expect("aux.v1 should exist");
    assert_eq!(view.name, "v1", "view should be stored under its bare name");
    assert_eq!(view.schema.as_deref(), Some("aux"));

    // It must NOT appear in the main schema at all (neither as "v1" nor as
    // the literal dotted "aux.v1").
    assert!(
        db.catalog.get_view_in_schema("v1", None).is_none(),
        "main schema must not contain a v1 view"
    );
    assert!(
        db.catalog.get_view_in_schema("aux.v1", None).is_none(),
        "main schema must not contain a literal 'aux.v1'-named view"
    );
}

/// A `main.v1` and an `aux.v1` view can coexist without colliding — SQLite
/// scopes view names per schema, exactly like tables (e_dropview.test 1.1).
#[test]
fn main_and_attached_schema_views_with_same_name_coexist() {
    let mut db = Database::new();
    db.catalog.attach_database("aux", ":memory:").expect("ATTACH DATABASE");
    exec_create_table(&mut db, "CREATE TABLE t(x INTEGER)");
    exec_create_table(&mut db, "CREATE TABLE aux.t(x INTEGER)");

    exec_create_view(&mut db, "CREATE VIEW v1 AS SELECT * FROM t").expect("CREATE VIEW main.v1");
    exec_create_view(&mut db, "CREATE VIEW aux.v1 AS SELECT * FROM aux.t")
        .expect("CREATE VIEW aux.v1 should not collide with main.v1");

    assert!(db.catalog.get_view_in_schema("v1", None).is_some());
    assert!(db.catalog.get_view_in_schema("v1", Some("aux")).is_some());

    // `list_views()` sees both bare-named entries (schema-blind); `iter_views()`
    // sees both distinct definitions.
    assert_eq!(db.catalog.iter_views().filter(|v| v.name == "v1").count(), 2);
}

/// `DROP VIEW aux.v1` removes exactly the attached-schema view, leaving a
/// same-named `main` view untouched (regression guard: pre-fix, DROP VIEW
/// "worked" only by accident of both landing on the same mis-homed entry).
#[test]
fn drop_view_attached_schema_qualified_is_scoped_to_alias() {
    let mut db = Database::new();
    db.catalog.attach_database("aux", ":memory:").expect("ATTACH DATABASE");
    exec_create_table(&mut db, "CREATE TABLE t(x INTEGER)");
    exec_create_table(&mut db, "CREATE TABLE aux.t(x INTEGER)");
    exec_create_view(&mut db, "CREATE VIEW v1 AS SELECT * FROM t").expect("CREATE VIEW main.v1");
    exec_create_view(&mut db, "CREATE VIEW aux.v1 AS SELECT * FROM aux.t")
        .expect("CREATE VIEW aux.v1");

    exec_drop_view(&mut db, "DROP VIEW aux.v1").expect("DROP VIEW aux.v1");

    assert!(db.catalog.get_view_in_schema("v1", Some("aux")).is_none(), "aux.v1 should be gone");
    assert!(db.catalog.get_view_in_schema("v1", None).is_some(), "main.v1 should survive");
}

/// An unqualified `FROM v1` resolves to the attached-schema view once no
/// `temp`/`main` view of that name remains, matching sqlite3's unqualified
/// search order (temp, main, then attached databases in attachment order) —
/// this is the "no such table: v1" regression called out in #6490's
/// discovery notes (e_dropview.test 3.4.0).
#[test]
fn unqualified_select_resolves_attached_schema_view_when_main_absent() {
    let mut db = Database::new();
    db.catalog.attach_database("aux", ":memory:").expect("ATTACH DATABASE");
    exec_create_table(&mut db, "CREATE TABLE aux.t(x INTEGER)");
    db.insert_row("aux.t", vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(7)]))
        .expect("insert into aux.t");
    exec_create_view(&mut db, "CREATE VIEW aux.v1 AS SELECT * FROM aux.t").expect("CREATE VIEW");

    let stmt = Parser::parse_sql("SELECT * FROM v1").expect("parse SELECT");
    let result = match stmt {
        vibesql_ast::Statement::Select(s) => {
            vibesql_executor::SelectExecutor::new(&db).execute_with_columns(&s)
        }
        other => panic!("expected SELECT, got {other:?}"),
    };
    let result = result.expect("SELECT * FROM v1 should resolve to aux.v1");
    assert_eq!(result.rows.len(), 1, "should read aux.t's single row through aux.v1");
}

/// An unrecognized schema qualifier is rejected with SQLite's exact `unknown
/// database <name>` wording (mirrors CREATE TABLE/CREATE TRIGGER/CREATE
/// INDEX parity).
#[test]
fn create_view_unknown_schema_qualifier_rejected() {
    let mut db = Database::new();
    exec_create_table(&mut db, "CREATE TABLE t(x INTEGER)");

    let result = exec_create_view(&mut db, "CREATE VIEW bogus.v1 AS SELECT * FROM t");
    assert!(result.is_err(), "CREATE VIEW bogus.v1 should fail");
    let err = result.unwrap_err().to_string();
    assert_eq!(err, "unknown database bogus", "unexpected error text: {err}");
}

/// `CREATE TEMP VIEW <schema>.<name>` for any schema other than `temp` itself
/// is rejected, mirroring `CREATE TEMP TABLE`'s identical guard
/// (#6173/#6406).
#[test]
fn create_temp_view_with_non_temp_schema_qualifier_rejected() {
    let mut db = Database::new();
    db.catalog.attach_database("aux", ":memory:").expect("ATTACH DATABASE");
    exec_create_table(&mut db, "CREATE TABLE aux.t(x INTEGER)");

    let result = exec_create_view(&mut db, "CREATE TEMP VIEW aux.v1 AS SELECT * FROM aux.t");
    assert!(result.is_err(), "CREATE TEMP VIEW aux.v1 should fail");
    let err = result.unwrap_err().to_string();
    assert_eq!(err, "temporary view name must be unqualified", "unexpected error text: {err}");
}

/// `CREATE TEMP VIEW temp.v1` (the redundant `temp.` qualifier) is allowed,
/// mirroring `CREATE TEMP TABLE temp.t(...)`.
#[test]
fn create_temp_view_with_redundant_temp_qualifier_allowed() {
    let mut db = Database::new();
    exec_create_table(&mut db, "CREATE TABLE t(x INTEGER)");

    let result = exec_create_view(&mut db, "CREATE TEMP VIEW temp.v1 AS SELECT * FROM t");
    assert!(result.is_ok(), "CREATE TEMP VIEW temp.v1 should succeed: {:?}", result.err());
    let view = db.catalog.get_view_in_schema("v1", Some("temp")).expect("temp.v1 should exist");
    assert!(view.is_temp());
}

/// `sqlite_master`-shaped listing for the `aux` schema (the mechanism behind
/// `<alias>.sqlite_master`) includes the attached-schema view and excludes
/// it from the `main` listing — the acceptance criteria from #6490.
#[test]
fn attached_schema_view_appears_only_in_its_own_schema_listing() {
    let mut db = Database::new();
    db.catalog.attach_database("aux", ":memory:").expect("ATTACH DATABASE");
    exec_create_table(&mut db, "CREATE TABLE t(x INTEGER)");
    exec_create_table(&mut db, "CREATE TABLE aux.t(x INTEGER)");
    exec_create_view(&mut db, "CREATE VIEW v1 AS SELECT * FROM t").expect("CREATE VIEW main.v1");
    exec_create_view(&mut db, "CREATE VIEW aux.v1 AS SELECT * FROM aux.t")
        .expect("CREATE VIEW aux.v1");

    let main_listing = vibesql_executor::sqlite_schema::execute_sqlite_schema_query(&db.catalog)
        .expect("main sqlite_master query");
    let main_views: Vec<&str> = main_listing
        .rows
        .iter()
        .filter(|row| matches!(&row.values[0], vibesql_types::SqlValue::Varchar(t) if t.as_str() == "view"))
        .map(|row| match &row.values[1] {
            vibesql_types::SqlValue::Varchar(n) => n.as_str(),
            _ => panic!("expected varchar name"),
        })
        .collect();
    assert_eq!(main_views, vec!["v1"], "main.sqlite_master should list exactly main's v1");

    let aux_listing =
        vibesql_executor::sqlite_schema::execute_sqlite_schema_query_for_schema(&db.catalog, "aux")
            .expect("aux sqlite_master query");
    let aux_views: Vec<&str> = aux_listing
        .rows
        .iter()
        .filter(|row| matches!(&row.values[0], vibesql_types::SqlValue::Varchar(t) if t.as_str() == "view"))
        .map(|row| match &row.values[1] {
            vibesql_types::SqlValue::Varchar(n) => n.as_str(),
            _ => panic!("expected varchar name"),
        })
        .collect();
    assert_eq!(aux_views, vec!["v1"], "aux.sqlite_master should list exactly aux's v1");
}
