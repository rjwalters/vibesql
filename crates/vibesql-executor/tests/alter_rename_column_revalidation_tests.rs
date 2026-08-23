//! Part of #6174: `ALTER TABLE ... RENAME COLUMN` re-validates the whole schema
//! before mutating anything — the same dependent-object re-parse SQLite runs
//! (and that `DROP COLUMN` already performed). A view or trigger that is
//! *already* broken (references a column or a table that does not resolve)
//! aborts the RENAME with SQLite's verbatim `error in <type> <name>: <inner>`
//! message, leaving the schema untouched.
//!
//! Verified against sqlite3 3.51.0 / `altercol.test` sections 13, 17, and 23:
//!   * trigger body reading a non-existent table  -> `error in trigger tr1: no such table:
//!     main.nosuchtable`
//!   * trigger body inserting into a missing table -> `error in trigger u7t: no such table:
//!     main.u8`
//!   * view selecting a non-existent column        -> `error in view t2: no such column: xyz`

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

fn rename_column_err(db: &mut Database, sql: &str) -> String {
    try_alter(db, sql).expect_err(&format!("expected RENAME COLUMN to fail: {sql}"))
}

/// altercol.test 13.1.2: a trigger whose body reads from a table that does not
/// exist aborts the RENAME. No "after rename" suffix — the trigger was broken
/// before the rename — and the missing table is qualified `main.<name>`.
#[test]
fn rename_rejected_when_trigger_reads_missing_table() {
    let mut db = Database::new();
    create_table(&mut db, "CREATE TABLE x1(i INTEGER, t TEXT UNIQUE)");
    create_trigger(
        &mut db,
        "CREATE TRIGGER tr1 AFTER INSERT ON x1 BEGIN SELECT * FROM nosuchtable; END",
    );
    assert_eq!(
        rename_column_err(&mut db, "ALTER TABLE x1 RENAME COLUMN t TO ttt"),
        "error in trigger tr1: no such table: main.nosuchtable"
    );
    // The failed ALTER must leave x1 untouched.
    assert!(db.get_table("x1").unwrap().schema.has_column("t"));
    assert!(!db.get_table("x1").unwrap().schema.has_column("ttt"));
}

/// altercol.test 17.1: a trigger whose body inserts into a table that does not
/// exist aborts the RENAME with the same `no such table: main.<name>` message.
#[test]
fn rename_rejected_when_trigger_inserts_into_missing_table() {
    let mut db = Database::new();
    create_table(&mut db, "CREATE TABLE u7(x, y, z)");
    create_trigger(
        &mut db,
        "CREATE TRIGGER u7t AFTER INSERT ON u7 BEGIN \
         INSERT INTO u8 VALUES(new.x, new.y, new.z); END",
    );
    assert_eq!(
        rename_column_err(&mut db, "ALTER TABLE u7 RENAME COLUMN x TO xxx"),
        "error in trigger u7t: no such table: main.u8"
    );
    assert!(db.get_table("u7").unwrap().schema.has_column("x"));
}

/// altercol.test 23.1: a view whose SELECT names a column that does not resolve
/// aborts the RENAME with `error in view <name>: no such column: <col>`.
#[test]
fn rename_rejected_when_view_selects_missing_column() {
    let mut db = Database::new();
    create_table(&mut db, "CREATE TABLE t1(a INT, b REAL, c TEXT, d BLOB, e ANY)");
    create_view(&mut db, "CREATE VIEW t2 AS SELECT a+10, b*5.0, xyz FROM t1");
    assert_eq!(
        rename_column_err(&mut db, "ALTER TABLE t1 RENAME COLUMN e TO eeee"),
        "error in view t2: no such column: xyz"
    );
    // The failed ALTER must leave t1 untouched.
    assert!(db.get_table("t1").unwrap().schema.has_column("e"));
    assert!(!db.get_table("t1").unwrap().schema.has_column("eeee"));
}

/// A rename whose only dependent objects are well-formed still succeeds: the
/// re-validation must not false-positive. The trigger reads from and writes to
/// tables that exist, and the view selects real columns.
#[test]
fn rename_succeeds_when_dependents_are_valid() {
    let mut db = Database::new();
    create_table(&mut db, "CREATE TABLE t1(a, b, c)");
    create_table(&mut db, "CREATE TABLE log(id)");
    create_view(&mut db, "CREATE VIEW v1 AS SELECT a, b FROM t1");
    create_trigger(
        &mut db,
        "CREATE TRIGGER tr AFTER INSERT ON t1 BEGIN INSERT INTO log VALUES(new.a); END",
    );
    try_alter(&mut db, "ALTER TABLE t1 RENAME COLUMN c TO cc").expect("rename should succeed");
    assert!(db.get_table("t1").unwrap().schema.has_column("cc"));
    assert!(!db.get_table("t1").unwrap().schema.has_column("c"));
}

/// altercol.test 12.2.2 / 12.2.3: renaming a column of a VIEW is rejected with
/// SQLite's dedicated `cannot rename columns of view "<name>"` message rather
/// than falling through to the generic `no such table` path.
#[test]
fn rename_column_of_view_is_rejected() {
    let mut db = Database::new();
    create_table(&mut db, "CREATE TABLE t1(a, b)");
    create_view(&mut db, "CREATE VIEW v1 AS SELECT * FROM t1");
    assert_eq!(
        rename_column_err(&mut db, "ALTER TABLE v1 RENAME a TO z"),
        "cannot rename columns of view \"v1\""
    );
    // The base table is untouched.
    assert!(db.get_table("t1").unwrap().schema.has_column("a"));
}

/// altercol.test 12.4.2: renaming a column that does not exist reports SQLite's
/// `no such column: "<col>"` (quoted), matching the DROP COLUMN wording.
#[test]
fn rename_missing_column_reports_no_such_column() {
    let mut db = Database::new();
    create_table(&mut db, "CREATE TABLE t2(x, y, z)");
    assert_eq!(
        rename_column_err(&mut db, "ALTER TABLE t2 RENAME COLUMN a TO b"),
        "no such column: \"a\""
    );
    // The table's columns are unchanged.
    assert!(db.get_table("t2").unwrap().schema.has_column("x"));
    assert!(!db.get_table("t2").unwrap().schema.has_column("b"));
}

/// A CTE name referenced in a trigger body is not a base table and must not be
/// mistaken for a missing table: the rename still succeeds.
#[test]
fn rename_succeeds_when_trigger_body_uses_cte() {
    let mut db = Database::new();
    create_table(&mut db, "CREATE TABLE t1(a, b, c)");
    create_table(&mut db, "CREATE TABLE dst(v)");
    create_trigger(
        &mut db,
        "CREATE TRIGGER tr AFTER INSERT ON t1 BEGIN \
         INSERT INTO dst WITH cte AS (SELECT new.a AS v) SELECT v FROM cte; END",
    );
    try_alter(&mut db, "ALTER TABLE t1 RENAME COLUMN c TO cc").expect("rename should succeed");
    assert!(db.get_table("t1").unwrap().schema.has_column("cc"));
}
