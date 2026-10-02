//! Tests for the expression-index leg of ALTER TABLE's dependent-object
//! schema re-parse (`alter::drop_column_checks::check_schema_objects`): an
//! expression index *on the altered table* whose expression holds a subquery
//! with a FROM-less `SELECT *` aborts the ALTER with
//! `error in index <name>: no tables specified` (altertab3.test 20.10,
//! verified against sqlite3 3.51.0). Part of #6174.
//!
//! SQLite only re-parses indexes on the altered table (`type != 'index' OR
//! tbl_name = ...`), so the same broken index on a different table must not
//! block an ALTER of an unrelated table.

use vibesql_ast::Statement;
use vibesql_storage::Database;

use crate::errors::ExecutorError;

/// altertab3-20.10's index: `IN ()` folds to a constant (so the index is
/// accepted), but the CTE body's FROM-less `SELECT *` is a schema error on
/// re-parse.
const BROKEN_INDEX: &str = "CREATE INDEX k ON s( (WITH s AS( SELECT * ) VALUES(2) ) IN () )";
const BROKEN_INDEX_MSG: &str = "error in index k: no tables specified";

fn exec(db: &mut Database, sql: &str) -> Result<String, ExecutorError> {
    let stmt = vibesql_parser::Parser::parse_sql(sql).expect("parse");
    match stmt {
        Statement::CreateTable(s) => crate::CreateTableExecutor::execute(&s, db),
        Statement::CreateIndex(s) => crate::CreateIndexExecutor::execute(&s, db),
        Statement::AlterTable(s) => crate::alter::AlterTableExecutor::execute(&s, db),
        other => panic!("unexpected statement: {:?}", other),
    }
}

fn columns(db: &Database, table: &str) -> Vec<String> {
    db.get_table(table)
        .unwrap_or_else(|| panic!("table {table} should exist"))
        .schema
        .columns
        .iter()
        .map(|c| c.name.clone())
        .collect()
}

fn setup() -> Database {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE s(a, b, c)").unwrap();
    exec(&mut db, BROKEN_INDEX).unwrap();
    db
}

#[test]
fn rename_column_fails_on_index_with_fromless_wildcard_subquery() {
    // altertab3.test 20.10.
    let mut db = setup();
    let err = exec(&mut db, "ALTER TABLE s RENAME a TO a2").unwrap_err();
    assert_eq!(err.to_string(), BROKEN_INDEX_MSG);

    // The failed ALTER is atomic: the schema is unchanged.
    assert_eq!(columns(&db, "s"), vec!["a", "b", "c"]);
}

#[test]
fn unaffected_expression_indexes_do_not_block_rename_column() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE s(a, b, c)").unwrap();
    exec(&mut db, "CREATE TABLE t(x)").unwrap();
    // No subquery at all.
    exec(&mut db, "CREATE INDEX k1 ON s( (b + c) )").unwrap();
    // A `SELECT *` that does have a FROM clause.
    exec(&mut db, "CREATE INDEX k2 ON s( (WITH w AS (SELECT * FROM t) VALUES(2)) IN () )").unwrap();

    exec(&mut db, "ALTER TABLE s RENAME a TO a2").unwrap();
    assert_eq!(columns(&db, "s"), vec!["a2", "b", "c"]);
}

#[test]
fn broken_index_on_another_table_does_not_block_rename_column() {
    // SQLite only re-parses indexes on the altered table.
    let mut db = setup();
    exec(&mut db, "CREATE TABLE u(p, q)").unwrap();

    exec(&mut db, "ALTER TABLE u RENAME p TO p2").unwrap();
    assert_eq!(columns(&db, "u"), vec!["p2", "q"]);
}

#[test]
fn writable_schema_on_skips_index_check() {
    // Same gate as the view/trigger legs: the re-parse is skipped wholesale
    // while PRAGMA writable_schema=ON.
    let mut db = setup();
    db.set_writable_schema(true);
    exec(&mut db, "ALTER TABLE s RENAME a TO a2").unwrap();
    assert_eq!(columns(&db, "s"), vec!["a2", "b", "c"]);

    // Turning it back OFF restores the check for a later ALTER.
    db.set_writable_schema(false);
    let err = exec(&mut db, "ALTER TABLE s RENAME b TO b2").unwrap_err();
    assert_eq!(err.to_string(), BROKEN_INDEX_MSG);
}

#[test]
fn drop_column_reports_index_error_from_precheck_without_suffix() {
    // DROP COLUMN runs the pre-check (no suffix) before the post-check
    // (`after drop column` suffix). The index is broken independently of
    // the dropped column, so it is caught by the pre-check and the message
    // carries no `after drop column` suffix — matching SQLite, whose first
    // schema re-parse fails before the drop is attempted.
    let mut db = setup();
    let err = exec(&mut db, "ALTER TABLE s DROP COLUMN b").unwrap_err();
    assert_eq!(err.to_string(), BROKEN_INDEX_MSG);
    assert_eq!(columns(&db, "s"), vec!["a", "b", "c"]);
}

#[test]
fn rename_table_reports_index_error_and_leaves_schema_unchanged() {
    // RENAME TO shares the same pre-check (gated on legacy_alter_table=OFF).
    let mut db = setup();
    let err = exec(&mut db, "ALTER TABLE s RENAME TO s2").unwrap_err();
    assert_eq!(err.to_string(), BROKEN_INDEX_MSG);
    assert!(db.get_table("s").is_some(), "s must still exist");
    assert!(db.get_table("s2").is_none(), "s2 must not have been created");
}
