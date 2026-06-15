//! End-to-end regression tests for issue #5634: `ALTER TABLE ... DROP COLUMN`
//! and `... RENAME TO` edit the verbatim `sqlite_master.sql` text in place
//! (matching sqlite3 3.51.0 byte-for-byte), and the preserved verbatim text
//! survives a save/reload round-trip through **both** the `.sql` text dump and
//! the binary `.vbsql` format.
//!
//! RENAME TO is the headline case: sqlite3 emits the new table name
//! double-quoted (`CREATE TABLE "t2" (...)`), which previously exposed a
//! pre-existing dump-splitter gap (`parse_sql_statements` treated a `'` inside a
//! `"…"`/`[…]` quoted identifier as a string delimiter, desyncing statement
//! splitting and failing reload with `near "CREATE": syntax error`). The
//! splitter is now quote-aware, so the in-place RENAME TO output reloads
//! cleanly.

use vibesql_executor::{load_sql_dump, AlterTableExecutor, CreateTableExecutor, SelectExecutor};
use vibesql_parser::Parser;
use vibesql_storage::Database;

/// Create a table preserving the verbatim source text (issue #5619), the way the
/// CLI/load paths capture it.
fn create_with_source(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).expect("parse CREATE");
    let vibesql_ast::Statement::CreateTable(create) = stmt else {
        panic!("expected CREATE TABLE");
    };
    CreateTableExecutor::execute_with_source(&create, db, Some(sql)).expect("CREATE TABLE");
}

/// Run an ALTER, passing the verbatim statement text (the path the CLI uses).
fn alter(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).expect("parse ALTER");
    let vibesql_ast::Statement::AlterTable(a) = stmt else {
        panic!("expected ALTER TABLE");
    };
    AlterTableExecutor::execute_with_source(&a, db, Some(sql)).expect("ALTER TABLE");
}

/// Return the single `sql` text for the named table from `sqlite_master`.
fn table_sql(db: &Database, table: &str) -> String {
    // Escape any embedded `'` in the literal so the SELECT we build is valid SQL.
    let escaped = table.replace('\'', "''");
    let query = format!("SELECT sql FROM sqlite_master WHERE type='table' AND name='{escaped}'");
    let stmt = Parser::parse_sql(&query).expect("parse SELECT");
    let vibesql_ast::Statement::Select(select) = stmt else {
        panic!("expected SELECT");
    };
    let result = SelectExecutor::new(db).execute_with_columns(&select).expect("SELECT");
    assert_eq!(result.rows.len(), 1, "expected one row for table {table}");
    match &result.rows[0].values[0] {
        vibesql_types::SqlValue::Varchar(s) | vibesql_types::SqlValue::Character(s) => {
            s.to_string()
        }
        other => panic!("expected text, got {other:?}"),
    }
}

/// Save to a `.sql` dump and reload it through the executor (this exercises the
/// quote-aware `parse_sql_statements` splitter), returning the reloaded db.
fn roundtrip_sql_dump(db: &Database, tag: &str) -> Database {
    let path = std::env::temp_dir().join(format!("vibesql_5634_{tag}.sql"));
    db.save_sql_dump(&path).expect("save_sql_dump");
    let reloaded = load_sql_dump(&path).expect("load_sql_dump must not fail (no `near CREATE`)");
    std::fs::remove_file(&path).ok();
    reloaded
}

/// Save to the binary `.vbsql` format and reload it, returning the reloaded db.
fn roundtrip_binary(db: &Database, tag: &str) -> Database {
    let path = std::env::temp_dir().join(format!("vibesql_5634_{tag}.vbsql"));
    db.save_binary(&path).expect("save_binary");
    let reloaded = Database::load_binary(&path).expect("load_binary");
    std::fs::remove_file(&path).ok();
    reloaded
}

#[test]
fn rename_to_inplace_survives_sql_dump_reload() {
    let mut db = Database::new();
    create_with_source(&mut db, "CREATE TABLE t (\n  a INTEGER PRIMARY KEY,\n  b TEXT\n)");
    alter(&mut db, "ALTER TABLE t RENAME TO t2");

    let expected = "CREATE TABLE \"t2\" (\n  a INTEGER PRIMARY KEY,\n  b TEXT\n)";
    assert_eq!(table_sql(&db, "t2"), expected, "RENAME TO in-place must match sqlite3");

    // The double-quoted name must round-trip through the `.sql` dump without the
    // pre-existing `near "CREATE"` splitter desync (issue #5634).
    let reloaded = roundtrip_sql_dump(&db, "rename_sql");
    assert_eq!(
        table_sql(&reloaded, "t2"),
        expected,
        "RENAME TO verbatim text must survive a .sql dump reload"
    );
}

#[test]
fn rename_to_inplace_survives_binary_reload() {
    let mut db = Database::new();
    create_with_source(&mut db, "CREATE TABLE t (\n  a INTEGER PRIMARY KEY,\n  b TEXT\n)");
    alter(&mut db, "ALTER TABLE t RENAME TO t2");

    let expected = "CREATE TABLE \"t2\" (\n  a INTEGER PRIMARY KEY,\n  b TEXT\n)";
    let reloaded = roundtrip_binary(&db, "rename_bin");
    assert_eq!(
        table_sql(&reloaded, "t2"),
        expected,
        "RENAME TO verbatim text must survive a .vbsql reload"
    );
}

#[test]
fn drop_column_inplace_survives_both_reloads() {
    let mut db = Database::new();
    create_with_source(
        &mut db,
        "CREATE TABLE t (\n  a   INTEGER PRIMARY KEY,\n  b   TEXT,\n  c   INTEGER\n)",
    );
    alter(&mut db, "ALTER TABLE t DROP COLUMN c");

    let expected = "CREATE TABLE t (\n  a   INTEGER PRIMARY KEY,\n  b   TEXT)";
    assert_eq!(table_sql(&db, "t"), expected, "DROP COLUMN in-place must match sqlite3");

    let reloaded_sql = roundtrip_sql_dump(&db, "drop_sql");
    assert_eq!(
        table_sql(&reloaded_sql, "t"),
        expected,
        "DROP COLUMN verbatim text must survive a .sql dump reload"
    );

    let reloaded_bin = roundtrip_binary(&db, "drop_bin");
    assert_eq!(
        table_sql(&reloaded_bin, "t"),
        expected,
        "DROP COLUMN verbatim text must survive a .vbsql reload"
    );
}

/// A renamed-to name that itself contains a `'` (legal as a quoted identifier)
/// is the exact case the old symmetric-quote splitter mangled. The new name is
/// emitted double-quoted, so a `'` inside it is literal — the dump must reload.
#[test]
fn rename_to_name_with_apostrophe_survives_sql_dump_reload() {
    let mut db = Database::new();
    create_with_source(&mut db, "CREATE TABLE t (a INTEGER, b TEXT)");
    alter(&mut db, "ALTER TABLE t RENAME TO \"weird'name\"");

    let expected = "CREATE TABLE \"weird'name\" (a INTEGER, b TEXT)";
    assert_eq!(table_sql(&db, "weird'name"), expected);

    let reloaded = roundtrip_sql_dump(&db, "rename_apostrophe");
    assert_eq!(
        table_sql(&reloaded, "weird'name"),
        expected,
        "a renamed name containing `'` must survive the quote-aware splitter (issue #5634)"
    );
}
