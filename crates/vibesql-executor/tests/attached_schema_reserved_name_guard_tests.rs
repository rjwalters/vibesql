//! Integration tests for the `sqlite_master`/`sqlite_schema` reserved-name
//! guards on ALTER TABLE, DROP TABLE, INSERT, UPDATE, DELETE, ANALYZE, and
//! CREATE INDEX recognizing a currently-attached database alias qualifier
//! (issue #6451).
//!
//! Before this fix, these guards only recognized the bare and
//! `main.`-qualified forms of the schema table name. `<alias>.sqlite_master`/
//! `<alias>.sqlite_schema` for a live `ATTACH`ed database fell through to
//! ordinary table resolution and failed with a generic "no such table"/schema
//! error instead of SQLite's specific rejection message. Verified against
//! sqlite3 3.51.0:
//!
//! ```sql
//! ATTACH ':memory:' AS aux;
//! ALTER TABLE aux.sqlite_master RENAME TO x;      -- table sqlite_master may not be altered
//! DROP TABLE aux.sqlite_master;                   -- table sqlite_master may not be dropped
//! INSERT INTO aux.sqlite_master VALUES (...);      -- table sqlite_master may not be modified
//! UPDATE aux.sqlite_master SET ...;                -- table sqlite_master may not be modified
//! DELETE FROM aux.sqlite_master;                   -- table sqlite_master may not be modified
//! ```
//!
//! Each guard reuses the same catalog-attachment lookup
//! (`sqlite_schema::is_sqlite_schema_table_ref` / the equivalent
//! `schema_name`-based check in `index_ddl/validation.rs`) that
//! `sqlite_schema::resolve_sqlite_schema_query_scope` already uses for the
//! read-side `SELECT ... FROM <alias>.sqlite_master` path (#6436). In all
//! cases, SQLite's message echoes the *canonical* `sqlite_master` name, never
//! the alias-qualified spelling the statement used.

use vibesql_ast::Statement;
use vibesql_executor::{
    AlterTableExecutor, AnalyzeExecutor, CreateIndexExecutor, DeleteExecutor, DropTableExecutor,
    ExecutorError, InsertExecutor, UpdateExecutor,
};
use vibesql_parser::Parser;
use vibesql_storage::Database;

/// A database with a live `aux` attachment and an ordinary `t1` table, so
/// non-schema-table statements against `aux` have something real to resolve.
fn setup_with_attached_aux() -> Database {
    let mut db = Database::new();
    db.catalog.attach_database("aux", ":memory:").expect("ATTACH aux");
    let stmt = Parser::parse_sql("CREATE TABLE t1(a INTEGER)").expect("parse CREATE TABLE");
    match stmt {
        Statement::CreateTable(s) => {
            vibesql_executor::CreateTableExecutor::execute(&s, &mut db).expect("CREATE TABLE t1")
        }
        other => panic!("expected CreateTable, got {other:?}"),
    };
    db
}

// `ALTER TABLE aux.sqlite_master RENAME TO x` is now exercised via
// `Parser::parse_sql` — issue #6504 fixed the ALTER TABLE parser
// (`parser/alter.rs::parse_alter_table`) to parse the table name with
// `parse_table_ref()` (the same helper DROP TABLE already used) instead of
// the single-token `parse_identifier()`, so schema-qualified names
// (including `<alias>.sqlite_master`) now reach this guard end-to-end via
// real SQL text.
fn alter_table_rename_stmt(sql: &str) -> vibesql_ast::AlterTableStmt {
    match Parser::parse_sql(sql).unwrap_or_else(|e| panic!("parse {sql:?}: {e:?}")) {
        Statement::AlterTable(stmt) => stmt,
        other => panic!("expected AlterTable, got {other:?}"),
    }
}

#[test]
fn alter_table_rejects_attached_alias_sqlite_master() {
    let mut db = setup_with_attached_aux();
    let stmt = alter_table_rename_stmt("ALTER TABLE aux.sqlite_master RENAME TO x");
    let err = AlterTableExecutor::execute(&stmt, &mut db).unwrap_err();
    assert_eq!(err.to_string(), "table sqlite_master may not be altered");
}

#[test]
fn alter_table_rejects_attached_alias_sqlite_schema_case_insensitive() {
    let mut db = setup_with_attached_aux();
    let stmt = alter_table_rename_stmt("ALTER TABLE AUX.SQLITE_SCHEMA RENAME TO x");
    let err = AlterTableExecutor::execute(&stmt, &mut db).unwrap_err();
    assert_eq!(err.to_string(), "table sqlite_master may not be altered");
}

#[test]
fn alter_table_does_not_recognize_unattached_alias_as_sqlite_master() {
    let mut db = setup_with_attached_aux();
    let stmt = alter_table_rename_stmt("ALTER TABLE bogus.sqlite_master RENAME TO x");
    let err = AlterTableExecutor::execute(&stmt, &mut db).unwrap_err();
    assert_ne!(
        err.to_string(),
        "table sqlite_master may not be altered",
        "an alias that was never attached must not trip the reserved-name guard: {err}"
    );
}

/// Regression / new acceptance criterion (#6504): an ordinary
/// attached-schema table (not the reserved schema table) now parses and
/// executes correctly end-to-end via real SQL text — it must rename `t1`
/// inside `aux`, not fall through to the guard.
#[test]
fn alter_table_rename_ordinary_attached_table_succeeds() {
    let mut db = setup_with_attached_aux();
    // `setup_with_attached_aux` creates `t1` in `main`; create the
    // `aux`-scoped `t1` as well so the qualified rename has a real target.
    let create =
        match Parser::parse_sql("CREATE TABLE aux.t1(a INTEGER)").expect("parse CREATE TABLE") {
            Statement::CreateTable(s) => s,
            other => panic!("expected CreateTable, got {other:?}"),
        };
    vibesql_executor::CreateTableExecutor::execute(&create, &mut db).expect("CREATE TABLE aux.t1");

    let stmt = alter_table_rename_stmt("ALTER TABLE aux.t1 RENAME TO t2");
    AlterTableExecutor::execute(&stmt, &mut db)
        .expect("ALTER TABLE aux.t1 RENAME TO t2 should succeed");

    assert!(
        db.catalog.table_exists("aux.t2"),
        "renamed table should exist as aux.t2 in the catalog"
    );
    assert!(
        !db.catalog.table_exists("aux.t1"),
        "old table name aux.t1 should no longer exist in the catalog"
    );
}

#[test]
fn drop_table_rejects_attached_alias_sqlite_master() {
    let mut db = setup_with_attached_aux();
    let stmt = match Parser::parse_sql("DROP TABLE aux.sqlite_master").expect("parse DROP TABLE") {
        Statement::DropTable(s) => s,
        other => panic!("expected DropTable, got {other:?}"),
    };
    let err = DropTableExecutor::execute(&stmt, &mut db).unwrap_err();
    assert_eq!(err.to_string(), "table sqlite_master may not be dropped");
}

#[test]
fn insert_rejects_attached_alias_sqlite_master() {
    let mut db = setup_with_attached_aux();
    let stmt =
        match Parser::parse_sql("INSERT INTO aux.sqlite_master VALUES ('table','z','z',0,'x')")
            .expect("parse INSERT")
        {
            Statement::Insert(s) => s,
            other => panic!("expected Insert, got {other:?}"),
        };
    let err = InsertExecutor::execute(&mut db, &stmt).unwrap_err();
    assert_eq!(err.to_string(), "table sqlite_master may not be modified");
}

#[test]
fn update_rejects_attached_alias_sqlite_master() {
    let mut db = setup_with_attached_aux();
    let stmt =
        match Parser::parse_sql("UPDATE aux.sqlite_master SET name = 'z'").expect("parse UPDATE") {
            Statement::Update(s) => s,
            other => panic!("expected Update, got {other:?}"),
        };
    let err = UpdateExecutor::execute(&stmt, &mut db).unwrap_err();
    assert_eq!(err.to_string(), "table sqlite_master may not be modified");
}

#[test]
fn delete_rejects_attached_alias_sqlite_master() {
    let mut db = setup_with_attached_aux();
    let stmt = match Parser::parse_sql("DELETE FROM aux.sqlite_master").expect("parse DELETE") {
        Statement::Delete(s) => s,
        other => panic!("expected Delete, got {other:?}"),
    };
    let err = DeleteExecutor::execute(&stmt, &mut db).unwrap_err();
    assert_eq!(err.to_string(), "table sqlite_master may not be modified");
}

// `ANALYZE aux.sqlite_master` is now exercised via `Parser::parse_sql` —
// issue #6504 fixed the ANALYZE parser
// (`parser/index.rs::parse_analyze_statement`) to parse the table name with
// `parse_table_ref()` instead of single-token `parse_identifier()`, and
// added `expect_statement_end()` so trailing garbage is rejected instead of
// silently truncating (previously `ANALYZE aux.sqlite_master` silently
// became `ANALYZE aux`, later failing with `TableNotFound("aux")`).
fn analyze_stmt(sql: &str) -> vibesql_ast::AnalyzeStmt {
    match Parser::parse_sql(sql).unwrap_or_else(|e| panic!("parse {sql:?}: {e:?}")) {
        Statement::Analyze(stmt) => stmt,
        other => panic!("expected Analyze, got {other:?}"),
    }
}

#[test]
fn analyze_attached_alias_sqlite_master_is_a_no_op() {
    let mut db = setup_with_attached_aux();
    let stmt = analyze_stmt("ANALYZE aux.sqlite_master");
    assert_eq!(stmt.table_name.as_deref(), Some("aux.sqlite_master"));
    // ANALYZE against the schema table is a no-op (sqlite_stat1 is virtual),
    // not an error — matching the bare/`main.`-qualified behavior.
    AnalyzeExecutor::execute(&stmt, &mut db).expect("ANALYZE aux.sqlite_master should succeed");
}

/// New acceptance criterion (#6504): an ordinary attached-schema table
/// analyzes successfully via real SQL text instead of failing with
/// `TableNotFound("aux")` (the pre-fix behavior, from silently truncating
/// `aux.t1` down to just `aux`).
#[test]
fn analyze_ordinary_attached_table_succeeds() {
    let mut db = setup_with_attached_aux();
    let create =
        match Parser::parse_sql("CREATE TABLE aux.t1(a INTEGER)").expect("parse CREATE TABLE") {
            Statement::CreateTable(s) => s,
            other => panic!("expected CreateTable, got {other:?}"),
        };
    vibesql_executor::CreateTableExecutor::execute(&create, &mut db).expect("CREATE TABLE aux.t1");

    let stmt = analyze_stmt("ANALYZE aux.t1");
    assert_eq!(stmt.table_name.as_deref(), Some("aux.t1"));
    AnalyzeExecutor::execute(&stmt, &mut db).expect("ANALYZE aux.t1 should succeed");
}

#[test]
fn create_index_rejects_attached_alias_sqlite_master() {
    let mut db = setup_with_attached_aux();
    let stmt = match Parser::parse_sql("CREATE INDEX aux.i1 ON sqlite_master(name)")
        .expect("parse CREATE INDEX")
    {
        Statement::CreateIndex(s) => s,
        other => panic!("expected CreateIndex, got {other:?}"),
    };
    let err = CreateIndexExecutor::execute(&stmt, &mut db).unwrap_err();
    assert_eq!(err.to_string(), "table sqlite_master may not be indexed");
}

/// Regression: an alias that was never attached must NOT be treated as a
/// schema-table hit — it falls through to ordinary resolution and fails with
/// an "unknown database"/"no such table" style error, not the reserved-name
/// message.
#[test]
fn delete_does_not_recognize_unattached_alias_as_sqlite_master() {
    let mut db = setup_with_attached_aux();
    let stmt = match Parser::parse_sql("DELETE FROM bogus.sqlite_master").expect("parse DELETE") {
        Statement::Delete(s) => s,
        other => panic!("expected Delete, got {other:?}"),
    };
    let err = DeleteExecutor::execute(&stmt, &mut db).unwrap_err();
    assert_ne!(
        err.to_string(),
        "table sqlite_master may not be modified",
        "an alias that was never attached must not trip the reserved-name guard: {err}"
    );
}

/// Regression: after `DETACH`, a formerly-attached alias no longer trips the
/// guard (it falls through to ordinary — now failing — resolution), matching
/// SQLite's behavior for a stale alias.
#[test]
fn delete_does_not_recognize_detached_alias_as_sqlite_master() {
    let mut db = setup_with_attached_aux();
    db.catalog.detach_database("aux").expect("DETACH aux");
    let stmt = match Parser::parse_sql("DELETE FROM aux.sqlite_master").expect("parse DELETE") {
        Statement::Delete(s) => s,
        other => panic!("expected Delete, got {other:?}"),
    };
    let err = DeleteExecutor::execute(&stmt, &mut db).unwrap_err();
    assert_ne!(
        err.to_string(),
        "table sqlite_master may not be modified",
        "a detached alias must not trip the reserved-name guard: {err}"
    );
}

/// Regression: the pre-existing bare and `main.`-qualified forms keep
/// working exactly as before.
#[test]
fn delete_still_rejects_bare_and_main_qualified_sqlite_master() {
    let mut db = setup_with_attached_aux();

    let stmt = match Parser::parse_sql("DELETE FROM sqlite_master").expect("parse DELETE") {
        Statement::Delete(s) => s,
        other => panic!("expected Delete, got {other:?}"),
    };
    let err = DeleteExecutor::execute(&stmt, &mut db).unwrap_err();
    assert_eq!(err.to_string(), "table sqlite_master may not be modified");

    let stmt = match Parser::parse_sql("DELETE FROM main.sqlite_master").expect("parse DELETE") {
        Statement::Delete(s) => s,
        other => panic!("expected Delete, got {other:?}"),
    };
    let err = DeleteExecutor::execute(&stmt, &mut db).unwrap_err();
    assert_eq!(
        err.to_string(),
        "table sqlite_master may not be modified",
        "main.-qualified form must report the canonical (unqualified) name, not \"main.sqlite_master\""
    );
}

/// Sanity check: an ordinary attached-schema table (not the schema table
/// itself) is unaffected by these guards.
#[test]
fn delete_from_ordinary_attached_table_is_unaffected() {
    let mut db = setup_with_attached_aux();
    // `t1` only exists in `main`; deleting from `aux.t1` should fail with an
    // ordinary "no such table" error, never the reserved-name message.
    let stmt = match Parser::parse_sql("DELETE FROM aux.t1").expect("parse DELETE") {
        Statement::Delete(s) => s,
        other => panic!("expected Delete, got {other:?}"),
    };
    let err = DeleteExecutor::execute(&stmt, &mut db).unwrap_err();
    assert!(
        !matches!(err, ExecutorError::SqliteSystemTableReadOnly { .. }),
        "unexpected reserved-name rejection for an ordinary table: {err}"
    );
}
