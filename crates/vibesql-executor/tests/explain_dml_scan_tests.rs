//! Tests for `EXPLAIN QUERY PLAN` on `DELETE`/`UPDATE` statements.
//!
//! Before this fix, `ExplainExecutor::execute` handled `Statement::Update`/
//! `Statement::Delete` with a hardcoded stub `PlanNode` carrying only a text
//! `detail` (used by the PostgreSQL-style `EXPLAIN` text renderer) and no
//! `scan_type`/children at all. Since `to_sqlite_eqp()` only renders nodes
//! reachable via `scan_type` or children, `EXPLAIN QUERY PLAN DELETE FROM t
//! WHERE ...` and `EXPLAIN QUERY PLAN UPDATE t SET ... WHERE ...` rendered as
//! a bare `QUERY PLAN` header with **no** scan/search line at all — even
//! though accessing the target table plans exactly like a `SELECT` over the
//! same WHERE clause (verified against sqlite3 3.51.0). Discovered via
//! e_fkey.test's e_fkey-25.2 (`EXPLAIN QUERY PLAN DELETE FROM artist WHERE 1`
//! expects `SCAN artist`).
//!
//! `explain_delete`/`explain_update` now reuse the same FROM-clause scan/
//! search selection as `explain_select` by building a synthetic single-table
//! FROM clause from the statement's target table. Two things remain
//! out of scope (tracked under issue #6170, not fixed here):
//! - SQLite's automatic FK child-table orphan-check sub-plan for parent-key DELETE/UPDATE when
//!   foreign key enforcement is active (e_fkey-25.3, e_fkey-26.x, e_fkey-27.3/27.4).
//! - The SQLite 3.33+ `UPDATE ... FROM <other-tables>` extension's additional scan entries.

use vibesql_ast::Statement;
use vibesql_executor::{CreateIndexExecutor, CreateTableExecutor, ExplainExecutor};
use vibesql_parser::Parser;
use vibesql_storage::Database;

fn run_ddl(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).expect("parse failed");
    match stmt {
        Statement::CreateTable(s) => {
            CreateTableExecutor::execute(&s, db).unwrap();
        }
        Statement::CreateIndex(s) => {
            CreateIndexExecutor::execute(&s, db).unwrap();
        }
        other => panic!("unsupported DDL in test: {:?}", other),
    }
}

/// Run EXPLAIN QUERY PLAN and return the SQLite-style EQP output.
fn eqp(db: &Database, sql: &str) -> String {
    let explain_sql = format!("EXPLAIN QUERY PLAN {}", sql);
    let stmt = Parser::parse_sql(&explain_sql).expect("Failed to parse SQL");

    if let Statement::Explain(explain_stmt) = stmt {
        let result = ExplainExecutor::execute(&explain_stmt, db).expect("EXPLAIN failed");
        result.to_sqlite_eqp()
    } else {
        panic!("Expected EXPLAIN statement");
    }
}

#[test]
fn delete_with_where_shows_table_scan() {
    // e_fkey-25.2: `EXPLAIN QUERY PLAN DELETE FROM artist WHERE 1` must show
    // the target table's scan, not an empty plan.
    let mut db = Database::new();
    run_ddl(&mut db, "CREATE TABLE artist(artistid INTEGER PRIMARY KEY, artistname TEXT)");

    let out = eqp(&db, "DELETE FROM artist WHERE 1");
    assert_eq!(out, "QUERY PLAN\n`--SCAN artist\n");
}

#[test]
fn delete_with_no_where_shows_table_scan() {
    let mut db = Database::new();
    run_ddl(&mut db, "CREATE TABLE t(a, b)");

    let out = eqp(&db, "DELETE FROM t");
    assert_eq!(out, "QUERY PLAN\n`--SCAN t\n");
}

#[test]
fn delete_with_indexed_equality_shows_search() {
    let mut db = Database::new();
    run_ddl(&mut db, "CREATE TABLE t(a, b)");
    run_ddl(&mut db, "CREATE INDEX idx_a ON t(a)");

    let out = eqp(&db, "DELETE FROM t WHERE a = 5");
    assert_eq!(out, "QUERY PLAN\n`--SEARCH t USING INDEX idx_a (a=?)\n");
}

#[test]
fn update_with_where_shows_table_scan() {
    let mut db = Database::new();
    run_ddl(&mut db, "CREATE TABLE artist(artistid INTEGER PRIMARY KEY, artistname TEXT)");

    let out = eqp(&db, "UPDATE artist SET artistname = 'x' WHERE artistid = 1");
    assert_eq!(out, "QUERY PLAN\n`--SEARCH artist USING INTEGER PRIMARY KEY (rowid=?)\n");
}

#[test]
fn update_with_no_where_shows_table_scan() {
    let mut db = Database::new();
    run_ddl(&mut db, "CREATE TABLE t(a, b)");

    let out = eqp(&db, "UPDATE t SET a = 1");
    assert_eq!(out, "QUERY PLAN\n`--SCAN t\n");
}

#[test]
fn update_with_indexed_equality_shows_search() {
    let mut db = Database::new();
    run_ddl(&mut db, "CREATE TABLE t(a, b)");
    run_ddl(&mut db, "CREATE INDEX idx_a ON t(a)");

    let out = eqp(&db, "UPDATE t SET b = 9 WHERE a = 5");
    assert_eq!(out, "QUERY PLAN\n`--SEARCH t USING INDEX idx_a (a=?)\n");
}

#[test]
fn two_independent_eqp_statements_each_show_their_own_scan() {
    // e_fkey-25.2's exact shape: two separate EXPLAIN QUERY PLAN statements
    // (DELETE FROM the parent, then a plain SELECT probing the child) each
    // render their own scan line independently.
    let mut db = Database::new();
    run_ddl(&mut db, "CREATE TABLE artist(artistid INTEGER PRIMARY KEY, artistname TEXT)");
    run_ddl(&mut db, "CREATE TABLE track(trackid INTEGER, trackname TEXT, trackartist INTEGER)");

    assert_eq!(eqp(&db, "DELETE FROM artist WHERE 1"), "QUERY PLAN\n`--SCAN artist\n");
    assert_eq!(
        eqp(&db, "SELECT rowid FROM track WHERE trackartist = 5"),
        "QUERY PLAN\n`--SCAN track\n"
    );
}
