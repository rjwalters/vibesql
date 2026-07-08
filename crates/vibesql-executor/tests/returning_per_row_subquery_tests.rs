//! Regression tests for issues #5972 (DELETE/UPDATE) and #5977 (INSERT)
//!
//! A subquery inside a RETURNING expression must be recomputed **per row, as
//! each affected row is mutated** — it sees the incremental post-DML table
//! state, not a single snapshot taken after the whole statement completes.
//! SQLite documents this (returning1.test section 20): a subquery that
//! references the table being modified is treated as correlated and recomputed
//! after each step.
//!
//! Previously VibeSQL collected all affected rows, then projected RETURNING
//! once at statement end, so every RETURNING row observed the same final
//! database state. This module pins the per-row behavior for DELETE, UPDATE,
//! and INSERT. Subquery-free RETURNING keeps the statement-end batch path (also
//! covered here to guard against a regression in the common case).
//!
//! All expected values were verified against sqlite3 3.51.0.

use vibesql_executor::{DeleteExecutor, InsertExecutor, SelectResult, UpdateExecutor};
use vibesql_types::SqlValue;

fn setup(db: &mut vibesql_storage::Database, sql: &str) {
    let stmt = vibesql_parser::Parser::parse_sql(sql)
        .unwrap_or_else(|e| panic!("Parse failed: {} -- {:?}", sql, e));
    match stmt {
        vibesql_ast::Statement::CreateTable(create_table) => {
            vibesql_executor::CreateTableExecutor::execute(&create_table, db).unwrap();
        }
        vibesql_ast::Statement::Insert(insert) => {
            InsertExecutor::execute(db, &insert)
                .unwrap_or_else(|e| panic!("Insert failed: {} -- {:?}", sql, e));
        }
        other => panic!("Unsupported statement in test setup: {:?}", other),
    }
}

fn delete_returning(db: &mut vibesql_storage::Database, sql: &str) -> SelectResult {
    let stmt = vibesql_parser::Parser::parse_sql(sql)
        .unwrap_or_else(|e| panic!("Parse failed: {} -- {:?}", sql, e));
    let vibesql_ast::Statement::Delete(delete) = stmt else {
        panic!("Expected DELETE: {}", sql);
    };
    let (_count, returning) = DeleteExecutor::execute_returning(&delete, db)
        .unwrap_or_else(|e| panic!("Delete failed: {} -- {:?}", sql, e));
    returning.expect("RETURNING result present")
}

fn update_returning(db: &mut vibesql_storage::Database, sql: &str) -> SelectResult {
    let stmt = vibesql_parser::Parser::parse_sql(sql)
        .unwrap_or_else(|e| panic!("Parse failed: {} -- {:?}", sql, e));
    let vibesql_ast::Statement::Update(update) = stmt else {
        panic!("Expected UPDATE: {}", sql);
    };
    let (_count, returning) = UpdateExecutor::execute_returning(&update, db)
        .unwrap_or_else(|e| panic!("Update failed: {} -- {:?}", sql, e));
    returning.expect("RETURNING result present")
}

fn insert_returning(db: &mut vibesql_storage::Database, sql: &str) -> SelectResult {
    let stmt = vibesql_parser::Parser::parse_sql(sql)
        .unwrap_or_else(|e| panic!("Parse failed: {} -- {:?}", sql, e));
    let vibesql_ast::Statement::Insert(insert) = stmt else {
        panic!("Expected INSERT: {}", sql);
    };
    let outcome = InsertExecutor::execute_returning(db, &insert)
        .unwrap_or_else(|e| panic!("Insert failed: {} -- {:?}", sql, e));
    outcome.returning.expect("RETURNING result present")
}

/// A variant-agnostic cell used for comparisons: any SQL float variant
/// (`Real`/`Double`/`Float`/`Numeric`) collapses to `Float(f64)` so the tests
/// pin the *numeric* result without depending on which float variant a given
/// expression happens to produce (e.g. `round(avg(...))` yields `Double`,
/// while `round(avg(...)) + int*100` yields `Float`). Integers and NULL stay
/// exact.
#[derive(Debug)]
enum Cell {
    Int(i64),
    Float(f64),
    Null,
}

impl PartialEq for Cell {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Cell::Int(a), Cell::Int(b)) => a == b,
            // Compare floats with a tolerance: the correlated subquery arm
            // yields an `f32` (`SqlValue::Float`), so 104.6 round-trips as
            // 104.5999984741211. sqlite3 3.51.0 displays 104.6; the values are
            // equal to f32 precision.
            (Cell::Float(a), Cell::Float(b)) => (a - b).abs() < 1e-3,
            (Cell::Null, Cell::Null) => true,
            _ => false,
        }
    }
}

fn int(v: i64) -> Cell {
    Cell::Int(v)
}

fn real(v: f64) -> Cell {
    Cell::Float(v)
}

const NULL: Cell = Cell::Null;

fn normalize(v: &SqlValue) -> Cell {
    match v {
        SqlValue::Integer(i) | SqlValue::Bigint(i) => Cell::Int(*i),
        SqlValue::Smallint(i) => Cell::Int(*i as i64),
        SqlValue::Real(f) | SqlValue::Double(f) | SqlValue::Numeric(f) => Cell::Float(*f),
        SqlValue::Float(f) => Cell::Float(*f as f64),
        SqlValue::Null => Cell::Null,
        other => panic!("unexpected value in RETURNING result: {:?}", other),
    }
}

fn rows(result: &SelectResult) -> Vec<Vec<Cell>> {
    result.rows.iter().map(|r| r.values.iter().map(normalize).collect()).collect()
}

fn seed(db: &mut vibesql_storage::Database) {
    setup(db, "CREATE TABLE t1(a INTEGER PRIMARY KEY, b INT)");
    setup(db, "INSERT INTO t1 VALUES(1,10),(2,20),(3,30),(4,40),(6,60),(8,80)");
}

/// returning1.test 20.1 — DELETE ... WHERE with min/max/avg subqueries.
/// Each returned row observes the table *after* that row is deleted.
#[test]
fn delete_returning_subquery_recomputes_per_row() {
    let mut db = vibesql_storage::Database::new();
    seed(&mut db);
    let result = delete_returning(
        &mut db,
        "DELETE FROM t1 WHERE a<>3 RETURNING a,\
         (SELECT min(a) FROM t1),(SELECT max(a) FROM t1),(SELECT round(avg(a),2) FROM t1)",
    );
    assert_eq!(
        rows(&result),
        vec![
            vec![int(1), int(2), int(8), real(4.6)],
            vec![int(2), int(3), int(8), real(5.25)],
            vec![int(4), int(3), int(8), real(5.67)],
            vec![int(6), int(3), int(8), real(5.5)],
            vec![int(8), int(3), int(3), real(3.0)],
        ]
    );
}

/// returning1.test 20.2 — DELETE the whole table; the last row's subqueries
/// see an empty table and return NULL.
#[test]
fn delete_returning_subquery_full_table_last_row_sees_empty() {
    let mut db = vibesql_storage::Database::new();
    seed(&mut db);
    let result = delete_returning(
        &mut db,
        "DELETE FROM t1 RETURNING a,\
         (SELECT min(a) FROM t1),(SELECT max(a) FROM t1),(SELECT round(avg(a),2) FROM t1)",
    );
    assert_eq!(
        rows(&result),
        vec![
            vec![int(1), int(2), int(8), real(4.6)],
            vec![int(2), int(3), int(8), real(5.25)],
            vec![int(3), int(4), int(8), real(6.0)],
            vec![int(4), int(6), int(8), real(7.0)],
            vec![int(6), int(8), int(8), real(8.0)],
            vec![int(8), NULL, NULL, NULL],
        ]
    );
}

/// returning1.test 20.3 — the subquery is correlated to the per-row value
/// (`t1.a` inside the subquery), and must still recompute per row.
#[test]
fn delete_returning_correlated_subquery_recomputes_per_row() {
    let mut db = vibesql_storage::Database::new();
    seed(&mut db);
    let result = delete_returning(
        &mut db,
        "DELETE FROM t1 RETURNING a,\
         (SELECT min(t2.a)+t1.a*100 FROM t1 AS t2),\
         (SELECT max(t2.a)+t1.a*100 FROM t1 AS t2),\
         (SELECT round(avg(t2.a),2)+t1.a*100 FROM t1 AS t2)",
    );
    assert_eq!(
        rows(&result),
        vec![
            vec![int(1), int(102), int(108), real(104.6)],
            vec![int(2), int(203), int(208), real(205.25)],
            vec![int(3), int(304), int(308), real(306.0)],
            vec![int(4), int(406), int(408), real(407.0)],
            vec![int(6), int(608), int(608), real(608.0)],
            vec![int(8), NULL, NULL, NULL],
        ]
    );
}

/// UPDATE ... RETURNING with a subquery: each row's NEW state is applied
/// incrementally, so `SELECT sum(b)` grows by one per row.
#[test]
fn update_returning_subquery_sees_incremental_new_state() {
    let mut db = vibesql_storage::Database::new();
    seed(&mut db);
    // Base sum(b) = 240; each row adds 1, so row N sees 240+N.
    let result =
        update_returning(&mut db, "UPDATE t1 SET b=b+1 RETURNING a, b, (SELECT sum(b) FROM t1)");
    assert_eq!(
        rows(&result),
        vec![
            vec![int(1), int(11), int(241)],
            vec![int(2), int(21), int(242)],
            vec![int(3), int(31), int(243)],
            vec![int(4), int(41), int(244)],
            vec![int(6), int(61), int(245)],
            vec![int(8), int(81), int(246)],
        ]
    );
}

/// Subquery-free RETURNING on DELETE keeps the statement-end batch path: the
/// projected columns are just the OLD row values, unaffected by the fix.
#[test]
fn delete_returning_without_subquery_unchanged() {
    let mut db = vibesql_storage::Database::new();
    seed(&mut db);
    let result = delete_returning(&mut db, "DELETE FROM t1 WHERE a<>3 RETURNING a, b");
    assert_eq!(
        rows(&result),
        vec![
            vec![int(1), int(10)],
            vec![int(2), int(20)],
            vec![int(4), int(40)],
            vec![int(6), int(60)],
            vec![int(8), int(80)],
        ]
    );
}

/// Subquery-free RETURNING on UPDATE keeps the statement-end batch path: the
/// projected columns are the NEW row values.
#[test]
fn update_returning_without_subquery_unchanged() {
    let mut db = vibesql_storage::Database::new();
    seed(&mut db);
    let result = update_returning(&mut db, "UPDATE t1 SET b=b+1 WHERE a<3 RETURNING a, b");
    assert_eq!(rows(&result), vec![vec![int(1), int(11)], vec![int(2), int(21)]]);
}

/// Seed a table with three base rows for the INSERT variants: subsequent
/// multi-row INSERTs then observe the table growing one row at a time.
fn seed_insert(db: &mut vibesql_storage::Database) {
    setup(db, "CREATE TABLE t1(a INTEGER PRIMARY KEY, b INT)");
    setup(db, "INSERT INTO t1 VALUES(1,10),(2,20),(3,30)");
}

/// Issue #5977 reproduction — INSERT ... RETURNING with scalar subqueries.
/// Each inserted row observes the table (count/sum) as of its own insertion,
/// so count/sum grow one row at a time rather than all seeing the final state.
///
/// sqlite3 3.51.0 ground truth:
///   4|4|100
///   6|5|160
///   8|6|240
#[test]
fn insert_returning_subquery_recomputes_per_row() {
    let mut db = vibesql_storage::Database::new();
    seed_insert(&mut db);
    let result = insert_returning(
        &mut db,
        "INSERT INTO t1 VALUES(4,40),(6,60),(8,80) \
         RETURNING a, (SELECT count(*) FROM t1), (SELECT sum(b) FROM t1)",
    );
    assert_eq!(
        rows(&result),
        vec![
            vec![int(4), int(4), int(100)],
            vec![int(6), int(5), int(160)],
            vec![int(8), int(6), int(240)],
        ]
    );
}

/// INSERT ... RETURNING with a correlated subquery referencing the inserted
/// value: `SELECT count(*) FROM t1 WHERE t1.a <= a` must still recompute per
/// row against the incremental table state.
///
/// sqlite3 3.51.0 ground truth:
///   4|4
///   6|5
///   8|6
#[test]
fn insert_returning_correlated_subquery_recomputes_per_row() {
    let mut db = vibesql_storage::Database::new();
    seed_insert(&mut db);
    let result = insert_returning(
        &mut db,
        "INSERT INTO t1 VALUES(4,40),(6,60),(8,80) \
         RETURNING a, (SELECT count(*) FROM t1 WHERE t1.a <= a)",
    );
    assert_eq!(
        rows(&result),
        vec![vec![int(4), int(4)], vec![int(6), int(5)], vec![int(8), int(6)]]
    );
}

/// Single-row INSERT ... RETURNING with a subquery: the one returned row sees
/// the post-insert state (count grows from 3 to 4).
#[test]
fn insert_returning_single_row_subquery() {
    let mut db = vibesql_storage::Database::new();
    seed_insert(&mut db);
    let result = insert_returning(
        &mut db,
        "INSERT INTO t1 VALUES(4,40) RETURNING a, (SELECT count(*) FROM t1)",
    );
    assert_eq!(rows(&result), vec![vec![int(4), int(4)]]);
}

/// Subquery-free INSERT ... RETURNING keeps the statement-end batch fast path:
/// the projected columns are just the inserted row values, unaffected by the
/// per-row machinery (regression guard for the common case).
#[test]
fn insert_returning_without_subquery_unchanged() {
    let mut db = vibesql_storage::Database::new();
    seed_insert(&mut db);
    let result =
        insert_returning(&mut db, "INSERT INTO t1 VALUES(4,40),(6,60),(8,80) RETURNING a, b");
    assert_eq!(
        rows(&result),
        vec![vec![int(4), int(40)], vec![int(6), int(60)], vec![int(8), int(80)]]
    );
}
