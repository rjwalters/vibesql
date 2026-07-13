//! End-to-end tests for SQLite tuple/row SET assignments (issue #5862).
//!
//! Covers `SET (a, b, ...) = (row-value | scalar-subquery)` in both the
//! regular UPDATE path and the ON CONFLICT ... DO UPDATE arm, including the
//! NULL-when-empty subquery semantics and the arity-mismatch error.

use vibesql_executor::{CreateTableExecutor, InsertExecutor, UpdateExecutor};
use vibesql_parser::Parser;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

fn setup() -> Database {
    let mut db = Database::new();
    for sql in [
        "CREATE TABLE t(a INTEGER PRIMARY KEY, b, c)",
        "INSERT INTO t VALUES(1, 10, 'one')",
        "INSERT INTO t VALUES(2, 20, 'two')",
    ] {
        let stmt = Parser::parse_sql(sql).expect("parse");
        match stmt {
            vibesql_ast::Statement::CreateTable(ct) => {
                CreateTableExecutor::execute(&ct, &mut db).expect("create");
            }
            vibesql_ast::Statement::Insert(ins) => {
                InsertExecutor::execute(&mut db, &ins).expect("insert");
            }
            _ => panic!("unexpected setup statement"),
        }
    }
    db
}

fn run_update(db: &mut Database, sql: &str) -> Result<(), vibesql_executor::ExecutorError> {
    match Parser::parse_sql(sql).expect("parse") {
        vibesql_ast::Statement::Update(u) => UpdateExecutor::execute(&u, db).map(|_| ()),
        vibesql_ast::Statement::Insert(i) => InsertExecutor::execute(db, &i).map(|_| ()),
        _ => panic!("expected UPDATE/INSERT"),
    }
}

fn row(db: &Database, key: i64) -> Vec<SqlValue> {
    let table = db.get_table("t").expect("table");
    table
        .scan()
        .iter()
        .find(|r| r.values[0] == SqlValue::Integer(key))
        .map(|r| r.values.to_vec())
        .unwrap_or_default()
}

#[test]
fn update_tuple_set_row_value() {
    let mut db = setup();
    run_update(&mut db, "UPDATE t SET (b, c) = (99, 'x') WHERE a = 1").expect("update");
    let r = row(&db, 1);
    assert_eq!(r[1], SqlValue::Integer(99));
    assert_eq!(r[2], SqlValue::Varchar("x".into()));
}

#[test]
fn update_tuple_set_reorders_columns() {
    // Column list order drives the mapping: `(c, b)` swaps the targets.
    let mut db = setup();
    run_update(&mut db, "UPDATE t SET (c, b) = ('z', 77) WHERE a = 2").expect("update");
    let r = row(&db, 2);
    assert_eq!(r[1], SqlValue::Integer(77));
    assert_eq!(r[2], SqlValue::Varchar("z".into()));
}

#[test]
fn update_tuple_set_subquery() {
    let mut db = setup();
    run_update(&mut db, "UPDATE t SET (b, c) = (SELECT 5, 'q') WHERE a = 1").expect("update");
    let r = row(&db, 1);
    assert_eq!(r[1], SqlValue::Integer(5));
    assert_eq!(r[2], SqlValue::Varchar("q".into()));
}

#[test]
fn update_tuple_set_subquery_empty_yields_nulls() {
    // SQLite: a subquery that returns no rows assigns NULL to every target.
    let mut db = setup();
    run_update(&mut db, "UPDATE t SET (b, c) = (SELECT b, c FROM t WHERE a = 999) WHERE a = 1")
        .expect("update");
    let r = row(&db, 1);
    assert_eq!(r[1], SqlValue::Null);
    assert_eq!(r[2], SqlValue::Null);
}

#[test]
fn update_tuple_set_arity_mismatch_is_error() {
    // Two target columns but a three-element row value is an arity error.
    let mut db = setup();
    let err = run_update(&mut db, "UPDATE t SET (b, c) = (1, 2, 3) WHERE a = 1");
    assert!(err.is_err(), "arity mismatch must be rejected");
}

#[test]
fn on_conflict_do_update_tuple_set() {
    // upsert4 1.x.8 shape: DO UPDATE SET (c, a) = ('four', 4).
    let mut db = setup();
    run_update(
        &mut db,
        "INSERT INTO t VALUES(2, NULL, 'zero') ON CONFLICT (a) DO UPDATE SET (c, b) = ('four', 44)",
    )
    .expect("upsert");
    let r = row(&db, 2);
    assert_eq!(r[1], SqlValue::Integer(44));
    assert_eq!(r[2], SqlValue::Varchar("four".into()));
}

#[test]
fn on_conflict_do_update_tuple_set_subquery() {
    // upsert4 1.x.7 shape: DO UPDATE SET (b, c) = (SELECT 'x', 'y').
    let mut db = setup();
    run_update(
        &mut db,
        "INSERT INTO t VALUES(1, NULL, 'zero') ON CONFLICT (a) DO UPDATE SET (b, c) = (SELECT 7, 'y')",
    )
    .expect("upsert");
    let r = row(&db, 1);
    assert_eq!(r[1], SqlValue::Integer(7));
    assert_eq!(r[2], SqlValue::Varchar("y".into()));
}

// ============================================================================
// UPDATE ... FROM tuple/row SET assignments (issue #6047, root cause 2)
//
// `UPDATE t2 SET (a, b) = (...) FROM src` rewrites into a synthetic SELECT.
// A tuple assignment's RHS is a *row value*, not a scalar, in this position;
// projecting it as a single scalar column would wrongly raise
// "sub-select returns 2 columns - expected 1". Each target column must be
// projected as its own scalar in the join context.
// ============================================================================

/// Fixture for UPDATE…FROM: a target `t2(a, b)` and a source `t1(x, y, z)`.
fn setup_from() -> Database {
    let mut db = Database::new();
    for sql in [
        "CREATE TABLE t1(x, y, z)",
        "CREATE TABLE t2(a, b)",
        "INSERT INTO t1 VALUES(1000, 2000, 3000)",
        "INSERT INTO t2 VALUES(NULL, NULL)",
    ] {
        match Parser::parse_sql(sql).expect("parse") {
            vibesql_ast::Statement::CreateTable(ct) => {
                CreateTableExecutor::execute(&ct, &mut db).expect("create");
            }
            vibesql_ast::Statement::Insert(ins) => {
                InsertExecutor::execute(&mut db, &ins).expect("insert");
            }
            _ => panic!("unexpected setup statement"),
        }
    }
    db
}

fn t2_first_row(db: &Database) -> Vec<SqlValue> {
    let table = db.get_table("t2").expect("t2");
    table.scan().iter().next().map(|r| r.values.to_vec()).unwrap_or_default()
}

/// The minimized root-cause-2 reproducer: a bare (FROM-less) subquery RHS with
/// a target tuple, driven by an UPDATE…FROM. Used to fail with
/// "sub-select returns 2 columns - expected 1".
#[test]
fn update_from_tuple_set_bare_subquery() {
    let mut db = setup_from();
    run_update(&mut db, "UPDATE t2 SET (a, b) = (SELECT 1, 2) FROM t1").expect("update");
    let r = t2_first_row(&db);
    assert_eq!(r[0], SqlValue::Integer(1));
    assert_eq!(r[1], SqlValue::Integer(2));
}

/// A literal row-value RHS (no subquery) driven by UPDATE…FROM.
#[test]
fn update_from_tuple_set_row_value() {
    let mut db = setup_from();
    run_update(&mut db, "UPDATE t2 SET (a, b) = (7, 8) FROM t1").expect("update");
    let r = t2_first_row(&db);
    assert_eq!(r[0], SqlValue::Integer(7));
    assert_eq!(r[1], SqlValue::Integer(8));
}

/// A correlated tuple RHS that references the FROM table's columns. The subquery
/// projects `t1.x, t1.y`, so `t2` should receive `(1000, 2000)`.
#[test]
fn update_from_tuple_set_correlated_subquery() {
    let mut db = setup_from();
    run_update(&mut db, "UPDATE t2 SET (a, b) = (SELECT t1.x, t1.y) FROM t1").expect("update");
    let r = t2_first_row(&db);
    assert_eq!(r[0], SqlValue::Integer(1000));
    assert_eq!(r[1], SqlValue::Integer(2000));
}

/// The FROM table's columns can be referenced directly (not via subquery) in the
/// tuple RHS.
#[test]
fn update_from_tuple_set_direct_column_refs() {
    let mut db = setup_from();
    run_update(&mut db, "UPDATE t2 SET (a, b) = (t1.z, t1.x) FROM t1").expect("update");
    let r = t2_first_row(&db);
    assert_eq!(r[0], SqlValue::Integer(3000));
    assert_eq!(r[1], SqlValue::Integer(1000));
}

/// A single-column assignment under UPDATE…FROM must still work (no regression
/// to the ordinary non-tuple synthetic-SELECT path).
#[test]
fn update_from_single_column_assignment_unaffected() {
    let mut db = setup_from();
    run_update(&mut db, "UPDATE t2 SET a = t1.y FROM t1").expect("update");
    let r = t2_first_row(&db);
    assert_eq!(r[0], SqlValue::Integer(2000));
}
