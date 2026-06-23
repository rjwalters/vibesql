//! `UPDATE ... [ORDER BY ...] LIMIT n [OFFSET m]` on base tables (issue #5735).
//!
//! SQLite (compiled with SQLITE_ENABLE_UPDATE_DELETE_LIMIT) restricts how many
//! rows an UPDATE touches via an optional ORDER BY / LIMIT / OFFSET tail, the
//! same way it does for DELETE. VibeSQL supports it unconditionally to match the
//! conformance suite (see docs/reference/sqlite/test/wherelimit2.test). These
//! tests mirror the DELETE...LIMIT executor behavior.

use vibesql_ast::Statement;
use vibesql_parser::Parser;
use vibesql_types::SqlValue;

use crate::*;

fn ddl(db: &mut vibesql_storage::Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).unwrap_or_else(|e| panic!("parse {:?}: {:?}", sql, e));
    match stmt {
        Statement::CreateTable(s) => {
            CreateTableExecutor::execute(&s, db).expect("CREATE TABLE");
        }
        Statement::Insert(s) => {
            InsertExecutor::execute(db, &s).expect("INSERT");
        }
        other => panic!("unsupported ddl: {:?}", other),
    }
}

fn update(db: &mut vibesql_storage::Database, sql: &str) -> usize {
    match Parser::parse_sql(sql).expect("parse update") {
        Statement::Update(s) => UpdateExecutor::execute(&s, db).expect("UPDATE"),
        other => panic!("expected UPDATE, got {:?}", other),
    }
}

fn select(db: &mut vibesql_storage::Database, sql: &str) -> Vec<vibesql_storage::Row> {
    match Parser::parse_sql(sql).expect("parse select") {
        Statement::Select(s) => SelectExecutor::new(db).execute(&s).expect("SELECT"),
        other => panic!("expected SELECT, got {:?}", other),
    }
}

fn int(v: &SqlValue) -> i64 {
    match v {
        SqlValue::Integer(i) => *i,
        SqlValue::Bigint(i) => *i,
        other => panic!("expected integer, got {:?}", other),
    }
}

/// Count how many rows in `t(a, b)` currently have `b = target`.
fn count_b(db: &mut vibesql_storage::Database, target: i64) -> usize {
    select(db, "SELECT a, b FROM t").iter().filter(|r| int(&r.values[1]) == target).count()
}

fn seed(db: &mut vibesql_storage::Database) {
    ddl(db, "CREATE TABLE t(a INTEGER PRIMARY KEY, b)");
    ddl(db, "INSERT INTO t VALUES(1, 10)");
    ddl(db, "INSERT INTO t VALUES(2, 10)");
    ddl(db, "INSERT INTO t VALUES(3, 10)");
    ddl(db, "INSERT INTO t VALUES(4, 10)");
    ddl(db, "INSERT INTO t VALUES(5, 10)");
}

/// `UPDATE ... LIMIT n` (no ORDER BY, no WHERE) updates exactly n rows.
#[test]
fn update_limit_updates_only_n_rows() {
    let mut db = vibesql_storage::Database::new();
    seed(&mut db);

    assert_eq!(update(&mut db, "UPDATE t SET b = 99 LIMIT 2"), 2);
    assert_eq!(count_b(&mut db, 99), 2);
    assert_eq!(count_b(&mut db, 10), 3);
}

/// `UPDATE ... WHERE ... LIMIT n` only updates n of the matching rows.
#[test]
fn update_where_limit_caps_matched_rows() {
    let mut db = vibesql_storage::Database::new();
    seed(&mut db);

    // All 5 rows match b=10, but LIMIT 1 caps it to a single update.
    assert_eq!(update(&mut db, "UPDATE t SET b = 99 WHERE b = 10 LIMIT 1"), 1);
    assert_eq!(count_b(&mut db, 99), 1);
    assert_eq!(count_b(&mut db, 10), 4);
}

/// `UPDATE ... ORDER BY ... LIMIT n` updates the first n rows in sort order.
#[test]
fn update_order_by_limit_updates_first_rows() {
    let mut db = vibesql_storage::Database::new();
    seed(&mut db);

    // ORDER BY a ASC LIMIT 2 -> rows a=1 and a=2 get b=99.
    assert_eq!(update(&mut db, "UPDATE t SET b = 99 ORDER BY a LIMIT 2"), 2);
    let rows = select(&mut db, "SELECT a, b FROM t ORDER BY a");
    let got: Vec<(i64, i64)> =
        rows.iter().map(|r| (int(&r.values[0]), int(&r.values[1]))).collect();
    assert_eq!(got, vec![(1, 99), (2, 99), (3, 10), (4, 10), (5, 10)]);
}

/// `UPDATE ... ORDER BY ... DESC LIMIT n` honors descending order.
#[test]
fn update_order_by_desc_limit() {
    let mut db = vibesql_storage::Database::new();
    seed(&mut db);

    // ORDER BY a DESC LIMIT 2 -> rows a=5 and a=4.
    assert_eq!(update(&mut db, "UPDATE t SET b = 99 ORDER BY a DESC LIMIT 2"), 2);
    let rows = select(&mut db, "SELECT a, b FROM t ORDER BY a");
    let got: Vec<(i64, i64)> =
        rows.iter().map(|r| (int(&r.values[0]), int(&r.values[1]))).collect();
    assert_eq!(got, vec![(1, 10), (2, 10), (3, 10), (4, 99), (5, 99)]);
}

/// `UPDATE ... ORDER BY ... LIMIT n OFFSET m` skips m rows before updating n.
#[test]
fn update_order_by_limit_offset() {
    let mut db = vibesql_storage::Database::new();
    seed(&mut db);

    // ORDER BY a LIMIT 2 OFFSET 1 -> rows a=2 and a=3.
    assert_eq!(update(&mut db, "UPDATE t SET b = 99 ORDER BY a LIMIT 2 OFFSET 1"), 2);
    let rows = select(&mut db, "SELECT a, b FROM t ORDER BY a");
    let got: Vec<(i64, i64)> =
        rows.iter().map(|r| (int(&r.values[0]), int(&r.values[1]))).collect();
    assert_eq!(got, vec![(1, 10), (2, 99), (3, 99), (4, 10), (5, 10)]);
}

/// `LIMIT 0` updates no rows.
#[test]
fn update_limit_zero_updates_nothing() {
    let mut db = vibesql_storage::Database::new();
    seed(&mut db);

    assert_eq!(update(&mut db, "UPDATE t SET b = 99 LIMIT 0"), 0);
    assert_eq!(count_b(&mut db, 10), 5);
}

/// `LIMIT -1` means "no limit" (SQLite extension): all matching rows update.
#[test]
fn update_limit_negative_one_updates_all() {
    let mut db = vibesql_storage::Database::new();
    seed(&mut db);

    assert_eq!(update(&mut db, "UPDATE t SET b = 99 LIMIT -1"), 5);
    assert_eq!(count_b(&mut db, 99), 5);
}

/// Any negative LIMIT (not just -1) means "no limit" in SQLite (#5747).
#[test]
fn update_limit_negative_other_updates_all() {
    let mut db = vibesql_storage::Database::new();
    seed(&mut db);

    // `LIMIT 0, -5` is `LIMIT offset=0, count=-5` -> no limit, all rows.
    assert_eq!(update(&mut db, "UPDATE t SET b = 99 ORDER BY a LIMIT 0, -5"), 5);
    assert_eq!(count_b(&mut db, 99), 5);
}

/// A negative OFFSET is treated as 0 in SQLite (#5747): the LIMIT still applies
/// from the start of the (ordered) result.
#[test]
fn update_negative_offset_treated_as_zero() {
    let mut db = vibesql_storage::Database::new();
    seed(&mut db);

    // ORDER BY a LIMIT 2 OFFSET -3 -> offset clamps to 0 -> rows a=1, a=2.
    assert_eq!(update(&mut db, "UPDATE t SET b = 99 ORDER BY a LIMIT 2 OFFSET -3"), 2);
    let rows = select(&mut db, "SELECT a, b FROM t ORDER BY a");
    let got: Vec<(i64, i64)> =
        rows.iter().map(|r| (int(&r.values[0]), int(&r.values[1]))).collect();
    assert_eq!(got, vec![(1, 99), (2, 99), (3, 10), (4, 10), (5, 10)]);
}

/// `UPDATE` without LIMIT still updates every matching row (no regression).
#[test]
fn update_without_limit_updates_all() {
    let mut db = vibesql_storage::Database::new();
    seed(&mut db);

    assert_eq!(update(&mut db, "UPDATE t SET b = 99 WHERE b = 10"), 5);
    assert_eq!(count_b(&mut db, 99), 5);
}
