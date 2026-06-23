//! `DELETE ... [ORDER BY ...] LIMIT n [OFFSET m]` on base tables (issue #5747).
//!
//! SQLite (compiled with SQLITE_ENABLE_UPDATE_DELETE_LIMIT) restricts how many
//! rows a DELETE removes via an optional ORDER BY / LIMIT / OFFSET tail. These
//! tests guard the LIMIT-without-ORDER-BY path (cluster A) and the negative
//! LIMIT/OFFSET semantics (cluster B) surfaced by wherelimit.test.

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

fn delete(db: &mut vibesql_storage::Database, sql: &str) -> usize {
    match Parser::parse_sql(sql).expect("parse delete") {
        Statement::Delete(s) => DeleteExecutor::execute(&s, db).expect("DELETE"),
        other => panic!("expected DELETE, got {:?}", other),
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

fn row_count(db: &mut vibesql_storage::Database) -> usize {
    select(db, "SELECT a FROM t").len()
}

fn seed(db: &mut vibesql_storage::Database) {
    ddl(db, "CREATE TABLE t(a INTEGER PRIMARY KEY, b)");
    for i in 1..=5 {
        ddl(db, &format!("INSERT INTO t VALUES({i}, 10)"));
    }
}

/// `DELETE ... LIMIT n` without ORDER BY removes exactly n rows (cluster A).
#[test]
fn delete_limit_without_order_by_caps_rows() {
    let mut db = vibesql_storage::Database::new();
    seed(&mut db);

    // 5 rows; LIMIT 2 must delete exactly 2, leaving 3.
    assert_eq!(delete(&mut db, "DELETE FROM t LIMIT 2"), 2);
    assert_eq!(row_count(&mut db), 3);
}

/// `DELETE ... WHERE ... LIMIT n` only removes n of the matching rows.
#[test]
fn delete_where_limit_caps_matched_rows() {
    let mut db = vibesql_storage::Database::new();
    seed(&mut db);

    // All 5 rows match b=10; LIMIT 1 deletes a single matching row.
    assert_eq!(delete(&mut db, "DELETE FROM t WHERE b = 10 LIMIT 1"), 1);
    assert_eq!(row_count(&mut db), 4);
}

/// `DELETE ... ORDER BY ... LIMIT n` removes the first n rows in sort order.
#[test]
fn delete_order_by_limit_removes_first_rows() {
    let mut db = vibesql_storage::Database::new();
    seed(&mut db);

    assert_eq!(delete(&mut db, "DELETE FROM t ORDER BY a LIMIT 2"), 2);
    let remaining: Vec<i64> =
        select(&mut db, "SELECT a FROM t ORDER BY a").iter().map(|r| int(&r.values[0])).collect();
    assert_eq!(remaining, vec![3, 4, 5]);
}

/// `LIMIT 0` deletes nothing.
#[test]
fn delete_limit_zero_deletes_nothing() {
    let mut db = vibesql_storage::Database::new();
    seed(&mut db);

    assert_eq!(delete(&mut db, "DELETE FROM t LIMIT 0"), 0);
    assert_eq!(row_count(&mut db), 5);
}

/// `LIMIT -1` means "no limit" (SQLite extension): all rows are removed.
#[test]
fn delete_limit_negative_one_deletes_all() {
    let mut db = vibesql_storage::Database::new();
    seed(&mut db);

    assert_eq!(delete(&mut db, "DELETE FROM t LIMIT -1"), 5);
    assert_eq!(row_count(&mut db), 0);
}

/// Any negative LIMIT (not just -1) means "no limit" in SQLite (#5747).
#[test]
fn delete_limit_negative_other_deletes_all() {
    let mut db = vibesql_storage::Database::new();
    seed(&mut db);

    // `LIMIT 0, -5` is `LIMIT offset=0, count=-5` -> no limit, all rows deleted.
    assert_eq!(delete(&mut db, "DELETE FROM t ORDER BY a LIMIT 0, -5"), 5);
    assert_eq!(row_count(&mut db), 0);
}

/// A negative OFFSET is treated as 0 in SQLite (#5747): the LIMIT applies from
/// the start of the (ordered) result.
#[test]
fn delete_negative_offset_treated_as_zero() {
    let mut db = vibesql_storage::Database::new();
    seed(&mut db);

    // ORDER BY a LIMIT 2 OFFSET -3 -> offset clamps to 0 -> deletes a=1, a=2.
    assert_eq!(delete(&mut db, "DELETE FROM t ORDER BY a LIMIT 2 OFFSET -3"), 2);
    let remaining: Vec<i64> =
        select(&mut db, "SELECT a FROM t ORDER BY a").iter().map(|r| int(&r.values[0])).collect();
    assert_eq!(remaining, vec![3, 4, 5]);
}

/// `DELETE` without LIMIT still removes every matching row (no regression).
#[test]
fn delete_without_limit_deletes_all_matching() {
    let mut db = vibesql_storage::Database::new();
    seed(&mut db);

    assert_eq!(delete(&mut db, "DELETE FROM t WHERE b = 10"), 5);
    assert_eq!(row_count(&mut db), 0);
}
