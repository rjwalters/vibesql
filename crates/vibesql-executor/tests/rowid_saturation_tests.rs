//! Regression tests for issue #5894: rowid allocation edge cases at the signed
//! 64-bit boundary and with negative INTEGER PRIMARY KEY values.
//!
//! Two allocation paths previously diverged from sqlite3 3.51.0:
//!
//!  1. Plain-rowid saturation: `Table::next_rowid_signed()` saturated at `i64::MAX`, so a table
//!     whose max rowid was `i64::MAX` silently allocated `i64::MAX` again — a *duplicate* rowid.
//!     sqlite3 instead picks a random unused rowid (returning SQLITE_FULL only if probing fails).
//!
//!  2. INTEGER PRIMARY KEY NULL auto-assign (`compute_next_integer_pk_value`) floored the max at 0
//!     (ignoring negative IPKs → allocated `1` instead of `max + 1`) and did an unchecked `max + 1`
//!     that panicked in debug / wrapped to `i64::MIN` in release when the max was `i64::MAX`.
//!
//! Both now delegate to `Table::allocate_rowid`, matching sqlite3 exactly. The
//! random-probe fallback is inherently nondeterministic, so the saturation tests
//! assert uniqueness + success, not specific rowid values.

use vibesql_executor::{InsertExecutor, SelectExecutor};
use vibesql_parser::Parser;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

fn exec(db: &mut Database, sql: &str) -> Result<(), vibesql_executor::ExecutorError> {
    match Parser::parse_sql(sql).expect("test SQL should parse") {
        vibesql_ast::Statement::CreateTable(s) => {
            vibesql_executor::CreateTableExecutor::execute(&s, db)?;
        }
        vibesql_ast::Statement::Insert(s) => {
            InsertExecutor::execute(db, &s)?;
        }
        other => panic!("unexpected statement in test: {other:?}"),
    }
    Ok(())
}

/// Reinterpret an integer-ish SqlValue as i64 (rowids are stored as the u64 bit
/// pattern and surface as Bigint; IPK columns surface as Integer).
fn as_i64(v: &SqlValue) -> i64 {
    match v {
        SqlValue::Integer(i) => *i,
        SqlValue::Bigint(i) => *i,
        other => panic!("expected an integer value, got {other:?}"),
    }
}

/// Collect one integer column across all rows, reinterpreted as i64.
fn column_i64(db: &Database, sql: &str) -> Vec<i64> {
    match Parser::parse_sql(sql).expect("select should parse") {
        vibesql_ast::Statement::Select(s) => SelectExecutor::new(db)
            .execute(&s)
            .expect("select should succeed")
            .into_iter()
            .map(|r| as_i64(&r.values[0]))
            .collect(),
        _ => panic!("expected SELECT"),
    }
}

/// Bug 1: a plain-rowid table saturated at `i64::MAX` allocates a *distinct*
/// random rowid on the next insert — never a silent duplicate `i64::MAX`.
#[test]
fn plain_rowid_at_i64_max_allocates_unique_rowid() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t(x INTEGER)").unwrap();
    exec(&mut db, "INSERT INTO t(rowid, x) VALUES(9223372036854775807, 1)").unwrap();
    // Previously: silent duplicate rowid = i64::MAX.
    exec(&mut db, "INSERT INTO t VALUES(2)").unwrap();

    let rowids = column_i64(&db, "SELECT rowid FROM t ORDER BY x");
    assert_eq!(rowids.len(), 2, "both rows present");
    assert_eq!(rowids[0], i64::MAX, "explicit MAX row unchanged");
    assert_ne!(rowids[1], i64::MAX, "second rowid must NOT duplicate i64::MAX");
    assert_ne!(rowids[0], rowids[1], "rowids must be unique");
    assert!(rowids[1] > 0, "sqlite3 probes positive rowids");
}

/// Bug 2a: NULL INTEGER PRIMARY KEY auto-assign on a table whose only rows are
/// negative allocates `max + 1` (sqlite3: after `-5`, next is `-4`), not `1`.
#[test]
fn ipk_negative_only_auto_assign_matches_sqlite() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t(a INTEGER PRIMARY KEY, b INTEGER)").unwrap();
    exec(&mut db, "INSERT INTO t VALUES(-5, 1)").unwrap();
    exec(&mut db, "INSERT INTO t(b) VALUES(2)").unwrap();

    let a = column_i64(&db, "SELECT a FROM t ORDER BY b");
    assert_eq!(a, vec![-5, -4], "sqlite3: max(-5) + 1 = -4, not 1");
}

/// Bug 2a (edge): after rowid `-1`, the next auto-assigned IPK is `0`.
#[test]
fn ipk_negative_one_auto_assign_yields_zero() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t(a INTEGER PRIMARY KEY, b INTEGER)").unwrap();
    exec(&mut db, "INSERT INTO t VALUES(-1, 1)").unwrap();
    exec(&mut db, "INSERT INTO t(b) VALUES(2)").unwrap();

    let a = column_i64(&db, "SELECT a FROM t ORDER BY b");
    assert_eq!(a, vec![-1, 0], "sqlite3: max(-1) + 1 = 0");
}

/// Bug 2a: a mix of negative and positive IPKs allocates `max_positive + 1`.
#[test]
fn ipk_mixed_sign_auto_assign_uses_max() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t(a INTEGER PRIMARY KEY, b INTEGER)").unwrap();
    exec(&mut db, "INSERT INTO t VALUES(-5, 1)").unwrap();
    exec(&mut db, "INSERT INTO t VALUES(10, 2)").unwrap();
    exec(&mut db, "INSERT INTO t(b) VALUES(3)").unwrap();

    let a = column_i64(&db, "SELECT a FROM t ORDER BY b");
    assert_eq!(a, vec![-5, 10, 11], "sqlite3: max(-5, 10) + 1 = 11");
}

/// Bug 2b: NULL INTEGER PRIMARY KEY auto-assign when the max IPK is `i64::MAX`
/// must NOT panic (debug) or wrap to `i64::MIN` (release) — it allocates a
/// distinct random rowid, matching sqlite3.
#[test]
fn ipk_at_i64_max_auto_assign_no_panic_no_wrap() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t(a INTEGER PRIMARY KEY, b INTEGER)").unwrap();
    exec(&mut db, "INSERT INTO t VALUES(9223372036854775807, 1)").unwrap();
    // Previously: debug panic ("attempt to add with overflow") / release wrap.
    exec(&mut db, "INSERT INTO t(b) VALUES(2)").unwrap();

    let a = column_i64(&db, "SELECT a FROM t ORDER BY b");
    assert_eq!(a.len(), 2, "both rows present");
    assert_eq!(a[0], i64::MAX, "explicit MAX row unchanged");
    assert_ne!(a[1], i64::MAX, "auto-assigned IPK must not duplicate i64::MAX");
    assert_ne!(a[1], i64::MIN, "auto-assigned IPK must not wrap to i64::MIN");
    assert!(a[1] > 0, "sqlite3 probes positive rowids");
}

/// Normal sequential allocation is unchanged for both plain-rowid and IPK
/// tables — the common path must not regress.
#[test]
fn sequential_allocation_unchanged() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE p(x INTEGER)").unwrap();
    exec(&mut db, "INSERT INTO p VALUES(10), (20), (30)").unwrap();
    assert_eq!(column_i64(&db, "SELECT rowid FROM p ORDER BY x"), vec![1, 2, 3]);

    exec(&mut db, "CREATE TABLE k(a INTEGER PRIMARY KEY, b INTEGER)").unwrap();
    exec(&mut db, "INSERT INTO k(b) VALUES(1)").unwrap();
    exec(&mut db, "INSERT INTO k(b) VALUES(2)").unwrap();
    exec(&mut db, "INSERT INTO k VALUES(100, 3)").unwrap();
    exec(&mut db, "INSERT INTO k(b) VALUES(4)").unwrap();
    assert_eq!(column_i64(&db, "SELECT a FROM k ORDER BY b"), vec![1, 2, 100, 101]);
}

/// A table one below the ceiling still allocates `i64::MAX` itself — the
/// boundary is exclusive, so this is NOT an error case.
#[test]
fn rowid_just_below_max_allocates_max() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t(x INTEGER)").unwrap();
    exec(&mut db, "INSERT INTO t(rowid, x) VALUES(9223372036854775806, 1)").unwrap();
    exec(&mut db, "INSERT INTO t VALUES(2)").unwrap();

    let rowids = column_i64(&db, "SELECT rowid FROM t ORDER BY x");
    assert_eq!(rowids, vec![i64::MAX - 1, i64::MAX]);
}
