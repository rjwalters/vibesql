//! Regression tests for issue #5520: rowid (INTEGER) affinity coercion on INSERT.
//!
//! SQLite applies INTEGER affinity to the value supplied for a rowid /
//! INTEGER PRIMARY KEY position before storing it (and before triggers observe
//! `NEW.rowid`):
//!   - a TEXT value that is losslessly an integer (e.g. `'45'`) is coerced,
//!   - a REAL value that is losslessly an integer (e.g. `45.0`, `-42.0`) is coerced to the integer
//!     rowid,
//!   - a value with a fractional part (e.g. `42.4`) or a non-numeric TEXT/BLOB raises `datatype
//!     mismatch`.
//!
//! Before #5520, VibeSQL handled positive REAL/TEXT rowid *literals*, but the
//! unary-minus path (e.g. `-42.0`) only handled integers, so the residual
//! triggerC-4.1.4 case
//! (`INSERT INTO t4(rowid,a,b,c) VALUES(-42.0, -42.0, -42.0, -42.0)`) failed
//! with `datatype mismatch`. This file pins the affinity behavior to match
//! sqlite3 3.51.0.

use vibesql_executor::{InsertExecutor, SelectExecutor, TriggerExecutor};
use vibesql_parser::Parser;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

fn exec(db: &mut Database, sql: &str) -> Result<(), vibesql_executor::ExecutorError> {
    match Parser::parse_sql(sql).expect("test SQL should parse") {
        vibesql_ast::Statement::CreateTable(s) => {
            vibesql_executor::CreateTableExecutor::execute(&s, db)?;
        }
        vibesql_ast::Statement::CreateTrigger(s) => {
            TriggerExecutor::create_trigger_with_sql(db, &s, Some(sql))?;
        }
        vibesql_ast::Statement::Insert(s) => {
            InsertExecutor::execute(db, &s)?;
        }
        other => panic!("unexpected statement in test: {other:?}"),
    }
    Ok(())
}

/// Run a SELECT, returning rows as Vec<Vec<SqlValue>>.
fn query(db: &Database, sql: &str) -> Vec<Vec<SqlValue>> {
    match Parser::parse_sql(sql).expect("select should parse") {
        vibesql_ast::Statement::Select(s) => SelectExecutor::new(db)
            .execute(&s)
            .expect("select should succeed")
            .into_iter()
            .map(|r| r.values.to_vec())
            .collect(),
        _ => panic!("expected SELECT"),
    }
}

/// Fetch the rowid (as i64) + typeof(rowid) for the single row of `t`, via a
/// LIVE SELECT. The rowid is stored as a u64 and surfaced by VibeSQL as
/// `Bigint`; reinterpret it as i64 so negative rowids round-trip (matching how
/// `NEW.rowid` is resolved and how sqlite3 reports them).
fn rowid_and_type(db: &Database, table: &str) -> (i64, SqlValue) {
    let rows = query(db, &format!("SELECT rowid, typeof(rowid) FROM {table}"));
    assert_eq!(rows.len(), 1, "expected exactly one row in {table}");
    let rowid = match &rows[0][0] {
        SqlValue::Integer(i) => *i,
        SqlValue::Bigint(i) => *i,
        other => panic!("rowid not an integer type: {other:?}"),
    };
    (rowid, rows[0][1].clone())
}

/// `INSERT INTO t(rowid, ...) VALUES('45', ...)` — a TEXT-integer rowid is
/// coerced to an INTEGER rowid (matches sqlite3 3.51.0).
#[test]
fn text_integer_rowid_coerced_to_integer() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t(a TEXT)").unwrap();
    exec(&mut db, "INSERT INTO t(rowid, a) VALUES('45', 'x')").unwrap();

    let (rowid, ty) = rowid_and_type(&db, "t");
    assert_eq!(rowid, 45);
    assert_eq!(ty, SqlValue::Varchar("integer".into()));
}

/// `INSERT INTO t(rowid, ...) VALUES(45.0, ...)` — a positive REAL-integer
/// rowid is coerced to an INTEGER rowid.
#[test]
fn positive_real_integer_rowid_coerced_to_integer() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t(a TEXT)").unwrap();
    exec(&mut db, "INSERT INTO t(rowid, a) VALUES(45.0, 'x')").unwrap();

    let (rowid, ty) = rowid_and_type(&db, "t");
    assert_eq!(rowid, 45);
    assert_eq!(ty, SqlValue::Varchar("integer".into()));
}

/// `INSERT INTO t(rowid, ...) VALUES(-42.0, ...)` — the triggerC-4.1.4 case: a
/// NEGATIVE REAL-integer rowid (parsed as unary minus over a real literal) is
/// coerced to an INTEGER rowid. Before #5520 this raised `datatype mismatch`.
#[test]
fn negative_real_integer_rowid_coerced_to_integer() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t(a TEXT)").unwrap();
    exec(&mut db, "INSERT INTO t(rowid, a) VALUES(-42.0, 'x')").unwrap();

    let (rowid, ty) = rowid_and_type(&db, "t");
    assert_eq!(rowid, -42);
    assert_eq!(ty, SqlValue::Varchar("integer".into()));
}

/// A NEGATIVE INTEGER rowid is still accepted for an explicit rowid INSERT.
#[test]
fn negative_integer_rowid_accepted() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t(a TEXT)").unwrap();
    exec(&mut db, "INSERT INTO t(rowid, a) VALUES(-7, 'x')").unwrap();

    let (rowid, ty) = rowid_and_type(&db, "t");
    assert_eq!(rowid, -7);
    assert_eq!(ty, SqlValue::Varchar("integer".into()));
}

/// A REAL rowid with a fractional part cannot be a rowid: `datatype mismatch`
/// (matches sqlite3 3.51.0), for both positive and negative.
#[test]
fn fractional_real_rowid_raises_datatype_mismatch() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t(a TEXT)").unwrap();

    let pos = exec(&mut db, "INSERT INTO t(rowid, a) VALUES(42.4, 'x')");
    assert!(pos.is_err(), "fractional rowid should be rejected");
    assert!(format!("{:?}", pos.unwrap_err()).contains("datatype mismatch"));

    let neg = exec(&mut db, "INSERT INTO t(rowid, a) VALUES(-42.4, 'x')");
    assert!(neg.is_err(), "fractional negative rowid should be rejected");
    assert!(format!("{:?}", neg.unwrap_err()).contains("datatype mismatch"));
}

/// A non-numeric TEXT rowid raises `datatype mismatch`.
#[test]
fn non_numeric_text_rowid_raises_datatype_mismatch() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t(a TEXT)").unwrap();

    let res = exec(&mut db, "INSERT INTO t(rowid, a) VALUES('abc', 'x')");
    assert!(res.is_err(), "non-numeric text rowid should be rejected");
    assert!(format!("{:?}", res.unwrap_err()).contains("datatype mismatch"));
}

/// triggerC-4.1.4 (rowid slice): an AFTER INSERT trigger reading `new.rowid`
/// (and `typeof(new.rowid)`) observes the coerced INTEGER rowid, and an INTEGER
/// column reading `new.b` observes the coerced integer — for the exact
/// triggerC-4.1.4 statement
/// `INSERT INTO t4(rowid,a,b,c) VALUES(-42.0, -42.0, -42.0, -42.0)`.
///
/// Note: the TEXT column `a` and REAL column `c` (real → text formatting,
/// e.g. `-42.0` vs `-42`) are a separate number-to-text affinity gap tracked
/// outside #5520; this test pins only the rowid/INTEGER coercion that #5520
/// closes.
#[test]
fn trigger_observes_coerced_rowid_for_negative_real() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE log(t TEXT)").unwrap();
    exec(&mut db, "CREATE TABLE t4(a TEXT, b INTEGER, c REAL)").unwrap();
    exec(
        &mut db,
        "CREATE TRIGGER t4ai AFTER INSERT ON t4 BEGIN \
         INSERT INTO log VALUES(new.rowid || ' ' || typeof(new.rowid) || ' ' || \
                                new.b     || ' ' || typeof(new.b)); \
         END",
    )
    .unwrap();

    // triggerC-4.1.4: VALUES(-42.0, -42.0, -42.0, -42.0)
    exec(&mut db, "INSERT INTO t4(rowid,a,b,c) VALUES(-42.0, -42.0, -42.0, -42.0)").unwrap();

    let rows = query(&db, "SELECT t FROM log");
    assert_eq!(rows.len(), 1);
    // sqlite3 3.51.0: new.rowid = -42 (integer), new.b = -42 (integer).
    assert_eq!(rows[0][0], SqlValue::Varchar("-42 integer -42 integer".into()));
}

/// triggerC-4.1.5 (issue #6176): `INSERT INTO t(rowid, ...) VALUES(NULL, ...)`
/// auto-assigns the rowid, so a BEFORE INSERT trigger must observe the
/// unwritten-rowid sentinel (-1) — exactly as it does when the rowid column is
/// omitted — while an AFTER INSERT trigger observes the real assigned rowid.
#[test]
fn before_trigger_sees_sentinel_for_explicit_null_rowid() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE log(t TEXT)").unwrap();
    exec(&mut db, "CREATE TABLE t4(a TEXT)").unwrap();
    exec(
        &mut db,
        "CREATE TRIGGER t4bi BEFORE INSERT ON t4 BEGIN \
         INSERT INTO log VALUES('before ' || new.rowid || ' ' || typeof(new.rowid)); \
         END",
    )
    .unwrap();
    exec(
        &mut db,
        "CREATE TRIGGER t4ai AFTER INSERT ON t4 BEGIN \
         INSERT INTO log VALUES('after ' || new.rowid || ' ' || typeof(new.rowid)); \
         END",
    )
    .unwrap();

    exec(&mut db, "INSERT INTO t4(rowid, a) VALUES(NULL, 'x')").unwrap();

    let rows = query(&db, "SELECT t FROM log ORDER BY rowid");
    assert_eq!(rows.len(), 2);
    // sqlite3 3.51.0: BEFORE sees -1 (sentinel), AFTER sees the assigned rowid 1.
    assert_eq!(rows[0][0], SqlValue::Varchar("before -1 integer".into()));
    assert_eq!(rows[1][0], SqlValue::Varchar("after 1 integer".into()));

    // The stored row carries the real assigned rowid.
    let (rowid, ty) = rowid_and_type(&db, "t4");
    assert_eq!(rowid, 1);
    assert_eq!(ty, SqlValue::Varchar("integer".into()));
}

/// An EXPLICIT (non-NULL) rowid remains visible as-is to BEFORE INSERT
/// triggers — the sentinel only applies to auto-assigned rowids.
#[test]
fn before_trigger_sees_explicit_rowid_value() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE log(t TEXT)").unwrap();
    exec(&mut db, "CREATE TABLE t4(a TEXT)").unwrap();
    exec(
        &mut db,
        "CREATE TRIGGER t4bi BEFORE INSERT ON t4 BEGIN \
         INSERT INTO log VALUES('before ' || new.rowid); \
         END",
    )
    .unwrap();

    exec(&mut db, "INSERT INTO t4(rowid, a) VALUES(45, 'x')").unwrap();

    let rows = query(&db, "SELECT t FROM log");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0][0], SqlValue::Varchar("before 45".into()));
}
