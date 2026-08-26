//! Regression tests for issue #6575: index range-scan false-equality for an
//! out-of-f64-safe-integer-precision INTEGER literal compared against a
//! REAL-affinity indexed column.
//!
//! `crates/vibesql-storage/src/database/indexes/value_normalization.rs`'s
//! `normalize_for_comparison` casts every numeric `SqlValue` to `Double` via
//! a lossy `as f64` cast before it's used as a BTreeMap range-scan bound.
//! When the *same* lossy cast independently rounds both a WHERE-clause
//! INTEGER literal and (at INSERT time) a REAL-affinity column's stored
//! value to the *same* `Double`, an exclusive bound (`col > literal`) was
//! treating the row's key as equal to the bound and excluding it — even
//! though the true (unrounded) comparison shows the row's value is strictly
//! greater than the literal (because the rounding moved the stored value
//! up). The fix corrects the inclusive/exclusive flag based on an exact
//! comparison between the original literal and its rounded `Double`
//! (`vibesql_storage::database::indexes::value_normalization::normalize_bound_for_range_scan`).
//!
//! Every test below asserts that the **indexed** (UNIQUE, so a BTreeMap
//! range-scan handles the WHERE clause) and **unindexed** (full scan through
//! the general expression evaluator) queries agree, since the general
//! evaluator was already correct per the issue's repro.

use vibesql_executor::{CreateIndexExecutor, CreateTableExecutor, InsertExecutor, SelectExecutor};
use vibesql_parser::Parser;
use vibesql_storage::Database;

fn exec(db: &mut Database, sql: &str) -> Result<(), vibesql_executor::ExecutorError> {
    match Parser::parse_sql(sql).expect("test SQL should parse") {
        vibesql_ast::Statement::CreateTable(s) => CreateTableExecutor::execute(&s, db).map(|_| ()),
        vibesql_ast::Statement::CreateIndex(s) => CreateIndexExecutor::execute(&s, db).map(|_| ()),
        vibesql_ast::Statement::Insert(s) => InsertExecutor::execute(db, &s).map(|_| ()),
        other => panic!("unexpected statement in test: {other:?}"),
    }
}

fn select_rows(db: &Database, sql: &str) -> Vec<vibesql_storage::Row> {
    let stmt = Parser::parse_sql(sql).unwrap();
    let vibesql_ast::Statement::Select(s) = stmt else {
        panic!("expected SELECT");
    };
    SelectExecutor::new(db).execute(&s).unwrap()
}

/// Primary repro from issue #6575: `3175546974276630385 < c0` on a
/// REAL-affinity UNIQUE-indexed column must return the row, matching the
/// (already-correct) unindexed full-scan behavior.
#[test]
fn indexed_exclusive_lower_bound_matches_unindexed_for_out_of_precision_literal() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t0(c0 REAL UNIQUE)").unwrap();
    exec(&mut db, "INSERT INTO t0(c0) VALUES (3175546974276630385)").unwrap();

    // Sanity: the general evaluator (no index involvement) agrees the
    // literal is less than the stored (rounded) value.
    let eval_rows = select_rows(&db, "SELECT 3175546974276630385 < c0 FROM t0");
    assert_eq!(eval_rows.len(), 1);
    assert_eq!(eval_rows[0].get(0).unwrap(), &vibesql_types::SqlValue::Boolean(true));

    // The indexed WHERE-clause path must agree: exactly 1 row.
    let where_rows = select_rows(&db, "SELECT 1 FROM t0 WHERE 3175546974276630385 < c0");
    assert_eq!(
        where_rows.len(),
        1,
        "indexed range scan must not falsely exclude the row due to precision-rounding equality"
    );
}

/// Same repro, phrased as `c0 > literal` (column on the left) rather than
/// `literal < c0`, to cover both `extract_range_predicate` branches.
#[test]
fn indexed_exclusive_lower_bound_column_on_left() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t0(c0 REAL UNIQUE)").unwrap();
    exec(&mut db, "INSERT INTO t0(c0) VALUES (3175546974276630385)").unwrap();

    let rows = select_rows(&db, "SELECT 1 FROM t0 WHERE c0 > 3175546974276630385");
    assert_eq!(rows.len(), 1);
}

/// `col >= literal` must NOT match when the literal rounds *up* past the
/// stored value and the true value is strictly less than the literal is not
/// the case here — this exercises the inclusive lower-bound branch, which
/// should already have matched before the fix (no false-equality risk) and
/// must continue to match after it.
#[test]
fn indexed_inclusive_lower_bound_out_of_precision_literal_still_matches() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t0(c0 REAL UNIQUE)").unwrap();
    exec(&mut db, "INSERT INTO t0(c0) VALUES (3175546974276630385)").unwrap();

    let rows = select_rows(&db, "SELECT 1 FROM t0 WHERE c0 >= 3175546974276630385");
    assert_eq!(rows.len(), 1, "inclusive lower bound must still find the row");
}

/// Upper-bound mirror: a literal that rounds *down* when cast to `Double`
/// must still satisfy a strict `<` comparison against the (larger, exact)
/// literal for a row whose stored value is the rounded-down double.
#[test]
fn indexed_exclusive_upper_bound_matches_unindexed_for_out_of_precision_literal() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t1(c0 REAL UNIQUE)").unwrap();
    // 2^53 + 1 rounds DOWN to 2^53 when cast through f64 (INTEGER -> REAL
    // affinity coercion at INSERT time uses the same lossy `as f64` cast).
    exec(&mut db, "INSERT INTO t1(c0) VALUES (9007199254740992)").unwrap(); // 2^53, stored exactly

    let literal = "9007199254740993"; // 2^53 + 1, rounds down to 2^53 as f64

    let eval_rows = select_rows(&db, &format!("SELECT c0 < {literal} FROM t1"));
    assert_eq!(eval_rows.len(), 1);
    assert_eq!(eval_rows[0].get(0).unwrap(), &vibesql_types::SqlValue::Boolean(true));

    let where_rows = select_rows(&db, &format!("SELECT 1 FROM t1 WHERE c0 < {literal}"));
    assert_eq!(
        where_rows.len(),
        1,
        "indexed range scan must not falsely exclude a row whose exact value is < the literal"
    );
}

/// A row whose value is *not* on the boundary must still be excluded by an
/// out-of-precision-literal bound (no over-broadening from the fix).
#[test]
fn indexed_bound_still_excludes_non_matching_rows() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE t0(c0 REAL UNIQUE)").unwrap();
    exec(&mut db, "INSERT INTO t0(c0) VALUES (3175546974276630385)").unwrap();
    exec(&mut db, "INSERT INTO t0(c0) VALUES (1.0)").unwrap();
    exec(&mut db, "INSERT INTO t0(c0) VALUES (-3175546974276630385)").unwrap();

    let rows = select_rows(&db, "SELECT c0 FROM t0 WHERE c0 > 3175546974276630385");
    assert_eq!(rows.len(), 1, "only the boundary row should qualify, not the smaller rows");
}
