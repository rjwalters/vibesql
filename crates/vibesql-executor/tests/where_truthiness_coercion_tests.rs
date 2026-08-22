//! Differential tests vs sqlite3 for WHERE/JOIN truthiness coercion (#5830).
//!
//! SQLite has no boolean storage class, so a WHERE (or JOIN ON) predicate that
//! evaluates to a non-boolean scalar is coerced to a truth value:
//! - numbers: zero is falsy, non-zero is truthy
//! - text/BLOB: the leading-numeric prefix is parsed (bytes read as text for BLOBs); non-zero
//!   prefix is truthy, otherwise falsy. `'first'` → 0 (falsy), `'1first'` → 1 (truthy).
//!
//! Before #5830, VibeSQL raised "Filter expression must evaluate to boolean"
//! for string/blob WHERE values on the FROM-table paths, and the no-FROM path
//! (`SELECT 1 WHERE 'first'`) treated any non-numeric value as truthy. Both now
//! delegate to the shared `crate::evaluator::operators::is_truthy` helper (the
//! same helper #5803 used to unify HAVING).
//!
//! Every expected value in this file was verified against sqlite3.

use vibesql_executor::SelectExecutor;
use vibesql_types::SqlValue;

/// t(x) with a single row (1), matching the issue's reproducers.
fn setup_t() -> vibesql_storage::Database {
    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "T".to_string(),
        vec![vibesql_catalog::ColumnSchema::new(
            "X".to_string(),
            vibesql_types::DataType::Integer,
            true,
        )],
    );
    db.create_table(schema).unwrap();
    db.insert_row("T", vibesql_storage::Row::new(vec![SqlValue::Integer(1)])).unwrap();
    db
}

/// t3(x) with three rows (1),(2),(3) to exercise the multi-row filter paths.
fn setup_t3() -> vibesql_storage::Database {
    let mut db = vibesql_storage::Database::new();
    let schema = vibesql_catalog::TableSchema::new(
        "T3".to_string(),
        vec![vibesql_catalog::ColumnSchema::new(
            "X".to_string(),
            vibesql_types::DataType::Integer,
            true,
        )],
    );
    db.create_table(schema).unwrap();
    for x in [1, 2, 3] {
        db.insert_row("T3", vibesql_storage::Row::new(vec![SqlValue::Integer(x)])).unwrap();
    }
    db
}

fn run_query(db: &vibesql_storage::Database, sql: &str) -> Vec<vibesql_storage::Row> {
    let executor = SelectExecutor::new(db);
    let stmt = vibesql_parser::Parser::parse_sql(sql)
        .unwrap_or_else(|e| panic!("parse failed for {sql}: {e:?}"));
    let vibesql_ast::Statement::Select(select_stmt) = stmt else {
        panic!("Expected SELECT statement: {sql}");
    };
    executor.execute(&select_stmt).unwrap_or_else(|e| panic!("execute failed for {sql}: {e:?}"))
}

fn row_count(db: &vibesql_storage::Database, sql: &str) -> usize {
    run_query(db, sql).len()
}

// ---------------------------------------------------------------------------
// Path A — FROM-table WHERE with string values (the primary reproducers)
// ---------------------------------------------------------------------------

#[test]
fn test_where_string_falsy_returns_no_rows() {
    let db = setup_t();
    // sqlite3: 'first' → 0 → falsy → 0 rows (previously errored)
    assert_eq!(row_count(&db, "SELECT x FROM t WHERE 'first'"), 0);
    // Empty and purely non-numeric strings are also falsy
    assert_eq!(row_count(&db, "SELECT x FROM t WHERE ''"), 0);
    assert_eq!(row_count(&db, "SELECT x FROM t WHERE 'abc'"), 0);
    // '0' parses to 0 → falsy
    assert_eq!(row_count(&db, "SELECT x FROM t WHERE '0'"), 0);
}

#[test]
fn test_where_string_truthy_returns_rows() {
    let db = setup_t();
    // sqlite3: '1first' → 1 → truthy → the row (previously errored)
    assert_eq!(row_count(&db, "SELECT x FROM t WHERE '1first'"), 1);
    assert_eq!(row_count(&db, "SELECT x FROM t WHERE '1'"), 1);
    // leading whitespace then a signed number is still parsed
    assert_eq!(row_count(&db, "SELECT x FROM t WHERE ' -3xyz'"), 1);
    assert_eq!(row_count(&db, "SELECT x FROM t WHERE '0.5abc'"), 1);
}

#[test]
fn test_where_numeric_regressions() {
    let db = setup_t();
    // Regression guard: numeric truthiness must be unchanged.
    assert_eq!(row_count(&db, "SELECT x FROM t WHERE 0.5"), 1);
    assert_eq!(row_count(&db, "SELECT x FROM t WHERE 0"), 0);
    assert_eq!(row_count(&db, "SELECT x FROM t WHERE 1"), 1);
    assert_eq!(row_count(&db, "SELECT x FROM t WHERE 0.0"), 0);
    // NULL is falsy
    assert_eq!(row_count(&db, "SELECT x FROM t WHERE NULL"), 0);
}

// ---------------------------------------------------------------------------
// Path A — BLOB WHERE values (bytes read as text, same leading-numeric parse)
// ---------------------------------------------------------------------------

#[test]
fn test_where_blob_truthiness() {
    let db = setup_t();
    // x'31' is the byte "1" → truthy
    assert_eq!(row_count(&db, "SELECT x FROM t WHERE x'31'"), 1);
    // x'3100' is "1\0" → leading-numeric 1 → truthy (verified in sqlite3)
    assert_eq!(row_count(&db, "SELECT x FROM t WHERE x'3100'"), 1);
    // x'00' is a NUL byte → 0 → falsy
    assert_eq!(row_count(&db, "SELECT x FROM t WHERE x'00'"), 0);
    // zeroblob(4) is four NUL bytes → 0 → falsy
    assert_eq!(row_count(&db, "SELECT x FROM t WHERE zeroblob(4)"), 0);
    // x'6162' is "ab" → non-numeric → 0 → falsy
    assert_eq!(row_count(&db, "SELECT x FROM t WHERE x'6162'"), 0);
}

// ---------------------------------------------------------------------------
// Path A — multi-row filter path (sequential FROM-table scan)
// ---------------------------------------------------------------------------

#[test]
fn test_where_string_multi_row_scan() {
    let db = setup_t3();
    // Constant string predicate applies to every row uniformly.
    assert_eq!(row_count(&db, "SELECT x FROM t3 WHERE 'first'"), 0);
    assert_eq!(row_count(&db, "SELECT x FROM t3 WHERE '1first'"), 3);
    assert_eq!(row_count(&db, "SELECT x FROM t3 WHERE zeroblob(2)"), 0);
    assert_eq!(row_count(&db, "SELECT x FROM t3 WHERE x'31'"), 3);
    // Column-dependent predicate still works alongside the coercion change.
    assert_eq!(row_count(&db, "SELECT x FROM t3 WHERE x > 1"), 2);
}

// ---------------------------------------------------------------------------
// Path B — no-FROM WHERE (synthetic single row)
// ---------------------------------------------------------------------------

#[test]
fn test_no_from_string_falsy_returns_no_rows() {
    let db = setup_t();
    // sqlite3: SELECT 1 WHERE 'first' → 0 rows (previously returned a row)
    assert_eq!(row_count(&db, "SELECT 1 WHERE 'first'"), 0);
    assert_eq!(row_count(&db, "SELECT 1 WHERE ''"), 0);
    assert_eq!(row_count(&db, "SELECT 1 WHERE '0'"), 0);
    assert_eq!(row_count(&db, "SELECT 1 WHERE zeroblob(4)"), 0);
    assert_eq!(row_count(&db, "SELECT 1 WHERE x'00'"), 0);
}

#[test]
fn test_no_from_string_truthy_returns_row() {
    let db = setup_t();
    // sqlite3: SELECT 1 WHERE '1' → 1 row
    assert_eq!(row_count(&db, "SELECT 1 WHERE '1'"), 1);
    assert_eq!(row_count(&db, "SELECT 1 WHERE '1first'"), 1);
    assert_eq!(row_count(&db, "SELECT 1 WHERE x'31'"), 1);
    // numeric regressions on the no-FROM path
    assert_eq!(row_count(&db, "SELECT 1 WHERE 0"), 0);
    assert_eq!(row_count(&db, "SELECT 1 WHERE 0.5"), 1);
    assert_eq!(row_count(&db, "SELECT 1 WHERE NULL"), 0);
}

// ---------------------------------------------------------------------------
// JOIN ON condition truthiness (same coercion class as WHERE, #5830)
// ---------------------------------------------------------------------------

#[test]
fn test_join_condition_string_truthiness() {
    let db = setup_t();
    // Cross join of t with itself is a single combined row; the ON predicate is
    // coerced the same way as WHERE.
    assert_eq!(row_count(&db, "SELECT a.x FROM t a JOIN t b ON 'first'"), 0);
    assert_eq!(row_count(&db, "SELECT a.x FROM t a JOIN t b ON '1first'"), 1);
    assert_eq!(row_count(&db, "SELECT a.x FROM t a JOIN t b ON x'31'"), 1);
    assert_eq!(row_count(&db, "SELECT a.x FROM t a JOIN t b ON zeroblob(4)"), 0);
}
