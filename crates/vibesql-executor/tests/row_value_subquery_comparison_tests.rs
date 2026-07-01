//! Tests for row-value (tuple) comparisons against scalar subqueries and for
//! row-value comparison affinity — issue #5781.
//!
//! Covers:
//! - `(a, b, c) <op> (SELECT a, b, c ...)` for every comparison operator, including the
//!   subquery-on-left form and `IS` / `IS NOT`.
//! - `(SELECT a, b) <op> (SELECT x, y)` (both sides multi-column subqueries).
//! - Per-column type affinity inside row-value comparison (mixed TEXT/INTEGER).
//! - Three-valued NULL semantics for row-value comparison.
//!
//! Expected values were verified against SQLite semantics (SQL:1999 §7.1).
//! Both a table-backed path (combined evaluator) and, where meaningful, the
//! FROM-less path (simple evaluator) are exercised.

use vibesql_executor::SelectExecutor;
use vibesql_types::SqlValue;

fn run_stmt(db: &mut vibesql_storage::Database, sql: &str) {
    let stmt = vibesql_parser::Parser::parse_sql(sql).unwrap();
    match stmt {
        vibesql_ast::Statement::CreateTable(create_table) => {
            vibesql_executor::CreateTableExecutor::execute(&create_table, db).unwrap();
        }
        vibesql_ast::Statement::Insert(insert) => {
            vibesql_executor::InsertExecutor::execute(db, &insert).unwrap();
        }
        other => panic!("Unsupported statement in test setup: {:?}", other),
    }
}

/// Run a SELECT that yields a single scalar value; normalize Boolean to 0/1
/// Integer (SQLite has no boolean storage class).
fn query_scalar(db: &vibesql_storage::Database, sql: &str) -> SqlValue {
    let stmt = vibesql_parser::Parser::parse_sql(sql).unwrap();
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let executor = SelectExecutor::new(db);
        let rows = executor
            .execute(&select_stmt)
            .unwrap_or_else(|e| panic!("Query failed: {} -- {:?}", sql, e));
        assert_eq!(rows.len(), 1, "Expected exactly one row for: {}", sql);
        assert_eq!(rows[0].values.len(), 1, "Expected exactly one column for: {}", sql);
        match &rows[0].values[0] {
            SqlValue::Boolean(b) => SqlValue::Integer(*b as i64),
            other => other.clone(),
        }
    } else {
        panic!("Expected SELECT statement: {}", sql);
    }
}

fn assert_scalar(db: &vibesql_storage::Database, sql: &str, expected: SqlValue) {
    let actual = query_scalar(db, sql);
    assert_eq!(actual, expected, "Query: {} -- expected {:?}, got {:?}", sql, expected, actual);
}

/// Run a SELECT and return the first column of every row as a Vec<SqlValue>.
fn query_column(db: &vibesql_storage::Database, sql: &str) -> Vec<SqlValue> {
    let stmt = vibesql_parser::Parser::parse_sql(sql).unwrap();
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let executor = SelectExecutor::new(db);
        let rows = executor
            .execute(&select_stmt)
            .unwrap_or_else(|e| panic!("Query failed: {} -- {:?}", sql, e));
        rows.iter().map(|r| r.values[0].clone()).collect()
    } else {
        panic!("Expected SELECT statement: {}", sql);
    }
}

/// t3 mirrors the SQLite rowvalue2.test section-3 fixture shape: typeless
/// columns holding string slices, plus a whole-word column `w`.
fn db_with_t3() -> vibesql_storage::Database {
    let mut db = vibesql_storage::Database::new();
    run_stmt(&mut db, "CREATE TABLE t3(a, b, c, w)");
    run_stmt(&mut db, "INSERT INTO t3 VALUES('air', 'far', 'e', 'airfare')");
    run_stmt(&mut db, "INSERT INTO t3 VALUES('air', 'fie', 'ld', 'airfield')");
    run_stmt(&mut db, "INSERT INTO t3 VALUES('air', 'fie', 'lds', 'airfields')");
    db
}

// ---------------------------------------------------------------------------
// Phase 1: (a, b, c) <op> (SELECT a, b, c ...)
// ---------------------------------------------------------------------------

#[test]
fn tuple_gt_subquery_matches_lexicographically_greater_rows() {
    let db = db_with_t3();
    // airfare is the smallest word; airfield and airfields are strictly greater.
    let sql = "SELECT w FROM t3 \
        WHERE (a, b, c) > (SELECT a, b, c FROM t3 WHERE w = 'airfare') \
        ORDER BY w";
    assert_eq!(
        query_column(&db, sql),
        vec![SqlValue::Varchar("airfield".into()), SqlValue::Varchar("airfields".into()),]
    );
}

#[test]
fn tuple_ge_subquery_includes_equal_row() {
    let db = db_with_t3();
    let sql = "SELECT w FROM t3 \
        WHERE (a, b, c) >= (SELECT a, b, c FROM t3 WHERE w = 'airfare') \
        ORDER BY w";
    assert_eq!(
        query_column(&db, sql),
        vec![
            SqlValue::Varchar("airfare".into()),
            SqlValue::Varchar("airfield".into()),
            SqlValue::Varchar("airfields".into()),
        ]
    );
}

#[test]
fn tuple_eq_and_is_subquery_match_single_row() {
    let db = db_with_t3();
    for op in ["==", "IS"] {
        let sql = format!(
            "SELECT w FROM t3 \
             WHERE (a, b, c) {op} (SELECT a, b, c FROM t3 WHERE w = 'airfield')"
        );
        assert_eq!(
            query_column(&db, &sql),
            vec![SqlValue::Varchar("airfield".into())],
            "operator {op}"
        );
    }
}

#[test]
fn tuple_lt_subquery_matches_smaller_rows() {
    let db = db_with_t3();
    let sql = "SELECT w FROM t3 \
        WHERE (a, b, c) < (SELECT a, b, c FROM t3 WHERE w = 'airfield') \
        ORDER BY w";
    assert_eq!(query_column(&db, sql), vec![SqlValue::Varchar("airfare".into())]);
}

#[test]
fn subquery_on_left_flips_operator_correctly() {
    let db = db_with_t3();
    // (SELECT airfare) < (a, b, c)  ==  (a, b, c) > (SELECT airfare)
    let left = "SELECT w FROM t3 \
        WHERE (SELECT a, b, c FROM t3 WHERE w = 'airfare') < (a, b, c) ORDER BY w";
    let right = "SELECT w FROM t3 \
        WHERE (a, b, c) > (SELECT a, b, c FROM t3 WHERE w = 'airfare') ORDER BY w";
    assert_eq!(query_column(&db, left), query_column(&db, right));
    assert_eq!(query_column(&db, left).len(), 2);
}

#[test]
fn tuple_is_not_subquery() {
    let db = db_with_t3();
    // Every row IS NOT DISTINCT-negated from a different row except itself.
    let sql = "SELECT w FROM t3 \
        WHERE (a, b, c) IS NOT (SELECT a, b, c FROM t3 WHERE w = 'airfare') \
        ORDER BY w";
    assert_eq!(
        query_column(&db, sql),
        vec![SqlValue::Varchar("airfield".into()), SqlValue::Varchar("airfields".into()),]
    );
}

// ---------------------------------------------------------------------------
// Phase 1 extension: (SELECT a, b) <op> (SELECT x, y)
// ---------------------------------------------------------------------------

#[test]
fn two_multicolumn_subqueries_equal() {
    let db = vibesql_storage::Database::new();
    assert_scalar(&db, "SELECT (SELECT 1, 2, 3) == (SELECT 1, 2, 3)", SqlValue::Integer(1));
    assert_scalar(&db, "SELECT (SELECT 1, 0, 3) == (SELECT 1, 2, 3)", SqlValue::Integer(0));
    assert_scalar(&db, "SELECT (SELECT 1, 2, 3) != (SELECT 1, 2, 3)", SqlValue::Integer(0));
    assert_scalar(&db, "SELECT (SELECT 1, 0, 3) != (SELECT 1, 2, 3)", SqlValue::Integer(1));
}

#[test]
fn two_multicolumn_subqueries_ordering() {
    let db = vibesql_storage::Database::new();
    assert_scalar(&db, "SELECT (SELECT 1, 1, 3) < (SELECT 1, 2, 3)", SqlValue::Integer(1));
    assert_scalar(&db, "SELECT (SELECT 1, 3, 3) < (SELECT 1, 2, 3)", SqlValue::Integer(0));
    assert_scalar(&db, "SELECT (SELECT 1, 2, 3) <= (SELECT 1, 2, 3)", SqlValue::Integer(1));
    assert_scalar(&db, "SELECT (SELECT 1, 3, 3) >= (SELECT 1, 2, 3)", SqlValue::Integer(1));
}

#[test]
fn single_column_subquery_comparison_still_scalar() {
    // Regression: two single-column subqueries must remain a scalar comparison.
    let db = vibesql_storage::Database::new();
    assert_scalar(&db, "SELECT (SELECT 5) > (SELECT 3)", SqlValue::Integer(1));
    assert_scalar(&db, "SELECT (SELECT 3) > (SELECT 5)", SqlValue::Integer(0));
    assert_scalar(&db, "SELECT (SELECT 5) == (SELECT 5)", SqlValue::Integer(1));
}

// ---------------------------------------------------------------------------
// Three-valued NULL semantics
// ---------------------------------------------------------------------------

#[test]
fn null_tuple_comparison_is_unknown() {
    let db = vibesql_storage::Database::new();
    // (1, NULL) = (1, 2): first pair equal, second is UNKNOWN → UNKNOWN (NULL).
    assert_scalar(&db, "SELECT (SELECT 1, NULL) == (SELECT 1, 2)", SqlValue::Null);
    // (2, NULL) = (1, 2): first pair definitively unequal → FALSE.
    assert_scalar(&db, "SELECT (SELECT 2, NULL) == (SELECT 1, 2)", SqlValue::Integer(0));
    // Ordering: (1, NULL) < (2, NULL): first pair 1<2 is a definite result → TRUE.
    assert_scalar(&db, "SELECT (SELECT 1, NULL) < (SELECT 2, NULL)", SqlValue::Integer(1));
    // (1, NULL) < (1, 2): first equal, second NULL before a difference → UNKNOWN.
    assert_scalar(&db, "SELECT (SELECT 1, NULL) < (SELECT 1, 2)", SqlValue::Null);
}

// ---------------------------------------------------------------------------
// Phase 3: type affinity inside row-value comparison
// ---------------------------------------------------------------------------

/// r1/r2 mirror rowvalue2.test section 5: mixed TEXT and INTEGER columns whose
/// row-value comparison must apply the same affinity as the scalar form.
fn db_with_affinity_tables() -> vibesql_storage::Database {
    let mut db = vibesql_storage::Database::new();
    run_stmt(&mut db, "CREATE TABLE r1(a TEXT, iB TEXT)");
    run_stmt(&mut db, "CREATE TABLE r2(x TEXT, zY INTEGER)");
    run_stmt(&mut db, "INSERT INTO r1 VALUES(35, 35)");
    run_stmt(&mut db, "INSERT INTO r2 VALUES(35, 36)");
    run_stmt(&mut db, "INSERT INTO r2 VALUES(35, 4)");
    run_stmt(&mut db, "INSERT INTO r2 VALUES(35, 35)");
    db
}

#[test]
fn row_value_comparison_applies_affinity() {
    let db = db_with_affinity_tables();
    // The row-value form must agree with the scalar-expanded form. zY has
    // INTEGER affinity, iB has TEXT affinity: (x, zY) == (a, iB) matches the
    // r2 row where zY == 35 after affinity coercion, exactly like
    // (x == a) AND (zY == iB).
    let row_value = "SELECT zY FROM r1, r2 WHERE (x, zY) == (a, iB) ORDER BY zY";
    let scalar = "SELECT zY FROM r1, r2 WHERE (x == a) AND (zY == iB) ORDER BY zY";
    let rv = query_column(&db, row_value);
    assert_eq!(rv, query_column(&db, scalar));
    assert_eq!(rv, vec![SqlValue::Integer(35)]);
}

// ---------------------------------------------------------------------------
// Arity mismatch errors (SQLite-compatible)
// ---------------------------------------------------------------------------

#[test]
fn arity_mismatch_between_tuple_and_subquery_errors() {
    let db = db_with_t3();
    let stmt = vibesql_parser::Parser::parse_sql(
        "SELECT w FROM t3 WHERE (a, b) > (SELECT a, b, c FROM t3 WHERE w = 'airfare')",
    )
    .unwrap();
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let executor = SelectExecutor::new(&db);
        let result = executor.execute(&select_stmt);
        assert!(result.is_err(), "expected arity-mismatch error, got {:?}", result);
    } else {
        panic!("expected SELECT");
    }
}
