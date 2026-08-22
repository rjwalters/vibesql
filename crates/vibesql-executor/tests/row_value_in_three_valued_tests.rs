//! Regression tests for issue #5268 (and #5232 / PR #5265)
//!
//! Row-value IN/NOT IN must follow SQL three-valued logic:
//!
//! - Per candidate row, the element-wise comparison is TRUE only if all pairs are non-NULL and
//!   equal; FALSE if ANY pair is non-NULL and unequal (three-valued AND short-circuits on FALSE);
//!   UNKNOWN otherwise.
//! - IN is then a three-valued OR over rows: TRUE if any row is TRUE; UNKNOWN if no row is TRUE but
//!   some row is UNKNOWN; FALSE otherwise.
//!
//! PR #5265 fixed this in the combined evaluator (table-backed queries),
//! but FROM-less constant queries route through the simple
//! `ExpressionEvaluator`, which retained the old behavior of forcing NULL
//! whenever a NULL appeared on either side — even for rows that are
//! definitively unequal. Issue #5268 ports the fix to that path.
//!
//! Expected values below were verified against sqlite3.

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

/// Run a SELECT that yields a single scalar value and return it,
/// normalizing Boolean to Integer (SQLite has no boolean storage class;
/// IN results display as 0/1).
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

fn assert_query(db: &vibesql_storage::Database, sql: &str, expected: SqlValue) {
    let actual = query_scalar(db, sql);
    assert_eq!(actual, expected, "Query: {} -- expected {:?}, got {:?}", sql, expected, actual);
}

/// Database with a single-row table so the same predicates can be run
/// through the combined evaluator (table-backed path).
fn db_with_t0() -> vibesql_storage::Database {
    let mut db = vibesql_storage::Database::new();
    run_stmt(&mut db, "CREATE TABLE t0(c0 INTEGER)");
    run_stmt(&mut db, "INSERT INTO t0 VALUES(0)");
    db
}

// ---------------------------------------------------------------------------
// FROM-less constant queries (simple evaluator path) — issue #5268
// ---------------------------------------------------------------------------

#[test]
fn fromless_definitively_unequal_row_is_false() {
    // (NULL, 1) vs (0, 0): 0=1 is definitively FALSE, so the row is FALSE
    // regardless of the NULL element. SQLite: 0.
    let db = vibesql_storage::Database::new();
    assert_query(&db, "SELECT (0,0) IN (SELECT NULL, 1)", SqlValue::Integer(0));
}

#[test]
fn fromless_not_in_definitively_unequal_row_is_true() {
    // NOT IN negation of a definitive FALSE. SQLite: 1.
    let db = vibesql_storage::Database::new();
    assert_query(&db, "SELECT (0,0) NOT IN (SELECT NULL, 1)", SqlValue::Integer(1));
}

#[test]
fn fromless_lhs_null_with_unequal_element_is_false() {
    // LHS has a NULL element, but 0=1 is definitively FALSE, so the row is
    // FALSE — the LHS NULL must not force UNKNOWN. SQLite: 0.
    let db = vibesql_storage::Database::new();
    assert_query(&db, "SELECT (NULL,0) IN (SELECT 1, 1)", SqlValue::Integer(0));
}

#[test]
fn fromless_lhs_null_with_unequal_element_not_in_is_true() {
    let db = vibesql_storage::Database::new();
    assert_query(&db, "SELECT (NULL,0) NOT IN (SELECT 1, 1)", SqlValue::Integer(1));
}

#[test]
fn fromless_unknown_rows_stay_null() {
    // All non-NULL element pairs are equal, but a NULL comparison makes the
    // row UNKNOWN — the IN result is NULL. SQLite: NULL for all three.
    let db = vibesql_storage::Database::new();
    assert_query(&db, "SELECT (0,0) IN (SELECT NULL, 0)", SqlValue::Null);
    assert_query(&db, "SELECT (0,0) IN (SELECT 0, NULL)", SqlValue::Null);
    assert_query(&db, "SELECT (NULL,0) IN (SELECT 1, 0)", SqlValue::Null);
}

#[test]
fn fromless_not_in_unknown_rows_stay_null() {
    // NOT IN negation of UNKNOWN is still UNKNOWN. SQLite: NULL.
    let db = vibesql_storage::Database::new();
    assert_query(&db, "SELECT (0,0) NOT IN (SELECT NULL, 0)", SqlValue::Null);
    assert_query(&db, "SELECT (0,0) NOT IN (SELECT 0, NULL)", SqlValue::Null);
    assert_query(&db, "SELECT (NULL,0) NOT IN (SELECT 1, 0)", SqlValue::Null);
}

#[test]
fn fromless_empty_subquery_is_false() {
    // IN over an empty set is FALSE (even with NULLs on the left);
    // NOT IN is TRUE. SQLite: 0 / 1.
    let db = vibesql_storage::Database::new();
    assert_query(&db, "SELECT (0,0) IN (SELECT NULL, 1 WHERE 0)", SqlValue::Integer(0));
    assert_query(&db, "SELECT (0,0) NOT IN (SELECT NULL, 1 WHERE 0)", SqlValue::Integer(1));
}

#[test]
fn fromless_mixed_false_and_unknown_rows_is_null() {
    // One row definitively FALSE, one row UNKNOWN: three-valued OR over
    // {FALSE, UNKNOWN} is UNKNOWN. SQLite: NULL.
    let db = vibesql_storage::Database::new();
    assert_query(&db, "SELECT (0,0) IN (SELECT NULL, 1 UNION ALL SELECT NULL, 0)", SqlValue::Null);
    assert_query(
        &db,
        "SELECT (0,0) NOT IN (SELECT NULL, 1 UNION ALL SELECT NULL, 0)",
        SqlValue::Null,
    );
}

#[test]
fn fromless_true_row_wins_over_unknown_rows() {
    // A definitive match makes IN TRUE even when other rows are UNKNOWN.
    // SQLite: 1.
    let db = vibesql_storage::Database::new();
    assert_query(
        &db,
        "SELECT (0,0) IN (SELECT 0, 0 UNION ALL SELECT NULL, 0)",
        SqlValue::Integer(1),
    );
    assert_query(&db, "SELECT (0,0) IN (SELECT 0, 0)", SqlValue::Integer(1));
}

// ---------------------------------------------------------------------------
// Table-backed parity (combined evaluator path) — fixed by PR #5265,
// must keep matching the FROM-less results above.
// ---------------------------------------------------------------------------

#[test]
fn table_backed_definitively_unequal_row_is_false() {
    let db = db_with_t0();
    assert_query(&db, "SELECT (0,0) IN (SELECT NULL, 1) FROM t0", SqlValue::Integer(0));
    assert_query(&db, "SELECT (0,0) NOT IN (SELECT NULL, 1) FROM t0", SqlValue::Integer(1));
    assert_query(&db, "SELECT (NULL,0) IN (SELECT 1, 1) FROM t0", SqlValue::Integer(0));
}

#[test]
fn table_backed_unknown_rows_stay_null() {
    let db = db_with_t0();
    assert_query(&db, "SELECT (0,0) IN (SELECT NULL, 0) FROM t0", SqlValue::Null);
    assert_query(&db, "SELECT (0,0) IN (SELECT 0, NULL) FROM t0", SqlValue::Null);
    assert_query(&db, "SELECT (NULL,0) IN (SELECT 1, 0) FROM t0", SqlValue::Null);
}
