//! Regression tests for the last two window/FILTER-family residuals of
//! issue #6191 (filter1.test 6.3, window1.test 67.1).
//!
//! Covers:
//! - filter1.test 6.3: a FROM-bearing scalar subquery whose aggregate argument resolves to an
//!   *outer* column (not one of its own FROM's columns) hoists the aggregate to the outer query,
//!   computed over all outer rows — not just the first/representative row.
//! - window1.test 67.1 / 67.2: name resolution of a subquery nested inside a compound query's ORDER
//!   BY term happens before the "does not match any column in the result set" fallback, so a
//!   missing table surfaces as `no such table: ...` instead of being masked by that fallback error.

use vibesql_ast::Statement;
use vibesql_parser::Parser;
use vibesql_types::SqlValue;

use super::super::*;

fn execute_sql(
    db: &mut vibesql_storage::Database,
    sql: &str,
) -> Result<Vec<vibesql_storage::Row>, ExecutorError> {
    let stmt = Parser::parse_sql(sql).map_err(|e| ExecutorError::ParseError(format!("{:?}", e)))?;
    match stmt {
        Statement::CreateTable(create_stmt) => {
            CreateTableExecutor::execute(&create_stmt, db)?;
            Ok(vec![])
        }
        Statement::Insert(insert_stmt) => {
            InsertExecutor::execute(db, &insert_stmt)?;
            Ok(vec![])
        }
        Statement::Select(select_stmt) => Ok(SelectExecutor::new(db).execute(&select_stmt)?),
        other => {
            Err(ExecutorError::UnsupportedFeature(format!("Unsupported statement: {:?}", other)))
        }
    }
}

fn single_value(db: &mut vibesql_storage::Database, sql: &str) -> SqlValue {
    let rows = execute_sql(db, sql).unwrap();
    assert_eq!(rows.len(), 1, "expected one row from {sql}");
    rows[0].values[0].clone()
}

// ------------------------------------------------------------------------
// filter1.test 6.3 — outer-correlated aggregate inside a FROM-bearing
// scalar subquery
// ------------------------------------------------------------------------

#[test]
fn test_from_bearing_scalar_subquery_aggregate_hoists_to_outer_query() {
    let mut db = vibesql_storage::Database::new();
    execute_sql(&mut db, "CREATE TABLE t1(a,b)").unwrap();
    execute_sql(&mut db, "INSERT INTO t1 VALUES(1,1)").unwrap();
    execute_sql(&mut db, "INSERT INTO t1 VALUES(2,2)").unwrap();
    execute_sql(&mut db, "CREATE TABLE t2(x,y)").unwrap();
    execute_sql(&mut db, "INSERT INTO t2 VALUES(1,1)").unwrap();

    // `a` is not a column of `t2`, so SQLite associates COUNT(a) with the
    // outer query t1, collapsing the whole statement to a single-row
    // aggregate computed over all of t1's rows (both non-NULL -> 2), not
    // just the representative/first outer row (which would wrongly give 1).
    assert_eq!(
        single_value(&mut db, "SELECT (SELECT COUNT(a) FROM t2) FROM t1"),
        SqlValue::Integer(2)
    );
}

#[test]
fn test_from_bearing_scalar_subquery_aggregate_still_correlated_when_column_is_inner() {
    let mut db = vibesql_storage::Database::new();
    execute_sql(&mut db, "CREATE TABLE t1(a,b)").unwrap();
    execute_sql(&mut db, "INSERT INTO t1 VALUES(1,1)").unwrap();
    execute_sql(&mut db, "INSERT INTO t1 VALUES(2,2)").unwrap();
    execute_sql(&mut db, "CREATE TABLE t2(x,y)").unwrap();
    execute_sql(&mut db, "INSERT INTO t2 VALUES(1,1)").unwrap();

    // filter1.test 6.1: FILTER references `x`, an inner (t2) column, so the
    // aggregate stays a genuinely correlated per-outer-row subquery and the
    // outer query does NOT collapse: one output row per t1 row.
    let rows =
        execute_sql(&mut db, "SELECT (SELECT COUNT(a) FILTER(WHERE x) FROM t2) FROM t1").unwrap();
    assert_eq!(rows.len(), 2, "outer query must not collapse when the aggregate is correlated");
    assert_eq!(rows[0].values[0], SqlValue::Integer(1));
    assert_eq!(rows[1].values[0], SqlValue::Integer(1));
}

// ------------------------------------------------------------------------
// window1.test 67.1 / 67.2 — ORDER BY name resolution precedes the
// "does not match any column" fallback for compound queries
// ------------------------------------------------------------------------

#[test]
fn test_compound_order_by_missing_table_in_window_order_by_subquery_reports_table_not_found() {
    let mut db = vibesql_storage::Database::new();
    execute_sql(&mut db, "CREATE TABLE t1(a, b, c)").unwrap();

    let err = execute_sql(
        &mut db,
        "SELECT a,c,b FROM t1 INTERSECT SELECT a,b,c FROM t1 ORDER BY ( \
             SELECT nth_value(a,2) OVER w1 \
             WINDOW w1 AS ( ORDER BY ((SELECT 1 FROM v1)) ) \
         )",
    )
    .unwrap_err();

    assert!(
        matches!(err, ExecutorError::TableNotFound(ref name) if name.eq_ignore_ascii_case("v1")),
        "expected TableNotFound(\"v1\"), got {err:?}"
    );
}

#[test]
fn test_compound_order_by_existing_table_still_reports_not_in_result_set() {
    let mut db = vibesql_storage::Database::new();
    execute_sql(&mut db, "CREATE TABLE t1(a, b, c)").unwrap();
    execute_sql(&mut db, "CREATE TABLE t2(a, b, c)").unwrap();

    // Same shape as the previous test, but the nested table (t2) exists, so
    // name resolution succeeds and the ORDER BY term is correctly rejected
    // for not matching any result-set column instead.
    let err = execute_sql(
        &mut db,
        "SELECT a,c,b FROM t1 INTERSECT SELECT a,b,c FROM t1 ORDER BY ( \
             SELECT nth_value(a,2) OVER w1 \
             WINDOW w1 AS ( ORDER BY ((SELECT 1 FROM t2)) ) \
         )",
    )
    .unwrap_err();

    assert!(
        matches!(err, ExecutorError::OrderByTermNotInResultSet { term_position: 1 }),
        "expected OrderByTermNotInResultSet, got {err:?}"
    );
}
