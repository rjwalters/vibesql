//! Regression tests for CTE scoping in nested subqueries (issue #6189).
//!
//! Two distinct CTE-scoping gaps surfaced by the SQLite `with*.test`
//! conformance fixtures, both fixed together because they share a root cause —
//! a nested query's CTE scope being computed without the enclosing scope:
//!
//! 1. A `WITH` clause declared *inside* an `IN` / `EXISTS` subquery was dropped by the
//!    IN/EXISTS→semi/anti-join optimizer, which treated the subquery's CTE name as a real base
//!    table and produced a bogus "no such table" (with2.test 7.5: `... WHERE y IN (WITH ss(x) AS
//!    (VALUES(7) UNION ALL SELECT x+7 FROM ss WHERE x<49) SELECT x FROM ss)`). The fix bails the
//!    transform when the subquery carries its own `WITH`, falling back to row-by-row evaluation
//!    which materializes it correctly.
//!
//! 2. A subquery that declares its own `WITH` could not reference a CTE from the enclosing query,
//!    because the inner CTE scope was seeded with an empty outer scope (with3.test 2.1: `WITH x1(a)
//!    AS (VALUES(100)) INSERT INTO t1(x) SELECT * FROM (WITH x2(y) AS (SELECT * FROM x1) SELECT y+a
//!    FROM x1, x2)`). The fix threads the enclosing CTE scope into the inner WITH execution; local
//!    names still shadow outer names.
//!
//! All expected values below were verified against sqlite3.

use vibesql_executor::{InsertExecutor, SelectExecutor};
use vibesql_types::SqlValue;

fn run_stmt(db: &mut vibesql_storage::Database, sql: &str) {
    let stmt = vibesql_parser::Parser::parse_sql(sql)
        .unwrap_or_else(|e| panic!("Parse failed: {} -- {:?}", sql, e));
    match stmt {
        vibesql_ast::Statement::CreateTable(create_table) => {
            vibesql_executor::CreateTableExecutor::execute(&create_table, db).unwrap();
        }
        vibesql_ast::Statement::Insert(insert) => {
            InsertExecutor::execute(db, &insert)
                .unwrap_or_else(|e| panic!("Insert failed: {} -- {:?}", sql, e));
        }
        other => panic!("Unsupported statement in test setup: {:?}", other),
    }
}

fn query(db: &vibesql_storage::Database, sql: &str) -> Vec<Vec<SqlValue>> {
    let stmt = vibesql_parser::Parser::parse_sql(sql)
        .unwrap_or_else(|e| panic!("Parse failed: {} -- {:?}", sql, e));
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let executor = SelectExecutor::new(db);
        executor
            .execute(&select_stmt)
            .unwrap_or_else(|e| panic!("Query failed: {} -- {:?}", sql, e))
            .into_iter()
            .map(|row| row.values.to_vec())
            .collect()
    } else {
        panic!("Expected SELECT statement: {}", sql);
    }
}

/// with2.test 7.5: a recursive CTE declared inside an `IN` subquery.
/// sqlite3 returns the rows of t6 whose y is a multiple of 7 present in the CTE.
#[test]
fn test_recursive_cte_in_in_subquery() {
    let mut db = vibesql_storage::Database::new();
    run_stmt(&mut db, "CREATE TABLE t6(y)");
    run_stmt(&mut db, "INSERT INTO t6 VALUES(14),(28),(42),(15)");
    let rows = query(
        &db,
        "SELECT y FROM t6 WHERE y IN ( \
           WITH ss(x) AS ( VALUES(7) UNION ALL SELECT x+7 FROM ss WHERE x<49 ) \
           SELECT x FROM ss \
         ) ORDER BY y",
    );
    assert_eq!(
        rows,
        vec![vec![SqlValue::Integer(14)], vec![SqlValue::Integer(28)], vec![SqlValue::Integer(42)],]
    );
}

/// A non-recursive `WITH` inside an `IN` subquery (simpler variant of the same
/// optimizer bug: the CTE name must not be read as a base table).
#[test]
fn test_non_recursive_cte_in_in_subquery() {
    let mut db = vibesql_storage::Database::new();
    run_stmt(&mut db, "CREATE TABLE t6(y)");
    run_stmt(&mut db, "INSERT INTO t6 VALUES(14),(28),(42),(15)");
    let rows = query(
        &db,
        "SELECT y FROM t6 WHERE y IN ( \
           WITH ss(x) AS (VALUES(14),(28)) SELECT x FROM ss \
         ) ORDER BY y",
    );
    assert_eq!(rows, vec![vec![SqlValue::Integer(14)], vec![SqlValue::Integer(28)]]);
}

/// A `WITH` inside an `EXISTS` subquery — the EXISTS→semi-join transform must
/// likewise bail so the subquery's WITH clause is honored.
#[test]
fn test_cte_in_exists_subquery() {
    let mut db = vibesql_storage::Database::new();
    run_stmt(&mut db, "CREATE TABLE t6(y)");
    run_stmt(&mut db, "INSERT INTO t6 VALUES(14),(28)");
    let rows = query(
        &db,
        "SELECT y FROM t6 WHERE EXISTS ( \
           WITH ss(x) AS (VALUES(14)) SELECT x FROM ss WHERE x = t6.y \
         )",
    );
    assert_eq!(rows, vec![vec![SqlValue::Integer(14)]]);
}

/// A normal `IN` subquery (no inner WITH) must still work — guards against the
/// bail-out being over-broad and disabling the semi-join transform everywhere.
#[test]
fn test_plain_in_subquery_still_works() {
    let mut db = vibesql_storage::Database::new();
    run_stmt(&mut db, "CREATE TABLE t6(y)");
    run_stmt(&mut db, "INSERT INTO t6 VALUES(1),(2),(3),(4)");
    run_stmt(&mut db, "CREATE TABLE t7(z)");
    run_stmt(&mut db, "INSERT INTO t7 VALUES(2),(4)");
    let rows = query(&db, "SELECT y FROM t6 WHERE y IN (SELECT z FROM t7) ORDER BY y");
    assert_eq!(rows, vec![vec![SqlValue::Integer(2)], vec![SqlValue::Integer(4)]]);
}

/// with3.test 2.1: a derived-table subquery that declares its own `WITH x2`
/// still references the enclosing `WITH x1`. sqlite3: 200.
#[test]
fn test_nested_with_references_outer_cte_in_derived_table() {
    let mut db = vibesql_storage::Database::new();
    run_stmt(&mut db, "CREATE TABLE t1(x)");
    run_stmt(
        &mut db,
        "WITH x1(a) AS (VALUES(100)) \
         INSERT INTO t1(x) \
           SELECT * FROM (WITH x2(y) AS (SELECT * FROM x1) SELECT y+a FROM x1, x2)",
    );
    assert_eq!(query(&db, "SELECT x FROM t1"), vec![vec![SqlValue::Integer(200)]]);
}

/// Plain-SELECT form of the same outer-CTE-into-nested-WITH scoping fix.
#[test]
fn test_nested_with_references_outer_cte_plain_select() {
    let db = vibesql_storage::Database::new();
    let rows = query(
        &db,
        "WITH x1(a) AS (VALUES(100)) \
         SELECT * FROM (WITH x2(y) AS (SELECT a FROM x1) SELECT y FROM x2)",
    );
    assert_eq!(rows, vec![vec![SqlValue::Integer(100)]]);
}

/// A nested `WITH` name shadows a same-named outer CTE (local precedence must be
/// preserved even though the outer scope is now visible).
#[test]
fn test_nested_with_shadows_outer_cte() {
    let db = vibesql_storage::Database::new();
    let rows = query(
        &db,
        "WITH c(a) AS (VALUES(1)) \
         SELECT * FROM (WITH c(a) AS (VALUES(99)) SELECT a FROM c)",
    );
    assert_eq!(rows, vec![vec![SqlValue::Integer(99)]]);
}
