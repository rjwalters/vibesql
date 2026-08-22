//! Regression tests for correlated IN-subquery predicate placement
//! (issue #5314, select1-18.1)
//!
//! Predicates containing IN subqueries (or correlated outer references) must
//! not be pushed down to single-table scans during join reordering, because
//! the correlated outer scope is unavailable at scan level. These tests cover
//! the bisection ladder from the issue:
//!
//! - Bug B: `Expression::In` predicates pushed to a single-table scan drop the correlated outer
//!   scope (`graph.rs` now inserts the `__subquery__` marker).
//! - Unresolvable (correlated) unqualified columns in predicates now insert `__outer_ref__` so the
//!   predicate stays post-join.
//! - The complex EXISTS → semi-join transform bails out when the subquery's FROM clause contains
//!   correlated derived tables.
//! - The IN → EXISTS rewrite no longer mis-qualifies the IN's left-hand expression when multiple
//!   outer tables are present.

use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;
use vibesql_storage::Database;

/// Create the select1-18.1 fixture: t1(c) = {123}, t2(x, y) = {(123, NULL)}
fn create_test_database() -> Database {
    let mut db = Database::new();

    for sql in ["CREATE TABLE t1 (c INTEGER)", "CREATE TABLE t2 (x INTEGER, y INTEGER)"] {
        let stmt = Parser::parse_sql(sql).unwrap();
        if let vibesql_ast::Statement::CreateTable(create_table) = stmt {
            vibesql_executor::CreateTableExecutor::execute(&create_table, &mut db).unwrap();
        }
    }

    for sql in [
        "INSERT INTO t1 (c) VALUES (123)",
        "INSERT INTO t2 (x) VALUES (123)",
        "INSERT INTO t2 (x) VALUES (200)",
    ] {
        let stmt = Parser::parse_sql(sql).unwrap();
        if let vibesql_ast::Statement::Insert(insert) = stmt {
            vibesql_executor::InsertExecutor::execute(&mut db, &insert).unwrap();
        }
    }

    db
}

/// Execute a SELECT and return the integer values of the first column
fn select_ints(db: &Database, sql: &str) -> Vec<i64> {
    let stmt = Parser::parse_sql(sql).unwrap();
    let rows = if let vibesql_ast::Statement::Select(select) = stmt {
        SelectExecutor::new(db)
            .execute(&select)
            .unwrap_or_else(|e| panic!("query failed: {e:?}\nsql: {sql}"))
    } else {
        panic!("Expected SELECT statement");
    };
    rows.iter()
        .map(|row| match &row.values[0] {
            vibesql_types::SqlValue::Integer(n) => *n,
            other => panic!("Expected integer, got {other:?}"),
        })
        .collect()
}

/// Bug B minimal repro: the correlated IN subquery references `c` from t1,
/// so the predicate must be evaluated post-join, not at the t2 scan.
/// sqlite3 returns an empty result for the single-row fixture; with x=200
/// added, only 200 satisfies `x IN (SELECT x FROM t2 WHERE x > c)`.
#[test]
fn test_correlated_in_subquery_not_pushed_to_scan() {
    let db = create_test_database();
    let result = select_ints(
        &db,
        "SELECT x FROM t2, t1 WHERE x IN ((SELECT x FROM t2 WHERE x > c)) ORDER BY x",
    );
    assert_eq!(result, vec![200]);
}

/// NOT IN variant of the same shape (matches sqlite3: only 123 qualifies)
#[test]
fn test_correlated_not_in_subquery_not_pushed_to_scan() {
    let db = create_test_database();
    let result = select_ints(
        &db,
        "SELECT x FROM t2, t1 WHERE x NOT IN ((SELECT x FROM t2 WHERE x > c)) ORDER BY x",
    );
    assert_eq!(result, vec![123]);
}

/// Uncorrelated IN subquery still returns correct results (the HashSet fast
/// path / semi-join transform must keep working after the post-join marker)
#[test]
fn test_uncorrelated_in_subquery_still_correct() {
    let db = create_test_database();
    let result = select_ints(
        &db,
        "SELECT x FROM t2, t1 WHERE x IN (SELECT x FROM t2 WHERE x > 150) ORDER BY x",
    );
    assert_eq!(result, vec![200]);
}

/// IN subquery whose FROM clause contains a correlated FROM-less derived
/// table: `x` and `c` inside `(SELECT x IN (c))` resolve to the outer t2/t1.
/// sqlite3 returns 123 then 200 for this fixture.
/// This exercises both the EXISTS-transform bail-out and the outer-ref marker.
#[test]
fn test_in_subquery_with_correlated_derived_table() {
    let db = create_test_database();
    let result = select_ints(
        &db,
        "SELECT x FROM t2, t1 WHERE x IN ((SELECT x FROM (SELECT x IN (c)), t1 WHERE x IN (c))) \
         ORDER BY x",
    );
    assert_eq!(result, vec![123]);
}

/// Multi-level nesting through NOT EXISTS (the issue's M4 / E8 shape):
/// the IN's left-hand `x` lives in a multi-table FROM (t1, t2), so the
/// IN → EXISTS rewrite must not blindly qualify it as `t1.x`.
#[test]
fn test_not_exists_with_nested_correlated_in() {
    let db = create_test_database();
    let result = select_ints(
        &db,
        "SELECT x FROM t2, t1 WHERE NOT EXISTS(SELECT 1 FROM t1, t2 WHERE x IN ((\
           SELECT x FROM (SELECT x FROM t2, t1 WHERE x BETWEEN (\
             SELECT x FROM (SELECT x IN (c)), t1 WHERE x IN (c)\
           ) AND null OR x AND x IN (c)), t1 WHERE x IN (c)\
         ))) ORDER BY x",
    );
    // sqlite3 returns an empty result for this query
    assert_eq!(result, Vec::<i64>::new());
}

/// The full select1-18.1 regression query (SQLite ticket c52b09c7f38903b1311).
/// SQLite returns an empty result set.
#[test]
fn test_select1_18_1_full_query() {
    let mut db = Database::new();

    for sql in ["CREATE TABLE t1 (c INTEGER)", "CREATE TABLE t2 (x INTEGER, y INTEGER)"] {
        let stmt = Parser::parse_sql(sql).unwrap();
        if let vibesql_ast::Statement::CreateTable(create_table) = stmt {
            vibesql_executor::CreateTableExecutor::execute(&create_table, &mut db).unwrap();
        }
    }
    for sql in ["INSERT INTO t1 (c) VALUES (123)", "INSERT INTO t2 (x) VALUES (123)"] {
        let stmt = Parser::parse_sql(sql).unwrap();
        if let vibesql_ast::Statement::Insert(insert) = stmt {
            vibesql_executor::InsertExecutor::execute(&mut db, &insert).unwrap();
        }
    }

    let result = select_ints(
        &db,
        "SELECT x FROM t2, t1 WHERE x BETWEEN c AND null OR x AND \
         x IN ((SELECT x FROM (SELECT x FROM t2, t1 \
         WHERE x BETWEEN (SELECT x FROM (SELECT x COLLATE rtrim \
         FROM t2, t1 WHERE x BETWEEN c AND null \
         OR x AND x IN (c)), t1 WHERE x BETWEEN c AND null \
         OR x AND x IN (c)) AND null \
         OR NOT EXISTS(SELECT -4.81 FROM t1, t2 WHERE x BETWEEN c AND null \
         OR x AND x IN ((SELECT x FROM (SELECT x FROM t2, t1 \
         WHERE x BETWEEN (SELECT x FROM (SELECT x BETWEEN c AND null \
         OR x AND x IN (c)), t1 WHERE x BETWEEN c AND null \
         OR x AND x IN (c)) AND null \
         OR x AND x IN (c)), t1 WHERE x BETWEEN c AND null \
         OR x AND x IN (c)))) AND x IN (c) \
         ), t1 WHERE x BETWEEN c AND null \
         OR x AND x IN (c)))",
    );
    assert_eq!(result, Vec::<i64>::new());
}
