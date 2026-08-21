//! Regression tests for issue #6306.
//!
//! A FROM-less outer query's SELECT-list alias (`SELECT 1 AS c`) should be
//! resolvable from a correlated subquery nested inside its own WHERE clause,
//! the same way a real FROM-clause row's column would be. Two independent
//! gaps combined to break this:
//!
//! 1. **Routing** (`without_from.rs`): the FROM-less WHERE-clause routing decision only diverted to
//!    the alias-binding execution path when it found a *direct* column reference in WHERE. A column
//!    reference living only inside a nested subquery (`WHERE (SELECT c)`) was invisible to that
//!    check, so the query fell through to the plain path with no outer context at all, producing
//!    `no such column: c`.
//! 2. **Correlation detection** (`correlation.rs`): `is_select_stmt_correlated_impl` never scanned
//!    a standalone-VALUES subquery body (`stmt.values`), so `(SELECT (VALUES(c)))` was
//!    misclassified as non-correlated, executed with no outer context, and (unsafely) cached.
//!
//! Expected values verified against sqlite3 3.51 (see issue #6306).

use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;
use vibesql_storage::{Database, Row};
use vibesql_types::SqlValue;

fn parse_select(sql: &str) -> vibesql_ast::SelectStmt {
    match Parser::parse_sql(sql) {
        Ok(vibesql_ast::Statement::Select(select_stmt)) => *select_stmt,
        _ => panic!("Failed to parse SELECT statement: {}", sql),
    }
}

fn run(db: &Database, sql: &str) -> Result<Vec<Row>, vibesql_executor::ExecutorError> {
    let select = parse_select(sql);
    SelectExecutor::new(db).execute(&select)
}

/// One scalar integer from the single result row/column.
fn scalar_int(rows: &[Row]) -> i64 {
    assert_eq!(rows.len(), 1, "expected exactly one result row, got {rows:?}");
    match rows[0].get(0) {
        Some(SqlValue::Integer(n)) => *n,
        Some(SqlValue::Bigint(n)) => *n,
        other => panic!("expected integer at index 0, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Acceptance criteria: the four repro queries from the issue.
// ---------------------------------------------------------------------------

#[test]
fn where_scalar_subquery_references_select_list_alias() {
    let db = Database::new();
    let rows = run(&db, "SELECT 1 AS c WHERE (SELECT c)").unwrap();
    assert_eq!(scalar_int(&rows), 1);
}

#[test]
fn where_doubly_nested_scalar_subquery_references_alias() {
    let db = Database::new();
    let rows = run(&db, "SELECT 1 AS c WHERE (SELECT (SELECT c))").unwrap();
    assert_eq!(scalar_int(&rows), 1);
}

#[test]
fn where_standalone_values_subquery_references_alias() {
    let db = Database::new();
    let rows = run(&db, "SELECT 1 AS c WHERE (SELECT (VALUES(c)))").unwrap();
    assert_eq!(scalar_int(&rows), 1);
}

#[test]
fn where_exists_subquery_references_alias() {
    let db = Database::new();
    let rows = run(&db, "SELECT 1 AS c WHERE EXISTS (SELECT 1 WHERE c)").unwrap();
    assert_eq!(scalar_int(&rows), 1);
}

#[test]
fn where_scalar_subquery_references_multiple_aliases() {
    let db = Database::new();
    let rows = run(&db, "SELECT 1 AS a, 2 AS b WHERE (SELECT a+b)").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get(0), Some(&SqlValue::Integer(1)));
    assert_eq!(rows[0].get(1), Some(&SqlValue::Integer(2)));
}

/// with2.test case 10.1 (CTE wrapper is incidental to the root cause — see
/// issue #6306 discovery context — but keep the exact upstream shape as a
/// conformance regression test).
#[test]
fn with2_test_case_10_1() {
    let db = Database::new();
    let sql = "SELECT 1 AS c WHERE (
        SELECT (
            WITH t1(a) AS (VALUES( c ))
            SELECT ( SELECT t1a.a FROM t1 AS t1a, t1 AS t1x )
            FROM t1 AS xyz GROUP BY 1
        )
    )";
    let rows = run(&db, sql).unwrap();
    assert_eq!(scalar_int(&rows), 1);
}

// ---------------------------------------------------------------------------
// No-regressions: acceptance criteria queries that must keep their existing
// behavior unchanged.
// ---------------------------------------------------------------------------

#[test]
fn constant_where_false_still_returns_empty() {
    let db = Database::new();
    let rows = run(&db, "SELECT 99 WHERE 0").unwrap();
    assert!(rows.is_empty());
}

/// window1.test 15.2 / #5830 truthiness path: a direct column reference to a
/// select-list alias in WHERE must keep routing to (and behaving correctly
/// through) the pre-existing alias-binding path.
#[test]
fn direct_alias_reference_in_where_unchanged() {
    let db = Database::new();
    let rows = run(&db, "SELECT (SELECT '') x WHERE x+x").unwrap();
    assert!(rows.is_empty(), "empty string coerces to 0, so x+x is falsy");
}

/// A SELECT-list subquery referencing an undefined column must still error;
/// this has no WHERE clause at all, so it is untouched by the WHERE-routing
/// change.
#[test]
fn select_list_subquery_missing_column_still_errors() {
    let db = Database::new();
    let err = run(&db, "SELECT (SELECT x)").unwrap_err();
    assert!(
        err.to_string().contains("no such column"),
        "expected a 'no such column' error, got: {err}"
    );
}

/// An uncorrelated subquery with its own FROM clause in WHERE must still
/// evaluate correctly regardless of which routing path handles the query.
#[test]
fn uncorrelated_subquery_with_own_from_in_where_unchanged() {
    let mut db = Database::new();
    let t = vibesql_catalog::TableSchema::new(
        "t".to_string(),
        vec![vibesql_catalog::ColumnSchema::new(
            "x".to_string(),
            vibesql_types::DataType::Integer,
            true,
        )],
    );
    db.create_table(t).unwrap();
    db.insert_row("t", Row::new(vec![SqlValue::Integer(1)])).unwrap();

    let rows = run(&db, "SELECT 1 AS c WHERE (SELECT count(*) FROM t)").unwrap();
    assert_eq!(scalar_int(&rows), 1);
}
