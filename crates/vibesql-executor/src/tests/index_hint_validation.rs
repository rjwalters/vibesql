//! Tests for SQLite index hint validation (issue #5235)
//!
//! `INDEXED BY <name>` must name an index that exists on that specific
//! table, otherwise SQLite fails at prepare time with `no such index: <name>`.
//! `NOT INDEXED` never errors. VibeSQL validates the hint and then ignores it
//! for planning (PR #5234).

use vibesql_ast::{IndexColumn, OrderDirection};
use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_parser::Parser;
use vibesql_storage::{Database, Row};
use vibesql_types::{DataType, SqlValue};

use crate::{errors::ExecutorError, select::SelectExecutor};

/// Create a test database with two tables (t1, t2) and an index on each.
///
/// - `t1(x INTEGER, y INTEGER)` with index `t1x` on `x`
/// - `t2(a INTEGER)` with index `t2a` on `a`
fn create_test_db() -> Database {
    let mut db = Database::new();
    db.catalog.set_case_sensitive_identifiers(false);

    let t1 = TableSchema::new(
        "t1".to_string(),
        vec![
            ColumnSchema::new("x".to_string(), DataType::Integer, false),
            ColumnSchema::new("y".to_string(), DataType::Integer, true),
        ],
    );
    db.create_table(t1).unwrap();
    db.insert_row("t1", Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(10)])).unwrap();
    db.insert_row("t1", Row::new(vec![SqlValue::Integer(2), SqlValue::Integer(20)])).unwrap();

    let t2 = TableSchema::new(
        "t2".to_string(),
        vec![ColumnSchema::new("a".to_string(), DataType::Integer, false)],
    );
    db.create_table(t2).unwrap();
    db.insert_row("t2", Row::new(vec![SqlValue::Integer(1)])).unwrap();

    db.create_index(
        "t1x".to_string(),
        "t1".to_string(),
        false,
        vec![IndexColumn::Column {
            column_name: "x".to_string(),
            prefix_length: None,
            direction: OrderDirection::Asc,
        }],
    )
    .unwrap();

    db.create_index(
        "t2a".to_string(),
        "t2".to_string(),
        false,
        vec![IndexColumn::Column {
            column_name: "a".to_string(),
            prefix_length: None,
            direction: OrderDirection::Asc,
        }],
    )
    .unwrap();

    db
}

/// Execute a SELECT statement and return the result.
fn run_select(db: &Database, sql: &str) -> Result<Vec<vibesql_storage::Row>, ExecutorError> {
    let stmt = Parser::parse_sql(sql).unwrap_or_else(|e| panic!("parse failed for {sql}: {e:?}"));
    match stmt {
        vibesql_ast::Statement::Select(select) => SelectExecutor::new(db).execute(&select),
        other => panic!("Expected SELECT statement, got {other:?}"),
    }
}

/// Assert that a query fails with exactly `no such index: <name>`.
fn assert_no_such_index(db: &Database, sql: &str, index_name: &str) {
    let result = run_select(db, sql);
    match result {
        Err(ExecutorError::NoSuchIndex { index_name: name }) => {
            assert_eq!(name, index_name);
        }
        other => panic!("Expected NoSuchIndex error for {sql}, got {other:?}"),
    }
}

#[test]
fn test_indexed_by_valid_index_succeeds() {
    let db = create_test_db();
    let rows = run_select(&db, "SELECT * FROM t1 INDEXED BY t1x WHERE x = 1").unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values[0], SqlValue::Integer(1));
}

#[test]
fn test_indexed_by_valid_index_unchanged_results() {
    let db = create_test_db();
    let with_hint = run_select(&db, "SELECT x, y FROM t1 INDEXED BY t1x ORDER BY x").unwrap();
    let without_hint = run_select(&db, "SELECT x, y FROM t1 ORDER BY x").unwrap();
    assert_eq!(with_hint, without_hint);
}

#[test]
fn test_indexed_by_nonexistent_index_errors() {
    let db = create_test_db();
    assert_no_such_index(&db, "SELECT * FROM t1 INDEXED BY no_such_index", "no_such_index");
}

#[test]
fn test_no_such_index_error_message_format() {
    let db = create_test_db();
    let err = run_select(&db, "SELECT * FROM t1 INDEXED BY nope").unwrap_err();
    assert_eq!(err.to_string(), "no such index: nope");
}

#[test]
fn test_indexed_by_index_on_wrong_table_errors() {
    let db = create_test_db();
    // t2a exists, but on t2 - not on t1
    assert_no_such_index(&db, "SELECT * FROM t1 INDEXED BY t2a", "t2a");
}

#[test]
fn test_not_indexed_never_errors() {
    let db = create_test_db();
    let rows = run_select(&db, "SELECT * FROM t1 NOT INDEXED").unwrap();
    assert_eq!(rows.len(), 2);
}

#[test]
fn test_indexed_by_case_insensitive_index_name() {
    let db = create_test_db();
    let rows = run_select(&db, "SELECT * FROM t1 INDEXED BY T1X WHERE x = 2").unwrap();
    assert_eq!(rows.len(), 1);
}

#[test]
fn test_indexed_by_with_table_alias() {
    let db = create_test_db();
    let rows = run_select(&db, "SELECT a.x FROM t1 AS a INDEXED BY t1x WHERE a.x = 1").unwrap();
    assert_eq!(rows.len(), 1);
}

#[test]
fn test_indexed_by_inside_join_branch_errors() {
    let db = create_test_db();
    assert_no_such_index(
        &db,
        "SELECT * FROM t1 JOIN t2 INDEXED BY missing_idx ON t1.x = t2.a",
        "missing_idx",
    );
}

#[test]
fn test_indexed_by_inside_join_branch_valid_succeeds() {
    let db = create_test_db();
    let rows = run_select(&db, "SELECT * FROM t1 JOIN t2 INDEXED BY t2a ON t1.x = t2.a").unwrap();
    assert_eq!(rows.len(), 1);
}

#[test]
fn test_indexed_by_on_cte_name_errors() {
    let db = create_test_db();
    // c is a CTE, not a real table; no index can belong to it.
    // SQLite: "no such index: t1x" (the hint cannot apply to a CTE).
    assert_no_such_index(&db, "WITH c AS (SELECT * FROM t1) SELECT * FROM c INDEXED BY t1x", "t1x");
}

#[test]
fn test_indexed_by_inside_cte_body_is_validated() {
    let db = create_test_db();
    assert_no_such_index(
        &db,
        "WITH c AS (SELECT * FROM t1 INDEXED BY missing_idx) SELECT * FROM c",
        "missing_idx",
    );
}

#[test]
fn test_indexed_by_in_union_arm_errors() {
    let db = create_test_db();
    assert_no_such_index(
        &db,
        "SELECT x FROM t1 UNION ALL SELECT a FROM t2 INDEXED BY missing_idx",
        "missing_idx",
    );
}

#[test]
fn test_indexed_by_in_from_subquery_errors() {
    let db = create_test_db();
    assert_no_such_index(
        &db,
        "SELECT * FROM (SELECT * FROM t1 INDEXED BY missing_idx) AS sub",
        "missing_idx",
    );
}

// --- CTE-name scope threaded into nested validation scopes (issue #5243) ---
//
// A CTE shadows any same-named real table, so `INDEXED BY` on a CTE
// reference must error even when the reference appears in a nested scope
// (derived-table subquery, set-operation arm, or a later CTE body). All SQL
// below was verified against sqlite3.

#[test]
fn test_indexed_by_on_outer_cte_inside_subquery_errors() {
    let db = create_test_db();
    // The inner t1 resolves to the CTE, which can't carry an index hint.
    // SQLite: no such index: t1x
    assert_no_such_index(
        &db,
        "WITH t1 AS (SELECT 1 AS x) SELECT * FROM (SELECT * FROM t1 INDEXED BY t1x) sub",
        "t1x",
    );
}

#[test]
fn test_indexed_by_on_outer_cte_in_union_arm_errors() {
    let db = create_test_db();
    // SQLite: no such index: t1x
    assert_no_such_index(
        &db,
        "WITH t1 AS (SELECT 1 AS x) SELECT * FROM t1 UNION SELECT * FROM t1 INDEXED BY t1x",
        "t1x",
    );
}

#[test]
fn test_indexed_by_on_earlier_cte_in_later_cte_body_errors() {
    let db = create_test_db();
    // SQLite: no such index: t1x
    assert_no_such_index(
        &db,
        "WITH t1 AS (SELECT 1 AS x), b AS (SELECT * FROM t1 INDEXED BY t1x) SELECT * FROM b",
        "t1x",
    );
}

#[test]
fn test_indexed_by_on_outer_cte_in_join_inside_subquery_errors() {
    let db = create_test_db();
    // CTE scope must thread through Join arms inside the subquery.
    // SQLite: no such index: t1x
    assert_no_such_index(
        &db,
        "WITH t1 AS (SELECT 1 AS x) SELECT * FROM (SELECT * FROM t2 JOIN t1 INDEXED BY t1x ON 1=1) sub",
        "t1x",
    );
}

#[test]
fn test_indexed_by_self_reference_in_cte_body_errors() {
    let db = create_test_db();
    // SQLite errors with "circular reference: t1"; any error beats silently
    // succeeding, so VibeSQL reports NoSuchIndex (message parity out of scope).
    assert_no_such_index(
        &db,
        "WITH t1 AS (SELECT * FROM t1 INDEXED BY t1x) SELECT * FROM t1",
        "t1x",
    );
}

#[test]
fn test_indexed_by_real_table_in_subquery_with_unrelated_cte_succeeds() {
    let db = create_test_db();
    // Negative control: an unrelated CTE in scope must not poison a valid
    // hint on a real table inside a nested scope. SQLite: succeeds.
    let rows = run_select(
        &db,
        "WITH q AS (SELECT 1 AS z) SELECT * FROM (SELECT * FROM t2 INDEXED BY t2a) sub",
    )
    .unwrap();
    assert_eq!(rows.len(), 1);
}

#[test]
fn test_indexed_by_real_table_with_inner_with_in_subquery_succeeds() {
    let db = create_test_db();
    // Negative control: an inner WITH inside a derived table doesn't poison
    // real-table hints. SQLite: succeeds.
    let rows = run_select(
        &db,
        "SELECT * FROM (WITH q AS (SELECT 1 AS z) SELECT * FROM t1 INDEXED BY t1x)",
    )
    .unwrap();
    assert_eq!(rows.len(), 2);
}
