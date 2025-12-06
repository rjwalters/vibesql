//! Tests for outer join predicate pushdown (issue #3773)
//!
//! This module tests the anti-join pattern: LEFT JOIN ... WHERE right.col IS NULL
//! which finds rows from the left table that have no matching row in the right table.
//!
//! The bug was that predicates like `t2.tid IS NULL` were incorrectly pushed down
//! to the right-side scan, filtering on stored NULLs instead of join-produced NULLs.

use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;
use vibesql_storage::{Database, Row};
use vibesql_types::{DataType, SqlValue};

/// Helper function to parse SELECT statements
fn parse_select(sql: &str) -> vibesql_ast::SelectStmt {
    match Parser::parse_sql(sql) {
        Ok(vibesql_ast::Statement::Select(select_stmt)) => *select_stmt,
        _ => panic!("Failed to parse SELECT statement: {}", sql),
    }
}

/// Create a test database with two tables for anti-join testing
fn setup_test_db() -> Database {
    let mut db = Database::new();

    // Create t1 table with 3 rows
    let t1_schema = TableSchema::with_primary_key(
        "T1".to_string(),
        vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
        vec!["id".to_string()],
    );
    db.create_table(t1_schema).unwrap();
    db.insert_row("T1", Row::new(vec![SqlValue::Integer(1)])).unwrap();
    db.insert_row("T1", Row::new(vec![SqlValue::Integer(2)])).unwrap();
    db.insert_row("T1", Row::new(vec![SqlValue::Integer(3)])).unwrap();

    // Create t2 table with 1 row (matches t1.id = 2)
    let t2_schema = TableSchema::new(
        "T2".to_string(),
        vec![ColumnSchema::new("tid".to_string(), DataType::Integer, true)],
    );
    db.create_table(t2_schema).unwrap();
    db.insert_row("T2", Row::new(vec![SqlValue::Integer(2)])).unwrap();

    db
}

/// Helper to extract integer values from result rows
fn extract_integers(result: &[Row]) -> Vec<i64> {
    result
        .iter()
        .map(|row| match &row.values[0] {
            SqlValue::Integer(i) => *i,
            _ => panic!("Expected integer"),
        })
        .collect()
}

/// Test 1: LEFT JOIN anti-join pattern (WHERE right IS NULL)
/// Should return rows 1 and 3 (rows with no match in t2)
#[test]
fn test_left_join_anti_join_pattern() {
    let db = setup_test_db();

    let sql = "SELECT t1.id FROM t1 LEFT JOIN t2 ON t1.id = t2.tid WHERE t2.tid IS NULL ORDER BY t1.id";
    let select = parse_select(sql);
    let result = SelectExecutor::new(&db).execute(&select).unwrap();

    let values = extract_integers(&result);

    assert_eq!(values, vec![1, 3], "Anti-join should return rows with no match");
}

/// Test 2: LEFT JOIN with IS NOT NULL (should return matching rows)
#[test]
fn test_left_join_is_not_null() {
    let db = setup_test_db();

    let sql =
        "SELECT t1.id FROM t1 LEFT JOIN t2 ON t1.id = t2.tid WHERE t2.tid IS NOT NULL ORDER BY t1.id";
    let select = parse_select(sql);
    let result = SelectExecutor::new(&db).execute(&select).unwrap();

    let values = extract_integers(&result);

    assert_eq!(values, vec![2], "Should return only matching row");
}

/// Test 3: LEFT JOIN without filter (all rows)
#[test]
fn test_left_join_no_filter() {
    let db = setup_test_db();

    let sql = "SELECT t1.id FROM t1 LEFT JOIN t2 ON t1.id = t2.tid ORDER BY t1.id";
    let select = parse_select(sql);
    let result = SelectExecutor::new(&db).execute(&select).unwrap();

    let values = extract_integers(&result);

    assert_eq!(values, vec![1, 2, 3], "LEFT JOIN should return all left rows");
}

/// Test 4: Compound predicate with left and right side predicates
#[test]
fn test_left_join_compound_predicate() {
    let db = setup_test_db();

    let sql = "SELECT t1.id FROM t1 LEFT JOIN t2 ON t1.id = t2.tid WHERE t1.id > 0 AND t2.tid IS NULL ORDER BY t1.id";
    let select = parse_select(sql);
    let result = SelectExecutor::new(&db).execute(&select).unwrap();

    let values = extract_integers(&result);

    assert_eq!(values, vec![1, 3], "Compound predicate should work correctly");
}

/// Test 5: NOT EXISTS pattern (equivalent to anti-join)
#[test]
fn test_not_exists_pattern() {
    let db = setup_test_db();

    let sql = "SELECT t1.id FROM t1 WHERE NOT EXISTS (SELECT 1 FROM t2 WHERE t2.tid = t1.id) ORDER BY t1.id";
    let select = parse_select(sql);
    let result = SelectExecutor::new(&db).execute(&select).unwrap();

    let values = extract_integers(&result);

    assert_eq!(values, vec![1, 3], "NOT EXISTS should match anti-join result");
}

/// Test 6: INNER JOIN should still work correctly
#[test]
fn test_inner_join_still_works() {
    let db = setup_test_db();

    let sql = "SELECT t1.id FROM t1 INNER JOIN t2 ON t1.id = t2.tid ORDER BY t1.id";
    let select = parse_select(sql);
    let result = SelectExecutor::new(&db).execute(&select).unwrap();

    let values = extract_integers(&result);

    assert_eq!(values, vec![2], "INNER JOIN should return matching rows");
}

/// Test 7: LEFT JOIN with left-side predicate only
#[test]
fn test_left_join_left_side_predicate() {
    let db = setup_test_db();

    let sql = "SELECT t1.id FROM t1 LEFT JOIN t2 ON t1.id = t2.tid WHERE t1.id > 1 ORDER BY t1.id";
    let select = parse_select(sql);
    let result = SelectExecutor::new(&db).execute(&select).unwrap();

    let values = extract_integers(&result);

    assert_eq!(values, vec![2, 3], "Left-side predicate should filter correctly");
}
