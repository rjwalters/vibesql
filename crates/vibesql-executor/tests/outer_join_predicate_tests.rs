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

    let sql =
        "SELECT t1.id FROM t1 LEFT JOIN t2 ON t1.id = t2.tid WHERE t2.tid IS NULL ORDER BY t1.id";
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

// =============================================================================
// Issue #4918: Unqualified column predicates with views in FULL OUTER JOIN
// =============================================================================

/// Setup a database with views for testing NATURAL FULL JOIN
fn setup_view_join_db() -> Database {
    let mut db = Database::new();

    // Create t4 table
    let t4_schema = TableSchema::with_primary_key(
        "T4".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("x".to_string(), DataType::Varchar { max_length: None }, true),
        ],
        vec!["id".to_string()],
    );
    db.create_table(t4_schema).unwrap();
    db.insert_row("T4", Row::new(vec![SqlValue::Integer(2), SqlValue::Varchar("alice".into())]))
        .unwrap();
    db.insert_row("T4", Row::new(vec![SqlValue::Integer(4), SqlValue::Varchar("bob".into())]))
        .unwrap();
    db.insert_row("T4", Row::new(vec![SqlValue::Integer(6), SqlValue::Varchar("cindy".into())]))
        .unwrap();

    // Create t5 table
    let t5_schema = TableSchema::with_primary_key(
        "T5".to_string(),
        vec![
            ColumnSchema::new("id".to_string(), DataType::Integer, false),
            ColumnSchema::new("y".to_string(), DataType::Varchar { max_length: None }, true),
        ],
        vec!["id".to_string()],
    );
    db.create_table(t5_schema).unwrap();
    db.insert_row("T5", Row::new(vec![SqlValue::Integer(2), SqlValue::Varchar("orange".into())]))
        .unwrap();
    db.insert_row("T5", Row::new(vec![SqlValue::Integer(4), SqlValue::Varchar("green".into())]))
        .unwrap();

    // Create view v4 - parse the SELECT statement and extract the SelectStmt
    let view_select = match Parser::parse_sql("SELECT id, x FROM t4") {
        Ok(vibesql_ast::Statement::Select(select)) => *select,
        _ => panic!("Failed to parse view SELECT"),
    };
    db.catalog
        .create_view(vibesql_catalog::ViewDefinition::new_with_sql(
            "v4".to_string(),
            None,
            view_select,
            false,
            "SELECT id, x FROM t4".to_string(),
        ))
        .unwrap();

    db
}

/// Issue #4918 regression test: Unqualified column IS NULL with view in NATURAL FULL JOIN
///
/// The bug was that predicates like `x IS NULL` were being pushed down to the view scan
/// before the FULL JOIN, incorrectly filtering out all view rows.
#[test]
fn test_issue_4918_unqualified_is_null_with_view_full_join() {
    let db = setup_view_join_db();

    // Test 1: x IS NULL should return 0 rows (no actual NULL x values)
    let sql = "SELECT id, x, y FROM v4 NATURAL FULL JOIN t5 WHERE x IS NULL ORDER BY 1";
    let select = parse_select(sql);
    let result = SelectExecutor::new(&db).execute(&select).unwrap();
    assert_eq!(result.len(), 0, "x IS NULL should return 0 rows - no actual NULL x values");

    // Test 2: x IS NOT NULL should return all 3 rows
    let sql = "SELECT id, x, y FROM v4 NATURAL FULL JOIN t5 WHERE x IS NOT NULL ORDER BY 1";
    let select = parse_select(sql);
    let result = SelectExecutor::new(&db).execute(&select).unwrap();
    assert_eq!(result.len(), 3, "x IS NOT NULL should return all 3 rows");

    // Test 3: x <> 'bob' OR x IS NULL should return 2 rows (ids 2 and 6)
    let sql =
        "SELECT id, x, y FROM v4 NATURAL FULL JOIN t5 WHERE x <> 'bob' OR x IS NULL ORDER BY 1";
    let select = parse_select(sql);
    let result = SelectExecutor::new(&db).execute(&select).unwrap();
    assert_eq!(result.len(), 2, "Should return 2 rows (alice and cindy)");

    // Verify the row values are correct (not NULL)
    let first_x = match &result[0].values[1] {
        SqlValue::Varchar(s) => s.as_str(),
        _ => panic!("Expected varchar for x"),
    };
    assert_eq!(first_x, "alice", "First row should have x='alice'");
}

/// Test qualified column references still work correctly
#[test]
fn test_qualified_column_with_view_full_join() {
    let db = setup_view_join_db();

    // Qualified reference (v4.x) should work correctly
    let sql = "SELECT id, x, y FROM v4 NATURAL FULL JOIN t5 WHERE v4.x IS NULL ORDER BY 1";
    let select = parse_select(sql);
    let result = SelectExecutor::new(&db).execute(&select).unwrap();
    assert_eq!(result.len(), 0, "v4.x IS NULL should return 0 rows");
}

/// Test with direct tables (no views) to ensure the fix doesn't break basic functionality
#[test]
fn test_unqualified_is_null_with_tables_full_join() {
    let db = setup_view_join_db();

    // Direct table reference should work correctly
    let sql = "SELECT id, x, y FROM t4 NATURAL FULL JOIN t5 WHERE x IS NULL ORDER BY 1";
    let select = parse_select(sql);
    let result = SelectExecutor::new(&db).execute(&select).unwrap();
    assert_eq!(result.len(), 0, "Direct table: x IS NULL should return 0 rows");
}
