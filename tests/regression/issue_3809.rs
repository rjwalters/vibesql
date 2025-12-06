//! Test for issue #3809: Window functions inside CASE expressions fail
//!
//! This test reproduces the bug where window functions work correctly in direct
//! SELECT columns but fail when nested inside CASE expressions with the error:
//! "Window functions require window mapping context"

use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;
use vibesql_storage::{Database, Row};
use vibesql_types::{DataType, SqlValue};

/// Helper to execute SELECT and return rows
fn select_rows(db: &Database, sql: &str) -> Vec<Row> {
    let stmt = Parser::parse_sql(sql).unwrap();
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let executor = SelectExecutor::new(db);
        executor.execute(&select_stmt).unwrap()
    } else {
        panic!("Expected SELECT statement");
    }
}

/// Helper to create the test table
fn create_test_table(db: &mut Database) {
    let schema = TableSchema::new(
        "T".to_string(),
        vec![ColumnSchema::new("X".to_string(), DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();

    let table = db.get_table_mut("T").unwrap();
    table.insert(Row::new(vec![SqlValue::Integer(1)])).unwrap();
    table.insert(Row::new(vec![SqlValue::Integer(2)])).unwrap();
    table.insert(Row::new(vec![SqlValue::Integer(3)])).unwrap();
}

#[test]
fn test_window_function_in_case_expression() {
    let mut db = Database::new();
    create_test_table(&mut db);

    // Test 1: Basic window function works (sanity check)
    let rows = select_rows(&db, "SELECT x, LAG(x) OVER (ORDER BY x) as prev FROM t");
    assert_eq!(rows.len(), 3);

    // Test 2: Window function in CASE WHEN condition - this was failing
    let rows = select_rows(
        &db,
        "SELECT x,
            CASE
                WHEN LAG(x) OVER (ORDER BY x) IS NULL THEN 'first'
                ELSE 'not_first'
            END as position
        FROM t",
    );
    assert_eq!(rows.len(), 3);
    // First row should have 'first', others should have 'not_first'
    assert_eq!(rows[0].values[1], vibesql_types::SqlValue::Varchar("first".to_string()));
    assert_eq!(rows[1].values[1], vibesql_types::SqlValue::Varchar("not_first".to_string()));
    assert_eq!(rows[2].values[1], vibesql_types::SqlValue::Varchar("not_first".to_string()));

    // Test 3: Window function in CASE THEN/ELSE result expressions
    let rows = select_rows(
        &db,
        "SELECT x,
            CASE
                WHEN x > 1 THEN x - LAG(x) OVER (ORDER BY x)
                ELSE NULL
            END as diff
        FROM t",
    );
    assert_eq!(rows.len(), 3);
    // First row: x=1, x > 1 is false, so NULL
    assert_eq!(rows[0].values[1], vibesql_types::SqlValue::Null);
    // Second row: x=2, diff = 2 - 1 = 1
    assert_eq!(rows[1].values[1], vibesql_types::SqlValue::Integer(1));
    // Third row: x=3, diff = 3 - 2 = 1
    assert_eq!(rows[2].values[1], vibesql_types::SqlValue::Integer(1));

    // Test 4: Multiple window functions in same CASE
    let rows = select_rows(
        &db,
        "SELECT x,
            CASE
                WHEN LAG(x) OVER (ORDER BY x) IS NULL THEN NULL
                ELSE x - LAG(x) OVER (ORDER BY x)
            END as diff
        FROM t",
    );
    assert_eq!(rows.len(), 3);
    // This is the pattern from the issue
    assert_eq!(rows[0].values[1], vibesql_types::SqlValue::Null);
    assert_eq!(rows[1].values[1], vibesql_types::SqlValue::Integer(1));
    assert_eq!(rows[2].values[1], vibesql_types::SqlValue::Integer(1));
}

#[test]
fn test_window_function_in_is_null() {
    let mut db = Database::new();

    // Create a 2-row table for this test
    let schema = TableSchema::new(
        "T".to_string(),
        vec![ColumnSchema::new("X".to_string(), DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();
    let table = db.get_table_mut("T").unwrap();
    table.insert(Row::new(vec![SqlValue::Integer(1)])).unwrap();
    table.insert(Row::new(vec![SqlValue::Integer(2)])).unwrap();

    // Test: IS NULL with window function
    let rows = select_rows(
        &db,
        "SELECT x, LAG(x) OVER (ORDER BY x) IS NULL as is_first FROM t",
    );
    assert_eq!(rows.len(), 2);
    // First row: LAG is NULL, so IS NULL = true
    assert_eq!(rows[0].values[1], vibesql_types::SqlValue::Boolean(true));
    // Second row: LAG = 1, so IS NULL = false
    assert_eq!(rows[1].values[1], vibesql_types::SqlValue::Boolean(false));
}

#[test]
fn test_window_function_in_nested_function() {
    let mut db = Database::new();
    create_test_table(&mut db);

    // Test: COALESCE with window function
    let rows = select_rows(
        &db,
        "SELECT x, COALESCE(LAG(x) OVER (ORDER BY x), 0) as prev_or_zero FROM t",
    );
    assert_eq!(rows.len(), 3);
    // First row: LAG is NULL, COALESCE returns 0
    assert_eq!(rows[0].values[1], vibesql_types::SqlValue::Integer(0));
    // Second row: LAG = 1
    assert_eq!(rows[1].values[1], vibesql_types::SqlValue::Integer(1));
    // Third row: LAG = 2
    assert_eq!(rows[2].values[1], vibesql_types::SqlValue::Integer(2));
}
