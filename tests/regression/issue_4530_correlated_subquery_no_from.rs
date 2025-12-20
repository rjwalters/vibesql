//! Test for issue #4530: Correlated subquery with no FROM clause
//!
//! When a scalar subquery has no FROM clause and references a column from the
//! outer query, the column reference should be properly correlated, not evaluated
//! once and reused for all rows.
//!
//! Previously, `(SELECT 10+x)` would evaluate `x` once (from the first row)
//! and return the same value for all rows because the correlation detection
//! wasn't handling subqueries without FROM clauses correctly.

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

/// Helper to create a test table with data
fn create_test_table(db: &mut Database) {
    let schema = TableSchema::new(
        "t1".to_string(),
        vec![ColumnSchema::new("x".to_string(), DataType::Integer, false)],
    );
    db.create_table(schema).unwrap();

    let table = db.get_table_mut("t1").unwrap();
    table.insert(Row::new(vec![SqlValue::Integer(1)])).unwrap();
    table.insert(Row::new(vec![SqlValue::Integer(2)])).unwrap();
    table.insert(Row::new(vec![SqlValue::Integer(3)])).unwrap();
}

#[test]
fn test_correlated_subquery_no_from_multiplication() {
    let mut db = Database::new();
    create_test_table(&mut db);

    // This was the failing case - scalar subquery with no FROM clause
    // should correlate with outer query's x column
    let rows = select_rows(&db, "SELECT x, (SELECT x * 2) as doubled FROM t1");
    assert_eq!(rows.len(), 3);

    // Each row should have doubled = x * 2
    assert_eq!(rows[0].values[0], SqlValue::Integer(1));
    assert_eq!(rows[0].values[1], SqlValue::Integer(2)); // 1 * 2 = 2
    assert_eq!(rows[1].values[0], SqlValue::Integer(2));
    assert_eq!(rows[1].values[1], SqlValue::Integer(4)); // 2 * 2 = 4
    assert_eq!(rows[2].values[0], SqlValue::Integer(3));
    assert_eq!(rows[2].values[1], SqlValue::Integer(6)); // 3 * 2 = 6
}

#[test]
fn test_correlated_subquery_no_from_addition() {
    let mut db = Database::new();
    create_test_table(&mut db);

    // SELECT 10 + x from subquery
    let rows = select_rows(&db, "SELECT x, (SELECT 10 + x) as added FROM t1");
    assert_eq!(rows.len(), 3);

    // Each row should have added = 10 + x
    assert_eq!(rows[0].values[0], SqlValue::Integer(1));
    assert_eq!(rows[0].values[1], SqlValue::Integer(11)); // 10 + 1 = 11
    assert_eq!(rows[1].values[0], SqlValue::Integer(2));
    assert_eq!(rows[1].values[1], SqlValue::Integer(12)); // 10 + 2 = 12
    assert_eq!(rows[2].values[0], SqlValue::Integer(3));
    assert_eq!(rows[2].values[1], SqlValue::Integer(13)); // 10 + 3 = 13
}

#[test]
fn test_correlated_subquery_no_from_in_derived_table() {
    let mut db = Database::new();

    let schema = TableSchema::new(
        "t1".to_string(),
        vec![
            ColumnSchema::new("x".to_string(), DataType::Integer, false),
            ColumnSchema::new("y".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();

    let table = db.get_table_mut("t1").unwrap();
    table.insert(Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(1)])).unwrap();
    table.insert(Row::new(vec![SqlValue::Integer(2), SqlValue::Integer(2)])).unwrap();
    table.insert(Row::new(vec![SqlValue::Integer(3), SqlValue::Integer(3)])).unwrap();
    table.insert(Row::new(vec![SqlValue::Integer(4), SqlValue::Integer(4)])).unwrap();

    // Test from select6-9.10: nested in a derived table with LIMIT/OFFSET
    let rows =
        select_rows(&db, "SELECT x, y FROM (SELECT x, (SELECT 10+x) y FROM t1 LIMIT -1 OFFSET 1)");
    assert_eq!(rows.len(), 3);

    // Results should skip first row and correlate x properly
    assert_eq!(rows[0].values[0], SqlValue::Integer(2));
    assert_eq!(rows[0].values[1], SqlValue::Integer(12)); // 10 + 2
    assert_eq!(rows[1].values[0], SqlValue::Integer(3));
    assert_eq!(rows[1].values[1], SqlValue::Integer(13)); // 10 + 3
    assert_eq!(rows[2].values[0], SqlValue::Integer(4));
    assert_eq!(rows[2].values[1], SqlValue::Integer(14)); // 10 + 4
}
