//! End-to-end test for issue #1710: NULL handling with NOT and IS NULL
//!
//! This test verifies that queries with `NOT col IS NULL` and `NOT (NULL) IS NULL`
//! produce correct results.

use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;
use vibesql_storage::{Database, Row};
use vibesql_types::{DataType, SqlValue};

fn execute_select(db: &Database, sql: &str) -> Result<Vec<Row>, String> {
    let stmt = Parser::parse_sql(sql).map_err(|e| format!("Parse error: {:?}", e))?;
    let select_stmt = match stmt {
        vibesql_ast::Statement::Select(s) => s,
        other => return Err(format!("Expected SELECT statement, got {:?}", other)),
    };

    let executor = SelectExecutor::new(db);
    executor.execute(&select_stmt).map_err(|e| format!("Execution error: {:?}", e))
}

#[test]
fn test_not_col_is_null() {
    // Create table with nullable column
    let schema = TableSchema::new(
        "TAB0".to_string(),
        vec![ColumnSchema::new("COL0".to_string(), DataType::Integer, true)],
    );

    let mut db = Database::new();
    db.create_table(schema).unwrap();
    db.insert_row("TAB0", Row::new(vec![SqlValue::Integer(99)])).unwrap();
    db.insert_row("TAB0", Row::new(vec![SqlValue::Integer(100)])).unwrap();
    db.insert_row("TAB0", Row::new(vec![SqlValue::Null])).unwrap();

    // Test: NOT col0 IS NULL should be equivalent to col0 IS NOT NULL
    // Should return rows where col0 is NOT NULL (i.e., 99 and 100)
    let results1 = execute_select(&db, "SELECT col0 FROM tab0 WHERE NOT col0 IS NULL")
        .expect("Query should succeed");

    let results2 = execute_select(&db, "SELECT col0 FROM tab0 WHERE col0 IS NOT NULL")
        .expect("Query should succeed");

    assert_eq!(results1.len(), 2, "NOT col0 IS NULL should return 2 rows");
    assert_eq!(results2.len(), 2, "col0 IS NOT NULL should return 2 rows");

    // Both queries should return the same results
    assert_eq!(results1, results2, "NOT col0 IS NULL should be equivalent to col0 IS NOT NULL");

    // Verify the actual values
    assert_eq!(results1[0].get(0), Some(&SqlValue::Integer(99)));
    assert_eq!(results1[1].get(0), Some(&SqlValue::Integer(100)));
}

#[test]
fn test_not_null_is_null() {
    // Test: NOT (NULL) IS NULL
    // NULL IS NULL = TRUE
    // NOT TRUE = FALSE
    // So WHERE clause filters out all rows

    let schema = TableSchema::new(
        "TAB0".to_string(),
        vec![ColumnSchema::new("COL0".to_string(), DataType::Integer, false)],
    );

    let mut db = Database::new();
    db.create_table(schema).unwrap();
    db.insert_row("TAB0", Row::new(vec![SqlValue::Integer(1)])).unwrap();
    db.insert_row("TAB0", Row::new(vec![SqlValue::Integer(2)])).unwrap();

    let results = execute_select(&db, "SELECT col0 FROM tab0 WHERE NOT (NULL) IS NULL")
        .expect("Query should succeed");

    assert_eq!(
        results.len(),
        0,
        "NOT (NULL) IS NULL should return 0 rows (NULL IS NULL = TRUE, NOT TRUE = FALSE)"
    );
}

#[test]
fn test_not_expr_is_null() {
    // Test: NOT col0 * col0 IS NULL
    // Should be parsed as: NOT (col0 * col0 IS NULL)

    let schema = TableSchema::new(
        "TAB1".to_string(),
        vec![ColumnSchema::new("COL0".to_string(), DataType::Integer, true)],
    );

    let mut db = Database::new();
    db.create_table(schema).unwrap();
    db.insert_row("TAB1", Row::new(vec![SqlValue::Integer(2)])).unwrap();
    db.insert_row("TAB1", Row::new(vec![SqlValue::Integer(3)])).unwrap();
    db.insert_row("TAB1", Row::new(vec![SqlValue::Null])).unwrap();

    // col0 * col0 IS NULL is only true for the NULL row
    // NOT (col0 * col0 IS NULL) should return the non-NULL rows
    let results = execute_select(&db, "SELECT col0 FROM tab1 WHERE NOT col0 * col0 IS NULL")
        .expect("Query should succeed");

    assert_eq!(results.len(), 2, "NOT col0 * col0 IS NULL should return non-NULL rows");
    assert_eq!(results[0].get(0), Some(&SqlValue::Integer(2)));
    assert_eq!(results[1].get(0), Some(&SqlValue::Integer(3)));
}

#[test]
fn test_not_in_with_null() {
    // Test: NOT ( + col0 ) IN ( col1 )
    // This involves both NOT and IN operator, testing complex NULL logic

    let schema = TableSchema::new(
        "TAB0".to_string(),
        vec![
            ColumnSchema::new("COL0".to_string(), DataType::Integer, true),
            ColumnSchema::new("COL1".to_string(), DataType::Integer, true),
        ],
    );

    let mut db = Database::new();
    db.create_table(schema).unwrap();
    db.insert_row("TAB0", Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(2)])).unwrap();
    db.insert_row("TAB0", Row::new(vec![SqlValue::Integer(2), SqlValue::Integer(2)])).unwrap();
    db.insert_row("TAB0", Row::new(vec![SqlValue::Integer(3), SqlValue::Integer(4)])).unwrap();

    // This query should work correctly with proper NULL handling
    let results = execute_select(&db, "SELECT col0 FROM tab0 WHERE NOT ( + col0 ) IN ( col1 )")
        .expect("Query should succeed");

    // Row 1: NOT (1 IN (2)) = NOT FALSE = TRUE ✓
    // Row 2: NOT (2 IN (2)) = NOT TRUE = FALSE ✗
    // Row 3: NOT (3 IN (4)) = NOT FALSE = TRUE ✓
    assert_eq!(results.len(), 2, "Should return rows where col0 NOT IN (col1)");
    assert_eq!(results[0].get(0), Some(&SqlValue::Integer(1)));
    assert_eq!(results[1].get(0), Some(&SqlValue::Integer(3)));
}
