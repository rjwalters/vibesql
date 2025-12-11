//! Test for GROUP BY with expression projections (#4236)
//!
//! This test ensures that SELECT expressions that reference GROUP BY columns
//! but are not themselves GROUP BY columns are correctly computed.
//!
//! For example: SELECT col1 * 11 FROM tab1 GROUP BY col1
//! The SELECT expression `col1 * 11` references the GROUP BY column `col1`,
//! but is not itself a GROUP BY expression. The multiplication must be applied.
//!
//! The bug was in the columnar execution path's validation logic, which counted
//! non-aggregate SELECT expressions but didn't verify they were identical to
//! GROUP BY expressions. This allowed expressions like `col1 * 11` to pass
//! the count check (1 non-agg == 1 GROUP BY key), but the columnar path then
//! returned just the group key value (col1) without the multiplication.

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

/// Helper to create the test table (matching sqllogictest tab1)
fn create_test_table(db: &mut Database) {
    let schema = TableSchema::new(
        "TAB1".to_string(),
        vec![
            ColumnSchema::new("COL0".to_string(), DataType::Integer, false),
            ColumnSchema::new("COL1".to_string(), DataType::Integer, false),
            ColumnSchema::new("COL2".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();

    let table = db.get_table_mut("TAB1").unwrap();
    // Insert test data: col1 values are 4, 57, 6, 44
    table
        .insert(Row::new(vec![SqlValue::Integer(0), SqlValue::Integer(4), SqlValue::Integer(3)]))
        .unwrap();
    table
        .insert(Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(57), SqlValue::Integer(6)]))
        .unwrap();
    table
        .insert(Row::new(vec![SqlValue::Integer(2), SqlValue::Integer(6), SqlValue::Integer(89)]))
        .unwrap();
    table
        .insert(Row::new(vec![SqlValue::Integer(3), SqlValue::Integer(44), SqlValue::Integer(11)]))
        .unwrap();
}

#[test]
fn test_group_by_column_with_multiplication() {
    // Regression test for issue #4236
    // SELECT col1 * 11 FROM tab1 GROUP BY col1
    // Expected: 44, 627, 66, 484 (col1 values multiplied by 11)
    // Bug: returned 4, 57, 6, 44 (just the col1 values without multiplication)

    let mut db = Database::new();
    create_test_table(&mut db);

    let rows = select_rows(&db, "SELECT col1 * 11 AS col2, COUNT(*) FROM tab1 GROUP BY col1");

    assert_eq!(rows.len(), 4, "Should return 4 groups");

    // Collect the col2 values (sorted for deterministic comparison)
    let mut values: Vec<i64> = rows
        .iter()
        .map(|r| match &r.values[0] {
            SqlValue::Integer(n) => *n,
            other => panic!("Expected Integer, got {:?}", other),
        })
        .collect();
    values.sort();

    // Expected: 4*11=44, 6*11=66, 44*11=484, 57*11=627
    assert_eq!(values, vec![44, 66, 484, 627], "col1 * 11 should be computed");
}

#[test]
fn test_group_by_with_unary_operators() {
    // Test the original failing case from sqllogictest:
    // SELECT - + col1 * - 11 AS col2, COUNT(*) FROM tab1 GROUP BY col1
    // Expression: -((+col1) * (-11)) = -(-11 * col1) = 11 * col1 = col1 * 11

    let mut db = Database::new();
    create_test_table(&mut db);

    let rows = select_rows(&db, "SELECT - + col1 * - 11 AS col2, COUNT(*) FROM tab1 GROUP BY col1");

    assert_eq!(rows.len(), 4, "Should return 4 groups");

    // Collect the col2 values (sorted for deterministic comparison)
    let mut values: Vec<i64> = rows
        .iter()
        .map(|r| match &r.values[0] {
            SqlValue::Integer(n) => *n,
            other => panic!("Expected Integer, got {:?}", other),
        })
        .collect();
    values.sort();

    // Expected: 4*11=44, 6*11=66, 44*11=484, 57*11=627
    assert_eq!(values, vec![44, 66, 484, 627], "- + col1 * - 11 should equal col1 * 11");
}

#[test]
fn test_group_by_column_selected_alongside_expression() {
    // When the GROUP BY column is also in SELECT, everything should work
    // SELECT col1, col1 * 11 FROM tab1 GROUP BY col1

    let mut db = Database::new();
    create_test_table(&mut db);

    let rows = select_rows(&db, "SELECT col1, col1 * 11 AS col2, COUNT(*) FROM tab1 GROUP BY col1");

    assert_eq!(rows.len(), 4, "Should return 4 groups");

    for row in &rows {
        let col1 = match &row.values[0] {
            SqlValue::Integer(n) => *n,
            other => panic!("Expected Integer, got {:?}", other),
        };
        let col2 = match &row.values[1] {
            SqlValue::Integer(n) => *n,
            other => panic!("Expected Integer, got {:?}", other),
        };
        assert_eq!(col2, col1 * 11, "col2 should be col1 * 11");
    }
}

#[test]
fn test_group_by_with_addition() {
    // Test with addition instead of multiplication
    // SELECT col1 + 100 FROM tab1 GROUP BY col1

    let mut db = Database::new();
    create_test_table(&mut db);

    let rows = select_rows(&db, "SELECT col1 + 100 AS col2, COUNT(*) FROM tab1 GROUP BY col1");

    assert_eq!(rows.len(), 4, "Should return 4 groups");

    // Collect the col2 values (sorted for deterministic comparison)
    let mut values: Vec<i64> = rows
        .iter()
        .map(|r| match &r.values[0] {
            SqlValue::Integer(n) => *n,
            other => panic!("Expected Integer, got {:?}", other),
        })
        .collect();
    values.sort();

    // Expected: 4+100=104, 6+100=106, 44+100=144, 57+100=157
    assert_eq!(values, vec![104, 106, 144, 157], "col1 + 100 should be computed");
}

#[test]
fn test_group_by_expression_only_aggregate() {
    // When SELECT only has aggregates (no group key columns), should work
    // SELECT COUNT(*), SUM(col1) FROM tab1 GROUP BY col1

    let mut db = Database::new();
    create_test_table(&mut db);

    let rows = select_rows(&db, "SELECT COUNT(*), SUM(col1) FROM tab1 GROUP BY col1");

    assert_eq!(rows.len(), 4, "Should return 4 groups");

    // Each group has 1 row, so COUNT(*) = 1 and SUM(col1) = col1 for each group
    for row in &rows {
        let count = match &row.values[0] {
            SqlValue::Integer(n) => *n,
            other => panic!("Expected Integer, got {:?}", other),
        };
        assert_eq!(count, 1, "Each group should have COUNT(*) = 1");
    }
}
