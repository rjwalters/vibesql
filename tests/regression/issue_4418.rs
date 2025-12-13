//! Test for issue #4418: ORDER BY +N not recognized as column position
//!
//! When using `ORDER BY +N` (positive unary operator with numeric column position),
//! VibeSQL should treat it the same as `ORDER BY N` - as a column position reference.
//! The `+` is a unary positive operator that doesn't change the value.

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

/// Helper to create the test table (matches select1.test t5 table)
fn create_test_table(db: &mut Database) {
    let schema = TableSchema::new(
        "t5".to_string(),
        vec![
            ColumnSchema::new("a".to_string(), DataType::Integer, false),
            ColumnSchema::new("b".to_string(), DataType::Integer, false),
        ],
    );
    db.create_table(schema).unwrap();

    let table = db.get_table_mut("t5").unwrap();
    table.insert(Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(10)])).unwrap();
    table.insert(Row::new(vec![SqlValue::Integer(2), SqlValue::Integer(9)])).unwrap();
}

#[test]
fn test_order_by_positive_unary_position() {
    let mut db = Database::new();
    create_test_table(&mut db);

    // Test 1: ORDER BY N works (sanity check)
    let rows = select_rows(&db, "SELECT * FROM t5 ORDER BY 2");
    assert_eq!(rows.len(), 2);
    // Should be sorted by column b (the 2nd column) ascending: 9, 10
    assert_eq!(rows[0].values[0], SqlValue::Integer(2)); // a=2, b=9
    assert_eq!(rows[0].values[1], SqlValue::Integer(9));
    assert_eq!(rows[1].values[0], SqlValue::Integer(1)); // a=1, b=10
    assert_eq!(rows[1].values[1], SqlValue::Integer(10));

    // Test 2: ORDER BY +N should work the same as ORDER BY N
    // This was the failing case from select1-4.9.2
    let rows = select_rows(&db, "SELECT * FROM t5 ORDER BY +2");
    assert_eq!(rows.len(), 2);
    // Should be sorted by column b (the 2nd column) ascending: 9, 10
    assert_eq!(rows[0].values[0], SqlValue::Integer(2)); // a=2, b=9
    assert_eq!(rows[0].values[1], SqlValue::Integer(9));
    assert_eq!(rows[1].values[0], SqlValue::Integer(1)); // a=1, b=10
    assert_eq!(rows[1].values[1], SqlValue::Integer(10));
}

#[test]
fn test_order_by_positive_unary_first_column() {
    let mut db = Database::new();
    create_test_table(&mut db);

    // Test ORDER BY +1 (first column)
    let rows = select_rows(&db, "SELECT * FROM t5 ORDER BY +1");
    assert_eq!(rows.len(), 2);
    // Should be sorted by column a ascending: 1, 2
    assert_eq!(rows[0].values[0], SqlValue::Integer(1));
    assert_eq!(rows[1].values[0], SqlValue::Integer(2));
}

#[test]
fn test_order_by_positive_unary_descending() {
    let mut db = Database::new();
    create_test_table(&mut db);

    // Test ORDER BY +2 DESC
    let rows = select_rows(&db, "SELECT * FROM t5 ORDER BY +2 DESC");
    assert_eq!(rows.len(), 2);
    // Should be sorted by column b descending: 10, 9
    assert_eq!(rows[0].values[0], SqlValue::Integer(1)); // a=1, b=10
    assert_eq!(rows[0].values[1], SqlValue::Integer(10));
    assert_eq!(rows[1].values[0], SqlValue::Integer(2)); // a=2, b=9
    assert_eq!(rows[1].values[1], SqlValue::Integer(9));
}

#[test]
fn test_order_by_positive_unary_with_alias() {
    let mut db = Database::new();
    create_test_table(&mut db);

    // Test ORDER BY +1 with aliased columns
    let rows = select_rows(&db, "SELECT a AS x, b AS y FROM t5 ORDER BY +2");
    assert_eq!(rows.len(), 2);
    // Should be sorted by y (column b) ascending: 9, 10
    assert_eq!(rows[0].values[1], SqlValue::Integer(9));
    assert_eq!(rows[1].values[1], SqlValue::Integer(10));
}
