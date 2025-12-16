//! Test for issue #4527: ORDER BY N fails with aggregate functions
//!
//! When using `ORDER BY N` (column position) with a SELECT list that includes
//! aggregate functions without aliases, VibeSQL should correctly resolve the
//! position to the aggregate function's result column.
//!
//! Previously, this would fail with "no such column: col2" because the position
//! resolution didn't handle AggregateFunction expressions.

use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;
use vibesql_storage::{Database, Row};
use vibesql_types::{DataType, SqlValue, StringValue};

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
        "t".to_string(),
        vec![
            ColumnSchema::new("a".to_string(), DataType::Varchar { max_length: Some(50) }, false),
            ColumnSchema::new("b".to_string(), DataType::Integer, true),
        ],
    );
    db.create_table(schema).unwrap();

    let table = db.get_table_mut("t").unwrap();
    table
        .insert(Row::new(vec![SqlValue::Varchar(StringValue::from("x")), SqlValue::Integer(1)]))
        .unwrap();
    table
        .insert(Row::new(vec![SqlValue::Varchar(StringValue::from("x")), SqlValue::Integer(2)]))
        .unwrap();
    table
        .insert(Row::new(vec![SqlValue::Varchar(StringValue::from("y")), SqlValue::Integer(1)]))
        .unwrap();
}

#[test]
fn test_order_by_position_with_count_aggregate() {
    let mut db = Database::new();
    create_test_table(&mut db);

    // This was the failing case - ORDER BY 2 with count(b) aggregate
    let rows = select_rows(&db, "SELECT a, count(b) FROM t GROUP BY a ORDER BY 2");
    assert_eq!(rows.len(), 2);

    // Should be sorted by count(b) ascending: y has count=1, x has count=2
    assert_eq!(rows[0].values[0], SqlValue::Varchar(StringValue::from("y")));
    assert_eq!(rows[0].values[1], SqlValue::Integer(1));
    assert_eq!(rows[1].values[0], SqlValue::Varchar(StringValue::from("x")));
    assert_eq!(rows[1].values[1], SqlValue::Integer(2));
}

#[test]
fn test_order_by_position_with_count_desc() {
    let mut db = Database::new();
    create_test_table(&mut db);

    // ORDER BY 2 DESC
    let rows = select_rows(&db, "SELECT a, count(b) FROM t GROUP BY a ORDER BY 2 DESC");
    assert_eq!(rows.len(), 2);

    // Should be sorted by count(b) descending: x has count=2, y has count=1
    assert_eq!(rows[0].values[0], SqlValue::Varchar(StringValue::from("x")));
    assert_eq!(rows[0].values[1], SqlValue::Integer(2));
    assert_eq!(rows[1].values[0], SqlValue::Varchar(StringValue::from("y")));
    assert_eq!(rows[1].values[1], SqlValue::Integer(1));
}

#[test]
fn test_order_by_position_with_sum_aggregate() {
    let mut db = Database::new();
    create_test_table(&mut db);

    // ORDER BY 2 with sum(b)
    let rows = select_rows(&db, "SELECT a, sum(b) FROM t GROUP BY a ORDER BY 2");
    assert_eq!(rows.len(), 2);

    // y has sum=1, x has sum=3 (1+2)
    assert_eq!(rows[0].values[0], SqlValue::Varchar(StringValue::from("y")));
    assert_eq!(rows[0].values[1], SqlValue::Integer(1));
    assert_eq!(rows[1].values[0], SqlValue::Varchar(StringValue::from("x")));
    assert_eq!(rows[1].values[1], SqlValue::Integer(3));
}

#[test]
fn test_order_by_position_with_aliased_aggregate() {
    let mut db = Database::new();
    create_test_table(&mut db);

    // With alias, ORDER BY 2 should still work
    let rows = select_rows(&db, "SELECT a, count(b) as cnt FROM t GROUP BY a ORDER BY 2");
    assert_eq!(rows.len(), 2);

    // Should be sorted by cnt ascending
    assert_eq!(rows[0].values[0], SqlValue::Varchar(StringValue::from("y")));
    assert_eq!(rows[0].values[1], SqlValue::Integer(1));
    assert_eq!(rows[1].values[0], SqlValue::Varchar(StringValue::from("x")));
    assert_eq!(rows[1].values[1], SqlValue::Integer(2));
}

#[test]
fn test_order_by_position_first_column_with_aggregate() {
    let mut db = Database::new();
    create_test_table(&mut db);

    // ORDER BY 1 should sort by the group column (a)
    let rows = select_rows(&db, "SELECT a, count(b) FROM t GROUP BY a ORDER BY 1");
    assert_eq!(rows.len(), 2);

    // Should be sorted by a alphabetically: x, y
    assert_eq!(rows[0].values[0], SqlValue::Varchar(StringValue::from("x")));
    assert_eq!(rows[1].values[0], SqlValue::Varchar(StringValue::from("y")));
}
