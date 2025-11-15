//! Integration test for random aggregate patterns from sqllogictest suite
//! These tests capture real failure patterns from random/aggregates tests

use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;
use vibesql_storage::{Database, Row};
use vibesql_types::{DataType, SqlValue};

fn execute_query(db: &Database, sql: &str) -> Result<Vec<Row>, String> {
    let stmt = Parser::parse_sql(sql).map_err(|e| format!("Parse error: {:?}", e))?;
    let select_stmt = match stmt {
        vibesql_ast::Statement::Select(s) => s,
        other => return Err(format!("Expected SELECT statement, got {:?}", other)),
    };

    let executor = SelectExecutor::new(db);
    executor.execute(&select_stmt).map_err(|e| format!("Execution error: {:?}", e))
}

fn create_table_schema(name: &str, columns: Vec<(&str, DataType)>) -> TableSchema {
    TableSchema::new(
        name.to_uppercase(),
        columns.into_iter()
            .map(|(col_name, data_type)| ColumnSchema::new(col_name.to_uppercase(), data_type, true))
            .collect(),
    )
}

#[test]
fn test_aggregate_with_unary_operators() {
    let mut db = Database::new();

    // Setup from slt_good_0.test
    let schema = create_table_schema("tab1", vec![
        ("col0", DataType::Integer),
        ("col1", DataType::Integer),
        ("col2", DataType::Integer),
    ]);
    db.create_table(schema).unwrap();

    db.insert_row("TAB1", Row::new(vec![SqlValue::Integer(51), SqlValue::Integer(14), SqlValue::Integer(96)])).unwrap();
    db.insert_row("TAB1", Row::new(vec![SqlValue::Integer(85), SqlValue::Integer(5), SqlValue::Integer(59)])).unwrap();
    db.insert_row("TAB1", Row::new(vec![SqlValue::Integer(91), SqlValue::Integer(47), SqlValue::Integer(68)])).unwrap();

    // Test COUNT with double unary + operators on argument
    // From label-11 in slt_good_0.test
    let result = execute_query(&db, "SELECT - COUNT( ALL + + col1 ) AS col2, COUNT( ALL - - col1 ) FROM tab1").unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], SqlValue::Integer(-3));
    assert_eq!(result[0].values[1], SqlValue::Integer(3));
}

#[test]
fn test_aggregate_with_arithmetic_in_select() {
    let mut db = Database::new();

    // Setup
    let schema = create_table_schema("tab0", vec![
        ("col0", DataType::Integer),
        ("col1", DataType::Integer),
        ("col2", DataType::Integer),
    ]);
    db.create_table(schema).unwrap();

    db.insert_row("TAB0", Row::new(vec![SqlValue::Integer(97), SqlValue::Integer(1), SqlValue::Integer(99)])).unwrap();
    db.insert_row("TAB0", Row::new(vec![SqlValue::Integer(15), SqlValue::Integer(81), SqlValue::Integer(47)])).unwrap();
    db.insert_row("TAB0", Row::new(vec![SqlValue::Integer(87), SqlValue::Integer(21), SqlValue::Integer(10)])).unwrap();

    // Test COUNT with arithmetic in SELECT
    // From label-13 in slt_good_0.test
    let result = execute_query(&db, "SELECT - COUNT( * ) * - 31 AS col2 FROM tab0").unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], SqlValue::Integer(93));
}

#[test]
fn test_aggregate_sum_with_cast() {
    let mut db = Database::new();

    // Setup
    let schema = create_table_schema("tab1", vec![
        ("col0", DataType::Integer),
        ("col1", DataType::Integer),
        ("col2", DataType::Integer),
    ]);
    db.create_table(schema).unwrap();

    db.insert_row("TAB1", Row::new(vec![SqlValue::Integer(51), SqlValue::Integer(14), SqlValue::Integer(96)])).unwrap();

    // Test SUM with CAST
    let result = execute_query(&db, "SELECT + SUM( CAST( NULL AS SIGNED ) ) AS col1 FROM tab1").unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], SqlValue::Null);
}

#[test]
fn test_aggregate_with_complex_where() {
    let mut db = Database::new();

    // Setup
    let schema = create_table_schema("tab0", vec![
        ("col0", DataType::Integer),
        ("col1", DataType::Integer),
    ]);
    db.create_table(schema).unwrap();

    db.insert_row("TAB0", Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(2)])).unwrap();

    // Test SUM with WHERE that filters all rows
    let result = execute_query(&db, "SELECT - SUM( 1 ) FROM tab0 AS cor0 WHERE NOT NULL NOT IN ( - col1 )").unwrap();
    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values[0], SqlValue::Null);
}

#[test]
fn test_aggregate_distinct_with_arithmetic() {
    let mut db = Database::new();

    // Setup from slt_good_0.test
    let schema = create_table_schema("tab1", vec![
        ("col0", DataType::Integer),
        ("col1", DataType::Integer),
        ("col2", DataType::Integer),
    ]);
    db.create_table(schema).unwrap();

    db.insert_row("TAB1", Row::new(vec![SqlValue::Integer(51), SqlValue::Integer(14), SqlValue::Integer(96)])).unwrap();
    db.insert_row("TAB1", Row::new(vec![SqlValue::Integer(85), SqlValue::Integer(5), SqlValue::Integer(59)])).unwrap();
    db.insert_row("TAB1", Row::new(vec![SqlValue::Integer(91), SqlValue::Integer(47), SqlValue::Integer(68)])).unwrap();

    // Test DISTINCT with negation and aggregate
    let result = execute_query(&db, "SELECT DISTINCT + - SUM( DISTINCT - col1 ) FROM tab1").unwrap();
    assert_eq!(result.len(), 1);
    // sum(distinct -14, -5, -47) = -66, then + - (-66) = 66
    assert_eq!(result[0].values[0], SqlValue::Integer(66));
}
