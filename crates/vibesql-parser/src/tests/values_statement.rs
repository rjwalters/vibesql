//! Tests for standalone VALUES statement parsing (SQL:1999)
//!
//! VALUES is a table value constructor that can be used:
//! 1. As a standalone statement: VALUES(1,2,3);
//! 2. In set operations: VALUES(1) UNION VALUES(2);
//! 3. Mixed with SELECT: SELECT * FROM t INTERSECT VALUES(1,2,3);

use crate::Parser;
use vibesql_ast::{Expression, SelectStmt, SetOperator, Statement};
use vibesql_types::SqlValue;

fn parse(sql: &str) -> Statement {
    Parser::parse_sql(sql).unwrap_or_else(|_| panic!("Failed to parse: {}", sql))
}

fn parse_select(sql: &str) -> SelectStmt {
    match parse(sql) {
        Statement::Select(stmt) => *stmt,
        other => panic!("Expected Select statement, got {:?}", other),
    }
}

#[test]
fn test_values_single_row_single_column() {
    let stmt = parse_select("VALUES(1);");

    assert!(stmt.values.is_some());
    let values = stmt.values.unwrap();
    assert_eq!(values.len(), 1);
    assert_eq!(values[0].len(), 1);
    assert_eq!(values[0][0], Expression::Literal(SqlValue::Integer(1)));
}

#[test]
fn test_values_single_row_multiple_columns() {
    let stmt = parse_select("VALUES(1, 2, 3);");

    assert!(stmt.values.is_some());
    let values = stmt.values.unwrap();
    assert_eq!(values.len(), 1);
    assert_eq!(values[0].len(), 3);
    assert_eq!(values[0][0], Expression::Literal(SqlValue::Integer(1)));
    assert_eq!(values[0][1], Expression::Literal(SqlValue::Integer(2)));
    assert_eq!(values[0][2], Expression::Literal(SqlValue::Integer(3)));
}

#[test]
fn test_values_multiple_rows() {
    let stmt = parse_select("VALUES(1),(2),(3);");

    assert!(stmt.values.is_some());
    let values = stmt.values.unwrap();
    assert_eq!(values.len(), 3);
    assert_eq!(values[0][0], Expression::Literal(SqlValue::Integer(1)));
    assert_eq!(values[1][0], Expression::Literal(SqlValue::Integer(2)));
    assert_eq!(values[2][0], Expression::Literal(SqlValue::Integer(3)));
}

#[test]
fn test_values_multiple_rows_multiple_columns() {
    let stmt = parse_select("VALUES(1,'a'),(2,'b'),(3,'c');");

    assert!(stmt.values.is_some());
    let values = stmt.values.unwrap();
    assert_eq!(values.len(), 3);
    assert_eq!(values[0].len(), 2);
    assert_eq!(values[0][0], Expression::Literal(SqlValue::Integer(1)));
    assert_eq!(
        values[0][1],
        Expression::Literal(SqlValue::Varchar("a".into()))
    );
    assert_eq!(values[2][0], Expression::Literal(SqlValue::Integer(3)));
    assert_eq!(
        values[2][1],
        Expression::Literal(SqlValue::Varchar("c".into()))
    );
}

#[test]
fn test_values_union_values() {
    let stmt = parse_select("VALUES(1) UNION VALUES(2);");

    assert!(stmt.values.is_some());
    let values = stmt.values.as_ref().unwrap();
    assert_eq!(values.len(), 1);
    assert_eq!(values[0][0], Expression::Literal(SqlValue::Integer(1)));

    // Check set operation
    assert!(stmt.set_operation.is_some());
    let set_op = stmt.set_operation.as_ref().unwrap();
    assert_eq!(set_op.op, SetOperator::Union);
    assert!(!set_op.all);

    // Check right side
    assert!(set_op.right.values.is_some());
    let right_values = set_op.right.values.as_ref().unwrap();
    assert_eq!(right_values.len(), 1);
    assert_eq!(
        right_values[0][0],
        Expression::Literal(SqlValue::Integer(2))
    );
}

#[test]
fn test_values_union_all_values() {
    let stmt = parse_select("VALUES(1),(2) UNION ALL VALUES(3);");

    assert!(stmt.values.is_some());
    let values = stmt.values.as_ref().unwrap();
    assert_eq!(values.len(), 2);

    assert!(stmt.set_operation.is_some());
    let set_op = stmt.set_operation.as_ref().unwrap();
    assert_eq!(set_op.op, SetOperator::Union);
    assert!(set_op.all);
}

#[test]
fn test_values_intersect_values() {
    let stmt = parse_select("VALUES(1),(2),(3) INTERSECT VALUES(2);");

    assert!(stmt.values.is_some());
    assert!(stmt.set_operation.is_some());
    let set_op = stmt.set_operation.as_ref().unwrap();
    assert_eq!(set_op.op, SetOperator::Intersect);
}

#[test]
fn test_values_except_values() {
    let stmt = parse_select("VALUES(1),(2),(3) EXCEPT VALUES(2);");

    assert!(stmt.values.is_some());
    assert!(stmt.set_operation.is_some());
    let set_op = stmt.set_operation.as_ref().unwrap();
    assert_eq!(set_op.op, SetOperator::Except);
}

#[test]
fn test_select_union_values() {
    let stmt = parse_select("SELECT 1 UNION VALUES(2);");

    // Left side is a SELECT (no values)
    assert!(stmt.values.is_none());
    assert_eq!(stmt.select_list.len(), 1);

    // Right side is VALUES
    assert!(stmt.set_operation.is_some());
    let set_op = stmt.set_operation.as_ref().unwrap();
    assert!(set_op.right.values.is_some());
}

#[test]
fn test_select_from_table_intersect_values() {
    let stmt = parse_select("SELECT * FROM t14 INTERSECT VALUES(1,2,3);");

    // Left side is a SELECT from table
    assert!(stmt.values.is_none());
    assert!(stmt.from.is_some());

    // Right side is VALUES
    assert!(stmt.set_operation.is_some());
    let set_op = stmt.set_operation.as_ref().unwrap();
    assert_eq!(set_op.op, SetOperator::Intersect);
    assert!(set_op.right.values.is_some());
}

#[test]
fn test_values_expressions() {
    let stmt = parse_select("VALUES(1+2, 'hello' || ' world');");

    assert!(stmt.values.is_some());
    let values = stmt.values.unwrap();
    assert_eq!(values.len(), 1);
    assert_eq!(values[0].len(), 2);

    // First expression is 1+2 (binary op)
    match &values[0][0] {
        Expression::BinaryOp { .. } => {}
        other => panic!("Expected BinaryOp, got {:?}", other),
    }

    // Second expression is string concatenation
    match &values[0][1] {
        Expression::BinaryOp { .. } => {}
        other => panic!("Expected BinaryOp, got {:?}", other),
    }
}

#[test]
fn test_values_without_semicolon() {
    // Should also parse without trailing semicolon
    let stmt = parse_select("VALUES(1,2,3)");
    assert!(stmt.values.is_some());
    assert_eq!(stmt.values.unwrap().len(), 1);
}
