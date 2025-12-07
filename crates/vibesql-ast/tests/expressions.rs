use vibesql_ast::*;
use vibesql_types::{SqlValue, StringValue};

// ============================================================================
// Expression Tests - SQL expressions
// ============================================================================

#[test]
fn test_literal_integer_expression() {
    let expr = Expression::Literal(SqlValue::Integer(42));
    match expr {
        Expression::Literal(SqlValue::Integer(42)) => {} // Success
        _ => panic!("Expected integer literal"),
    }
}

#[test]
fn test_literal_string_expression() {
    let expr = Expression::Literal(SqlValue::Varchar(StringValue::from("hello")));
    match expr {
        Expression::Literal(SqlValue::Varchar(s)) if &*s == "hello" => {} // Success
        _ => panic!("Expected string literal"),
    }
}

#[test]
fn test_column_reference_expression() {
    let expr = Expression::ColumnRef { table: None, column: "id".to_string() };

    match expr {
        Expression::ColumnRef { table: None, column } if column == "id" => {} // Success
        _ => panic!("Expected column reference"),
    }
}

#[test]
fn test_qualified_column_reference() {
    let expr = Expression::ColumnRef { table: Some("users".to_string()), column: "id".to_string() };

    match expr {
        Expression::ColumnRef { table: Some(t), column: c } if t == "users" && c == "id" => {} /* Success */
        _ => panic!("Expected qualified column reference"),
    }
}

#[test]
fn test_binary_operation_addition() {
    let expr = Expression::BinaryOp {
        op: BinaryOperator::Plus,
        left: Box::new(Expression::Literal(SqlValue::Integer(1))),
        right: Box::new(Expression::Literal(SqlValue::Integer(2))),
    };

    match expr {
        Expression::BinaryOp { op: BinaryOperator::Plus, .. } => {} // Success
        _ => panic!("Expected addition operation"),
    }
}

#[test]
fn test_binary_operation_equality() {
    let expr = Expression::BinaryOp {
        op: BinaryOperator::Equal,
        left: Box::new(Expression::ColumnRef { table: None, column: "id".to_string() }),
        right: Box::new(Expression::Literal(SqlValue::Integer(1))),
    };

    match expr {
        Expression::BinaryOp { op: BinaryOperator::Equal, .. } => {} // Success
        _ => panic!("Expected equality operation"),
    }
}

#[test]
fn test_function_call_count_star() {
    let expr = Expression::Function {
        name: "COUNT".to_string(),
        args: vec![Expression::Wildcard],
        character_unit: None,
    };

    match expr {
        Expression::Function { name, .. } if name == "COUNT" => {} // Success
        _ => panic!("Expected function call"),
    }
}

#[test]
fn test_is_null_predicate() {
    let expr = Expression::IsNull {
        expr: Box::new(Expression::ColumnRef { table: None, column: "name".to_string() }),
        negated: false,
    };

    match expr {
        Expression::IsNull { negated: false, .. } => {} // Success
        _ => panic!("Expected IS NULL predicate"),
    }
}

#[test]
fn test_is_not_null_predicate() {
    let expr = Expression::IsNull {
        expr: Box::new(Expression::ColumnRef { table: None, column: "name".to_string() }),
        negated: true,
    };

    match expr {
        Expression::IsNull { negated: true, .. } => {} // Success
        _ => panic!("Expected IS NOT NULL predicate"),
    }
}

#[test]
fn test_wildcard_expression() {
    let expr = Expression::Wildcard;
    match expr {
        Expression::Wildcard => {} // Success
        _ => panic!("Expected wildcard expression"),
    }
}
