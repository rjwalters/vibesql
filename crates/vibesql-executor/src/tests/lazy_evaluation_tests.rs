//! Tests for lazy evaluation optimizations
//!
//! These tests verify that COALESCE and NULLIF use lazy evaluation
//! to avoid evaluating unnecessary expressions, which is critical for
//! performance when dealing with complex nested expressions.

use vibesql_catalog::TableSchema;
use vibesql_storage::Row;
use vibesql_types::SqlValue;

use crate::evaluator::ExpressionEvaluator;

#[test]
fn test_coalesce_lazy_evaluation_short_circuits() {
    // COALESCE(10, expensive_expr) should return 10 without evaluating expensive_expr
    let schema = TableSchema::new("test".to_string(), vec![]);

    let evaluator = ExpressionEvaluator::new(&schema);
    let row = Row::new(vec![]);

    // Create expression: COALESCE(10, NULL/0) - should return 10 and not evaluate division
    let expr = vibesql_ast::Expression::Function {
        name: "COALESCE".to_string(),
        args: vec![
            vibesql_ast::Expression::Literal(SqlValue::Integer(10)),
            // This expression would cause a division by zero if evaluated
            vibesql_ast::Expression::BinaryOp {
                left: Box::new(vibesql_ast::Expression::Literal(SqlValue::Integer(1))),
                op: vibesql_ast::BinaryOperator::Divide,
                right: Box::new(vibesql_ast::Expression::Literal(SqlValue::Integer(0))),
            },
        ],
        character_unit: None,
    };

    // Should succeed and return 10 without evaluating the division
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_ok(), "COALESCE should short-circuit on non-NULL value");
    assert_eq!(result.unwrap(), SqlValue::Integer(10));
}

#[test]
fn test_coalesce_evaluates_all_when_all_null() {
    // COALESCE(NULL, NULL, 42) should evaluate all arguments until non-NULL found
    let schema = TableSchema::new("test".to_string(), vec![]);

    let evaluator = ExpressionEvaluator::new(&schema);
    let row = Row::new(vec![]);

    let expr = vibesql_ast::Expression::Function {
        name: "COALESCE".to_string(),
        args: vec![
            vibesql_ast::Expression::Literal(SqlValue::Null),
            vibesql_ast::Expression::Literal(SqlValue::Null),
            vibesql_ast::Expression::Literal(SqlValue::Integer(42)),
        ],
        character_unit: None,
    };

    let result = evaluator.eval(&expr, &row);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), SqlValue::Integer(42));
}

#[test]
fn test_nullif_lazy_evaluation_short_circuits() {
    // NULLIF(val1, val2) should not evaluate val2 if val1 is NULL
    let schema = TableSchema::new("test".to_string(), vec![]);

    let evaluator = ExpressionEvaluator::new(&schema);
    let row = Row::new(vec![]);

    // Create expression: NULLIF(NULL, expensive_expr) - should return NULL without evaluating
    // division
    let expr = vibesql_ast::Expression::Function {
        name: "NULLIF".to_string(),
        args: vec![
            vibesql_ast::Expression::Literal(SqlValue::Null),
            // This expression would cause a division by zero if evaluated
            vibesql_ast::Expression::BinaryOp {
                left: Box::new(vibesql_ast::Expression::Literal(SqlValue::Integer(1))),
                op: vibesql_ast::BinaryOperator::Divide,
                right: Box::new(vibesql_ast::Expression::Literal(SqlValue::Integer(0))),
            },
        ],
        character_unit: None,
    };

    // Should succeed and return NULL without evaluating the division
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_ok(), "NULLIF should short-circuit on NULL first argument");
    assert_eq!(result.unwrap(), SqlValue::Null);
}

#[test]
fn test_nullif_evaluates_second_when_first_not_null() {
    // NULLIF(42, 42) should return NULL
    let schema = TableSchema::new("test".to_string(), vec![]);

    let evaluator = ExpressionEvaluator::new(&schema);
    let row = Row::new(vec![]);

    let expr = vibesql_ast::Expression::Function {
        name: "NULLIF".to_string(),
        args: vec![
            vibesql_ast::Expression::Literal(SqlValue::Integer(42)),
            vibesql_ast::Expression::Literal(SqlValue::Integer(42)),
        ],
        character_unit: None,
    };

    let result = evaluator.eval(&expr, &row);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), SqlValue::Null);
}

#[test]
fn test_nullif_returns_first_when_not_equal() {
    // NULLIF(42, 43) should return 42
    let schema = TableSchema::new("test".to_string(), vec![]);

    let evaluator = ExpressionEvaluator::new(&schema);
    let row = Row::new(vec![]);

    let expr = vibesql_ast::Expression::Function {
        name: "NULLIF".to_string(),
        args: vec![
            vibesql_ast::Expression::Literal(SqlValue::Integer(42)),
            vibesql_ast::Expression::Literal(SqlValue::Integer(43)),
        ],
        character_unit: None,
    };

    let result = evaluator.eval(&expr, &row);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), SqlValue::Integer(42));
}

#[test]
fn test_coalesce_with_strings() {
    // COALESCE should work with string values
    let schema = TableSchema::new("test".to_string(), vec![]);

    let evaluator = ExpressionEvaluator::new(&schema);
    let row = Row::new(vec![]);

    let expr = vibesql_ast::Expression::Function {
        name: "COALESCE".to_string(),
        args: vec![
            vibesql_ast::Expression::Literal(SqlValue::Null),
            vibesql_ast::Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("hello"))),
            vibesql_ast::Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("world"))),
        ],
        character_unit: None,
    };

    let result = evaluator.eval(&expr, &row);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), SqlValue::Varchar(arcstr::ArcStr::from("hello")));
}

#[test]
fn test_nested_coalesce() {
    // Test nested COALESCE: COALESCE(NULL, COALESCE(NULL, 42))
    let schema = TableSchema::new("test".to_string(), vec![]);

    let evaluator = ExpressionEvaluator::new(&schema);
    let row = Row::new(vec![]);

    let expr = vibesql_ast::Expression::Function {
        name: "COALESCE".to_string(),
        args: vec![
            vibesql_ast::Expression::Literal(SqlValue::Null),
            vibesql_ast::Expression::Function {
                name: "COALESCE".to_string(),
                args: vec![
                    vibesql_ast::Expression::Literal(SqlValue::Null),
                    vibesql_ast::Expression::Literal(SqlValue::Integer(42)),
                ],
                character_unit: None,
            },
        ],
        character_unit: None,
    };

    let result = evaluator.eval(&expr, &row);
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), SqlValue::Integer(42));
}
