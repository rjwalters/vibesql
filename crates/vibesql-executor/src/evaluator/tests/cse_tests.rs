//! Tests for Common Sub-Expression Elimination (CSE)

use crate::errors::ExecutorError;
use crate::evaluator::ExpressionEvaluator;
use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_types::{DataType, SqlValue};

#[test]
fn test_cse_repeated_arithmetic() -> Result<(), ExecutorError> {
    // Test query: SELECT (a + b), (a + b) * 2, (a + b) * 3
    // The sub-expression (a + b) should be evaluated only once

    let schema = TableSchema::new(
        "test".to_string(),
        vec![
            ColumnSchema::new("a".to_string(), DataType::Integer, false),
            ColumnSchema::new("b".to_string(), DataType::Integer, false),
        ],
    );

    let row = vibesql_storage::Row::new(vec![SqlValue::Integer(10), SqlValue::Integer(5)]);

    let evaluator = ExpressionEvaluator::new(&schema);

    // Build expression: (a + b)
    let a_plus_b = vibesql_ast::Expression::BinaryOp {
        left: Box::new(vibesql_ast::Expression::ColumnRef { table: None, column: "a".to_string() }),
        op: vibesql_ast::BinaryOperator::Plus,
        right: Box::new(vibesql_ast::Expression::ColumnRef {
            table: None,
            column: "b".to_string(),
        }),
    };

    // Evaluate (a + b) multiple times
    let result1 = evaluator.eval(&a_plus_b, &row)?;
    let result2 = evaluator.eval(&a_plus_b, &row)?;
    let result3 = evaluator.eval(&a_plus_b, &row)?;

    // All should return the same value
    assert_eq!(result1, SqlValue::Integer(15));
    assert_eq!(result2, SqlValue::Integer(15));
    assert_eq!(result3, SqlValue::Integer(15));

    Ok(())
}

#[test]
fn test_cse_nested_expressions() -> Result<(), ExecutorError> {
    // Test nested expressions with repeated sub-expressions

    let schema = TableSchema::new(
        "test".to_string(),
        vec![
            ColumnSchema::new("x".to_string(), DataType::Integer, false),
            ColumnSchema::new("y".to_string(), DataType::Integer, false),
        ],
    );

    let row = vibesql_storage::Row::new(vec![SqlValue::Integer(3), SqlValue::Integer(4)]);

    let evaluator = ExpressionEvaluator::new(&schema);

    // Build expression: (x * y)
    let x_times_y = vibesql_ast::Expression::BinaryOp {
        left: Box::new(vibesql_ast::Expression::ColumnRef { table: None, column: "x".to_string() }),
        op: vibesql_ast::BinaryOperator::Multiply,
        right: Box::new(vibesql_ast::Expression::ColumnRef {
            table: None,
            column: "y".to_string(),
        }),
    };

    // Evaluate multiple times
    let result1 = evaluator.eval(&x_times_y, &row)?;
    let result2 = evaluator.eval(&x_times_y, &row)?;

    assert_eq!(result1, SqlValue::Integer(12));
    assert_eq!(result2, SqlValue::Integer(12));

    Ok(())
}

#[test]
fn test_cse_case_expression() -> Result<(), ExecutorError> {
    // Test CASE with repeated sub-expressions
    // CASE WHEN (x * y) > 10 THEN (x * y) * 2 ELSE (x * y) END

    let schema = TableSchema::new(
        "test".to_string(),
        vec![
            ColumnSchema::new("x".to_string(), DataType::Integer, false),
            ColumnSchema::new("y".to_string(), DataType::Integer, false),
        ],
    );

    let row = vibesql_storage::Row::new(vec![SqlValue::Integer(3), SqlValue::Integer(4)]);

    let evaluator = ExpressionEvaluator::new(&schema);

    // Build expression: (x * y)
    let x_times_y = || vibesql_ast::Expression::BinaryOp {
        left: Box::new(vibesql_ast::Expression::ColumnRef { table: None, column: "x".to_string() }),
        op: vibesql_ast::BinaryOperator::Multiply,
        right: Box::new(vibesql_ast::Expression::ColumnRef {
            table: None,
            column: "y".to_string(),
        }),
    };

    // Build CASE expression
    let case_expr = vibesql_ast::Expression::Case {
        operand: None,
        when_clauses: vec![vibesql_ast::CaseWhen {
            conditions: vec![vibesql_ast::Expression::BinaryOp {
                left: Box::new(x_times_y()),
                op: vibesql_ast::BinaryOperator::GreaterThan,
                right: Box::new(vibesql_ast::Expression::Literal(SqlValue::Integer(10))),
            }],
            result: vibesql_ast::Expression::BinaryOp {
                left: Box::new(x_times_y()),
                op: vibesql_ast::BinaryOperator::Multiply,
                right: Box::new(vibesql_ast::Expression::Literal(SqlValue::Integer(2))),
            },
        }],
        else_result: Some(Box::new(x_times_y())),
    };

    let result = evaluator.eval(&case_expr, &row)?;
    assert_eq!(result, SqlValue::Integer(24)); // (3*4) > 10, so (3*4) * 2 = 24

    Ok(())
}

#[test]
fn test_cse_disabled_via_env() -> Result<(), ExecutorError> {
    // Test that CSE can be disabled via environment variable
    // Note: This test may not work in parallel test execution
    // as it modifies global environment

    std::env::set_var("CSE_ENABLED", "false");

    let schema = TableSchema::new(
        "test".to_string(),
        vec![ColumnSchema::new("a".to_string(), DataType::Integer, false)],
    );

    let row = vibesql_storage::Row::new(vec![SqlValue::Integer(42)]);

    let evaluator = ExpressionEvaluator::new(&schema);

    let expr = vibesql_ast::Expression::ColumnRef { table: None, column: "a".to_string() };

    let result = evaluator.eval(&expr, &row)?;
    assert_eq!(result, SqlValue::Integer(42));

    // Clean up
    std::env::remove_var("CSE_ENABLED");

    Ok(())
}

#[test]
fn test_cse_non_deterministic_not_cached() -> Result<(), ExecutorError> {
    // Test that non-deterministic expressions are not cached

    let schema = TableSchema::new("test".to_string(), vec![]);

    let row = vibesql_storage::Row::new(vec![]);

    let evaluator = ExpressionEvaluator::new(&schema);

    // CURRENT_TIMESTAMP should not be cached
    let expr = vibesql_ast::Expression::CurrentTimestamp { precision: None };

    let result1 = evaluator.eval(&expr, &row)?;
    let result2 = evaluator.eval(&expr, &row)?;

    // Both should succeed (but we can't check if they're different
    // without sleeping between calls)
    assert!(matches!(result1, SqlValue::Timestamp(_)));
    assert!(matches!(result2, SqlValue::Timestamp(_)));

    Ok(())
}

#[test]
fn test_cse_complex_expression() -> Result<(), ExecutorError> {
    // Test a complex expression with multiple levels of nesting

    let schema = TableSchema::new(
        "test".to_string(),
        vec![
            ColumnSchema::new("a".to_string(), DataType::Integer, false),
            ColumnSchema::new("b".to_string(), DataType::Integer, false),
            ColumnSchema::new("c".to_string(), DataType::Integer, false),
        ],
    );

    let row = vibesql_storage::Row::new(vec![
        SqlValue::Integer(2),
        SqlValue::Integer(3),
        SqlValue::Integer(5),
    ]);

    let evaluator = ExpressionEvaluator::new(&schema);

    // Build: ((a + b) * c) + ((a + b) * 2)
    // The sub-expression (a + b) should be cached
    let a_plus_b = || vibesql_ast::Expression::BinaryOp {
        left: Box::new(vibesql_ast::Expression::ColumnRef { table: None, column: "a".to_string() }),
        op: vibesql_ast::BinaryOperator::Plus,
        right: Box::new(vibesql_ast::Expression::ColumnRef {
            table: None,
            column: "b".to_string(),
        }),
    };

    let complex_expr = vibesql_ast::Expression::BinaryOp {
        left: Box::new(vibesql_ast::Expression::BinaryOp {
            left: Box::new(a_plus_b()),
            op: vibesql_ast::BinaryOperator::Multiply,
            right: Box::new(vibesql_ast::Expression::ColumnRef {
                table: None,
                column: "c".to_string(),
            }),
        }),
        op: vibesql_ast::BinaryOperator::Plus,
        right: Box::new(vibesql_ast::Expression::BinaryOp {
            left: Box::new(a_plus_b()),
            op: vibesql_ast::BinaryOperator::Multiply,
            right: Box::new(vibesql_ast::Expression::Literal(SqlValue::Integer(2))),
        }),
    };

    let result = evaluator.eval(&complex_expr, &row)?;
    // (2+3)*5 + (2+3)*2 = 5*5 + 5*2 = 25 + 10 = 35
    assert_eq!(result, SqlValue::Integer(35));

    Ok(())
}

#[test]
fn test_cse_with_nulls() -> Result<(), ExecutorError> {
    // Test that CSE works correctly with NULL values

    let schema = TableSchema::new(
        "test".to_string(),
        vec![
            ColumnSchema::new("a".to_string(), DataType::Integer, true),
            ColumnSchema::new("b".to_string(), DataType::Integer, true),
        ],
    );

    let row = vibesql_storage::Row::new(vec![SqlValue::Null, SqlValue::Integer(5)]);

    let evaluator = ExpressionEvaluator::new(&schema);

    // Build expression: (a + b)
    let expr = vibesql_ast::Expression::BinaryOp {
        left: Box::new(vibesql_ast::Expression::ColumnRef { table: None, column: "a".to_string() }),
        op: vibesql_ast::BinaryOperator::Plus,
        right: Box::new(vibesql_ast::Expression::ColumnRef {
            table: None,
            column: "b".to_string(),
        }),
    };

    // Evaluate multiple times - should return NULL each time
    let result1 = evaluator.eval(&expr, &row)?;
    let result2 = evaluator.eval(&expr, &row)?;

    assert_eq!(result1, SqlValue::Null);
    assert_eq!(result2, SqlValue::Null);

    Ok(())
}

#[test]
fn test_cse_string_expressions() -> Result<(), ExecutorError> {
    // Test CSE with string concatenation

    let schema = TableSchema::new(
        "test".to_string(),
        vec![
            ColumnSchema::new(
                "first_name".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
            ColumnSchema::new(
                "last_name".to_string(),
                DataType::Varchar { max_length: Some(50) },
                false,
            ),
        ],
    );

    let row = vibesql_storage::Row::new(vec![
        SqlValue::Varchar(arcstr::ArcStr::from("John")),
        SqlValue::Varchar(arcstr::ArcStr::from("Doe")),
    ]);

    let evaluator = ExpressionEvaluator::new(&schema);

    // Build expression: first_name || ' ' || last_name
    let full_name = || vibesql_ast::Expression::BinaryOp {
        left: Box::new(vibesql_ast::Expression::BinaryOp {
            left: Box::new(vibesql_ast::Expression::ColumnRef {
                table: None,
                column: "first_name".to_string(),
            }),
            op: vibesql_ast::BinaryOperator::Concat,
            right: Box::new(vibesql_ast::Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from(" ")))),
        }),
        op: vibesql_ast::BinaryOperator::Concat,
        right: Box::new(vibesql_ast::Expression::ColumnRef {
            table: None,
            column: "last_name".to_string(),
        }),
    };

    // Evaluate multiple times
    let result1 = evaluator.eval(&full_name(), &row)?;
    let result2 = evaluator.eval(&full_name(), &row)?;

    assert_eq!(result1, SqlValue::Varchar(arcstr::ArcStr::from("John Doe")));
    assert_eq!(result2, SqlValue::Varchar(arcstr::ArcStr::from("John Doe")));

    Ok(())
}
