//! Expression evaluator tests
//!
//! Tests for evaluating SQL expressions including literals, column references,
//! and binary operations.

use super::super::*;

#[test]
fn test_eval_integer_literal() {
    let schema = vibesql_catalog::TableSchema::new("test".to_string(), vec![]);
    let evaluator = ExpressionEvaluator::new(&schema);
    let row = vibesql_storage::Row::new(vec![]);

    let expr = vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(42));
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Integer(42));
}

#[test]
fn test_eval_string_literal() {
    let schema = vibesql_catalog::TableSchema::new("test".to_string(), vec![]);
    let evaluator = ExpressionEvaluator::new(&schema);
    let row = vibesql_storage::Row::new(vec![]);

    let expr =
        vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar("hello".to_string()));
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Varchar("hello".to_string()));
}

#[test]
fn test_eval_null_literal() {
    let schema = vibesql_catalog::TableSchema::new("test".to_string(), vec![]);
    let evaluator = ExpressionEvaluator::new(&schema);
    let row = vibesql_storage::Row::new(vec![]);

    let expr = vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Null);
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Null);
}

#[test]
fn test_eval_column_ref() {
    let schema = vibesql_catalog::TableSchema::new(
        "users".to_string(),
        vec![
            vibesql_catalog::ColumnSchema::new(
                "id".to_string(),
                vibesql_types::DataType::Integer,
                false,
            ),
            vibesql_catalog::ColumnSchema::new(
                "name".to_string(),
                vibesql_types::DataType::Varchar { max_length: Some(100) },
                true,
            ),
        ],
    );
    let evaluator = ExpressionEvaluator::new(&schema);
    let row = vibesql_storage::Row::new(vec![
        vibesql_types::SqlValue::Integer(1),
        vibesql_types::SqlValue::Varchar("Alice".to_string()),
    ]);

    let expr = vibesql_ast::Expression::ColumnRef { table: None, column: "id".to_string() };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Integer(1));

    let expr = vibesql_ast::Expression::ColumnRef { table: None, column: "name".to_string() };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Varchar("Alice".to_string()));
}

#[test]
fn test_eval_column_not_found() {
    let schema = vibesql_catalog::TableSchema::new(
        "users".to_string(),
        vec![vibesql_catalog::ColumnSchema::new(
            "id".to_string(),
            vibesql_types::DataType::Integer,
            false,
        )],
    );
    let evaluator = ExpressionEvaluator::new(&schema);
    let row = vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(1)]);

    let expr = vibesql_ast::Expression::ColumnRef { table: None, column: "missing".to_string() };
    let err = evaluator.eval(&expr, &row).unwrap_err();
    match err {
        ExecutorError::ColumnNotFound { column_name, .. } => assert_eq!(column_name, "missing"),
        other => panic!("Expected ColumnNotFound, got {:?}", other),
    }
}

#[test]
fn test_eval_addition() {
    let schema = vibesql_catalog::TableSchema::new("test".to_string(), vec![]);
    let evaluator = ExpressionEvaluator::new(&schema);
    let row = vibesql_storage::Row::new(vec![]);

    let expr = vibesql_ast::Expression::BinaryOp {
        left: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(10))),
        op: vibesql_ast::BinaryOperator::Plus,
        right: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(5))),
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Integer(15));
}

#[test]
fn test_eval_null_in_addition() {
    let schema = vibesql_catalog::TableSchema::new("test".to_string(), vec![]);
    let evaluator = ExpressionEvaluator::new(&schema);
    let row = vibesql_storage::Row::new(vec![]);

    // NULL + 5 = NULL
    let expr = vibesql_ast::Expression::BinaryOp {
        left: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Null)),
        op: vibesql_ast::BinaryOperator::Plus,
        right: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(5))),
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Null);
}

#[test]
fn test_eval_null_in_subtraction() {
    let schema = vibesql_catalog::TableSchema::new("test".to_string(), vec![]);
    let evaluator = ExpressionEvaluator::new(&schema);
    let row = vibesql_storage::Row::new(vec![]);

    // 10 - NULL = NULL
    let expr = vibesql_ast::Expression::BinaryOp {
        left: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(10))),
        op: vibesql_ast::BinaryOperator::Minus,
        right: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Null)),
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Null);
}

#[test]
fn test_eval_null_in_multiplication() {
    let schema = vibesql_catalog::TableSchema::new("test".to_string(), vec![]);
    let evaluator = ExpressionEvaluator::new(&schema);
    let row = vibesql_storage::Row::new(vec![]);

    // NULL * 5 = NULL
    let expr = vibesql_ast::Expression::BinaryOp {
        left: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Null)),
        op: vibesql_ast::BinaryOperator::Multiply,
        right: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(5))),
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Null);
}

#[test]
fn test_eval_null_in_division() {
    let schema = vibesql_catalog::TableSchema::new("test".to_string(), vec![]);
    let evaluator = ExpressionEvaluator::new(&schema);
    let row = vibesql_storage::Row::new(vec![]);

    // NULL / 5 = NULL
    let expr = vibesql_ast::Expression::BinaryOp {
        left: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Null)),
        op: vibesql_ast::BinaryOperator::Divide,
        right: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(5))),
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Null);
}

#[test]
fn test_eval_division_by_zero() {
    let schema = vibesql_catalog::TableSchema::new("test".to_string(), vec![]);
    let evaluator = ExpressionEvaluator::new(&schema);
    let row = vibesql_storage::Row::new(vec![]);

    // 10 / 0 = NULL (SQL standard behavior)
    let expr = vibesql_ast::Expression::BinaryOp {
        left: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(10))),
        op: vibesql_ast::BinaryOperator::Divide,
        right: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(0))),
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Null);
}

#[test]
fn test_eval_type_mismatch_in_addition() {
    let schema = vibesql_catalog::TableSchema::new("test".to_string(), vec![]);
    let evaluator = ExpressionEvaluator::new(&schema);
    let row = vibesql_storage::Row::new(vec![]);

    // 10 + "hello" = Error
    let expr = vibesql_ast::Expression::BinaryOp {
        left: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(10))),
        op: vibesql_ast::BinaryOperator::Plus,
        right: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
            "hello".to_string(),
        ))),
    };
    let err = evaluator.eval(&expr, &row).unwrap_err();
    assert!(matches!(err, ExecutorError::TypeMismatch { .. }));
}

#[test]
fn test_eval_null_in_comparison() {
    let schema = vibesql_catalog::TableSchema::new("test".to_string(), vec![]);
    let evaluator = ExpressionEvaluator::new(&schema);
    let row = vibesql_storage::Row::new(vec![]);

    // NULL = 5 should return NULL (unknown)
    let expr = vibesql_ast::Expression::BinaryOp {
        left: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Null)),
        op: vibesql_ast::BinaryOperator::Equal,
        right: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(5))),
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Null);
}

#[test]
fn test_eval_subtraction() {
    let schema = vibesql_catalog::TableSchema::new("test".to_string(), vec![]);
    let evaluator = ExpressionEvaluator::new(&schema);
    let row = vibesql_storage::Row::new(vec![]);

    let expr = vibesql_ast::Expression::BinaryOp {
        left: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(10))),
        op: vibesql_ast::BinaryOperator::Minus,
        right: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(3))),
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Integer(7));
}

#[test]
fn test_eval_multiplication() {
    let schema = vibesql_catalog::TableSchema::new("test".to_string(), vec![]);
    let evaluator = ExpressionEvaluator::new(&schema);
    let row = vibesql_storage::Row::new(vec![]);

    let expr = vibesql_ast::Expression::BinaryOp {
        left: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(6))),
        op: vibesql_ast::BinaryOperator::Multiply,
        right: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(7))),
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Integer(42));
}

#[test]
fn test_eval_division() {
    // Use MySQL mode to get Numeric result from integer division
    let mut db = vibesql_storage::Database::new();
    db.set_sql_mode(vibesql_types::SqlMode::MySQL {
        flags: vibesql_types::MySqlModeFlags::default(),
    });
    let schema = vibesql_catalog::TableSchema::new("test".to_string(), vec![]);
    let evaluator = ExpressionEvaluator::with_database(&schema, &db);
    let row = vibesql_storage::Row::new(vec![]);

    let expr = vibesql_ast::Expression::BinaryOp {
        left: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(20))),
        op: vibesql_ast::BinaryOperator::Divide,
        right: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(4))),
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    // Division returns Numeric for integer operands in MySQL mode (exact decimal arithmetic)
    assert_eq!(result, vibesql_types::SqlValue::Numeric(5.0));
}

#[test]
fn test_eval_string_concat_varchar() {
    let schema = vibesql_catalog::TableSchema::new("test".to_string(), vec![]);
    let evaluator = ExpressionEvaluator::new(&schema);
    let row = vibesql_storage::Row::new(vec![]);

    let expr = vibesql_ast::Expression::BinaryOp {
        left: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
            "Hello".to_string(),
        ))),
        op: vibesql_ast::BinaryOperator::Concat,
        right: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
            " World".to_string(),
        ))),
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Varchar("Hello World".to_string()));
}

#[test]
fn test_eval_string_concat_char() {
    let schema = vibesql_catalog::TableSchema::new("test".to_string(), vec![]);
    let evaluator = ExpressionEvaluator::new(&schema);
    let row = vibesql_storage::Row::new(vec![]);

    let expr = vibesql_ast::Expression::BinaryOp {
        left: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Character(
            "Hello".to_string(),
        ))),
        op: vibesql_ast::BinaryOperator::Concat,
        right: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Character(
            " World".to_string(),
        ))),
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Varchar("Hello World".to_string()));
}

#[test]
fn test_eval_string_concat_mixed() {
    let schema = vibesql_catalog::TableSchema::new("test".to_string(), vec![]);
    let evaluator = ExpressionEvaluator::new(&schema);
    let row = vibesql_storage::Row::new(vec![]);

    let expr = vibesql_ast::Expression::BinaryOp {
        left: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
            "Hello".to_string(),
        ))),
        op: vibesql_ast::BinaryOperator::Concat,
        right: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Character(
            " World".to_string(),
        ))),
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Varchar("Hello World".to_string()));
}

#[test]
fn test_eval_string_concat_null() {
    let schema = vibesql_catalog::TableSchema::new("test".to_string(), vec![]);
    let evaluator = ExpressionEvaluator::new(&schema);
    let row = vibesql_storage::Row::new(vec![]);

    let expr = vibesql_ast::Expression::BinaryOp {
        left: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
            "Hello".to_string(),
        ))),
        op: vibesql_ast::BinaryOperator::Concat,
        right: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Null)),
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Null);
}

#[test]
fn test_eval_string_concat_multiple() {
    let schema = vibesql_catalog::TableSchema::new("test".to_string(), vec![]);
    let evaluator = ExpressionEvaluator::new(&schema);
    let row = vibesql_storage::Row::new(vec![]);

    // ("Hello" || " " || "World")
    let expr = vibesql_ast::Expression::BinaryOp {
        left: Box::new(vibesql_ast::Expression::BinaryOp {
            left: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                "Hello".to_string(),
            ))),
            op: vibesql_ast::BinaryOperator::Concat,
            right: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                " ".to_string(),
            ))),
        }),
        op: vibesql_ast::BinaryOperator::Concat,
        right: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
            "World".to_string(),
        ))),
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Varchar("Hello World".to_string()));
}
