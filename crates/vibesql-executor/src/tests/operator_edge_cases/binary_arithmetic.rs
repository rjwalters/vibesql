//! Binary arithmetic operator edge case tests
//!
//! Tests for complex nested arithmetic expressions,
//! operator precedence, and associativity.

use crate::*;

#[test]
fn test_nested_arithmetic() {
    // Use MySQL mode for Numeric division result
    let mut db = vibesql_storage::Database::new();
    db.set_sql_mode(vibesql_types::SqlMode::MySQL {
        flags: vibesql_types::MySqlModeFlags::default(),
    });
    let executor = SelectExecutor::new(&db);

    // SELECT ((5 + 3) * 2) - (10 / 2)
    let stmt = vibesql_ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::BinaryOp {
                left: Box::new(vibesql_ast::Expression::BinaryOp {
                    left: Box::new(vibesql_ast::Expression::BinaryOp {
                        left: Box::new(vibesql_ast::Expression::Literal(
                            vibesql_types::SqlValue::Integer(5),
                        )),
                        op: vibesql_ast::BinaryOperator::Plus,
                        right: Box::new(vibesql_ast::Expression::Literal(
                            vibesql_types::SqlValue::Integer(3),
                        )),
                    }),
                    op: vibesql_ast::BinaryOperator::Multiply,
                    right: Box::new(vibesql_ast::Expression::Literal(
                        vibesql_types::SqlValue::Integer(2),
                    )),
                }),
                op: vibesql_ast::BinaryOperator::Minus,
                right: Box::new(vibesql_ast::Expression::BinaryOp {
                    left: Box::new(vibesql_ast::Expression::Literal(
                        vibesql_types::SqlValue::Integer(10),
                    )),
                    op: vibesql_ast::BinaryOperator::Divide,
                    right: Box::new(vibesql_ast::Expression::Literal(
                        vibesql_types::SqlValue::Integer(2),
                    )),
                }),
            },
            alias: Some("result".to_string()),
        }],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
        into_variables: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    // Division returns Numeric, so the result is Numeric(11.0)
    // (8 * 2) = Integer(16), 10 / 2 = Numeric(5.0), 16 - 5.0 = Numeric(11.0)
    assert!(matches!(result[0].values[0], vibesql_types::SqlValue::Numeric(_)));
    if let vibesql_types::SqlValue::Numeric(n) = result[0].values[0] {
        assert!((n - 11.0).abs() < 0.001); // (8 * 2) - (10 / 2) = 11.0
    }
}

#[test]
fn test_integer_division_basic() {
    let db = vibesql_storage::Database::new();
    let executor = SelectExecutor::new(&db);

    // SELECT 81 DIV 31
    let stmt = vibesql_ast::SelectStmt {
        with_clause: None,
        set_operation: None,
        distinct: false,
        select_list: vec![vibesql_ast::SelectItem::Expression {
            expr: vibesql_ast::Expression::BinaryOp {
                left: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(
                    81,
                ))),
                op: vibesql_ast::BinaryOperator::IntegerDivide,
                right: Box::new(vibesql_ast::Expression::Literal(
                    vibesql_types::SqlValue::Integer(31),
                )),
            },
            alias: Some("result".to_string()),
        }],
        from: None,
        where_clause: None,
        group_by: None,
        having: None,
        order_by: None,
        limit: None,
        offset: None,
        into_table: None,
        into_variables: None,
    };

    let result = executor.execute(&stmt).unwrap();
    assert_eq!(result.len(), 1);
    // 81 / 31 = 2.6129..., truncated to 2
    assert_eq!(result[0].values[0], vibesql_types::SqlValue::Integer(2));
}

#[test]
fn test_integer_division_with_floats() {
    use crate::evaluator::operators::OperatorRegistry;

    // 10.7 DIV 3.2 should return 3 (not 3.34)
    let result = OperatorRegistry::eval_binary_op(
        &vibesql_types::SqlValue::Float(10.7),
        &vibesql_ast::BinaryOperator::IntegerDivide,
        &vibesql_types::SqlValue::Float(3.2),
        vibesql_types::SqlMode::default(),
    )
    .unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Integer(3));
}

#[test]
fn test_integer_division_negative_operands() {
    use crate::evaluator::operators::OperatorRegistry;

    // 96 DIV -2 should return -48
    let result = OperatorRegistry::eval_binary_op(
        &vibesql_types::SqlValue::Integer(96),
        &vibesql_ast::BinaryOperator::IntegerDivide,
        &vibesql_types::SqlValue::Integer(-2),
        vibesql_types::SqlMode::default(),
    )
    .unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Integer(-48));

    // -96 DIV 2 should return -48
    let result = OperatorRegistry::eval_binary_op(
        &vibesql_types::SqlValue::Integer(-96),
        &vibesql_ast::BinaryOperator::IntegerDivide,
        &vibesql_types::SqlValue::Integer(2),
        vibesql_types::SqlMode::default(),
    )
    .unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Integer(-48));

    // -96 DIV -2 should return 48
    let result = OperatorRegistry::eval_binary_op(
        &vibesql_types::SqlValue::Integer(-96),
        &vibesql_ast::BinaryOperator::IntegerDivide,
        &vibesql_types::SqlValue::Integer(-2),
        vibesql_types::SqlMode::default(),
    )
    .unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Integer(48));
}

#[test]
fn test_integer_division_by_zero() {
    use crate::evaluator::operators::OperatorRegistry;

    // 5 DIV 0 should return NULL (SQL standard behavior)
    let result = OperatorRegistry::eval_binary_op(
        &vibesql_types::SqlValue::Integer(5),
        &vibesql_ast::BinaryOperator::IntegerDivide,
        &vibesql_types::SqlValue::Integer(0),
        vibesql_types::SqlMode::default(),
    );
    assert_eq!(result.unwrap(), vibesql_types::SqlValue::Null);
}

#[test]
fn test_integer_division_equal_operands() {
    use crate::evaluator::operators::OperatorRegistry;

    // 5 DIV 5 should return 1
    let result = OperatorRegistry::eval_binary_op(
        &vibesql_types::SqlValue::Integer(5),
        &vibesql_ast::BinaryOperator::IntegerDivide,
        &vibesql_types::SqlValue::Integer(5),
        vibesql_types::SqlMode::default(),
    )
    .unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Integer(1));
}

#[test]
fn test_modulo_operator() {
    use crate::evaluator::operators::OperatorRegistry;

    // 10 % 3 should return 1
    let result = OperatorRegistry::eval_binary_op(
        &vibesql_types::SqlValue::Integer(10),
        &vibesql_ast::BinaryOperator::Modulo,
        &vibesql_types::SqlValue::Integer(3),
        vibesql_types::SqlMode::default(),
    )
    .unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Integer(1));

    // 15 % 4 should return 3
    let result = OperatorRegistry::eval_binary_op(
        &vibesql_types::SqlValue::Integer(15),
        &vibesql_ast::BinaryOperator::Modulo,
        &vibesql_types::SqlValue::Integer(4),
        vibesql_types::SqlMode::default(),
    )
    .unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Integer(3));
}
