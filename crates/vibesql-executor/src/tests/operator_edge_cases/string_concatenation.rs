//! String concatenation edge case tests
//!
//! Tests for string concatenation including empty strings,
//! NULL handling, and complex multi-part concatenations.

use super::operator_test_utils::*;

#[test]
fn test_string_concat_basic() {
    let db = vibesql_storage::Database::new();
    let expr = vibesql_ast::Expression::BinaryOp {
        left: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
            "Hello".to_string(),
        ))),
        op: vibesql_ast::BinaryOperator::Concat,
        right: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
            " World".to_string(),
        ))),
    };
    assert_expression_result(
        &db,
        expr,
        vibesql_types::SqlValue::Varchar("Hello World".to_string()),
    );
}

#[test]
fn test_string_concat_empty() {
    let db = vibesql_storage::Database::new();
    let expr = vibesql_ast::Expression::BinaryOp {
        left: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
            "Hello".to_string(),
        ))),
        op: vibesql_ast::BinaryOperator::Concat,
        right: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
            "".to_string(),
        ))),
    };
    assert_expression_result(&db, expr, vibesql_types::SqlValue::Varchar("Hello".to_string()));
}

#[test]
fn test_string_concat_null() {
    let db = vibesql_storage::Database::new();
    let expr = vibesql_ast::Expression::BinaryOp {
        left: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
            "Hello".to_string(),
        ))),
        op: vibesql_ast::BinaryOperator::Concat,
        right: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Null)),
    };
    assert_expression_result(&db, expr, vibesql_types::SqlValue::Null);
}

#[test]
fn test_string_concat_multiple() {
    let db = vibesql_storage::Database::new();

    // "Hello" || " " || "Beautiful" || " " || "World"
    let expr = vibesql_ast::Expression::BinaryOp {
        left: Box::new(vibesql_ast::Expression::BinaryOp {
            left: Box::new(vibesql_ast::Expression::BinaryOp {
                left: Box::new(vibesql_ast::Expression::BinaryOp {
                    left: Box::new(vibesql_ast::Expression::Literal(
                        vibesql_types::SqlValue::Varchar("Hello".to_string()),
                    )),
                    op: vibesql_ast::BinaryOperator::Concat,
                    right: Box::new(vibesql_ast::Expression::Literal(
                        vibesql_types::SqlValue::Varchar(" ".to_string()),
                    )),
                }),
                op: vibesql_ast::BinaryOperator::Concat,
                right: Box::new(vibesql_ast::Expression::Literal(
                    vibesql_types::SqlValue::Varchar("Beautiful".to_string()),
                )),
            }),
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

    assert_expression_result(
        &db,
        expr,
        vibesql_types::SqlValue::Varchar("Hello Beautiful World".to_string()),
    );
}
