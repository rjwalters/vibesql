//! String concatenation edge case tests
//!
//! Tests for string concatenation including empty strings,
//! NULL handling, and complex multi-part concatenations.

use super::operator_test_utils::*;

#[test]
fn test_string_concat_basic() {
    let db = vibesql_storage::Database::new();
    let expr = vibesql_ast::Expression::BinaryOp {
        left: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Hello")))),
        op: vibesql_ast::BinaryOperator::Concat,
        right: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(" World")))),
    };
    assert_expression_result(
        &db,
        expr,
        vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Hello World")),
    );
}

#[test]
fn test_string_concat_empty() {
    let db = vibesql_storage::Database::new();
    let expr = vibesql_ast::Expression::BinaryOp {
        left: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Hello")))),
        op: vibesql_ast::BinaryOperator::Concat,
        right: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("")))),
    };
    assert_expression_result(&db, expr, vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Hello")));
}

#[test]
fn test_string_concat_null() {
    let db = vibesql_storage::Database::new();
    let expr = vibesql_ast::Expression::BinaryOp {
        left: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Hello")))),
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
                        vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Hello")),
                    )),
                    op: vibesql_ast::BinaryOperator::Concat,
                    right: Box::new(vibesql_ast::Expression::Literal(
                        vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(" ")),
                    )),
                }),
                op: vibesql_ast::BinaryOperator::Concat,
                right: Box::new(vibesql_ast::Expression::Literal(
                    vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Beautiful")),
                )),
            }),
            op: vibesql_ast::BinaryOperator::Concat,
            right: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(" ")))),
        }),
        op: vibesql_ast::BinaryOperator::Concat,
        right: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("World")))),
    };

    assert_expression_result(
        &db,
        expr,
        vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Hello Beautiful World")),
    );
}
