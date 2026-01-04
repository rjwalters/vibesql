//! Unary operator edge case tests
//!
//! Tests for unary plus and minus operators on various types,
//! including NULL propagation and type validation.

use super::operator_test_utils::*;

#[test]
fn test_unary_plus_integer() {
    let db = vibesql_storage::Database::new();
    let expr = vibesql_ast::Expression::UnaryOp {
        op: vibesql_ast::UnaryOperator::Plus,
        expr: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(42))),
    };
    assert_expression_result(&db, expr, vibesql_types::SqlValue::Integer(42));
}

#[test]
fn test_unary_plus_float() {
    let db = vibesql_storage::Database::new();
    let expr = vibesql_ast::Expression::UnaryOp {
        op: vibesql_ast::UnaryOperator::Plus,
        expr: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Float(3.5))),
    };
    assert_expression_result(&db, expr, vibesql_types::SqlValue::Float(3.5));
}

#[test]
fn test_unary_minus_integer() {
    let db = vibesql_storage::Database::new();
    let expr = vibesql_ast::Expression::UnaryOp {
        op: vibesql_ast::UnaryOperator::Minus,
        expr: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(42))),
    };
    assert_expression_result(&db, expr, vibesql_types::SqlValue::Integer(-42));
}

#[test]
fn test_unary_minus_negative() {
    let db = vibesql_storage::Database::new();
    let expr = vibesql_ast::Expression::UnaryOp {
        op: vibesql_ast::UnaryOperator::Minus,
        expr: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(-42))),
    };
    assert_expression_result(&db, expr, vibesql_types::SqlValue::Integer(42));
}

#[test]
fn test_unary_minus_numeric_string() {
    let db = vibesql_storage::Database::new();
    let expr = vibesql_ast::Expression::UnaryOp {
        op: vibesql_ast::UnaryOperator::Minus,
        expr: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Numeric(123.45))),
    };
    assert_expression_result(&db, expr, vibesql_types::SqlValue::Numeric(-123.45));
}

#[test]
fn test_unary_minus_negative_numeric() {
    let db = vibesql_storage::Database::new();
    let expr = vibesql_ast::Expression::UnaryOp {
        op: vibesql_ast::UnaryOperator::Minus,
        expr: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Numeric(-123.45))),
    };
    assert_expression_result(&db, expr, vibesql_types::SqlValue::Numeric(123.45));
}

#[test]
fn test_unary_plus_null() {
    let db = vibesql_storage::Database::new();
    let expr = vibesql_ast::Expression::UnaryOp {
        op: vibesql_ast::UnaryOperator::Plus,
        expr: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Null)),
    };
    assert_expression_result(&db, expr, vibesql_types::SqlValue::Null);
}

#[test]
fn test_unary_minus_null() {
    let db = vibesql_storage::Database::new();
    let expr = vibesql_ast::Expression::UnaryOp {
        op: vibesql_ast::UnaryOperator::Minus,
        expr: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Null)),
    };
    assert_expression_result(&db, expr, vibesql_types::SqlValue::Null);
}

#[test]
fn test_unary_plus_text() {
    // SQLite behavior: unary + on text returns text unchanged (identity operation)
    let db = vibesql_storage::Database::new();
    let expr = vibesql_ast::Expression::UnaryOp {
        op: vibesql_ast::UnaryOperator::Plus,
        expr: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
            arcstr::ArcStr::from("hello"),
        ))),
    };
    assert_expression_result(
        &db,
        expr,
        vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("hello")),
    );
}

/// SQLite behavior: unary minus on non-numeric strings converts to 0
/// -"hello" → -0 → 0 (string_to_number("hello") returns 0)
#[test]
fn test_unary_minus_string_converts_to_zero() {
    let db = vibesql_storage::Database::new();
    let expr = vibesql_ast::Expression::UnaryOp {
        op: vibesql_ast::UnaryOperator::Minus,
        expr: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
            arcstr::ArcStr::from("hello"),
        ))),
    };
    // SQLite converts non-numeric strings to 0, then negates: -0 = 0
    assert_expression_result(&db, expr, vibesql_types::SqlValue::Integer(0));
}
