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
        expr: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("hello")))),
    };
    assert_expression_result(&db, expr, vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("hello")));
}

#[test]
fn test_unary_minus_invalid_type() {
    let db = vibesql_storage::Database::new();
    let expr = vibesql_ast::Expression::UnaryOp {
        op: vibesql_ast::UnaryOperator::Minus,
        expr: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("hello")))),
    };
    assert_type_mismatch(&db, expr);
}
