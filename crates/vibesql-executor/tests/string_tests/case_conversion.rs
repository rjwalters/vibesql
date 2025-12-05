//! Test suite for case conversion string functions (UPPER, LOWER)
//!
//! Tests cover:
//! - NULL handling
//! - Empty string handling
//! - Unicode and multi-byte character handling
//! - Mixed case input
//! - Both VARCHAR and CHARACTER data types
//! - Error conditions (wrong argument count, wrong type)

use crate::common::create_test_evaluator;

#[test]
fn test_upper_null() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: "UPPER".to_string(),
        args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Null)],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Null);
}

#[test]
fn test_upper_empty() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: "UPPER".to_string(),
        args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
            "".to_string(),
        ))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Varchar("".to_string()));
}

#[test]
fn test_upper_unicode() {
    let (evaluator, row) = create_test_evaluator();
    // Test Greek, accented characters, emojis
    let expr = vibesql_ast::Expression::Function {
        name: "UPPER".to_string(),
        args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
            "café".to_string(),
        ))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Varchar("CAFÉ".to_string()));
}

#[test]
fn test_upper_mixed_case() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: "UPPER".to_string(),
        args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
            "HeLLo WoRLd".to_string(),
        ))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Varchar("HELLO WORLD".to_string()));
}

#[test]
fn test_upper_character_type() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: "UPPER".to_string(),
        args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Character(
            "test".to_string(),
        ))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Varchar("TEST".to_string()));
}

#[test]
fn test_upper_wrong_arg_count() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: "UPPER".to_string(),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar("test".to_string())),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar("extra".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err());
}

#[test]
fn test_upper_wrong_type() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: "UPPER".to_string(),
        args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(123))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err());
}

#[test]
fn test_lower_null() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: "LOWER".to_string(),
        args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Null)],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Null);
}

#[test]
fn test_lower_empty() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: "LOWER".to_string(),
        args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
            "".to_string(),
        ))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Varchar("".to_string()));
}

#[test]
fn test_lower_unicode() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: "LOWER".to_string(),
        args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
            "CAFÉ".to_string(),
        ))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Varchar("café".to_string()));
}

#[test]
fn test_lower_wrong_arg_count() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: "LOWER".to_string(),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar("TEST".to_string())),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar("extra".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err());
}

#[test]
fn test_lower_wrong_type() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: "LOWER".to_string(),
        args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(123))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err());
}

#[test]
fn test_lower_character_type() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: "LOWER".to_string(),
        args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Character(
            "TEST".to_string(),
        ))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Varchar("test".to_string()));
}
