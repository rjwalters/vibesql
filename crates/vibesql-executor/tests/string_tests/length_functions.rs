//! Test suite for string length functions (LENGTH, CHAR_LENGTH, OCTET_LENGTH)
//!
//! Tests cover:
//! - NULL handling
//! - Empty string handling
//! - Multi-byte UTF-8 character handling
//! - Character vs byte counting (CHAR_LENGTH vs OCTET_LENGTH)
//! - Character unit specifications (CHARACTERS vs OCTETS)
//! - Both VARCHAR and CHARACTER data types
//! - Error conditions (wrong argument count, wrong type)

use crate::common::create_test_evaluator;

#[test]
fn test_char_length_null() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: "CHAR_LENGTH".to_string(),
        args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Null)],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Null);
}

#[test]
fn test_char_length_empty() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: "CHAR_LENGTH".to_string(),
        args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("")))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Integer(0));
}

#[test]
fn test_char_length_multibyte() {
    let (evaluator, row) = create_test_evaluator();
    // "café" is 4 characters but 5 bytes
    let expr = vibesql_ast::Expression::Function {
        name: "CHAR_LENGTH".to_string(),
        args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("café")))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Integer(4));
}

#[test]
fn test_char_length_using_characters() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: "CHAR_LENGTH".to_string(),
        args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("café")))],
        character_unit: Some(vibesql_ast::CharacterUnit::Characters),
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Integer(4));
}

#[test]
fn test_char_length_using_octets() {
    let (evaluator, row) = create_test_evaluator();
    // "café" is 5 bytes in UTF-8 (c=1, a=1, f=1, é=2)
    let expr = vibesql_ast::Expression::Function {
        name: "CHAR_LENGTH".to_string(),
        args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("café")))],
        character_unit: Some(vibesql_ast::CharacterUnit::Octets),
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Integer(5));
}

#[test]
fn test_octet_length_null() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: "OCTET_LENGTH".to_string(),
        args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Null)],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Null);
}

#[test]
fn test_octet_length_empty() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: "OCTET_LENGTH".to_string(),
        args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("")))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Integer(0));
}

#[test]
fn test_octet_length_multibyte() {
    let (evaluator, row) = create_test_evaluator();
    // "café" is 5 bytes in UTF-8
    let expr = vibesql_ast::Expression::Function {
        name: "OCTET_LENGTH".to_string(),
        args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("café")))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Integer(5));
}

#[test]
fn test_octet_length_vs_char_length() {
    let (evaluator, row) = create_test_evaluator();

    // ASCII: same count
    let ascii_expr = vibesql_ast::Expression::Function {
        name: "CHAR_LENGTH".to_string(),
        args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("hello")))],
        character_unit: None,
    };
    let char_result = evaluator.eval(&ascii_expr, &row).unwrap();

    let octet_expr = vibesql_ast::Expression::Function {
        name: "OCTET_LENGTH".to_string(),
        args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("hello")))],
        character_unit: None,
    };
    let octet_result = evaluator.eval(&octet_expr, &row).unwrap();

    assert_eq!(char_result, vibesql_types::SqlValue::Integer(5));
    assert_eq!(octet_result, vibesql_types::SqlValue::Integer(5));
}

#[test]
fn test_length_null() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: "LENGTH".to_string(),
        args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Null)],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Null);
}

#[test]
fn test_length_empty() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: "LENGTH".to_string(),
        args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("")))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Integer(0));
}

#[test]
fn test_length_basic() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: "LENGTH".to_string(),
        args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("hello")))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Integer(5));
}

#[test]
fn test_length_multibyte() {
    let (evaluator, row) = create_test_evaluator();
    // LENGTH returns byte count (unlike CHAR_LENGTH)
    let expr = vibesql_ast::Expression::Function {
        name: "LENGTH".to_string(),
        args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("café")))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    // "café" is 5 bytes in UTF-8
    assert_eq!(result, vibesql_types::SqlValue::Integer(5));
}

#[test]
fn test_length_wrong_arg_count() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: "LENGTH".to_string(),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("hello"))),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("extra"))),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err());
}

#[test]
fn test_length_wrong_type() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: "LENGTH".to_string(),
        args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(123))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err());
}

#[test]
fn test_char_length_wrong_arg_count() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: "CHAR_LENGTH".to_string(),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("hello"))),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("extra"))),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err());
}

#[test]
fn test_char_length_wrong_type() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: "CHAR_LENGTH".to_string(),
        args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(123))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err());
}

#[test]
fn test_octet_length_wrong_arg_count() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: "OCTET_LENGTH".to_string(),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("hello"))),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("extra"))),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err());
}

#[test]
fn test_octet_length_wrong_type() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: "OCTET_LENGTH".to_string(),
        args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(123))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err());
}

#[test]
fn test_char_length_character_type() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: "CHAR_LENGTH".to_string(),
        args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Character(arcstr::ArcStr::from("test")))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Integer(4));
}

#[test]
fn test_octet_length_character_type() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: "OCTET_LENGTH".to_string(),
        args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Character(arcstr::ArcStr::from("café")))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Integer(5));
}

#[test]
fn test_length_character_type() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: "LENGTH".to_string(),
        args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Character(arcstr::ArcStr::from("test")))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Integer(4));
}
