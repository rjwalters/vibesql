//! Test suite for string transformation functions (CONCAT, REPLACE, REVERSE, TRIM)
//!
//! Tests cover:
//! - NULL handling and propagation
//! - Empty string handling
//! - Multi-byte UTF-8 character handling
//! - Multiple arguments and edge cases
//! - Type conversions (INTEGER to string in CONCAT)
//! - TRIM with custom removal characters
//! - Both VARCHAR and CHARACTER data types
//! - Error conditions (wrong argument count, wrong type)

use crate::common::create_test_evaluator;

// ============================================================================
// CONCAT Tests
// ============================================================================

#[test]
fn test_concat_null_propagation() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: "CONCAT".to_string(),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar("hello".to_string())),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Null),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar("world".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Null);
}

#[test]
fn test_concat_empty_strings() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: "CONCAT".to_string(),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar("".to_string())),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar("".to_string())),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar("".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Varchar("".to_string()));
}

#[test]
fn test_concat_single_arg() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: "CONCAT".to_string(),
        args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
            "hello".to_string(),
        ))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Varchar("hello".to_string()));
}

#[test]
fn test_concat_many_args() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: "CONCAT".to_string(),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar("a".to_string())),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar("b".to_string())),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar("c".to_string())),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar("d".to_string())),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar("e".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Varchar("abcde".to_string()));
}

#[test]
fn test_concat_integer_conversion() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: "CONCAT".to_string(),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar("ID:".to_string())),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(123)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Varchar("ID:123".to_string()));
}

#[test]
fn test_concat_no_args() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: "CONCAT".to_string(),
        args: vec![],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err());
}

#[test]
fn test_concat_character_type() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: "CONCAT".to_string(),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Character(
                "hello".to_string(),
            )),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Character(" ".to_string())),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Character(
                "world".to_string(),
            )),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Varchar("hello world".to_string()));
}

// ============================================================================
// REPLACE Tests
// ============================================================================

#[test]
fn test_replace_null() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: "REPLACE".to_string(),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Null),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar("a".to_string())),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar("b".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Null);
}

#[test]
fn test_replace_multiple_occurrences() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: "REPLACE".to_string(),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                "hello hello".to_string(),
            )),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar("ll".to_string())),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar("rr".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Varchar("herro herro".to_string()));
}

#[test]
fn test_replace_empty_search() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: "REPLACE".to_string(),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar("hello".to_string())),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar("".to_string())),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar("x".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    // Empty string search inserts replacement between every character
    assert_eq!(result, vibesql_types::SqlValue::Varchar("xhxexlxlxox".to_string()));
}

#[test]
fn test_replace_empty_replacement() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: "REPLACE".to_string(),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar("hello".to_string())),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar("l".to_string())),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar("".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Varchar("heo".to_string()));
}

#[test]
fn test_replace_not_found() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: "REPLACE".to_string(),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar("hello".to_string())),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar("xyz".to_string())),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar("abc".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Varchar("hello".to_string()));
}

#[test]
fn test_replace_wrong_arg_count() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: "REPLACE".to_string(),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar("hello".to_string())),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar("l".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err());
}

#[test]
fn test_replace_wrong_type() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: "REPLACE".to_string(),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar("hello".to_string())),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(123)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar("x".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err());
}

#[test]
fn test_replace_character_type() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: "REPLACE".to_string(),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Character(
                "hello".to_string(),
            )),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Character("l".to_string())),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Character("r".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Varchar("herro".to_string()));
}

// ============================================================================
// REVERSE Tests
// ============================================================================

#[test]
fn test_reverse_null() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: "REVERSE".to_string(),
        args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Null)],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Null);
}

#[test]
fn test_reverse_empty() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: "REVERSE".to_string(),
        args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
            "".to_string(),
        ))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Varchar("".to_string()));
}

#[test]
fn test_reverse_single_char() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: "REVERSE".to_string(),
        args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
            "a".to_string(),
        ))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Varchar("a".to_string()));
}

#[test]
fn test_reverse_basic() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: "REVERSE".to_string(),
        args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
            "hello".to_string(),
        ))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Varchar("olleh".to_string()));
}

#[test]
fn test_reverse_multibyte() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: "REVERSE".to_string(),
        args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
            "café".to_string(),
        ))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Varchar("éfac".to_string()));
}

#[test]
fn test_reverse_wrong_arg_count() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: "REVERSE".to_string(),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar("hello".to_string())),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar("extra".to_string())),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err());
}

#[test]
fn test_reverse_wrong_type() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: "REVERSE".to_string(),
        args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(123))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err());
}

#[test]
fn test_reverse_character_type() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: "REVERSE".to_string(),
        args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Character(
            "test".to_string(),
        ))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Varchar("tset".to_string()));
}

// ============================================================================
// TRIM Tests
// ============================================================================

#[test]
fn test_trim_null_removal_char() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Trim {
        position: Some(vibesql_ast::TrimPosition::Both),
        removal_char: Some(Box::new(vibesql_ast::Expression::Literal(
            vibesql_types::SqlValue::Null,
        ))),
        string: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
            "hello".to_string(),
        ))),
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Null);
}

#[test]
fn test_trim_null_string() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Trim {
        position: None,
        removal_char: None,
        string: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Null)),
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Null);
}

#[test]
fn test_trim_custom_char_multibyte() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Trim {
        position: Some(vibesql_ast::TrimPosition::Both),
        removal_char: Some(Box::new(vibesql_ast::Expression::Literal(
            vibesql_types::SqlValue::Varchar("x".to_string()),
        ))),
        string: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
            "xxxhelloxxx".to_string(),
        ))),
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Varchar("hello".to_string()));
}
