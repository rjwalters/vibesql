//! Test suite for string search functions (POSITION, INSTR, LOCATE)
//!
//! Tests cover:
//! - NULL handling (NULL string, NULL search, NULL position)
//! - Empty string searching
//! - Search found and not found cases
//! - Multiple occurrences (returns first)
//! - Start position parameter (for LOCATE)
//! - Both VARCHAR and CHARACTER data types
//! - Error conditions (wrong argument count, wrong type)

use crate::common::create_test_evaluator;

// ============================================================================
// POSITION Tests
// ============================================================================

#[test]
fn test_position_null() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: vibesql_ast::FunctionIdentifier::new("POSITION"),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Null),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                arcstr::ArcStr::from("hello"),
            )),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Null);
}

#[test]
fn test_position_not_found() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: vibesql_ast::FunctionIdentifier::new("POSITION"),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                arcstr::ArcStr::from("xyz"),
            )),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                arcstr::ArcStr::from("hello"),
            )),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Integer(0));
}

#[test]
fn test_position_found() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: vibesql_ast::FunctionIdentifier::new("POSITION"),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                arcstr::ArcStr::from("lo"),
            )),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                arcstr::ArcStr::from("hello"),
            )),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Integer(4)); // 1-indexed
}

#[test]
fn test_position_empty_needle() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: vibesql_ast::FunctionIdentifier::new("POSITION"),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                arcstr::ArcStr::from(""),
            )),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                arcstr::ArcStr::from("hello"),
            )),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    // Empty string is found at position 1
    assert_eq!(result, vibesql_types::SqlValue::Integer(1));
}

#[test]
fn test_position_multiple_occurrences() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: vibesql_ast::FunctionIdentifier::new("POSITION"),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                arcstr::ArcStr::from("l"),
            )),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                arcstr::ArcStr::from("hello"),
            )),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    // Returns first occurrence (position 3, 1-indexed)
    assert_eq!(result, vibesql_types::SqlValue::Integer(3));
}

#[test]
fn test_position_wrong_arg_count() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: vibesql_ast::FunctionIdentifier::new("POSITION"),
        args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
            arcstr::ArcStr::from("hello"),
        ))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err());
}

#[test]
fn test_position_wrong_type() {
    // SQLite compatibility: numeric types are coerced to strings
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: vibesql_ast::FunctionIdentifier::new("POSITION"),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(123)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                arcstr::ArcStr::from("hello"),
            )),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    // "123" is not found in "hello", so returns 0
    assert_eq!(result, vibesql_types::SqlValue::Integer(0));
}

#[test]
fn test_position_character_type() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: vibesql_ast::FunctionIdentifier::new("POSITION"),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Character(
                arcstr::ArcStr::from("lo"),
            )),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Character(
                arcstr::ArcStr::from("hello"),
            )),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Integer(4));
}

// ============================================================================
// INSTR Tests
// ============================================================================

#[test]
fn test_instr_null() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: vibesql_ast::FunctionIdentifier::new("INSTR"),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Null),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                arcstr::ArcStr::from("lo"),
            )),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Null);
}

#[test]
fn test_instr_not_found() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: vibesql_ast::FunctionIdentifier::new("INSTR"),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                arcstr::ArcStr::from("hello"),
            )),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                arcstr::ArcStr::from("xyz"),
            )),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Integer(0));
}

#[test]
fn test_instr_found() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: vibesql_ast::FunctionIdentifier::new("INSTR"),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                arcstr::ArcStr::from("hello"),
            )),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                arcstr::ArcStr::from("ll"),
            )),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Integer(3)); // 1-indexed
}

#[test]
fn test_instr_wrong_arg_count() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: vibesql_ast::FunctionIdentifier::new("INSTR"),
        args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
            arcstr::ArcStr::from("hello"),
        ))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err());
}

#[test]
fn test_instr_integer_coercion() {
    // SQLite coerces integers to strings for INSTR
    // INSTR(123, 'l') -> INSTR('123', 'l') -> 0 (not found)
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: vibesql_ast::FunctionIdentifier::new("INSTR"),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(123)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                arcstr::ArcStr::from("l"),
            )),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Integer(0));
}

#[test]
fn test_instr_character_type() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: vibesql_ast::FunctionIdentifier::new("INSTR"),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Character(
                arcstr::ArcStr::from("hello"),
            )),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Character(
                arcstr::ArcStr::from("ll"),
            )),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Integer(3));
}

// ============================================================================
// LOCATE Tests
// ============================================================================

#[test]
fn test_locate_null() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: vibesql_ast::FunctionIdentifier::new("LOCATE"),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Null),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                arcstr::ArcStr::from("hello"),
            )),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Null);
}

#[test]
fn test_locate_not_found() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: vibesql_ast::FunctionIdentifier::new("LOCATE"),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                arcstr::ArcStr::from("xyz"),
            )),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                arcstr::ArcStr::from("hello"),
            )),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Integer(0));
}

#[test]
fn test_locate_with_start_position() {
    let (evaluator, row) = create_test_evaluator();
    // Find second occurrence of 'l' in 'hello'
    let expr = vibesql_ast::Expression::Function {
        name: vibesql_ast::FunctionIdentifier::new("LOCATE"),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                arcstr::ArcStr::from("l"),
            )),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                arcstr::ArcStr::from("hello"),
            )),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(4)), /* Start after first 'l' */
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Integer(4)); // Found at position 4
}

#[test]
fn test_locate_start_beyond_length() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: vibesql_ast::FunctionIdentifier::new("LOCATE"),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                arcstr::ArcStr::from("l"),
            )),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                arcstr::ArcStr::from("hello"),
            )),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(100)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Integer(0)); // Not found
}

#[test]
fn test_locate_wrong_arg_count() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: vibesql_ast::FunctionIdentifier::new("LOCATE"),
        args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
            arcstr::ArcStr::from("l"),
        ))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err());
}

#[test]
fn test_locate_wrong_type_needle() {
    // SQLite compatibility: numeric types are coerced to strings
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: vibesql_ast::FunctionIdentifier::new("LOCATE"),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(123)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                arcstr::ArcStr::from("hello"),
            )),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    // "123" is not found in "hello", so returns 0
    assert_eq!(result, vibesql_types::SqlValue::Integer(0));
}

#[test]
fn test_locate_wrong_type_start() {
    // SQLite compatibility: string start position is coerced to integer (becomes 0)
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: vibesql_ast::FunctionIdentifier::new("LOCATE"),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                arcstr::ArcStr::from("l"),
            )),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                arcstr::ArcStr::from("hello"),
            )),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                arcstr::ArcStr::from("one"),
            )),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    // "one" coerces to 0, so search starts at beginning, finds "l" at position 3
    assert_eq!(result, vibesql_types::SqlValue::Integer(3));
}

#[test]
fn test_locate_null_start() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: vibesql_ast::FunctionIdentifier::new("LOCATE"),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                arcstr::ArcStr::from("l"),
            )),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                arcstr::ArcStr::from("hello"),
            )),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Null),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Null);
}

#[test]
fn test_locate_character_type() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: vibesql_ast::FunctionIdentifier::new("LOCATE"),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Character(
                arcstr::ArcStr::from("ll"),
            )),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Character(
                arcstr::ArcStr::from("hello"),
            )),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Integer(3));
}
