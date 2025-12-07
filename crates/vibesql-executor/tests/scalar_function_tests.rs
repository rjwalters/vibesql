//! Tests for new scalar functions (numeric and string functions)

// Allow approximate constants in tests - these are test data values, not mathematical constants
#![allow(clippy::approx_constant)]

mod common;

use common::create_test_evaluator;

// ==================== NUMERIC FUNCTION TESTS ====================

#[test]
fn test_abs_positive() {
    let (evaluator, row) = create_test_evaluator();

    let expr = vibesql_ast::Expression::Function {
        name: "ABS".to_string(),
        args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Double(-5.2))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Double(5.2));
}

#[test]
fn test_abs_integer() {
    let (evaluator, row) = create_test_evaluator();

    let expr = vibesql_ast::Expression::Function {
        name: "ABS".to_string(),
        args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(-42))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Integer(42));
}

#[test]
fn test_round_basic() {
    let (evaluator, row) = create_test_evaluator();

    let expr = vibesql_ast::Expression::Function {
        name: "ROUND".to_string(),
        args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Double(3.7))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Double(4.0));
}

#[test]
fn test_round_with_precision() {
    let (evaluator, row) = create_test_evaluator();

    let expr = vibesql_ast::Expression::Function {
        name: "ROUND".to_string(),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Double(3.14159)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(2)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Double(3.14));
}

#[test]
fn test_floor_function() {
    let (evaluator, row) = create_test_evaluator();

    let expr = vibesql_ast::Expression::Function {
        name: "FLOOR".to_string(),
        args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Double(3.9))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Double(3.0));
}

#[test]
fn test_ceil_function() {
    let (evaluator, row) = create_test_evaluator();

    let expr = vibesql_ast::Expression::Function {
        name: "CEIL".to_string(),
        args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Double(3.1))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Double(4.0));
}

#[test]
fn test_ceiling_function() {
    let (evaluator, row) = create_test_evaluator();

    let expr = vibesql_ast::Expression::Function {
        name: "CEILING".to_string(),
        args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Double(3.1))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Double(4.0));
}

#[test]
fn test_mod_function() {
    let (evaluator, row) = create_test_evaluator();

    let expr = vibesql_ast::Expression::Function {
        name: "MOD".to_string(),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(17)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(5)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Integer(2));
}

#[test]
fn test_power_function() {
    let (evaluator, row) = create_test_evaluator();

    let expr = vibesql_ast::Expression::Function {
        name: "POWER".to_string(),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(2)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(3)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Double(8.0));
}

#[test]
fn test_pow_alias() {
    let (evaluator, row) = create_test_evaluator();

    let expr = vibesql_ast::Expression::Function {
        name: "POW".to_string(),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(5)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(2)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Double(25.0));
}

#[test]
fn test_sqrt_function() {
    let (evaluator, row) = create_test_evaluator();

    let expr = vibesql_ast::Expression::Function {
        name: "SQRT".to_string(),
        args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(16))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Double(4.0));
}

// ==================== STRING FUNCTION TESTS ====================

#[test]
fn test_concat_function() {
    let (evaluator, row) = create_test_evaluator();

    let expr = vibesql_ast::Expression::Function {
        name: "CONCAT".to_string(),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Hello"))),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(" "))),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("World"))),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Hello World")));
}

#[test]
fn test_concat_with_integer() {
    let (evaluator, row) = create_test_evaluator();

    let expr = vibesql_ast::Expression::Function {
        name: "CONCAT".to_string(),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("ID:"))),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(42)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("ID:42")));
}

#[test]
fn test_length_function() {
    let (evaluator, row) = create_test_evaluator();

    let expr = vibesql_ast::Expression::Function {
        name: "LENGTH".to_string(),
        args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Hello")))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Integer(5));
}

#[test]
fn test_position_function() {
    let (evaluator, row) = create_test_evaluator();

    let expr = vibesql_ast::Expression::Function {
        name: "POSITION".to_string(),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("lo"))),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Hello"))),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Integer(4)); // 'lo' starts at position 4 (1-indexed)
}

#[test]
fn test_position_not_found() {
    let (evaluator, row) = create_test_evaluator();

    let expr = vibesql_ast::Expression::Function {
        name: "POSITION".to_string(),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("xyz"))),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Hello"))),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Integer(0)); // Not found returns 0
}

#[test]
fn test_replace_function() {
    let (evaluator, row) = create_test_evaluator();

    let expr = vibesql_ast::Expression::Function {
        name: "REPLACE".to_string(),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Hello World"))),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("World"))),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Rust"))),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Hello Rust")));
}

#[test]
fn test_reverse_function() {
    let (evaluator, row) = create_test_evaluator();

    let expr = vibesql_ast::Expression::Function {
        name: "REVERSE".to_string(),
        args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Hello")))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("olleH")));
}

#[test]
fn test_left_function() {
    let (evaluator, row) = create_test_evaluator();

    let expr = vibesql_ast::Expression::Function {
        name: "LEFT".to_string(),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Hello"))),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(3)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Hel")));
}

#[test]
fn test_right_function() {
    let (evaluator, row) = create_test_evaluator();

    let expr = vibesql_ast::Expression::Function {
        name: "RIGHT".to_string(),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Hello"))),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(3)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("llo")));
}

// ==================== NULL HANDLING TESTS ====================

#[test]
fn test_abs_with_null() {
    let (evaluator, row) = create_test_evaluator();

    let expr = vibesql_ast::Expression::Function {
        name: "ABS".to_string(),
        args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Null)],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Null);
}

#[test]
fn test_concat_with_null() {
    let (evaluator, row) = create_test_evaluator();

    let expr = vibesql_ast::Expression::Function {
        name: "CONCAT".to_string(),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Hello"))),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Null),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Null); // NULL in concat makes result NULL
}

#[test]
fn test_length_with_null() {
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
fn test_octet_length_ascii() {
    let (evaluator, row) = create_test_evaluator();

    let expr = vibesql_ast::Expression::Function {
        name: "OCTET_LENGTH".to_string(),
        args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("foo")))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Integer(3));
}

#[test]
fn test_octet_length_empty_string() {
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

    // Emoji is 4 bytes in UTF-8
    let expr = vibesql_ast::Expression::Function {
        name: "OCTET_LENGTH".to_string(),
        args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("🦀")))],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Integer(4));
}

#[test]
fn test_octet_length_with_null() {
    let (evaluator, row) = create_test_evaluator();

    let expr = vibesql_ast::Expression::Function {
        name: "OCTET_LENGTH".to_string(),
        args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Null)],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Null);
}

// ==================== NESTED FUNCTION TESTS ====================

#[test]
fn test_nested_functions() {
    let (evaluator, row) = create_test_evaluator();

    // ABS(ROUND(-3.7)) should equal 4.0
    let expr = vibesql_ast::Expression::Function {
        name: "ABS".to_string(),
        args: vec![vibesql_ast::Expression::Function {
            name: "ROUND".to_string(),
            args: vec![vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Double(-3.7))],
            character_unit: None,
        }],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Double(4.0));
}

#[test]
fn test_upper_left_nested() {
    let (evaluator, row) = create_test_evaluator();

    // UPPER(LEFT('hello', 3)) should equal 'HEL'
    let expr = vibesql_ast::Expression::Function {
        name: "UPPER".to_string(),
        args: vec![vibesql_ast::Expression::Function {
            name: "LEFT".to_string(),
            args: vec![
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("hello"))),
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(3)),
            ],
            character_unit: None,
        }],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("HEL")));
}
