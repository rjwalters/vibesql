//! Utility and System Function Tests - SQL CORE Phase 3D
//!
//! Tests for:
//! - String utilities: SUBSTR, INSTR, LOCATE, FORMAT
//! - System functions: VERSION, DATABASE, USER

mod common;

use common::create_test_evaluator;

// ============================================================================
// SUBSTR / SUBSTRING Tests
// ============================================================================

#[test]
fn test_substr_basic() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: vibesql_ast::FunctionIdentifier::new("SUBSTR"),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                arcstr::ArcStr::from("Hello World"),
            )),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(5)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("Hello")));
}

#[test]
fn test_substr_to_end() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: vibesql_ast::FunctionIdentifier::new("SUBSTR"),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                arcstr::ArcStr::from("Hello World"),
            )),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(7)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("World")));
}

#[test]
fn test_substr_null() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: vibesql_ast::FunctionIdentifier::new("SUBSTR"),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Null),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1)),
        ],
        character_unit: None,
    };
    assert_eq!(evaluator.eval(&expr, &row).unwrap(), vibesql_types::SqlValue::Null);
}

// ============================================================================
// INSTR Tests
// ============================================================================

#[test]
fn test_instr_found() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: vibesql_ast::FunctionIdentifier::new("INSTR"),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                arcstr::ArcStr::from("Hello World"),
            )),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                arcstr::ArcStr::from("World"),
            )),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Integer(7));
}

#[test]
fn test_instr_not_found() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: vibesql_ast::FunctionIdentifier::new("INSTR"),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                arcstr::ArcStr::from("Hello World"),
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
fn test_instr_null() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: vibesql_ast::FunctionIdentifier::new("INSTR"),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Null),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                arcstr::ArcStr::from("test"),
            )),
        ],
        character_unit: None,
    };
    assert_eq!(evaluator.eval(&expr, &row).unwrap(), vibesql_types::SqlValue::Null);
}

// ============================================================================
// LOCATE Tests
// ============================================================================

#[test]
fn test_locate_basic() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: vibesql_ast::FunctionIdentifier::new("LOCATE"),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                arcstr::ArcStr::from("World"),
            )),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                arcstr::ArcStr::from("Hello World"),
            )),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Integer(7));
}

#[test]
fn test_locate_with_start() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: vibesql_ast::FunctionIdentifier::new("LOCATE"),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                arcstr::ArcStr::from("o"),
            )),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                arcstr::ArcStr::from("Hello World"),
            )),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(6)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Integer(8)); // Second 'o' at position 8
}

// ============================================================================
// FORMAT Tests
// ============================================================================

#[test]
fn test_format_basic() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: vibesql_ast::FunctionIdentifier::new("FORMAT"),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Double(1234567.89)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(2)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("1,234,567.89")));
}

#[test]
fn test_format_zero_decimals() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: vibesql_ast::FunctionIdentifier::new("FORMAT"),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Double(1234567.89)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(0)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("1,234,568")));
    // Rounds
}

#[test]
fn test_format_adds_zeros() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: vibesql_ast::FunctionIdentifier::new("FORMAT"),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(42)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(2)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("42.00")));
}

#[test]
fn test_format_negative() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: vibesql_ast::FunctionIdentifier::new("FORMAT"),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Double(-1234567.89)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(2)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("-1,234,567.89")));
}

#[test]
fn test_format_string_first_arg_routes_to_printf() {
    // SQLite's format() is an alias for printf(): a string first argument is
    // a format string (date3.test 5.0 uses format('%+d days', x))
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: vibesql_ast::FunctionIdentifier::new("FORMAT"),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(
                arcstr::ArcStr::from("%+d days"),
            )),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(5)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("+5 days")));
}

#[test]
fn test_format_null_first_arg_returns_null() {
    // SQLite: format(NULL, ...) is NULL (printf propagates a NULL format)
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: vibesql_ast::FunctionIdentifier::new("FORMAT"),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Null),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(2)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Null);
}

// ============================================================================
// VERSION Tests
// ============================================================================

#[test]
fn test_version() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: vibesql_ast::FunctionIdentifier::new("VERSION"),
        args: vec![],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    match result {
        vibesql_types::SqlValue::Varchar(s) => {
            assert!(
                s.starts_with("NistMemSQL"),
                "VERSION should start with 'NistMemSQL', got: {}",
                s
            );
        }
        _ => panic!("VERSION should return Varchar"),
    }
}

// ============================================================================
// DATABASE / SCHEMA Tests
// ============================================================================

#[test]
fn test_database() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: vibesql_ast::FunctionIdentifier::new("DATABASE"),
        args: vec![],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    match result {
        vibesql_types::SqlValue::Varchar(s) => {
            assert!(!s.is_empty(), "DATABASE should return non-empty string");
        }
        vibesql_types::SqlValue::Null => {
            // NULL is also acceptable if no database selected
        }
        _ => panic!("DATABASE should return Varchar or Null"),
    }
}

#[test]
fn test_schema_alias() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: vibesql_ast::FunctionIdentifier::new("SCHEMA"),
        args: vec![],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    // SCHEMA is an alias for DATABASE
    assert!(matches!(result, vibesql_types::SqlValue::Varchar(_) | vibesql_types::SqlValue::Null));
}

// ============================================================================
// USER / CURRENT_USER Tests
// ============================================================================

#[test]
fn test_user() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: vibesql_ast::FunctionIdentifier::new("USER"),
        args: vec![],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    match result {
        vibesql_types::SqlValue::Varchar(s) => {
            assert!(!s.is_empty(), "USER should return non-empty string");
        }
        _ => panic!("USER should return Varchar"),
    }
}

#[test]
fn test_current_user_alias() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: vibesql_ast::FunctionIdentifier::new("CURRENT_USER"),
        args: vec![],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert!(matches!(result, vibesql_types::SqlValue::Varchar(_)));
}
