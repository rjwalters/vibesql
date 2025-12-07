//! Tests for string functions, particularly SUBSTRING syntax variants

mod common;

use common::create_test_evaluator;

// ============================================================================
// SUBSTRING Function Tests
// ============================================================================

#[test]
fn test_substring_from_for() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: "SUBSTRING".to_string(),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("hello"))),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(2)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(3)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("ell")));
}

#[test]
fn test_substring_from_only() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Function {
        name: "SUBSTRING".to_string(),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("hello"))),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(2)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("ello")));
}

#[test]
fn test_substring_both_syntaxes_equivalent() {
    let (evaluator, row) = create_test_evaluator();

    // Test comma syntax
    let comma_expr = vibesql_ast::Expression::Function {
        name: "SUBSTRING".to_string(),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("hello"))),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(2)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(3)),
        ],
        character_unit: None,
    };
    let comma_result = evaluator.eval(&comma_expr, &row).unwrap();

    // Test FROM/FOR syntax (same AST)
    let from_for_expr = vibesql_ast::Expression::Function {
        name: "SUBSTRING".to_string(),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("hello"))),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(2)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(3)),
        ],
        character_unit: None,
    };
    let from_for_result = evaluator.eval(&from_for_expr, &row).unwrap();

    assert_eq!(comma_result, from_for_result);
    assert_eq!(comma_result, vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("ell")));
}

// ============================================================================
// TRIM Function Tests
// ============================================================================

#[test]
fn test_trim_from_no_char() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Trim {
        position: None,     // Defaults to BOTH
        removal_char: None, // Defaults to space
        string: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("  hello  ")))),
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("hello")));
}

#[test]
fn test_trim_both_from_no_char() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Trim {
        position: Some(vibesql_ast::TrimPosition::Both),
        removal_char: None, // Defaults to space
        string: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("  hello  ")))),
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("hello")));
}

#[test]
fn test_trim_leading_from_no_char() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Trim {
        position: Some(vibesql_ast::TrimPosition::Leading),
        removal_char: None, // Defaults to space
        string: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("  hello  ")))),
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("hello  ")));
}

#[test]
fn test_trim_trailing_from_no_char() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Trim {
        position: Some(vibesql_ast::TrimPosition::Trailing),
        removal_char: None, // Defaults to space
        string: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("  hello  ")))),
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("  hello")));
}

#[test]
fn test_trim_from_only_spaces() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Trim {
        position: None,
        removal_char: None, // Defaults to space
        string: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("    ")))),
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("")));
}

#[test]
fn test_trim_from_empty_string() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Trim {
        position: None,
        removal_char: None, // Defaults to space
        string: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("")))),
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("")));
}

#[test]
fn test_trim_from_no_spaces() {
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Trim {
        position: None,
        removal_char: None, // Defaults to space
        string: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("hello")))),
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("hello")));
}

#[test]
fn test_trim_with_char_still_works() {
    // Verify existing functionality is preserved
    let (evaluator, row) = create_test_evaluator();
    let expr = vibesql_ast::Expression::Trim {
        position: Some(vibesql_ast::TrimPosition::Both),
        removal_char: Some(Box::new(vibesql_ast::Expression::Literal(
            vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("x")),
        ))),
        string: Box::new(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("xfoox")))),
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("foo")));
}

// ============================================================================
// SUBSTRING with Date/Timestamp Tests (Issue #2955, TPC-H Q7/Q9 support)
// ============================================================================

#[test]
fn test_substring_with_date_extract_year() {
    // TPC-H Q7/Q9 pattern: SUBSTR(l_shipdate, 1, 4) to extract year from date
    let (evaluator, row) = create_test_evaluator();
    let date = vibesql_types::Date::new(1996, 7, 15).unwrap();
    let expr = vibesql_ast::Expression::Function {
        name: "SUBSTRING".to_string(),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Date(date)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(4)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("1996")));
}

#[test]
fn test_substring_with_date_extract_month() {
    // Extract month from date: SUBSTR(date, 6, 2)
    let (evaluator, row) = create_test_evaluator();
    let date = vibesql_types::Date::new(1996, 7, 15).unwrap();
    let expr = vibesql_ast::Expression::Function {
        name: "SUBSTR".to_string(), // Test alias
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Date(date)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(6)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(2)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("07")));
}

#[test]
fn test_substring_with_date_extract_day() {
    // Extract day from date: SUBSTR(date, 9, 2)
    let (evaluator, row) = create_test_evaluator();
    let date = vibesql_types::Date::new(1996, 7, 15).unwrap();
    let expr = vibesql_ast::Expression::Function {
        name: "SUBSTRING".to_string(),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Date(date)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(9)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(2)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("15")));
}

#[test]
fn test_substring_with_timestamp() {
    // Extract date portion from timestamp: SUBSTRING(timestamp, 1, 10)
    let (evaluator, row) = create_test_evaluator();
    let date = vibesql_types::Date::new(1996, 7, 15).unwrap();
    let time = vibesql_types::Time::new(8, 30, 0, 0).unwrap();
    let timestamp = vibesql_types::Timestamp::new(date, time);
    let expr = vibesql_ast::Expression::Function {
        name: "SUBSTRING".to_string(),
        args: vec![
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Timestamp(timestamp)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(1)),
            vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(10)),
        ],
        character_unit: None,
    };
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("1996-07-15")));
}
