//! Tests for type coercion in date/time operations
//!
//! Covers:
//! - VARCHAR to DATE coercion in DATEDIFF
//! - VARCHAR + INTERVAL binary operations
//! - VARCHAR - INTERVAL binary operations
//! - DATE_ADD/DATE_SUB with INTERVAL syntax and VARCHAR dates

use crate::common::create_test_evaluator;
use vibesql_types::{Interval, SqlValue};

// ==================== DATEDIFF COERCION TESTS ====================

#[test]
fn test_datediff_varchar_dates() {
    let (evaluator, row) = create_test_evaluator();

    let expr = vibesql_ast::Expression::Function {
        name: "DATEDIFF".to_string(),
        args: vec![
            vibesql_ast::Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("2024-01-10"))),
            vibesql_ast::Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("2024-01-01"))),
        ],
        character_unit: None,
    };

    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, SqlValue::Integer(9));
}

#[test]
fn test_datediff_mixed_varchar_date() {
    let (evaluator, row) = create_test_evaluator();

    let expr = vibesql_ast::Expression::Function {
        name: "DATEDIFF".to_string(),
        args: vec![
            vibesql_ast::Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("2024-01-10"))),
            vibesql_ast::Expression::Literal(SqlValue::Date("2024-01-01".parse().unwrap())),
        ],
        character_unit: None,
    };

    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, SqlValue::Integer(9));
}

#[test]
fn test_datediff_invalid_varchar() {
    let (evaluator, row) = create_test_evaluator();

    let expr = vibesql_ast::Expression::Function {
        name: "DATEDIFF".to_string(),
        args: vec![
            vibesql_ast::Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("not-a-date"))),
            vibesql_ast::Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("2024-01-01"))),
        ],
        character_unit: None,
    };

    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err());
}

#[test]
fn test_datediff_varchar_null() {
    let (evaluator, row) = create_test_evaluator();

    let expr = vibesql_ast::Expression::Function {
        name: "DATEDIFF".to_string(),
        args: vec![
            vibesql_ast::Expression::Literal(SqlValue::Null),
            vibesql_ast::Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("2024-01-01"))),
        ],
        character_unit: None,
    };

    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, SqlValue::Null);
}

// ==================== BINARY OPERATOR COERCION TESTS ====================

#[test]
fn test_varchar_plus_interval() {
    let (evaluator, row) = create_test_evaluator();

    // '2024-01-01' + INTERVAL '5' DAY
    let expr = vibesql_ast::Expression::BinaryOp {
        left: Box::new(vibesql_ast::Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("2024-01-01")))),
        op: vibesql_ast::BinaryOperator::Plus,
        right: Box::new(vibesql_ast::Expression::Literal(SqlValue::Interval(Interval::new(
            "5 DAY".to_string(),
        )))),
    };

    let result = evaluator.eval(&expr, &row).unwrap();

    // Should return DATE '2024-01-06'
    match result {
        SqlValue::Date(d) => {
            assert_eq!(d.year, 2024);
            assert_eq!(d.month, 1);
            assert_eq!(d.day, 6);
        }
        _ => panic!("Expected Date result, got {:?}", result),
    }
}

#[test]
fn test_interval_plus_varchar_commutative() {
    let (evaluator, row) = create_test_evaluator();

    // INTERVAL '5' DAY + '2024-01-01' (commutative)
    let expr = vibesql_ast::Expression::BinaryOp {
        left: Box::new(vibesql_ast::Expression::Literal(SqlValue::Interval(Interval::new(
            "5 DAY".to_string(),
        )))),
        op: vibesql_ast::BinaryOperator::Plus,
        right: Box::new(vibesql_ast::Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("2024-01-01")))),
    };

    let result = evaluator.eval(&expr, &row).unwrap();

    // Should return DATE '2024-01-06'
    match result {
        SqlValue::Date(d) => {
            assert_eq!(d.year, 2024);
            assert_eq!(d.month, 1);
            assert_eq!(d.day, 6);
        }
        _ => panic!("Expected Date result, got {:?}", result),
    }
}

#[test]
fn test_varchar_minus_interval() {
    let (evaluator, row) = create_test_evaluator();

    // '2024-01-10' - INTERVAL '5' DAY
    let expr = vibesql_ast::Expression::BinaryOp {
        left: Box::new(vibesql_ast::Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("2024-01-10")))),
        op: vibesql_ast::BinaryOperator::Minus,
        right: Box::new(vibesql_ast::Expression::Literal(SqlValue::Interval(Interval::new(
            "5 DAY".to_string(),
        )))),
    };

    let result = evaluator.eval(&expr, &row).unwrap();

    // Should return DATE '2024-01-05'
    match result {
        SqlValue::Date(d) => {
            assert_eq!(d.year, 2024);
            assert_eq!(d.month, 1);
            assert_eq!(d.day, 5);
        }
        _ => panic!("Expected Date result, got {:?}", result),
    }
}

#[test]
fn test_interval_minus_varchar_error() {
    let (evaluator, row) = create_test_evaluator();

    // INTERVAL '5' DAY - '2024-01-01' (should error - not commutative)
    let expr = vibesql_ast::Expression::BinaryOp {
        left: Box::new(vibesql_ast::Expression::Literal(SqlValue::Interval(Interval::new(
            "5 DAY".to_string(),
        )))),
        op: vibesql_ast::BinaryOperator::Minus,
        right: Box::new(vibesql_ast::Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("2024-01-01")))),
    };

    let result = evaluator.eval(&expr, &row);
    assert!(result.is_err());
}

#[test]
fn test_varchar_plus_interval_null() {
    let (evaluator, row) = create_test_evaluator();

    // NULL + INTERVAL '5' DAY
    let expr = vibesql_ast::Expression::BinaryOp {
        left: Box::new(vibesql_ast::Expression::Literal(SqlValue::Null)),
        op: vibesql_ast::BinaryOperator::Plus,
        right: Box::new(vibesql_ast::Expression::Literal(SqlValue::Interval(Interval::new(
            "5 DAY".to_string(),
        )))),
    };

    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, SqlValue::Null);
}

#[test]
fn test_varchar_plus_interval_month() {
    let (evaluator, row) = create_test_evaluator();

    // '2024-01-31' + INTERVAL '1' MONTH
    let expr = vibesql_ast::Expression::BinaryOp {
        left: Box::new(vibesql_ast::Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("2024-01-31")))),
        op: vibesql_ast::BinaryOperator::Plus,
        right: Box::new(vibesql_ast::Expression::Literal(SqlValue::Interval(Interval::new(
            "1 MONTH".to_string(),
        )))),
    };

    let result = evaluator.eval(&expr, &row).unwrap();

    // Should return DATE '2024-02-29' (clamped to last day of February, leap year)
    match result {
        SqlValue::Date(d) => {
            assert_eq!(d.year, 2024);
            assert_eq!(d.month, 2);
            assert_eq!(d.day, 29); // Leap year
        }
        _ => panic!("Expected Date result, got {:?}", result),
    }
}

// ==================== DATE_ADD/DATE_SUB WITH INTERVAL SYNTAX ====================

#[test]
fn test_date_add_varchar_with_interval() {
    let (evaluator, row) = create_test_evaluator();

    // DATE_ADD('2024-01-01', INTERVAL '5' DAY)
    let expr = vibesql_ast::Expression::Function {
        name: "DATE_ADD".to_string(),
        args: vec![
            vibesql_ast::Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("2024-01-01"))),
            vibesql_ast::Expression::Literal(SqlValue::Interval(Interval::new(
                "5 DAY".to_string(),
            ))),
        ],
        character_unit: None,
    };

    let result = evaluator.eval(&expr, &row).unwrap();

    match result {
        SqlValue::Date(d) => {
            assert_eq!(d.year, 2024);
            assert_eq!(d.month, 1);
            assert_eq!(d.day, 6);
        }
        _ => panic!("Expected Date result, got {:?}", result),
    }
}

#[test]
fn test_date_add_varchar_legacy_syntax() {
    let (evaluator, row) = create_test_evaluator();

    // DATE_ADD('2024-01-01', 5, 'DAY') - legacy 3-arg syntax
    let expr = vibesql_ast::Expression::Function {
        name: "DATE_ADD".to_string(),
        args: vec![
            vibesql_ast::Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("2024-01-01"))),
            vibesql_ast::Expression::Literal(SqlValue::Integer(5)),
            vibesql_ast::Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("DAY"))),
        ],
        character_unit: None,
    };

    let result = evaluator.eval(&expr, &row).unwrap();

    match result {
        SqlValue::Date(d) => {
            assert_eq!(d.year, 2024);
            assert_eq!(d.month, 1);
            assert_eq!(d.day, 6);
        }
        _ => panic!("Expected Date result, got {:?}", result),
    }
}

#[test]
fn test_date_sub_varchar_with_interval() {
    let (evaluator, row) = create_test_evaluator();

    // DATE_SUB('2024-01-10', INTERVAL '5' DAY)
    let expr = vibesql_ast::Expression::Function {
        name: "DATE_SUB".to_string(),
        args: vec![
            vibesql_ast::Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("2024-01-10"))),
            vibesql_ast::Expression::Literal(SqlValue::Interval(Interval::new(
                "5 DAY".to_string(),
            ))),
        ],
        character_unit: None,
    };

    let result = evaluator.eval(&expr, &row).unwrap();

    match result {
        SqlValue::Date(d) => {
            assert_eq!(d.year, 2024);
            assert_eq!(d.month, 1);
            assert_eq!(d.day, 5);
        }
        _ => panic!("Expected Date result, got {:?}", result),
    }
}

#[test]
fn test_date_sub_varchar_legacy_syntax() {
    let (evaluator, row) = create_test_evaluator();

    // DATE_SUB('2024-01-10', 5, 'DAY') - legacy 3-arg syntax
    let expr = vibesql_ast::Expression::Function {
        name: "DATE_SUB".to_string(),
        args: vec![
            vibesql_ast::Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("2024-01-10"))),
            vibesql_ast::Expression::Literal(SqlValue::Integer(5)),
            vibesql_ast::Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("DAY"))),
        ],
        character_unit: None,
    };

    let result = evaluator.eval(&expr, &row).unwrap();

    match result {
        SqlValue::Date(d) => {
            assert_eq!(d.year, 2024);
            assert_eq!(d.month, 1);
            assert_eq!(d.day, 5);
        }
        _ => panic!("Expected Date result, got {:?}", result),
    }
}

// ==================== EDGE CASE TESTS ====================

#[test]
fn test_varchar_leap_year_date() {
    let (evaluator, row) = create_test_evaluator();

    // '2024-02-29' + INTERVAL '1' YEAR (should handle leap year correctly)
    let expr = vibesql_ast::Expression::BinaryOp {
        left: Box::new(vibesql_ast::Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("2024-02-29")))),
        op: vibesql_ast::BinaryOperator::Plus,
        right: Box::new(vibesql_ast::Expression::Literal(SqlValue::Interval(Interval::new(
            "1 YEAR".to_string(),
        )))),
    };

    let result = evaluator.eval(&expr, &row).unwrap();

    // Should return DATE '2025-02-28' (clamped to last day of Feb in non-leap year)
    match result {
        SqlValue::Date(d) => {
            assert_eq!(d.year, 2025);
            assert_eq!(d.month, 2);
            assert_eq!(d.day, 28); // Not a leap year
        }
        _ => panic!("Expected Date result, got {:?}", result),
    }
}

#[test]
fn test_varchar_year_boundary() {
    let (evaluator, row) = create_test_evaluator();

    // '2024-12-31' + INTERVAL '1' DAY
    let expr = vibesql_ast::Expression::BinaryOp {
        left: Box::new(vibesql_ast::Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("2024-12-31")))),
        op: vibesql_ast::BinaryOperator::Plus,
        right: Box::new(vibesql_ast::Expression::Literal(SqlValue::Interval(Interval::new(
            "1 DAY".to_string(),
        )))),
    };

    let result = evaluator.eval(&expr, &row).unwrap();

    // Should return DATE '2025-01-01'
    match result {
        SqlValue::Date(d) => {
            assert_eq!(d.year, 2025);
            assert_eq!(d.month, 1);
            assert_eq!(d.day, 1);
        }
        _ => panic!("Expected Date result, got {:?}", result),
    }
}

#[test]
fn test_varchar_minus_interval_as_negative() {
    let (evaluator, row) = create_test_evaluator();

    // '2024-01-10' - INTERVAL '5' DAY (expressing negative as subtraction)
    // This is the preferred way to express "subtract 5 days"
    let expr = vibesql_ast::Expression::BinaryOp {
        left: Box::new(vibesql_ast::Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("2024-01-10")))),
        op: vibesql_ast::BinaryOperator::Minus,
        right: Box::new(vibesql_ast::Expression::Literal(SqlValue::Interval(Interval::new(
            "5 DAY".to_string(),
        )))),
    };

    let result = evaluator.eval(&expr, &row).unwrap();

    // Should return DATE '2024-01-05'
    match result {
        SqlValue::Date(d) => {
            assert_eq!(d.year, 2024);
            assert_eq!(d.month, 1);
            assert_eq!(d.day, 5);
        }
        _ => panic!("Expected Date result, got {:?}", result),
    }
}
