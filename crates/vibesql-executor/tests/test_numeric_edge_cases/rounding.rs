#![allow(clippy::approx_constant)]
//! Rounding function edge cases (ROUND, FLOOR, CEIL)

use vibesql_types::SqlValue;

use super::{
    basic::{
        assert_function_returns_double, assert_function_returns_null_on_null_input,
        create_function_expr,
    },
    common::create_test_evaluator,
};

#[test]
fn test_round_null() {
    let (evaluator, row) = create_test_evaluator();
    assert_function_returns_null_on_null_input(
        &evaluator,
        &row,
        "ROUND",
        vec![SqlValue::Double(3.14159)],
    );
}

#[test]
fn test_round_with_null_precision() {
    let (evaluator, row) = create_test_evaluator();
    let expr = create_function_expr("ROUND", vec![SqlValue::Double(3.14159), SqlValue::Null]);
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, SqlValue::Null);
}

#[test]
fn test_round_negative_precision() {
    let (evaluator, row) = create_test_evaluator();
    assert_function_returns_double(
        &evaluator,
        &row,
        "ROUND",
        vec![SqlValue::Double(123.456), SqlValue::Integer(-1)],
        120.0,
        0.001,
    );
}

#[test]
fn test_floor_null() {
    let (evaluator, row) = create_test_evaluator();
    assert_function_returns_null_on_null_input(
        &evaluator,
        &row,
        "FLOOR",
        vec![SqlValue::Double(3.14159)],
    );
}

#[test]
fn test_floor_negative() {
    let (evaluator, row) = create_test_evaluator();
    assert_function_returns_double(
        &evaluator,
        &row,
        "FLOOR",
        vec![SqlValue::Double(-1.5)],
        -2.0,
        0.001,
    );
}

#[test]
fn test_ceil_null() {
    let (evaluator, row) = create_test_evaluator();
    assert_function_returns_null_on_null_input(
        &evaluator,
        &row,
        "CEIL",
        vec![SqlValue::Double(3.14159)],
    );
}

#[test]
fn test_ceil_negative() {
    let (evaluator, row) = create_test_evaluator();
    assert_function_returns_double(
        &evaluator,
        &row,
        "CEIL",
        vec![SqlValue::Double(-1.5)],
        -1.0,
        0.001,
    );
}

// Tests for Numeric type support (issue #1708)

#[test]
fn test_round_numeric() {
    let (evaluator, row) = create_test_evaluator();
    let expr = create_function_expr("ROUND", vec![SqlValue::Numeric(3.14159)]);
    let result = evaluator.eval(&expr, &row).unwrap();
    match result {
        SqlValue::Numeric(n) => {
            assert!((n - 3.0).abs() < 0.001, "Expected 3.0, got {}", n);
        }
        _ => panic!("Expected Numeric result, got {:?}", result),
    }
}

#[test]
fn test_round_numeric_with_precision() {
    let (evaluator, row) = create_test_evaluator();
    let expr =
        create_function_expr("ROUND", vec![SqlValue::Numeric(3.14159), SqlValue::Integer(2)]);
    let result = evaluator.eval(&expr, &row).unwrap();
    match result {
        SqlValue::Numeric(n) => {
            assert!((n - 3.14).abs() < 0.001, "Expected 3.14, got {}", n);
        }
        _ => panic!("Expected Numeric result, got {:?}", result),
    }
}

#[test]
fn test_round_numeric_negative_precision() {
    let (evaluator, row) = create_test_evaluator();
    let expr =
        create_function_expr("ROUND", vec![SqlValue::Numeric(123.456), SqlValue::Integer(-1)]);
    let result = evaluator.eval(&expr, &row).unwrap();
    match result {
        SqlValue::Numeric(n) => {
            assert!((n - 120.0).abs() < 0.001, "Expected 120.0, got {}", n);
        }
        _ => panic!("Expected Numeric result, got {:?}", result),
    }
}

#[test]
fn test_floor_numeric() {
    let (evaluator, row) = create_test_evaluator();
    let expr = create_function_expr("FLOOR", vec![SqlValue::Numeric(3.7)]);
    let result = evaluator.eval(&expr, &row).unwrap();
    match result {
        SqlValue::Numeric(n) => {
            assert!((n - 3.0).abs() < 0.001, "Expected 3.0, got {}", n);
        }
        _ => panic!("Expected Numeric result, got {:?}", result),
    }
}

#[test]
fn test_floor_numeric_negative() {
    let (evaluator, row) = create_test_evaluator();
    let expr = create_function_expr("FLOOR", vec![SqlValue::Numeric(-1.5)]);
    let result = evaluator.eval(&expr, &row).unwrap();
    match result {
        SqlValue::Numeric(n) => {
            assert!((n - (-2.0)).abs() < 0.001, "Expected -2.0, got {}", n);
        }
        _ => panic!("Expected Numeric result, got {:?}", result),
    }
}

#[test]
fn test_ceil_numeric() {
    let (evaluator, row) = create_test_evaluator();
    let expr = create_function_expr("CEIL", vec![SqlValue::Numeric(3.2)]);
    let result = evaluator.eval(&expr, &row).unwrap();
    match result {
        SqlValue::Numeric(n) => {
            assert!((n - 4.0).abs() < 0.001, "Expected 4.0, got {}", n);
        }
        _ => panic!("Expected Numeric result, got {:?}", result),
    }
}

#[test]
fn test_ceil_numeric_negative() {
    let (evaluator, row) = create_test_evaluator();
    let expr = create_function_expr("CEIL", vec![SqlValue::Numeric(-1.5)]);
    let result = evaluator.eval(&expr, &row).unwrap();
    match result {
        SqlValue::Numeric(n) => {
            assert!((n - (-1.0)).abs() < 0.001, "Expected -1.0, got {}", n);
        }
        _ => panic!("Expected Numeric result, got {:?}", result),
    }
}

#[test]
fn test_truncate_numeric() {
    let (evaluator, row) = create_test_evaluator();
    let expr = create_function_expr("TRUNCATE", vec![SqlValue::Numeric(3.7)]);
    let result = evaluator.eval(&expr, &row).unwrap();
    match result {
        SqlValue::Numeric(n) => {
            assert!((n - 3.0).abs() < 0.001, "Expected 3.0, got {}", n);
        }
        _ => panic!("Expected Numeric result, got {:?}", result),
    }
}

#[test]
fn test_truncate_numeric_with_precision() {
    let (evaluator, row) = create_test_evaluator();
    let expr =
        create_function_expr("TRUNCATE", vec![SqlValue::Numeric(3.14159), SqlValue::Integer(2)]);
    let result = evaluator.eval(&expr, &row).unwrap();
    match result {
        SqlValue::Numeric(n) => {
            assert!((n - 3.14).abs() < 0.001, "Expected 3.14, got {}", n);
        }
        _ => panic!("Expected Numeric result, got {:?}", result),
    }
}
