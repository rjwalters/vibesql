//! Common assertion helpers for numeric edge case tests

#![allow(dead_code)]

use vibesql_executor::ExpressionEvaluator;
use vibesql_storage::Row;
use vibesql_types::SqlValue;

/// Helper to create a function expression with literal arguments
pub fn create_function_expr(name: &str, args: Vec<SqlValue>) -> vibesql_ast::Expression {
    vibesql_ast::Expression::Function {
        name: name.to_string(),
        args: args.into_iter().map(vibesql_ast::Expression::Literal).collect(),
        character_unit: None,
    }
}

/// Assert that a function returns NULL when given NULL input
pub fn assert_function_returns_null_on_null_input(
    evaluator: &ExpressionEvaluator,
    row: &Row,
    function_name: &str,
    non_null_args: Vec<SqlValue>,
) {
    let mut args = non_null_args;
    // Insert NULL at the position where we want to test NULL input
    // For simplicity, we assume the first NULL in args is the test position
    // This can be extended if needed
    let null_pos = args.iter().position(|arg| matches!(arg, SqlValue::Null)).unwrap_or(0);
    args[null_pos] = SqlValue::Null;

    let expr = create_function_expr(function_name, args);
    let result = evaluator.eval(&expr, row).unwrap();
    assert_eq!(
        result,
        SqlValue::Null,
        "Function {} should return NULL when input contains NULL",
        function_name
    );
}

/// Assert that a function call results in an error
pub fn assert_function_errors(
    evaluator: &ExpressionEvaluator,
    row: &Row,
    function_name: &str,
    args: Vec<SqlValue>,
) {
    let expr = create_function_expr(function_name, args);
    let result = evaluator.eval(&expr, row);
    assert!(
        result.is_err(),
        "Function {} should have returned an error but got: {:?}",
        function_name,
        result
    );
}

/// Assert that a function returns a specific double value with tolerance
pub fn assert_function_returns_double(
    evaluator: &ExpressionEvaluator,
    row: &Row,
    function_name: &str,
    args: Vec<SqlValue>,
    expected: f64,
    tolerance: f64,
) {
    let expr = create_function_expr(function_name, args);
    let result = evaluator.eval(&expr, row).unwrap();

    match result {
        SqlValue::Double(val) => {
            assert!(
                (val - expected).abs() < tolerance,
                "Function {} returned {} but expected {} (tolerance: {})",
                function_name,
                val,
                expected,
                tolerance
            );
        }
        _ => panic!("Function {} should return Double but got {:?}", function_name, result),
    }
}

/// Assert that a function returns a specific integer value
pub fn assert_function_returns_integer(
    evaluator: &ExpressionEvaluator,
    row: &Row,
    function_name: &str,
    args: Vec<SqlValue>,
    expected: i64,
) {
    let expr = create_function_expr(function_name, args);
    let result = evaluator.eval(&expr, row).unwrap();

    match result {
        SqlValue::Integer(val) => {
            assert_eq!(
                val, expected,
                "Function {} returned {} but expected {}",
                function_name, val, expected
            );
        }
        _ => panic!("Function {} should return Integer but got {:?}", function_name, result),
    }
}

/// Assert that a function returns the same value as input (identity-like functions)
pub fn assert_function_returns_same_value(
    evaluator: &ExpressionEvaluator,
    row: &Row,
    function_name: &str,
    input: SqlValue,
) {
    let expr = create_function_expr(function_name, vec![input.clone()]);
    let result = evaluator.eval(&expr, row).unwrap();
    assert_eq!(
        result, input,
        "Function {} should return same value as input {:?}",
        function_name, input
    );
}

/// Test a function across multiple numeric types
pub fn test_function_across_types<F>(
    evaluator: &ExpressionEvaluator,
    row: &Row,
    function_name: &str,
    test_fn: F,
) where
    F: Fn(SqlValue) -> SqlValue,
{
    let types_to_test = vec![
        SqlValue::Smallint(42),
        SqlValue::Integer(42),
        SqlValue::Bigint(42),
        SqlValue::Float(42.0),
        SqlValue::Double(42.0),
        SqlValue::Real(42.0),
    ];

    for input_type in &types_to_test {
        let expr = create_function_expr(function_name, vec![input_type.clone()]);
        let result = evaluator.eval(&expr, row).unwrap();
        let expected = test_fn(input_type.clone());

        match (&result, &expected) {
            (SqlValue::Double(r), SqlValue::Double(e)) => {
                assert!(
                    (r - e).abs() < 0.001,
                    "Type coercion test failed for input {:?}: got {}, expected {}",
                    input_type,
                    r,
                    e
                );
            }
            _ => {
                assert_eq!(result, expected, "Type coercion test failed for input {:?}", input_type)
            }
        }
    }
}
