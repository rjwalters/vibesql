//! Basic numeric function edge cases (ABS, SIGN, MOD)

use vibesql_types::SqlValue;

use super::common::create_test_evaluator;

// Helper functions for this module
pub fn create_function_expr(name: &str, args: Vec<SqlValue>) -> vibesql_ast::Expression {
    vibesql_ast::Expression::Function {
        name: name.to_string(),
        args: args.into_iter().map(vibesql_ast::Expression::Literal).collect(),
        character_unit: None,
    }
}

pub fn assert_function_returns_null_on_null_input(
    evaluator: &vibesql_executor::ExpressionEvaluator,
    row: &vibesql_storage::Row,
    function_name: &str,
    non_null_args: Vec<SqlValue>,
) {
    let mut args = non_null_args;
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

pub fn assert_function_errors(
    evaluator: &vibesql_executor::ExpressionEvaluator,
    row: &vibesql_storage::Row,
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

pub fn assert_function_returns_double(
    evaluator: &vibesql_executor::ExpressionEvaluator,
    row: &vibesql_storage::Row,
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

pub fn assert_function_returns_integer(
    evaluator: &vibesql_executor::ExpressionEvaluator,
    row: &vibesql_storage::Row,
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

#[test]
fn test_abs_min_int() {
    let (evaluator, row) = create_test_evaluator();

    // Test ABS of i32::MIN (should overflow but let's see what happens)
    let expr = create_function_expr("ABS", vec![vibesql_types::SqlValue::Integer(i32::MIN as i64)]);

    // This might panic or error - depending on implementation
    let result = evaluator.eval(&expr, &row);
    match result {
        Ok(_) => println!("ABS(i32::MIN) succeeded"),
        Err(e) => println!("ABS(i32::MIN) failed: {:?}", e),
    }
}

#[test]
fn test_abs_null() {
    let (evaluator, row) = create_test_evaluator();
    assert_function_returns_null_on_null_input(
        &evaluator,
        &row,
        "ABS",
        vec![SqlValue::Integer(42)],
    );
}

#[test]
fn test_sign_null() {
    let (evaluator, row) = create_test_evaluator();
    assert_function_returns_null_on_null_input(
        &evaluator,
        &row,
        "SIGN",
        vec![SqlValue::Integer(42)],
    );
}

#[test]
fn test_sign_float_zero() {
    let (evaluator, row) = create_test_evaluator();
    assert_function_returns_double(
        &evaluator,
        &row,
        "SIGN",
        vec![SqlValue::Double(-0.0)],
        0.0,
        0.001,
    );
}

#[test]
fn test_mod_by_zero() {
    // MySQL and SQLite return NULL for MOD(x, 0), not an error
    let (evaluator, row) = create_test_evaluator();
    let expr = create_function_expr("MOD", vec![SqlValue::Integer(10), SqlValue::Integer(0)]);
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, SqlValue::Null, "MOD by zero should return NULL");
}

#[test]
fn test_mod_with_null() {
    let (evaluator, row) = create_test_evaluator();
    assert_function_returns_null_on_null_input(
        &evaluator,
        &row,
        "MOD",
        vec![SqlValue::Integer(10), SqlValue::Integer(5)],
    );
}

#[test]
fn test_mod_negative_divisor() {
    let (evaluator, row) = create_test_evaluator();
    assert_function_returns_integer(
        &evaluator,
        &row,
        "MOD",
        vec![SqlValue::Integer(10), SqlValue::Integer(-3)],
        10 % -3,
    );
}
