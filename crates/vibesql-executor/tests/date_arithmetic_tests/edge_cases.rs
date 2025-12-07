//! Edge case tests: leap years, DST, timezone boundaries, overflow, null handling, error handling

use crate::common::create_test_evaluator;

// ==================== HELPER FUNCTIONS ====================

/// Helper to create a function expression
fn create_function_expr(name: &str, args: Vec<vibesql_ast::Expression>) -> vibesql_ast::Expression {
    vibesql_ast::Expression::Function { name: name.to_string(), args, character_unit: None }
}

/// Helper to evaluate a function and return the result
fn eval_function(name: &str, args: Vec<vibesql_ast::Expression>) -> vibesql_types::SqlValue {
    let (evaluator, row) = create_test_evaluator();
    let expr = create_function_expr(name, args);
    evaluator.eval(&expr, &row).unwrap()
}

/// Helper to evaluate a function and expect an error
fn eval_function_expect_error(name: &str, args: Vec<vibesql_ast::Expression>) {
    let (evaluator, row) = create_test_evaluator();
    let expr = create_function_expr(name, args);
    assert!(evaluator.eval(&expr, &row).is_err());
}

/// Helper to create a date literal expression
fn date_lit(date_str: &str) -> vibesql_ast::Expression {
    vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Date(date_str.parse().unwrap()))
}

/// Helper to create an integer literal expression
fn int_lit(value: i64) -> vibesql_ast::Expression {
    vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(value))
}

/// Helper to create a varchar literal expression
fn varchar_lit(value: &str) -> vibesql_ast::Expression {
    vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(value)))
}

/// Helper to create a null literal expression
fn null_lit() -> vibesql_ast::Expression {
    vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Null)
}

// ==================== EDGE CASES AND NULL HANDLING ====================

#[test]
fn test_date_add_with_null() {
    let result = eval_function("DATE_ADD", vec![null_lit(), int_lit(7), varchar_lit("DAY")]);
    assert_eq!(result, vibesql_types::SqlValue::Null);
}

#[test]
fn test_extract_with_null() {
    let result = eval_function("EXTRACT", vec![varchar_lit("YEAR"), null_lit()]);
    assert_eq!(result, vibesql_types::SqlValue::Null);
}

#[test]
fn test_age_with_null() {
    let result = eval_function("AGE", vec![null_lit(), date_lit("2024-01-15")]);
    assert_eq!(result, vibesql_types::SqlValue::Null);
}

#[test]
fn test_datediff_with_null() {
    let result = eval_function("DATEDIFF", vec![null_lit(), date_lit("2024-01-05")]);
    assert_eq!(result, vibesql_types::SqlValue::Null);
}

// ==================== LEAP YEAR EDGE CASES ====================

#[test]
fn test_date_add_leap_year_to_non_leap() {
    // Feb 29, 2024 (leap year) + 1 year → Feb 28, 2025 (non-leap year)
    let result =
        eval_function("DATE_ADD", vec![date_lit("2024-02-29"), int_lit(1), varchar_lit("YEAR")]);
    assert_eq!(result, vibesql_types::SqlValue::Date("2025-02-28".parse().unwrap()));
}

#[test]
fn test_date_add_leap_year_to_leap() {
    // Feb 29, 2024 (leap year) + 4 years → Feb 29, 2028 (leap year)
    let result =
        eval_function("DATE_ADD", vec![date_lit("2024-02-29"), int_lit(4), varchar_lit("YEAR")]);
    assert_eq!(result, vibesql_types::SqlValue::Date("2028-02-29".parse().unwrap()));
}

#[test]
fn test_date_sub_leap_year() {
    // Feb 29, 2024 - 1 year → Feb 28, 2023 (non-leap year)
    let result =
        eval_function("DATE_SUB", vec![date_lit("2024-02-29"), int_lit(1), varchar_lit("YEAR")]);
    assert_eq!(result, vibesql_types::SqlValue::Date("2023-02-28".parse().unwrap()));
}

#[test]
fn test_datediff_across_leap_year() {
    // Difference between leap year and non-leap year Feb dates
    let result = eval_function("DATEDIFF", vec![date_lit("2024-03-01"), date_lit("2024-02-01")]);
    assert_eq!(result, vibesql_types::SqlValue::Integer(29)); // 29 days in Feb 2024 (leap year)
}

#[test]
fn test_datediff_non_leap_year_february() {
    // Difference in non-leap year February
    let result = eval_function("DATEDIFF", vec![date_lit("2023-03-01"), date_lit("2023-02-01")]);
    assert_eq!(result, vibesql_types::SqlValue::Integer(28)); // 28 days in Feb 2023 (non-leap)
}

// ==================== MONTH BOUNDARY EDGE CASES ====================

#[test]
fn test_date_add_month_end_to_shorter_month() {
    // Jan 31 + 1 month → Feb 28/29 (depending on year)
    let result =
        eval_function("DATE_ADD", vec![date_lit("2024-01-31"), int_lit(1), varchar_lit("MONTH")]);
    assert_eq!(result, vibesql_types::SqlValue::Date("2024-02-29".parse().unwrap()));
    // 2024 is leap year
}

#[test]
fn test_date_add_month_end_to_shorter_month_non_leap() {
    // Jan 31, 2023 + 1 month → Feb 28, 2023
    let result =
        eval_function("DATE_ADD", vec![date_lit("2023-01-31"), int_lit(1), varchar_lit("MONTH")]);
    assert_eq!(result, vibesql_types::SqlValue::Date("2023-02-28".parse().unwrap()));
}

#[test]
fn test_date_sub_month_from_march_31() {
    // Mar 31 - 1 month → Feb 29 (in leap year)
    let result =
        eval_function("DATE_SUB", vec![date_lit("2024-03-31"), int_lit(1), varchar_lit("MONTH")]);
    assert_eq!(result, vibesql_types::SqlValue::Date("2024-02-29".parse().unwrap()));
}

#[test]
fn test_date_add_month_may_31_to_june() {
    // May 31 + 1 month → Jun 30 (June has 30 days)
    let result =
        eval_function("DATE_ADD", vec![date_lit("2024-05-31"), int_lit(1), varchar_lit("MONTH")]);
    assert_eq!(result, vibesql_types::SqlValue::Date("2024-06-30".parse().unwrap()));
}

#[test]
fn test_date_add_multiple_months_across_year_boundary() {
    // Oct 15 + 5 months → Mar 15 (crosses year boundary)
    let result =
        eval_function("DATE_ADD", vec![date_lit("2023-10-15"), int_lit(5), varchar_lit("MONTH")]);
    assert_eq!(result, vibesql_types::SqlValue::Date("2024-03-15".parse().unwrap()));
}

#[test]
fn test_date_sub_months_across_year_boundary() {
    // Feb 15, 2024 - 5 months → Sep 15, 2023
    let result =
        eval_function("DATE_SUB", vec![date_lit("2024-02-15"), int_lit(5), varchar_lit("MONTH")]);
    assert_eq!(result, vibesql_types::SqlValue::Date("2023-09-15".parse().unwrap()));
}

// ==================== YEAR BOUNDARY TESTS ====================

#[test]
fn test_date_add_days_across_year_boundary() {
    // Dec 31, 2023 + 1 day → Jan 1, 2024
    let result =
        eval_function("DATE_ADD", vec![date_lit("2023-12-31"), int_lit(1), varchar_lit("DAY")]);
    assert_eq!(result, vibesql_types::SqlValue::Date("2024-01-01".parse().unwrap()));
}

#[test]
fn test_date_sub_days_across_year_boundary() {
    // Jan 1, 2024 - 1 day → Dec 31, 2023
    let result =
        eval_function("DATE_SUB", vec![date_lit("2024-01-01"), int_lit(1), varchar_lit("DAY")]);
    assert_eq!(result, vibesql_types::SqlValue::Date("2023-12-31".parse().unwrap()));
}

#[test]
fn test_datediff_across_year_boundary() {
    // Difference across year boundary
    let result = eval_function("DATEDIFF", vec![date_lit("2024-01-05"), date_lit("2023-12-28")]);
    assert_eq!(result, vibesql_types::SqlValue::Integer(8)); // 3 days in Dec + 5 days in Jan
}

// ==================== LARGE INTERVAL TESTS ====================

#[test]
fn test_date_add_large_year_interval() {
    // Add 1000 years (should not panic or overflow)
    let result =
        eval_function("DATE_ADD", vec![date_lit("2024-01-15"), int_lit(1000), varchar_lit("YEAR")]);
    assert_eq!(result, vibesql_types::SqlValue::Date("3024-01-15".parse().unwrap()));
}

#[test]
fn test_date_add_large_day_interval() {
    // Add 365 days (1 year worth)
    let result =
        eval_function("DATE_ADD", vec![date_lit("2024-01-01"), int_lit(365), varchar_lit("DAY")]);
    assert_eq!(result, vibesql_types::SqlValue::Date("2024-12-31".parse().unwrap()));
    // 2024 is leap year with
    // 366 days
}

#[test]
fn test_date_sub_large_interval() {
    // Subtract 50 years
    let result =
        eval_function("DATE_SUB", vec![date_lit("2024-06-15"), int_lit(50), varchar_lit("YEAR")]);
    assert_eq!(result, vibesql_types::SqlValue::Date("1974-06-15".parse().unwrap()));
}

#[test]
fn test_datediff_large_interval() {
    // Difference over many years (should not panic)
    let result = eval_function("DATEDIFF", vec![date_lit("2024-01-01"), date_lit("1900-01-01")]);
    // Should return a large positive number (approximately 45,290 days)
    if let vibesql_types::SqlValue::Integer(days) = result {
        assert!(days > 45000 && days < 46000);
    } else {
        panic!("Expected Integer result");
    }
}

// ==================== COMPREHENSIVE NULL PROPAGATION TESTS ====================

#[test]
fn test_date_add_null_amount() {
    let result =
        eval_function("DATE_ADD", vec![date_lit("2024-01-15"), null_lit(), varchar_lit("DAY")]);
    assert_eq!(result, vibesql_types::SqlValue::Null);
}

#[test]
fn test_date_add_null_unit() {
    let result = eval_function("DATE_ADD", vec![date_lit("2024-01-15"), int_lit(5), null_lit()]);
    assert_eq!(result, vibesql_types::SqlValue::Null);
}

#[test]
fn test_date_sub_null_date() {
    let result = eval_function("DATE_SUB", vec![null_lit(), int_lit(5), varchar_lit("DAY")]);
    assert_eq!(result, vibesql_types::SqlValue::Null);
}

#[test]
fn test_date_sub_null_amount() {
    let result =
        eval_function("DATE_SUB", vec![date_lit("2024-01-15"), null_lit(), varchar_lit("MONTH")]);
    assert_eq!(result, vibesql_types::SqlValue::Null);
}

#[test]
fn test_datediff_null_second_arg() {
    let result = eval_function("DATEDIFF", vec![date_lit("2024-01-10"), null_lit()]);
    assert_eq!(result, vibesql_types::SqlValue::Null);
}

#[test]
fn test_extract_null_unit() {
    // EXTRACT requires a string unit, not NULL (unit is a keyword, not a value)
    eval_function_expect_error("EXTRACT", vec![null_lit(), date_lit("2024-01-15")]);
}

#[test]
fn test_age_null_second_date() {
    let result = eval_function("AGE", vec![date_lit("2024-01-15"), null_lit()]);
    assert_eq!(result, vibesql_types::SqlValue::Null);
}

// ==================== ERROR HANDLING TESTS ====================

#[test]
fn test_datediff_invalid_date_format() {
    eval_function_expect_error(
        "DATEDIFF",
        vec![varchar_lit("invalid-date"), date_lit("2024-01-05")],
    );
}

#[test]
fn test_datediff_wrong_argument_count() {
    eval_function_expect_error("DATEDIFF", vec![date_lit("2024-01-10")]);
}

#[test]
fn test_date_add_wrong_argument_count() {
    eval_function_expect_error("DATE_ADD", vec![date_lit("2024-01-15"), int_lit(5)]);
}

#[test]
fn test_date_add_invalid_unit() {
    eval_function_expect_error(
        "DATE_ADD",
        vec![date_lit("2024-01-15"), int_lit(5), varchar_lit("INVALID_UNIT")],
    );
}

#[test]
fn test_date_add_wrong_type_for_amount() {
    eval_function_expect_error(
        "DATE_ADD",
        vec![date_lit("2024-01-15"), varchar_lit("not-a-number"), varchar_lit("DAY")],
    );
}

#[test]
fn test_date_add_wrong_type_for_date() {
    eval_function_expect_error("DATE_ADD", vec![int_lit(12345), int_lit(5), varchar_lit("DAY")]);
}

#[test]
fn test_datediff_wrong_types() {
    eval_function_expect_error("DATEDIFF", vec![int_lit(123), varchar_lit("not-a-date")]);
}
