//! Tests for date/time extraction functions
//!
//! This module tests extraction of components from date/time values:
//! - YEAR, MONTH, DAY (date components)
//! - HOUR, MINUTE, SECOND (time components)
//! - NULL handling for all extraction functions

use super::fixtures::*;

// ==================== DATE EXTRACTION FUNCTIONS ====================

#[test]
fn test_year_from_date() {
    let (evaluator, row) = setup_test();

    let expr = create_datetime_function(
        "YEAR",
        vec![create_literal(vibesql_types::SqlValue::Date(TEST_DATE.parse().unwrap()))],
    );
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Integer(2024));
}

#[test]
fn test_year_from_timestamp() {
    let (evaluator, row) = setup_test();

    let expr = create_datetime_function(
        "YEAR",
        vec![create_literal(vibesql_types::SqlValue::Timestamp(TEST_TIMESTAMP.parse().unwrap()))],
    );
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Integer(2024));
}

#[test]
fn test_year_with_null() {
    let (evaluator, row) = setup_test();

    let expr =
        create_datetime_function("YEAR", vec![create_literal(vibesql_types::SqlValue::Null)]);
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Null);
}

#[test]
fn test_month_from_date() {
    let (evaluator, row) = setup_test();

    let expr = create_datetime_function(
        "MONTH",
        vec![create_literal(vibesql_types::SqlValue::Date(TEST_DATE.parse().unwrap()))],
    );
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Integer(3));
}

#[test]
fn test_month_from_timestamp() {
    let (evaluator, row) = setup_test();

    let expr = create_datetime_function(
        "MONTH",
        vec![create_literal(vibesql_types::SqlValue::Timestamp(
            TEST_TIMESTAMP_CHRISTMAS.parse().unwrap(),
        ))],
    );
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Integer(12));
}

#[test]
fn test_day_from_date() {
    let (evaluator, row) = setup_test();

    let expr = create_datetime_function(
        "DAY",
        vec![create_literal(vibesql_types::SqlValue::Date(TEST_DATE.parse().unwrap()))],
    );
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Integer(15));
}

#[test]
fn test_day_from_timestamp() {
    let (evaluator, row) = setup_test();

    let expr = create_datetime_function(
        "DAY",
        vec![create_literal(vibesql_types::SqlValue::Timestamp(
            (TEST_DATE_LATE_MONTH.to_string() + " 14:30:45").parse().unwrap(),
        ))],
    );
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Integer(27));
}

// ==================== TIME EXTRACTION FUNCTIONS ====================

#[test]
fn test_hour_from_time() {
    let (evaluator, row) = setup_test();

    let expr = create_datetime_function(
        "HOUR",
        vec![create_literal(vibesql_types::SqlValue::Time(TEST_TIME.parse().unwrap()))],
    );
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Integer(14));
}

#[test]
fn test_hour_from_timestamp() {
    let (evaluator, row) = setup_test();

    let expr = create_datetime_function(
        "HOUR",
        vec![create_literal(vibesql_types::SqlValue::Timestamp(
            "2024-03-15 23:59:59".parse().unwrap(),
        ))],
    );
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Integer(23));
}

#[test]
fn test_minute_from_time() {
    let (evaluator, row) = setup_test();

    let expr = create_datetime_function(
        "MINUTE",
        vec![create_literal(vibesql_types::SqlValue::Time(TEST_TIME.parse().unwrap()))],
    );
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Integer(30));
}

#[test]
fn test_minute_from_timestamp() {
    let (evaluator, row) = setup_test();

    let expr = create_datetime_function(
        "MINUTE",
        vec![create_literal(vibesql_types::SqlValue::Timestamp(
            "2024-03-15 14:45:30".parse().unwrap(),
        ))],
    );
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Integer(45));
}

#[test]
fn test_second_from_time() {
    let (evaluator, row) = setup_test();

    let expr = create_datetime_function(
        "SECOND",
        vec![create_literal(vibesql_types::SqlValue::Time(TEST_TIME.parse().unwrap()))],
    );
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Integer(45));
}

#[test]
fn test_second_from_timestamp() {
    let (evaluator, row) = setup_test();

    let expr = create_datetime_function(
        "SECOND",
        vec![create_literal(vibesql_types::SqlValue::Timestamp(
            "2024-03-15 14:30:59".parse().unwrap(),
        ))],
    );
    let result = evaluator.eval(&expr, &row).unwrap();
    assert_eq!(result, vibesql_types::SqlValue::Integer(59));
}

// ==================== NULL HANDLING ====================

#[test]
fn test_extraction_functions_with_null() {
    let (evaluator, row) = setup_test();

    let functions = vec!["YEAR", "MONTH", "DAY", "HOUR", "MINUTE", "SECOND"];

    for func_name in functions {
        let expr = create_datetime_function(
            func_name,
            vec![create_literal(vibesql_types::SqlValue::Null)],
        );
        let result = evaluator.eval(&expr, &row).unwrap();
        assert_eq!(
            result,
            vibesql_types::SqlValue::Null,
            "{} should return NULL for NULL input",
            func_name
        );
    }
}
