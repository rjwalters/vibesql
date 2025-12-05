//! Common fixtures and helpers for date/time function tests

use crate::common::create_test_evaluator;

/// Helper to create a datetime function expression with given name and arguments
pub fn create_datetime_function(
    name: &str,
    args: Vec<vibesql_ast::Expression>,
) -> vibesql_ast::Expression {
    vibesql_ast::Expression::Function { name: name.to_string(), args, character_unit: None }
}

/// Helper to create a literal expression from a SQL value
pub fn create_literal(value: vibesql_types::SqlValue) -> vibesql_ast::Expression {
    vibesql_ast::Expression::Literal(value)
}

/// Setup for tests that need a standard evaluator and row
pub fn setup_test() -> (vibesql_executor::ExpressionEvaluator<'static>, vibesql_storage::Row) {
    create_test_evaluator()
}

/// Common test datetime values
pub const TEST_DATE: &str = "2024-03-15";
pub const TEST_TIMESTAMP: &str = "2024-03-15 14:30:45";
pub const TEST_TIME: &str = "14:30:45";
pub const TEST_TIMESTAMP_CHRISTMAS: &str = "2024-12-25 23:59:58";
pub const TEST_DATE_LATE_MONTH: &str = "2024-03-27";

/// Helper to validate date format (YYYY-MM-DD)
pub fn validate_date_format(date_str: &str) {
    let parts: Vec<&str> = date_str.split('-').collect();
    assert_eq!(parts.len(), 3, "Date should have 3 parts (YYYY-MM-DD)");
    assert_eq!(parts[0].len(), 4, "Year should be 4 digits");
    assert_eq!(parts[1].len(), 2, "Month should be 2 digits");
    assert_eq!(parts[2].len(), 2, "Day should be 2 digits");
}

/// Helper to validate time format (HH:MM:SS)
pub fn validate_time_format(time_str: &str) {
    let parts: Vec<&str> = time_str.split(':').collect();
    assert_eq!(parts.len(), 3, "Time should have 3 parts (HH:MM:SS)");
    assert_eq!(parts[0].len(), 2, "Hour should be 2 digits");
    assert_eq!(parts[1].len(), 2, "Minute should be 2 digits");
    assert_eq!(parts[2].len(), 2, "Second should be 2 digits");
}

/// Helper to validate timestamp format (YYYY-MM-DD HH:MM:SS)
pub fn validate_timestamp_format(timestamp_str: &str) {
    let main_parts: Vec<&str> = timestamp_str.split(' ').collect();
    assert_eq!(main_parts.len(), 2, "Timestamp should have date and time");

    let date_parts: Vec<&str> = main_parts[0].split('-').collect();
    assert_eq!(date_parts.len(), 3, "Date part should have 3 components");

    let time_parts: Vec<&str> = main_parts[1].split(':').collect();
    assert_eq!(time_parts.len(), 3, "Time part should have 3 components");
}

/// Helper to validate fractional seconds precision
pub fn validate_fractional_precision(value_str: &str, expected_precision: usize) {
    if expected_precision == 0 {
        assert!(
            !value_str.contains('.'),
            "Precision 0 should not contain fractional seconds, got: {}",
            value_str
        );
    } else {
        assert!(
            value_str.contains('.'),
            "Precision {} should contain fractional seconds",
            expected_precision
        );
        let fractional = value_str.split('.').nth(1).expect("Should have fractional part");
        assert_eq!(
            fractional.len(),
            expected_precision,
            "Fractional part should be {} digits, got: {}",
            expected_precision,
            value_str
        );
    }
}
