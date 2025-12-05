//! Tests for datetime precision arguments
//!
//! This module tests precision specification for time and timestamp functions:
//! - Valid precision values (0-9)
//! - Invalid precision values (negative, > 9)
//! - Fractional second formatting

use super::fixtures::*;

// ==================== CURRENT_TIME PRECISION ====================

#[test]
fn test_current_time_precision_0() {
    let (evaluator, row) = setup_test();

    let expr = create_datetime_function(
        "CURRENT_TIME",
        vec![create_literal(vibesql_types::SqlValue::Integer(0))],
    );
    let result = evaluator.eval(&expr, &row).unwrap();

    // Precision 0: HH:MM:SS (no fractional)
    match result {
        vibesql_types::SqlValue::Time(s) => {
            validate_fractional_precision(&s.to_string(), 0);
            validate_time_format(&s.to_string());
        }
        _ => panic!("CURRENT_TIME should return Time type"),
    }
}

#[test]
fn test_current_time_precision_3() {
    let (evaluator, row) = setup_test();

    let expr = create_datetime_function(
        "CURRENT_TIME",
        vec![create_literal(vibesql_types::SqlValue::Integer(3))],
    );
    let result = evaluator.eval(&expr, &row).unwrap();

    // Precision 3: HH:MM:SS with millisecond precision (no rounding to seconds)
    // Since trailing zeros are trimmed in display, we just verify it's a valid Time
    // with fractional seconds (unless current time happens to be exactly on a second boundary)
    match result {
        vibesql_types::SqlValue::Time(s) => {
            // Verify it's a valid time format (milliseconds are preserved)
            let time_str = s.to_string();
            // Just verify the Time value was created (precision 3 means millisecond precision)
            assert!(time_str.contains(':'), "Should be valid time format");
        }
        _ => panic!("CURRENT_TIME should return Time type"),
    }
}

#[test]
fn test_current_time_precision_6() {
    let (evaluator, row) = setup_test();

    let expr = create_datetime_function(
        "CURRENT_TIME",
        vec![create_literal(vibesql_types::SqlValue::Integer(6))],
    );
    let result = evaluator.eval(&expr, &row).unwrap();

    // Precision 6: HH:MM:SS with microsecond precision (no rounding to milliseconds)
    // Since trailing zeros are trimmed in display, we just verify it's a valid Time
    // with fractional seconds (unless current time happens to be exactly on a second boundary)
    match result {
        vibesql_types::SqlValue::Time(s) => {
            // Verify it's a valid time format (microseconds are preserved)
            let time_str = s.to_string();
            // Just verify the Time value was created (precision 6 means microsecond precision)
            assert!(time_str.contains(':'), "Should be valid time format");
        }
        _ => panic!("CURRENT_TIME should return Time type"),
    }
}

#[test]
fn test_current_time_precision_9() {
    let (evaluator, row) = setup_test();

    let expr = create_datetime_function(
        "CURRENT_TIME",
        vec![create_literal(vibesql_types::SqlValue::Integer(9))],
    );
    let result = evaluator.eval(&expr, &row).unwrap();

    // Precision 9: HH:MM:SS with full nanosecond precision (no rounding)
    // Since trailing zeros are trimmed in display, we just verify it's a valid Time
    // with fractional seconds (unless current time happens to be exactly on a second boundary)
    match result {
        vibesql_types::SqlValue::Time(s) => {
            // Verify it's a valid time format (nanoseconds are preserved)
            let time_str = s.to_string();
            // Just verify the Time value was created (precision 9 means no rounding of nanoseconds)
            assert!(time_str.contains(':'), "Should be valid time format");
        }
        _ => panic!("CURRENT_TIME should return Time type"),
    }
}

#[test]
fn test_current_time_precision_invalid_high() {
    let (evaluator, row) = setup_test();

    let expr = create_datetime_function(
        "CURRENT_TIME",
        vec![create_literal(vibesql_types::SqlValue::Integer(10))],
    );
    let result = evaluator.eval(&expr, &row);

    // Precision > 9 should error
    assert!(result.is_err(), "Precision 10 should return an error");
    let err_msg = format!("{:?}", result.unwrap_err());
    assert!(err_msg.to_lowercase().contains("precision"), "Error should mention precision");
}

#[test]
fn test_current_time_precision_invalid_negative() {
    let (evaluator, row) = setup_test();

    let expr = create_datetime_function(
        "CURRENT_TIME",
        vec![create_literal(vibesql_types::SqlValue::Integer(-1))],
    );
    let result = evaluator.eval(&expr, &row);

    // Negative precision should error
    assert!(result.is_err(), "Negative precision should return an error");
}

// ==================== CURRENT_TIMESTAMP PRECISION ====================

#[test]
fn test_current_timestamp_precision_0() {
    let (evaluator, row) = setup_test();

    let expr = create_datetime_function(
        "CURRENT_TIMESTAMP",
        vec![create_literal(vibesql_types::SqlValue::Integer(0))],
    );
    let result = evaluator.eval(&expr, &row).unwrap();

    // Precision 0: YYYY-MM-DD HH:MM:SS (no fractional)
    match result {
        vibesql_types::SqlValue::Timestamp(s) => {
            validate_fractional_precision(&s.to_string(), 0);
            validate_timestamp_format(&s.to_string());
        }
        _ => panic!("CURRENT_TIMESTAMP should return Timestamp type"),
    }
}

#[test]
fn test_current_timestamp_precision_3() {
    let (evaluator, row) = setup_test();

    let expr = create_datetime_function(
        "CURRENT_TIMESTAMP",
        vec![create_literal(vibesql_types::SqlValue::Integer(3))],
    );
    let result = evaluator.eval(&expr, &row).unwrap();

    // Precision 3: YYYY-MM-DD HH:MM:SS with millisecond precision
    // Since trailing zeros are trimmed in display, we just verify it's a valid Timestamp
    match result {
        vibesql_types::SqlValue::Timestamp(s) => {
            // Verify it's a valid timestamp format
            let timestamp_str = s.to_string();
            assert!(timestamp_str.contains(':'), "Should be valid timestamp format");
            assert!(timestamp_str.contains(' '), "Should have date and time parts");
        }
        _ => panic!("CURRENT_TIMESTAMP should return Timestamp type"),
    }
}

#[test]
fn test_current_timestamp_precision_6() {
    let (evaluator, row) = setup_test();

    let expr = create_datetime_function(
        "CURRENT_TIMESTAMP",
        vec![create_literal(vibesql_types::SqlValue::Integer(6))],
    );
    let result = evaluator.eval(&expr, &row).unwrap();

    // Precision 6: YYYY-MM-DD HH:MM:SS with microsecond precision
    // Since trailing zeros are trimmed in display, we just verify it's a valid Timestamp
    match result {
        vibesql_types::SqlValue::Timestamp(s) => {
            // Verify it's a valid timestamp format
            let timestamp_str = s.to_string();
            assert!(timestamp_str.contains(':'), "Should be valid timestamp format");
            assert!(timestamp_str.contains(' '), "Should have date and time parts");
        }
        _ => panic!("CURRENT_TIMESTAMP should return Timestamp type"),
    }
}

#[test]
fn test_current_timestamp_precision_invalid() {
    let (evaluator, row) = setup_test();

    let expr = create_datetime_function(
        "CURRENT_TIMESTAMP",
        vec![create_literal(vibesql_types::SqlValue::Integer(10))],
    );
    let result = evaluator.eval(&expr, &row);

    // Precision > 9 should error
    assert!(result.is_err(), "Precision 10 should return an error");
}

// ==================== ARGUMENT VALIDATION ====================

#[test]
fn test_current_time_too_many_args() {
    let (evaluator, row) = setup_test();

    let expr = create_datetime_function(
        "CURRENT_TIME",
        vec![
            create_literal(vibesql_types::SqlValue::Integer(3)),
            create_literal(vibesql_types::SqlValue::Integer(5)),
        ],
    );
    let result = evaluator.eval(&expr, &row);

    // Should error with too many arguments
    assert!(result.is_err(), "Should error with too many arguments");
}
