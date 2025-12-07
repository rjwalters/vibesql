//! Tests for current date/time functions (CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP, DATETIME)
//!
//! This module tests the SQL standard CURRENT_* functions and their aliases:
//! - CURRENT_DATE / CURDATE
//! - CURRENT_TIME / CURTIME
//! - CURRENT_TIMESTAMP / NOW
//! - DATETIME (SQLite-compatible datetime function)

use super::fixtures::*;

// ==================== CURRENT_DATE ====================

#[test]
fn test_current_date_format() {
    let (evaluator, row) = setup_test();

    let expr = create_datetime_function("CURRENT_DATE", vec![]);
    let result = evaluator.eval(&expr, &row).unwrap();

    // Verify it returns a Date type with YYYY-MM-DD format
    match result {
        vibesql_types::SqlValue::Date(s) => {
            validate_date_format(&s.to_string());
        }
        _ => panic!("CURRENT_DATE should return Date type"),
    }
}

#[test]
fn test_curdate_alias() {
    let (evaluator, row) = setup_test();

    let expr = create_datetime_function("CURDATE", vec![]);
    let result = evaluator.eval(&expr, &row).unwrap();

    // Verify CURDATE is an alias for CURRENT_DATE
    assert!(matches!(result, vibesql_types::SqlValue::Date(_)));
}

// ==================== CURRENT_TIME ====================

#[test]
fn test_current_time_format() {
    let (evaluator, row) = setup_test();

    let expr = create_datetime_function("CURRENT_TIME", vec![]);
    let result = evaluator.eval(&expr, &row).unwrap();

    // Verify it returns a Time type with HH:MM:SS format
    match result {
        vibesql_types::SqlValue::Time(s) => {
            validate_time_format(&s.to_string());
        }
        _ => panic!("CURRENT_TIME should return Time type"),
    }
}

#[test]
fn test_curtime_alias() {
    let (evaluator, row) = setup_test();

    let expr = create_datetime_function("CURTIME", vec![]);
    let result = evaluator.eval(&expr, &row).unwrap();

    // Verify CURTIME is an alias for CURRENT_TIME
    assert!(matches!(result, vibesql_types::SqlValue::Time(_)));
}

// ==================== CURRENT_TIMESTAMP ====================

#[test]
fn test_current_timestamp_format() {
    let (evaluator, row) = setup_test();

    let expr = create_datetime_function("CURRENT_TIMESTAMP", vec![]);
    let result = evaluator.eval(&expr, &row).unwrap();

    // Verify it returns a Timestamp type with YYYY-MM-DD HH:MM:SS format
    match result {
        vibesql_types::SqlValue::Timestamp(s) => {
            validate_timestamp_format(&s.to_string());
        }
        _ => panic!("CURRENT_TIMESTAMP should return Timestamp type"),
    }
}

#[test]
fn test_now_alias() {
    let (evaluator, row) = setup_test();

    let expr = create_datetime_function("NOW", vec![]);
    let result = evaluator.eval(&expr, &row).unwrap();

    // Verify NOW is an alias for CURRENT_TIMESTAMP
    assert!(matches!(result, vibesql_types::SqlValue::Timestamp(_)));
}

// ==================== DATETIME ====================

#[test]
fn test_datetime_now() {
    let (evaluator, row) = setup_test();

    let expr = create_datetime_function(
        "DATETIME",
        vec![create_literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("now")))],
    );
    let result = evaluator.eval(&expr, &row).unwrap();

    // Verify it returns a Timestamp type with YYYY-MM-DD HH:MM:SS format
    match result {
        vibesql_types::SqlValue::Timestamp(s) => {
            validate_timestamp_format(&s.to_string());
        }
        _ => panic!("DATETIME('now') should return Timestamp type"),
    }
}

#[test]
fn test_datetime_now_case_insensitive() {
    let (evaluator, row) = setup_test();

    let expr = create_datetime_function(
        "DATETIME",
        vec![create_literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("NOW")))],
    );
    let result = evaluator.eval(&expr, &row).unwrap();

    // Verify case-insensitive 'NOW' works
    assert!(matches!(result, vibesql_types::SqlValue::Timestamp(_)));
}

#[test]
fn test_datetime_with_timestamp_string() {
    let (evaluator, row) = setup_test();

    let expr = create_datetime_function(
        "DATETIME",
        vec![create_literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("2024-03-15 14:30:45")))],
    );
    let result = evaluator.eval(&expr, &row).unwrap();

    match result {
        vibesql_types::SqlValue::Timestamp(ts) => {
            assert_eq!(ts.to_string(), "2024-03-15 14:30:45");
        }
        _ => panic!("DATETIME with timestamp string should return Timestamp type"),
    }
}

#[test]
fn test_datetime_with_date_string() {
    let (evaluator, row) = setup_test();

    let expr = create_datetime_function(
        "DATETIME",
        vec![create_literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("2024-03-15")))],
    );
    let result = evaluator.eval(&expr, &row).unwrap();

    match result {
        vibesql_types::SqlValue::Timestamp(ts) => {
            // Should add 00:00:00 time
            assert_eq!(ts.to_string(), "2024-03-15 00:00:00");
        }
        _ => panic!("DATETIME with date string should return Timestamp type"),
    }
}

#[test]
fn test_datetime_with_iso_format() {
    let (evaluator, row) = setup_test();

    let expr = create_datetime_function(
        "DATETIME",
        vec![create_literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("2024-03-15T14:30:45")))],
    );
    let result = evaluator.eval(&expr, &row).unwrap();

    match result {
        vibesql_types::SqlValue::Timestamp(ts) => {
            assert_eq!(ts.to_string(), "2024-03-15 14:30:45");
        }
        _ => panic!("DATETIME with ISO format should return Timestamp type"),
    }
}

#[test]
fn test_datetime_with_null() {
    let (evaluator, row) = setup_test();

    let expr =
        create_datetime_function("DATETIME", vec![create_literal(vibesql_types::SqlValue::Null)]);
    let result = evaluator.eval(&expr, &row).unwrap();

    assert!(matches!(result, vibesql_types::SqlValue::Null));
}

#[test]
fn test_datetime_with_date_value() {
    let (evaluator, row) = setup_test();

    let date = vibesql_types::Date::new(2024, 3, 15).unwrap();
    let expr = create_datetime_function(
        "DATETIME",
        vec![create_literal(vibesql_types::SqlValue::Date(date))],
    );
    let result = evaluator.eval(&expr, &row).unwrap();

    match result {
        vibesql_types::SqlValue::Timestamp(ts) => {
            assert_eq!(ts.to_string(), "2024-03-15 00:00:00");
        }
        _ => panic!("DATETIME with Date value should return Timestamp type"),
    }
}

#[test]
fn test_datetime_with_timestamp_value() {
    let (evaluator, row) = setup_test();

    let date = vibesql_types::Date::new(2024, 3, 15).unwrap();
    let time = vibesql_types::Time::new(14, 30, 45, 0).unwrap();
    let timestamp = vibesql_types::Timestamp::new(date, time);
    let expr = create_datetime_function(
        "DATETIME",
        vec![create_literal(vibesql_types::SqlValue::Timestamp(timestamp))],
    );
    let result = evaluator.eval(&expr, &row).unwrap();

    match result {
        vibesql_types::SqlValue::Timestamp(ts) => {
            assert_eq!(ts.to_string(), "2024-03-15 14:30:45");
        }
        _ => panic!("DATETIME with Timestamp value should return Timestamp type"),
    }
}

#[test]
fn test_datetime_no_arguments_error() {
    let (evaluator, row) = setup_test();

    let expr = create_datetime_function("DATETIME", vec![]);
    let result = evaluator.eval(&expr, &row);

    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("requires at least 1 argument"));
}

#[test]
fn test_datetime_invalid_string_error() {
    let (evaluator, row) = setup_test();

    let expr = create_datetime_function(
        "DATETIME",
        vec![create_literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("invalid-date")))],
    );
    let result = evaluator.eval(&expr, &row);

    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("Unable to parse datetime string"));
}

#[test]
fn test_datetime_modifiers_not_supported() {
    let (evaluator, row) = setup_test();

    let expr = create_datetime_function(
        "DATETIME",
        vec![
            create_literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("now"))),
            create_literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("+1 day"))),
        ],
    );
    let result = evaluator.eval(&expr, &row);

    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("modifiers not yet supported"));
}
