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
        vec![create_literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(
            "2024-03-15 14:30:45",
        )))],
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
        vec![create_literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(
            "2024-03-15T14:30:45",
        )))],
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
fn test_datetime_no_arguments_is_now() {
    // SQLite: an omitted time-value defaults to 'now', so datetime() is the
    // current date and time (date.test 2.40)
    let (evaluator, row) = setup_test();

    let expr = create_datetime_function("DATETIME", vec![]);
    let result = evaluator.eval(&expr, &row).unwrap();

    assert!(
        matches!(result, vibesql_types::SqlValue::Timestamp(_)),
        "datetime() should return the current timestamp, got {:?}",
        result
    );
}

#[test]
fn test_datetime_invalid_string_returns_null() {
    let (evaluator, row) = setup_test();

    let expr = create_datetime_function(
        "DATETIME",
        vec![create_literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(
            "invalid-date",
        )))],
    );
    let result = evaluator.eval(&expr, &row).unwrap();

    // Invalid datetime strings return NULL (SQLite behavior)
    assert!(matches!(result, vibesql_types::SqlValue::Null));
}

// ==================== DATETIME MODIFIERS ====================

#[test]
fn test_datetime_plus_one_day() {
    let (evaluator, row) = setup_test();

    let expr = create_datetime_function(
        "DATETIME",
        vec![
            create_literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(
                "2003-10-22 12:34:00",
            ))),
            create_literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("+1 day"))),
        ],
    );
    let result = evaluator.eval(&expr, &row).unwrap();

    match result {
        vibesql_types::SqlValue::Timestamp(ts) => {
            assert_eq!(ts.to_string(), "2003-10-23 12:34:00");
        }
        _ => panic!("Expected Timestamp, got {:?}", result),
    }
}

#[test]
fn test_datetime_minus_one_day() {
    let (evaluator, row) = setup_test();

    let expr = create_datetime_function(
        "DATETIME",
        vec![
            create_literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(
                "2003-10-22 12:34:00",
            ))),
            create_literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("-1 day"))),
        ],
    );
    let result = evaluator.eval(&expr, &row).unwrap();

    match result {
        vibesql_types::SqlValue::Timestamp(ts) => {
            assert_eq!(ts.to_string(), "2003-10-21 12:34:00");
        }
        _ => panic!("Expected Timestamp, got {:?}", result),
    }
}

#[test]
fn test_datetime_fractional_day() {
    let (evaluator, row) = setup_test();

    // +1.25 days = 1 day + 6 hours
    let expr = create_datetime_function(
        "DATETIME",
        vec![
            create_literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(
                "2003-10-22 12:34:00",
            ))),
            create_literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("+1.25 day"))),
        ],
    );
    let result = evaluator.eval(&expr, &row).unwrap();

    match result {
        vibesql_types::SqlValue::Timestamp(ts) => {
            assert_eq!(ts.to_string(), "2003-10-23 18:34:00");
        }
        _ => panic!("Expected Timestamp, got {:?}", result),
    }
}

#[test]
fn test_datetime_plus_months() {
    let (evaluator, row) = setup_test();

    let expr = create_datetime_function(
        "DATETIME",
        vec![
            create_literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(
                "2003-10-22 12:34:00",
            ))),
            create_literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("11 month"))),
        ],
    );
    let result = evaluator.eval(&expr, &row).unwrap();

    match result {
        vibesql_types::SqlValue::Timestamp(ts) => {
            assert_eq!(ts.to_string(), "2004-09-22 12:34:00");
        }
        _ => panic!("Expected Timestamp, got {:?}", result),
    }
}

#[test]
fn test_datetime_minus_years() {
    let (evaluator, row) = setup_test();

    let expr = create_datetime_function(
        "DATETIME",
        vec![
            create_literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(
                "2003-10-22 12:34:00",
            ))),
            create_literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("-5 years"))),
        ],
    );
    let result = evaluator.eval(&expr, &row).unwrap();

    match result {
        vibesql_types::SqlValue::Timestamp(ts) => {
            assert_eq!(ts.to_string(), "1998-10-22 12:34:00");
        }
        _ => panic!("Expected Timestamp, got {:?}", result),
    }
}

#[test]
fn test_datetime_plus_minutes() {
    let (evaluator, row) = setup_test();

    let expr = create_datetime_function(
        "DATETIME",
        vec![
            create_literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(
                "2003-10-22 12:34:00",
            ))),
            create_literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("+10.5 minutes"))),
        ],
    );
    let result = evaluator.eval(&expr, &row).unwrap();

    match result {
        vibesql_types::SqlValue::Timestamp(ts) => {
            assert_eq!(ts.to_string(), "2003-10-22 12:44:30");
        }
        _ => panic!("Expected Timestamp, got {:?}", result),
    }
}

#[test]
fn test_datetime_minus_hours() {
    let (evaluator, row) = setup_test();

    let expr = create_datetime_function(
        "DATETIME",
        vec![
            create_literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(
                "2003-10-22 12:34:00",
            ))),
            create_literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("-1.25 hours"))),
        ],
    );
    let result = evaluator.eval(&expr, &row).unwrap();

    match result {
        vibesql_types::SqlValue::Timestamp(ts) => {
            assert_eq!(ts.to_string(), "2003-10-22 11:19:00");
        }
        _ => panic!("Expected Timestamp, got {:?}", result),
    }
}

#[test]
fn test_datetime_plus_seconds() {
    let (evaluator, row) = setup_test();

    let expr = create_datetime_function(
        "DATETIME",
        vec![
            create_literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(
                "2003-10-22 12:34:00",
            ))),
            create_literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("11 seconds"))),
        ],
    );
    let result = evaluator.eval(&expr, &row).unwrap();

    match result {
        vibesql_types::SqlValue::Timestamp(ts) => {
            assert_eq!(ts.to_string(), "2003-10-22 12:34:11");
        }
        _ => panic!("Expected Timestamp, got {:?}", result),
    }
}

#[test]
fn test_datetime_start_of_month() {
    let (evaluator, row) = setup_test();

    let expr = create_datetime_function(
        "DATETIME",
        vec![
            create_literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(
                "2003-10-22 12:34:00",
            ))),
            create_literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(
                "start of month",
            ))),
        ],
    );
    let result = evaluator.eval(&expr, &row).unwrap();

    match result {
        vibesql_types::SqlValue::Timestamp(ts) => {
            assert_eq!(ts.to_string(), "2003-10-01 00:00:00");
        }
        _ => panic!("Expected Timestamp, got {:?}", result),
    }
}

#[test]
fn test_datetime_start_of_year() {
    let (evaluator, row) = setup_test();

    let expr = create_datetime_function(
        "DATETIME",
        vec![
            create_literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(
                "2003-10-22 12:34:00",
            ))),
            create_literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("start of year"))),
        ],
    );
    let result = evaluator.eval(&expr, &row).unwrap();

    match result {
        vibesql_types::SqlValue::Timestamp(ts) => {
            assert_eq!(ts.to_string(), "2003-01-01 00:00:00");
        }
        _ => panic!("Expected Timestamp, got {:?}", result),
    }
}

#[test]
fn test_datetime_start_of_day() {
    let (evaluator, row) = setup_test();

    let expr = create_datetime_function(
        "DATETIME",
        vec![
            create_literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(
                "2003-10-22 12:34:00",
            ))),
            create_literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("start of day"))),
        ],
    );
    let result = evaluator.eval(&expr, &row).unwrap();

    match result {
        vibesql_types::SqlValue::Timestamp(ts) => {
            assert_eq!(ts.to_string(), "2003-10-22 00:00:00");
        }
        _ => panic!("Expected Timestamp, got {:?}", result),
    }
}

#[test]
fn test_datetime_weekday_modifier() {
    let (evaluator, row) = setup_test();

    // 2003-10-22 is a Wednesday (weekday 3)
    // weekday 0 (Sunday) should advance to 2003-10-26
    let expr = create_datetime_function(
        "DATETIME",
        vec![
            create_literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(
                "2003-10-22 12:34:00",
            ))),
            create_literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("weekday 0"))),
        ],
    );
    let result = evaluator.eval(&expr, &row).unwrap();

    match result {
        vibesql_types::SqlValue::Timestamp(ts) => {
            assert_eq!(ts.to_string(), "2003-10-26 12:34:00");
        }
        _ => panic!("Expected Timestamp, got {:?}", result),
    }
}

#[test]
fn test_datetime_weekday_same_day() {
    let (evaluator, row) = setup_test();

    // 2003-10-22 is a Wednesday (weekday 3)
    // weekday 3 should stay on the same day
    let expr = create_datetime_function(
        "DATETIME",
        vec![
            create_literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(
                "2003-10-22 12:34:00",
            ))),
            create_literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("weekday 3"))),
        ],
    );
    let result = evaluator.eval(&expr, &row).unwrap();

    match result {
        vibesql_types::SqlValue::Timestamp(ts) => {
            assert_eq!(ts.to_string(), "2003-10-22 12:34:00");
        }
        _ => panic!("Expected Timestamp, got {:?}", result),
    }
}

#[test]
fn test_datetime_multiple_modifiers() {
    let (evaluator, row) = setup_test();

    // Test chaining multiple modifiers: start of month, then +1 day
    let expr = create_datetime_function(
        "DATETIME",
        vec![
            create_literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(
                "2003-10-22 12:34:00",
            ))),
            create_literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(
                "start of month",
            ))),
            create_literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("+1 day"))),
        ],
    );
    let result = evaluator.eval(&expr, &row).unwrap();

    match result {
        vibesql_types::SqlValue::Timestamp(ts) => {
            assert_eq!(ts.to_string(), "2003-10-02 00:00:00");
        }
        _ => panic!("Expected Timestamp, got {:?}", result),
    }
}

#[test]
fn test_datetime_multiple_modifiers_complex() {
    let (evaluator, row) = setup_test();

    // Test: start of month, +1 month, -1 day = last day of current month at 00:00:00
    // From 2003-10-22: start of month -> 2003-10-01, +1 month -> 2003-11-01, -1 day -> 2003-10-31
    let expr = create_datetime_function(
        "DATETIME",
        vec![
            create_literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(
                "2003-10-22 12:34:00",
            ))),
            create_literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(
                "start of month",
            ))),
            create_literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("+1 month"))),
            create_literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("-1 day"))),
        ],
    );
    let result = evaluator.eval(&expr, &row).unwrap();

    match result {
        vibesql_types::SqlValue::Timestamp(ts) => {
            assert_eq!(ts.to_string(), "2003-10-31 00:00:00");
        }
        _ => panic!("Expected Timestamp, got {:?}", result),
    }
}

#[test]
fn test_datetime_unixepoch_modifier() {
    let (evaluator, row) = setup_test();

    // Unix epoch 946684800 = 2000-01-01 00:00:00 UTC
    let expr = create_datetime_function(
        "DATETIME",
        vec![
            create_literal(vibesql_types::SqlValue::Integer(946684800)),
            create_literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("unixepoch"))),
        ],
    );
    let result = evaluator.eval(&expr, &row).unwrap();

    match result {
        vibesql_types::SqlValue::Timestamp(ts) => {
            assert_eq!(ts.to_string(), "2000-01-01 00:00:00");
        }
        _ => panic!("Expected Timestamp, got {:?}", result),
    }
}

#[test]
fn test_datetime_unixepoch_zero() {
    let (evaluator, row) = setup_test();

    // Unix epoch 0 = 1970-01-01 00:00:00 UTC
    let expr = create_datetime_function(
        "DATETIME",
        vec![
            create_literal(vibesql_types::SqlValue::Integer(0)),
            create_literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("unixepoch"))),
        ],
    );
    let result = evaluator.eval(&expr, &row).unwrap();

    match result {
        vibesql_types::SqlValue::Timestamp(ts) => {
            assert_eq!(ts.to_string(), "1970-01-01 00:00:00");
        }
        _ => panic!("Expected Timestamp, got {:?}", result),
    }
}

#[test]
fn test_datetime_invalid_modifier_returns_null() {
    let (evaluator, row) = setup_test();

    let expr = create_datetime_function(
        "DATETIME",
        vec![
            create_literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(
                "2003-10-22 12:34:00",
            ))),
            create_literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("+5 bogus"))),
        ],
    );
    let result = evaluator.eval(&expr, &row).unwrap();

    // Invalid modifiers return NULL (SQLite behavior)
    assert!(matches!(result, vibesql_types::SqlValue::Null));
}

#[test]
fn test_datetime_invalid_weekday_returns_null() {
    let (evaluator, row) = setup_test();

    let expr = create_datetime_function(
        "DATETIME",
        vec![
            create_literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(
                "2003-10-22 12:34:00",
            ))),
            create_literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("weekday 7"))),
        ],
    );
    let result = evaluator.eval(&expr, &row).unwrap();

    // weekday 7 is invalid (should be 0-6)
    assert!(matches!(result, vibesql_types::SqlValue::Null));
}

#[test]
fn test_datetime_incomplete_start_of_returns_null() {
    let (evaluator, row) = setup_test();

    let expr = create_datetime_function(
        "DATETIME",
        vec![
            create_literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(
                "2003-10-22 12:34:00",
            ))),
            create_literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("start of"))),
        ],
    );
    let result = evaluator.eval(&expr, &row).unwrap();

    // Incomplete "start of" returns NULL
    assert!(matches!(result, vibesql_types::SqlValue::Null));
}

#[test]
fn test_datetime_invalid_start_of_unit_returns_null() {
    let (evaluator, row) = setup_test();

    let expr = create_datetime_function(
        "DATETIME",
        vec![
            create_literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(
                "2003-10-22 12:34:00",
            ))),
            create_literal(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(
                "start of bogus",
            ))),
        ],
    );
    let result = evaluator.eval(&expr, &row).unwrap();

    // Invalid "start of" unit returns NULL
    assert!(matches!(result, vibesql_types::SqlValue::Null));
}
