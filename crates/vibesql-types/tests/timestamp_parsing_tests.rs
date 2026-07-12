use vibesql_types::*;

// ============================================================================
// Timestamp Parsing Tests - Test flexible timestamp format support
// Issue #1187: Support multiple timestamp formats for automatic parsing
// ============================================================================

// ----------------------------------------------------------------------------
// ISO 8601 Format Tests (with 'T' separator)
// ----------------------------------------------------------------------------

#[test]
fn test_iso8601_basic_format() {
    let result = "2025-11-10T08:24:34".parse::<Timestamp>();
    assert!(result.is_ok());
    let ts = result.unwrap();
    assert_eq!(ts.date.year, 2025);
    assert_eq!(ts.date.month, 11);
    assert_eq!(ts.date.day, 10);
    assert_eq!(ts.time.hour, 8);
    assert_eq!(ts.time.minute, 24);
    assert_eq!(ts.time.second, 34);
}

#[test]
fn test_iso8601_with_microseconds() {
    let result = "2025-11-10T08:24:34.602988".parse::<Timestamp>();
    assert!(result.is_ok());
    let ts = result.unwrap();
    assert_eq!(ts.date.year, 2025);
    assert_eq!(ts.time.hour, 8);
    assert_eq!(ts.time.nanosecond, 602988000);
}

#[test]
fn test_iso8601_with_milliseconds() {
    let result = "2025-11-10T08:24:34.123".parse::<Timestamp>();
    assert!(result.is_ok());
    let ts = result.unwrap();
    assert_eq!(ts.time.nanosecond, 123000000);
}

#[test]
fn test_iso8601_with_nanoseconds() {
    let result = "2025-11-10T08:24:34.123456789".parse::<Timestamp>();
    assert!(result.is_ok());
    let ts = result.unwrap();
    assert_eq!(ts.time.nanosecond, 123456789);
}

// ----------------------------------------------------------------------------
// ISO 8601 with Timezone Tests
// ----------------------------------------------------------------------------

#[test]
fn test_iso8601_with_utc_z() {
    let result = "2025-11-10T08:24:34Z".parse::<Timestamp>();
    assert!(result.is_ok());
    let ts = result.unwrap();
    assert_eq!(ts.date.year, 2025);
    assert_eq!(ts.date.month, 11);
    assert_eq!(ts.date.day, 10);
    assert_eq!(ts.time.hour, 8);
    assert_eq!(ts.time.minute, 24);
    assert_eq!(ts.time.second, 34);
}

#[test]
fn test_iso8601_with_utc_z_lowercase() {
    let result = "2025-11-10T08:24:34z".parse::<Timestamp>();
    assert!(result.is_ok());
}

#[test]
fn test_iso8601_with_positive_timezone() {
    let result = "2025-11-10T08:24:34+05:00".parse::<Timestamp>();
    assert!(result.is_ok());
    let ts = result.unwrap();
    assert_eq!(ts.time.hour, 8);
    assert_eq!(ts.time.minute, 24);
}

#[test]
fn test_iso8601_with_negative_timezone() {
    let result = "2025-11-10T08:24:34-05:00".parse::<Timestamp>();
    assert!(result.is_ok());
}

#[test]
fn test_iso8601_with_timezone_no_colon() {
    let result = "2025-11-10T08:24:34+0500".parse::<Timestamp>();
    assert!(result.is_ok());
}

#[test]
fn test_iso8601_with_timezone_short_format() {
    let result = "2025-11-10T08:24:34+05".parse::<Timestamp>();
    assert!(result.is_ok());
}

#[test]
fn test_rfc3339_format() {
    // RFC 3339 is a profile of ISO 8601
    let result = "2025-11-10T08:24:34.602988Z".parse::<Timestamp>();
    assert!(result.is_ok());
    let ts = result.unwrap();
    assert_eq!(ts.date.year, 2025);
    assert_eq!(ts.time.nanosecond, 602988000);
}

// ----------------------------------------------------------------------------
// Space-Separated Format Tests (backward compatibility)
// ----------------------------------------------------------------------------

#[test]
fn test_space_separated_basic() {
    let result = "2025-11-10 08:24:34".parse::<Timestamp>();
    assert!(result.is_ok());
    let ts = result.unwrap();
    assert_eq!(ts.date.year, 2025);
    assert_eq!(ts.date.month, 11);
    assert_eq!(ts.date.day, 10);
    assert_eq!(ts.time.hour, 8);
    assert_eq!(ts.time.minute, 24);
    assert_eq!(ts.time.second, 34);
}

#[test]
fn test_space_separated_with_microseconds() {
    let result = "2025-11-10 08:24:34.123456".parse::<Timestamp>();
    assert!(result.is_ok());
    let ts = result.unwrap();
    assert_eq!(ts.time.nanosecond, 123456000);
}

#[test]
fn test_space_separated_with_extra_whitespace() {
    let result = "  2025-11-10   08:24:34  ".parse::<Timestamp>();
    assert!(result.is_ok());
}

// ----------------------------------------------------------------------------
// Date-Only Format Tests
// ----------------------------------------------------------------------------

#[test]
fn test_date_only_format() {
    let result = "2025-11-10".parse::<Timestamp>();
    assert!(result.is_ok());
    let ts = result.unwrap();
    assert_eq!(ts.date.year, 2025);
    assert_eq!(ts.date.month, 11);
    assert_eq!(ts.date.day, 10);
    // Should default to midnight
    assert_eq!(ts.time.hour, 0);
    assert_eq!(ts.time.minute, 0);
    assert_eq!(ts.time.second, 0);
    assert_eq!(ts.time.nanosecond, 0);
}

// ----------------------------------------------------------------------------
// Edge Cases and Special Values
// ----------------------------------------------------------------------------

#[test]
fn test_midnight_timestamp() {
    let result = "2025-01-01T00:00:00".parse::<Timestamp>();
    assert!(result.is_ok());
    let ts = result.unwrap();
    assert_eq!(ts.time.hour, 0);
    assert_eq!(ts.time.minute, 0);
    assert_eq!(ts.time.second, 0);
}

#[test]
fn test_end_of_day_timestamp() {
    let result = "2025-01-01T23:59:59".parse::<Timestamp>();
    assert!(result.is_ok());
    let ts = result.unwrap();
    assert_eq!(ts.time.hour, 23);
    assert_eq!(ts.time.minute, 59);
    assert_eq!(ts.time.second, 59);
}

#[test]
fn test_leap_year_date() {
    let result = "2024-02-29T12:00:00".parse::<Timestamp>();
    assert!(result.is_ok());
    let ts = result.unwrap();
    assert_eq!(ts.date.year, 2024);
    assert_eq!(ts.date.month, 2);
    assert_eq!(ts.date.day, 29);
}

// ----------------------------------------------------------------------------
// Error Cases
// ----------------------------------------------------------------------------

#[test]
fn test_invalid_format_no_time() {
    let result = "not-a-timestamp".parse::<Timestamp>();
    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(err.contains("Invalid timestamp format"));
    assert!(err.contains("ISO 8601"));
}

#[test]
fn test_invalid_date_component() {
    let result = "2025-13-10T08:24:34".parse::<Timestamp>();
    assert!(result.is_err());
}

// ----------------------------------------------------------------------------
// Calendar-invalid dates (issue #6022)
// Date::new() must reject dates that are in-range component-wise but do not
// exist on the calendar (wrong day-for-month, non-leap Feb 29). These must be
// rejected both via Date::new() directly and via the FromStr / Timestamp paths.
// ----------------------------------------------------------------------------

#[test]
fn test_invalid_date_feb30() {
    // February never has 30 days, in any year.
    assert!(Date::new(2024, 2, 30).is_err());
    assert!(Date::new(2023, 2, 30).is_err());
    assert!("2024-02-30".parse::<Date>().is_err());
    assert!("2024-02-30T12:00:00".parse::<Timestamp>().is_err());
}

#[test]
fn test_invalid_date_feb29_non_leap_year() {
    // 2023 is not a leap year, so Feb 29 does not exist.
    assert!(Date::new(2023, 2, 29).is_err());
    assert!("2023-02-29".parse::<Date>().is_err());
    assert!("2023-02-29T12:00:00".parse::<Timestamp>().is_err());

    // 1900 is divisible by 100 but not 400 -> not a leap year.
    assert!(Date::new(1900, 2, 29).is_err());
    assert!("1900-02-29".parse::<Date>().is_err());

    // Sanity: 2000 IS a leap year (divisible by 400).
    assert!(Date::new(2000, 2, 29).is_ok());
}

#[test]
fn test_invalid_date_apr31() {
    // April has 30 days, so the 31st does not exist. Same for the other
    // 30-day months (Jun, Sep, Nov).
    assert!(Date::new(2024, 4, 31).is_err());
    assert!(Date::new(2024, 6, 31).is_err());
    assert!(Date::new(2024, 9, 31).is_err());
    assert!(Date::new(2024, 11, 31).is_err());
    assert!("2024-04-31".parse::<Date>().is_err());
    assert!("2024-04-31T00:00:00".parse::<Timestamp>().is_err());

    // Sanity: 31-day months still accept the 31st.
    assert!(Date::new(2024, 1, 31).is_ok());
    assert!(Date::new(2024, 12, 31).is_ok());
}

#[test]
fn test_valid_leap_day_via_date_new() {
    // Direct Date::new() confirmation that a valid leap day is accepted
    // (complements test_leap_year_date which goes through the Timestamp path).
    assert!(Date::new(2024, 2, 29).is_ok());
}

#[test]
fn test_invalid_time_component() {
    let result = "2025-11-10T25:00:00".parse::<Timestamp>();
    assert!(result.is_err());
}

#[test]
fn test_empty_string() {
    let result = "".parse::<Timestamp>();
    assert!(result.is_err());
}

// ----------------------------------------------------------------------------
// Display Format Tests
// ----------------------------------------------------------------------------

#[test]
fn test_timestamp_display_format() {
    let ts = "2025-11-10T08:24:34".parse::<Timestamp>().unwrap();
    let display = format!("{}", ts);
    // Display should use space-separated format
    assert_eq!(display, "2025-11-10 08:24:34");
}

#[test]
fn test_timestamp_roundtrip() {
    let original = "2025-11-10T08:24:34.123456";
    let ts = original.parse::<Timestamp>().unwrap();
    // Should be able to parse the display format back
    let display = format!("{}", ts);
    let ts2 = display.parse::<Timestamp>().unwrap();
    assert_eq!(ts.date, ts2.date);
    assert_eq!(ts.time.hour, ts2.time.hour);
    assert_eq!(ts.time.minute, ts2.time.minute);
    assert_eq!(ts.time.second, ts2.time.second);
}

// ----------------------------------------------------------------------------
// Real-World Use Cases
// ----------------------------------------------------------------------------

#[test]
fn test_sql_dump_timestamp_format() {
    // This is the format that was failing in the issue
    let result = "2025-11-10T08:24:34.602988".parse::<Timestamp>();
    assert!(result.is_ok());
    let ts = result.unwrap();
    assert_eq!(ts.date.year, 2025);
    assert_eq!(ts.date.month, 11);
    assert_eq!(ts.date.day, 10);
    assert_eq!(ts.time.hour, 8);
    assert_eq!(ts.time.minute, 24);
    assert_eq!(ts.time.second, 34);
    assert_eq!(ts.time.nanosecond, 602988000);
}

#[test]
fn test_api_response_format() {
    // Common format from REST APIs
    let result = "2025-11-10T14:30:00Z".parse::<Timestamp>();
    assert!(result.is_ok());
}

#[test]
fn test_database_export_format() {
    // Common format from PostgreSQL exports
    let result = "2025-11-10 14:30:00.123456".parse::<Timestamp>();
    assert!(result.is_ok());
}
