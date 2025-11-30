//! Value conversion utilities for SIMD filter operations
//!
//! This module provides functions to convert SqlValue types to native numeric types
//! that can be used in SIMD-accelerated comparisons.

use super::super::filter::parse_date_string;
use vibesql_types::SqlValue;

/// Convert SqlValue to f64 for numeric comparisons
pub fn value_to_f64(value: &SqlValue) -> Option<f64> {
    match value {
        SqlValue::Integer(n) => Some(*n as f64),
        SqlValue::Bigint(n) => Some(*n as f64),
        SqlValue::Smallint(n) => Some(*n as f64),
        SqlValue::Float(n) => Some(*n as f64),
        SqlValue::Double(n) => Some(*n),
        // SqlValue::Numeric contains an f64 internally, extract directly
        // without lossy string round-trip (fixes #2857)
        SqlValue::Numeric(n) => Some(*n),
        SqlValue::Real(n) => Some(*n as f64),
        _ => None,
    }
}

/// Convert SqlValue::Date or string to i32 (days since Unix epoch)
pub fn value_to_date_i32(value: &SqlValue) -> Option<i32> {
    match value {
        SqlValue::Date(date) => Some(date_to_days_since_epoch(date)),
        // Handle text strings that look like dates (YYYY-MM-DD format)
        SqlValue::Character(s) | SqlValue::Varchar(s) => {
            parse_date_string(s).map(|d| date_to_days_since_epoch(&d))
        }
        _ => None,
    }
}

/// Convert Date to days since Unix epoch (1970-01-01)
pub fn date_to_days_since_epoch(date: &vibesql_types::Date) -> i32 {
    // Simple calculation: days since Unix epoch
    let year_days = (date.year - 1970) * 365;
    let leap_years =
        ((date.year - 1969) / 4) - ((date.year - 1901) / 100) + ((date.year - 1601) / 400);
    let month_days: i32 = match date.month {
        1 => 0,
        2 => 31,
        3 => 59,
        4 => 90,
        5 => 120,
        6 => 151,
        7 => 181,
        8 => 212,
        9 => 243,
        10 => 273,
        11 => 304,
        12 => 334,
        _ => 0,
    };

    // Add leap day if after February in a leap year
    let is_leap = date.year % 4 == 0 && (date.year % 100 != 0 || date.year % 400 == 0);
    let leap_adjustment = if is_leap && date.month > 2 { 1 } else { 0 };

    year_days + leap_years + month_days + date.day as i32 - 1 + leap_adjustment
}

/// Convert SqlValue::Timestamp to i64 (microseconds since Unix epoch)
pub fn value_to_timestamp_i64(value: &SqlValue) -> Option<i64> {
    match value {
        SqlValue::Timestamp(ts) => Some(timestamp_to_microseconds(ts)),
        _ => None,
    }
}

/// Convert Timestamp to microseconds since Unix epoch
pub fn timestamp_to_microseconds(ts: &vibesql_types::Timestamp) -> i64 {
    let days = date_to_days_since_epoch(&ts.date);
    let micros_from_days = days as i64 * 86_400_000_000; // 24 * 60 * 60 * 1_000_000
    let micros_from_time = (ts.time.hour as i64 * 3_600_000_000)
        + (ts.time.minute as i64 * 60_000_000)
        + (ts.time.second as i64 * 1_000_000)
        + (ts.time.nanosecond as i64 / 1000); // Convert nanoseconds to microseconds
    micros_from_days + micros_from_time
}
