//! Type coercion utilities for automatic type conversion
//!
//! This module provides utilities for coercing between SQL types, particularly
//! for string-to-date conversions in date/time contexts.

use chrono::{Datelike, NaiveDate};
use vibesql_types::SqlValue;

use crate::errors::ExecutorError;

/// Coerce a VARCHAR SqlValue to DATE if it's in a valid date format
///
/// Supports:
/// - ISO 8601 format: YYYY-MM-DD
/// - DATE and TIMESTAMP values (pass through or extract date)
/// - NULL values (returns NULL)
///
/// # Examples
///
/// ```
/// use vibesql_types::SqlValue;
/// use vibesql_executor::evaluator::coercion::coerce_to_date;
///
/// // String to date
/// let result = coerce_to_date(&SqlValue::Varchar(arcstr::ArcStr::from("2024-01-01")));
/// assert!(matches!(result, Ok(SqlValue::Date(_))));
///
/// // NULL handling
/// let result = coerce_to_date(&SqlValue::Null);
/// assert_eq!(result, Ok(SqlValue::Null));
/// ```
pub fn coerce_to_date(value: &SqlValue) -> Result<SqlValue, ExecutorError> {
    match value {
        SqlValue::Date(_) => Ok(value.clone()),
        SqlValue::Timestamp(ts) => Ok(SqlValue::Date(ts.date)),
        SqlValue::Varchar(s) | SqlValue::Character(s) => parse_date_string(s),
        SqlValue::Null => Ok(SqlValue::Null),
        _ => Err(ExecutorError::TypeMismatch {
            left: value.clone(),
            op: "date coercion".to_string(),
            right: SqlValue::Null,
        }),
    }
}

/// Parse a date string in various formats
///
/// Currently supports:
/// - ISO 8601: YYYY-MM-DD (e.g., "2024-01-01")
///
/// Future formats can be added here (MySQL format, etc.)
fn parse_date_string(s: &str) -> Result<SqlValue, ExecutorError> {
    // Try parsing as ISO 8601 (YYYY-MM-DD)
    if let Ok(date) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        let vibe_date = vibesql_types::Date::new(date.year(), date.month() as u8, date.day() as u8)
            .map_err(|e| ExecutorError::UnsupportedFeature(format!("Invalid date: {}", e)))?;
        return Ok(SqlValue::Date(vibe_date));
    }

    Err(ExecutorError::UnsupportedFeature(format!(
        "Cannot parse '{}' as date. Expected format: YYYY-MM-DD",
        s
    )))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_coerce_date_passthrough() {
        let date = vibesql_types::Date::new(2024, 1, 15).unwrap();
        let result = coerce_to_date(&SqlValue::Date(date)).unwrap();
        assert_eq!(result, SqlValue::Date(date));
    }

    #[test]
    fn test_coerce_timestamp_to_date() {
        let date = vibesql_types::Date::new(2024, 1, 15).unwrap();
        let time = vibesql_types::Time::new(10, 30, 45, 0).unwrap();
        let timestamp = vibesql_types::Timestamp::new(date, time);
        let result = coerce_to_date(&SqlValue::Timestamp(timestamp)).unwrap();
        assert_eq!(result, SqlValue::Date(date));
    }

    #[test]
    fn test_coerce_varchar_to_date() {
        let result = coerce_to_date(&SqlValue::Varchar(arcstr::ArcStr::from("2024-01-15"))).unwrap();
        let expected = vibesql_types::Date::new(2024, 1, 15).unwrap();
        assert_eq!(result, SqlValue::Date(expected));
    }

    #[test]
    fn test_coerce_character_to_date() {
        let result = coerce_to_date(&SqlValue::Character(arcstr::ArcStr::from("2024-12-31"))).unwrap();
        let expected = vibesql_types::Date::new(2024, 12, 31).unwrap();
        assert_eq!(result, SqlValue::Date(expected));
    }

    #[test]
    fn test_coerce_null_returns_null() {
        let result = coerce_to_date(&SqlValue::Null).unwrap();
        assert_eq!(result, SqlValue::Null);
    }

    #[test]
    fn test_coerce_invalid_date_string() {
        let result = coerce_to_date(&SqlValue::Varchar(arcstr::ArcStr::from("not-a-date")));
        assert!(result.is_err());
    }

    #[test]
    fn test_coerce_invalid_date_format() {
        let result = coerce_to_date(&SqlValue::Varchar(arcstr::ArcStr::from("01/15/2024")));
        assert!(result.is_err());
    }

    #[test]
    fn test_coerce_invalid_date_values() {
        // Month out of range
        let result = coerce_to_date(&SqlValue::Varchar(arcstr::ArcStr::from("2024-13-01")));
        assert!(result.is_err());

        // Day out of range
        let result = coerce_to_date(&SqlValue::Varchar(arcstr::ArcStr::from("2024-02-30")));
        assert!(result.is_err());
    }

    #[test]
    fn test_coerce_integer_returns_error() {
        let result = coerce_to_date(&SqlValue::Integer(20240115));
        assert!(result.is_err());
    }

    #[test]
    fn test_coerce_leap_year_date() {
        let result = coerce_to_date(&SqlValue::Varchar(arcstr::ArcStr::from("2024-02-29"))).unwrap();
        let expected = vibesql_types::Date::new(2024, 2, 29).unwrap();
        assert_eq!(result, SqlValue::Date(expected));
    }

    #[test]
    fn test_coerce_non_leap_year_feb_29() {
        let result = coerce_to_date(&SqlValue::Varchar(arcstr::ArcStr::from("2023-02-29")));
        assert!(result.is_err());
    }
}
