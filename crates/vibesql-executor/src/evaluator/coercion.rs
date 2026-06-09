//! Type coercion utilities for automatic type conversion
//!
//! This module provides utilities for coercing between SQL types, particularly
//! for string-to-date conversions in date/time contexts, and for coercing
//! WHERE-clause literals to match column types for primary-key index lookups.

use chrono::{Datelike, NaiveDate};
use vibesql_types::{DataType, SqlValue, TypeAffinity};

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
/// use vibesql_executor::evaluator::coercion::coerce_to_date;
/// use vibesql_types::SqlValue;
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

/// Coerce a value to match a column's data type using SQLite affinity rules.
///
/// This is used for PRIMARY KEY (and similar) index lookups where the literal
/// value in the WHERE clause may have a different type than the column. The
/// primary-key index `HashMap` is keyed on the **stored** representation of
/// PK values, so a WHERE-clause literal must be coerced into the same affinity
/// before the `HashMap::get(...)` call — otherwise lookups silently miss even
/// though the row exists and would match the full WHERE clause.
///
/// SQLite affinity rules applied:
/// - INTEGER/NUMERIC affinity column with string literal: try to parse as i64,
///   then f64; if neither parses, return the original value (lookup will miss).
/// - REAL affinity column with string literal: try to parse as f64.
/// - TEXT affinity column with numeric literal: format the number as a string.
/// - All other combinations: pass through unchanged.
///
/// # Examples
/// - `WHERE i = '12'` on INTEGER PRIMARY KEY → coerce `'12'` to `Integer(12)`
/// - `WHERE p = 1200` on TEXT PRIMARY KEY → coerce `1200` to `Varchar("1200")`
///
/// # Why this lives here
/// Previously this helper was duplicated at two SELECT-side sites and missing
/// entirely on the UPDATE/DELETE sites — see issue #5145. Consolidating here
/// guarantees the same affinity rules apply everywhere we look a PK literal
/// up in the in-memory index.
pub fn coerce_value_to_column_type(val: SqlValue, col_type: &DataType) -> SqlValue {
    let col_affinity = col_type.sqlite_affinity();

    match (col_affinity, &val) {
        // INTEGER/NUMERIC affinity column with string value: try to parse as number
        (
            TypeAffinity::Integer | TypeAffinity::Numeric,
            SqlValue::Varchar(s) | SqlValue::Character(s),
        ) => {
            // Try to parse as integer first
            if let Ok(i) = s.parse::<i64>() {
                return SqlValue::Integer(i);
            }
            // Try to parse as float
            if let Ok(f) = s.parse::<f64>() {
                return SqlValue::Double(f);
            }
            // Can't convert - keep original (will fail lookup, which is correct)
            val
        }
        // REAL affinity column with string value: try to parse as float
        (TypeAffinity::Real, SqlValue::Varchar(s) | SqlValue::Character(s)) => {
            if let Ok(f) = s.parse::<f64>() {
                return SqlValue::Double(f);
            }
            val
        }
        // TEXT affinity column with numeric value: convert to string
        (TypeAffinity::Text, SqlValue::Integer(i)) => {
            SqlValue::Varchar(arcstr::ArcStr::from(i.to_string()))
        }
        (TypeAffinity::Text, SqlValue::Double(f)) => {
            SqlValue::Varchar(arcstr::ArcStr::from(f.to_string()))
        }
        (TypeAffinity::Text, SqlValue::Float(f)) => {
            SqlValue::Varchar(arcstr::ArcStr::from(f.to_string()))
        }
        (TypeAffinity::Text, SqlValue::Real(f)) => {
            SqlValue::Varchar(arcstr::ArcStr::from(f.to_string()))
        }
        // No conversion needed
        _ => val,
    }
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
        let result =
            coerce_to_date(&SqlValue::Varchar(arcstr::ArcStr::from("2024-01-15"))).unwrap();
        let expected = vibesql_types::Date::new(2024, 1, 15).unwrap();
        assert_eq!(result, SqlValue::Date(expected));
    }

    #[test]
    fn test_coerce_character_to_date() {
        let result =
            coerce_to_date(&SqlValue::Character(arcstr::ArcStr::from("2024-12-31"))).unwrap();
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
        let result =
            coerce_to_date(&SqlValue::Varchar(arcstr::ArcStr::from("2024-02-29"))).unwrap();
        let expected = vibesql_types::Date::new(2024, 2, 29).unwrap();
        assert_eq!(result, SqlValue::Date(expected));
    }

    #[test]
    fn test_coerce_non_leap_year_feb_29() {
        let result = coerce_to_date(&SqlValue::Varchar(arcstr::ArcStr::from("2023-02-29")));
        assert!(result.is_err());
    }

    // ----- coerce_value_to_column_type tests -----

    #[test]
    fn test_pk_coerce_text_column_integer_literal() {
        // WHERE p = 1200 on TEXT PRIMARY KEY column: 1200 -> "1200"
        let result = coerce_value_to_column_type(
            SqlValue::Integer(1200),
            &DataType::Varchar { max_length: Some(255) },
        );
        assert_eq!(result, SqlValue::Varchar(arcstr::ArcStr::from("1200")));
    }

    #[test]
    fn test_pk_coerce_integer_column_string_literal() {
        // WHERE i = '12' on INTEGER PRIMARY KEY column: '12' -> 12
        let result = coerce_value_to_column_type(
            SqlValue::Varchar(arcstr::ArcStr::from("12")),
            &DataType::Integer,
        );
        assert_eq!(result, SqlValue::Integer(12));
    }

    #[test]
    fn test_pk_coerce_integer_column_unparseable_string() {
        // WHERE i = 'foo' on INTEGER column: 'foo' stays as Varchar (lookup will miss)
        let result = coerce_value_to_column_type(
            SqlValue::Varchar(arcstr::ArcStr::from("foo")),
            &DataType::Integer,
        );
        assert_eq!(result, SqlValue::Varchar(arcstr::ArcStr::from("foo")));
    }

    #[test]
    fn test_pk_coerce_text_column_text_literal_passthrough() {
        // WHERE p = '1200' on TEXT column: stays as '1200'
        let result = coerce_value_to_column_type(
            SqlValue::Varchar(arcstr::ArcStr::from("1200")),
            &DataType::Varchar { max_length: Some(255) },
        );
        assert_eq!(result, SqlValue::Varchar(arcstr::ArcStr::from("1200")));
    }

    #[test]
    fn test_pk_coerce_integer_column_integer_literal_passthrough() {
        // WHERE i = 12 on INTEGER column: stays as Integer(12)
        let result = coerce_value_to_column_type(SqlValue::Integer(12), &DataType::Integer);
        assert_eq!(result, SqlValue::Integer(12));
    }

    #[test]
    fn test_pk_coerce_real_column_string_literal() {
        // WHERE r = '1.5' on REAL column: '1.5' -> 1.5
        let result = coerce_value_to_column_type(
            SqlValue::Varchar(arcstr::ArcStr::from("1.5")),
            &DataType::Real,
        );
        assert_eq!(result, SqlValue::Double(1.5));
    }
}
