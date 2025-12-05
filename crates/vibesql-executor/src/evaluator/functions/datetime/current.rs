//! Current date/time functions
//!
//! Implements CURRENT_DATE, CURRENT_TIME, and CURRENT_TIMESTAMP functions.

use chrono::{Local, Timelike};
use vibesql_types::SqlValue;

use crate::errors::ExecutorError;

/// CURRENT_DATE / CURDATE - Returns current date
/// Alias: CURDATE
/// SQL:1999 Section 6.31: Datetime value functions
pub fn current_date(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if !args.is_empty() {
        return Err(ExecutorError::UnsupportedFeature(
            "CURRENT_DATE takes no arguments".to_string(),
        ));
    }

    let now = Local::now();
    use chrono::Datelike;
    let date =
        vibesql_types::Date::new(now.year(), now.month() as u8, now.day() as u8).map_err(|e| {
            ExecutorError::UnsupportedFeature(format!("Failed to create current date: {}", e))
        })?;
    Ok(SqlValue::Date(date))
}

/// CURRENT_TIME / CURTIME - Returns current time
/// Alias: CURTIME
/// SQL:1999 Section 6.31: Datetime value functions
/// Supports optional precision argument (0-9) for fractional seconds
pub fn current_time(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    // Parse precision argument if provided
    let precision = if args.is_empty() {
        None
    } else if args.len() == 1 {
        match &args[0] {
            SqlValue::Integer(n) if *n >= 0 && *n <= 9 => Some(*n as u32),
            SqlValue::Integer(n) => {
                return Err(ExecutorError::UnsupportedFeature(format!(
                    "CURRENT_TIME precision must be 0-9, got {}",
                    n
                )));
            }
            _ => {
                return Err(ExecutorError::UnsupportedFeature(
                    "CURRENT_TIME precision must be an integer between 0 and 9".to_string(),
                ));
            }
        }
    } else {
        return Err(ExecutorError::UnsupportedFeature(
            "CURRENT_TIME takes 0 or 1 arguments".to_string(),
        ));
    };

    let now = Local::now();

    let time_naive = now.time();
    let nanosecond = match precision {
        None => 0,
        Some(prec) => {
            let nanos = time_naive.nanosecond();
            let divisor = 10_u32.pow(9 - prec);
            (nanos / divisor) * divisor
        }
    };

    let time = vibesql_types::Time::new(
        time_naive.hour() as u8,
        time_naive.minute() as u8,
        time_naive.second() as u8,
        nanosecond,
    )
    .map_err(|e| {
        ExecutorError::UnsupportedFeature(format!("Failed to create current time: {}", e))
    })?;

    Ok(SqlValue::Time(time))
}

/// CURRENT_TIMESTAMP / NOW - Returns current timestamp
/// Alias: NOW
/// SQL:1999 Section 6.31: Datetime value functions
/// Supports optional precision argument (0-9) for fractional seconds
pub fn current_timestamp(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    // Parse precision argument if provided
    let precision = if args.is_empty() {
        None
    } else if args.len() == 1 {
        match &args[0] {
            SqlValue::Integer(n) if *n >= 0 && *n <= 9 => Some(*n as u32),
            SqlValue::Integer(n) => {
                return Err(ExecutorError::UnsupportedFeature(format!(
                    "CURRENT_TIMESTAMP precision must be 0-9, got {}",
                    n
                )));
            }
            _ => {
                return Err(ExecutorError::UnsupportedFeature(
                    "CURRENT_TIMESTAMP precision must be an integer between 0 and 9".to_string(),
                ));
            }
        }
    } else {
        return Err(ExecutorError::UnsupportedFeature(
            "CURRENT_TIMESTAMP takes 0 or 1 arguments".to_string(),
        ));
    };

    let now = Local::now();

    use chrono::Datelike;
    let time_naive = now.time();
    let nanosecond = match precision {
        None => 0,
        Some(prec) => {
            let nanos = time_naive.nanosecond();
            let divisor = 10_u32.pow(9 - prec);
            (nanos / divisor) * divisor
        }
    };

    let date = vibesql_types::Date::new(now.year(), now.month() as u8, now.day() as u8)
        .map_err(|e| ExecutorError::UnsupportedFeature(format!("Failed to create date: {}", e)))?;
    let time = vibesql_types::Time::new(
        time_naive.hour() as u8,
        time_naive.minute() as u8,
        time_naive.second() as u8,
        nanosecond,
    )
    .map_err(|e| ExecutorError::UnsupportedFeature(format!("Failed to create time: {}", e)))?;

    Ok(SqlValue::Timestamp(vibesql_types::Timestamp::new(date, time)))
}

/// Helper function to format time with fractional seconds precision
#[allow(dead_code)]
fn format_time_with_precision(time: chrono::NaiveTime, precision: u32) -> String {
    let base = time.format("%H:%M:%S").to_string();
    if precision == 0 {
        return base;
    }

    // Get fractional seconds
    let nanos = time.nanosecond();
    let divisor = 10_u32.pow(9 - precision);
    let fractional = nanos / divisor;

    format!("{}.{:0width$}", base, fractional, width = precision as usize)
}

/// DATETIME - SQLite-compatible datetime function
/// Returns timestamp as string in 'YYYY-MM-DD HH:MM:SS' format
///
/// Supports:
/// - DATETIME('now') - Returns current timestamp
/// - DATETIME(timestring) - Parses and formats a date/time string
///
/// SQLite Reference: https://www.sqlite.org/lang_datefunc.html
pub fn datetime(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.is_empty() {
        return Err(ExecutorError::UnsupportedFeature(
            "DATETIME requires at least 1 argument".to_string(),
        ));
    }

    if args.len() > 1 {
        return Err(ExecutorError::UnsupportedFeature(
            "DATETIME with modifiers not yet supported. Use DATETIME('now') or DATETIME(timestring)".to_string(),
        ));
    }

    match &args[0] {
        SqlValue::Null => Ok(SqlValue::Null),
        SqlValue::Varchar(s) | SqlValue::Character(s) => {
            // Handle 'now' special case
            if s.to_lowercase() == "now" {
                // Return current timestamp
                let now = Local::now();
                use chrono::Datelike;
                let date = vibesql_types::Date::new(now.year(), now.month() as u8, now.day() as u8)
                    .map_err(|e| {
                        ExecutorError::UnsupportedFeature(format!("Failed to create date: {}", e))
                    })?;
                let time_naive = now.time();
                let time = vibesql_types::Time::new(
                    time_naive.hour() as u8,
                    time_naive.minute() as u8,
                    time_naive.second() as u8,
                    0, // No fractional seconds for basic DATETIME
                )
                .map_err(|e| {
                    ExecutorError::UnsupportedFeature(format!("Failed to create time: {}", e))
                })?;
                Ok(SqlValue::Timestamp(vibesql_types::Timestamp::new(date, time)))
            } else {
                // Parse the timestring - try various common formats
                parse_datetime_string(s)
            }
        }
        SqlValue::Date(d) => {
            // Convert Date to Timestamp with time 00:00:00
            let time = vibesql_types::Time::new(0, 0, 0, 0).map_err(|e| {
                ExecutorError::UnsupportedFeature(format!("Failed to create time: {}", e))
            })?;
            Ok(SqlValue::Timestamp(vibesql_types::Timestamp::new(*d, time)))
        }
        SqlValue::Timestamp(ts) => {
            // Already a timestamp, return as-is
            Ok(SqlValue::Timestamp(*ts))
        }
        _ => Err(ExecutorError::UnsupportedFeature(format!(
            "DATETIME requires string, date, or timestamp argument, got {:?}",
            args[0]
        ))),
    }
}

/// Helper to parse datetime strings in various formats
fn parse_datetime_string(s: &str) -> Result<SqlValue, ExecutorError> {
    use chrono::{Datelike, NaiveDate, NaiveDateTime, Timelike};

    // Try parsing as full timestamp: "YYYY-MM-DD HH:MM:SS"
    if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
        let date = vibesql_types::Date::new(dt.year(), dt.month() as u8, dt.day() as u8)
            .map_err(|e| ExecutorError::UnsupportedFeature(format!("Invalid date: {}", e)))?;
        let time =
            vibesql_types::Time::new(dt.hour() as u8, dt.minute() as u8, dt.second() as u8, 0)
                .map_err(|e| ExecutorError::UnsupportedFeature(format!("Invalid time: {}", e)))?;
        return Ok(SqlValue::Timestamp(vibesql_types::Timestamp::new(date, time)));
    }

    // Try parsing as date only: "YYYY-MM-DD" - return as timestamp with 00:00:00
    if let Ok(d) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        let date = vibesql_types::Date::new(d.year(), d.month() as u8, d.day() as u8)
            .map_err(|e| ExecutorError::UnsupportedFeature(format!("Invalid date: {}", e)))?;
        let time = vibesql_types::Time::new(0, 0, 0, 0)
            .map_err(|e| ExecutorError::UnsupportedFeature(format!("Invalid time: {}", e)))?;
        return Ok(SqlValue::Timestamp(vibesql_types::Timestamp::new(date, time)));
    }

    // Try parsing with T separator: "YYYY-MM-DDTHH:MM:SS"
    if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S") {
        let date = vibesql_types::Date::new(dt.year(), dt.month() as u8, dt.day() as u8)
            .map_err(|e| ExecutorError::UnsupportedFeature(format!("Invalid date: {}", e)))?;
        let time =
            vibesql_types::Time::new(dt.hour() as u8, dt.minute() as u8, dt.second() as u8, 0)
                .map_err(|e| ExecutorError::UnsupportedFeature(format!("Invalid time: {}", e)))?;
        return Ok(SqlValue::Timestamp(vibesql_types::Timestamp::new(date, time)));
    }

    Err(ExecutorError::UnsupportedFeature(
        format!("DATETIME: Unable to parse datetime string '{}'. Expected formats: 'YYYY-MM-DD HH:MM:SS', 'YYYY-MM-DD', or 'now'", s)
    ))
}

/// Helper function to format timestamp with fractional seconds precision
#[allow(dead_code)]
fn format_timestamp_with_precision(dt: chrono::DateTime<Local>, precision: u32) -> String {
    let base = dt.format("%Y-%m-%d %H:%M:%S").to_string();
    if precision == 0 {
        return base;
    }

    let nanos = dt.timestamp_subsec_nanos();
    let divisor = 10_u32.pow(9 - precision);
    let fractional = nanos / divisor;

    format!("{}.{:0width$}", base, fractional, width = precision as usize)
}
