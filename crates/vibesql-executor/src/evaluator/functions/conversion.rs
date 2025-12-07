//! Type conversion functions (CAST, TO_NUMBER, TO_DATE, TO_TIMESTAMP, TO_CHAR)
//!
//! SQL:1999 Section 6.10-6.12: CAST specification and type conversions

use crate::errors::ExecutorError;

/// TO_NUMBER(string) - Convert string to numeric value
/// SQL:1999 Section 6.12: Type conversions
pub(super) fn to_number(
    args: &[vibesql_types::SqlValue],
) -> Result<vibesql_types::SqlValue, ExecutorError> {
    if args.len() != 1 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "TO_NUMBER requires exactly 1 argument, got {}",
            args.len()
        )));
    }

    match &args[0] {
        vibesql_types::SqlValue::Null => Ok(vibesql_types::SqlValue::Null),
        vibesql_types::SqlValue::Varchar(s) | vibesql_types::SqlValue::Character(s) => {
            // Remove whitespace and commas
            let cleaned = s.trim().replace(",", "");

            // Try parsing as integer first
            if let Ok(n) = cleaned.parse::<i64>() {
                Ok(vibesql_types::SqlValue::Integer(n))
            } else if let Ok(n) = cleaned.parse::<f64>() {
                // Parse as floating point
                Ok(vibesql_types::SqlValue::Double(n))
            } else {
                Err(ExecutorError::UnsupportedFeature(format!(
                    "TO_NUMBER: Invalid numeric string '{}'",
                    s
                )))
            }
        }
        val => Err(ExecutorError::UnsupportedFeature(format!(
            "TO_NUMBER requires string argument, got {:?}",
            val
        ))),
    }
}

/// TO_DATE(string, format) - Convert string to date with format
/// SQL:1999 Section 6.12: Type conversions
pub(super) fn to_date(
    args: &[vibesql_types::SqlValue],
) -> Result<vibesql_types::SqlValue, ExecutorError> {
    if args.len() != 2 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "TO_DATE requires exactly 2 arguments (string, format), got {}",
            args.len()
        )));
    }

    match (&args[0], &args[1]) {
        (vibesql_types::SqlValue::Null, _) | (_, vibesql_types::SqlValue::Null) => {
            Ok(vibesql_types::SqlValue::Null)
        }
        (
            vibesql_types::SqlValue::Varchar(input) | vibesql_types::SqlValue::Character(input),
            vibesql_types::SqlValue::Varchar(format) | vibesql_types::SqlValue::Character(format),
        ) => {
            let naive_date = super::super::date_format::parse_date(input, format)?;
            // Convert NaiveDate to our Date type
            use chrono::Datelike;
            let date = vibesql_types::Date::new(
                naive_date.year(),
                naive_date.month() as u8,
                naive_date.day() as u8,
            )
            .map_err(|e| ExecutorError::UnsupportedFeature(format!("Invalid date: {}", e)))?;
            Ok(vibesql_types::SqlValue::Date(date))
        }
        (input, format) => Err(ExecutorError::UnsupportedFeature(format!(
            "TO_DATE requires string arguments, got {:?} and {:?}",
            input, format
        ))),
    }
}

/// TO_TIMESTAMP(string, format) - Convert string to timestamp with format
/// SQL:1999 Section 6.12: Type conversions
pub(super) fn to_timestamp(
    args: &[vibesql_types::SqlValue],
) -> Result<vibesql_types::SqlValue, ExecutorError> {
    if args.len() != 2 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "TO_TIMESTAMP requires exactly 2 arguments (string, format), got {}",
            args.len()
        )));
    }

    match (&args[0], &args[1]) {
        (vibesql_types::SqlValue::Null, _) | (_, vibesql_types::SqlValue::Null) => {
            Ok(vibesql_types::SqlValue::Null)
        }
        (
            vibesql_types::SqlValue::Varchar(input) | vibesql_types::SqlValue::Character(input),
            vibesql_types::SqlValue::Varchar(format) | vibesql_types::SqlValue::Character(format),
        ) => {
            let naive_timestamp = super::super::date_format::parse_timestamp(input, format)?;
            // Convert NaiveDateTime to our Timestamp type
            use chrono::{Datelike, Timelike};
            let date = vibesql_types::Date::new(
                naive_timestamp.year(),
                naive_timestamp.month() as u8,
                naive_timestamp.day() as u8,
            )
            .map_err(|e| ExecutorError::UnsupportedFeature(format!("Invalid date: {}", e)))?;
            let time = vibesql_types::Time::new(
                naive_timestamp.hour() as u8,
                naive_timestamp.minute() as u8,
                naive_timestamp.second() as u8,
                naive_timestamp.nanosecond(),
            )
            .map_err(|e| ExecutorError::UnsupportedFeature(format!("Invalid time: {}", e)))?;
            Ok(vibesql_types::SqlValue::Timestamp(vibesql_types::Timestamp::new(date, time)))
        }
        (input, format) => Err(ExecutorError::UnsupportedFeature(format!(
            "TO_TIMESTAMP requires string arguments, got {:?} and {:?}",
            input, format
        ))),
    }
}

/// TO_CHAR(value, format) - Convert date/number to formatted string
/// SQL:1999 Section 6.12: Type conversions
pub(super) fn to_char(
    args: &[vibesql_types::SqlValue],
) -> Result<vibesql_types::SqlValue, ExecutorError> {
    if args.len() != 2 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "TO_CHAR requires exactly 2 arguments (value, format), got {}",
            args.len()
        )));
    }

    let format_str = match &args[1] {
        vibesql_types::SqlValue::Varchar(s) | vibesql_types::SqlValue::Character(s) => s,
        vibesql_types::SqlValue::Null => return Ok(vibesql_types::SqlValue::Null),
        val => {
            return Err(ExecutorError::UnsupportedFeature(format!(
                "TO_CHAR format must be string, got {:?}",
                val
            )))
        }
    };

    match &args[0] {
        vibesql_types::SqlValue::Null => Ok(vibesql_types::SqlValue::Null),

        // Date/Time formatting
        vibesql_types::SqlValue::Date(date) => {
            use chrono::NaiveDate;
            // Convert our Date type to NaiveDate for formatting
            let naive_date = NaiveDate::from_ymd_opt(date.year, date.month as u32, date.day as u32)
                .ok_or_else(|| ExecutorError::UnsupportedFeature("Invalid date".to_string()))?;
            let formatted = super::super::date_format::format_date(&naive_date, format_str)?;
            Ok(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(formatted)))
        }
        vibesql_types::SqlValue::Timestamp(ts) => {
            use chrono::NaiveDate;
            use chrono::NaiveDateTime;
            use chrono::NaiveTime;
            // Convert our Timestamp type to NaiveDateTime for formatting
            let naive_date =
                NaiveDate::from_ymd_opt(ts.date.year, ts.date.month as u32, ts.date.day as u32)
                    .ok_or_else(|| ExecutorError::UnsupportedFeature("Invalid date".to_string()))?;
            let naive_time = NaiveTime::from_hms_nano_opt(
                ts.time.hour as u32,
                ts.time.minute as u32,
                ts.time.second as u32,
                ts.time.nanosecond,
            )
            .ok_or_else(|| ExecutorError::UnsupportedFeature("Invalid time".to_string()))?;
            let naive_timestamp = NaiveDateTime::new(naive_date, naive_time);
            let formatted =
                super::super::date_format::format_timestamp(&naive_timestamp, format_str)?;
            Ok(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(formatted)))
        }
        vibesql_types::SqlValue::Time(time) => {
            use chrono::NaiveTime;
            // Convert our Time type to NaiveTime for formatting
            let naive_time = NaiveTime::from_hms_nano_opt(
                time.hour as u32,
                time.minute as u32,
                time.second as u32,
                time.nanosecond,
            )
            .ok_or_else(|| ExecutorError::UnsupportedFeature("Invalid time".to_string()))?;
            let formatted = super::super::date_format::format_time(&naive_time, format_str)?;
            Ok(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(formatted)))
        }

        // Number formatting
        vibesql_types::SqlValue::Integer(n) => {
            let formatted = super::super::date_format::format_number(*n as f64, format_str)?;
            Ok(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(formatted)))
        }
        vibesql_types::SqlValue::Smallint(n) => {
            let formatted = super::super::date_format::format_number(*n as f64, format_str)?;
            Ok(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(formatted)))
        }
        vibesql_types::SqlValue::Bigint(n) => {
            let formatted = super::super::date_format::format_number(*n as f64, format_str)?;
            Ok(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(formatted)))
        }
        vibesql_types::SqlValue::Float(n) => {
            let formatted = super::super::date_format::format_number(*n as f64, format_str)?;
            Ok(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(formatted)))
        }
        vibesql_types::SqlValue::Double(n) => {
            let formatted = super::super::date_format::format_number(*n, format_str)?;
            Ok(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(formatted)))
        }
        vibesql_types::SqlValue::Real(n) => {
            let formatted = super::super::date_format::format_number(*n as f64, format_str)?;
            Ok(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(formatted)))
        }

        val => {
            Err(ExecutorError::UnsupportedFeature(format!("TO_CHAR cannot format type {:?}", val)))
        }
    }
}

/// CAST(value AS type) - SQL:1999 standard type conversion
/// SQL:1999 Section 6.10: CAST specification
/// Note: CAST syntax is handled specially by the parser
/// This function receives [value, DataType] as arguments
pub(super) fn cast(
    args: &[vibesql_types::SqlValue],
    sql_mode: &vibesql_types::SqlMode,
) -> Result<vibesql_types::SqlValue, ExecutorError> {
    if args.len() != 2 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "CAST requires exactly 2 arguments (value, target_type), got {}",
            args.len()
        )));
    }

    // The second argument should be a DataType wrapped in a SqlValue
    // For now, we'll extract the target type information from string representation
    // This is a simplified implementation - ideally the parser would pass DataType directly
    let target_type_str = match &args[1] {
        vibesql_types::SqlValue::Varchar(s) | vibesql_types::SqlValue::Character(s) => {
            s.to_uppercase()
        }
        val => {
            return Err(ExecutorError::UnsupportedFeature(format!(
                "CAST target type must be string, got {:?}",
                val
            )))
        }
    };

    // Map string to DataType
    let target_type = match target_type_str.as_str() {
        "INTEGER" | "INT" => vibesql_types::DataType::Integer,
        "SMALLINT" => vibesql_types::DataType::Smallint,
        "BIGINT" => vibesql_types::DataType::Bigint,
        "FLOAT" => vibesql_types::DataType::Float { precision: 53 },
        "DOUBLE PRECISION" | "DOUBLE" => vibesql_types::DataType::DoublePrecision,
        "VARCHAR" => vibesql_types::DataType::Varchar { max_length: Some(255) },
        "DATE" => vibesql_types::DataType::Date,
        "TIME" => vibesql_types::DataType::Time { with_timezone: false },
        "TIMESTAMP" => vibesql_types::DataType::Timestamp { with_timezone: false },
        _ => {
            return Err(ExecutorError::UnsupportedFeature(format!(
                "CAST to type '{}' not supported",
                target_type_str
            )))
        }
    };

    // Use the existing cast_value function from casting module
    super::super::casting::cast_value(&args[0], &target_type, sql_mode)
}
