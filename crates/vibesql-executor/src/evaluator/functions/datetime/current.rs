//! Current date/time functions
//!
//! Implements CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP, and DATETIME functions.

use chrono::{Datelike, Duration, Local, NaiveDateTime, Timelike, Weekday};
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
/// - DATETIME(timestring, modifier, modifier, ...) - Applies modifiers in sequence
///
/// Modifiers supported:
/// - Time shifts: +N days, -N hours, +N minutes, +N seconds, +N months, +N years
/// - Special: 'start of month', 'start of year', 'start of day', 'weekday N'
/// - Timezone: 'localtime', 'utc'
/// - Unix: 'unixepoch' (interprets numeric input as Unix timestamp)
///
/// SQLite Reference: https://www.sqlite.org/lang_datefunc.html
pub fn datetime(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.is_empty() {
        return Err(ExecutorError::UnsupportedFeature(
            "DATETIME requires at least 1 argument".to_string(),
        ));
    }

    // Check if 'unixepoch' is the first modifier - affects how we parse the base value
    let has_unixepoch_first = args.len() > 1
        && matches!(&args[1], SqlValue::Varchar(s) | SqlValue::Character(s) if s.eq_ignore_ascii_case("unixepoch"));

    // Parse the base datetime value
    let base_result = if has_unixepoch_first {
        // For unixepoch, parse numeric values as Unix timestamps instead of Julian Days
        parse_base_datetime_for_unixepoch(&args[0])?
    } else {
        parse_base_datetime(&args[0])?
    };

    // If base value is NULL, return NULL
    let mut dt = match base_result {
        Some(dt) => dt,
        None => return Ok(SqlValue::Null),
    };

    // Apply each modifier in sequence
    for (idx, modifier_arg) in args.iter().enumerate().skip(1) {
        let modifier_str = match modifier_arg {
            SqlValue::Varchar(s) | SqlValue::Character(s) => s.as_str(),
            SqlValue::Null => return Ok(SqlValue::Null),
            _ => {
                return Err(ExecutorError::UnsupportedFeature(format!(
                    "DATETIME modifier must be a string, got {:?}",
                    modifier_arg
                )))
            }
        };

        // Special case: 'unixepoch' must be the first modifier
        // We've already handled it above by parsing as unix timestamp
        if modifier_str.eq_ignore_ascii_case("unixepoch") {
            if idx != 1 {
                // unixepoch only valid as first modifier
                return Ok(SqlValue::Null);
            }
            // Already handled during parsing, just continue
            continue;
        }

        // Apply the modifier
        match apply_datetime_modifier(dt, modifier_str) {
            Some(new_dt) => dt = new_dt,
            None => return Ok(SqlValue::Null), // Invalid modifier returns NULL
        }
    }

    // Convert NaiveDateTime to SqlValue::Timestamp
    naive_datetime_to_timestamp(dt)
}

/// Parse the base datetime value (first argument to DATETIME)
fn parse_base_datetime(value: &SqlValue) -> Result<Option<NaiveDateTime>, ExecutorError> {
    match value {
        SqlValue::Null => Ok(None),
        SqlValue::Varchar(s) | SqlValue::Character(s) => {
            // Handle 'now' special case
            if s.eq_ignore_ascii_case("now") {
                let now = Local::now();
                Ok(Some(now.naive_local()))
            } else {
                // Parse the timestring
                Ok(parse_datetime_string_to_naive(s))
            }
        }
        SqlValue::Date(d) => {
            // Convert Date to NaiveDateTime with time 00:00:00
            let naive_date = chrono::NaiveDate::from_ymd_opt(d.year, d.month as u32, d.day as u32);
            Ok(naive_date.map(|date| date.and_hms_opt(0, 0, 0).unwrap()))
        }
        SqlValue::Timestamp(ts) => {
            // Convert Timestamp to NaiveDateTime
            let naive_date =
                chrono::NaiveDate::from_ymd_opt(ts.date.year, ts.date.month as u32, ts.date.day as u32);
            let naive_time = chrono::NaiveTime::from_hms_opt(
                ts.time.hour as u32,
                ts.time.minute as u32,
                ts.time.second as u32,
            );
            match (naive_date, naive_time) {
                (Some(date), Some(time)) => Ok(Some(NaiveDateTime::new(date, time))),
                _ => Ok(None),
            }
        }
        // Integer or float: treat as Julian Day number by default
        SqlValue::Integer(n) => Ok(julian_day_to_naive(*n as f64)),
        SqlValue::Bigint(n) => Ok(julian_day_to_naive(*n as f64)),
        SqlValue::Smallint(n) => Ok(julian_day_to_naive(*n as f64)),
        SqlValue::Float(n) => Ok(julian_day_to_naive(*n as f64)),
        SqlValue::Double(n) | SqlValue::Real(n) | SqlValue::Numeric(n) => {
            Ok(julian_day_to_naive(*n))
        }
        _ => Err(ExecutorError::UnsupportedFeature(format!(
            "DATETIME requires string, date, timestamp, or numeric argument, got {:?}",
            value
        ))),
    }
}

/// Parse the base datetime value when 'unixepoch' is the first modifier
/// Numeric values are interpreted as Unix timestamps instead of Julian Days
fn parse_base_datetime_for_unixepoch(value: &SqlValue) -> Result<Option<NaiveDateTime>, ExecutorError> {
    match value {
        SqlValue::Null => Ok(None),
        SqlValue::Varchar(s) | SqlValue::Character(s) => {
            // Try to parse string as number for unixepoch
            if let Ok(n) = s.parse::<i64>() {
                Ok(unix_epoch_to_datetime(n))
            } else {
                // String that's not a valid number + unixepoch = NULL
                Ok(None)
            }
        }
        SqlValue::Date(_) | SqlValue::Timestamp(_) => {
            // Date/Timestamp with unixepoch doesn't make sense - return NULL
            Ok(None)
        }
        // Integer or float: treat as Unix timestamp
        SqlValue::Integer(n) => Ok(unix_epoch_to_datetime(*n as i64)),
        SqlValue::Bigint(n) => Ok(unix_epoch_to_datetime(*n)),
        SqlValue::Smallint(n) => Ok(unix_epoch_to_datetime(*n as i64)),
        SqlValue::Float(n) => Ok(unix_epoch_to_datetime(*n as i64)),
        SqlValue::Double(n) | SqlValue::Real(n) | SqlValue::Numeric(n) => {
            Ok(unix_epoch_to_datetime(*n as i64))
        }
        _ => Err(ExecutorError::UnsupportedFeature(format!(
            "DATETIME requires string, date, timestamp, or numeric argument, got {:?}",
            value
        ))),
    }
}

/// Apply a single modifier to a NaiveDateTime
/// Returns None if the modifier is invalid (SQLite returns NULL for invalid modifiers)
fn apply_datetime_modifier(dt: NaiveDateTime, modifier: &str) -> Option<NaiveDateTime> {
    let modifier = modifier.trim();
    let lower = modifier.to_lowercase();

    // Handle "start of" modifiers
    if lower.starts_with("start of ") {
        let unit = &lower[9..].trim();
        return match *unit {
            "month" => {
                let date = chrono::NaiveDate::from_ymd_opt(dt.year(), dt.month(), 1)?;
                Some(date.and_hms_opt(0, 0, 0)?)
            }
            "year" => {
                let date = chrono::NaiveDate::from_ymd_opt(dt.year(), 1, 1)?;
                Some(date.and_hms_opt(0, 0, 0)?)
            }
            "day" => {
                let date = dt.date();
                Some(date.and_hms_opt(0, 0, 0)?)
            }
            _ => None, // Invalid "start of" unit
        };
    }

    // Handle incomplete "start of" (returns NULL)
    if lower == "start of" {
        return None;
    }

    // Handle "weekday N" modifier
    if lower.starts_with("weekday ") {
        let rest = &modifier[8..].trim();
        // weekday must be a single digit 0-6
        if rest.len() == 1 {
            if let Ok(weekday_num) = rest.parse::<u32>() {
                if weekday_num <= 6 {
                    return apply_weekday_modifier(dt, weekday_num);
                }
            }
        }
        return None; // Invalid weekday
    }

    // Handle "localtime" and "utc" (currently no-op as we work with naive datetimes)
    if lower == "localtime" || lower == "utc" {
        // For now, these are no-ops since we work with naive datetimes
        // A full implementation would need timezone-aware handling
        return Some(dt);
    }

    // Handle time shift modifiers: +N unit, -N unit, N unit
    parse_and_apply_time_shift(dt, modifier)
}

/// Apply weekday modifier - advances to the next occurrence of the specified weekday
/// weekday_num: 0 = Sunday, 1 = Monday, ..., 6 = Saturday
fn apply_weekday_modifier(dt: NaiveDateTime, weekday_num: u32) -> Option<NaiveDateTime> {
    // Validate weekday number
    if weekday_num > 6 {
        return None;
    }

    let current_weekday = dt.weekday();

    // Calculate days until target weekday
    // If we're already on the target weekday, stay on this day
    let current_num = weekday_to_num(current_weekday);
    let target_num = weekday_num;

    let days_to_add = if current_num == target_num {
        0
    } else if target_num > current_num {
        target_num - current_num
    } else {
        7 - current_num + target_num
    };

    Some(dt + Duration::days(days_to_add as i64))
}

/// Convert chrono Weekday to our numeric format (0=Sunday)
fn weekday_to_num(wd: Weekday) -> u32 {
    match wd {
        Weekday::Sun => 0,
        Weekday::Mon => 1,
        Weekday::Tue => 2,
        Weekday::Wed => 3,
        Weekday::Thu => 4,
        Weekday::Fri => 5,
        Weekday::Sat => 6,
    }
}

/// Parse and apply a time shift modifier like "+1 day", "-2 hours", "3 months"
fn parse_and_apply_time_shift(dt: NaiveDateTime, modifier: &str) -> Option<NaiveDateTime> {
    let modifier = modifier.trim();

    // Parse the amount and unit
    // Format: [+/-]N[.N] unit[s]
    let (amount_str, unit) = split_amount_and_unit(modifier)?;

    // Parse the amount (supports fractional values)
    let amount: f64 = amount_str.parse().ok()?;

    // Normalize unit (remove trailing 's' for plurals)
    let unit_lower = unit.to_lowercase();
    let unit_normalized = unit_lower.trim_end_matches('s');

    match unit_normalized {
        "day" => {
            // Fractional days: 1.25 days = 1 day + 6 hours
            let total_seconds = amount * 86400.0;
            Some(dt + Duration::seconds(total_seconds as i64))
        }
        "hour" => {
            let total_seconds = amount * 3600.0;
            Some(dt + Duration::seconds(total_seconds as i64))
        }
        "minute" => {
            let total_seconds = amount * 60.0;
            Some(dt + Duration::seconds(total_seconds as i64))
        }
        "second" => Some(dt + Duration::seconds(amount as i64)),
        "month" => {
            // For fractional months, SQLite adds the fractional part as days
            // 1.5 months = 1 month + 15 days (roughly)
            let whole_months = amount.trunc() as i32;
            let frac_days = (amount.fract() * 30.0) as i64; // Approximate month as 30 days

            let new_dt = add_months(dt, whole_months)?;
            Some(new_dt + Duration::days(frac_days))
        }
        "year" => {
            // Similar to months, but years
            let whole_years = amount.trunc() as i32;
            let frac_days = (amount.fract() * 365.0) as i64; // Approximate year as 365 days

            let new_dt = add_months(dt, whole_years * 12)?;
            Some(new_dt + Duration::days(frac_days))
        }
        _ => None, // Unknown unit
    }
}

/// Split a modifier string into amount and unit parts
fn split_amount_and_unit(s: &str) -> Option<(&str, &str)> {
    let s = s.trim();

    // Find where the number ends and the unit begins
    let mut unit_start = 0;
    let mut found_digit = false;

    for (i, c) in s.char_indices() {
        if c.is_ascii_digit() || c == '.' || c == '+' || c == '-' {
            found_digit = true;
            unit_start = i + c.len_utf8();
        } else if found_digit && (c == ' ' || c.is_alphabetic()) {
            break;
        }
    }

    if !found_digit || unit_start >= s.len() {
        return None;
    }

    let amount = s[..unit_start].trim();
    let unit = s[unit_start..].trim();

    if amount.is_empty() || unit.is_empty() {
        return None;
    }

    Some((amount, unit))
}

/// Add months to a NaiveDateTime, handling edge cases like Jan 31 + 1 month
fn add_months(dt: NaiveDateTime, months: i32) -> Option<NaiveDateTime> {
    let total_months = dt.year() as i64 * 12 + dt.month() as i64 - 1 + months as i64;
    let new_year = (total_months / 12) as i32;
    let new_month = (total_months % 12 + 1) as u32;

    // Handle month overflow for the day (e.g., Jan 31 -> Feb 28)
    let max_day = days_in_month(new_year, new_month);
    let new_day = dt.day().min(max_day);

    let new_date = chrono::NaiveDate::from_ymd_opt(new_year, new_month, new_day)?;
    Some(NaiveDateTime::new(new_date, dt.time()))
}

/// Get the number of days in a month
fn days_in_month(year: i32, month: u32) -> u32 {
    match month {
        1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
        4 | 6 | 9 | 11 => 30,
        2 => {
            if is_leap_year(year) {
                29
            } else {
                28
            }
        }
        _ => 30,
    }
}

/// Check if a year is a leap year
fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)
}

/// Convert Unix epoch timestamp to NaiveDateTime
fn unix_epoch_to_datetime(timestamp: i64) -> Option<NaiveDateTime> {
    chrono::DateTime::from_timestamp(timestamp, 0).map(|dt| dt.naive_utc())
}

/// Convert Julian Day to NaiveDateTime
fn julian_day_to_naive(jd: f64) -> Option<NaiveDateTime> {
    // JD 2440587.5 = January 1, 1970 00:00:00 UTC (Unix epoch)
    let unix_jd: f64 = 2440587.5;
    let seconds_since_unix = (jd - unix_jd) * 86400.0;

    // Check for reasonable bounds
    if jd < 0.0 || jd > 5373484.0 {
        return None;
    }

    chrono::DateTime::from_timestamp(seconds_since_unix as i64, 0).map(|dt| dt.naive_utc())
}

/// Convert NaiveDateTime to SqlValue::Timestamp
fn naive_datetime_to_timestamp(dt: NaiveDateTime) -> Result<SqlValue, ExecutorError> {
    let date = vibesql_types::Date::new(dt.year(), dt.month() as u8, dt.day() as u8)
        .map_err(|e| ExecutorError::UnsupportedFeature(format!("Invalid date: {}", e)))?;
    let time = vibesql_types::Time::new(dt.hour() as u8, dt.minute() as u8, dt.second() as u8, 0)
        .map_err(|e| ExecutorError::UnsupportedFeature(format!("Invalid time: {}", e)))?;
    Ok(SqlValue::Timestamp(vibesql_types::Timestamp::new(date, time)))
}

/// Helper to parse datetime strings to NaiveDateTime
fn parse_datetime_string_to_naive(s: &str) -> Option<NaiveDateTime> {
    use chrono::NaiveDate;

    // Try parsing as full timestamp: "YYYY-MM-DD HH:MM:SS"
    if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
        return Some(dt);
    }

    // Try parsing as timestamp with fractional seconds: "YYYY-MM-DD HH:MM:SS.fff"
    if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.f") {
        return Some(dt);
    }

    // Try parsing as "YYYY-MM-DD HH:MM" (no seconds)
    if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M") {
        return Some(dt);
    }

    // Try parsing as date only: "YYYY-MM-DD" - return with time 00:00:00
    if let Ok(d) = NaiveDate::parse_from_str(s, "%Y-%m-%d") {
        return d.and_hms_opt(0, 0, 0);
    }

    // Try parsing with T separator: "YYYY-MM-DDTHH:MM:SS"
    if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S") {
        return Some(dt);
    }

    None
}
