//! Date/time arithmetic functions
//!
//! Implements DATEDIFF, DATE_ADD, DATE_SUB, AGE, and supporting helpers.

use chrono::{Datelike, Duration, Local, NaiveDate, NaiveDateTime};
use vibesql_types::SqlValue;

use super::extract::{day, hour, minute, month, second, year};
use crate::errors::ExecutorError;
use crate::evaluator::coercion::coerce_to_date;

/// DATEDIFF(date1, date2) - Calculate day difference between two dates
/// SQL:1999 Core Feature E021-02: Date and time arithmetic
/// Returns: date1 - date2 in days
///
/// Supports automatic type coercion from VARCHAR to DATE:
/// - `DATEDIFF('2024-01-10', '2024-01-01')` returns 9
/// - `DATEDIFF(DATE '2024-01-10', DATE '2024-01-01')` returns 9
pub fn datediff(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 2 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "DATEDIFF requires exactly 2 arguments, got {}",
            args.len()
        )));
    }

    // Coerce arguments to dates first (handles VARCHAR, DATE, TIMESTAMP, NULL)
    let date1 = coerce_to_date(&args[0])?;
    let date2 = coerce_to_date(&args[1])?;

    // Extract date components
    let (d1, d2) = match (&date1, &date2) {
        (SqlValue::Null, _) | (_, SqlValue::Null) => return Ok(SqlValue::Null),
        (SqlValue::Date(date1), SqlValue::Date(date2)) => (date1, date2),
        (SqlValue::Timestamp(ts1), SqlValue::Date(date2)) => (&ts1.date, date2),
        (SqlValue::Date(date1), SqlValue::Timestamp(ts2)) => (date1, &ts2.date),
        (SqlValue::Timestamp(ts1), SqlValue::Timestamp(ts2)) => (&ts1.date, &ts2.date),
        (a, b) => {
            return Err(ExecutorError::UnsupportedFeature(format!(
                "DATEDIFF requires date or timestamp arguments, got {:?} and {:?}",
                a, b
            )))
        }
    };

    // Convert to NaiveDate for arithmetic
    let naive_date1 = NaiveDate::from_ymd_opt(d1.year, d1.month as u32, d1.day as u32)
        .ok_or_else(|| ExecutorError::UnsupportedFeature("Invalid date".to_string()))?;
    let naive_date2 = NaiveDate::from_ymd_opt(d2.year, d2.month as u32, d2.day as u32)
        .ok_or_else(|| ExecutorError::UnsupportedFeature("Invalid date".to_string()))?;

    // Calculate difference in days
    let diff = naive_date1.signed_duration_since(naive_date2).num_days();
    Ok(SqlValue::Integer(diff))
}

/// DATE_ADD(date, amount, unit) - Add interval to date
/// Alias: ADDDATE
/// SQL:1999 Core Feature E021-02: Date and time arithmetic
/// Units: 'YEAR', 'MONTH', 'DAY', 'HOUR', 'MINUTE', 'SECOND'
///
/// Supports two syntaxes:
/// 1. Legacy: `DATE_ADD(date, amount, unit)` - 3 arguments
///    Example: `DATE_ADD('2024-01-01', 5, 'DAY')`
/// 2. Standard: `DATE_ADD(date, INTERVAL)` - 2 arguments
///    Example: `DATE_ADD('2024-01-01', INTERVAL '5' DAY)`
///
/// Supports automatic type coercion from VARCHAR to DATE.
pub fn date_add(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    // Support both syntaxes: 2 args (new) or 3 args (legacy)
    if args.len() == 2 {
        // New syntax: DATE_ADD(date, INTERVAL '5' DAY)
        if matches!(&args[0], SqlValue::Null) || matches!(&args[1], SqlValue::Null) {
            return Ok(SqlValue::Null);
        }

        // Get date/timestamp value (handles VARCHAR, but preserves Timestamp vs Date)
        let date_val = match &args[0] {
            SqlValue::Date(_) | SqlValue::Timestamp(_) => args[0].clone(),
            SqlValue::Varchar(_) | SqlValue::Character(_) => coerce_to_date(&args[0])?,
            SqlValue::Null => SqlValue::Null,
            val => {
                return Err(ExecutorError::TypeMismatch {
                    left: val.clone(),
                    op: "DATE_ADD".to_string(),
                    right: SqlValue::Null,
                })
            }
        };

        match &args[1] {
            SqlValue::Interval(interval) => {
                // Parse interval and apply
                let (amount, unit) = parse_simple_interval(&interval.value)?;
                let date_str = date_val_to_string(&date_val)?;
                return date_add_subtract(&date_str, amount, &unit, true);
            }
            _ => {
                return Err(ExecutorError::UnsupportedFeature(
                    "DATE_ADD with 2 arguments requires INTERVAL as second argument".to_string(),
                ))
            }
        }
    }

    if args.len() != 3 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "DATE_ADD requires 2 or 3 arguments, got {}",
            args.len()
        )));
    }

    // Legacy 3-argument syntax: DATE_ADD(date, amount, unit)
    // Handle NULL inputs
    if matches!(&args[0], SqlValue::Null)
        || matches!(&args[1], SqlValue::Null)
        || matches!(&args[2], SqlValue::Null)
    {
        return Ok(SqlValue::Null);
    }

    // Get date/timestamp value (handles VARCHAR, but preserves Timestamp vs Date)
    let date_val = match &args[0] {
        SqlValue::Date(_) | SqlValue::Timestamp(_) => args[0].clone(),
        SqlValue::Varchar(_) | SqlValue::Character(_) => coerce_to_date(&args[0])?,
        SqlValue::Null => SqlValue::Null,
        val => {
            return Err(ExecutorError::TypeMismatch {
                left: val.clone(),
                op: "DATE_ADD".to_string(),
                right: SqlValue::Null,
            })
        }
    };
    let date_str = date_val_to_string(&date_val)?;

    // Extract amount
    let amount = match &args[1] {
        SqlValue::Integer(n) => *n,
        val => {
            return Err(ExecutorError::UnsupportedFeature(format!(
                "DATE_ADD requires integer amount, got {:?}",
                val
            )))
        }
    };

    // Extract unit
    let unit = match &args[2] {
        SqlValue::Varchar(s) | SqlValue::Character(s) => s.to_uppercase(),
        val => {
            return Err(ExecutorError::UnsupportedFeature(format!(
                "DATE_ADD requires string unit, got {:?}",
                val
            )))
        }
    };

    // Perform date arithmetic based on unit
    date_add_subtract(&date_str, amount, &unit, true)
}

/// DATE_SUB(date, amount, unit) - Subtract interval from date
/// Alias: SUBDATE
/// SQL:1999 Core Feature E021-02: Date and time arithmetic
///
/// Supports two syntaxes:
/// 1. Legacy: `DATE_SUB(date, amount, unit)` - 3 arguments
///    Example: `DATE_SUB('2024-01-01', 5, 'DAY')`
/// 2. Standard: `DATE_SUB(date, INTERVAL)` - 2 arguments
///    Example: `DATE_SUB('2024-01-01', INTERVAL '5' DAY)`
///
/// Supports automatic type coercion from VARCHAR to DATE.
pub fn date_sub(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    // Support both syntaxes: 2 args (new) or 3 args (legacy)
    if args.len() == 2 {
        // New syntax: DATE_SUB(date, INTERVAL '5' DAY)
        if matches!(&args[0], SqlValue::Null) || matches!(&args[1], SqlValue::Null) {
            return Ok(SqlValue::Null);
        }

        // Get date/timestamp value (handles VARCHAR, but preserves Timestamp vs Date)
        let date_val = match &args[0] {
            SqlValue::Date(_) | SqlValue::Timestamp(_) => args[0].clone(),
            SqlValue::Varchar(_) | SqlValue::Character(_) => coerce_to_date(&args[0])?,
            SqlValue::Null => SqlValue::Null,
            val => {
                return Err(ExecutorError::TypeMismatch {
                    left: val.clone(),
                    op: "DATE_SUB".to_string(),
                    right: SqlValue::Null,
                })
            }
        };

        match &args[1] {
            SqlValue::Interval(interval) => {
                // Parse interval and apply (negate for subtraction)
                let (amount, unit) = parse_simple_interval(&interval.value)?;
                let date_str = date_val_to_string(&date_val)?;
                return date_add_subtract(&date_str, -amount, &unit, true);
            }
            _ => {
                return Err(ExecutorError::UnsupportedFeature(
                    "DATE_SUB with 2 arguments requires INTERVAL as second argument".to_string(),
                ))
            }
        }
    }

    if args.len() != 3 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "DATE_SUB requires 2 or 3 arguments, got {}",
            args.len()
        )));
    }

    // Legacy 3-argument syntax: DATE_SUB(date, amount, unit)
    // Handle NULL inputs
    if matches!(&args[0], SqlValue::Null)
        || matches!(&args[1], SqlValue::Null)
        || matches!(&args[2], SqlValue::Null)
    {
        return Ok(SqlValue::Null);
    }

    // Get date/timestamp value (handles VARCHAR, but preserves Timestamp vs Date)
    let date_val = match &args[0] {
        SqlValue::Date(_) | SqlValue::Timestamp(_) => args[0].clone(),
        SqlValue::Varchar(_) | SqlValue::Character(_) => coerce_to_date(&args[0])?,
        SqlValue::Null => SqlValue::Null,
        val => {
            return Err(ExecutorError::TypeMismatch {
                left: val.clone(),
                op: "DATE_SUB".to_string(),
                right: SqlValue::Null,
            })
        }
    };
    let date_str = date_val_to_string(&date_val)?;

    // Extract amount
    let amount = match &args[1] {
        SqlValue::Integer(n) => *n,
        val => {
            return Err(ExecutorError::UnsupportedFeature(format!(
                "DATE_SUB requires integer amount, got {:?}",
                val
            )))
        }
    };

    // Extract unit
    let unit = match &args[2] {
        SqlValue::Varchar(s) | SqlValue::Character(s) => s.to_uppercase(),
        val => {
            return Err(ExecutorError::UnsupportedFeature(format!(
                "DATE_SUB requires string unit, got {:?}",
                val
            )))
        }
    };

    // Perform date arithmetic (negate amount for subtraction)
    date_add_subtract(&date_str, -amount, &unit, true)
}

/// AGE(date1, date2) - Calculate age as interval between two dates
/// AGE(date) - Calculate age from date to current date
/// Returns VARCHAR representation of interval (e.g., "2 years 3 months 5 days")
pub fn age(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.is_empty() || args.len() > 2 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "AGE requires 1 or 2 arguments, got {}",
            args.len()
        )));
    }

    // Get the two dates to compare
    let (date1_str, date2_str) = if args.len() == 1 {
        // AGE(date) - compare with current date
        match &args[0] {
            SqlValue::Null => return Ok(SqlValue::Null),
            SqlValue::Date(d) => {
                let now = Local::now();
                let current_date = now.format("%Y-%m-%d").to_string();
                (current_date, d.to_string())
            }
            SqlValue::Timestamp(ts) => {
                let now = Local::now();
                let current_date = now.format("%Y-%m-%d").to_string();
                (current_date, ts.date.to_string())
            }
            val => {
                return Err(ExecutorError::UnsupportedFeature(format!(
                    "AGE requires date/timestamp argument, got {:?}",
                    val
                )))
            }
        }
    } else {
        // AGE(date1, date2) - calculate date1 - date2
        match (&args[0], &args[1]) {
            (SqlValue::Null, _) | (_, SqlValue::Null) => {
                return Ok(SqlValue::Null);
            }
            (SqlValue::Date(d1), SqlValue::Date(d2)) => (d1.to_string(), d2.to_string()),
            (SqlValue::Timestamp(ts1), SqlValue::Timestamp(ts2)) => {
                (ts1.date.to_string(), ts2.date.to_string())
            }
            (SqlValue::Date(d1), SqlValue::Timestamp(ts2)) => {
                (d1.to_string(), ts2.date.to_string())
            }
            (SqlValue::Timestamp(ts1), SqlValue::Date(d2)) => {
                (ts1.date.to_string(), d2.to_string())
            }
            (a, b) => {
                return Err(ExecutorError::UnsupportedFeature(format!(
                    "AGE requires date/timestamp arguments, got {:?} and {:?}",
                    a, b
                )))
            }
        }
    };

    // Dates are already in YYYY-MM-DD format
    let date1_part = &date1_str;
    let date2_part = &date2_str;

    // Parse dates
    let date1 = NaiveDate::parse_from_str(date1_part, "%Y-%m-%d").map_err(|_| {
        ExecutorError::UnsupportedFeature(format!("Invalid date format for AGE: {}", date1_part))
    })?;
    let date2 = NaiveDate::parse_from_str(date2_part, "%Y-%m-%d").map_err(|_| {
        ExecutorError::UnsupportedFeature(format!("Invalid date format for AGE: {}", date2_part))
    })?;

    // Calculate age components
    let (years, months, days) = calculate_age_components(date1, date2);

    // Format as interval string
    let mut parts = Vec::new();
    if years != 0 {
        parts.push(format!("{} year{}", years.abs(), if years.abs() == 1 { "" } else { "s" }));
    }
    if months != 0 {
        parts.push(format!("{} month{}", months.abs(), if months.abs() == 1 { "" } else { "s" }));
    }
    if days != 0 || parts.is_empty() {
        parts.push(format!("{} day{}", days.abs(), if days.abs() == 1 { "" } else { "s" }));
    }

    let result = if date1 < date2 { format!("-{}", parts.join(" ")) } else { parts.join(" ") };

    Ok(SqlValue::Varchar(arcstr::ArcStr::from(result)))
}

/// EXTRACT(unit, date) - Extract date/time component
/// SQL:1999 Section 6.32: Datetime field extraction
/// Simplified syntax: EXTRACT('YEAR', date) instead of EXTRACT(YEAR FROM date)
pub fn extract(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 2 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "EXTRACT requires exactly 2 arguments (unit, date), got {}",
            args.len()
        )));
    }

    // Extract unit
    let unit = match &args[0] {
        SqlValue::Varchar(s) | SqlValue::Character(s) => s.to_uppercase(),
        val => {
            return Err(ExecutorError::UnsupportedFeature(format!(
                "EXTRACT requires string unit as first argument, got {:?}",
                val
            )))
        }
    };

    // Delegate to existing extraction functions based on unit
    match unit.as_ref() {
        "YEAR" => year(&[args[1].clone()]),
        "MONTH" => month(&[args[1].clone()]),
        "DAY" => day(&[args[1].clone()]),
        "HOUR" => hour(&[args[1].clone()]),
        "MINUTE" => minute(&[args[1].clone()]),
        "SECOND" => second(&[args[1].clone()]),
        _ => Err(ExecutorError::UnsupportedFeature(format!("Unsupported EXTRACT unit: {}", unit))),
    }
}

// ==================== HELPER FUNCTIONS ====================

/// Helper to safely change year and month, clamping day to last valid day of month
/// This handles edge cases like Jan 31 + 1 month → Feb 28/29
fn safe_date_with_year_month(date: NaiveDate, year: i32, month: u32) -> Option<NaiveDate> {
    let day = date.day();

    // Get the last valid day for the target year/month
    let last_day_of_month = last_day_of_month(year, month);
    let clamped_day = day.min(last_day_of_month);

    // Create date with clamped day
    NaiveDate::from_ymd_opt(year, month, clamped_day)
}

/// Helper to get the last day of a given month/year
fn last_day_of_month(year: i32, month: u32) -> u32 {
    // Days in each month (non-leap year)
    const DAYS_IN_MONTH: [u32; 12] = [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];

    if month == 2 && is_leap_year(year) {
        29
    } else {
        DAYS_IN_MONTH[(month - 1) as usize]
    }
}

/// Check if a year is a leap year
fn is_leap_year(year: i32) -> bool {
    (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0)
}

/// Helper for DATE_ADD and DATE_SUB functions
/// Adds or subtracts an interval from a date/timestamp
/// preserve_time: if true and input is timestamp, preserve time component
pub(crate) fn date_add_subtract(
    date_str: &str,
    amount: i64,
    unit: &str,
    preserve_time: bool,
) -> Result<SqlValue, ExecutorError> {
    // Check if input is a timestamp (has time component)
    let is_timestamp = date_str.contains(' ');

    if is_timestamp && preserve_time {
        // Parse as timestamp and preserve time
        let timestamp =
            NaiveDateTime::parse_from_str(date_str, "%Y-%m-%d %H:%M:%S").map_err(|_| {
                ExecutorError::UnsupportedFeature(format!("Invalid timestamp format: {}", date_str))
            })?;

        let new_timestamp = match unit {
            "YEAR" | "YEARS" => {
                let new_year = timestamp.year() + amount as i32;
                let date = timestamp.date();
                let new_date =
                    safe_date_with_year_month(date, new_year, date.month()).ok_or_else(|| {
                        ExecutorError::UnsupportedFeature(format!("Invalid year: {}", new_year))
                    })?;

                // Combine new date with original time
                let time = timestamp.time();
                NaiveDateTime::new(new_date, time)
            }
            "MONTH" | "MONTHS" => {
                let total_months =
                    timestamp.year() as i64 * 12 + timestamp.month() as i64 - 1 + amount;
                let new_year = (total_months / 12) as i32;
                let new_month = (total_months % 12 + 1) as u32;

                let date = timestamp.date();
                let new_date =
                    safe_date_with_year_month(date, new_year, new_month).ok_or_else(|| {
                        ExecutorError::UnsupportedFeature(format!(
                            "Invalid date: {}-{:02}",
                            new_year, new_month
                        ))
                    })?;

                // Combine new date with original time
                let time = timestamp.time();
                NaiveDateTime::new(new_date, time)
            }
            "DAY" | "DAYS" => timestamp + Duration::days(amount),
            "HOUR" | "HOURS" => timestamp + Duration::hours(amount),
            "MINUTE" | "MINUTES" => timestamp + Duration::minutes(amount),
            "SECOND" | "SECONDS" => timestamp + Duration::seconds(amount),
            _ => {
                return Err(ExecutorError::UnsupportedFeature(format!(
                    "Unsupported date unit: {}",
                    unit
                )))
            }
        };

        // Convert NaiveDateTime to our Timestamp type
        use chrono::{Datelike, Timelike};
        let date = vibesql_types::Date::new(
            new_timestamp.year(),
            new_timestamp.month() as u8,
            new_timestamp.day() as u8,
        )
        .map_err(|e| ExecutorError::UnsupportedFeature(format!("Invalid date: {}", e)))?;
        let time = vibesql_types::Time::new(
            new_timestamp.hour() as u8,
            new_timestamp.minute() as u8,
            new_timestamp.second() as u8,
            new_timestamp.nanosecond(),
        )
        .map_err(|e| ExecutorError::UnsupportedFeature(format!("Invalid time: {}", e)))?;
        Ok(SqlValue::Timestamp(vibesql_types::Timestamp::new(date, time)))
    } else {
        // Parse as date (extract date part if timestamp)
        let date_part = date_str.split(' ').next().unwrap_or(date_str);
        let date = NaiveDate::parse_from_str(date_part, "%Y-%m-%d").map_err(|_| {
            ExecutorError::UnsupportedFeature(format!("Invalid date format: {}", date_part))
        })?;

        let new_date = match unit {
            "YEAR" | "YEARS" => {
                let new_year = date.year() + amount as i32;
                safe_date_with_year_month(date, new_year, date.month()).ok_or_else(|| {
                    ExecutorError::UnsupportedFeature(format!("Invalid year: {}", new_year))
                })?
            }
            "MONTH" | "MONTHS" => {
                let total_months = date.year() as i64 * 12 + date.month() as i64 - 1 + amount;
                let new_year = (total_months / 12) as i32;
                let new_month = (total_months % 12 + 1) as u32;

                safe_date_with_year_month(date, new_year, new_month).ok_or_else(|| {
                    ExecutorError::UnsupportedFeature(format!(
                        "Invalid date: {}-{:02}",
                        new_year, new_month
                    ))
                })?
            }
            "DAY" | "DAYS" => date + Duration::days(amount),
            "HOUR" | "HOURS" | "MINUTE" | "MINUTES" | "SECOND" | "SECONDS" => {
                // Time units on dates don't change the date
                // Convert NaiveDate to our Date type
                use chrono::Datelike;
                let result_date =
                    vibesql_types::Date::new(date.year(), date.month() as u8, date.day() as u8)
                        .map_err(|e| {
                            ExecutorError::UnsupportedFeature(format!("Invalid date: {}", e))
                        })?;
                return Ok(SqlValue::Date(result_date));
            }
            _ => {
                return Err(ExecutorError::UnsupportedFeature(format!(
                    "Unsupported date unit: {}",
                    unit
                )))
            }
        };

        // Convert NaiveDate to our Date type
        use chrono::Datelike;
        let result_date =
            vibesql_types::Date::new(new_date.year(), new_date.month() as u8, new_date.day() as u8)
                .map_err(|e| ExecutorError::UnsupportedFeature(format!("Invalid date: {}", e)))?;
        Ok(SqlValue::Date(result_date))
    }
}

/// Helper for AGE function - calculate years, months, and days between two dates
/// Returns (years, months, days) where all values have the same sign
fn calculate_age_components(date1: NaiveDate, date2: NaiveDate) -> (i32, i32, i32) {
    // Determine if result should be negative
    let is_negative = date1 < date2;
    let (later, earlier) = if is_negative { (date2, date1) } else { (date1, date2) };

    let mut years = later.year() - earlier.year();
    let mut months = later.month() as i32 - earlier.month() as i32;
    let mut days = later.day() as i32 - earlier.day() as i32;

    // Adjust if days are negative
    if days < 0 {
        months -= 1;
        // Add days in the previous month
        let prev_month = if later.month() == 1 { 12 } else { later.month() - 1 };
        let prev_year = if later.month() == 1 { later.year() - 1 } else { later.year() };

        // Get days in previous month
        let days_in_prev_month =
            if let Some(date) = NaiveDate::from_ymd_opt(prev_year, prev_month, 1) {
                let next_month_date = if prev_month == 12 {
                    NaiveDate::from_ymd_opt(prev_year + 1, 1, 1).unwrap()
                } else {
                    NaiveDate::from_ymd_opt(prev_year, prev_month + 1, 1).unwrap()
                };
                (next_month_date - date).num_days() as i32
            } else {
                30 // Fallback to 30 days
            };

        days += days_in_prev_month;
    }

    // Adjust if months are negative
    if months < 0 {
        years -= 1;
        months += 12;
    }

    // Apply sign
    if is_negative {
        (-years, -months, -days)
    } else {
        (years, months, days)
    }
}

/// Parse a simple interval string like "5 DAY" or "1 MONTH"
/// Returns (amount, unit)
///
/// Supports formats:
/// - "5 DAY" -> (5, "DAY")
/// - "1 MONTH" -> (1, "MONTH")
/// - "-5 DAY" -> (-5, "DAY")
///
/// Note: This is a simplified parser for basic INTERVAL expressions.
/// Compound intervals (e.g., "1-6 YEAR TO MONTH") are not yet supported.
fn parse_simple_interval(interval_str: &str) -> Result<(i64, String), ExecutorError> {
    let parts: Vec<&str> = interval_str.split_whitespace().collect();

    if parts.len() < 2 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "Invalid INTERVAL format: '{}'. Expected format: 'amount unit' (e.g., '5 DAY')",
            interval_str
        )));
    }

    // Parse amount (first part)
    let amount = parts[0].parse::<i64>().map_err(|_| {
        ExecutorError::UnsupportedFeature(format!(
            "Invalid INTERVAL amount: '{}'. Expected integer",
            parts[0]
        ))
    })?;

    // Parse unit (remaining parts joined, in case unit is multi-word)
    let unit = parts[1..].join(" ").to_uppercase();

    Ok((amount, unit))
}

/// Convert a SqlValue (Date or Timestamp) to string representation
fn date_val_to_string(value: &SqlValue) -> Result<String, ExecutorError> {
    match value {
        SqlValue::Date(d) => Ok(d.to_string()),
        SqlValue::Timestamp(ts) => Ok(ts.to_string()),
        SqlValue::Null => Ok("NULL".to_string()), // Should not reach here due to NULL checks
        val => Err(ExecutorError::UnsupportedFeature(format!(
            "Expected DATE or TIMESTAMP, got {:?}",
            val
        ))),
    }
}
