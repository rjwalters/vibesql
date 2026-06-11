//! Current date/time functions
//!
//! Implements CURRENT_DATE, CURRENT_TIME, CURRENT_TIMESTAMP, and DATETIME functions.

use chrono::{Datelike, Duration, Local, NaiveDateTime, Timelike, Weekday};
use vibesql_types::SqlValue;

use crate::errors::ExecutorError;

/// Milliseconds between the start of the Julian Day epoch (-4713-11-24 12:00:00)
/// and the Unix epoch (1970-01-01 00:00:00). Matches SQLite's internal iJD origin.
pub(super) const JULIAN_EPOCH_OFFSET_MS: i64 = 210_866_760_000_000;

/// Largest valid iJD value in milliseconds (9999-12-31 23:59:59.999), matching
/// SQLite's `validJulianDay()`.
const MAX_IJD_MS: i64 = 464_269_060_799_999;

/// Convert a `NaiveDateTime` to SQLite iJD milliseconds (milliseconds since the
/// Julian Day origin). Sub-millisecond precision is truncated, like SQLite.
pub(super) fn naive_to_ijd_ms(dt: &NaiveDateTime) -> i64 {
    dt.and_utc().timestamp_millis() + JULIAN_EPOCH_OFFSET_MS
}

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
/// - Overflow: 'floor', 'ceiling' (day-of-month overflow handling)
/// - Timezone: 'localtime', 'utc'
/// - Unix: 'unixepoch' (interprets numeric input as Unix timestamp)
///
/// An omitted time-value defaults to 'now' (SQLite: `datetime()` is the
/// current date and time).
///
/// SQLite Reference: https://www.sqlite.org/lang_datefunc.html
pub fn datetime(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    // Resolve the time value (base + modifiers); None means SQL NULL
    let dt = match resolve_time_value(args, "DATETIME")? {
        Some(dt) => dt,
        None => return Ok(SqlValue::Null),
    };

    // Convert NaiveDateTime to SqlValue::Timestamp
    naive_datetime_to_timestamp(dt)
}

/// A resolved datetime plus the day-of-month overflow count needed by the
/// 'floor'/'ceiling' modifiers (SQLite date.c `nFloor`).
///
/// `n_floor` is the number of days the *nominal* Y-M-D overflowed past the end
/// of its month before rolling into the following month (rolling is the
/// default / 'ceiling' behavior). It is (re)computed at exactly three sites,
/// matching SQLite's `computeFloor` call sites: the base `YYYY-MM-DD` parse,
/// the `±N months`/`±N years` shifts, and the `±YYYY-MM-DD[ HH:MM:SS]`
/// date-offset modifier. The 'floor' modifier subtracts it; 'ceiling' clears
/// it; pure-duration shifts leave it untouched.
#[derive(Clone, Copy)]
struct ResolvedDateTime {
    dt: NaiveDateTime,
    n_floor: i64,
}

impl ResolvedDateTime {
    /// Wrap a datetime whose nominal Y-M-D did not overflow (n_floor = 0)
    fn exact(dt: NaiveDateTime) -> Self {
        ResolvedDateTime { dt, n_floor: 0 }
    }
}

/// Compute the day-of-month overflow count for a *nominal* (pre-roll) Y/M/D,
/// mirroring SQLite date.c `computeFloor`:
/// - `D <= 28` -> 0
/// - months with 31 days (Jan/Mar/May/Jul/Aug/Oct/Dec) -> 0
/// - non-February months -> 1 if `D == 31`, else 0
/// - February -> `D - 28` (non-leap year) or `D - 29` (leap year)
fn compute_floor(year: i32, month: u32, day: i64) -> i64 {
    if day <= 28 || matches!(month, 1 | 3 | 5 | 7 | 8 | 10 | 12) {
        0
    } else if month != 2 {
        i64::from(day == 31)
    } else if year % 4 != 0 || (year % 100 == 0 && year % 400 != 0) {
        day - 28
    } else {
        day - 29
    }
}

/// Resolve a SQLite time-value argument list (timestring + modifiers) to a `NaiveDateTime`.
///
/// `args[0]` is the base time value; `args[1..]` are modifiers applied in sequence.
/// If `args` is empty, the current local time is used (SQLite treats an omitted
/// time-value as 'now').
///
/// Returns:
/// - `Ok(None)` for NULL input, unparseable timestrings, or invalid modifiers
///   (SQLite returns NULL in these cases)
/// - `Err(...)` for argument types that are not supported at all
///
/// Shared by `datetime()` and `strftime()`; `func_name` is used in error messages.
pub(super) fn resolve_time_value(
    args: &[SqlValue],
    func_name: &str,
) -> Result<Option<NaiveDateTime>, ExecutorError> {
    if args.is_empty() {
        // SQLite: omitted time-value defaults to 'now'
        return Ok(Some(Local::now().naive_local()));
    }

    // The first modifier may change how the base value is interpreted:
    // 'unixepoch' (Unix timestamp), 'julianday' (forced Julian Day number),
    // 'auto' (Julian Day or Unix timestamp depending on magnitude).
    let first_modifier = match args.get(1) {
        Some(SqlValue::Varchar(s)) | Some(SqlValue::Character(s)) => Some(s.as_str()),
        _ => None,
    };

    // Parse the base datetime value
    let base_result = match first_modifier {
        Some(m) if m.eq_ignore_ascii_case("unixepoch") => {
            // For unixepoch, parse numeric values as Unix timestamps instead of Julian Days
            parse_base_datetime_for_unixepoch(&args[0], func_name)?
        }
        Some(m) if m.eq_ignore_ascii_case("auto") => parse_base_datetime_auto(&args[0], func_name)?,
        Some(m) if m.eq_ignore_ascii_case("julianday") => parse_base_datetime_julianday(&args[0]),
        _ => parse_base_datetime(&args[0], func_name)?,
    };

    // If base value is NULL, return NULL
    let mut rdt = match base_result {
        Some(rdt) => rdt,
        None => return Ok(None),
    };

    // Apply each modifier in sequence
    for (idx, modifier_arg) in args.iter().enumerate().skip(1) {
        let modifier_str = match modifier_arg {
            SqlValue::Varchar(s) | SqlValue::Character(s) => s.as_str(),
            SqlValue::Null => return Ok(None),
            _ => {
                return Err(ExecutorError::UnsupportedFeature(format!(
                    "{} modifier must be a string, got {:?}",
                    func_name, modifier_arg
                )))
            }
        };

        // Special case: 'unixepoch', 'auto', and 'julianday' must immediately
        // follow the time value (i.e. be the first modifier). They were already
        // handled above when parsing the base value.
        if modifier_str.eq_ignore_ascii_case("unixepoch")
            || modifier_str.eq_ignore_ascii_case("auto")
            || modifier_str.eq_ignore_ascii_case("julianday")
        {
            if idx != 1 {
                // Only valid as the first modifier (SQLite returns NULL otherwise)
                return Ok(None);
            }
            // Already handled during parsing, just continue
            continue;
        }

        // Apply the modifier
        match apply_datetime_modifier(rdt, modifier_str) {
            Some(new_rdt) => rdt = new_rdt,
            None => return Ok(None), // Invalid modifier returns NULL
        }
    }

    // SQLite validates the final result against the valid Julian Day range
    // (`isDate` -> `validJulianDay`): modifiers that push the date outside
    // -4713-11-24 12:00:00 .. 9999-12-31 23:59:59.999 yield NULL.
    if !(0..=MAX_IJD_MS).contains(&naive_to_ijd_ms(&rdt.dt)) {
        return Ok(None);
    }

    Ok(Some(rdt.dt))
}

/// Parse the base datetime value (first argument to DATETIME/STRFTIME's time value)
fn parse_base_datetime(
    value: &SqlValue,
    func_name: &str,
) -> Result<Option<ResolvedDateTime>, ExecutorError> {
    match value {
        SqlValue::Null => Ok(None),
        SqlValue::Varchar(s) | SqlValue::Character(s) => {
            // Handle 'now' special case
            if s.eq_ignore_ascii_case("now") {
                let now = Local::now();
                Ok(Some(ResolvedDateTime::exact(now.naive_local())))
            } else {
                // Parse the timestring (the only base form whose nominal
                // day-of-month can overflow, e.g. '2000-02-31')
                Ok(parse_datetime_string_to_naive(s))
            }
        }
        SqlValue::Date(d) => {
            // Convert Date to NaiveDateTime with time 00:00:00
            let naive_date = chrono::NaiveDate::from_ymd_opt(d.year, d.month as u32, d.day as u32);
            Ok(naive_date.map(|date| ResolvedDateTime::exact(date.and_hms_opt(0, 0, 0).unwrap())))
        }
        SqlValue::Timestamp(ts) => {
            // Convert Timestamp to NaiveDateTime
            let naive_date = chrono::NaiveDate::from_ymd_opt(
                ts.date.year,
                ts.date.month as u32,
                ts.date.day as u32,
            );
            let naive_time = chrono::NaiveTime::from_hms_opt(
                ts.time.hour as u32,
                ts.time.minute as u32,
                ts.time.second as u32,
            );
            match (naive_date, naive_time) {
                (Some(date), Some(time)) => {
                    Ok(Some(ResolvedDateTime::exact(NaiveDateTime::new(date, time))))
                }
                _ => Ok(None),
            }
        }
        // Integer or float: treat as Julian Day number by default
        SqlValue::Integer(n) => Ok(julian_day_to_naive(*n as f64).map(ResolvedDateTime::exact)),
        SqlValue::Bigint(n) => Ok(julian_day_to_naive(*n as f64).map(ResolvedDateTime::exact)),
        SqlValue::Smallint(n) => Ok(julian_day_to_naive(*n as f64).map(ResolvedDateTime::exact)),
        SqlValue::Float(n) => Ok(julian_day_to_naive(*n as f64).map(ResolvedDateTime::exact)),
        SqlValue::Double(n) | SqlValue::Real(n) | SqlValue::Numeric(n) => {
            Ok(julian_day_to_naive(*n).map(ResolvedDateTime::exact))
        }
        _ => Err(ExecutorError::UnsupportedFeature(format!(
            "{} requires string, date, timestamp, or numeric argument, got {:?}",
            func_name, value
        ))),
    }
}

/// Parse the base datetime value when 'unixepoch' is the first modifier
/// Numeric values are interpreted as Unix timestamps instead of Julian Days
fn parse_base_datetime_for_unixepoch(
    value: &SqlValue,
    func_name: &str,
) -> Result<Option<ResolvedDateTime>, ExecutorError> {
    let dt = match value {
        SqlValue::Null => None,
        SqlValue::Varchar(s) | SqlValue::Character(s) => {
            // Try to parse string as number for unixepoch
            if let Ok(n) = s.trim().parse::<i64>() {
                unix_epoch_to_datetime(n)
            } else if let Ok(n) = s.trim().parse::<f64>() {
                unix_epoch_seconds_f64_to_datetime(n)
            } else {
                // String that's not a valid number + unixepoch = NULL
                None
            }
        }
        SqlValue::Date(_) | SqlValue::Timestamp(_) => {
            // Date/Timestamp with unixepoch doesn't make sense - return NULL
            None
        }
        // Integer or float: treat as Unix timestamp (floats keep ms precision)
        SqlValue::Integer(n) => unix_epoch_to_datetime(*n as i64),
        SqlValue::Bigint(n) => unix_epoch_to_datetime(*n),
        SqlValue::Smallint(n) => unix_epoch_to_datetime(*n as i64),
        SqlValue::Float(n) => unix_epoch_seconds_f64_to_datetime(*n as f64),
        SqlValue::Double(n) | SqlValue::Real(n) | SqlValue::Numeric(n) => {
            unix_epoch_seconds_f64_to_datetime(*n)
        }
        _ => {
            return Err(ExecutorError::UnsupportedFeature(format!(
                "{} requires string, date, timestamp, or numeric argument, got {:?}",
                func_name, value
            )))
        }
    };
    Ok(dt.map(ResolvedDateTime::exact))
}

/// Extract the numeric interpretation of a time value, if it has one.
///
/// Mirrors SQLite's "raw number" classification (`setRawDateNumber` /
/// `sqlite3AtoF` fallback in `parseDateOrTime`): SQL numerics and strings that
/// are plain numeric literals qualify; everything else does not.
fn numeric_time_value(value: &SqlValue) -> Option<f64> {
    match value {
        SqlValue::Integer(n) => Some(*n as f64),
        SqlValue::Bigint(n) => Some(*n as f64),
        SqlValue::Smallint(n) => Some(*n as f64),
        SqlValue::Float(n) => Some(*n as f64),
        SqlValue::Double(n) | SqlValue::Real(n) | SqlValue::Numeric(n) => Some(*n),
        SqlValue::Varchar(s) | SqlValue::Character(s) => s.trim().parse::<f64>().ok(),
        _ => None,
    }
}

/// Parse the base datetime value when 'auto' is the first modifier.
///
/// SQLite semantics (date.c, 'auto' case): numeric values in `[0.0, 5373484.5)`
/// are Julian Day numbers; other numerics within `[-210866760000, 253402300799]`
/// are Unix timestamps; numerics outside both ranges yield NULL. 'auto' is a
/// no-op for text (non-numeric) time values.
fn parse_base_datetime_auto(
    value: &SqlValue,
    func_name: &str,
) -> Result<Option<ResolvedDateTime>, ExecutorError> {
    if matches!(value, SqlValue::Null) {
        return Ok(None);
    }
    match numeric_time_value(value) {
        Some(n) if (0.0..5_373_484.5).contains(&n) => {
            Ok(julian_day_to_naive(n).map(ResolvedDateTime::exact))
        }
        Some(n) if (-210_866_760_000.0..=253_402_300_799.0).contains(&n) => {
            Ok(unix_epoch_seconds_f64_to_datetime(n).map(ResolvedDateTime::exact))
        }
        Some(_) => Ok(None), // Out of range for both interpretations
        None => parse_base_datetime(value, func_name), // No-op for text values
    }
}

/// Parse the base datetime value when 'julianday' is the first modifier.
///
/// SQLite semantics: only valid when the time value is numeric (the DDDDDDDDDD
/// format) and within the valid Julian Day range; all other uses return NULL.
fn parse_base_datetime_julianday(value: &SqlValue) -> Option<ResolvedDateTime> {
    match numeric_time_value(value) {
        Some(n) if (0.0..5_373_484.5).contains(&n) => {
            julian_day_to_naive(n).map(ResolvedDateTime::exact)
        }
        _ => None,
    }
}

/// Apply a single modifier to a resolved datetime
/// Returns None if the modifier is invalid (SQLite returns NULL for invalid modifiers)
fn apply_datetime_modifier(rdt: ResolvedDateTime, modifier: &str) -> Option<ResolvedDateTime> {
    let modifier = modifier.trim();
    let lower = modifier.to_lowercase();
    let dt = rdt.dt;

    // Handle 'floor' / 'ceiling' (SQLite 3.46+): resolve a pending
    // day-of-month overflow. Rolling forward is the default, so 'ceiling'
    // just clears the pending count; 'floor' rolls back to the last day of
    // the nominal month by subtracting the overflow days.
    if lower == "floor" {
        return Some(ResolvedDateTime::exact(dt - Duration::days(rdt.n_floor)));
    }
    if lower == "ceiling" {
        return Some(ResolvedDateTime::exact(dt));
    }

    // Handle "start of" modifiers
    if lower.starts_with("start of ") {
        let unit = &lower[9..].trim();
        let new_dt = match *unit {
            "month" => {
                let date = chrono::NaiveDate::from_ymd_opt(dt.year(), dt.month(), 1)?;
                date.and_hms_opt(0, 0, 0)?
            }
            "year" => {
                let date = chrono::NaiveDate::from_ymd_opt(dt.year(), 1, 1)?;
                date.and_hms_opt(0, 0, 0)?
            }
            "day" => dt.date().and_hms_opt(0, 0, 0)?,
            _ => return None, // Invalid "start of" unit
        };
        return Some(ResolvedDateTime { dt: new_dt, n_floor: rdt.n_floor });
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
                    return apply_weekday_modifier(dt, weekday_num)
                        .map(|new_dt| ResolvedDateTime { dt: new_dt, n_floor: rdt.n_floor });
                }
            }
        }
        return None; // Invalid weekday
    }

    // Handle "localtime" and "utc" (currently no-op as we work with naive datetimes)
    if lower == "localtime" || lower == "utc" {
        // For now, these are no-ops since we work with naive datetimes
        // A full implementation would need timezone-aware handling
        return Some(rdt);
    }

    // Handle (+|-)YYYY-MM-DD[ HH:MM[:SS[.FFF]]] modifiers (the inverse of
    // timediff(); SQLite 3.43+). Must be checked before generic time shifts.
    // This is one of the sites that recomputes n_floor.
    if let Some(result) = try_apply_date_offset_modifier(dt, modifier) {
        return result;
    }

    // Handle (+|-)HH:MM[:SS[.FFF]] modifiers (no sign means plus). Must be
    // checked before generic time shifts. Pure-duration shift: n_floor is
    // preserved.
    if let Some(result) = try_apply_hh_mm_modifier(dt, modifier) {
        return result.map(|new_dt| ResolvedDateTime { dt: new_dt, n_floor: rdt.n_floor });
    }

    // Handle time shift modifiers: +N unit, -N unit, N unit
    parse_and_apply_time_shift(rdt, modifier)
}

/// Detect and apply a `[+-]HH:MM[:SS[.FFF]]` modifier (SQLite date.c
/// `parseModifier`, '+'/'-'/digit case with a ':' after the leading number).
/// A missing sign means plus (e.g. `'12:30'` adds 12 hours 30 minutes).
///
/// Returns:
/// - `None` if the modifier is not of this form (caller should fall through)
/// - `Some(None)` if it is of this form but invalid (SQLite returns NULL,
///   e.g. `'12:60'`)
/// - `Some(Some(dt))` on success
fn try_apply_hh_mm_modifier(dt: NaiveDateTime, modifier: &str) -> Option<Option<NaiveDateTime>> {
    let (sign, rest) = match modifier.as_bytes().first() {
        Some(b'+') => (1i64, &modifier[1..]),
        Some(b'-') => (-1i64, &modifier[1..]),
        Some(b) if b.is_ascii_digit() => (1i64, modifier),
        _ => return None,
    };
    let b = rest.as_bytes();
    if !(b.len() >= 5 && b[0].is_ascii_digit() && b[1].is_ascii_digit() && b[2] == b':') {
        return None; // Not the HH:MM form; fall through to other modifiers
    }
    Some(parse_hh_mm_ss_ms(rest).map(|ms| dt + Duration::milliseconds(sign * ms)))
}

/// Detect and apply a `(+|-)YYYY-MM-DD[ HH:MM[:SS[.FFF]]]` modifier.
///
/// Returns:
/// - `None` if the modifier is not of this form (caller should fall through)
/// - `Some(None)` if it is of this form but invalid (SQLite returns NULL)
/// - `Some(Some(dt))` on success
///
/// SQLite semantics (date.c `parseModifier`, '+'/'-' digit case): the year may
/// be 4 or 5 digits, MM is limited to 0-11 and DD to 0-30. Years and months are
/// applied with calendar normalization (day-of-month overflow rolls into the
/// next month), then days and the optional time offset are added as exact
/// durations.
fn try_apply_date_offset_modifier(
    dt: NaiveDateTime,
    modifier: &str,
) -> Option<Option<ResolvedDateTime>> {
    let sign: i64 = match modifier.as_bytes().first() {
        Some(b'+') => 1,
        Some(b'-') => -1,
        _ => return None,
    };
    let rest = &modifier[1..];
    let year_digits = rest.bytes().take_while(|b| b.is_ascii_digit()).count();
    if !((year_digits == 4 || year_digits == 5) && rest.as_bytes().get(year_digits) == Some(&b'-'))
    {
        return None; // Not the date-offset form; fall through to other modifiers
    }
    Some(apply_date_offset_modifier(dt, sign, rest, year_digits))
}

/// Apply a validated-prefix `YYYY-MM-DD[ HH:MM[:SS[.FFF]]]` offset to `dt`.
/// Any malformation makes the whole modifier invalid (returns None -> NULL).
///
/// Recomputes `n_floor` from the nominal year/month/day after the year+month
/// shift but before the day/time offsets are added (matching SQLite's
/// `computeFloor` call in the date-offset modifier).
fn apply_date_offset_modifier(
    dt: NaiveDateTime,
    sign: i64,
    rest: &str,
    year_digits: usize,
) -> Option<ResolvedDateTime> {
    let years: i64 = rest[..year_digits].parse().ok()?;
    let after = &rest[year_digits + 1..];
    let ab = after.as_bytes();
    if ab.len() < 5
        || !ab[0].is_ascii_digit()
        || !ab[1].is_ascii_digit()
        || ab[2] != b'-'
        || !ab[3].is_ascii_digit()
        || !ab[4].is_ascii_digit()
    {
        return None;
    }
    let months: i64 = after[..2].parse().ok()?;
    let days: i64 = after[3..5].parse().ok()?;
    // SQLite limits: MM in 0..11, DD in 0..30
    if months >= 12 || days >= 31 {
        return None;
    }

    // Optional time component: exactly one space, then HH:MM[:SS[.FFF...]]
    let tail = &after[5..];
    let time_offset_ms: i64 = if tail.is_empty() {
        0
    } else {
        let mut chars = tail.chars();
        if !chars.next().is_some_and(|c| c.is_ascii_whitespace()) {
            return None;
        }
        parse_hh_mm_ss_ms(chars.as_str())?
    };

    // Apply years + months with calendar normalization (matching SQLite's
    // computeJD: an out-of-range day-of-month rolls into the following month)
    let total_months =
        dt.year() as i64 * 12 + (dt.month() as i64 - 1) + sign * (years * 12 + months);
    let new_year = i32::try_from(total_months.div_euclid(12)).ok()?;
    let new_month = total_months.rem_euclid(12) as u32 + 1;
    let n_floor = compute_floor(new_year, new_month, dt.day() as i64);
    let base = chrono::NaiveDate::from_ymd_opt(new_year, new_month, 1)?.and_time(dt.time())
        + Duration::days(dt.day() as i64 - 1);

    Some(ResolvedDateTime {
        dt: base + Duration::days(sign * days) + Duration::milliseconds(sign * time_offset_ms),
        n_floor,
    })
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
///
/// Pure-duration shifts (days/hours/minutes/seconds) preserve the incoming
/// `n_floor`; month/year shifts recompute it (SQLite's `computeFloor` call in
/// the month/year transform cases).
fn parse_and_apply_time_shift(rdt: ResolvedDateTime, modifier: &str) -> Option<ResolvedDateTime> {
    let modifier = modifier.trim();
    let dt = rdt.dt;

    // Parse the amount and unit
    // Format: [+/-]N[.N] unit[s]
    let (amount_str, unit) = split_amount_and_unit(modifier)?;

    // Parse the amount (supports fractional values)
    let amount: f64 = amount_str.parse().ok()?;

    // Normalize unit (remove trailing 's' for plurals)
    let unit_lower = unit.to_lowercase();
    let unit_normalized = unit_lower.trim_end_matches('s');

    // SQLite rounds the millisecond result half away from zero
    let to_ms = |value: f64| -> i64 {
        let rounder = if value < 0.0 { -0.5 } else { 0.5 };
        (value + rounder) as i64
    };

    let duration_shift = |ms: i64| -> Option<ResolvedDateTime> {
        Some(ResolvedDateTime { dt: dt + Duration::milliseconds(ms), n_floor: rdt.n_floor })
    };

    match unit_normalized {
        "day" => duration_shift(to_ms(amount * 86_400_000.0)),
        "hour" => duration_shift(to_ms(amount * 3_600_000.0)),
        "minute" => duration_shift(to_ms(amount * 60_000.0)),
        "second" => duration_shift(to_ms(amount * 1000.0)),
        "month" => {
            // Whole months shift the calendar; the fractional residue is added
            // as 30-day months in milliseconds (SQLite aXformType rXform)
            let whole_months = amount.trunc() as i32;
            let new_rdt = add_months(dt, whole_months)?;
            Some(ResolvedDateTime {
                dt: new_rdt.dt + Duration::milliseconds(to_ms(amount.fract() * 2_592_000_000.0)),
                n_floor: new_rdt.n_floor,
            })
        }
        "year" => {
            // Whole years shift the calendar; the fractional residue is added
            // as 365-day years in milliseconds (SQLite aXformType rXform)
            let whole_years = amount.trunc() as i32;
            let new_rdt = add_months(dt, whole_years * 12)?;
            Some(ResolvedDateTime {
                dt: new_rdt.dt + Duration::milliseconds(to_ms(amount.fract() * 31_536_000_000.0)),
                n_floor: new_rdt.n_floor,
            })
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

/// Add months to a NaiveDateTime.
///
/// Matches SQLite's `computeJD` normalization: when the day-of-month overflows
/// the target month (e.g. Jan 31 + 1 month), the date rolls into the following
/// month (2000-01-31 '+1 month' -> 2000-03-02) rather than clamping. The
/// overflow-day count is recorded in `n_floor` so a subsequent 'floor'
/// modifier can clamp to the last day of the nominal month instead.
fn add_months(dt: NaiveDateTime, months: i32) -> Option<ResolvedDateTime> {
    let total_months = dt.year() as i64 * 12 + dt.month() as i64 - 1 + months as i64;
    let new_year = i32::try_from(total_months.div_euclid(12)).ok()?;
    let new_month = total_months.rem_euclid(12) as u32 + 1;
    let n_floor = compute_floor(new_year, new_month, dt.day() as i64);

    // Day-of-month overflow rolls into the next month (SQLite normalization)
    let new_date = chrono::NaiveDate::from_ymd_opt(new_year, new_month, 1)?
        + Duration::days(dt.day() as i64 - 1);
    Some(ResolvedDateTime { dt: NaiveDateTime::new(new_date, dt.time()), n_floor })
}

/// Convert Unix epoch timestamp to NaiveDateTime
fn unix_epoch_to_datetime(timestamp: i64) -> Option<NaiveDateTime> {
    chrono::DateTime::from_timestamp(timestamp, 0).map(|dt| dt.naive_utc())
}

/// Convert a fractional Unix epoch timestamp (seconds) to NaiveDateTime with
/// millisecond precision, matching SQLite's `iJD = (i64)(r*1000.0 + offset + 0.5)`.
fn unix_epoch_seconds_f64_to_datetime(seconds: f64) -> Option<NaiveDateTime> {
    if !seconds.is_finite() {
        return None;
    }
    let ijd_ms = (seconds * 1000.0 + JULIAN_EPOCH_OFFSET_MS as f64 + 0.5).floor() as i64;
    if !(0..=MAX_IJD_MS).contains(&ijd_ms) {
        return None;
    }
    chrono::DateTime::from_timestamp_millis(ijd_ms - JULIAN_EPOCH_OFFSET_MS)
        .map(|dt| dt.naive_utc())
}

/// Convert Julian Day to NaiveDateTime
///
/// Matches SQLite: `iJD = (sqlite3_int64)(jd*86400000.0 + 0.5)` with millisecond
/// precision, valid for 0 <= iJD <= 464269060799999 (-4713-11-24 .. 9999-12-31).
fn julian_day_to_naive(jd: f64) -> Option<NaiveDateTime> {
    if !jd.is_finite() || jd < 0.0 {
        return None;
    }
    let ijd_ms = (jd * 86_400_000.0 + 0.5).floor() as i64;
    if ijd_ms > MAX_IJD_MS {
        return None;
    }
    chrono::DateTime::from_timestamp_millis(ijd_ms - JULIAN_EPOCH_OFFSET_MS)
        .map(|dt| dt.naive_utc())
}

/// Convert NaiveDateTime to SqlValue::Timestamp
fn naive_datetime_to_timestamp(dt: NaiveDateTime) -> Result<SqlValue, ExecutorError> {
    let date = vibesql_types::Date::new(dt.year(), dt.month() as u8, dt.day() as u8)
        .map_err(|e| ExecutorError::UnsupportedFeature(format!("Invalid date: {}", e)))?;
    let time = vibesql_types::Time::new(dt.hour() as u8, dt.minute() as u8, dt.second() as u8, 0)
        .map_err(|e| ExecutorError::UnsupportedFeature(format!("Invalid time: {}", e)))?;
    Ok(SqlValue::Timestamp(vibesql_types::Timestamp::new(date, time)))
}

/// Helper to parse SQLite datetime strings to NaiveDateTime
///
/// Follows SQLite's `parseYyyyMmDd` grammar strictly:
/// `[-]YYYY-MM-DD[<space|T>HH:MM[:SS[.FFF...]][TZ]]` where every field is
/// exactly the documented number of digits and `TZ` is an optional timezone
/// suffix (`Z`/`z` or `[+-]HH:MM`) that converts the timestamp to UTC. An
/// out-of-range day-of-month (e.g. `2003-02-31`) normalizes into the following
/// month, matching SQLite's `computeJD`. A time-only value
/// `HH:MM[:SS[.FFF...]]` defaults the date to 2000-01-01 (SQLite
/// `parseTimeOnly`). A plain numeric string is interpreted as a Julian Day
/// number.
fn parse_datetime_string_to_naive(s: &str) -> Option<ResolvedDateTime> {
    let s = s.trim();

    if let Some(rdt) = parse_yyyy_mm_dd(s) {
        return Some(rdt);
    }

    // SQLite: time-only values `HH:MM[:SS[.FFF...]]` default the date to
    // 2000-01-01 (parseTimeOnly in date.c)
    if let Some(time_ms) = parse_hh_mm_ss_tz_ms(s) {
        let midnight = chrono::NaiveDate::from_ymd_opt(2000, 1, 1)?.and_hms_opt(0, 0, 0)?;
        return Some(ResolvedDateTime::exact(midnight + Duration::milliseconds(time_ms)));
    }

    // SQLite: a string that is a plain numeric literal is interpreted as a
    // Julian Day number, e.g. datetime('2451545.0') = '2000-01-01 12:00:00'
    if let Ok(jd) = s.parse::<f64>() {
        return julian_day_to_naive(jd).map(ResolvedDateTime::exact);
    }

    None
}

/// Strict `[-]YYYY-MM-DD[<sep>HH:MM[:SS[.FFF...]]]` parser (SQLite parseYyyyMmDd).
///
/// Records the day-of-month overflow count in `n_floor` (e.g. '2000-02-31'
/// rolls to 2000-03-02 with `n_floor` = 2) for the 'floor'/'ceiling' modifiers.
fn parse_yyyy_mm_dd(s: &str) -> Option<ResolvedDateTime> {
    // Optional leading '-' for negative (astronomical) years
    let (negative_year, rest) = match s.strip_prefix('-') {
        Some(r) => (true, r),
        None => (false, s),
    };

    // Date part: exactly "YYYY-MM-DD"
    let b = rest.as_bytes();
    if b.len() < 10
        || !b[..4].iter().all(u8::is_ascii_digit)
        || b[4] != b'-'
        || !b[5].is_ascii_digit()
        || !b[6].is_ascii_digit()
        || b[7] != b'-'
        || !b[8].is_ascii_digit()
        || !b[9].is_ascii_digit()
    {
        return None;
    }
    let year: i32 = rest[..4].parse().ok()?;
    let year = if negative_year { -year } else { year };
    let month: u32 = rest[5..7].parse().ok()?;
    let day: i64 = rest[8..10].parse().ok()?;
    if !(1..=12).contains(&month) || !(1..=31).contains(&day) {
        return None;
    }
    // Day-of-month overflow rolls into the next month (SQLite computeJD
    // normalization, e.g. 2003-02-31 -> 2003-03-03); the overflow count is
    // tracked so the 'floor' modifier can clamp instead
    let n_floor = compute_floor(year, month, day);
    let date = chrono::NaiveDate::from_ymd_opt(year, month, 1)? + Duration::days(day - 1);

    // Optional time part. SQLite skips any run of whitespace and/or 'T'
    // characters between the date and the time (including none at all).
    let tail = rest[10..].trim_start_matches(|c: char| c.is_ascii_whitespace() || c == 'T');
    if tail.is_empty() {
        return Some(ResolvedDateTime { dt: date.and_hms_opt(0, 0, 0)?, n_floor });
    }

    let time_ms = parse_hh_mm_ss_tz_ms(tail)?;
    Some(ResolvedDateTime {
        dt: date.and_hms_opt(0, 0, 0)? + Duration::milliseconds(time_ms),
        n_floor,
    })
}

/// Strict `HH:MM[:SS[.FFF...]]` parser returning milliseconds since midnight
/// plus the unconsumed tail. Mirrors SQLite's parseHhMmSs limits: HH <= 24,
/// MM/SS <= 59, the fraction needs at least one digit, sub-millisecond
/// fractions round like SQLite.
fn parse_hh_mm_ss_core(t: &str) -> Option<(i64, &str)> {
    let b = t.as_bytes();
    if b.len() < 5
        || !b[0].is_ascii_digit()
        || !b[1].is_ascii_digit()
        || b[2] != b':'
        || !b[3].is_ascii_digit()
        || !b[4].is_ascii_digit()
    {
        return None;
    }
    let hours: i64 = t[..2].parse().ok()?;
    let minutes: i64 = t[3..5].parse().ok()?;
    if hours > 24 || minutes > 59 {
        return None;
    }
    let mut total_ms = (hours * 3600 + minutes * 60) * 1000;

    let mut rest = &t[5..];
    if let Some(sec_part) = rest.strip_prefix(':') {
        let sb = sec_part.as_bytes();
        if sb.len() < 2 || !sb[0].is_ascii_digit() || !sb[1].is_ascii_digit() {
            return None;
        }
        let seconds: i64 = sec_part[..2].parse().ok()?;
        if seconds > 59 {
            return None;
        }
        total_ms += seconds * 1000;
        rest = &sec_part[2..];
        // Optional fractional seconds: '.' followed by at least one digit
        if let Some(frac_part) = rest.strip_prefix('.') {
            let frac_digits = frac_part.bytes().take_while(|c| c.is_ascii_digit()).count();
            if frac_digits == 0 {
                return None;
            }
            let frac: f64 = format!("0.{}", &frac_part[..frac_digits]).parse().ok()?;
            // SQLite caps sub-millisecond fractions then rounds to ms
            total_ms += ((frac.min(0.999)) * 1000.0 + 0.5).floor() as i64;
            rest = &frac_part[frac_digits..];
        }
    }

    Some((total_ms, rest))
}

/// Strict `HH:MM[:SS[.FFF...]]` parser returning milliseconds since midnight.
/// Used for modifiers, where a timezone suffix is not allowed: trailing
/// content other than whitespace is an error.
fn parse_hh_mm_ss_ms(t: &str) -> Option<i64> {
    let (total_ms, rest) = parse_hh_mm_ss_core(t)?;
    // Only trailing whitespace is allowed
    if rest.chars().all(|c| c.is_ascii_whitespace()) {
        Some(total_ms)
    } else {
        None
    }
}

/// `HH:MM[:SS[.FFF...]]` parser for timestrings, with an optional timezone
/// suffix (`Z`/`z` or `[+-]HH:MM`, SQLite date.c `parseTimezone`). Returns the
/// UTC-adjusted milliseconds offset from midnight (the timezone offset is
/// subtracted, converting the timestamp to UTC).
fn parse_hh_mm_ss_tz_ms(t: &str) -> Option<i64> {
    let (total_ms, rest) = parse_hh_mm_ss_core(t)?;
    let tz_minutes = parse_timezone_suffix(rest)?;
    Some(total_ms - tz_minutes * 60_000)
}

/// Parse an optional timezone suffix following the time component, mirroring
/// SQLite's `parseTimezone`: optional whitespace, then either `Z`/`z` or
/// `[+-]HH:MM` (HH <= 14, MM <= 59), then optional trailing whitespace.
/// Returns the offset in minutes (0 for Zulu or no suffix); `None` for
/// malformed suffixes or trailing junk.
fn parse_timezone_suffix(s: &str) -> Option<i64> {
    let s = s.trim_start_matches(|c: char| c.is_ascii_whitespace());
    if s.is_empty() {
        return Some(0);
    }
    let (offset_minutes, rest) = match s.as_bytes()[0] {
        b'Z' | b'z' => (0, &s[1..]),
        sign @ (b'+' | b'-') => {
            let b = s.as_bytes();
            if b.len() < 6
                || !b[1].is_ascii_digit()
                || !b[2].is_ascii_digit()
                || b[3] != b':'
                || !b[4].is_ascii_digit()
                || !b[5].is_ascii_digit()
            {
                return None;
            }
            let hours: i64 = s[1..3].parse().ok()?;
            let minutes: i64 = s[4..6].parse().ok()?;
            if hours > 14 || minutes > 59 {
                return None;
            }
            let total = hours * 60 + minutes;
            (if sign == b'-' { -total } else { total }, &s[6..])
        }
        _ => return None,
    };
    // Only trailing whitespace is allowed after the timezone
    if rest.chars().all(|c| c.is_ascii_whitespace()) {
        Some(offset_minutes)
    } else {
        None
    }
}
