//! STRFTIME - SQLite-compatible date/time formatting function
//!
//! Implements `strftime(format, timevalue, modifier, modifier, ...)` following
//! SQLite semantics: the time value and modifiers are resolved exactly like
//! `datetime()` (see `current::resolve_time_value`), then the resolved instant
//! is rendered through the format string.
//!
//! Supported format specifiers (the classic SQLite set):
//!
//! | Specifier | Meaning                                  | Example (`2003-10-31 12:34:56.432`) |
//! |-----------|------------------------------------------|--------------------------------------|
//! | `%d`      | day of month, zero-padded (01-31)        | `31`                                 |
//! | `%f`      | fractional seconds `SS.SSS`              | `56.432`                             |
//! | `%H`      | hour, zero-padded (00-23)                | `12`                                 |
//! | `%j`      | day of year, zero-padded (001-366)       | `304`                                |
//! | `%J`      | fractional Julian Day number             | `2452944.024264259`                  |
//! | `%m`      | month, zero-padded (01-12)               | `10`                                 |
//! | `%M`      | minute, zero-padded (00-59)              | `34`                                 |
//! | `%s`      | seconds since Unix epoch (integer)       | `1067603696`                         |
//! | `%S`      | seconds, zero-padded (00-59)             | `56`                                 |
//! | `%w`      | day of week (0-6, Sunday=0)              | `5`                                  |
//! | `%W`      | week of year (00-53, Monday-based)       | `43`                                 |
//! | `%Y`      | year, zero-padded to 4 digits            | `2003`                               |
//! | `%%`      | literal `%`                              | `%`                                  |
//!
//! Out of scope for v1 (returns NULL like older SQLite versions):
//! - SQLite 3.44+ additions: `%e %F %G %g %I %k %l %p %P %R %T %u %U %V`
//! - Modifiers beyond what `datetime()` supports (`subsec`, real timezone
//!   conversion for `localtime`/`utc`, `±HH:MM` offsets, `auto`, `julianday`)
//!
//! SQLite Reference: https://www.sqlite.org/lang_datefunc.html

use chrono::{Datelike, NaiveDateTime, Timelike};
use vibesql_types::SqlValue;

use super::current::{resolve_time_value, JULIAN_EPOCH_OFFSET_MS};
use crate::errors::ExecutorError;

/// STRFTIME - Format a time value according to a format string
///
/// `strftime(format, timevalue, modifier, modifier, ...)`
///
/// Returns TEXT (SQLite semantics). Returns NULL when:
/// - the format or time value is NULL
/// - the time value cannot be parsed
/// - a modifier is invalid
/// - the format contains an unsupported `%x` specifier
pub fn strftime(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.is_empty() {
        return Err(ExecutorError::UnsupportedFeature(
            "STRFTIME requires at least 1 argument".to_string(),
        ));
    }

    // First argument is the format string
    let format = match &args[0] {
        SqlValue::Varchar(s) | SqlValue::Character(s) => s.as_str(),
        SqlValue::Null => return Ok(SqlValue::Null),
        other => {
            return Err(ExecutorError::UnsupportedFeature(format!(
                "STRFTIME format must be a string, got {:?}",
                other
            )))
        }
    };

    // Remaining arguments are the time value + modifiers (an empty list means 'now')
    let dt = match resolve_time_value(&args[1..], "STRFTIME")? {
        Some(dt) => dt,
        None => return Ok(SqlValue::Null),
    };

    match format_datetime(format, dt) {
        Some(s) => Ok(SqlValue::Varchar(s.into())),
        None => Ok(SqlValue::Null), // Unknown specifier returns NULL (SQLite behavior)
    }
}

/// Render `dt` through the strftime format string.
/// Returns None for unsupported/unknown specifiers (including a trailing `%`).
fn format_datetime(format: &str, dt: NaiveDateTime) -> Option<String> {
    use std::fmt::Write;

    let mut out = String::with_capacity(format.len() + 16);
    let mut chars = format.chars();

    while let Some(c) = chars.next() {
        if c != '%' {
            out.push(c);
            continue;
        }

        let spec = chars.next()?; // trailing '%' is an error -> NULL
        match spec {
            'd' => write!(out, "{:02}", dt.day()).ok()?,
            'f' => {
                // Seconds with milliseconds, "%06.3f" in SQLite (e.g. 56.432, 06.400).
                // Truncate (not round) sub-millisecond precision; clamp leap seconds.
                let millis = (dt.nanosecond() / 1_000_000).min(999);
                write!(out, "{:02}.{:03}", dt.second(), millis).ok()?
            }
            'H' => write!(out, "{:02}", dt.hour()).ok()?,
            'j' => write!(out, "{:03}", dt.ordinal()).ok()?,
            'J' => out.push_str(&format_julian_day(dt)),
            'm' => write!(out, "{:02}", dt.month()).ok()?,
            'M' => write!(out, "{:02}", dt.minute()).ok()?,
            's' => write!(out, "{}", dt.and_utc().timestamp()).ok()?,
            'S' => write!(out, "{:02}", dt.second()).ok()?,
            'w' => write!(out, "{}", dt.weekday().num_days_from_sunday()).ok()?,
            'W' => {
                // SQLite: week 01 starts on the first Monday of the year; days
                // before that are week 00. Computed as (nDay + 7 - wd) / 7 where
                // nDay is the 0-based day of year and wd is the Monday-based
                // day of week (0=Monday).
                let n_day = dt.ordinal0();
                let wd = dt.weekday().num_days_from_monday();
                write!(out, "{:02}", (n_day + 7 - wd) / 7).ok()?
            }
            'Y' => write!(out, "{:04}", dt.year()).ok()?,
            '%' => out.push('%'),
            _ => return None, // Unsupported specifier -> NULL (matches SQLite)
        }
    }

    Some(out)
}

/// Format the fractional Julian Day number for `%J`.
///
/// SQLite computes `iJD / 86400000.0` (iJD = milliseconds since the Julian Day
/// origin) and prints it with `"%.16g"`. We reproduce both the arithmetic and
/// the 16-significant-digit rendering so output matches SQLite byte-for-byte.
fn format_julian_day(dt: NaiveDateTime) -> String {
    let epoch_ms = dt.and_utc().timestamp_millis();
    let jd = (epoch_ms + JULIAN_EPOCH_OFFSET_MS) as f64 / 86_400_000.0;
    format_g16(jd)
}

/// Emulate C's `"%.16g"` (16 significant digits, trailing zeros trimmed) for
/// the magnitude range Julian Day numbers occupy (0 .. ~5.4 million). Values in
/// this range never use scientific notation under `%g`.
fn format_g16(v: f64) -> String {
    if v == 0.0 {
        return "0".to_string();
    }
    // Number of digits before the decimal point
    let int_digits = if v.abs() < 1.0 { 1 } else { v.abs().log10().floor() as i32 + 1 };
    let decimals = (16 - int_digits).clamp(0, 17) as usize;
    let s = format!("{:.*}", decimals, v);
    if s.contains('.') {
        s.trim_end_matches('0').trim_end_matches('.').to_string()
    } else {
        s
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sf(args: &[SqlValue]) -> SqlValue {
        strftime(args).expect("strftime should not error")
    }

    fn text(s: &str) -> SqlValue {
        SqlValue::Varchar(s.into())
    }

    fn assert_strftime(format: &str, timevalue: &str, expected: &str) {
        let result = sf(&[text(format), text(timevalue)]);
        assert_eq!(
            result,
            text(expected),
            "strftime('{}', '{}') should be '{}'",
            format,
            timevalue,
            expected
        );
    }

    // Ground truth: SQLite date.test section 3 ('2003-10-31 12:34:56.432')

    #[test]
    fn test_day_of_month() {
        assert_strftime("%d", "2003-10-31 12:34:56.432", "31");
        assert_strftime("%d", "2004-01-02", "02");
    }

    #[test]
    fn test_fractional_seconds() {
        // date.test 3.2.1
        assert_strftime("pre%fpost", "2003-10-31 12:34:56.432", "pre56.432post");
        // date.test 3.2.2: truncation, not rounding
        assert_strftime("%f", "2003-10-31 12:34:59.9999999", "59.999");
        // Zero-padding on both sides
        assert_strftime("%f", "2003-10-31 12:34:06.4", "06.400");
        assert_strftime("%f", "2003-10-31 12:34:56", "56.000");
    }

    #[test]
    fn test_hour() {
        assert_strftime("%H", "2003-10-31 12:34:56.432", "12");
        assert_strftime("%H", "2003-10-31 04:34:56", "04");
        assert_strftime("%H", "2003-10-31 00:00:00", "00");
    }

    #[test]
    fn test_day_of_year() {
        assert_strftime("%j", "2003-10-31 12:34:56.432", "304");
        assert_strftime("%j", "2004-01-01", "001");
        assert_strftime("%j", "2004-12-31", "366"); // leap year
    }

    #[test]
    fn test_julian_day() {
        // date.test 3.5
        assert_strftime("%J", "2003-10-31 12:34:56.432", "2452944.024264259");
        // Noon on the Unix epoch is an exact Julian Day boundary
        assert_strftime("%J", "1970-01-01 12:00:00", "2440588");
        assert_strftime("%J", "1970-01-01 00:00:00", "2440587.5");
    }

    #[test]
    fn test_month() {
        assert_strftime("%m", "2003-10-31 12:34:56.432", "10");
        assert_strftime("%m", "2003-01-31", "01");
    }

    #[test]
    fn test_minute() {
        assert_strftime("%M", "2003-10-31 12:34:56.432", "34");
        assert_strftime("%M", "2003-10-31 12:04:56", "04");
    }

    #[test]
    fn test_unix_epoch_seconds() {
        // date.test 3.8.x
        assert_strftime("%s", "2003-10-31 12:34:56.432", "1067603696");
        assert_strftime("%s", "2038-01-19 03:14:07", "2147483647");
        assert_strftime("%s", "2038-01-19 03:14:08", "2147483648");
        assert_strftime("%s", "2201-04-09 12:00:00", "7298164800");
        assert_strftime("%s", "9999-12-31 23:59:59", "253402300799");
        assert_strftime("%s", "1969-12-31 23:59:59", "-1");
        assert_strftime("%s", "1901-12-13 20:45:52", "-2147483648");
        assert_strftime("%s", "1901-12-13 20:45:51", "-2147483649");
        assert_strftime("%s", "1776-07-04 00:00:00", "-6106060800");
    }

    #[test]
    fn test_seconds() {
        assert_strftime("%S", "2003-10-31 12:34:56.432", "56");
        assert_strftime("%S", "2003-10-31 12:34:06", "06");
    }

    #[test]
    fn test_day_of_week() {
        // 2003-10-31 was a Friday (Sunday=0 -> 5)
        assert_strftime("%w", "2003-10-31 12:34:56.432", "5");
        // 2004-01-04 was a Sunday
        assert_strftime("%w", "2004-01-04", "0");
        // 2004-01-03 was a Saturday
        assert_strftime("%w", "2004-01-03", "6");
    }

    #[test]
    fn test_week_of_year() {
        // date.test 3.11.x
        assert_strftime("%W", "2003-10-31 12:34:56.432", "43");
        assert_strftime("%W", "2004-01-01", "00");
        assert_strftime("%W", "2004-01-02", "00");
        assert_strftime("%W", "2004-01-03", "00");
        assert_strftime("abc%Wxyz", "2004-01-04", "abc00xyz");
        assert_strftime("%W", "2004-01-05", "01");
        assert_strftime("%W", "2004-01-06", "01");
        assert_strftime("%W", "2004-07-18", "28");
        assert_strftime("%W", "2004-12-31", "52");
        assert_strftime("%W", "2007-12-31", "53");
        assert_strftime("%W", "2007-01-01", "01");
    }

    #[test]
    fn test_year() {
        assert_strftime("%Y", "2003-10-31 12:34:56.432", "2003");
        assert_strftime("%Y", "0100-01-01", "0100");
    }

    #[test]
    fn test_percent_literal() {
        assert_strftime("%%", "2003-10-31 12:34:56.432", "%");
        assert_strftime("100%%", "2003-10-31", "100%");
    }

    #[test]
    fn test_multi_specifier_format() {
        assert_strftime("%Y-%m-%d", "2003-10-31", "2003-10-31");
        assert_strftime("%Y-%m-%d %H:%M:%S", "2003-10-31 12:34:56", "2003-10-31 12:34:56");
        assert_strftime("%d/%m/%Y", "1995-03-15", "15/03/1995");
    }

    #[test]
    fn test_plain_text_passthrough() {
        assert_strftime("no specifiers here", "2003-10-31", "no specifiers here");
        assert_strftime("", "2003-10-31", "");
    }

    #[test]
    fn test_unknown_specifier_returns_null() {
        // date.test 3.14: %_ -> NULL
        assert_eq!(sf(&[text("%_"), text("2003-10-31 12:34:56.432")]), SqlValue::Null);
        // 3.44+ specifiers are out of scope for v1 -> NULL
        for spec in ["%e", "%F", "%I", "%k", "%p", "%P", "%R", "%T", "%u", "%G", "%g", "%U", "%V"] {
            assert_eq!(
                sf(&[text(spec), text("2003-10-31")]),
                SqlValue::Null,
                "{} should return NULL",
                spec
            );
        }
        // Trailing bare '%' -> NULL
        assert_eq!(sf(&[text("abc%"), text("2003-10-31")]), SqlValue::Null);
    }

    #[test]
    fn test_null_arguments() {
        assert_eq!(sf(&[SqlValue::Null, text("2003-10-31")]), SqlValue::Null);
        assert_eq!(sf(&[text("%Y"), SqlValue::Null]), SqlValue::Null);
        assert_eq!(sf(&[text("%Y"), text("2003-10-31"), SqlValue::Null]), SqlValue::Null);
    }

    #[test]
    fn test_invalid_timestring_returns_null() {
        assert_eq!(sf(&[text("%Y"), text("not-a-date")]), SqlValue::Null);
        assert_eq!(sf(&[text("%Y"), text("")]), SqlValue::Null);
    }

    #[test]
    fn test_modifiers_flow_through() {
        let result = sf(&[text("%Y"), text("1995-03-15"), text("+1 year")]);
        assert_eq!(result, text("1996"));

        let result = sf(&[text("%Y-%m-%d"), text("2003-10-31"), text("start of month")]);
        assert_eq!(result, text("2003-10-01"));

        let result = sf(&[text("%Y-%m-%d"), text("2003-10-31"), text("+2 days")]);
        assert_eq!(result, text("2003-11-02"));

        // Invalid modifier -> NULL
        assert_eq!(sf(&[text("%Y"), text("2003-10-31"), text("not a modifier")]), SqlValue::Null);
    }

    #[test]
    fn test_unixepoch_modifier() {
        let result =
            sf(&[text("%Y-%m-%d %H:%M:%S"), SqlValue::Integer(1067603696), text("unixepoch")]);
        assert_eq!(result, text("2003-10-31 12:34:56"));
    }

    #[test]
    fn test_date_and_timestamp_values() {
        let date = SqlValue::Date(vibesql_types::Date::new(1995, 3, 15).unwrap());
        assert_eq!(sf(&[text("%Y"), date]), text("1995"));

        let ts = SqlValue::Timestamp(vibesql_types::Timestamp::new(
            vibesql_types::Date::new(2003, 10, 31).unwrap(),
            vibesql_types::Time::new(12, 34, 56, 0).unwrap(),
        ));
        assert_eq!(sf(&[text("%Y-%m-%d %H:%M:%S"), ts]), text("2003-10-31 12:34:56"));
    }

    #[test]
    fn test_returns_text_type() {
        // SQLite's strftime returns TEXT, never a numeric type — this matters
        // for GROUP BY/ORDER BY semantics in TPC-H Q7/Q8/Q9
        let result = sf(&[text("%Y"), text("1995-03-15")]);
        assert!(matches!(result, SqlValue::Varchar(_)), "strftime must return TEXT");
    }

    #[test]
    fn test_now_default_timevalue() {
        // strftime('%Y') uses the current time (SQLite behavior)
        let result = sf(&[text("%Y")]);
        match result {
            SqlValue::Varchar(s) => {
                let year: i32 = s.parse().expect("year should be numeric");
                assert!((2020..=2200).contains(&year), "unexpected year {}", year);
            }
            other => panic!("expected TEXT, got {:?}", other),
        }
    }

    #[test]
    fn test_format_g16() {
        assert_eq!(format_g16(2452944.0242642593), "2452944.024264259");
        assert_eq!(format_g16(2440588.0), "2440588");
        assert_eq!(format_g16(2440587.5), "2440587.5");
        assert_eq!(format_g16(0.5), "0.5");
        assert_eq!(format_g16(0.0), "0");
    }
}
