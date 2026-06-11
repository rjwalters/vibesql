//! SQLite-compatible DATE and TIME scalar functions
//!
//! `date(time-value, modifier, ...)` and `time(time-value, modifier, ...)`
//! share the SQLite time-value + modifiers model implemented by
//! `current::resolve_time_value` (the same machinery behind `datetime()`,
//! `strftime()`, `julianday()`, and `unixepoch()`).
//!
//! - `date()` returns the date component (SQLite renders `YYYY-MM-DD`)
//! - `time()` returns the time component truncated to whole seconds (SQLite's
//!   `time('12:34:56.43')` is `12:34:56`)
//!
//! Both return NULL for NULL/unparseable input or invalid modifiers, and treat
//! an omitted time-value as 'now' (matching SQLite).
//!
//! SQLite Reference: https://www.sqlite.org/lang_datefunc.html

use chrono::{Datelike, Timelike};
use vibesql_types::SqlValue;

use super::current::resolve_time_value;
use crate::{errors::ExecutorError, evaluator::SchemaExprContext};

/// DATE - Return the date component of a time value
///
/// `date(time-value, modifier, modifier, ...)`
pub fn date(args: &[SqlValue], ctx: SchemaExprContext) -> Result<SqlValue, ExecutorError> {
    let dt = match resolve_time_value(args, "DATE", ctx)? {
        Some(dt) => dt,
        None => return Ok(SqlValue::Null),
    };

    let date = vibesql_types::Date::new(dt.year(), dt.month() as u8, dt.day() as u8)
        .map_err(|e| ExecutorError::UnsupportedFeature(format!("Invalid date: {}", e)))?;
    Ok(SqlValue::Date(date))
}

/// TIME - Return the time-of-day component of a time value
///
/// `time(time-value, modifier, modifier, ...)`
///
/// Truncates to whole seconds: SQLite's `time()` renders `HH:MM:SS` without a
/// fractional part, while `vibesql_types::Time` Display prints fractional
/// seconds when nonzero - so nanoseconds must not be carried through.
pub fn time(args: &[SqlValue], ctx: SchemaExprContext) -> Result<SqlValue, ExecutorError> {
    let dt = match resolve_time_value(args, "TIME", ctx)? {
        Some(dt) => dt,
        None => return Ok(SqlValue::Null),
    };

    let time = vibesql_types::Time::new(dt.hour() as u8, dt.minute() as u8, dt.second() as u8, 0)
        .map_err(|e| ExecutorError::UnsupportedFeature(format!("Invalid time: {}", e)))?;
    Ok(SqlValue::Time(time))
}
