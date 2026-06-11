//! SQLite-compatible date/time scalar functions: JULIANDAY, UNIXEPOCH, TIMEDIFF
//!
//! These functions share the SQLite time-value + modifiers model implemented by
//! `current::resolve_time_value` (the same machinery behind `datetime()` and
//! `strftime()`).
//!
//! - `julianday(time-value, modifiers...)` — fractional Julian Day number as REAL
//! - `unixepoch(time-value, modifiers...)` — seconds since the Unix epoch as
//!   INTEGER, or as REAL (millisecond precision) with the `'subsec'` modifier
//! - `timediff(A, B)` — TEXT in `±YYYY-MM-DD HH:MM:SS.SSS` format such that
//!   `datetime(B, timediff(A, B)) == datetime(A)`
//!
//! SQLite Reference: https://www.sqlite.org/lang_datefunc.html

use chrono::{Datelike, NaiveDateTime, Timelike};
use vibesql_types::SqlValue;

use super::current::{naive_to_ijd_ms, resolve_time_value};
use crate::errors::ExecutorError;

/// JULIANDAY - Return the fractional Julian Day number of a time value as REAL
///
/// `julianday(time-value, modifier, modifier, ...)`
///
/// SQLite computes `iJD / 86400000.0` where iJD is milliseconds since the Julian
/// Day origin (-4713-11-24 12:00:00). Returns NULL for NULL/unparseable input or
/// invalid modifiers.
pub fn julianday(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if has_misplaced_base_modifier(args) {
        return Ok(SqlValue::Null);
    }
    // 'subsec' is accepted but has no effect: julianday is always full precision
    let (filtered, _subsec) = strip_subsec_modifiers(args);
    let dt = match resolve_time_value(&filtered, "JULIANDAY")? {
        Some(dt) => dt,
        None => return Ok(SqlValue::Null),
    };
    Ok(SqlValue::Real(naive_to_ijd_ms(&dt) as f64 / 86_400_000.0))
}

/// Check whether a base-interpreting modifier ('unixepoch', 'julianday', or
/// 'auto') appears at any position other than the first modifier slot
/// (args[1]) in the ORIGINAL argument list.
///
/// In SQLite these modifiers are only valid when they immediately follow the
/// time value; any other position yields NULL. `resolve_time_value` enforces
/// that rule, but only on the list it receives — `strip_subsec_modifiers`
/// would otherwise reposition a later 'unixepoch' into first place (e.g.
/// `unixepoch(x,'subsec','unixepoch')`, NULL in SQLite). So callers must run
/// this check on the original positions BEFORE stripping 'subsec'.
///
/// Long-term alternative (Option B in issue #5315): drop the stripping
/// entirely and handle 'subsec'/'subsecond' as a flag-setting no-op inside
/// `resolve_time_value`'s modifier loop, surfacing the flag to callers. That
/// is the design that eventually enables 'subsec' OUTPUT for datetime()/time()
/// — but until that lands, datetime()/time() must keep rejecting 'subsec'.
fn has_misplaced_base_modifier(args: &[SqlValue]) -> bool {
    args.iter().skip(2).any(|arg| {
        if let SqlValue::Varchar(s) | SqlValue::Character(s) = arg {
            let t = s.trim();
            t.eq_ignore_ascii_case("unixepoch")
                || t.eq_ignore_ascii_case("julianday")
                || t.eq_ignore_ascii_case("auto")
        } else {
            false
        }
    })
}

/// Remove 'subsec'/'subsecond' modifiers from an argument list, returning the
/// filtered arguments and whether any were present. In SQLite, 'subsec' only
/// changes output precision and is order-transparent with respect to ordinary
/// modifiers such as '+1 day'.
///
/// CAUTION: 'subsec' still occupies a modifier position like any other, so it
/// does NOT exempt 'unixepoch'/'julianday'/'auto' from the must-be-first rule.
/// Because stripping repositions later modifiers, callers must reject
/// misplaced base-interpreting modifiers on the original argument list first
/// (see `has_misplaced_base_modifier`).
fn strip_subsec_modifiers(args: &[SqlValue]) -> (Vec<SqlValue>, bool) {
    let mut subsec = false;
    let mut filtered: Vec<SqlValue> = Vec::with_capacity(args.len());
    for (idx, arg) in args.iter().enumerate() {
        if idx > 0 {
            if let SqlValue::Varchar(s) | SqlValue::Character(s) = arg {
                let t = s.trim();
                if t.eq_ignore_ascii_case("subsec") || t.eq_ignore_ascii_case("subsecond") {
                    subsec = true;
                    continue;
                }
            }
        }
        filtered.push(arg.clone());
    }
    (filtered, subsec)
}

/// UNIXEPOCH - Return seconds since 1970-01-01 00:00:00 UTC
///
/// `unixepoch(time-value, modifier, modifier, ...)`
///
/// Returns INTEGER seconds (floor division, so fractional seconds round toward
/// negative infinity, matching SQLite). With the `'subsec'` (or `'subsecond'`)
/// modifier, returns REAL with millisecond precision.
pub fn unixepoch(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if has_misplaced_base_modifier(args) {
        return Ok(SqlValue::Null);
    }
    // Strip 'subsec'/'subsecond' modifiers; they only change the output type
    let (filtered, subsec) = strip_subsec_modifiers(args);

    let dt = match resolve_time_value(&filtered, "UNIXEPOCH")? {
        Some(dt) => dt,
        None => return Ok(SqlValue::Null),
    };

    let unix_ms = dt.and_utc().timestamp_millis();
    if subsec {
        Ok(SqlValue::Real(unix_ms as f64 / 1000.0))
    } else {
        Ok(SqlValue::Integer(unix_ms.div_euclid(1000)))
    }
}

/// TIMEDIFF - Return the difference between two time values as TEXT
///
/// `timediff(A, B)` returns a string in the format `±YYYY-MM-DD HH:MM:SS.SSS`
/// which is the amount of time that must be added to B in order to reach A,
/// i.e. the invariant `datetime(B, timediff(A, B)) == datetime(A)` holds.
///
/// Years and months are counted by calendar-aware stepping (mirroring SQLite's
/// `timediffFunc` in date.c), so the day component is month-aware, not a fixed
/// 30-day approximation. Returns NULL if either argument is NULL/unparseable.
pub fn timediff(args: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    if args.len() != 2 {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "TIMEDIFF requires exactly 2 arguments, got {}",
            args.len()
        )));
    }

    let d1 = match resolve_time_value(&args[0..1], "TIMEDIFF")? {
        Some(dt) => dt,
        None => return Ok(SqlValue::Null),
    };
    // Resolve 'now' only once so timediff('now','now') is exactly zero
    let d2 = if is_now(&args[0]) && is_now(&args[1]) {
        d1
    } else {
        match resolve_time_value(&args[1..2], "TIMEDIFF")? {
            Some(dt) => dt,
            None => return Ok(SqlValue::Null),
        }
    };

    match timediff_string(&d1, &d2) {
        Some(s) => Ok(SqlValue::Varchar(s.into())),
        None => Ok(SqlValue::Null),
    }
}

fn is_now(value: &SqlValue) -> bool {
    matches!(value, SqlValue::Varchar(s) | SqlValue::Character(s) if s.eq_ignore_ascii_case("now"))
}

/// Milliseconds within the day (time-of-day component), truncated to ms.
fn time_of_day_ms(dt: &NaiveDateTime) -> i64 {
    let time = dt.time();
    time.num_seconds_from_midnight() as i64 * 1000 + (time.nanosecond() / 1_000_000).min(999) as i64
}

/// Compose a Unix-epoch-milliseconds instant from (year, month 1-12, day-of-month,
/// time-of-day ms). A day-of-month past the end of the month rolls over into the
/// following month, matching SQLite's `computeJD` normalization (e.g. Feb 31 ->
/// Mar 2/3).
fn compose_unix_ms(year: i64, month: i64, day: u32, tod_ms: i64) -> Option<i64> {
    debug_assert!((1..=12).contains(&month));
    let year = i32::try_from(year).ok()?;
    let base = chrono::NaiveDate::from_ymd_opt(year, month as u32, 1)?;
    let base_ms = base.and_hms_opt(0, 0, 0)?.and_utc().timestamp_millis();
    Some(base_ms + (day as i64 - 1) * 86_400_000 + tod_ms)
}

/// Core of `timediff()`: mirrors SQLite's `timediffFunc` algorithm.
///
/// The smaller value is stepped toward the larger one a whole calendar month at
/// a time (counting years and months); the remaining millisecond difference is
/// rendered as days, hours, minutes, seconds, and milliseconds.
fn timediff_string(d1: &NaiveDateTime, d2: &NaiveDateTime) -> Option<String> {
    let ms1 = d1.and_utc().timestamp_millis();
    let ms2 = d2.and_utc().timestamp_millis();

    let day = d2.day();
    let tod = time_of_day_ms(d2);

    let (sign, y, m, diff_ms) = if ms1 >= ms2 {
        let mut y = d1.year() as i64 - d2.year() as i64;
        let mut m = d1.month() as i64 - d2.month() as i64;
        if m < 0 {
            y -= 1;
            m += 12;
        }
        // Move d2 to d1's year/month, keeping its day-of-month and time
        let mut cy = d1.year() as i64;
        let mut cm = d1.month() as i64;
        let mut cur = compose_unix_ms(cy, cm, day, tod)?;
        // Back off whole months while the stepped value exceeds d1
        while ms1 < cur {
            m -= 1;
            if m < 0 {
                m = 11;
                y -= 1;
            }
            cm -= 1;
            if cm < 1 {
                cm = 12;
                cy -= 1;
            }
            cur = compose_unix_ms(cy, cm, day, tod)?;
        }
        ('+', y, m, ms1 - cur)
    } else {
        let mut y = d2.year() as i64 - d1.year() as i64;
        let mut m = d2.month() as i64 - d1.month() as i64;
        if m < 0 {
            y -= 1;
            m += 12;
        }
        // Move d2 to d1's year/month, keeping its day-of-month and time
        let mut cy = d1.year() as i64;
        let mut cm = d1.month() as i64;
        let mut cur = compose_unix_ms(cy, cm, day, tod)?;
        // Step forward whole months while the stepped value is below d1
        while ms1 > cur {
            m -= 1;
            if m < 0 {
                m = 11;
                y -= 1;
            }
            cm += 1;
            if cm > 12 {
                cm = 1;
                cy += 1;
            }
            cur = compose_unix_ms(cy, cm, day, tod)?;
        }
        ('-', y, m, cur - ms1)
    };

    let days = diff_ms / 86_400_000;
    let rem = diff_ms % 86_400_000;
    let hours = rem / 3_600_000;
    let rem = rem % 3_600_000;
    let minutes = rem / 60_000;
    let rem = rem % 60_000;
    let seconds = rem / 1000;
    let millis = rem % 1000;

    Some(format!(
        "{}{:04}-{:02}-{:02} {:02}:{:02}:{:02}.{:03}",
        sign, y, m, days, hours, minutes, seconds, millis
    ))
}

#[cfg(test)]
mod tests {
    use super::super::current::datetime;
    use super::*;

    fn text(s: &str) -> SqlValue {
        SqlValue::Varchar(s.into())
    }

    fn jd(args: &[SqlValue]) -> SqlValue {
        julianday(args).expect("julianday should not error")
    }

    fn ue(args: &[SqlValue]) -> SqlValue {
        unixepoch(args).expect("unixepoch should not error")
    }

    fn td(a: &str, b: &str) -> SqlValue {
        timediff(&[text(a), text(b)]).expect("timediff should not error")
    }

    fn assert_real(value: SqlValue, expected: f64) {
        match value {
            SqlValue::Real(r) => {
                assert!((r - expected).abs() < 1e-9, "expected {}, got {}", expected, r)
            }
            other => panic!("expected REAL {}, got {:?}", expected, other),
        }
    }

    // ---- julianday() (values verified against sqlite3 3.51.0) ----

    #[test]
    fn test_julianday_basic() {
        assert_real(jd(&[text("2000-01-01")]), 2451544.5);
        assert_real(jd(&[text("1970-01-01")]), 2440587.5);
        assert_real(jd(&[text("2024-01-01 12:00:00")]), 2460311.0);
    }

    #[test]
    fn test_julianday_with_modifier() {
        assert_real(jd(&[text("2024-01-01"), text("+1 day")]), 2460311.5);
    }

    #[test]
    fn test_julianday_numeric_passthrough() {
        assert_real(jd(&[SqlValue::Integer(2451545)]), 2451545.0);
        assert_real(jd(&[SqlValue::Real(2451544.5)]), 2451544.5);
    }

    #[test]
    fn test_julianday_fractional_seconds() {
        // strftime('%J', ...) renders this as 2452944.024264259 (16 sig digits)
        assert_real(jd(&[text("2003-10-31 12:34:56.432")]), 2452944.0242642593);
    }

    #[test]
    fn test_julianday_returns_real_type() {
        assert!(matches!(jd(&[text("2024-01-01")]), SqlValue::Real(_)));
    }

    #[test]
    fn test_julianday_null_and_invalid() {
        assert_eq!(jd(&[SqlValue::Null]), SqlValue::Null);
        assert_eq!(jd(&[text("not-a-date")]), SqlValue::Null);
        assert_eq!(jd(&[text("2024-01-01"), text("bogus")]), SqlValue::Null);
    }

    #[test]
    fn test_julianday_string_julian_day_input() {
        // Julian-day numeric *strings* are also accepted as time values
        assert_real(jd(&[text("2451545.0")]), 2451545.0);
    }

    // ---- unixepoch() (values verified against sqlite3 3.51.0) ----

    #[test]
    fn test_unixepoch_basic() {
        assert_eq!(ue(&[text("2024-01-01")]), SqlValue::Integer(1704067200));
        assert_eq!(ue(&[text("1970-01-01")]), SqlValue::Integer(0));
    }

    #[test]
    fn test_unixepoch_floors_fractional_seconds() {
        assert_eq!(ue(&[text("2024-01-01 00:00:00.500")]), SqlValue::Integer(1704067200));
        // Floor (toward negative infinity), not truncation toward zero
        assert_eq!(ue(&[text("1969-12-31 23:59:59.5")]), SqlValue::Integer(-1));
    }

    #[test]
    fn test_unixepoch_subsec() {
        assert_real(ue(&[text("2024-01-01 00:00:00.500"), text("subsec")]), 1704067200.5);
        assert!(matches!(ue(&[text("2024-01-01"), text("subsec")]), SqlValue::Real(_)));
        // 'subsecond' alias
        assert_real(ue(&[text("2024-01-01 00:00:00.250"), text("subsecond")]), 1704067200.25);
    }

    #[test]
    fn test_unixepoch_round_trip() {
        assert_eq!(
            ue(&[SqlValue::Integer(1704067200), text("unixepoch")]),
            SqlValue::Integer(1704067200)
        );
    }

    #[test]
    fn test_unixepoch_subsec_after_unixepoch_modifier() {
        assert_eq!(
            ue(&[SqlValue::Integer(1704067200), text("unixepoch"), text("subsec")]),
            SqlValue::Real(1704067200.0)
        );
    }

    #[test]
    fn test_unixepoch_null_and_invalid() {
        assert_eq!(ue(&[SqlValue::Null]), SqlValue::Null);
        assert_eq!(ue(&[text("not-a-date")]), SqlValue::Null);
    }

    // ---- 'unixepoch'/'julianday'/'auto' must-be-first rule with 'subsec'
    //      (issue #5315; all values verified against sqlite3 3.51.0) ----

    #[test]
    fn test_unixepoch_modifier_after_subsec_is_null() {
        // 'subsec' occupies a modifier position, so 'unixepoch' is no longer first
        assert_eq!(
            ue(&[SqlValue::Integer(1704067200), text("subsec"), text("unixepoch")]),
            SqlValue::Null
        );
        // Double 'subsec' before 'unixepoch' is just as invalid
        assert_eq!(
            ue(&[SqlValue::Integer(1704067200), text("subsec"), text("subsec"), text("unixepoch")]),
            SqlValue::Null
        );
        // 'subsecond' alias triggers the same rule
        assert_eq!(
            ue(&[SqlValue::Integer(1704067200), text("subsecond"), text("unixepoch")]),
            SqlValue::Null
        );
    }

    #[test]
    fn test_julianday_modifier_after_subsec_is_null() {
        assert_eq!(
            jd(&[SqlValue::Integer(2451545), text("subsec"), text("julianday")]),
            SqlValue::Null
        );
        // Valid ordering still works: 'julianday' first, 'subsec' after
        assert_real(
            jd(&[SqlValue::Integer(2451545), text("julianday"), text("subsec")]),
            2451545.0,
        );
    }

    #[test]
    fn test_auto_modifier_after_subsec_is_null() {
        assert_eq!(ue(&[SqlValue::Real(1.234), text("subsec"), text("auto")]), SqlValue::Null);
    }

    #[test]
    fn test_subsec_remains_order_transparent_for_ordinary_modifiers() {
        // 'subsec' before or after an ordinary modifier like '+1 day' is fine
        assert_real(
            ue(&[text("2024-01-01 00:00:00.5"), text("subsec"), text("+1 day")]),
            1704153600.5,
        );
        assert_real(
            ue(&[text("2024-01-01 00:00:00.5"), text("+1 day"), text("subsec")]),
            1704153600.5,
        );
    }

    #[test]
    fn test_misplaced_base_modifier_spelling_variants() {
        // Mixed case and whitespace padding are still recognized as misplaced
        assert_eq!(
            ue(&[SqlValue::Integer(1704067200), text("SubSec"), text("UNIXEPOCH")]),
            SqlValue::Null
        );
        assert_eq!(
            ue(&[SqlValue::Integer(1704067200), text("subsec"), text("  unixepoch ")]),
            SqlValue::Null
        );
        // 'subsec' as the time value itself still yields NULL (parse failure)
        assert_eq!(ue(&[text("subsec")]), SqlValue::Null);
    }

    // ---- timediff() (values verified against sqlite3 3.51.0) ----

    #[test]
    fn test_timediff_basic_days() {
        assert_eq!(td("2024-01-02", "2024-01-01"), text("+0000-00-01 00:00:00.000"));
        assert_eq!(td("2024-01-01", "2024-01-02"), text("-0000-00-01 00:00:00.000"));
    }

    #[test]
    fn test_timediff_whole_years_and_months() {
        assert_eq!(td("2025-01-01", "2024-01-01"), text("+0001-00-00 00:00:00.000"));
        assert_eq!(td("2024-03-31", "2024-01-31"), text("+0000-02-00 00:00:00.000"));
    }

    #[test]
    fn test_timediff_month_borrowing() {
        // NOT 1 month — month stepping borrows when the intermediate overshoots
        assert_eq!(td("2024-03-01", "2024-01-31"), text("+0000-00-30 00:00:00.000"));
        assert_eq!(td("2024-04-30", "2024-05-31"), text("-0000-01-01 00:00:00.000"));
    }

    #[test]
    fn test_timediff_leap_year() {
        assert_eq!(td("2024-03-01", "2024-02-28"), text("+0000-00-02 00:00:00.000"));
        assert_eq!(td("2023-03-01", "2023-02-28"), text("+0000-00-01 00:00:00.000"));
    }

    #[test]
    fn test_timediff_across_year_boundary() {
        assert_eq!(td("2023-02-15", "2022-12-31"), text("+0000-01-15 00:00:00.000"));
    }

    #[test]
    fn test_timediff_with_time_and_millis() {
        assert_eq!(
            td("2024-02-28 12:30:45.250", "2023-12-31 23:45:50.500"),
            text("+0000-01-27 12:44:54.750")
        );
        assert_eq!(
            td("2024-01-01 00:00:00", "2024-01-01 00:00:00.001"),
            text("-0000-00-00 00:00:00.001")
        );
    }

    #[test]
    fn test_timediff_now_now_is_zero() {
        assert_eq!(td("now", "now"), text("+0000-00-00 00:00:00.000"));
    }

    #[test]
    fn test_timediff_returns_text_type() {
        assert!(matches!(td("2024-01-02", "2024-01-01"), SqlValue::Varchar(_)));
    }

    #[test]
    fn test_timediff_null_and_invalid() {
        assert_eq!(timediff(&[SqlValue::Null, text("2024-01-01")]).unwrap(), SqlValue::Null);
        assert_eq!(timediff(&[text("2024-01-01"), SqlValue::Null]).unwrap(), SqlValue::Null);
        assert_eq!(td("garbage", "2024-01-01"), SqlValue::Null);
        assert_eq!(td("2024-01-01", "garbage"), SqlValue::Null);
    }

    #[test]
    fn test_timediff_wrong_arg_count_errors() {
        assert!(timediff(&[text("2024-01-01")]).is_err());
        assert!(timediff(&[text("a"), text("b"), text("c")]).is_err());
    }

    #[test]
    fn test_timediff_sqlite_timediff1_section3() {
        // timediff1.test 3.1-3.4
        assert_eq!(td("2000-03-02", "2000-01-31"), text("+0000-01-00 00:00:00.000"));
        assert_eq!(td("2000-01-31", "2000-03-02"), text("-0000-01-02 00:00:00.000"));
        assert_eq!(td("2000-03-02", "1999-01-31"), text("+0001-01-00 00:00:00.000"));
        assert_eq!(td("1999-01-31", "2000-03-02"), text("-0001-01-02 00:00:00.000"));
    }

    // ---- invariant: datetime(B, timediff(A,B)) == datetime(A) ----

    fn datetime_text(args: &[SqlValue]) -> String {
        match datetime(args).expect("datetime should not error") {
            SqlValue::Timestamp(ts) => ts.to_string(),
            other => panic!("expected timestamp, got {:?}", other),
        }
    }

    #[test]
    fn test_timediff_invariant_grid() {
        let dates = [
            "2000-01-01 00:00:00",
            "2000-02-29 13:00:00",
            "2001-02-28 23:59:59",
            "2001-03-31 15:15:00",
            "2004-05-01 23:59:59",
            "2008-01-01 01:59:00",
            "2023-12-31 23:45:50.500",
            "2024-01-31 00:00:00",
            "2024-02-28 12:30:45.250",
            "2024-03-01 00:00:00",
            "2024-05-31 00:00:00",
        ];
        for a in &dates {
            for b in &dates {
                let diff = match td(a, b) {
                    SqlValue::Varchar(s) => s.to_string(),
                    other => panic!("timediff({}, {}) returned {:?}", a, b, other),
                };
                let expected = datetime_text(&[text(a)]);
                let actual = datetime_text(&[text(b), text(&diff)]);
                assert_eq!(
                    actual, expected,
                    "datetime('{}', timediff('{}','{}') = '{}') should equal datetime('{}')",
                    b, a, b, diff, a
                );
            }
        }
    }

    // ---- strict SQLite time-string grammar (date.test 1.x) ----

    #[test]
    fn test_strict_date_string_grammar() {
        // date-1.14..1.17: field widths are exact, no leading '+'
        assert_eq!(jd(&[text("+2000-01-01")]), SqlValue::Null);
        assert_eq!(jd(&[text("200-01-01")]), SqlValue::Null);
        assert_eq!(jd(&[text("2000-1-01")]), SqlValue::Null);
        assert_eq!(jd(&[text("2000-01-1")]), SqlValue::Null);
        // date-1.27: seconds must be < 60
        assert_eq!(jd(&[text("2001-01-01 12:59:60")]), SqlValue::Null);
    }

    #[test]
    fn test_day_of_month_overflow_normalizes() {
        // date-1.12: 2003-02-31 normalizes to 2003-03-03 (SQLite computeJD)
        assert_real(jd(&[text("2003-02-31")]), 2452701.5);
    }

    #[test]
    fn test_t_separator_variants() {
        // date-1.18.x: 'T' may be surrounded by spaces
        assert_real(jd(&[text("2000-01-01T12:00:00")]), 2451545.0);
        assert_real(jd(&[text("2000-01-01 T12:00:00")]), 2451545.0);
        assert_real(jd(&[text("2000-01-01T 12:00:00")]), 2451545.0);
        assert_real(jd(&[text("2000-01-01 T 12:00:00")]), 2451545.0);
    }

    #[test]
    fn test_negative_year_input() {
        // The Julian Day origin
        assert_real(jd(&[text("-4713-11-24 12:00:00")]), 0.0);
        // date-18.4: julianday accepts (and ignores) 'subsec'
        assert_real(jd(&[text("-4713-11-24 13:40:48.864"), text("subsec")]), 0.07001);
    }

    // ---- fractional shifts and unixepoch numeric input ----

    #[test]
    fn test_fractional_year_shift() {
        // date-13.23 / 13.24
        assert_real(jd(&[SqlValue::Real(2454832.5), text("-1.5 years")]), 2454284.0);
        assert_real(jd(&[SqlValue::Real(2454832.5), text("+1.5 years")]), 2455380.0);
    }

    #[test]
    fn test_unixepoch_fractional_input() {
        // date-18.2 / 18.3
        assert_real(ue(&[text("1970-01-01T00:00:00.1"), text("subsec")]), 0.1);
        assert_real(ue(&[text("1970-01-01T00:00:00.2"), text("subsecond")]), 0.2);
        // date-18.1 relies on fractional numeric 'unixepoch' input keeping ms
        assert_real(ue(&[SqlValue::Real(1.234), text("unixepoch"), text("subsec")]), 1.234);
    }

    // ---- (+|-)YYYY-MM-DD HH:MM:SS.SSS modifiers (timediff1.test section 5) ----

    fn datetime_or_null(args: &[SqlValue]) -> Option<String> {
        match datetime(args).expect("datetime should not error") {
            SqlValue::Timestamp(ts) => Some(ts.to_string()),
            SqlValue::Null => None,
            other => panic!("expected timestamp or NULL, got {:?}", other),
        }
    }

    #[test]
    fn test_date_offset_modifier() {
        let base = text("2000-01-01");
        let case = |modifier: &str| datetime_or_null(&[base.clone(), text(modifier)]);

        // timediff-5-x (sqlite3-verified)
        assert_eq!(case("+0001-02-03"), Some("2001-03-04 00:00:00".into()));
        assert_eq!(case("+0001-02-03x"), None);
        assert_eq!(case("+0001-11-03"), Some("2001-12-04 00:00:00".into()));
        assert_eq!(case("+0001-12-03"), None); // MM limited to 0..11
        assert_eq!(case("+0001-02-30"), Some("2001-03-31 00:00:00".into()));
        assert_eq!(case("+0001-02-31"), None); // DD limited to 0..30
        assert_eq!(case("+0001-02-03 0"), None);
        assert_eq!(case("+0001-02-03 01"), None);
        assert_eq!(case("+0001-02-03 01:"), None);
        assert_eq!(case("+0001-02-03 01:0"), None);
        assert_eq!(case("+0001-02-03 01:02"), Some("2001-03-04 01:02:00".into()));
        assert_eq!(case("+0001-02-03 01:02:"), None);
        assert_eq!(case("+0001-02-03 01:02:0"), None);
        assert_eq!(case("+0001-02-03 01:02:03"), Some("2001-03-04 01:02:03".into()));
        assert_eq!(case("+0001-02-03 01:02:03."), None);
        assert_eq!(case("+0001-02-03 01:02:03.5"), Some("2001-03-04 01:02:03".into()));
        assert_eq!(case("+0001-02-03 01:02:03.500x"), None);
        assert_eq!(case("+0001-02-03 01:02:03.500 x"), None);

        // timediff1.test 1.10-1.13 (calendar normalization with full offsets)
        assert_eq!(
            datetime_or_null(&[text("1998-11-10"), text("+0001-03-19 12:34:56")]),
            Some("2000-02-29 12:34:56".into())
        );
        assert_eq!(
            datetime_or_null(&[text("2001-03-31"), text("-0001-01-00 06:10")]),
            Some("2000-03-01 17:50:00".into())
        );
    }

    // ---- regression: julian-day numeric input to datetime() ----

    #[test]
    fn test_datetime_julian_day_numeric_input() {
        assert_eq!(datetime_text(&[SqlValue::Real(2451544.5)]), "2000-01-01 00:00:00");
        assert_eq!(datetime_text(&[SqlValue::Real(2451545.0)]), "2000-01-01 12:00:00");
    }

    #[test]
    fn test_datetime_julian_day_string_input() {
        assert_eq!(datetime_text(&[text("2451545.0")]), "2000-01-01 12:00:00");
        assert_eq!(datetime_text(&[text("2440587.5")]), "1970-01-01 00:00:00");
    }
}
