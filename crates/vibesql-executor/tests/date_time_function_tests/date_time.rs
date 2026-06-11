//! Integration tests for the SQLite-compatible DATE and TIME scalar functions
//!
//! `date()` and `time()` share the time-value + modifiers machinery behind
//! `datetime()` and `strftime()`. Conformance values are taken from SQLite's
//! date.test (docs/reference/sqlite/test/date.test).

use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

fn execute_query(db: &Database, sql: &str) -> Vec<vibesql_storage::Row> {
    let stmt = Parser::parse_sql(sql).unwrap();
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let executor = SelectExecutor::new(db);
        executor.execute(&select_stmt).unwrap()
    } else {
        panic!("Expected SELECT statement");
    }
}

/// Execute `SELECT <expr>` and return the single result value
fn eval_scalar(sql_expr: &str) -> SqlValue {
    let db = Database::new();
    let rows = execute_query(&db, &format!("SELECT {}", sql_expr));
    assert_eq!(rows.len(), 1, "expected a single row for {}", sql_expr);
    rows[0].values[0].clone()
}

/// Assert the textual rendering of an expression result
fn assert_renders(sql_expr: &str, expected: &str) {
    let value = eval_scalar(sql_expr);
    assert_eq!(value.to_string(), expected, "for {}", sql_expr);
}

fn assert_null(sql_expr: &str) {
    let value = eval_scalar(sql_expr);
    assert!(matches!(value, SqlValue::Null), "{} should be NULL, got {:?}", sql_expr, value);
}

// ==================== date() ====================

#[test]
fn test_date_basic() {
    assert_renders("date('2024-01-01')", "2024-01-01");
    assert_renders("date('2003-10-22 12:34:00')", "2003-10-22");
}

#[test]
fn test_date_with_modifiers() {
    assert_renders("date('2024-01-01', '+1 day')", "2024-01-02");
    assert_renders("date('2024-03-15', 'start of month')", "2024-03-01");
    assert_renders("date('2024-01-15', '+1 month')", "2024-02-15");
    assert_renders("date('2024-01-01', '-1 day')", "2023-12-31");
}

#[test]
fn test_month_overflow_normalizes_like_sqlite() {
    // SQLite timediff1.test 1.x/2.x: day-of-month overflow rolls into the
    // following month rather than clamping (date.c computeJD normalization)
    assert_renders("datetime('2000-01-31','+1 month')", "2000-03-02 00:00:00");
    assert_renders("datetime('2004-01-29','+1 month')", "2004-02-29 00:00:00");
    assert_renders("datetime('2000-03-31','-1 month')", "2000-03-02 00:00:00");
    assert_renders("datetime('2000-02-29','+1 year')", "2001-03-01 00:00:00");
    assert_renders("datetime('2001-01-31','+1 month')", "2001-03-03 00:00:00");
    assert_renders("datetime('2001-03-31','-1 month')", "2001-03-03 00:00:00");
}

#[test]
fn test_floor_ceiling_modifiers_base_parse() {
    // SQLite date.test 19.1-19.32: 'floor' clamps a day-of-month overflow to
    // the last day of the nominal month; 'ceiling' (the default) rolls forward
    assert_renders("date('2000-01-31','floor')", "2000-01-31"); // 19.1: no overflow
    assert_renders("date('2000-02-31','floor')", "2000-02-29"); // 19.2a: leap year
    assert_renders("date('1999-02-31','floor')", "1999-02-28"); // 19.2b
    assert_renders("date('1900-02-31','floor')", "1900-02-28"); // 19.2c: century non-leap
    assert_renders("date('2000-04-31','floor')", "2000-04-30"); // 19.4
    assert_renders("date('2000-01-31','ceiling')", "2000-01-31"); // 19.21
    assert_renders("date('2000-02-31','ceiling')", "2000-03-02"); // 19.22a
    assert_renders("date('1999-02-31','ceiling')", "1999-03-03"); // 19.22b
    assert_renders("date('1900-02-31','ceiling')", "1900-03-03"); // 19.22c
    assert_renders("date('2000-04-31','ceiling')", "2000-05-01"); // 19.24
}

#[test]
fn test_floor_ceiling_modifiers_month_year_shift() {
    // SQLite date.test 19.40-19.47: ±N months/years recompute the overflow count
    assert_renders("date('2024-01-31','+1 month','ceiling')", "2024-03-02"); // 19.40
    assert_renders("date('2024-01-31','+1 month','floor')", "2024-02-29"); // 19.41
    assert_renders("date('2023-01-31','+1 month','ceiling')", "2023-03-03"); // 19.42
    assert_renders("date('2023-01-31','+1 month','floor')", "2023-02-28"); // 19.43
    assert_renders("date('2024-02-29','+1 year','ceiling')", "2025-03-01"); // 19.44
    assert_renders("date('2024-02-29','+1 year','floor')", "2025-02-28"); // 19.45
    assert_renders("date('2024-02-29','-110 years','ceiling')", "1914-03-01"); // 19.46
    assert_renders("date('2024-02-29','-110 years','floor')", "1914-02-28"); // 19.47
}

#[test]
fn test_floor_ceiling_modifiers_date_offset() {
    // SQLite date.test 19.48-19.53: ±YYYY-MM-DD offsets recompute the overflow count
    assert_renders("date('2024-02-29','-0110-00-00','floor')", "1914-02-28"); // 19.48
    assert_renders("date('2024-02-29','-0110-00-00','ceiling')", "1914-03-01"); // 19.49
    assert_renders("date('2000-08-31','+0023-06-00','floor')", "2024-02-29"); // 19.50
    assert_renders("date('2000-08-31','+0022-06-00','floor')", "2023-02-28"); // 19.51
    assert_renders("date('2000-08-31','+0023-06-00','ceiling')", "2024-03-02"); // 19.52
    assert_renders("date('2000-08-31','+0022-06-00','ceiling')", "2023-03-03"); // 19.53
}

#[test]
fn test_floor_is_consumed_by_subsequent_modifiers() {
    // 'floor' resolves the pending overflow; later duration shifts apply to
    // the clamped date (verified against SQLite 3.51:
    // date('2000-01-31','floor','+1 day') -> 2000-02-01)
    assert_renders("date('2000-01-31','floor','+1 day')", "2000-02-01");
    assert_renders("date('2000-02-31','floor','+1 day')", "2000-03-01");
    // A pure-duration shift CLEARS the pending overflow count (date.c resets
    // nFloor before the aXformType loop), so a later 'floor' is a no-op
    assert_renders("datetime('2000-02-31','+1 hour','floor')", "2000-03-02 01:00:00");
}

#[test]
fn test_duration_shift_resets_pending_floor() {
    // SQLite's parseModifier executes `p->nFloor = 0;` before the aXformType
    // loop, so every '±N unit' shift clears the pending day-of-month overflow
    // (month/year shifts then recompute it via computeFloor). Verified against
    // sqlite3 3.51.0.
    assert_renders("datetime('2000-02-31','+1 hour','floor')", "2000-03-02 01:00:00");
    assert_renders("date('2024-01-31','+1 month','+0 seconds','floor')", "2024-03-02");
    assert_renders("date('2024-01-31','+1 month','+1 day','floor')", "2024-03-03");
    // Contrast: the colon-form ±HH:MM modifier does NOT touch nFloor ...
    assert_renders("datetime('2024-01-31','+1 month','+02:00','floor')", "2024-02-29 02:00:00");
    // ... and neither does 'start of X'
    assert_renders("date('2024-01-31','+1 month','start of month','floor')", "2024-02-28");
}

#[test]
fn test_zero_argument_datetime_is_now() {
    // SQLite date.test 2.40: datetime() with no arguments means 'now'.
    // The harness cannot pin the clock, so just assert a non-NULL current
    // timestamp that matches datetime('now') to minute precision.
    let value = eval_scalar("datetime()");
    assert!(
        matches!(value, SqlValue::Timestamp(_)),
        "datetime() should return a timestamp, got {:?}",
        value
    );
    let in_range = eval_scalar(
        "datetime() BETWEEN datetime('now','-1 minute') AND datetime('now','+1 minute')",
    );
    assert_eq!(in_range, SqlValue::Boolean(true), "datetime() should be the current datetime");
}

#[test]
fn test_timezone_offset_suffix() {
    // SQLite date.test 5.x: TZ-offset/Z suffix converts the timestamp to UTC
    assert_renders("datetime('1994-04-16 14:00:00 +05:00')", "1994-04-16 09:00:00");
    assert_renders("datetime('1994-04-16 14:00:00 -05:15')", "1994-04-16 19:15:00");
    assert_renders("datetime('1994-04-16 05:00:00 +08:30')", "1994-04-15 20:30:00");
    assert_renders("datetime('1994-04-16 14:00:00 -11:55')", "1994-04-17 01:55:00");
    assert_renders("datetime('1994-04-16 14:00:00 -11:55  ')", "1994-04-17 01:55:00");
    assert_renders("datetime('1994-04-16T14:00:00Z')", "1994-04-16 14:00:00");
    assert_renders("datetime('1994-04-16 14:00:00z')", "1994-04-16 14:00:00");
    assert_renders("datetime('1994-04-16 14:00:00 Z')", "1994-04-16 14:00:00");
    assert_renders("datetime('1994-04-16 14:00:00z    ')", "1994-04-16 14:00:00");
    // Invalid offsets / trailing junk / combined Z+offset -> NULL
    assert_null("datetime('1994-04-16 14:00:00 -11:60')");
    assert_null("datetime('1994-04-16 14:00:00 -11:55 x')");
    assert_null("datetime('1994-04-16 14:00:00Zulu')");
    assert_null("datetime('1994-04-16 14:00:00Z +05:00')");
    assert_null("datetime('1994-04-16 14:00:00 +05:00 Z')");
}

#[test]
fn test_hh_mm_ss_modifiers() {
    // SQLite date.test 11.x: ±HH:MM[:SS] modifiers; no sign means plus
    assert_renders("datetime('2004-02-28 20:00:00', '-01:20:30')", "2004-02-28 18:39:30");
    assert_renders("datetime('2004-02-28 20:00:00', '+12:30:00')", "2004-02-29 08:30:00");
    assert_renders("datetime('2004-02-28 20:00:00', '+12:30')", "2004-02-29 08:30:00");
    assert_renders("datetime('2004-02-28 20:00:00', '12:30')", "2004-02-29 08:30:00");
    assert_renders("datetime('2004-02-28 20:00:00', '-12:00')", "2004-02-28 08:00:00");
    assert_renders("datetime('2004-02-28 20:00:00', '-12:01')", "2004-02-28 07:59:00");
    assert_renders("datetime('2004-02-28 20:00:00', '12:01')", "2004-02-29 08:01:00");
    // Out-of-range minutes -> NULL
    assert_null("datetime('2004-02-28 20:00:00', '12:60')");
}

#[test]
fn test_auto_modifier() {
    // SQLite date3.test 2.x: numeric values in the julian-day range are
    // julian days; other in-range numerics are unix timestamps
    assert_renders("datetime(2440587.5, 'auto')", "1970-01-01 00:00:00");
    assert_renders("datetime(2440615.7475463, 'auto')", "1970-01-29 05:56:28");
    assert_renders("datetime(-1, 'auto')", "1969-12-31 23:59:59");
    assert_renders("datetime(5373485, 'auto')", "1970-03-04 04:38:05");
    assert_renders("datetime(253402300799, 'auto')", "9999-12-31 23:59:59");
    // Out of range for both interpretations -> NULL
    assert_null("datetime(-210866760001, 'auto')");
    assert_null("datetime(253402300800, 'auto')");
    // No-op for text values
    assert_renders("date('2022-01-29', 'auto')", "2022-01-29");
    // Only valid as the first modifier
    assert_null("datetime(2459607.05, '+1 hour', 'auto')");
}

#[test]
fn test_julianday_modifier() {
    // SQLite date3.test 4.x: 'julianday' forces julian-day interpretation
    // and is only valid immediately after a numeric time value
    assert_renders("datetime(2459607, 'julianday')", "2022-01-27 12:00:00");
    assert_renders("datetime('2459607', 'julianday')", "2022-01-27 12:00:00");
    assert_null("datetime(2459607, '+1 hour', 'julianday')");
    assert_null("datetime('2022-01-27', 'julianday')");
}

#[test]
fn test_julian_day_range_bounds() {
    // SQLite date.test 16.x/17.x: modifiers that push the result outside the
    // valid julian-day range yield NULL
    assert_renders("datetime(0, '+464269060799 seconds')", "9999-12-31 23:59:59");
    assert_null("datetime(0, '+464269060800 seconds')");
    assert_null("datetime(0, '+5373485 days')");
    assert_null("datetime(0, '+176546 months')");
    assert_null("datetime(0, '+14713 years')");
    assert_null("datetime(5373484, '-5373485 days')");
    assert_null("datetime(37, 'start of year')");
    assert_renders("datetime(38, 'start of year')", "-4712-01-01 00:00:00");
}

#[test]
fn test_date_julian_day_input() {
    // SQLite date.test date5/jd cases: numeric input is a Julian Day number
    assert_renders("date(2451545.0)", "2000-01-01");
    assert_renders("date('2451545.0')", "2000-01-01");
    assert_renders("date(2440587.5)", "1970-01-01");
}

#[test]
fn test_date_now_returns_a_date() {
    match eval_scalar("date('now')") {
        SqlValue::Date(_) => {}
        other => panic!("date('now') should return a Date, got {:?}", other),
    }
    // Omitted time-value defaults to 'now' in SQLite
    match eval_scalar("date()") {
        SqlValue::Date(_) => {}
        other => panic!("date() should return a Date, got {:?}", other),
    }
}

#[test]
fn test_date_null_and_invalid_input() {
    assert_null("date(NULL)");
    assert_null("date('bogus')");
    assert_null("date('2024-01-01', 'bogus modifier')");
}

// ==================== time() ====================

#[test]
fn test_time_basic() {
    assert_renders("time('12:00:00')", "12:00:00");
    assert_renders("time('2003-10-22 12:34:56')", "12:34:56");
}

#[test]
fn test_time_truncates_fractional_seconds() {
    // SQLite: time('12:34:56.43') is '12:34:56' (no fractional part)
    assert_renders("time('12:34:56.43')", "12:34:56");
    assert_renders("time('2003-10-31 12:34:56.432')", "12:34:56");
}

#[test]
fn test_time_with_modifiers() {
    assert_renders("time('12:00:00', '+1 hour')", "13:00:00");
    assert_renders("time('2024-01-01 23:30:00', '+45 minutes')", "00:15:00");
    assert_renders("time('2024-01-01 12:34:56', 'start of day')", "00:00:00");
}

#[test]
fn test_time_now_returns_a_time() {
    match eval_scalar("time('now')") {
        SqlValue::Time(_) => {}
        other => panic!("time('now') should return a Time, got {:?}", other),
    }
}

#[test]
fn test_time_null_and_invalid_input() {
    assert_null("time(NULL)");
    assert_null("time('bogus')");
}

// ==================== typed literals & column refs still work ====================

#[test]
fn test_typed_literals_still_parse() {
    assert_renders("DATE '2024-01-01'", "2024-01-01");
    assert_renders("TIME '12:00:00'", "12:00:00");
}

#[test]
fn test_date_time_as_column_names_still_resolve() {
    use vibesql_executor::{CreateTableExecutor, InsertExecutor};

    let mut db = Database::new();

    let create = Parser::parse_sql("CREATE TABLE t (date VARCHAR(20), time VARCHAR(20))").unwrap();
    if let vibesql_ast::Statement::CreateTable(stmt) = create {
        CreateTableExecutor::execute(&stmt, &mut db).unwrap();
    } else {
        panic!("Expected CREATE TABLE");
    }

    let insert = Parser::parse_sql("INSERT INTO t VALUES ('d-value', 't-value')").unwrap();
    if let vibesql_ast::Statement::Insert(stmt) = insert {
        InsertExecutor::execute(&mut db, &stmt).unwrap();
    } else {
        panic!("Expected INSERT");
    }

    let rows = execute_query(&db, "SELECT date, time FROM t");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values[0], SqlValue::Varchar("d-value".into()));
    assert_eq!(rows[0].values[1], SqlValue::Varchar("t-value".into()));
}

// ==================== both parser paths ====================

#[test]
fn test_date_function_via_insert_recursive_descent_parser() {
    use vibesql_executor::{CreateTableExecutor, InsertExecutor};

    let mut db = Database::new();

    let create = Parser::parse_sql("CREATE TABLE t2 (d DATE)").unwrap();
    if let vibesql_ast::Statement::CreateTable(stmt) = create {
        CreateTableExecutor::execute(&stmt, &mut db).unwrap();
    } else {
        panic!("Expected CREATE TABLE");
    }

    // INSERT goes through the recursive-descent parser
    let insert = Parser::parse_sql("INSERT INTO t2 VALUES (date('2024-01-01', '+1 day'))").unwrap();
    if let vibesql_ast::Statement::Insert(stmt) = insert {
        InsertExecutor::execute(&mut db, &stmt).unwrap();
    } else {
        panic!("Expected INSERT");
    }

    let rows = execute_query(&db, "SELECT d FROM t2");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values[0].to_string(), "2024-01-02");
}

#[test]
fn test_date_function_via_arena_parser() {
    // parse_with_arena_fallback routes SELECT through the arena parser first
    let stmt = vibesql_parser::parse_with_arena_fallback("SELECT date('2024-01-01', '+1 day')")
        .expect("arena path should parse date() call");
    if let vibesql_ast::Statement::Select(select_stmt) = stmt {
        let db = Database::new();
        let executor = SelectExecutor::new(&db);
        let rows = executor.execute(&select_stmt).unwrap();
        assert_eq!(rows[0].values[0].to_string(), "2024-01-02");
    } else {
        panic!("Expected SELECT statement");
    }
}
