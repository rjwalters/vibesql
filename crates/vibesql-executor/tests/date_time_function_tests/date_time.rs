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
    // NOTE: SQLite month-overflow rollover (2024-01-31 '+1 month' -> 2024-03-02)
    // is a known divergence tracked in #5309; not asserted here.
    assert_renders("date('2024-01-01', '-1 day')", "2023-12-31");
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
