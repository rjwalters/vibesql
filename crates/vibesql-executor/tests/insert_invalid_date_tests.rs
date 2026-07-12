//! Regression tests for issue #6022: calendar-invalid dates must be rejected
//! at INSERT / coercion time with a clean, per-row SQL error — not silently
//! accepted and later blown up inside DATE_ADD / DATEDIFF as a
//! statement-aborting error.
//!
//! `Date::new()` (in `vibesql-types`) is the single construction choke point and
//! now validates calendar validity (days-per-month + leap years). These tests
//! confirm the fix surfaces correctly through the full INSERT path
//! (`insert/validation.rs` -> `s.parse::<Date>()` -> `FromStr` -> `Date::new()`).

use vibesql_executor::{CreateTableExecutor, InsertExecutor};
use vibesql_parser::Parser;
use vibesql_storage::Database;

/// Execute a single statement, returning the `Result` so the test can inspect
/// errors instead of panicking.
fn exec(db: &mut Database, sql: &str) -> Result<(), String> {
    let stmt = Parser::parse_sql(sql).map_err(|e| format!("parse error: {e:?}"))?;
    match stmt {
        vibesql_ast::Statement::CreateTable(s) => {
            CreateTableExecutor::execute(&s, db).map_err(|e| format!("{e:?}")).map(|_| ())
        }
        vibesql_ast::Statement::Insert(s) => {
            InsertExecutor::execute(db, &s).map_err(|e| format!("{e:?}")).map(|_| ())
        }
        other => panic!("unexpected statement: {other:?}"),
    }
}

fn setup(db: &mut Database) {
    exec(db, "CREATE TABLE t (d DATE)").expect("create table");
}

#[test]
fn insert_feb30_is_rejected_at_insert_time() {
    let mut db = Database::new();
    setup(&mut db);

    let result = exec(&mut db, "INSERT INTO t (d) VALUES ('2024-02-30')");
    assert!(result.is_err(), "Feb 30 must be rejected at INSERT time, got: {result:?}");
    // The error is a clean parse/coercion error, not a deferred DATE_ADD crash.
    let msg = result.unwrap_err();
    assert!(
        msg.contains("2024-02-30") || msg.to_lowercase().contains("date"),
        "error should reference the offending date value, got: {msg}"
    );
}

#[test]
fn insert_feb29_non_leap_year_is_rejected() {
    let mut db = Database::new();
    setup(&mut db);

    // 2023 is not a leap year.
    let result = exec(&mut db, "INSERT INTO t (d) VALUES ('2023-02-29')");
    assert!(result.is_err(), "Feb 29 in non-leap 2023 must be rejected, got: {result:?}");
}

#[test]
fn insert_apr31_is_rejected() {
    let mut db = Database::new();
    setup(&mut db);

    // April has 30 days.
    let result = exec(&mut db, "INSERT INTO t (d) VALUES ('2024-04-31')");
    assert!(result.is_err(), "Apr 31 must be rejected, got: {result:?}");
}

#[test]
fn insert_valid_leap_day_succeeds() {
    let mut db = Database::new();
    setup(&mut db);

    // 2024 is a leap year: Feb 29 is valid and must still be accepted.
    let result = exec(&mut db, "INSERT INTO t (d) VALUES ('2024-02-29')");
    assert!(result.is_ok(), "valid leap day must be accepted, got: {result:?}");
}
