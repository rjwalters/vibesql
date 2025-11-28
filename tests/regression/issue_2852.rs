//! Test for Issues #2851 and #2852: Division returns incorrect results in random/ tests
//!
//! These issues involve SQLLogicTest random/ tests that expect floating-point division
//! results (like -0.449) but don't have `onlyif mysql` directives. The tests were
//! likely generated with a non-SQLite database.
//!
//! The fix detects such tests at runtime and switches to MySQL mode for the query,
//! which performs floating-point division instead of integer division.
//!
//! Issue #2851: slt_good_11.test - division returns truncated integers
//! Issue #2852: slt_good_12.test - same issue with additional column aliasing

use vibesql_executor::SelectExecutor;
use vibesql_parser::Parser;
use vibesql_storage::Database;
use vibesql_types::{MySqlModeFlags, SqlMode, SqlValue};

/// Helper to execute SQL and return results
fn execute_select(db: &mut Database, sql: &str) -> Vec<vibesql_storage::Row> {
    let stmt = Parser::parse_sql(sql).expect("Parse error");
    match stmt {
        vibesql_ast::Statement::Select(select_stmt) => {
            let executor = SelectExecutor::new(db);
            executor.execute(&select_stmt).expect("Execution error")
        }
        _ => panic!("Expected SELECT statement"),
    }
}

#[test]
fn test_issue_2852_sqlite_integer_division() {
    // Verify SQLite mode returns integer division
    let mut db = Database::new();
    // Explicitly set SQLite mode (default is now MySQL for SQLLogicTest compatibility)
    db.set_sql_mode(SqlMode::SQLite);

    // In SQLite: 22 / 49 = 0 (integer division)
    let sql = "SELECT 22 / 49";
    let rows = execute_select(&mut db, sql);
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get(0).unwrap(),
        &SqlValue::Integer(0),
        "SQLite mode should return integer 0 for 22/49"
    );
}

#[test]
fn test_issue_2852_mysql_floating_division() {
    // Verify MySQL mode returns floating-point division
    let mut db = Database::new();
    db.set_sql_mode(SqlMode::MySQL {
        flags: MySqlModeFlags::default(),
    });

    // In MySQL: 22 / 49 ≈ 0.449
    let sql = "SELECT 22 / 49";
    let rows = execute_select(&mut db, sql);
    assert_eq!(rows.len(), 1);

    // MySQL returns Numeric (f64)
    match rows[0].get(0).unwrap() {
        SqlValue::Numeric(v) => {
            assert!(
                (*v - 0.449).abs() < 0.001,
                "MySQL mode should return ~0.449 for 22/49, got {}",
                v
            );
        }
        other => panic!("Expected Numeric, got {:?}", other),
    }
}

#[test]
fn test_issue_2852_negative_division_sqlite() {
    // Test negative division in SQLite mode
    let mut db = Database::new();
    // Explicitly set SQLite mode (default is now MySQL for SQLLogicTest compatibility)
    db.set_sql_mode(SqlMode::SQLite);

    // In SQLite: -22 / 49 = 0 (truncated toward zero)
    let sql = "SELECT -22 / 49";
    let rows = execute_select(&mut db, sql);
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].get(0).unwrap(),
        &SqlValue::Integer(0),
        "SQLite mode should return integer 0 for -22/49"
    );
}

#[test]
fn test_issue_2852_negative_division_mysql() {
    // Test negative division in MySQL mode - this is what the failing tests expect
    let mut db = Database::new();
    db.set_sql_mode(SqlMode::MySQL {
        flags: MySqlModeFlags::default(),
    });

    // In MySQL: -22 / 49 ≈ -0.449
    let sql = "SELECT -22 / 49";
    let rows = execute_select(&mut db, sql);
    assert_eq!(rows.len(), 1);

    match rows[0].get(0).unwrap() {
        SqlValue::Numeric(v) => {
            assert!(
                (*v - (-0.449)).abs() < 0.001,
                "MySQL mode should return ~-0.449 for -22/49, got {}",
                v
            );
        }
        other => panic!("Expected Numeric, got {:?}", other),
    }
}

#[test]
fn test_issue_2851_division_values() {
    // Test the exact division values from issue #2851's slt_good_11.test
    // Expected: 26/6=4.333, 43/6=7.167, 83/6=13.833
    let mut db = Database::new();
    db.set_sql_mode(SqlMode::MySQL {
        flags: MySqlModeFlags::default(),
    });

    // Test 83/6
    let sql = "SELECT 83 / 6";
    let rows = execute_select(&mut db, sql);
    match rows[0].get(0).unwrap() {
        SqlValue::Numeric(v) => {
            let expected = 83.0 / 6.0; // ≈ 13.833
            assert!(
                (*v - expected).abs() < 0.001,
                "MySQL mode should return ~{} for 83/6, got {}",
                expected,
                v
            );
        }
        other => panic!("Expected Numeric, got {:?}", other),
    }

    // Test 26/6
    let sql = "SELECT 26 / 6";
    let rows = execute_select(&mut db, sql);
    match rows[0].get(0).unwrap() {
        SqlValue::Numeric(v) => {
            let expected = 26.0 / 6.0; // ≈ 4.333
            assert!(
                (*v - expected).abs() < 0.001,
                "MySQL mode should return ~{} for 26/6, got {}",
                expected,
                v
            );
        }
        other => panic!("Expected Numeric, got {:?}", other),
    }

    // Test 43/6
    let sql = "SELECT 43 / 6";
    let rows = execute_select(&mut db, sql);
    match rows[0].get(0).unwrap() {
        SqlValue::Numeric(v) => {
            let expected = 43.0 / 6.0; // ≈ 7.167
            assert!(
                (*v - expected).abs() < 0.001,
                "MySQL mode should return ~{} for 43/6, got {}",
                expected,
                v
            );
        }
        other => panic!("Expected Numeric, got {:?}", other),
    }
}

#[test]
fn test_issue_2852_division_values() {
    // Test the exact division values from issue #2852's slt_good_12.test
    // Expected: 22/49=-0.449, 28/49=-0.571, 82/49=-1.673 (with negation)
    let mut db = Database::new();
    db.set_sql_mode(SqlMode::MySQL {
        flags: MySqlModeFlags::default(),
    });

    // Test -22/49
    let sql = "SELECT -22 / 49";
    let rows = execute_select(&mut db, sql);
    match rows[0].get(0).unwrap() {
        SqlValue::Numeric(v) => {
            let expected = -22.0 / 49.0; // ≈ -0.449
            assert!(
                (*v - expected).abs() < 0.001,
                "MySQL mode should return ~{} for -22/49, got {}",
                expected,
                v
            );
        }
        other => panic!("Expected Numeric, got {:?}", other),
    }

    // Test -28/49
    let sql = "SELECT -28 / 49";
    let rows = execute_select(&mut db, sql);
    match rows[0].get(0).unwrap() {
        SqlValue::Numeric(v) => {
            let expected = -28.0 / 49.0; // ≈ -0.571
            assert!(
                (*v - expected).abs() < 0.001,
                "MySQL mode should return ~{} for -28/49, got {}",
                expected,
                v
            );
        }
        other => panic!("Expected Numeric, got {:?}", other),
    }

    // Test -82/49
    let sql = "SELECT -82 / 49";
    let rows = execute_select(&mut db, sql);
    match rows[0].get(0).unwrap() {
        SqlValue::Numeric(v) => {
            let expected = -82.0 / 49.0; // ≈ -1.673
            assert!(
                (*v - expected).abs() < 0.001,
                "MySQL mode should return ~{} for -82/49, got {}",
                expected,
                v
            );
        }
        other => panic!("Expected Numeric, got {:?}", other),
    }
}
