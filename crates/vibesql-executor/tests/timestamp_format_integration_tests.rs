//! Integration tests for timestamp format support
//! Tests both parser (TIMESTAMP literals) and casting (string to timestamp conversion)
//! Issue #1187: Support multiple timestamp formats for automatic parsing

mod common;

use common::setup_timestamps_table as setup_test_table;
use vibesql_ast::Statement;
use vibesql_executor::{InsertExecutor, SelectExecutor};
use vibesql_parser::Parser;
use vibesql_storage::Database;

// ============================================================================
// Helper Functions
// ============================================================================

fn execute_insert(db: &mut Database, sql: &str) -> Result<usize, String> {
    let stmt = Parser::parse_sql(sql).map_err(|e| format!("Parse error: {:?}", e))?;

    match stmt {
        Statement::Insert(insert_stmt) => InsertExecutor::execute(db, &insert_stmt)
            .map_err(|e| format!("Execution error: {:?}", e)),
        other => Err(format!("Expected INSERT statement, got {:?}", other)),
    }
}

fn assert_insert_succeeds(db: &mut Database, insert_sql: &str) {
    let result = execute_insert(db, insert_sql);
    assert!(result.is_ok(), "Expected INSERT to succeed but got error: {:?}", result.err());
}

fn assert_insert_fails(db: &mut Database, insert_sql: &str) {
    let result = execute_insert(db, insert_sql);
    assert!(result.is_err(), "Expected INSERT to fail but it succeeded");
}

fn query_count(db: &Database, sql: &str) -> Result<usize, String> {
    let stmt = Parser::parse_sql(sql).map_err(|e| format!("Parse error: {:?}", e))?;
    let select_stmt = match stmt {
        Statement::Select(s) => s,
        other => return Err(format!("Expected SELECT statement, got {:?}", other)),
    };

    let executor = SelectExecutor::new(db);
    let rows = executor.execute(&select_stmt).map_err(|e| format!("Execution error: {:?}", e))?;
    Ok(rows.len())
}

// ============================================================================
// TIMESTAMP Literal Tests (Parser Path)
// ============================================================================

#[test]
fn test_timestamp_literal_iso8601() {
    let mut db = Database::new();
    setup_test_table(&mut db).expect("Failed to create table");

    assert_insert_succeeds(
        &mut db,
        "INSERT INTO timestamps VALUES (1, TIMESTAMP '2025-11-10T08:24:34')",
    );
}

#[test]
fn test_timestamp_literal_iso8601_with_fractional() {
    let mut db = Database::new();
    setup_test_table(&mut db).expect("Failed to create table");

    assert_insert_succeeds(
        &mut db,
        "INSERT INTO timestamps VALUES (1, TIMESTAMP '2025-11-10T08:24:34.602988')",
    );
}

#[test]
fn test_timestamp_literal_iso8601_with_utc() {
    let mut db = Database::new();
    setup_test_table(&mut db).expect("Failed to create table");

    assert_insert_succeeds(
        &mut db,
        "INSERT INTO timestamps VALUES (1, TIMESTAMP '2025-11-10T08:24:34Z')",
    );
}

#[test]
fn test_timestamp_literal_iso8601_with_timezone() {
    let mut db = Database::new();
    setup_test_table(&mut db).expect("Failed to create table");

    assert_insert_succeeds(
        &mut db,
        "INSERT INTO timestamps VALUES (1, TIMESTAMP '2025-11-10T08:24:34+05:00')",
    );
}

#[test]
fn test_timestamp_literal_space_separated() {
    let mut db = Database::new();
    setup_test_table(&mut db).expect("Failed to create table");

    assert_insert_succeeds(
        &mut db,
        "INSERT INTO timestamps VALUES (1, TIMESTAMP '2025-11-10 08:24:34')",
    );
}

#[test]
fn test_timestamp_literal_date_only() {
    let mut db = Database::new();
    setup_test_table(&mut db).expect("Failed to create table");

    assert_insert_succeeds(&mut db, "INSERT INTO timestamps VALUES (1, TIMESTAMP '2025-11-10')");
}

// ============================================================================
// String Literal Tests (Implicit Casting Path)
// ============================================================================

#[test]
fn test_string_literal_iso8601() {
    let mut db = Database::new();
    setup_test_table(&mut db).expect("Failed to create table");

    // This tests implicit conversion from VARCHAR to TIMESTAMP
    assert_insert_succeeds(&mut db, "INSERT INTO timestamps VALUES (1, '2025-11-10T08:24:34')");
}

#[test]
fn test_string_literal_iso8601_with_fractional() {
    let mut db = Database::new();
    setup_test_table(&mut db).expect("Failed to create table");

    assert_insert_succeeds(
        &mut db,
        "INSERT INTO timestamps VALUES (1, '2025-11-10T08:24:34.602988')",
    );
}

#[test]
fn test_string_literal_iso8601_with_utc() {
    let mut db = Database::new();
    setup_test_table(&mut db).expect("Failed to create table");

    assert_insert_succeeds(&mut db, "INSERT INTO timestamps VALUES (1, '2025-11-10T08:24:34Z')");
}

#[test]
fn test_string_literal_space_separated() {
    let mut db = Database::new();
    setup_test_table(&mut db).expect("Failed to create table");

    assert_insert_succeeds(&mut db, "INSERT INTO timestamps VALUES (1, '2025-11-10 08:24:34')");
}

#[test]
fn test_string_literal_date_only() {
    let mut db = Database::new();
    setup_test_table(&mut db).expect("Failed to create table");

    assert_insert_succeeds(&mut db, "INSERT INTO timestamps VALUES (1, '2025-11-10')");
}

// ============================================================================
// CAST Tests
// ============================================================================
// Note: CAST expressions in INSERT VALUES are not currently supported.
// The CAST functionality is tested via the casting module in executor directly.
// These tests verify that TIMESTAMP parsing works with explicit TIMESTAMP literals
// and implicit VARCHAR→TIMESTAMP conversion, which covers the practical use cases.

// ============================================================================
// Real-World Use Case Tests
// ============================================================================

#[test]
fn test_sql_dump_restore() {
    // This is the exact format that was failing in the issue
    let mut db = Database::new();
    setup_test_table(&mut db).expect("Failed to create table");

    assert_insert_succeeds(
        &mut db,
        "INSERT INTO timestamps VALUES (1, '2025-11-10T08:24:34.602988')",
    );
}

#[test]
fn test_api_response_format() {
    let mut db = Database::new();
    setup_test_table(&mut db).expect("Failed to create table");

    assert_insert_succeeds(&mut db, "INSERT INTO timestamps VALUES (1, '2025-11-10T14:30:00Z')");
}

#[test]
fn test_multiple_formats_in_same_table() {
    let mut db = Database::new();
    setup_test_table(&mut db).expect("Failed to create table");

    // Mix of formats should all work
    assert_insert_succeeds(&mut db, "INSERT INTO timestamps VALUES (1, '2025-11-10T08:24:34')");
    assert_insert_succeeds(&mut db, "INSERT INTO timestamps VALUES (2, '2025-11-10 08:24:34')");
    assert_insert_succeeds(
        &mut db,
        "INSERT INTO timestamps VALUES (3, TIMESTAMP '2025-11-10T08:24:34Z')",
    );
    assert_insert_succeeds(&mut db, "INSERT INTO timestamps VALUES (4, '2025-11-10')");
}

// ============================================================================
// Error Cases
// ============================================================================

#[test]
fn test_invalid_timestamp_format_fails() {
    let mut db = Database::new();
    setup_test_table(&mut db).expect("Failed to create table");

    assert_insert_fails(&mut db, "INSERT INTO timestamps VALUES (1, 'not-a-timestamp')");
}

#[test]
fn test_invalid_date_component_fails() {
    let mut db = Database::new();
    setup_test_table(&mut db).expect("Failed to create table");

    assert_insert_fails(&mut db, "INSERT INTO timestamps VALUES (1, '2025-13-10T08:24:34')");
}

#[test]
fn test_invalid_time_component_fails() {
    let mut db = Database::new();
    setup_test_table(&mut db).expect("Failed to create table");

    assert_insert_fails(&mut db, "INSERT INTO timestamps VALUES (1, '2025-11-10T25:00:00')");
}

// ============================================================================
// Query Tests (SELECT with timestamp values)
// ============================================================================

#[test]
fn test_select_after_insert_iso8601() {
    let mut db = Database::new();
    setup_test_table(&mut db).expect("Failed to create table");

    // Insert with ISO 8601 format
    execute_insert(&mut db, "INSERT INTO timestamps VALUES (1, '2025-11-10T08:24:34')")
        .expect("INSERT failed");

    // SELECT should work
    let result = query_count(&db, "SELECT * FROM timestamps WHERE id = 1");
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 1);
}

#[test]
fn test_multiple_iso8601_inserts() {
    let mut db = Database::new();
    setup_test_table(&mut db).expect("Failed to create table");

    // Insert multiple timestamps with ISO 8601 format
    execute_insert(&mut db, "INSERT INTO timestamps VALUES (1, '2025-11-10T08:00:00')")
        .expect("INSERT failed");
    execute_insert(&mut db, "INSERT INTO timestamps VALUES (2, '2025-11-10T09:00:00')")
        .expect("INSERT failed");

    // Verify both rows were inserted
    let result = query_count(&db, "SELECT * FROM timestamps");
    assert!(result.is_ok());
    assert_eq!(result.unwrap(), 2);
}
