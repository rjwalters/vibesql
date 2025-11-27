//! Dialect-specific integration tests for vibesql.
//!
//! These tests verify that the SQL dialect switching functionality works correctly,
//! including the `dialect` directive in sqllogictest files.

#[path = "../sqllogictest/mod.rs"]
mod sqllogictest;

use std::fs;
use std::path::Path;
use sqllogictest::execution::run_test_file_with_details;

/// Helper function to run a test file and check for success
fn run_dialect_test(test_name: &str) {
    let test_path = format!("tests/dialect/{}.test", test_name);
    let path = Path::new(&test_path);

    if !path.exists() {
        panic!("Test file not found: {:?}", path);
    }

    let contents = fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("Failed to read test file {:?}: {}", path, e));

    let test_result = run_test_file_with_details(&contents, &test_path);

    if let Err(ref e) = test_result.result {
        eprintln!("\n=== {} test failures ===", test_name);
        for failure in &test_result.failures {
            eprintln!("SQL: {}", failure.sql_statement);
            if let Some(ref expected) = failure.expected_result {
                eprintln!("Expected: {}", expected);
            }
            if let Some(ref actual) = failure.actual_result {
                eprintln!("Actual: {}", actual);
            }
            eprintln!("Error: {}", failure.error_message);
            if let Some(line) = failure.line_number {
                eprintln!("Line: {}", line);
            }
            eprintln!("---");
        }
        panic!("{} dialect test failed: {:?}", test_name, e);
    }
}

#[test]
fn test_dialect_division() {
    run_dialect_test("division");
}

#[test]
fn test_dialect_string_handling() {
    run_dialect_test("string_handling");
}

#[test]
fn test_dialect_boolean_and_comparison() {
    run_dialect_test("boolean_and_comparison");
}

#[test]
fn test_dialect_aggregates() {
    run_dialect_test("aggregates");
}
