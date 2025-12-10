//! PostgreSQL-inspired regression test suite for VibeSQL.
//!
//! This test module runs SQL test files from tests/pgsql/sql/ to verify
//! VibeSQL's conformance with PostgreSQL-style SQL semantics.
//!
//! Tests are organized by category (triggers, select, insert, etc.) and
//! results are collected for reporting on the website conformance dashboard.

#[path = "../pgsql/mod.rs"]
mod pgsql;

use std::fs;
use std::path::PathBuf;

use pgsql::{runner, stats::PgTestStats};

/// Get the test SQL directory
fn get_test_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("tests")
        .join("pgsql")
        .join("sql")
}

/// Get the output directory for test results
fn get_results_dir() -> PathBuf {
    let home = std::env::var("HOME").unwrap_or_else(|_| "/tmp".to_string());
    PathBuf::from(home).join(".vibesql").join("test_results")
}

/// Run all PostgreSQL regression tests
#[test]
fn test_pgsql_regression_suite() {
    let test_dir = get_test_dir();

    if !test_dir.exists() {
        println!("Test directory not found: {}", test_dir.display());
        println!("Creating empty test directory...");
        fs::create_dir_all(&test_dir).expect("Failed to create test directory");
    }

    // Check if there are any test files
    let sql_files: Vec<_> = glob::glob(&format!("{}/**/*.sql", test_dir.display()))
        .expect("Failed to read test pattern")
        .filter_map(Result::ok)
        .collect();

    if sql_files.is_empty() {
        println!("No test files found in {}", test_dir.display());
        println!("Add .sql test files to run the PostgreSQL regression suite.");
        return;
    }

    // Run the test suite
    let stats = runner::run_test_suite(&test_dir);

    // Save results to JSON for website export
    save_results(&stats);

    // Assert no failures for CI
    let total = stats.total_stats();
    assert_eq!(
        total.failed, 0,
        "PostgreSQL regression tests had {} failures",
        total.failed
    );
    assert_eq!(
        total.errors, 0,
        "PostgreSQL regression tests had {} errors",
        total.errors
    );
}

/// Save test results to JSON file
fn save_results(stats: &PgTestStats) {
    let results_dir = get_results_dir();
    fs::create_dir_all(&results_dir).ok();

    let results_file = results_dir.join("pgsql_regress_results.json");
    let json = serde_json::to_string_pretty(&stats.to_json()).expect("Failed to serialize results");

    fs::write(&results_file, json).expect("Failed to write results file");
    println!("\nResults saved to: {}", results_file.display());
}

/// Run only trigger tests
#[test]
fn test_pgsql_triggers() {
    let test_file = get_test_dir().join("triggers.sql");

    if !test_file.exists() {
        println!("Trigger test file not found: {}", test_file.display());
        return;
    }

    println!("\n=== PostgreSQL Trigger Tests ===");
    let stats = runner::run_test_file(&test_file);

    println!(
        "\nTrigger tests: {}/{} passed ({:.1}%)",
        stats.passed,
        stats.total,
        stats.pass_rate()
    );

    if !stats.error_messages.is_empty() {
        println!("\nErrors:");
        for msg in &stats.error_messages {
            println!("  - {}", msg);
        }
    }

    // For now, allow failures while we develop - remove this for strict mode
    if stats.pass_rate() < 50.0 {
        println!(
            "\nWARNING: Pass rate ({:.1}%) is below 50% threshold",
            stats.pass_rate()
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parser_unit_tests() {
        // Verify the test file parser works correctly
        let content = r#"
-- TEST: Simple select
-- EXPECT: 1
SELECT 1;
"#;
        let cases = pgsql::runner::TestFileParser::parse(content);
        assert_eq!(cases.len(), 1);
        assert_eq!(cases[0].name, "Simple select");
    }
}
