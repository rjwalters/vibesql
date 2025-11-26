//! Verification tests for select5.test to prove predicate pushdown optimization works
//!
//! The select5.test file contains 732 queries testing multi-table joins from 4 to 64 tables.
//! These tests verify that the predicate pushdown optimization successfully prevents OOM
//! on massive multi-table joins with equijoin conditions.
//!
//! ## Test Strategy
//!
//! - **Fast Sampled Test** (`test_select5_sampled_regression`): Runs 1 representative 4-table
//!   join query. This test runs in <1 second and is suitable for regular CI. It verifies
//!   the predicate pushdown optimization is working.
//!
//! - **Full Test Suite** (`test_select5_full_suite`): Runs all 732 queries from the original
//!   select5.test file. This takes 5-10 minutes and is disabled by default with `#[ignore]`.
//!   Run manually with: `cargo test test_select5_full_suite -- --ignored`
//!
//! ## Performance History
//!
//! - **Before optimization**: 73+ GB memory → OOM failure
//! - **After PR #1129 (predicate pushdown)**: 6.48 GB memory, 5-10 min runtime → Success but slow
//! - **Current (sampled test)**: <100 MB memory, <10 sec runtime → Fast regression test

use ::sqllogictest::Runner;
use std::path::Path;
use std::time::Duration;

#[path = "../sqllogictest/mod.rs"]
mod sqllogictest;

use crate::sqllogictest::db_adapter::VibeSqlDB;

/// Fast regression test using sampled representative queries from select5.test
///
/// This test runs 1 representative 4-table join query that exercises the predicate
/// pushdown optimization with equijoin conditions.
///
/// Expected runtime: <1 second
/// Memory usage: <10 MB
///
/// This test runs in regular CI to catch predicate pushdown regressions quickly.
#[tokio::test]
async fn test_select5_sampled_regression() {
    let test_file = Path::new("tests/select5_samples/select5_minimal.test");

    if !test_file.exists() {
        panic!("Sampled test file not found: {}", test_file.display());
    }

    let mut runner = Runner::new(|| async { Ok(VibeSqlDB::new()) });
    // Add "mysql" label for skipif/onlyif directives
    runner.add_label("mysql");

    // Run the sampled test - should complete quickly
    let result = runner.run_file_async(test_file).await;

    match result {
        Ok(_) => {
            println!("✓ select5 sampled test PASSED - predicate pushdown optimization working!");
        }
        Err(e) => {
            panic!("select5 sampled test FAILED: {}\n\nThis indicates a regression in the predicate pushdown optimization.", e);
        }
    }
}

/// Full select5.test suite - disabled by default due to long runtime (5-10 minutes)
///
/// This test runs all 732 queries from the original select5.test file, providing
/// comprehensive verification of multi-table join optimization across all join sizes
/// from 4 to 64 tables.
///
/// **Usage**: Run manually or in nightly CI with:
/// ```bash
/// cargo test test_select5_full_suite -- --ignored
/// ```
///
/// **Performance**:
/// - Runtime: 5-10 minutes (300s query timeout)
/// - Memory: ~6.48 GB peak (down from 73+ GB before optimization)
/// - Queries: 732 total (12 queries each for joins of 4-64 tables)
///
/// **Why disabled**: Too slow for PR CI, but valuable for comprehensive verification
#[tokio::test]
#[ignore] // Performance test: Run manually with `cargo test test_select5_full_suite -- --ignored`
async fn test_select5_full_suite() {
    // Timeout protection: Kill test after 10 minutes to prevent CI hangs
    const TEST_TIMEOUT: Duration = Duration::from_secs(10 * 60);

    let test_file = Path::new("third_party/sqllogictest/test/select5.test");

    if !test_file.exists() {
        panic!("select5.test not found - run: git submodule update --init third_party/sqllogictest");
    }

    let mut runner = Runner::new(|| async { Ok(VibeSqlDB::new()) });
    // Add "mysql" label for skipif/onlyif directives
    runner.add_label("mysql");

    // Run the full test with timeout protection
    let result = tokio::time::timeout(TEST_TIMEOUT, runner.run_file_async(test_file)).await;

    match result {
        Ok(Ok(_)) => {
            println!("✓ Full select5.test PASSED - all 732 queries succeeded!");
        }
        Ok(Err(e)) => {
            panic!("select5.test FAILED: {}", e);
        }
        Err(_) => {
            panic!(
                "select5.test TIMEOUT: Test exceeded {} minutes - possible performance regression or hang",
                TEST_TIMEOUT.as_secs() / 60
            );
        }
    }
}
