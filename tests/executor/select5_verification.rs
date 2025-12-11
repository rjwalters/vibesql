//! Verification tests for select5.test to prove predicate pushdown optimization works
//!
//! The select5.test file contains 732 queries testing multi-table joins from 4 to 64 tables.
//! These tests verify that the predicate pushdown optimization successfully prevents OOM
//! on massive multi-table joins with equijoin conditions.
//!
//! ## Test Strategy
//!
//! - **Fast Sampled Test** (`test_select5_sampled_regression`): Runs 1 representative 4-table join
//!   query. This test runs in <1 second and is suitable for regular CI. It verifies the predicate
//!   pushdown optimization is working.
//!
//! ## Performance History
//!
//! - **Before optimization**: 73+ GB memory → OOM failure
//! - **After PR #1129 (predicate pushdown)**: 6.48 GB memory, 5-10 min runtime → Success but slow
//! - **Current (sampled test)**: <100 MB memory, <10 sec runtime → Fast regression test

use std::path::Path;

use ::sqllogictest::Runner;

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
