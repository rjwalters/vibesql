//! Test result statistics and failure information tracking.

use std::collections::HashSet;

/// Detailed failure information for a single test file
#[allow(dead_code)]
#[derive(Debug, Clone)]
pub struct TestFailure {
    pub sql_statement: String,
    pub expected_result: Option<String>,
    pub actual_result: Option<String>,
    pub error_message: String,
    pub line_number: Option<usize>,
}

/// Test result statistics
#[allow(dead_code)]
#[derive(Debug, Default)]
pub struct TestStats {
    pub total: usize,
    pub passed: usize,
    pub failed: usize,
    pub timed_out: usize,  // Tests that exceeded time limit
    pub errors: usize,
    pub skipped: usize,
    pub tested_files: HashSet<String>, // Files that were actually tested this run
    pub detailed_failures: Vec<(String, Vec<TestFailure>)>, // (file_path, failures) pairs
    pub timed_out_files: Vec<String>, // Files that timed out
}

impl TestStats {
    #[allow(dead_code)]
    pub fn pass_rate(&self) -> f64 {
        let relevant_total = self.total - self.skipped;
        if relevant_total == 0 {
            0.0
        } else {
            (self.passed as f64 / relevant_total as f64) * 100.0
        }
    }
}
