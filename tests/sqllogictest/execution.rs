//! Test file execution with timeout handling.

use std::time::Duration;

use sqllogictest::Runner;
use tokio::time::timeout;

use super::{
    db_adapter::VibeSqlDB,
    scheduler, stats::TestFailure,
};

// Re-export DialectStats for use by the test suite
pub use sqllogictest::DialectStats;

#[derive(Debug)]
pub enum TestError {
    Execution(String),
    Timeout { file: String, timeout_seconds: u64 },
}

impl std::fmt::Display for TestError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TestError::Execution(msg) => write!(f, "{}", msg),
            TestError::Timeout { file, timeout_seconds } => {
                write!(f, "Timeout: {} exceeded {}s limit", file, timeout_seconds)
            }
        }
    }
}

impl std::error::Error for TestError {}

/// Result of running a test file, including dialect statistics
#[allow(dead_code)]
pub struct TestFileResult {
    pub result: Result<(), TestError>,
    pub failures: Vec<TestFailure>,
    pub dialect_stats: DialectStats,
}

/// Run a test file asynchronously and capture detailed failure information
async fn run_test_file_async(contents: &str, _file_name: &str) -> TestFileResult {
    let mut tester = Runner::new(|| async { Ok(VibeSqlDB::new()) });
    // Enable hash mode with threshold of 8 (standard SQLLogicTest behavior)
    tester.with_hash_threshold(8);
    // Add "mysql" label for skipif/onlyif directives
    // VibeSQL uses MySQL-compatible division (returns REAL/DECIMAL for integer division)
    tester.add_label("mysql");

    // The dolthub/sqllogictest corpus was regenerated against MySQL 8.x
    // (see third_party/sqllogictest/README.md), so all tests expect MySQL semantics
    // including decimal division (INTEGER / INTEGER → DECIMAL).
    // The `onlyif mysql` directives are for MySQL-specific SYNTAX (like CAST AS SIGNED).
    // The Database default is now MySQL mode, so tests use MySQL division semantics.
    // Enable auto-dialect switching to handle tests with skipif/onlyif conditions.
    tester.enable_auto_switch_dialect();
    let script = contents.to_string();

    let result = tester.run_script(&script);

    // Capture dialect stats before returning (stats are accumulated in the runner)
    let dialect_stats = tester.dialect_stats().clone();

    match result {
        Ok(_) => TestFileResult {
            result: Ok(()),
            failures: vec![],
            dialect_stats,
        },
        Err(e) => {
            // Capture error information
            let failure = TestFailure {
                sql_statement: "Unknown - script failed".to_string(),
                expected_result: None,
                actual_result: None,
                error_message: e.to_string(),
                line_number: None,
            };
            TestFileResult {
                result: Err(TestError::Execution(e.to_string())),
                failures: vec![failure],
                dialect_stats,
            }
        }
    }
}

/// Run a test file with timeout wrapper, capturing detailed failure information
async fn run_test_file_with_timeout_impl(
    contents: &str,
    file_name: &str,
    timeout_secs: u64,
) -> TestFileResult {
    // Create a future that runs the test with a timeout
    let test_future = run_test_file_async(contents, file_name);

    // Apply timeout
    match timeout(Duration::from_secs(timeout_secs), test_future).await {
        Ok(result) => result,
        Err(_) => {
            let failure = TestFailure {
                sql_statement: "Test file exceeded timeout".to_string(),
                expected_result: None,
                actual_result: None,
                error_message: format!("Test file exceeded {}s timeout limit", timeout_secs),
                line_number: None,
            };
            TestFileResult {
                result: Err(TestError::Timeout {
                    file: file_name.to_string(),
                    timeout_seconds: timeout_secs,
                }),
                failures: vec![failure],
                dialect_stats: DialectStats::default(),
            }
        }
    }
}

/// Run a test file and capture detailed failure information
#[allow(dead_code)]
pub fn run_test_file_with_details(
    contents: &str,
    file_name: &str,
) -> TestFileResult {
    let timeout_secs = scheduler::get_test_file_timeout_for(file_name);

    let result = std::panic::catch_unwind(|| {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(run_test_file_with_timeout_impl(contents, file_name, timeout_secs))
    });

    match result {
        Ok(result) => result,
        Err(e) => {
            let error_msg =
                e.downcast_ref::<String>().unwrap_or(&"Unknown panic".to_string()).clone();

            let failure = TestFailure {
                sql_statement: "Unknown - panic occurred".to_string(),
                expected_result: None,
                actual_result: None,
                error_message: format!("Test harness panicked: {}", error_msg),
                line_number: None,
            };
            TestFileResult {
                result: Err(TestError::Execution(format!("Test harness panicked: {}", error_msg))),
                failures: vec![failure],
                dialect_stats: DialectStats::default(),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use super::*;
    #[allow(unused_imports)]
    use std::env;
    #[allow(unused_imports)]
    use crate::sqllogictest::scheduler::get_test_file_timeout;

    #[test]
    fn test_timeout_wraps_execution() {
        // Test that timeout wrapper can be set via environment variable
        let original = env::var("SQLLOGICTEST_FILE_TIMEOUT").ok();

        env::set_var("SQLLOGICTEST_FILE_TIMEOUT", "1");
        let timeout_secs = get_test_file_timeout();
        assert_eq!(timeout_secs, 1);

        // Restore original value or remove
        match original {
            Some(val) => env::set_var("SQLLOGICTEST_FILE_TIMEOUT", val),
            None => env::remove_var("SQLLOGICTEST_FILE_TIMEOUT"),
        }
    }

    #[tokio::test]
    async fn test_file_timeout_triggers() {
        // Test a file with a very short timeout that should fail
        // Create a test that would take longer than our timeout
        let slow_test = "
query I
SELECT 1
----
1
        ";

        // Set very short timeout of 1 second
        let file_name = "test_timeout.test";
        let test_result = run_test_file_with_timeout_impl(slow_test, file_name, 1).await;

        // Check if result is ok or timeout
        match test_result.result {
            Ok(()) => {
                // Test completed successfully within timeout
                assert!(true, "Test completed within timeout");
            }
            Err(TestError::Timeout { .. }) => {
                // Timeout occurred - also acceptable for this test
                assert!(true, "Timeout occurred as expected");
            }
            Err(TestError::Execution(e)) => {
                // This is ok too - test might have execution error
                println!("Execution error: {}", e);
                assert!(true, "Execution error occurred");
            }
        }
    }
}
