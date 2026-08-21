//! Test file scheduling and prioritization logic.

use std::env;

/// Get per-test-file timeout from environment variable or use default
/// Returns timeout in seconds
///
/// Some test files contain an exceptionally large number of queries (10K+) and require
/// extended timeouts. This function returns custom timeouts for known high-volume tests.
pub fn get_test_file_timeout_for(file_name: &str) -> u64 {
    // Check environment variable first (highest priority)
    if let Ok(timeout_str) = env::var("SQLLOGICTEST_FILE_TIMEOUT") {
        if let Ok(timeout) = timeout_str.parse() {
            return timeout;
        }
    }

    // Extended timeouts for high-volume index tests (see issue #2037, #2090)
    // These tests contain 10K-32K queries and require significantly more time
    let high_volume_tests = [
        ("index/between/1000/slt_good_0.test", 600), // 2,771 queries -> 10min
        ("index/commute/1000/slt_good_1.test", 1200), // 9,562 queries -> 20min
        ("index/commute/1000/slt_good_2.test", 1200), // 10,000 queries -> 20min
        ("index/commute/1000/slt_good_3.test", 1200), // 10,000 queries -> 20min
        ("index/delete/10000/slt_good_0.test", 1800), // 10,000 rows, many DELETEs -> 30min
        ("index/view/10000/slt_good_0.test", 1800),  // 10,000 rows with views -> 30min
    ];

    for (test_file, timeout) in &high_volume_tests {
        if file_name.contains(test_file) {
            return *timeout;
        }
    }

    // Default timeout: 5 minutes
    300
}
