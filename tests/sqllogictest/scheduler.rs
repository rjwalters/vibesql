//! Test file scheduling and prioritization logic.

use std::{collections::HashSet, env, fs, path::PathBuf};

/// Load historical test results from JSON file
#[allow(dead_code)]
pub fn load_historical_results() -> serde_json::Value {
    // Try to load from target/sqllogictest_cumulative.json (workflow format)
    if let Ok(content) = fs::read_to_string("target/sqllogictest_cumulative.json") {
        if let Ok(json) = serde_json::from_str(&content) {
            return json;
        }
    }

    // Try to load from target/sqllogictest_analysis.json (analysis format)
    if let Ok(content) = fs::read_to_string("target/sqllogictest_analysis.json") {
        if let Ok(json) = serde_json::from_str(&content) {
            return json;
        }
    }

    // Return empty object if no historical data found
    serde_json::Value::Object(serde_json::Map::new())
}

/// Prioritize test files based on historical results: failed first, then untested, then passed
#[allow(dead_code)]
pub fn prioritize_test_files(
    all_files: &[PathBuf],
    historical: &serde_json::Value,
    test_dir: &PathBuf,
    seed: u64,
) -> Vec<PathBuf> {
    // Extract historical passed and failed sets
    let mut historical_passed = HashSet::new();
    let mut historical_failed = HashSet::new();

    if let Some(tested_files) = historical.get("tested_files") {
        if let Some(passed) = tested_files.get("passed").and_then(|p| p.as_array()) {
            for file in passed {
                if let Some(file_str) = file.as_str() {
                    historical_passed.insert(file_str.to_string());
                }
            }
        }
        if let Some(failed) = tested_files.get("failed").and_then(|f| f.as_array()) {
            for file in failed {
                if let Some(file_str) = file.as_str() {
                    historical_failed.insert(file_str.to_string());
                }
            }
        }
    }

    // Categorize files by priority
    let mut failed_files = Vec::new();
    let mut untested_files = Vec::new();
    let mut passed_files = Vec::new();

    for file_path in all_files {
        let relative_path =
            file_path.strip_prefix(test_dir).unwrap_or(file_path).to_string_lossy().to_string();

        if historical_failed.contains(&relative_path) {
            failed_files.push(file_path.clone());
        } else if historical_passed.contains(&relative_path) {
            passed_files.push(file_path.clone());
        } else {
            untested_files.push(file_path.clone());
        }
    }

    // Shuffle within each category using deterministic seed
    use std::{
        collections::hash_map::DefaultHasher,
        hash::{Hash, Hasher},
    };

    let shuffle_with_seed = |files: &mut Vec<PathBuf>| {
        files.sort_by_cached_key(|path| {
            let mut hasher = DefaultHasher::new();
            path.hash(&mut hasher);
            seed.hash(&mut hasher);
            hasher.finish()
        });
    };

    shuffle_with_seed(&mut failed_files);
    shuffle_with_seed(&mut untested_files);
    shuffle_with_seed(&mut passed_files);

    // Apply worker-based partitioning if parallel workers are configured
    // This ensures each worker tests a unique slice of untested files
    let (worker_id, total_workers) = get_worker_config();

    if total_workers > 1 && worker_id > 0 && worker_id <= total_workers {
        println!("Worker partitioning: This is worker {}/{}", worker_id, total_workers);

        // Partition untested files among workers
        let untested_partition = partition_files(&untested_files, worker_id, total_workers);
        println!(
            "  Untested files assigned to this worker: {} of {}",
            untested_partition.len(),
            untested_files.len()
        );

        // All workers test failed files (high priority)
        // But each worker gets their own slice of untested files
        // Passed files are shared (lowest priority, rarely reached)
        let mut prioritized = Vec::new();
        prioritized.extend(failed_files);
        prioritized.extend(untested_partition);
        prioritized.extend(passed_files);

        prioritized
    } else {
        // Single worker mode: test everything in priority order
        let mut prioritized = Vec::new();
        prioritized.extend(failed_files);
        prioritized.extend(untested_files);
        prioritized.extend(passed_files);

        prioritized
    }
}

/// Extract worker configuration from environment variables
#[allow(dead_code)]
pub fn get_worker_config() -> (usize, usize) {
    let worker_id =
        env::var("SQLLOGICTEST_WORKER_ID").ok().and_then(|s| s.parse().ok()).unwrap_or(0);

    let total_workers =
        env::var("SQLLOGICTEST_TOTAL_WORKERS").ok().and_then(|s| s.parse().ok()).unwrap_or(1);

    (worker_id, total_workers)
}

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
        ("index/between/1000/slt_good_0.test", 600),  // 2,771 queries -> 10min
        ("index/commute/1000/slt_good_1.test", 1200), // 9,562 queries -> 20min
        ("index/commute/1000/slt_good_2.test", 1200), // 10,000 queries -> 20min
        ("index/commute/1000/slt_good_3.test", 1200), // 10,000 queries -> 20min
        ("index/delete/10000/slt_good_0.test", 1800), // 10,000 rows, many DELETEs -> 30min
        ("index/view/10000/slt_good_0.test", 1800),   // 10,000 rows with views -> 30min
    ];

    for (test_file, timeout) in &high_volume_tests {
        if file_name.contains(test_file) {
            return *timeout;
        }
    }

    // Default timeout: 5 minutes
    300
}

/// Get per-test-file timeout from environment variable or use default
/// Returns timeout in seconds
///
/// DEPRECATED: Use get_test_file_timeout_for() instead to support per-file timeouts
#[allow(dead_code)]
pub fn get_test_file_timeout() -> u64 {
    get_test_file_timeout_for("")
}

/// Partition files into equal slices for parallel workers
/// Returns the slice for the specified worker_id (1-indexed)
#[allow(dead_code)]
fn partition_files(files: &[PathBuf], worker_id: usize, total_workers: usize) -> Vec<PathBuf> {
    if total_workers <= 1 || worker_id == 0 || worker_id > total_workers {
        return files.to_vec();
    }

    let chunk_size = (files.len() + total_workers - 1) / total_workers; // Ceiling division
    let start = (worker_id - 1) * chunk_size;
    let end = start + chunk_size;

    files.get(start..end.min(files.len())).unwrap_or(&[]).to_vec()
}
