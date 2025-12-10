//! Test result statistics for PostgreSQL regression tests.

use std::collections::HashMap;

/// Result status for a single test case
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TestStatus {
    Passed,
    Failed,
    Skipped,
    Error,
}

/// Statistics for a single test file
#[derive(Debug, Clone, Default)]
pub struct FileStats {
    pub total: usize,
    pub passed: usize,
    pub failed: usize,
    pub skipped: usize,
    pub errors: usize,
    pub error_messages: Vec<String>,
}

impl FileStats {
    pub fn pass_rate(&self) -> f64 {
        let relevant_total = self.total - self.skipped;
        if relevant_total == 0 {
            100.0
        } else {
            (self.passed as f64 / relevant_total as f64) * 100.0
        }
    }

    pub fn add_result(&mut self, status: TestStatus, error_msg: Option<String>) {
        self.total += 1;
        match status {
            TestStatus::Passed => self.passed += 1,
            TestStatus::Failed => {
                self.failed += 1;
                if let Some(msg) = error_msg {
                    self.error_messages.push(msg);
                }
            }
            TestStatus::Skipped => self.skipped += 1,
            TestStatus::Error => {
                self.errors += 1;
                if let Some(msg) = error_msg {
                    self.error_messages.push(msg);
                }
            }
        }
    }
}

/// Aggregate statistics across all test files
#[derive(Debug, Clone, Default)]
pub struct PgTestStats {
    pub files: HashMap<String, FileStats>,
    pub by_category: HashMap<String, FileStats>,
}

impl PgTestStats {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add_file_result(&mut self, category: &str, filename: &str, stats: FileStats) {
        // Add to file-level stats
        self.files.insert(filename.to_string(), stats.clone());

        // Aggregate to category-level stats
        let cat_stats = self.by_category.entry(category.to_string()).or_default();
        cat_stats.total += stats.total;
        cat_stats.passed += stats.passed;
        cat_stats.failed += stats.failed;
        cat_stats.skipped += stats.skipped;
        cat_stats.errors += stats.errors;
        cat_stats.error_messages.extend(stats.error_messages);
    }

    pub fn total_stats(&self) -> FileStats {
        let mut total = FileStats::default();
        for stats in self.files.values() {
            total.total += stats.total;
            total.passed += stats.passed;
            total.failed += stats.failed;
            total.skipped += stats.skipped;
            total.errors += stats.errors;
        }
        total
    }

    pub fn to_json(&self) -> serde_json::Value {
        let total = self.total_stats();
        serde_json::json!({
            "summary": {
                "total": total.total,
                "passed": total.passed,
                "failed": total.failed,
                "skipped": total.skipped,
                "errors": total.errors,
                "pass_rate": total.pass_rate()
            },
            "by_category": self.by_category.iter().map(|(cat, stats)| {
                (cat.clone(), serde_json::json!({
                    "total": stats.total,
                    "passed": stats.passed,
                    "failed": stats.failed,
                    "skipped": stats.skipped,
                    "pass_rate": stats.pass_rate()
                }))
            }).collect::<serde_json::Map<String, serde_json::Value>>(),
            "files": self.files.iter().map(|(file, stats)| {
                (file.clone(), serde_json::json!({
                    "total": stats.total,
                    "passed": stats.passed,
                    "failed": stats.failed,
                    "skipped": stats.skipped,
                    "pass_rate": stats.pass_rate()
                }))
            }).collect::<serde_json::Map<String, serde_json::Value>>()
        })
    }
}
