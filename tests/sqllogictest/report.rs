//! Benchmark comparison report generator with multiple output formats.
//!
//! This module provides infrastructure for comparing performance metrics from
//! different database engines (e.g., vibesql vs SQLite) and presenting the
//! results in human-readable and machine-parseable formats.
//!
//! # Usage
//!
//! ```rust,ignore
//! use vibesql::tests::sqllogictest::{metrics::BenchmarkMetrics, report::ComparisonReport};
//! use std::time::Duration;
//!
//! // Collect metrics from SQLite (baseline)
//! let mut sqlite_metrics = BenchmarkMetrics::new("sqlite");
//! sqlite_metrics.record_success(Duration::from_millis(100));
//! sqlite_metrics.update_peak_memory(10 * 1024 * 1024);
//!
//! // Collect metrics from vibesql (comparison)
//! let mut vibesql_metrics = BenchmarkMetrics::new("vibesql");
//! vibesql_metrics.record_success(Duration::from_millis(250));
//! vibesql_metrics.update_peak_memory(25 * 1024 * 1024);
//!
//! // Generate comparison report
//! let report = ComparisonReport::new("select1.test", sqlite_metrics, vibesql_metrics);
//!
//! // Output in different formats
//! println!("{}", report.to_console());           // Human-readable console output
//! let json = report.to_json().unwrap();          // JSON for programmatic analysis
//! let markdown = report.to_markdown();            // Markdown for documentation
//! ```
//!
//! # Output Formats
//!
//! ## Console Output
//! Human-readable tables with performance metrics and ratios:
//! - Summary table with key metrics (time, memory, queries/sec)
//! - Performance breakdown (median, p95, p99)
//! - Failed queries listing
//!
//! ## JSON Output
//! Structured data including:
//! - Full metrics from both engines
//! - Calculated comparison statistics
//! - Metadata (timestamp, test name, engine names)
//!
//! ## Markdown Output
//! GitHub-compatible markdown with:
//! - Summary table with emoji indicators
//! - Detailed performance breakdown
//! - Failed queries section
//!
//! # Integration with Benchmark Harness
//!
//! This module is designed to be used with the benchmark harness (issue #1115).
//! The harness will:
//! 1. Execute tests on both engines
//! 2. Collect BenchmarkMetrics for each
//! 3. Create ComparisonReport
//! 4. Output in requested format (console/JSON/markdown)

use std::fmt;

use serde::{Deserialize, Serialize};

use super::metrics::BenchmarkMetrics;

/// Comparison report containing metrics from multiple engines
#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComparisonReport {
    /// Name of the test file or suite
    pub test_name: String,
    /// Timestamp when the report was generated
    pub timestamp: String,
    /// Metrics from the first engine (baseline)
    pub baseline: BenchmarkMetrics,
    /// Metrics from the second engine (comparison)
    pub comparison: BenchmarkMetrics,
    /// Calculated comparison statistics
    pub statistics: ComparisonStatistics,
}

/// Calculated statistics comparing two engines
#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComparisonStatistics {
    /// Ratio of total execution time (comparison / baseline)
    pub time_ratio: f64,
    /// Ratio of peak memory usage (comparison / baseline)
    pub memory_ratio: f64,
    /// Difference in pass rate (comparison - baseline, in percentage points)
    pub pass_rate_diff: f64,
    /// Ratio of queries per second (comparison / baseline)
    pub qps_ratio: f64,
    /// Ratio of average query time (comparison / baseline)
    pub avg_query_time_ratio: f64,
    /// Ratio of median query time (comparison / baseline)
    pub median_query_time_ratio: f64,
    /// Ratio of p95 query time (comparison / baseline)
    pub p95_query_time_ratio: f64,
    /// Ratio of p99 query time (comparison / baseline)
    pub p99_query_time_ratio: f64,
}

#[allow(dead_code)]
impl ComparisonReport {
    /// Create a new comparison report from two sets of metrics
    pub fn new(
        test_name: impl Into<String>,
        baseline: BenchmarkMetrics,
        comparison: BenchmarkMetrics,
    ) -> Self {
        let statistics = Self::calculate_statistics(&baseline, &comparison);

        Self {
            test_name: test_name.into(),
            timestamp: chrono::Utc::now().to_rfc3339(),
            baseline,
            comparison,
            statistics,
        }
    }

    /// Calculate comparison statistics between two metrics
    fn calculate_statistics(
        baseline: &BenchmarkMetrics,
        comparison: &BenchmarkMetrics,
    ) -> ComparisonStatistics {
        let time_ratio = if baseline.total_duration.as_secs_f64() > 0.0 {
            comparison.total_duration.as_secs_f64() / baseline.total_duration.as_secs_f64()
        } else {
            0.0
        };

        let memory_ratio = if baseline.peak_memory_bytes > 0 {
            comparison.peak_memory_bytes as f64 / baseline.peak_memory_bytes as f64
        } else {
            0.0
        };

        let pass_rate_diff = comparison.pass_rate() - baseline.pass_rate();

        let qps_ratio = if baseline.queries_per_second() > 0.0 {
            comparison.queries_per_second() / baseline.queries_per_second()
        } else {
            0.0
        };

        let avg_query_time_ratio = if baseline.average_query_time().as_secs_f64() > 0.0 {
            comparison.average_query_time().as_secs_f64()
                / baseline.average_query_time().as_secs_f64()
        } else {
            0.0
        };

        let median_query_time_ratio = if baseline.median_query_time().as_secs_f64() > 0.0 {
            comparison.median_query_time().as_secs_f64()
                / baseline.median_query_time().as_secs_f64()
        } else {
            0.0
        };

        let p95_query_time_ratio = if baseline.p95_query_time().as_secs_f64() > 0.0 {
            comparison.p95_query_time().as_secs_f64() / baseline.p95_query_time().as_secs_f64()
        } else {
            0.0
        };

        let p99_query_time_ratio = if baseline.p99_query_time().as_secs_f64() > 0.0 {
            comparison.p99_query_time().as_secs_f64() / baseline.p99_query_time().as_secs_f64()
        } else {
            0.0
        };

        ComparisonStatistics {
            time_ratio,
            memory_ratio,
            pass_rate_diff,
            qps_ratio,
            avg_query_time_ratio,
            median_query_time_ratio,
            p95_query_time_ratio,
            p99_query_time_ratio,
        }
    }

    /// Format the report as JSON
    pub fn to_json(&self) -> Result<String, serde_json::Error> {
        serde_json::to_string_pretty(self)
    }

    /// Format the report as console output with colored indicators
    pub fn to_console(&self) -> String {
        let mut output = String::new();

        // Header
        output.push_str(&format!(
            "\nPerformance Comparison: {}\n{}\n",
            self.test_name,
            "=".repeat(50)
        ));

        // Summary table
        output.push_str(&format!(
            "\n{:<25} {:<15} {:<15} {:<12}\n",
            "Metric", &self.baseline.engine_name, &self.comparison.engine_name, "Ratio"
        ));
        output.push_str(&format!("{:-<70}\n", ""));

        // Total time
        output.push_str(&format!(
            "{:<25} {:<15} {:<15} {:<12}\n",
            "Total Time",
            format!("{:.3}s", self.baseline.total_duration.as_secs_f64()),
            format!("{:.3}s", self.comparison.total_duration.as_secs_f64()),
            self.format_ratio(self.statistics.time_ratio, false)
        ));

        // Peak memory
        output.push_str(&format!(
            "{:<25} {:<15} {:<15} {:<12}\n",
            "Peak Memory",
            Self::format_bytes(self.baseline.peak_memory_bytes),
            Self::format_bytes(self.comparison.peak_memory_bytes),
            self.format_ratio(self.statistics.memory_ratio, false)
        ));

        // Queries per second
        output.push_str(&format!(
            "{:<25} {:<15} {:<15} {:<12}\n",
            "Queries/sec",
            format!("{:.0}", self.baseline.queries_per_second()),
            format!("{:.0}", self.comparison.queries_per_second()),
            self.format_ratio(self.statistics.qps_ratio, true)
        ));

        // Average query time
        output.push_str(&format!(
            "{:<25} {:<15} {:<15} {:<12}\n",
            "Avg Query Time",
            format!("{:.2}ms", self.baseline.average_query_time().as_secs_f64() * 1000.0),
            format!("{:.2}ms", self.comparison.average_query_time().as_secs_f64() * 1000.0),
            self.format_ratio(self.statistics.avg_query_time_ratio, false)
        ));

        // Pass rate
        output.push_str(&format!(
            "\n{:<25} {:<15} {:<15}\n",
            "Pass Rate",
            format!("{:.1}%", self.baseline.pass_rate()),
            format!("{:.1}%", self.comparison.pass_rate()),
        ));

        output.push_str(&format!(
            "{:<25} {:<15} {:<15}\n",
            "Failures", self.baseline.fail_count, self.comparison.fail_count,
        ));

        // Performance breakdown
        output.push_str("\nPerformance Breakdown:\n");
        output.push_str(&format!(
            "  Median Query Time:     {:.2}ms vs {:.2}ms ({})\n",
            self.baseline.median_query_time().as_secs_f64() * 1000.0,
            self.comparison.median_query_time().as_secs_f64() * 1000.0,
            self.format_ratio(self.statistics.median_query_time_ratio, false)
        ));
        output.push_str(&format!(
            "  P95 Query Time:        {:.2}ms vs {:.2}ms ({})\n",
            self.baseline.p95_query_time().as_secs_f64() * 1000.0,
            self.comparison.p95_query_time().as_secs_f64() * 1000.0,
            self.format_ratio(self.statistics.p95_query_time_ratio, false)
        ));
        output.push_str(&format!(
            "  P99 Query Time:        {:.2}ms vs {:.2}ms ({})\n",
            self.baseline.p99_query_time().as_secs_f64() * 1000.0,
            self.comparison.p99_query_time().as_secs_f64() * 1000.0,
            self.format_ratio(self.statistics.p99_query_time_ratio, false)
        ));

        // Failed queries section
        if !self.comparison.errors.is_empty() {
            output.push_str(&format!(
                "\n{} Failed Queries in {}:\n",
                self.comparison.fail_count, self.comparison.engine_name
            ));
            for (i, error) in self.comparison.errors.iter().take(10).enumerate() {
                output.push_str(&format!("  {}. {}\n", i + 1, error));
            }
            if self.comparison.errors.len() > 10 {
                output.push_str(&format!("  ... and {} more\n", self.comparison.errors.len() - 10));
            }
        }

        output.push('\n');
        output
    }

    /// Format the report as markdown
    pub fn to_markdown(&self) -> String {
        let mut output = String::new();

        // Header
        output.push_str(&format!("# Performance Comparison: {}\n\n", self.test_name));
        output.push_str(&format!("**Generated:** {}\n\n", self.timestamp));

        // Summary section
        output.push_str("## Summary\n\n");
        output.push_str("| Metric | ");
        output.push_str(&format!("{} | ", self.baseline.engine_name));
        output.push_str(&format!("{} | ", self.comparison.engine_name));
        output.push_str("Ratio |\n");
        output.push_str("|--------|");
        output.push_str("--------|");
        output.push_str("--------|");
        output.push_str("-------|\n");

        // Metrics rows
        output.push_str(&format!(
            "| Total Time | {:.3}s | {:.3}s | {} |\n",
            self.baseline.total_duration.as_secs_f64(),
            self.comparison.total_duration.as_secs_f64(),
            self.format_ratio_markdown(self.statistics.time_ratio, false)
        ));

        output.push_str(&format!(
            "| Peak Memory | {} | {} | {} |\n",
            Self::format_bytes(self.baseline.peak_memory_bytes),
            Self::format_bytes(self.comparison.peak_memory_bytes),
            self.format_ratio_markdown(self.statistics.memory_ratio, false)
        ));

        output.push_str(&format!(
            "| Queries/sec | {:.0} | {:.0} | {} |\n",
            self.baseline.queries_per_second(),
            self.comparison.queries_per_second(),
            self.format_ratio_markdown(self.statistics.qps_ratio, true)
        ));

        output.push_str(&format!(
            "| Avg Query Time | {:.2}ms | {:.2}ms | {} |\n",
            self.baseline.average_query_time().as_secs_f64() * 1000.0,
            self.comparison.average_query_time().as_secs_f64() * 1000.0,
            self.format_ratio_markdown(self.statistics.avg_query_time_ratio, false)
        ));

        output.push_str(&format!(
            "| Pass Rate | {:.1}% | {:.1}% | {}{:.1} pp |\n",
            self.baseline.pass_rate(),
            self.comparison.pass_rate(),
            if self.statistics.pass_rate_diff >= 0.0 { "+" } else { "" },
            self.statistics.pass_rate_diff.abs(),
        ));

        output.push_str(&format!(
            "| Failures | {} | {} | - |\n",
            self.baseline.fail_count, self.comparison.fail_count,
        ));

        // Detailed breakdown
        output.push_str("\n## Performance Breakdown\n\n");
        output.push_str(&format!(
            "- **Median Query Time:** {:.2}ms vs {:.2}ms ({})\n",
            self.baseline.median_query_time().as_secs_f64() * 1000.0,
            self.comparison.median_query_time().as_secs_f64() * 1000.0,
            self.format_ratio_markdown(self.statistics.median_query_time_ratio, false)
        ));
        output.push_str(&format!(
            "- **P95 Query Time:** {:.2}ms vs {:.2}ms ({})\n",
            self.baseline.p95_query_time().as_secs_f64() * 1000.0,
            self.comparison.p95_query_time().as_secs_f64() * 1000.0,
            self.format_ratio_markdown(self.statistics.p95_query_time_ratio, false)
        ));
        output.push_str(&format!(
            "- **P99 Query Time:** {:.2}ms vs {:.2}ms ({})\n",
            self.baseline.p99_query_time().as_secs_f64() * 1000.0,
            self.comparison.p99_query_time().as_secs_f64() * 1000.0,
            self.format_ratio_markdown(self.statistics.p99_query_time_ratio, false)
        ));

        // Failed queries
        if !self.comparison.errors.is_empty() {
            output.push_str(&format!("\n## Failed Queries ({})\n\n", self.comparison.fail_count));
            for (i, error) in self.comparison.errors.iter().take(10).enumerate() {
                output.push_str(&format!("{}. `{}`\n", i + 1, error));
            }
            if self.comparison.errors.len() > 10 {
                output.push_str(&format!(
                    "\n*... and {} more failures*\n",
                    self.comparison.errors.len() - 10
                ));
            }
        }

        output
    }

    /// Format a ratio value with appropriate suffix
    fn format_ratio(&self, ratio: f64, higher_is_better: bool) -> String {
        if ratio == 0.0 {
            return "N/A".to_string();
        }

        let suffix = if ratio > 1.0 {
            if higher_is_better {
                "faster"
            } else {
                "slower"
            }
        } else if ratio < 1.0 {
            if higher_is_better {
                "slower"
            } else {
                "faster"
            }
        } else {
            return "same".to_string();
        };

        format!("{:.2}x {}", ratio, suffix)
    }

    /// Format a ratio value for markdown with emoji indicators
    fn format_ratio_markdown(&self, ratio: f64, higher_is_better: bool) -> String {
        if ratio == 0.0 {
            return "N/A".to_string();
        }

        let (emoji, suffix) = if ratio > 1.0 {
            if higher_is_better {
                ("✅", "faster")
            } else {
                ("⚠️", "slower")
            }
        } else if ratio < 1.0 {
            if higher_is_better {
                ("⚠️", "slower")
            } else {
                ("✅", "faster")
            }
        } else {
            return "same".to_string();
        };

        format!("{} {:.2}x {}", emoji, ratio, suffix)
    }

    /// Format bytes in human-readable form
    fn format_bytes(bytes: usize) -> String {
        const KB: usize = 1024;
        const MB: usize = KB * 1024;
        const GB: usize = MB * 1024;

        if bytes >= GB {
            format!("{:.2} GB", bytes as f64 / GB as f64)
        } else if bytes >= MB {
            format!("{:.2} MB", bytes as f64 / MB as f64)
        } else if bytes >= KB {
            format!("{:.2} KB", bytes as f64 / KB as f64)
        } else {
            format!("{} B", bytes)
        }
    }
}

impl fmt::Display for ComparisonReport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_console())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[allow(dead_code)]
    fn create_sample_metrics(
        name: &str,
        total_ms: u64,
        peak_mem_mb: usize,
        pass_count: usize,
        fail_count: usize,
    ) -> BenchmarkMetrics {
        let mut metrics = BenchmarkMetrics::new(name);

        // Add some query times to create realistic distributions
        let avg_query_time = if pass_count + fail_count > 0 {
            total_ms / (pass_count + fail_count) as u64
        } else {
            0
        };

        for _ in 0..pass_count {
            let duration = Duration::from_millis(avg_query_time);
            metrics.record_success(duration);
        }

        for i in 0..fail_count {
            let duration = Duration::from_millis(avg_query_time);
            metrics.record_failure(duration, format!("Error {}", i));
        }

        metrics.update_peak_memory(peak_mem_mb * 1024 * 1024);
        metrics
    }

    #[test]
    fn test_comparison_report_creation() {
        let baseline = create_sample_metrics("sqlite", 1000, 10, 100, 0);
        let comparison = create_sample_metrics("vibesql", 2000, 20, 95, 5);

        let report = ComparisonReport::new("test.sql", baseline, comparison);

        assert_eq!(report.test_name, "test.sql");
        assert!(!report.timestamp.is_empty());
        assert_eq!(report.baseline.engine_name, "sqlite");
        assert_eq!(report.comparison.engine_name, "vibesql");
    }

    #[test]
    fn test_comparison_statistics() {
        let baseline = create_sample_metrics("sqlite", 1000, 10, 100, 0);
        let comparison = create_sample_metrics("vibesql", 2000, 20, 90, 10);

        let report = ComparisonReport::new("test.sql", baseline, comparison);

        // Time ratio should be ~2.0
        assert!((report.statistics.time_ratio - 2.0).abs() < 0.1);

        // Memory ratio should be ~2.0
        assert!((report.statistics.memory_ratio - 2.0).abs() < 0.1);

        // Pass rate diff should be negative (vibesql has lower pass rate)
        assert!(report.statistics.pass_rate_diff < 0.0);
    }

    #[test]
    fn test_json_output() {
        let baseline = create_sample_metrics("sqlite", 1000, 10, 100, 0);
        let comparison = create_sample_metrics("vibesql", 2000, 20, 95, 5);

        let report = ComparisonReport::new("test.sql", baseline, comparison);
        let json = report.to_json().unwrap();

        // Verify it's valid JSON and can be deserialized
        let deserialized: ComparisonReport = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.test_name, "test.sql");
    }

    #[test]
    fn test_console_output() {
        let baseline = create_sample_metrics("sqlite", 1000, 10, 100, 0);
        let comparison = create_sample_metrics("vibesql", 2000, 20, 95, 5);

        let report = ComparisonReport::new("test.sql", baseline, comparison);
        let console = report.to_console();

        // Verify key sections are present
        assert!(console.contains("Performance Comparison: test.sql"));
        assert!(console.contains("Total Time"));
        assert!(console.contains("Peak Memory"));
        assert!(console.contains("Queries/sec"));
        assert!(console.contains("Pass Rate"));
        assert!(console.contains("Performance Breakdown"));
    }

    #[test]
    fn test_markdown_output() {
        let baseline = create_sample_metrics("sqlite", 1000, 10, 100, 0);
        let comparison = create_sample_metrics("vibesql", 2000, 20, 95, 5);

        let report = ComparisonReport::new("test.sql", baseline, comparison);
        let markdown = report.to_markdown();

        // Verify key markdown sections are present
        assert!(markdown.contains("# Performance Comparison: test.sql"));
        assert!(markdown.contains("## Summary"));
        assert!(markdown.contains("| Metric |"));
        assert!(markdown.contains("## Performance Breakdown"));
        assert!(markdown.contains("## Failed Queries"));
    }

    #[test]
    fn test_format_bytes() {
        assert_eq!(ComparisonReport::format_bytes(512), "512 B");
        assert_eq!(ComparisonReport::format_bytes(2048), "2.00 KB");
        assert_eq!(ComparisonReport::format_bytes(2 * 1024 * 1024), "2.00 MB");
        assert_eq!(ComparisonReport::format_bytes(3 * 1024 * 1024 * 1024), "3.00 GB");
    }

    #[test]
    fn test_display_trait() {
        let baseline = create_sample_metrics("sqlite", 1000, 10, 100, 0);
        let comparison = create_sample_metrics("vibesql", 2000, 20, 95, 5);

        let report = ComparisonReport::new("test.sql", baseline, comparison);
        let display = format!("{}", report);

        // Display should use console format
        assert!(display.contains("Performance Comparison: test.sql"));
    }
}
