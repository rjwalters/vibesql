//! Example benchmark test showing how to use the benchmark framework.
//!
//! This demonstrates the basic usage of BenchmarkMetrics and ComparisonReport.
//! For more comprehensive benchmarking, see the sqllogictest_sqlite.rs tests
//! and the BENCHMARKING.md documentation.

use std::time::Duration;

#[path = "../sqllogictest/mod.rs"]
mod sqllogictest;

use sqllogictest::{metrics::BenchmarkMetrics, report::ComparisonReport};

#[test]
fn example_benchmark_metrics() {
    // Create metrics for two "engines" (this is a simplified example)
    let mut sqlite_metrics = BenchmarkMetrics::new("SQLite");
    let mut vibesql_metrics = BenchmarkMetrics::new("VibeSQL");

    // Simulate SQLite execution - faster, less memory
    for i in 1..=100 {
        sqlite_metrics.record_success(Duration::from_millis(i));
    }
    sqlite_metrics.update_peak_memory(10 * 1024 * 1024); // 10 MB

    // Simulate VibeSQL execution - slower, more memory (but functional!)
    for i in 1..=100 {
        vibesql_metrics.record_success(Duration::from_millis(i * 2));
    }
    vibesql_metrics.update_peak_memory(25 * 1024 * 1024); // 25 MB

    // Generate comparison report
    let report = ComparisonReport::new("example.test", sqlite_metrics, vibesql_metrics);

    // Print console output
    println!("\n{}", report.to_console());

    // Verify statistics
    assert!(report.statistics.time_ratio > 1.0, "VibeSQL should be slower in this example");
    assert!(report.statistics.memory_ratio > 1.0, "VibeSQL should use more memory in this example");
}

#[test]
fn example_metrics_aggregation() {
    let mut metrics = BenchmarkMetrics::new("test_engine");

    // Record various query executions
    for i in 1..=100 {
        metrics.record_success(Duration::from_millis(i));
    }

    // Check aggregation functions
    let avg = metrics.average_query_time();
    assert!(avg >= Duration::from_millis(50) && avg <= Duration::from_millis(51));

    let median = metrics.median_query_time();
    assert!(median.as_millis() == 50);

    assert_eq!(metrics.p95_query_time(), Duration::from_millis(95));
    assert_eq!(metrics.p99_query_time(), Duration::from_millis(99));

    assert_eq!(metrics.query_count, 100);
    assert_eq!(metrics.pass_count, 100);
    assert_eq!(metrics.pass_rate(), 100.0);
}

#[test]
fn example_json_serialization() {
    let mut metrics = BenchmarkMetrics::new("test_engine");
    metrics.record_success(Duration::from_millis(100));
    metrics.update_peak_memory(1024 * 1024);

    // Serialize to JSON
    let json = serde_json::to_string_pretty(&metrics).unwrap();
    println!("\n{}", json);

    // Verify can be deserialized
    let deserialized: BenchmarkMetrics = serde_json::from_str(&json).unwrap();
    assert_eq!(deserialized.engine_name, "test_engine");
    assert_eq!(deserialized.query_count, 1);
}

#[test]
fn example_report_formats() {
    let mut sqlite_metrics = BenchmarkMetrics::new("SQLite");
    sqlite_metrics.record_success(Duration::from_secs(1));
    sqlite_metrics.update_peak_memory(10 * 1024 * 1024);

    let mut vibesql_metrics = BenchmarkMetrics::new("VibeSQL");
    vibesql_metrics.record_success(Duration::from_secs(2));
    vibesql_metrics.update_peak_memory(25 * 1024 * 1024);

    let report = ComparisonReport::new("example.test", sqlite_metrics, vibesql_metrics);

    // Console format (human-readable)
    let console_output = report.to_console();
    assert!(console_output.contains("Performance Comparison"));

    // Markdown format (for documentation)
    let markdown = report.to_markdown();
    assert!(markdown.contains("##"));
    assert!(markdown.contains("VibeSQL"));

    // JSON format (for programmatic analysis)
    let json = report.to_json().unwrap();
    assert!(json.contains("example.test"));
    assert!(json.contains("timestamp"));
}
