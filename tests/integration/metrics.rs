//! Unit tests for the metrics module

#[path = "../sqllogictest/mod.rs"]
mod sqllogictest;

use std::time::Duration;

use sqllogictest::metrics::{BenchmarkMetrics, MemoryTracker, QueryStopwatch};

#[test]
fn test_benchmark_metrics_creation() {
    let metrics = BenchmarkMetrics::new("test_engine");
    assert_eq!(metrics.engine_name, "test_engine");
    assert_eq!(metrics.query_count, 0);
}

#[test]
fn test_record_operations() {
    let mut metrics = BenchmarkMetrics::new("test");

    metrics.record_success(Duration::from_millis(100));
    assert_eq!(metrics.pass_count, 1);

    metrics.record_failure(Duration::from_millis(50), "error".to_string());
    assert_eq!(metrics.fail_count, 1);
    assert_eq!(metrics.query_count, 2);
}

#[test]
fn test_aggregation_functions() {
    let mut metrics = BenchmarkMetrics::new("test");

    for i in 1..=100 {
        metrics.record_success(Duration::from_millis(i));
    }

    // Average of 1..=100 is 50.5
    let avg = metrics.average_query_time();
    assert!(avg >= Duration::from_millis(50) && avg <= Duration::from_millis(51));

    // Median of 100 items is average of 50th and 51st elements (50ms and 51ms) = 50.5ms
    let median = metrics.median_query_time();
    assert!(
        median >= Duration::from_nanos(50_500_000) && median <= Duration::from_nanos(50_500_001)
    );

    assert_eq!(metrics.p95_query_time(), Duration::from_millis(95));
    assert_eq!(metrics.p99_query_time(), Duration::from_millis(99));
}

#[test]
fn test_stopwatch_timing() {
    let stopwatch = QueryStopwatch::start();
    std::thread::sleep(Duration::from_millis(10));
    let elapsed = stopwatch.stop();

    assert!(elapsed >= Duration::from_millis(10));
}

#[test]
fn test_memory_tracker() {
    let tracker = MemoryTracker::new();
    let _peak = tracker.peak_usage();
    // Just verify it doesn't panic
}

#[test]
fn test_json_serialization() {
    let mut metrics = BenchmarkMetrics::new("test_engine");
    metrics.record_success(Duration::from_millis(100));

    let json = serde_json::to_string(&metrics).unwrap();
    let deserialized: BenchmarkMetrics = serde_json::from_str(&json).unwrap();

    assert_eq!(deserialized.engine_name, "test_engine");
    assert_eq!(deserialized.query_count, 1);
}
