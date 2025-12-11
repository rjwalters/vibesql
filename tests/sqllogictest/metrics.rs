//! Benchmark metrics collection and aggregation for performance comparison.
//!
//! This module provides infrastructure for collecting timing, memory, and correctness
//! metrics during sqllogictest execution. Metrics can be collected for any AsyncDB
//! implementation (vibesql, SQLite, etc.) and exported to JSON for analysis.

use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

/// Detailed metrics for a single SQL query execution
#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryMetrics {
    /// The SQL statement that was executed
    pub sql: String,
    /// Time taken to execute the query
    pub duration: Duration,
    /// Whether the query executed successfully
    pub success: bool,
    /// Error message if execution failed
    pub error: Option<String>,
}

/// Aggregate benchmark metrics for a test file or suite
#[allow(dead_code)]
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BenchmarkMetrics {
    /// Name of the database engine (e.g., "vibesql", "sqlite")
    pub engine_name: String,
    /// Total execution time for all queries
    #[serde(with = "duration_serde")]
    pub total_duration: Duration,
    /// Peak memory usage in bytes during test execution
    pub peak_memory_bytes: usize,
    /// Total number of queries executed
    pub query_count: usize,
    /// Number of queries that passed
    pub pass_count: usize,
    /// Number of queries that failed
    pub fail_count: usize,
    /// Execution time for each query
    #[serde(with = "vec_duration_serde")]
    pub per_query_times: Vec<Duration>,
    /// Error messages encountered during execution
    pub errors: Vec<String>,
}

impl BenchmarkMetrics {
    /// Create a new BenchmarkMetrics instance
    pub fn new(engine_name: impl Into<String>) -> Self {
        Self {
            engine_name: engine_name.into(),
            total_duration: Duration::ZERO,
            peak_memory_bytes: 0,
            query_count: 0,
            pass_count: 0,
            fail_count: 0,
            per_query_times: Vec::new(),
            errors: Vec::new(),
        }
    }

    /// Record a successful query execution
    pub fn record_success(&mut self, duration: Duration) {
        self.query_count += 1;
        self.pass_count += 1;
        self.per_query_times.push(duration);
        self.total_duration += duration;
    }

    /// Record a failed query execution
    pub fn record_failure(&mut self, duration: Duration, error: String) {
        self.query_count += 1;
        self.fail_count += 1;
        self.per_query_times.push(duration);
        self.total_duration += duration;
        self.errors.push(error);
    }

    /// Update peak memory usage if current value is higher
    pub fn update_peak_memory(&mut self, current_bytes: usize) {
        if current_bytes > self.peak_memory_bytes {
            self.peak_memory_bytes = current_bytes;
        }
    }

    /// Calculate pass rate as a percentage
    pub fn pass_rate(&self) -> f64 {
        if self.query_count == 0 {
            0.0
        } else {
            (self.pass_count as f64 / self.query_count as f64) * 100.0
        }
    }

    /// Calculate queries per second
    pub fn queries_per_second(&self) -> f64 {
        let total_secs = self.total_duration.as_secs_f64();
        if total_secs == 0.0 {
            0.0
        } else {
            self.query_count as f64 / total_secs
        }
    }

    /// Calculate average query execution time
    pub fn average_query_time(&self) -> Duration {
        if self.per_query_times.is_empty() {
            Duration::ZERO
        } else {
            let total_nanos: u128 = self.per_query_times.iter().map(|d| d.as_nanos()).sum();
            Duration::from_nanos((total_nanos / self.per_query_times.len() as u128) as u64)
        }
    }

    /// Calculate median query execution time
    pub fn median_query_time(&self) -> Duration {
        if self.per_query_times.is_empty() {
            return Duration::ZERO;
        }

        let mut sorted = self.per_query_times.clone();
        sorted.sort();

        let mid = sorted.len() / 2;
        if sorted.len().is_multiple_of(2) {
            // Even number of elements - average the two middle values
            let sum_nanos = sorted[mid - 1].as_nanos() + sorted[mid].as_nanos();
            Duration::from_nanos((sum_nanos / 2) as u64)
        } else {
            // Odd number of elements - return the middle value
            sorted[mid]
        }
    }

    /// Calculate percentile query execution time (p50, p95, p99, etc.)
    pub fn percentile_query_time(&self, percentile: f64) -> Duration {
        if self.per_query_times.is_empty() {
            return Duration::ZERO;
        }

        assert!((0.0..=100.0).contains(&percentile), "Percentile must be between 0 and 100");

        let mut sorted = self.per_query_times.clone();
        sorted.sort();

        let index = ((percentile / 100.0) * (sorted.len() - 1) as f64).round() as usize;
        sorted[index]
    }

    /// Get p95 query execution time
    pub fn p95_query_time(&self) -> Duration {
        self.percentile_query_time(95.0)
    }

    /// Get p99 query execution time
    pub fn p99_query_time(&self) -> Duration {
        self.percentile_query_time(99.0)
    }
}

/// Stopwatch for measuring query execution time with minimal overhead
#[allow(dead_code)]
#[derive(Debug)]
pub struct QueryStopwatch {
    start: Instant,
}

#[allow(dead_code)]
impl QueryStopwatch {
    /// Start timing a query
    pub fn start() -> Self {
        Self { start: Instant::now() }
    }

    /// Stop timing and return elapsed duration
    pub fn stop(self) -> Duration {
        self.start.elapsed()
    }
}

/// Memory tracker for cross-platform memory usage monitoring
#[allow(dead_code)]
#[derive(Debug)]
pub struct MemoryTracker {
    #[cfg(feature = "memory-tracking")]
    initial_usage: Option<usize>,
}

#[allow(dead_code)]
impl MemoryTracker {
    /// Create a new memory tracker
    pub fn new() -> Self {
        Self {
            #[cfg(feature = "memory-tracking")]
            initial_usage: Self::current_usage(),
        }
    }

    /// Get current memory usage in bytes
    #[cfg(feature = "memory-tracking")]
    fn current_usage() -> Option<usize> {
        memory_stats::memory_stats().map(|stats| stats.physical_mem)
    }

    #[cfg(not(feature = "memory-tracking"))]
    fn current_usage() -> Option<usize> {
        None
    }

    /// Get peak memory usage since tracker creation
    pub fn peak_usage(&self) -> usize {
        #[cfg(feature = "memory-tracking")]
        {
            if let (Some(initial), Some(current)) = (self.initial_usage, Self::current_usage()) {
                current.saturating_sub(initial)
            } else {
                0
            }
        }

        #[cfg(not(feature = "memory-tracking"))]
        {
            0
        }
    }
}

impl Default for MemoryTracker {
    fn default() -> Self {
        Self::new()
    }
}

// Custom serde serialization for Duration (since Duration doesn't implement Serialize by default)
#[allow(dead_code)]
mod duration_serde {
    use std::time::Duration;

    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    #[allow(dead_code)]
    pub fn serialize<S>(duration: &Duration, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        duration.as_secs_f64().serialize(serializer)
    }

    #[allow(dead_code)]
    pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
    where
        D: Deserializer<'de>,
    {
        let secs = f64::deserialize(deserializer)?;
        Ok(Duration::from_secs_f64(secs))
    }
}

#[allow(dead_code)]
mod vec_duration_serde {
    use std::time::Duration;

    use serde::{Deserialize, Deserializer, Serialize, Serializer};

    #[allow(dead_code)]
    pub fn serialize<S>(durations: &[Duration], serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let secs: Vec<f64> = durations.iter().map(|d| d.as_secs_f64()).collect();
        secs.serialize(serializer)
    }

    #[allow(dead_code)]
    pub fn deserialize<'de, D>(deserializer: D) -> Result<Vec<Duration>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let secs = Vec::<f64>::deserialize(deserializer)?;
        Ok(secs.into_iter().map(Duration::from_secs_f64).collect())
    }
}

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use super::*;

    #[test]
    fn test_benchmark_metrics_new() {
        let metrics = BenchmarkMetrics::new("test_engine");
        assert_eq!(metrics.engine_name, "test_engine");
        assert_eq!(metrics.query_count, 0);
        assert_eq!(metrics.pass_count, 0);
        assert_eq!(metrics.fail_count, 0);
    }

    #[test]
    fn test_record_success() {
        let mut metrics = BenchmarkMetrics::new("test");
        metrics.record_success(Duration::from_millis(100));

        assert_eq!(metrics.query_count, 1);
        assert_eq!(metrics.pass_count, 1);
        assert_eq!(metrics.fail_count, 0);
        assert_eq!(metrics.per_query_times.len(), 1);
        assert!(metrics.total_duration >= Duration::from_millis(100));
    }

    #[test]
    fn test_record_failure() {
        let mut metrics = BenchmarkMetrics::new("test");
        metrics.record_failure(Duration::from_millis(50), "Test error".to_string());

        assert_eq!(metrics.query_count, 1);
        assert_eq!(metrics.pass_count, 0);
        assert_eq!(metrics.fail_count, 1);
        assert_eq!(metrics.errors.len(), 1);
    }

    #[test]
    fn test_pass_rate() {
        let mut metrics = BenchmarkMetrics::new("test");
        metrics.record_success(Duration::from_millis(10));
        metrics.record_success(Duration::from_millis(10));
        metrics.record_failure(Duration::from_millis(10), "error".to_string());

        assert_eq!(metrics.pass_rate(), 66.66666666666666);
    }

    #[test]
    fn test_queries_per_second() {
        let mut metrics = BenchmarkMetrics::new("test");
        metrics.record_success(Duration::from_secs(1));
        metrics.record_success(Duration::from_secs(1));

        assert_eq!(metrics.queries_per_second(), 1.0);
    }

    #[test]
    fn test_average_query_time() {
        let mut metrics = BenchmarkMetrics::new("test");
        metrics.record_success(Duration::from_millis(100));
        metrics.record_success(Duration::from_millis(200));
        metrics.record_success(Duration::from_millis(300));

        let avg = metrics.average_query_time();
        assert_eq!(avg, Duration::from_millis(200));
    }

    #[test]
    fn test_median_query_time_odd() {
        let mut metrics = BenchmarkMetrics::new("test");
        metrics.record_success(Duration::from_millis(100));
        metrics.record_success(Duration::from_millis(300));
        metrics.record_success(Duration::from_millis(200));

        let median = metrics.median_query_time();
        assert_eq!(median, Duration::from_millis(200));
    }

    #[test]
    fn test_median_query_time_even() {
        let mut metrics = BenchmarkMetrics::new("test");
        metrics.record_success(Duration::from_millis(100));
        metrics.record_success(Duration::from_millis(200));
        metrics.record_success(Duration::from_millis(300));
        metrics.record_success(Duration::from_millis(400));

        let median = metrics.median_query_time();
        assert_eq!(median, Duration::from_millis(250));
    }

    #[test]
    fn test_percentile_query_time() {
        let mut metrics = BenchmarkMetrics::new("test");
        for i in 1..=100 {
            metrics.record_success(Duration::from_millis(i as u64));
        }

        assert_eq!(metrics.percentile_query_time(0.0), Duration::from_millis(1));
        // 50th percentile of 100 items is at index 49.5, rounds to 50, which is 51ms
        assert_eq!(metrics.percentile_query_time(50.0), Duration::from_millis(51));
        // 95th percentile: 0.95 * 99 = 94.05, rounds to 94, item at index 94 is 95ms
        assert_eq!(metrics.percentile_query_time(95.0), Duration::from_millis(95));
        // 99th percentile: 0.99 * 99 = 98.01, rounds to 98, item at index 98 is 99ms
        assert_eq!(metrics.percentile_query_time(99.0), Duration::from_millis(99));
        assert_eq!(metrics.percentile_query_time(100.0), Duration::from_millis(100));
    }

    #[test]
    fn test_p95_and_p99() {
        let mut metrics = BenchmarkMetrics::new("test");
        for i in 1..=100 {
            metrics.record_success(Duration::from_millis(i as u64));
        }

        assert_eq!(metrics.p95_query_time(), Duration::from_millis(95));
        assert_eq!(metrics.p99_query_time(), Duration::from_millis(99));
    }

    #[test]
    fn test_update_peak_memory() {
        let mut metrics = BenchmarkMetrics::new("test");

        metrics.update_peak_memory(1000);
        assert_eq!(metrics.peak_memory_bytes, 1000);

        metrics.update_peak_memory(500); // Lower value - should not update
        assert_eq!(metrics.peak_memory_bytes, 1000);

        metrics.update_peak_memory(2000); // Higher value - should update
        assert_eq!(metrics.peak_memory_bytes, 2000);
    }

    #[test]
    fn test_query_stopwatch() {
        let stopwatch = QueryStopwatch::start();
        std::thread::sleep(Duration::from_millis(10));
        let elapsed = stopwatch.stop();

        assert!(elapsed >= Duration::from_millis(10));
    }

    #[test]
    fn test_metrics_serialization() {
        let mut metrics = BenchmarkMetrics::new("test_engine");
        metrics.record_success(Duration::from_millis(100));
        metrics.record_failure(Duration::from_millis(50), "test error".to_string());

        let json = serde_json::to_string(&metrics).unwrap();
        let deserialized: BenchmarkMetrics = serde_json::from_str(&json).unwrap();

        assert_eq!(deserialized.engine_name, "test_engine");
        assert_eq!(deserialized.query_count, 2);
        assert_eq!(deserialized.pass_count, 1);
        assert_eq!(deserialized.fail_count, 1);
    }

    #[test]
    fn test_memory_tracker_creation() {
        let tracker = MemoryTracker::new();
        let _peak = tracker.peak_usage();
        // Just ensure it doesn't panic - actual value depends on feature flag
    }
}
