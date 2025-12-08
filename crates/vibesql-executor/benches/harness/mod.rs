//! Shared timing harness for benchmarks
//!
//! This module provides a consistent timing infrastructure for TPC-H, TPC-DS,
//! Sysbench, and other benchmarks. It replaces the Criterion dependency with
//! a simpler, deterministic timing approach that:
//!
//! - Runs a fixed number of iterations (configurable via environment variables)
//! - Reports min/max/avg/median statistics
//! - Supports warmup phases
//! - Produces consistent output for parsing by scripts/bench
//!
//! Environment Variables:
//!   WARMUP_ITERATIONS - Number of warmup runs (default: 3)
//!   BENCHMARK_ITERATIONS - Number of timed runs (default: 10)
//!   BENCHMARK_TIMEOUT_SECS - Timeout per query (default: 30)

// Allow unused functions - this is a shared utility module and not all
// benchmarks use every function. These will be used as more benchmarks
// migrate to this harness.
#![allow(dead_code)]

use std::env;
use std::time::{Duration, Instant};

/// Default configuration for benchmarks
pub const DEFAULT_WARMUP_ITERATIONS: usize = 3;
pub const DEFAULT_BENCHMARK_ITERATIONS: usize = 10;
pub const DEFAULT_TIMEOUT_SECS: u64 = 30;

/// Configuration for a benchmark run
#[derive(Debug, Clone)]
pub struct BenchConfig {
    pub warmup_iterations: usize,
    pub benchmark_iterations: usize,
    pub timeout: Duration,
}

impl Default for BenchConfig {
    fn default() -> Self {
        Self {
            warmup_iterations: env::var("WARMUP_ITERATIONS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(DEFAULT_WARMUP_ITERATIONS),
            benchmark_iterations: env::var("BENCHMARK_ITERATIONS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(DEFAULT_BENCHMARK_ITERATIONS),
            timeout: Duration::from_secs(
                env::var("BENCHMARK_TIMEOUT_SECS")
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .unwrap_or(DEFAULT_TIMEOUT_SECS),
            ),
        }
    }
}

impl BenchConfig {
    /// Create a new config with specified values
    pub fn new(warmup_iterations: usize, benchmark_iterations: usize, timeout_secs: u64) -> Self {
        Self {
            warmup_iterations,
            benchmark_iterations,
            timeout: Duration::from_secs(timeout_secs),
        }
    }

    /// Create a config for smoke tests (minimal iterations)
    pub fn smoke() -> Self {
        Self { warmup_iterations: 1, benchmark_iterations: 3, timeout: Duration::from_secs(5) }
    }

    /// Create a config for quick CI tests
    pub fn quick() -> Self {
        Self { warmup_iterations: 2, benchmark_iterations: 5, timeout: Duration::from_secs(10) }
    }
}

/// Statistics collected from a benchmark run
#[derive(Debug, Clone)]
pub struct BenchStats {
    pub name: String,
    pub iterations: usize,
    pub times: Vec<Duration>,
    pub min: Duration,
    pub max: Duration,
    pub mean: Duration,
    pub median: Duration,
    pub stddev: Duration,
}

impl BenchStats {
    /// Calculate statistics from a collection of timing samples
    pub fn from_times(name: &str, times: Vec<Duration>) -> Self {
        let iterations = times.len();
        if iterations == 0 {
            return Self {
                name: name.to_string(),
                iterations: 0,
                times: vec![],
                min: Duration::ZERO,
                max: Duration::ZERO,
                mean: Duration::ZERO,
                median: Duration::ZERO,
                stddev: Duration::ZERO,
            };
        }

        let mut sorted = times.clone();
        sorted.sort();

        let min = sorted[0];
        let max = sorted[iterations - 1];
        let median = sorted[iterations / 2];

        // Calculate mean
        let total: Duration = times.iter().sum();
        let mean = total / iterations as u32;

        // Calculate standard deviation
        let mean_nanos = mean.as_nanos() as f64;
        let variance: f64 = times
            .iter()
            .map(|t| {
                let diff = t.as_nanos() as f64 - mean_nanos;
                diff * diff
            })
            .sum::<f64>()
            / iterations as f64;
        let stddev = Duration::from_nanos(variance.sqrt() as u64);

        Self { name: name.to_string(), iterations, times, min, max, mean, median, stddev }
    }

    /// Print statistics in a format compatible with existing benchmark parsers
    pub fn print(&self) {
        eprintln!("  {} ({} iterations):", self.name, self.iterations);
        eprintln!("    Min:    {:>10.2?}", self.min);
        eprintln!("    Max:    {:>10.2?}", self.max);
        eprintln!("    Mean:   {:>10.2?}", self.mean);
        eprintln!("    Median: {:>10.2?}", self.median);
        eprintln!("    StdDev: {:>10.2?}", self.stddev);
    }

    /// Print in a compact single-line format for comparison tables
    pub fn print_compact(&self) {
        eprintln!(
            "  {:<20} {:>10.2?} avg ({:>10.2?} - {:>10.2?})",
            self.name, self.mean, self.min, self.max
        );
    }
}

/// Result of running a single benchmark iteration
#[derive(Debug)]
pub enum BenchResult {
    /// Successful execution with timing
    Ok(Duration),
    /// Execution timed out
    Timeout,
    /// Execution failed with error
    Error(String),
}

/// Benchmark harness for running and timing operations
pub struct Harness {
    config: BenchConfig,
}

impl Harness {
    /// Create a new harness with default configuration (from environment)
    pub fn new() -> Self {
        Self { config: BenchConfig::default() }
    }

    /// Create a harness with specific configuration
    pub fn with_config(config: BenchConfig) -> Self {
        Self { config }
    }

    /// Run a benchmark with the given function
    ///
    /// The function should perform a single iteration of the benchmark.
    /// Returns statistics from all iterations.
    pub fn run<F>(&self, name: &str, mut f: F) -> BenchStats
    where
        F: FnMut() -> BenchResult,
    {
        // Warmup phase
        for _ in 0..self.config.warmup_iterations {
            let _ = f();
        }

        // Timed phase
        let mut times = Vec::with_capacity(self.config.benchmark_iterations);

        for _ in 0..self.config.benchmark_iterations {
            match f() {
                BenchResult::Ok(duration) => times.push(duration),
                BenchResult::Timeout => {
                    eprintln!("  {} TIMEOUT", name);
                    break;
                }
                BenchResult::Error(e) => {
                    eprintln!("  {} ERROR: {}", name, e);
                    break;
                }
            }
        }

        BenchStats::from_times(name, times)
    }

    /// Run a benchmark that self-times (returns Duration directly)
    ///
    /// This is useful when the benchmark needs to control its own timing,
    /// such as when running queries with internal timing.
    pub fn run_timed<F>(&self, name: &str, mut f: F) -> BenchStats
    where
        F: FnMut() -> Result<Duration, String>,
    {
        self.run(name, || match f() {
            Ok(d) => BenchResult::Ok(d),
            Err(e) => BenchResult::Error(e),
        })
    }

    /// Run a benchmark, measuring execution time automatically
    ///
    /// This wraps the function call with Instant::now() timing.
    pub fn run_auto_timed<F, T>(&self, name: &str, mut f: F) -> BenchStats
    where
        F: FnMut() -> Result<T, String>,
    {
        self.run(name, || {
            let start = Instant::now();
            match f() {
                Ok(_) => BenchResult::Ok(start.elapsed()),
                Err(e) => BenchResult::Error(e),
            }
        })
    }

    /// Get the configured timeout duration
    pub fn timeout(&self) -> Duration {
        self.config.timeout
    }
}

impl Default for Harness {
    fn default() -> Self {
        Self::new()
    }
}

/// Print a benchmark group header
pub fn print_group_header(name: &str) {
    eprintln!("\n=== {} ===", name);
}

/// Print a benchmark group summary table
pub fn print_summary_table(engine_name: &str, results: &[BenchStats]) {
    eprintln!("\n--- {} Results ---", engine_name);
    eprintln!("{:<25} {:>12} {:>12} {:>12}", "Benchmark", "Mean", "Min", "Max");
    eprintln!("{:-<25} {:->12} {:->12} {:->12}", "", "", "", "");

    for stat in results {
        eprintln!(
            "{:<25} {:>12.2?} {:>12.2?} {:>12.2?}",
            stat.name, stat.mean, stat.min, stat.max
        );
    }
}

/// Print a comparison table across multiple engines
pub fn print_comparison_table(results: &[(&str, Vec<BenchStats>)]) {
    if results.is_empty() {
        return;
    }

    // Get all unique benchmark names
    let mut bench_names: Vec<&str> = Vec::new();
    if let Some((_, stats)) = results.first() {
        for s in stats {
            bench_names.push(&s.name);
        }
    }

    eprintln!("\n=== Comparison Summary ===");

    for bench_name in bench_names {
        eprintln!("\n{}", bench_name);
        eprintln!("{:<12} {:>12} {:>12} {:>12}", "Engine", "Mean", "Min", "Max");
        eprintln!("{:-<12} {:->12} {:->12} {:->12}", "", "", "", "");

        for (engine, stats) in results {
            if let Some(stat) = stats.iter().find(|s| s.name == bench_name) {
                eprintln!(
                    "{:<12} {:>12.2?} {:>12.2?} {:>12.2?}",
                    engine, stat.mean, stat.min, stat.max
                );
            }
        }
    }
}

/// Format a duration for display
pub fn format_duration(d: Duration) -> String {
    if d.as_secs() >= 1 {
        format!("{:.2}s", d.as_secs_f64())
    } else if d.as_millis() >= 1 {
        format!("{:.2}ms", d.as_secs_f64() * 1000.0)
    } else {
        format!("{:.2}us", d.as_secs_f64() * 1_000_000.0)
    }
}

#[cfg(test)]
mod tests {
    #[allow(unused_imports)]
    use super::*;

    #[test]
    fn test_bench_stats_from_times() {
        let times = vec![
            Duration::from_millis(10),
            Duration::from_millis(20),
            Duration::from_millis(15),
            Duration::from_millis(25),
            Duration::from_millis(12),
        ];

        let stats = BenchStats::from_times("test", times);

        assert_eq!(stats.iterations, 5);
        assert_eq!(stats.min, Duration::from_millis(10));
        assert_eq!(stats.max, Duration::from_millis(25));
        // Median of [10, 12, 15, 20, 25] is 15
        assert_eq!(stats.median, Duration::from_millis(15));
    }

    #[test]
    fn test_empty_stats() {
        let stats = BenchStats::from_times("empty", vec![]);
        assert_eq!(stats.iterations, 0);
        assert_eq!(stats.min, Duration::ZERO);
    }
}
