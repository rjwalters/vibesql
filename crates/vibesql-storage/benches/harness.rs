//! Minimal timing harness for storage benchmarks
//!
//! A simplified version of the shared harness from vibesql-executor
//! for storage crate benchmarks.

#![allow(dead_code)]

use std::env;
use std::time::{Duration, Instant};

/// Default configuration for benchmarks
pub const DEFAULT_WARMUP_ITERATIONS: usize = 3;
pub const DEFAULT_BENCHMARK_ITERATIONS: usize = 10;

/// Configuration for a benchmark run
#[derive(Debug, Clone)]
pub struct BenchConfig {
    pub warmup_iterations: usize,
    pub benchmark_iterations: usize,
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
        }
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
            "  {:<30} {:>10.2?} avg ({:>10.2?} - {:>10.2?})",
            self.name, self.mean, self.min, self.max
        );
    }
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

    /// Run a benchmark, measuring execution time automatically
    pub fn run<F, T>(&self, name: &str, mut f: F) -> BenchStats
    where
        F: FnMut() -> T,
    {
        // Warmup phase
        for _ in 0..self.config.warmup_iterations {
            std::hint::black_box(f());
        }

        // Timed phase
        let mut times = Vec::with_capacity(self.config.benchmark_iterations);

        for _ in 0..self.config.benchmark_iterations {
            let start = Instant::now();
            std::hint::black_box(f());
            times.push(start.elapsed());
        }

        BenchStats::from_times(name, times)
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
pub fn print_summary_table(results: &[BenchStats]) {
    eprintln!("\n--- Summary ---");
    eprintln!("{:<35} {:>12} {:>12} {:>12}", "Benchmark", "Mean", "Min", "Max");
    eprintln!("{:-<35} {:->12} {:->12} {:->12}", "", "", "", "");

    for stat in results {
        eprintln!(
            "{:<35} {:>12.2?} {:>12.2?} {:>12.2?}",
            stat.name, stat.mean, stat.min, stat.max
        );
    }
}
