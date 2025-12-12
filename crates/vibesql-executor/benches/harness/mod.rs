//! Shared timing harness for benchmarks
//!
//! This module re-exports the harness infrastructure from `vibesql-bench-common`.
//! See that crate for full documentation.
//!
//! Environment Variables:
//!   ENGINE_FILTER - Comma-separated list of engines to run (default: all)
//!                   Valid values: vibesql, sqlite, duckdb, mysql, all
//!   WARMUP_ITERATIONS - Number of warmup runs (default: 3)
//!   BENCHMARK_ITERATIONS - Number of timed runs (default: 10)
//!   BENCHMARK_TIMEOUT_SECS - Timeout per query (default: 30)
//!   SUPPRESS_COMPARISON_SUMMARY - Set to "1" or "true" to suppress comparison summary output

// Re-export everything from the shared crate
#[allow(unused_imports)]
pub use vibesql_bench_common::harness::*;
