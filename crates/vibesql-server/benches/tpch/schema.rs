// ============================================================================
// BENCHMARK INTEGRITY WARNING
// ============================================================================
// DO NOT add "fast paths", "optimizations", or shortcuts that bypass SQL
// execution in benchmark code. Benchmarks MUST execute actual SQL to produce
// meaningful results. "Optimizing" benchmarks this way is cheating.
// ============================================================================

//! TPC-H Schema Creation and Data Loading
//!
//! This module re-exports schema creation and data loading functions from
//! `vibesql-bench-common` for TPC-H benchmark tables across multiple database
//! engines (VibeSQL, SQLite, DuckDB, MySQL).

// Re-export all schema loading functions from the shared crate
pub use vibesql_bench_common::tpch::schema::*;
