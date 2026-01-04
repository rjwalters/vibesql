// ============================================================================
// BENCHMARK INTEGRITY WARNING
// ============================================================================
// DO NOT add "fast paths", "optimizations", or shortcuts that bypass SQL
// execution in benchmark code. Benchmarks MUST execute actual SQL to produce
// meaningful results. "Optimizing" benchmarks this way is cheating.
// ============================================================================

//! TPC-H Benchmark Module
//!
//! This module provides TPC-H benchmark utilities including:
//! - Data generation (from `vibesql-bench-common`)
//! - Query definitions (from `vibesql-bench-common`)
//! - Schema creation and data loading (from `vibesql-bench-common`)

#![allow(dead_code)]
#![allow(unused_imports)]

// Schema loading re-exports from bench-common
pub mod schema;

// Re-export queries module from shared crate for backwards compatibility
// Re-export schema loaders
#[cfg(feature = "duckdb")]
pub use schema::load_duckdb;
#[cfg(feature = "mysql")]
pub use schema::load_mysql;
#[cfg(feature = "sqlite")]
pub use schema::load_sqlite;
pub use schema::load_vibesql;
pub use vibesql_bench_common::tpch::queries;
// Re-export data generators and queries from shared crate
pub use vibesql_bench_common::tpch::{
    // Helper function
    generate_part_type,
    // Data generation
    TPCHData,
    // Reference data constants
    COLORS,
    CONTAINERS,
    NATIONS,
    PRIORITIES,
    REGIONS,
    SEGMENTS,
    SHIP_MODES,
    TYPE_SYLLABLE1,
    TYPE_SYLLABLE2,
    TYPE_SYLLABLE3,
};
