//! TPC-C Schema Creation and Data Loading
//!
//! This module re-exports schema creation and data loading functions from
//! `vibesql-bench-common` for TPC-C benchmark tables across multiple database
//! engines (VibeSQL, SQLite, DuckDB, MySQL).

// Re-export all schema loading functions from the shared crate
pub use vibesql_bench_common::tpcc::schema::*;
