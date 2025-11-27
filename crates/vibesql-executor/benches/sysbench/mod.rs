//! Sysbench OLTP Benchmark Module
//!
//! This module provides sysbench-compatible OLTP benchmarks including:
//! - Data generation for sbtest tables
//! - Schema creation and data loading
//! - OLTP query workloads (point select, insert, read/write mix, range queries)
//!
//! The schema matches the standard sysbench OLTP schema:
//! <https://github.com/akopytov/sysbench>

#![allow(dead_code)]
#![allow(unused_imports)]

pub mod data;
pub mod schema;

// Re-export commonly used items for convenience
pub use data::SysbenchData;
pub use schema::load_vibesql;

#[cfg(feature = "benchmark-comparison")]
pub use schema::{load_duckdb, load_sqlite};
