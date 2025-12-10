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

// SQL constants for consistent column naming across engines
pub use schema::INSERT_SQL;
#[cfg(any(feature = "sqlite", feature = "duckdb"))]
pub use schema::INSERT_SQL_NUMBERED;

#[cfg(feature = "duckdb")]
pub use schema::load_duckdb;
#[cfg(feature = "mysql")]
pub use schema::load_mysql;
#[cfg(feature = "sqlite")]
pub use schema::load_sqlite;
