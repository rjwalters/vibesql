//! Sysbench OLTP Benchmark Module
//!
//! This module provides sysbench-compatible OLTP benchmarks including:
//! - Data generation (from `vibesql-bench-common`)
//! - Schema creation and data loading (`schema` module - engine-specific)
//! - OLTP query workloads (point select, insert, read/write mix, range queries)
//!
//! The schema matches the standard sysbench OLTP schema:
//! <https://github.com/akopytov/sysbench>

#![allow(dead_code)]
#![allow(unused_imports)]

// Engine-specific schema loading code (stays here due to vibesql dependencies)
pub mod schema;

// Re-export data generators from shared crate
pub use vibesql_bench_common::sysbench::{SysbenchData, DEFAULT_TABLE_SIZE};

// Re-export schema loaders
#[cfg(feature = "duckdb")]
pub use schema::load_duckdb;
#[cfg(feature = "mysql")]
pub use schema::load_mysql;
#[cfg(feature = "sqlite")]
pub use schema::load_sqlite;
pub use schema::load_vibesql;
// SQL constants for consistent column naming across engines
pub use schema::INSERT_SQL;
#[cfg(any(feature = "sqlite", feature = "duckdb"))]
pub use schema::INSERT_SQL_NUMBERED;
