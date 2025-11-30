//! TPC-DS Benchmark Module
//!
//! This module provides TPC-DS benchmark utilities including:
//! - Data generation (`data` module)
//! - Query definitions (`queries` module)
//! - Schema creation and data loading (`schema` module)
//!
//! TPC-DS is a decision support benchmark that models several generally
//! applicable aspects of a decision support system including queries and
//! data maintenance. It features 99 queries that stress-test analytical
//! SQL capabilities.
//!
//! Reference: https://www.tpc.org/tpcds/

#![allow(dead_code)]
#![allow(unused_imports)]

pub mod data;
pub mod memory;
pub mod queries;
pub mod schema;

// Re-export commonly used items for convenience
pub use data::{TPCDSConfig, TPCDSData, TimeGranularity};
pub use memory::{get_memory_usage, hint_memory_release, MemoryStats, MemoryTracker};
pub use queries::*;
pub use schema::load_vibesql;

#[cfg(feature = "benchmark-comparison")]
pub use schema::{load_duckdb, load_sqlite};
