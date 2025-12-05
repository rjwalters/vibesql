//! TPC-H Benchmark Module
//!
//! This module provides TPC-H benchmark utilities including:
//! - Data generation (`data` module)
//! - Query definitions (`queries` module)
//! - Schema creation and data loading (`schema` module)

#![allow(dead_code)]
#![allow(unused_imports)]

pub mod data;
pub mod queries;
pub mod schema;

// Re-export commonly used items for convenience
pub use data::{TPCHData, NATIONS, PRIORITIES, REGIONS, SEGMENTS, SHIP_MODES};
pub use queries::*;
pub use schema::load_vibesql;

#[cfg(feature = "sqlite-comparison")]
pub use schema::load_sqlite;
#[cfg(feature = "duckdb-comparison")]
pub use schema::load_duckdb;
#[cfg(feature = "mysql-comparison")]
pub use schema::load_mysql;
