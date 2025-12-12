//! TPC-H Benchmark Module
//!
//! This module provides TPC-H (Transaction Processing Performance Council - H)
//! benchmark utilities for testing decision support / OLAP workload performance.
//!
//! ## Data Generation
//!
//! The `data` module provides generators for creating TPC-H benchmark data:
//! - TPCHData: Main data generator with scale factor support
//! - Reference data constants (NATIONS, REGIONS, SEGMENTS, etc.)
//!
//! ## Queries
//!
//! TPC-H defines 22 queries that represent complex analytical operations:
//! - Q1: Pricing Summary Report
//! - Q2: Minimum Cost Supplier
//! - Q3-Q22: Various analytical queries
//!
//! SQLite-specific variants are provided for queries using EXTRACT(YEAR FROM date)
//! since SQLite uses strftime('%Y', date) instead.
//!
//! ## Usage
//!
//! ```rust,ignore
//! use vibesql_bench_common::tpch::{TPCHData, NATIONS, REGIONS};
//! use vibesql_bench_common::tpch::queries::*;
//!
//! // Create data generator for scale factor 0.01
//! let data = TPCHData::new(0.01);
//!
//! // Get standard TPC-H Q1
//! let query = TPCH_Q1;
//!
//! // For SQLite, use variant queries for Q7, Q8, Q9
//! let query_sqlite = TPCH_Q7_SQLITE;
//! ```

pub mod data;
pub mod queries;

pub use data::*;
pub use queries::*;
