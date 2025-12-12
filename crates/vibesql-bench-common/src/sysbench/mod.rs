//! Sysbench Benchmark Module
//!
//! This module provides Sysbench benchmark utilities for testing MySQL-compatible
//! OLTP micro-benchmarks. Sysbench tests include:
//!
//! - Point select: Lookup by primary key
//! - Range select: Range scan on primary key
//! - Index scan: Secondary index lookups
//! - Insert: Single-row inserts
//! - Update index: Update with indexed column
//! - Update non-index: Update without indexed column
//! - Delete: Delete by primary key
//!
//! ## Schema
//!
//! The standard sysbench OLTP schema:
//! ```sql
//! CREATE TABLE sbtest1 (
//!   id INTEGER PRIMARY KEY,
//!   k INTEGER NOT NULL DEFAULT 0,
//!   c CHAR(120) NOT NULL DEFAULT '',
//!   padding CHAR(60) NOT NULL DEFAULT ''
//! );
//! CREATE INDEX k_1 ON sbtest1(k);
//! ```
//!
//! ## Usage
//!
//! ```rust,ignore
//! use vibesql_bench_common::sysbench::SysbenchData;
//!
//! // Generate sysbench data for 10,000 rows
//! let mut data = SysbenchData::new(10_000);
//! while let Some((id, k, c, pad)) = data.next_row() {
//!     // Insert row into database
//! }
//!
//! // Generate random query parameters
//! data.reset();
//! let random_id = data.random_id();
//! let (start, end) = data.random_range(100);
//! ```

mod data;

pub use data::*;
