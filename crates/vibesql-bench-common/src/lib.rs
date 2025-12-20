//! Shared benchmark infrastructure for vibesql benchmarks
//!
//! This crate provides common benchmark code used by both `vibesql-executor`
//! and `vibesql-server` benchmark suites:
//!
//! - **TPC-C**: OLTP transaction processing benchmark
//! - **TPC-H**: Decision support (OLAP) benchmark
//! - **TPC-DS**: Decision support benchmark with complex queries
//! - **TPC-E**: Financial trading benchmark
//! - **Sysbench**: MySQL-compatible micro-benchmarks
//! - **Harness**: Timing and statistics infrastructure
//!
//! # Phase 1 - Data Generators and Types
//!
//! This initial version provides portable data generators and type definitions
//! that don't depend on any vibesql crates. Engine-specific schema loading
//! and transaction execution code remains in each benchmark crate.
//!
//! # Usage
//!
//! Add as a dev-dependency in your crate's `Cargo.toml`:
//!
//! ```toml
//! [dev-dependencies]
//! vibesql-bench-common = { path = "../vibesql-bench-common" }
//! ```

pub mod harness;
pub mod sysbench;
pub mod tpcc;
pub mod tpch;

// TPC-DS and TPC-E to be added in Phase 2
// pub mod tpcds;
// pub mod tpce;
