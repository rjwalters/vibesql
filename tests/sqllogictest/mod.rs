//! SQLLogicTest suite infrastructure modules.
//!
//! This module contains the refactored components of the SQLLogicTest suite:
//! - `db_adapter`: Database adapter implementing AsyncDB trait
//! - `execution`: Test file execution with timeout handling
//! - `formatting`: SQL value formatting utilities
//! - `scheduler`: Test prioritization and worker partitioning
//! - `stats`: Test statistics and failure tracking
//! - `metrics`: Benchmark metrics collection and aggregation
//! - `report`: Comparison report generator with multiple output formats
//!
//! Note: Dialect-specific preprocessing has been removed. Tests now run with
//! automatic dialect switching via the vendored sqllogictest crate's
//! `enable_auto_switch_dialect()` feature. Tests marked with `skipif mysql`
//! or `onlyif sqlite` will run in SQLite mode instead of being skipped.

pub mod db_adapter;
pub mod execution;
pub mod formatting;
pub mod metrics;
pub mod report;
pub mod scheduler;
pub mod stats;
