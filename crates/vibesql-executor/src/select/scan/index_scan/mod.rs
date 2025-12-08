//! Index scan execution
//!
//! This module provides index-based table scanning for improved query performance.
//! It integrates with the index catalog to use user-defined indexes when beneficial.
//!
//! # Module Organization
//!
//! - `selection`: Index selection logic - determines when and which index to use
//! - `predicate`: Predicate extraction - extracts range/IN predicates from WHERE clauses
//! - `execution`: Index scan execution - performs the actual index scan and fetches rows
//! - `covering`: Covering index scan - returns data directly from index keys (index-only scan)
//!
//! # Public API
//!
//! The main entry points are:
//! - `should_use_index_scan()`: Determines if an index scan is beneficial
//! - `execute_index_scan()`: Executes an index scan to retrieve rows
//! - `try_covering_index_scan()`: Attempts a covering index scan (index-only)

pub(crate) mod covering;
mod execution;
pub(crate) mod predicate;
pub(crate) mod selection;

// Re-export public APIs
pub(crate) use execution::execute_index_scan;
pub(super) use execution::execute_skip_scan;
pub(crate) use selection::cost_based_index_selection;
pub(crate) use selection::select_index_scan_method;
pub(crate) use selection::IndexScanChoice;
// predicate types are accessed directly via predicate::* for better type clarity
