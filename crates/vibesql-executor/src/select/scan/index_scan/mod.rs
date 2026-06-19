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
// MULTI-INDEX OR branch analysis (epic #5668, PR 1). Pure plan representation +
// analysis; not yet wired into selection/execution (no behavior change).
pub(crate) mod or_analysis;
pub(crate) mod predicate;
pub(crate) mod selection;

// Re-export public APIs
pub(crate) use execution::execute_index_scan;
// MULTI-INDEX OR execution (epic #5668, PR 2).
pub(crate) use execution::execute_multi_index_or;
pub(super) use execution::execute_skip_scan;
pub(crate) use selection::{
    cost_based_index_selection, eqp_ordering_index, multi_index_or_enabled,
    needs_temp_btree_for_order_by_eqp, select_index_scan_method, IndexScanChoice,
};
// predicate types are accessed directly via predicate::* for better type clarity
