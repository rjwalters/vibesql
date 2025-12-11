//! Cost estimation for query execution plans
//!
//! This module provides cost models for different access methods:
//! - Table scan (sequential scan)
//! - Index scan (B-tree lookup + random access)
//!
//! And DML operations:
//! - INSERT (row storage + index maintenance)
//! - UPDATE (row modification + selective index updates)
//! - DELETE (bitmap marking + index removal + potential compaction)
//!
//! Costs are estimated in arbitrary units representing relative work,
//! not absolute time. The optimizer uses these costs to compare different
//! execution strategies and choose the most efficient one.
//!
//! # Module Structure
//!
//! - [`estimator`] - Core `CostEstimator` struct and cost parameters
//! - [`types`] - `AccessMethod`, `TableIndexInfo`, and row size estimation
//! - [`scan`] - Scan cost estimation (table scan, index scan, skip-scan)
//! - [`dml`] - DML cost estimation (INSERT, UPDATE, DELETE)

mod dml;
mod estimator;
mod scan;
mod types;

// Re-export all public types
pub use estimator::CostEstimator;
pub use types::{
    estimate_row_size, estimate_type_size, AccessMethod, TableIndexInfo, BASE_ROW_SIZE,
    MAX_WAL_SIZE_FACTOR,
};

#[cfg(test)]
mod tests;
