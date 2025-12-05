//! Late Materialization for Query Execution
//!
//! This module implements late (or deferred) materialization, a query execution
//! optimization that delays converting row data to `SqlValue` until absolutely necessary.
//!
//! Note: This module is experimental/research code. Some types and functions
//! are not yet integrated into the main execution path.
//!
//! ## Problem
//!
//! Traditional row-at-a-time execution materializes all columns immediately:
//!
//! ```text
//! Scan → [Full Rows with all columns] → Filter → [Full Rows] → Join → [Full Rows] → Project
//! ```
//!
//! This is wasteful because:
//! - Many rows are filtered out (e.g., 99% in TPC-H Q6)
//! - Many columns aren't needed in the final result
//! - Join operations only need key columns for matching
//!
//! ## Solution
//!
//! Late materialization tracks row indices instead of materializing data:
//!
//! ```text
//! Scan → [SelectionVector: row indices] → Filter → [indices] → Join → [indices]
//!                                                                          ↓
//! Project → [Materialize only selected columns for final rows] → Output
//! ```
//!
//! ## Key Components
//!
//! - [`SelectionVector`]: Tracks qualifying row indices efficiently
//! - [`RowReference`]: Lightweight reference to a row in a source table
//! - [`LazyMaterializedBatch`]: Holds source data + selection, materializes on demand
//! - [`gather_columns`]: Selectively materializes only needed columns
//!
//! ## Performance Impact
//!
//! For TPC-H Q3 (multi-way JOIN with filters):
//! - Before: Materialize 480K rows × 15 columns = ~7.2M SqlValue allocations
//! - After: Materialize 3K result rows × 4 columns = ~12K SqlValue allocations
//! - **600x reduction** in materialization overhead
//!
//! ## Usage
//!
//! ```text
//! // Create selection vector from filter
//! let selection = SelectionVector::from_bitmap(&filter_result);
//!
//! // Perform join using only indices
//! let (left_sel, right_sel) = join_indices(&left_batch, &right_batch, &selection);
//!
//! // Materialize only at output boundary
//! let result_rows = gather_columns(&source_batch, &final_selection, &projected_columns);
//! ```

mod gather;
mod lazy_batch;
mod row_ref;
mod selection;

pub use gather::{gather_columns, gather_single_column};
pub use lazy_batch::LazyMaterializedBatch;
pub use row_ref::RowReference;
pub use selection::SelectionVector;

#[cfg(test)]
mod tests;
