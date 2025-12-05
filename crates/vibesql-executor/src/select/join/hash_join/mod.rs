// Hash join module - Optimized equi-join algorithm
//
// This module provides hash join implementations for INNER and OUTER JOINs,
// offering O(n+m) time complexity compared to O(n*m) for nested loop joins.
//
// Module structure:
// - build.rs: Hash table building (sequential, parallel, and composite key)
// - inner.rs: INNER JOIN implementation (single and multi-column)
// - outer.rs: LEFT/RIGHT/FULL OUTER JOIN implementations
// - columnar/: Columnar hash join module (high-performance, no row materialization)
//   - hash_table.rs: Hash table structures for single and multi-column keys
//   - probe.rs: Probe phase implementations for inner and outer joins
//   - output.rs: Result construction and column gathering
//   - row_extract.rs: Row-to-column extraction utilities
// - tests.rs: Comprehensive test suite

mod build;
pub mod columnar;
mod inner;
mod outer;

#[cfg(test)]
mod tests;

// Re-export public API
pub(super) use inner::hash_join_inner;
pub(super) use inner::hash_join_inner_arithmetic;
pub(super) use inner::hash_join_inner_multi;
pub(super) use outer::hash_join_left_outer;

// Re-export existence hash table builders for semi-join and anti-join
pub(super) use build::build_existence_hash_table_parallel;

// columnar hash join is used directly in inner.rs

// Re-export FromResult type for use in submodules
pub(super) use super::FromResult;

/// Helper function to combine two rows without unnecessary cloning
/// Only creates a single combined row, avoiding intermediate clones
#[inline]
pub(super) fn combine_rows(
    left_row: &vibesql_storage::Row,
    right_row: &vibesql_storage::Row,
) -> vibesql_storage::Row {
    let mut combined_values = Vec::with_capacity(left_row.values.len() + right_row.values.len());
    combined_values.extend_from_slice(&left_row.values);
    combined_values.extend_from_slice(&right_row.values);
    vibesql_storage::Row::new(combined_values)
}
