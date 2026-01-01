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
// Re-export existence hash table builders for semi-join and anti-join
pub(super) use build::build_existence_hash_table_parallel;
pub(super) use inner::{hash_join_inner, hash_join_inner_arithmetic, hash_join_inner_multi};
pub(super) use outer::{
    hash_join_left_outer, hash_join_left_outer_multi, hash_join_left_outer_with_filter,
};

// columnar hash join is used directly in inner.rs

// Re-export FromResult type for use in submodules
pub(super) use super::FromResult;

/// Batch combine rows from join index pairs, preserving ROWIDs for each table
///
/// This function tracks row IDs per table for JOIN operations,
/// enabling qualified ROWID references like `t1.rowid`.
///
/// # Arguments
/// * `build_rows` - Rows from the build side of the join
/// * `probe_rows` - Rows from the probe side of the join
/// * `join_pairs` - Index pairs (build_idx, probe_idx) of matching rows
/// * `left_is_build` - True if the left table is the build side
/// * `build_table_names` - Table names for the build side (for ROWID tracking)
/// * `probe_table_names` - Table names for the probe side (for ROWID tracking)
pub(super) fn batch_combine_rows_with_table_names(
    build_rows: &[vibesql_storage::Row],
    probe_rows: &[vibesql_storage::Row],
    join_pairs: &[(usize, usize)],
    left_is_build: bool,
    build_table_names: &[String],
    probe_table_names: &[String],
) -> Vec<vibesql_storage::Row> {
    if join_pairs.is_empty() {
        return Vec::new();
    }

    // Pre-allocate result vector with exact capacity
    let mut result_rows = Vec::with_capacity(join_pairs.len());

    // Determine left/right table names based on build order
    let (left_table_names, right_table_names) = if left_is_build {
        (build_table_names, probe_table_names)
    } else {
        (probe_table_names, build_table_names)
    };

    for &(build_idx, probe_idx) in join_pairs {
        let build_row = &build_rows[build_idx];
        let probe_row = &probe_rows[probe_idx];

        // Use Row::combine_for_join to handle both values and ROWIDs
        let result_row = if left_is_build {
            vibesql_storage::Row::combine_for_join(
                build_row,
                probe_row,
                left_table_names,
                right_table_names,
            )
        } else {
            vibesql_storage::Row::combine_for_join(
                probe_row,
                build_row,
                left_table_names,
                right_table_names,
            )
        };

        result_rows.push(result_row);
    }

    result_rows
}
