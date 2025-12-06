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
pub(super) use outer::hash_join_left_outer_multi;

// Re-export existence hash table builders for semi-join and anti-join
pub(super) use build::build_existence_hash_table_parallel;

// columnar hash join is used directly in inner.rs

// Re-export FromResult type for use in submodules
pub(super) use super::FromResult;

/// Batch combine rows from join index pairs with optimized allocation
///
/// This function reduces allocation overhead by:
/// 1. Pre-allocating the result vector with exact capacity
/// 2. Pre-computing combined row size to avoid repeated capacity calculations
///
/// For large join results (e.g., 60K rows in TPC-H Q19), this provides
/// significant performance improvement over per-row allocations.
pub(super) fn batch_combine_rows(
    build_rows: &[vibesql_storage::Row],
    probe_rows: &[vibesql_storage::Row],
    join_pairs: &[(usize, usize)],
    left_is_build: bool,
) -> Vec<vibesql_storage::Row> {
    if join_pairs.is_empty() {
        return Vec::new();
    }

    // Pre-allocate result vector with exact capacity
    let mut result_rows = Vec::with_capacity(join_pairs.len());

    // Calculate combined row size from first pair for consistent allocation
    let (first_build_idx, first_probe_idx) = join_pairs[0];
    let combined_size =
        build_rows[first_build_idx].values.len() + probe_rows[first_probe_idx].values.len();

    for &(build_idx, probe_idx) in join_pairs {
        // Pre-allocate combined values vector with exact size
        let mut combined_values = Vec::with_capacity(combined_size);

        // Combine rows in correct order (left first, then right)
        if left_is_build {
            combined_values.extend_from_slice(&build_rows[build_idx].values);
            combined_values.extend_from_slice(&probe_rows[probe_idx].values);
        } else {
            combined_values.extend_from_slice(&probe_rows[probe_idx].values);
            combined_values.extend_from_slice(&build_rows[build_idx].values);
        }

        result_rows.push(vibesql_storage::Row::new(combined_values));
    }

    result_rows
}
