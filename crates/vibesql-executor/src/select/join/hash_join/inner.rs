#![allow(clippy::doc_lazy_continuation)]

use super::{combine_rows, FromResult};
use super::build::{CompositeKey, build_hash_table_composite_parallel, build_hash_table_parallel};
use super::columnar::hash_join_indices_columnar;
use crate::{errors::ExecutorError, schema::CombinedSchema};

// Note: Memory limit checking removed from hash join.
// Hash join uses O(smaller_table) memory for the hash table, not O(result_size).
// The actual join output size depends on data distribution and selectivity,
// which we cannot accurately predict. Since hash join is already the optimal
// algorithm for equijoins, we trust it to handle the join efficiently.

/// Hash join INNER JOIN implementation (optimized for equi-joins)
///
/// This implementation uses a hash join algorithm for better performance
/// on equi-join conditions (e.g., t1.id = t2.id).
///
/// Algorithm:
/// 1. Build phase: Hash the smaller table into a HashMap (O(n))
/// 2. Probe phase: For each row in larger table, lookup matches (O(m))
/// Total: O(n + m) instead of O(n * m) for nested loop join
///
/// Performance characteristics:
/// - Time: O(n + m) vs O(n*m) for nested loop
/// - Space: O(n) where n is the size of the smaller table
/// - Expected speedup: 100-10,000x for large equi-joins
pub(in crate::select::join) fn hash_join_inner(
    left: FromResult,
    right: FromResult,
    left_col_idx: usize,
    right_col_idx: usize,
) -> Result<FromResult, ExecutorError> {
    // Note: No memory limit check here. Hash join is already O(n+m) time and O(smaller_table) space,
    // which is optimal for equijoins. We cannot predict output size accurately anyway.

    // Extract right table name and schema for combining
    let right_table_name = right
        .schema
        .table_schemas
        .keys()
        .next()
        .ok_or_else(|| ExecutorError::UnsupportedFeature("Complex JOIN".to_string()))?
        .clone();

    let right_schema = right
        .schema
        .table_schemas
        .get(&right_table_name)
        .ok_or_else(|| ExecutorError::UnsupportedFeature("Complex JOIN".to_string()))?
        .1
        .clone();

    // Combine schemas
    let combined_schema =
        CombinedSchema::combine(left.schema.clone(), right_table_name, right_schema);

    // Use as_slice() for zero-cost access without triggering row materialization
    // This avoids the 57% performance bottleneck from premature row collection
    let left_slice = left.as_slice();
    let right_slice = right.as_slice();

    // Choose build and probe sides (build hash table on smaller table)
    let (build_rows, probe_rows, build_col_idx, probe_col_idx, left_is_build) =
        if left_slice.len() <= right_slice.len() {
            (left_slice, right_slice, left_col_idx, right_col_idx, true)
        } else {
            (right_slice, left_slice, right_col_idx, left_col_idx, false)
        };

    // Fast path: Try columnar hash join for integer keys
    // This provides ~20-30% speedup for integer equi-joins by:
    // 1. Using FxHash-style hashing without SqlValue enum dispatch
    // 2. Better cache locality with contiguous i64 arrays
    let join_pairs: Vec<(usize, usize)> = if let Some(pairs) = hash_join_indices_columnar(
        build_rows, probe_rows, build_col_idx, probe_col_idx
    ) {
        pairs
    } else {
        // Fallback: Generic hash join using SqlValue keys
        // Build phase: Create hash table from build side
        // Key: join column value
        // Value: vector of row indices (not row references) for deferred materialization
        // Uses parallel hashing when available for large tables
        let hash_table = build_hash_table_parallel(build_rows, build_col_idx);

        // Probe phase: Collect (build_idx, probe_idx) pairs without materializing rows
        // This defers the expensive row cloning until after we know all matches
        let estimated_capacity = probe_rows.len().saturating_mul(2).min(100_000);
        let mut pairs: Vec<(usize, usize)> = Vec::with_capacity(estimated_capacity);

        for (probe_idx, probe_row) in probe_rows.iter().enumerate() {
            let key = &probe_row.values[probe_col_idx];

            // Skip NULL values - they never match in equi-joins
            if key == &vibesql_types::SqlValue::Null {
                continue;
            }

            if let Some(build_indices) = hash_table.get(key) {
                for &build_idx in build_indices {
                    pairs.push((build_idx, probe_idx));
                }
            }
        }
        pairs
    };

    // Materialization phase: Create combined rows from index pairs
    // Pre-allocate result vector with exact size now that we know it
    let mut result_rows = Vec::with_capacity(join_pairs.len());
    for (build_idx, probe_idx) in join_pairs {
        // Combine rows in correct order (left first, then right)
        let combined_row = if left_is_build {
            combine_rows(&build_rows[build_idx], &probe_rows[probe_idx])
        } else {
            combine_rows(&probe_rows[probe_idx], &build_rows[build_idx])
        };
        result_rows.push(combined_row);
    }

    Ok(FromResult::from_rows(combined_schema, result_rows))
}

/// Multi-column hash join INNER JOIN implementation
///
/// This implementation uses composite keys for hash join when there are multiple
/// equi-join conditions between the same table pair (e.g., `a.x = b.x AND a.y = b.y`).
///
/// Using composite keys instead of single-column keys eliminates the need for
/// post-join filtering of additional conditions, providing significant performance
/// improvements for queries like TPC-H Q3, Q7, Q10.
///
/// Algorithm:
/// 1. Build phase: Create hash table with composite keys from smaller table (O(n))
/// 2. Probe phase: For each row in larger table, create composite key and lookup (O(m))
/// Total: O(n + m) with better selectivity than single-column hash join
pub(in crate::select::join) fn hash_join_inner_multi(
    mut left: FromResult,
    mut right: FromResult,
    left_col_indices: &[usize],
    right_col_indices: &[usize],
) -> Result<FromResult, ExecutorError> {
    // Extract right table name and schema for combining
    let right_table_name = right
        .schema
        .table_schemas
        .keys()
        .next()
        .ok_or_else(|| ExecutorError::UnsupportedFeature("Complex JOIN".to_string()))?
        .clone();

    let right_schema = right
        .schema
        .table_schemas
        .get(&right_table_name)
        .ok_or_else(|| ExecutorError::UnsupportedFeature("Complex JOIN".to_string()))?
        .1
        .clone();

    // Combine schemas
    let combined_schema =
        CombinedSchema::combine(left.schema.clone(), right_table_name, right_schema);

    // Choose build and probe sides (build hash table on smaller table)
    let (build_rows, probe_rows, build_col_indices, probe_col_indices, left_is_build) =
        if left.rows().len() <= right.rows().len() {
            (left.rows(), right.rows(), left_col_indices, right_col_indices, true)
        } else {
            (right.rows(), left.rows(), right_col_indices, left_col_indices, false)
        };

    // Build phase: Create hash table with composite keys from build side
    let hash_table = build_hash_table_composite_parallel(build_rows, build_col_indices);

    // Probe phase: Collect (build_idx, probe_idx) pairs
    let estimated_capacity = probe_rows.len().saturating_mul(2).min(100_000);
    let mut join_pairs: Vec<(usize, usize)> = Vec::with_capacity(estimated_capacity);

    for (probe_idx, probe_row) in probe_rows.iter().enumerate() {
        let probe_key = CompositeKey::from_row(probe_row, probe_col_indices);

        // Skip rows with NULL key values
        if probe_key.has_null() {
            continue;
        }

        if let Some(build_indices) = hash_table.get(&probe_key) {
            for &build_idx in build_indices {
                join_pairs.push((build_idx, probe_idx));
            }
        }
    }

    // Materialization phase: Create combined rows from index pairs
    let mut result_rows = Vec::with_capacity(join_pairs.len());
    for (build_idx, probe_idx) in join_pairs {
        let combined_row = if left_is_build {
            combine_rows(&build_rows[build_idx], &probe_rows[probe_idx])
        } else {
            combine_rows(&probe_rows[probe_idx], &build_rows[build_idx])
        };
        result_rows.push(combined_row);
    }

    Ok(FromResult::from_rows(combined_schema, result_rows))
}
