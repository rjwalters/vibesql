#![allow(clippy::doc_lazy_continuation)]

use super::build::{build_hash_table_composite_parallel, build_hash_table_parallel, CompositeKey};
use super::columnar::{hash_join_indices_columnar, hash_join_indices_columnar_multi};
use super::{batch_combine_rows, FromResult};
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
    let join_pairs: Vec<(usize, usize)> = if let Some(pairs) =
        hash_join_indices_columnar(build_rows, probe_rows, build_col_idx, probe_col_idx)
    {
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

    // Materialization phase: Create combined rows from index pairs using batch combine
    // This optimizes allocation by pre-computing combined row size
    let result_rows = batch_combine_rows(build_rows, probe_rows, &join_pairs, left_is_build);

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

    // Fast path: Try columnar hash join for integer keys
    // This provides significant speedup for integer equi-joins by:
    // 1. Using FxHash-style hashing without SqlValue enum dispatch
    // 2. Better cache locality with contiguous i64 arrays
    // 3. Pre-computed composite hashes for multi-column keys
    let join_pairs: Vec<(usize, usize)> = if let Some(pairs) = hash_join_indices_columnar_multi(
        build_rows,
        probe_rows,
        build_col_indices,
        probe_col_indices,
    ) {
        pairs
    } else {
        // Fallback: Generic hash join using SqlValue-based CompositeKey
        // Build phase: Create hash table with composite keys from build side
        let hash_table = build_hash_table_composite_parallel(build_rows, build_col_indices);

        // Probe phase: Collect (build_idx, probe_idx) pairs
        let estimated_capacity = probe_rows.len().saturating_mul(2).min(100_000);
        let mut pairs: Vec<(usize, usize)> = Vec::with_capacity(estimated_capacity);

        for (probe_idx, probe_row) in probe_rows.iter().enumerate() {
            let probe_key = CompositeKey::from_row(probe_row, probe_col_indices);

            // Skip rows with NULL key values
            if probe_key.has_null() {
                continue;
            }

            if let Some(build_indices) = hash_table.get(&probe_key) {
                for &build_idx in build_indices {
                    pairs.push((build_idx, probe_idx));
                }
            }
        }
        pairs
    };

    // Materialization phase: Create combined rows from index pairs using batch combine
    // This optimizes allocation by pre-computing combined row size
    let result_rows = batch_combine_rows(build_rows, probe_rows, &join_pairs, left_is_build);

    Ok(FromResult::from_rows(combined_schema, result_rows))
}

/// Hash join INNER JOIN with arithmetic offset transformation
///
/// This implementation optimizes joins with arithmetic conditions like:
/// `left_col = right_col - offset` (e.g., TPC-DS Q2: `d_week_seq1 = d_week_seq2 - 53`)
///
/// The transformation applies the offset during hash table building:
/// - Build phase: hash(right_col + offset) → for `left = right - 53`, offset = -53
/// - Probe phase: lookup hash(left_col)
///
/// This converts arithmetic equijoins to regular hash joins with O(n+m) complexity.
pub(in crate::select::join) fn hash_join_inner_arithmetic(
    left: FromResult,
    right: FromResult,
    left_col_idx: usize,
    right_col_idx: usize,
    offset: i64,
) -> Result<FromResult, ExecutorError> {
    use ahash::AHashMap;
    use vibesql_types::SqlValue;

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

    let left_slice = left.as_slice();
    let right_slice = right.as_slice();

    // Build hash table on right side with offset applied
    // Key: right_col_value + offset
    // Value: vector of row indices
    let mut hash_table: AHashMap<i64, Vec<usize>> = AHashMap::with_capacity(right_slice.len());

    for (idx, row) in right_slice.iter().enumerate() {
        let value = &row.values[right_col_idx];

        // Only handle integer values - skip NULLs
        if let SqlValue::Integer(n) = value {
            // Apply offset during build: for condition `left = right - 53`,
            // we store hash(right + (-53)) = hash(right - 53)
            let key = n + offset;
            hash_table.entry(key).or_default().push(idx);
        }
        // Skip NULL values - they never match in equi-joins
    }

    // Probe phase: For each left row, lookup by left_col value
    let estimated_capacity = left_slice.len().saturating_mul(2).min(100_000);
    let mut pairs: Vec<(usize, usize)> = Vec::with_capacity(estimated_capacity);

    for (left_idx, left_row) in left_slice.iter().enumerate() {
        let value = &left_row.values[left_col_idx];

        // Only handle integer values
        if let SqlValue::Integer(n) = value {
            // Probe with the raw left column value
            if let Some(right_indices) = hash_table.get(n) {
                for &right_idx in right_indices {
                    pairs.push((left_idx, right_idx));
                }
            }
        }
    }

    // Materialization phase: Create combined rows from index pairs
    // For arithmetic join, left is always probed so left_is_build = false equivalent
    // We need to pass (left_idx, right_idx) pairs where left is in "probe" position
    // Since batch_combine_rows expects (build_idx, probe_idx), we pass:
    // - build_rows = right_slice (the side we built hash table on)
    // - probe_rows = left_slice (the side we probed with)
    // - But pairs are (left_idx, right_idx), so we need to swap for batch_combine_rows
    let swapped_pairs: Vec<(usize, usize)> =
        pairs.into_iter().map(|(left, right)| (right, left)).collect();
    let result_rows = batch_combine_rows(right_slice, left_slice, &swapped_pairs, false);

    Ok(FromResult::from_rows(combined_schema, result_rows))
}
