use super::build::build_hash_table_parallel;
use super::FromResult;
use crate::{errors::ExecutorError, schema::CombinedSchema};

/// Create a row with all NULL values
#[allow(dead_code)]
pub(crate) fn create_null_row(col_count: usize) -> vibesql_storage::Row {
    vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Null; col_count])
}

/// Hash join LEFT OUTER JOIN implementation (optimized for equi-joins)
///
/// This implementation uses a hash join algorithm for better performance
/// on equi-join conditions with LEFT OUTER JOIN semantics.
///
/// Algorithm:
/// 1. Build phase: Hash the right table into a HashMap (O(m))
/// 2. Probe phase: For each left row, lookup matches (O(n))
///    - If matches found: emit left + right rows
///    - If no match: emit left + NULLs (preserves left rows)
///
/// Total: O(n + m) instead of O(n * m) for nested loop join
///
/// Performance: Critical for Q13 where customer LEFT JOIN orders
/// with 150k customers and 1.5M orders.
pub(in crate::select::join) fn hash_join_left_outer(
    left: FromResult,
    right: FromResult,
    left_col_idx: usize,
    right_col_idx: usize,
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

    let right_col_count = right_schema.columns.len();
    let left_col_count: usize =
        left.schema.table_schemas.values().map(|(_, s)| s.columns.len()).sum();

    // Combine schemas
    let combined_schema =
        CombinedSchema::combine(left.schema.clone(), right_table_name, right_schema);

    // Use as_slice() for zero-cost access without triggering row materialization
    let left_slice = left.as_slice();
    let right_slice = right.as_slice();

    // Build hash table on the RIGHT side (we need to preserve ALL left rows)
    // For LEFT OUTER JOIN, we always probe with left, so build on right
    // Uses parallel hashing when available for large tables
    let hash_table = build_hash_table_parallel(right_slice, right_col_idx);

    // Pre-compute combined row size for efficient allocation
    let combined_size = left_col_count + right_col_count;

    // Two-phase approach for better allocation:
    // Phase 1: Count total rows needed (matched + unmatched)
    let mut match_count = 0usize;
    let mut unmatched_count = 0usize;

    for left_row in left_slice {
        let key = &left_row.values[left_col_idx];

        if key == &vibesql_types::SqlValue::Null {
            unmatched_count += 1;
        } else if let Some(right_indices) = hash_table.get(key) {
            match_count += right_indices.len();
        } else {
            unmatched_count += 1;
        }
    }

    // Phase 2: Allocate result with exact capacity and populate
    let mut result_rows = Vec::with_capacity(match_count + unmatched_count);

    // Create a single null row for reuse (reduces allocations for unmatched rows)
    let null_values = vec![vibesql_types::SqlValue::Null; right_col_count];

    for left_row in left_slice {
        let key = &left_row.values[left_col_idx];

        // For NULL keys in left, still emit the row with NULL right side
        if key == &vibesql_types::SqlValue::Null {
            let mut combined = Vec::with_capacity(combined_size);
            combined.extend_from_slice(&left_row.values);
            combined.extend_from_slice(&null_values);
            result_rows.push(vibesql_storage::Row::new(combined));
            continue;
        }

        if let Some(right_indices) = hash_table.get(key) {
            // Found matches - emit all combinations
            for &right_idx in right_indices {
                let mut combined = Vec::with_capacity(combined_size);
                combined.extend_from_slice(&left_row.values);
                combined.extend_from_slice(&right_slice[right_idx].values);
                result_rows.push(vibesql_storage::Row::new(combined));
            }
        } else {
            // No match - emit left row with NULLs for right columns
            let mut combined = Vec::with_capacity(combined_size);
            combined.extend_from_slice(&left_row.values);
            combined.extend_from_slice(&null_values);
            result_rows.push(vibesql_storage::Row::new(combined));
        }
    }

    Ok(FromResult::from_rows(combined_schema, result_rows))
}
