use super::build::{build_hash_table_composite_parallel, build_hash_table_parallel, CompositeKey};
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

/// Multi-column hash join LEFT OUTER JOIN implementation
///
/// This implementation uses composite keys for hash join when there are multiple
/// equi-join conditions between the same table pair (e.g., `a.x = b.x AND a.y = b.y`).
///
/// Using composite keys instead of single-column keys eliminates the need for
/// post-join filtering of additional equi-join conditions. This is critical for
/// LEFT OUTER JOIN correctness because post-join filters incorrectly skip rows
/// where conditions evaluate to NULL (unmatched left rows have NULL right columns).
///
/// Algorithm:
/// 1. Build phase: Create hash table with composite keys from right table (O(m))
/// 2. Probe phase: For each left row, create composite key and lookup (O(n))
///    - If matches found: emit left + right rows
///    - If no match (or NULL key): emit left + NULLs (preserves left rows)
///
/// Total: O(n + m) with correct LEFT JOIN semantics
///
/// This fixes TPC-DS Q75 where compound LEFT JOIN conditions like:
/// `ss_ticket_number = sr_ticket_number AND ss_item_sk = sr_item_sk`
/// must match on BOTH columns, not filter after single-column match.
pub(in crate::select::join) fn hash_join_left_outer_multi(
    left: FromResult,
    right: FromResult,
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

    let right_col_count = right_schema.columns.len();
    let left_col_count: usize =
        left.schema.table_schemas.values().map(|(_, s)| s.columns.len()).sum();

    // Combine schemas
    let combined_schema =
        CombinedSchema::combine(left.schema.clone(), right_table_name, right_schema);

    // Use as_slice() for zero-cost access (we can't swap build/probe for LEFT JOIN)
    let left_slice = left.as_slice();
    let right_slice = right.as_slice();

    // Build hash table on the RIGHT side with composite keys
    // For LEFT OUTER JOIN, we always probe with left, so build on right
    let hash_table = build_hash_table_composite_parallel(right_slice, right_col_indices);

    // Pre-compute combined row size for efficient allocation
    let combined_size = left_col_count + right_col_count;

    // Create a single null row for reuse (reduces allocations for unmatched rows)
    let null_values = vec![vibesql_types::SqlValue::Null; right_col_count];

    // Estimate result size - at least left_slice.len() since we preserve all left rows
    let mut result_rows = Vec::with_capacity(left_slice.len());

    for left_row in left_slice {
        let probe_key = CompositeKey::from_row(left_row, left_col_indices);

        // For NULL keys in left (any column is NULL), still emit with NULL right side
        // This preserves LEFT JOIN semantics: all left rows must appear in output
        if probe_key.has_null() {
            let mut combined = Vec::with_capacity(combined_size);
            combined.extend_from_slice(&left_row.values);
            combined.extend_from_slice(&null_values);
            result_rows.push(vibesql_storage::Row::new(combined));
            continue;
        }

        if let Some(right_indices) = hash_table.get(&probe_key) {
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
