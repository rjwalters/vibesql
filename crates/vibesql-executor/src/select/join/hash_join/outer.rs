use super::build::build_hash_table_parallel;
use super::{combine_rows, FromResult};
use crate::{errors::ExecutorError, schema::CombinedSchema};

/// Create a row with all NULL values
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

    // Probe with LEFT side, preserving unmatched left rows
    let mut result_rows = Vec::new();

    for left_row in left_slice {
        let key = &left_row.values[left_col_idx];

        // For NULL keys in left, still emit the row with NULL right side
        if key == &vibesql_types::SqlValue::Null {
            // Left row with NULLs for right columns
            let null_right = create_null_row(right_col_count);
            result_rows.push(combine_rows(left_row, &null_right));
            continue;
        }

        if let Some(right_indices) = hash_table.get(key) {
            // Found matches - emit all combinations
            for &right_idx in right_indices {
                result_rows.push(combine_rows(left_row, &right_slice[right_idx]));
            }
        } else {
            // No match - emit left row with NULLs for right columns
            let null_right = create_null_row(right_col_count);
            result_rows.push(combine_rows(left_row, &null_right));
        }
    }

    Ok(FromResult::from_rows(combined_schema, result_rows))
}
