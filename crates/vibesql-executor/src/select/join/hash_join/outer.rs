#[cfg(feature = "parallel")]
use rayon::prelude::*;

use super::{
    build::{build_hash_table_composite_parallel, build_hash_table_parallel, CompositeKey},
    FromResult,
};
#[cfg(feature = "parallel")]
use crate::select::parallel::ParallelConfig;
use crate::{
    errors::ExecutorError, evaluator::CombinedExpressionEvaluator, schema::CombinedSchema,
    select::join::combine_rows,
};

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
    // Get column counts (handles nested joins with multiple tables)
    let right_col_count = right.schema.total_columns;
    let left_col_count = left.schema.total_columns;

    // Combine schemas using merge to preserve all tables from nested joins
    let combined_schema = CombinedSchema::merge(left.schema.clone(), right.schema.clone());

    // Use as_slice() for zero-cost access without triggering row materialization
    let left_slice = left.as_slice();
    let right_slice = right.as_slice();

    // Build hash table on the RIGHT side (we need to preserve ALL left rows)
    // For LEFT OUTER JOIN, we always probe with left, so build on right
    // Uses parallel hashing when available for large tables
    let hash_table = build_hash_table_parallel(right_slice, right_col_idx);

    // Pre-compute combined row size for efficient allocation
    let combined_size = left_col_count + right_col_count;

    // Parallel probe phase for LEFT OUTER JOIN
    // Every left row must appear in output (with matches or NULLs)
    #[cfg(feature = "parallel")]
    {
        let config = ParallelConfig::global();
        if config.should_parallelize_join(left_slice.len()) {
            // Pre-create null values for unmatched rows (shared across threads via reference)
            let null_values = vec![vibesql_types::SqlValue::Null; right_col_count];

            let result_rows: Vec<vibesql_storage::Row> = left_slice
                .par_iter()
                .flat_map(|left_row| {
                    let key = &left_row.values[left_col_idx];

                    // For NULL keys in left, emit left + NULLs
                    if key == &vibesql_types::SqlValue::Null {
                        let mut combined = Vec::with_capacity(combined_size);
                        combined.extend_from_slice(&left_row.values);
                        combined.extend_from_slice(&null_values);
                        return vec![vibesql_storage::Row::new(combined)];
                    }

                    if let Some(right_indices) = hash_table.get(key) {
                        // Found matches - emit all combinations
                        right_indices
                            .iter()
                            .map(|&right_idx| {
                                let mut combined = Vec::with_capacity(combined_size);
                                combined.extend_from_slice(&left_row.values);
                                combined.extend_from_slice(&right_slice[right_idx].values);
                                vibesql_storage::Row::new(combined)
                            })
                            .collect()
                    } else {
                        // No match - emit left row with NULLs
                        let mut combined = Vec::with_capacity(combined_size);
                        combined.extend_from_slice(&left_row.values);
                        combined.extend_from_slice(&null_values);
                        vec![vibesql_storage::Row::new(combined)]
                    }
                })
                .collect();

            return Ok(FromResult::from_rows(combined_schema, result_rows));
        }
    }

    // Sequential fallback: Two-phase approach for better allocation
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
    // Get column counts (handles nested joins with multiple tables)
    let right_col_count = right.schema.total_columns;
    let left_col_count = left.schema.total_columns;

    // Combine schemas using merge to preserve all tables from nested joins
    let combined_schema = CombinedSchema::merge(left.schema.clone(), right.schema.clone());

    // Use as_slice() for zero-cost access (we can't swap build/probe for LEFT JOIN)
    let left_slice = left.as_slice();
    let right_slice = right.as_slice();

    // Build hash table on the RIGHT side with composite keys
    // For LEFT OUTER JOIN, we always probe with left, so build on right
    let hash_table = build_hash_table_composite_parallel(right_slice, right_col_indices);

    // Pre-compute combined row size for efficient allocation
    let combined_size = left_col_count + right_col_count;

    // Parallel probe phase for multi-column LEFT OUTER JOIN
    // Every left row must appear in output (with matches or NULLs)
    #[cfg(feature = "parallel")]
    {
        let config = ParallelConfig::global();
        if config.should_parallelize_join(left_slice.len()) {
            // Pre-create null values for unmatched rows
            let null_values = vec![vibesql_types::SqlValue::Null; right_col_count];

            let result_rows: Vec<vibesql_storage::Row> = left_slice
                .par_iter()
                .flat_map(|left_row| {
                    let probe_key = CompositeKey::from_row(left_row, left_col_indices);

                    // For NULL keys in left (any column is NULL), emit left + NULLs
                    if probe_key.has_null() {
                        let mut combined = Vec::with_capacity(combined_size);
                        combined.extend_from_slice(&left_row.values);
                        combined.extend_from_slice(&null_values);
                        return vec![vibesql_storage::Row::new(combined)];
                    }

                    if let Some(right_indices) = hash_table.get(&probe_key) {
                        // Found matches - emit all combinations
                        right_indices
                            .iter()
                            .map(|&right_idx| {
                                let mut combined = Vec::with_capacity(combined_size);
                                combined.extend_from_slice(&left_row.values);
                                combined.extend_from_slice(&right_slice[right_idx].values);
                                vibesql_storage::Row::new(combined)
                            })
                            .collect()
                    } else {
                        // No match - emit left row with NULLs
                        let mut combined = Vec::with_capacity(combined_size);
                        combined.extend_from_slice(&left_row.values);
                        combined.extend_from_slice(&null_values);
                        vec![vibesql_storage::Row::new(combined)]
                    }
                })
                .collect();

            return Ok(FromResult::from_rows(combined_schema, result_rows));
        }
    }

    // Sequential fallback
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

/// Hash join LEFT OUTER JOIN with additional filter conditions
///
/// This implementation applies non-equi-join ON conditions DURING the join,
/// not as a post-filter. This is critical for correct LEFT JOIN semantics.
///
/// For queries like:
/// ```sql
/// SELECT * FROM t1 LEFT JOIN t2 ON t1.a = t2.x AND t2.z = 'ok'
/// ```
///
/// When t1.a = t2.x matches but t2.z != 'ok':
/// - Incorrect (post-filter): Row is filtered out entirely
/// - Correct (this impl): Emit t1 columns + NULLs for t2 columns
///
/// Algorithm:
/// 1. Build phase: Hash the right table (O(m))
/// 2. Probe phase: For each left row (O(n)):
///    a. If no equi-join match: emit left + NULLs
///    b. If equi-join match: check additional filter for each match
///       - If any match passes filter: emit left + right for each passing match
///       - If ALL matches fail filter: emit left + NULLs (preserves left row!)
///
/// Total: O(n + m) with correct LEFT JOIN semantics
pub(in crate::select::join) fn hash_join_left_outer_with_filter(
    left: FromResult,
    right: FromResult,
    left_col_idx: usize,
    right_col_idx: usize,
    additional_filter: &vibesql_ast::Expression,
    combined_schema: &CombinedSchema,
    database: &vibesql_storage::Database,
) -> Result<FromResult, ExecutorError> {
    // Get column counts
    let right_col_count = right.schema.total_columns;
    let left_col_count = left.schema.total_columns;

    // Use as_slice() for zero-cost access
    let left_slice = left.as_slice();
    let right_slice = right.as_slice();

    // Build hash table on the RIGHT side
    let hash_table = build_hash_table_parallel(right_slice, right_col_idx);

    // Pre-compute combined row size for efficient allocation
    let combined_size = left_col_count + right_col_count;

    // Create evaluator for filter conditions
    let evaluator = CombinedExpressionEvaluator::with_database(combined_schema, database);

    // Pre-create null values for unmatched/filtered rows
    let null_values = vec![vibesql_types::SqlValue::Null; right_col_count];

    // Estimate result size - at least left_slice.len() since we preserve all left rows
    let mut result_rows = Vec::with_capacity(left_slice.len());

    for left_row in left_slice {
        let key = &left_row.values[left_col_idx];

        // For NULL keys in left, emit left + NULLs (NULL never matches in equi-join)
        if key == &vibesql_types::SqlValue::Null {
            let mut combined = Vec::with_capacity(combined_size);
            combined.extend_from_slice(&left_row.values);
            combined.extend_from_slice(&null_values);
            result_rows.push(vibesql_storage::Row::new(combined));
            continue;
        }

        if let Some(right_indices) = hash_table.get(key) {
            // Found equi-join matches - check additional filter for each
            let mut any_match_passed = false;

            for &right_idx in right_indices {
                let right_row = &right_slice[right_idx];

                // Create combined row for filter evaluation
                let combined_row = combine_rows(left_row, right_row);

                // Clear CSE cache before evaluation
                evaluator.clear_cse_cache();

                // Evaluate the additional filter
                match evaluator.eval(additional_filter, &combined_row) {
                    Ok(vibesql_types::SqlValue::Boolean(true)) => {
                        // Filter passed - emit this combination
                        any_match_passed = true;
                        let mut combined = Vec::with_capacity(combined_size);
                        combined.extend_from_slice(&left_row.values);
                        combined.extend_from_slice(&right_row.values);
                        result_rows.push(vibesql_storage::Row::new(combined));
                    }
                    Ok(vibesql_types::SqlValue::Boolean(false))
                    | Ok(vibesql_types::SqlValue::Null) => {
                        // Filter didn't pass - continue checking other matches
                        continue;
                    }
                    Ok(_) | Err(_) => {
                        // Non-boolean result or error - treat as non-match
                        continue;
                    }
                }
            }

            // If NO matches passed the filter, emit left + NULLs
            // This preserves LEFT JOIN semantics: all left rows must appear
            if !any_match_passed {
                let mut combined = Vec::with_capacity(combined_size);
                combined.extend_from_slice(&left_row.values);
                combined.extend_from_slice(&null_values);
                result_rows.push(vibesql_storage::Row::new(combined));
            }
        } else {
            // No equi-join match - emit left + NULLs
            let mut combined = Vec::with_capacity(combined_size);
            combined.extend_from_slice(&left_row.values);
            combined.extend_from_slice(&null_values);
            result_rows.push(vibesql_storage::Row::new(combined));
        }
    }

    // Re-create combined schema (we consumed left/right)
    let result_schema = combined_schema.clone();
    Ok(FromResult::from_rows(result_schema, result_rows))
}
