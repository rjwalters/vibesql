//! Nested loop join implementations.
//!
//! This module provides nested loop join algorithms for various join types:
//! - INNER JOIN
//! - LEFT OUTER JOIN
//! - RIGHT OUTER JOIN
//! - FULL OUTER JOIN
//! - CROSS JOIN
//! - SEMI JOIN
//! - ANTI JOIN
//!
//! # Module Organization
//!
//! - `condition`: Join condition analysis and evaluation helpers
//! - `memory_guard`: Memory limit checking for large joins
//! - `sequential`: Sequential nested loop implementations
//! - `parallel`: Parallel nested loop implementation (feature-gated)

mod condition;
mod memory_guard;
#[cfg(feature = "parallel")]
pub mod parallel;
mod sequential;

#[cfg(test)]
mod tests;

// Re-export items used by this module
use condition::{analyze_join_condition, eval_join_condition_to_bool, EquijoinEvalStrategy};
use memory_guard::check_cross_join_size_limit;
use sequential::execute_with_strategy;

use super::{combine_rows, FromResult};
use crate::{
    errors::ExecutorError,
    evaluator::CombinedExpressionEvaluator,
    schema::CombinedSchema,
    timeout::{TimeoutContext, CHECK_INTERVAL},
};

/// Nested loop INNER JOIN implementation
pub(in crate::select::join) fn nested_loop_inner_join(
    left: FromResult,
    right: FromResult,
    condition: &Option<vibesql_ast::Expression>,
    database: &vibesql_storage::Database,
    timeout_ctx: &TimeoutContext,
) -> Result<FromResult, ExecutorError> {
    // Check for potential cartesian product before execution
    // This catches INNER JOINs with non-selective conditions (e.g., WHERE true)
    // that would create massive intermediate results
    let is_cartesian_like = match condition {
        None => true, // No condition = cartesian product
        Some(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Boolean(true))) => true, /* Always true = cartesian product */
        _ => false, // Has a meaningful condition - let it proceed
    };

    // Use as_slice() for zero-cost access without triggering row materialization
    // This avoids the 57% performance bottleneck from premature row collection
    let left_slice = left.as_slice();
    let right_slice = right.as_slice();

    if is_cartesian_like {
        // Apply same memory check as CROSS JOIN
        check_cross_join_size_limit(left_slice.len(), right_slice.len())?;
    }

    // Combine schemas using merge to preserve all tables from nested joins
    // This handles cases like `t1 JOIN (t2 JOIN t3 USING(a)) USING(a)` where
    // the right side contains multiple tables that must remain visible
    let combined_schema = CombinedSchema::merge(left.schema.clone(), right.schema.clone());

    // OPTIMIZATION: Analyze condition to see if we can evaluate equijoin before allocation
    // This prevents creating combined_row for pairs that won't match the join condition
    let left_col_count: usize =
        left.schema.table_schemas.values().map(|(_, schema)| schema.columns.len()).sum();

    let eval_strategy = if let Some(cond) = condition {
        analyze_join_condition(cond, &combined_schema, left_col_count)
    } else {
        EquijoinEvalStrategy::Complex
    };

    // Note: No memory check here. Hash join is selected in mod.rs BEFORE this function is called.
    // If equijoins exist (either in condition OR in additional_equijoins), hash join will be used.
    // This function only handles cases where hash join cannot be used (e.g., complex conditions).

    // Extract table names for ROWID tracking (issue #4370)
    let left_table_names = left.schema.table_names();
    let right_table_names = right.schema.table_names();

    // Execute join with optimized strategy
    let result_rows = execute_with_strategy(
        left_slice,
        right_slice,
        condition,
        eval_strategy,
        &combined_schema,
        database,
        timeout_ctx,
        &left_table_names,
        &right_table_names,
    )?;

    Ok(FromResult::from_rows(combined_schema, result_rows))
}

/// Nested loop LEFT OUTER JOIN implementation
pub(in crate::select::join) fn nested_loop_left_outer_join(
    left: FromResult,
    right: FromResult,
    condition: &Option<vibesql_ast::Expression>,
    database: &vibesql_storage::Database,
    timeout_ctx: &TimeoutContext,
) -> Result<FromResult, ExecutorError> {
    // Note: No memory check here. Hash join is selected in mod.rs BEFORE this function is called.
    // OUTER JOINs typically preserve at least the left table size, making estimates more reliable.

    // Get total right column count (handles nested joins with multiple tables)
    let right_column_count = right.schema.total_columns;

    // Extract table names for ROWID tracking (issue #4370)
    let left_table_names = left.schema.table_names();
    let right_table_names = right.schema.table_names();

    // Combine schemas using merge to preserve all tables from nested joins
    let combined_schema = CombinedSchema::merge(left.schema.clone(), right.schema.clone());
    let evaluator = CombinedExpressionEvaluator::with_database(&combined_schema, database);

    // Use as_slice() for zero-cost access without triggering row materialization
    let left_slice = left.as_slice();
    let right_slice = right.as_slice();

    // Nested loop LEFT OUTER JOIN algorithm
    let mut result_rows = Vec::new();
    let mut iterations = 0;
    for left_row in left_slice {
        let mut matched = false;

        for right_row in right_slice {
            // Check timeout periodically
            iterations += 1;
            if iterations % CHECK_INTERVAL == 0 {
                timeout_ctx.check()?;
            }

            // Combine rows preserving ROWIDs for multi-table queries (issue #4370)
            let combined_row = vibesql_storage::Row::combine_for_join(
                left_row,
                right_row,
                &left_table_names,
                &right_table_names,
            );

            // Clear CSE cache before evaluating join condition for this row combination
            // to prevent stale cached column values from previous combinations
            evaluator.clear_cse_cache();

            // Evaluate join condition
            let matches = if let Some(cond) = condition {
                eval_join_condition_to_bool(evaluator.eval(cond, &combined_row)?)?
            } else {
                true // No condition = CROSS JOIN
            };

            if matches {
                result_rows.push(combined_row);
                matched = true;
            }
        }

        // If no match found, add left row with NULLs for right columns
        // Preserve left side ROWIDs (issue #4370)
        if !matched {
            let mut combined_values =
                Vec::with_capacity(left_row.values.len() + right_column_count);
            combined_values.extend_from_slice(&left_row.values);
            combined_values.extend(vec![vibesql_types::SqlValue::Null; right_column_count]);

            // Preserve left side ROWIDs for unmatched rows
            let mut row_ids = std::collections::HashMap::new();
            if let Some(ref left_row_ids) = left_row.row_ids {
                row_ids.extend(left_row_ids.iter().map(|(k, v)| (k.clone(), *v)));
            } else if let Some(row_id) = left_row.row_id {
                // Single table case - use first left table name if available
                if let Some(table_name) = left_table_names.first() {
                    row_ids.insert(table_name.to_lowercase(), row_id);
                }
            }

            result_rows.push(vibesql_storage::Row::with_row_ids(combined_values, row_ids));
        }
    }

    Ok(FromResult::from_rows(combined_schema, result_rows))
}

/// Nested loop RIGHT OUTER JOIN implementation
pub(in crate::select::join) fn nested_loop_right_outer_join(
    left: FromResult,
    right: FromResult,
    condition: &Option<vibesql_ast::Expression>,
    database: &vibesql_storage::Database,
    timeout_ctx: &TimeoutContext,
) -> Result<FromResult, ExecutorError> {
    // Note: Memory check removed - delegates to LEFT OUTER JOIN which also doesn't check.

    // RIGHT OUTER JOIN = LEFT OUTER JOIN with sides swapped
    // Then we need to reorder columns to put left first, right second

    // Save original schemas before swapping - we need these to build the correct output schema
    let left_schema = left.schema.clone();
    let right_schema = right.schema.clone();

    // Get the right column count (handles nested joins with multiple tables)
    let right_col_count = right_schema.total_columns;

    // Do LEFT OUTER JOIN with swapped sides
    let swapped_result =
        nested_loop_left_outer_join(right, left, condition, database, timeout_ctx)?;

    // Now we need to reorder the columns in the result
    // The swapped result has right columns first, then left columns
    // We need to reverse this to left first, then right

    // Reorder rows: move left columns (currently at positions right_col_count..) to front
    // Use as_slice() for zero-cost access
    let reordered_rows: Vec<vibesql_storage::Row> = swapped_result
        .as_slice()
        .iter()
        .map(|row| {
            let mut new_values = Vec::new();
            // Add left columns (currently at end)
            new_values.extend_from_slice(&row.values[right_col_count..]);
            // Add right columns (currently at start)
            new_values.extend_from_slice(&row.values[0..right_col_count]);
            vibesql_storage::Row::new(new_values)
        })
        .collect();

    // Build the correct schema: left columns first, then right columns
    // The swapped_result.schema has [right, left] order, so we need to merge correctly
    let correct_schema = CombinedSchema::merge(left_schema, right_schema);

    Ok(FromResult::from_rows(correct_schema, reordered_rows))
}

/// Nested loop FULL OUTER JOIN implementation
pub(in crate::select::join) fn nested_loop_full_outer_join(
    left: FromResult,
    right: FromResult,
    condition: &Option<vibesql_ast::Expression>,
    database: &vibesql_storage::Database,
    timeout_ctx: &TimeoutContext,
) -> Result<FromResult, ExecutorError> {
    // Note: Memory check removed - full outer joins are rare and typically used
    // with smaller datasets. Hash join is tried first for equijoins anyway.

    // Get column counts (handles nested joins with multiple tables)
    let left_column_count = left.schema.total_columns;
    let right_column_count = right.schema.total_columns;

    // Combine schemas using merge to preserve all tables from nested joins
    let combined_schema = CombinedSchema::merge(left.schema.clone(), right.schema.clone());
    let evaluator = CombinedExpressionEvaluator::with_database(&combined_schema, database);

    // Use as_slice() for zero-cost access without triggering row materialization
    let left_slice = left.as_slice();
    let right_slice = right.as_slice();

    // FULL OUTER JOIN = LEFT OUTER JOIN + unmatched rows from right
    let mut result_rows = Vec::new();
    let mut right_matched = vec![false; right_slice.len()];
    let mut iterations = 0;

    // First pass: LEFT OUTER JOIN logic
    for left_row in left_slice {
        let mut matched = false;

        for (right_idx, right_row) in right_slice.iter().enumerate() {
            // Check timeout periodically
            iterations += 1;
            if iterations % CHECK_INTERVAL == 0 {
                timeout_ctx.check()?;
            }

            // Combine rows using optimized helper (single allocation)
            let combined_row = combine_rows(left_row, right_row);

            // Clear CSE cache before evaluating join condition for this row combination
            // to prevent stale cached column values from previous combinations
            evaluator.clear_cse_cache();

            // Evaluate join condition
            let matches = if let Some(cond) = condition {
                eval_join_condition_to_bool(evaluator.eval(cond, &combined_row)?)?
            } else {
                true
            };

            if matches {
                result_rows.push(combined_row);
                matched = true;
                right_matched[right_idx] = true;
            }
        }

        // If no match found, add left row with NULLs for right columns
        if !matched {
            let mut combined_values =
                Vec::with_capacity(left_row.values.len() + right_column_count);
            combined_values.extend_from_slice(&left_row.values);
            combined_values.extend(vec![vibesql_types::SqlValue::Null; right_column_count]);
            result_rows.push(vibesql_storage::Row::new(combined_values));
        }
    }

    // Second pass: Add unmatched right rows with NULLs for left columns
    for (right_idx, right_row) in right_slice.iter().enumerate() {
        if !right_matched[right_idx] {
            let mut combined_values =
                Vec::with_capacity(left_column_count + right_row.values.len());
            combined_values.extend(vec![vibesql_types::SqlValue::Null; left_column_count]);
            combined_values.extend_from_slice(&right_row.values);
            result_rows.push(vibesql_storage::Row::new(combined_values));
        }
    }

    Ok(FromResult::from_rows(combined_schema, result_rows))
}

/// Nested loop CROSS JOIN implementation (Cartesian product)
pub(in crate::select::join) fn nested_loop_cross_join(
    left: FromResult,
    right: FromResult,
    condition: &Option<vibesql_ast::Expression>,
    _database: &vibesql_storage::Database,
    timeout_ctx: &TimeoutContext,
) -> Result<FromResult, ExecutorError> {
    // CROSS JOIN should not have a condition
    if condition.is_some() {
        return Err(ExecutorError::UnsupportedFeature(
            "CROSS JOIN does not support ON clause".to_string(),
        ));
    }

    // Use as_slice() for zero-cost access without triggering row materialization
    let left_slice = left.as_slice();
    let right_slice = right.as_slice();

    // Check if cross join would exceed memory limits before executing
    // CROSS JOIN always creates Cartesian products, so this check is appropriate
    check_cross_join_size_limit(left_slice.len(), right_slice.len())?;

    // Extract right table name and schema
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

    // Get display name from the TableIdentifier
    let right_table_display_name = right_table_name.display().to_string();

    // Combine schemas
    let combined_schema = CombinedSchema::combine(
        left.schema.clone(),
        right_table_display_name.clone(),
        right_schema,
    );

    // Get table names for ROWID tracking (issue #4370)
    let left_table_names = left.schema.table_names();
    let right_table_names = right.schema.table_names();

    // CROSS JOIN = Cartesian product (every row from left × every row from right)
    let mut result_rows = Vec::new();
    let mut iterations = 0;
    for left_row in left_slice {
        for right_row in right_slice {
            // Check timeout periodically
            iterations += 1;
            if iterations % CHECK_INTERVAL == 0 {
                timeout_ctx.check()?;
            }
            // Use combine_for_join for proper ROWID tracking (issue #4370)
            result_rows.push(vibesql_storage::Row::combine_for_join(
                left_row,
                right_row,
                &left_table_names,
                &right_table_names,
            ));
        }
    }

    Ok(FromResult::from_rows(combined_schema, result_rows))
}

/// Nested loop SEMI JOIN implementation
///
/// Semi-join returns left rows that have at least one match in the right table.
/// Unlike INNER JOIN, each left row is returned at most once (no duplicates).
pub(in crate::select::join) fn nested_loop_semi_join(
    left: FromResult,
    right: FromResult,
    condition: &Option<vibesql_ast::Expression>,
    database: &vibesql_storage::Database,
    timeout_ctx: &TimeoutContext,
) -> Result<FromResult, ExecutorError> {
    let left_schema = left.schema.clone();

    // Extract right table info for combined schema (needed for condition evaluation)
    let right_table_name = right
        .schema
        .table_schemas
        .keys()
        .next()
        .ok_or_else(|| ExecutorError::UnsupportedFeature("Complex JOIN".to_string()))?
        .clone();

    let right_schema_def = right
        .schema
        .table_schemas
        .get(&right_table_name)
        .ok_or_else(|| ExecutorError::UnsupportedFeature("Complex JOIN".to_string()))?
        .1
        .clone();

    // Get display name from the TableIdentifier
    let right_table_display_name = right_table_name.display().to_string();

    // Create combined schema for condition evaluation
    let combined_schema = CombinedSchema::combine(
        left.schema.clone(),
        right_table_display_name.clone(),
        right_schema_def,
    );

    // Create evaluator for condition
    let evaluator = CombinedExpressionEvaluator::with_database(&combined_schema, database);

    // Use as_slice() for zero-cost access without triggering row materialization
    let left_slice = left.as_slice();
    let right_slice = right.as_slice();

    // Get table names for ROWID tracking (issue #4716)
    let left_table_names = left.schema.table_names();
    let right_table_names = right.schema.table_names();

    let mut result_rows = Vec::new();
    let mut iterations = 0;

    // For each left row, check if there's at least one matching right row
    for left_row in left_slice {
        let mut has_match = false;

        for right_row in right_slice {
            // Check timeout periodically
            iterations += 1;
            if iterations % CHECK_INTERVAL == 0 {
                timeout_ctx.check()?;
            }

            // Use combine_for_join for proper ROWID tracking (issue #4716)
            let combined_row = vibesql_storage::Row::combine_for_join(
                left_row,
                right_row,
                &left_table_names,
                &right_table_names,
            );

            // Check join condition
            let matches = match condition {
                None => true, // No condition means all rows match
                Some(expr) => {
                    evaluator.clear_cse_cache();
                    let value = evaluator.eval(expr, &combined_row)?;
                    match value {
                        vibesql_types::SqlValue::Boolean(b) => b,
                        vibesql_types::SqlValue::Null => false,
                        _ => {
                            return Err(ExecutorError::InvalidWhereClause(format!(
                                "Join condition must evaluate to boolean, got: {:?}",
                                value
                            )))
                        }
                    }
                }
            };

            if matches {
                has_match = true;
                break; // Found a match, no need to check remaining right rows
            }
        }

        if has_match {
            // Return only the left row (not combined)
            result_rows.push(left_row.clone());
        }
    }

    Ok(FromResult::from_rows(left_schema, result_rows))
}

/// Nested loop ANTI JOIN implementation
///
/// Anti-join returns left rows that have NO matches in the right table.
pub(in crate::select::join) fn nested_loop_anti_join(
    left: FromResult,
    right: FromResult,
    condition: &Option<vibesql_ast::Expression>,
    database: &vibesql_storage::Database,
    timeout_ctx: &TimeoutContext,
) -> Result<FromResult, ExecutorError> {
    let left_schema = left.schema.clone();

    // Extract right table info for combined schema (needed for condition evaluation)
    let right_table_name = right
        .schema
        .table_schemas
        .keys()
        .next()
        .ok_or_else(|| ExecutorError::UnsupportedFeature("Complex JOIN".to_string()))?
        .clone();

    let right_schema_def = right
        .schema
        .table_schemas
        .get(&right_table_name)
        .ok_or_else(|| ExecutorError::UnsupportedFeature("Complex JOIN".to_string()))?
        .1
        .clone();

    // Get display name from the TableIdentifier
    let right_table_display_name = right_table_name.display().to_string();

    // Create combined schema for condition evaluation
    let combined_schema = CombinedSchema::combine(
        left.schema.clone(),
        right_table_display_name.clone(),
        right_schema_def,
    );

    // Create evaluator for condition
    let evaluator = CombinedExpressionEvaluator::with_database(&combined_schema, database);

    // Use as_slice() for zero-cost access without triggering row materialization
    let left_slice = left.as_slice();
    let right_slice = right.as_slice();

    // Get table names for ROWID tracking (issue #4716)
    let left_table_names = left.schema.table_names();
    let right_table_names = right.schema.table_names();

    let mut result_rows = Vec::new();
    let mut iterations = 0;

    // For each left row, check if there are NO matching right rows
    for left_row in left_slice {
        let mut has_match = false;

        for right_row in right_slice {
            // Check timeout periodically
            iterations += 1;
            if iterations % CHECK_INTERVAL == 0 {
                timeout_ctx.check()?;
            }

            // Use combine_for_join for proper ROWID tracking (issue #4716)
            let combined_row = vibesql_storage::Row::combine_for_join(
                left_row,
                right_row,
                &left_table_names,
                &right_table_names,
            );

            // Check join condition
            let matches = match condition {
                None => true, // No condition means all rows match
                Some(expr) => {
                    evaluator.clear_cse_cache();
                    let value = evaluator.eval(expr, &combined_row)?;
                    match value {
                        vibesql_types::SqlValue::Boolean(b) => b,
                        vibesql_types::SqlValue::Null => false,
                        _ => {
                            return Err(ExecutorError::InvalidWhereClause(format!(
                                "Join condition must evaluate to boolean, got: {:?}",
                                value
                            )))
                        }
                    }
                }
            };

            if matches {
                has_match = true;
                break; // Found a match, this left row won't be in result
            }
        }

        if !has_match {
            // Return only the left row (not combined) when NO matches found
            result_rows.push(left_row.clone());
        }
    }

    Ok(FromResult::from_rows(left_schema, result_rows))
}
