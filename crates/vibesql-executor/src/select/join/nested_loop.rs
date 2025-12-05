use super::{combine_rows, FromResult};
use crate::{
    errors::ExecutorError,
    evaluator::CombinedExpressionEvaluator,
    limits::MAX_MEMORY_BYTES,
    schema::CombinedSchema,
    timeout::{TimeoutContext, CHECK_INTERVAL},
};

/// Maximum number of rows allowed in a join result to prevent memory exhaustion
/// With average row size of ~100 bytes, this allows up to ~10GB
const MAX_JOIN_RESULT_ROWS: usize = 100_000_000;

/// Check if a CROSS JOIN would exceed memory limits
/// Only used for true CROSS JOINs (no join condition)
fn check_cross_join_size_limit(left_count: usize, right_count: usize) -> Result<(), ExecutorError> {
    // CROSS JOIN creates Cartesian product
    let estimated_result_rows = left_count.saturating_mul(right_count);

    if estimated_result_rows > MAX_JOIN_RESULT_ROWS {
        // Estimate memory usage (conservative: 100 bytes per row average)
        let estimated_bytes = estimated_result_rows.saturating_mul(100);
        return Err(ExecutorError::MemoryLimitExceeded {
            used_bytes: estimated_bytes,
            max_bytes: MAX_MEMORY_BYTES,
        });
    }

    Ok(())
}

/// Optimized evaluation result for equijoin conditions
#[derive(Debug)]
enum EquijoinEvalStrategy {
    /// Simple equijoin - can evaluate by direct value comparison
    /// (left_col_idx, right_col_idx, evaluator for remaining conditions)
    Simple {
        left_col_idx: usize,
        right_col_idx: usize,
        remaining_condition: Option<vibesql_ast::Expression>,
    },
    /// Complex condition - need full evaluation with combined_row
    Complex,
}

/// Analyze join condition to determine optimization strategy
fn analyze_join_condition(
    condition: &vibesql_ast::Expression,
    schema: &CombinedSchema,
    left_col_count: usize,
) -> EquijoinEvalStrategy {
    use super::join_analyzer;

    // Try to detect a simple equijoin pattern
    if let Some(equi_info) = join_analyzer::analyze_equi_join(condition, schema, left_col_count) {
        // Simple equijoin detected - use optimized path
        return EquijoinEvalStrategy::Simple {
            left_col_idx: equi_info.left_col_idx,
            right_col_idx: equi_info.right_col_idx,
            remaining_condition: None,
        };
    }

    // Check if condition is an AND with at least one simple equijoin
    if let vibesql_ast::Expression::BinaryOp { op: vibesql_ast::BinaryOperator::And, left, right } =
        condition
    {
        // Try left side
        if let Some(equi_info) = join_analyzer::analyze_equi_join(left, schema, left_col_count) {
            return EquijoinEvalStrategy::Simple {
                left_col_idx: equi_info.left_col_idx,
                right_col_idx: equi_info.right_col_idx,
                remaining_condition: Some(right.as_ref().clone()),
            };
        }
        // Try right side
        if let Some(equi_info) = join_analyzer::analyze_equi_join(right, schema, left_col_count) {
            return EquijoinEvalStrategy::Simple {
                left_col_idx: equi_info.left_col_idx,
                right_col_idx: equi_info.right_col_idx,
                remaining_condition: Some(left.as_ref().clone()),
            };
        }
    }

    // Complex condition - fall back to classic algorithm
    EquijoinEvalStrategy::Complex
}

/// Execute optimized equijoin by comparing values before allocating combined_row
#[allow(clippy::too_many_arguments)]
fn execute_optimized_equijoin(
    left_rows: &[vibesql_storage::Row],
    right_rows: &[vibesql_storage::Row],
    left_col_idx: usize,
    right_col_idx: usize,
    remaining_condition: Option<&vibesql_ast::Expression>,
    schema: &CombinedSchema,
    database: &vibesql_storage::Database,
    timeout_ctx: &TimeoutContext,
) -> Result<Vec<vibesql_storage::Row>, ExecutorError> {
    let mut result_rows = Vec::new();
    let mut iterations = 0;

    // Create evaluator for remaining conditions if needed
    let evaluator = if remaining_condition.is_some() {
        Some(CombinedExpressionEvaluator::with_database(schema, database))
    } else {
        None
    };

    for left_row in left_rows {
        let left_value = &left_row.values[left_col_idx];

        for right_row in right_rows {
            // Check timeout periodically
            iterations += 1;
            if iterations % CHECK_INTERVAL == 0 {
                timeout_ctx.check()?;
            }

            let right_value = &right_row.values[right_col_idx];

            // OPTIMIZATION: Compare values BEFORE allocating combined_row
            // This prevents allocation for pairs that won't match
            if left_value != right_value {
                continue; // Skip this pair - equijoin doesn't match
            }

            // Values match! Now check remaining conditions if any
            if let Some(remaining_cond) = remaining_condition {
                // Need to create combined_row to evaluate remaining condition
                let combined_row = combine_rows(left_row, right_row);

                // Clear CSE cache before evaluation
                evaluator.as_ref().unwrap().clear_cse_cache();

                let matches =
                    match evaluator.as_ref().unwrap().eval(remaining_cond, &combined_row)? {
                        vibesql_types::SqlValue::Boolean(true) => true,
                        vibesql_types::SqlValue::Boolean(false) | vibesql_types::SqlValue::Null => {
                            false
                        }
                        other => {
                            return Err(ExecutorError::InvalidWhereClause(format!(
                                "JOIN condition must evaluate to boolean, got: {:?}",
                                other
                            )))
                        }
                    };

                if matches {
                    result_rows.push(combined_row);
                }
            } else {
                // No remaining conditions - equijoin matched, add the row
                result_rows.push(combine_rows(left_row, right_row));
            }
        }
    }

    Ok(result_rows)
}

/// Classic nested loop join algorithm (allocate then evaluate)
fn execute_nested_loop_classic(
    left_rows: &[vibesql_storage::Row],
    right_rows: &[vibesql_storage::Row],
    condition: &Option<vibesql_ast::Expression>,
    schema: &CombinedSchema,
    database: &vibesql_storage::Database,
    timeout_ctx: &TimeoutContext,
) -> Result<Vec<vibesql_storage::Row>, ExecutorError> {
    // OPTIMIZATION: Fast path for cross joins with no condition (#3388)
    // When there's no condition to evaluate, skip evaluator overhead entirely.
    // This is critical for 6-way cross joins with single-table predicates where
    // each join has no equijoin condition but tables are pre-filtered to small sizes.
    if condition.is_none() {
        return execute_cross_product_fast(left_rows, right_rows, timeout_ctx);
    }

    let evaluator = CombinedExpressionEvaluator::with_database(schema, database);
    let mut result_rows = Vec::new();
    let mut iterations = 0;

    for left_row in left_rows {
        for right_row in right_rows {
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
            let matches = match evaluator.eval(condition.as_ref().unwrap(), &combined_row)? {
                vibesql_types::SqlValue::Boolean(true) => true,
                vibesql_types::SqlValue::Boolean(false) => false,
                vibesql_types::SqlValue::Null => false,
                other => {
                    return Err(ExecutorError::InvalidWhereClause(format!(
                        "JOIN condition must evaluate to boolean, got: {:?}",
                        other
                    )))
                }
            };

            if matches {
                result_rows.push(combined_row);
            }
        }
    }

    Ok(result_rows)
}

/// Fast cross product implementation for joins with no condition (#3388)
///
/// This is an optimized path for cross joins where there's no condition to evaluate.
/// It avoids creating an evaluator and CSE cache, providing significant speedup for
/// multi-way cross joins with single-table predicates (like select4.test queries).
///
/// # Performance
/// For a 6-table cross join producing 6720 rows from pre-filtered tables:
/// - Old: ~37s (evaluator overhead per row)
/// - New: ~0.5s (direct row combination)
#[inline]
fn execute_cross_product_fast(
    left_rows: &[vibesql_storage::Row],
    right_rows: &[vibesql_storage::Row],
    timeout_ctx: &TimeoutContext,
) -> Result<Vec<vibesql_storage::Row>, ExecutorError> {
    // Pre-allocate result vector with exact capacity
    let result_size = left_rows.len() * right_rows.len();
    let mut result_rows = Vec::with_capacity(result_size);

    // Use a larger check interval for cross products since there's no condition evaluation
    // Check every 100K iterations instead of the default (typically 10K)
    const CROSS_CHECK_INTERVAL: usize = 100_000;
    let mut iterations = 0;

    for left_row in left_rows {
        for right_row in right_rows {
            // Check timeout less frequently since we're just doing row combination
            iterations += 1;
            if iterations % CROSS_CHECK_INTERVAL == 0 {
                timeout_ctx.check()?;
            }
            result_rows.push(combine_rows(left_row, right_row));
        }
    }

    Ok(result_rows)
}

/// Nested loop INNER JOIN implementation
pub(super) fn nested_loop_inner_join(
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
        Some(vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Boolean(true))) => true, // Always true = cartesian product
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

    // Extract right table name (assume single table for now)
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

    // Execute join with optimized strategy
    let result_rows = match eval_strategy {
        EquijoinEvalStrategy::Simple { left_col_idx, right_col_idx, remaining_condition } => {
            // FAST PATH: Evaluate equijoin by direct value comparison before allocation
            execute_optimized_equijoin(
                left_slice,
                right_slice,
                left_col_idx,
                right_col_idx,
                remaining_condition.as_ref(),
                &combined_schema,
                database,
                timeout_ctx,
            )?
        }
        EquijoinEvalStrategy::Complex => {
            // SLOW PATH: Use existing algorithm (allocate then evaluate)
            execute_nested_loop_classic(
                left_slice,
                right_slice,
                condition,
                &combined_schema,
                database,
                timeout_ctx,
            )?
        }
    };

    Ok(FromResult::from_rows(combined_schema, result_rows))
}

/// Nested loop LEFT OUTER JOIN implementation
pub(super) fn nested_loop_left_outer_join(
    left: FromResult,
    right: FromResult,
    condition: &Option<vibesql_ast::Expression>,
    database: &vibesql_storage::Database,
    timeout_ctx: &TimeoutContext,
) -> Result<FromResult, ExecutorError> {
    // Note: No memory check here. Hash join is selected in mod.rs BEFORE this function is called.
    // OUTER JOINs typically preserve at least the left table size, making estimates more reliable.

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

    let right_column_count = right_schema.columns.len();

    // Combine schemas
    let combined_schema =
        CombinedSchema::combine(left.schema.clone(), right_table_name, right_schema);
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

            // Combine rows using optimized helper (single allocation)
            let combined_row = combine_rows(left_row, right_row);

            // Clear CSE cache before evaluating join condition for this row combination
            // to prevent stale cached column values from previous combinations
            evaluator.clear_cse_cache();

            // Evaluate join condition
            let matches = if let Some(cond) = condition {
                match evaluator.eval(cond, &combined_row)? {
                    vibesql_types::SqlValue::Boolean(true) => true,
                    vibesql_types::SqlValue::Boolean(false) => false,
                    vibesql_types::SqlValue::Null => false,
                    other => {
                        return Err(ExecutorError::InvalidWhereClause(format!(
                            "JOIN condition must evaluate to boolean, got: {:?}",
                            other
                        )))
                    }
                }
            } else {
                true // No condition = CROSS JOIN
            };

            if matches {
                result_rows.push(combined_row);
                matched = true;
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

    Ok(FromResult::from_rows(combined_schema, result_rows))
}

/// Nested loop RIGHT OUTER JOIN implementation
pub(super) fn nested_loop_right_outer_join(
    left: FromResult,
    right: FromResult,
    condition: &Option<vibesql_ast::Expression>,
    database: &vibesql_storage::Database,
    timeout_ctx: &TimeoutContext,
) -> Result<FromResult, ExecutorError> {
    // Note: Memory check removed - delegates to LEFT OUTER JOIN which also doesn't check.

    // RIGHT OUTER JOIN = LEFT OUTER JOIN with sides swapped
    // Then we need to reorder columns to put left first, right second

    // Get the right column count before moving
    let right_col_count = right
        .schema
        .table_schemas
        .values()
        .next()
        .ok_or_else(|| ExecutorError::UnsupportedFeature("Complex JOIN".to_string()))?
        .1
        .columns
        .len();

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

    Ok(FromResult::from_rows(swapped_result.schema, reordered_rows))
}

/// Nested loop FULL OUTER JOIN implementation
pub(super) fn nested_loop_full_outer_join(
    left: FromResult,
    right: FromResult,
    condition: &Option<vibesql_ast::Expression>,
    database: &vibesql_storage::Database,
    timeout_ctx: &TimeoutContext,
) -> Result<FromResult, ExecutorError> {
    // Note: Memory check removed - full outer joins are rare and typically used
    // with smaller datasets. Hash join is tried first for equijoins anyway.

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

    let left_column_count = left
        .schema
        .table_schemas
        .values()
        .next()
        .ok_or_else(|| ExecutorError::UnsupportedFeature("Complex JOIN".to_string()))?
        .1
        .columns
        .len();
    let right_column_count = right_schema.columns.len();

    // Combine schemas
    let combined_schema =
        CombinedSchema::combine(left.schema.clone(), right_table_name, right_schema);
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
                match evaluator.eval(cond, &combined_row)? {
                    vibesql_types::SqlValue::Boolean(true) => true,
                    vibesql_types::SqlValue::Boolean(false) => false,
                    vibesql_types::SqlValue::Null => false,
                    other => {
                        return Err(ExecutorError::InvalidWhereClause(format!(
                            "JOIN condition must evaluate to boolean, got: {:?}",
                            other
                        )))
                    }
                }
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
pub(super) fn nested_loop_cross_join(
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

    // Combine schemas
    let combined_schema =
        CombinedSchema::combine(left.schema.clone(), right_table_name, right_schema);

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
            result_rows.push(combine_rows(left_row, right_row));
        }
    }

    Ok(FromResult::from_rows(combined_schema, result_rows))
}

/// Nested loop SEMI JOIN implementation
///
/// Semi-join returns left rows that have at least one match in the right table.
/// Unlike INNER JOIN, each left row is returned at most once (no duplicates).
pub(super) fn nested_loop_semi_join(
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

    // Create combined schema for condition evaluation
    let combined_schema =
        CombinedSchema::combine(left.schema.clone(), right_table_name, right_schema_def);

    // Create evaluator for condition
    let evaluator = CombinedExpressionEvaluator::with_database(&combined_schema, database);

    // Use as_slice() for zero-cost access without triggering row materialization
    let left_slice = left.as_slice();
    let right_slice = right.as_slice();

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

            let combined_row = combine_rows(left_row, right_row);

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
pub(super) fn nested_loop_anti_join(
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

    // Create combined schema for condition evaluation
    let combined_schema =
        CombinedSchema::combine(left.schema.clone(), right_table_name, right_schema_def);

    // Create evaluator for condition
    let evaluator = CombinedExpressionEvaluator::with_database(&combined_schema, database);

    // Use as_slice() for zero-cost access without triggering row materialization
    let left_slice = left.as_slice();
    let right_slice = right.as_slice();

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

            let combined_row = combine_rows(left_row, right_row);

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
