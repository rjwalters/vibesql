//! Sequential nested loop join implementations.
//!
//! This module provides sequential (non-parallel) implementations of nested loop
//! join algorithms, including optimized equijoin and fast cross product paths.

use super::condition::{eval_join_condition_to_bool, EquijoinEvalStrategy};
use crate::{
    errors::ExecutorError,
    evaluator::CombinedExpressionEvaluator,
    schema::CombinedSchema,
    timeout::{TimeoutContext, CHECK_INTERVAL},
};

/// Execute optimized equijoin by comparing values before allocating combined_row
#[allow(clippy::too_many_arguments)]
pub fn execute_optimized_equijoin(
    left_rows: &[vibesql_storage::Row],
    right_rows: &[vibesql_storage::Row],
    left_col_idx: usize,
    right_col_idx: usize,
    remaining_condition: Option<&vibesql_ast::Expression>,
    schema: &CombinedSchema,
    database: &vibesql_storage::Database,
    timeout_ctx: &TimeoutContext,
    left_table_names: &[String],
    right_table_names: &[String],
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
                // Use combine_for_join to preserve ROWIDs (issue #4370)
                let combined_row = vibesql_storage::Row::combine_for_join(
                    left_row,
                    right_row,
                    left_table_names,
                    right_table_names,
                );

                // Clear CSE cache before evaluation
                evaluator.as_ref().unwrap().clear_cse_cache();

                let matches = eval_join_condition_to_bool(
                    evaluator.as_ref().unwrap().eval(remaining_cond, &combined_row)?,
                )?;

                if matches {
                    result_rows.push(combined_row);
                }
            } else {
                // No remaining conditions - equijoin matched, add the row
                // Use combine_for_join to preserve ROWIDs (issue #4370)
                result_rows.push(vibesql_storage::Row::combine_for_join(
                    left_row,
                    right_row,
                    left_table_names,
                    right_table_names,
                ));
            }
        }
    }

    Ok(result_rows)
}

/// Classic nested loop join algorithm (allocate then evaluate)
///
/// This function uses morsel-driven parallelism when the outer relation is large enough
/// to benefit from parallel execution. The decision is based on
/// `ParallelConfig::should_parallelize_join()`.
#[allow(clippy::too_many_arguments)]
pub fn execute_nested_loop_classic(
    left_rows: &[vibesql_storage::Row],
    right_rows: &[vibesql_storage::Row],
    condition: &Option<vibesql_ast::Expression>,
    schema: &CombinedSchema,
    database: &vibesql_storage::Database,
    timeout_ctx: &TimeoutContext,
    left_table_names: &[String],
    right_table_names: &[String],
) -> Result<Vec<vibesql_storage::Row>, ExecutorError> {
    // OPTIMIZATION: Fast path for cross joins with no condition (#3388)
    // When there's no condition to evaluate, skip evaluator overhead entirely.
    // This is critical for 6-way cross joins with single-table predicates where
    // each join has no equijoin condition but tables are pre-filtered to small sizes.
    if condition.is_none() {
        return execute_cross_product_fast(
            left_rows,
            right_rows,
            timeout_ctx,
            left_table_names,
            right_table_names,
        );
    }

    // OPTIMIZATION: Use morsel-driven parallelism for large joins (#4276)
    // Parallelize over the outer relation when it exceeds the join threshold.
    // Each worker processes a morsel of outer rows against the entire inner relation.
    #[cfg(feature = "parallel")]
    {
        use crate::select::parallel::ParallelConfig;
        let config = ParallelConfig::global();
        if config.should_parallelize_join(left_rows.len()) {
            return super::parallel::execute_nested_loop_parallel(
                left_rows,
                right_rows,
                condition,
                schema,
                database,
                timeout_ctx,
            );
        }
    }

    // Sequential fallback for small datasets or non-parallel builds
    execute_nested_loop_sequential(
        left_rows,
        right_rows,
        condition,
        schema,
        database,
        timeout_ctx,
        left_table_names,
        right_table_names,
    )
}

/// Pure sequential nested loop join (no parallel dispatch)
#[allow(clippy::too_many_arguments)]
pub fn execute_nested_loop_sequential(
    left_rows: &[vibesql_storage::Row],
    right_rows: &[vibesql_storage::Row],
    condition: &Option<vibesql_ast::Expression>,
    schema: &CombinedSchema,
    database: &vibesql_storage::Database,
    timeout_ctx: &TimeoutContext,
    left_table_names: &[String],
    right_table_names: &[String],
) -> Result<Vec<vibesql_storage::Row>, ExecutorError> {
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

            // Combine rows preserving ROWIDs for multi-table queries (issue #4370)
            let combined_row = vibesql_storage::Row::combine_for_join(
                left_row,
                right_row,
                left_table_names,
                right_table_names,
            );

            // Clear CSE cache before evaluating join condition for this row combination
            // to prevent stale cached column values from previous combinations
            evaluator.clear_cse_cache();

            // Evaluate join condition
            let matches = eval_join_condition_to_bool(
                evaluator.eval(condition.as_ref().unwrap(), &combined_row)?,
            )?;

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
pub fn execute_cross_product_fast(
    left_rows: &[vibesql_storage::Row],
    right_rows: &[vibesql_storage::Row],
    timeout_ctx: &TimeoutContext,
    left_table_names: &[String],
    right_table_names: &[String],
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
            // Use combine_for_join for proper ROWID tracking (issue #4370)
            result_rows.push(vibesql_storage::Row::combine_for_join(
                left_row,
                right_row,
                left_table_names,
                right_table_names,
            ));
        }
    }

    Ok(result_rows)
}

/// Execute join with selected strategy
#[allow(clippy::too_many_arguments)]
pub fn execute_with_strategy(
    left_rows: &[vibesql_storage::Row],
    right_rows: &[vibesql_storage::Row],
    condition: &Option<vibesql_ast::Expression>,
    eval_strategy: EquijoinEvalStrategy,
    schema: &CombinedSchema,
    database: &vibesql_storage::Database,
    timeout_ctx: &TimeoutContext,
    left_table_names: &[String],
    right_table_names: &[String],
) -> Result<Vec<vibesql_storage::Row>, ExecutorError> {
    match eval_strategy {
        EquijoinEvalStrategy::Simple { left_col_idx, right_col_idx, remaining_condition } => {
            // FAST PATH: Evaluate equijoin by direct value comparison before allocation
            execute_optimized_equijoin(
                left_rows,
                right_rows,
                left_col_idx,
                right_col_idx,
                remaining_condition.as_deref(),
                schema,
                database,
                timeout_ctx,
                left_table_names,
                right_table_names,
            )
        }
        EquijoinEvalStrategy::Complex => {
            // SLOW PATH: Use existing algorithm (allocate then evaluate)
            execute_nested_loop_classic(
                left_rows,
                right_rows,
                condition,
                schema,
                database,
                timeout_ctx,
                left_table_names,
                right_table_names,
            )
        }
    }
}
