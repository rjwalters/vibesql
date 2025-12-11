#[cfg(feature = "parallel")]
use std::sync::{Arc, Mutex};

#[cfg(feature = "parallel")]
use crossbeam_deque::{Injector, Steal, Worker};

use super::{combine_rows, FromResult};
use crate::{
    errors::ExecutorError,
    evaluator::CombinedExpressionEvaluator,
    limits::MAX_MEMORY_BYTES,
    schema::CombinedSchema,
    timeout::{TimeoutContext, CHECK_INTERVAL},
};
#[cfg(feature = "parallel")]
use crate::select::morsel::{Morsel, MorselConfig};
#[cfg(feature = "parallel")]
use crate::select::parallel::ParallelConfig;

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
///
/// This function uses morsel-driven parallelism when the outer relation is large enough
/// to benefit from parallel execution. The decision is based on `ParallelConfig::should_parallelize_join()`.
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

    // OPTIMIZATION: Use morsel-driven parallelism for large joins (#4276)
    // Parallelize over the outer relation when it exceeds the join threshold.
    // Each worker processes a morsel of outer rows against the entire inner relation.
    #[cfg(feature = "parallel")]
    {
        let config = ParallelConfig::global();
        if config.should_parallelize_join(left_rows.len()) {
            return execute_nested_loop_parallel(
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

/// Environment variable to enable morsel execution debug logging for nested loop join
#[cfg(feature = "parallel")]
const NESTED_LOOP_DEBUG_ENV: &str = "NESTED_LOOP_DEBUG";

/// Check if nested loop debug logging is enabled
#[cfg(feature = "parallel")]
fn nested_loop_debug_enabled() -> bool {
    std::env::var(NESTED_LOOP_DEBUG_ENV).is_ok()
}

/// Create morsels from a row count (duplicated from morsel.rs for encapsulation)
#[cfg(feature = "parallel")]
fn create_morsels(total_rows: usize, morsel_size: usize) -> Vec<Morsel> {
    let mut morsels = Vec::with_capacity(total_rows.div_ceil(morsel_size));
    let mut start = 0;

    while start < total_rows {
        let count = (total_rows - start).min(morsel_size);
        morsels.push(Morsel::new(start, count));
        start += count;
    }

    morsels
}

/// Helper to steal a morsel from the injector queue
#[cfg(feature = "parallel")]
fn steal_morsel(injector: &Injector<Morsel>, worker: &Worker<Morsel>) -> Option<Morsel> {
    // Try local queue first
    worker.pop().or_else(|| {
        // Try to steal from global injector
        loop {
            match injector.steal() {
                Steal::Success(m) => return Some(m),
                Steal::Empty => return None,
                Steal::Retry => continue,
            }
        }
    })
}

/// Parallel nested loop join with morsel-driven parallelism.
///
/// Uses work-stealing to process morsels of the outer relation in parallel,
/// with each worker probing against the entire inner relation.
///
/// # Architecture
///
/// ```text
/// Outer Relation (morsels)
///     ├── Morsel 1 ──► Thread 1 ──► Probe Inner ──► Local Results
///     ├── Morsel 2 ──► Thread 2 ──► Probe Inner ──► Local Results
///     ├── Morsel 3 ──► Thread 3 ──► Probe Inner ──► Local Results
///     └── ...
///          └── Merge All Results (preserving order)
/// ```
///
/// # Performance
///
/// This provides 2-4x speedup for large nested loop joins where hash join
/// isn't applicable (complex predicates, non-equi joins).
#[cfg(feature = "parallel")]
#[allow(clippy::too_many_arguments)]
fn execute_nested_loop_parallel(
    outer_rows: &[vibesql_storage::Row],
    inner_rows: &[vibesql_storage::Row],
    condition: &Option<vibesql_ast::Expression>,
    schema: &CombinedSchema,
    database: &vibesql_storage::Database,
    timeout_ctx: &TimeoutContext,
) -> Result<Vec<vibesql_storage::Row>, ExecutorError> {
    let config = MorselConfig::optimal();
    let morsels = create_morsels(outer_rows.len(), config.morsel_size);
    let morsel_count = morsels.len();

    if nested_loop_debug_enabled() {
        eprintln!(
            "[NESTED_LOOP] Parallel join: {} outer rows, {} inner rows, {} morsels (size={})",
            outer_rows.len(),
            inner_rows.len(),
            morsel_count,
            config.morsel_size
        );
    }

    // Create global injector queue
    let injector: Injector<Morsel> = Injector::new();
    for morsel in morsels {
        injector.push(morsel);
    }

    // Thread-safe results storage: (morsel_start_idx, result_rows)
    // We track morsel order to maintain row ordering in final result
    type MorselResults = Arc<Mutex<Vec<(usize, Vec<vibesql_storage::Row>)>>>;
    let results: MorselResults = Arc::new(Mutex::new(Vec::with_capacity(morsel_count)));

    // Track any error that occurs during parallel execution
    let error: Arc<Mutex<Option<ExecutorError>>> = Arc::new(Mutex::new(None));

    // Process morsels in parallel using rayon's thread pool
    rayon::scope(|s| {
        let num_threads = rayon::current_num_threads();

        for _ in 0..num_threads {
            let injector_ref = &injector;
            let results_ref = results.clone();
            let error_ref = error.clone();

            s.spawn(move |_| {
                // Create thread-local worker queue
                let worker: Worker<Morsel> = Worker::new_fifo();

                // Create thread-local evaluator (CSE cache is per-thread)
                let evaluator = CombinedExpressionEvaluator::with_database(schema, database);

                while let Some(m) = steal_morsel(injector_ref, &worker) {
                    // Check if another thread hit an error
                    if error_ref.lock().unwrap().is_some() {
                        break;
                    }

                    let start_idx = m.start_idx();
                    let morsel_rows = m.rows(outer_rows);
                    let mut local_results = Vec::new();
                    let mut iterations = 0;

                    // Process this morsel against the entire inner relation
                    'outer: for outer_row in morsel_rows {
                        for inner_row in inner_rows {
                            // Check timeout periodically
                            iterations += 1;
                            if iterations % CHECK_INTERVAL == 0 {
                                if let Err(e) = timeout_ctx.check() {
                                    *error_ref.lock().unwrap() = Some(e);
                                    break 'outer;
                                }
                            }

                            // Combine rows using optimized helper (single allocation)
                            let combined_row = combine_rows(outer_row, inner_row);

                            // Evaluate join condition
                            let matches = match condition {
                                None => true, // No condition = CROSS JOIN behavior
                                Some(cond) => {
                                    // Clear CSE cache before evaluating join condition
                                    evaluator.clear_cse_cache();

                                    match evaluator.eval(cond, &combined_row) {
                                        Ok(vibesql_types::SqlValue::Boolean(true)) => true,
                                        Ok(vibesql_types::SqlValue::Boolean(false))
                                        | Ok(vibesql_types::SqlValue::Null) => false,
                                        Ok(other) => {
                                            *error_ref.lock().unwrap() =
                                                Some(ExecutorError::InvalidWhereClause(format!(
                                                    "JOIN condition must evaluate to boolean, got: {:?}",
                                                    other
                                                )));
                                            break 'outer;
                                        }
                                        Err(e) => {
                                            *error_ref.lock().unwrap() = Some(e);
                                            break 'outer;
                                        }
                                    }
                                }
                            };

                            if matches {
                                local_results.push(combined_row);
                            }
                        }
                    }

                    if nested_loop_debug_enabled() {
                        eprintln!(
                            "[NESTED_LOOP] Thread processed morsel at {} ({} outer rows -> {} results)",
                            start_idx,
                            morsel_rows.len(),
                            local_results.len()
                        );
                    }

                    // Store results with morsel index for ordering
                    results_ref.lock().unwrap().push((start_idx, local_results));
                }
            });
        }
    });

    // Check if any error occurred
    if let Some(e) = error.lock().unwrap().take() {
        return Err(e);
    }

    // Extract results after scope completes (all threads have finished)
    let mut sorted_results = Arc::try_unwrap(results)
        .expect("all threads should have completed")
        .into_inner()
        .expect("mutex not poisoned");

    // Sort by morsel start index to maintain row order
    sorted_results.sort_by_key(|(start_idx, _)| *start_idx);

    // Flatten results
    let total_rows: usize = sorted_results.iter().map(|(_, r)| r.len()).sum();
    let mut final_results = Vec::with_capacity(total_rows);
    for (_, result) in sorted_results {
        final_results.extend(result);
    }

    if nested_loop_debug_enabled() {
        eprintln!(
            "[NESTED_LOOP] Parallel join complete: {} morsels, {} result rows",
            morsel_count,
            final_results.len()
        );
    }

    Ok(final_results)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::schema::CombinedSchema;
    use vibesql_catalog::{ColumnSchema, TableSchema};
    use vibesql_storage::{Database, Row};
    use vibesql_types::{DataType, SqlValue};

    /// Helper to create a simple FromResult for testing
    fn create_test_from_result(
        table_name: &str,
        columns: Vec<(&str, DataType)>,
        rows: Vec<Vec<SqlValue>>,
    ) -> FromResult {
        let schema = TableSchema::new(
            table_name.to_string(),
            columns
                .iter()
                .map(|(name, dtype)| {
                    ColumnSchema::new(
                        name.to_string(),
                        dtype.clone(),
                        true, // nullable
                    )
                })
                .collect(),
        );

        let combined_schema = CombinedSchema::from_table(table_name.to_string(), schema);
        let rows = rows.into_iter().map(Row::new).collect();

        FromResult::from_rows(combined_schema, rows)
    }

    /// Create a CombinedSchema for testing
    fn create_combined_schema(
        left_table: &str,
        left_cols: Vec<(&str, DataType)>,
        right_table: &str,
        right_cols: Vec<(&str, DataType)>,
    ) -> CombinedSchema {
        let left_schema = TableSchema::new(
            left_table.to_string(),
            left_cols
                .iter()
                .map(|(name, dtype)| ColumnSchema::new(name.to_string(), dtype.clone(), true))
                .collect(),
        );

        let right_schema = TableSchema::new(
            right_table.to_string(),
            right_cols
                .iter()
                .map(|(name, dtype)| ColumnSchema::new(name.to_string(), dtype.clone(), true))
                .collect(),
        );

        CombinedSchema::combine(
            CombinedSchema::from_table(left_table.to_string(), left_schema),
            right_table.to_string(),
            right_schema,
        )
    }

    // ===== Tests for execute_nested_loop_classic (sequential path) =====

    #[test]
    fn test_nested_loop_classic_simple_condition() {
        // Test basic nested loop join with a simple equality condition
        let left_rows: Vec<Row> = vec![
            Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))]),
            Row::new(vec![SqlValue::Integer(2), SqlValue::Varchar(arcstr::ArcStr::from("Bob"))]),
            Row::new(vec![SqlValue::Integer(3), SqlValue::Varchar(arcstr::ArcStr::from("Charlie"))]),
        ];

        let right_rows: Vec<Row> = vec![
            Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(100)]),
            Row::new(vec![SqlValue::Integer(2), SqlValue::Integer(200)]),
            Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(150)]),
        ];

        let schema = create_combined_schema(
            "users",
            vec![("id", DataType::Integer), ("name", DataType::Varchar { max_length: Some(50) })],
            "orders",
            vec![("user_id", DataType::Integer), ("amount", DataType::Integer)],
        );

        let db = Database::default();
        let timeout_ctx = crate::timeout::TimeoutContext::new_default();

        // Condition: users.id = orders.user_id (column 0 = column 2 in combined row)
        let condition = vibesql_ast::Expression::BinaryOp {
            left: Box::new(vibesql_ast::Expression::ColumnRef {
                table: Some("users".to_string()),
                column: "id".to_string(),
            }),
            op: vibesql_ast::BinaryOperator::Equal,
            right: Box::new(vibesql_ast::Expression::ColumnRef {
                table: Some("orders".to_string()),
                column: "user_id".to_string(),
            }),
        };

        let result = execute_nested_loop_classic(
            &left_rows,
            &right_rows,
            &Some(condition),
            &schema,
            &db,
            &timeout_ctx,
        )
        .unwrap();

        // Should have 3 matches: Alice (1) x 2 orders, Bob (1) x 1 order
        assert_eq!(result.len(), 3);

        // Verify Alice appears twice (2 orders with user_id=1)
        let alice_count = result.iter().filter(|r| r.values[0] == SqlValue::Integer(1)).count();
        assert_eq!(alice_count, 2);

        // Verify Bob appears once
        let bob_count = result.iter().filter(|r| r.values[0] == SqlValue::Integer(2)).count();
        assert_eq!(bob_count, 1);

        // Verify Charlie doesn't appear (no orders)
        let charlie_count = result.iter().filter(|r| r.values[0] == SqlValue::Integer(3)).count();
        assert_eq!(charlie_count, 0);
    }

    #[test]
    fn test_nested_loop_classic_no_condition() {
        // Test cross product (no condition)
        let left_rows: Vec<Row> = vec![
            Row::new(vec![SqlValue::Integer(1)]),
            Row::new(vec![SqlValue::Integer(2)]),
        ];

        let right_rows: Vec<Row> = vec![
            Row::new(vec![SqlValue::Integer(10)]),
            Row::new(vec![SqlValue::Integer(20)]),
            Row::new(vec![SqlValue::Integer(30)]),
        ];

        let schema = create_combined_schema(
            "left",
            vec![("a", DataType::Integer)],
            "right",
            vec![("b", DataType::Integer)],
        );

        let db = Database::default();
        let timeout_ctx = crate::timeout::TimeoutContext::new_default();

        let result =
            execute_nested_loop_classic(&left_rows, &right_rows, &None, &schema, &db, &timeout_ctx)
                .unwrap();

        // Cross product: 2 x 3 = 6 rows
        assert_eq!(result.len(), 6);
    }

    #[test]
    fn test_nested_loop_classic_empty_input() {
        let left_rows: Vec<Row> = vec![];
        let right_rows: Vec<Row> = vec![Row::new(vec![SqlValue::Integer(1)])];

        let schema = create_combined_schema(
            "left",
            vec![("a", DataType::Integer)],
            "right",
            vec![("b", DataType::Integer)],
        );

        let db = Database::default();
        let timeout_ctx = crate::timeout::TimeoutContext::new_default();

        let result =
            execute_nested_loop_classic(&left_rows, &right_rows, &None, &schema, &db, &timeout_ctx)
                .unwrap();

        assert_eq!(result.len(), 0);
    }

    // ===== Tests for parallel nested loop join (when feature = "parallel") =====

    #[cfg(feature = "parallel")]
    mod parallel_tests {
        use super::*;

        #[test]
        fn test_parallel_nested_loop_simple() {
            // Test parallel nested loop with a simple condition
            let left_rows: Vec<Row> = (0..100)
                .map(|i| {
                    Row::new(vec![
                        SqlValue::Integer(i % 10), // id (0-9, repeating)
                        SqlValue::Varchar(arcstr::ArcStr::from(format!("left{}", i))),
                    ])
                })
                .collect();

            let right_rows: Vec<Row> = (0..50)
                .map(|i| {
                    Row::new(vec![
                        SqlValue::Integer(i % 10), // user_id (0-9, repeating)
                        SqlValue::Integer(i as i64 * 100),
                    ])
                })
                .collect();

            let schema = create_combined_schema(
                "users",
                vec![("id", DataType::Integer), ("name", DataType::Varchar { max_length: Some(50) })],
                "orders",
                vec![("user_id", DataType::Integer), ("amount", DataType::Integer)],
            );

            let db = Database::default();
            let timeout_ctx = crate::timeout::TimeoutContext::new_default();

            // Condition: users.id = orders.user_id
            let condition = vibesql_ast::Expression::BinaryOp {
                left: Box::new(vibesql_ast::Expression::ColumnRef {
                    table: Some("users".to_string()),
                    column: "id".to_string(),
                }),
                op: vibesql_ast::BinaryOperator::Equal,
                right: Box::new(vibesql_ast::Expression::ColumnRef {
                    table: Some("orders".to_string()),
                    column: "user_id".to_string(),
                }),
            };

            let result = execute_nested_loop_parallel(
                &left_rows,
                &right_rows,
                &Some(condition),
                &schema,
                &db,
                &timeout_ctx,
            )
            .unwrap();

            // Each of 10 keys appears 10 times in left (100/10) and 5 times in right (50/10)
            // So expected result: 10 keys * 10 left_matches * 5 right_matches = 500
            assert_eq!(result.len(), 500);
        }

        #[test]
        fn test_parallel_nested_loop_cross_product() {
            // Test parallel cross product (no condition)
            let left_rows: Vec<Row> =
                (0..100).map(|i| Row::new(vec![SqlValue::Integer(i)])).collect();

            let right_rows: Vec<Row> =
                (0..50).map(|i| Row::new(vec![SqlValue::Integer(i)])).collect();

            let schema = create_combined_schema(
                "left",
                vec![("a", DataType::Integer)],
                "right",
                vec![("b", DataType::Integer)],
            );

            let db = Database::default();
            let timeout_ctx = crate::timeout::TimeoutContext::new_default();

            let result = execute_nested_loop_parallel(
                &left_rows,
                &right_rows,
                &None,
                &schema,
                &db,
                &timeout_ctx,
            )
            .unwrap();

            // Cross product: 100 x 50 = 5000
            assert_eq!(result.len(), 5000);
        }

        #[test]
        fn test_parallel_nested_loop_empty_input() {
            let left_rows: Vec<Row> = vec![];
            let right_rows: Vec<Row> =
                (0..50).map(|i| Row::new(vec![SqlValue::Integer(i)])).collect();

            let schema = create_combined_schema(
                "left",
                vec![("a", DataType::Integer)],
                "right",
                vec![("b", DataType::Integer)],
            );

            let db = Database::default();
            let timeout_ctx = crate::timeout::TimeoutContext::new_default();

            let result = execute_nested_loop_parallel(
                &left_rows,
                &right_rows,
                &None,
                &schema,
                &db,
                &timeout_ctx,
            )
            .unwrap();

            assert_eq!(result.len(), 0);
        }

        #[test]
        fn test_parallel_nested_loop_no_matches() {
            // Test with condition that produces no matches
            let left_rows: Vec<Row> = (0..100)
                .map(|i| Row::new(vec![SqlValue::Integer(i), SqlValue::Integer(1)]))
                .collect();

            let right_rows: Vec<Row> = (100..150)
                .map(|i| Row::new(vec![SqlValue::Integer(i), SqlValue::Integer(2)]))
                .collect();

            let schema = create_combined_schema(
                "left",
                vec![("a", DataType::Integer), ("x", DataType::Integer)],
                "right",
                vec![("b", DataType::Integer), ("y", DataType::Integer)],
            );

            let db = Database::default();
            let timeout_ctx = crate::timeout::TimeoutContext::new_default();

            // Condition: left.a = right.b (no matches since ranges don't overlap)
            let condition = vibesql_ast::Expression::BinaryOp {
                left: Box::new(vibesql_ast::Expression::ColumnRef {
                    table: Some("left".to_string()),
                    column: "a".to_string(),
                }),
                op: vibesql_ast::BinaryOperator::Equal,
                right: Box::new(vibesql_ast::Expression::ColumnRef {
                    table: Some("right".to_string()),
                    column: "b".to_string(),
                }),
            };

            let result = execute_nested_loop_parallel(
                &left_rows,
                &right_rows,
                &Some(condition),
                &schema,
                &db,
                &timeout_ctx,
            )
            .unwrap();

            assert_eq!(result.len(), 0);
        }

        #[test]
        fn test_parallel_sequential_equivalence() {
            // Verify that parallel and sequential produce the same results
            let left_rows: Vec<Row> = (0..50)
                .map(|i| {
                    Row::new(vec![
                        SqlValue::Integer(i % 5),
                        SqlValue::Varchar(arcstr::ArcStr::from(format!("L{}", i))),
                    ])
                })
                .collect();

            let right_rows: Vec<Row> = (0..30)
                .map(|i| {
                    Row::new(vec![
                        SqlValue::Integer(i % 5),
                        SqlValue::Varchar(arcstr::ArcStr::from(format!("R{}", i))),
                    ])
                })
                .collect();

            let schema = create_combined_schema(
                "left",
                vec![("id", DataType::Integer), ("data", DataType::Varchar { max_length: Some(50) })],
                "right",
                vec![("id", DataType::Integer), ("info", DataType::Varchar { max_length: Some(50) })],
            );

            let db = Database::default();
            let timeout_ctx = crate::timeout::TimeoutContext::new_default();

            // Condition: left.id = right.id
            let condition = vibesql_ast::Expression::BinaryOp {
                left: Box::new(vibesql_ast::Expression::ColumnRef {
                    table: Some("left".to_string()),
                    column: "id".to_string(),
                }),
                op: vibesql_ast::BinaryOperator::Equal,
                right: Box::new(vibesql_ast::Expression::ColumnRef {
                    table: Some("right".to_string()),
                    column: "id".to_string(),
                }),
            };

            // Sequential result
            let seq_result = {
                let evaluator = CombinedExpressionEvaluator::with_database(&schema, &db);
                let mut result = Vec::new();
                for left_row in &left_rows {
                    for right_row in &right_rows {
                        let combined = combine_rows(left_row, right_row);
                        evaluator.clear_cse_cache();
                        if let Ok(SqlValue::Boolean(true)) = evaluator.eval(&condition, &combined) {
                            result.push(combined);
                        }
                    }
                }
                result
            };

            // Parallel result
            let par_result = execute_nested_loop_parallel(
                &left_rows,
                &right_rows,
                &Some(condition.clone()),
                &schema,
                &db,
                &timeout_ctx,
            )
            .unwrap();

            // Should have same number of results
            assert_eq!(
                seq_result.len(),
                par_result.len(),
                "Sequential and parallel should produce same result count"
            );

            // Both should be 50/5 * 30/5 * 5 = 10 * 6 * 5 = 300
            assert_eq!(seq_result.len(), 300);
        }

        #[test]
        fn test_parallel_nested_loop_large_dataset() {
            // Test with a larger dataset to ensure parallel execution is triggered
            let left_rows: Vec<Row> = (0..10000)
                .map(|i| Row::new(vec![SqlValue::Integer(i % 100), SqlValue::Integer(i)]))
                .collect();

            let right_rows: Vec<Row> = (0..1000)
                .map(|i| Row::new(vec![SqlValue::Integer(i % 100), SqlValue::Integer(i)]))
                .collect();

            let schema = create_combined_schema(
                "left",
                vec![("key", DataType::Integer), ("id", DataType::Integer)],
                "right",
                vec![("key", DataType::Integer), ("id", DataType::Integer)],
            );

            let db = Database::default();
            let timeout_ctx = crate::timeout::TimeoutContext::new_default();

            // Condition: left.key = right.key
            let condition = vibesql_ast::Expression::BinaryOp {
                left: Box::new(vibesql_ast::Expression::ColumnRef {
                    table: Some("left".to_string()),
                    column: "key".to_string(),
                }),
                op: vibesql_ast::BinaryOperator::Equal,
                right: Box::new(vibesql_ast::Expression::ColumnRef {
                    table: Some("right".to_string()),
                    column: "key".to_string(),
                }),
            };

            let result = execute_nested_loop_parallel(
                &left_rows,
                &right_rows,
                &Some(condition),
                &schema,
                &db,
                &timeout_ctx,
            )
            .unwrap();

            // Each key (0-99) appears 100 times in left and 10 times in right
            // Expected: 100 keys * 100 * 10 = 100,000
            assert_eq!(result.len(), 100_000);
        }
    }
}
