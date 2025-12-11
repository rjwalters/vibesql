//! Core window function evaluation logic
//!
//! When the `parallel` feature is enabled and there are multiple partitions,
//! partition sorting and evaluation is parallelized using rayon.

use vibesql_ast::{Expression, WindowFunctionSpec};
use vibesql_storage::Row;
use vibesql_types::SqlValue;

#[cfg(feature = "parallel")]
use rayon::prelude::*;

use super::types::WindowFunctionInfo;
use crate::{
    errors::ExecutorError,
    evaluator::{
        window::{
            calculate_frame, evaluate_avg_window, evaluate_count_window, evaluate_max_window,
            evaluate_min_window, evaluate_sum_window, partition_rows, sort_partition, Partition,
        },
        CombinedExpressionEvaluator,
    },
};

#[cfg(feature = "parallel")]
use crate::select::parallel::ParallelConfig;

/// Evaluate a single window function over all rows
///
/// When the `parallel` feature is enabled and there are multiple partitions,
/// partition sorting is parallelized for better performance on multi-core systems.
pub(super) fn evaluate_single_window_function(
    rows: &[Row],
    win_func: &WindowFunctionInfo,
    evaluator: &CombinedExpressionEvaluator,
) -> Result<Vec<SqlValue>, ExecutorError> {
    // Extract function details
    let (func_name, args) = match &win_func.function_spec {
        WindowFunctionSpec::Aggregate { name, args } => (name.as_str(), args.as_slice()),
        WindowFunctionSpec::Ranking { name, args } => (name.as_str(), args.as_slice()),
        WindowFunctionSpec::Value { name, args } => (name.as_str(), args.as_slice()),
    };

    // Partition rows using evaluator for column resolution
    let eval_fn = |expr: &Expression, row: &Row| -> Result<SqlValue, String> {
        // Clear CSE cache before evaluating each row to prevent column values
        // from being incorrectly cached across different rows
        evaluator.clear_cse_cache();
        evaluator.eval(expr, row).map_err(|e| format!("{:?}", e))
    };
    let mut partitions = partition_rows(rows.to_vec(), &win_func.window_spec.partition_by, eval_fn);

    // Sort each partition - parallelize when beneficial
    #[cfg(feature = "parallel")]
    {
        let config = ParallelConfig::global();
        // Use parallel sorting when we have multiple partitions and enough rows
        // Threshold: parallelize if total rows > sort threshold AND partitions > 1
        if partitions.len() > 1 && config.should_parallelize_sort(rows.len()) {
            let order_by = &win_func.window_spec.order_by;
            partitions.par_iter_mut().for_each(|partition| {
                sort_partition(partition, order_by);
            });
        } else {
            for partition in &mut partitions {
                sort_partition(partition, &win_func.window_spec.order_by);
            }
        }
    }

    #[cfg(not(feature = "parallel"))]
    {
        for partition in &mut partitions {
            sort_partition(partition, &win_func.window_spec.order_by);
        }
    }

    // Evaluate window function for each partition
    // We need to collect results with their original indices, then reorder

    #[cfg(feature = "parallel")]
    let results_with_indices = {
        let config = ParallelConfig::global();
        // Parallelize evaluation when we have multiple partitions and enough rows
        if partitions.len() > 1 && config.should_parallelize_aggregate(rows.len()) {
            // Get components for thread-local evaluators
            let components = evaluator.get_parallel_components();

            // Parallel evaluation of partitions
            let partition_results: Vec<Result<Vec<(usize, SqlValue)>, ExecutorError>> = partitions
                .par_iter()
                .map(|partition| {
                    // Create thread-local evaluator
                    let (schema, database, outer_row, outer_schema, window_mapping, cte_context, enable_cse) = components;
                    let local_evaluator = CombinedExpressionEvaluator::from_parallel_components(
                        schema,
                        database,
                        outer_row,
                        outer_schema,
                        window_mapping,
                        cte_context,
                        enable_cse,
                    );

                    let partition_results = evaluate_window_function_for_partition(
                        partition,
                        func_name,
                        args,
                        &win_func.window_spec.order_by,
                        &win_func.window_spec.frame,
                        &local_evaluator,
                    )?;

                    // Pair each result with its original index
                    let indexed: Vec<(usize, SqlValue)> = partition_results
                        .into_iter()
                        .zip(partition.original_indices.iter())
                        .map(|(result, &idx)| (idx, result))
                        .collect();

                    Ok(indexed)
                })
                .collect();

            // Flatten results and check for errors
            let mut all_results = Vec::with_capacity(rows.len());
            for result in partition_results {
                all_results.extend(result?);
            }
            all_results
        } else {
            // Sequential fallback for small datasets
            evaluate_partitions_sequential(&partitions, func_name, args, &win_func.window_spec, evaluator)?
        }
    };

    #[cfg(not(feature = "parallel"))]
    let results_with_indices = evaluate_partitions_sequential(&partitions, func_name, args, &win_func.window_spec, evaluator)?;

    // Sort by original index to restore original row order
    let mut results_with_indices = results_with_indices;
    results_with_indices.sort_by_key(|(idx, _)| *idx);

    // Extract just the results
    let all_results = results_with_indices.into_iter().map(|(_, result)| result).collect();

    Ok(all_results)
}

/// Sequential evaluation of window function partitions
fn evaluate_partitions_sequential(
    partitions: &[Partition],
    func_name: &str,
    args: &[Expression],
    window_spec: &vibesql_ast::WindowSpec,
    evaluator: &CombinedExpressionEvaluator,
) -> Result<Vec<(usize, SqlValue)>, ExecutorError> {
    let mut results_with_indices = Vec::new();

    for partition in partitions {
        let partition_results = evaluate_window_function_for_partition(
            partition,
            func_name,
            args,
            &window_spec.order_by,
            &window_spec.frame,
            evaluator,
        )?;

        // Pair each result with its original index
        for (result, &original_idx) in
            partition_results.iter().zip(partition.original_indices.iter())
        {
            results_with_indices.push((original_idx, result.clone()));
        }
    }

    Ok(results_with_indices)
}

/// Evaluate a window function for a single partition
fn evaluate_window_function_for_partition(
    partition: &Partition,
    func_name: &str,
    args: &[Expression],
    order_by: &Option<Vec<vibesql_ast::OrderByItem>>,
    frame_spec: &Option<vibesql_ast::WindowFrame>,
    evaluator: &CombinedExpressionEvaluator,
) -> Result<Vec<SqlValue>, ExecutorError> {
    // Handle ranking functions (they don't use frames)
    let results = match func_name.to_uppercase().as_str() {
        "ROW_NUMBER" => crate::evaluator::window::evaluate_row_number(partition),
        "RANK" => crate::evaluator::window::evaluate_rank(partition, order_by),
        "DENSE_RANK" => crate::evaluator::window::evaluate_dense_rank(partition, order_by),
        "NTILE" => {
            if args.is_empty() {
                return Err(ExecutorError::UnsupportedExpression(
                    "NTILE requires an argument".to_string(),
                ));
            }
            // Evaluate the NTILE argument (should be a constant)
            let n_value = evaluator.eval(&args[0], &partition.rows[0])?;
            let n = match n_value {
                vibesql_types::SqlValue::Integer(n) => n,
                _ => {
                    return Err(ExecutorError::UnsupportedExpression(
                        "NTILE argument must be an integer".to_string(),
                    ))
                }
            };
            crate::evaluator::window::evaluate_ntile(partition, n)
                .map_err(ExecutorError::UnsupportedExpression)?
        }
        "LAG" => {
            // LAG(expr [, offset [, default]])
            if args.is_empty() {
                return Err(ExecutorError::UnsupportedExpression(
                    "LAG requires at least one argument (value expression)".to_string(),
                ));
            }

            let value_expr = &args[0];
            let offset = if args.len() > 1 {
                // Evaluate offset argument (should be constant)
                let offset_value = evaluator.eval(&args[1], &partition.rows[0])?;
                match offset_value {
                    SqlValue::Integer(n) => Some(n),
                    _ => {
                        return Err(ExecutorError::UnsupportedExpression(
                            "LAG offset must be an integer".to_string(),
                        ))
                    }
                }
            } else {
                None // Default offset is 1
            };

            let default_expr = if args.len() > 2 { Some(&args[2]) } else { None };

            // Create closure that evaluates expressions using the evaluator
            let eval_fn = |expr: &Expression, row: &Row| -> Result<SqlValue, String> {
                evaluator.clear_cse_cache();
                evaluator.eval(expr, row).map_err(|e| format!("{:?}", e))
            };

            // Evaluate LAG for each row in partition
            let mut results = Vec::with_capacity(partition.len());
            for row_idx in 0..partition.len() {
                let value = crate::evaluator::window::evaluate_lag(
                    partition,
                    row_idx,
                    value_expr,
                    offset,
                    default_expr,
                    eval_fn,
                )
                .map_err(ExecutorError::UnsupportedExpression)?;
                results.push(value);
            }
            results
        }
        "LEAD" => {
            // LEAD(expr [, offset [, default]])
            if args.is_empty() {
                return Err(ExecutorError::UnsupportedExpression(
                    "LEAD requires at least one argument (value expression)".to_string(),
                ));
            }

            let value_expr = &args[0];
            let offset = if args.len() > 1 {
                // Evaluate offset argument (should be constant)
                let offset_value = evaluator.eval(&args[1], &partition.rows[0])?;
                match offset_value {
                    SqlValue::Integer(n) => Some(n),
                    _ => {
                        return Err(ExecutorError::UnsupportedExpression(
                            "LEAD offset must be an integer".to_string(),
                        ))
                    }
                }
            } else {
                None // Default offset is 1
            };

            let default_expr = if args.len() > 2 { Some(&args[2]) } else { None };

            // Create closure that evaluates expressions using the evaluator
            let eval_fn = |expr: &Expression, row: &Row| -> Result<SqlValue, String> {
                evaluator.clear_cse_cache();
                evaluator.eval(expr, row).map_err(|e| format!("{:?}", e))
            };

            // Evaluate LEAD for each row in partition
            let mut results = Vec::with_capacity(partition.len());
            for row_idx in 0..partition.len() {
                let value = crate::evaluator::window::evaluate_lead(
                    partition,
                    row_idx,
                    value_expr,
                    offset,
                    default_expr,
                    eval_fn,
                )
                .map_err(ExecutorError::UnsupportedExpression)?;
                results.push(value);
            }
            results
        }
        "FIRST_VALUE" => {
            // FIRST_VALUE(expr)
            if args.is_empty() {
                return Err(ExecutorError::UnsupportedExpression(
                    "FIRST_VALUE requires an argument (value expression)".to_string(),
                ));
            }

            let value_expr = &args[0];

            // Create closure that evaluates expressions using the evaluator
            let eval_fn = |expr: &Expression, row: &Row| -> Result<SqlValue, String> {
                evaluator.clear_cse_cache();
                evaluator.eval(expr, row).map_err(|e| format!("{:?}", e))
            };

            // FIRST_VALUE returns the same value for all rows in the partition
            // (the value from the first row)
            let value =
                crate::evaluator::window::evaluate_first_value(partition, value_expr, eval_fn)
                    .map_err(ExecutorError::UnsupportedExpression)?;

            // Return the same value for all rows
            vec![value; partition.len()]
        }
        "LAST_VALUE" => {
            // LAST_VALUE(expr)
            if args.is_empty() {
                return Err(ExecutorError::UnsupportedExpression(
                    "LAST_VALUE requires an argument (value expression)".to_string(),
                ));
            }

            let value_expr = &args[0];

            // Create closure that evaluates expressions using the evaluator
            let eval_fn = |expr: &Expression, row: &Row| -> Result<SqlValue, String> {
                evaluator.clear_cse_cache();
                evaluator.eval(expr, row).map_err(|e| format!("{:?}", e))
            };

            // LAST_VALUE returns the same value for all rows in the partition
            // (the value from the last row)
            let value =
                crate::evaluator::window::evaluate_last_value(partition, value_expr, eval_fn)
                    .map_err(ExecutorError::UnsupportedExpression)?;

            // Return the same value for all rows
            vec![value; partition.len()]
        }
        _ => {
            // Handle aggregate functions that use frames
            let mut results: Vec<SqlValue> = Vec::with_capacity(partition.len());

            // Evaluate function for each row in the partition
            for row_idx in 0..partition.len() {
                // Calculate frame for this row
                let frame = calculate_frame(partition, row_idx, order_by, frame_spec);

                // Create closure that evaluates expressions using the evaluator
                let eval_fn = |expr: &Expression, row: &Row| -> Result<SqlValue, String> {
                    // Clear CSE cache before evaluating each row to prevent column values
                    // from being incorrectly cached across different rows
                    evaluator.clear_cse_cache();
                    evaluator.eval(expr, row).map_err(|e| format!("{:?}", e))
                };

                // Evaluate the aggregate function over the frame
                let value = match func_name.to_uppercase().as_str() {
                    "COUNT" => {
                        // COUNT(*) or COUNT(expr)
                        // Check if arg is the special "*" column reference
                        let arg_expr = if args.is_empty()
                            || matches!(&args[0], Expression::ColumnRef { column, .. } if column == "*")
                        {
                            None // COUNT(*) should count all rows
                        } else {
                            Some(&args[0])
                        };
                        evaluate_count_window(partition, &frame, arg_expr, eval_fn)
                    }
                    "SUM" => {
                        if args.is_empty() {
                            return Err(ExecutorError::UnsupportedExpression(
                                "SUM requires an argument".to_string(),
                            ));
                        }
                        evaluate_sum_window(partition, &frame, &args[0], eval_fn)
                    }
                    "AVG" => {
                        if args.is_empty() {
                            return Err(ExecutorError::UnsupportedExpression(
                                "AVG requires an argument".to_string(),
                            ));
                        }
                        evaluate_avg_window(partition, &frame, &args[0], eval_fn)
                    }
                    "MIN" => {
                        if args.is_empty() {
                            return Err(ExecutorError::UnsupportedExpression(
                                "MIN requires an argument".to_string(),
                            ));
                        }
                        evaluate_min_window(partition, &frame, &args[0], eval_fn)
                    }
                    "MAX" => {
                        if args.is_empty() {
                            return Err(ExecutorError::UnsupportedExpression(
                                "MAX requires an argument".to_string(),
                            ));
                        }
                        evaluate_max_window(partition, &frame, &args[0], eval_fn)
                    }
                    _ => {
                        return Err(ExecutorError::UnsupportedExpression(format!(
                            "Unsupported window function: {}",
                            func_name
                        )))
                    }
                };

                results.push(value);
            }

            results
        }
    };

    Ok(results)
}
