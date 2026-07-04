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
            calculate_frame_with_exclusion, evaluate_avg_window, evaluate_count_window,
            evaluate_group_concat_window_with_expr, evaluate_max_window, evaluate_min_window,
            evaluate_percentile_window, evaluate_sum_window, evaluate_total_window,
            partition_rows_with_collations, sort_partition_with_collations, validate_frame,
            validate_range_order_by, Partition,
        },
        CombinedExpressionEvaluator,
    },
};

#[cfg(feature = "parallel")]
use crate::select::parallel::ParallelConfig;

/// Result from evaluating a window function, including optional row reordering
pub(super) struct WindowEvaluationResult {
    /// Window function values in partition order
    pub values: Vec<SqlValue>,
    /// Row indices in partition order (if PARTITION BY is present)
    /// This maps: partition_order_index -> original_row_index
    pub partition_order: Option<Vec<usize>>,
}

/// Evaluate a single window function over all rows
///
/// When the `parallel` feature is enabled and there are multiple partitions,
/// partition sorting is parallelized for better performance on multi-core systems.
///
/// Returns values in partition order (grouped by partition, then by ORDER BY within partition)
/// along with the mapping from partition order to original row indices.
pub(super) fn evaluate_single_window_function(
    rows: &[Row],
    win_func: &WindowFunctionInfo,
    evaluator: &CombinedExpressionEvaluator,
) -> Result<WindowEvaluationResult, ExecutorError> {
    // Validate frame specification (checks for non-negative offsets, etc.)
    validate_frame(&win_func.window_spec.frame).map_err(ExecutorError::SqliteCompatError)?;
    // Validate RANGE with offset requires exactly one ORDER BY expression
    validate_range_order_by(&win_func.window_spec.frame, &win_func.window_spec.order_by)
        .map_err(ExecutorError::SqliteCompatError)?;

    // Extract function details including optional FILTER clause
    let (func_name, args, filter) = match &win_func.function_spec {
        WindowFunctionSpec::Aggregate { name, args, filter } => {
            (name.as_str(), args.as_slice(), filter.as_ref().map(|f| f.as_ref()))
        }
        WindowFunctionSpec::Ranking { name, args } => (name.as_str(), args.as_slice(), None),
        WindowFunctionSpec::Value { name, args } => (name.as_str(), args.as_slice(), None),
    };

    // Resolve collations for PARTITION BY expressions
    let partition_collations: Vec<Option<String>> =
        if let Some(partition_exprs) = &win_func.window_spec.partition_by {
            partition_exprs.iter().map(|e| evaluator.get_expression_collation(e)).collect()
        } else {
            Vec::new()
        };

    // Resolve collations for ORDER BY expressions
    let order_collations: Vec<Option<String>> =
        if let Some(order_items) = &win_func.window_spec.order_by {
            order_items.iter().map(|item| evaluator.get_expression_collation(&item.expr)).collect()
        } else {
            Vec::new()
        };

    // Partition rows using evaluator for column resolution
    let eval_fn = |expr: &Expression, row: &Row| -> Result<SqlValue, String> {
        // Clear CSE cache before evaluating each row to prevent column values
        // from being incorrectly cached across different rows
        evaluator.clear_cse_cache();
        evaluator.eval(expr, row).map_err(|e| format!("{:?}", e))
    };
    let mut partitions = partition_rows_with_collations(
        rows.to_vec(),
        &win_func.window_spec.partition_by,
        &partition_collations,
        eval_fn,
    )
    .map_err(ExecutorError::SqliteCompatError)?;

    // Build column name map for frame calculations (RANGE/GROUPS need to resolve named columns)
    let column_map = evaluator.get_schema().build_column_name_map();
    for partition in &mut partitions {
        partition.column_map = column_map.clone();
    }

    // Sort each partition with collation support - parallelize when beneficial
    #[cfg(feature = "parallel")]
    {
        let config = ParallelConfig::global();
        // Use parallel sorting when we have multiple partitions and enough rows
        // Threshold: parallelize if total rows > sort threshold AND partitions > 1
        if partitions.len() > 1 && config.should_parallelize_sort(rows.len()) {
            let order_by = &win_func.window_spec.order_by;
            let order_colls = &order_collations;
            // Get components for thread-local evaluators
            let sort_components = evaluator.get_parallel_components();
            partitions.par_iter_mut().for_each(|partition| {
                let (
                    schema,
                    database,
                    outer_row,
                    outer_schema,
                    window_mapping,
                    cte_context,
                    enable_cse,
                ) = sort_components;
                let local_evaluator = CombinedExpressionEvaluator::from_parallel_components(
                    schema,
                    database,
                    outer_row,
                    outer_schema,
                    window_mapping,
                    cte_context,
                    enable_cse,
                );
                let sort_eval_fn =
                    |expr: &Expression, row: &vibesql_storage::Row| -> Result<SqlValue, String> {
                        local_evaluator.clear_cse_cache();
                        local_evaluator.eval(expr, row).map_err(|e| format!("{:?}", e))
                    };
                sort_partition_with_collations(partition, order_by, order_colls, sort_eval_fn);
            });
        } else {
            for partition in &mut partitions {
                let sort_eval_fn =
                    |expr: &Expression, row: &vibesql_storage::Row| -> Result<SqlValue, String> {
                        evaluator.clear_cse_cache();
                        evaluator.eval(expr, row).map_err(|e| format!("{:?}", e))
                    };
                sort_partition_with_collations(
                    partition,
                    &win_func.window_spec.order_by,
                    &order_collations,
                    sort_eval_fn,
                );
            }
        }
    }

    #[cfg(not(feature = "parallel"))]
    {
        for partition in &mut partitions {
            let sort_eval_fn =
                |expr: &Expression, row: &vibesql_storage::Row| -> Result<SqlValue, String> {
                    evaluator.clear_cse_cache();
                    evaluator.eval(expr, row).map_err(|e| format!("{:?}", e))
                };
            sort_partition_with_collations(
                partition,
                &win_func.window_spec.order_by,
                &order_collations,
                sort_eval_fn,
            );
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
                    let (
                        schema,
                        database,
                        outer_row,
                        outer_schema,
                        window_mapping,
                        cte_context,
                        enable_cse,
                    ) = components;
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
                        filter,
                        &win_func.window_spec.order_by,
                        &win_func.window_spec.frame,
                        &order_collations,
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
            evaluate_partitions_sequential(
                &partitions,
                func_name,
                args,
                filter,
                &win_func.window_spec,
                &order_collations,
                evaluator,
            )?
        }
    };

    #[cfg(not(feature = "parallel"))]
    let results_with_indices = evaluate_partitions_sequential(
        &partitions,
        func_name,
        args,
        filter,
        &win_func.window_spec,
        &order_collations,
        evaluator,
    )?;

    // Determine if we have PARTITION BY or ORDER BY - if so, capture the window order
    let has_partition_by =
        win_func.window_spec.partition_by.as_ref().is_some_and(|p| !p.is_empty());
    let has_order_by = win_func.window_spec.order_by.as_ref().is_some_and(|o| !o.is_empty());

    // Capture window function order before sorting back to original
    // This captures the order after partitioning and sorting (the "window order")
    let partition_order: Option<Vec<usize>> = if has_partition_by || has_order_by {
        Some(results_with_indices.iter().map(|(idx, _)| *idx).collect())
    } else {
        None
    };

    // Always sort values back to original order for consistent indexing
    let mut results_with_indices = results_with_indices;
    results_with_indices.sort_by_key(|(idx, _)| *idx);
    let values = results_with_indices.into_iter().map(|(_, result)| result).collect();

    Ok(WindowEvaluationResult { values, partition_order })
}

/// Sequential evaluation of window function partitions
fn evaluate_partitions_sequential(
    partitions: &[Partition],
    func_name: &str,
    args: &[Expression],
    filter: Option<&Expression>,
    window_spec: &vibesql_ast::WindowSpec,
    order_collations: &[Option<String>],
    evaluator: &CombinedExpressionEvaluator,
) -> Result<Vec<(usize, SqlValue)>, ExecutorError> {
    let mut results_with_indices = Vec::new();

    for partition in partitions {
        let partition_results = evaluate_window_function_for_partition(
            partition,
            func_name,
            args,
            filter,
            &window_spec.order_by,
            &window_spec.frame,
            order_collations,
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
    filter: Option<&Expression>,
    order_by: &Option<Vec<vibesql_ast::OrderByItem>>,
    frame_spec: &Option<vibesql_ast::WindowFrame>,
    order_collations: &[Option<String>],
    evaluator: &CombinedExpressionEvaluator,
) -> Result<Vec<SqlValue>, ExecutorError> {
    // Create the eval_fn closure that uses the full evaluator
    let eval_fn =
        |expr: &Expression, row: &vibesql_storage::Row| -> Result<SqlValue, String> {
            evaluator.clear_cse_cache();
            evaluator.eval(expr, row).map_err(|e| format!("{:?}", e))
        };

    // Handle ranking functions (they don't use frames)
    let results = match func_name.to_uppercase().as_str() {
        "ROW_NUMBER" => {
            if !args.is_empty() {
                return Err(ExecutorError::WrongNumberOfArguments {
                    function_name: "row_number".to_string(),
                });
            }
            crate::evaluator::window::evaluate_row_number(partition)
        }
        "RANK" => {
            if !args.is_empty() {
                return Err(ExecutorError::WrongNumberOfArguments {
                    function_name: "rank".to_string(),
                });
            }
            crate::evaluator::window::evaluate_rank_with_collations(
                partition,
                order_by,
                order_collations,
                &eval_fn,
            )
        }
        "DENSE_RANK" => {
            if !args.is_empty() {
                return Err(ExecutorError::WrongNumberOfArguments {
                    function_name: "dense_rank".to_string(),
                });
            }
            crate::evaluator::window::evaluate_dense_rank_with_collations(
                partition,
                order_by,
                order_collations,
                &eval_fn,
            )
        }
        "PERCENT_RANK" => {
            if !args.is_empty() {
                return Err(ExecutorError::WrongNumberOfArguments {
                    function_name: "percent_rank".to_string(),
                });
            }
            // percent_rank delegates to rank internally, pass collations through
            let ranks = crate::evaluator::window::evaluate_rank_with_collations(
                partition,
                order_by,
                order_collations,
                &eval_fn,
            );
            let n = partition.len();
            if n <= 1 {
                vec![SqlValue::Numeric(0.0); n]
            } else {
                let denominator = (n - 1) as f64;
                ranks
                    .into_iter()
                    .map(|rank| {
                        if let SqlValue::Integer(r) = rank {
                            SqlValue::Numeric((r - 1) as f64 / denominator)
                        } else {
                            SqlValue::Numeric(0.0)
                        }
                    })
                    .collect()
            }
        }
        "CUME_DIST" => {
            if !args.is_empty() {
                return Err(ExecutorError::WrongNumberOfArguments {
                    function_name: "cume_dist".to_string(),
                });
            }
            crate::evaluator::window::evaluate_cume_dist_with_collations(
                partition,
                order_by,
                order_collations,
                &eval_fn,
            )
        }
        "NTILE" => {
            if args.len() != 1 {
                return Err(ExecutorError::WrongNumberOfArguments {
                    function_name: "ntile".to_string(),
                });
            }
            // Evaluate the NTILE argument first (even for empty partitions)
            // to ensure validation errors like ntile(0) are always reported
            let eval_row = if partition.is_empty() {
                &vibesql_storage::Row::new(vec![])
            } else {
                &partition.rows[0]
            };
            let n_value = evaluator.eval(&args[0], eval_row)?;
            let n = match n_value {
                vibesql_types::SqlValue::Integer(n) => n,
                _ => {
                    return Err(ExecutorError::SqliteCompatError(
                        "argument of ntile must be a positive integer".to_string(),
                    ))
                }
            };
            // Validate ntile argument is positive (even for empty partitions)
            if n <= 0 {
                return Err(ExecutorError::SqliteCompatError(
                    "argument of ntile must be a positive integer".to_string(),
                ));
            }
            // Handle empty partition after validation
            if partition.is_empty() {
                return Ok(vec![]);
            }
            crate::evaluator::window::evaluate_ntile(partition, n)
                .map_err(ExecutorError::SqliteCompatError)?
        }
        "LAG" => {
            // LAG(expr [, offset [, default]])
            if args.is_empty() {
                return Err(ExecutorError::UnsupportedExpression(
                    "LAG requires at least one argument (value expression)".to_string(),
                ));
            }
            // Handle empty partition
            if partition.is_empty() {
                return Ok(vec![]);
            }

            let value_expr = &args[0];
            let has_offset = args.len() > 1;
            let default_expr = if args.len() > 2 { Some(&args[2]) } else { None };

            // Create closure that evaluates expressions using the evaluator
            let eval_fn = |expr: &Expression, row: &Row| -> Result<SqlValue, String> {
                evaluator.clear_cse_cache();
                evaluator.eval(expr, row).map_err(|e| format!("{:?}", e))
            };

            // Evaluate LAG for each row in partition
            let mut results = Vec::with_capacity(partition.len());
            for row_idx in 0..partition.len() {
                // Evaluate offset per row (supports expressions like LAG(b, b))
                let offset = if has_offset {
                    let offset_value = evaluator.eval(&args[1], &partition.rows[row_idx])
                        .map_err(|e| ExecutorError::UnsupportedExpression(format!("{:?}", e)))?;
                    match offset_value {
                        SqlValue::Integer(n) => Some(n),
                        SqlValue::Null => Some(0),
                        _ => {
                            return Err(ExecutorError::UnsupportedExpression(
                                "LAG offset must be an integer".to_string(),
                            ))
                        }
                    }
                } else {
                    None // Default offset is 1
                };
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
            // Handle empty partition
            if partition.is_empty() {
                return Ok(vec![]);
            }

            let value_expr = &args[0];
            let has_offset = args.len() > 1;
            let default_expr = if args.len() > 2 { Some(&args[2]) } else { None };

            // Create closure that evaluates expressions using the evaluator
            let eval_fn = |expr: &Expression, row: &Row| -> Result<SqlValue, String> {
                evaluator.clear_cse_cache();
                evaluator.eval(expr, row).map_err(|e| format!("{:?}", e))
            };

            // Evaluate LEAD for each row in partition
            let mut results = Vec::with_capacity(partition.len());
            for row_idx in 0..partition.len() {
                // Evaluate offset per row (supports expressions like LEAD(b, b))
                let offset = if has_offset {
                    let offset_value = evaluator.eval(&args[1], &partition.rows[row_idx])
                        .map_err(|e| ExecutorError::UnsupportedExpression(format!("{:?}", e)))?;
                    match offset_value {
                        SqlValue::Integer(n) => Some(n),
                        SqlValue::Null => Some(0),
                        _ => {
                            return Err(ExecutorError::UnsupportedExpression(
                                "LEAD offset must be an integer".to_string(),
                            ))
                        }
                    }
                } else {
                    None // Default offset is 1
                };
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

            // FIRST_VALUE respects frame boundaries - evaluate per-row
            let mut results = Vec::with_capacity(partition.len());
            for row_idx in 0..partition.len() {
                let frame_result = calculate_frame_with_exclusion(
                    partition, row_idx, order_by, frame_spec, &eval_fn,
                );
                let value = if let Some(first_idx) =
                    frame_result.included_indices(partition, order_by, &eval_fn).next()
                {
                    eval_fn(value_expr, &partition.rows[first_idx])
                        .unwrap_or(SqlValue::Null)
                } else {
                    SqlValue::Null
                };
                results.push(value);
            }
            results
        }
        "LAST_VALUE" => {
            // LAST_VALUE(expr)
            if args.is_empty() {
                return Err(ExecutorError::UnsupportedExpression(
                    "LAST_VALUE requires an argument (value expression)".to_string(),
                ));
            }

            let value_expr = &args[0];

            // LAST_VALUE respects frame boundaries - evaluate per-row
            let mut results = Vec::with_capacity(partition.len());
            for row_idx in 0..partition.len() {
                let frame_result = calculate_frame_with_exclusion(
                    partition, row_idx, order_by, frame_spec, &eval_fn,
                );
                let value = frame_result
                    .included_indices(partition, order_by, &eval_fn)
                    .last()
                    .map(|last_idx| {
                        eval_fn(value_expr, &partition.rows[last_idx])
                            .unwrap_or(SqlValue::Null)
                    })
                    .unwrap_or(SqlValue::Null);
                results.push(value);
            }
            results
        }
        "NTH_VALUE" => {
            // NTH_VALUE(expr, n)
            if args.len() != 2 {
                return Err(ExecutorError::WrongNumberOfArguments {
                    function_name: "nth_value".to_string(),
                });
            }
            // Handle empty partition
            if partition.is_empty() {
                return Ok(vec![]);
            }

            let value_expr = &args[0];

            // NTH_VALUE respects frame boundaries - evaluate per-row
            // The n argument is evaluated per row to support expressions like nth_value(b, b+1)
            let mut results = Vec::with_capacity(partition.len());
            for row_idx in 0..partition.len() {
                // Evaluate n argument against current row
                let n_value = evaluator.eval(&args[1], &partition.rows[row_idx])
                    .map_err(|e| ExecutorError::UnsupportedExpression(format!("{:?}", e)))?;
                let n = match n_value {
                    SqlValue::Integer(n) => n,
                    _ => {
                        return Err(ExecutorError::UnsupportedExpression(
                            "NTH_VALUE second argument must be an integer".to_string(),
                        ))
                    }
                };

                if n < 1 {
                    return Err(ExecutorError::UnsupportedExpression(
                        format!("NTH_VALUE n must be a positive integer, got {}", n),
                    ));
                }

                let nth_zero_based = (n - 1) as usize;
                let frame_result = calculate_frame_with_exclusion(
                    partition, row_idx, order_by, frame_spec, &eval_fn,
                );
                let value = frame_result
                    .included_indices(partition, order_by, &eval_fn)
                    .nth(nth_zero_based)
                    .map(|nth_idx| {
                        eval_fn(value_expr, &partition.rows[nth_idx])
                            .unwrap_or(SqlValue::Null)
                    })
                    .unwrap_or(SqlValue::Null); // NULL if frame has fewer than N rows
                results.push(value);
            }
            results
        }
        _ => {
            // Handle aggregate functions that use frames
            let mut results: Vec<SqlValue> = Vec::with_capacity(partition.len());

            // Evaluate function for each row in the partition
            for row_idx in 0..partition.len() {
                // Calculate frame for this row with exclusion support
                let frame_result =
                    calculate_frame_with_exclusion(partition, row_idx, order_by, frame_spec, &eval_fn);

                // Get the iterator of included indices (applies EXCLUDE filtering)
                let frame_indices = frame_result.included_indices(partition, order_by, &eval_fn);

                // Create closure that evaluates expressions using the evaluator
                let agg_eval_fn = |expr: &Expression, row: &Row| -> Result<SqlValue, String> {
                    // Clear CSE cache before evaluating each row to prevent column values
                    // from being incorrectly cached across different rows
                    evaluator.clear_cse_cache();
                    evaluator.eval(expr, row).map_err(|e| format!("{:?}", e))
                };

                // Evaluate the aggregate function over the frame
                let value = match func_name.to_uppercase().as_str() {
                    "COUNT" => {
                        // COUNT(*) or COUNT(expr)
                        // Check if arg is the special "*" (Wildcard) or empty args
                        let arg_expr = if args.is_empty()
                            || matches!(&args[0], Expression::Wildcard)
                            || matches!(&args[0], Expression::ColumnRef(col_id) if col_id.column_canonical() == "*")
                        {
                            None // COUNT(*) should count all rows
                        } else {
                            Some(&args[0])
                        };
                        evaluate_count_window(partition, frame_indices, arg_expr, filter, agg_eval_fn)
                    }
                    "SUM" => {
                        if args.is_empty() {
                            return Err(ExecutorError::UnsupportedExpression(
                                "SUM requires an argument".to_string(),
                            ));
                        }
                        evaluate_sum_window(partition, frame_indices, &args[0], filter, agg_eval_fn)
                    }
                    "AVG" => {
                        if args.is_empty() {
                            return Err(ExecutorError::UnsupportedExpression(
                                "AVG requires an argument".to_string(),
                            ));
                        }
                        evaluate_avg_window(partition, frame_indices, &args[0], filter, agg_eval_fn)
                    }
                    "MIN" => {
                        if args.is_empty() {
                            return Err(ExecutorError::UnsupportedExpression(
                                "MIN requires an argument".to_string(),
                            ));
                        }
                        evaluate_min_window(partition, frame_indices, &args[0], filter, agg_eval_fn)
                    }
                    "MAX" => {
                        if args.is_empty() {
                            return Err(ExecutorError::UnsupportedExpression(
                                "MAX requires an argument".to_string(),
                            ));
                        }
                        evaluate_max_window(partition, frame_indices, &args[0], filter, agg_eval_fn)
                    }
                    "TOTAL" => {
                        if args.is_empty() {
                            return Err(ExecutorError::UnsupportedExpression(
                                "TOTAL requires an argument".to_string(),
                            ));
                        }
                        evaluate_total_window(partition, frame_indices, &args[0], filter, agg_eval_fn)
                    }
                    "GROUP_CONCAT" | "STRING_AGG" => {
                        if args.is_empty() {
                            return Err(ExecutorError::UnsupportedExpression(
                                "GROUP_CONCAT/STRING_AGG requires an argument".to_string(),
                            ));
                        }
                        // Pass separator expression for per-row evaluation
                        let separator_expr = if args.len() > 1 {
                            Some(&args[1])
                        } else {
                            None
                        };
                        evaluate_group_concat_window_with_expr(
                            partition,
                            frame_indices,
                            &args[0],
                            separator_expr,
                            ",",
                            filter,
                            agg_eval_fn,
                        )
                    }
                    "MEDIAN" | "PERCENTILE" | "PERCENTILE_CONT" | "PERCENTILE_DISC" => {
                        let fname = func_name.to_lowercase();
                        let expected_args = if fname == "median" { 1 } else { 2 };
                        if args.len() != expected_args {
                            return Err(ExecutorError::WrongNumberOfArguments {
                                function_name: fname,
                            });
                        }
                        let fraction_expr = args.get(1);
                        evaluate_percentile_window(
                            partition,
                            frame_indices,
                            &args[0],
                            fraction_expr,
                            &fname,
                            filter,
                            agg_eval_fn,
                        )?
                    }
                    "JSON_GROUP_ARRAY" => {
                        if args.is_empty() {
                            return Err(ExecutorError::UnsupportedExpression(
                                "JSON_GROUP_ARRAY requires an argument".to_string(),
                            ));
                        }
                        // Collect values in the frame and format as a JSON array
                        let mut json_values = Vec::new();
                        for idx in frame_indices {
                            // Apply FILTER clause
                            if let Some(filter_expr) = filter {
                                if let Ok(fv) = agg_eval_fn(filter_expr, &partition.rows[idx]) {
                                    if !is_filter_truthy(&fv) {
                                        continue;
                                    }
                                } else {
                                    continue;
                                }
                            }
                            let val = agg_eval_fn(&args[0], &partition.rows[idx])
                                .unwrap_or(SqlValue::Null);
                            json_values.push(sql_value_to_json_string(&val));
                        }
                        SqlValue::Varchar(format!("[{}]", json_values.join(",")).into())
                    }
                    "JSON_GROUP_OBJECT" => {
                        if args.len() < 2 {
                            return Err(ExecutorError::UnsupportedExpression(
                                "JSON_GROUP_OBJECT requires two arguments".to_string(),
                            ));
                        }
                        // Collect key-value pairs in the frame and format as a JSON object
                        let mut json_entries = Vec::new();
                        for idx in frame_indices {
                            // Apply FILTER clause
                            if let Some(filter_expr) = filter {
                                if let Ok(fv) = agg_eval_fn(filter_expr, &partition.rows[idx]) {
                                    if !is_filter_truthy(&fv) {
                                        continue;
                                    }
                                } else {
                                    continue;
                                }
                            }
                            let key = agg_eval_fn(&args[0], &partition.rows[idx])
                                .unwrap_or(SqlValue::Null);
                            let val = agg_eval_fn(&args[1], &partition.rows[idx])
                                .unwrap_or(SqlValue::Null);
                            let key_str = match &key {
                                SqlValue::Null => "null".to_string(),
                                other => escape_json_str(&other.to_string()),
                            };
                            json_entries
                                .push(format!("{}:{}", key_str, sql_value_to_json_string(&val)));
                        }
                        SqlValue::Varchar(format!("{{{}}}", json_entries.join(",")).into())
                    }
                    _ => {
                        return Err(ExecutorError::UnsupportedExpression(format!(
                            "{}() may not be used as a window function",
                            func_name.to_lowercase()
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

/// Check if a SQL value is truthy for FILTER clause (SQLite semantics)
fn is_filter_truthy(value: &SqlValue) -> bool {
    match value {
        SqlValue::Boolean(b) => *b,
        SqlValue::Null => false,
        SqlValue::Integer(n) => *n != 0,
        SqlValue::Smallint(n) => *n != 0,
        SqlValue::Bigint(n) => *n != 0,
        SqlValue::Float(f) => *f != 0.0,
        SqlValue::Real(f) => *f != 0.0,
        SqlValue::Double(f) => *f != 0.0,
        SqlValue::Numeric(f) => *f != 0.0,
        _ => false,
    }
}

/// Convert a SqlValue to its JSON string representation
///
/// Strings that are already valid JSON (objects, arrays) are embedded directly
/// without re-quoting, matching SQLite's json_group_array behavior.
fn sql_value_to_json_string(value: &SqlValue) -> String {
    match value {
        SqlValue::Null => "null".to_string(),
        SqlValue::Boolean(b) => if *b { "true" } else { "false" }.to_string(),
        SqlValue::Integer(i) => i.to_string(),
        SqlValue::Bigint(i) => i.to_string(),
        SqlValue::Smallint(i) => i.to_string(),
        SqlValue::Unsigned(u) => u.to_string(),
        SqlValue::Numeric(n) => n.to_string(),
        SqlValue::Real(r) => r.to_string(),
        SqlValue::Double(d) => d.to_string(),
        SqlValue::Float(f) => f.to_string(),
        SqlValue::Varchar(s) | SqlValue::Character(s) => {
            let trimmed = s.trim();
            // If the string looks like a JSON object or array, embed it directly
            if (trimmed.starts_with('{') && trimmed.ends_with('}'))
                || (trimmed.starts_with('[') && trimmed.ends_with(']'))
            {
                s.to_string()
            } else {
                escape_json_str(s)
            }
        }
        other => escape_json_str(&other.to_string()),
    }
}

/// Escape a string for JSON output, wrapping in double quotes
fn escape_json_str(s: &str) -> String {
    let mut result = String::with_capacity(s.len() + 2);
    result.push('"');
    for c in s.chars() {
        match c {
            '"' => result.push_str("\\\""),
            '\\' => result.push_str("\\\\"),
            '\n' => result.push_str("\\n"),
            '\r' => result.push_str("\\r"),
            '\t' => result.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                result.push_str(&format!("\\u{:04x}", c as u32));
            }
            c => result.push(c),
        }
    }
    result.push('"');
    result
}
