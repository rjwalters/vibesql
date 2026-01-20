//! Window function evaluation for aggregate queries
//!
//! This module handles window functions in GROUP BY queries, including:
//! - Aggregate window functions like AVG(SUM(x)) OVER (...)
//! - Value window functions like LEAD(x) OVER (...)
//! - Ranking window functions like ROW_NUMBER() OVER (...)
//!
//! After GROUP BY processing computes the inner values, this module applies the
//! window function over the aggregated rows.

use vibesql_ast::{Expression, SelectItem, WindowFunctionSpec, WindowSpec};
use vibesql_catalog::ColumnSchema;
use vibesql_storage::Row;
use vibesql_types::{DataType, SqlValue};

use crate::{
    errors::ExecutorError,
    evaluator::{
        window::{
            calculate_frame_with_exclusion, evaluate_avg_window, evaluate_count_window,
            evaluate_cume_dist, evaluate_dense_rank, evaluate_first_value, evaluate_lag,
            evaluate_last_value, evaluate_lead, evaluate_max_window, evaluate_min_window,
            evaluate_nth_value, evaluate_ntile, evaluate_percent_rank, evaluate_rank,
            evaluate_row_number, evaluate_sum_window, partition_rows, sort_partition,
            validate_frame,
        },
        CombinedExpressionEvaluator,
    },
    schema::CombinedSchema,
};

/// The type of window function for dispatch
#[derive(Debug, Clone)]
enum WindowFunctionType {
    /// Aggregate: SUM, AVG, COUNT, MIN, MAX, etc.
    Aggregate,
    /// Ranking: ROW_NUMBER, RANK, DENSE_RANK, NTILE, PERCENT_RANK, CUME_DIST
    Ranking,
    /// Value: LAG, LEAD, FIRST_VALUE, LAST_VALUE, NTH_VALUE
    Value,
}

/// Information about a window function that needs post-aggregation evaluation
struct PostAggregateWindowFunction {
    /// Index in the SELECT list / result row
    select_index: usize,
    /// The window function name
    func_name: String,
    /// The type of window function
    func_type: WindowFunctionType,
    /// Arguments to the window function
    args: Vec<Expression>,
    /// The window specification (PARTITION BY, ORDER BY, frame)
    window_spec: WindowSpec,
}

/// Check if the SELECT list contains window functions that need post-aggregation evaluation
pub(super) fn has_aggregate_window_functions(select_list: &[SelectItem]) -> bool {
    select_list.iter().any(|item| {
        if let SelectItem::Expression { expr, .. } = item {
            is_window_function(expr)
        } else {
            false
        }
    })
}

/// Check if an expression is any window function
fn is_window_function(expr: &Expression) -> bool {
    matches!(expr, Expression::WindowFunction { .. })
}

/// Collect all window functions from the SELECT list
fn collect_window_functions(select_list: &[SelectItem]) -> Vec<PostAggregateWindowFunction> {
    let mut result = Vec::new();

    for (idx, item) in select_list.iter().enumerate() {
        if let SelectItem::Expression {
            expr: Expression::WindowFunction { function, over },
            ..
        } = item
        {
            let (func_name, func_type, args) = match function {
                WindowFunctionSpec::Aggregate { name, args, .. } => {
                    (name.to_string(), WindowFunctionType::Aggregate, args.clone())
                }
                WindowFunctionSpec::Ranking { name, args } => {
                    (name.to_string(), WindowFunctionType::Ranking, args.clone())
                }
                WindowFunctionSpec::Value { name, args } => {
                    (name.to_string(), WindowFunctionType::Value, args.clone())
                }
            };

            result.push(PostAggregateWindowFunction {
                select_index: idx,
                func_name,
                func_type,
                args,
                window_spec: over.clone(),
            });
        }
    }

    result
}

/// Apply window functions to aggregated rows
///
/// This is called after GROUP BY processing. At this point, the result rows contain
/// the inner values (e.g., for LEAD(x), each row has the x value).
/// This function applies the window function over these values.
pub(super) fn apply_window_functions_to_aggregates(
    mut rows: Vec<Row>,
    select_list: &[SelectItem],
    database: &vibesql_storage::Database,
) -> Result<Vec<Row>, ExecutorError> {
    let window_funcs = collect_window_functions(select_list);

    if window_funcs.is_empty() {
        return Ok(rows);
    }

    // Build a schema for the aggregate result rows
    // Each column corresponds to a SELECT list item
    let result_schema = build_aggregate_result_schema(select_list);
    let evaluator = CombinedExpressionEvaluator::with_database(&result_schema, database);

    // Process each window function
    for win_func in &window_funcs {
        // Validate frame specification (checks for non-negative offsets, etc.)
        validate_frame(&win_func.window_spec.frame).map_err(ExecutorError::SqliteCompatError)?;

        // For partition/order expressions, we need to map them to column indices
        // in the aggregate result schema. Create column reference expressions.
        let partition_exprs: Option<Vec<Expression>> =
            win_func.window_spec.partition_by.as_ref().map(|exprs| {
                exprs.iter().map(|e| map_expr_to_result_column(e, select_list)).collect::<Vec<_>>()
            });

        // Partition the rows
        let eval_fn = |expr: &Expression, row: &Row| -> Result<SqlValue, String> {
            evaluator.clear_cse_cache();
            evaluator.eval(expr, row).map_err(|e| format!("{:?}", e))
        };

        let mut partitions = partition_rows(rows.clone(), &partition_exprs, eval_fn);

        // Sort each partition
        let order_by_items: Option<Vec<vibesql_ast::OrderByItem>> =
            win_func.window_spec.order_by.as_ref().map(|items| {
                items
                    .iter()
                    .map(|item| vibesql_ast::OrderByItem {
                        expr: map_expr_to_result_column(&item.expr, select_list),
                        direction: item.direction.clone(),
                        nulls_order: item.nulls_order,
                    })
                    .collect::<Vec<_>>()
            });

        let order_by_ref = order_by_items.clone();

        for partition in &mut partitions {
            sort_partition(partition, &order_by_ref);
        }

        // Compute window function values for each partition
        let mut results_with_indices: Vec<(usize, SqlValue)> = Vec::new();

        for partition in &partitions {
            // The argument column is at select_index
            let arg_col_idx = win_func.select_index;

            // Create an expression that references this column
            let arg_expr = Expression::ColumnRef(vibesql_ast::ColumnIdentifier::qualified(
                "result",
                false,
                &format!("col{}", arg_col_idx),
                false,
            ));

            // Map arguments to result columns for value functions
            let mapped_args: Vec<Expression> = win_func
                .args
                .iter()
                .map(|arg| map_expr_to_result_column(arg, select_list))
                .collect();

            // Evaluate the window function for each row in the partition
            match &win_func.func_type {
                WindowFunctionType::Aggregate => {
                    for row_idx in 0..partition.len() {
                        let frame_result = calculate_frame_with_exclusion(
                            partition,
                            row_idx,
                            &order_by_ref,
                            &win_func.window_spec.frame,
                        );
                        let frame_indices = frame_result.included_indices(partition, &order_by_ref);

                        let eval_fn = |expr: &Expression, row: &Row| -> Result<SqlValue, String> {
                            evaluator.clear_cse_cache();
                            evaluator.eval(expr, row).map_err(|e| format!("{:?}", e))
                        };

                        let value = match win_func.func_name.to_uppercase().as_str() {
                            "COUNT" => evaluate_count_window(
                                partition,
                                frame_indices,
                                Some(&arg_expr),
                                None,
                                eval_fn,
                            ),
                            "SUM" => {
                                evaluate_sum_window(partition, frame_indices, &arg_expr, None, eval_fn)
                            }
                            "AVG" => {
                                evaluate_avg_window(partition, frame_indices, &arg_expr, None, eval_fn)
                            }
                            "MIN" => {
                                evaluate_min_window(partition, frame_indices, &arg_expr, None, eval_fn)
                            }
                            "MAX" => {
                                evaluate_max_window(partition, frame_indices, &arg_expr, None, eval_fn)
                            }
                            other => {
                                return Err(ExecutorError::UnsupportedExpression(format!(
                                    "Unsupported aggregate window function: {}",
                                    other
                                )))
                            }
                        };

                        results_with_indices.push((partition.original_indices[row_idx], value));
                    }
                }
                WindowFunctionType::Ranking => {
                    let values = match win_func.func_name.to_uppercase().as_str() {
                        "ROW_NUMBER" => evaluate_row_number(partition),
                        "RANK" => evaluate_rank(partition, &order_by_ref),
                        "DENSE_RANK" => evaluate_dense_rank(partition, &order_by_ref),
                        "PERCENT_RANK" => evaluate_percent_rank(partition, &order_by_ref),
                        "CUME_DIST" => evaluate_cume_dist(partition, &order_by_ref),
                        "NTILE" => {
                            // NTILE requires a constant argument
                            if partition.is_empty() {
                                vec![]
                            } else {
                                let n_val = if !mapped_args.is_empty() {
                                    let eval_fn =
                                        |expr: &Expression, row: &Row| -> Result<SqlValue, String> {
                                            evaluator.clear_cse_cache();
                                            evaluator.eval(expr, row).map_err(|e| format!("{:?}", e))
                                        };
                                    eval_fn(&mapped_args[0], &partition.rows[0])
                                        .ok()
                                        .and_then(|v| match v {
                                            SqlValue::Integer(n) => Some(n),
                                            _ => None,
                                        })
                                        .unwrap_or(1)
                                } else {
                                    1
                                };
                                evaluate_ntile(partition, n_val)
                                    .map_err(ExecutorError::UnsupportedExpression)?
                            }
                        }
                        other => {
                            return Err(ExecutorError::UnsupportedExpression(format!(
                                "Unsupported ranking window function: {}",
                                other
                            )))
                        }
                    };

                    for (row_idx, value) in values.into_iter().enumerate() {
                        results_with_indices.push((partition.original_indices[row_idx], value));
                    }
                }
                WindowFunctionType::Value => {
                    let eval_fn = |expr: &Expression, row: &Row| -> Result<SqlValue, String> {
                        evaluator.clear_cse_cache();
                        evaluator.eval(expr, row).map_err(|e| format!("{:?}", e))
                    };

                    match win_func.func_name.to_uppercase().as_str() {
                        "LEAD" => {
                            let value_expr =
                                if !mapped_args.is_empty() { &mapped_args[0] } else { &arg_expr };
                            let offset = if mapped_args.len() > 1 && !partition.is_empty() {
                                eval_fn(&mapped_args[1], &partition.rows[0])
                                    .ok()
                                    .and_then(|v| match v {
                                        SqlValue::Integer(n) => Some(n),
                                        _ => None,
                                    })
                            } else {
                                None
                            };
                            let default_expr =
                                if mapped_args.len() > 2 { Some(&mapped_args[2]) } else { None };

                            for row_idx in 0..partition.len() {
                                let value = evaluate_lead(
                                    partition,
                                    row_idx,
                                    value_expr,
                                    offset,
                                    default_expr,
                                    eval_fn,
                                )
                                .map_err(ExecutorError::UnsupportedExpression)?;
                                results_with_indices
                                    .push((partition.original_indices[row_idx], value));
                            }
                        }
                        "LAG" => {
                            let value_expr =
                                if !mapped_args.is_empty() { &mapped_args[0] } else { &arg_expr };
                            let offset = if mapped_args.len() > 1 && !partition.is_empty() {
                                eval_fn(&mapped_args[1], &partition.rows[0])
                                    .ok()
                                    .and_then(|v| match v {
                                        SqlValue::Integer(n) => Some(n),
                                        _ => None,
                                    })
                            } else {
                                None
                            };
                            let default_expr =
                                if mapped_args.len() > 2 { Some(&mapped_args[2]) } else { None };

                            for row_idx in 0..partition.len() {
                                let value = evaluate_lag(
                                    partition,
                                    row_idx,
                                    value_expr,
                                    offset,
                                    default_expr,
                                    eval_fn,
                                )
                                .map_err(ExecutorError::UnsupportedExpression)?;
                                results_with_indices
                                    .push((partition.original_indices[row_idx], value));
                            }
                        }
                        "FIRST_VALUE" => {
                            let value_expr =
                                if !mapped_args.is_empty() { &mapped_args[0] } else { &arg_expr };
                            let value = evaluate_first_value(partition, value_expr, eval_fn)
                                .map_err(ExecutorError::UnsupportedExpression)?;
                            for row_idx in 0..partition.len() {
                                results_with_indices
                                    .push((partition.original_indices[row_idx], value.clone()));
                            }
                        }
                        "LAST_VALUE" => {
                            let value_expr =
                                if !mapped_args.is_empty() { &mapped_args[0] } else { &arg_expr };
                            let value = evaluate_last_value(partition, value_expr, eval_fn)
                                .map_err(ExecutorError::UnsupportedExpression)?;
                            for row_idx in 0..partition.len() {
                                results_with_indices
                                    .push((partition.original_indices[row_idx], value.clone()));
                            }
                        }
                        "NTH_VALUE" => {
                            let value_expr =
                                if !mapped_args.is_empty() { &mapped_args[0] } else { &arg_expr };
                            let n = if mapped_args.len() > 1 && !partition.is_empty() {
                                eval_fn(&mapped_args[1], &partition.rows[0])
                                    .ok()
                                    .and_then(|v| match v {
                                        SqlValue::Integer(n) => Some(n),
                                        _ => None,
                                    })
                                    .unwrap_or(1)
                            } else {
                                1
                            };
                            let value = evaluate_nth_value(partition, n, value_expr, eval_fn)
                                .map_err(ExecutorError::UnsupportedExpression)?;
                            for row_idx in 0..partition.len() {
                                results_with_indices
                                    .push((partition.original_indices[row_idx], value.clone()));
                            }
                        }
                        other => {
                            return Err(ExecutorError::UnsupportedExpression(format!(
                                "Unsupported value window function: {}",
                                other
                            )))
                        }
                    }
                }
            }
        }

        // Sort by original index
        results_with_indices.sort_by_key(|(idx, _)| *idx);

        // Update the rows with window function results
        for (row_idx, value) in results_with_indices {
            rows[row_idx].values[win_func.select_index] = value;
        }
    }

    Ok(rows)
}

/// Build a schema for aggregate result rows
///
/// Uses consistent column naming: col0, col1, col2, ... so column references work correctly.
fn build_aggregate_result_schema(select_list: &[SelectItem]) -> CombinedSchema {
    let mut columns = Vec::new();

    for idx in 0..select_list.len() {
        // Use consistent naming pattern: col0, col1, col2, ...
        let column_name = format!("col{}", idx);

        columns.push(ColumnSchema::new(
            column_name,
            DataType::Varchar { max_length: Some(255) }, // Placeholder type
            true,
        ));
    }

    let table_schema = vibesql_catalog::TableSchema::new("result".to_string(), columns);

    let mut table_schemas = std::collections::HashMap::new();
    let table_id = vibesql_catalog::TableIdentifier::unquoted("result");
    table_schemas.insert(table_id, (0, table_schema.clone()));

    CombinedSchema {
        table_schemas,
        total_columns: table_schema.columns.len(),
        hidden_columns: std::collections::HashSet::new(),
        outer_schema: None,
        duplicate_aliases: std::collections::HashSet::new(),
        joined_columns: std::collections::HashSet::new(),
        using_coalesce_indices: std::collections::HashMap::new(),
        column_replacement_map: std::collections::HashMap::new(),
        alias_tables: std::collections::HashSet::new(),
        shadowed_tables: std::collections::HashMap::new(),
    }
}

/// Map an expression to a column reference in the result schema
///
/// For expressions that appear in the SELECT list, we create a ColumnRef
/// that references the computed value. For others, we return the expression as-is.
fn map_expr_to_result_column(expr: &Expression, select_list: &[SelectItem]) -> Expression {
    // Try to find this expression in the SELECT list
    for (idx, item) in select_list.iter().enumerate() {
        if let SelectItem::Expression { expr: select_expr, alias, .. } = item {
            // Check if expressions match
            if expressions_match(expr, select_expr) {
                let col_name = alias.clone().unwrap_or_else(|| format!("col{}", idx));
                return Expression::ColumnRef(vibesql_ast::ColumnIdentifier::qualified(
                    "result", false, &col_name, false,
                ));
            }

            // Also check if expr matches an alias
            if let Some(alias) = alias {
                if let Expression::ColumnRef(col_id) = expr {
                    if col_id.table_canonical().is_none()
                        && col_id.column_canonical().eq_ignore_ascii_case(alias)
                    {
                        return Expression::ColumnRef(vibesql_ast::ColumnIdentifier::qualified(
                            "result", false, alias, false,
                        ));
                    }
                }
            }
        }
    }

    // Expression not in SELECT list - return as-is
    // This might cause evaluation issues if the expression references source columns
    expr.clone()
}

/// Check if two expressions are equivalent
fn expressions_match(expr1: &Expression, expr2: &Expression) -> bool {
    // Simple structural comparison
    format!("{:?}", expr1) == format!("{:?}", expr2)
}
