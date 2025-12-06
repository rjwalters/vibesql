//! Window function evaluation for aggregate queries
//!
//! This module handles window functions that wrap aggregates, like AVG(SUM(x)) OVER (...).
//! After GROUP BY processing computes the inner aggregates, this module applies the
//! outer window function over the aggregated rows.

use vibesql_ast::{Expression, SelectItem, WindowFunctionSpec, WindowSpec};
use vibesql_catalog::ColumnSchema;
use vibesql_storage::Row;
use vibesql_types::{DataType, SqlValue};

use crate::{
    errors::ExecutorError,
    evaluator::{
        window::{
            calculate_frame, evaluate_avg_window, evaluate_count_window, evaluate_max_window,
            evaluate_min_window, evaluate_sum_window, partition_rows, sort_partition,
        },
        CombinedExpressionEvaluator,
    },
    schema::{CombinedSchema, TableKey},
};

/// Information about a window function that needs post-aggregation evaluation
struct AggregateWindowFunction {
    /// Index in the SELECT list / result row
    select_index: usize,
    /// The outer window function name (e.g., "AVG" in AVG(SUM(x)))
    outer_func_name: String,
    /// The window specification (PARTITION BY, ORDER BY, frame)
    window_spec: WindowSpec,
}

/// Check if the SELECT list contains window functions that wrap aggregates
pub(super) fn has_aggregate_window_functions(select_list: &[SelectItem]) -> bool {
    select_list.iter().any(|item| {
        if let SelectItem::Expression { expr, .. } = item {
            is_aggregate_window_function(expr)
        } else {
            false
        }
    })
}

/// Check if an expression is a window function wrapping an aggregate
fn is_aggregate_window_function(expr: &Expression) -> bool {
    matches!(
        expr,
        Expression::WindowFunction {
            function: WindowFunctionSpec::Aggregate { .. },
            ..
        }
    )
}

/// Collect aggregate window functions from the SELECT list
fn collect_aggregate_window_functions(select_list: &[SelectItem]) -> Vec<AggregateWindowFunction> {
    let mut result = Vec::new();

    for (idx, item) in select_list.iter().enumerate() {
        if let SelectItem::Expression {
            expr: Expression::WindowFunction {
                function: WindowFunctionSpec::Aggregate { name, .. },
                over,
            },
            ..
        } = item
        {
            result.push(AggregateWindowFunction {
                select_index: idx,
                outer_func_name: name.clone(),
                window_spec: over.clone(),
            });
        }
    }

    result
}

/// Apply window functions to aggregated rows
///
/// This is called after GROUP BY processing. At this point, the result rows contain
/// the inner aggregate values (e.g., for AVG(SUM(x)), each row has the SUM(x) value).
/// This function applies the outer window function (AVG) over these values.
pub(super) fn apply_window_functions_to_aggregates(
    mut rows: Vec<Row>,
    select_list: &[SelectItem],
    database: &vibesql_storage::Database,
) -> Result<Vec<Row>, ExecutorError> {
    let window_funcs = collect_aggregate_window_functions(select_list);

    if window_funcs.is_empty() {
        return Ok(rows);
    }

    // Build a schema for the aggregate result rows
    // Each column corresponds to a SELECT list item
    let result_schema = build_aggregate_result_schema(select_list);
    let evaluator = CombinedExpressionEvaluator::with_database(&result_schema, database);

    // Process each window function
    for win_func in &window_funcs {
        // The window function's argument (e.g., SUM(x)) has already been evaluated
        // and is stored at the same column index. We need to read this value from
        // each row and apply the outer window function.

        // For partition/order expressions, we need to map them to column indices
        // in the aggregate result schema. Create column reference expressions.
        let partition_exprs: Option<Vec<Expression>> = win_func.window_spec.partition_by.as_ref().map(
            |exprs| {
                exprs.iter().map(|e| map_expr_to_result_column(e, select_list)).collect::<Vec<_>>()
            },
        );

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
            let arg_expr = Expression::ColumnRef {
                table: Some("result".to_string()),
                column: format!("col{}", arg_col_idx),
            };

            // Evaluate the window function for each row in the partition
            for row_idx in 0..partition.len() {
                let frame = calculate_frame(
                    partition,
                    row_idx,
                    &order_by_ref,
                    &win_func.window_spec.frame,
                );

                let eval_fn = |expr: &Expression, row: &Row| -> Result<SqlValue, String> {
                    evaluator.clear_cse_cache();
                    evaluator.eval(expr, row).map_err(|e| format!("{:?}", e))
                };

                let value = match win_func.outer_func_name.to_uppercase().as_str() {
                    "COUNT" => evaluate_count_window(partition, &frame, Some(&arg_expr), eval_fn),
                    "SUM" => evaluate_sum_window(partition, &frame, &arg_expr, eval_fn),
                    "AVG" => evaluate_avg_window(partition, &frame, &arg_expr, eval_fn),
                    "MIN" => evaluate_min_window(partition, &frame, &arg_expr, eval_fn),
                    "MAX" => evaluate_max_window(partition, &frame, &arg_expr, eval_fn),
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
    table_schemas.insert(TableKey::new("result"), (0, table_schema.clone()));

    CombinedSchema { table_schemas, total_columns: table_schema.columns.len() }
}

/// Map an expression to a column reference in the result schema
///
/// For expressions that appear in the SELECT list, we create a ColumnRef
/// that references the computed value. For others, we return the expression as-is.
fn map_expr_to_result_column(expr: &Expression, select_list: &[SelectItem]) -> Expression {
    // Try to find this expression in the SELECT list
    for (idx, item) in select_list.iter().enumerate() {
        if let SelectItem::Expression { expr: select_expr, alias } = item {
            // Check if expressions match
            if expressions_match(expr, select_expr) {
                let col_name = alias.clone().unwrap_or_else(|| format!("col{}", idx));
                return Expression::ColumnRef { table: Some("result".to_string()), column: col_name };
            }

            // Also check if expr matches an alias
            if let Some(alias) = alias {
                if let Expression::ColumnRef { column, table: None } = expr {
                    if column.eq_ignore_ascii_case(alias) {
                        return Expression::ColumnRef {
                            table: Some("result".to_string()),
                            column: alias.clone(),
                        };
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
