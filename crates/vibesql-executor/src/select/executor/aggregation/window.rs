//! Window function evaluation for aggregate queries
//!
//! This module handles window functions in GROUP BY queries, including:
//! - Aggregate window functions like AVG(SUM(x)) OVER (...)
//! - Value window functions like LEAD(x) OVER (...)
//! - Ranking window functions like ROW_NUMBER() OVER (...)
//!
//! After GROUP BY processing computes the inner values, this module applies the
//! window function over the aggregated rows.

use std::collections::HashMap;

use vibesql_ast::{Expression, SelectItem, WindowDefinition, WindowFunctionSpec, WindowSpec};
use vibesql_catalog::ColumnSchema;
use vibesql_storage::Row;
use vibesql_types::{DataType, SqlValue};

use crate::{
    errors::ExecutorError,
    evaluator::{
        window::{
            calculate_frame_with_exclusion, evaluate_avg_window, evaluate_count_window,
            evaluate_cume_dist, evaluate_dense_rank, evaluate_group_concat_window_with_expr,
            evaluate_lag, evaluate_lead, evaluate_max_window, evaluate_min_window,
            evaluate_ntile, evaluate_percent_rank, evaluate_rank, evaluate_row_number,
            evaluate_sum_window, evaluate_total_window, partition_rows, sort_partition,
            validate_frame, validate_range_order_by,
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

/// Build a map of window names to their resolved definitions
/// This handles window inheritance (e.g., WINDOW win AS (...), win2 AS (win ORDER BY x))
fn build_window_map(
    window_definitions: Option<&Vec<WindowDefinition>>,
) -> Result<HashMap<String, WindowSpec>, ExecutorError> {
    let mut resolved_map: HashMap<String, WindowSpec> = HashMap::new();

    if let Some(defs) = window_definitions {
        let raw_map: HashMap<String, &WindowSpec> = defs
            .iter()
            .map(|def| (def.name.to_lowercase(), &def.spec))
            .collect();

        for def in defs {
            let resolved =
                resolve_window_spec_recursive(&def.spec, &raw_map, &resolved_map)?;
            resolved_map.insert(def.name.to_lowercase(), resolved);
        }
    }

    Ok(resolved_map)
}

/// Recursively resolve a window spec, handling inheritance chains
fn resolve_window_spec_recursive(
    spec: &WindowSpec,
    raw_map: &HashMap<String, &WindowSpec>,
    resolved_map: &HashMap<String, WindowSpec>,
) -> Result<WindowSpec, ExecutorError> {
    match &spec.base_window_name {
        Some(base_name) => {
            let base_name_lower = base_name.to_lowercase();
            let base_spec = if let Some(resolved) = resolved_map.get(&base_name_lower) {
                resolved.clone()
            } else if let Some(raw) = raw_map.get(&base_name_lower) {
                resolve_window_spec_recursive(raw, raw_map, resolved_map)?
            } else {
                return Err(ExecutorError::Other(format!("no such window: {}", base_name)));
            };

            Ok(WindowSpec {
                base_window_name: None,
                partition_by: spec.partition_by.clone().or(base_spec.partition_by),
                order_by: spec.order_by.clone().or(base_spec.order_by),
                frame: spec.frame.clone().or(base_spec.frame),
            })
        }
        None => Ok(spec.clone()),
    }
}

/// Resolve a window spec by looking up a named window from the resolved map
fn resolve_window_spec(
    spec: &WindowSpec,
    window_map: &HashMap<String, WindowSpec>,
) -> Result<WindowSpec, ExecutorError> {
    match &spec.base_window_name {
        Some(base_name) => {
            let base_spec = window_map.get(&base_name.to_lowercase()).ok_or_else(|| {
                ExecutorError::Other(format!("no such window: {}", base_name))
            })?;

            Ok(WindowSpec {
                base_window_name: None,
                partition_by: spec.partition_by.clone().or_else(|| base_spec.partition_by.clone()),
                order_by: spec.order_by.clone().or_else(|| base_spec.order_by.clone()),
                frame: spec.frame.clone().or_else(|| base_spec.frame.clone()),
            })
        }
        None => Ok(spec.clone()),
    }
}

/// Collect all window functions from the SELECT list, resolving named window references
fn collect_window_functions(
    select_list: &[SelectItem],
    window_definitions: Option<&Vec<WindowDefinition>>,
) -> Result<Vec<PostAggregateWindowFunction>, ExecutorError> {
    let window_map = build_window_map(window_definitions)?;
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

            // Resolve named window references
            let resolved_spec = resolve_window_spec(over, &window_map)?;

            result.push(PostAggregateWindowFunction {
                select_index: idx,
                func_name,
                func_type,
                args,
                window_spec: resolved_spec,
            });
        }
    }

    Ok(result)
}

/// Apply window functions to aggregated rows
///
/// This is called after GROUP BY processing. At this point, the result rows contain
/// the inner values (e.g., for LEAD(x), each row has the x value).
/// This function applies the window function over these values.
///
/// `group_by_exprs` contains the GROUP BY expressions. Their evaluated values are
/// appended as hidden columns after the SELECT items in each row. This allows
/// window ORDER BY / PARTITION BY clauses to reference GROUP BY columns.
pub(super) fn apply_window_functions_to_aggregates(
    mut rows: Vec<Row>,
    select_list: &[SelectItem],
    database: &vibesql_storage::Database,
    window_definitions: Option<&Vec<WindowDefinition>>,
    group_by_exprs: &[Expression],
) -> Result<Vec<Row>, ExecutorError> {
    let window_funcs = collect_window_functions(select_list, window_definitions)?;

    if window_funcs.is_empty() {
        return Ok(rows);
    }

    // Build a schema for the aggregate result rows
    // Each column corresponds to a SELECT list item, plus hidden GROUP BY columns
    let result_schema = build_aggregate_result_schema(select_list, group_by_exprs);
    let evaluator = CombinedExpressionEvaluator::with_database(&result_schema, database);

    // Process each window function
    for win_func in &window_funcs {
        // Validate frame specification (checks for non-negative offsets, etc.)
        validate_frame(&win_func.window_spec.frame).map_err(ExecutorError::SqliteCompatError)?;
        // Validate RANGE with offset requires exactly one ORDER BY expression
        validate_range_order_by(&win_func.window_spec.frame, &win_func.window_spec.order_by)
            .map_err(ExecutorError::SqliteCompatError)?;

        // For partition/order expressions, we need to map them to column indices
        // in the aggregate result schema. Create column reference expressions.
        let partition_exprs: Option<Vec<Expression>> =
            win_func.window_spec.partition_by.as_ref().map(|exprs| {
                exprs.iter().map(|e| map_expr_to_result_column(e, select_list, group_by_exprs)).collect::<Vec<_>>()
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
                        expr: map_expr_to_result_column(&item.expr, select_list, group_by_exprs),
                        direction: item.direction.clone(),
                        nulls_order: item.nulls_order,
                    })
                    .collect::<Vec<_>>()
            });

        let order_by_ref = order_by_items.clone();

        // Create the eval_fn closure for sorting, ranking, and frame calculations
        let agg_eval_fn = |expr: &Expression, row: &Row| -> Result<SqlValue, String> {
            evaluator.clear_cse_cache();
            evaluator.eval(expr, row).map_err(|e| format!("{:?}", e))
        };

        for partition in &mut partitions {
            let sort_eval_fn = |expr: &Expression, row: &Row| -> Result<SqlValue, String> {
                evaluator.clear_cse_cache();
                evaluator.eval(expr, row).map_err(|e| format!("{:?}", e))
            };
            sort_partition(partition, &order_by_ref, sort_eval_fn);
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
                .map(|arg| map_expr_to_result_column(arg, select_list, group_by_exprs))
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
                            &agg_eval_fn,
                        );
                        let frame_indices =
                            frame_result.included_indices(partition, &order_by_ref, &agg_eval_fn);

                        let inner_eval_fn =
                            |expr: &Expression, row: &Row| -> Result<SqlValue, String> {
                                evaluator.clear_cse_cache();
                                evaluator.eval(expr, row).map_err(|e| format!("{:?}", e))
                            };

                        let value = match win_func.func_name.to_uppercase().as_str() {
                            "COUNT" => {
                                // COUNT(*) or COUNT() should count all rows in the
                                // frame (passing `None`). Only when there is a real
                                // argument do we delegate to the synthetic
                                // `arg_expr` referencing the post-aggregation result
                                // column. Mirrors the non-aggregate path in
                                // crates/vibesql-executor/src/select/window/evaluation.rs.
                                let count_arg = if win_func.args.is_empty()
                                    || matches!(&win_func.args[0], Expression::Wildcard)
                                    || matches!(
                                        &win_func.args[0],
                                        Expression::ColumnRef(col_id)
                                            if col_id.column_canonical() == "*"
                                    )
                                {
                                    None // COUNT(*) / COUNT() - count all rows
                                } else {
                                    Some(&arg_expr)
                                };
                                evaluate_count_window(
                                    partition,
                                    frame_indices,
                                    count_arg,
                                    None,
                                    inner_eval_fn,
                                )
                            }
                            "SUM" => evaluate_sum_window(
                                partition,
                                frame_indices,
                                &arg_expr,
                                None,
                                inner_eval_fn,
                            ),
                            "AVG" => evaluate_avg_window(
                                partition,
                                frame_indices,
                                &arg_expr,
                                None,
                                inner_eval_fn,
                            ),
                            "MIN" => evaluate_min_window(
                                partition,
                                frame_indices,
                                &arg_expr,
                                None,
                                inner_eval_fn,
                            ),
                            "MAX" => evaluate_max_window(
                                partition,
                                frame_indices,
                                &arg_expr,
                                None,
                                inner_eval_fn,
                            ),
                            "TOTAL" => evaluate_total_window(
                                partition,
                                frame_indices,
                                &arg_expr,
                                None,
                                inner_eval_fn,
                            ),
                            "GROUP_CONCAT" | "STRING_AGG" => {
                                // Use separator expression if present (second arg)
                                let separator_expr = if mapped_args.len() > 1 {
                                    Some(&mapped_args[1])
                                } else {
                                    None
                                };
                                evaluate_group_concat_window_with_expr(
                                    partition,
                                    frame_indices,
                                    &arg_expr,
                                    separator_expr,
                                    ",",
                                    None,
                                    inner_eval_fn,
                                )
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
                        "RANK" => evaluate_rank(partition, &order_by_ref, &agg_eval_fn),
                        "DENSE_RANK" => evaluate_dense_rank(partition, &order_by_ref, &agg_eval_fn),
                        "PERCENT_RANK" => {
                            evaluate_percent_rank(partition, &order_by_ref, &agg_eval_fn)
                        }
                        "CUME_DIST" => evaluate_cume_dist(partition, &order_by_ref, &agg_eval_fn),
                        "NTILE" => {
                            // NTILE requires a constant argument
                            if partition.is_empty() {
                                vec![]
                            } else {
                                let n_val = if !mapped_args.is_empty() {
                                    agg_eval_fn(&mapped_args[0], &partition.rows[0])
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
                            for row_idx in 0..partition.len() {
                                let frame_result = calculate_frame_with_exclusion(
                                    partition,
                                    row_idx,
                                    &order_by_ref,
                                    &win_func.window_spec.frame,
                                    &eval_fn,
                                );
                                let value = if let Some(first_idx) = frame_result
                                    .included_indices(partition, &order_by_ref, &eval_fn)
                                    .next()
                                {
                                    eval_fn(value_expr, &partition.rows[first_idx])
                                        .unwrap_or(SqlValue::Null)
                                } else {
                                    SqlValue::Null
                                };
                                results_with_indices
                                    .push((partition.original_indices[row_idx], value));
                            }
                        }
                        "LAST_VALUE" => {
                            let value_expr =
                                if !mapped_args.is_empty() { &mapped_args[0] } else { &arg_expr };
                            for row_idx in 0..partition.len() {
                                let frame_result = calculate_frame_with_exclusion(
                                    partition,
                                    row_idx,
                                    &order_by_ref,
                                    &win_func.window_spec.frame,
                                    &eval_fn,
                                );
                                let value = frame_result
                                    .included_indices(partition, &order_by_ref, &eval_fn)
                                    .last()
                                    .map(|last_idx| {
                                        eval_fn(value_expr, &partition.rows[last_idx])
                                            .unwrap_or(SqlValue::Null)
                                    })
                                    .unwrap_or(SqlValue::Null);
                                results_with_indices
                                    .push((partition.original_indices[row_idx], value));
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
                            let nth_zero_based = (n - 1).max(0) as usize;
                            for row_idx in 0..partition.len() {
                                let frame_result = calculate_frame_with_exclusion(
                                    partition,
                                    row_idx,
                                    &order_by_ref,
                                    &win_func.window_spec.frame,
                                    &eval_fn,
                                );
                                let value = frame_result
                                    .included_indices(partition, &order_by_ref, &eval_fn)
                                    .nth(nth_zero_based)
                                    .map(|nth_idx| {
                                        eval_fn(value_expr, &partition.rows[nth_idx])
                                            .unwrap_or(SqlValue::Null)
                                    })
                                    .unwrap_or(SqlValue::Null);
                                results_with_indices
                                    .push((partition.original_indices[row_idx], value));
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
/// Includes hidden columns for GROUP BY expressions (appended after SELECT items)
/// so that window ORDER BY / PARTITION BY can reference GROUP BY columns.
fn build_aggregate_result_schema(
    select_list: &[SelectItem],
    group_by_exprs: &[Expression],
) -> CombinedSchema {
    let mut columns = Vec::new();

    // SELECT list columns
    for idx in 0..select_list.len() {
        let column_name = format!("col{}", idx);
        columns.push(ColumnSchema::new(
            column_name,
            DataType::Varchar { max_length: Some(255) },
            true,
        ));
    }

    // Hidden GROUP BY columns (these exist in the rows at positions select_list.len() + i)
    // Use consistent col{idx} naming so make_result_col_ref() references work.
    for i in 0..group_by_exprs.len() {
        let column_name = format!("col{}", select_list.len() + i);
        columns.push(ColumnSchema::new(
            column_name,
            DataType::Varchar { max_length: Some(255) },
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
/// For expressions that appear in the SELECT list or GROUP BY, we create a ColumnRef
/// that references the computed value. For others, we return the expression as-is.
///
/// GROUP BY expressions are stored as hidden columns at positions
/// `select_list.len() + i` in the aggregate result rows.
fn map_expr_to_result_column(
    expr: &Expression,
    select_list: &[SelectItem],
    group_by_exprs: &[Expression],
) -> Expression {
    // Helper to create a column ref for the synthetic result schema.
    // The schema uses col0, col1, col2, ... naming, so we always use that format.
    let make_result_col_ref = |idx: usize| -> Expression {
        Expression::ColumnRef(vibesql_ast::ColumnIdentifier::qualified(
            "result",
            false,
            &format!("col{}", idx),
            false,
        ))
    };

    // Try to find this expression in the SELECT list
    for (idx, item) in select_list.iter().enumerate() {
        if let SelectItem::Expression { expr: select_expr, alias, .. } = item {
            // Check if expressions match structurally
            if expressions_match(expr, select_expr) {
                return make_result_col_ref(idx);
            }

            // Also check if expr matches an alias
            if let Some(alias) = alias {
                if let Expression::ColumnRef(col_id) = expr {
                    if col_id.table_canonical().is_none()
                        && col_id.column_canonical().eq_ignore_ascii_case(alias)
                    {
                        return make_result_col_ref(idx);
                    }
                }
            }

            // Check if expr is a column reference that matches a column reference in the
            // SELECT list by column name (ignoring table qualifier differences).
            // This handles named window PARTITION BY expressions where the column reference
            // from the window definition may not have the same table qualifier as the
            // SELECT list expression.
            if let Expression::ColumnRef(col_id) = expr {
                if let Expression::ColumnRef(select_col_id) = select_expr {
                    if col_id.column_canonical().eq_ignore_ascii_case(
                        select_col_id.column_canonical(),
                    ) {
                        return make_result_col_ref(idx);
                    }
                }
                // Also check if the column ref matches the expression's column name
                // when the select expr is an aggregate or window function
                // E.g., partition by column `x` matching `SELECT x, sum(x) OVER win`
                if let Expression::WindowFunction { .. } | Expression::AggregateFunction { .. } =
                    select_expr
                {
                    // Skip - window/aggregate functions won't match a simple column ref
                } else {
                    // For non-window, non-aggregate expressions, try extracting a column name
                    if let Some(select_col_name) = extract_column_name(select_expr) {
                        if col_id.column_canonical().eq_ignore_ascii_case(&select_col_name) {
                            return make_result_col_ref(idx);
                        }
                    }
                }
            }
        }
    }

    // If the expression is a column reference that wasn't found in the SELECT list,
    // try to find it by scanning SELECT list column names in the synthetic result schema.
    // This is a fallback for cases where PARTITION BY references a column that appears
    // in the SELECT list under a different form.
    if let Expression::ColumnRef(col_id) = expr {
        let col_name = col_id.column_canonical();
        for (idx, item) in select_list.iter().enumerate() {
            if let SelectItem::Expression { expr: select_expr, alias, .. } = item {
                // Match against alias
                if let Some(alias) = alias {
                    if col_name.eq_ignore_ascii_case(alias) {
                        return make_result_col_ref(idx);
                    }
                }
                // Match against column name extracted from expression
                if let Some(select_col_name) = extract_column_name(select_expr) {
                    if col_name.eq_ignore_ascii_case(&select_col_name) {
                        return make_result_col_ref(idx);
                    }
                }
            }
        }
    }

    // Check GROUP BY expressions (stored as hidden columns after SELECT items)
    let select_count = select_list.len();
    for (i, gb_expr) in group_by_exprs.iter().enumerate() {
        if expressions_match(expr, gb_expr) {
            return make_result_col_ref(select_count + i);
        }
        // Also check column name match for GROUP BY column references
        if let Expression::ColumnRef(col_id) = expr {
            if let Expression::ColumnRef(gb_col_id) = gb_expr {
                if col_id
                    .column_canonical()
                    .eq_ignore_ascii_case(gb_col_id.column_canonical())
                {
                    return make_result_col_ref(select_count + i);
                }
            }
        }
    }

    // Also check if the expression matches a window function's first argument in the SELECT list.
    // The window function slot holds the pre-computed inner value (the first arg evaluated in
    // the GROUP BY context), so if ORDER BY references the same expression, we can use that slot.
    for (idx, item) in select_list.iter().enumerate() {
        if let SelectItem::Expression {
            expr: Expression::WindowFunction { function, .. },
            ..
        } = item
        {
            let win_args = match function {
                WindowFunctionSpec::Aggregate { args, .. } => args,
                WindowFunctionSpec::Ranking { args, .. } => args,
                WindowFunctionSpec::Value { args, .. } => args,
            };
            if !win_args.is_empty() && expressions_match(expr, &win_args[0]) {
                return make_result_col_ref(idx);
            }
        }
    }

    // Expression not in SELECT list or GROUP BY - return as-is
    // This might cause evaluation issues if the expression references source columns
    expr.clone()
}

/// Extract the column name from an expression if it's a simple column reference
fn extract_column_name(expr: &Expression) -> Option<String> {
    match expr {
        Expression::ColumnRef(col_id) => Some(col_id.column_canonical().to_string()),
        _ => None,
    }
}

/// Check if two expressions are semantically equivalent
fn expressions_match(expr1: &Expression, expr2: &Expression) -> bool {
    match (expr1, expr2) {
        // Column references: compare by canonical column name (ignoring table qualifier)
        (Expression::ColumnRef(a), Expression::ColumnRef(b)) => {
            // If both have table qualifiers, compare fully
            if a.table_canonical().is_some() && b.table_canonical().is_some() {
                a.canonical().eq_ignore_ascii_case(b.canonical())
            } else {
                // If either lacks a table qualifier, just compare column names
                a.column_canonical()
                    .eq_ignore_ascii_case(b.column_canonical())
            }
        }
        // Literals: compare by value
        (Expression::Literal(a), Expression::Literal(b)) => a == b,
        // Binary operations: compare recursively
        (
            Expression::BinaryOp { left: l1, op: op1, right: r1 },
            Expression::BinaryOp { left: l2, op: op2, right: r2 },
        ) => op1 == op2 && expressions_match(l1, l2) && expressions_match(r1, r2),
        // Unary operations: compare recursively
        (
            Expression::UnaryOp { op: op1, expr: e1 },
            Expression::UnaryOp { op: op2, expr: e2 },
        ) => op1 == op2 && expressions_match(e1, e2),
        // Functions: compare by name and arguments
        (
            Expression::Function { name: n1, args: a1, .. },
            Expression::Function { name: n2, args: a2, .. },
        ) => {
            n1.canonical() == n2.canonical()
                && a1.len() == a2.len()
                && a1.iter().zip(a2.iter()).all(|(x, y)| expressions_match(x, y))
        }
        // Aggregate functions: compare by name and arguments
        (
            Expression::AggregateFunction {
                name: n1, args: a1, distinct: d1, ..
            },
            Expression::AggregateFunction {
                name: n2, args: a2, distinct: d2, ..
            },
        ) => {
            n1.canonical() == n2.canonical()
                && d1 == d2
                && a1.len() == a2.len()
                && a1.iter().zip(a2.iter()).all(|(x, y)| expressions_match(x, y))
        }
        // Cast: compare type and inner expression
        (
            Expression::Cast { expr: e1, data_type: dt1 },
            Expression::Cast { expr: e2, data_type: dt2 },
        ) => format!("{:?}", dt1) == format!("{:?}", dt2) && expressions_match(e1, e2),
        // For everything else, fall back to Debug comparison
        _ => format!("{:?}", expr1) == format!("{:?}", expr2),
    }
}
