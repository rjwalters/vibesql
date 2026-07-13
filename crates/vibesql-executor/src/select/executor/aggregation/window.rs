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
            evaluate_lag, evaluate_lead, evaluate_max_window, evaluate_min_window, evaluate_ntile,
            evaluate_percent_rank, evaluate_percentile_window, evaluate_rank, evaluate_row_number,
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

// ------------------------------------------------------------------------
// Embedded window functions in aggregate SELECT items (#5232)
// ------------------------------------------------------------------------
//
// A window function can appear *inside* a larger expression in an aggregate
// SELECT, e.g. `SELECT lead(2) OVER() + sum(c) FROM a`. During the GROUP BY
// pass, `evaluate_with_aggregates` evaluates the WindowFunction node to a
// placeholder (its first argument), expecting the post-aggregation window
// pass to fix it up — but that pass historically handled only *top-level*
// window SELECT items. The machinery below decomposes such expressions:
//
// 1. Each maximal window-free subexpression becomes a hidden "component"
//    column, evaluated per group during aggregation (e.g. `sum(c)` → 4).
// 2. Each embedded WindowFunction node becomes a hidden component column
//    holding its placeholder, which the window pass later overwrites with
//    the real window value (same contract as top-level windows).
// 3. The original expression is rewritten into a "residual" expression that
//    references the hidden columns; it is re-evaluated per row after the
//    window pass and stored into the SELECT slot.

/// One hidden component column backing an embedded-window rewrite.
pub(in crate::select::executor) struct EmbeddedWindowComponent {
    /// The original subexpression. Evaluated per group via
    /// `evaluate_with_aggregates` (for window components this naturally
    /// yields the placeholder value — the window's first argument).
    pub expr: Expression,
    /// True when this component is a WindowFunction node whose hidden column
    /// must be overwritten by the post-aggregation window pass.
    pub is_window: bool,
}

/// A SELECT item whose expression embeds window functions.
pub(in crate::select::executor) struct EmbeddedWindowRewrite {
    /// Index of the SELECT item to overwrite with the residual's value.
    pub select_index: usize,
    /// The original expression with components replaced by hidden-column
    /// references into the synthetic post-aggregation result schema.
    pub residual: Expression,
}

/// Decomposition plan for all embedded-window SELECT items.
#[derive(Default)]
pub(in crate::select::executor) struct EmbeddedWindowPlan {
    pub rewrites: Vec<EmbeddedWindowRewrite>,
    pub components: Vec<EmbeddedWindowComponent>,
}

impl EmbeddedWindowPlan {
    pub fn is_empty(&self) -> bool {
        self.rewrites.is_empty()
    }
}

/// Check whether an expression contains a window function anywhere within it,
/// WITHOUT descending into subqueries (which have their own scope).
pub(in crate::select::executor) fn expression_contains_window_function(expr: &Expression) -> bool {
    match expr {
        Expression::WindowFunction { .. } => true,
        Expression::BinaryOp { left, right, .. } => {
            expression_contains_window_function(left) || expression_contains_window_function(right)
        }
        Expression::UnaryOp { expr, .. }
        | Expression::Cast { expr, .. }
        | Expression::IsNull { expr, .. }
        | Expression::IsTruthValue { expr, .. }
        | Expression::Collate { expr, .. } => expression_contains_window_function(expr),
        Expression::IsDistinctFrom { left, right, .. } => {
            expression_contains_window_function(left) || expression_contains_window_function(right)
        }
        Expression::Function { args, .. } => args.iter().any(expression_contains_window_function),
        Expression::Case { operand, when_clauses, else_result } => {
            operand.as_ref().is_some_and(|e| expression_contains_window_function(e))
                || when_clauses.iter().any(|w| {
                    w.conditions.iter().any(expression_contains_window_function)
                        || expression_contains_window_function(&w.result)
                })
                || else_result.as_ref().is_some_and(|e| expression_contains_window_function(e))
        }
        Expression::Between { expr, low, high, .. } => {
            expression_contains_window_function(expr)
                || expression_contains_window_function(low)
                || expression_contains_window_function(high)
        }
        Expression::InList { expr, values, .. } => {
            expression_contains_window_function(expr)
                || values.iter().any(expression_contains_window_function)
        }
        Expression::Like { expr, pattern, .. } | Expression::Glob { expr, pattern, .. } => {
            expression_contains_window_function(expr)
                || expression_contains_window_function(pattern)
        }
        Expression::Conjunction(exprs)
        | Expression::Disjunction(exprs)
        | Expression::RowValueConstructor(exprs) => {
            exprs.iter().any(expression_contains_window_function)
        }
        // IN / quantified comparison: only the LHS shares this scope.
        Expression::In { expr, .. } | Expression::QuantifiedComparison { expr, .. } => {
            expression_contains_window_function(expr)
        }
        // Aggregate args cannot legally contain window functions (SQLite
        // rejects them at parse/resolve time) — don't descend.
        // Subqueries (ScalarSubquery / Exists) have their own scope.
        _ => false,
    }
}

/// Build the decomposition plan for SELECT items that embed window functions
/// inside larger expressions. `hidden_base` is the absolute column index in
/// the post-aggregation row layout where the embedded component columns
/// start (after SELECT items, GROUP BY columns, ORDER BY aggregates, and
/// window aggregates).
pub(in crate::select::executor) fn plan_embedded_window_rewrites(
    select_list: &[SelectItem],
    hidden_base: usize,
) -> EmbeddedWindowPlan {
    let mut plan = EmbeddedWindowPlan::default();

    for (idx, item) in select_list.iter().enumerate() {
        if let SelectItem::Expression { expr, .. } = item {
            // Top-level window functions are handled by the existing
            // select-slot placeholder machinery.
            if is_window_function(expr) {
                continue;
            }
            if !expression_contains_window_function(expr) {
                continue;
            }
            let residual = rewrite_embedded_expression(expr, hidden_base, &mut plan.components);
            plan.rewrites.push(EmbeddedWindowRewrite { select_index: idx, residual });
        }
    }

    plan
}

/// Create a column reference into the synthetic post-aggregation result schema.
fn embedded_col_ref(idx: usize) -> Expression {
    Expression::ColumnRef(vibesql_ast::ColumnIdentifier::qualified(
        "result",
        false,
        &format!("col{}", idx),
        false,
    ))
}

/// Rewrite an expression containing embedded window functions: WindowFunction
/// nodes and maximal window-free subexpressions are replaced with hidden
/// column references, registering each as a component to be computed per
/// group during aggregation.
fn rewrite_embedded_expression(
    expr: &Expression,
    hidden_base: usize,
    components: &mut Vec<EmbeddedWindowComponent>,
) -> Expression {
    // Window function: becomes a hidden placeholder column that the window
    // pass overwrites with the computed window value.
    if is_window_function(expr) {
        let idx = hidden_base + components.len();
        components.push(EmbeddedWindowComponent { expr: expr.clone(), is_window: true });
        return embedded_col_ref(idx);
    }

    // Maximal window-free subexpression: computed per group during
    // aggregation (may contain aggregates, subqueries, literals, ...).
    if !expression_contains_window_function(expr) {
        let idx = hidden_base + components.len();
        components.push(EmbeddedWindowComponent { expr: expr.clone(), is_window: false });
        return embedded_col_ref(idx);
    }

    // Otherwise recurse structurally (the expression contains a window
    // function somewhere below).
    match expr {
        Expression::BinaryOp { left, op, right } => Expression::BinaryOp {
            left: Box::new(rewrite_embedded_expression(left, hidden_base, components)),
            op: op.clone(),
            right: Box::new(rewrite_embedded_expression(right, hidden_base, components)),
        },
        Expression::UnaryOp { op, expr } => Expression::UnaryOp {
            op: op.clone(),
            expr: Box::new(rewrite_embedded_expression(expr, hidden_base, components)),
        },
        Expression::Cast { expr, data_type } => Expression::Cast {
            expr: Box::new(rewrite_embedded_expression(expr, hidden_base, components)),
            data_type: data_type.clone(),
        },
        Expression::Collate { expr, collation } => Expression::Collate {
            expr: Box::new(rewrite_embedded_expression(expr, hidden_base, components)),
            collation: collation.clone(),
        },
        Expression::IsNull { expr, negated } => Expression::IsNull {
            expr: Box::new(rewrite_embedded_expression(expr, hidden_base, components)),
            negated: *negated,
        },
        Expression::IsDistinctFrom { left, right, negated } => Expression::IsDistinctFrom {
            left: Box::new(rewrite_embedded_expression(left, hidden_base, components)),
            right: Box::new(rewrite_embedded_expression(right, hidden_base, components)),
            negated: *negated,
        },
        Expression::IsTruthValue { expr, truth_value, negated } => Expression::IsTruthValue {
            expr: Box::new(rewrite_embedded_expression(expr, hidden_base, components)),
            truth_value: truth_value.clone(),
            negated: *negated,
        },
        Expression::Function { name, args, character_unit } => Expression::Function {
            name: name.clone(),
            args: args
                .iter()
                .map(|a| rewrite_embedded_expression(a, hidden_base, components))
                .collect(),
            character_unit: character_unit.clone(),
        },
        Expression::Case { operand, when_clauses, else_result } => Expression::Case {
            operand: operand
                .as_ref()
                .map(|e| Box::new(rewrite_embedded_expression(e, hidden_base, components))),
            when_clauses: when_clauses
                .iter()
                .map(|w| vibesql_ast::CaseWhen {
                    conditions: w
                        .conditions
                        .iter()
                        .map(|c| rewrite_embedded_expression(c, hidden_base, components))
                        .collect(),
                    result: rewrite_embedded_expression(&w.result, hidden_base, components),
                })
                .collect(),
            else_result: else_result
                .as_ref()
                .map(|e| Box::new(rewrite_embedded_expression(e, hidden_base, components))),
        },
        Expression::Between { expr, low, high, negated, symmetric } => Expression::Between {
            expr: Box::new(rewrite_embedded_expression(expr, hidden_base, components)),
            low: Box::new(rewrite_embedded_expression(low, hidden_base, components)),
            high: Box::new(rewrite_embedded_expression(high, hidden_base, components)),
            negated: *negated,
            symmetric: *symmetric,
        },
        Expression::InList { expr, values, negated } => Expression::InList {
            expr: Box::new(rewrite_embedded_expression(expr, hidden_base, components)),
            values: values
                .iter()
                .map(|v| rewrite_embedded_expression(v, hidden_base, components))
                .collect(),
            negated: *negated,
        },
        Expression::Like { expr, pattern, negated, escape } => Expression::Like {
            expr: Box::new(rewrite_embedded_expression(expr, hidden_base, components)),
            pattern: Box::new(rewrite_embedded_expression(pattern, hidden_base, components)),
            negated: *negated,
            escape: escape.clone(),
        },
        Expression::Conjunction(exprs) => Expression::Conjunction(
            exprs.iter().map(|e| rewrite_embedded_expression(e, hidden_base, components)).collect(),
        ),
        Expression::Disjunction(exprs) => Expression::Disjunction(
            exprs.iter().map(|e| rewrite_embedded_expression(e, hidden_base, components)).collect(),
        ),
        Expression::RowValueConstructor(exprs) => Expression::RowValueConstructor(
            exprs.iter().map(|e| rewrite_embedded_expression(e, hidden_base, components)).collect(),
        ),
        Expression::In { expr, subquery, negated } => Expression::In {
            expr: Box::new(rewrite_embedded_expression(expr, hidden_base, components)),
            subquery: subquery.clone(),
            negated: *negated,
        },
        // Fallback: treat as an opaque window-free component (preserves the
        // pre-#5232 placeholder behavior for exotic shapes).
        other => {
            let idx = hidden_base + components.len();
            components.push(EmbeddedWindowComponent { expr: other.clone(), is_window: false });
            embedded_col_ref(idx)
        }
    }
}

/// Build a map of window names to their resolved definitions
/// This handles window inheritance (e.g., WINDOW win AS (...), win2 AS (win ORDER BY x))
fn build_window_map(
    window_definitions: Option<&Vec<WindowDefinition>>,
) -> Result<HashMap<String, WindowSpec>, ExecutorError> {
    let mut resolved_map: HashMap<String, WindowSpec> = HashMap::new();

    if let Some(defs) = window_definitions {
        let raw_map: HashMap<String, &WindowSpec> =
            defs.iter().map(|def| (def.name.to_lowercase(), &def.spec)).collect();

        for def in defs {
            let resolved = resolve_window_spec_recursive(&def.spec, &raw_map, &resolved_map)?;
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
            let base_spec = window_map
                .get(&base_name.to_lowercase())
                .ok_or_else(|| ExecutorError::Other(format!("no such window: {}", base_name)))?;

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

/// Build a `PostAggregateWindowFunction` for a WindowFunction expression
/// whose placeholder value lives at `target_index` in the result rows.
fn make_post_aggregate_window_fn(
    function: &WindowFunctionSpec,
    over: &WindowSpec,
    target_index: usize,
    window_map: &HashMap<String, WindowSpec>,
) -> Result<PostAggregateWindowFunction, ExecutorError> {
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
    let resolved_spec = resolve_window_spec(over, window_map)?;

    Ok(PostAggregateWindowFunction {
        select_index: target_index,
        func_name,
        func_type,
        args,
        window_spec: resolved_spec,
    })
}

/// Collect all window functions from the SELECT list, resolving named window
/// references. Includes embedded window functions from the decomposition plan
/// (#5232), whose placeholder/result slot is their hidden component column.
fn collect_window_functions(
    select_list: &[SelectItem],
    window_definitions: Option<&Vec<WindowDefinition>>,
    embedded_plan: &EmbeddedWindowPlan,
    embedded_hidden_base: usize,
) -> Result<Vec<PostAggregateWindowFunction>, ExecutorError> {
    let window_map = build_window_map(window_definitions)?;
    let mut result = Vec::new();

    for (idx, item) in select_list.iter().enumerate() {
        if let SelectItem::Expression {
            expr: Expression::WindowFunction { function, over }, ..
        } = item
        {
            result.push(make_post_aggregate_window_fn(function, over, idx, &window_map)?);
        }
    }

    // Embedded window functions (#5232): their placeholder lives in a hidden
    // component column, which the window pass overwrites in place.
    for (i, comp) in embedded_plan.components.iter().enumerate() {
        if !comp.is_window {
            continue;
        }
        if let Expression::WindowFunction { function, over } = &comp.expr {
            result.push(make_post_aggregate_window_fn(
                function,
                over,
                embedded_hidden_base + i,
                &window_map,
            )?);
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
/// Hidden column layout in each row (after the SELECT items):
/// 1. `group_by_exprs` (count: G)
/// 2. `order_by_aggregates` (count: order_by_aggregates_count) — pre-computed
///    aggregates from the outer SQL ORDER BY (not used here, but skip past them)
/// 3. `window_aggregates` (count: W) — pre-computed aggregates from the window's
///    PARTITION BY/ORDER BY/frame (used to support `OVER (ORDER BY <agg>)`).
///    See issue #5106.
pub(super) fn apply_window_functions_to_aggregates(
    mut rows: Vec<Row>,
    select_list: &[SelectItem],
    database: &vibesql_storage::Database,
    window_definitions: Option<&Vec<WindowDefinition>>,
    group_by_exprs: &[Expression],
    order_by_aggregates_count: usize,
    window_aggregates: &[Expression],
    embedded_plan: &EmbeddedWindowPlan,
) -> Result<Vec<Row>, ExecutorError> {
    let embedded_hidden_base = select_list.len()
        + group_by_exprs.len()
        + order_by_aggregates_count
        + window_aggregates.len();
    let window_funcs = collect_window_functions(
        select_list,
        window_definitions,
        embedded_plan,
        embedded_hidden_base,
    )?;

    if window_funcs.is_empty() && embedded_plan.is_empty() {
        return Ok(rows);
    }

    // Build a schema for the aggregate result rows
    // Each column corresponds to a SELECT list item, plus hidden GROUP BY columns,
    // window-aggregate columns, and embedded-window component columns (#5232).
    let result_schema = build_aggregate_result_schema(
        select_list,
        group_by_exprs,
        order_by_aggregates_count,
        window_aggregates,
        embedded_plan.components.len(),
    );
    let evaluator = CombinedExpressionEvaluator::with_database(&result_schema, database);

    // Track row reordering from the last window function with PARTITION BY/ORDER BY.
    // SQLite leaves rows in the order of the last window sort pass when there is
    // no statement-level ORDER BY; mirror the non-aggregate path in
    // select/window/mod.rs (issue #5291, windowpushd.test 2.x.4.1).
    // Indices refer to positions in `rows`, which stay stable across the loop
    // below (values are updated in place; rows are never reordered mid-loop).
    let mut row_reordering: Option<Vec<usize>> = None;

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
                exprs
                    .iter()
                    .map(|e| {
                        map_expr_to_result_column(
                            e,
                            select_list,
                            group_by_exprs,
                            order_by_aggregates_count,
                            window_aggregates,
                        )
                    })
                    .collect::<Vec<_>>()
            });

        // Partition the rows
        let eval_fn = |expr: &Expression, row: &Row| -> Result<SqlValue, String> {
            evaluator.clear_cse_cache();
            evaluator.eval(expr, row).map_err(|e| format!("{:?}", e))
        };

        let mut partitions = partition_rows(rows.clone(), &partition_exprs, eval_fn)
            .map_err(ExecutorError::SqliteCompatError)?;

        // Sort each partition
        let order_by_items: Option<Vec<vibesql_ast::OrderByItem>> =
            win_func.window_spec.order_by.as_ref().map(|items| {
                items
                    .iter()
                    .map(|item| vibesql_ast::OrderByItem {
                        expr: map_expr_to_result_column(
                            &item.expr,
                            select_list,
                            group_by_exprs,
                            order_by_aggregates_count,
                            window_aggregates,
                        ),
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

        // Capture the window order (partition order, then ORDER BY within each
        // partition) of the last window function that has PARTITION BY or
        // ORDER BY. `partition_rows` orders partitions by key (BTreeMap), so
        // concatenating original_indices yields SQLite's last-pass row order.
        let has_partition_by =
            win_func.window_spec.partition_by.as_ref().is_some_and(|p| !p.is_empty());
        let has_order_by = win_func.window_spec.order_by.as_ref().is_some_and(|o| !o.is_empty());
        if has_partition_by || has_order_by {
            row_reordering =
                Some(partitions.iter().flat_map(|p| p.original_indices.iter().copied()).collect());
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
                .map(|arg| {
                    map_expr_to_result_column(
                        arg,
                        select_list,
                        group_by_exprs,
                        order_by_aggregates_count,
                        window_aggregates,
                    )
                })
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
                                    ) {
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
                            "MEDIAN" | "PERCENTILE" | "PERCENTILE_CONT" | "PERCENTILE_DISC" => {
                                let fname = win_func.func_name.to_lowercase();
                                let expected_args = if fname == "median" { 1 } else { 2 };
                                if win_func.args.len() != expected_args {
                                    return Err(ExecutorError::WrongNumberOfArguments {
                                        function_name: fname,
                                    });
                                }
                                // Fraction expression (second arg) is evaluated
                                // per row via the mapped result column
                                let fraction_expr = mapped_args.get(1);
                                evaluate_percentile_window(
                                    partition,
                                    frame_indices,
                                    &arg_expr,
                                    fraction_expr,
                                    &fname,
                                    None,
                                    inner_eval_fn,
                                )?
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
                                // SqliteCompatError keeps the bare SQLite message
                                // (e.g. "argument of ntile must be a positive
                                // integer") — mirrors the non-aggregate path in
                                // select/window/evaluation.rs. (window1.test 44.2.2)
                                evaluate_ntile(partition, n_val)
                                    .map_err(ExecutorError::SqliteCompatError)?
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
                                eval_fn(&mapped_args[1], &partition.rows[0]).ok().and_then(|v| {
                                    match v {
                                        SqlValue::Integer(n) => Some(n),
                                        _ => None,
                                    }
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
                                eval_fn(&mapped_args[1], &partition.rows[0]).ok().and_then(|v| {
                                    match v {
                                        SqlValue::Integer(n) => Some(n),
                                        _ => None,
                                    }
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

    // Re-evaluate residual expressions for SELECT items with embedded window
    // functions (#5232). At this point the hidden component columns hold the
    // computed window values (for window components) and the per-group values
    // (for window-free components), so the residual evaluates to the final
    // SELECT value.
    for rewrite in &embedded_plan.rewrites {
        for row in rows.iter_mut() {
            evaluator.clear_cse_cache();
            let value = evaluator.eval(&rewrite.residual, row)?;
            row.values[rewrite.select_index] = value;
        }
    }

    // Reorder output rows into the last window pass's order (see comment at
    // `row_reordering` above). A statement-level ORDER BY, if present, is
    // applied downstream and overrides this order.
    if let Some(order) = row_reordering {
        rows = order.into_iter().map(|idx| rows[idx].clone()).collect();
    }

    Ok(rows)
}

/// Build a schema for aggregate result rows
///
/// Uses consistent column naming: col0, col1, col2, ... so column references work correctly.
/// Includes hidden columns for GROUP BY expressions, ORDER BY aggregates, and window
/// aggregates (in that order, appended after SELECT items) so that window
/// ORDER BY / PARTITION BY clauses can reference any of them.
fn build_aggregate_result_schema(
    select_list: &[SelectItem],
    group_by_exprs: &[Expression],
    order_by_aggregates_count: usize,
    window_aggregates: &[Expression],
    embedded_component_count: usize,
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

    // Hidden ORDER BY aggregate columns (used by outer SQL ORDER BY, but referenced
    // here only to keep schema column count aligned with row width).
    let order_by_offset = select_list.len() + group_by_exprs.len();
    for i in 0..order_by_aggregates_count {
        let column_name = format!("col{}", order_by_offset + i);
        columns.push(ColumnSchema::new(
            column_name,
            DataType::Varchar { max_length: Some(255) },
            true,
        ));
    }

    // Hidden window-aggregate columns (used by window's PARTITION BY/ORDER BY/frame).
    let window_offset = order_by_offset + order_by_aggregates_count;
    for i in 0..window_aggregates.len() {
        let column_name = format!("col{}", window_offset + i);
        columns.push(ColumnSchema::new(
            column_name,
            DataType::Varchar { max_length: Some(255) },
            true,
        ));
    }

    // Hidden embedded-window component columns (#5232): placeholders for
    // embedded window functions and per-group values for window-free
    // subexpressions of SELECT items that mix windows with aggregates.
    let embedded_offset = window_offset + window_aggregates.len();
    for i in 0..embedded_component_count {
        let column_name = format!("col{}", embedded_offset + i);
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
        always_hidden_columns: std::collections::HashSet::new(),
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
/// For expressions that appear in the SELECT list, GROUP BY, or window aggregates,
/// we create a ColumnRef that references the computed value. For others, we return
/// the expression as-is.
///
/// Hidden column layout (after SELECT items):
/// - GROUP BY expressions at positions `select_list.len() + i`
/// - ORDER BY aggregates at positions `select_list.len() + G + i`
/// - Window aggregates at positions `select_list.len() + G + O + i`
fn map_expr_to_result_column(
    expr: &Expression,
    select_list: &[SelectItem],
    group_by_exprs: &[Expression],
    order_by_aggregates_count: usize,
    window_aggregates: &[Expression],
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
                    if col_id
                        .column_canonical()
                        .eq_ignore_ascii_case(select_col_id.column_canonical())
                    {
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
                if col_id.column_canonical().eq_ignore_ascii_case(gb_col_id.column_canonical()) {
                    return make_result_col_ref(select_count + i);
                }
            }
        }
    }

    // Check window-function aggregate expressions (stored as hidden columns after
    // GROUP BY and ORDER BY aggregate columns). This enables window ORDER BY /
    // PARTITION BY clauses that reference aggregates like `OVER (ORDER BY max(c))`.
    // See issue #5106.
    let window_agg_offset = select_count + group_by_exprs.len() + order_by_aggregates_count;
    for (i, win_agg_expr) in window_aggregates.iter().enumerate() {
        if expressions_match(expr, win_agg_expr) {
            return make_result_col_ref(window_agg_offset + i);
        }
    }

    // Also check if the expression matches a window function's first argument in the SELECT list.
    // The window function slot holds the pre-computed inner value (the first arg evaluated in
    // the GROUP BY context), so if ORDER BY references the same expression, we can use that slot.
    for (idx, item) in select_list.iter().enumerate() {
        if let SelectItem::Expression {
            expr: Expression::WindowFunction { function, .. }, ..
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
                a.column_canonical().eq_ignore_ascii_case(b.column_canonical())
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
        (Expression::UnaryOp { op: op1, expr: e1 }, Expression::UnaryOp { op: op2, expr: e2 }) => {
            op1 == op2 && expressions_match(e1, e2)
        }
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
            Expression::AggregateFunction { name: n1, args: a1, distinct: d1, .. },
            Expression::AggregateFunction { name: n2, args: a2, distinct: d2, .. },
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
