//! ORDER BY window function support

use std::collections::HashMap;

use vibesql_ast::{Expression, WindowFunctionSpec};
use vibesql_types::SqlValue;

use super::{
    evaluation::evaluate_single_window_function,
    types::{WindowFunctionInfo, WindowFunctionKey},
};
use crate::{errors::ExecutorError, evaluator::CombinedExpressionEvaluator};

/// Collect window functions from ORDER BY expressions
pub(in crate::select) fn collect_order_by_window_functions(
    order_by: &[vibesql_ast::OrderByItem],
) -> Vec<(WindowFunctionSpec, vibesql_ast::WindowSpec)> {
    let mut window_functions = Vec::new();

    for item in order_by {
        collect_window_functions_from_expression(&item.expr, &mut window_functions);
    }

    window_functions
}

/// Recursively collect window functions from an expression
fn collect_window_functions_from_expression(
    expr: &Expression,
    window_functions: &mut Vec<(WindowFunctionSpec, vibesql_ast::WindowSpec)>,
) {
    match expr {
        Expression::WindowFunction { function, over } => {
            window_functions.push((function.clone(), over.clone()));
        }
        Expression::Default => {} // DEFAULT has no window functions
        Expression::DuplicateKeyValue { .. } => {} // DuplicateKeyValue has no window functions
        Expression::BinaryOp { left, right, .. } => {
            collect_window_functions_from_expression(left, window_functions);
            collect_window_functions_from_expression(right, window_functions);
        }
        Expression::UnaryOp { expr, .. } => {
            collect_window_functions_from_expression(expr, window_functions);
        }
        Expression::Interval { value, .. } => {
            collect_window_functions_from_expression(value, window_functions);
        }
        Expression::Function { args, .. } => {
            for arg in args {
                collect_window_functions_from_expression(arg, window_functions);
            }
        }
        Expression::Case { when_clauses, else_result, .. } => {
            for when_clause in when_clauses {
                for cond in &when_clause.conditions {
                    collect_window_functions_from_expression(cond, window_functions);
                }
                collect_window_functions_from_expression(&when_clause.result, window_functions);
            }
            if let Some(else_result) = else_result {
                collect_window_functions_from_expression(else_result, window_functions);
            }
        }
        Expression::In { expr, .. } => {
            collect_window_functions_from_expression(expr, window_functions);
        }
        Expression::InList { expr, .. } => {
            collect_window_functions_from_expression(expr, window_functions);
        }
        Expression::Between { expr, low, high, .. } => {
            collect_window_functions_from_expression(expr, window_functions);
            collect_window_functions_from_expression(low, window_functions);
            collect_window_functions_from_expression(high, window_functions);
        }
        Expression::Like { expr, pattern, .. } => {
            collect_window_functions_from_expression(expr, window_functions);
            collect_window_functions_from_expression(pattern, window_functions);
        }
        Expression::Cast { expr, .. } => {
            collect_window_functions_from_expression(expr, window_functions);
        }
        Expression::IsNull { expr, .. } => {
            collect_window_functions_from_expression(expr, window_functions);
        }
        Expression::Position { substring, string, character_unit: _ } => {
            collect_window_functions_from_expression(substring, window_functions);
            collect_window_functions_from_expression(string, window_functions);
        }
        Expression::Trim { removal_char, string, .. } => {
            if let Some(removal_char) = removal_char {
                collect_window_functions_from_expression(removal_char, window_functions);
            }
            collect_window_functions_from_expression(string, window_functions);
        }
        Expression::Extract { expr, .. } => {
            collect_window_functions_from_expression(expr, window_functions);
        }
        Expression::Exists { .. }
        | Expression::ScalarSubquery(_)
        | Expression::QuantifiedComparison { .. } => {
            // These don't contain window functions in their expressions
        }
        Expression::AggregateFunction { .. } => {
            // Aggregate functions don't contain window functions
        }
        Expression::Wildcard => {
            // Wildcard doesn't contain window functions
        }
        Expression::Literal(_)
        | Expression::ColumnRef { .. }
        | Expression::PseudoVariable { .. }
        | Expression::SessionVariable { .. } => {
            // These are leaf nodes
        }
        Expression::CurrentDate
        | Expression::CurrentTime { .. }
        | Expression::CurrentTimestamp { .. } => {
            // Current date/time functions don't contain window functions
        }
        Expression::NextValue { .. } => {
            // Sequence expressions don't contain window functions
        }
        Expression::MatchAgainst { search_modifier, .. } => {
            // Recursively collect window functions from the search term
            collect_window_functions_from_expression(search_modifier, window_functions);
        }

        // Placeholders don't contain window functions
        Expression::Placeholder(_)
        | Expression::NumberedPlaceholder(_)
        | Expression::NamedPlaceholder(_) => {}

        Expression::Conjunction(children) | Expression::Disjunction(children) => {
            for child in children {
                collect_window_functions_from_expression(child, window_functions);
            }
        }
    }
}

/// Evaluate window functions found in ORDER BY expressions
pub(in crate::select) fn evaluate_order_by_window_functions(
    mut rows: Vec<vibesql_storage::Row>,
    order_by_window_functions: Vec<(WindowFunctionSpec, vibesql_ast::WindowSpec)>,
    evaluator: &CombinedExpressionEvaluator,
    existing_mapping: Option<&HashMap<WindowFunctionKey, usize>>,
) -> Result<(Vec<vibesql_storage::Row>, HashMap<WindowFunctionKey, usize>), ExecutorError> {
    if order_by_window_functions.is_empty() {
        return Ok((rows, HashMap::new()));
    }

    // Build mapping from existing functions to avoid duplicates
    let existing_keys: std::collections::HashSet<_> =
        existing_mapping.map(|m| m.keys().collect()).unwrap_or_default();

    let mut window_results: Vec<Vec<SqlValue>> = Vec::new();
    let mut window_mapping = HashMap::new();

    // Track the column index where window function results start
    let base_column_count = if rows.is_empty() { 0 } else { rows[0].values.len() };

    for (idx, (function_spec, window_spec)) in order_by_window_functions.iter().enumerate() {
        let key = WindowFunctionKey::from_expression(function_spec, window_spec);

        // Skip if this window function is already evaluated
        if existing_keys.contains(&key) {
            continue;
        }

        let win_func_info = WindowFunctionInfo {
            _select_index: 0, // Dummy value for ORDER BY functions
            function_spec: function_spec.clone(),
            window_spec: window_spec.clone(),
        };
        let values = evaluate_single_window_function(&rows, &win_func_info, evaluator)?;
        window_results.push(values);

        // Build mapping: WindowFunctionKey -> column index
        let col_idx = base_column_count + idx;
        window_mapping.insert(key, col_idx);
    }

    // Extend each row with window function results
    for (row_idx, row) in rows.iter_mut().enumerate() {
        for results in &window_results {
            row.values.push(results[row_idx].clone());
        }
    }

    Ok((rows, window_mapping))
}
