//! Window function collection logic

use vibesql_ast::{Expression, SelectItem, WindowFunctionSpec};

use super::types::WindowFunctionInfo;
use crate::errors::ExecutorError;

/// Collect all window functions from SELECT list
pub(super) fn collect_window_functions(
    select_list: &[SelectItem],
) -> Result<Vec<WindowFunctionInfo>, ExecutorError> {
    let mut window_functions = Vec::new();

    for (idx, item) in select_list.iter().enumerate() {
        if let SelectItem::Expression { expr, .. } = item {
            collect_from_expression(expr, idx, &mut window_functions)?;
        }
    }

    Ok(window_functions)
}

/// Recursively collect window functions from an expression
fn collect_from_expression(
    expr: &Expression,
    select_index: usize,
    window_functions: &mut Vec<WindowFunctionInfo>,
) -> Result<(), ExecutorError> {
    match expr {
        Expression::WindowFunction { function, over } => {
            // Handle all window function types: Aggregate, Ranking, and Value
            match function {
                WindowFunctionSpec::Aggregate { .. }
                | WindowFunctionSpec::Ranking { .. }
                | WindowFunctionSpec::Value { .. } => {
                    window_functions.push(WindowFunctionInfo {
                        _select_index: select_index,
                        function_spec: function.clone(),
                        window_spec: over.clone(),
                    });
                }
            }
        }
        Expression::BinaryOp { left, right, .. } => {
            collect_from_expression(left, select_index, window_functions)?;
            collect_from_expression(right, select_index, window_functions)?;
        }
        Expression::UnaryOp { expr, .. } => {
            collect_from_expression(expr, select_index, window_functions)?;
        }
        Expression::Function { args, .. } => {
            for arg in args {
                collect_from_expression(arg, select_index, window_functions)?;
            }
        }
        Expression::Case { operand, when_clauses, else_result } => {
            if let Some(op) = operand {
                collect_from_expression(op, select_index, window_functions)?;
            }
            for when_clause in when_clauses {
                for cond in &when_clause.conditions {
                    collect_from_expression(cond, select_index, window_functions)?;
                }
                collect_from_expression(&when_clause.result, select_index, window_functions)?;
            }
            if let Some(else_expr) = else_result {
                collect_from_expression(else_expr, select_index, window_functions)?;
            }
        }
        Expression::IsNull { expr, .. } => {
            collect_from_expression(expr, select_index, window_functions)?;
        }
        _ => {}
    }
    Ok(())
}
