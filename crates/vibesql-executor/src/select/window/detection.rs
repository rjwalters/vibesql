//! Window function detection helpers

use vibesql_ast::{Expression, SelectItem};

/// Check if SELECT list contains any window functions
pub(in crate::select) fn has_window_functions(select_list: &[SelectItem]) -> bool {
    select_list.iter().any(|item| match item {
        SelectItem::Expression { expr, .. } => expression_has_window_function(expr),
        SelectItem::Wildcard { .. } | SelectItem::QualifiedWildcard { .. } => false,
    })
}

/// Check if an expression contains a window function
pub(in crate::select) fn expression_has_window_function(expr: &Expression) -> bool {
    match expr {
        Expression::WindowFunction { .. } => true,
        Expression::BinaryOp { left, right, .. } => {
            expression_has_window_function(left) || expression_has_window_function(right)
        }
        Expression::UnaryOp { expr, .. } => expression_has_window_function(expr),
        Expression::Function { args, .. } => args.iter().any(expression_has_window_function),
        Expression::Case { operand, when_clauses, else_result } => {
            operand.as_ref().is_some_and(|e| expression_has_window_function(e))
                || when_clauses.iter().any(|when_clause| {
                    when_clause.conditions.iter().any(expression_has_window_function)
                        || expression_has_window_function(&when_clause.result)
                })
                || else_result.as_ref().is_some_and(|e| expression_has_window_function(e))
        }
        Expression::IsNull { expr, .. } => expression_has_window_function(expr),
        _ => false,
    }
}
