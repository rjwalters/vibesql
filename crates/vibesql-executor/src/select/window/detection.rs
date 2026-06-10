//! Window function detection helpers

use vibesql_ast::{Expression, SelectItem};

/// Check if SELECT list contains any window functions
pub(in crate::select) fn has_window_functions(select_list: &[SelectItem]) -> bool {
    select_list.iter().any(|item| match item {
        SelectItem::Expression { expr, .. } => expression_has_window_function(expr),
        SelectItem::Wildcard { .. } | SelectItem::QualifiedWildcard { .. } => false,
    })
}

/// Check if an expression contains a window function in the current scope.
///
/// Mirrors the recursion shape of
/// `crate::select::window::collection::collect_from_expression` so that this
/// detection function and the collector agree on which expressions contain a
/// window function. If they diverge, queries can be routed to a non-window
/// executor and then trip the window evaluator's misuse-of-window check on a
/// legitimate window expression (issue #5095).
///
/// Subquery variants (`ScalarSubquery`, `In { subquery }`, `Exists`,
/// `QuantifiedComparison`) are intentionally treated as leaves: a window
/// inside a subquery belongs to the subquery's own scope, not the outer
/// SELECT's.
pub(crate) fn expression_has_window_function(expr: &Expression) -> bool {
    match expr {
        Expression::WindowFunction { .. } => true,
        Expression::BinaryOp { left, right, .. } => {
            expression_has_window_function(left) || expression_has_window_function(right)
        }
        Expression::Conjunction(exprs) | Expression::Disjunction(exprs) => {
            exprs.iter().any(expression_has_window_function)
        }
        Expression::UnaryOp { expr, .. } => expression_has_window_function(expr),
        Expression::Function { args, .. } => args.iter().any(expression_has_window_function),
        Expression::AggregateFunction { args, order_by, filter, .. } => {
            args.iter().any(expression_has_window_function)
                || order_by.as_ref().is_some_and(|items| {
                    items.iter().any(|i| expression_has_window_function(&i.expr))
                })
                || filter.as_ref().is_some_and(|f| expression_has_window_function(f))
        }
        Expression::Case { operand, when_clauses, else_result } => {
            operand.as_ref().is_some_and(|e| expression_has_window_function(e))
                || when_clauses.iter().any(|when_clause| {
                    when_clause.conditions.iter().any(expression_has_window_function)
                        || expression_has_window_function(&when_clause.result)
                })
                || else_result.as_ref().is_some_and(|e| expression_has_window_function(e))
        }
        Expression::IsNull { expr, .. } => expression_has_window_function(expr),
        Expression::IsDistinctFrom { left, right, .. } => {
            expression_has_window_function(left) || expression_has_window_function(right)
        }
        Expression::IsTruthValue { expr, .. } => expression_has_window_function(expr),
        Expression::Cast { expr, .. } => expression_has_window_function(expr),
        Expression::Like { expr, pattern, escape, .. }
        | Expression::Glob { expr, pattern, escape, .. } => {
            expression_has_window_function(expr)
                || expression_has_window_function(pattern)
                || escape.as_ref().is_some_and(|e| expression_has_window_function(e))
        }
        Expression::InList { expr, values, .. } => {
            expression_has_window_function(expr)
                || values.iter().any(expression_has_window_function)
        }
        Expression::In { expr, .. } => expression_has_window_function(expr),
        Expression::QuantifiedComparison { expr, .. } => expression_has_window_function(expr),
        Expression::Between { expr, low, high, .. } => {
            expression_has_window_function(expr)
                || expression_has_window_function(low)
                || expression_has_window_function(high)
        }
        Expression::Position { substring, string, .. } => {
            expression_has_window_function(substring) || expression_has_window_function(string)
        }
        Expression::Trim { removal_char, string, .. } => {
            removal_char.as_ref().is_some_and(|e| expression_has_window_function(e))
                || expression_has_window_function(string)
        }
        Expression::Extract { expr, .. } => expression_has_window_function(expr),
        Expression::Interval { value, .. } => expression_has_window_function(value),
        _ => false,
    }
}
