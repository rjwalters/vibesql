//! Expression traversal utilities for detecting aggregate, arithmetic, and window functions

use vibesql_ast::{BinaryOperator, Expression};

/// Recursively check if an expression contains aggregate functions
pub(super) fn contains_aggregate(expr: &Expression) -> bool {
    match expr {
        Expression::AggregateFunction { .. } => true,
        Expression::Function { args, .. } => {
            // Check arguments for nested aggregates
            args.iter().any(contains_aggregate)
        }
        Expression::BinaryOp { left, right, .. } => {
            contains_aggregate(left) || contains_aggregate(right)
        }
        Expression::UnaryOp { expr, .. } => contains_aggregate(expr),
        Expression::Case { operand, when_clauses, else_result, .. } => {
            operand.as_ref().is_some_and(|e| contains_aggregate(e))
                || when_clauses.iter().any(|clause| {
                    clause.conditions.iter().any(contains_aggregate)
                        || contains_aggregate(&clause.result)
                })
                || else_result.as_ref().is_some_and(|e| contains_aggregate(e))
        }
        Expression::InList { expr, values, .. } => {
            contains_aggregate(expr) || values.iter().any(contains_aggregate)
        }
        Expression::Between { expr, low, high, .. } => {
            contains_aggregate(expr) || contains_aggregate(low) || contains_aggregate(high)
        }
        Expression::ScalarSubquery(_) | Expression::In { .. } => {
            // Conservative: assume subqueries may contain aggregates
            true
        }
        _ => false,
    }
}

/// Recursively check if an expression contains arithmetic operations
pub(super) fn contains_arithmetic(expr: &Expression) -> bool {
    match expr {
        Expression::BinaryOp { op, left, right } => {
            let is_arithmetic = matches!(
                op,
                BinaryOperator::Plus
                    | BinaryOperator::Minus
                    | BinaryOperator::Multiply
                    | BinaryOperator::Divide
                    | BinaryOperator::Modulo
                    | BinaryOperator::IntegerDivide
                    | BinaryOperator::Concat
            );

            is_arithmetic || contains_arithmetic(left) || contains_arithmetic(right)
        }
        Expression::UnaryOp { expr, .. } => contains_arithmetic(expr),
        Expression::Case { operand, when_clauses, else_result, .. } => {
            operand.as_ref().is_some_and(|e| contains_arithmetic(e))
                || when_clauses.iter().any(|clause| {
                    clause.conditions.iter().any(contains_arithmetic)
                        || contains_arithmetic(&clause.result)
                })
                || else_result.as_ref().is_some_and(|e| contains_arithmetic(e))
        }
        Expression::InList { expr, values, .. } => {
            contains_arithmetic(expr) || values.iter().any(contains_arithmetic)
        }
        Expression::Between { expr, low, high, .. } => {
            contains_arithmetic(expr) || contains_arithmetic(low) || contains_arithmetic(high)
        }
        Expression::Function { args, .. } | Expression::AggregateFunction { args, .. } => {
            args.iter().any(contains_arithmetic)
        }
        _ => false,
    }
}

/// Recursively check if an expression contains window functions
pub(super) fn contains_window_function(expr: &Expression) -> bool {
    match expr {
        Expression::WindowFunction { .. } => true,
        Expression::BinaryOp { left, right, .. } => {
            contains_window_function(left) || contains_window_function(right)
        }
        Expression::UnaryOp { expr, .. } => contains_window_function(expr),
        Expression::Function { args, .. } | Expression::AggregateFunction { args, .. } => {
            args.iter().any(contains_window_function)
        }
        Expression::Case { operand, when_clauses, else_result, .. } => {
            operand.as_ref().is_some_and(|e| contains_window_function(e))
                || when_clauses.iter().any(|clause| {
                    clause.conditions.iter().any(contains_window_function)
                        || contains_window_function(&clause.result)
                })
                || else_result.as_ref().is_some_and(|e| contains_window_function(e))
        }
        _ => false,
    }
}
