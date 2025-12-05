//! Common Sub-Expression Elimination (CSE) Helper Functions
//!
//! These functions support optimizing GROUP BY queries by detecting and reusing
//! previously computed expression columns, avoiding redundant arithmetic operations.

use vibesql_ast::{BinaryOperator, Expression};

/// Compute the depth of an expression tree (used for sorting by complexity)
pub(super) fn expression_depth(expr: &Expression) -> usize {
    match expr {
        Expression::ColumnRef { .. } | Expression::Literal(_) => 1,
        Expression::BinaryOp { left, right, .. } => {
            1 + expression_depth(left).max(expression_depth(right))
        }
        Expression::UnaryOp { expr: inner, .. } => 1 + expression_depth(inner),
        _ => 1,
    }
}

/// Hash an expression for cache lookup
/// Uses a simple string-based hash that captures the expression structure
pub(super) fn hash_expression(expr: &Expression) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::Hasher;

    let mut hasher = DefaultHasher::new();
    hash_expression_recursive(expr, &mut hasher);
    hasher.finish()
}

fn hash_expression_recursive<H: std::hash::Hasher>(expr: &Expression, hasher: &mut H) {
    use std::hash::Hash;
    match expr {
        Expression::ColumnRef { table, column } => {
            "col".hash(hasher);
            table.hash(hasher);
            column.hash(hasher);
        }
        Expression::Literal(val) => {
            "lit".hash(hasher);
            format!("{:?}", val).hash(hasher);
        }
        Expression::BinaryOp { left, op, right } => {
            "binop".hash(hasher);
            format!("{:?}", op).hash(hasher);
            hash_expression_recursive(left, hasher);
            hash_expression_recursive(right, hasher);
        }
        Expression::UnaryOp { op, expr: inner } => {
            "unop".hash(hasher);
            format!("{:?}", op).hash(hasher);
            hash_expression_recursive(inner, hasher);
        }
        _ => {
            // For unsupported expressions, use a unique hash
            format!("{:?}", expr).hash(hasher);
        }
    }
}

/// Find a cached sub-expression that can be used to compute the given expression
///
/// For TPC-H Q1 style queries:
/// - E1 = l_extendedprice * (1 - l_discount)
/// - E2 = l_extendedprice * (1 - l_discount) * (1 + l_tax)
///
/// If E1 is already computed (in expr_cache), this function detects that E2 = E1 * (1 + l_tax)
/// and returns (cached_column_for_E1, remaining_expression = (1 + l_tax))
pub(super) fn find_cached_subexpression(
    expr: &Expression,
    expr_cache: &std::collections::HashMap<u64, usize>,
) -> Option<(usize, Expression)> {
    // Only handle binary multiply operations for now (most common pattern)
    if let Expression::BinaryOp { left, op: BinaryOperator::Multiply, right } = expr {
        // Check if left operand is a cached expression
        let left_hash = hash_expression(left);
        if let Some(&cached_col) = expr_cache.get(&left_hash) {
            // E = cached_expr * right → use cached column
            return Some((cached_col, *right.clone()));
        }

        // Check if right operand is a cached expression
        let right_hash = hash_expression(right);
        if let Some(&cached_col) = expr_cache.get(&right_hash) {
            // E = left * cached_expr → use cached column
            return Some((cached_col, *left.clone()));
        }

        // Recursively check if left or right contains a cached sub-expression
        // This handles nested cases like: (A * B) * C where A * B is cached
        if let Some((cached_col, remaining)) = find_cached_subexpression(left, expr_cache) {
            // Build: remaining * right
            return Some((
                cached_col,
                Expression::BinaryOp {
                    left: Box::new(remaining),
                    op: BinaryOperator::Multiply,
                    right: right.clone(),
                },
            ));
        }

        if let Some((cached_col, remaining)) = find_cached_subexpression(right, expr_cache) {
            // Build: left * remaining
            return Some((
                cached_col,
                Expression::BinaryOp {
                    left: left.clone(),
                    op: BinaryOperator::Multiply,
                    right: Box::new(remaining),
                },
            ));
        }
    }

    None
}
