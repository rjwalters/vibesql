//! IN subquery detection logic
//!
//! This module provides fast detection of IN subqueries in SQL AST nodes
//! to enable early-exit optimization checks.

use vibesql_ast::{Expression, SelectItem, SelectStmt};

/// Check if a SELECT statement contains any IN subqueries
///
/// This is a fast pre-check to avoid expensive AST cloning and rewriting
/// for queries that don't have IN subqueries.
pub(super) fn has_in_subqueries(stmt: &SelectStmt) -> bool {
    // Check WHERE clause
    if let Some(where_clause) = &stmt.where_clause {
        if expression_has_in_subquery(where_clause) {
            return true;
        }
    }

    // Check HAVING clause
    if let Some(having) = &stmt.having {
        if expression_has_in_subquery(having) {
            return true;
        }
    }

    // Check SELECT list
    for item in &stmt.select_list {
        if let SelectItem::Expression { expr, .. } = item {
            if expression_has_in_subquery(expr) {
                return true;
            }
        }
    }

    // Check FROM clause subqueries
    if let Some(from) = &stmt.from {
        if from_clause_has_in_subquery(from) {
            return true;
        }
    }

    // Check set operations
    if let Some(set_op) = &stmt.set_operation {
        if has_in_subqueries(&set_op.right) {
            return true;
        }
    }

    false
}

/// Check if an expression contains an IN subquery
pub(super) fn expression_has_in_subquery(expr: &Expression) -> bool {
    match expr {
        Expression::In { .. } => true,

        Expression::BinaryOp { left, right, .. } => {
            expression_has_in_subquery(left) || expression_has_in_subquery(right)
        }

        Expression::UnaryOp { expr, .. } => expression_has_in_subquery(expr),

        Expression::IsNull { expr, .. } => expression_has_in_subquery(expr),

        Expression::Case { operand, when_clauses, else_result } => {
            operand.as_ref().is_some_and(|e| expression_has_in_subquery(e))
                || when_clauses.iter().any(|clause| {
                    clause.conditions.iter().any(expression_has_in_subquery)
                        || expression_has_in_subquery(&clause.result)
                })
                || else_result.as_ref().is_some_and(|e| expression_has_in_subquery(e))
        }

        Expression::ScalarSubquery(subquery) => has_in_subqueries(subquery),

        Expression::Exists { subquery, .. } => has_in_subqueries(subquery),

        Expression::QuantifiedComparison { expr, subquery, .. } => {
            expression_has_in_subquery(expr) || has_in_subqueries(subquery)
        }

        Expression::InList { expr, values, .. } => {
            expression_has_in_subquery(expr) || values.iter().any(expression_has_in_subquery)
        }

        Expression::Between { expr, low, high, .. } => {
            expression_has_in_subquery(expr)
                || expression_has_in_subquery(low)
                || expression_has_in_subquery(high)
        }

        Expression::Cast { expr, .. } => expression_has_in_subquery(expr),

        Expression::Function { args, .. } | Expression::AggregateFunction { args, .. } => {
            args.iter().any(expression_has_in_subquery)
        }

        Expression::Position { substring, string, .. } => {
            expression_has_in_subquery(substring) || expression_has_in_subquery(string)
        }

        Expression::Trim { removal_char, string, .. } => {
            removal_char.as_ref().is_some_and(|e| expression_has_in_subquery(e))
                || expression_has_in_subquery(string)
        }

        Expression::Like { expr, pattern, .. } => {
            expression_has_in_subquery(expr) || expression_has_in_subquery(pattern)
        }

        Expression::Interval { value, .. } => expression_has_in_subquery(value),

        _ => false,
    }
}

/// Check if a FROM clause contains IN subqueries
fn from_clause_has_in_subquery(from: &vibesql_ast::FromClause) -> bool {
    match from {
        vibesql_ast::FromClause::Table { .. } => false,
        vibesql_ast::FromClause::Join { left, right, condition, .. } => {
            from_clause_has_in_subquery(left)
                || from_clause_has_in_subquery(right)
                || condition.as_ref().is_some_and(expression_has_in_subquery)
        }
        vibesql_ast::FromClause::Subquery { query, .. } => has_in_subqueries(query),
    }
}
