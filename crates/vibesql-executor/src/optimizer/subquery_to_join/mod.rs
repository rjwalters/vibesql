//! Transform IN/EXISTS subqueries to semi/anti-joins
//!
//! Note: Some clone_on_copy lints are suppressed because the code is clearer
//! with explicit clones for operators that may not be Copy in future.

#![allow(clippy::clone_on_copy)]

//!
//! This module transforms decorrelated IN/NOT IN/EXISTS/NOT EXISTS subqueries
//! in the WHERE clause into SEMI/ANTI joins in the FROM clause, enabling
//! efficient hash-based join execution instead of row-by-row subquery evaluation.
//!
//! ## Transformation Examples
//!
//! ### IN → SEMI JOIN
//! ```sql
//! -- Before:
//! SELECT * FROM orders WHERE o_orderkey IN (SELECT l_orderkey FROM lineitem)
//!
//! -- After:
//! SELECT orders.* FROM orders SEMI JOIN lineitem ON o_orderkey = l_orderkey
//! ```
//!
//! ### NOT IN → ANTI JOIN
//! ```sql
//! -- Before:
//! SELECT * FROM orders WHERE o_orderkey NOT IN (SELECT l_orderkey FROM lineitem)
//!
//! -- After:
//! SELECT orders.* FROM orders ANTI JOIN lineitem ON o_orderkey = l_orderkey
//! ```
//!
//! ### EXISTS → SEMI JOIN
//! ```sql
//! -- Before (after decorrelation):
//! SELECT * FROM orders WHERE o_orderkey IN (SELECT DISTINCT l_orderkey FROM lineitem)
//!
//! -- After:
//! SELECT orders.* FROM orders SEMI JOIN lineitem ON o_orderkey = l_orderkey
//! ```

mod exists;
mod helpers;
mod in_clause;

use vibesql_ast::{BinaryOperator, Expression, FromClause, SelectStmt};

use exists::try_convert_exists_to_join;
use in_clause::try_convert_in_to_join;

/// Transform a SELECT statement by converting IN/NOT IN subqueries to semi/anti-joins
///
/// This transformation only applies to simple IN subqueries with single-column SELECT lists
/// and simple table references. Complex subqueries (joins, aggregations, etc.) are left unchanged.
///
/// The transformation is applied iteratively to handle queries with multiple subqueries
/// (e.g., Q21 with both EXISTS and NOT EXISTS clauses).
pub fn transform_subqueries_to_joins(stmt: &SelectStmt) -> SelectStmt {
    let mut result = stmt.clone();

    // Only transform if we have a FROM clause and a WHERE clause
    if result.from.is_none() || result.where_clause.is_none() {
        return result;
    }

    // Apply transformation iteratively until no more changes
    // This handles queries with multiple IN/EXISTS subqueries
    let max_iterations = 10; // Safety limit to prevent infinite loops
    for iteration in 0..max_iterations {
        let mut made_progress = false;

        // Try to extract IN/NOT IN/EXISTS subqueries from WHERE clause and convert to joins
        if let Some(where_clause) = &result.where_clause {
            if let Some((new_from, new_where)) =
                try_extract_subqueries_to_joins(result.from.as_ref().unwrap(), where_clause)
            {
                // Debug output for subquery transformation
                if std::env::var("SUBQUERY_TRANSFORM_VERBOSE").is_ok() {
                    eprintln!(
                        "[SUBQUERY_TRANSFORM] Iteration {}: Converted subquery to join",
                        iteration + 1
                    );
                }
                result.from = Some(new_from);
                result.where_clause = new_where;
                made_progress = true;
            }
        }

        // If no transformation was applied this iteration, we're done
        if !made_progress {
            if std::env::var("SUBQUERY_TRANSFORM_VERBOSE").is_ok() && iteration > 0 {
                eprintln!("[SUBQUERY_TRANSFORM] Completed after {} iterations", iteration);
            }
            break;
        }
    }

    result
}

/// Try to extract IN/NOT IN subqueries from WHERE clause and convert to semi/anti-joins
fn try_extract_subqueries_to_joins(
    from: &FromClause,
    where_clause: &Expression,
) -> Option<(FromClause, Option<Expression>)> {
    // Look for IN subquery at the top level or in AND branches
    match where_clause {
        // Single IN subquery
        Expression::In { expr, subquery, negated } => {
            if let Some(result) = try_convert_in_to_join(from, expr, subquery, *negated) {
                return Some((result.from, None)); // Removed WHERE clause entirely
            }
            None
        }

        // AND with potential IN/EXISTS subqueries
        Expression::BinaryOp { op: BinaryOperator::And, left, right } => {
            // Try left side first - check both IN and EXISTS
            match left.as_ref() {
                Expression::In { expr, subquery, negated } => {
                    if let Some(result) = try_convert_in_to_join(from, expr, subquery, *negated) {
                        // Successfully converted left IN side, keep right side as WHERE clause
                        // Note: We no longer qualify remaining WHERE clause columns because it
                        // incorrectly qualifies columns from OTHER tables (not the self-join table).
                        // The subquery's columns have already been rewritten to use the new alias,
                        // so column resolution will work correctly during execution.
                        return Some((result.from, Some((**right).clone())));
                    }
                }
                Expression::Exists { subquery, negated } => {
                    if let Some((join, _)) = try_convert_exists_to_join(from, subquery, *negated) {
                        // Successfully converted left EXISTS side, keep right side as WHERE clause
                        return Some((join, Some((**right).clone())));
                    }
                }
                _ => {}
            }

            // Try right side - check both IN and EXISTS
            match right.as_ref() {
                Expression::In { expr, subquery, negated } => {
                    if let Some(result) = try_convert_in_to_join(from, expr, subquery, *negated) {
                        // Successfully converted right IN side, keep left side as WHERE clause
                        // Note: We no longer qualify remaining WHERE clause columns because it
                        // incorrectly qualifies columns from OTHER tables (not the self-join table).
                        return Some((result.from, Some((**left).clone())));
                    }
                }
                Expression::Exists { subquery, negated } => {
                    if let Some((join, _)) = try_convert_exists_to_join(from, subquery, *negated) {
                        // Successfully converted right EXISTS side, keep left side as WHERE clause
                        return Some((join, Some((**left).clone())));
                    }
                }
                _ => {}
            }

            // Try recursively on left side
            if let Some((new_left_from, new_left_where)) =
                try_extract_subqueries_to_joins(from, left)
            {
                let combined_where = match new_left_where {
                    Some(new_left) => Some(Expression::BinaryOp {
                        op: BinaryOperator::And,
                        left: Box::new(new_left),
                        right: right.clone(),
                    }),
                    None => Some((**right).clone()),
                };
                return Some((new_left_from, combined_where));
            }

            // Try recursively on right side
            if let Some((new_right_from, new_right_where)) =
                try_extract_subqueries_to_joins(from, right)
            {
                let combined_where = match new_right_where {
                    Some(new_right) => Some(Expression::BinaryOp {
                        op: BinaryOperator::And,
                        left: left.clone(),
                        right: Box::new(new_right),
                    }),
                    None => Some((**left).clone()),
                };
                return Some((new_right_from, combined_where));
            }

            None
        }

        // EXISTS can also be converted (after decorrelation it becomes IN, but handle it directly too)
        Expression::Exists { subquery, negated } => {
            // Try to convert EXISTS to a semi-join by extracting correlation
            try_convert_exists_to_join(from, subquery, *negated)
        }

        _ => None,
    }
}

#[cfg(test)]
mod tests;
