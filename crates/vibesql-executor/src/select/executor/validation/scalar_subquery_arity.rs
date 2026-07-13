//! Prepare-time validation of bare scalar-subquery arity in scalar positions.
//!
//! SQLite validates at prepare time that a subquery used where a single value is
//! required returns exactly one column, producing
//! `sub-select returns N columns - expected 1`; and that a multi-column subquery
//! compared against a plain scalar in a *value* position is
//! `row value misused`.
//!
//! VibeSQL's expression evaluator performs equivalent checks, but only when the
//! expression is actually evaluated against a row. When the target table is
//! empty the expression never executes, so the malformed statement silently
//! succeeds (`rowvalue.test` 15.2/15.3/15.4, and the `28.10` trigger body over
//! an empty table). These validators inspect the specific top-level shapes that
//! SQLite rejects at prepare time so the error is raised regardless of row
//! count.
//!
//! Scope is deliberately narrow: only the *top-level* expression of a
//! value-producing position (a SELECT-list item, an UPDATE `SET` value) or a
//! boolean predicate (`WHERE`) is inspected, because that is the position that
//! goes unevaluated when the table is empty. A multi-column subquery paired with
//! a `RowValueConstructor` or another subquery is a legal row-value comparison
//! and is left untouched (the evaluator and the `row_values` validator cover
//! those shapes). Nested sub-expressions are left to the runtime check.
//!
//! ## Context-sensitive messages (matching SQLite 3.51)
//!
//! | Context                    | shape                       | message           |
//! |----------------------------|-----------------------------|-------------------|
//! | value (SELECT item, SET)   | bare `(SELECT a,b)`         | arity error       |
//! |                            | `(SELECT a,b) <op> scalar`  | row value misused |
//! |                            | `scalar <op> (SELECT a,b)`  | arity error       |
//! | predicate (WHERE)          | `col <op> (SELECT a,b)`     | arity error       |
//! |                            | `(SELECT a,b) <op> col`     | arity error       |

use vibesql_ast::{BinaryOperator, Expression};

use crate::errors::ExecutorError;

fn is_comparison_op(op: &BinaryOperator) -> bool {
    matches!(
        op,
        BinaryOperator::Equal
            | BinaryOperator::NotEqual
            | BinaryOperator::LessThan
            | BinaryOperator::LessThanOrEqual
            | BinaryOperator::GreaterThan
            | BinaryOperator::GreaterThanOrEqual
    )
}

/// Column count of a scalar subquery, if it can be determined statically.
/// Returns `None` when the count cannot be computed (e.g. a wildcard over a
/// not-yet-materialized CTE), in which case validation defers to the runtime
/// check rather than guessing.
fn subquery_column_count(
    subquery: &vibesql_ast::SelectStmt,
    database: &vibesql_storage::Database,
) -> Option<usize> {
    crate::evaluator::compute_select_list_column_count(subquery, database, None).ok()
}

/// If `expr` is a scalar subquery returning more than one column (statically
/// determinable), return that column count; otherwise `None`.
fn multi_col_subquery(expr: &Expression, database: &vibesql_storage::Database) -> Option<usize> {
    if let Expression::ScalarSubquery(subquery) = expr {
        match subquery_column_count(subquery, database) {
            Some(n) if n > 1 => Some(n),
            _ => None,
        }
    } else {
        None
    }
}

/// True when `expr` is a row value or a scalar subquery — the two operand
/// shapes that make a comparison a legal *row-value* comparison (so a
/// multi-column subquery on the other side is not a scalar misuse).
fn is_row_value_or_subquery(expr: &Expression) -> bool {
    matches!(expr, Expression::RowValueConstructor(e) if e.len() > 1)
        || matches!(expr, Expression::ScalarSubquery(_))
}

fn arity_error(actual: usize) -> ExecutorError {
    ExecutorError::SubqueryColumnCountMismatch { expected: 1, actual }
}

/// Validate scalar-subquery arity for an expression appearing in a
/// value-producing position (a SELECT-list item or an UPDATE `SET` value).
///
/// * A bare multi-column subquery is `sub-select returns N columns - expected 1`.
/// * A multi-column subquery on the **left** of a comparison against a plain
///   scalar is `row value misused`; on the **right** it is the arity error.
pub fn validate_value_expr(
    expr: &Expression,
    database: &vibesql_storage::Database,
) -> Result<(), ExecutorError> {
    match expr {
        Expression::ScalarSubquery(subquery) => {
            if let Some(n) = subquery_column_count(subquery, database) {
                if n > 1 {
                    return Err(arity_error(n));
                }
            }
            Ok(())
        }
        Expression::BinaryOp { left, op, right } if is_comparison_op(op) => {
            // Legal row-value comparison — leave to the evaluator / row_values.
            if is_row_value_or_subquery(left) && is_row_value_or_subquery(right) {
                return Ok(());
            }
            // `(SELECT a,b) <op> scalar` — subquery on the left of a value-
            // context comparison against a plain scalar is `row value misused`.
            if multi_col_subquery(left, database).is_some() && !is_row_value_or_subquery(right) {
                return Err(ExecutorError::RowValueMisused);
            }
            // `scalar <op> (SELECT a,b)` — subquery on the right is the arity
            // error.
            if !is_row_value_or_subquery(left) {
                if let Some(n) = multi_col_subquery(right, database) {
                    return Err(arity_error(n));
                }
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

/// Validate scalar-subquery arity for an expression appearing in a boolean
/// predicate position (UPDATE / DELETE `WHERE`).
///
/// A multi-column subquery compared against a plain scalar (column / literal)
/// is `sub-select returns N columns - expected 1`, regardless of which side it
/// is on. A subquery paired with a row value or another subquery is a legal
/// row-value comparison and is left untouched. `AND` / `OR` chains are walked so
/// each conjunct's top-level comparison is inspected.
pub fn validate_predicate_expr(
    expr: &Expression,
    database: &vibesql_storage::Database,
) -> Result<(), ExecutorError> {
    match expr {
        Expression::BinaryOp { left, op, right } if is_comparison_op(op) => {
            // Legal row-value comparison — leave to the evaluator / row_values.
            if is_row_value_or_subquery(left) && is_row_value_or_subquery(right) {
                return Ok(());
            }
            // A multi-column subquery compared against a plain scalar on either
            // side is the arity error in a WHERE predicate.
            if !is_row_value_or_subquery(left) {
                if let Some(n) = multi_col_subquery(right, database) {
                    return Err(arity_error(n));
                }
            }
            if !is_row_value_or_subquery(right) {
                if let Some(n) = multi_col_subquery(left, database) {
                    return Err(arity_error(n));
                }
            }
            Ok(())
        }
        // Walk boolean combinators so each conjunct/disjunct is inspected.
        Expression::Conjunction(children) | Expression::Disjunction(children) => {
            for c in children {
                validate_predicate_expr(c, database)?;
            }
            Ok(())
        }
        Expression::BinaryOp { left, op, right }
            if matches!(op, BinaryOperator::And | BinaryOperator::Or) =>
        {
            validate_predicate_expr(left, database)?;
            validate_predicate_expr(right, database)
        }
        Expression::UnaryOp { op: vibesql_ast::UnaryOperator::Not, expr: inner } => {
            validate_predicate_expr(inner, database)
        }
        _ => Ok(()),
    }
}
