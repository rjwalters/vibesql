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

/// Static arity of an `IN` left-hand side, if it can be determined: a
/// multi-element `RowValueConstructor` has that arity, a scalar subquery has its
/// projected column count (a multi-column subquery LHS is a legal row value —
/// e.g. `(SELECT a, b) IN (SELECT a, b FROM t)`), and any other expression is a
/// scalar (arity 1). Returns `None` when a subquery's column count cannot be
/// computed statically, in which case the arity check is skipped in favor of the
/// runtime check.
fn in_lhs_arity(expr: &Expression, database: &vibesql_storage::Database) -> Option<usize> {
    match expr {
        Expression::RowValueConstructor(elems) if elems.len() > 1 => Some(elems.len()),
        Expression::ScalarSubquery(subquery) => subquery_column_count(subquery, database),
        _ => Some(1),
    }
}

/// Validate a `WHERE` predicate of a top-level `SELECT`.
///
/// SQLite's SELECT-WHERE handling differs from the UPDATE/DELETE WHERE rule
/// covered by [`validate_predicate_expr`]: a scalar compared against a
/// multi-column subquery in a SELECT WHERE is reported as `row value misused`,
/// whereas the same shape in an UPDATE/DELETE WHERE is the arity error
/// `sub-select returns N columns - expected 1`. (Verified against sqlite3 3.51:
/// `SELECT * FROM t WHERE a < (SELECT b, 2)` → `row value misused`, while the
/// UPDATE/DELETE forms give the arity error — see `rowvalue4.test` 8.2 and
/// `rowvalue.test` 15.3/15.4.)
///
/// The check is a prepare-time walk so the misuse surfaces even when the target
/// table is empty and the WHERE predicate is never evaluated. In addition to
/// the top-level scalar-vs-subquery shape, the walk descends into nested
/// scalar-subquery bodies so a deep arity misuse such as
/// `(a,b) > (SELECT 2 IN (SELECT 2,2), 2)` (`rowvalue9.test` 8.2, where the
/// inner `2 IN (SELECT 2,2)` compares a scalar against a 2-column subquery) is
/// caught regardless of row count.
pub fn validate_select_where_expr(
    expr: &Expression,
    database: &vibesql_storage::Database,
) -> Result<(), ExecutorError> {
    match expr {
        Expression::BinaryOp { left, op, right } if is_comparison_op(op) => {
            // Legal row-value comparison (row value / subquery on both sides):
            // the arities are checked elsewhere; still descend into each operand
            // so a nested subquery misuse is not missed.
            if is_row_value_or_subquery(left) && is_row_value_or_subquery(right) {
                validate_select_where_descend(left, database)?;
                validate_select_where_descend(right, database)?;
                return Ok(());
            }
            // A plain scalar compared against a multi-column subquery in a
            // SELECT WHERE is `row value misused` (not the arity error).
            if !is_row_value_or_subquery(left) && multi_col_subquery(right, database).is_some() {
                return Err(ExecutorError::RowValueMisused);
            }
            if !is_row_value_or_subquery(right) && multi_col_subquery(left, database).is_some() {
                return Err(ExecutorError::RowValueMisused);
            }
            // Otherwise descend into operands for nested subquery misuse.
            validate_select_where_descend(left, database)?;
            validate_select_where_descend(right, database)
        }
        Expression::Conjunction(children) | Expression::Disjunction(children) => {
            for c in children {
                validate_select_where_expr(c, database)?;
            }
            Ok(())
        }
        Expression::BinaryOp { left, right, .. } => {
            validate_select_where_expr(left, database)?;
            validate_select_where_expr(right, database)
        }
        Expression::UnaryOp { expr: inner, .. } => validate_select_where_expr(inner, database),
        other => validate_select_where_descend(other, database),
    }
}

/// Descend into an expression's nested subqueries looking for a scalar
/// `IN (multi-column subquery)` misuse (arity error). This is the shape that
/// escapes the top-level comparison check because it lives inside a subquery's
/// projection (e.g. `(SELECT 2 IN (SELECT 2,2), 2)`), which is never evaluated
/// when the outer table is empty.
fn validate_select_where_descend(
    expr: &Expression,
    database: &vibesql_storage::Database,
) -> Result<(), ExecutorError> {
    match expr {
        // `scalar IN (SELECT c1, c2, ...)` where the subquery returns more than
        // one column is the arity error, independent of row count.
        //
        // The LHS may itself be a row value: a multi-element
        // `RowValueConstructor`, or a *subquery* that projects multiple columns
        // (`(SELECT a, b) IN (SELECT a, b FROM t)` is a legal row-value IN — see
        // rowvalue.test 18.1/18.2/18.4/18.5). Only flag when the LHS is a genuine
        // scalar (arity 1) and the IN subquery returns more than one column.
        Expression::In { expr: lhs, subquery, .. } => {
            if in_lhs_arity(lhs, database) == Some(1) {
                if let Some(n) = subquery_column_count(subquery, database) {
                    if n > 1 {
                        return Err(arity_error(n));
                    }
                }
            }
            validate_select_where_descend(lhs, database)?;
            validate_select_stmt(subquery, database)
        }
        Expression::ScalarSubquery(subquery) => validate_select_stmt(subquery, database),
        Expression::BinaryOp { left, right, .. } => {
            validate_select_where_descend(left, database)?;
            validate_select_where_descend(right, database)
        }
        Expression::UnaryOp { expr: inner, .. } => validate_select_where_descend(inner, database),
        Expression::Conjunction(children)
        | Expression::Disjunction(children)
        | Expression::RowValueConstructor(children) => {
            for c in children {
                validate_select_where_descend(c, database)?;
            }
            Ok(())
        }
        Expression::InList { expr: lhs, values, .. } => {
            validate_select_where_descend(lhs, database)?;
            for v in values {
                validate_select_where_descend(v, database)?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

/// Walk a nested `SELECT` body for scalar-subquery arity misuse: each
/// projected item and its `WHERE` predicate are validated the same way the
/// top-level SELECT is, so a misuse buried in a subquery's projection surfaces
/// even when no row ever reaches it.
fn validate_select_stmt(
    stmt: &vibesql_ast::SelectStmt,
    database: &vibesql_storage::Database,
) -> Result<(), ExecutorError> {
    for item in &stmt.select_list {
        if let vibesql_ast::SelectItem::Expression { expr, .. } = item {
            validate_select_where_descend(expr, database)?;
        }
    }
    if let Some(where_expr) = &stmt.where_clause {
        validate_select_where_descend(where_expr, database)?;
    }
    Ok(())
}
