//! Prepare-time validation of `IN (SELECT ...)` subquery column counts
//!
//! SQLite validates at prepare time that the subquery on the right-hand side
//! of an IN operator returns exactly as many columns as the left-hand side
//! expects (1 for a scalar LHS, N for a row-value LHS), producing
//! `sub-select returns N columns - expected M`.
//!
//! VibeSQL's expression evaluator performs the same check, but only when the
//! IN expression is actually evaluated against a row. When the outer table is
//! empty the expression is never evaluated and the malformed query silently
//! succeeds (window9.test 3.4). This module walks the statement's expressions
//! up front so the error is raised regardless of row count, like SQLite's
//! prepare step.

use vibesql_ast::{Expression, SelectItem, SelectStmt};

use crate::errors::ExecutorError;

/// Validate the column counts of all `IN (SELECT ...)` subqueries appearing in
/// this statement's SELECT list, WHERE, HAVING, and ORDER BY clauses.
///
/// Nested subquery *statements* (derived tables, scalar subqueries, the IN
/// subqueries themselves) are validated when they execute through
/// `SelectExecutor::execute`, so this walk only needs to cover the current
/// statement's own expression trees (plus set-operation arms, which do not
/// re-enter `execute`).
///
/// If the subquery's column count cannot be computed statically (e.g. a
/// wildcard over a CTE that has not been materialized yet), validation is
/// skipped and deferred to the runtime check in the expression evaluator.
pub fn validate_in_subquery_column_counts(
    stmt: &SelectStmt,
    database: &vibesql_storage::Database,
) -> Result<(), ExecutorError> {
    for item in &stmt.select_list {
        if let SelectItem::Expression { expr, .. } = item {
            validate_expr(expr, database)?;
        }
    }
    if let Some(where_clause) = &stmt.where_clause {
        validate_expr(where_clause, database)?;
    }
    if let Some(having) = &stmt.having {
        validate_expr(having, database)?;
    }
    if let Some(order_by) = stmt.order_by.as_deref() {
        for item in order_by {
            validate_expr(&item.expr, database)?;
        }
    }

    // Set-operation arms share this prepare step (they are executed as part of
    // this statement, not through a separate SelectExecutor::execute call).
    if let Some(set_op) = &stmt.set_operation {
        validate_in_subquery_column_counts(&set_op.right, database)?;
    }

    Ok(())
}

/// Recursively walk an expression tree looking for `Expression::In` nodes.
fn validate_expr(
    expr: &Expression,
    database: &vibesql_storage::Database,
) -> Result<(), ExecutorError> {
    match expr {
        Expression::In { expr: lhs, subquery, .. } => {
            // Row-value LHS expects one subquery column per LHS element; a
            // multi-column scalar-subquery LHS (`(SELECT a, b) IN (...)`)
            // expects its own column count; any other LHS is scalar and
            // expects exactly one column.
            let expected = match lhs.as_ref() {
                Expression::RowValueConstructor(items) => items.len(),
                Expression::ScalarSubquery(left_sub) => {
                    match crate::evaluator::compute_select_list_column_count(
                        left_sub, database, None,
                    ) {
                        Ok(n) => n,
                        // Cannot be determined statically — defer to runtime.
                        Err(_) => return validate_expr(lhs, database),
                    }
                }
                _ => 1,
            };

            // Compute the subquery's column count. If it cannot be determined
            // statically (e.g. wildcard over a not-yet-materialized CTE),
            // defer to the runtime check instead of failing the query.
            if let Ok(actual) =
                crate::evaluator::compute_select_list_column_count(subquery, database, None)
            {
                if actual != expected {
                    return Err(ExecutorError::SubqueryColumnCountMismatch { expected, actual });
                }
            }

            validate_expr(lhs, database)
        }

        Expression::BinaryOp { left, right, .. } => {
            validate_expr(left, database)?;
            validate_expr(right, database)
        }
        Expression::UnaryOp { expr: inner, .. }
        | Expression::Cast { expr: inner, .. }
        | Expression::Collate { expr: inner, .. }
        | Expression::IsNull { expr: inner, .. }
        | Expression::IsTruthValue { expr: inner, .. }
        | Expression::Extract { expr: inner, .. } => validate_expr(inner, database),
        Expression::IsDistinctFrom { left, right, .. } => {
            validate_expr(left, database)?;
            validate_expr(right, database)
        }
        Expression::Function { args, .. } => {
            for arg in args {
                validate_expr(arg, database)?;
            }
            Ok(())
        }
        Expression::AggregateFunction { args, filter, .. } => {
            for arg in args {
                validate_expr(arg, database)?;
            }
            if let Some(f) = filter {
                validate_expr(f, database)?;
            }
            Ok(())
        }
        Expression::WindowFunction { function, over } => {
            let args = match function {
                vibesql_ast::WindowFunctionSpec::Aggregate { args, .. }
                | vibesql_ast::WindowFunctionSpec::Ranking { args, .. }
                | vibesql_ast::WindowFunctionSpec::Value { args, .. } => args,
            };
            for arg in args {
                validate_expr(arg, database)?;
            }
            if let Some(partition_by) = &over.partition_by {
                for p in partition_by {
                    validate_expr(p, database)?;
                }
            }
            if let Some(order_by) = &over.order_by {
                for item in order_by {
                    validate_expr(&item.expr, database)?;
                }
            }
            Ok(())
        }
        Expression::Case { operand, when_clauses, else_result } => {
            if let Some(op) = operand {
                validate_expr(op, database)?;
            }
            for clause in when_clauses {
                for cond in &clause.conditions {
                    validate_expr(cond, database)?;
                }
                validate_expr(&clause.result, database)?;
            }
            if let Some(e) = else_result {
                validate_expr(e, database)?;
            }
            Ok(())
        }
        Expression::Between { expr: inner, low, high, .. } => {
            validate_expr(inner, database)?;
            validate_expr(low, database)?;
            validate_expr(high, database)
        }
        Expression::InList { expr: inner, values, .. } => {
            validate_expr(inner, database)?;
            for v in values {
                validate_expr(v, database)?;
            }
            Ok(())
        }
        Expression::Like { expr: inner, pattern, .. }
        | Expression::Glob { expr: inner, pattern, .. } => {
            validate_expr(inner, database)?;
            validate_expr(pattern, database)
        }
        Expression::Conjunction(children)
        | Expression::Disjunction(children)
        | Expression::RowValueConstructor(children) => {
            for c in children {
                validate_expr(c, database)?;
            }
            Ok(())
        }
        Expression::QuantifiedComparison { expr: inner, .. } => validate_expr(inner, database),

        // Leaf expressions and subquery statements (validated on their own
        // execution): nothing to do.
        _ => Ok(()),
    }
}
