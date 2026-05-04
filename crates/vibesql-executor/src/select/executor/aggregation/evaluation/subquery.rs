//! Subquery evaluation in aggregate context (scalar, IN, quantified comparisons)

use super::super::super::builder::SelectExecutor;
use crate::{
    errors::ExecutorError,
    evaluator::{CombinedExpressionEvaluator, ExpressionEvaluator},
};

/// Evaluate scalar subqueries and EXISTS expressions in aggregate context
///
/// This function handles SQLite's behavior where correlated subqueries in aggregate
/// SELECT lists use the row that corresponds to the aggregate result. For example,
/// in `SELECT max(a), (SELECT d FROM t2 WHERE a=c) FROM t1`, the subquery uses
/// `a` from the row where `a` has its maximum value, not just the first row.
///
/// See issue #4683 for details.
///
/// **Issue #5104 — implicit-outer-aggregate-collapse**: When the scalar
/// subquery is *bare* (no FROM) and its body contains an aggregate referencing
/// outer columns, SQLite collapses the outer query into a single-row aggregate
/// with the inner aggregate computed over all outer rows. We propagate
/// `group_rows` as `outer_rows` to the inner subquery's evaluator so the
/// existing #4930 inner-aggregate path can iterate them. Without this, the
/// inner aggregate would only see the representative row (1 row → wrong avg).
pub(super) fn evaluate_scalar(
    executor: &SelectExecutor,
    expr: &vibesql_ast::Expression,
    group_rows: &[vibesql_storage::Row],
    evaluator: &CombinedExpressionEvaluator,
) -> Result<vibesql_types::SqlValue, ExecutorError> {
    // Use the representative row for aggregate-context subquery evaluation (issue #4683)
    // The representative row is the row that corresponds to MAX/MIN aggregate result.
    // If no representative row was set (no MAX/MIN aggregate), fall back to first row.
    let representative_row = if let Some(idx) = executor.get_aggregate_representative_row() {
        group_rows.get(idx)
    } else {
        group_rows.first()
    };

    if let Some(row) = representative_row {
        // Issue #5104: when the subquery is a bare scalar subquery whose body
        // has an aggregate referencing an outer column, override `outer_rows`
        // with the current group's rows so the inner aggregate can iterate
        // them (via the #4930 outer-correlated-aggregate path). For other
        // subqueries the parent evaluator's outer_rows are preserved.
        if scalar_subquery_needs_outer_rows(expr) {
            let mut overridden = evaluator.clone_for_new_expression();
            overridden.set_outer_rows(group_rows);
            return overridden.eval(expr, row);
        }
        evaluator.eval(expr, row)
    } else {
        Ok(vibesql_types::SqlValue::Null)
    }
}

/// Check whether `expr` is a scalar subquery (or a compound expression
/// containing one) that needs `outer_rows` set to the current group's rows
/// for SQLite's implicit-outer-aggregate-collapse semantics (#5104).
///
/// The pattern: a bare (FROM-less) scalar subquery whose body contains an
/// aggregate function whose argument / FILTER / ORDER BY references an outer
/// column. Inside a bare subquery, *any* column reference is necessarily an
/// outer reference, so we just check for any column ref in the aggregate.
fn scalar_subquery_needs_outer_rows(expr: &vibesql_ast::Expression) -> bool {
    use vibesql_ast::Expression;

    match expr {
        Expression::ScalarSubquery(stmt) => {
            if stmt.from.is_some() {
                return false;
            }
            stmt.select_list.iter().any(|item| match item {
                vibesql_ast::SelectItem::Expression { expr: inner, .. } => {
                    bare_subquery_inner_has_outer_aggregate(inner)
                }
                _ => false,
            })
        }
        Expression::BinaryOp { left, right, .. } => {
            scalar_subquery_needs_outer_rows(left)
                || scalar_subquery_needs_outer_rows(right)
        }
        Expression::UnaryOp { expr, .. } => scalar_subquery_needs_outer_rows(expr),
        Expression::Cast { expr, .. } => scalar_subquery_needs_outer_rows(expr),
        Expression::IsNull { expr, .. } => scalar_subquery_needs_outer_rows(expr),
        Expression::Function { args, .. } => args.iter().any(scalar_subquery_needs_outer_rows),
        Expression::Case { operand, when_clauses, else_result } => {
            operand.as_ref().is_some_and(|e| scalar_subquery_needs_outer_rows(e))
                || when_clauses.iter().any(|w| {
                    w.conditions.iter().any(scalar_subquery_needs_outer_rows)
                        || scalar_subquery_needs_outer_rows(&w.result)
                })
                || else_result.as_ref().is_some_and(|e| scalar_subquery_needs_outer_rows(e))
        }
        _ => false,
    }
}

/// Inside a bare scalar subquery, check whether `expr` contains an aggregate
/// whose args/filter/order_by reference any column (which is necessarily an
/// outer reference — bare subqueries have no inner columns).
fn bare_subquery_inner_has_outer_aggregate(expr: &vibesql_ast::Expression) -> bool {
    use vibesql_ast::Expression;

    match expr {
        Expression::AggregateFunction { args, filter, order_by, .. } => {
            if args.iter().any(any_column_ref) {
                return true;
            }
            if filter.as_ref().is_some_and(|f| any_column_ref(f)) {
                return true;
            }
            if order_by
                .as_ref()
                .is_some_and(|items| items.iter().any(|i| any_column_ref(&i.expr)))
            {
                return true;
            }
            args.iter().any(bare_subquery_inner_has_outer_aggregate)
        }
        Expression::Function { name, args, .. } => {
            // Old Function variant for aggregate names
            let upper = name.to_uppercase();
            let is_agg = matches!(
                upper.as_str(),
                "COUNT" | "SUM" | "AVG" | "TOTAL" | "MIN" | "MAX" | "GROUP_CONCAT" | "STRING_AGG"
            );
            let is_scalar_minmax = matches!(upper.as_str(), "MIN" | "MAX") && args.len() > 1;
            if is_agg && !is_scalar_minmax && args.iter().any(any_column_ref) {
                return true;
            }
            args.iter().any(bare_subquery_inner_has_outer_aggregate)
        }
        Expression::BinaryOp { left, right, .. } => {
            bare_subquery_inner_has_outer_aggregate(left)
                || bare_subquery_inner_has_outer_aggregate(right)
        }
        Expression::UnaryOp { expr, .. } => bare_subquery_inner_has_outer_aggregate(expr),
        Expression::Cast { expr, .. } => bare_subquery_inner_has_outer_aggregate(expr),
        Expression::IsNull { expr, .. } => bare_subquery_inner_has_outer_aggregate(expr),
        Expression::Case { operand, when_clauses, else_result } => {
            operand.as_ref().is_some_and(|e| bare_subquery_inner_has_outer_aggregate(e))
                || when_clauses.iter().any(|w| {
                    w.conditions.iter().any(bare_subquery_inner_has_outer_aggregate)
                        || bare_subquery_inner_has_outer_aggregate(&w.result)
                })
                || else_result.as_ref().is_some_and(|e| bare_subquery_inner_has_outer_aggregate(e))
        }
        // Don't descend into nested subqueries / window functions (own scope).
        _ => false,
    }
}

/// Recursively check whether `expr` contains any column reference. Used inside
/// bare subqueries where any column ref is necessarily outer.
fn any_column_ref(expr: &vibesql_ast::Expression) -> bool {
    use vibesql_ast::Expression;

    match expr {
        Expression::ColumnRef(_) => true,
        Expression::BinaryOp { left, right, .. } => {
            any_column_ref(left) || any_column_ref(right)
        }
        Expression::UnaryOp { expr, .. } => any_column_ref(expr),
        Expression::Cast { expr, .. } => any_column_ref(expr),
        Expression::IsNull { expr, .. } => any_column_ref(expr),
        Expression::Function { args, .. } => args.iter().any(any_column_ref),
        Expression::AggregateFunction { args, filter, order_by, .. } => {
            args.iter().any(any_column_ref)
                || filter.as_ref().is_some_and(|f| any_column_ref(f))
                || order_by
                    .as_ref()
                    .is_some_and(|items| items.iter().any(|i| any_column_ref(&i.expr)))
        }
        Expression::Case { operand, when_clauses, else_result } => {
            operand.as_ref().is_some_and(|e| any_column_ref(e))
                || when_clauses.iter().any(|w| {
                    w.conditions.iter().any(any_column_ref)
                        || any_column_ref(&w.result)
                })
                || else_result.as_ref().is_some_and(|e| any_column_ref(e))
        }
        Expression::Like { expr, pattern, .. } => {
            any_column_ref(expr) || any_column_ref(pattern)
        }
        Expression::Between { expr, low, high, .. } => {
            any_column_ref(expr) || any_column_ref(low) || any_column_ref(high)
        }
        Expression::InList { expr, values, .. } => {
            any_column_ref(expr) || values.iter().any(any_column_ref)
        }
        // Don't descend into nested subqueries or window functions (own scope).
        _ => false,
    }
}

/// Evaluate IN predicate with subquery in aggregate context
#[allow(clippy::too_many_arguments)]
pub(super) fn evaluate_in(
    executor: &SelectExecutor,
    expr: &vibesql_ast::Expression,
    group_rows: &[vibesql_storage::Row],
    group_key: &[vibesql_types::SqlValue],
    evaluator: &CombinedExpressionEvaluator,
) -> Result<vibesql_types::SqlValue, ExecutorError> {
    let (left_expr, subquery, negated) = match expr {
        vibesql_ast::Expression::In { expr: left_expr, subquery, negated } => {
            (left_expr, subquery, *negated)
        }
        _ => unreachable!("evaluate_in called with non-IN expression"),
    };

    // Evaluate left-hand expression (which may be an aggregate)
    let left_val =
        executor.evaluate_with_aggregates(left_expr, group_rows, group_key, evaluator)?;

    // Execute subquery to get values to compare against
    let database = executor.database;
    let select_executor = crate::select::SelectExecutor::new(database);
    let rows = select_executor.execute(subquery)?;

    // Check subquery column count
    if subquery.select_list.len() != 1 {
        return Err(ExecutorError::SubqueryColumnCountMismatch {
            expected: 1,
            actual: subquery.select_list.len(),
        });
    }

    // If left value is NULL, result is NULL
    if matches!(left_val, vibesql_types::SqlValue::Null) {
        return Ok(vibesql_types::SqlValue::Null);
    }

    let mut found_null = false;

    // Check each row from subquery
    for subquery_row in &rows {
        let subquery_val =
            subquery_row.get(0).ok_or(ExecutorError::ColumnIndexOutOfBounds { index: 0 })?;

        // Track if we encounter NULL
        if matches!(subquery_val, vibesql_types::SqlValue::Null) {
            found_null = true;
            continue;
        }

        // Compare using equality
        if left_val == *subquery_val {
            return Ok(vibesql_types::SqlValue::Boolean(!negated));
        }
    }

    // No match found
    if found_null {
        Ok(vibesql_types::SqlValue::Null)
    } else {
        Ok(vibesql_types::SqlValue::Boolean(negated))
    }
}

/// Evaluate quantified comparison (ALL/ANY/SOME) with subquery in aggregate context
#[allow(clippy::too_many_arguments)]
pub(super) fn evaluate_quantified(
    executor: &SelectExecutor,
    expr: &vibesql_ast::Expression,
    group_rows: &[vibesql_storage::Row],
    group_key: &[vibesql_types::SqlValue],
    evaluator: &CombinedExpressionEvaluator,
) -> Result<vibesql_types::SqlValue, ExecutorError> {
    let (left_expr, op, quantifier, subquery) = match expr {
        vibesql_ast::Expression::QuantifiedComparison {
            expr: left_expr,
            op,
            quantifier,
            subquery,
        } => (left_expr, op, quantifier, subquery),
        _ => unreachable!("evaluate_quantified called with non-quantified expression"),
    };

    // Evaluate left-hand expression (which may be an aggregate)
    let left_val =
        executor.evaluate_with_aggregates(left_expr, group_rows, group_key, evaluator)?;

    // Execute subquery
    let database = executor.database;
    let select_executor = crate::select::SelectExecutor::new(database);
    let rows = select_executor.execute(subquery)?;

    // Empty subquery special cases
    if rows.is_empty() {
        return Ok(vibesql_types::SqlValue::Boolean(matches!(
            quantifier,
            vibesql_ast::Quantifier::All
        )));
    }

    // If left value is NULL, return NULL
    if matches!(left_val, vibesql_types::SqlValue::Null) {
        return Ok(vibesql_types::SqlValue::Null);
    }

    let mut has_null = false;

    match quantifier {
        vibesql_ast::Quantifier::All => {
            for subquery_row in &rows {
                if subquery_row.values.len() != 1 {
                    return Err(ExecutorError::SubqueryColumnCountMismatch {
                        expected: 1,
                        actual: subquery_row.values.len(),
                    });
                }

                let right_val = &subquery_row.values[0];

                if matches!(right_val, vibesql_types::SqlValue::Null) {
                    has_null = true;
                    continue;
                }

                // Create temp evaluator for comparison
                let temp_schema = vibesql_catalog::TableSchema::new("temp".to_string(), vec![]);
                let temp_evaluator =
                    ExpressionEvaluator::with_database(&temp_schema, executor.database);
                let cmp_result = temp_evaluator.eval_binary_op(&left_val, op, right_val)?;

                match cmp_result {
                    vibesql_types::SqlValue::Boolean(false) => {
                        return Ok(vibesql_types::SqlValue::Boolean(false))
                    }
                    vibesql_types::SqlValue::Null => has_null = true,
                    _ => {}
                }
            }

            if has_null {
                Ok(vibesql_types::SqlValue::Null)
            } else {
                Ok(vibesql_types::SqlValue::Boolean(true))
            }
        }

        vibesql_ast::Quantifier::Any | vibesql_ast::Quantifier::Some => {
            for subquery_row in &rows {
                if subquery_row.values.len() != 1 {
                    return Err(ExecutorError::SubqueryColumnCountMismatch {
                        expected: 1,
                        actual: subquery_row.values.len(),
                    });
                }

                let right_val = &subquery_row.values[0];

                if matches!(right_val, vibesql_types::SqlValue::Null) {
                    has_null = true;
                    continue;
                }

                // Create temp evaluator for comparison
                let temp_schema = vibesql_catalog::TableSchema::new("temp".to_string(), vec![]);
                let temp_evaluator =
                    ExpressionEvaluator::with_database(&temp_schema, executor.database);
                let cmp_result = temp_evaluator.eval_binary_op(&left_val, op, right_val)?;

                match cmp_result {
                    vibesql_types::SqlValue::Boolean(true) => {
                        return Ok(vibesql_types::SqlValue::Boolean(true))
                    }
                    vibesql_types::SqlValue::Null => has_null = true,
                    _ => {}
                }
            }

            if has_null {
                Ok(vibesql_types::SqlValue::Null)
            } else {
                Ok(vibesql_types::SqlValue::Boolean(false))
            }
        }
    }
}
