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
        // Issue #5104 / #5135: when the scalar subquery (bare or FROM-bearing)
        // has a body whose aggregate references an outer column, override
        // `outer_rows` with the current group's rows so the inner aggregate
        // can iterate them (via the #4930 outer-correlated-aggregate path).
        // For other subqueries the parent evaluator's outer_rows are preserved.
        if scalar_subquery_needs_outer_rows(executor, expr) {
            let mut overridden = evaluator.clone_for_new_expression();
            overridden.set_outer_rows(group_rows);
            return overridden.eval(expr, row);
        }
        evaluator.eval(expr, row)
    } else if scalar_subquery_needs_outer_rows(executor, expr) {
        // Issue #5232 (window1.test 61.4.2): empty group, but the subquery
        // triggers implicit-outer-aggregate-collapse — the aggregation still
        // produces exactly one row, with the inner aggregate computed over
        // zero outer rows. Evaluate against an all-NULL stand-in row so
        // outer column references resolve to NULL while the inner aggregate
        // iterates the (empty) outer_rows.
        let mut overridden = evaluator.clone_for_new_expression();
        overridden.set_outer_rows(group_rows);
        let null_row = vibesql_storage::Row::new(vec![
            vibesql_types::SqlValue::Null;
            evaluator.get_schema().total_columns
        ]);
        overridden.eval(expr, &null_row)
    } else {
        Ok(vibesql_types::SqlValue::Null)
    }
}

/// Check whether `expr` is a scalar subquery (or a compound expression
/// containing one) that needs `outer_rows` set to the current group's rows
/// for SQLite's implicit-outer-aggregate-collapse semantics (#5104 / #5135).
///
/// Delegates the actual collapse test to
/// `SelectExecutor::subquery_triggers_outer_aggregate_collapse`, the same
/// scope-aware analysis used to decide whether the *outer* query collapses
/// to a single-row aggregate in the first place (detection.rs). This covers
/// both a bare (FROM-less) subquery, where any column reference is
/// necessarily outer, and a FROM-bearing subquery whose aggregate argument
/// resolves to a column outside its own FROM scope (filter1.test 6.3:
/// `SELECT (SELECT COUNT(a) FROM t2) FROM t1` — `a` is not a column of `t2`).
fn scalar_subquery_needs_outer_rows(
    executor: &SelectExecutor,
    expr: &vibesql_ast::Expression,
) -> bool {
    use vibesql_ast::Expression;

    match expr {
        Expression::ScalarSubquery(stmt) => {
            executor.subquery_triggers_outer_aggregate_collapse(stmt)
        }
        Expression::BinaryOp { left, right, .. } => {
            scalar_subquery_needs_outer_rows(executor, left)
                || scalar_subquery_needs_outer_rows(executor, right)
        }
        Expression::UnaryOp { expr, .. } => scalar_subquery_needs_outer_rows(executor, expr),
        Expression::Cast { expr, .. } => scalar_subquery_needs_outer_rows(executor, expr),
        Expression::IsNull { expr, .. } => scalar_subquery_needs_outer_rows(executor, expr),
        Expression::Function { args, .. } => {
            args.iter().any(|a| scalar_subquery_needs_outer_rows(executor, a))
        }
        Expression::Case { operand, when_clauses, else_result } => {
            operand.as_ref().is_some_and(|e| scalar_subquery_needs_outer_rows(executor, e))
                || when_clauses.iter().any(|w| {
                    w.conditions.iter().any(|c| scalar_subquery_needs_outer_rows(executor, c))
                        || scalar_subquery_needs_outer_rows(executor, &w.result)
                })
                || else_result
                    .as_ref()
                    .is_some_and(|e| scalar_subquery_needs_outer_rows(executor, e))
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
            if order_by.as_ref().is_some_and(|items| items.iter().any(|i| any_column_ref(&i.expr)))
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
        // Window function: the outer aggregate can hide inside the window's
        // args or its OVER (PARTITION BY / ORDER BY / frame), e.g.
        // `(SELECT count(a) OVER (ORDER BY sum(a)))` (window1.test 61.4.2).
        // Only *plain* aggregates found by the recursion count — the window
        // function itself (e.g. `total(a) OVER()`) does not. (#5232)
        Expression::WindowFunction { function, over, .. } => {
            let args = match function {
                vibesql_ast::WindowFunctionSpec::Aggregate { args, .. }
                | vibesql_ast::WindowFunctionSpec::Ranking { args, .. }
                | vibesql_ast::WindowFunctionSpec::Value { args, .. } => args,
            };
            args.iter().any(bare_subquery_inner_has_outer_aggregate)
                || over
                    .partition_by
                    .as_ref()
                    .is_some_and(|exprs| exprs.iter().any(bare_subquery_inner_has_outer_aggregate))
                || over.order_by.as_ref().is_some_and(|items| {
                    items.iter().any(|i| bare_subquery_inner_has_outer_aggregate(&i.expr))
                })
        }
        // Don't descend into nested subqueries (own scope).
        _ => false,
    }
}

/// Check whether an IN-subquery triggers SQLite's
/// implicit-outer-aggregate-collapse semantics (#5232, window1.test 44.x):
/// a bare (FROM-less) subquery whose SELECT list contains an aggregate
/// referencing an outer column, e.g.
/// `(0, 0) IN (SELECT MIN(c0), NTILE(1) OVER())`.
fn in_subquery_needs_outer_rows(subquery: &vibesql_ast::SelectStmt) -> bool {
    if subquery.from.is_some() {
        return false;
    }
    subquery.select_list.iter().any(|item| match item {
        vibesql_ast::SelectItem::Expression { expr: inner, .. } => {
            bare_subquery_inner_has_outer_aggregate(inner)
        }
        _ => false,
    })
}

/// Recursively check whether `expr` contains any column reference. Used inside
/// bare subqueries where any column ref is necessarily outer.
fn any_column_ref(expr: &vibesql_ast::Expression) -> bool {
    use vibesql_ast::Expression;

    match expr {
        Expression::ColumnRef(_) => true,
        Expression::BinaryOp { left, right, .. } => any_column_ref(left) || any_column_ref(right),
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
                || when_clauses
                    .iter()
                    .any(|w| w.conditions.iter().any(any_column_ref) || any_column_ref(&w.result))
                || else_result.as_ref().is_some_and(|e| any_column_ref(e))
        }
        Expression::Like { expr, pattern, .. } => any_column_ref(expr) || any_column_ref(pattern),
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

/// Normalize a value for comparison under a named collation.
///
/// NOCASE: uppercase strings for case-insensitive comparison.
/// RTRIM: trim trailing whitespace.
/// Any other (or no) collation leaves the value unchanged (BINARY semantics).
fn transform_for_collation(
    val: vibesql_types::SqlValue,
    collation: Option<&str>,
) -> vibesql_types::SqlValue {
    use vibesql_types::SqlValue;
    let Some(collation_name) = collation else {
        return val;
    };
    if collation_name.eq_ignore_ascii_case("nocase") {
        match val {
            SqlValue::Varchar(s) => SqlValue::Varchar(arcstr::ArcStr::from(s.to_uppercase())),
            SqlValue::Character(s) => SqlValue::Character(arcstr::ArcStr::from(s.to_uppercase())),
            other => other,
        }
    } else if collation_name.eq_ignore_ascii_case("rtrim") {
        match val {
            SqlValue::Varchar(s) => SqlValue::Varchar(arcstr::ArcStr::from(s.trim_end())),
            SqlValue::Character(s) => SqlValue::Character(arcstr::ArcStr::from(s.trim_end())),
            other => other,
        }
    } else {
        val
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

    // Issue #5232 (window1.test 44.x): when the IN-subquery is a bare
    // (FROM-less) subquery whose body has an aggregate referencing an outer
    // column, SQLite's implicit-outer-aggregate-collapse applies: the inner
    // aggregate is computed over the current group's rows. Delegate the
    // whole IN expression to the combined evaluator (which supports
    // row-value LHS and propagates outer context to the subquery) with
    // `outer_rows` overridden to the group rows — mirroring the scalar
    // subquery path above.
    if in_subquery_needs_outer_rows(subquery) {
        let mut overridden = evaluator.clone_for_new_expression();
        overridden.set_outer_rows(group_rows);

        let representative_row = if let Some(idx) = executor.get_aggregate_representative_row() {
            group_rows.get(idx)
        } else {
            group_rows.first()
        };

        return match representative_row {
            Some(row) => overridden.eval(expr, row),
            None => {
                // Empty group: the collapsed aggregation still produces one
                // row; evaluate against an all-NULL stand-in row so outer
                // column references resolve to NULL while inner aggregates
                // iterate the (empty) outer_rows.
                let null_row = vibesql_storage::Row::new(vec![
                    vibesql_types::SqlValue::Null;
                    evaluator.get_schema().total_columns
                ]);
                overridden.eval(expr, &null_row)
            }
        };
    }

    // Evaluate left-hand expression (which may be an aggregate)
    let left_val =
        executor.evaluate_with_aggregates(left_expr, group_rows, group_key, evaluator)?;

    // Effective collation of the LHS — explicit COLLATE clauses propagate
    // through single-argument aggregates (SQLite datatype3 §7.1), so e.g.
    // max(c1 COLLATE nocase) IN (SELECT 'aBCd') compares case-insensitively.
    let collation = evaluator.get_expression_collation(left_expr);
    let left_val = transform_for_collation(left_val, collation.as_deref());

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

        // Compare using equality (under the LHS collation, if any)
        if left_val == transform_for_collation(subquery_val.clone(), collation.as_deref()) {
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
