//! Subquery evaluation in aggregate context (scalar, IN, quantified comparisons)

use super::super::super::builder::SelectExecutor;
use crate::{
    errors::ExecutorError,
    evaluator::{CombinedExpressionEvaluator, ExpressionEvaluator},
};

/// Evaluate scalar subqueries and EXISTS expressions in aggregate context
pub(super) fn evaluate_scalar(
    _executor: &SelectExecutor,
    expr: &vibesql_ast::Expression,
    group_rows: &[vibesql_storage::Row],
    evaluator: &CombinedExpressionEvaluator,
) -> Result<vibesql_types::SqlValue, ExecutorError> {
    // Use first row from group as context for subquery evaluation
    if let Some(first_row) = group_rows.first() {
        evaluator.eval(expr, first_row)
    } else {
        Ok(vibesql_types::SqlValue::Null)
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
