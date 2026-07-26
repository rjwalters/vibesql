//! CASE expression evaluation in aggregate context

use super::super::super::builder::SelectExecutor;
use crate::{
    errors::ExecutorError,
    evaluator::CombinedExpressionEvaluator,
};

/// Evaluate CASE expression with potential aggregates in operand/conditions/results
///
/// Handles both:
/// - Simple CASE: CASE COUNT(*) WHEN 5 THEN 'five' END
/// - Searched CASE: CASE WHEN COUNT(*) > 5 THEN 'many' END
pub(super) fn evaluate(
    executor: &SelectExecutor,
    operand: &Option<Box<vibesql_ast::Expression>>,
    when_clauses: &[vibesql_ast::CaseWhen],
    else_result: &Option<Box<vibesql_ast::Expression>>,
    group_rows: &[vibesql_storage::Row],
    group_key: &[vibesql_types::SqlValue],
    evaluator: &CombinedExpressionEvaluator,
) -> Result<vibesql_types::SqlValue, ExecutorError> {
    match operand {
        // Simple CASE: CASE operand WHEN value THEN result ...
        Some(operand_expr) => {
            // Evaluate operand (may contain aggregates like COUNT(*))
            let operand_value = executor.evaluate_with_aggregates(
                operand_expr,
                group_rows,
                group_key,
                evaluator,
            )?;

            for when_clause in when_clauses {
                // Check if ANY condition matches (OR logic)
                for condition_expr in &when_clause.conditions {
                    // Evaluate condition (may contain aggregates)
                    let when_value = executor.evaluate_with_aggregates(
                        condition_expr,
                        group_rows,
                        group_key,
                        evaluator,
                    )?;

                    // Affinity-aware equality (not the permissive
                    // `values_are_equal` used for hash-join keys): a bare
                    // literal operand carries no affinity, so `CASE COUNT(*)
                    // WHEN '2' THEN ...` must not match an INTEGER 2 against
                    // TEXT '2' (e_expr-23.1.6, same fix as the main
                    // evaluators). The operand/condition expressions are
                    // threaded through here, so route through the shared
                    // affinity-aware comparator on the combined evaluator.
                    if evaluator.affinity_aware_equal(
                        operand_expr,
                        operand_value.clone(),
                        condition_expr,
                        when_value,
                    )? {
                        // Evaluate result (may contain aggregates)
                        return executor.evaluate_with_aggregates(
                            &when_clause.result,
                            group_rows,
                            group_key,
                            evaluator,
                        );
                    }
                }
            }

            // No match - evaluate ELSE clause if present
            if let Some(else_expr) = else_result {
                executor.evaluate_with_aggregates(else_expr, group_rows, group_key, evaluator)
            } else {
                Ok(vibesql_types::SqlValue::Null)
            }
        }

        // Searched CASE: CASE WHEN condition THEN result ...
        None => {
            for when_clause in when_clauses {
                // Each when_clause can have multiple conditions (OR logic within a clause)
                for condition_expr in &when_clause.conditions {
                    // Evaluate condition (may contain aggregates)
                    let condition_value = executor.evaluate_with_aggregates(
                        condition_expr,
                        group_rows,
                        group_key,
                        evaluator,
                    )?;

                    // Check if condition is truthy. Delegate to the shared
                    // SQLite truthiness helper so any expression is accepted
                    // (strings/blobs coerce via the leading-numeric parse)
                    // instead of erroring on non-boolean conditions — this
                    // was the last grouped-aggregation path rejecting
                    // non-boolean expressions (#5856).
                    let is_true = crate::evaluator::operators::is_truthy(&condition_value);

                    if is_true {
                        // Evaluate result (may contain aggregates)
                        return executor.evaluate_with_aggregates(
                            &when_clause.result,
                            group_rows,
                            group_key,
                            evaluator,
                        );
                    }
                }
            }

            // No match - evaluate ELSE clause if present
            if let Some(else_expr) = else_result {
                executor.evaluate_with_aggregates(else_expr, group_rows, group_key, evaluator)
            } else {
                Ok(vibesql_types::SqlValue::Null)
            }
        }
    }
}
