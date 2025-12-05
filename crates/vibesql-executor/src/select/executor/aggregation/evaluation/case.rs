//! CASE expression evaluation in aggregate context

use super::super::super::builder::SelectExecutor;
use crate::{
    errors::ExecutorError,
    evaluator::{CombinedExpressionEvaluator, ExpressionEvaluator},
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

                    // Use IS NOT DISTINCT FROM semantics (NULL = NULL is TRUE)
                    if ExpressionEvaluator::values_are_equal(&operand_value, &when_value) {
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

                    // Check if condition is TRUE (not FALSE or NULL)
                    let is_true = match condition_value {
                        vibesql_types::SqlValue::Boolean(true) => true,
                        vibesql_types::SqlValue::Boolean(false) | vibesql_types::SqlValue::Null => {
                            false
                        }
                        // SQLLogicTest compatibility: treat integers as truthy/falsy
                        vibesql_types::SqlValue::Integer(0) => false,
                        vibesql_types::SqlValue::Integer(_) => true,
                        vibesql_types::SqlValue::Smallint(0) => false,
                        vibesql_types::SqlValue::Smallint(_) => true,
                        vibesql_types::SqlValue::Bigint(0) => false,
                        vibesql_types::SqlValue::Bigint(_) => true,
                        vibesql_types::SqlValue::Float(0.0) => false,
                        vibesql_types::SqlValue::Float(_) => true,
                        vibesql_types::SqlValue::Real(0.0) => false,
                        vibesql_types::SqlValue::Real(_) => true,
                        vibesql_types::SqlValue::Double(0.0) => false,
                        vibesql_types::SqlValue::Double(_) => true,
                        other => {
                            return Err(ExecutorError::UnsupportedExpression(format!(
                                "CASE condition must evaluate to boolean, got: {:?}",
                                other
                            )))
                        }
                    };

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
