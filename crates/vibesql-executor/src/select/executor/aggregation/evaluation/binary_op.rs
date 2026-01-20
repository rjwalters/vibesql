//! Binary and unary operator evaluation in aggregate context

use super::super::super::builder::SelectExecutor;
use crate::{errors::ExecutorError, evaluator::CombinedExpressionEvaluator};

/// Evaluate binary operations in aggregate context
pub(super) fn evaluate_binary(
    executor: &SelectExecutor,
    left: &vibesql_ast::Expression,
    op: &vibesql_ast::BinaryOperator,
    right: &vibesql_ast::Expression,
    group_rows: &[vibesql_storage::Row],
    group_key: &[vibesql_types::SqlValue],
    evaluator: &CombinedExpressionEvaluator,
) -> Result<vibesql_types::SqlValue, ExecutorError> {
    let left_val = executor.evaluate_with_aggregates(left, group_rows, group_key, evaluator)?;
    let right_val = executor.evaluate_with_aggregates(right, group_rows, group_key, evaluator)?;

    // Apply SQLite type affinity rules before comparison.
    // The evaluator has the correct schema to determine column affinities.
    // Without this, TEXT columns compared to INTEGER columns would use strict
    // type ordering instead of affinity-based coercion.
    let (left_val, right_val) =
        evaluator.apply_affinity_for_comparison(left, left_val, right, right_val);

    // Use the static binary op evaluation
    let sql_mode = executor.database.sql_mode();
    crate::evaluator::ExpressionEvaluator::eval_binary_op_static(
        &left_val, op, &right_val, sql_mode,
    )
}

/// Evaluate unary operations in aggregate context
///
/// This is a helper function for evaluating unary operators (+, -, NOT) on values
/// that may result from aggregate functions like COUNT(*).
pub(super) fn evaluate_unary(
    executor: &SelectExecutor,
    op: &vibesql_ast::UnaryOperator,
    inner_expr: &vibesql_ast::Expression,
    group_rows: &[vibesql_storage::Row],
    group_key: &[vibesql_types::SqlValue],
    evaluator: &CombinedExpressionEvaluator,
) -> Result<vibesql_types::SqlValue, ExecutorError> {
    let val = executor.evaluate_with_aggregates(inner_expr, group_rows, group_key, evaluator)?;
    // Use shared eval_unary_op implementation
    crate::evaluator::eval_unary_op(op, &val)
}
