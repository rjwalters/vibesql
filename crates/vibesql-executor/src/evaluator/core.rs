//! Core expression evaluation utilities
//!
//! This module provides shared evaluation logic used by both ExpressionEvaluator
//! and CombinedExpressionEvaluator.

use crate::errors::ExecutorError;

// Re-export the evaluator types for backwards compatibility
pub use super::combined_core::CombinedExpressionEvaluator;
pub use super::single::ExpressionEvaluator;

/// Static version of eval_binary_op for shared logic
///
/// Delegates to the new trait-based operator registry for improved modularity.
pub(crate) fn eval_binary_op_static(
    left: &vibesql_types::SqlValue,
    op: &vibesql_ast::BinaryOperator,
    right: &vibesql_types::SqlValue,
    sql_mode: vibesql_types::SqlMode,
) -> Result<vibesql_types::SqlValue, ExecutorError> {
    super::operators::OperatorRegistry::eval_binary_op(left, op, right, sql_mode)
}

/// Static version of eval_between for constant folding during optimization
///
/// Evaluates BETWEEN predicate: expr BETWEEN low AND high
/// Handles SYMMETRIC and NOT BETWEEN variants with proper NULL semantics.
pub(crate) fn eval_between_static(
    expr_val: &vibesql_types::SqlValue,
    low_val: &vibesql_types::SqlValue,
    high_val: &vibesql_types::SqlValue,
    negated: bool,
    symmetric: bool,
    sql_mode: vibesql_types::SqlMode,
) -> Result<vibesql_types::SqlValue, ExecutorError> {
    let mut low = low_val.clone();
    let mut high = high_val.clone();

    // Check if bounds are reversed (low > high)
    let gt_result = eval_binary_op_static(
        &low,
        &vibesql_ast::BinaryOperator::GreaterThan,
        &high,
        sql_mode.clone(),
    )?;

    if let vibesql_types::SqlValue::Boolean(true) = gt_result {
        if symmetric {
            // For SYMMETRIC: swap bounds to normalize range
            std::mem::swap(&mut low, &mut high);
        } else {
            // For standard BETWEEN with reversed bounds: return empty set
            // However, if expr is NULL, we must preserve NULL semantics
            if matches!(expr_val, vibesql_types::SqlValue::Null) {
                return Ok(vibesql_types::SqlValue::Null);
            }
            return Ok(vibesql_types::SqlValue::Boolean(negated));
        }
    }

    // Check if expr >= low
    let ge_low = eval_binary_op_static(
        expr_val,
        &vibesql_ast::BinaryOperator::GreaterThanOrEqual,
        &low,
        sql_mode.clone(),
    )?;

    // Check if expr <= high
    let le_high = eval_binary_op_static(
        expr_val,
        &vibesql_ast::BinaryOperator::LessThanOrEqual,
        &high,
        sql_mode.clone(),
    )?;

    // Combine with AND/OR depending on negated
    if negated {
        // NOT BETWEEN: expr < low OR expr > high
        let lt_low = eval_binary_op_static(
            expr_val,
            &vibesql_ast::BinaryOperator::LessThan,
            &low,
            sql_mode.clone(),
        )?;
        let gt_high = eval_binary_op_static(
            expr_val,
            &vibesql_ast::BinaryOperator::GreaterThan,
            &high,
            sql_mode.clone(),
        )?;
        eval_binary_op_static(&lt_low, &vibesql_ast::BinaryOperator::Or, &gt_high, sql_mode)
    } else {
        // BETWEEN: expr >= low AND expr <= high
        eval_binary_op_static(&ge_low, &vibesql_ast::BinaryOperator::And, &le_high, sql_mode)
    }
}

/// Compare two SQL values for equality in simple CASE expressions
/// Uses regular = comparison semantics where NULL = anything is UNKNOWN (false)
pub(crate) fn values_are_equal(
    left: &vibesql_types::SqlValue,
    right: &vibesql_types::SqlValue,
) -> bool {
    use vibesql_types::SqlValue::*;

    // SQL standard semantics for simple CASE equality:
    // - Uses regular = comparison, not IS NOT DISTINCT FROM
    // - NULL = anything is UNKNOWN, so no match occurs
    // - CASE operand falls through to ELSE or returns NULL
    match (left, right) {
        // NULL never matches anything in simple CASE (including NULL)
        (Null, _) | (_, Null) => false,

        // Exact type matches
        (Integer(a), Integer(b)) => a == b,
        (Varchar(a), Varchar(b)) => a == b,
        (Character(a), Character(b)) => a == b,
        (Character(a), Varchar(b)) | (Varchar(a), Character(b)) => a == b,
        (Boolean(a), Boolean(b)) => a == b,

        // Numeric type comparisons - convert to f64 for comparison
        // This handles: Numeric, Integer, Smallint, Bigint, Unsigned, Float, Real, Double
        (
            Integer(_) | Smallint(_) | Bigint(_) | Unsigned(_) | Numeric(_) | Float(_) | Real(_)
            | Double(_),
            Integer(_) | Smallint(_) | Bigint(_) | Unsigned(_) | Numeric(_) | Float(_) | Real(_)
            | Double(_),
        ) => {
            // Convert both to f64 and compare
            match (
                crate::evaluator::casting::to_f64(left),
                crate::evaluator::casting::to_f64(right),
            ) {
                (Ok(a), Ok(b)) => (a - b).abs() < f64::EPSILON,
                _ => false,
            }
        }

        _ => false, // Type mismatch = not equal
    }
}
