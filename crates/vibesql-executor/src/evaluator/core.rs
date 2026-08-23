//! Core expression evaluation utilities
//!
//! This module provides shared evaluation logic used by both ExpressionEvaluator
//! and CombinedExpressionEvaluator.

// Re-export the evaluator types for backwards compatibility
pub use super::{combined_core::CombinedExpressionEvaluator, single::ExpressionEvaluator};
use crate::errors::ExecutorError;

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

        // BLOB values compare byte-for-byte. Without this arm two identical
        // blobs fell through to the `_ => false` catch-all, so `X'ABCDEF' IS
        // X'ABCDEF'` wrongly reported "distinct" (e_expr-8.2.10.10.1 /
        // 8.2.11.11.1 / 8.2.12.12.1) even though `X'ABCDEF' = X'ABCDEF'` is 1.
        (Blob(a), Blob(b)) => a == b,

        // SQLite compatibility: Boolean/Integer comparisons.
        //
        // SQLite has no separate boolean storage class — TRUE is stored as the
        // integer 1 and FALSE as the integer 0. For binary `=` and binary `IS`
        // (IS NOT DISTINCT FROM) semantics, TRUE must therefore compare equal
        // only to the literal integer 1, not to any non-zero integer. The
        // truth-value operator `IS TRUE` (handled separately in
        // expressions/eval.rs) is the only place where non-zero integers are
        // treated as truthy — that operator must not be conflated with `=` or
        // binary `IS` here.
        //
        // Examples (binary IS / =):
        //   500 IS TRUE  -> false  (500 != 1)
        //   1   IS TRUE  -> true   (1 == 1)
        //   0   IS FALSE -> true   (0 == 0)
        //   2   IS TRUE  -> false  (2 != 1)
        (Boolean(b), Integer(i)) | (Integer(i), Boolean(b)) => {
            if *b {
                *i == 1
            } else {
                *i == 0
            }
        }
        (Boolean(b), Bigint(i)) | (Bigint(i), Boolean(b)) => {
            if *b {
                *i == 1
            } else {
                *i == 0
            }
        }
        (Boolean(b), Smallint(i)) | (Smallint(i), Boolean(b)) => {
            if *b {
                *i == 1
            } else {
                *i == 0
            }
        }

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

        // SQLite type affinity: TEXT vs NUMERIC comparison
        // When comparing a numeric value with a text value, try to parse the text as a number.
        // If parseable, compare numerically. This enables joins like TEXT '1.0' = INTEGER 1.
        (
            Integer(_) | Smallint(_) | Bigint(_) | Unsigned(_) | Numeric(_) | Float(_) | Real(_)
            | Double(_),
            Varchar(s) | Character(s),
        ) => {
            // Try to parse string as f64
            if let Ok(text_f64) = s.trim().parse::<f64>() {
                match crate::evaluator::casting::to_f64(left) {
                    Ok(num_f64) => (num_f64 - text_f64).abs() < f64::EPSILON,
                    _ => false,
                }
            } else {
                false // String doesn't parse as number
            }
        }

        // Symmetric case: TEXT on left, NUMERIC on right
        (
            Varchar(s) | Character(s),
            Integer(_) | Smallint(_) | Bigint(_) | Unsigned(_) | Numeric(_) | Float(_) | Real(_)
            | Double(_),
        ) => {
            // Try to parse string as f64
            if let Ok(text_f64) = s.trim().parse::<f64>() {
                match crate::evaluator::casting::to_f64(right) {
                    Ok(num_f64) => (text_f64 - num_f64).abs() < f64::EPSILON,
                    _ => false,
                }
            } else {
                false // String doesn't parse as number
            }
        }

        _ => false, // Type mismatch = not equal
    }
}

/// Compare two SQL values using IS DISTINCT FROM semantics (SQL:1999)
/// - NULL IS NOT DISTINCT FROM NULL is TRUE (both NULL = not distinct)
/// - NULL IS DISTINCT FROM non-NULL is TRUE (one NULL, one not = distinct)
/// - For non-NULL values, uses regular != comparison
pub(crate) fn values_are_distinct(
    left: &vibesql_types::SqlValue,
    right: &vibesql_types::SqlValue,
) -> bool {
    use vibesql_types::SqlValue::*;

    // IS DISTINCT FROM semantics (SQL:1999):
    // - Both NULL: NOT distinct (they are considered equal)
    // - One NULL, one not: distinct
    // - Both non-NULL: use normal inequality comparison
    match (left, right) {
        // Both NULL - not distinct
        (Null, Null) => false,

        // One NULL, one not - distinct
        (Null, _) | (_, Null) => true,

        // Both non-NULL - compare for inequality
        // Reuse the values_are_equal logic but invert it
        _ => !values_are_equal(left, right),
    }
}

#[cfg(test)]
mod blob_equality_tests {
    use vibesql_types::SqlValue;

    use super::{values_are_distinct, values_are_equal};

    // Regression for e_expr-8.2.10.10.1 / 8.2.11.11.1 / 8.2.12.12.1: identical
    // BLOB values must compare equal (and NOT distinct), matching `=`.
    #[test]
    fn identical_blobs_are_equal_and_not_distinct() {
        let a = SqlValue::Blob(vec![0xAB, 0xCD, 0xEF]);
        let b = SqlValue::Blob(vec![0xAB, 0xCD, 0xEF]);
        assert!(values_are_equal(&a, &b));
        assert!(!values_are_distinct(&a, &b));
    }

    #[test]
    fn empty_blobs_are_equal_and_not_distinct() {
        let a = SqlValue::Blob(vec![]);
        let b = SqlValue::Blob(vec![]);
        assert!(values_are_equal(&a, &b));
        assert!(!values_are_distinct(&a, &b));
    }

    #[test]
    fn differing_blobs_are_distinct() {
        let a = SqlValue::Blob(vec![0xAB]);
        let b = SqlValue::Blob(vec![0xCD]);
        assert!(!values_are_equal(&a, &b));
        assert!(values_are_distinct(&a, &b));
    }
}
