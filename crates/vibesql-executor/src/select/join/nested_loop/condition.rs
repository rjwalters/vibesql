//! Join condition analysis and evaluation helpers.
//!
//! This module provides utilities for analyzing join conditions to determine
//! optimization strategies and for evaluating join predicates.

use crate::{errors::ExecutorError, schema::CombinedSchema};

/// Optimized evaluation result for equijoin conditions
#[derive(Debug)]
pub enum EquijoinEvalStrategy {
    /// Simple equijoin - can evaluate by direct value comparison
    /// (left_col_idx, right_col_idx, evaluator for remaining conditions)
    Simple {
        left_col_idx: usize,
        right_col_idx: usize,
        remaining_condition: Option<Box<vibesql_ast::Expression>>,
    },
    /// Complex condition - need full evaluation with combined_row
    Complex,
}

/// Convert a SqlValue to boolean for JOIN condition evaluation.
///
/// SQLite allows any expression in JOIN ON clauses, treating non-zero values as true
/// and zero as false. This function implements that behavior for SQLite compatibility.
///
/// - Boolean(true) -> true
/// - Boolean(false) -> false
/// - Null -> false
/// - Integer(0) -> false
/// - Integer(non-zero) -> true
/// - Strings/BLOBs -> SQLite leading-numeric coercion (non-zero prefix is true)
pub fn eval_join_condition_to_bool(value: vibesql_types::SqlValue) -> Result<bool, ExecutorError> {
    match value {
        vibesql_types::SqlValue::Boolean(b) => Ok(b),
        vibesql_types::SqlValue::Null => Ok(false),
        // SQLite treats integer 0 as false, non-zero as true
        vibesql_types::SqlValue::Integer(i) => Ok(i != 0),
        // Float/string/blob and any remaining scalar types: delegate to the
        // shared helper so SQLite truthiness (leading-numeric coercion) applies
        // (#5830).
        ref other => Ok(crate::evaluator::operators::is_truthy(other)),
    }
}

/// Analyze join condition to determine optimization strategy
pub fn analyze_join_condition(
    condition: &vibesql_ast::Expression,
    schema: &CombinedSchema,
    left_col_count: usize,
) -> EquijoinEvalStrategy {
    use super::super::join_analyzer;

    // Try to detect a simple equijoin pattern
    if let Some(equi_info) = join_analyzer::analyze_equi_join(condition, schema, left_col_count) {
        // Simple equijoin detected - use optimized path
        return EquijoinEvalStrategy::Simple {
            left_col_idx: equi_info.left_col_idx,
            right_col_idx: equi_info.right_col_idx,
            remaining_condition: None,
        };
    }

    // Check if condition is an AND with at least one simple equijoin
    if let vibesql_ast::Expression::BinaryOp { op: vibesql_ast::BinaryOperator::And, left, right } =
        condition
    {
        // Try left side
        if let Some(equi_info) = join_analyzer::analyze_equi_join(left, schema, left_col_count) {
            return EquijoinEvalStrategy::Simple {
                left_col_idx: equi_info.left_col_idx,
                right_col_idx: equi_info.right_col_idx,
                remaining_condition: Some(Box::new(right.as_ref().clone())),
            };
        }
        // Try right side
        if let Some(equi_info) = join_analyzer::analyze_equi_join(right, schema, left_col_count) {
            return EquijoinEvalStrategy::Simple {
                left_col_idx: equi_info.left_col_idx,
                right_col_idx: equi_info.right_col_idx,
                remaining_condition: Some(Box::new(left.as_ref().clone())),
            };
        }
    }

    // Complex condition - fall back to classic algorithm
    EquijoinEvalStrategy::Complex
}
