//! SIMD-accelerated expression evaluation for general arithmetic expressions
//!
//! This module extends SIMD support beyond aggregates to general expression evaluation
//! in WHERE clauses, SELECT projections, ORDER BY, and other query contexts.
//!
//! # Overview
//!
//! Evaluates expressions in batch mode using SIMD operations when beneficial:
//! - Collects values from multiple rows into columnar buffers
//! - Applies SIMD arithmetic operations
//! - Converts results back to row-based format
//!
//! # When SIMD is Used
//!
//! SIMD path is chosen when:
//! 1. Row count >= SIMD_THRESHOLD (100 rows)
//! 2. Expression is simple arithmetic (+, -, *, /)
//! 3. All operands are numeric (Int64 or Float64)
//! 4. No complex sub-expressions (subqueries, aggregates, etc.)
//!
//! # Performance
//!
//! Expected improvements:
//! - 2-4x for expression-heavy queries (conservative)
//! - 5-10x for computation-dominated queries (optimistic)
//!
//! Overhead considerations:
//! - Row → columnar → row conversion has cost
//! - Only beneficial when computation cost exceeds conversion cost
//! - Threshold tuned to break-even point (~100-1000 rows)

use crate::errors::ExecutorError;
use vibesql_ast::{BinaryOperator, Expression};
use vibesql_types::SqlValue;

/// Threshold for using SIMD expression evaluation
/// Below this, scalar evaluation is more efficient due to conversion overhead
pub const SIMD_THRESHOLD: usize = 100;

/// Maximum recursion depth for nested expressions
/// Prevents stack overflow on deeply nested binary operations
const MAX_RECURSION_DEPTH: usize = 32;

/// Check if an expression can benefit from SIMD evaluation
///
/// Returns true if:
/// - Expression is simple binary arithmetic (+, -, *, /)
/// - Operands are column references or literals (no complex sub-expressions)
/// - No subqueries, aggregates, or other complex operations
#[cfg(feature = "simd")]
pub fn can_use_simd_for_expression(expr: &Expression, row_count: usize) -> bool {
    // Must have enough rows to amortize conversion overhead
    if row_count < SIMD_THRESHOLD {
        return false;
    }

    match expr {
        Expression::BinaryOp { left, op, right } => {
            // Check if operator is SIMD-compatible
            let is_simd_op = matches!(
                op,
                BinaryOperator::Plus
                    | BinaryOperator::Minus
                    | BinaryOperator::Multiply
                    | BinaryOperator::Divide
            );

            if !is_simd_op {
                return false;
            }

            // Check if operands are simple (column refs or literals)
            is_simple_operand(left) && is_simple_operand(right)
        }
        _ => false,
    }
}

/// Check if an expression is a simple operand (column reference or literal)
#[cfg(feature = "simd")]
fn is_simple_operand(expr: &Expression) -> bool {
    match expr {
        // Column references are simple
        Expression::ColumnRef { .. } => true,

        // Literals are simple
        Expression::Literal(_) => true,

        // Nested binary ops are simple if their operands are simple
        Expression::BinaryOp { left, op, right } => {
            matches!(
                op,
                BinaryOperator::Plus
                    | BinaryOperator::Minus
                    | BinaryOperator::Multiply
                    | BinaryOperator::Divide
            ) && is_simple_operand(left)
                && is_simple_operand(right)
        }

        // Everything else is complex (subqueries, aggregates, functions, etc.)
        _ => false,
    }
}

/// Evaluate an expression in batch mode using SIMD
///
/// # Arguments
///
/// * `expr` - Expression to evaluate
/// * `rows` - Input rows to evaluate against
/// * `evaluator` - Expression evaluator for column lookups and fallback
///
/// # Returns
///
/// Vector of SqlValues, one per row, or an error
///
/// # Algorithm
///
/// 1. Analyze expression to determine column dependencies
/// 2. Extract column values into typed buffers (i64 or f64)
/// 3. Apply SIMD arithmetic operations
/// 4. Convert results back to SqlValues
#[cfg(feature = "simd")]
pub fn eval_expression_batch_simd(
    expr: &Expression,
    rows: &[vibesql_storage::Row],
    evaluator: &crate::evaluator::CombinedExpressionEvaluator,
) -> Result<Vec<SqlValue>, ExecutorError> {
    // Early return for empty input
    if rows.is_empty() {
        return Ok(Vec::new());
    }

    // Check if we should use SIMD
    if !can_use_simd_for_expression(expr, rows.len()) {
        // Fall back to scalar evaluation
        return eval_expression_scalar(expr, rows, evaluator);
    }

    // Evaluate expression using SIMD with depth tracking
    match expr {
        Expression::BinaryOp { left, op, right } => {
            eval_binary_op_simd(left, *op, right, rows, evaluator, 0)
        }
        _ => eval_expression_scalar(expr, rows, evaluator),
    }
}

/// Evaluate a binary operation using SIMD
#[cfg(feature = "simd")]
fn eval_binary_op_simd(
    left: &Expression,
    op: BinaryOperator,
    right: &Expression,
    rows: &[vibesql_storage::Row],
    evaluator: &crate::evaluator::CombinedExpressionEvaluator,
    depth: usize,
) -> Result<Vec<SqlValue>, ExecutorError> {
    // Check recursion depth to prevent stack overflow
    if depth >= MAX_RECURSION_DEPTH {
        // Fall back to scalar evaluation for deeply nested expressions
        return eval_expression_scalar(
            &Expression::BinaryOp {
                left: Box::new(left.clone()),
                op,
                right: Box::new(right.clone()),
            },
            rows,
            evaluator,
        );
    }

    // Evaluate left and right operands
    let left_values = eval_operand_to_buffer(left, rows, evaluator, depth)?;
    let right_values = eval_operand_to_buffer(right, rows, evaluator, depth)?;

    // Determine result type and perform SIMD operation
    apply_simd_operation(&left_values, op, &right_values)
}

/// Evaluate an operand expression to a typed buffer
#[cfg(feature = "simd")]
fn eval_operand_to_buffer(
    expr: &Expression,
    rows: &[vibesql_storage::Row],
    evaluator: &crate::evaluator::CombinedExpressionEvaluator,
    depth: usize,
) -> Result<NumericBuffer, ExecutorError> {
    match expr {
        // Recursively handle nested binary operations
        Expression::BinaryOp { left, op, right } => {
            let result_values = eval_binary_op_simd(left, *op, right, rows, evaluator, depth + 1)?;
            // Convert Vec<SqlValue> to NumericBuffer
            let numeric_values: Result<Vec<_>, _> = result_values
                .into_iter()
                .map(|v| NumericValue::from_sql_value(&v))
                .collect();
            convert_to_buffer(numeric_values?)
        }

        // For simple expressions (columns, literals), evaluate normally
        _ => {
            let numeric_values: Result<Vec<_>, _> = rows
                .iter()
                .map(|row| {
                    let value = evaluator.eval(expr, row)?;
                    NumericValue::from_sql_value(&value)
                })
                .collect();
            convert_to_buffer(numeric_values?)
        }
    }
}

/// Convert a vector of NumericValues to a typed NumericBuffer
#[cfg(feature = "simd")]
fn convert_to_buffer(values: Vec<NumericValue>) -> Result<NumericBuffer, ExecutorError> {
    // Determine if we can use Int64 or need Float64
    let has_float = values.iter().any(|v| matches!(v, NumericValue::Float64(_)));
    let has_null = values.iter().any(|v| matches!(v, NumericValue::Null));

    if has_null {
        // For now, we don't handle NULL values in SIMD path
        // This is a simplification - full implementation would need NULL bitmask
        return Err(ExecutorError::UnsupportedExpression(
            "NULL values not yet supported in SIMD expression evaluation".to_string(),
        ));
    }

    if has_float {
        // Use Float64
        let buf: Vec<f64> = values
            .into_iter()
            .map(|v| match v {
                NumericValue::Int64(n) => n as f64,
                NumericValue::Float64(f) => f,
                NumericValue::Null => 0.0, // Won't happen due to check above
            })
            .collect();
        Ok(NumericBuffer::Float64(buf))
    } else {
        // Use Int64
        let buf: Vec<i64> = values
            .into_iter()
            .map(|v| match v {
                NumericValue::Int64(n) => n,
                NumericValue::Float64(_) => unreachable!(),
                NumericValue::Null => 0, // Won't happen due to check above
            })
            .collect();
        Ok(NumericBuffer::Int64(buf))
    }
}

/// Apply SIMD arithmetic operation to two buffers
#[cfg(feature = "simd")]
fn apply_simd_operation(
    left: &NumericBuffer,
    op: BinaryOperator,
    right: &NumericBuffer,
) -> Result<Vec<SqlValue>, ExecutorError> {
    use super::arithmetic::*;

    match (left, right) {
        // Both i64 - use integer SIMD
        (NumericBuffer::Int64(a), NumericBuffer::Int64(b)) => {
            let result = match op {
                BinaryOperator::Plus => simd_add_i64(a, b),
                BinaryOperator::Minus => simd_sub_i64(a, b),
                BinaryOperator::Multiply => simd_mul_i64(a, b),
                BinaryOperator::Divide => {
                    // Integer division requires special handling (div by zero)
                    return Ok(a
                        .iter()
                        .zip(b.iter())
                        .map(|(a, b)| {
                            if *b == 0 {
                                SqlValue::Null
                            } else {
                                SqlValue::Bigint(a / b)
                            }
                        })
                        .collect());
                }
                _ => {
                    return Err(ExecutorError::UnsupportedExpression(format!(
                        "Unsupported SIMD operation: {:?}",
                        op
                    )))
                }
            };
            Ok(result.into_iter().map(SqlValue::Bigint).collect())
        }

        // At least one f64 - promote to float SIMD
        _ => {
            let a_f64 = left.to_f64();
            let b_f64 = right.to_f64();

            let result = match op {
                BinaryOperator::Plus => simd_add_f64(&a_f64, &b_f64),
                BinaryOperator::Minus => simd_sub_f64(&a_f64, &b_f64),
                BinaryOperator::Multiply => simd_mul_f64(&a_f64, &b_f64),
                BinaryOperator::Divide => simd_div_f64(&a_f64, &b_f64),
                _ => {
                    return Err(ExecutorError::UnsupportedExpression(format!(
                        "Unsupported SIMD operation: {:?}",
                        op
                    )))
                }
            };
            Ok(result.into_iter().map(SqlValue::Double).collect())
        }
    }
}

/// Scalar fallback for expressions that can't use SIMD
#[cfg(feature = "simd")]
fn eval_expression_scalar(
    expr: &Expression,
    rows: &[vibesql_storage::Row],
    evaluator: &crate::evaluator::CombinedExpressionEvaluator,
) -> Result<Vec<SqlValue>, ExecutorError> {
    rows.iter().map(|row| evaluator.eval(expr, row)).collect()
}

/// Numeric value that can be used in SIMD operations
#[cfg(feature = "simd")]
#[derive(Debug, Clone)]
enum NumericValue {
    Int64(i64),
    Float64(f64),
    Null,
}

#[cfg(feature = "simd")]
impl NumericValue {
    fn from_sql_value(value: &SqlValue) -> Result<Self, ExecutorError> {
        match value {
            SqlValue::Integer(n) => Ok(NumericValue::Int64(*n)),
            SqlValue::Bigint(n) => Ok(NumericValue::Int64(*n)),
            SqlValue::Smallint(n) => Ok(NumericValue::Int64(*n as i64)),
            SqlValue::Double(f) => Ok(NumericValue::Float64(*f)),
            SqlValue::Float(f) => Ok(NumericValue::Float64(*f as f64)),
            SqlValue::Numeric(f) => Ok(NumericValue::Float64(*f)),
            SqlValue::Real(f) => Ok(NumericValue::Float64(*f as f64)),
            SqlValue::Null => Ok(NumericValue::Null),
            _ => Err(ExecutorError::UnsupportedExpression(format!(
                "Cannot use non-numeric value in SIMD expression: {:?}",
                value
            ))),
        }
    }
}

/// Buffer of numeric values for SIMD operations
#[cfg(feature = "simd")]
#[derive(Debug)]
enum NumericBuffer {
    Int64(Vec<i64>),
    Float64(Vec<f64>),
}

#[cfg(feature = "simd")]
impl NumericBuffer {
    /// Convert buffer to f64 (promoting integers if necessary)
    fn to_f64(&self) -> Vec<f64> {
        match self {
            NumericBuffer::Int64(v) => v.iter().map(|&x| x as f64).collect(),
            NumericBuffer::Float64(v) => v.clone(),
        }
    }
}

// TODO: Add comprehensive unit tests for SIMD expression evaluation
// Basic functionality verified through integration tests
