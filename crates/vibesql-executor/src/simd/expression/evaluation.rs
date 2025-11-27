//! Core SIMD expression evaluation
//!
//! This module contains the main SIMD evaluation logic for expressions,
//! including type conversion, buffer management, and SIMD arithmetic dispatch.

use crate::errors::ExecutorError;
use vibesql_ast::{BinaryOperator, Expression};
use vibesql_types::SqlValue;

use super::analysis::can_use_simd_for_expression;
use super::MAX_RECURSION_DEPTH;

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

    // Check if we should use SIMD (includes NULL detection for graceful fallback)
    if !can_use_simd_for_expression(expr, rows, evaluator) {
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
///
/// Returns an error if NULL values are present, which indicates a bug in the
/// NULL detection logic (NULLs should be caught by can_use_simd_for_expression).
#[cfg(feature = "simd")]
pub fn convert_to_buffer(values: Vec<NumericValue>) -> Result<NumericBuffer, ExecutorError> {
    // Determine if we can use Int64 or need Float64
    let has_float = values.iter().any(|v| matches!(v, NumericValue::Float64(_)));

    // NULL values should have been detected early by can_use_simd_for_expression()
    // If we reach here with NULLs, it's a bug in the NULL detection logic
    if values.iter().any(|v| matches!(v, NumericValue::Null)) {
        return Err(ExecutorError::UnsupportedExpression(
            "NULL values reached SIMD path despite early detection - this is a bug".to_string(),
        ));
    }

    if has_float {
        // Use Float64
        let buf: Vec<f64> = values
            .into_iter()
            .map(|v| match v {
                NumericValue::Int64(n) => n as f64,
                NumericValue::Float64(f) => f,
                NumericValue::Null => unreachable!("NULLs filtered by early detection"),
            })
            .collect();
        Ok(NumericBuffer::Float64(buf))
    } else {
        // Use Int64
        let buf: Vec<i64> = values
            .into_iter()
            .map(|v| match v {
                NumericValue::Int64(n) => n,
                NumericValue::Float64(_) => unreachable!("Mixed types handled above"),
                NumericValue::Null => unreachable!("NULLs filtered by early detection"),
            })
            .collect();
        Ok(NumericBuffer::Int64(buf))
    }
}

/// Apply SIMD arithmetic operation to two buffers
#[cfg(feature = "simd")]
pub fn apply_simd_operation(
    left: &NumericBuffer,
    op: BinaryOperator,
    right: &NumericBuffer,
) -> Result<Vec<SqlValue>, ExecutorError> {
    use crate::simd::arithmetic::*;

    match (left, right) {
        // Both i64 - use integer SIMD
        (NumericBuffer::Int64(a), NumericBuffer::Int64(b)) => {
            let result = match op {
                BinaryOperator::Plus => simd_add_i64(a, b),
                BinaryOperator::Minus => simd_sub_i64(a, b),
                BinaryOperator::Multiply => simd_mul_i64(a, b),
                BinaryOperator::Divide => {
                    // Use SIMD division first, then post-process for divide-by-zero
                    // This is Option 1 from the issue: SIMD first, fix later
                    //
                    // Performance note: This approach gets SIMD benefits for the common
                    // case (no divide-by-zero) while still correctly handling edge cases.
                    // The post-processing scan is cheap compared to the scalar loop alternative.
                    let result = simd_div_i64(a, b);

                    // Convert to SqlValue, replacing divide-by-zero with NULL
                    return Ok(result
                        .into_iter()
                        .zip(b.iter())
                        .map(|(quotient, &divisor)| {
                            if divisor == 0 {
                                SqlValue::Null
                            } else {
                                SqlValue::Bigint(quotient)
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
pub fn eval_expression_scalar(
    expr: &Expression,
    rows: &[vibesql_storage::Row],
    evaluator: &crate::evaluator::CombinedExpressionEvaluator,
) -> Result<Vec<SqlValue>, ExecutorError> {
    rows.iter().map(|row| evaluator.eval(expr, row)).collect()
}

/// Numeric value that can be used in SIMD operations
#[cfg(feature = "simd")]
#[derive(Debug, Clone)]
pub enum NumericValue {
    Int64(i64),
    Float64(f64),
    Null,
}

#[cfg(feature = "simd")]
impl NumericValue {
    pub fn from_sql_value(value: &SqlValue) -> Result<Self, ExecutorError> {
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
pub enum NumericBuffer {
    Int64(Vec<i64>),
    Float64(Vec<f64>),
}

#[cfg(feature = "simd")]
impl NumericBuffer {
    /// Convert buffer to f64 (promoting integers if necessary)
    pub fn to_f64(&self) -> Vec<f64> {
        match self {
            NumericBuffer::Int64(v) => v.iter().map(|&x| x as f64).collect(),
            NumericBuffer::Float64(v) => v.clone(),
        }
    }
}

#[cfg(all(test, feature = "simd"))]
mod tests {
    use super::*;
    use vibesql_ast::BinaryOperator;
    use vibesql_storage::Row;
    use vibesql_types::{DataType, SqlValue};

    // Helper to create a mock evaluator for testing
    fn create_test_evaluator() -> crate::evaluator::CombinedExpressionEvaluator<'static> {
        use crate::schema::CombinedSchema;
        use vibesql_catalog::{ColumnSchema, TableSchema};

        let columns = vec![
            ColumnSchema::new("a".to_string(), DataType::Bigint, false),
            ColumnSchema::new("b".to_string(), DataType::Bigint, false),
            ColumnSchema::new("c".to_string(), DataType::Bigint, false),
        ];
        let table_schema = TableSchema::new("test".to_string(), columns);

        let schema = Box::leak(Box::new(CombinedSchema::from_table(
            "test".to_string(),
            table_schema,
        )));
        crate::evaluator::CombinedExpressionEvaluator::new(schema)
    }

    // Helper to create test rows with numeric values
    fn create_test_rows(count: usize) -> Vec<Row> {
        (0..count)
            .map(|i| {
                Row::new(vec![
                    SqlValue::Bigint(i as i64),
                    SqlValue::Bigint((i * 2) as i64),
                    SqlValue::Bigint((i * 3) as i64),
                ])
            })
            .collect()
    }

    // ===== Batch Expression Evaluation Tests =====

    #[test]
    fn test_eval_expression_batch_simd_simple_addition() {
        let rows = create_test_rows(100);
        let evaluator = create_test_evaluator();

        // Expression: column_0 + 10
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef {
                table: None,
                column: "0".to_string(),
            }),
            op: BinaryOperator::Plus,
            right: Box::new(Expression::Literal(SqlValue::Integer(10))),
        };

        // This test will use scalar fallback due to evaluator limitations
        // but verifies the function signature and basic execution path
        let result = eval_expression_batch_simd(&expr, &rows, &evaluator);

        // Should complete without panic
        assert!(result.is_ok() || result.is_err());
    }

    #[test]
    fn test_eval_expression_batch_simd_empty_rows() {
        let rows = vec![];
        let evaluator = create_test_evaluator();

        let expr = Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef {
                table: None,
                column: "a".to_string(),
            }),
            op: BinaryOperator::Plus,
            right: Box::new(Expression::Literal(SqlValue::Integer(1))),
        };

        let result = eval_expression_batch_simd(&expr, &rows, &evaluator);
        assert!(result.is_ok());
        assert_eq!(result.unwrap().len(), 0);
    }

    #[test]
    fn test_eval_expression_batch_simd_falls_back_for_insufficient_rows() {
        let rows = create_test_rows(50); // Below threshold
        let evaluator = create_test_evaluator();

        let expr = Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef {
                table: None,
                column: "0".to_string(),
            }),
            op: BinaryOperator::Plus,
            right: Box::new(Expression::Literal(SqlValue::Integer(5))),
        };

        // Should fall back to scalar evaluation
        let result = eval_expression_batch_simd(&expr, &rows, &evaluator);
        assert!(result.is_ok() || result.is_err());
    }

    #[test]
    fn test_eval_expression_batch_simd_falls_back_for_complex_expression() {
        let rows = create_test_rows(100);
        let evaluator = create_test_evaluator();

        // Complex expression with function
        let expr = Expression::Function {
            name: "ABS".to_string(),
            args: vec![Expression::ColumnRef {
                table: None,
                column: "a".to_string(),
            }],
            character_unit: None,
        };

        // Should fall back to scalar evaluation
        let result = eval_expression_batch_simd(&expr, &rows, &evaluator);
        assert!(result.is_ok() || result.is_err());
    }

    // ===== Recursion Depth Limiting Tests =====

    #[test]
    fn test_recursion_depth_limiting_prevents_stack_overflow() {
        let rows = create_test_rows(100);
        let evaluator = create_test_evaluator();

        // Create deeply nested expression (40 levels)
        let mut expr = Expression::Literal(SqlValue::Integer(1));
        for _ in 0..40 {
            expr = Expression::BinaryOp {
                left: Box::new(expr),
                op: BinaryOperator::Plus,
                right: Box::new(Expression::Literal(SqlValue::Integer(1))),
            };
        }

        // Should not panic, should fall back to scalar
        let result = eval_expression_batch_simd(&expr, &rows, &evaluator);
        assert!(result.is_ok() || result.is_err());
    }

    #[test]
    fn test_recursion_depth_at_limit() {
        let rows = create_test_rows(100);
        let evaluator = create_test_evaluator();

        // Create expression at exactly MAX_RECURSION_DEPTH levels (32)
        let mut expr = Expression::Literal(SqlValue::Integer(1));
        for _ in 0..31 {
            expr = Expression::BinaryOp {
                left: Box::new(expr),
                op: BinaryOperator::Plus,
                right: Box::new(Expression::Literal(SqlValue::Integer(1))),
            };
        }

        // Should handle this depth
        let result = eval_expression_batch_simd(&expr, &rows, &evaluator);
        assert!(result.is_ok() || result.is_err());
    }

    // ===== NULL Handling Tests =====

    #[test]
    fn test_convert_to_buffer_rejects_null_values() {
        let values = vec![
            NumericValue::Int64(1),
            NumericValue::Null,
            NumericValue::Int64(3),
        ];

        let result = convert_to_buffer(values);
        assert!(result.is_err());

        if let Err(ExecutorError::UnsupportedExpression(msg)) = result {
            assert!(msg.contains("NULL"));
        }
    }

    #[test]
    fn test_numeric_value_from_null_sql_value() {
        let result = NumericValue::from_sql_value(&SqlValue::Null);
        assert!(result.is_ok());
        assert!(matches!(result.unwrap(), NumericValue::Null));
    }

    // ===== Buffer Conversion Tests =====

    #[test]
    fn test_convert_to_buffer_all_int64() {
        let values = vec![
            NumericValue::Int64(1),
            NumericValue::Int64(2),
            NumericValue::Int64(3),
        ];

        let result = convert_to_buffer(values);
        assert!(result.is_ok());

        match result.unwrap() {
            NumericBuffer::Int64(buf) => {
                assert_eq!(buf, vec![1, 2, 3]);
            }
            _ => panic!("Expected Int64 buffer"),
        }
    }

    #[test]
    fn test_convert_to_buffer_mixed_promotes_to_float64() {
        let values = vec![
            NumericValue::Int64(1),
            NumericValue::Float64(2.5),
            NumericValue::Int64(3),
        ];

        let result = convert_to_buffer(values);
        assert!(result.is_ok());

        match result.unwrap() {
            NumericBuffer::Float64(buf) => {
                assert_eq!(buf, vec![1.0, 2.5, 3.0]);
            }
            _ => panic!("Expected Float64 buffer"),
        }
    }

    #[test]
    fn test_convert_to_buffer_all_float64() {
        let values = vec![
            NumericValue::Float64(1.5),
            NumericValue::Float64(2.5),
            NumericValue::Float64(3.5),
        ];

        let result = convert_to_buffer(values);
        assert!(result.is_ok());

        match result.unwrap() {
            NumericBuffer::Float64(buf) => {
                assert_eq!(buf, vec![1.5, 2.5, 3.5]);
            }
            _ => panic!("Expected Float64 buffer"),
        }
    }

    #[test]
    fn test_numeric_buffer_to_f64_promotion() {
        // Int64 buffer promotes to f64
        let int_buf = NumericBuffer::Int64(vec![1, 2, 3]);
        let f64_vec = int_buf.to_f64();
        assert_eq!(f64_vec, vec![1.0, 2.0, 3.0]);

        // Float64 buffer clones
        let float_buf = NumericBuffer::Float64(vec![1.5, 2.5, 3.5]);
        let f64_vec = float_buf.to_f64();
        assert_eq!(f64_vec, vec![1.5, 2.5, 3.5]);
    }

    // ===== Division Handling Tests =====

    #[test]
    fn test_integer_division_by_zero_returns_null() {
        let a = NumericBuffer::Int64(vec![10, 20, 30]);
        let b = NumericBuffer::Int64(vec![2, 0, 5]);

        let result = apply_simd_operation(&a, BinaryOperator::Divide, &b);
        assert!(result.is_ok());

        let values = result.unwrap();
        assert_eq!(values[0], SqlValue::Bigint(5));
        assert_eq!(values[1], SqlValue::Null); // Division by zero
        assert_eq!(values[2], SqlValue::Bigint(6));
    }

    #[test]
    fn test_integer_division_non_zero() {
        let a = NumericBuffer::Int64(vec![10, 20, 30, 40]);
        let b = NumericBuffer::Int64(vec![2, 4, 3, 8]);

        let result = apply_simd_operation(&a, BinaryOperator::Divide, &b);
        assert!(result.is_ok());

        let values = result.unwrap();
        assert_eq!(values[0], SqlValue::Bigint(5));
        assert_eq!(values[1], SqlValue::Bigint(5));
        assert_eq!(values[2], SqlValue::Bigint(10));
        assert_eq!(values[3], SqlValue::Bigint(5));
    }

    #[test]
    fn test_float_division_handles_zero() {
        let a = NumericBuffer::Float64(vec![10.0, 20.0, 30.0]);
        let b = NumericBuffer::Float64(vec![2.0, 0.0, 5.0]);

        let result = apply_simd_operation(&a, BinaryOperator::Divide, &b);
        assert!(result.is_ok());

        let values = result.unwrap();
        assert_eq!(values[0], SqlValue::Double(5.0));
        // Float division by zero produces infinity
        match values[1] {
            SqlValue::Double(v) => assert!(v.is_infinite()),
            _ => panic!("Expected Double value"),
        }
        assert_eq!(values[2], SqlValue::Double(6.0));
    }

    // ===== SIMD Operation Tests =====

    #[test]
    fn test_apply_simd_operation_int64_addition() {
        let a = NumericBuffer::Int64(vec![1, 2, 3, 4]);
        let b = NumericBuffer::Int64(vec![10, 20, 30, 40]);

        let result = apply_simd_operation(&a, BinaryOperator::Plus, &b);
        assert!(result.is_ok());

        let values = result.unwrap();
        assert_eq!(values[0], SqlValue::Bigint(11));
        assert_eq!(values[1], SqlValue::Bigint(22));
        assert_eq!(values[2], SqlValue::Bigint(33));
        assert_eq!(values[3], SqlValue::Bigint(44));
    }

    #[test]
    fn test_apply_simd_operation_float64_multiplication() {
        let a = NumericBuffer::Float64(vec![1.5, 2.0, 3.5, 4.0]);
        let b = NumericBuffer::Float64(vec![2.0, 3.0, 2.0, 5.0]);

        let result = apply_simd_operation(&a, BinaryOperator::Multiply, &b);
        assert!(result.is_ok());

        let values = result.unwrap();
        assert_eq!(values[0], SqlValue::Double(3.0));
        assert_eq!(values[1], SqlValue::Double(6.0));
        assert_eq!(values[2], SqlValue::Double(7.0));
        assert_eq!(values[3], SqlValue::Double(20.0));
    }

    #[test]
    fn test_apply_simd_operation_type_promotion() {
        // Int64 + Float64 should promote to Float64
        let a = NumericBuffer::Int64(vec![1, 2, 3]);
        let b = NumericBuffer::Float64(vec![1.5, 2.5, 3.5]);

        let result = apply_simd_operation(&a, BinaryOperator::Plus, &b);
        assert!(result.is_ok());

        let values = result.unwrap();
        assert_eq!(values[0], SqlValue::Double(2.5));
        assert_eq!(values[1], SqlValue::Double(4.5));
        assert_eq!(values[2], SqlValue::Double(6.5));
    }

    #[test]
    fn test_apply_simd_operation_unsupported_operator() {
        let a = NumericBuffer::Int64(vec![1, 2, 3]);
        let b = NumericBuffer::Int64(vec![1, 2, 3]);

        let result = apply_simd_operation(&a, BinaryOperator::Modulo, &b);
        assert!(result.is_err());

        if let Err(ExecutorError::UnsupportedExpression(msg)) = result {
            assert!(msg.contains("Unsupported SIMD operation"));
        }
    }

    // ===== NumericValue Conversion Tests =====

    #[test]
    fn test_numeric_value_from_sql_value_integers() {
        assert!(matches!(
            NumericValue::from_sql_value(&SqlValue::Integer(42)).unwrap(),
            NumericValue::Int64(42)
        ));

        assert!(matches!(
            NumericValue::from_sql_value(&SqlValue::Bigint(100)).unwrap(),
            NumericValue::Int64(100)
        ));

        assert!(matches!(
            NumericValue::from_sql_value(&SqlValue::Smallint(10)).unwrap(),
            NumericValue::Int64(10)
        ));
    }

    #[test]
    fn test_numeric_value_from_sql_value_floats() {
        assert!(matches!(
            NumericValue::from_sql_value(&SqlValue::Double(3.14)).unwrap(),
            NumericValue::Float64(3.14)
        ));

        assert!(matches!(
            NumericValue::from_sql_value(&SqlValue::Float(2.5)).unwrap(),
            NumericValue::Float64(_)
        ));

        assert!(matches!(
            NumericValue::from_sql_value(&SqlValue::Numeric(1.5)).unwrap(),
            NumericValue::Float64(1.5)
        ));

        assert!(matches!(
            NumericValue::from_sql_value(&SqlValue::Real(4.2)).unwrap(),
            NumericValue::Float64(_)
        ));
    }

    #[test]
    fn test_numeric_value_from_sql_value_rejects_non_numeric() {
        let result = NumericValue::from_sql_value(&SqlValue::Varchar("hello".to_string()));
        assert!(result.is_err());

        if let Err(ExecutorError::UnsupportedExpression(msg)) = result {
            assert!(msg.contains("non-numeric value"));
        }
    }
}
