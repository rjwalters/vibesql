//! SIMD-accelerated arithmetic using Arrow compute kernels
//!
//! This module provides vectorized arithmetic operations for:
//! - Addition (+)
//! - Subtraction (-)
//! - Multiplication (*)
//! - Division (/)
//!
//! These operations process multiple values per instruction using SIMD,
//! providing 4-8x performance improvement over scalar operations.

#![allow(clippy::useless_conversion)]

use crate::errors::ExecutorError;
use arrow::array::{Array, ArrayRef, Float64Array, Int64Array};
use arrow::compute::kernels::numeric::{add, div, mul, sub};
use arrow::record_batch::RecordBatch;
use vibesql_ast::{BinaryOperator, Expression};
use vibesql_types::SqlValue;

/// Evaluate an arithmetic expression on a RecordBatch using SIMD operations
///
/// Returns an ArrayRef containing the computed values.
/// This function vectorizes arithmetic operations like:
/// - `col1 * col2`
/// - `col1 * 5` (column * literal)
/// - `(col1 * (1 - col2))` (nested arithmetic)
pub fn evaluate_arithmetic_simd(
    batch: &RecordBatch,
    expr: &Expression,
) -> Result<ArrayRef, ExecutorError> {
    match expr {
        Expression::BinaryOp { left, op, right } => {
            evaluate_binary_arithmetic_simd(batch, left, op, right)
        }
        Expression::ColumnRef { column, .. } => {
            // Simple column reference
            get_numeric_column(batch, column)
        }
        Expression::Literal(value) => {
            // Create array filled with literal value
            create_literal_array(value, batch.num_rows())
        }
        _ => Err(ExecutorError::UnsupportedFeature(format!(
            "SIMD arithmetic expression: {:?}",
            expr
        ))),
    }
}

/// Evaluate a binary arithmetic operation
fn evaluate_binary_arithmetic_simd(
    batch: &RecordBatch,
    left: &Expression,
    op: &BinaryOperator,
    right: &Expression,
) -> Result<ArrayRef, ExecutorError> {
    // Recursively evaluate left and right sides
    let left_array = evaluate_arithmetic_simd(batch, left)?;
    let right_array = evaluate_arithmetic_simd(batch, right)?;

    // Apply the operation based on data types
    match (left_array.data_type(), right_array.data_type()) {
        (arrow::datatypes::DataType::Int64, arrow::datatypes::DataType::Int64) => {
            apply_int64_op(&left_array, &right_array, op)
        }
        (arrow::datatypes::DataType::Float64, arrow::datatypes::DataType::Float64) => {
            apply_float64_op(&left_array, &right_array, op)
        }
        // Mixed types: cast to float64
        _ => {
            let left_f64 = cast_to_float64(&left_array)?;
            let right_f64 = cast_to_float64(&right_array)?;
            apply_float64_op(&left_f64, &right_f64, op)
        }
    }
}

/// Apply arithmetic operation to Int64 arrays
fn apply_int64_op(
    left: &ArrayRef,
    right: &ArrayRef,
    op: &BinaryOperator,
) -> Result<ArrayRef, ExecutorError> {
    let left_arr = left.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
        ExecutorError::ArrowDowncastError {
            expected_type: "Int64Array".to_string(),
            context: "apply_int64_op (left)".to_string(),
        }
    })?;
    let right_arr = right.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
        ExecutorError::ArrowDowncastError {
            expected_type: "Int64Array".to_string(),
            context: "apply_int64_op (right)".to_string(),
        }
    })?;

    let result: ArrayRef = match op {
        BinaryOperator::Plus => {
            add(left_arr, right_arr).map_err(|e| ExecutorError::SimdOperationFailed {
                operation: "add".to_string(),
                reason: e.to_string(),
            })?
        }
        BinaryOperator::Minus => {
            sub(left_arr, right_arr).map_err(|e| ExecutorError::SimdOperationFailed {
                operation: "subtract".to_string(),
                reason: e.to_string(),
            })?
        }
        BinaryOperator::Multiply => {
            mul(left_arr, right_arr).map_err(|e| ExecutorError::SimdOperationFailed {
                operation: "multiply".to_string(),
                reason: e.to_string(),
            })?
        }
        BinaryOperator::Divide => {
            // For integer division, cast to float64 first
            let left_f64 = cast_int64_to_float64(left_arr)?;
            let right_f64 = cast_int64_to_float64(right_arr)?;
            div(&left_f64, &right_f64).map_err(|e| ExecutorError::SimdOperationFailed {
                operation: "divide".to_string(),
                reason: e.to_string(),
            })?
        }
        _ => {
            return Err(ExecutorError::UnsupportedFeature(format!("arithmetic operator {:?}", op)))
        }
    };

    Ok(result)
}

/// Apply arithmetic operation to Float64 arrays
fn apply_float64_op(
    left: &ArrayRef,
    right: &ArrayRef,
    op: &BinaryOperator,
) -> Result<ArrayRef, ExecutorError> {
    let left_arr = left.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
        ExecutorError::ArrowDowncastError {
            expected_type: "Float64Array".to_string(),
            context: "apply_float64_op (left)".to_string(),
        }
    })?;
    let right_arr = right.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
        ExecutorError::ArrowDowncastError {
            expected_type: "Float64Array".to_string(),
            context: "apply_float64_op (right)".to_string(),
        }
    })?;

    let result: ArrayRef = match op {
        BinaryOperator::Plus => add(left_arr, right_arr)
            .map_err(|e| ExecutorError::SimdOperationFailed {
                operation: "add".to_string(),
                reason: e.to_string(),
            })?
            .into(),
        BinaryOperator::Minus => sub(left_arr, right_arr)
            .map_err(|e| ExecutorError::SimdOperationFailed {
                operation: "subtract".to_string(),
                reason: e.to_string(),
            })?
            .into(),
        BinaryOperator::Multiply => mul(left_arr, right_arr)
            .map_err(|e| ExecutorError::SimdOperationFailed {
                operation: "multiply".to_string(),
                reason: e.to_string(),
            })?
            .into(),
        BinaryOperator::Divide => div(left_arr, right_arr)
            .map_err(|e| ExecutorError::SimdOperationFailed {
                operation: "divide".to_string(),
                reason: e.to_string(),
            })?
            .into(),
        _ => {
            return Err(ExecutorError::UnsupportedFeature(format!("arithmetic operator {:?}", op)))
        }
    };

    Ok(result)
}

/// Get a numeric column from the RecordBatch
fn get_numeric_column(batch: &RecordBatch, col_name: &str) -> Result<ArrayRef, ExecutorError> {
    let schema = batch.schema();
    let (col_idx, _) =
        schema.column_with_name(col_name).ok_or_else(|| ExecutorError::ColumnarColumnNotFound {
            column_index: 0, // Column referenced by name
            batch_columns: schema.fields().len(),
        })?;
    let column = batch.column(col_idx);

    // Verify it's a numeric type
    match column.data_type() {
        arrow::datatypes::DataType::Int64
        | arrow::datatypes::DataType::Float64
        | arrow::datatypes::DataType::Int32
        | arrow::datatypes::DataType::Float32 => Ok(column.clone()),
        _ => Err(ExecutorError::UnsupportedArrayType {
            operation: format!("arithmetic on column {}", col_name),
            array_type: format!("{:?}", column.data_type()),
        }),
    }
}

/// Create an array filled with a literal value
fn create_literal_array(value: &SqlValue, len: usize) -> Result<ArrayRef, ExecutorError> {
    match value {
        SqlValue::Integer(i) | SqlValue::Bigint(i) => {
            Ok(Arc::new(Int64Array::from(vec![*i; len])) as ArrayRef)
        }
        SqlValue::Smallint(i) => Ok(Arc::new(Int64Array::from(vec![*i as i64; len])) as ArrayRef),
        SqlValue::Float(f) | SqlValue::Real(f) => {
            Ok(Arc::new(Float64Array::from(vec![*f as f64; len])) as ArrayRef)
        }
        SqlValue::Double(f) | SqlValue::Numeric(f) => {
            Ok(Arc::new(Float64Array::from(vec![*f; len])) as ArrayRef)
        }
        SqlValue::Null => {
            // Create array of nulls
            Ok(Arc::new(Int64Array::from(vec![None; len])) as ArrayRef)
        }
        _ => Err(ExecutorError::ColumnarTypeMismatch {
            operation: "literal array creation".to_string(),
            left_type: format!("{:?}", value),
            right_type: Some("numeric".to_string()),
        }),
    }
}

/// Cast any numeric array to Float64Array
fn cast_to_float64(array: &ArrayRef) -> Result<ArrayRef, ExecutorError> {
    match array.data_type() {
        arrow::datatypes::DataType::Int64 => {
            let int_arr = array.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
                ExecutorError::ArrowDowncastError {
                    expected_type: "Int64Array".to_string(),
                    context: "cast_to_float64".to_string(),
                }
            })?;
            Ok(Arc::new(cast_int64_to_float64(int_arr)?) as ArrayRef)
        }
        arrow::datatypes::DataType::Float64 => Ok(array.clone()),
        _ => Err(ExecutorError::UnsupportedArrayType {
            operation: "float64 cast".to_string(),
            array_type: format!("{:?}", array.data_type()),
        }),
    }
}

/// Cast Int64Array to Float64Array
fn cast_int64_to_float64(array: &Int64Array) -> Result<Float64Array, ExecutorError> {
    let values: Vec<Option<f64>> = array.iter().map(|v| v.map(|i| i as f64)).collect();
    Ok(Float64Array::from(values))
}

use std::sync::Arc;

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;

    // ===== Basic functionality tests =====

    #[test]
    fn test_simd_multiply_columns() {
        // Test: price * quantity
        let schema = Schema::new(vec![
            Field::new("price", DataType::Float64, false),
            Field::new("quantity", DataType::Int64, false),
        ]);
        let price_array = Float64Array::from(vec![10.0, 20.0, 30.0, 40.0, 50.0]);
        let quantity_array = Int64Array::from(vec![2, 3, 4, 5, 6]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(price_array), Arc::new(quantity_array)],
        )
        .unwrap();

        let expr = Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef { table: None, column: "price".to_string() }),
            op: BinaryOperator::Multiply,
            right: Box::new(Expression::ColumnRef { table: None, column: "quantity".to_string() }),
        };

        let result = evaluate_arithmetic_simd(&batch, &expr).unwrap();
        let result_arr = result.as_any().downcast_ref::<Float64Array>().unwrap();

        // Expected: [20.0, 60.0, 120.0, 200.0, 300.0]
        assert_eq!(result_arr.len(), 5);
        assert!((result_arr.value(0) - 20.0).abs() < 0.001);
        assert!((result_arr.value(1) - 60.0).abs() < 0.001);
        assert!((result_arr.value(2) - 120.0).abs() < 0.001);
    }

    #[test]
    fn test_simd_multiply_literal() {
        // Test: price * 0.9 (10% discount)
        let schema = Schema::new(vec![Field::new("price", DataType::Float64, false)]);
        let price_array = Float64Array::from(vec![100.0, 200.0, 300.0]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(price_array)]).unwrap();

        let expr = Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef { table: None, column: "price".to_string() }),
            op: BinaryOperator::Multiply,
            right: Box::new(Expression::Literal(SqlValue::Float(0.9))),
        };

        let result = evaluate_arithmetic_simd(&batch, &expr).unwrap();
        let result_arr = result.as_any().downcast_ref::<Float64Array>().unwrap();

        // Expected: [90.0, 180.0, 270.0]
        assert_eq!(result_arr.len(), 3);
        assert!((result_arr.value(0) - 90.0).abs() < 0.001);
        assert!((result_arr.value(1) - 180.0).abs() < 0.001);
        assert!((result_arr.value(2) - 270.0).abs() < 0.001);
    }

    #[test]
    fn test_simd_nested_arithmetic() {
        // Test: price * (1 - discount)
        let schema = Schema::new(vec![
            Field::new("price", DataType::Float64, false),
            Field::new("discount", DataType::Float64, false),
        ]);
        let price_array = Float64Array::from(vec![100.0, 200.0, 300.0]);
        let discount_array = Float64Array::from(vec![0.1, 0.2, 0.15]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(price_array), Arc::new(discount_array)],
        )
        .unwrap();

        // price * (1 - discount)
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef { table: None, column: "price".to_string() }),
            op: BinaryOperator::Multiply,
            right: Box::new(Expression::BinaryOp {
                left: Box::new(Expression::Literal(SqlValue::Float(1.0))),
                op: BinaryOperator::Minus,
                right: Box::new(Expression::ColumnRef {
                    table: None,
                    column: "discount".to_string(),
                }),
            }),
        };

        let result = evaluate_arithmetic_simd(&batch, &expr).unwrap();
        let result_arr = result.as_any().downcast_ref::<Float64Array>().unwrap();

        // Expected: [90.0, 160.0, 255.0]
        assert_eq!(result_arr.len(), 3);
        assert!((result_arr.value(0) - 90.0).abs() < 0.001);
        assert!((result_arr.value(1) - 160.0).abs() < 0.001);
        assert!((result_arr.value(2) - 255.0).abs() < 0.001);
    }

    // ===== All arithmetic operations =====

    #[test]
    fn test_simd_addition() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Float64, false),
            Field::new("b", DataType::Float64, false),
        ]);
        let a_array = Float64Array::from(vec![10.0, 20.0, 30.0, 40.0]);
        let b_array = Float64Array::from(vec![1.0, 2.0, 3.0, 4.0]);
        let batch =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a_array), Arc::new(b_array)])
                .unwrap();

        let expr = Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef { table: None, column: "a".to_string() }),
            op: BinaryOperator::Plus,
            right: Box::new(Expression::ColumnRef { table: None, column: "b".to_string() }),
        };

        let result = evaluate_arithmetic_simd(&batch, &expr).unwrap();
        let result_arr = result.as_any().downcast_ref::<Float64Array>().unwrap();

        assert_eq!(result_arr.value(0), 11.0);
        assert_eq!(result_arr.value(1), 22.0);
        assert_eq!(result_arr.value(2), 33.0);
        assert_eq!(result_arr.value(3), 44.0);
    }

    #[test]
    fn test_simd_subtraction() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Float64, false),
            Field::new("b", DataType::Float64, false),
        ]);
        let a_array = Float64Array::from(vec![100.0, 200.0, 300.0, 400.0]);
        let b_array = Float64Array::from(vec![10.0, 20.0, 30.0, 40.0]);
        let batch =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a_array), Arc::new(b_array)])
                .unwrap();

        let expr = Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef { table: None, column: "a".to_string() }),
            op: BinaryOperator::Minus,
            right: Box::new(Expression::ColumnRef { table: None, column: "b".to_string() }),
        };

        let result = evaluate_arithmetic_simd(&batch, &expr).unwrap();
        let result_arr = result.as_any().downcast_ref::<Float64Array>().unwrap();

        assert_eq!(result_arr.value(0), 90.0);
        assert_eq!(result_arr.value(1), 180.0);
        assert_eq!(result_arr.value(2), 270.0);
        assert_eq!(result_arr.value(3), 360.0);
    }

    #[test]
    fn test_simd_division() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Float64, false),
            Field::new("b", DataType::Float64, false),
        ]);
        let a_array = Float64Array::from(vec![100.0, 200.0, 300.0, 400.0]);
        let b_array = Float64Array::from(vec![10.0, 20.0, 30.0, 40.0]);
        let batch =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a_array), Arc::new(b_array)])
                .unwrap();

        let expr = Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef { table: None, column: "a".to_string() }),
            op: BinaryOperator::Divide,
            right: Box::new(Expression::ColumnRef { table: None, column: "b".to_string() }),
        };

        let result = evaluate_arithmetic_simd(&batch, &expr).unwrap();
        let result_arr = result.as_any().downcast_ref::<Float64Array>().unwrap();

        assert_eq!(result_arr.value(0), 10.0);
        assert_eq!(result_arr.value(1), 10.0);
        assert_eq!(result_arr.value(2), 10.0);
        assert_eq!(result_arr.value(3), 10.0);
    }

    // ===== Integer operations =====

    #[test]
    fn test_simd_int64_operations() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Int64, false),
            Field::new("b", DataType::Int64, false),
        ]);
        let a_array = Int64Array::from(vec![10, 20, 30, 40]);
        let b_array = Int64Array::from(vec![2, 3, 4, 5]);
        let batch =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a_array), Arc::new(b_array)])
                .unwrap();

        // Test addition
        let add_expr = Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef { table: None, column: "a".to_string() }),
            op: BinaryOperator::Plus,
            right: Box::new(Expression::ColumnRef { table: None, column: "b".to_string() }),
        };

        let result = evaluate_arithmetic_simd(&batch, &add_expr).unwrap();
        let result_arr = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(result_arr.value(0), 12);
        assert_eq!(result_arr.value(1), 23);

        // Test multiplication
        let mul_expr = Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef { table: None, column: "a".to_string() }),
            op: BinaryOperator::Multiply,
            right: Box::new(Expression::ColumnRef { table: None, column: "b".to_string() }),
        };

        let result = evaluate_arithmetic_simd(&batch, &mul_expr).unwrap();
        let result_arr = result.as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(result_arr.value(0), 20);
        assert_eq!(result_arr.value(1), 60);
    }

    // ===== Edge cases =====

    #[test]
    fn test_empty_batch() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Float64, false),
            Field::new("b", DataType::Float64, false),
        ]);
        let a_array = Float64Array::from(Vec::<f64>::new());
        let b_array = Float64Array::from(Vec::<f64>::new());
        let batch =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a_array), Arc::new(b_array)])
                .unwrap();

        let expr = Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef { table: None, column: "a".to_string() }),
            op: BinaryOperator::Plus,
            right: Box::new(Expression::ColumnRef { table: None, column: "b".to_string() }),
        };

        let result = evaluate_arithmetic_simd(&batch, &expr).unwrap();
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_single_row() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Float64, false),
            Field::new("b", DataType::Float64, false),
        ]);
        let a_array = Float64Array::from(vec![5.0]);
        let b_array = Float64Array::from(vec![3.0]);
        let batch =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a_array), Arc::new(b_array)])
                .unwrap();

        let expr = Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef { table: None, column: "a".to_string() }),
            op: BinaryOperator::Multiply,
            right: Box::new(Expression::ColumnRef { table: None, column: "b".to_string() }),
        };

        let result = evaluate_arithmetic_simd(&batch, &expr).unwrap();
        let result_arr = result.as_any().downcast_ref::<Float64Array>().unwrap();
        assert_eq!(result_arr.value(0), 15.0);
    }

    #[test]
    fn test_large_batch() {
        let size = 10000;
        let schema = Schema::new(vec![
            Field::new("a", DataType::Float64, false),
            Field::new("b", DataType::Float64, false),
        ]);
        let a_vals: Vec<f64> = (0..size).map(|i| i as f64).collect();
        let b_vals: Vec<f64> = (0..size).map(|i| (i * 2) as f64).collect();
        let a_array = Float64Array::from(a_vals);
        let b_array = Float64Array::from(b_vals);
        let batch =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a_array), Arc::new(b_array)])
                .unwrap();

        let expr = Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef { table: None, column: "a".to_string() }),
            op: BinaryOperator::Plus,
            right: Box::new(Expression::ColumnRef { table: None, column: "b".to_string() }),
        };

        let result = evaluate_arithmetic_simd(&batch, &expr).unwrap();
        let result_arr = result.as_any().downcast_ref::<Float64Array>().unwrap();

        assert_eq!(result_arr.len(), size);
        assert_eq!(result_arr.value(0), 0.0);
        assert_eq!(result_arr.value(100), 300.0);
    }

    #[test]
    fn test_negative_values() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Float64, false),
            Field::new("b", DataType::Float64, false),
        ]);
        let a_array = Float64Array::from(vec![-10.0, -20.0, -30.0]);
        let b_array = Float64Array::from(vec![5.0, 10.0, 15.0]);
        let batch =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a_array), Arc::new(b_array)])
                .unwrap();

        let expr = Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef { table: None, column: "a".to_string() }),
            op: BinaryOperator::Multiply,
            right: Box::new(Expression::ColumnRef { table: None, column: "b".to_string() }),
        };

        let result = evaluate_arithmetic_simd(&batch, &expr).unwrap();
        let result_arr = result.as_any().downcast_ref::<Float64Array>().unwrap();

        assert_eq!(result_arr.value(0), -50.0);
        assert_eq!(result_arr.value(1), -200.0);
        assert_eq!(result_arr.value(2), -450.0);
    }

    #[test]
    fn test_division_by_zero() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Float64, false),
            Field::new("b", DataType::Float64, false),
        ]);
        let a_array = Float64Array::from(vec![10.0, 20.0, 30.0]);
        let b_array = Float64Array::from(vec![2.0, 0.0, 5.0]);
        let batch =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a_array), Arc::new(b_array)])
                .unwrap();

        let expr = Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef { table: None, column: "a".to_string() }),
            op: BinaryOperator::Divide,
            right: Box::new(Expression::ColumnRef { table: None, column: "b".to_string() }),
        };

        let result = evaluate_arithmetic_simd(&batch, &expr).unwrap();
        let result_arr = result.as_any().downcast_ref::<Float64Array>().unwrap();

        assert_eq!(result_arr.value(0), 5.0);
        assert!(result_arr.value(1).is_infinite());
        assert_eq!(result_arr.value(2), 6.0);
    }

    #[test]
    fn test_zero_values() {
        let schema = Schema::new(vec![
            Field::new("a", DataType::Float64, false),
            Field::new("b", DataType::Float64, false),
        ]);
        let a_array = Float64Array::from(vec![0.0, 5.0, 0.0, 10.0]);
        let b_array = Float64Array::from(vec![10.0, 0.0, 0.0, 20.0]);
        let batch =
            RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a_array), Arc::new(b_array)])
                .unwrap();

        let expr = Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef { table: None, column: "a".to_string() }),
            op: BinaryOperator::Multiply,
            right: Box::new(Expression::ColumnRef { table: None, column: "b".to_string() }),
        };

        let result = evaluate_arithmetic_simd(&batch, &expr).unwrap();
        let result_arr = result.as_any().downcast_ref::<Float64Array>().unwrap();

        assert_eq!(result_arr.value(0), 0.0);
        assert_eq!(result_arr.value(1), 0.0);
        assert_eq!(result_arr.value(2), 0.0);
        assert_eq!(result_arr.value(3), 200.0);
    }

    // ===== Literal operations =====

    #[test]
    fn test_literal_addition() {
        let schema = Schema::new(vec![Field::new("a", DataType::Float64, false)]);
        let a_array = Float64Array::from(vec![10.0, 20.0, 30.0]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a_array)]).unwrap();

        let expr = Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef { table: None, column: "a".to_string() }),
            op: BinaryOperator::Plus,
            right: Box::new(Expression::Literal(SqlValue::Float(5.0))),
        };

        let result = evaluate_arithmetic_simd(&batch, &expr).unwrap();
        let result_arr = result.as_any().downcast_ref::<Float64Array>().unwrap();

        assert_eq!(result_arr.value(0), 15.0);
        assert_eq!(result_arr.value(1), 25.0);
        assert_eq!(result_arr.value(2), 35.0);
    }

    #[test]
    fn test_literal_int() {
        let schema = Schema::new(vec![Field::new("a", DataType::Int64, false)]);
        let a_array = Int64Array::from(vec![10, 20, 30]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a_array)]).unwrap();

        let expr = Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef { table: None, column: "a".to_string() }),
            op: BinaryOperator::Multiply,
            right: Box::new(Expression::Literal(SqlValue::Integer(2))),
        };

        let result = evaluate_arithmetic_simd(&batch, &expr).unwrap();
        let result_arr = result.as_any().downcast_ref::<Int64Array>().unwrap();

        assert_eq!(result_arr.value(0), 20);
        assert_eq!(result_arr.value(1), 40);
        assert_eq!(result_arr.value(2), 60);
    }

    // ===== Complex nested expressions =====

    #[test]
    fn test_deeply_nested_expression() {
        // Test: (a + b) * (c - d)
        let schema = Schema::new(vec![
            Field::new("a", DataType::Float64, false),
            Field::new("b", DataType::Float64, false),
            Field::new("c", DataType::Float64, false),
            Field::new("d", DataType::Float64, false),
        ]);
        let a_array = Float64Array::from(vec![10.0, 20.0, 30.0]);
        let b_array = Float64Array::from(vec![5.0, 10.0, 15.0]);
        let c_array = Float64Array::from(vec![100.0, 200.0, 300.0]);
        let d_array = Float64Array::from(vec![10.0, 20.0, 30.0]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(a_array), Arc::new(b_array), Arc::new(c_array), Arc::new(d_array)],
        )
        .unwrap();

        let expr = Expression::BinaryOp {
            left: Box::new(Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef { table: None, column: "a".to_string() }),
                op: BinaryOperator::Plus,
                right: Box::new(Expression::ColumnRef { table: None, column: "b".to_string() }),
            }),
            op: BinaryOperator::Multiply,
            right: Box::new(Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef { table: None, column: "c".to_string() }),
                op: BinaryOperator::Minus,
                right: Box::new(Expression::ColumnRef { table: None, column: "d".to_string() }),
            }),
        };

        let result = evaluate_arithmetic_simd(&batch, &expr).unwrap();
        let result_arr = result.as_any().downcast_ref::<Float64Array>().unwrap();

        // Expected: (10+5)*(100-10) = 15*90 = 1350
        assert!((result_arr.value(0) - 1350.0).abs() < 0.001);
        // Expected: (20+10)*(200-20) = 30*180 = 5400
        assert!((result_arr.value(1) - 5400.0).abs() < 0.001);
        // Expected: (30+15)*(300-30) = 45*270 = 12150
        assert!((result_arr.value(2) - 12150.0).abs() < 0.001);
    }

    // ===== Column reference test =====

    #[test]
    fn test_simple_column_reference() {
        let schema = Schema::new(vec![Field::new("a", DataType::Float64, false)]);
        let a_array = Float64Array::from(vec![10.0, 20.0, 30.0]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(a_array)]).unwrap();

        let expr = Expression::ColumnRef { table: None, column: "a".to_string() };

        let result = evaluate_arithmetic_simd(&batch, &expr).unwrap();
        let result_arr = result.as_any().downcast_ref::<Float64Array>().unwrap();

        assert_eq!(result_arr.value(0), 10.0);
        assert_eq!(result_arr.value(1), 20.0);
        assert_eq!(result_arr.value(2), 30.0);
    }

    // ===== NULL handling tests =====

    #[test]
    fn test_simd_null_in_left_operand() {
        // Test: NULL + price
        // Expected: All results should be NULL (NULL propagation)
        let schema = Schema::new(vec![Field::new("price", DataType::Float64, true)]);
        let price_array = Float64Array::from(vec![Some(10.0), Some(20.0), Some(30.0)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(price_array)]).unwrap();

        let expr = Expression::BinaryOp {
            left: Box::new(Expression::Literal(SqlValue::Null)),
            op: BinaryOperator::Plus,
            right: Box::new(Expression::ColumnRef { table: None, column: "price".to_string() }),
        };

        let result = evaluate_arithmetic_simd(&batch, &expr).unwrap();

        // Result could be Int64Array or Float64Array depending on type casting
        // Check using the Array trait's is_null method which works for both
        assert_eq!(result.len(), 3);
        assert!(result.is_null(0));
        assert!(result.is_null(1));
        assert!(result.is_null(2));
    }

    #[test]
    fn test_simd_null_in_right_operand() {
        // Test: price + NULL
        // Expected: All results should be NULL (NULL propagation)
        let schema = Schema::new(vec![Field::new("price", DataType::Float64, true)]);
        let price_array = Float64Array::from(vec![Some(10.0), Some(20.0), Some(30.0)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(price_array)]).unwrap();

        let expr = Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef { table: None, column: "price".to_string() }),
            op: BinaryOperator::Plus,
            right: Box::new(Expression::Literal(SqlValue::Null)),
        };

        let result = evaluate_arithmetic_simd(&batch, &expr).unwrap();

        // Result could be Int64Array or Float64Array depending on type casting
        // Check using the Array trait's is_null method which works for both
        assert_eq!(result.len(), 3);
        assert!(result.is_null(0));
        assert!(result.is_null(1));
        assert!(result.is_null(2));
    }

    #[test]
    fn test_simd_mixed_null_non_null() {
        // Test: quantity * discount where discount can be NULL
        // Expected: NULL * anything = NULL, non-NULL values computed correctly
        let schema = Schema::new(vec![
            Field::new("quantity", DataType::Int64, false),
            Field::new("discount", DataType::Float64, true),
        ]);
        let quantity_array = Int64Array::from(vec![10, 20, 30, 40]);
        // discount: [0.1, NULL, 0.2, NULL]
        let discount_array = Float64Array::from(vec![Some(0.1), None, Some(0.2), None]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(quantity_array), Arc::new(discount_array)],
        )
        .unwrap();

        let expr = Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef { table: None, column: "quantity".to_string() }),
            op: BinaryOperator::Multiply,
            right: Box::new(Expression::ColumnRef { table: None, column: "discount".to_string() }),
        };

        let result = evaluate_arithmetic_simd(&batch, &expr).unwrap();
        let result_arr = result.as_any().downcast_ref::<Float64Array>().unwrap();

        // Expected: [1.0, NULL, 6.0, NULL]
        assert_eq!(result_arr.len(), 4);
        assert!(!result_arr.is_null(0));
        assert!((result_arr.value(0) - 1.0).abs() < 0.001);
        assert!(result_arr.is_null(1)); // NULL discount propagates
        assert!(!result_arr.is_null(2));
        assert!((result_arr.value(2) - 6.0).abs() < 0.001);
        assert!(result_arr.is_null(3)); // NULL discount propagates
    }

    #[test]
    fn test_simd_all_nulls() {
        // Test: NULL + NULL
        // Expected: All results are NULL
        let schema = Schema::new(vec![Field::new("dummy", DataType::Int64, true)]);
        let dummy_array = Int64Array::from(vec![Some(1), Some(2), Some(3)]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(dummy_array)]).unwrap();

        let expr = Expression::BinaryOp {
            left: Box::new(Expression::Literal(SqlValue::Null)),
            op: BinaryOperator::Plus,
            right: Box::new(Expression::Literal(SqlValue::Null)),
        };

        let result = evaluate_arithmetic_simd(&batch, &expr).unwrap();

        // Result type is Int64Array (both operands are NULL literals which default to Int64)
        // Check using the Array trait's is_null method
        assert_eq!(result.len(), 3);
        assert!(result.is_null(0));
        assert!(result.is_null(1));
        assert!(result.is_null(2));
    }

    #[test]
    fn test_simd_nested_null_propagation() {
        // Test: price * (1 - discount) where discount can be NULL
        // Expected: NULL in nested expression propagates to final result
        let schema = Schema::new(vec![
            Field::new("price", DataType::Float64, false),
            Field::new("discount", DataType::Float64, true),
        ]);
        let price_array = Float64Array::from(vec![100.0, 200.0, 300.0, 400.0]);
        // discount: [0.1, NULL, 0.15, NULL]
        let discount_array = Float64Array::from(vec![Some(0.1), None, Some(0.15), None]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(price_array), Arc::new(discount_array)],
        )
        .unwrap();

        // price * (1 - discount)
        let expr = Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef { table: None, column: "price".to_string() }),
            op: BinaryOperator::Multiply,
            right: Box::new(Expression::BinaryOp {
                left: Box::new(Expression::Literal(SqlValue::Float(1.0))),
                op: BinaryOperator::Minus,
                right: Box::new(Expression::ColumnRef {
                    table: None,
                    column: "discount".to_string(),
                }),
            }),
        };

        let result = evaluate_arithmetic_simd(&batch, &expr).unwrap();
        let result_arr = result.as_any().downcast_ref::<Float64Array>().unwrap();

        // Expected: [90.0, NULL, 255.0, NULL]
        assert_eq!(result_arr.len(), 4);
        assert!(!result_arr.is_null(0));
        assert!((result_arr.value(0) - 90.0).abs() < 0.001);
        assert!(result_arr.is_null(1)); // NULL discount propagates through nested expression
        assert!(!result_arr.is_null(2));
        assert!((result_arr.value(2) - 255.0).abs() < 0.001);
        assert!(result_arr.is_null(3)); // NULL discount propagates through nested expression
    }

    #[test]
    fn test_simd_division_with_nulls() {
        // Test: price / quantity with NULLs
        // Expected: NULL propagation and correct division for non-NULL values
        let schema = Schema::new(vec![
            Field::new("price", DataType::Float64, true),
            Field::new("quantity", DataType::Int64, true),
        ]);
        let price_array = Float64Array::from(vec![Some(100.0), None, Some(300.0), Some(400.0)]);
        let quantity_array = Int64Array::from(vec![Some(2), Some(5), None, Some(8)]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(price_array), Arc::new(quantity_array)],
        )
        .unwrap();

        let expr = Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef { table: None, column: "price".to_string() }),
            op: BinaryOperator::Divide,
            right: Box::new(Expression::ColumnRef { table: None, column: "quantity".to_string() }),
        };

        let result = evaluate_arithmetic_simd(&batch, &expr).unwrap();
        let result_arr = result.as_any().downcast_ref::<Float64Array>().unwrap();

        // Expected: [50.0, NULL, NULL, 50.0]
        assert_eq!(result_arr.len(), 4);
        assert!(!result_arr.is_null(0));
        assert!((result_arr.value(0) - 50.0).abs() < 0.001);
        assert!(result_arr.is_null(1)); // NULL price
        assert!(result_arr.is_null(2)); // NULL quantity
        assert!(!result_arr.is_null(3));
        assert!((result_arr.value(3) - 50.0).abs() < 0.001);
    }

    #[test]
    fn test_simd_subtraction_with_nulls() {
        // Test: revenue - cost with NULLs
        let schema = Schema::new(vec![
            Field::new("revenue", DataType::Float64, true),
            Field::new("cost", DataType::Float64, true),
        ]);
        let revenue_array =
            Float64Array::from(vec![Some(1000.0), Some(2000.0), None, Some(4000.0)]);
        let cost_array = Float64Array::from(vec![Some(600.0), None, Some(1500.0), Some(2500.0)]);
        let batch = RecordBatch::try_new(
            Arc::new(schema),
            vec![Arc::new(revenue_array), Arc::new(cost_array)],
        )
        .unwrap();

        let expr = Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef { table: None, column: "revenue".to_string() }),
            op: BinaryOperator::Minus,
            right: Box::new(Expression::ColumnRef { table: None, column: "cost".to_string() }),
        };

        let result = evaluate_arithmetic_simd(&batch, &expr).unwrap();
        let result_arr = result.as_any().downcast_ref::<Float64Array>().unwrap();

        // Expected: [400.0, NULL, NULL, 1500.0]
        assert_eq!(result_arr.len(), 4);
        assert!(!result_arr.is_null(0));
        assert!((result_arr.value(0) - 400.0).abs() < 0.001);
        assert!(result_arr.is_null(1)); // NULL cost
        assert!(result_arr.is_null(2)); // NULL revenue
        assert!(!result_arr.is_null(3));
        assert!((result_arr.value(3) - 1500.0).abs() < 0.001);
    }
}
