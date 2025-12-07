//! SIMD-accelerated filtering using Arrow compute kernels  
//!
//! Simplified implementation using Arrow 53 scalar comparison API

use crate::errors::ExecutorError;
use arrow::array::{
    Array, ArrayRef, BooleanArray, Date32Array, Float64Array, Int64Array, Scalar, StringArray,
    TimestampMicrosecondArray,
};
use arrow::compute::kernels::cmp::{eq, gt, gt_eq, lt, lt_eq, neq};
use arrow::compute::{
    and_kleene as and_op, filter_record_batch, like, not as not_op, or_kleene as or_op,
};
use arrow::datatypes::TimeUnit;
use arrow::record_batch::RecordBatch;
use vibesql_ast::{BinaryOperator, Expression};
use vibesql_types::SqlValue;

/// Apply WHERE clause filter to a RecordBatch using SIMD operations
pub fn filter_record_batch_simd(
    batch: &RecordBatch,
    predicate: &Expression,
) -> Result<RecordBatch, ExecutorError> {
    let mask = evaluate_predicate_simd(batch, predicate)?;
    filter_record_batch(batch, &mask).map_err(|e| ExecutorError::SimdOperationFailed {
        operation: "filter".to_string(),
        reason: e.to_string(),
    })
}

/// Evaluate a predicate expression on a RecordBatch
fn evaluate_predicate_simd(
    batch: &RecordBatch,
    expr: &Expression,
) -> Result<BooleanArray, ExecutorError> {
    match expr {
        Expression::BinaryOp { left, op, right } => evaluate_binary_op_simd(batch, left, op, right),
        Expression::Literal(value) => match value {
            SqlValue::Boolean(b) => Ok(BooleanArray::from(vec![*b; batch.num_rows()])),
            _ => Err(ExecutorError::ColumnarTypeMismatch {
                operation: "WHERE clause literal".to_string(),
                left_type: format!("{:?}", value),
                right_type: Some("Boolean".to_string()),
            }),
        },
        Expression::ColumnRef { column, .. } => get_boolean_column(batch, column),
        Expression::UnaryOp { op, expr } => match op {
            vibesql_ast::UnaryOperator::Not => {
                let expr_mask = evaluate_predicate_simd(batch, expr)?;
                not_op(&expr_mask).map_err(|e| ExecutorError::SimdOperationFailed {
                    operation: "NOT".to_string(),
                    reason: e.to_string(),
                })
            }
            _ => Err(ExecutorError::UnsupportedFeature(format!(
                "unary operator {:?} in SIMD predicate",
                op
            ))),
        },
        Expression::Like { expr, pattern, negated } => {
            evaluate_like_simd(batch, expr, pattern, *negated)
        }
        _ => {
            Err(ExecutorError::UnsupportedFeature(format!("SIMD predicate expression: {:?}", expr)))
        }
    }
}

/// Evaluate a binary operation with short-circuit optimization
fn evaluate_binary_op_simd(
    batch: &RecordBatch,
    left: &Expression,
    op: &BinaryOperator,
    right: &Expression,
) -> Result<BooleanArray, ExecutorError> {
    match op {
        BinaryOperator::And => {
            // Evaluate left side first
            let left_mask = evaluate_predicate_simd(batch, left)?;

            // Short-circuit optimization: if left mask is all-false, skip right evaluation
            if is_all_false(&left_mask) {
                return Ok(left_mask);
            }

            // Evaluate right side
            let right_mask = evaluate_predicate_simd(batch, right)?;
            and_op(&left_mask, &right_mask).map_err(|e| ExecutorError::SimdOperationFailed {
                operation: "AND".to_string(),
                reason: e.to_string(),
            })
        }
        BinaryOperator::Or => {
            // Evaluate left side first
            let left_mask = evaluate_predicate_simd(batch, left)?;

            // Short-circuit optimization: if left mask is all-true, skip right evaluation
            if is_all_true(&left_mask) {
                return Ok(left_mask);
            }

            // Evaluate right side
            let right_mask = evaluate_predicate_simd(batch, right)?;
            or_op(&left_mask, &right_mask).map_err(|e| ExecutorError::SimdOperationFailed {
                operation: "OR".to_string(),
                reason: e.to_string(),
            })
        }
        BinaryOperator::Equal
        | BinaryOperator::NotEqual
        | BinaryOperator::LessThan
        | BinaryOperator::LessThanOrEqual
        | BinaryOperator::GreaterThan
        | BinaryOperator::GreaterThanOrEqual => evaluate_comparison_simd(batch, left, op, right),
        _ => Err(ExecutorError::UnsupportedFeature(format!("SIMD binary operator: {:?}", op))),
    }
}

/// Check if a boolean array is all false (for AND short-circuit)
#[inline]
fn is_all_false(mask: &BooleanArray) -> bool {
    mask.true_count() == 0
}

/// Check if a boolean array is all true (for OR short-circuit)
#[inline]
fn is_all_true(mask: &BooleanArray) -> bool {
    mask.true_count() == mask.len()
}

/// Evaluate a comparison operation
fn evaluate_comparison_simd(
    batch: &RecordBatch,
    left: &Expression,
    op: &BinaryOperator,
    right: &Expression,
) -> Result<BooleanArray, ExecutorError> {
    let (col_name, literal_value) = match (left, right) {
        (Expression::ColumnRef { column, .. }, Expression::Literal(val)) => (column, val),
        _ => {
            return Err(ExecutorError::UnsupportedFeature(
                "SIMD comparison requires: column <op> literal".to_string(),
            ))
        }
    };

    let schema = batch.schema();
    let (col_idx, _) =
        schema.column_with_name(col_name).ok_or_else(|| ExecutorError::ColumnarColumnNotFound {
            column_index: 0, // Column referenced by name, not index
            batch_columns: schema.fields().len(),
        })?;
    let column = batch.column(col_idx);

    match column.data_type() {
        arrow::datatypes::DataType::Int64 => compare_int64(column, literal_value, op),
        arrow::datatypes::DataType::Float64 => compare_float64(column, literal_value, op),
        arrow::datatypes::DataType::Utf8 => compare_string(column, literal_value, op),
        arrow::datatypes::DataType::Date32 => compare_date32(column, literal_value, op),
        arrow::datatypes::DataType::Timestamp(TimeUnit::Microsecond, None) => {
            compare_timestamp(column, literal_value, op)
        }
        _ => Err(ExecutorError::UnsupportedArrayType {
            operation: "comparison".to_string(),
            array_type: format!("{:?}", column.data_type()),
        }),
    }
}

fn compare_int64(
    column: &ArrayRef,
    literal: &SqlValue,
    op: &BinaryOperator,
) -> Result<BooleanArray, ExecutorError> {
    let array = column.as_any().downcast_ref::<Int64Array>().ok_or_else(|| {
        ExecutorError::ArrowDowncastError {
            expected_type: "Int64Array".to_string(),
            context: "compare_int64".to_string(),
        }
    })?;

    let val = match literal {
        SqlValue::Integer(i) | SqlValue::Bigint(i) => *i,
        SqlValue::Smallint(i) => *i as i64,
        _ => {
            return Err(ExecutorError::ColumnarTypeMismatch {
                operation: "Int64 comparison".to_string(),
                left_type: "Int64".to_string(),
                right_type: Some(format!("{:?}", literal)),
            })
        }
    };

    // Create scalar array for comparison
    let scalar_array = Int64Array::from(vec![val; array.len()]);

    let result = match op {
        BinaryOperator::Equal => {
            eq(array, &scalar_array).map_err(|e| ExecutorError::SimdOperationFailed {
                operation: "eq".to_string(),
                reason: e.to_string(),
            })?
        }
        BinaryOperator::NotEqual => {
            neq(array, &scalar_array).map_err(|e| ExecutorError::SimdOperationFailed {
                operation: "neq".to_string(),
                reason: e.to_string(),
            })?
        }
        BinaryOperator::LessThan => {
            lt(array, &scalar_array).map_err(|e| ExecutorError::SimdOperationFailed {
                operation: "lt".to_string(),
                reason: e.to_string(),
            })?
        }
        BinaryOperator::LessThanOrEqual => {
            lt_eq(array, &scalar_array).map_err(|e| ExecutorError::SimdOperationFailed {
                operation: "lt_eq".to_string(),
                reason: e.to_string(),
            })?
        }
        BinaryOperator::GreaterThan => {
            gt(array, &scalar_array).map_err(|e| ExecutorError::SimdOperationFailed {
                operation: "gt".to_string(),
                reason: e.to_string(),
            })?
        }
        BinaryOperator::GreaterThanOrEqual => {
            gt_eq(array, &scalar_array).map_err(|e| ExecutorError::SimdOperationFailed {
                operation: "gt_eq".to_string(),
                reason: e.to_string(),
            })?
        }
        _ => {
            return Err(ExecutorError::UnsupportedFeature(format!("comparison operator {:?}", op)))
        }
    };

    Ok(result)
}

fn compare_float64(
    column: &ArrayRef,
    literal: &SqlValue,
    op: &BinaryOperator,
) -> Result<BooleanArray, ExecutorError> {
    let array = column.as_any().downcast_ref::<Float64Array>().ok_or_else(|| {
        ExecutorError::ArrowDowncastError {
            expected_type: "Float64Array".to_string(),
            context: "compare_float64".to_string(),
        }
    })?;

    let val = match literal {
        SqlValue::Double(f) | SqlValue::Numeric(f) => *f,
        SqlValue::Float(f) | SqlValue::Real(f) => *f as f64,
        SqlValue::Integer(i) => *i as f64,
        _ => {
            return Err(ExecutorError::ColumnarTypeMismatch {
                operation: "Float64 comparison".to_string(),
                left_type: "Float64".to_string(),
                right_type: Some(format!("{:?}", literal)),
            })
        }
    };

    // Create scalar array for comparison
    let scalar_array = Float64Array::from(vec![val; array.len()]);

    let result = match op {
        BinaryOperator::Equal => {
            eq(array, &scalar_array).map_err(|e| ExecutorError::SimdOperationFailed {
                operation: "eq".to_string(),
                reason: e.to_string(),
            })?
        }
        BinaryOperator::NotEqual => {
            neq(array, &scalar_array).map_err(|e| ExecutorError::SimdOperationFailed {
                operation: "neq".to_string(),
                reason: e.to_string(),
            })?
        }
        BinaryOperator::LessThan => {
            lt(array, &scalar_array).map_err(|e| ExecutorError::SimdOperationFailed {
                operation: "lt".to_string(),
                reason: e.to_string(),
            })?
        }
        BinaryOperator::LessThanOrEqual => {
            lt_eq(array, &scalar_array).map_err(|e| ExecutorError::SimdOperationFailed {
                operation: "lt_eq".to_string(),
                reason: e.to_string(),
            })?
        }
        BinaryOperator::GreaterThan => {
            gt(array, &scalar_array).map_err(|e| ExecutorError::SimdOperationFailed {
                operation: "gt".to_string(),
                reason: e.to_string(),
            })?
        }
        BinaryOperator::GreaterThanOrEqual => {
            gt_eq(array, &scalar_array).map_err(|e| ExecutorError::SimdOperationFailed {
                operation: "gt_eq".to_string(),
                reason: e.to_string(),
            })?
        }
        _ => {
            return Err(ExecutorError::UnsupportedFeature(format!("comparison operator {:?}", op)))
        }
    };

    Ok(result)
}

fn compare_string(
    column: &ArrayRef,
    literal: &SqlValue,
    op: &BinaryOperator,
) -> Result<BooleanArray, ExecutorError> {
    let array = column.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
        ExecutorError::ArrowDowncastError {
            expected_type: "StringArray".to_string(),
            context: "compare_string".to_string(),
        }
    })?;

    let val = match literal {
        SqlValue::Varchar(s) | SqlValue::Character(s) => &**s,
        _ => {
            return Err(ExecutorError::ColumnarTypeMismatch {
                operation: "String comparison".to_string(),
                left_type: "String".to_string(),
                right_type: Some(format!("{:?}", literal)),
            })
        }
    };

    // Create scalar array for comparison
    let scalar_array = StringArray::from(vec![val; array.len()]);

    let result = match op {
        BinaryOperator::Equal => {
            eq(array, &scalar_array).map_err(|e| ExecutorError::SimdOperationFailed {
                operation: "eq".to_string(),
                reason: e.to_string(),
            })?
        }
        BinaryOperator::NotEqual => {
            neq(array, &scalar_array).map_err(|e| ExecutorError::SimdOperationFailed {
                operation: "neq".to_string(),
                reason: e.to_string(),
            })?
        }
        BinaryOperator::LessThan => {
            lt(array, &scalar_array).map_err(|e| ExecutorError::SimdOperationFailed {
                operation: "lt".to_string(),
                reason: e.to_string(),
            })?
        }
        BinaryOperator::LessThanOrEqual => {
            lt_eq(array, &scalar_array).map_err(|e| ExecutorError::SimdOperationFailed {
                operation: "lt_eq".to_string(),
                reason: e.to_string(),
            })?
        }
        BinaryOperator::GreaterThan => {
            gt(array, &scalar_array).map_err(|e| ExecutorError::SimdOperationFailed {
                operation: "gt".to_string(),
                reason: e.to_string(),
            })?
        }
        BinaryOperator::GreaterThanOrEqual => {
            gt_eq(array, &scalar_array).map_err(|e| ExecutorError::SimdOperationFailed {
                operation: "gt_eq".to_string(),
                reason: e.to_string(),
            })?
        }
        _ => {
            return Err(ExecutorError::UnsupportedFeature(format!("comparison operator {:?}", op)))
        }
    };

    Ok(result)
}

fn compare_date32(
    column: &ArrayRef,
    literal: &SqlValue,
    op: &BinaryOperator,
) -> Result<BooleanArray, ExecutorError> {
    use super::batch::date_to_days_since_epoch;

    let array = column.as_any().downcast_ref::<Date32Array>().ok_or_else(|| {
        ExecutorError::ArrowDowncastError {
            expected_type: "Date32Array".to_string(),
            context: "compare_date32".to_string(),
        }
    })?;

    let val = match literal {
        SqlValue::Date(d) => date_to_days_since_epoch(d),
        _ => {
            return Err(ExecutorError::ColumnarTypeMismatch {
                operation: "Date comparison".to_string(),
                left_type: "Date32".to_string(),
                right_type: Some(format!("{:?}", literal)),
            })
        }
    };

    // Create scalar array for comparison
    let scalar_array = Date32Array::from(vec![val; array.len()]);

    let result = match op {
        BinaryOperator::Equal => {
            eq(array, &scalar_array).map_err(|e| ExecutorError::SimdOperationFailed {
                operation: "eq".to_string(),
                reason: e.to_string(),
            })?
        }
        BinaryOperator::NotEqual => {
            neq(array, &scalar_array).map_err(|e| ExecutorError::SimdOperationFailed {
                operation: "neq".to_string(),
                reason: e.to_string(),
            })?
        }
        BinaryOperator::LessThan => {
            lt(array, &scalar_array).map_err(|e| ExecutorError::SimdOperationFailed {
                operation: "lt".to_string(),
                reason: e.to_string(),
            })?
        }
        BinaryOperator::LessThanOrEqual => {
            lt_eq(array, &scalar_array).map_err(|e| ExecutorError::SimdOperationFailed {
                operation: "lt_eq".to_string(),
                reason: e.to_string(),
            })?
        }
        BinaryOperator::GreaterThan => {
            gt(array, &scalar_array).map_err(|e| ExecutorError::SimdOperationFailed {
                operation: "gt".to_string(),
                reason: e.to_string(),
            })?
        }
        BinaryOperator::GreaterThanOrEqual => {
            gt_eq(array, &scalar_array).map_err(|e| ExecutorError::SimdOperationFailed {
                operation: "gt_eq".to_string(),
                reason: e.to_string(),
            })?
        }
        _ => {
            return Err(ExecutorError::UnsupportedFeature(format!("comparison operator {:?}", op)))
        }
    };

    Ok(result)
}

fn compare_timestamp(
    column: &ArrayRef,
    literal: &SqlValue,
    op: &BinaryOperator,
) -> Result<BooleanArray, ExecutorError> {
    use super::batch::timestamp_to_microseconds;

    let array = column.as_any().downcast_ref::<TimestampMicrosecondArray>().ok_or_else(|| {
        ExecutorError::ArrowDowncastError {
            expected_type: "TimestampMicrosecondArray".to_string(),
            context: "compare_timestamp".to_string(),
        }
    })?;

    let val = match literal {
        SqlValue::Timestamp(ts) => timestamp_to_microseconds(ts),
        _ => {
            return Err(ExecutorError::ColumnarTypeMismatch {
                operation: "Timestamp comparison".to_string(),
                left_type: "Timestamp".to_string(),
                right_type: Some(format!("{:?}", literal)),
            })
        }
    };

    // Create scalar array for comparison
    let scalar_array = TimestampMicrosecondArray::from(vec![val; array.len()]);

    let result = match op {
        BinaryOperator::Equal => {
            eq(array, &scalar_array).map_err(|e| ExecutorError::SimdOperationFailed {
                operation: "eq".to_string(),
                reason: e.to_string(),
            })?
        }
        BinaryOperator::NotEqual => {
            neq(array, &scalar_array).map_err(|e| ExecutorError::SimdOperationFailed {
                operation: "neq".to_string(),
                reason: e.to_string(),
            })?
        }
        BinaryOperator::LessThan => {
            lt(array, &scalar_array).map_err(|e| ExecutorError::SimdOperationFailed {
                operation: "lt".to_string(),
                reason: e.to_string(),
            })?
        }
        BinaryOperator::LessThanOrEqual => {
            lt_eq(array, &scalar_array).map_err(|e| ExecutorError::SimdOperationFailed {
                operation: "lt_eq".to_string(),
                reason: e.to_string(),
            })?
        }
        BinaryOperator::GreaterThan => {
            gt(array, &scalar_array).map_err(|e| ExecutorError::SimdOperationFailed {
                operation: "gt".to_string(),
                reason: e.to_string(),
            })?
        }
        BinaryOperator::GreaterThanOrEqual => {
            gt_eq(array, &scalar_array).map_err(|e| ExecutorError::SimdOperationFailed {
                operation: "gt_eq".to_string(),
                reason: e.to_string(),
            })?
        }
        _ => {
            return Err(ExecutorError::UnsupportedFeature(format!("comparison operator {:?}", op)))
        }
    };

    Ok(result)
}

fn get_boolean_column(batch: &RecordBatch, col_name: &str) -> Result<BooleanArray, ExecutorError> {
    let schema = batch.schema();
    let (col_idx, _) =
        schema.column_with_name(col_name).ok_or_else(|| ExecutorError::ColumnarColumnNotFound {
            column_index: 0, // Column referenced by name
            batch_columns: schema.fields().len(),
        })?;
    let column = batch.column(col_idx);
    let array = column.as_any().downcast_ref::<BooleanArray>().ok_or_else(|| {
        ExecutorError::ArrowDowncastError {
            expected_type: "BooleanArray".to_string(),
            context: "get_boolean_column".to_string(),
        }
    })?;
    Ok(array.clone())
}

/// Evaluate a LIKE pattern match using SIMD
fn evaluate_like_simd(
    batch: &RecordBatch,
    expr: &Expression,
    pattern: &Expression,
    negated: bool,
) -> Result<BooleanArray, ExecutorError> {
    // Extract column name from expression
    let col_name = match expr {
        Expression::ColumnRef { column, .. } => column,
        _ => {
            return Err(ExecutorError::UnsupportedFeature(
                "SIMD LIKE requires: column LIKE literal".to_string(),
            ))
        }
    };

    // Extract pattern string from literal
    let pattern_str = match pattern {
        Expression::Literal(SqlValue::Varchar(s)) | Expression::Literal(SqlValue::Character(s)) => {
            &**s
        }
        Expression::Literal(SqlValue::Null) => {
            // NULL pattern matches nothing - return all false
            return Ok(BooleanArray::from(vec![false; batch.num_rows()]));
        }
        _ => {
            return Err(ExecutorError::ColumnarTypeMismatch {
                operation: "LIKE pattern".to_string(),
                left_type: "String".to_string(),
                right_type: Some(format!("{:?}", pattern)),
            })
        }
    };

    // Get the string column
    let schema = batch.schema();
    let (col_idx, _) =
        schema.column_with_name(col_name).ok_or_else(|| ExecutorError::ColumnarColumnNotFound {
            column_index: 0, // Column referenced by name
            batch_columns: schema.fields().len(),
        })?;
    let column = batch.column(col_idx);

    // Ensure column is string type
    if !matches!(column.data_type(), arrow::datatypes::DataType::Utf8) {
        return Err(ExecutorError::UnsupportedArrayType {
            operation: "LIKE".to_string(),
            array_type: format!("{:?}", column.data_type()),
        });
    }

    let string_array = column.as_any().downcast_ref::<StringArray>().ok_or_else(|| {
        ExecutorError::ArrowDowncastError {
            expected_type: "StringArray".to_string(),
            context: "evaluate_like_simd".to_string(),
        }
    })?;

    // Use Arrow's SIMD-accelerated like kernel
    let pattern_scalar = Scalar::new(StringArray::from(vec![pattern_str]));
    let result = like(string_array, &pattern_scalar).map_err(|e| {
        ExecutorError::SimdOperationFailed { operation: "LIKE".to_string(), reason: e.to_string() }
    })?;

    // Apply negation if needed
    if negated {
        not_op(&result).map_err(|e| ExecutorError::SimdOperationFailed {
            operation: "NOT".to_string(),
            reason: e.to_string(),
        })
    } else {
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use std::sync::Arc;

    #[test]
    fn test_simd_filter_int64_gt() {
        let schema = Schema::new(vec![Field::new("value", DataType::Int64, false)]);
        let array = Int64Array::from(vec![1, 2, 3, 4, 5]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array)]).unwrap();

        let predicate = Expression::BinaryOp {
            left: Box::new(Expression::ColumnRef { table: None, column: "value".to_string() }),
            op: BinaryOperator::GreaterThan,
            right: Box::new(Expression::Literal(SqlValue::Integer(3))),
        };

        let filtered = filter_record_batch_simd(&batch, &predicate).unwrap();
        assert_eq!(filtered.num_rows(), 2);
    }

    #[test]
    fn test_short_circuit_and_all_false() {
        // Test that AND short-circuits when left side is all false
        let schema = Schema::new(vec![Field::new("value", DataType::Int64, false)]);
        let array = Int64Array::from(vec![1, 2, 3, 4, 5]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array)]).unwrap();

        // value > 10 (all false) AND value < 100 (would be all true)
        let predicate = Expression::BinaryOp {
            left: Box::new(Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef { table: None, column: "value".to_string() }),
                op: BinaryOperator::GreaterThan,
                right: Box::new(Expression::Literal(SqlValue::Integer(10))),
            }),
            op: BinaryOperator::And,
            right: Box::new(Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef { table: None, column: "value".to_string() }),
                op: BinaryOperator::LessThan,
                right: Box::new(Expression::Literal(SqlValue::Integer(100))),
            }),
        };

        let filtered = filter_record_batch_simd(&batch, &predicate).unwrap();
        // Result should be 0 rows (all false)
        assert_eq!(filtered.num_rows(), 0);
    }

    #[test]
    fn test_short_circuit_or_all_true() {
        // Test that OR short-circuits when left side is all true
        let schema = Schema::new(vec![Field::new("value", DataType::Int64, false)]);
        let array = Int64Array::from(vec![1, 2, 3, 4, 5]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array)]).unwrap();

        // value < 100 (all true) OR value > 10 (would be all false)
        let predicate = Expression::BinaryOp {
            left: Box::new(Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef { table: None, column: "value".to_string() }),
                op: BinaryOperator::LessThan,
                right: Box::new(Expression::Literal(SqlValue::Integer(100))),
            }),
            op: BinaryOperator::Or,
            right: Box::new(Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef { table: None, column: "value".to_string() }),
                op: BinaryOperator::GreaterThan,
                right: Box::new(Expression::Literal(SqlValue::Integer(10))),
            }),
        };

        let filtered = filter_record_batch_simd(&batch, &predicate).unwrap();
        // Result should be 5 rows (all true)
        assert_eq!(filtered.num_rows(), 5);
    }

    #[test]
    fn test_combined_predicates() {
        // Test combined AND predicates
        let schema = Schema::new(vec![Field::new("value", DataType::Int64, false)]);
        let array = Int64Array::from(vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array)]).unwrap();

        // value > 3 AND value < 8
        let predicate = Expression::BinaryOp {
            left: Box::new(Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef { table: None, column: "value".to_string() }),
                op: BinaryOperator::GreaterThan,
                right: Box::new(Expression::Literal(SqlValue::Integer(3))),
            }),
            op: BinaryOperator::And,
            right: Box::new(Expression::BinaryOp {
                left: Box::new(Expression::ColumnRef { table: None, column: "value".to_string() }),
                op: BinaryOperator::LessThan,
                right: Box::new(Expression::Literal(SqlValue::Integer(8))),
            }),
        };

        let filtered = filter_record_batch_simd(&batch, &predicate).unwrap();
        // Should match values: 4, 5, 6, 7 (4 rows)
        assert_eq!(filtered.num_rows(), 4);
    }

    #[test]
    fn test_simd_like_prefix_pattern() {
        // Test LIKE with prefix pattern (Al%)
        let schema = Schema::new(vec![Field::new("name", DataType::Utf8, false)]);
        let array = StringArray::from(vec!["Alice", "Bob", "Alex", "Charlie"]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array)]).unwrap();

        // name LIKE 'Al%'
        let predicate = Expression::Like {
            expr: Box::new(Expression::ColumnRef { table: None, column: "name".to_string() }),
            pattern: Box::new(Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("Al%")))),
            negated: false,
        };

        let filtered = filter_record_batch_simd(&batch, &predicate).unwrap();
        // Should match: Alice, Alex (2 rows)
        assert_eq!(filtered.num_rows(), 2);

        // Verify the matched values
        let result_array = filtered.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(result_array.value(0), "Alice");
        assert_eq!(result_array.value(1), "Alex");
    }

    #[test]
    fn test_simd_like_suffix_pattern() {
        // Test LIKE with suffix pattern (%lie)
        let schema = Schema::new(vec![Field::new("name", DataType::Utf8, false)]);
        let array = StringArray::from(vec!["Alice", "Bob", "Charlie", "Julie"]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array)]).unwrap();

        // name LIKE '%lie'
        let predicate = Expression::Like {
            expr: Box::new(Expression::ColumnRef { table: None, column: "name".to_string() }),
            pattern: Box::new(Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("%lie")))),
            negated: false,
        };

        let filtered = filter_record_batch_simd(&batch, &predicate).unwrap();
        // Should match: Charlie, Julie (2 rows)
        assert_eq!(filtered.num_rows(), 2);
    }

    #[test]
    fn test_simd_not_like() {
        // Test NOT LIKE
        let schema = Schema::new(vec![Field::new("name", DataType::Utf8, false)]);
        let array = StringArray::from(vec!["Alice", "Bob", "Alex", "Charlie"]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array)]).unwrap();

        // name NOT LIKE 'Al%'
        let predicate = Expression::Like {
            expr: Box::new(Expression::ColumnRef { table: None, column: "name".to_string() }),
            pattern: Box::new(Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("Al%")))),
            negated: true,
        };

        let filtered = filter_record_batch_simd(&batch, &predicate).unwrap();
        // Should match: Bob, Charlie (2 rows)
        assert_eq!(filtered.num_rows(), 2);

        let result_array = filtered.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(result_array.value(0), "Bob");
        assert_eq!(result_array.value(1), "Charlie");
    }

    #[test]
    fn test_simd_like_contains_pattern() {
        // Test LIKE with contains pattern (%li%)
        let schema = Schema::new(vec![Field::new("name", DataType::Utf8, false)]);
        let array = StringArray::from(vec!["Alice", "Bob", "Charlie", "Julie", "David"]);
        let batch = RecordBatch::try_new(Arc::new(schema), vec![Arc::new(array)]).unwrap();

        // name LIKE '%li%'
        let predicate = Expression::Like {
            expr: Box::new(Expression::ColumnRef { table: None, column: "name".to_string() }),
            pattern: Box::new(Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from("%li%")))),
            negated: false,
        };

        let filtered = filter_record_batch_simd(&batch, &predicate).unwrap();
        // Should match: Alice, Charlie, Julie (3 rows)
        assert_eq!(filtered.num_rows(), 3);
    }
}
