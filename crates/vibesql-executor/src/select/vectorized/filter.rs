//! SIMD-accelerated filtering using Arrow compute kernels  
//!
//! Simplified implementation using Arrow 53 scalar comparison API

use crate::errors::ExecutorError;
use arrow::array::{Array, ArrayRef, BooleanArray, Float64Array, Int64Array, StringArray, Date32Array, TimestampMicrosecondArray};
use arrow::compute::{and_kleene as and_op, or_kleene as or_op, not as not_op, filter_record_batch, like};
use arrow::compute::kernels::cmp::{eq, neq, lt, lt_eq, gt, gt_eq};
use arrow::array::Scalar;
use arrow::datatypes::DataType;
use arrow::record_batch::RecordBatch;
use arrow::datatypes::TimeUnit;
use vibesql_ast::{BinaryOperator, Expression};
use vibesql_types::SqlValue;

/// Apply WHERE clause filter to a RecordBatch using SIMD operations
pub fn filter_record_batch_simd(
    batch: &RecordBatch,
    predicate: &Expression,
) -> Result<RecordBatch, ExecutorError> {
    let mask = evaluate_predicate_simd(batch, predicate)?;
    filter_record_batch(batch, &mask)
        .map_err(|e| ExecutorError::Other(format!("SIMD filter failed: {}", e)))
}

/// Evaluate a predicate expression on a RecordBatch
fn evaluate_predicate_simd(
    batch: &RecordBatch,
    expr: &Expression,
) -> Result<BooleanArray, ExecutorError> {
    match expr {
        Expression::BinaryOp { left, op, right } => {
            evaluate_binary_op_simd(batch, left, op, right)
        }
        Expression::Literal(value) => {
            match value {
                SqlValue::Boolean(b) => Ok(BooleanArray::from(vec![*b; batch.num_rows()])),
                _ => Err(ExecutorError::Other("Non-boolean literal in WHERE clause".to_string())),
            }
        }
        Expression::ColumnRef { column, .. } => {
            get_boolean_column(batch, column)
        }
        Expression::UnaryOp { op, expr } => {
            match op {
                vibesql_ast::UnaryOperator::Not => {
                    let expr_mask = evaluate_predicate_simd(batch, expr)?;
                    not_op(&expr_mask).map_err(|e| ExecutorError::Other(format!("SIMD NOT failed: {}", e)))
                }
                _ => Err(ExecutorError::Other(format!("Unsupported unary operator: {:?}", op))),
            }
        }
        Expression::Like { expr, pattern, negated } => {
            evaluate_like_simd(batch, expr, pattern, *negated)
        }
        _ => Err(ExecutorError::Other(format!("Unsupported SIMD predicate: {:?}", expr))),
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
            and_op(&left_mask, &right_mask).map_err(|e| ExecutorError::Other(format!("SIMD AND failed: {}", e)))
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
            or_op(&left_mask, &right_mask).map_err(|e| ExecutorError::Other(format!("SIMD OR failed: {}", e)))
        }
        BinaryOperator::Equal | BinaryOperator::NotEqual | BinaryOperator::LessThan
        | BinaryOperator::LessThanOrEqual | BinaryOperator::GreaterThan | BinaryOperator::GreaterThanOrEqual => {
            evaluate_comparison_simd(batch, left, op, right)
        }
        _ => Err(ExecutorError::Other(format!("Unsupported SIMD binary operator: {:?}", op))),
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
        _ => return Err(ExecutorError::Other("SIMD comparison requires: column <op> literal".to_string())),
    };

    let schema = batch.schema();
    let (col_idx, _) = schema.column_with_name(col_name)
        .ok_or_else(|| ExecutorError::Other(format!("Column not found: {}", col_name)))?;
    let column = batch.column(col_idx);

    match column.data_type() {
        arrow::datatypes::DataType::Int64 => compare_int64(column, literal_value, op),
        arrow::datatypes::DataType::Float64 => compare_float64(column, literal_value, op),
        arrow::datatypes::DataType::Utf8 => compare_string(column, literal_value, op),
        arrow::datatypes::DataType::Date32 => compare_date32(column, literal_value, op),
        arrow::datatypes::DataType::Timestamp(TimeUnit::Microsecond, None) => compare_timestamp(column, literal_value, op),
        _ => Err(ExecutorError::Other(format!("Unsupported column type: {:?}", column.data_type()))),
    }
}

fn compare_int64(column: &ArrayRef, literal: &SqlValue, op: &BinaryOperator) -> Result<BooleanArray, ExecutorError> {
    let array = column.as_any().downcast_ref::<Int64Array>()
        .ok_or_else(|| ExecutorError::Other("Failed to downcast Int64Array".to_string()))?;

    let val = match literal {
        SqlValue::Integer(i) | SqlValue::Bigint(i) => *i,
        SqlValue::Smallint(i) => *i as i64,
        _ => return Err(ExecutorError::Other("Type mismatch".to_string())),
    };

    // Create scalar array for comparison
    let scalar_array = Int64Array::from(vec![val; array.len()]);

    let result = match op {
        BinaryOperator::Equal => eq(array, &scalar_array)
            .map_err(|e| ExecutorError::Other(format!("SIMD eq failed: {}", e)))?,
        BinaryOperator::NotEqual => neq(array, &scalar_array)
            .map_err(|e| ExecutorError::Other(format!("SIMD neq failed: {}", e)))?,
        BinaryOperator::LessThan => lt(array, &scalar_array)
            .map_err(|e| ExecutorError::Other(format!("SIMD lt failed: {}", e)))?,
        BinaryOperator::LessThanOrEqual => lt_eq(array, &scalar_array)
            .map_err(|e| ExecutorError::Other(format!("SIMD lt_eq failed: {}", e)))?,
        BinaryOperator::GreaterThan => gt(array, &scalar_array)
            .map_err(|e| ExecutorError::Other(format!("SIMD gt failed: {}", e)))?,
        BinaryOperator::GreaterThanOrEqual => gt_eq(array, &scalar_array)
            .map_err(|e| ExecutorError::Other(format!("SIMD gt_eq failed: {}", e)))?,
        _ => return Err(ExecutorError::Other("Unsupported operator".to_string())),
    };

    Ok(result)
}

fn compare_float64(column: &ArrayRef, literal: &SqlValue, op: &BinaryOperator) -> Result<BooleanArray, ExecutorError> {
    let array = column.as_any().downcast_ref::<Float64Array>()
        .ok_or_else(|| ExecutorError::Other("Failed to downcast Float64Array".to_string()))?;

    let val = match literal {
        SqlValue::Double(f) | SqlValue::Numeric(f) => *f,
        SqlValue::Float(f) | SqlValue::Real(f) => *f as f64,
        SqlValue::Integer(i) => *i as f64,
        _ => return Err(ExecutorError::Other("Type mismatch".to_string())),
    };

    // Create scalar array for comparison
    let scalar_array = Float64Array::from(vec![val; array.len()]);

    let result = match op {
        BinaryOperator::Equal => eq(array, &scalar_array)
            .map_err(|e| ExecutorError::Other(format!("SIMD eq failed: {}", e)))?,
        BinaryOperator::NotEqual => neq(array, &scalar_array)
            .map_err(|e| ExecutorError::Other(format!("SIMD neq failed: {}", e)))?,
        BinaryOperator::LessThan => lt(array, &scalar_array)
            .map_err(|e| ExecutorError::Other(format!("SIMD lt failed: {}", e)))?,
        BinaryOperator::LessThanOrEqual => lt_eq(array, &scalar_array)
            .map_err(|e| ExecutorError::Other(format!("SIMD lt_eq failed: {}", e)))?,
        BinaryOperator::GreaterThan => gt(array, &scalar_array)
            .map_err(|e| ExecutorError::Other(format!("SIMD gt failed: {}", e)))?,
        BinaryOperator::GreaterThanOrEqual => gt_eq(array, &scalar_array)
            .map_err(|e| ExecutorError::Other(format!("SIMD gt_eq failed: {}", e)))?,
        _ => return Err(ExecutorError::Other("Unsupported operator".to_string())),
    };

    Ok(result)
}

fn compare_string(column: &ArrayRef, literal: &SqlValue, op: &BinaryOperator) -> Result<BooleanArray, ExecutorError> {
    let array = column.as_any().downcast_ref::<StringArray>()
        .ok_or_else(|| ExecutorError::Other("Failed to downcast StringArray".to_string()))?;

    let val = match literal {
        SqlValue::Varchar(s) | SqlValue::Character(s) => s.as_str(),
        _ => return Err(ExecutorError::Other("Type mismatch".to_string())),
    };

    // Create scalar array for comparison
    let scalar_array = StringArray::from(vec![val; array.len()]);

    let result = match op {
        BinaryOperator::Equal => eq(array, &scalar_array)
            .map_err(|e| ExecutorError::Other(format!("SIMD eq failed: {}", e)))?,
        BinaryOperator::NotEqual => neq(array, &scalar_array)
            .map_err(|e| ExecutorError::Other(format!("SIMD neq failed: {}", e)))?,
        BinaryOperator::LessThan => lt(array, &scalar_array)
            .map_err(|e| ExecutorError::Other(format!("SIMD lt failed: {}", e)))?,
        BinaryOperator::LessThanOrEqual => lt_eq(array, &scalar_array)
            .map_err(|e| ExecutorError::Other(format!("SIMD lt_eq failed: {}", e)))?,
        BinaryOperator::GreaterThan => gt(array, &scalar_array)
            .map_err(|e| ExecutorError::Other(format!("SIMD gt failed: {}", e)))?,
        BinaryOperator::GreaterThanOrEqual => gt_eq(array, &scalar_array)
            .map_err(|e| ExecutorError::Other(format!("SIMD gt_eq failed: {}", e)))?,
        _ => return Err(ExecutorError::Other("Unsupported operator".to_string())),
    };

    Ok(result)
}

fn compare_date32(column: &ArrayRef, literal: &SqlValue, op: &BinaryOperator) -> Result<BooleanArray, ExecutorError> {
    use super::batch::date_to_days_since_epoch;

    let array = column.as_any().downcast_ref::<Date32Array>()
        .ok_or_else(|| ExecutorError::Other("Failed to downcast Date32Array".to_string()))?;

    let val = match literal {
        SqlValue::Date(d) => date_to_days_since_epoch(d),
        _ => return Err(ExecutorError::Other("Type mismatch: expected Date".to_string())),
    };

    // Create scalar array for comparison
    let scalar_array = Date32Array::from(vec![val; array.len()]);

    let result = match op {
        BinaryOperator::Equal => eq(array, &scalar_array)
            .map_err(|e| ExecutorError::Other(format!("SIMD eq failed: {}", e)))?,
        BinaryOperator::NotEqual => neq(array, &scalar_array)
            .map_err(|e| ExecutorError::Other(format!("SIMD neq failed: {}", e)))?,
        BinaryOperator::LessThan => lt(array, &scalar_array)
            .map_err(|e| ExecutorError::Other(format!("SIMD lt failed: {}", e)))?,
        BinaryOperator::LessThanOrEqual => lt_eq(array, &scalar_array)
            .map_err(|e| ExecutorError::Other(format!("SIMD lt_eq failed: {}", e)))?,
        BinaryOperator::GreaterThan => gt(array, &scalar_array)
            .map_err(|e| ExecutorError::Other(format!("SIMD gt failed: {}", e)))?,
        BinaryOperator::GreaterThanOrEqual => gt_eq(array, &scalar_array)
            .map_err(|e| ExecutorError::Other(format!("SIMD gt_eq failed: {}", e)))?,
        _ => return Err(ExecutorError::Other("Unsupported operator".to_string())),
    };

    Ok(result)
}

fn compare_timestamp(column: &ArrayRef, literal: &SqlValue, op: &BinaryOperator) -> Result<BooleanArray, ExecutorError> {
    use super::batch::timestamp_to_microseconds;

    let array = column.as_any().downcast_ref::<TimestampMicrosecondArray>()
        .ok_or_else(|| ExecutorError::Other("Failed to downcast TimestampMicrosecondArray".to_string()))?;

    let val = match literal {
        SqlValue::Timestamp(ts) => timestamp_to_microseconds(ts),
        _ => return Err(ExecutorError::Other("Type mismatch: expected Timestamp".to_string())),
    };

    // Create scalar array for comparison
    let scalar_array = TimestampMicrosecondArray::from(vec![val; array.len()]);

    let result = match op {
        BinaryOperator::Equal => eq(array, &scalar_array)
            .map_err(|e| ExecutorError::Other(format!("SIMD eq failed: {}", e)))?,
        BinaryOperator::NotEqual => neq(array, &scalar_array)
            .map_err(|e| ExecutorError::Other(format!("SIMD neq failed: {}", e)))?,
        BinaryOperator::LessThan => lt(array, &scalar_array)
            .map_err(|e| ExecutorError::Other(format!("SIMD lt failed: {}", e)))?,
        BinaryOperator::LessThanOrEqual => lt_eq(array, &scalar_array)
            .map_err(|e| ExecutorError::Other(format!("SIMD lt_eq failed: {}", e)))?,
        BinaryOperator::GreaterThan => gt(array, &scalar_array)
            .map_err(|e| ExecutorError::Other(format!("SIMD gt failed: {}", e)))?,
        BinaryOperator::GreaterThanOrEqual => gt_eq(array, &scalar_array)
            .map_err(|e| ExecutorError::Other(format!("SIMD gt_eq failed: {}", e)))?,
        _ => return Err(ExecutorError::Other("Unsupported operator".to_string())),
    };

    Ok(result)
}

fn get_boolean_column(batch: &RecordBatch, col_name: &str) -> Result<BooleanArray, ExecutorError> {
    let schema = batch.schema();
    let (col_idx, _) = schema.column_with_name(col_name)
        .ok_or_else(|| ExecutorError::Other(format!("Column not found: {}", col_name)))?;
    let column = batch.column(col_idx);
    let array = column.as_any().downcast_ref::<BooleanArray>()
        .ok_or_else(|| ExecutorError::Other("Column is not boolean type".to_string()))?;
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
        _ => return Err(ExecutorError::Other("SIMD LIKE requires: column LIKE literal".to_string())),
    };

    // Extract pattern string from literal
    let pattern_str = match pattern {
        Expression::Literal(SqlValue::Varchar(s)) | Expression::Literal(SqlValue::Character(s)) => s.as_str(),
        Expression::Literal(SqlValue::Null) => {
            // NULL pattern matches nothing - return all false
            return Ok(BooleanArray::from(vec![false; batch.num_rows()]));
        }
        _ => return Err(ExecutorError::Other("SIMD LIKE pattern must be a string literal".to_string())),
    };

    // Get the string column
    let schema = batch.schema();
    let (col_idx, _) = schema.column_with_name(col_name)
        .ok_or_else(|| ExecutorError::Other(format!("Column not found: {}", col_name)))?;
    let column = batch.column(col_idx);

    // Ensure column is string type
    if !matches!(column.data_type(), arrow::datatypes::DataType::Utf8) {
        return Err(ExecutorError::Other(format!("LIKE requires string column, got: {:?}", column.data_type())));
    }

    let string_array = column.as_any().downcast_ref::<StringArray>()
        .ok_or_else(|| ExecutorError::Other("Failed to downcast StringArray".to_string()))?;

    // Use Arrow's SIMD-accelerated like kernel
    // Create a StringArray with a single value for the pattern
    let pattern_array = StringArray::from(vec![pattern_str]);
    let pattern_scalar = Scalar::new(&pattern_array);
    let result = like(string_array, &pattern_scalar)
        .map_err(|e| ExecutorError::Other(format!("SIMD LIKE failed: {}", e)))?;

    // Apply negation if needed
    if negated {
        not_op(&result).map_err(|e| ExecutorError::Other(format!("SIMD NOT failed: {}", e)))
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
            pattern: Box::new(Expression::Literal(SqlValue::Varchar("Al%".to_string()))),
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
            pattern: Box::new(Expression::Literal(SqlValue::Varchar("%lie".to_string()))),
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
            pattern: Box::new(Expression::Literal(SqlValue::Varchar("Al%".to_string()))),
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
            pattern: Box::new(Expression::Literal(SqlValue::Varchar("%li%".to_string()))),
            negated: false,
        };

        let filtered = filter_record_batch_simd(&batch, &predicate).unwrap();
        // Should match: Alice, Charlie, Julie (3 rows)
        assert_eq!(filtered.num_rows(), 3);
    }
}
