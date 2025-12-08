//! Auto-vectorized filtering for columnar batches
//!
//! Uses the centralized simd_ops module for consistent, optimized operations.
//!
//! ## Module Structure
//!
//! - `comparison` - Numeric comparison operations (i64, i32, f64)
//! - `string` - String-specific filter operations
//! - `mask` - Filter mask application to batches
//! - `conversion` - Value type conversion utilities

mod comparison;
mod conversion;
mod mask;
mod string;

use super::batch::{ColumnArray, ColumnarBatch};
use super::filter::{evaluate_column_compare, ColumnPredicate, CompareOp};
use super::simd_ops::{self, PackedMask};
use crate::errors::ExecutorError;
use vibesql_types::SqlValue;

use comparison::{
    evaluate_predicate_f64_packed, evaluate_predicate_f64_simd, evaluate_predicate_i32_packed,
    evaluate_predicate_i32_simd, evaluate_predicate_i64_packed, evaluate_predicate_i64_simd,
};
use mask::apply_filter_mask;
use string::evaluate_predicate_string_batch;

/// Check if any value in a predicate is NULL
/// Per SQL standard, any comparison with NULL returns UNKNOWN (treated as false in WHERE)
fn predicate_contains_null(predicate: &ColumnPredicate) -> bool {
    match predicate {
        ColumnPredicate::LessThan { value, .. }
        | ColumnPredicate::GreaterThan { value, .. }
        | ColumnPredicate::GreaterThanOrEqual { value, .. }
        | ColumnPredicate::LessThanOrEqual { value, .. }
        | ColumnPredicate::Equal { value, .. }
        | ColumnPredicate::NotEqual { value, .. } => matches!(value, SqlValue::Null),
        ColumnPredicate::Between { low, high, .. } => {
            matches!(low, SqlValue::Null) || matches!(high, SqlValue::Null)
        }
        // LIKE patterns don't have NULL values in the pattern itself
        ColumnPredicate::Like { .. } => false,
        // IN list may contain NULL values
        ColumnPredicate::InList { values, .. } => {
            values.iter().any(|v| matches!(v, SqlValue::Null))
        }
        // Column-to-column comparisons don't have literal NULLs
        // (NULL columns are handled during evaluation)
        ColumnPredicate::ColumnCompare { .. } => false,
    }
}

/// Apply SIMD-accelerated filtering to a columnar batch
///
/// Returns a new batch containing only the rows that pass all predicates.
/// Uses SIMD operations for numeric columns when possible, falls back to
/// scalar evaluation for other column types.
///
/// # Arguments
///
/// * `batch` - The columnar batch to filter
/// * `predicates` - Column-based predicates to evaluate
///
/// # Returns
///
/// A new ColumnarBatch containing only rows that pass all predicates
pub fn simd_filter_batch(
    batch: &ColumnarBatch,
    predicates: &[ColumnPredicate],
) -> Result<ColumnarBatch, ExecutorError> {
    if predicates.is_empty() {
        // No predicates: return a clone of the batch
        return Ok(batch.clone());
    }

    // Create filter bitmap using SIMD operations
    let filter_mask = simd_create_filter_mask(batch, predicates)?;

    // Apply filter mask to batch
    apply_filter_mask(batch, &filter_mask)
}

/// Create a filter mask using SIMD operations where possible
///
/// Returns a Vec<bool> where true means the row passes all predicates.
/// This function uses SIMD operations for numeric columns (i64/f64) and
/// falls back to scalar evaluation for other types.
///
/// This function is public to enable fused filter+aggregate optimization,
/// where the filter mask is used directly for aggregation without creating
/// an intermediate filtered batch.
pub fn simd_create_filter_mask(
    batch: &ColumnarBatch,
    predicates: &[ColumnPredicate],
) -> Result<Vec<bool>, ExecutorError> {
    let row_count = batch.row_count();

    // Start with all rows passing
    let mut mask = vec![true; row_count];

    // Evaluate each predicate and AND the results using vectorized operation
    for predicate in predicates {
        let predicate_mask = evaluate_predicate_simd(batch, predicate)?;

        // Vectorized AND with existing mask
        simd_ops::and_masks_inplace(&mut mask, &predicate_mask);
    }

    Ok(mask)
}

/// Create a filter mask using packed bitmasks for improved efficiency.
///
/// This function provides 8x memory reduction compared to `simd_create_filter_mask`
/// by using packed bitmasks (1 bit per row) instead of Vec<bool> (1 byte per row).
///
/// # Performance Benefits
///
/// - **8x memory reduction**: For 6M rows, uses 750KB instead of 6MB
/// - **Native SIMD bitwise AND**: Combining predicates uses hardware SIMD instructions
/// - **Better cache utilization**: Smaller footprint means better cache hit rates
/// - **Faster popcount**: `count_ones()` maps to hardware popcount instruction
///
/// # Usage
///
/// This function is designed for use with fused filter+aggregate optimization:
///
/// ```text
/// let filter_mask = simd_create_filter_mask_packed(batch, predicates)?;
/// let sum = simd_ops::sum_f64_packed_filtered(values, &filter_mask);
/// let count = filter_mask.count_ones();
/// ```
pub fn simd_create_filter_mask_packed(
    batch: &ColumnarBatch,
    predicates: &[ColumnPredicate],
) -> Result<PackedMask, ExecutorError> {
    let row_count = batch.row_count();

    // Start with all rows passing
    let mut mask = PackedMask::new_all_set(row_count);

    // Evaluate each predicate and AND the results
    for predicate in predicates {
        let predicate_mask = evaluate_predicate_simd_packed(batch, predicate)?;

        // Bitwise AND - this is a native SIMD operation
        mask.and_inplace(&predicate_mask);
    }

    Ok(mask)
}

/// Evaluate a single predicate returning a packed mask
fn evaluate_predicate_simd_packed(
    batch: &ColumnarBatch,
    predicate: &ColumnPredicate,
) -> Result<PackedMask, ExecutorError> {
    // NULL handling: any comparison with NULL returns false (UNKNOWN in SQL)
    if predicate_contains_null(predicate) {
        return Ok(PackedMask::new_all_clear(batch.row_count()));
    }

    // Handle ColumnCompare specially - needs two columns
    if let ColumnPredicate::ColumnCompare { left_column_idx, op, right_column_idx } = predicate {
        let bool_mask = evaluate_column_compare_simd(batch, *left_column_idx, *op, *right_column_idx)?;
        return Ok(PackedMask::from_bool_slice(&bool_mask));
    }

    let column_idx = match predicate {
        ColumnPredicate::LessThan { column_idx, .. }
        | ColumnPredicate::GreaterThan { column_idx, .. }
        | ColumnPredicate::GreaterThanOrEqual { column_idx, .. }
        | ColumnPredicate::LessThanOrEqual { column_idx, .. }
        | ColumnPredicate::Equal { column_idx, .. }
        | ColumnPredicate::NotEqual { column_idx, .. }
        | ColumnPredicate::Between { column_idx, .. }
        | ColumnPredicate::Like { column_idx, .. }
        | ColumnPredicate::InList { column_idx, .. } => *column_idx,
        ColumnPredicate::ColumnCompare { .. } => unreachable!(), // Handled above
    };

    let column = batch.column(column_idx).ok_or_else(|| ExecutorError::ColumnarColumnNotFound {
        column_index: column_idx,
        batch_columns: batch.column_count(),
    })?;

    match column {
        // Packed path for i64 columns
        ColumnArray::Int64(values, nulls) => {
            evaluate_predicate_i64_packed(predicate, values, nulls.as_ref().map(|n| n.as_slice()))
        }

        // Packed path for f64 columns
        ColumnArray::Float64(values, nulls) => {
            evaluate_predicate_f64_packed(predicate, values, nulls.as_ref().map(|n| n.as_slice()))
        }

        // Packed path for Date columns (i32)
        ColumnArray::Date(values, nulls) => {
            evaluate_predicate_i32_packed(predicate, values, nulls.as_ref().map(|n| n.as_slice()))
        }

        // Packed path for Timestamp columns (i64)
        ColumnArray::Timestamp(values, nulls) => {
            evaluate_predicate_i64_packed(predicate, values, nulls.as_ref().map(|n| n.as_slice()))
        }

        // For other types, fall back to Vec<bool> and convert
        _ => {
            let bool_mask = evaluate_predicate_simd(batch, predicate)?;
            Ok(PackedMask::from_bool_slice(&bool_mask))
        }
    }
}

/// Evaluate a single predicate using SIMD operations when possible
fn evaluate_predicate_simd(
    batch: &ColumnarBatch,
    predicate: &ColumnPredicate,
) -> Result<Vec<bool>, ExecutorError> {
    // NULL handling: any comparison with NULL returns false (UNKNOWN in SQL)
    // Return all-false mask immediately if predicate contains NULL literal
    if predicate_contains_null(predicate) {
        return Ok(vec![false; batch.row_count()]);
    }

    // Handle ColumnCompare specially - needs two columns
    if let ColumnPredicate::ColumnCompare { left_column_idx, op, right_column_idx } = predicate {
        return evaluate_column_compare_simd(batch, *left_column_idx, *op, *right_column_idx);
    }

    let column_idx = match predicate {
        ColumnPredicate::LessThan { column_idx, .. }
        | ColumnPredicate::GreaterThan { column_idx, .. }
        | ColumnPredicate::GreaterThanOrEqual { column_idx, .. }
        | ColumnPredicate::LessThanOrEqual { column_idx, .. }
        | ColumnPredicate::Equal { column_idx, .. }
        | ColumnPredicate::NotEqual { column_idx, .. }
        | ColumnPredicate::Between { column_idx, .. }
        | ColumnPredicate::Like { column_idx, .. }
        | ColumnPredicate::InList { column_idx, .. } => *column_idx,
        ColumnPredicate::ColumnCompare { .. } => unreachable!(), // Handled above
    };

    let column = batch.column(column_idx).ok_or_else(|| ExecutorError::ColumnarColumnNotFound {
        column_index: column_idx,
        batch_columns: batch.column_count(),
    })?;

    match column {
        // SIMD path for i64 columns
        ColumnArray::Int64(values, nulls) => {
            evaluate_predicate_i64_simd(predicate, values, nulls.as_ref().map(|n| n.as_slice()))
        }

        // SIMD path for f64 columns
        ColumnArray::Float64(values, nulls) => {
            evaluate_predicate_f64_simd(predicate, values, nulls.as_ref().map(|n| n.as_slice()))
        }

        // SIMD path for Date columns (i32 - days since epoch)
        ColumnArray::Date(values, nulls) => {
            evaluate_predicate_i32_simd(predicate, values, nulls.as_ref().map(|n| n.as_slice()))
        }

        // SIMD path for Timestamp columns (i64 - microseconds since epoch)
        ColumnArray::Timestamp(values, nulls) => {
            evaluate_predicate_i64_simd(predicate, values, nulls.as_ref().map(|n| n.as_slice()))
        }

        // Batch string operations for String columns
        ColumnArray::String(values, nulls) => {
            evaluate_predicate_string_batch(predicate, values, nulls.as_ref().map(|n| n.as_slice()))
        }

        // Batch string operations for FixedString columns
        ColumnArray::FixedString(values, nulls) => {
            evaluate_predicate_string_batch(predicate, values, nulls.as_ref().map(|n| n.as_slice()))
        }

        // Scalar fallback for other column types
        _ => evaluate_predicate_scalar(batch, predicate, column_idx),
    }
}

/// Evaluate a column-to-column comparison using vectorized operations where possible
///
/// This is the main optimization for predicates like `l_commitdate < l_receiptdate` in TPC-H Q4.
/// For Date columns (i32), this uses a tight scalar loop that the compiler can auto-vectorize.
fn evaluate_column_compare_simd(
    batch: &ColumnarBatch,
    left_column_idx: usize,
    op: CompareOp,
    right_column_idx: usize,
) -> Result<Vec<bool>, ExecutorError> {
    let row_count = batch.row_count();

    let left_column =
        batch.column(left_column_idx).ok_or_else(|| ExecutorError::ColumnarColumnNotFound {
            column_index: left_column_idx,
            batch_columns: batch.column_count(),
        })?;

    let right_column =
        batch.column(right_column_idx).ok_or_else(|| ExecutorError::ColumnarColumnNotFound {
            column_index: right_column_idx,
            batch_columns: batch.column_count(),
        })?;

    // Try SIMD path for matching column types
    match (left_column, right_column) {
        // Date columns (i32) - common case for TPC-H Q4 (l_commitdate < l_receiptdate)
        (ColumnArray::Date(left_values, left_nulls), ColumnArray::Date(right_values, right_nulls)) => {
            evaluate_column_compare_i32_simd(
                left_values,
                left_nulls.as_ref().map(|n| n.as_slice()),
                op,
                right_values,
                right_nulls.as_ref().map(|n| n.as_slice()),
            )
        }

        // Int64 columns
        (ColumnArray::Int64(left_values, left_nulls), ColumnArray::Int64(right_values, right_nulls)) => {
            evaluate_column_compare_i64_simd(
                left_values,
                left_nulls.as_ref().map(|n| n.as_slice()),
                op,
                right_values,
                right_nulls.as_ref().map(|n| n.as_slice()),
            )
        }

        // Float64 columns
        (ColumnArray::Float64(left_values, left_nulls), ColumnArray::Float64(right_values, right_nulls)) => {
            evaluate_column_compare_f64_simd(
                left_values,
                left_nulls.as_ref().map(|n| n.as_slice()),
                op,
                right_values,
                right_nulls.as_ref().map(|n| n.as_slice()),
            )
        }

        // Timestamp columns (i64)
        (ColumnArray::Timestamp(left_values, left_nulls), ColumnArray::Timestamp(right_values, right_nulls)) => {
            evaluate_column_compare_i64_simd(
                left_values,
                left_nulls.as_ref().map(|n| n.as_slice()),
                op,
                right_values,
                right_nulls.as_ref().map(|n| n.as_slice()),
            )
        }

        // Fallback to scalar evaluation for other types or mismatched types
        _ => {
            let mut result = Vec::with_capacity(row_count);
            for row_idx in 0..row_count {
                let left_val = batch.get_value(row_idx, left_column_idx)?;
                let right_val = batch.get_value(row_idx, right_column_idx)?;
                let passes = evaluate_column_compare(op, Some(&left_val), Some(&right_val));
                result.push(passes);
            }
            Ok(result)
        }
    }
}

/// Evaluate column-to-column comparison for i32 arrays (Date columns)
/// Uses a tight loop that LLVM can auto-vectorize
fn evaluate_column_compare_i32_simd(
    left_values: &[i32],
    left_nulls: Option<&[bool]>,
    op: CompareOp,
    right_values: &[i32],
    right_nulls: Option<&[bool]>,
) -> Result<Vec<bool>, ExecutorError> {
    let len = left_values.len();
    let mut result = vec![false; len];

    // Pre-compute null masks
    let has_left_nulls = left_nulls.is_some();
    let has_right_nulls = right_nulls.is_some();

    match op {
        CompareOp::LessThan => {
            for i in 0..len {
                let is_null = (has_left_nulls && left_nulls.unwrap()[i])
                    || (has_right_nulls && right_nulls.unwrap()[i]);
                result[i] = !is_null && left_values[i] < right_values[i];
            }
        }
        CompareOp::GreaterThan => {
            for i in 0..len {
                let is_null = (has_left_nulls && left_nulls.unwrap()[i])
                    || (has_right_nulls && right_nulls.unwrap()[i]);
                result[i] = !is_null && left_values[i] > right_values[i];
            }
        }
        CompareOp::LessThanOrEqual => {
            for i in 0..len {
                let is_null = (has_left_nulls && left_nulls.unwrap()[i])
                    || (has_right_nulls && right_nulls.unwrap()[i]);
                result[i] = !is_null && left_values[i] <= right_values[i];
            }
        }
        CompareOp::GreaterThanOrEqual => {
            for i in 0..len {
                let is_null = (has_left_nulls && left_nulls.unwrap()[i])
                    || (has_right_nulls && right_nulls.unwrap()[i]);
                result[i] = !is_null && left_values[i] >= right_values[i];
            }
        }
        CompareOp::Equal => {
            for i in 0..len {
                let is_null = (has_left_nulls && left_nulls.unwrap()[i])
                    || (has_right_nulls && right_nulls.unwrap()[i]);
                result[i] = !is_null && left_values[i] == right_values[i];
            }
        }
        CompareOp::NotEqual => {
            for i in 0..len {
                let is_null = (has_left_nulls && left_nulls.unwrap()[i])
                    || (has_right_nulls && right_nulls.unwrap()[i]);
                result[i] = !is_null && left_values[i] != right_values[i];
            }
        }
    }

    Ok(result)
}

/// Evaluate column-to-column comparison for i64 arrays
fn evaluate_column_compare_i64_simd(
    left_values: &[i64],
    left_nulls: Option<&[bool]>,
    op: CompareOp,
    right_values: &[i64],
    right_nulls: Option<&[bool]>,
) -> Result<Vec<bool>, ExecutorError> {
    let len = left_values.len();
    let mut result = vec![false; len];

    let has_left_nulls = left_nulls.is_some();
    let has_right_nulls = right_nulls.is_some();

    match op {
        CompareOp::LessThan => {
            for i in 0..len {
                let is_null = (has_left_nulls && left_nulls.unwrap()[i])
                    || (has_right_nulls && right_nulls.unwrap()[i]);
                result[i] = !is_null && left_values[i] < right_values[i];
            }
        }
        CompareOp::GreaterThan => {
            for i in 0..len {
                let is_null = (has_left_nulls && left_nulls.unwrap()[i])
                    || (has_right_nulls && right_nulls.unwrap()[i]);
                result[i] = !is_null && left_values[i] > right_values[i];
            }
        }
        CompareOp::LessThanOrEqual => {
            for i in 0..len {
                let is_null = (has_left_nulls && left_nulls.unwrap()[i])
                    || (has_right_nulls && right_nulls.unwrap()[i]);
                result[i] = !is_null && left_values[i] <= right_values[i];
            }
        }
        CompareOp::GreaterThanOrEqual => {
            for i in 0..len {
                let is_null = (has_left_nulls && left_nulls.unwrap()[i])
                    || (has_right_nulls && right_nulls.unwrap()[i]);
                result[i] = !is_null && left_values[i] >= right_values[i];
            }
        }
        CompareOp::Equal => {
            for i in 0..len {
                let is_null = (has_left_nulls && left_nulls.unwrap()[i])
                    || (has_right_nulls && right_nulls.unwrap()[i]);
                result[i] = !is_null && left_values[i] == right_values[i];
            }
        }
        CompareOp::NotEqual => {
            for i in 0..len {
                let is_null = (has_left_nulls && left_nulls.unwrap()[i])
                    || (has_right_nulls && right_nulls.unwrap()[i]);
                result[i] = !is_null && left_values[i] != right_values[i];
            }
        }
    }

    Ok(result)
}

/// Evaluate column-to-column comparison for f64 arrays
fn evaluate_column_compare_f64_simd(
    left_values: &[f64],
    left_nulls: Option<&[bool]>,
    op: CompareOp,
    right_values: &[f64],
    right_nulls: Option<&[bool]>,
) -> Result<Vec<bool>, ExecutorError> {
    let len = left_values.len();
    let mut result = vec![false; len];

    let has_left_nulls = left_nulls.is_some();
    let has_right_nulls = right_nulls.is_some();

    match op {
        CompareOp::LessThan => {
            for i in 0..len {
                let is_null = (has_left_nulls && left_nulls.unwrap()[i])
                    || (has_right_nulls && right_nulls.unwrap()[i]);
                result[i] = !is_null && left_values[i] < right_values[i];
            }
        }
        CompareOp::GreaterThan => {
            for i in 0..len {
                let is_null = (has_left_nulls && left_nulls.unwrap()[i])
                    || (has_right_nulls && right_nulls.unwrap()[i]);
                result[i] = !is_null && left_values[i] > right_values[i];
            }
        }
        CompareOp::LessThanOrEqual => {
            for i in 0..len {
                let is_null = (has_left_nulls && left_nulls.unwrap()[i])
                    || (has_right_nulls && right_nulls.unwrap()[i]);
                result[i] = !is_null && left_values[i] <= right_values[i];
            }
        }
        CompareOp::GreaterThanOrEqual => {
            for i in 0..len {
                let is_null = (has_left_nulls && left_nulls.unwrap()[i])
                    || (has_right_nulls && right_nulls.unwrap()[i]);
                result[i] = !is_null && left_values[i] >= right_values[i];
            }
        }
        CompareOp::Equal => {
            for i in 0..len {
                let is_null = (has_left_nulls && left_nulls.unwrap()[i])
                    || (has_right_nulls && right_nulls.unwrap()[i]);
                result[i] = !is_null && left_values[i] == right_values[i];
            }
        }
        CompareOp::NotEqual => {
            for i in 0..len {
                let is_null = (has_left_nulls && left_nulls.unwrap()[i])
                    || (has_right_nulls && right_nulls.unwrap()[i]);
                result[i] = !is_null && left_values[i] != right_values[i];
            }
        }
    }

    Ok(result)
}

/// Scalar fallback for non-numeric columns
fn evaluate_predicate_scalar(
    batch: &ColumnarBatch,
    predicate: &ColumnPredicate,
    column_idx: usize,
) -> Result<Vec<bool>, ExecutorError> {
    let row_count = batch.row_count();
    let mut result = Vec::with_capacity(row_count);

    for row_idx in 0..row_count {
        let value = batch.get_value(row_idx, column_idx)?;

        // NULL values always fail
        if value == SqlValue::Null {
            result.push(false);
            continue;
        }

        let passes = super::filter::evaluate_predicate(predicate, &value);
        result.push(passes);
    }

    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use vibesql_storage::Row;

    #[test]
    fn test_simd_filter_i64() {
        // Create a batch with i64 column
        let rows = vec![
            Row::new(vec![SqlValue::Integer(5)]),
            Row::new(vec![SqlValue::Integer(10)]),
            Row::new(vec![SqlValue::Integer(15)]),
            Row::new(vec![SqlValue::Integer(20)]),
            Row::new(vec![SqlValue::Integer(25)]),
        ];

        let batch = ColumnarBatch::from_rows(&rows).unwrap();

        // Filter: column_0 < 18
        let predicates =
            vec![ColumnPredicate::LessThan { column_idx: 0, value: SqlValue::Integer(18) }];

        let filtered = simd_filter_batch(&batch, &predicates).unwrap();

        assert_eq!(filtered.row_count(), 3);
        assert_eq!(filtered.get_value(0, 0).unwrap(), SqlValue::Integer(5));
        assert_eq!(filtered.get_value(1, 0).unwrap(), SqlValue::Integer(10));
        assert_eq!(filtered.get_value(2, 0).unwrap(), SqlValue::Integer(15));
    }

    #[test]
    fn test_simd_filter_f64() {
        // Create a batch with f64 column
        let rows = vec![
            Row::new(vec![SqlValue::Double(0.04)]),
            Row::new(vec![SqlValue::Double(0.05)]),
            Row::new(vec![SqlValue::Double(0.06)]),
            Row::new(vec![SqlValue::Double(0.07)]),
            Row::new(vec![SqlValue::Double(0.08)]),
        ];

        let batch = ColumnarBatch::from_rows(&rows).unwrap();

        // Filter: column_0 BETWEEN 0.05 AND 0.07
        let predicates = vec![ColumnPredicate::Between {
            column_idx: 0,
            low: SqlValue::Double(0.05),
            high: SqlValue::Double(0.07),
        }];

        let filtered = simd_filter_batch(&batch, &predicates).unwrap();

        assert_eq!(filtered.row_count(), 3);
        assert_eq!(filtered.get_value(0, 0).unwrap(), SqlValue::Double(0.05));
        assert_eq!(filtered.get_value(1, 0).unwrap(), SqlValue::Double(0.06));
        assert_eq!(filtered.get_value(2, 0).unwrap(), SqlValue::Double(0.07));
    }

    #[test]
    fn test_simd_filter_multiple_predicates() {
        // Create a batch with two columns
        let rows = vec![
            Row::new(vec![SqlValue::Integer(5), SqlValue::Double(0.04)]),
            Row::new(vec![SqlValue::Integer(10), SqlValue::Double(0.05)]),
            Row::new(vec![SqlValue::Integer(15), SqlValue::Double(0.06)]),
            Row::new(vec![SqlValue::Integer(20), SqlValue::Double(0.07)]),
            Row::new(vec![SqlValue::Integer(25), SqlValue::Double(0.08)]),
        ];

        let batch = ColumnarBatch::from_rows(&rows).unwrap();

        // Filter: column_0 > 10 AND column_1 < 0.07
        let predicates = vec![
            ColumnPredicate::GreaterThan { column_idx: 0, value: SqlValue::Integer(10) },
            ColumnPredicate::LessThan { column_idx: 1, value: SqlValue::Double(0.07) },
        ];

        let filtered = simd_filter_batch(&batch, &predicates).unwrap();

        // Should only match row 2 (15, 0.06)
        assert_eq!(filtered.row_count(), 1);
        assert_eq!(filtered.get_value(0, 0).unwrap(), SqlValue::Integer(15));
        assert_eq!(filtered.get_value(0, 1).unwrap(), SqlValue::Double(0.06));
    }

    #[test]
    fn test_simd_filter_date_less_than() {
        use vibesql_types::Date;

        // Create a batch with date column
        let rows = vec![
            Row::new(vec![SqlValue::Date(Date { year: 1994, month: 1, day: 1 })]),
            Row::new(vec![SqlValue::Date(Date { year: 1995, month: 6, day: 15 })]),
            Row::new(vec![SqlValue::Date(Date { year: 1996, month: 12, day: 31 })]),
            Row::new(vec![SqlValue::Date(Date { year: 1997, month: 3, day: 10 })]),
        ];

        let batch = ColumnarBatch::from_rows(&rows).unwrap();

        // Filter: date < 1996-01-01
        let predicates = vec![ColumnPredicate::LessThan {
            column_idx: 0,
            value: SqlValue::Date(Date { year: 1996, month: 1, day: 1 }),
        }];

        let filtered = simd_filter_batch(&batch, &predicates).unwrap();

        // Should match first two rows (1994-01-01, 1995-06-15)
        assert_eq!(filtered.row_count(), 2);
    }

    #[test]
    fn test_simd_filter_date_between() {
        use vibesql_types::Date;

        // Create a batch with date column
        let rows = vec![
            Row::new(vec![SqlValue::Date(Date { year: 1994, month: 1, day: 1 })]),
            Row::new(vec![SqlValue::Date(Date { year: 1995, month: 6, day: 15 })]),
            Row::new(vec![SqlValue::Date(Date { year: 1996, month: 12, day: 31 })]),
            Row::new(vec![SqlValue::Date(Date { year: 1997, month: 3, day: 10 })]),
        ];

        let batch = ColumnarBatch::from_rows(&rows).unwrap();

        // Filter: date BETWEEN 1995-01-01 AND 1996-12-31
        let predicates = vec![ColumnPredicate::Between {
            column_idx: 0,
            low: SqlValue::Date(Date { year: 1995, month: 1, day: 1 }),
            high: SqlValue::Date(Date { year: 1996, month: 12, day: 31 }),
        }];

        let filtered = simd_filter_batch(&batch, &predicates).unwrap();

        // Should match middle two rows (1995-06-15, 1996-12-31)
        assert_eq!(filtered.row_count(), 2);
    }

    #[test]
    fn test_simd_filter_date_with_nulls() {
        use vibesql_types::Date;

        // Create a batch with date column including NULLs
        let rows = vec![
            Row::new(vec![SqlValue::Date(Date { year: 1994, month: 1, day: 1 })]),
            Row::new(vec![SqlValue::Null]),
            Row::new(vec![SqlValue::Date(Date { year: 1996, month: 12, day: 31 })]),
            Row::new(vec![SqlValue::Date(Date { year: 1997, month: 3, day: 10 })]),
        ];

        let batch = ColumnarBatch::from_rows(&rows).unwrap();

        // Filter: date >= 1996-01-01
        let predicates = vec![ColumnPredicate::GreaterThanOrEqual {
            column_idx: 0,
            value: SqlValue::Date(Date { year: 1996, month: 1, day: 1 }),
        }];

        let filtered = simd_filter_batch(&batch, &predicates).unwrap();

        // Should match last two rows (1996-12-31, 1997-03-10), NULLs excluded
        assert_eq!(filtered.row_count(), 2);
    }

    #[test]
    fn test_simd_filter_date_equal() {
        use vibesql_types::Date;

        // Create a batch with date column
        let rows = vec![
            Row::new(vec![SqlValue::Date(Date { year: 1994, month: 1, day: 1 })]),
            Row::new(vec![SqlValue::Date(Date { year: 1995, month: 6, day: 15 })]),
            Row::new(vec![SqlValue::Date(Date { year: 1995, month: 6, day: 15 })]),
            Row::new(vec![SqlValue::Date(Date { year: 1997, month: 3, day: 10 })]),
        ];

        let batch = ColumnarBatch::from_rows(&rows).unwrap();

        // Filter: date = 1995-06-15
        let predicates = vec![ColumnPredicate::Equal {
            column_idx: 0,
            value: SqlValue::Date(Date { year: 1995, month: 6, day: 15 }),
        }];

        let filtered = simd_filter_batch(&batch, &predicates).unwrap();

        // Should match two middle rows
        assert_eq!(filtered.row_count(), 2);
    }

    #[test]
    fn test_between_f64_bug() {
        // Create a batch with f64 column - matching the issue test case
        let rows = vec![
            Row::new(vec![SqlValue::Double(0.02)]), // Should pass BETWEEN 0.02 AND 0.03
            Row::new(vec![SqlValue::Double(0.03)]), // Should pass BETWEEN 0.02 AND 0.03
            Row::new(vec![SqlValue::Double(0.025)]), // Should pass BETWEEN 0.02 AND 0.03
        ];

        let batch = ColumnarBatch::from_rows(&rows).unwrap();

        // Filter: column_0 BETWEEN 0.02 AND 0.03
        let predicates = vec![ColumnPredicate::Between {
            column_idx: 0,
            low: SqlValue::Double(0.02),
            high: SqlValue::Double(0.03),
        }];

        let filtered = simd_filter_batch(&batch, &predicates).unwrap();

        // All 3 rows should pass
        assert_eq!(filtered.row_count(), 3, "All rows should pass BETWEEN 0.02 AND 0.03");
    }

    /// Reproduce the exact issue #2857 scenario
    /// The bug appears when combining:
    /// - Multiple predicates including date range and BETWEEN on float
    /// - Expression aggregate SUM(amount * fee)
    #[test]
    fn test_issue_2857_scenario() {
        use vibesql_types::Date;

        // Match the test case exactly:
        // TXN_DATE (Date), AMOUNT (Double), FEE (Double)
        // (2024-01-10, 1000.0, 0.02)
        // (2024-01-15, 2000.0, 0.03)
        // (2024-01-20, 1500.0, 0.025)
        let rows = vec![
            Row::new(vec![
                SqlValue::Date(Date { year: 2024, month: 1, day: 10 }),
                SqlValue::Double(1000.0),
                SqlValue::Double(0.02),
            ]),
            Row::new(vec![
                SqlValue::Date(Date { year: 2024, month: 1, day: 15 }),
                SqlValue::Double(2000.0),
                SqlValue::Double(0.03),
            ]),
            Row::new(vec![
                SqlValue::Date(Date { year: 2024, month: 1, day: 20 }),
                SqlValue::Double(1500.0),
                SqlValue::Double(0.025),
            ]),
        ];

        let batch = ColumnarBatch::from_rows(&rows).unwrap();

        // Predicates from the query:
        // WHERE txn_date >= '2024-01-01'
        //   AND txn_date < '2024-02-01'
        //   AND fee BETWEEN 0.02 AND 0.03
        let predicates = vec![
            ColumnPredicate::GreaterThanOrEqual {
                column_idx: 0,
                value: SqlValue::Date(Date { year: 2024, month: 1, day: 1 }),
            },
            ColumnPredicate::LessThan {
                column_idx: 0,
                value: SqlValue::Date(Date { year: 2024, month: 2, day: 1 }),
            },
            ColumnPredicate::Between {
                column_idx: 2,
                low: SqlValue::Double(0.02),
                high: SqlValue::Double(0.03),
            },
        ];

        let filtered = simd_filter_batch(&batch, &predicates).unwrap();

        // All 3 rows should pass all predicates:
        // - All dates are in Jan 2024
        // - All fees are in [0.02, 0.03] range
        assert_eq!(filtered.row_count(), 3, "All rows should pass all predicates");
    }

    /// Test with SqlValue::Numeric predicates (what the parser generates)
    #[test]
    fn test_issue_2857_with_numeric_predicates() {
        use vibesql_types::Date;

        // Data uses SqlValue::Double (from storage)
        let rows = vec![
            Row::new(vec![
                SqlValue::Date(Date { year: 2024, month: 1, day: 10 }),
                SqlValue::Double(1000.0),
                SqlValue::Double(0.02), // Data is Double
            ]),
            Row::new(vec![
                SqlValue::Date(Date { year: 2024, month: 1, day: 15 }),
                SqlValue::Double(2000.0),
                SqlValue::Double(0.03),
            ]),
            Row::new(vec![
                SqlValue::Date(Date { year: 2024, month: 1, day: 20 }),
                SqlValue::Double(1500.0),
                SqlValue::Double(0.025),
            ]),
        ];

        let batch = ColumnarBatch::from_rows(&rows).unwrap();

        // Predicates use SqlValue::Numeric (from parser)
        // This matches what happens in real queries
        let predicates = vec![
            ColumnPredicate::GreaterThanOrEqual {
                column_idx: 0,
                value: SqlValue::Date(Date { year: 2024, month: 1, day: 1 }),
            },
            ColumnPredicate::LessThan {
                column_idx: 0,
                value: SqlValue::Date(Date { year: 2024, month: 2, day: 1 }),
            },
            ColumnPredicate::Between {
                column_idx: 2,
                low: SqlValue::Numeric(0.02), // Parser generates Numeric!
                high: SqlValue::Numeric(0.03),
            },
        ];

        let filtered = simd_filter_batch(&batch, &predicates).unwrap();

        // All 3 rows should pass
        assert_eq!(
            filtered.row_count(),
            3,
            "All rows should pass (Numeric predicate on Double column)"
        );
    }

    /// Test using from_storage_columnar which is the path used by native columnar execution
    #[test]
    fn test_issue_2857_from_storage_columnar() {
        use vibesql_types::Date;

        // Create rows the same way the integration test does
        let rows = vec![
            Row::new(vec![
                SqlValue::Date(Date { year: 2024, month: 1, day: 10 }),
                SqlValue::Double(1000.0),
                SqlValue::Double(0.02),
            ]),
            Row::new(vec![
                SqlValue::Date(Date { year: 2024, month: 1, day: 15 }),
                SqlValue::Double(2000.0),
                SqlValue::Double(0.03),
            ]),
            Row::new(vec![
                SqlValue::Date(Date { year: 2024, month: 1, day: 20 }),
                SqlValue::Double(1500.0),
                SqlValue::Double(0.025),
            ]),
        ];

        let column_names = vec!["TXN_DATE".to_string(), "AMOUNT".to_string(), "FEE".to_string()];
        let storage_columnar =
            vibesql_storage::ColumnarTable::from_rows(&rows, &column_names).unwrap();

        // This is the path used by try_native_columnar_execution
        let batch = ColumnarBatch::from_storage_columnar(&storage_columnar).unwrap();

        // Predicates exactly like what the parser would generate
        // Note: Date predicates come from 'YYYY-MM-DD' string literals parsed to SqlValue::Date
        let predicates = vec![
            ColumnPredicate::GreaterThanOrEqual {
                column_idx: 0,
                value: SqlValue::Date(Date { year: 2024, month: 1, day: 1 }),
            },
            ColumnPredicate::LessThan {
                column_idx: 0,
                value: SqlValue::Date(Date { year: 2024, month: 2, day: 1 }),
            },
            ColumnPredicate::Between {
                column_idx: 2,
                low: SqlValue::Numeric(0.02), // Parser generates Numeric
                high: SqlValue::Numeric(0.03),
            },
        ];

        let filtered = simd_filter_batch(&batch, &predicates).unwrap();

        // All 3 rows should pass all predicates
        assert_eq!(filtered.row_count(), 3, "All rows should pass from_storage_columnar path");
    }

    /// Test column-to-column comparison for Date columns (TPC-H Q4 pattern)
    #[test]
    fn test_simd_filter_column_compare_date() {
        use vibesql_types::Date;

        // Test case: l_commitdate < l_receiptdate (TPC-H Q4 pattern)
        // Two date columns where we compare values within the same row
        let rows = vec![
            // Row 0: commitdate < receiptdate -> passes
            Row::new(vec![
                SqlValue::Date(Date { year: 1994, month: 6, day: 10 }), // commitdate
                SqlValue::Date(Date { year: 1994, month: 6, day: 20 }), // receiptdate
            ]),
            // Row 1: commitdate == receiptdate -> fails
            Row::new(vec![
                SqlValue::Date(Date { year: 1994, month: 6, day: 15 }),
                SqlValue::Date(Date { year: 1994, month: 6, day: 15 }),
            ]),
            // Row 2: commitdate > receiptdate -> fails
            Row::new(vec![
                SqlValue::Date(Date { year: 1994, month: 6, day: 25 }),
                SqlValue::Date(Date { year: 1994, month: 6, day: 20 }),
            ]),
            // Row 3: commitdate < receiptdate -> passes
            Row::new(vec![
                SqlValue::Date(Date { year: 1995, month: 1, day: 5 }),
                SqlValue::Date(Date { year: 1995, month: 3, day: 10 }),
            ]),
        ];

        let batch = ColumnarBatch::from_rows(&rows).unwrap();

        // Filter: col0 < col1 (commitdate < receiptdate)
        let predicates = vec![ColumnPredicate::ColumnCompare {
            left_column_idx: 0,
            op: CompareOp::LessThan,
            right_column_idx: 1,
        }];

        let filtered = simd_filter_batch(&batch, &predicates).unwrap();

        // Should match rows 0 and 3
        assert_eq!(filtered.row_count(), 2);
    }

    /// Test column-to-column comparison for Int64 columns
    #[test]
    fn test_simd_filter_column_compare_int64() {
        let rows = vec![
            // Row 0: 5 < 10 -> passes
            Row::new(vec![SqlValue::Integer(5), SqlValue::Integer(10)]),
            // Row 1: 10 < 10 -> fails
            Row::new(vec![SqlValue::Integer(10), SqlValue::Integer(10)]),
            // Row 2: 15 < 10 -> fails
            Row::new(vec![SqlValue::Integer(15), SqlValue::Integer(10)]),
            // Row 3: 3 < 20 -> passes
            Row::new(vec![SqlValue::Integer(3), SqlValue::Integer(20)]),
        ];

        let batch = ColumnarBatch::from_rows(&rows).unwrap();

        let predicates = vec![ColumnPredicate::ColumnCompare {
            left_column_idx: 0,
            op: CompareOp::LessThan,
            right_column_idx: 1,
        }];

        let filtered = simd_filter_batch(&batch, &predicates).unwrap();

        // Should match rows 0 and 3
        assert_eq!(filtered.row_count(), 2);
    }

    /// Test column-to-column comparison with NULL handling
    #[test]
    fn test_simd_filter_column_compare_with_nulls() {
        use vibesql_types::Date;

        let rows = vec![
            // Row 0: commitdate < receiptdate -> passes
            Row::new(vec![
                SqlValue::Date(Date { year: 1994, month: 6, day: 10 }),
                SqlValue::Date(Date { year: 1994, month: 6, day: 20 }),
            ]),
            // Row 1: NULL < receiptdate -> fails (NULL comparison)
            Row::new(vec![SqlValue::Null, SqlValue::Date(Date { year: 1994, month: 6, day: 20 })]),
            // Row 2: commitdate < NULL -> fails (NULL comparison)
            Row::new(vec![SqlValue::Date(Date { year: 1994, month: 6, day: 10 }), SqlValue::Null]),
            // Row 3: commitdate < receiptdate -> passes
            Row::new(vec![
                SqlValue::Date(Date { year: 1995, month: 1, day: 5 }),
                SqlValue::Date(Date { year: 1995, month: 3, day: 10 }),
            ]),
        ];

        let batch = ColumnarBatch::from_rows(&rows).unwrap();

        let predicates = vec![ColumnPredicate::ColumnCompare {
            left_column_idx: 0,
            op: CompareOp::LessThan,
            right_column_idx: 1,
        }];

        let filtered = simd_filter_batch(&batch, &predicates).unwrap();

        // Should match rows 0 and 3 (NULL rows fail)
        assert_eq!(filtered.row_count(), 2);
    }

    /// Test different comparison operators for column-to-column
    #[test]
    fn test_simd_filter_column_compare_operators() {
        let rows = vec![
            Row::new(vec![SqlValue::Integer(5), SqlValue::Integer(10)]),
            Row::new(vec![SqlValue::Integer(10), SqlValue::Integer(10)]),
            Row::new(vec![SqlValue::Integer(15), SqlValue::Integer(10)]),
        ];

        let batch = ColumnarBatch::from_rows(&rows).unwrap();

        // Test Equal
        let predicates = vec![ColumnPredicate::ColumnCompare {
            left_column_idx: 0,
            op: CompareOp::Equal,
            right_column_idx: 1,
        }];
        let filtered = simd_filter_batch(&batch, &predicates).unwrap();
        assert_eq!(filtered.row_count(), 1); // Only row 1

        // Test GreaterThan
        let predicates = vec![ColumnPredicate::ColumnCompare {
            left_column_idx: 0,
            op: CompareOp::GreaterThan,
            right_column_idx: 1,
        }];
        let filtered = simd_filter_batch(&batch, &predicates).unwrap();
        assert_eq!(filtered.row_count(), 1); // Only row 2

        // Test GreaterThanOrEqual
        let predicates = vec![ColumnPredicate::ColumnCompare {
            left_column_idx: 0,
            op: CompareOp::GreaterThanOrEqual,
            right_column_idx: 1,
        }];
        let filtered = simd_filter_batch(&batch, &predicates).unwrap();
        assert_eq!(filtered.row_count(), 2); // Rows 1 and 2

        // Test LessThanOrEqual
        let predicates = vec![ColumnPredicate::ColumnCompare {
            left_column_idx: 0,
            op: CompareOp::LessThanOrEqual,
            right_column_idx: 1,
        }];
        let filtered = simd_filter_batch(&batch, &predicates).unwrap();
        assert_eq!(filtered.row_count(), 2); // Rows 0 and 1

        // Test NotEqual
        let predicates = vec![ColumnPredicate::ColumnCompare {
            left_column_idx: 0,
            op: CompareOp::NotEqual,
            right_column_idx: 1,
        }];
        let filtered = simd_filter_batch(&batch, &predicates).unwrap();
        assert_eq!(filtered.row_count(), 2); // Rows 0 and 2
    }
}
