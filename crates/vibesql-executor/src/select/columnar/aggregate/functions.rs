//! Individual aggregate function implementations
//!
//! This module contains the core implementations of aggregate functions
//! (SUM, COUNT, AVG, MIN, MAX) that operate on columnar data.

#![allow(clippy::collapsible_else_if)]

use crate::errors::ExecutorError;
use vibesql_types::SqlValue;

use super::super::batch::{ColumnArray, ColumnarBatch};
use super::super::scan::ColumnarScan;
use super::super::simd_ops;
use super::AggregateOp;

// Re-export optimized SIMD operations from simd_ops module
// See simd_ops.rs for documentation on why the 4-accumulator pattern is used
#[inline]
fn simd_sum_i64(values: &[i64]) -> i64 {
    simd_ops::sum_i64(values)
}

#[inline]
fn simd_min_i64(values: &[i64]) -> Option<i64> {
    simd_ops::min_i64(values)
}

#[inline]
fn simd_max_i64(values: &[i64]) -> Option<i64> {
    simd_ops::max_i64(values)
}

#[inline]
fn simd_sum_f64(values: &[f64]) -> f64 {
    simd_ops::sum_f64(values)
}

#[inline]
fn simd_min_f64(values: &[f64]) -> Option<f64> {
    simd_ops::min_f64(values)
}

#[inline]
fn simd_max_f64(values: &[f64]) -> Option<f64> {
    simd_ops::max_f64(values)
}

/// Compute SUM aggregate on a column
pub(super) fn compute_sum(
    scan: &ColumnarScan,
    column_idx: usize,
    filter_bitmap: Option<&[bool]>,
) -> Result<SqlValue, ExecutorError> {
    let mut int_sum: i64 = 0;
    let mut float_sum = 0.0;
    let mut count = 0;
    let mut has_float = false;

    for (row_idx, value_opt) in scan.column(column_idx).enumerate() {
        // Check filter bitmap
        if let Some(bitmap) = filter_bitmap {
            if !bitmap.get(row_idx).copied().unwrap_or(false) {
                continue;
            }
        }

        // Add to sum
        if let Some(value) = value_opt {
            match value {
                SqlValue::Integer(v) => {
                    if has_float {
                        float_sum += *v as f64;
                    } else {
                        int_sum += v;
                    }
                }
                SqlValue::Bigint(v) => {
                    if has_float {
                        float_sum += *v as f64;
                    } else {
                        int_sum += v;
                    }
                }
                SqlValue::Smallint(v) => {
                    if has_float {
                        float_sum += *v as f64;
                    } else {
                        int_sum += *v as i64;
                    }
                }
                SqlValue::Float(v) => {
                    if !has_float {
                        // Convert accumulated integer sum to float
                        float_sum = int_sum as f64;
                        has_float = true;
                    }
                    float_sum += *v as f64;
                }
                SqlValue::Double(v) => {
                    if !has_float {
                        // Convert accumulated integer sum to float
                        float_sum = int_sum as f64;
                        has_float = true;
                    }
                    float_sum += v;
                }
                SqlValue::Numeric(v) => {
                    if !has_float {
                        // Convert accumulated integer sum to float
                        float_sum = int_sum as f64;
                        has_float = true;
                    }
                    float_sum += v;
                }
                SqlValue::Null => {} // NULL values don't contribute to sum
                _ => {
                    return Err(ExecutorError::UnsupportedExpression(format!(
                        "Cannot compute SUM on non-numeric value: {:?}",
                        value
                    )))
                }
            }
            count += 1;
        }
    }

    // Return appropriate type based on input types
    Ok(if count > 0 {
        if has_float {
            SqlValue::Double(float_sum)
        } else {
            SqlValue::Integer(int_sum)
        }
    } else {
        SqlValue::Null
    })
}

/// Compute COUNT aggregate
pub(super) fn compute_count(
    scan: &ColumnarScan,
    filter_bitmap: Option<&[bool]>,
) -> Result<SqlValue, ExecutorError> {
    let count = if let Some(bitmap) = filter_bitmap {
        bitmap.iter().filter(|&&pass| pass).count()
    } else {
        scan.len()
    };

    Ok(SqlValue::Integer(count as i64))
}

/// Compute AVG aggregate on a column
pub(super) fn compute_avg(
    scan: &ColumnarScan,
    column_idx: usize,
    filter_bitmap: Option<&[bool]>,
) -> Result<SqlValue, ExecutorError> {
    let sum_result = compute_sum(scan, column_idx, filter_bitmap)?;
    let count_result = compute_count(scan, filter_bitmap)?;

    match (sum_result, count_result) {
        (SqlValue::Integer(sum), SqlValue::Integer(count)) if count > 0 => {
            Ok(SqlValue::Double(sum as f64 / count as f64))
        }
        (SqlValue::Double(sum), SqlValue::Integer(count)) if count > 0 => {
            Ok(SqlValue::Double(sum / count as f64))
        }
        (SqlValue::Null, _) | (_, SqlValue::Integer(0)) => Ok(SqlValue::Null),
        _ => Err(ExecutorError::UnsupportedExpression("Invalid AVG computation".to_string())),
    }
}

/// Compute MIN aggregate on a column
pub(super) fn compute_min(
    scan: &ColumnarScan,
    column_idx: usize,
    filter_bitmap: Option<&[bool]>,
) -> Result<SqlValue, ExecutorError> {
    let mut min_value: Option<SqlValue> = None;

    for (row_idx, value_opt) in scan.column(column_idx).enumerate() {
        // Check filter bitmap
        if let Some(bitmap) = filter_bitmap {
            if !bitmap.get(row_idx).copied().unwrap_or(false) {
                continue;
            }
        }

        if let Some(value) = value_opt {
            if !matches!(value, SqlValue::Null) {
                min_value = Some(match &min_value {
                    None => value.clone(),
                    Some(current_min) => {
                        if compare_for_min_max(value, current_min) {
                            value.clone()
                        } else {
                            current_min.clone()
                        }
                    }
                });
            }
        }
    }

    Ok(min_value.unwrap_or(SqlValue::Null))
}

/// Compute MAX aggregate on a column
pub(super) fn compute_max(
    scan: &ColumnarScan,
    column_idx: usize,
    filter_bitmap: Option<&[bool]>,
) -> Result<SqlValue, ExecutorError> {
    let mut max_value: Option<SqlValue> = None;

    for (row_idx, value_opt) in scan.column(column_idx).enumerate() {
        // Check filter bitmap
        if let Some(bitmap) = filter_bitmap {
            if !bitmap.get(row_idx).copied().unwrap_or(false) {
                continue;
            }
        }

        if let Some(value) = value_opt {
            if !matches!(value, SqlValue::Null) {
                max_value = Some(match &max_value {
                    None => value.clone(),
                    Some(current_max) => {
                        if compare_for_min_max(current_max, value) {
                            value.clone()
                        } else {
                            current_max.clone()
                        }
                    }
                });
            }
        }
    }

    Ok(max_value.unwrap_or(SqlValue::Null))
}

/// Compare two values for MIN/MAX (returns true if a < b)
pub(super) fn compare_for_min_max(a: &SqlValue, b: &SqlValue) -> bool {
    use std::cmp::Ordering;

    let ordering = match (a, b) {
        (SqlValue::Integer(a), SqlValue::Integer(b)) => a.cmp(b),
        (SqlValue::Bigint(a), SqlValue::Bigint(b)) => a.cmp(b),
        (SqlValue::Smallint(a), SqlValue::Smallint(b)) => a.cmp(b),
        (SqlValue::Float(a), SqlValue::Float(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
        (SqlValue::Double(a), SqlValue::Double(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
        (SqlValue::Numeric(a), SqlValue::Numeric(b)) => a.partial_cmp(b).unwrap_or(Ordering::Equal),
        _ => Ordering::Equal,
    };

    ordering == Ordering::Less
}

/// Compute an aggregate on a column (dispatcher for all aggregate types)
pub(super) fn compute_columnar_aggregate_impl(
    scan: &ColumnarScan,
    column_idx: usize,
    op: AggregateOp,
    filter_bitmap: Option<&[bool]>,
) -> Result<SqlValue, ExecutorError> {
    match op {
        AggregateOp::Sum => compute_sum(scan, column_idx, filter_bitmap),
        AggregateOp::Count => compute_count(scan, filter_bitmap),
        AggregateOp::Avg => compute_avg(scan, column_idx, filter_bitmap),
        AggregateOp::Min => compute_min(scan, column_idx, filter_bitmap),
        AggregateOp::Max => compute_max(scan, column_idx, filter_bitmap),
    }
}

// =============================================================================
// Batch-native aggregate functions (work directly on ColumnarBatch)
// =============================================================================

/// Compute SUM aggregate directly on a ColumnarBatch column
///
/// This function works directly on the typed column arrays in ColumnarBatch,
/// avoiding the overhead of converting back to rows.
pub(super) fn compute_batch_sum(
    batch: &ColumnarBatch,
    column_idx: usize,
) -> Result<SqlValue, ExecutorError> {
    let column = batch.column(column_idx).ok_or_else(|| ExecutorError::ColumnarColumnNotFound {
        column_index: column_idx,
        batch_columns: batch.column_count(),
    })?;

    match column {
        ColumnArray::Int64(values, nulls) => {
            if let Some(null_mask) = nulls {
                // Handle NULLs by filtering them out
                let filtered: Vec<i64> = values
                    .iter()
                    .zip(null_mask.iter())
                    .filter(|(_, &is_null)| !is_null)
                    .map(|(&v, _)| v)
                    .collect();
                if filtered.is_empty() {
                    Ok(SqlValue::Null)
                } else {
                    Ok(SqlValue::Integer(simd_sum_i64(&filtered)))
                }
            } else {
                if values.is_empty() {
                    Ok(SqlValue::Null)
                } else {
                    Ok(SqlValue::Integer(simd_sum_i64(values)))
                }
            }
        }
        ColumnArray::Float64(values, nulls) => {
            if let Some(null_mask) = nulls {
                let filtered: Vec<f64> = values
                    .iter()
                    .zip(null_mask.iter())
                    .filter(|(_, &is_null)| !is_null)
                    .map(|(&v, _)| v)
                    .collect();
                if filtered.is_empty() {
                    Ok(SqlValue::Null)
                } else {
                    Ok(SqlValue::Double(simd_sum_f64(&filtered)))
                }
            } else {
                if values.is_empty() {
                    Ok(SqlValue::Null)
                } else {
                    Ok(SqlValue::Double(simd_sum_f64(values)))
                }
            }
        }
        ColumnArray::Mixed(values) => {
            // Fallback for mixed type columns
            compute_mixed_sum(values)
        }
        _ => Err(ExecutorError::UnsupportedExpression(format!(
            "Cannot compute SUM on column type: {:?}",
            column.data_type()
        ))),
    }
}

/// Compute COUNT aggregate directly on a ColumnarBatch
pub(super) fn compute_batch_count(batch: &ColumnarBatch) -> Result<SqlValue, ExecutorError> {
    Ok(SqlValue::Integer(batch.row_count() as i64))
}

/// Compute AVG aggregate directly on a ColumnarBatch column
pub(super) fn compute_batch_avg(
    batch: &ColumnarBatch,
    column_idx: usize,
) -> Result<SqlValue, ExecutorError> {
    let column = batch.column(column_idx).ok_or_else(|| ExecutorError::ColumnarColumnNotFound {
        column_index: column_idx,
        batch_columns: batch.column_count(),
    })?;

    match column {
        ColumnArray::Int64(values, nulls) => {
            let (sum, count) = if let Some(null_mask) = nulls {
                let filtered: Vec<i64> = values
                    .iter()
                    .zip(null_mask.iter())
                    .filter(|(_, &is_null)| !is_null)
                    .map(|(&v, _)| v)
                    .collect();
                (simd_sum_i64(&filtered), filtered.len())
            } else {
                (simd_sum_i64(values), values.len())
            };
            if count == 0 {
                Ok(SqlValue::Null)
            } else {
                Ok(SqlValue::Double(sum as f64 / count as f64))
            }
        }
        ColumnArray::Float64(values, nulls) => {
            let (sum, count) = if let Some(null_mask) = nulls {
                let filtered: Vec<f64> = values
                    .iter()
                    .zip(null_mask.iter())
                    .filter(|(_, &is_null)| !is_null)
                    .map(|(&v, _)| v)
                    .collect();
                (simd_sum_f64(&filtered), filtered.len())
            } else {
                (simd_sum_f64(values), values.len())
            };
            if count == 0 {
                Ok(SqlValue::Null)
            } else {
                Ok(SqlValue::Double(sum / count as f64))
            }
        }
        ColumnArray::Mixed(values) => {
            // Fallback for mixed type columns
            let sum_result = compute_mixed_sum(values)?;
            let count = values.iter().filter(|v| !matches!(v, SqlValue::Null)).count();
            match sum_result {
                SqlValue::Integer(sum) if count > 0 => {
                    Ok(SqlValue::Double(sum as f64 / count as f64))
                }
                SqlValue::Double(sum) if count > 0 => Ok(SqlValue::Double(sum / count as f64)),
                SqlValue::Null => Ok(SqlValue::Null),
                _ => Ok(SqlValue::Null),
            }
        }
        _ => Err(ExecutorError::UnsupportedExpression(format!(
            "Cannot compute AVG on column type: {:?}",
            column.data_type()
        ))),
    }
}

/// Compute MIN aggregate directly on a ColumnarBatch column
pub(super) fn compute_batch_min(
    batch: &ColumnarBatch,
    column_idx: usize,
) -> Result<SqlValue, ExecutorError> {
    let column = batch.column(column_idx).ok_or_else(|| ExecutorError::ColumnarColumnNotFound {
        column_index: column_idx,
        batch_columns: batch.column_count(),
    })?;

    match column {
        ColumnArray::Int64(values, nulls) => {
            if let Some(null_mask) = nulls {
                let filtered: Vec<i64> = values
                    .iter()
                    .zip(null_mask.iter())
                    .filter(|(_, &is_null)| !is_null)
                    .map(|(&v, _)| v)
                    .collect();
                match simd_min_i64(&filtered) {
                    Some(min) => Ok(SqlValue::Integer(min)),
                    None => Ok(SqlValue::Null),
                }
            } else {
                match simd_min_i64(values) {
                    Some(min) => Ok(SqlValue::Integer(min)),
                    None => Ok(SqlValue::Null),
                }
            }
        }
        ColumnArray::Float64(values, nulls) => {
            if let Some(null_mask) = nulls {
                let filtered: Vec<f64> = values
                    .iter()
                    .zip(null_mask.iter())
                    .filter(|(_, &is_null)| !is_null)
                    .map(|(&v, _)| v)
                    .collect();
                match simd_min_f64(&filtered) {
                    Some(min) => Ok(SqlValue::Double(min)),
                    None => Ok(SqlValue::Null),
                }
            } else {
                match simd_min_f64(values) {
                    Some(min) => Ok(SqlValue::Double(min)),
                    None => Ok(SqlValue::Null),
                }
            }
        }
        ColumnArray::Mixed(values) => compute_mixed_min(values),
        _ => Err(ExecutorError::UnsupportedExpression(format!(
            "Cannot compute MIN on column type: {:?}",
            column.data_type()
        ))),
    }
}

/// Compute MAX aggregate directly on a ColumnarBatch column
pub(super) fn compute_batch_max(
    batch: &ColumnarBatch,
    column_idx: usize,
) -> Result<SqlValue, ExecutorError> {
    let column = batch.column(column_idx).ok_or_else(|| ExecutorError::ColumnarColumnNotFound {
        column_index: column_idx,
        batch_columns: batch.column_count(),
    })?;

    match column {
        ColumnArray::Int64(values, nulls) => {
            if let Some(null_mask) = nulls {
                let filtered: Vec<i64> = values
                    .iter()
                    .zip(null_mask.iter())
                    .filter(|(_, &is_null)| !is_null)
                    .map(|(&v, _)| v)
                    .collect();
                match simd_max_i64(&filtered) {
                    Some(max) => Ok(SqlValue::Integer(max)),
                    None => Ok(SqlValue::Null),
                }
            } else {
                match simd_max_i64(values) {
                    Some(max) => Ok(SqlValue::Integer(max)),
                    None => Ok(SqlValue::Null),
                }
            }
        }
        ColumnArray::Float64(values, nulls) => {
            if let Some(null_mask) = nulls {
                let filtered: Vec<f64> = values
                    .iter()
                    .zip(null_mask.iter())
                    .filter(|(_, &is_null)| !is_null)
                    .map(|(&v, _)| v)
                    .collect();
                match simd_max_f64(&filtered) {
                    Some(max) => Ok(SqlValue::Double(max)),
                    None => Ok(SqlValue::Null),
                }
            } else {
                match simd_max_f64(values) {
                    Some(max) => Ok(SqlValue::Double(max)),
                    None => Ok(SqlValue::Null),
                }
            }
        }
        ColumnArray::Mixed(values) => compute_mixed_max(values),
        _ => Err(ExecutorError::UnsupportedExpression(format!(
            "Cannot compute MAX on column type: {:?}",
            column.data_type()
        ))),
    }
}

/// Compute batch aggregate (dispatcher for all aggregate types)
pub(super) fn compute_batch_aggregate(
    batch: &ColumnarBatch,
    column_idx: usize,
    op: AggregateOp,
) -> Result<SqlValue, ExecutorError> {
    match op {
        AggregateOp::Sum => compute_batch_sum(batch, column_idx),
        AggregateOp::Count => compute_batch_count(batch),
        AggregateOp::Avg => compute_batch_avg(batch, column_idx),
        AggregateOp::Min => compute_batch_min(batch, column_idx),
        AggregateOp::Max => compute_batch_max(batch, column_idx),
    }
}

// Helper functions for mixed-type columns

fn compute_mixed_sum(values: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    let mut int_sum: i64 = 0;
    let mut float_sum = 0.0;
    let mut count = 0;
    let mut has_float = false;

    for value in values {
        match value {
            SqlValue::Integer(v) => {
                if has_float {
                    float_sum += *v as f64;
                } else {
                    int_sum += v;
                }
                count += 1;
            }
            SqlValue::Bigint(v) => {
                if has_float {
                    float_sum += *v as f64;
                } else {
                    int_sum += v;
                }
                count += 1;
            }
            SqlValue::Double(v) => {
                if !has_float {
                    float_sum = int_sum as f64;
                    has_float = true;
                }
                float_sum += v;
                count += 1;
            }
            SqlValue::Float(v) => {
                if !has_float {
                    float_sum = int_sum as f64;
                    has_float = true;
                }
                float_sum += *v as f64;
                count += 1;
            }
            SqlValue::Numeric(v) => {
                if !has_float {
                    float_sum = int_sum as f64;
                    has_float = true;
                }
                float_sum += v;
                count += 1;
            }
            SqlValue::Null => {}
            _ => {
                return Err(ExecutorError::UnsupportedExpression(format!(
                    "Cannot compute SUM on value: {:?}",
                    value
                )))
            }
        }
    }

    Ok(if count > 0 {
        if has_float {
            SqlValue::Double(float_sum)
        } else {
            SqlValue::Integer(int_sum)
        }
    } else {
        SqlValue::Null
    })
}

fn compute_mixed_min(values: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    let mut min_value: Option<SqlValue> = None;

    for value in values {
        if !matches!(value, SqlValue::Null) {
            min_value = Some(match &min_value {
                None => value.clone(),
                Some(current_min) => {
                    if compare_for_min_max(value, current_min) {
                        value.clone()
                    } else {
                        current_min.clone()
                    }
                }
            });
        }
    }

    Ok(min_value.unwrap_or(SqlValue::Null))
}

fn compute_mixed_max(values: &[SqlValue]) -> Result<SqlValue, ExecutorError> {
    let mut max_value: Option<SqlValue> = None;

    for value in values {
        if !matches!(value, SqlValue::Null) {
            max_value = Some(match &max_value {
                None => value.clone(),
                Some(current_max) => {
                    if compare_for_min_max(current_max, value) {
                        value.clone()
                    } else {
                        current_max.clone()
                    }
                }
            });
        }
    }

    Ok(max_value.unwrap_or(SqlValue::Null))
}
