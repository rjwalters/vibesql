//! Individual aggregate function implementations
//!
//! This module contains the core implementations of aggregate functions
//! (SUM, COUNT, AVG, MIN, MAX) that operate on columnar data.

use crate::errors::ExecutorError;
use vibesql_types::SqlValue;

use super::super::scan::ColumnarScan;
use super::AggregateOp;

/// Compute SUM aggregate on a column
pub(super) fn compute_sum(
    scan: &ColumnarScan,
    column_idx: usize,
    filter_bitmap: Option<&[bool]>,
) -> Result<SqlValue, ExecutorError> {
    let mut sum = 0.0;
    let mut count = 0;

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
                SqlValue::Integer(v) => sum += *v as f64,
                SqlValue::Bigint(v) => sum += *v as f64,
                SqlValue::Smallint(v) => sum += *v as f64,
                SqlValue::Float(v) => sum += *v as f64,
                SqlValue::Double(v) => sum += v,
                SqlValue::Numeric(v) => sum += v,
                SqlValue::Null => {}, // NULL values don't contribute to sum
                _ => {
                    return Err(ExecutorError::UnsupportedExpression(
                        format!("Cannot compute SUM on non-numeric value: {:?}", value)
                    ))
                }
            }
            count += 1;
        }
    }

    // Return appropriate type based on input
    // For now, always return Double for simplicity
    Ok(if count > 0 {
        SqlValue::Double(sum)
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
        (SqlValue::Float(a), SqlValue::Float(b)) => {
            a.partial_cmp(b).unwrap_or(Ordering::Equal)
        }
        (SqlValue::Double(a), SqlValue::Double(b)) => {
            a.partial_cmp(b).unwrap_or(Ordering::Equal)
        }
        (SqlValue::Numeric(a), SqlValue::Numeric(b)) => {
            a.partial_cmp(b).unwrap_or(Ordering::Equal)
        }
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
