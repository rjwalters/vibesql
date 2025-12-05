//! Standard aggregation functions for columnar batch execution
//!
//! This module provides the standard (non-fused) aggregation path that operates
//! on filtered ColumnarBatch data. It supports all aggregate operations and
//! handles NULL values correctly.

use super::super::aggregate::{AggregateOp, AggregateSource, AggregateSpec};
use super::super::batch::{ColumnArray, ColumnarBatch};
use super::super::simd_ops;
use super::expression::compute_expression_aggregate_batch;
use crate::errors::ExecutorError;
use vibesql_types::SqlValue;

// Re-export optimized SIMD operations from simd_ops module
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

/// Compute multiple aggregates on a ColumnarBatch
///
/// This function operates directly on typed column arrays, avoiding
/// the overhead of SqlValue pattern matching for each value.
pub fn compute_batch_aggregates(
    batch: &ColumnarBatch,
    aggregates: &[AggregateSpec],
) -> Result<Vec<SqlValue>, ExecutorError> {
    let mut results = Vec::with_capacity(aggregates.len());

    for spec in aggregates {
        let result = match &spec.source {
            AggregateSource::Column(col_idx) => compute_column_aggregate(batch, *col_idx, spec.op)?,
            AggregateSource::CountStar => SqlValue::Integer(batch.row_count() as i64),
            AggregateSource::Expression(expr) => {
                // Vectorized expression evaluation directly on ColumnarBatch
                compute_expression_aggregate_batch(batch, expr, spec.op)?
            }
        };
        results.push(result);
    }

    Ok(results)
}

/// Compute an aggregate on a single column of a ColumnarBatch
pub fn compute_column_aggregate(
    batch: &ColumnarBatch,
    col_idx: usize,
    op: AggregateOp,
) -> Result<SqlValue, ExecutorError> {
    let column = batch.column(col_idx).ok_or_else(|| ExecutorError::ColumnarColumnNotFound {
        column_index: col_idx,
        batch_columns: batch.column_count(),
    })?;

    match column {
        // SIMD path for i64 columns
        ColumnArray::Int64(values, nulls) => {
            compute_i64_aggregate(values, nulls.as_ref().map(|n| n.as_slice()), op)
        }
        // SIMD path for f64 columns
        ColumnArray::Float64(values, nulls) => {
            compute_f64_aggregate(values, nulls.as_ref().map(|n| n.as_slice()), op)
        }
        // Scalar fallback for other types
        _ => compute_mixed_aggregate(batch, col_idx, op),
    }
}

/// Compute aggregate on i64 column using auto-vectorized operations
///
/// Optimized to avoid allocations when there are no NULL values.
pub fn compute_i64_aggregate(
    values: &[i64],
    nulls: Option<&[bool]>,
    op: AggregateOp,
) -> Result<SqlValue, ExecutorError> {
    log::debug!("[SIMD] compute_i64_aggregate: {} values, op={:?}", values.len(), op);
    if values.is_empty() {
        return Ok(match op {
            AggregateOp::Count => SqlValue::Integer(0),
            _ => SqlValue::Null,
        });
    }

    // Fast path: no nulls - operate directly on the slice without allocation
    if nulls.is_none() || nulls.as_ref().map_or(true, |n| !n.iter().any(|&x| x)) {
        log::debug!(
            "[SIMD] Using auto-vectorized path for i64 aggregate with {} values (no nulls)",
            values.len()
        );
        return match op {
            AggregateOp::Sum => Ok(SqlValue::Integer(simd_sum_i64(values))),
            AggregateOp::Count => Ok(SqlValue::Integer(values.len() as i64)),
            AggregateOp::Avg => {
                let sum = simd_sum_i64(values);
                Ok(SqlValue::Double(sum as f64 / values.len() as f64))
            }
            AggregateOp::Min => simd_min_i64(values).map(SqlValue::Integer).ok_or_else(|| {
                ExecutorError::SimdOperationFailed {
                    operation: "MIN".to_string(),
                    reason: "empty set".to_string(),
                }
            }),
            AggregateOp::Max => simd_max_i64(values).map(SqlValue::Integer).ok_or_else(|| {
                ExecutorError::SimdOperationFailed {
                    operation: "MAX".to_string(),
                    reason: "empty set".to_string(),
                }
            }),
        };
    }

    // Slow path with nulls: use inline accumulation to avoid Vec allocation
    let null_mask = nulls.unwrap();

    match op {
        AggregateOp::Sum => {
            let mut sum: i64 = 0;
            for (i, &v) in values.iter().enumerate() {
                if !null_mask[i] {
                    sum += v;
                }
            }
            Ok(SqlValue::Integer(sum))
        }
        AggregateOp::Count => {
            let count = null_mask.iter().filter(|&&is_null| !is_null).count();
            Ok(SqlValue::Integer(count as i64))
        }
        AggregateOp::Avg => {
            let mut sum: i64 = 0;
            let mut count: i64 = 0;
            for (i, &v) in values.iter().enumerate() {
                if !null_mask[i] {
                    sum += v;
                    count += 1;
                }
            }
            if count == 0 {
                Ok(SqlValue::Null)
            } else {
                Ok(SqlValue::Double(sum as f64 / count as f64))
            }
        }
        AggregateOp::Min => {
            let mut min: Option<i64> = None;
            for (i, &v) in values.iter().enumerate() {
                if !null_mask[i] {
                    min = Some(min.map_or(v, |m| m.min(v)));
                }
            }
            min.map(SqlValue::Integer).ok_or_else(|| ExecutorError::SimdOperationFailed {
                operation: "MIN".to_string(),
                reason: "all values NULL".to_string(),
            })
        }
        AggregateOp::Max => {
            let mut max: Option<i64> = None;
            for (i, &v) in values.iter().enumerate() {
                if !null_mask[i] {
                    max = Some(max.map_or(v, |m| m.max(v)));
                }
            }
            max.map(SqlValue::Integer).ok_or_else(|| ExecutorError::SimdOperationFailed {
                operation: "MAX".to_string(),
                reason: "all values NULL".to_string(),
            })
        }
    }
}

/// Compute aggregate on f64 column using auto-vectorized operations
///
/// Optimized to avoid allocations when there are no NULL values.
pub fn compute_f64_aggregate(
    values: &[f64],
    nulls: Option<&[bool]>,
    op: AggregateOp,
) -> Result<SqlValue, ExecutorError> {
    log::debug!("[SIMD] compute_f64_aggregate: {} values, op={:?}", values.len(), op);
    if values.is_empty() {
        return Ok(match op {
            AggregateOp::Count => SqlValue::Integer(0),
            _ => SqlValue::Null,
        });
    }

    // Fast path: no nulls - operate directly on the slice without allocation
    if nulls.is_none() || nulls.as_ref().map_or(true, |n| !n.iter().any(|&x| x)) {
        log::debug!(
            "[SIMD] Using auto-vectorized path for f64 aggregate with {} values (no nulls)",
            values.len()
        );
        return match op {
            AggregateOp::Sum => Ok(SqlValue::Double(simd_sum_f64(values))),
            AggregateOp::Count => Ok(SqlValue::Integer(values.len() as i64)),
            AggregateOp::Avg => {
                let sum = simd_sum_f64(values);
                Ok(SqlValue::Double(sum / values.len() as f64))
            }
            AggregateOp::Min => simd_min_f64(values).map(SqlValue::Double).ok_or_else(|| {
                ExecutorError::SimdOperationFailed {
                    operation: "MIN".to_string(),
                    reason: "empty set".to_string(),
                }
            }),
            AggregateOp::Max => simd_max_f64(values).map(SqlValue::Double).ok_or_else(|| {
                ExecutorError::SimdOperationFailed {
                    operation: "MAX".to_string(),
                    reason: "empty set".to_string(),
                }
            }),
        };
    }

    // Slow path with nulls: use inline accumulation to avoid Vec allocation
    let null_mask = nulls.unwrap();

    match op {
        AggregateOp::Sum => {
            let mut sum: f64 = 0.0;
            for (i, &v) in values.iter().enumerate() {
                if !null_mask[i] {
                    sum += v;
                }
            }
            Ok(SqlValue::Double(sum))
        }
        AggregateOp::Count => {
            let count = null_mask.iter().filter(|&&is_null| !is_null).count();
            Ok(SqlValue::Integer(count as i64))
        }
        AggregateOp::Avg => {
            let mut sum: f64 = 0.0;
            let mut count: i64 = 0;
            for (i, &v) in values.iter().enumerate() {
                if !null_mask[i] {
                    sum += v;
                    count += 1;
                }
            }
            if count == 0 {
                Ok(SqlValue::Null)
            } else {
                Ok(SqlValue::Double(sum / count as f64))
            }
        }
        AggregateOp::Min => {
            let mut min: Option<f64> = None;
            for (i, &v) in values.iter().enumerate() {
                if !null_mask[i] {
                    min = Some(min.map_or(v, |m| m.min(v)));
                }
            }
            min.map(SqlValue::Double).ok_or_else(|| ExecutorError::SimdOperationFailed {
                operation: "MIN".to_string(),
                reason: "all values NULL".to_string(),
            })
        }
        AggregateOp::Max => {
            let mut max: Option<f64> = None;
            for (i, &v) in values.iter().enumerate() {
                if !null_mask[i] {
                    max = Some(max.map_or(v, |m| m.max(v)));
                }
            }
            max.map(SqlValue::Double).ok_or_else(|| ExecutorError::SimdOperationFailed {
                operation: "MAX".to_string(),
                reason: "all values NULL".to_string(),
            })
        }
    }
}

/// Scalar fallback for non-numeric column types
pub fn compute_mixed_aggregate(
    batch: &ColumnarBatch,
    col_idx: usize,
    op: AggregateOp,
) -> Result<SqlValue, ExecutorError> {
    let row_count = batch.row_count();

    match op {
        AggregateOp::Count => {
            // Count non-NULL values
            let mut count = 0i64;
            for row_idx in 0..row_count {
                let value = batch.get_value(row_idx, col_idx)?;
                if !matches!(value, SqlValue::Null) {
                    count += 1;
                }
            }
            Ok(SqlValue::Integer(count))
        }
        AggregateOp::Sum | AggregateOp::Avg => {
            // For Mixed columns, accumulate as f64
            let mut sum = 0.0f64;
            let mut count = 0i64;
            for row_idx in 0..row_count {
                let value = batch.get_value(row_idx, col_idx)?;
                match value {
                    SqlValue::Integer(v) => {
                        sum += v as f64;
                        count += 1;
                    }
                    SqlValue::Double(v) => {
                        sum += v;
                        count += 1;
                    }
                    SqlValue::Float(v) => {
                        sum += v as f64;
                        count += 1;
                    }
                    SqlValue::Null => {}
                    _ => {
                        return Err(ExecutorError::UnsupportedExpression(format!(
                            "Cannot compute SUM/AVG on non-numeric value: {:?}",
                            value
                        )));
                    }
                }
            }

            if count == 0 {
                Ok(SqlValue::Null)
            } else if op == AggregateOp::Sum {
                Ok(SqlValue::Double(sum))
            } else {
                Ok(SqlValue::Double(sum / count as f64))
            }
        }
        AggregateOp::Min | AggregateOp::Max => {
            // For MIN/MAX, iterate and compare
            let mut result: Option<SqlValue> = None;
            for row_idx in 0..row_count {
                let value = batch.get_value(row_idx, col_idx)?;
                if matches!(value, SqlValue::Null) {
                    continue;
                }

                result = Some(match result {
                    None => value,
                    Some(current) => {
                        let is_better =
                            if op == AggregateOp::Min { value < current } else { value > current };
                        if is_better {
                            value
                        } else {
                            current
                        }
                    }
                });
            }
            Ok(result.unwrap_or(SqlValue::Null))
        }
    }
}
