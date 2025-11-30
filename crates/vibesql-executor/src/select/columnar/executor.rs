//! Native columnar executor - end-to-end columnar query execution
//!
//! This module provides true columnar query execution that operates on `ColumnarBatch`
//! throughout the entire pipeline, avoiding row materialization until final output.

#![allow(clippy::needless_range_loop, clippy::unnecessary_map_or)]
//!
//! ## Architecture
//!
//! ```text
//! Storage → ColumnarBatch → SIMD Filter → SIMD Aggregate → Vec<Row> (only at output)
//!          ↑ Zero-copy     ↑ 4-8x faster  ↑ 10x faster   ↑ Minimal materialization
//! ```
//!
//! ## Key Benefits
//!
//! - **Zero-copy**: ColumnarBatch flows through without row materialization
//! - **SIMD acceleration**: All filtering and aggregation uses vectorized instructions
//! - **Cache efficiency**: Columnar data access patterns are cache-friendly
//! - **Minimal allocations**: Only allocate result rows at the end

use super::batch::{ColumnArray, ColumnarBatch};
use super::aggregate::{AggregateOp, AggregateSource, AggregateSpec};
use super::filter::ColumnPredicate;
use super::simd_filter::{simd_create_filter_mask, simd_filter_batch};
use super::simd_ops;
use crate::errors::ExecutorError;
use crate::schema::CombinedSchema;
use vibesql_ast::{BinaryOperator, Expression};
use vibesql_storage::Row;
use vibesql_types::SqlValue;

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

/// Execute a columnar query end-to-end on a ColumnarBatch
///
/// This is the main entry point for native columnar execution. It accepts
/// a ColumnarBatch from storage and executes filtering and aggregation
/// entirely in columnar format.
///
/// # Arguments
///
/// * `batch` - Input ColumnarBatch from storage layer
/// * `predicates` - Column predicates for SIMD filtering
/// * `aggregates` - Aggregate specifications (SUM, COUNT, etc.)
/// * `schema` - Optional schema for expression evaluation
///
/// # Returns
///
/// A vector of rows containing the aggregated results
pub fn execute_columnar_batch(
    batch: &ColumnarBatch,
    predicates: &[ColumnPredicate],
    aggregates: &[AggregateSpec],
    _schema: Option<&CombinedSchema>,
) -> Result<Vec<Row>, ExecutorError> {
    // Early return for empty input
    if batch.row_count() == 0 {
        let values: Vec<SqlValue> = aggregates
            .iter()
            .map(|spec| match spec.op {
                AggregateOp::Count => SqlValue::Integer(0),
                _ => SqlValue::Null,
            })
            .collect();
        return Ok(vec![Row::new(values)]);
    }

    // Try fused filter+aggregate path first (avoids intermediate batch allocation)
    // This is faster for simple aggregate queries like TPC-H Q6
    if !predicates.is_empty() && can_use_fused_aggregation(aggregates) {
        if let Ok(results) = execute_fused_filter_aggregate(batch, predicates, aggregates) {
            return Ok(vec![Row::new(results)]);
        }
        // Fall through to standard path on failure
    }

    // Standard path: filter first, then aggregate
    // Used when fused path is not applicable or fails

    // Phase 1: Apply auto-vectorized filtering
    #[cfg(feature = "profile-q6")]
    let filter_start = std::time::Instant::now();

    let filtered_batch = if predicates.is_empty() {
        batch.clone()
    } else {
        simd_filter_batch(batch, predicates)?
    };

    #[cfg(feature = "profile-q6")]
    {
        let filter_time = filter_start.elapsed();
        eprintln!(
            "[PROFILE-Q6]   Phase 1 - SIMD Filter: {:?} ({}/{} rows passed)",
            filter_time,
            filtered_batch.row_count(),
            batch.row_count()
        );
    }

    // Phase 2: Compute aggregates on filtered batch
    #[cfg(feature = "profile-q6")]
    let agg_start = std::time::Instant::now();

    let results = compute_batch_aggregates(&filtered_batch, aggregates)?;

    #[cfg(feature = "profile-q6")]
    {
        let agg_time = agg_start.elapsed();
        eprintln!(
            "[PROFILE-Q6]   Phase 2 - SIMD Aggregate: {:?} ({} aggregates)",
            agg_time,
            aggregates.len()
        );
    }

    // Phase 3: Convert to output rows (only materialization point)
    Ok(vec![Row::new(results)])
}

/// Check if all aggregates can use the fused filter+aggregate optimization
fn can_use_fused_aggregation(aggregates: &[AggregateSpec]) -> bool {
    aggregates.iter().all(|spec| {
        matches!(
            spec.op,
            AggregateOp::Sum | AggregateOp::Count | AggregateOp::Min | AggregateOp::Max | AggregateOp::Avg
        ) && matches!(
            spec.source,
            AggregateSource::Column(_) | AggregateSource::CountStar | AggregateSource::Expression(_)
        )
    })
}

/// Fused filter+aggregate execution path
///
/// This optimization avoids creating an intermediate filtered batch by:
/// 1. Creating a filter mask using SIMD predicates
/// 2. Aggregating directly with the filter mask
///
/// This provides significant performance improvement for simple aggregate queries
/// like TPC-H Q6 by eliminating intermediate allocations.
fn execute_fused_filter_aggregate(
    batch: &ColumnarBatch,
    predicates: &[ColumnPredicate],
    aggregates: &[AggregateSpec],
) -> Result<Vec<SqlValue>, ExecutorError> {
    #[cfg(feature = "profile-q6")]
    let start = std::time::Instant::now();

    // Phase 1: Create filter mask (no intermediate batch allocation)
    let filter_mask = simd_create_filter_mask(batch, predicates)?;

    #[cfg(feature = "profile-q6")]
    {
        let mask_time = start.elapsed();
        let passing = filter_mask.iter().filter(|&&b| b).count();
        eprintln!(
            "[PROFILE-Q6] Fused Path - Filter mask: {:?} ({}/{} pass)",
            mask_time, passing, batch.row_count()
        );
    }

    // Phase 2: Compute aggregates directly using filter mask
    #[cfg(feature = "profile-q6")]
    let agg_start = std::time::Instant::now();

    let results = compute_fused_aggregates(batch, &filter_mask, aggregates)?;

    #[cfg(feature = "profile-q6")]
    {
        let agg_time = agg_start.elapsed();
        eprintln!(
            "[PROFILE-Q6] Fused Path - Aggregate: {:?} ({} aggregates)",
            agg_time, aggregates.len()
        );
    }

    Ok(results)
}

/// Compute aggregates using filter mask directly (fused path)
fn compute_fused_aggregates(
    batch: &ColumnarBatch,
    filter_mask: &[bool],
    aggregates: &[AggregateSpec],
) -> Result<Vec<SqlValue>, ExecutorError> {
    let mut results = Vec::with_capacity(aggregates.len());

    for spec in aggregates {
        let result = match &spec.source {
            AggregateSource::Column(col_idx) => {
                compute_fused_column_aggregate(batch, *col_idx, spec.op, filter_mask)?
            }
            AggregateSource::CountStar => {
                // COUNT(*) with filter = count of true values in filter mask
                SqlValue::Integer(simd_ops::count_filtered(filter_mask))
            }
            AggregateSource::Expression(expr) => {
                compute_fused_expression_aggregate(batch, expr, spec.op, filter_mask)?
            }
        };
        results.push(result);
    }

    Ok(results)
}

/// Compute aggregate on a column using filter mask
fn compute_fused_column_aggregate(
    batch: &ColumnarBatch,
    col_idx: usize,
    op: AggregateOp,
    filter_mask: &[bool],
) -> Result<SqlValue, ExecutorError> {
    let column = batch.column(col_idx).ok_or_else(|| {
        ExecutorError::ColumnarColumnNotFound {
            column_index: col_idx,
            batch_columns: batch.column_count(),
        }
    })?;

    match column {
        ColumnArray::Int64(values, nulls) => {
            // Has NULLs - fall back to standard path which handles them correctly
            if nulls.as_ref().map_or(false, |n| n.iter().any(|&x| x)) {
                return Err(ExecutorError::UnsupportedArrayType {
                    operation: "fused_aggregate".to_string(),
                    array_type: "Int64 with NULLs".to_string(),
                });
            }
            compute_fused_i64_aggregate(values, op, filter_mask)
        }
        ColumnArray::Float64(values, nulls) => {
            // Has NULLs - fall back to standard path which handles them correctly
            if nulls.as_ref().map_or(false, |n| n.iter().any(|&x| x)) {
                return Err(ExecutorError::UnsupportedArrayType {
                    operation: "fused_aggregate".to_string(),
                    array_type: "Float64 with NULLs".to_string(),
                });
            }
            compute_fused_f64_aggregate(values, op, filter_mask)
        }
        // Fall back to non-fused path for other types
        _ => {
            // Return error to signal fallback to standard path
            Err(ExecutorError::UnsupportedArrayType {
                operation: "fused_aggregate".to_string(),
                array_type: format!("{:?}", std::mem::discriminant(column)),
            })
        }
    }
}

/// Compute aggregate on i64 values using filter mask
fn compute_fused_i64_aggregate(
    values: &[i64],
    op: AggregateOp,
    filter_mask: &[bool],
) -> Result<SqlValue, ExecutorError> {
    match op {
        AggregateOp::Sum => {
            Ok(SqlValue::Integer(simd_ops::sum_i64_filtered(values, filter_mask)))
        }
        AggregateOp::Count => {
            Ok(SqlValue::Integer(simd_ops::count_filtered(filter_mask)))
        }
        AggregateOp::Avg => {
            let sum = simd_ops::sum_i64_filtered(values, filter_mask);
            let count = simd_ops::count_filtered(filter_mask);
            if count == 0 {
                Ok(SqlValue::Null)
            } else {
                Ok(SqlValue::Double(sum as f64 / count as f64))
            }
        }
        AggregateOp::Min => {
            simd_ops::min_i64_filtered(values, filter_mask)
                .map(SqlValue::Integer)
                .ok_or_else(|| ExecutorError::SimdOperationFailed {
                    operation: "MIN".to_string(),
                    reason: "no rows pass filter".to_string(),
                })
        }
        AggregateOp::Max => {
            simd_ops::max_i64_filtered(values, filter_mask)
                .map(SqlValue::Integer)
                .ok_or_else(|| ExecutorError::SimdOperationFailed {
                    operation: "MAX".to_string(),
                    reason: "no rows pass filter".to_string(),
                })
        }
    }
}

/// Compute aggregate on f64 values using filter mask
fn compute_fused_f64_aggregate(
    values: &[f64],
    op: AggregateOp,
    filter_mask: &[bool],
) -> Result<SqlValue, ExecutorError> {
    match op {
        AggregateOp::Sum => {
            Ok(SqlValue::Double(simd_ops::sum_f64_filtered(values, filter_mask)))
        }
        AggregateOp::Count => {
            Ok(SqlValue::Integer(simd_ops::count_filtered(filter_mask)))
        }
        AggregateOp::Avg => {
            let sum = simd_ops::sum_f64_filtered(values, filter_mask);
            let count = simd_ops::count_filtered(filter_mask);
            if count == 0 {
                Ok(SqlValue::Null)
            } else {
                Ok(SqlValue::Double(sum / count as f64))
            }
        }
        AggregateOp::Min => {
            simd_ops::min_f64_filtered(values, filter_mask)
                .map(SqlValue::Double)
                .ok_or_else(|| ExecutorError::SimdOperationFailed {
                    operation: "MIN".to_string(),
                    reason: "no rows pass filter".to_string(),
                })
        }
        AggregateOp::Max => {
            simd_ops::max_f64_filtered(values, filter_mask)
                .map(SqlValue::Double)
                .ok_or_else(|| ExecutorError::SimdOperationFailed {
                    operation: "MAX".to_string(),
                    reason: "no rows pass filter".to_string(),
                })
        }
    }
}

/// Compute expression aggregate using filter mask (for SUM(col_a * col_b))
fn compute_fused_expression_aggregate(
    batch: &ColumnarBatch,
    expr: &Expression,
    op: AggregateOp,
    filter_mask: &[bool],
) -> Result<SqlValue, ExecutorError> {
    // Try to evaluate as simple binary operation on columns (most common case)
    if let Expression::BinaryOp { left, op: bin_op, right } = expr {
        if *bin_op == BinaryOperator::Multiply {
            if let (
                Expression::ColumnRef { column: col1, .. },
                Expression::ColumnRef { column: col2, .. }
            ) = (left.as_ref(), right.as_ref()) {
                let left_idx = batch.column_index_by_name(col1);
                let right_idx = batch.column_index_by_name(col2);

                if let (Some(l_idx), Some(r_idx)) = (left_idx, right_idx) {
                    return compute_fused_multiply_aggregate(batch, l_idx, r_idx, op, filter_mask);
                }
            }
        }
    }

    // Fall back to error for complex expressions (triggers standard path)
    Err(ExecutorError::UnsupportedExpression(
        "Complex expression not supported in fused path".to_string()
    ))
}

/// Fused SUM(col_a * col_b) with filter mask
///
/// This is the key operation for TPC-H Q6: SUM(l_extendedprice * l_discount)
fn compute_fused_multiply_aggregate(
    batch: &ColumnarBatch,
    left_idx: usize,
    right_idx: usize,
    op: AggregateOp,
    filter_mask: &[bool],
) -> Result<SqlValue, ExecutorError> {
    let left_col = batch.column(left_idx).ok_or_else(|| {
        ExecutorError::ColumnarColumnNotFound {
            column_index: left_idx,
            batch_columns: batch.column_count(),
        }
    })?;
    let right_col = batch.column(right_idx).ok_or_else(|| {
        ExecutorError::ColumnarColumnNotFound {
            column_index: right_idx,
            batch_columns: batch.column_count(),
        }
    })?;

    // Fast path: Both columns are Float64
    if let (
        ColumnArray::Float64(left_values, left_nulls),
        ColumnArray::Float64(right_values, right_nulls),
    ) = (left_col, right_col)
    {
        let (sum, count) = simd_ops::sum_product_f64_filtered_masked(
            left_values,
            right_values,
            filter_mask,
            left_nulls.as_ref().map(|v| v.as_slice()),
            right_nulls.as_ref().map(|v| v.as_slice()),
        );

        return match op {
            AggregateOp::Sum => Ok(if count > 0 { SqlValue::Double(sum) } else { SqlValue::Null }),
            AggregateOp::Count => Ok(SqlValue::Integer(count)),
            AggregateOp::Avg => Ok(if count > 0 { SqlValue::Double(sum / count as f64) } else { SqlValue::Null }),
            _ => Err(ExecutorError::UnsupportedExpression(
                "MIN/MAX not supported for expression aggregates".to_string()
            ))
        };
    }

    // Fall back to error for non-Float64 columns (triggers standard path)
    Err(ExecutorError::UnsupportedArrayType {
        operation: "fused_multiply_aggregate".to_string(),
        array_type: "non-Float64".to_string(),
    })
}

/// Compute multiple aggregates on a ColumnarBatch
///
/// This function operates directly on typed column arrays, avoiding
/// the overhead of SqlValue pattern matching for each value.
fn compute_batch_aggregates(
    batch: &ColumnarBatch,
    aggregates: &[AggregateSpec],
) -> Result<Vec<SqlValue>, ExecutorError> {
    let mut results = Vec::with_capacity(aggregates.len());

    for spec in aggregates {
        let result = match &spec.source {
            AggregateSource::Column(col_idx) => {
                compute_column_aggregate(batch, *col_idx, spec.op)?
            }
            AggregateSource::CountStar => {
                SqlValue::Integer(batch.row_count() as i64)
            }
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
fn compute_column_aggregate(
    batch: &ColumnarBatch,
    col_idx: usize,
    op: AggregateOp,
) -> Result<SqlValue, ExecutorError> {
    let column = batch.column(col_idx).ok_or_else(|| {
        ExecutorError::ColumnarColumnNotFound {
            column_index: col_idx,
            batch_columns: batch.column_count(),
        }
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
fn compute_i64_aggregate(
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
        log::debug!("[SIMD] Using auto-vectorized path for i64 aggregate with {} values (no nulls)", values.len());
        return match op {
            AggregateOp::Sum => Ok(SqlValue::Integer(simd_sum_i64(values))),
            AggregateOp::Count => Ok(SqlValue::Integer(values.len() as i64)),
            AggregateOp::Avg => {
                let sum = simd_sum_i64(values);
                Ok(SqlValue::Double(sum as f64 / values.len() as f64))
            }
            AggregateOp::Min => {
                simd_min_i64(values)
                    .map(SqlValue::Integer)
                    .ok_or_else(|| ExecutorError::SimdOperationFailed {
                        operation: "MIN".to_string(),
                        reason: "empty set".to_string(),
                    })
            }
            AggregateOp::Max => {
                simd_max_i64(values)
                    .map(SqlValue::Integer)
                    .ok_or_else(|| ExecutorError::SimdOperationFailed {
                        operation: "MAX".to_string(),
                        reason: "empty set".to_string(),
                    })
            }
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
            min.map(SqlValue::Integer)
                .ok_or_else(|| ExecutorError::SimdOperationFailed {
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
            max.map(SqlValue::Integer)
                .ok_or_else(|| ExecutorError::SimdOperationFailed {
                    operation: "MAX".to_string(),
                    reason: "all values NULL".to_string(),
                })
        }
    }
}

/// Compute aggregate on f64 column using auto-vectorized operations
///
/// Optimized to avoid allocations when there are no NULL values.
fn compute_f64_aggregate(
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
        log::debug!("[SIMD] Using auto-vectorized path for f64 aggregate with {} values (no nulls)", values.len());
        return match op {
            AggregateOp::Sum => Ok(SqlValue::Double(simd_sum_f64(values))),
            AggregateOp::Count => Ok(SqlValue::Integer(values.len() as i64)),
            AggregateOp::Avg => {
                let sum = simd_sum_f64(values);
                Ok(SqlValue::Double(sum / values.len() as f64))
            }
            AggregateOp::Min => {
                simd_min_f64(values)
                    .map(SqlValue::Double)
                    .ok_or_else(|| ExecutorError::SimdOperationFailed {
                        operation: "MIN".to_string(),
                        reason: "empty set".to_string(),
                    })
            }
            AggregateOp::Max => {
                simd_max_f64(values)
                    .map(SqlValue::Double)
                    .ok_or_else(|| ExecutorError::SimdOperationFailed {
                        operation: "MAX".to_string(),
                        reason: "empty set".to_string(),
                    })
            }
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
            min.map(SqlValue::Double)
                .ok_or_else(|| ExecutorError::SimdOperationFailed {
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
            max.map(SqlValue::Double)
                .ok_or_else(|| ExecutorError::SimdOperationFailed {
                    operation: "MAX".to_string(),
                    reason: "all values NULL".to_string(),
                })
        }
    }
}

/// Scalar fallback for non-numeric column types
fn compute_mixed_aggregate(
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
                        return Err(ExecutorError::UnsupportedExpression(
                            format!("Cannot compute SUM/AVG on non-numeric value: {:?}", value)
                        ));
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
                        let is_better = if op == AggregateOp::Min {
                            value < current
                        } else {
                            value > current
                        };
                        if is_better { value } else { current }
                    }
                });
            }
            Ok(result.unwrap_or(SqlValue::Null))
        }
    }
}

/// Create a filter bitmap for a batch (scalar fallback)
#[allow(dead_code)]
fn create_filter_bitmap_for_batch(
    batch: &ColumnarBatch,
    predicates: &[ColumnPredicate],
) -> Result<Vec<bool>, ExecutorError> {
    let row_count = batch.row_count();
    let mut bitmap = vec![true; row_count];

    for predicate in predicates {
        for row_idx in 0..row_count {
            if !bitmap[row_idx] {
                continue; // Already filtered out
            }

            let col_idx = match predicate {
                ColumnPredicate::LessThan { column_idx, .. }
                | ColumnPredicate::GreaterThan { column_idx, .. }
                | ColumnPredicate::LessThanOrEqual { column_idx, .. }
                | ColumnPredicate::GreaterThanOrEqual { column_idx, .. }
                | ColumnPredicate::Equal { column_idx, .. }
                | ColumnPredicate::NotEqual { column_idx, .. }
                | ColumnPredicate::Between { column_idx, .. }
                | ColumnPredicate::Like { column_idx, .. } => *column_idx,
            };

            let value = batch.get_value(row_idx, col_idx)?;

            // NULL values never pass predicates
            if matches!(value, SqlValue::Null) {
                bitmap[row_idx] = false;
                continue;
            }

            bitmap[row_idx] = super::filter::evaluate_predicate(predicate, &value);
        }
    }

    Ok(bitmap)
}

/// Apply a filter bitmap to a batch (scalar fallback)
#[allow(dead_code)]
fn apply_filter_bitmap_to_batch(
    batch: &ColumnarBatch,
    bitmap: &[bool],
) -> Result<ColumnarBatch, ExecutorError> {
    // Count passing rows
    let passing_count = bitmap.iter().filter(|&&b| b).count();

    if passing_count == 0 {
        return ColumnarBatch::empty(batch.column_count());
    }

    // For scalar fallback, convert to rows, filter, convert back
    // This is inefficient but maintains correctness
    let rows = batch.to_rows()?;
    let filtered_rows: Vec<Row> = rows
        .into_iter()
        .zip(bitmap.iter())
        .filter_map(|(row, &pass)| if pass { Some(row) } else { None })
        .collect();

    ColumnarBatch::from_rows(&filtered_rows)
}

/// Compute an aggregate over an expression on a ColumnarBatch
///
/// For simple binary operations like `col_a * col_b`, this evaluates the
/// expression element-wise on the columnar data and then aggregates.
fn compute_expression_aggregate_batch(
    batch: &ColumnarBatch,
    expr: &Expression,
    op: AggregateOp,
) -> Result<SqlValue, ExecutorError> {
    // Try to evaluate the expression as a simple binary operation on columns
    if let Expression::BinaryOp { left, op: bin_op, right } = expr {
        // For now, only support multiplication (most common in TPC-H)
        if *bin_op == BinaryOperator::Multiply {
            // Get column indices from left and right operands
            if let (
                Expression::ColumnRef { column: col1, .. },
                Expression::ColumnRef { column: col2, .. }
            ) = (left.as_ref(), right.as_ref()) {
                // Find column indices by name
                let left_idx = batch.column_index_by_name(col1);
                let right_idx = batch.column_index_by_name(col2);

                if let (Some(l_idx), Some(r_idx)) = (left_idx, right_idx) {
                    return compute_multiply_aggregate(batch, l_idx, r_idx, op);
                }
            }
        }
    }

    // Fall back to row-by-row evaluation for complex expressions
    // This is slower but handles all cases
    let row_count = batch.row_count();
    let mut sum = 0.0f64;
    let mut count = 0i64;

    for row_idx in 0..row_count {
        // Evaluate expression for this row
        if let Ok(value) = eval_expr_on_batch(batch, expr, row_idx) {
            match value {
                SqlValue::Integer(v) => { sum += v as f64; count += 1; }
                SqlValue::Double(v) => { sum += v; count += 1; }
                SqlValue::Float(v) => { sum += v as f64; count += 1; }
                SqlValue::Bigint(v) => { sum += v as f64; count += 1; }
                SqlValue::Numeric(v) => { sum += v; count += 1; }
                SqlValue::Null => {}
                _ => {}
            }
        }
    }

    match op {
        AggregateOp::Sum => Ok(if count > 0 { SqlValue::Double(sum) } else { SqlValue::Null }),
        AggregateOp::Count => Ok(SqlValue::Integer(count)),
        AggregateOp::Avg => Ok(if count > 0 { SqlValue::Double(sum / count as f64) } else { SqlValue::Null }),
        _ => Err(ExecutorError::UnsupportedExpression(
            "MIN/MAX not supported for expression aggregates".to_string()
        ))
    }
}

/// Optimized path for SUM(col_a * col_b) - the most common expression aggregate in TPC-H
///
/// Uses SIMD-accelerated sum_product_f64 for maximum performance. This provides
/// ~4x speedup compared to the previous Vec<Option<f64>> approach.
fn compute_multiply_aggregate(
    batch: &ColumnarBatch,
    left_idx: usize,
    right_idx: usize,
    op: AggregateOp,
) -> Result<SqlValue, ExecutorError> {
    let left_col = batch.column(left_idx).ok_or_else(|| {
        ExecutorError::ColumnarColumnNotFound {
            column_index: left_idx,
            batch_columns: batch.column_count(),
        }
    })?;
    let right_col = batch.column(right_idx).ok_or_else(|| {
        ExecutorError::ColumnarColumnNotFound {
            column_index: right_idx,
            batch_columns: batch.column_count(),
        }
    })?;

    // Fast path: Both columns are Float64 - use SIMD-accelerated sum_product
    if let (
        ColumnArray::Float64(left_values, left_nulls),
        ColumnArray::Float64(right_values, right_nulls),
    ) = (left_col, right_col)
    {
        if left_values.len() != right_values.len() {
            return Err(ExecutorError::ColumnarLengthMismatch {
                context: "multiply_aggregate".to_string(),
                expected: left_values.len(),
                actual: right_values.len(),
            });
        }

        let (sum, count) = simd_ops::sum_product_f64_masked(
            left_values,
            right_values,
            left_nulls.as_ref().map(|v| v.as_slice()),
            right_nulls.as_ref().map(|v| v.as_slice()),
        );

        return match op {
            AggregateOp::Sum => Ok(if count > 0 { SqlValue::Double(sum) } else { SqlValue::Null }),
            AggregateOp::Count => Ok(SqlValue::Integer(count)),
            AggregateOp::Avg => Ok(if count > 0 { SqlValue::Double(sum / count as f64) } else { SqlValue::Null }),
            _ => Err(ExecutorError::UnsupportedExpression(
                "MIN/MAX not supported for expression aggregates".to_string()
            ))
        };
    }

    // Fast path: Both columns are Int64 - convert to f64 and use SIMD
    if let (
        ColumnArray::Int64(left_values, left_nulls),
        ColumnArray::Int64(right_values, right_nulls),
    ) = (left_col, right_col)
    {
        if left_values.len() != right_values.len() {
            return Err(ExecutorError::ColumnarLengthMismatch {
                context: "multiply_aggregate".to_string(),
                expected: left_values.len(),
                actual: right_values.len(),
            });
        }

        // Check if we have any nulls
        let has_left_nulls = left_nulls.as_ref().map_or(false, |n| n.iter().any(|&x| x));
        let has_right_nulls = right_nulls.as_ref().map_or(false, |n| n.iter().any(|&x| x));

        if !has_left_nulls && !has_right_nulls {
            // No nulls: use 4-accumulator SIMD pattern directly on i64
            let len = left_values.len();
            let (mut s0, mut s1, mut s2, mut s3) = (0i64, 0i64, 0i64, 0i64);
            let chunks = len / 4;

            for i in 0..chunks {
                let off = i * 4;
                s0 = s0.wrapping_add(left_values[off].wrapping_mul(right_values[off]));
                s1 = s1.wrapping_add(left_values[off + 1].wrapping_mul(right_values[off + 1]));
                s2 = s2.wrapping_add(left_values[off + 2].wrapping_mul(right_values[off + 2]));
                s3 = s3.wrapping_add(left_values[off + 3].wrapping_mul(right_values[off + 3]));
            }

            let mut sum = s0.wrapping_add(s1).wrapping_add(s2).wrapping_add(s3);
            for i in (chunks * 4)..len {
                sum = sum.wrapping_add(left_values[i].wrapping_mul(right_values[i]));
            }

            return match op {
                AggregateOp::Sum => Ok(SqlValue::Integer(sum)),
                AggregateOp::Count => Ok(SqlValue::Integer(len as i64)),
                AggregateOp::Avg => Ok(if len > 0 { SqlValue::Double(sum as f64 / len as f64) } else { SqlValue::Null }),
                _ => Err(ExecutorError::UnsupportedExpression(
                    "MIN/MAX not supported for expression aggregates".to_string()
                ))
            };
        }

        // Has nulls: use masked path
        let null_a = left_nulls.as_ref().map(|v| v.as_slice());
        let null_b = right_nulls.as_ref().map(|v| v.as_slice());

        let mut sum = 0i64;
        let mut count = 0i64;
        for i in 0..left_values.len() {
            let is_null_a = null_a.map_or(false, |n| n.get(i).copied().unwrap_or(false));
            let is_null_b = null_b.map_or(false, |n| n.get(i).copied().unwrap_or(false));
            if !is_null_a && !is_null_b {
                sum = sum.wrapping_add(left_values[i].wrapping_mul(right_values[i]));
                count += 1;
            }
        }

        return match op {
            AggregateOp::Sum => Ok(if count > 0 { SqlValue::Integer(sum) } else { SqlValue::Null }),
            AggregateOp::Count => Ok(SqlValue::Integer(count)),
            AggregateOp::Avg => Ok(if count > 0 { SqlValue::Double(sum as f64 / count as f64) } else { SqlValue::Null }),
            _ => Err(ExecutorError::UnsupportedExpression(
                "MIN/MAX not supported for expression aggregates".to_string()
            ))
        };
    }

    // Fallback: Mixed column types - use the slower Vec<Option<f64>> path
    let left_f64 = column_to_f64_vec(left_col)?;
    let right_f64 = column_to_f64_vec(right_col)?;

    if left_f64.len() != right_f64.len() {
        return Err(ExecutorError::ColumnarLengthMismatch {
            context: "multiply_aggregate".to_string(),
            expected: left_f64.len(),
            actual: right_f64.len(),
        });
    }

    // Vectorized multiply and aggregate
    let mut sum = 0.0f64;
    let mut count = 0i64;

    for i in 0..left_f64.len() {
        if let (Some(l), Some(r)) = (left_f64[i], right_f64[i]) {
            sum += l * r;
            count += 1;
        }
    }

    match op {
        AggregateOp::Sum => Ok(if count > 0 { SqlValue::Double(sum) } else { SqlValue::Null }),
        AggregateOp::Count => Ok(SqlValue::Integer(count)),
        AggregateOp::Avg => Ok(if count > 0 { SqlValue::Double(sum / count as f64) } else { SqlValue::Null }),
        _ => Err(ExecutorError::UnsupportedExpression(
            "MIN/MAX not supported for expression aggregates".to_string()
        ))
    }
}

/// Convert a ColumnArray to Vec<Option<f64>> for arithmetic operations
fn column_to_f64_vec(column: &ColumnArray) -> Result<Vec<Option<f64>>, ExecutorError> {
    match column {
        ColumnArray::Int64(values, nulls) => {
            Ok(values.iter().enumerate().map(|(i, &v)| {
                if nulls.as_ref().map_or(false, |n| n[i]) {
                    None
                } else {
                    Some(v as f64)
                }
            }).collect())
        }
        ColumnArray::Float64(values, nulls) => {
            Ok(values.iter().enumerate().map(|(i, &v)| {
                if nulls.as_ref().map_or(false, |n| n[i]) {
                    None
                } else {
                    Some(v)
                }
            }).collect())
        }
        ColumnArray::Mixed(values) => {
            Ok(values.iter().map(|v| {
                match v {
                    SqlValue::Integer(n) => Some(*n as f64),
                    SqlValue::Bigint(n) => Some(*n as f64),
                    SqlValue::Float(n) => Some(*n as f64),
                    SqlValue::Double(n) => Some(*n),
                    SqlValue::Numeric(n) => Some(*n),
                    SqlValue::Null => None,
                    _ => None,
                }
            }).collect())
        }
        _ => Err(ExecutorError::UnsupportedArrayType {
            operation: "column_to_f64".to_string(),
            array_type: format!("{:?}", std::mem::discriminant(column)),
        })
    }
}

/// Evaluate an expression for a single row in a ColumnarBatch
fn eval_expr_on_batch(
    batch: &ColumnarBatch,
    expr: &Expression,
    row_idx: usize,
) -> Result<SqlValue, ExecutorError> {
    match expr {
        Expression::ColumnRef { column, .. } => {
            if let Some(col_idx) = batch.column_index_by_name(column) {
                batch.get_value(row_idx, col_idx)
            } else {
                Ok(SqlValue::Null)
            }
        }
        Expression::Literal(val) => Ok(val.clone()),
        Expression::BinaryOp { left, op, right } => {
            let left_val = eval_expr_on_batch(batch, left, row_idx)?;
            let right_val = eval_expr_on_batch(batch, right, row_idx)?;

            // Simple arithmetic evaluation
            match (left_val, right_val) {
                (SqlValue::Null, _) | (_, SqlValue::Null) => Ok(SqlValue::Null),
                (l, r) => {
                    let l_f64 = sql_value_to_f64(&l).ok_or_else(|| {
                        ExecutorError::ColumnarTypeMismatch {
                            operation: "binary_op".to_string(),
                            left_type: format!("{:?}", l),
                            right_type: None,
                        }
                    })?;
                    let r_f64 = sql_value_to_f64(&r).ok_or_else(|| {
                        ExecutorError::ColumnarTypeMismatch {
                            operation: "binary_op".to_string(),
                            left_type: format!("{:?}", r),
                            right_type: None,
                        }
                    })?;

                    let result = match op {
                        BinaryOperator::Plus => l_f64 + r_f64,
                        BinaryOperator::Minus => l_f64 - r_f64,
                        BinaryOperator::Multiply => l_f64 * r_f64,
                        BinaryOperator::Divide => l_f64 / r_f64,
                        _ => return Err(ExecutorError::UnsupportedExpression(
                            format!("Unsupported binary operator: {:?}", op)
                        ))
                    };
                    Ok(SqlValue::Double(result))
                }
            }
        }
        _ => Err(ExecutorError::UnsupportedExpression(
            "Complex expression not supported".to_string()
        ))
    }
}

/// Convert SqlValue to f64
fn sql_value_to_f64(val: &SqlValue) -> Option<f64> {
    match val {
        SqlValue::Integer(v) => Some(*v as f64),
        SqlValue::Bigint(v) => Some(*v as f64),
        SqlValue::Float(v) => Some(*v as f64),
        SqlValue::Double(v) => Some(*v),
        SqlValue::Numeric(v) => Some(*v),
        SqlValue::Smallint(v) => Some(*v as f64),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_batch() -> ColumnarBatch {
        let rows = vec![
            Row::new(vec![SqlValue::Integer(10), SqlValue::Double(1.5)]),
            Row::new(vec![SqlValue::Integer(20), SqlValue::Double(2.5)]),
            Row::new(vec![SqlValue::Integer(30), SqlValue::Double(3.5)]),
            Row::new(vec![SqlValue::Integer(40), SqlValue::Double(4.5)]),
        ];
        ColumnarBatch::from_rows(&rows).unwrap()
    }

    #[test]
    fn test_execute_columnar_batch_sum() {
        let batch = make_test_batch();
        let aggregates = vec![
            AggregateSpec {
                op: AggregateOp::Sum,
                source: AggregateSource::Column(0),
            },
        ];

        let result = execute_columnar_batch(&batch, &[], &aggregates, None).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].get(0), Some(&SqlValue::Integer(100)));
    }

    #[test]
    fn test_execute_columnar_batch_with_filter() {
        let batch = make_test_batch();
        let predicates = vec![
            ColumnPredicate::LessThan {
                column_idx: 0,
                value: SqlValue::Integer(25),
            },
        ];
        let aggregates = vec![
            AggregateSpec {
                op: AggregateOp::Sum,
                source: AggregateSource::Column(0),
            },
        ];

        let result = execute_columnar_batch(&batch, &predicates, &aggregates, None).unwrap();
        assert_eq!(result.len(), 1);
        // Only rows 0 (10) and 1 (20) pass the filter
        assert_eq!(result[0].get(0), Some(&SqlValue::Integer(30)));
    }

    #[test]
    fn test_execute_columnar_batch_multiple_aggregates() {
        let batch = make_test_batch();
        let aggregates = vec![
            AggregateSpec {
                op: AggregateOp::Sum,
                source: AggregateSource::Column(0),
            },
            AggregateSpec {
                op: AggregateOp::Avg,
                source: AggregateSource::Column(1),
            },
            AggregateSpec {
                op: AggregateOp::Count,
                source: AggregateSource::CountStar,
            },
        ];

        let result = execute_columnar_batch(&batch, &[], &aggregates, None).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].len(), 3);

        // SUM(col0) = 100
        assert_eq!(result[0].get(0), Some(&SqlValue::Integer(100)));

        // AVG(col1) = 3.0
        if let Some(SqlValue::Double(avg)) = result[0].get(1) {
            assert!((avg - 3.0).abs() < 0.001);
        } else {
            panic!("Expected Double for AVG");
        }

        // COUNT(*) = 4
        assert_eq!(result[0].get(2), Some(&SqlValue::Integer(4)));
    }

    #[test]
    fn test_execute_columnar_batch_empty() {
        let batch = ColumnarBatch::new(2);
        let aggregates = vec![
            AggregateSpec {
                op: AggregateOp::Sum,
                source: AggregateSource::Column(0),
            },
            AggregateSpec {
                op: AggregateOp::Count,
                source: AggregateSource::CountStar,
            },
        ];

        let result = execute_columnar_batch(&batch, &[], &aggregates, None).unwrap();
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].get(0), Some(&SqlValue::Null)); // SUM of empty = NULL
        assert_eq!(result[0].get(1), Some(&SqlValue::Integer(0))); // COUNT of empty = 0
    }

    /// Test that aggregation correctly handles NULL values.
    /// This verifies the fix for NULL handling in fused column aggregates.
    #[test]
    fn test_execute_columnar_batch_with_nulls() {
        // Create batch with some NULL values
        let rows = vec![
            Row::new(vec![SqlValue::Integer(10), SqlValue::Double(1.5)]),
            Row::new(vec![SqlValue::Null, SqlValue::Double(2.5)]),       // NULL in first column
            Row::new(vec![SqlValue::Integer(30), SqlValue::Null]),       // NULL in second column
            Row::new(vec![SqlValue::Integer(40), SqlValue::Double(4.5)]),
        ];
        let batch = ColumnarBatch::from_rows(&rows).unwrap();

        let aggregates = vec![
            AggregateSpec {
                op: AggregateOp::Sum,
                source: AggregateSource::Column(0),
            },
            AggregateSpec {
                op: AggregateOp::Sum,
                source: AggregateSource::Column(1),
            },
            AggregateSpec {
                op: AggregateOp::Count,
                source: AggregateSource::CountStar,
            },
        ];

        let result = execute_columnar_batch(&batch, &[], &aggregates, None).unwrap();
        assert_eq!(result.len(), 1);

        // SUM(col0) should be 10 + 30 + 40 = 80 (NULL excluded)
        assert_eq!(result[0].get(0), Some(&SqlValue::Integer(80)));

        // SUM(col1) should be 1.5 + 2.5 + 4.5 = 8.5 (NULL excluded)
        if let Some(SqlValue::Double(sum)) = result[0].get(1) {
            assert!((sum - 8.5).abs() < 0.001);
        } else {
            panic!("Expected Double for SUM(col1)");
        }

        // COUNT(*) = 4 (counts all rows regardless of NULLs)
        assert_eq!(result[0].get(2), Some(&SqlValue::Integer(4)));
    }

    /// Test aggregation with NULLs and filter predicates.
    /// Verifies fused path correctly falls back when NULLs are present.
    #[test]
    fn test_execute_columnar_batch_with_nulls_and_filter() {
        let rows = vec![
            Row::new(vec![SqlValue::Integer(10), SqlValue::Double(1.0)]),
            Row::new(vec![SqlValue::Integer(20), SqlValue::Null]),       // NULL - should be excluded from sum
            Row::new(vec![SqlValue::Integer(30), SqlValue::Double(3.0)]),
            Row::new(vec![SqlValue::Integer(40), SqlValue::Double(4.0)]),
        ];
        let batch = ColumnarBatch::from_rows(&rows).unwrap();

        // Filter: col0 < 35 (includes rows 0, 1, 2)
        let predicates = vec![
            ColumnPredicate::LessThan {
                column_idx: 0,
                value: SqlValue::Integer(35),
            },
        ];

        let aggregates = vec![
            AggregateSpec {
                op: AggregateOp::Sum,
                source: AggregateSource::Column(1),
            },
        ];

        let result = execute_columnar_batch(&batch, &predicates, &aggregates, None).unwrap();
        assert_eq!(result.len(), 1);

        // SUM(col1) where col0 < 35: 1.0 + 3.0 = 4.0 (row 1's NULL excluded)
        if let Some(SqlValue::Double(sum)) = result[0].get(0) {
            assert!((sum - 4.0).abs() < 0.001);
        } else {
            panic!("Expected Double for SUM(col1)");
        }
    }
}
