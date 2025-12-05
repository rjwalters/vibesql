//! Fused filter+aggregate execution path
//!
//! This module implements an optimized execution path that avoids creating
//! intermediate filtered batches by fusing filtering and aggregation into
//! a single pass. This provides significant performance improvement for
//! simple aggregate queries like TPC-H Q6.

use super::super::aggregate::{AggregateOp, AggregateSource, AggregateSpec};
use super::super::batch::{ColumnArray, ColumnarBatch};
use super::super::simd_filter::simd_create_filter_mask;
use super::super::simd_ops;
use crate::errors::ExecutorError;
use vibesql_ast::{BinaryOperator, Expression};
use vibesql_types::SqlValue;

/// Check if all aggregates can use the fused filter+aggregate optimization
pub fn can_use_fused_aggregation(aggregates: &[AggregateSpec]) -> bool {
    aggregates.iter().all(|spec| {
        matches!(
            spec.op,
            AggregateOp::Sum
                | AggregateOp::Count
                | AggregateOp::Min
                | AggregateOp::Max
                | AggregateOp::Avg
        ) && matches!(
            spec.source,
            AggregateSource::Column(_)
                | AggregateSource::CountStar
                | AggregateSource::Expression(_)
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
pub fn execute_fused_filter_aggregate(
    batch: &ColumnarBatch,
    predicates: &[super::super::filter::ColumnPredicate],
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
            mask_time,
            passing,
            batch.row_count()
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
            agg_time,
            aggregates.len()
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
    let column = batch.column(col_idx).ok_or_else(|| ExecutorError::ColumnarColumnNotFound {
        column_index: col_idx,
        batch_columns: batch.column_count(),
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
        AggregateOp::Sum => Ok(SqlValue::Integer(simd_ops::sum_i64_filtered(values, filter_mask))),
        AggregateOp::Count => Ok(SqlValue::Integer(simd_ops::count_filtered(filter_mask))),
        AggregateOp::Avg => {
            let sum = simd_ops::sum_i64_filtered(values, filter_mask);
            let count = simd_ops::count_filtered(filter_mask);
            if count == 0 {
                Ok(SqlValue::Null)
            } else {
                Ok(SqlValue::Double(sum as f64 / count as f64))
            }
        }
        AggregateOp::Min => simd_ops::min_i64_filtered(values, filter_mask)
            .map(SqlValue::Integer)
            .ok_or_else(|| ExecutorError::SimdOperationFailed {
                operation: "MIN".to_string(),
                reason: "no rows pass filter".to_string(),
            }),
        AggregateOp::Max => simd_ops::max_i64_filtered(values, filter_mask)
            .map(SqlValue::Integer)
            .ok_or_else(|| ExecutorError::SimdOperationFailed {
                operation: "MAX".to_string(),
                reason: "no rows pass filter".to_string(),
            }),
    }
}

/// Compute aggregate on f64 values using filter mask
fn compute_fused_f64_aggregate(
    values: &[f64],
    op: AggregateOp,
    filter_mask: &[bool],
) -> Result<SqlValue, ExecutorError> {
    match op {
        AggregateOp::Sum => Ok(SqlValue::Double(simd_ops::sum_f64_filtered(values, filter_mask))),
        AggregateOp::Count => Ok(SqlValue::Integer(simd_ops::count_filtered(filter_mask))),
        AggregateOp::Avg => {
            let sum = simd_ops::sum_f64_filtered(values, filter_mask);
            let count = simd_ops::count_filtered(filter_mask);
            if count == 0 {
                Ok(SqlValue::Null)
            } else {
                Ok(SqlValue::Double(sum / count as f64))
            }
        }
        AggregateOp::Min => simd_ops::min_f64_filtered(values, filter_mask)
            .map(SqlValue::Double)
            .ok_or_else(|| ExecutorError::SimdOperationFailed {
                operation: "MIN".to_string(),
                reason: "no rows pass filter".to_string(),
            }),
        AggregateOp::Max => simd_ops::max_f64_filtered(values, filter_mask)
            .map(SqlValue::Double)
            .ok_or_else(|| ExecutorError::SimdOperationFailed {
                operation: "MAX".to_string(),
                reason: "no rows pass filter".to_string(),
            }),
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
                Expression::ColumnRef { column: col2, .. },
            ) = (left.as_ref(), right.as_ref())
            {
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
        "Complex expression not supported in fused path".to_string(),
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
    let left_col = batch.column(left_idx).ok_or_else(|| ExecutorError::ColumnarColumnNotFound {
        column_index: left_idx,
        batch_columns: batch.column_count(),
    })?;
    let right_col =
        batch.column(right_idx).ok_or_else(|| ExecutorError::ColumnarColumnNotFound {
            column_index: right_idx,
            batch_columns: batch.column_count(),
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
            AggregateOp::Avg => {
                Ok(if count > 0 { SqlValue::Double(sum / count as f64) } else { SqlValue::Null })
            }
            _ => Err(ExecutorError::UnsupportedExpression(
                "MIN/MAX not supported for expression aggregates".to_string(),
            )),
        };
    }

    // Fall back to error for non-Float64 columns (triggers standard path)
    Err(ExecutorError::UnsupportedArrayType {
        operation: "fused_multiply_aggregate".to_string(),
        array_type: "non-Float64".to_string(),
    })
}
