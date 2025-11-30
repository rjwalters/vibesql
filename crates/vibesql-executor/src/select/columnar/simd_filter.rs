//! Auto-vectorized filtering for columnar batches
//!
//! Uses the centralized simd_ops module for consistent, optimized operations.

use std::sync::Arc;
use super::batch::{ColumnArray, ColumnarBatch};
use super::filter::{parse_date_string, ColumnPredicate};
use super::simd_ops::{self, PackedMask};
use super::string_ops::{
    batch_string_eq, batch_string_ge, batch_string_gt, batch_string_le, batch_string_like,
    batch_string_lt, batch_string_ne, LikePattern,
};
use crate::errors::ExecutorError;
use vibesql_types::SqlValue;

// Re-export comparison functions from simd_ops module for consistency
// Note: comparisons vectorize well even with simple iterator patterns,
// but we centralize them in simd_ops to prevent accidental regressions.

#[inline]
fn simd_lt_i64(values: &[i64], threshold: i64) -> Vec<bool> {
    simd_ops::lt_i64(values, threshold)
}

#[inline]
fn simd_gt_i64(values: &[i64], threshold: i64) -> Vec<bool> {
    simd_ops::gt_i64(values, threshold)
}

#[inline]
fn simd_le_i64(values: &[i64], threshold: i64) -> Vec<bool> {
    simd_ops::le_i64(values, threshold)
}

#[inline]
fn simd_ge_i64(values: &[i64], threshold: i64) -> Vec<bool> {
    simd_ops::ge_i64(values, threshold)
}

#[inline]
fn simd_eq_i64(values: &[i64], target: i64) -> Vec<bool> {
    simd_ops::eq_i64(values, target)
}

#[inline]
fn simd_lt_i32(values: &[i32], threshold: i32) -> Vec<bool> {
    simd_ops::lt_i32(values, threshold)
}

#[inline]
fn simd_gt_i32(values: &[i32], threshold: i32) -> Vec<bool> {
    simd_ops::gt_i32(values, threshold)
}

#[inline]
fn simd_le_i32(values: &[i32], threshold: i32) -> Vec<bool> {
    simd_ops::le_i32(values, threshold)
}

#[inline]
fn simd_ge_i32(values: &[i32], threshold: i32) -> Vec<bool> {
    simd_ops::ge_i32(values, threshold)
}

#[inline]
fn simd_eq_i32(values: &[i32], target: i32) -> Vec<bool> {
    simd_ops::eq_i32(values, target)
}

#[inline]
fn simd_lt_f64(values: &[f64], threshold: f64) -> Vec<bool> {
    simd_ops::lt_f64(values, threshold)
}

#[inline]
fn simd_gt_f64(values: &[f64], threshold: f64) -> Vec<bool> {
    simd_ops::gt_f64(values, threshold)
}

#[inline]
fn simd_le_f64(values: &[f64], threshold: f64) -> Vec<bool> {
    simd_ops::le_f64(values, threshold)
}

#[inline]
fn simd_ge_f64(values: &[f64], threshold: f64) -> Vec<bool> {
    simd_ops::ge_f64(values, threshold)
}

#[inline]
fn simd_eq_f64(values: &[f64], target: f64) -> Vec<bool> {
    simd_ops::eq_f64(values, target)
}

#[inline]
fn simd_ne_i64(values: &[i64], target: i64) -> Vec<bool> {
    simd_ops::ne_i64(values, target)
}

#[inline]
fn simd_ne_i32(values: &[i32], target: i32) -> Vec<bool> {
    simd_ops::ne_i32(values, target)
}

#[inline]
fn simd_ne_f64(values: &[f64], target: f64) -> Vec<bool> {
    simd_ops::ne_f64(values, target)
}

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
/// ```ignore
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

    let column_idx = match predicate {
        ColumnPredicate::LessThan { column_idx, .. }
        | ColumnPredicate::GreaterThan { column_idx, .. }
        | ColumnPredicate::GreaterThanOrEqual { column_idx, .. }
        | ColumnPredicate::LessThanOrEqual { column_idx, .. }
        | ColumnPredicate::Equal { column_idx, .. }
        | ColumnPredicate::NotEqual { column_idx, .. }
        | ColumnPredicate::Between { column_idx, .. }
        | ColumnPredicate::Like { column_idx, .. } => *column_idx,
    };

    let column = batch
        .column(column_idx)
        .ok_or_else(|| ExecutorError::ColumnarColumnNotFound {
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

/// Evaluate predicate on i64 column returning packed mask
fn evaluate_predicate_i64_packed(
    predicate: &ColumnPredicate,
    values: &[i64],
    nulls: Option<&[bool]>,
) -> Result<PackedMask, ExecutorError> {
    let mut result = match predicate {
        ColumnPredicate::LessThan { value, .. } => {
            if let SqlValue::Integer(threshold) = value {
                simd_ops::lt_i64_packed(values, *threshold)
            } else if let SqlValue::Bigint(threshold) = value {
                simd_ops::lt_i64_packed(values, *threshold)
            } else if let Some(threshold) = value_to_timestamp_i64(value) {
                simd_ops::lt_i64_packed(values, threshold)
            } else {
                // Type mismatch: fall back to Vec<bool> path
                let threshold = value_to_f64(value)
                    .ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                        operation: "comparison".to_string(),
                        left_type: "Int64".to_string(),
                        right_type: Some(format!("{:?}", value)),
                    })?;
                let f64_values: Vec<f64> = values.iter().map(|&v| v as f64).collect();
                simd_ops::lt_f64_packed(&f64_values, threshold)
            }
        }

        ColumnPredicate::GreaterThan { value, .. } => {
            if let SqlValue::Integer(threshold) = value {
                simd_ops::gt_i64_packed(values, *threshold)
            } else if let SqlValue::Bigint(threshold) = value {
                simd_ops::gt_i64_packed(values, *threshold)
            } else {
                let threshold = value_to_f64(value)
                    .ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                        operation: "comparison".to_string(),
                        left_type: "Int64".to_string(),
                        right_type: Some(format!("{:?}", value)),
                    })?;
                let f64_values: Vec<f64> = values.iter().map(|&v| v as f64).collect();
                simd_ops::gt_f64_packed(&f64_values, threshold)
            }
        }

        ColumnPredicate::Equal { value, .. } => {
            if let SqlValue::Integer(target) = value {
                simd_ops::eq_i64_packed(values, *target)
            } else if let SqlValue::Bigint(target) = value {
                simd_ops::eq_i64_packed(values, *target)
            } else {
                let target = value_to_f64(value)
                    .ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                        operation: "comparison".to_string(),
                        left_type: "Int64".to_string(),
                        right_type: Some(format!("{:?}", value)),
                    })?;
                let f64_values: Vec<f64> = values.iter().map(|&v| v as f64).collect();
                simd_ops::eq_f64_packed(&f64_values, target)
            }
        }

        ColumnPredicate::Between { low, high, .. } => {
            let low_i64 = match low {
                SqlValue::Integer(v) => *v,
                SqlValue::Bigint(v) => *v,
                _ => {
                    return Err(ExecutorError::ColumnarTypeMismatch {
                        operation: "BETWEEN".to_string(),
                        left_type: "Int64".to_string(),
                        right_type: None,
                    })
                }
            };
            let high_i64 = match high {
                SqlValue::Integer(v) => *v,
                SqlValue::Bigint(v) => *v,
                _ => {
                    return Err(ExecutorError::ColumnarTypeMismatch {
                        operation: "BETWEEN".to_string(),
                        left_type: "Int64".to_string(),
                        right_type: None,
                    })
                }
            };
            simd_ops::between_i64_packed(values, low_i64, high_i64)
        }

        ColumnPredicate::GreaterThanOrEqual { value, .. } => {
            if let SqlValue::Integer(threshold) = value {
                simd_ops::ge_i64_packed(values, *threshold)
            } else if let SqlValue::Bigint(threshold) = value {
                simd_ops::ge_i64_packed(values, *threshold)
            } else {
                let threshold = value_to_f64(value)
                    .ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                        operation: "comparison".to_string(),
                        left_type: "Int64".to_string(),
                        right_type: Some(format!("{:?}", value)),
                    })?;
                let f64_values: Vec<f64> = values.iter().map(|&v| v as f64).collect();
                simd_ops::ge_f64_packed(&f64_values, threshold)
            }
        }

        ColumnPredicate::LessThanOrEqual { value, .. } => {
            if let SqlValue::Integer(threshold) = value {
                simd_ops::le_i64_packed(values, *threshold)
            } else if let SqlValue::Bigint(threshold) = value {
                simd_ops::le_i64_packed(values, *threshold)
            } else {
                let threshold = value_to_f64(value)
                    .ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                        operation: "comparison".to_string(),
                        left_type: "Int64".to_string(),
                        right_type: Some(format!("{:?}", value)),
                    })?;
                let f64_values: Vec<f64> = values.iter().map(|&v| v as f64).collect();
                simd_ops::le_f64_packed(&f64_values, threshold)
            }
        }

        ColumnPredicate::NotEqual { value, .. } => {
            if let SqlValue::Integer(target) = value {
                simd_ops::ne_i64_packed(values, *target)
            } else if let SqlValue::Bigint(target) = value {
                simd_ops::ne_i64_packed(values, *target)
            } else {
                let target = value_to_f64(value)
                    .ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                        operation: "comparison".to_string(),
                        left_type: "Int64".to_string(),
                        right_type: Some(format!("{:?}", value)),
                    })?;
                let f64_values: Vec<f64> = values.iter().map(|&v| v as f64).collect();
                simd_ops::ne_f64_packed(&f64_values, target)
            }
        }

        ColumnPredicate::Like { .. } => {
            return Err(ExecutorError::ColumnarTypeMismatch {
                operation: "LIKE".to_string(),
                left_type: "Int64".to_string(),
                right_type: Some("String pattern".to_string()),
            });
        }
    };

    // Apply NULL mask: NULLs always fail predicates
    if let Some(null_mask) = nulls {
        for (i, &is_null) in null_mask.iter().enumerate() {
            if is_null {
                result.set(i, false);
            }
        }
    }

    Ok(result)
}

/// Evaluate predicate on i32 column returning packed mask (for dates)
fn evaluate_predicate_i32_packed(
    predicate: &ColumnPredicate,
    values: &[i32],
    nulls: Option<&[bool]>,
) -> Result<PackedMask, ExecutorError> {
    let mut result = match predicate {
        ColumnPredicate::LessThan { value, .. } => {
            let threshold = value_to_date_i32(value)
                .ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                    operation: "date comparison".to_string(),
                    left_type: "Date".to_string(),
                    right_type: None,
                })?;
            simd_ops::lt_i32_packed(values, threshold)
        }

        ColumnPredicate::GreaterThan { value, .. } => {
            let threshold = value_to_date_i32(value)
                .ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                    operation: "date comparison".to_string(),
                    left_type: "Date".to_string(),
                    right_type: None,
                })?;
            simd_ops::gt_i32_packed(values, threshold)
        }

        ColumnPredicate::GreaterThanOrEqual { value, .. } => {
            let threshold = value_to_date_i32(value)
                .ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                    operation: "date comparison".to_string(),
                    left_type: "Date".to_string(),
                    right_type: None,
                })?;
            simd_ops::ge_i32_packed(values, threshold)
        }

        ColumnPredicate::LessThanOrEqual { value, .. } => {
            let threshold = value_to_date_i32(value)
                .ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                    operation: "date comparison".to_string(),
                    left_type: "Date".to_string(),
                    right_type: None,
                })?;
            simd_ops::le_i32_packed(values, threshold)
        }

        ColumnPredicate::Equal { value, .. } => {
            let target = value_to_date_i32(value)
                .ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                    operation: "date comparison".to_string(),
                    left_type: "Date".to_string(),
                    right_type: None,
                })?;
            simd_ops::eq_i32_packed(values, target)
        }

        ColumnPredicate::Between { low, high, .. } => {
            let low_i32 = value_to_date_i32(low)
                .ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                    operation: "BETWEEN".to_string(),
                    left_type: "Date".to_string(),
                    right_type: Some(format!("{:?}", low)),
                })?;
            let high_i32 = value_to_date_i32(high)
                .ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                    operation: "BETWEEN".to_string(),
                    left_type: "Date".to_string(),
                    right_type: Some(format!("{:?}", high)),
                })?;
            simd_ops::between_i32_packed(values, low_i32, high_i32)
        }

        ColumnPredicate::NotEqual { value, .. } => {
            let target = value_to_date_i32(value)
                .ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                    operation: "date comparison".to_string(),
                    left_type: "Date".to_string(),
                    right_type: None,
                })?;
            simd_ops::ne_i32_packed(values, target)
        }

        ColumnPredicate::Like { .. } => {
            return Err(ExecutorError::ColumnarTypeMismatch {
                operation: "LIKE".to_string(),
                left_type: "Date".to_string(),
                right_type: Some("String pattern".to_string()),
            });
        }
    };

    // Apply NULL mask
    if let Some(null_mask) = nulls {
        for (i, &is_null) in null_mask.iter().enumerate() {
            if is_null {
                result.set(i, false);
            }
        }
    }

    Ok(result)
}

/// Evaluate predicate on f64 column returning packed mask
fn evaluate_predicate_f64_packed(
    predicate: &ColumnPredicate,
    values: &[f64],
    nulls: Option<&[bool]>,
) -> Result<PackedMask, ExecutorError> {
    let mut result = match predicate {
        ColumnPredicate::LessThan { value, .. } => {
            let threshold = value_to_f64(value)
                .ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                    operation: "comparison".to_string(),
                    left_type: "Float64".to_string(),
                    right_type: Some(format!("{:?}", value)),
                })?;
            simd_ops::lt_f64_packed(values, threshold)
        }

        ColumnPredicate::GreaterThan { value, .. } => {
            let threshold = value_to_f64(value)
                .ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                    operation: "comparison".to_string(),
                    left_type: "Float64".to_string(),
                    right_type: Some(format!("{:?}", value)),
                })?;
            simd_ops::gt_f64_packed(values, threshold)
        }

        ColumnPredicate::GreaterThanOrEqual { value, .. } => {
            let threshold = value_to_f64(value)
                .ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                    operation: "comparison".to_string(),
                    left_type: "Float64".to_string(),
                    right_type: Some(format!("{:?}", value)),
                })?;
            simd_ops::ge_f64_packed(values, threshold)
        }

        ColumnPredicate::LessThanOrEqual { value, .. } => {
            let threshold = value_to_f64(value)
                .ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                    operation: "comparison".to_string(),
                    left_type: "Float64".to_string(),
                    right_type: Some(format!("{:?}", value)),
                })?;
            simd_ops::le_f64_packed(values, threshold)
        }

        ColumnPredicate::Equal { value, .. } => {
            let target = value_to_f64(value)
                .ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                    operation: "comparison".to_string(),
                    left_type: "Float64".to_string(),
                    right_type: Some(format!("{:?}", value)),
                })?;
            simd_ops::eq_f64_packed(values, target)
        }

        ColumnPredicate::Between { low, high, .. } => {
            let low_f64 = value_to_f64(low)
                .ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                    operation: "BETWEEN".to_string(),
                    left_type: "Float64".to_string(),
                    right_type: Some(format!("{:?}", low)),
                })?;
            let high_f64 = value_to_f64(high)
                .ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                    operation: "BETWEEN".to_string(),
                    left_type: "Float64".to_string(),
                    right_type: Some(format!("{:?}", high)),
                })?;
            simd_ops::between_f64_packed(values, low_f64, high_f64)
        }

        ColumnPredicate::NotEqual { value, .. } => {
            let target = value_to_f64(value)
                .ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                    operation: "comparison".to_string(),
                    left_type: "Float64".to_string(),
                    right_type: Some(format!("{:?}", value)),
                })?;
            simd_ops::ne_f64_packed(values, target)
        }

        ColumnPredicate::Like { .. } => {
            return Err(ExecutorError::ColumnarTypeMismatch {
                operation: "LIKE".to_string(),
                left_type: "Float64".to_string(),
                right_type: Some("String pattern".to_string()),
            });
        }
    };

    // Apply NULL mask
    if let Some(null_mask) = nulls {
        for (i, &is_null) in null_mask.iter().enumerate() {
            if is_null {
                result.set(i, false);
            }
        }
    }

    Ok(result)
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

    let column_idx = match predicate {
        ColumnPredicate::LessThan { column_idx, .. }
        | ColumnPredicate::GreaterThan { column_idx, .. }
        | ColumnPredicate::GreaterThanOrEqual { column_idx, .. }
        | ColumnPredicate::LessThanOrEqual { column_idx, .. }
        | ColumnPredicate::Equal { column_idx, .. }
        | ColumnPredicate::NotEqual { column_idx, .. }
        | ColumnPredicate::Between { column_idx, .. }
        | ColumnPredicate::Like { column_idx, .. } => *column_idx,
    };

    let column = batch
        .column(column_idx)
        .ok_or_else(|| ExecutorError::ColumnarColumnNotFound {
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

/// Evaluate predicate on i64 column using SIMD
fn evaluate_predicate_i64_simd(
    predicate: &ColumnPredicate,
    values: &[i64],
    nulls: Option<&[bool]>,
) -> Result<Vec<bool>, ExecutorError> {
    let mut result = match predicate {
        ColumnPredicate::LessThan { value, .. } => {
            if let SqlValue::Integer(threshold) = value {
                simd_lt_i64(values, *threshold)
            } else if let SqlValue::Bigint(threshold) = value {
                simd_lt_i64(values, *threshold)
            } else if let Some(threshold) = value_to_timestamp_i64(value) {
                simd_lt_i64(values, threshold)
            } else {
                // Type mismatch: convert to f64 and use f64 SIMD
                let threshold = value_to_f64(value)
                    .ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                        operation: "comparison".to_string(),
                        left_type: "Int64".to_string(),
                        right_type: Some(format!("{:?}", value)),
                    })?;
                let f64_values: Vec<f64> = values.iter().map(|&v| v as f64).collect();
                simd_lt_f64(&f64_values, threshold)
            }
        }

        ColumnPredicate::GreaterThan { value, .. } => {
            if let SqlValue::Integer(threshold) = value {
                simd_gt_i64(values, *threshold)
            } else if let SqlValue::Bigint(threshold) = value {
                simd_gt_i64(values, *threshold)
            } else {
                let threshold = value_to_f64(value)
                    .ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                        operation: "comparison".to_string(),
                        left_type: "Int64".to_string(),
                        right_type: Some(format!("{:?}", value)),
                    })?;
                let f64_values: Vec<f64> = values.iter().map(|&v| v as f64).collect();
                simd_gt_f64(&f64_values, threshold)
            }
        }

        ColumnPredicate::Equal { value, .. } => {
            if let SqlValue::Integer(target) = value {
                simd_eq_i64(values, *target)
            } else if let SqlValue::Bigint(target) = value {
                simd_eq_i64(values, *target)
            } else {
                let target = value_to_f64(value)
                    .ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                        operation: "comparison".to_string(),
                        left_type: "Int64".to_string(),
                        right_type: Some(format!("{:?}", value)),
                    })?;
                let f64_values: Vec<f64> = values.iter().map(|&v| v as f64).collect();
                simd_eq_f64(&f64_values, target)
            }
        }

        ColumnPredicate::Between { low, high, .. } => {
            // BETWEEN is equivalent to: value >= low AND value <= high
            // Use single-pass BETWEEN operation for better performance
            let low_i64 = match low {
                SqlValue::Integer(v) => *v,
                SqlValue::Bigint(v) => *v,
                _ => {
                    return Err(ExecutorError::ColumnarTypeMismatch {
                        operation: "BETWEEN".to_string(),
                        left_type: "Int64".to_string(),
                        right_type: None,
                    })
                }
            };
            let high_i64 = match high {
                SqlValue::Integer(v) => *v,
                SqlValue::Bigint(v) => *v,
                _ => {
                    return Err(ExecutorError::ColumnarTypeMismatch {
                        operation: "BETWEEN".to_string(),
                        left_type: "Int64".to_string(),
                        right_type: None,
                    })
                }
            };

            // Single-pass BETWEEN check (more cache-efficient)
            simd_ops::between_i64(values, low_i64, high_i64)
        }

        ColumnPredicate::GreaterThanOrEqual { value, .. } => {
            if let SqlValue::Integer(threshold) = value {
                simd_ge_i64(values, *threshold)
            } else if let SqlValue::Bigint(threshold) = value {
                simd_ge_i64(values, *threshold)
            } else {
                let threshold = value_to_f64(value)
                    .ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                        operation: "comparison".to_string(),
                        left_type: "Int64".to_string(),
                        right_type: Some(format!("{:?}", value)),
                    })?;
                let f64_values: Vec<f64> = values.iter().map(|&v| v as f64).collect();
                simd_ge_f64(&f64_values, threshold)
            }
        }

        ColumnPredicate::LessThanOrEqual { value, .. } => {
            if let SqlValue::Integer(threshold) = value {
                simd_le_i64(values, *threshold)
            } else if let SqlValue::Bigint(threshold) = value {
                simd_le_i64(values, *threshold)
            } else {
                let threshold = value_to_f64(value)
                    .ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                        operation: "comparison".to_string(),
                        left_type: "Int64".to_string(),
                        right_type: Some(format!("{:?}", value)),
                    })?;
                let f64_values: Vec<f64> = values.iter().map(|&v| v as f64).collect();
                simd_le_f64(&f64_values, threshold)
            }
        }

        ColumnPredicate::NotEqual { value, .. } => {
            if let SqlValue::Integer(target) = value {
                simd_ne_i64(values, *target)
            } else if let SqlValue::Bigint(target) = value {
                simd_ne_i64(values, *target)
            } else {
                let target = value_to_f64(value)
                    .ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                        operation: "comparison".to_string(),
                        left_type: "Int64".to_string(),
                        right_type: Some(format!("{:?}", value)),
                    })?;
                let f64_values: Vec<f64> = values.iter().map(|&v| v as f64).collect();
                simd_ne_f64(&f64_values, target)
            }
        }

        ColumnPredicate::Like { .. } => {
            // LIKE is not applicable to numeric columns
            return Err(ExecutorError::ColumnarTypeMismatch {
                operation: "LIKE".to_string(),
                left_type: "Int64".to_string(),
                right_type: Some("String pattern".to_string()),
            });
        }
    };

    // Apply NULL mask: NULLs always fail predicates
    if let Some(null_mask) = nulls {
        for i in 0..result.len() {
            if null_mask[i] {
                result[i] = false;
            }
        }
    }

    Ok(result)
}

/// Evaluate predicate on i32 column using SIMD (for dates)
fn evaluate_predicate_i32_simd(
    predicate: &ColumnPredicate,
    values: &[i32],
    nulls: Option<&[bool]>,
) -> Result<Vec<bool>, ExecutorError> {
    let mut result = match predicate {
        ColumnPredicate::LessThan { value, .. } => {
            let threshold = value_to_date_i32(value)
                .ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                    operation: "date comparison".to_string(),
                    left_type: "Date".to_string(),
                    right_type: None,
                })?;
            simd_lt_i32(values, threshold)
        }

        ColumnPredicate::GreaterThan { value, .. } => {
            let threshold = value_to_date_i32(value)
                .ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                    operation: "date comparison".to_string(),
                    left_type: "Date".to_string(),
                    right_type: None,
                })?;
            simd_gt_i32(values, threshold)
        }

        ColumnPredicate::GreaterThanOrEqual { value, .. } => {
            let threshold = value_to_date_i32(value)
                .ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                    operation: "date comparison".to_string(),
                    left_type: "Date".to_string(),
                    right_type: None,
                })?;
            simd_ge_i32(values, threshold)
        }

        ColumnPredicate::LessThanOrEqual { value, .. } => {
            let threshold = value_to_date_i32(value)
                .ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                    operation: "date comparison".to_string(),
                    left_type: "Date".to_string(),
                    right_type: None,
                })?;
            simd_le_i32(values, threshold)
        }

        ColumnPredicate::Equal { value, .. } => {
            let target = value_to_date_i32(value)
                .ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                    operation: "date comparison".to_string(),
                    left_type: "Date".to_string(),
                    right_type: None,
                })?;
            simd_eq_i32(values, target)
        }

        ColumnPredicate::Between { low, high, ..} => {
            let low_i32 = value_to_date_i32(low)
                .ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                    operation: "BETWEEN".to_string(),
                    left_type: "Date".to_string(),
                    right_type: Some(format!("{:?}", low)),
                })?;
            let high_i32 = value_to_date_i32(high)
                .ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                    operation: "BETWEEN".to_string(),
                    left_type: "Date".to_string(),
                    right_type: Some(format!("{:?}", high)),
                })?;

            // Single-pass BETWEEN check (more cache-efficient)
            simd_ops::between_i32(values, low_i32, high_i32)
        }

        ColumnPredicate::NotEqual { value, .. } => {
            let target = value_to_date_i32(value)
                .ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                    operation: "date comparison".to_string(),
                    left_type: "Date".to_string(),
                    right_type: None,
                })?;
            simd_ne_i32(values, target)
        }

        ColumnPredicate::Like { .. } => {
            // LIKE is not applicable to Date columns
            return Err(ExecutorError::ColumnarTypeMismatch {
                operation: "LIKE".to_string(),
                left_type: "Date".to_string(),
                right_type: Some("String pattern".to_string()),
            });
        }
    };

    // Apply NULL mask: NULLs always fail predicates
    if let Some(null_mask) = nulls {
        for i in 0..result.len() {
            if null_mask[i] {
                result[i] = false;
            }
        }
    }

    Ok(result)
}

/// Evaluate predicate on f64 column using SIMD
fn evaluate_predicate_f64_simd(
    predicate: &ColumnPredicate,
    values: &[f64],
    nulls: Option<&[bool]>,
) -> Result<Vec<bool>, ExecutorError> {
    let mut result = match predicate {
        ColumnPredicate::LessThan { value, .. } => {
            let threshold = value_to_f64(value)
                .ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                    operation: "comparison".to_string(),
                    left_type: "Float64".to_string(),
                    right_type: Some(format!("{:?}", value)),
                })?;
            simd_lt_f64(values, threshold)
        }

        ColumnPredicate::GreaterThan { value, .. } => {
            let threshold = value_to_f64(value)
                .ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                    operation: "comparison".to_string(),
                    left_type: "Float64".to_string(),
                    right_type: Some(format!("{:?}", value)),
                })?;
            simd_gt_f64(values, threshold)
        }

        ColumnPredicate::GreaterThanOrEqual { value, .. } => {
            let threshold = value_to_f64(value)
                .ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                    operation: "comparison".to_string(),
                    left_type: "Float64".to_string(),
                    right_type: Some(format!("{:?}", value)),
                })?;
            simd_ge_f64(values, threshold)
        }

        ColumnPredicate::LessThanOrEqual { value, .. } => {
            let threshold = value_to_f64(value)
                .ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                    operation: "comparison".to_string(),
                    left_type: "Float64".to_string(),
                    right_type: Some(format!("{:?}", value)),
                })?;
            simd_le_f64(values, threshold)
        }

        ColumnPredicate::Equal { value, .. } => {
            let target = value_to_f64(value)
                .ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                    operation: "comparison".to_string(),
                    left_type: "Float64".to_string(),
                    right_type: Some(format!("{:?}", value)),
                })?;
            simd_eq_f64(values, target)
        }

        ColumnPredicate::Between { low, high, .. } => {
            let low_f64 = value_to_f64(low)
                .ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                    operation: "BETWEEN".to_string(),
                    left_type: "Float64".to_string(),
                    right_type: Some(format!("{:?}", low)),
                })?;
            let high_f64 = value_to_f64(high)
                .ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                    operation: "BETWEEN".to_string(),
                    left_type: "Float64".to_string(),
                    right_type: Some(format!("{:?}", high)),
                })?;

            // Single-pass BETWEEN check (more cache-efficient)
            simd_ops::between_f64(values, low_f64, high_f64)
        }

        ColumnPredicate::NotEqual { value, .. } => {
            let target = value_to_f64(value)
                .ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                    operation: "comparison".to_string(),
                    left_type: "Float64".to_string(),
                    right_type: Some(format!("{:?}", value)),
                })?;
            simd_ne_f64(values, target)
        }

        ColumnPredicate::Like { .. } => {
            // LIKE is not applicable to Float64 columns
            return Err(ExecutorError::ColumnarTypeMismatch {
                operation: "LIKE".to_string(),
                left_type: "Float64".to_string(),
                right_type: Some("String pattern".to_string()),
            });
        }
    };

    // Apply NULL mask: NULLs always fail predicates
    if let Some(null_mask) = nulls {
        for i in 0..result.len() {
            if null_mask[i] {
                result[i] = false;
            }
        }
    }

    Ok(result)
}

/// Evaluate predicate on string column using batch operations
fn evaluate_predicate_string_batch(
    predicate: &ColumnPredicate,
    values: &[String],
    nulls: Option<&[bool]>,
) -> Result<Vec<bool>, ExecutorError> {
    let result = match predicate {
        ColumnPredicate::Equal { value, .. } => {
            // Extract target string
            let target = match value {
                SqlValue::Character(s) | SqlValue::Varchar(s) => s.as_str(),
                _ => return Err(ExecutorError::ColumnarTypeMismatch {
                    operation: "string equality".to_string(),
                    left_type: "String".to_string(),
                    right_type: Some(format!("{:?}", value)),
                }),
            };
            batch_string_eq(values, nulls, target)
        }

        ColumnPredicate::LessThan { value, .. } => {
            let target = match value {
                SqlValue::Character(s) | SqlValue::Varchar(s) => s.as_str(),
                _ => return Err(ExecutorError::ColumnarTypeMismatch {
                    operation: "string comparison".to_string(),
                    left_type: "String".to_string(),
                    right_type: Some(format!("{:?}", value)),
                }),
            };
            batch_string_lt(values, nulls, target)
        }

        ColumnPredicate::GreaterThan { value, .. } => {
            let target = match value {
                SqlValue::Character(s) | SqlValue::Varchar(s) => s.as_str(),
                _ => return Err(ExecutorError::ColumnarTypeMismatch {
                    operation: "string comparison".to_string(),
                    left_type: "String".to_string(),
                    right_type: Some(format!("{:?}", value)),
                }),
            };
            batch_string_gt(values, nulls, target)
        }

        ColumnPredicate::LessThanOrEqual { value, .. } => {
            let target = match value {
                SqlValue::Character(s) | SqlValue::Varchar(s) => s.as_str(),
                _ => return Err(ExecutorError::ColumnarTypeMismatch {
                    operation: "string comparison".to_string(),
                    left_type: "String".to_string(),
                    right_type: Some(format!("{:?}", value)),
                }),
            };
            batch_string_le(values, nulls, target)
        }

        ColumnPredicate::GreaterThanOrEqual { value, .. } => {
            let target = match value {
                SqlValue::Character(s) | SqlValue::Varchar(s) => s.as_str(),
                _ => return Err(ExecutorError::ColumnarTypeMismatch {
                    operation: "string comparison".to_string(),
                    left_type: "String".to_string(),
                    right_type: Some(format!("{:?}", value)),
                }),
            };
            batch_string_ge(values, nulls, target)
        }

        ColumnPredicate::NotEqual { value, .. } => {
            let target = match value {
                SqlValue::Character(s) | SqlValue::Varchar(s) => s.as_str(),
                _ => return Err(ExecutorError::ColumnarTypeMismatch {
                    operation: "string comparison".to_string(),
                    left_type: "String".to_string(),
                    right_type: Some(format!("{:?}", value)),
                }),
            };
            batch_string_ne(values, nulls, target)
        }

        ColumnPredicate::Like { pattern, negated, .. } => {
            let parsed_pattern = LikePattern::parse(pattern);
            let mut mask = batch_string_like(values, nulls, &parsed_pattern);

            // Handle NOT LIKE by inverting the mask (but keeping NULLs as false)
            if *negated {
                for (i, result) in mask.iter_mut().enumerate() {
                    // Only invert non-NULL values
                    if let Some(null_mask) = nulls {
                        if !null_mask[i] {
                            *result = !*result;
                        }
                    } else {
                        *result = !*result;
                    }
                }
            }
            mask
        }

        ColumnPredicate::Between { low, high, .. } => {
            // String BETWEEN - compare lexicographically
            let low_str = match low {
                SqlValue::Character(s) | SqlValue::Varchar(s) => s.as_str(),
                _ => return Err(ExecutorError::ColumnarTypeMismatch {
                    operation: "string BETWEEN".to_string(),
                    left_type: "String".to_string(),
                    right_type: Some(format!("{:?}", low)),
                }),
            };
            let high_str = match high {
                SqlValue::Character(s) | SqlValue::Varchar(s) => s.as_str(),
                _ => return Err(ExecutorError::ColumnarTypeMismatch {
                    operation: "string BETWEEN".to_string(),
                    left_type: "String".to_string(),
                    right_type: Some(format!("{:?}", high)),
                }),
            };

            // value >= low AND value <= high
            let ge_low = batch_string_ge(values, nulls, low_str);
            let le_high = batch_string_le(values, nulls, high_str);

            ge_low
                .iter()
                .zip(le_high.iter())
                .map(|(&a, &b)| a && b)
                .collect()
        }
    };

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

/// Apply a filter mask to a batch, keeping only rows where mask[i] == true
fn apply_filter_mask(
    batch: &ColumnarBatch,
    mask: &[bool],
) -> Result<ColumnarBatch, ExecutorError> {
    if mask.len() != batch.row_count() {
        return Err(ExecutorError::ColumnarLengthMismatch {
            context: "filter mask".to_string(),
            expected: batch.row_count(),
            actual: mask.len(),
        });
    }

    // Count how many rows will pass
    let new_row_count = mask.iter().filter(|&&b| b).count();

    if new_row_count == 0 {
        // Empty result
        return ColumnarBatch::empty(batch.column_count());
    }

    // Build new columns by filtering each column
    let mut new_columns = Vec::new();

    for col_idx in 0..batch.column_count() {
        let column = batch
            .column(col_idx)
            .ok_or_else(|| ExecutorError::ColumnarColumnNotFound {
                column_index: col_idx,
                batch_columns: batch.column_count(),
            })?;

        let new_column = filter_column(column, mask)?;
        new_columns.push(new_column);
    }

    ColumnarBatch::from_columns(new_columns, batch.column_names().map(|names| names.to_vec()))
}

/// Filter a single column based on the mask
fn filter_column(
    column: &ColumnArray,
    mask: &[bool],
) -> Result<ColumnArray, ExecutorError> {
    match column {
        ColumnArray::Int64(values, nulls) => {
            let new_values: Vec<i64> = values
                .iter()
                .zip(mask.iter())
                .filter_map(|(&v, &keep)| if keep { Some(v) } else { None })
                .collect();

            let new_nulls = nulls.as_ref().map(|null_mask| {
                Arc::new(null_mask
                    .iter()
                    .zip(mask.iter())
                    .filter_map(|(&n, &keep)| if keep { Some(n) } else { None })
                    .collect::<Vec<bool>>())
            });

            Ok(ColumnArray::Int64(Arc::new(new_values), new_nulls))
        }

        ColumnArray::Int32(values, nulls) => {
            let new_values: Vec<i32> = values
                .iter()
                .zip(mask.iter())
                .filter_map(|(&v, &keep)| if keep { Some(v) } else { None })
                .collect();

            let new_nulls = nulls.as_ref().map(|null_mask| {
                Arc::new(null_mask
                    .iter()
                    .zip(mask.iter())
                    .filter_map(|(&n, &keep)| if keep { Some(n) } else { None })
                    .collect::<Vec<bool>>())
            });

            Ok(ColumnArray::Int32(Arc::new(new_values), new_nulls))
        }

        ColumnArray::Float64(values, nulls) => {
            let new_values: Vec<f64> = values
                .iter()
                .zip(mask.iter())
                .filter_map(|(&v, &keep)| if keep { Some(v) } else { None })
                .collect();

            let new_nulls = nulls.as_ref().map(|null_mask| {
                Arc::new(null_mask
                    .iter()
                    .zip(mask.iter())
                    .filter_map(|(&n, &keep)| if keep { Some(n) } else { None })
                    .collect::<Vec<bool>>())
            });

            Ok(ColumnArray::Float64(Arc::new(new_values), new_nulls))
        }

        ColumnArray::Float32(values, nulls) => {
            let new_values: Vec<f32> = values
                .iter()
                .zip(mask.iter())
                .filter_map(|(&v, &keep)| if keep { Some(v) } else { None })
                .collect();

            let new_nulls = nulls.as_ref().map(|null_mask| {
                Arc::new(null_mask
                    .iter()
                    .zip(mask.iter())
                    .filter_map(|(&n, &keep)| if keep { Some(n) } else { None })
                    .collect::<Vec<bool>>())
            });

            Ok(ColumnArray::Float32(Arc::new(new_values), new_nulls))
        }

        ColumnArray::String(values, nulls) => {
            let new_values: Vec<String> = values
                .iter()
                .zip(mask.iter())
                .filter_map(|(v, &keep)| if keep { Some(v.clone()) } else { None })
                .collect();

            let new_nulls = nulls.as_ref().map(|null_mask| {
                Arc::new(null_mask
                    .iter()
                    .zip(mask.iter())
                    .filter_map(|(&n, &keep)| if keep { Some(n) } else { None })
                    .collect::<Vec<bool>>())
            });

            Ok(ColumnArray::String(Arc::new(new_values), new_nulls))
        }

        ColumnArray::FixedString(values, nulls) => {
            let new_values: Vec<String> = values
                .iter()
                .zip(mask.iter())
                .filter_map(|(v, &keep)| if keep { Some(v.clone()) } else { None })
                .collect();

            let new_nulls = nulls.as_ref().map(|null_mask| {
                Arc::new(null_mask
                    .iter()
                    .zip(mask.iter())
                    .filter_map(|(&n, &keep)| if keep { Some(n) } else { None })
                    .collect::<Vec<bool>>())
            });

            Ok(ColumnArray::FixedString(Arc::new(new_values), new_nulls))
        }

        ColumnArray::Date(values, nulls) => {
            let new_values: Vec<i32> = values
                .iter()
                .zip(mask.iter())
                .filter_map(|(&v, &keep)| if keep { Some(v) } else { None })
                .collect();

            let new_nulls = nulls.as_ref().map(|null_mask| {
                Arc::new(null_mask
                    .iter()
                    .zip(mask.iter())
                    .filter_map(|(&n, &keep)| if keep { Some(n) } else { None })
                    .collect::<Vec<bool>>())
            });

            Ok(ColumnArray::Date(Arc::new(new_values), new_nulls))
        }

        ColumnArray::Timestamp(values, nulls) => {
            let new_values: Vec<i64> = values
                .iter()
                .zip(mask.iter())
                .filter_map(|(&v, &keep)| if keep { Some(v) } else { None })
                .collect();

            let new_nulls = nulls.as_ref().map(|null_mask| {
                Arc::new(null_mask
                    .iter()
                    .zip(mask.iter())
                    .filter_map(|(&n, &keep)| if keep { Some(n) } else { None })
                    .collect::<Vec<bool>>())
            });

            Ok(ColumnArray::Timestamp(Arc::new(new_values), new_nulls))
        }

        ColumnArray::Boolean(values, nulls) => {
            let new_values: Vec<u8> = values
                .iter()
                .zip(mask.iter())
                .filter_map(|(&v, &keep)| if keep { Some(v) } else { None })
                .collect();

            let new_nulls = nulls.as_ref().map(|null_mask| {
                Arc::new(null_mask
                    .iter()
                    .zip(mask.iter())
                    .filter_map(|(&n, &keep)| if keep { Some(n) } else { None })
                    .collect::<Vec<bool>>())
            });

            Ok(ColumnArray::Boolean(Arc::new(new_values), new_nulls))
        }

        ColumnArray::Mixed(values) => {
            let new_values: Vec<SqlValue> = values
                .iter()
                .zip(mask.iter())
                .filter_map(|(v, &keep)| if keep { Some(v.clone()) } else { None })
                .collect();

            Ok(ColumnArray::Mixed(Arc::new(new_values)))
        }
    }
}

/// Convert SqlValue to f64 for numeric comparisons
fn value_to_f64(value: &SqlValue) -> Option<f64> {
    match value {
        SqlValue::Integer(n) => Some(*n as f64),
        SqlValue::Bigint(n) => Some(*n as f64),
        SqlValue::Smallint(n) => Some(*n as f64),
        SqlValue::Float(n) => Some(*n as f64),
        SqlValue::Double(n) => Some(*n),
        // SqlValue::Numeric contains an f64 internally, extract directly
        // without lossy string round-trip (fixes #2857)
        SqlValue::Numeric(n) => Some(*n),
        SqlValue::Real(n) => Some(*n as f64),
        _ => None,
    }
}

/// Convert SqlValue::Date or string to i32 (days since Unix epoch)
fn value_to_date_i32(value: &SqlValue) -> Option<i32> {
    match value {
        SqlValue::Date(date) => Some(date_to_days_since_epoch(date)),
        // Handle text strings that look like dates (YYYY-MM-DD format)
        SqlValue::Character(s) | SqlValue::Varchar(s) => {
            parse_date_string(s).map(|d| date_to_days_since_epoch(&d))
        }
        _ => None,
    }
}

/// Convert Date to days since Unix epoch (1970-01-01)
fn date_to_days_since_epoch(date: &vibesql_types::Date) -> i32 {
    // Simple calculation: days since Unix epoch
    let year_days = (date.year - 1970) * 365;
    let leap_years = ((date.year - 1969) / 4) - ((date.year - 1901) / 100) + ((date.year - 1601) / 400);
    let month_days: i32 = match date.month {
        1 => 0,
        2 => 31,
        3 => 59,
        4 => 90,
        5 => 120,
        6 => 151,
        7 => 181,
        8 => 212,
        9 => 243,
        10 => 273,
        11 => 304,
        12 => 334,
        _ => 0,
    };

    // Add leap day if after February in a leap year
    let is_leap = date.year % 4 == 0 && (date.year % 100 != 0 || date.year % 400 == 0);
    let leap_adjustment = if is_leap && date.month > 2 { 1 } else { 0 };

    year_days + leap_years + month_days + date.day as i32 - 1 + leap_adjustment
}

/// Convert SqlValue::Timestamp to i64 (microseconds since Unix epoch)
fn value_to_timestamp_i64(value: &SqlValue) -> Option<i64> {
    match value {
        SqlValue::Timestamp(ts) => Some(timestamp_to_microseconds(ts)),
        _ => None,
    }
}

/// Convert Timestamp to microseconds since Unix epoch
fn timestamp_to_microseconds(ts: &vibesql_types::Timestamp) -> i64 {
    let days = date_to_days_since_epoch(&ts.date);
    let micros_from_days = days as i64 * 86_400_000_000; // 24 * 60 * 60 * 1_000_000
    let micros_from_time = (ts.time.hour as i64 * 3_600_000_000)
        + (ts.time.minute as i64 * 60_000_000)
        + (ts.time.second as i64 * 1_000_000)
        + (ts.time.nanosecond as i64 / 1000); // Convert nanoseconds to microseconds
    micros_from_days + micros_from_time
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
        let predicates = vec![ColumnPredicate::LessThan {
            column_idx: 0,
            value: SqlValue::Integer(18),
        }];

        let filtered = simd_filter_batch(&batch, &predicates).unwrap();

        assert_eq!(filtered.row_count(), 3);
        assert_eq!(
            filtered.get_value(0, 0).unwrap(),
            SqlValue::Integer(5)
        );
        assert_eq!(
            filtered.get_value(1, 0).unwrap(),
            SqlValue::Integer(10)
        );
        assert_eq!(
            filtered.get_value(2, 0).unwrap(),
            SqlValue::Integer(15)
        );
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
        assert_eq!(
            filtered.get_value(0, 0).unwrap(),
            SqlValue::Double(0.05)
        );
        assert_eq!(
            filtered.get_value(1, 0).unwrap(),
            SqlValue::Double(0.06)
        );
        assert_eq!(
            filtered.get_value(2, 0).unwrap(),
            SqlValue::Double(0.07)
        );
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
            ColumnPredicate::GreaterThan {
                column_idx: 0,
                value: SqlValue::Integer(10),
            },
            ColumnPredicate::LessThan {
                column_idx: 1,
                value: SqlValue::Double(0.07),
            },
        ];

        let filtered = simd_filter_batch(&batch, &predicates).unwrap();

        // Should only match row 2 (15, 0.06)
        assert_eq!(filtered.row_count(), 1);
        assert_eq!(
            filtered.get_value(0, 0).unwrap(),
            SqlValue::Integer(15)
        );
        assert_eq!(
            filtered.get_value(0, 1).unwrap(),
            SqlValue::Double(0.06)
        );
    }

    #[test]
    fn test_simd_filter_date_less_than() {
        use vibesql_types::Date;

        // Create a batch with date column
        let rows = vec![
            Row::new(vec![SqlValue::Date(Date {
                year: 1994,
                month: 1,
                day: 1,
            })]),
            Row::new(vec![SqlValue::Date(Date {
                year: 1995,
                month: 6,
                day: 15,
            })]),
            Row::new(vec![SqlValue::Date(Date {
                year: 1996,
                month: 12,
                day: 31,
            })]),
            Row::new(vec![SqlValue::Date(Date {
                year: 1997,
                month: 3,
                day: 10,
            })]),
        ];

        let batch = ColumnarBatch::from_rows(&rows).unwrap();

        // Filter: date < 1996-01-01
        let predicates = vec![ColumnPredicate::LessThan {
            column_idx: 0,
            value: SqlValue::Date(Date {
                year: 1996,
                month: 1,
                day: 1,
            }),
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
            Row::new(vec![SqlValue::Date(Date {
                year: 1994,
                month: 1,
                day: 1,
            })]),
            Row::new(vec![SqlValue::Date(Date {
                year: 1995,
                month: 6,
                day: 15,
            })]),
            Row::new(vec![SqlValue::Date(Date {
                year: 1996,
                month: 12,
                day: 31,
            })]),
            Row::new(vec![SqlValue::Date(Date {
                year: 1997,
                month: 3,
                day: 10,
            })]),
        ];

        let batch = ColumnarBatch::from_rows(&rows).unwrap();

        // Filter: date BETWEEN 1995-01-01 AND 1996-12-31
        let predicates = vec![ColumnPredicate::Between {
            column_idx: 0,
            low: SqlValue::Date(Date {
                year: 1995,
                month: 1,
                day: 1,
            }),
            high: SqlValue::Date(Date {
                year: 1996,
                month: 12,
                day: 31,
            }),
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
            Row::new(vec![SqlValue::Date(Date {
                year: 1994,
                month: 1,
                day: 1,
            })]),
            Row::new(vec![SqlValue::Null]),
            Row::new(vec![SqlValue::Date(Date {
                year: 1996,
                month: 12,
                day: 31,
            })]),
            Row::new(vec![SqlValue::Date(Date {
                year: 1997,
                month: 3,
                day: 10,
            })]),
        ];

        let batch = ColumnarBatch::from_rows(&rows).unwrap();

        // Filter: date >= 1996-01-01
        let predicates = vec![ColumnPredicate::GreaterThanOrEqual {
            column_idx: 0,
            value: SqlValue::Date(Date {
                year: 1996,
                month: 1,
                day: 1,
            }),
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
            Row::new(vec![SqlValue::Date(Date {
                year: 1994,
                month: 1,
                day: 1,
            })]),
            Row::new(vec![SqlValue::Date(Date {
                year: 1995,
                month: 6,
                day: 15,
            })]),
            Row::new(vec![SqlValue::Date(Date {
                year: 1995,
                month: 6,
                day: 15,
            })]),
            Row::new(vec![SqlValue::Date(Date {
                year: 1997,
                month: 3,
                day: 10,
            })]),
        ];

        let batch = ColumnarBatch::from_rows(&rows).unwrap();

        // Filter: date = 1995-06-15
        let predicates = vec![ColumnPredicate::Equal {
            column_idx: 0,
            value: SqlValue::Date(Date {
                year: 1995,
                month: 6,
                day: 15,
            }),
        }];

        let filtered = simd_filter_batch(&batch, &predicates).unwrap();

        // Should match two middle rows
        assert_eq!(filtered.row_count(), 2);
    }

    #[test]
    fn test_between_f64_bug() {
        // Create a batch with f64 column - matching the issue test case
        let rows = vec![
            Row::new(vec![SqlValue::Double(0.02)]),  // Should pass BETWEEN 0.02 AND 0.03
            Row::new(vec![SqlValue::Double(0.03)]),  // Should pass BETWEEN 0.02 AND 0.03
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
        assert_eq!(
            filtered.row_count(),
            3,
            "All rows should pass BETWEEN 0.02 AND 0.03"
        );
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
        assert_eq!(
            filtered.row_count(),
            3,
            "All rows should pass all predicates"
        );
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
                SqlValue::Double(0.02),  // Data is Double
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
                low: SqlValue::Numeric(0.02),  // Parser generates Numeric!
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
        let storage_columnar = vibesql_storage::ColumnarTable::from_rows(&rows, &column_names).unwrap();

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
                low: SqlValue::Numeric(0.02),  // Parser generates Numeric
                high: SqlValue::Numeric(0.03),
            },
        ];

        let filtered = simd_filter_batch(&batch, &predicates).unwrap();

        // All 3 rows should pass all predicates
        assert_eq!(
            filtered.row_count(),
            3,
            "All rows should pass from_storage_columnar path"
        );
    }
}
