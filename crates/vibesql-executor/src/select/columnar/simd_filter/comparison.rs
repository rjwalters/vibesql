//! Numeric comparison operations for SIMD-accelerated filtering
//!
//! This module provides SIMD-accelerated comparison operations for i64, i32, and f64 columns.
//! Uses the centralized simd_ops module for consistent, optimized operations.

use vibesql_types::SqlValue;

use super::{
    super::{
        filter::ColumnPredicate,
        simd_ops::{self, PackedMask},
    },
    conversion::{
        is_string_value, try_parse_string_as_f64, value_to_date_i32, value_to_f64,
        value_to_timestamp_i64,
    },
};
use crate::errors::ExecutorError;

/// Helper to apply null mask and return result with given constant value
fn apply_null_mask_constant(nulls: Option<&[bool]>, len: usize, constant: bool) -> Vec<bool> {
    if let Some(null_mask) = nulls {
        null_mask.iter().map(|&is_null| !is_null && constant).collect()
    } else {
        vec![constant; len]
    }
}

/// Evaluate predicate on i64 column using SIMD
pub fn evaluate_predicate_i64_simd(
    predicate: &ColumnPredicate,
    values: &[i64],
    nulls: Option<&[bool]>,
) -> Result<Vec<bool>, ExecutorError> {
    let mut result = match predicate {
        ColumnPredicate::LessThan { value, .. } => {
            // SQLite affinity: try to parse string as number for numeric comparison
            if is_string_value(value) {
                if let Some(threshold) = try_parse_string_as_f64(value) {
                    let f64_values: Vec<f64> = values.iter().map(|&v| v as f64).collect();
                    simd_ops::lt_f64(&f64_values, threshold)
                } else {
                    // String is not a number - INTEGER < TEXT is always true
                    return Ok(apply_null_mask_constant(nulls, values.len(), true));
                }
            } else if let SqlValue::Integer(threshold) = value {
                simd_ops::lt_i64(values, *threshold)
            } else if let SqlValue::Bigint(threshold) = value {
                simd_ops::lt_i64(values, *threshold)
            } else if let Some(threshold) = value_to_timestamp_i64(value) {
                simd_ops::lt_i64(values, threshold)
            } else {
                let threshold =
                    value_to_f64(value).ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                        operation: "comparison".to_string(),
                        left_type: "Int64".to_string(),
                        right_type: Some(format!("{:?}", value)),
                    })?;
                let f64_values: Vec<f64> = values.iter().map(|&v| v as f64).collect();
                simd_ops::lt_f64(&f64_values, threshold)
            }
        }

        ColumnPredicate::GreaterThan { value, .. } => {
            // SQLite affinity: try to parse string as number for numeric comparison
            if is_string_value(value) {
                if let Some(threshold) = try_parse_string_as_f64(value) {
                    let f64_values: Vec<f64> = values.iter().map(|&v| v as f64).collect();
                    simd_ops::gt_f64(&f64_values, threshold)
                } else {
                    // String is not a number - INTEGER > TEXT is always false
                    return Ok(apply_null_mask_constant(nulls, values.len(), false));
                }
            } else if let SqlValue::Integer(threshold) = value {
                simd_ops::gt_i64(values, *threshold)
            } else if let SqlValue::Bigint(threshold) = value {
                simd_ops::gt_i64(values, *threshold)
            } else {
                let threshold =
                    value_to_f64(value).ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                        operation: "comparison".to_string(),
                        left_type: "Int64".to_string(),
                        right_type: Some(format!("{:?}", value)),
                    })?;
                let f64_values: Vec<f64> = values.iter().map(|&v| v as f64).collect();
                simd_ops::gt_f64(&f64_values, threshold)
            }
        }

        ColumnPredicate::Equal { value, .. } => {
            // SQLite affinity: try to parse string as number for numeric comparison
            if is_string_value(value) {
                // Try to parse the string as a number
                if let Some(target) = try_parse_string_as_f64(value) {
                    // String parsed as number - compare as f64
                    let f64_values: Vec<f64> = values.iter().map(|&v| v as f64).collect();
                    simd_ops::eq_f64(&f64_values, target)
                } else {
                    // String is not a number - INTEGER = TEXT is always false
                    return Ok(apply_null_mask_constant(nulls, values.len(), false));
                }
            } else if let SqlValue::Integer(target) = value {
                simd_ops::eq_i64(values, *target)
            } else if let SqlValue::Bigint(target) = value {
                simd_ops::eq_i64(values, *target)
            } else {
                let target =
                    value_to_f64(value).ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                        operation: "comparison".to_string(),
                        left_type: "Int64".to_string(),
                        right_type: Some(format!("{:?}", value)),
                    })?;
                let f64_values: Vec<f64> = values.iter().map(|&v| v as f64).collect();
                simd_ops::eq_f64(&f64_values, target)
            }
        }

        ColumnPredicate::Between { low, high, .. } => {
            // SQLite affinity: try to parse strings as numbers for numeric comparison
            let low_f64 =
                if is_string_value(low) { try_parse_string_as_f64(low) } else { value_to_f64(low) };
            let high_f64 = if is_string_value(high) {
                try_parse_string_as_f64(high)
            } else {
                value_to_f64(high)
            };

            // If both bounds can be parsed as numbers, do numeric comparison
            if let (Some(lo_f64), Some(hi_f64)) = (low_f64, high_f64) {
                // Try integer bounds first for optimal i64 SIMD path
                let low_i64 = if !is_string_value(low) {
                    match low {
                        SqlValue::Integer(v) => Some(*v),
                        SqlValue::Bigint(v) => Some(*v),
                        _ => None,
                    }
                } else {
                    None
                };
                let high_i64 = if !is_string_value(high) {
                    match high {
                        SqlValue::Integer(v) => Some(*v),
                        SqlValue::Bigint(v) => Some(*v),
                        _ => None,
                    }
                } else {
                    None
                };

                if let (Some(lo), Some(hi)) = (low_i64, high_i64) {
                    simd_ops::between_i64(values, lo, hi)
                } else {
                    let f64_values: Vec<f64> = values.iter().map(|&v| v as f64).collect();
                    simd_ops::between_f64(&f64_values, lo_f64, hi_f64)
                }
            } else {
                // Can't parse as numbers - fall back to type ordering
                // INTEGER BETWEEN TEXT AND TEXT is always false (since INTEGER < TEXT)
                return Ok(apply_null_mask_constant(nulls, values.len(), false));
            }
        }

        ColumnPredicate::GreaterThanOrEqual { value, .. } => {
            // SQLite affinity: try to parse string as number for numeric comparison
            if is_string_value(value) {
                if let Some(threshold) = try_parse_string_as_f64(value) {
                    let f64_values: Vec<f64> = values.iter().map(|&v| v as f64).collect();
                    simd_ops::ge_f64(&f64_values, threshold)
                } else {
                    // String is not a number - INTEGER >= TEXT is always false
                    return Ok(apply_null_mask_constant(nulls, values.len(), false));
                }
            } else if let SqlValue::Integer(threshold) = value {
                simd_ops::ge_i64(values, *threshold)
            } else if let SqlValue::Bigint(threshold) = value {
                simd_ops::ge_i64(values, *threshold)
            } else {
                let threshold =
                    value_to_f64(value).ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                        operation: "comparison".to_string(),
                        left_type: "Int64".to_string(),
                        right_type: Some(format!("{:?}", value)),
                    })?;
                let f64_values: Vec<f64> = values.iter().map(|&v| v as f64).collect();
                simd_ops::ge_f64(&f64_values, threshold)
            }
        }

        ColumnPredicate::LessThanOrEqual { value, .. } => {
            // SQLite affinity: try to parse string as number for numeric comparison
            if is_string_value(value) {
                if let Some(threshold) = try_parse_string_as_f64(value) {
                    let f64_values: Vec<f64> = values.iter().map(|&v| v as f64).collect();
                    simd_ops::le_f64(&f64_values, threshold)
                } else {
                    // String is not a number - INTEGER <= TEXT is always true
                    return Ok(apply_null_mask_constant(nulls, values.len(), true));
                }
            } else if let SqlValue::Integer(threshold) = value {
                simd_ops::le_i64(values, *threshold)
            } else if let SqlValue::Bigint(threshold) = value {
                simd_ops::le_i64(values, *threshold)
            } else {
                let threshold =
                    value_to_f64(value).ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                        operation: "comparison".to_string(),
                        left_type: "Int64".to_string(),
                        right_type: Some(format!("{:?}", value)),
                    })?;
                let f64_values: Vec<f64> = values.iter().map(|&v| v as f64).collect();
                simd_ops::le_f64(&f64_values, threshold)
            }
        }

        ColumnPredicate::NotEqual { value, .. } => {
            // SQLite affinity: try to parse string as number for numeric comparison
            if is_string_value(value) {
                if let Some(target) = try_parse_string_as_f64(value) {
                    let f64_values: Vec<f64> = values.iter().map(|&v| v as f64).collect();
                    simd_ops::ne_f64(&f64_values, target)
                } else {
                    // String is not a number - INTEGER <> TEXT is always true
                    return Ok(apply_null_mask_constant(nulls, values.len(), true));
                }
            } else if let SqlValue::Integer(target) = value {
                simd_ops::ne_i64(values, *target)
            } else if let SqlValue::Bigint(target) = value {
                simd_ops::ne_i64(values, *target)
            } else {
                let target =
                    value_to_f64(value).ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                        operation: "comparison".to_string(),
                        left_type: "Int64".to_string(),
                        right_type: Some(format!("{:?}", value)),
                    })?;
                let f64_values: Vec<f64> = values.iter().map(|&v| v as f64).collect();
                simd_ops::ne_f64(&f64_values, target)
            }
        }

        ColumnPredicate::Like { .. } => {
            return Err(ExecutorError::ColumnarTypeMismatch {
                operation: "LIKE".to_string(),
                left_type: "Int64".to_string(),
                right_type: Some("String pattern".to_string()),
            });
        }

        ColumnPredicate::InList { values: list_values, negated, .. } => {
            // For i64 columns, check if value is in the list
            let mut result = vec![false; values.len()];
            for i64_val in list_values {
                // SQLite affinity: try to parse string values as numbers
                let target = match i64_val {
                    SqlValue::Integer(n) => *n,
                    SqlValue::Bigint(n) => *n,
                    _ if is_string_value(i64_val) => {
                        // Try to parse string as number
                        if let Some(n) = try_parse_string_as_f64(i64_val) {
                            // Use f64 comparison for this value
                            let f64_values: Vec<f64> = values.iter().map(|&v| v as f64).collect();
                            let matches = simd_ops::eq_f64(&f64_values, n);
                            for (i, &m) in matches.iter().enumerate() {
                                result[i] = result[i] || m;
                            }
                            continue;
                        } else {
                            continue; // String is not a number, skip
                        }
                    }
                    _ => continue,
                };
                let matches = simd_ops::eq_i64(values, target);
                for (i, &m) in matches.iter().enumerate() {
                    result[i] = result[i] || m;
                }
            }
            if *negated {
                result.iter_mut().for_each(|v| *v = !*v);
            }
            result
        }

        // ColumnCompare is handled at higher level in simd_filter/mod.rs
        ColumnPredicate::ColumnCompare { .. } => {
            return Err(ExecutorError::ColumnarTypeMismatch {
                operation: "column-to-column comparison".to_string(),
                left_type: "Int64".to_string(),
                right_type: Some("Should be handled in simd_filter/mod.rs".to_string()),
            });
        }
    };

    // Apply NULL mask: NULLs always fail predicates
    if let Some(null_mask) = nulls {
        for (i, is_null) in null_mask.iter().enumerate() {
            if *is_null {
                result[i] = false;
            }
        }
    }

    Ok(result)
}

/// Evaluate predicate on i32 column using SIMD (for dates)
pub fn evaluate_predicate_i32_simd(
    predicate: &ColumnPredicate,
    values: &[i32],
    nulls: Option<&[bool]>,
) -> Result<Vec<bool>, ExecutorError> {
    let mut result = match predicate {
        ColumnPredicate::LessThan { value, .. } => {
            let threshold =
                value_to_date_i32(value).ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                    operation: "date comparison".to_string(),
                    left_type: "Date".to_string(),
                    right_type: None,
                })?;
            simd_ops::lt_i32(values, threshold)
        }

        ColumnPredicate::GreaterThan { value, .. } => {
            let threshold =
                value_to_date_i32(value).ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                    operation: "date comparison".to_string(),
                    left_type: "Date".to_string(),
                    right_type: None,
                })?;
            simd_ops::gt_i32(values, threshold)
        }

        ColumnPredicate::GreaterThanOrEqual { value, .. } => {
            let threshold =
                value_to_date_i32(value).ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                    operation: "date comparison".to_string(),
                    left_type: "Date".to_string(),
                    right_type: None,
                })?;
            simd_ops::ge_i32(values, threshold)
        }

        ColumnPredicate::LessThanOrEqual { value, .. } => {
            let threshold =
                value_to_date_i32(value).ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                    operation: "date comparison".to_string(),
                    left_type: "Date".to_string(),
                    right_type: None,
                })?;
            simd_ops::le_i32(values, threshold)
        }

        ColumnPredicate::Equal { value, .. } => {
            let target =
                value_to_date_i32(value).ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                    operation: "date comparison".to_string(),
                    left_type: "Date".to_string(),
                    right_type: None,
                })?;
            simd_ops::eq_i32(values, target)
        }

        ColumnPredicate::Between { low, high, .. } => {
            let low_i32 =
                value_to_date_i32(low).ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                    operation: "BETWEEN".to_string(),
                    left_type: "Date".to_string(),
                    right_type: Some(format!("{:?}", low)),
                })?;
            let high_i32 =
                value_to_date_i32(high).ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                    operation: "BETWEEN".to_string(),
                    left_type: "Date".to_string(),
                    right_type: Some(format!("{:?}", high)),
                })?;
            simd_ops::between_i32(values, low_i32, high_i32)
        }

        ColumnPredicate::NotEqual { value, .. } => {
            let target =
                value_to_date_i32(value).ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                    operation: "date comparison".to_string(),
                    left_type: "Date".to_string(),
                    right_type: None,
                })?;
            simd_ops::ne_i32(values, target)
        }

        ColumnPredicate::Like { .. } => {
            return Err(ExecutorError::ColumnarTypeMismatch {
                operation: "LIKE".to_string(),
                left_type: "Date".to_string(),
                right_type: Some("String pattern".to_string()),
            });
        }

        ColumnPredicate::InList { values: list_values, negated, .. } => {
            // For date (i32) columns, check if value is in the list
            let mut result = vec![false; values.len()];
            for date_val in list_values {
                if let Some(target) = value_to_date_i32(date_val) {
                    let matches = simd_ops::eq_i32(values, target);
                    for (i, &m) in matches.iter().enumerate() {
                        result[i] = result[i] || m;
                    }
                }
            }
            if *negated {
                result.iter_mut().for_each(|v| *v = !*v);
            }
            result
        }

        // ColumnCompare is handled at higher level in simd_filter/mod.rs
        ColumnPredicate::ColumnCompare { .. } => {
            return Err(ExecutorError::ColumnarTypeMismatch {
                operation: "column-to-column comparison".to_string(),
                left_type: "Date".to_string(),
                right_type: Some("Should be handled in simd_filter/mod.rs".to_string()),
            });
        }
    };

    // Apply NULL mask: NULLs always fail predicates
    if let Some(null_mask) = nulls {
        for (i, is_null) in null_mask.iter().enumerate() {
            if *is_null {
                result[i] = false;
            }
        }
    }

    Ok(result)
}

/// Evaluate predicate on f64 column using SIMD
pub fn evaluate_predicate_f64_simd(
    predicate: &ColumnPredicate,
    values: &[f64],
    nulls: Option<&[bool]>,
) -> Result<Vec<bool>, ExecutorError> {
    let mut result = match predicate {
        ColumnPredicate::LessThan { value, .. } => {
            // SQLite affinity: try to parse string as number for numeric comparison
            if is_string_value(value) {
                if let Some(threshold) = try_parse_string_as_f64(value) {
                    simd_ops::lt_f64(values, threshold)
                } else {
                    // String is not a number - REAL < TEXT is always true
                    return Ok(apply_null_mask_constant(nulls, values.len(), true));
                }
            } else {
                let threshold =
                    value_to_f64(value).ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                        operation: "comparison".to_string(),
                        left_type: "Float64".to_string(),
                        right_type: Some(format!("{:?}", value)),
                    })?;
                simd_ops::lt_f64(values, threshold)
            }
        }

        ColumnPredicate::GreaterThan { value, .. } => {
            // SQLite affinity: try to parse string as number for numeric comparison
            if is_string_value(value) {
                if let Some(threshold) = try_parse_string_as_f64(value) {
                    simd_ops::gt_f64(values, threshold)
                } else {
                    // String is not a number - REAL > TEXT is always false
                    return Ok(apply_null_mask_constant(nulls, values.len(), false));
                }
            } else {
                let threshold =
                    value_to_f64(value).ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                        operation: "comparison".to_string(),
                        left_type: "Float64".to_string(),
                        right_type: Some(format!("{:?}", value)),
                    })?;
                simd_ops::gt_f64(values, threshold)
            }
        }

        ColumnPredicate::GreaterThanOrEqual { value, .. } => {
            // SQLite affinity: try to parse string as number for numeric comparison
            if is_string_value(value) {
                if let Some(threshold) = try_parse_string_as_f64(value) {
                    simd_ops::ge_f64(values, threshold)
                } else {
                    // String is not a number - REAL >= TEXT is always false
                    return Ok(apply_null_mask_constant(nulls, values.len(), false));
                }
            } else {
                let threshold =
                    value_to_f64(value).ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                        operation: "comparison".to_string(),
                        left_type: "Float64".to_string(),
                        right_type: Some(format!("{:?}", value)),
                    })?;
                simd_ops::ge_f64(values, threshold)
            }
        }

        ColumnPredicate::LessThanOrEqual { value, .. } => {
            // SQLite affinity: try to parse string as number for numeric comparison
            if is_string_value(value) {
                if let Some(threshold) = try_parse_string_as_f64(value) {
                    simd_ops::le_f64(values, threshold)
                } else {
                    // String is not a number - REAL <= TEXT is always true
                    return Ok(apply_null_mask_constant(nulls, values.len(), true));
                }
            } else {
                let threshold =
                    value_to_f64(value).ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                        operation: "comparison".to_string(),
                        left_type: "Float64".to_string(),
                        right_type: Some(format!("{:?}", value)),
                    })?;
                simd_ops::le_f64(values, threshold)
            }
        }

        ColumnPredicate::Equal { value, .. } => {
            // SQLite affinity: try to parse string as number for numeric comparison
            if is_string_value(value) {
                if let Some(target) = try_parse_string_as_f64(value) {
                    simd_ops::eq_f64(values, target)
                } else {
                    // String is not a number - REAL = TEXT is always false
                    return Ok(apply_null_mask_constant(nulls, values.len(), false));
                }
            } else {
                let target =
                    value_to_f64(value).ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                        operation: "comparison".to_string(),
                        left_type: "Float64".to_string(),
                        right_type: Some(format!("{:?}", value)),
                    })?;
                simd_ops::eq_f64(values, target)
            }
        }

        ColumnPredicate::Between { low, high, .. } => {
            // SQLite affinity: try to parse strings as numbers for numeric comparison
            let low_f64 =
                if is_string_value(low) { try_parse_string_as_f64(low) } else { value_to_f64(low) };
            let high_f64 = if is_string_value(high) {
                try_parse_string_as_f64(high)
            } else {
                value_to_f64(high)
            };

            if let (Some(lo), Some(hi)) = (low_f64, high_f64) {
                simd_ops::between_f64(values, lo, hi)
            } else {
                // Can't parse as numbers - REAL BETWEEN TEXT AND TEXT is always false
                return Ok(apply_null_mask_constant(nulls, values.len(), false));
            }
        }

        ColumnPredicate::NotEqual { value, .. } => {
            // SQLite affinity: try to parse string as number for numeric comparison
            if is_string_value(value) {
                if let Some(target) = try_parse_string_as_f64(value) {
                    simd_ops::ne_f64(values, target)
                } else {
                    // String is not a number - REAL <> TEXT is always true
                    return Ok(apply_null_mask_constant(nulls, values.len(), true));
                }
            } else {
                let target =
                    value_to_f64(value).ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                        operation: "comparison".to_string(),
                        left_type: "Float64".to_string(),
                        right_type: Some(format!("{:?}", value)),
                    })?;
                simd_ops::ne_f64(values, target)
            }
        }

        ColumnPredicate::Like { .. } => {
            return Err(ExecutorError::ColumnarTypeMismatch {
                operation: "LIKE".to_string(),
                left_type: "Float64".to_string(),
                right_type: Some("String pattern".to_string()),
            });
        }

        ColumnPredicate::InList { values: list_values, negated, .. } => {
            // For f64 columns, check if value is in the list
            let mut result = vec![false; values.len()];
            for f_val in list_values {
                // SQLite affinity: try to parse string values as numbers
                let target = if is_string_value(f_val) {
                    try_parse_string_as_f64(f_val)
                } else {
                    value_to_f64(f_val)
                };
                if let Some(t) = target {
                    let matches = simd_ops::eq_f64(values, t);
                    for (i, &m) in matches.iter().enumerate() {
                        result[i] = result[i] || m;
                    }
                }
            }
            if *negated {
                result.iter_mut().for_each(|v| *v = !*v);
            }
            result
        }

        // ColumnCompare is handled at higher level in simd_filter/mod.rs
        ColumnPredicate::ColumnCompare { .. } => {
            return Err(ExecutorError::ColumnarTypeMismatch {
                operation: "column-to-column comparison".to_string(),
                left_type: "Float64".to_string(),
                right_type: Some("Should be handled in simd_filter/mod.rs".to_string()),
            });
        }
    };

    // Apply NULL mask: NULLs always fail predicates
    if let Some(null_mask) = nulls {
        for (i, is_null) in null_mask.iter().enumerate() {
            if *is_null {
                result[i] = false;
            }
        }
    }

    Ok(result)
}

// ============================================================================
// Packed mask versions for improved memory efficiency
// ============================================================================

/// Helper to create packed mask with constant value and null handling
fn apply_null_mask_constant_packed(
    nulls: Option<&[bool]>,
    len: usize,
    constant: bool,
) -> PackedMask {
    if constant {
        let mut mask = PackedMask::new_all_set(len);
        if let Some(null_mask) = nulls {
            for (i, &is_null) in null_mask.iter().enumerate() {
                if is_null {
                    mask.set(i, false);
                }
            }
        }
        mask
    } else {
        PackedMask::new_all_clear(len)
    }
}

/// Evaluate predicate on i64 column returning packed mask
pub fn evaluate_predicate_i64_packed(
    predicate: &ColumnPredicate,
    values: &[i64],
    nulls: Option<&[bool]>,
) -> Result<PackedMask, ExecutorError> {
    let mut result = match predicate {
        ColumnPredicate::LessThan { value, .. } => {
            // SQLite affinity: try to parse string as number for numeric comparison
            if is_string_value(value) {
                if let Some(threshold) = try_parse_string_as_f64(value) {
                    let f64_values: Vec<f64> = values.iter().map(|&v| v as f64).collect();
                    simd_ops::lt_f64_packed(&f64_values, threshold)
                } else {
                    // String is not a number - INTEGER < TEXT is always true
                    return Ok(apply_null_mask_constant_packed(nulls, values.len(), true));
                }
            } else if let SqlValue::Integer(threshold) = value {
                simd_ops::lt_i64_packed(values, *threshold)
            } else if let SqlValue::Bigint(threshold) = value {
                simd_ops::lt_i64_packed(values, *threshold)
            } else if let Some(threshold) = value_to_timestamp_i64(value) {
                simd_ops::lt_i64_packed(values, threshold)
            } else {
                let threshold =
                    value_to_f64(value).ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                        operation: "comparison".to_string(),
                        left_type: "Int64".to_string(),
                        right_type: Some(format!("{:?}", value)),
                    })?;
                let f64_values: Vec<f64> = values.iter().map(|&v| v as f64).collect();
                simd_ops::lt_f64_packed(&f64_values, threshold)
            }
        }

        ColumnPredicate::GreaterThan { value, .. } => {
            // SQLite affinity: try to parse string as number for numeric comparison
            if is_string_value(value) {
                if let Some(threshold) = try_parse_string_as_f64(value) {
                    let f64_values: Vec<f64> = values.iter().map(|&v| v as f64).collect();
                    simd_ops::gt_f64_packed(&f64_values, threshold)
                } else {
                    // String is not a number - INTEGER > TEXT is always false
                    return Ok(apply_null_mask_constant_packed(nulls, values.len(), false));
                }
            } else if let SqlValue::Integer(threshold) = value {
                simd_ops::gt_i64_packed(values, *threshold)
            } else if let SqlValue::Bigint(threshold) = value {
                simd_ops::gt_i64_packed(values, *threshold)
            } else {
                let threshold =
                    value_to_f64(value).ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                        operation: "comparison".to_string(),
                        left_type: "Int64".to_string(),
                        right_type: Some(format!("{:?}", value)),
                    })?;
                let f64_values: Vec<f64> = values.iter().map(|&v| v as f64).collect();
                simd_ops::gt_f64_packed(&f64_values, threshold)
            }
        }

        ColumnPredicate::Equal { value, .. } => {
            // SQLite affinity: try to parse string as number for numeric comparison
            if is_string_value(value) {
                if let Some(target) = try_parse_string_as_f64(value) {
                    let f64_values: Vec<f64> = values.iter().map(|&v| v as f64).collect();
                    simd_ops::eq_f64_packed(&f64_values, target)
                } else {
                    // String is not a number - INTEGER = TEXT is always false
                    return Ok(apply_null_mask_constant_packed(nulls, values.len(), false));
                }
            } else if let SqlValue::Integer(target) = value {
                simd_ops::eq_i64_packed(values, *target)
            } else if let SqlValue::Bigint(target) = value {
                simd_ops::eq_i64_packed(values, *target)
            } else {
                let target =
                    value_to_f64(value).ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                        operation: "comparison".to_string(),
                        left_type: "Int64".to_string(),
                        right_type: Some(format!("{:?}", value)),
                    })?;
                let f64_values: Vec<f64> = values.iter().map(|&v| v as f64).collect();
                simd_ops::eq_f64_packed(&f64_values, target)
            }
        }

        ColumnPredicate::Between { low, high, .. } => {
            // SQLite affinity: try to parse strings as numbers for numeric comparison
            let low_f64 =
                if is_string_value(low) { try_parse_string_as_f64(low) } else { value_to_f64(low) };
            let high_f64 = if is_string_value(high) {
                try_parse_string_as_f64(high)
            } else {
                value_to_f64(high)
            };

            // If both bounds can be parsed as numbers, do numeric comparison
            if let (Some(lo_f64), Some(hi_f64)) = (low_f64, high_f64) {
                // Try integer bounds first for optimal i64 SIMD path
                let low_i64 = if !is_string_value(low) {
                    match low {
                        SqlValue::Integer(v) => Some(*v),
                        SqlValue::Bigint(v) => Some(*v),
                        _ => None,
                    }
                } else {
                    None
                };
                let high_i64 = if !is_string_value(high) {
                    match high {
                        SqlValue::Integer(v) => Some(*v),
                        SqlValue::Bigint(v) => Some(*v),
                        _ => None,
                    }
                } else {
                    None
                };

                if let (Some(lo), Some(hi)) = (low_i64, high_i64) {
                    simd_ops::between_i64_packed(values, lo, hi)
                } else {
                    let f64_values: Vec<f64> = values.iter().map(|&v| v as f64).collect();
                    simd_ops::between_f64_packed(&f64_values, lo_f64, hi_f64)
                }
            } else {
                // Can't parse as numbers - fall back to type ordering
                // INTEGER BETWEEN TEXT AND TEXT is always false (since INTEGER < TEXT)
                return Ok(apply_null_mask_constant_packed(nulls, values.len(), false));
            }
        }

        ColumnPredicate::GreaterThanOrEqual { value, .. } => {
            // SQLite affinity: try to parse string as number for numeric comparison
            if is_string_value(value) {
                if let Some(threshold) = try_parse_string_as_f64(value) {
                    let f64_values: Vec<f64> = values.iter().map(|&v| v as f64).collect();
                    simd_ops::ge_f64_packed(&f64_values, threshold)
                } else {
                    // String is not a number - INTEGER >= TEXT is always false
                    return Ok(apply_null_mask_constant_packed(nulls, values.len(), false));
                }
            } else if let SqlValue::Integer(threshold) = value {
                simd_ops::ge_i64_packed(values, *threshold)
            } else if let SqlValue::Bigint(threshold) = value {
                simd_ops::ge_i64_packed(values, *threshold)
            } else {
                let threshold =
                    value_to_f64(value).ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                        operation: "comparison".to_string(),
                        left_type: "Int64".to_string(),
                        right_type: Some(format!("{:?}", value)),
                    })?;
                let f64_values: Vec<f64> = values.iter().map(|&v| v as f64).collect();
                simd_ops::ge_f64_packed(&f64_values, threshold)
            }
        }

        ColumnPredicate::LessThanOrEqual { value, .. } => {
            // SQLite affinity: try to parse string as number for numeric comparison
            if is_string_value(value) {
                if let Some(threshold) = try_parse_string_as_f64(value) {
                    let f64_values: Vec<f64> = values.iter().map(|&v| v as f64).collect();
                    simd_ops::le_f64_packed(&f64_values, threshold)
                } else {
                    // String is not a number - INTEGER <= TEXT is always true
                    return Ok(apply_null_mask_constant_packed(nulls, values.len(), true));
                }
            } else if let SqlValue::Integer(threshold) = value {
                simd_ops::le_i64_packed(values, *threshold)
            } else if let SqlValue::Bigint(threshold) = value {
                simd_ops::le_i64_packed(values, *threshold)
            } else {
                let threshold =
                    value_to_f64(value).ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                        operation: "comparison".to_string(),
                        left_type: "Int64".to_string(),
                        right_type: Some(format!("{:?}", value)),
                    })?;
                let f64_values: Vec<f64> = values.iter().map(|&v| v as f64).collect();
                simd_ops::le_f64_packed(&f64_values, threshold)
            }
        }

        ColumnPredicate::NotEqual { value, .. } => {
            // SQLite affinity: try to parse string as number for numeric comparison
            if is_string_value(value) {
                if let Some(target) = try_parse_string_as_f64(value) {
                    let f64_values: Vec<f64> = values.iter().map(|&v| v as f64).collect();
                    simd_ops::ne_f64_packed(&f64_values, target)
                } else {
                    // String is not a number - INTEGER <> TEXT is always true
                    return Ok(apply_null_mask_constant_packed(nulls, values.len(), true));
                }
            } else if let SqlValue::Integer(target) = value {
                simd_ops::ne_i64_packed(values, *target)
            } else if let SqlValue::Bigint(target) = value {
                simd_ops::ne_i64_packed(values, *target)
            } else {
                let target =
                    value_to_f64(value).ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
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

        ColumnPredicate::InList { values: list_values, negated, .. } => {
            // For i64 columns with packed mask
            let mut result = PackedMask::new_all_clear(values.len());
            for i64_val in list_values {
                // SQLite affinity: try to parse string values as numbers
                let target = match i64_val {
                    SqlValue::Integer(n) => *n,
                    SqlValue::Bigint(n) => *n,
                    _ if is_string_value(i64_val) => {
                        // Try to parse string as i64 first, then f64
                        if let Some(n) = try_parse_string_as_f64(i64_val) {
                            // Use f64 comparison for this value
                            let f64_values: Vec<f64> = values.iter().map(|&v| v as f64).collect();
                            let matches = simd_ops::eq_f64_packed(&f64_values, n);
                            result.or_inplace(&matches);
                            continue;
                        } else {
                            continue; // String is not a number, skip
                        }
                    }
                    _ => continue,
                };
                let matches = simd_ops::eq_i64_packed(values, target);
                result.or_inplace(&matches);
            }
            if *negated {
                result = result.not();
            }
            result
        }

        // ColumnCompare is handled at higher level in simd_filter/mod.rs
        ColumnPredicate::ColumnCompare { .. } => {
            return Err(ExecutorError::ColumnarTypeMismatch {
                operation: "column-to-column comparison".to_string(),
                left_type: "Int64".to_string(),
                right_type: Some("Should be handled in simd_filter/mod.rs".to_string()),
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
pub fn evaluate_predicate_i32_packed(
    predicate: &ColumnPredicate,
    values: &[i32],
    nulls: Option<&[bool]>,
) -> Result<PackedMask, ExecutorError> {
    let mut result = match predicate {
        ColumnPredicate::LessThan { value, .. } => {
            let threshold =
                value_to_date_i32(value).ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                    operation: "date comparison".to_string(),
                    left_type: "Date".to_string(),
                    right_type: None,
                })?;
            simd_ops::lt_i32_packed(values, threshold)
        }

        ColumnPredicate::GreaterThan { value, .. } => {
            let threshold =
                value_to_date_i32(value).ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                    operation: "date comparison".to_string(),
                    left_type: "Date".to_string(),
                    right_type: None,
                })?;
            simd_ops::gt_i32_packed(values, threshold)
        }

        ColumnPredicate::GreaterThanOrEqual { value, .. } => {
            let threshold =
                value_to_date_i32(value).ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                    operation: "date comparison".to_string(),
                    left_type: "Date".to_string(),
                    right_type: None,
                })?;
            simd_ops::ge_i32_packed(values, threshold)
        }

        ColumnPredicate::LessThanOrEqual { value, .. } => {
            let threshold =
                value_to_date_i32(value).ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                    operation: "date comparison".to_string(),
                    left_type: "Date".to_string(),
                    right_type: None,
                })?;
            simd_ops::le_i32_packed(values, threshold)
        }

        ColumnPredicate::Equal { value, .. } => {
            let target =
                value_to_date_i32(value).ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                    operation: "date comparison".to_string(),
                    left_type: "Date".to_string(),
                    right_type: None,
                })?;
            simd_ops::eq_i32_packed(values, target)
        }

        ColumnPredicate::Between { low, high, .. } => {
            let low_i32 =
                value_to_date_i32(low).ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                    operation: "BETWEEN".to_string(),
                    left_type: "Date".to_string(),
                    right_type: Some(format!("{:?}", low)),
                })?;
            let high_i32 =
                value_to_date_i32(high).ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                    operation: "BETWEEN".to_string(),
                    left_type: "Date".to_string(),
                    right_type: Some(format!("{:?}", high)),
                })?;
            simd_ops::between_i32_packed(values, low_i32, high_i32)
        }

        ColumnPredicate::NotEqual { value, .. } => {
            let target =
                value_to_date_i32(value).ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
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

        ColumnPredicate::InList { values: list_values, negated, .. } => {
            // For date (i32) columns with packed mask
            let mut result = PackedMask::new_all_clear(values.len());
            for date_val in list_values {
                if let Some(target) = value_to_date_i32(date_val) {
                    let matches = simd_ops::eq_i32_packed(values, target);
                    result.or_inplace(&matches);
                }
            }
            if *negated {
                result = result.not();
            }
            result
        }

        // ColumnCompare is handled at higher level in simd_filter/mod.rs
        ColumnPredicate::ColumnCompare { .. } => {
            return Err(ExecutorError::ColumnarTypeMismatch {
                operation: "column-to-column comparison".to_string(),
                left_type: "Date".to_string(),
                right_type: Some("Should be handled in simd_filter/mod.rs".to_string()),
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
pub fn evaluate_predicate_f64_packed(
    predicate: &ColumnPredicate,
    values: &[f64],
    nulls: Option<&[bool]>,
) -> Result<PackedMask, ExecutorError> {
    let mut result = match predicate {
        ColumnPredicate::LessThan { value, .. } => {
            // SQLite affinity: try to parse string as number for numeric comparison
            let threshold = if is_string_value(value) {
                match try_parse_string_as_f64(value) {
                    Some(t) => t,
                    None => {
                        // String is not a number - REAL < TEXT is always true
                        return Ok(apply_null_mask_constant_packed(nulls, values.len(), true));
                    }
                }
            } else {
                value_to_f64(value).ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                    operation: "comparison".to_string(),
                    left_type: "Float64".to_string(),
                    right_type: Some(format!("{:?}", value)),
                })?
            };
            simd_ops::lt_f64_packed(values, threshold)
        }

        ColumnPredicate::GreaterThan { value, .. } => {
            // SQLite affinity: try to parse string as number for numeric comparison
            let threshold = if is_string_value(value) {
                match try_parse_string_as_f64(value) {
                    Some(t) => t,
                    None => {
                        // String is not a number - REAL > TEXT is always false
                        return Ok(apply_null_mask_constant_packed(nulls, values.len(), false));
                    }
                }
            } else {
                value_to_f64(value).ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                    operation: "comparison".to_string(),
                    left_type: "Float64".to_string(),
                    right_type: Some(format!("{:?}", value)),
                })?
            };
            simd_ops::gt_f64_packed(values, threshold)
        }

        ColumnPredicate::GreaterThanOrEqual { value, .. } => {
            // SQLite affinity: try to parse string as number for numeric comparison
            let threshold = if is_string_value(value) {
                match try_parse_string_as_f64(value) {
                    Some(t) => t,
                    None => {
                        // String is not a number - REAL >= TEXT is always false
                        return Ok(apply_null_mask_constant_packed(nulls, values.len(), false));
                    }
                }
            } else {
                value_to_f64(value).ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                    operation: "comparison".to_string(),
                    left_type: "Float64".to_string(),
                    right_type: Some(format!("{:?}", value)),
                })?
            };
            simd_ops::ge_f64_packed(values, threshold)
        }

        ColumnPredicate::LessThanOrEqual { value, .. } => {
            // SQLite affinity: try to parse string as number for numeric comparison
            let threshold = if is_string_value(value) {
                match try_parse_string_as_f64(value) {
                    Some(t) => t,
                    None => {
                        // String is not a number - REAL <= TEXT is always true
                        return Ok(apply_null_mask_constant_packed(nulls, values.len(), true));
                    }
                }
            } else {
                value_to_f64(value).ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                    operation: "comparison".to_string(),
                    left_type: "Float64".to_string(),
                    right_type: Some(format!("{:?}", value)),
                })?
            };
            simd_ops::le_f64_packed(values, threshold)
        }

        ColumnPredicate::Equal { value, .. } => {
            // SQLite affinity: try to parse string as number for numeric comparison
            let target = if is_string_value(value) {
                match try_parse_string_as_f64(value) {
                    Some(t) => t,
                    None => {
                        // String is not a number - REAL = TEXT is always false
                        return Ok(apply_null_mask_constant_packed(nulls, values.len(), false));
                    }
                }
            } else {
                value_to_f64(value).ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                    operation: "comparison".to_string(),
                    left_type: "Float64".to_string(),
                    right_type: Some(format!("{:?}", value)),
                })?
            };
            simd_ops::eq_f64_packed(values, target)
        }

        ColumnPredicate::Between { low, high, .. } => {
            // SQLite affinity: try to parse strings as numbers for numeric comparison
            let low_f64 =
                if is_string_value(low) { try_parse_string_as_f64(low) } else { value_to_f64(low) };
            let high_f64 = if is_string_value(high) {
                try_parse_string_as_f64(high)
            } else {
                value_to_f64(high)
            };

            if let (Some(lo), Some(hi)) = (low_f64, high_f64) {
                simd_ops::between_f64_packed(values, lo, hi)
            } else {
                // Can't parse as numbers - REAL BETWEEN TEXT AND TEXT is always false
                return Ok(apply_null_mask_constant_packed(nulls, values.len(), false));
            }
        }

        ColumnPredicate::NotEqual { value, .. } => {
            // SQLite affinity: try to parse string as number for numeric comparison
            let target = if is_string_value(value) {
                match try_parse_string_as_f64(value) {
                    Some(t) => t,
                    None => {
                        // String is not a number - REAL <> TEXT is always true
                        return Ok(apply_null_mask_constant_packed(nulls, values.len(), true));
                    }
                }
            } else {
                value_to_f64(value).ok_or_else(|| ExecutorError::ColumnarTypeMismatch {
                    operation: "comparison".to_string(),
                    left_type: "Float64".to_string(),
                    right_type: Some(format!("{:?}", value)),
                })?
            };
            simd_ops::ne_f64_packed(values, target)
        }

        ColumnPredicate::Like { .. } => {
            return Err(ExecutorError::ColumnarTypeMismatch {
                operation: "LIKE".to_string(),
                left_type: "Float64".to_string(),
                right_type: Some("String pattern".to_string()),
            });
        }

        ColumnPredicate::InList { values: list_values, negated, .. } => {
            // For f64 columns with packed mask
            let mut result = PackedMask::new_all_clear(values.len());
            for f_val in list_values {
                // SQLite affinity: try to parse string values as numbers
                let target = if is_string_value(f_val) {
                    try_parse_string_as_f64(f_val)
                } else {
                    value_to_f64(f_val)
                };
                if let Some(t) = target {
                    let matches = simd_ops::eq_f64_packed(values, t);
                    result.or_inplace(&matches);
                }
            }
            if *negated {
                result = result.not();
            }
            result
        }

        // ColumnCompare is handled at higher level in simd_filter/mod.rs
        ColumnPredicate::ColumnCompare { .. } => {
            return Err(ExecutorError::ColumnarTypeMismatch {
                operation: "column-to-column comparison".to_string(),
                left_type: "Float64".to_string(),
                right_type: Some("Should be handled in simd_filter/mod.rs".to_string()),
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
