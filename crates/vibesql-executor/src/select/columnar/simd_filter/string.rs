//! String-specific filter operations for SIMD-accelerated filtering
//!
//! This module provides batch string comparison and pattern matching operations
//! using optimized string_ops functions.

use super::super::filter::ColumnPredicate;
use super::super::string_ops::{
    batch_string_eq, batch_string_ge, batch_string_gt, batch_string_le, batch_string_like,
    batch_string_lt, batch_string_ne, LikePattern,
};
use crate::errors::ExecutorError;
use vibesql_types::SqlValue;

/// Evaluate predicate on string column using batch operations
pub fn evaluate_predicate_string_batch(
    predicate: &ColumnPredicate,
    values: &[std::sync::Arc<str>],
    nulls: Option<&[bool]>,
) -> Result<Vec<bool>, ExecutorError> {
    let result = match predicate {
        ColumnPredicate::Equal { value, .. } => {
            // Extract target string
            let target = match value {
                SqlValue::Character(s) | SqlValue::Varchar(s) => &**s,
                _ => {
                    return Err(ExecutorError::ColumnarTypeMismatch {
                        operation: "string equality".to_string(),
                        left_type: "String".to_string(),
                        right_type: Some(format!("{:?}", value)),
                    })
                }
            };
            batch_string_eq(values, nulls, target)
        }

        ColumnPredicate::LessThan { value, .. } => {
            let target = match value {
                SqlValue::Character(s) | SqlValue::Varchar(s) => &**s,
                _ => {
                    return Err(ExecutorError::ColumnarTypeMismatch {
                        operation: "string comparison".to_string(),
                        left_type: "String".to_string(),
                        right_type: Some(format!("{:?}", value)),
                    })
                }
            };
            batch_string_lt(values, nulls, target)
        }

        ColumnPredicate::GreaterThan { value, .. } => {
            let target = match value {
                SqlValue::Character(s) | SqlValue::Varchar(s) => &**s,
                _ => {
                    return Err(ExecutorError::ColumnarTypeMismatch {
                        operation: "string comparison".to_string(),
                        left_type: "String".to_string(),
                        right_type: Some(format!("{:?}", value)),
                    })
                }
            };
            batch_string_gt(values, nulls, target)
        }

        ColumnPredicate::LessThanOrEqual { value, .. } => {
            let target = match value {
                SqlValue::Character(s) | SqlValue::Varchar(s) => &**s,
                _ => {
                    return Err(ExecutorError::ColumnarTypeMismatch {
                        operation: "string comparison".to_string(),
                        left_type: "String".to_string(),
                        right_type: Some(format!("{:?}", value)),
                    })
                }
            };
            batch_string_le(values, nulls, target)
        }

        ColumnPredicate::GreaterThanOrEqual { value, .. } => {
            let target = match value {
                SqlValue::Character(s) | SqlValue::Varchar(s) => &**s,
                _ => {
                    return Err(ExecutorError::ColumnarTypeMismatch {
                        operation: "string comparison".to_string(),
                        left_type: "String".to_string(),
                        right_type: Some(format!("{:?}", value)),
                    })
                }
            };
            batch_string_ge(values, nulls, target)
        }

        ColumnPredicate::NotEqual { value, .. } => {
            let target = match value {
                SqlValue::Character(s) | SqlValue::Varchar(s) => &**s,
                _ => {
                    return Err(ExecutorError::ColumnarTypeMismatch {
                        operation: "string comparison".to_string(),
                        left_type: "String".to_string(),
                        right_type: Some(format!("{:?}", value)),
                    })
                }
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
                SqlValue::Character(s) | SqlValue::Varchar(s) => &**s,
                _ => {
                    return Err(ExecutorError::ColumnarTypeMismatch {
                        operation: "string BETWEEN".to_string(),
                        left_type: "String".to_string(),
                        right_type: Some(format!("{:?}", low)),
                    })
                }
            };
            let high_str = match high {
                SqlValue::Character(s) | SqlValue::Varchar(s) => &**s,
                _ => {
                    return Err(ExecutorError::ColumnarTypeMismatch {
                        operation: "string BETWEEN".to_string(),
                        left_type: "String".to_string(),
                        right_type: Some(format!("{:?}", high)),
                    })
                }
            };

            // value >= low AND value <= high
            let ge_low = batch_string_ge(values, nulls, low_str);
            let le_high = batch_string_le(values, nulls, high_str);

            ge_low.iter().zip(le_high.iter()).map(|(&a, &b)| a && b).collect()
        }

        ColumnPredicate::InList { values: list_values, negated, .. } => {
            // For string columns, check if value is in the list
            let mut result = vec![false; values.len()];

            // Apply null mask first
            if let Some(null_mask) = nulls {
                for (i, &is_null) in null_mask.iter().enumerate() {
                    if is_null {
                        result[i] = false;
                    }
                }
            }

            // Check each list value
            for list_val in list_values {
                let target = match list_val {
                    SqlValue::Character(s) | SqlValue::Varchar(s) => &**s,
                    _ => continue, // Skip non-string values
                };
                let matches = batch_string_eq(values, nulls, target);
                for (i, &m) in matches.iter().enumerate() {
                    result[i] = result[i] || m;
                }
            }

            if *negated {
                result.iter_mut().for_each(|v| *v = !*v);
            }
            result
        }

        // ColumnCompare is not supported for string columns in this path
        // It's handled at a higher level in simd_filter/mod.rs
        ColumnPredicate::ColumnCompare { .. } => {
            return Err(ExecutorError::ColumnarTypeMismatch {
                operation: "column-to-column comparison".to_string(),
                left_type: "String".to_string(),
                right_type: Some("Should be handled in simd_filter/mod.rs".to_string()),
            });
        }
    };

    Ok(result)
}
