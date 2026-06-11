//! String-specific filter operations for SIMD-accelerated filtering
//!
//! This module provides batch string comparison and pattern matching operations
//! using optimized string_ops functions.

use vibesql_types::SqlValue;

use super::super::{
    filter::ColumnPredicate,
    string_ops::{
        batch_string_eq, batch_string_ge, batch_string_gt, batch_string_le, batch_string_like,
        batch_string_lt, batch_string_ne, LikePattern,
    },
};
use crate::errors::ExecutorError;

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
                // SQLite affinity: TEXT vs INTEGER/REAL equality is always false
                // Different types never match in equality comparison
                SqlValue::Integer(_)
                | SqlValue::Bigint(_)
                | SqlValue::Real(_)
                | SqlValue::Numeric(_)
                | SqlValue::Smallint(_) => {
                    // Return all false - type mismatch never matches
                    return Ok(vec![false; values.len()]);
                }
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
                // SQLite affinity: TEXT is always GREATER than INTEGER/REAL
                // So "string < number" is always false
                SqlValue::Integer(_)
                | SqlValue::Bigint(_)
                | SqlValue::Real(_)
                | SqlValue::Numeric(_)
                | SqlValue::Smallint(_) => {
                    return Ok(vec![false; values.len()]);
                }
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
                // SQLite affinity: TEXT is always GREATER than INTEGER/REAL
                // So "string > number" is always true (for non-null values)
                SqlValue::Integer(_)
                | SqlValue::Bigint(_)
                | SqlValue::Real(_)
                | SqlValue::Numeric(_)
                | SqlValue::Smallint(_) => {
                    return Ok(if let Some(null_mask) = nulls {
                        null_mask.iter().map(|&is_null| !is_null).collect()
                    } else {
                        vec![true; values.len()]
                    });
                }
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
                // SQLite affinity: TEXT is always GREATER than INTEGER/REAL
                // So "string <= number" is always false
                SqlValue::Integer(_)
                | SqlValue::Bigint(_)
                | SqlValue::Real(_)
                | SqlValue::Numeric(_)
                | SqlValue::Smallint(_) => {
                    return Ok(vec![false; values.len()]);
                }
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
                // SQLite affinity: TEXT is always GREATER than INTEGER/REAL
                // So "string >= number" is always true (for non-null values)
                SqlValue::Integer(_)
                | SqlValue::Bigint(_)
                | SqlValue::Real(_)
                | SqlValue::Numeric(_)
                | SqlValue::Smallint(_) => {
                    return Ok(if let Some(null_mask) = nulls {
                        null_mask.iter().map(|&is_null| !is_null).collect()
                    } else {
                        vec![true; values.len()]
                    });
                }
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
                // SQLite affinity: TEXT vs INTEGER/REAL are always not equal
                // So "string <> number" is always true (for non-null values)
                SqlValue::Integer(_)
                | SqlValue::Bigint(_)
                | SqlValue::Real(_)
                | SqlValue::Numeric(_)
                | SqlValue::Smallint(_) => {
                    return Ok(if let Some(null_mask) = nulls {
                        null_mask.iter().map(|&is_null| !is_null).collect()
                    } else {
                        vec![true; values.len()]
                    });
                }
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

        ColumnPredicate::Like { pattern, negated, case_sensitive, escape, .. } => {
            // When escape is set or case_sensitive is true, use the full pattern matcher
            // since the optimized batch functions only handle case-insensitive without escape
            let mut mask = if escape.is_some() || *case_sensitive {
                let mut result = Vec::with_capacity(values.len());
                for (i, value) in values.iter().enumerate() {
                    if let Some(null_mask) = nulls {
                        if null_mask[i] {
                            result.push(false);
                            continue;
                        }
                    }
                    result.push(crate::evaluator::pattern::like_match(
                        value,
                        pattern,
                        *case_sensitive,
                        *escape,
                    ));
                }
                result
            } else {
                let parsed_pattern = LikePattern::parse(pattern);
                batch_string_like(values, nulls, &parsed_pattern)
            };

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
            // Helper to check if a value is numeric
            let is_numeric = |v: &SqlValue| {
                matches!(
                    v,
                    SqlValue::Integer(_)
                        | SqlValue::Bigint(_)
                        | SqlValue::Real(_)
                        | SqlValue::Numeric(_)
                        | SqlValue::Smallint(_)
                )
            };

            // SQLite affinity: TEXT is always GREATER than INTEGER/REAL
            // For "string BETWEEN low_num AND high_num":
            //   - string >= low_num is true (text > numbers)
            //   - string <= high_num is false (text > numbers)
            //   - Result: true AND false = false
            if is_numeric(low) && is_numeric(high) {
                return Ok(vec![false; values.len()]);
            }

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
            // SQL three-valued logic (issue #5341): a NULL list element never
            // matches, but it poisons NOT IN — when no element matches the
            // result is UNKNOWN, so `x NOT IN (..., NULL)` is never TRUE.
            if *negated && list_values.iter().any(|v| matches!(v, SqlValue::Null)) {
                return Ok(vec![false; values.len()]);
            }

            // For string columns, check if value is in the list
            let mut result = vec![false; values.len()];

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
                // Invert only non-NULL rows: a NULL column value is UNKNOWN
                // for NOT IN, not TRUE (issue #5341 — the unconditional
                // inversion used to resurrect NULL rows)
                if let Some(null_mask) = nulls {
                    for (i, v) in result.iter_mut().enumerate() {
                        if !null_mask[i] {
                            *v = !*v;
                        }
                    }
                } else {
                    result.iter_mut().for_each(|v| *v = !*v);
                }
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
