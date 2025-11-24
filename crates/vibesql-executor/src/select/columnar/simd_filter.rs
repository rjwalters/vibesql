//! SIMD-accelerated filtering for columnar batches

use super::batch::{ColumnArray, ColumnarBatch};
use super::filter::ColumnPredicate;
use crate::errors::ExecutorError;

#[cfg(feature = "simd")]
use crate::simd::comparison::{
    simd_eq_f64, simd_eq_i32, simd_eq_i64, simd_ge_f64, simd_ge_i32, simd_ge_i64, simd_gt_f64,
    simd_gt_i32, simd_gt_i64, simd_le_f64, simd_le_i32, simd_le_i64, simd_lt_f64, simd_lt_i32,
    simd_lt_i64, simd_ne_i32, simd_ne_i64,
};

use vibesql_types::SqlValue;

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
fn simd_create_filter_mask(
    batch: &ColumnarBatch,
    predicates: &[ColumnPredicate],
) -> Result<Vec<bool>, ExecutorError> {
    let row_count = batch.row_count();

    // Start with all rows passing
    let mut mask = vec![true; row_count];

    // Evaluate each predicate and AND the results
    for predicate in predicates {
        let predicate_mask = evaluate_predicate_simd(batch, predicate)?;

        // AND with existing mask
        for i in 0..row_count {
            mask[i] = mask[i] && predicate_mask[i];
        }
    }

    Ok(mask)
}

/// Evaluate a single predicate using SIMD operations when possible
fn evaluate_predicate_simd(
    batch: &ColumnarBatch,
    predicate: &ColumnPredicate,
) -> Result<Vec<bool>, ExecutorError> {
    let column_idx = match predicate {
        ColumnPredicate::LessThan { column_idx, .. }
        | ColumnPredicate::GreaterThan { column_idx, .. }
        | ColumnPredicate::GreaterThanOrEqual { column_idx, .. }
        | ColumnPredicate::LessThanOrEqual { column_idx, .. }
        | ColumnPredicate::Equal { column_idx, .. }
        | ColumnPredicate::Between { column_idx, .. } => *column_idx,
    };

    let column = batch
        .column(column_idx)
        .ok_or_else(|| ExecutorError::Other(format!("Column index {} out of bounds", column_idx).to_string()))?;

    match column {
        // SIMD path for i64 columns
        ColumnArray::Int64(values, nulls) => {
            evaluate_predicate_i64_simd(predicate, values, nulls.as_ref())
        }

        // SIMD path for f64 columns
        ColumnArray::Float64(values, nulls) => {
            evaluate_predicate_f64_simd(predicate, values, nulls.as_ref())
        }

        // SIMD path for Date columns (i32 - days since epoch)
        ColumnArray::Date(values, nulls) => {
            evaluate_predicate_i32_simd(predicate, values, nulls.as_ref())
        }

        // SIMD path for Timestamp columns (i64 - microseconds since epoch)
        ColumnArray::Timestamp(values, nulls) => {
            evaluate_predicate_i64_simd(predicate, values, nulls.as_ref())
        }

        // Scalar fallback for other column types
        _ => evaluate_predicate_scalar(batch, predicate, column_idx),
    }
}

/// Evaluate predicate on i64 column using SIMD
fn evaluate_predicate_i64_simd(
    predicate: &ColumnPredicate,
    values: &[i64],
    nulls: Option<&Vec<bool>>,
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
                    .ok_or_else(|| ExecutorError::Other("Incompatible types for comparison".to_string()))?;
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
                    .ok_or_else(|| ExecutorError::Other("Incompatible types for comparison".to_string()))?;
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
                    .ok_or_else(|| ExecutorError::Other("Incompatible types for comparison".to_string()))?;
                let f64_values: Vec<f64> = values.iter().map(|&v| v as f64).collect();
                simd_eq_f64(&f64_values, target)
            }
        }

        ColumnPredicate::Between { low, high, .. } => {
            // BETWEEN is equivalent to: value >= low AND value <= high
            let low_i64 = match low {
                SqlValue::Integer(v) => *v,
                SqlValue::Bigint(v) => *v,
                _ => {
                    return Err(ExecutorError::Other(
                        "Incompatible types for BETWEEN".to_string(),
                    ))
                }
            };
            let high_i64 = match high {
                SqlValue::Integer(v) => *v,
                SqlValue::Bigint(v) => *v,
                _ => {
                    return Err(ExecutorError::Other(
                        "Incompatible types for BETWEEN".to_string(),
                    ))
                }
            };

            let mut result = vec![true; values.len()];
            for i in 0..values.len() {
                result[i] = values[i] >= low_i64 && values[i] <= high_i64;
            }
            result
        }

        // For LTE and GTE, we use LT/GT and equality checks
        ColumnPredicate::GreaterThanOrEqual { value, .. } => {
            let gt = if let SqlValue::Integer(threshold) = value {
                simd_gt_i64(values, *threshold)
            } else if let SqlValue::Bigint(threshold) = value {
                simd_gt_i64(values, *threshold)
            } else {
                let threshold = value_to_f64(value)
                    .ok_or_else(|| ExecutorError::Other("Incompatible types for comparison".to_string()))?;
                let f64_values: Vec<f64> = values.iter().map(|&v| v as f64).collect();
                simd_gt_f64(&f64_values, threshold)
            };

            let eq = if let SqlValue::Integer(target) = value {
                simd_eq_i64(values, *target)
            } else if let SqlValue::Bigint(target) = value {
                simd_eq_i64(values, *target)
            } else {
                let target = value_to_f64(value)
                    .ok_or_else(|| ExecutorError::Other("Incompatible types for comparison".to_string()))?;
                let f64_values: Vec<f64> = values.iter().map(|&v| v as f64).collect();
                simd_eq_f64(&f64_values, target)
            };

            // OR the two masks
            gt.iter().zip(eq.iter()).map(|(&a, &b)| a || b).collect()
        }

        ColumnPredicate::LessThanOrEqual { value, .. } => {
            let lt = if let SqlValue::Integer(threshold) = value {
                simd_lt_i64(values, *threshold)
            } else if let SqlValue::Bigint(threshold) = value {
                simd_lt_i64(values, *threshold)
            } else {
                let threshold = value_to_f64(value)
                    .ok_or_else(|| ExecutorError::Other("Incompatible types for comparison".to_string()))?;
                let f64_values: Vec<f64> = values.iter().map(|&v| v as f64).collect();
                simd_lt_f64(&f64_values, threshold)
            };

            let eq = if let SqlValue::Integer(target) = value {
                simd_eq_i64(values, *target)
            } else if let SqlValue::Bigint(target) = value {
                simd_eq_i64(values, *target)
            } else {
                let target = value_to_f64(value)
                    .ok_or_else(|| ExecutorError::Other("Incompatible types for comparison".to_string()))?;
                let f64_values: Vec<f64> = values.iter().map(|&v| v as f64).collect();
                simd_eq_f64(&f64_values, target)
            };

            // OR the two masks
            lt.iter().zip(eq.iter()).map(|(&a, &b)| a || b).collect()
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
    nulls: Option<&Vec<bool>>,
) -> Result<Vec<bool>, ExecutorError> {
    let mut result = match predicate {
        ColumnPredicate::LessThan { value, .. } => {
            let threshold = value_to_date_i32(value)
                .ok_or_else(|| ExecutorError::Other("Incompatible types for date comparison".to_string()))?;
            simd_lt_i32(values, threshold)
        }

        ColumnPredicate::GreaterThan { value, .. } => {
            let threshold = value_to_date_i32(value)
                .ok_or_else(|| ExecutorError::Other("Incompatible types for date comparison".to_string()))?;
            simd_gt_i32(values, threshold)
        }

        ColumnPredicate::GreaterThanOrEqual { value, .. } => {
            let threshold = value_to_date_i32(value)
                .ok_or_else(|| ExecutorError::Other("Incompatible types for date comparison".to_string()))?;
            simd_ge_i32(values, threshold)
        }

        ColumnPredicate::LessThanOrEqual { value, .. } => {
            let threshold = value_to_date_i32(value)
                .ok_or_else(|| ExecutorError::Other("Incompatible types for date comparison".to_string()))?;
            simd_le_i32(values, threshold)
        }

        ColumnPredicate::Equal { value, .. } => {
            let target = value_to_date_i32(value)
                .ok_or_else(|| ExecutorError::Other("Incompatible types for date comparison".to_string()))?;
            simd_eq_i32(values, target)
        }

        ColumnPredicate::Between { low, high, ..} => {
            let low_i32 = value_to_date_i32(low)
                .ok_or_else(|| ExecutorError::Other("Incompatible types for BETWEEN".to_string()))?;
            let high_i32 = value_to_date_i32(high)
                .ok_or_else(|| ExecutorError::Other("Incompatible types for BETWEEN".to_string()))?;

            let ge_low = simd_ge_i32(values, low_i32);
            let le_high = simd_le_i32(values, high_i32);

            // AND the two masks
            ge_low
                .iter()
                .zip(le_high.iter())
                .map(|(&a, &b)| a && b)
                .collect()
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
    nulls: Option<&Vec<bool>>,
) -> Result<Vec<bool>, ExecutorError> {
    let mut result = match predicate {
        ColumnPredicate::LessThan { value, .. } => {
            let threshold = value_to_f64(value)
                .ok_or_else(|| ExecutorError::Other("Incompatible types for comparison".to_string()))?;
            simd_lt_f64(values, threshold)
        }

        ColumnPredicate::GreaterThan { value, .. } => {
            let threshold = value_to_f64(value)
                .ok_or_else(|| ExecutorError::Other("Incompatible types for comparison".to_string()))?;
            simd_gt_f64(values, threshold)
        }

        ColumnPredicate::GreaterThanOrEqual { value, .. } => {
            let threshold = value_to_f64(value)
                .ok_or_else(|| ExecutorError::Other("Incompatible types for comparison".to_string()))?;
            simd_ge_f64(values, threshold)
        }

        ColumnPredicate::LessThanOrEqual { value, .. } => {
            let threshold = value_to_f64(value)
                .ok_or_else(|| ExecutorError::Other("Incompatible types for comparison".to_string()))?;
            simd_le_f64(values, threshold)
        }

        ColumnPredicate::Equal { value, .. } => {
            let target = value_to_f64(value)
                .ok_or_else(|| ExecutorError::Other("Incompatible types for comparison".to_string()))?;
            simd_eq_f64(values, target)
        }

        ColumnPredicate::Between { low, high, .. } => {
            let low_f64 = value_to_f64(low)
                .ok_or_else(|| ExecutorError::Other("Incompatible types for BETWEEN".to_string()))?;
            let high_f64 = value_to_f64(high)
                .ok_or_else(|| ExecutorError::Other("Incompatible types for BETWEEN".to_string()))?;

            let ge_low = simd_ge_f64(values, low_f64);
            let le_high = simd_le_f64(values, high_f64);

            // AND the two masks
            ge_low
                .iter()
                .zip(le_high.iter())
                .map(|(&a, &b)| a && b)
                .collect()
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
        return Err(ExecutorError::Other(
            "Filter mask length does not match batch row count".to_string(),
        ));
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
            .ok_or_else(|| ExecutorError::Other(format!("Column {} not found", col_idx).to_string()))?;

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
                null_mask
                    .iter()
                    .zip(mask.iter())
                    .filter_map(|(&n, &keep)| if keep { Some(n) } else { None })
                    .collect()
            });

            Ok(ColumnArray::Int64(new_values, new_nulls))
        }

        ColumnArray::Int32(values, nulls) => {
            let new_values: Vec<i32> = values
                .iter()
                .zip(mask.iter())
                .filter_map(|(&v, &keep)| if keep { Some(v) } else { None })
                .collect();

            let new_nulls = nulls.as_ref().map(|null_mask| {
                null_mask
                    .iter()
                    .zip(mask.iter())
                    .filter_map(|(&n, &keep)| if keep { Some(n) } else { None })
                    .collect()
            });

            Ok(ColumnArray::Int32(new_values, new_nulls))
        }

        ColumnArray::Float64(values, nulls) => {
            let new_values: Vec<f64> = values
                .iter()
                .zip(mask.iter())
                .filter_map(|(&v, &keep)| if keep { Some(v) } else { None })
                .collect();

            let new_nulls = nulls.as_ref().map(|null_mask| {
                null_mask
                    .iter()
                    .zip(mask.iter())
                    .filter_map(|(&n, &keep)| if keep { Some(n) } else { None })
                    .collect()
            });

            Ok(ColumnArray::Float64(new_values, new_nulls))
        }

        ColumnArray::Float32(values, nulls) => {
            let new_values: Vec<f32> = values
                .iter()
                .zip(mask.iter())
                .filter_map(|(&v, &keep)| if keep { Some(v) } else { None })
                .collect();

            let new_nulls = nulls.as_ref().map(|null_mask| {
                null_mask
                    .iter()
                    .zip(mask.iter())
                    .filter_map(|(&n, &keep)| if keep { Some(n) } else { None })
                    .collect()
            });

            Ok(ColumnArray::Float32(new_values, new_nulls))
        }

        ColumnArray::String(values, nulls) => {
            let new_values: Vec<String> = values
                .iter()
                .zip(mask.iter())
                .filter_map(|(v, &keep)| if keep { Some(v.clone()) } else { None })
                .collect();

            let new_nulls = nulls.as_ref().map(|null_mask| {
                null_mask
                    .iter()
                    .zip(mask.iter())
                    .filter_map(|(&n, &keep)| if keep { Some(n) } else { None })
                    .collect()
            });

            Ok(ColumnArray::String(new_values, new_nulls))
        }

        ColumnArray::FixedString(values, nulls) => {
            let new_values: Vec<String> = values
                .iter()
                .zip(mask.iter())
                .filter_map(|(v, &keep)| if keep { Some(v.clone()) } else { None })
                .collect();

            let new_nulls = nulls.as_ref().map(|null_mask| {
                null_mask
                    .iter()
                    .zip(mask.iter())
                    .filter_map(|(&n, &keep)| if keep { Some(n) } else { None })
                    .collect()
            });

            Ok(ColumnArray::FixedString(new_values, new_nulls))
        }

        ColumnArray::Date(values, nulls) => {
            let new_values: Vec<i32> = values
                .iter()
                .zip(mask.iter())
                .filter_map(|(&v, &keep)| if keep { Some(v) } else { None })
                .collect();

            let new_nulls = nulls.as_ref().map(|null_mask| {
                null_mask
                    .iter()
                    .zip(mask.iter())
                    .filter_map(|(&n, &keep)| if keep { Some(n) } else { None })
                    .collect()
            });

            Ok(ColumnArray::Date(new_values, new_nulls))
        }

        ColumnArray::Timestamp(values, nulls) => {
            let new_values: Vec<i64> = values
                .iter()
                .zip(mask.iter())
                .filter_map(|(&v, &keep)| if keep { Some(v) } else { None })
                .collect();

            let new_nulls = nulls.as_ref().map(|null_mask| {
                null_mask
                    .iter()
                    .zip(mask.iter())
                    .filter_map(|(&n, &keep)| if keep { Some(n) } else { None })
                    .collect()
            });

            Ok(ColumnArray::Timestamp(new_values, new_nulls))
        }

        ColumnArray::Boolean(values, nulls) => {
            let new_values: Vec<u8> = values
                .iter()
                .zip(mask.iter())
                .filter_map(|(&v, &keep)| if keep { Some(v) } else { None })
                .collect();

            let new_nulls = nulls.as_ref().map(|null_mask| {
                null_mask
                    .iter()
                    .zip(mask.iter())
                    .filter_map(|(&n, &keep)| if keep { Some(n) } else { None })
                    .collect()
            });

            Ok(ColumnArray::Boolean(new_values, new_nulls))
        }

        ColumnArray::Mixed(values) => {
            let new_values: Vec<SqlValue> = values
                .iter()
                .zip(mask.iter())
                .filter_map(|(v, &keep)| if keep { Some(v.clone()) } else { None })
                .collect();

            Ok(ColumnArray::Mixed(new_values))
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
        SqlValue::Numeric(n) => n.to_string().parse().ok(),
        SqlValue::Real(n) => Some(*n as f64),
        _ => None,
    }
}

/// Convert SqlValue::Date to i32 (days since Unix epoch)
fn value_to_date_i32(value: &SqlValue) -> Option<i32> {
    match value {
        SqlValue::Date(date) => Some(date_to_days_since_epoch(date)),
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
}
