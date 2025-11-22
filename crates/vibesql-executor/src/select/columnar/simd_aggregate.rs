//! SIMD-accelerated columnar aggregation operations
//!
//! This module provides high-performance aggregate computations for Int64 and Float64
//! columns using SIMD operations, achieving 5-10x speedup over scalar implementations.

use crate::errors::ExecutorError;
use crate::simd::aggregation::*;
use super::aggregate::AggregateOp;
use super::scan::ColumnarScan;
use vibesql_types::SqlValue;

/// Compute SIMD aggregate for Int64 columns using streaming batches
///
/// Processes values in fixed-size batches to avoid allocating entire column in memory.
///
/// # Arguments
///
/// * `scan` - Columnar scan over the data
/// * `column_idx` - Index of the column to aggregate
/// * `op` - Aggregate operation (SUM, AVG, MIN, MAX, COUNT)
/// * `filter_bitmap` - Optional bitmap of which rows to include
///
/// # Returns
///
/// The aggregated SqlValue or an error if the column contains non-i64 values
#[cfg(feature = "simd")]
pub fn simd_aggregate_i64(
    scan: &ColumnarScan,
    column_idx: usize,
    op: AggregateOp,
    filter_bitmap: Option<&[bool]>,
) -> Result<SqlValue, ExecutorError> {
    const BATCH_SIZE: usize = 1024; // Process 1K values at a time

    let mut batch = Vec::with_capacity(BATCH_SIZE);
    let mut original_type: Option<SqlValue> = None;
    let mut count = 0i64;

    // Accumulators for different operations
    let mut sum = 0i64;
    let mut min = i64::MAX;
    let mut max = i64::MIN;

    for (row_idx, value_opt) in scan.column(column_idx).enumerate() {
        // Check filter bitmap
        if let Some(bitmap) = filter_bitmap {
            if !bitmap.get(row_idx).copied().unwrap_or(false) {
                continue;
            }
        }

        // Extract i64 value, skip NULLs
        if let Some(value) = value_opt {
            let i64_value = match value {
                SqlValue::Integer(v) => {
                    if original_type.is_none() {
                        original_type = Some(SqlValue::Integer(0));
                    }
                    *v
                }
                SqlValue::Bigint(v) => {
                    if original_type.is_none() {
                        original_type = Some(SqlValue::Bigint(0));
                    }
                    *v
                }
                SqlValue::Smallint(v) => {
                    if original_type.is_none() {
                        original_type = Some(SqlValue::Smallint(0));
                    }
                    *v as i64
                }
                SqlValue::Null => continue,
                _ => {
                    return Err(ExecutorError::UnsupportedExpression(
                        format!("Cannot compute SIMD aggregate on non-integer value: {:?}", value)
                    ))
                }
            };

            batch.push(i64_value);
            count += 1;

            // Process batch when full
            if batch.len() >= BATCH_SIZE {
                match op {
                    AggregateOp::Sum | AggregateOp::Avg => {
                        sum += simd_sum_i64(&batch);
                    }
                    AggregateOp::Min => {
                        if let Some(batch_min) = simd_min_i64(&batch) {
                            min = min.min(batch_min);
                        }
                    }
                    AggregateOp::Max => {
                        if let Some(batch_max) = simd_max_i64(&batch) {
                            max = max.max(batch_max);
                        }
                    }
                    AggregateOp::Count => {} // Just count, no SIMD needed
                }
                batch.clear();
            }
        }
    }

    // Process final partial batch
    if !batch.is_empty() {
        match op {
            AggregateOp::Sum | AggregateOp::Avg => {
                sum += simd_sum_i64(&batch);
            }
            AggregateOp::Min => {
                if let Some(batch_min) = simd_min_i64(&batch) {
                    min = min.min(batch_min);
                }
            }
            AggregateOp::Max => {
                if let Some(batch_max) = simd_max_i64(&batch) {
                    max = max.max(batch_max);
                }
            }
            AggregateOp::Count => {}
        }
    }

    // Handle empty result set
    if count == 0 {
        return Ok(match op {
            AggregateOp::Count => SqlValue::Integer(0),
            _ => SqlValue::Null,
        });
    }

    // Return aggregated result
    match op {
        AggregateOp::Sum => Ok(SqlValue::Double(sum as f64)),
        AggregateOp::Avg => Ok(SqlValue::Double(sum as f64 / count as f64)),
        AggregateOp::Min => {
            Ok(match original_type.unwrap_or(SqlValue::Bigint(0)) {
                SqlValue::Integer(_) => SqlValue::Integer(min),
                SqlValue::Smallint(_) => SqlValue::Smallint(min as i16),
                _ => SqlValue::Bigint(min),
            })
        }
        AggregateOp::Max => {
            Ok(match original_type.unwrap_or(SqlValue::Bigint(0)) {
                SqlValue::Integer(_) => SqlValue::Integer(max),
                SqlValue::Smallint(_) => SqlValue::Smallint(max as i16),
                _ => SqlValue::Bigint(max),
            })
        }
        AggregateOp::Count => Ok(SqlValue::Integer(count)),
    }
}

/// Compute SIMD aggregate for Float64 columns using streaming batches
///
/// Processes values in fixed-size batches to avoid allocating entire column in memory.
///
/// # Arguments
///
/// * `scan` - Columnar scan over the data
/// * `column_idx` - Index of the column to aggregate
/// * `op` - Aggregate operation (SUM, AVG, MIN, MAX, COUNT)
/// * `filter_bitmap` - Optional bitmap of which rows to include
///
/// # Returns
///
/// The aggregated SqlValue or an error if the column contains non-f64 values
#[cfg(feature = "simd")]
pub fn simd_aggregate_f64(
    scan: &ColumnarScan,
    column_idx: usize,
    op: AggregateOp,
    filter_bitmap: Option<&[bool]>,
) -> Result<SqlValue, ExecutorError> {
    const BATCH_SIZE: usize = 1024; // Process 1K values at a time

    let mut batch = Vec::with_capacity(BATCH_SIZE);
    let mut count = 0i64;

    // Accumulators for different operations
    let mut sum = 0.0f64;
    let mut min = f64::INFINITY;
    let mut max = f64::NEG_INFINITY;

    for (row_idx, value_opt) in scan.column(column_idx).enumerate() {
        // Check filter bitmap
        if let Some(bitmap) = filter_bitmap {
            if !bitmap.get(row_idx).copied().unwrap_or(false) {
                continue;
            }
        }

        // Extract f64 value, skip NULLs
        if let Some(value) = value_opt {
            let f64_value = match value {
                SqlValue::Double(v) => *v,
                SqlValue::Float(v) => *v as f64,
                SqlValue::Numeric(v) => *v,
                SqlValue::Integer(v) => *v as f64,
                SqlValue::Bigint(v) => *v as f64,
                SqlValue::Smallint(v) => *v as f64,
                SqlValue::Null => continue,
                _ => {
                    return Err(ExecutorError::UnsupportedExpression(
                        format!("Cannot compute SIMD aggregate on non-numeric value: {:?}", value)
                    ))
                }
            };

            batch.push(f64_value);
            count += 1;

            // Process batch when full
            if batch.len() >= BATCH_SIZE {
                match op {
                    AggregateOp::Sum | AggregateOp::Avg => {
                        sum += simd_sum_f64(&batch);
                    }
                    AggregateOp::Min => {
                        if let Some(batch_min) = simd_min_f64(&batch) {
                            min = min.min(batch_min);
                        }
                    }
                    AggregateOp::Max => {
                        if let Some(batch_max) = simd_max_f64(&batch) {
                            max = max.max(batch_max);
                        }
                    }
                    AggregateOp::Count => {} // Just count, no SIMD needed
                }
                batch.clear();
            }
        }
    }

    // Process final partial batch
    if !batch.is_empty() {
        match op {
            AggregateOp::Sum | AggregateOp::Avg => {
                sum += simd_sum_f64(&batch);
            }
            AggregateOp::Min => {
                if let Some(batch_min) = simd_min_f64(&batch) {
                    min = min.min(batch_min);
                }
            }
            AggregateOp::Max => {
                if let Some(batch_max) = simd_max_f64(&batch) {
                    max = max.max(batch_max);
                }
            }
            AggregateOp::Count => {}
        }
    }

    // Handle empty result set
    if count == 0 {
        return Ok(match op {
            AggregateOp::Count => SqlValue::Integer(0),
            _ => SqlValue::Null,
        });
    }

    // Return aggregated result
    match op {
        AggregateOp::Sum => Ok(SqlValue::Double(sum)),
        AggregateOp::Avg => Ok(SqlValue::Double(sum / count as f64)),
        AggregateOp::Min => Ok(SqlValue::Double(min)),
        AggregateOp::Max => Ok(SqlValue::Double(max)),
        AggregateOp::Count => Ok(SqlValue::Integer(count)),
    }
}

/// Determine if a column can use SIMD path based on its data type
///
/// Checks the first non-NULL value in the column to determine if it's
/// an integer or float type suitable for SIMD operations.
///
/// # Returns
///
/// - `Some(true)` if column is i64-compatible (Integer, Bigint, Smallint)
/// - `Some(false)` if column is f64-compatible (Double, Float, Numeric)
/// - `None` if column is not SIMD-compatible (String, Date, etc.) or all NULL
#[cfg(feature = "simd")]
pub fn can_use_simd_for_column(scan: &ColumnarScan, column_idx: usize) -> Option<bool> {
    // Check first non-NULL value to determine column type
    // IMPORTANT: Only check first 100 rows to avoid materializing huge columns
    for (idx, value_opt) in scan.column(column_idx).enumerate() {
        if idx >= 100 {
            break; // Stop after checking 100 rows
        }

        if let Some(value) = value_opt {
            return match value {
                SqlValue::Integer(_) | SqlValue::Bigint(_) | SqlValue::Smallint(_) => Some(true),
                SqlValue::Double(_) | SqlValue::Float(_) | SqlValue::Numeric(_) => Some(false),
                SqlValue::Null => continue, // Skip NULLs
                _ => None, // Not SIMD-compatible
            };
        }
    }

    None // All NULL or empty column
}

#[cfg(all(test, feature = "simd"))]
mod tests {
    use super::*;
    use vibesql_storage::Row;

    #[test]
    fn test_simd_aggregate_i64_sum() {
        let rows = vec![
            Row::new(vec![SqlValue::Integer(10)]),
            Row::new(vec![SqlValue::Integer(20)]),
            Row::new(vec![SqlValue::Integer(30)]),
        ];
        let scan = ColumnarScan::new(&rows);

        let result = simd_aggregate_i64(&scan, 0, AggregateOp::Sum, None).unwrap();
        // SUM returns Double to match scalar behavior
        assert!(matches!(result, SqlValue::Double(sum) if (sum - 60.0).abs() < 0.001));
    }

    #[test]
    fn test_simd_aggregate_i64_avg() {
        let rows = vec![
            Row::new(vec![SqlValue::Integer(10)]),
            Row::new(vec![SqlValue::Integer(20)]),
            Row::new(vec![SqlValue::Integer(30)]),
        ];
        let scan = ColumnarScan::new(&rows);

        let result = simd_aggregate_i64(&scan, 0, AggregateOp::Avg, None).unwrap();
        assert!(matches!(result, SqlValue::Double(avg) if (avg - 20.0).abs() < 0.001));
    }

    #[test]
    fn test_simd_aggregate_i64_min_max() {
        let rows = vec![
            Row::new(vec![SqlValue::Integer(30)]),
            Row::new(vec![SqlValue::Integer(10)]),
            Row::new(vec![SqlValue::Integer(20)]),
        ];
        let scan = ColumnarScan::new(&rows);

        let min_result = simd_aggregate_i64(&scan, 0, AggregateOp::Min, None).unwrap();
        // MIN/MAX preserve original type
        assert_eq!(min_result, SqlValue::Integer(10));

        let max_result = simd_aggregate_i64(&scan, 0, AggregateOp::Max, None).unwrap();
        assert_eq!(max_result, SqlValue::Integer(30));
    }

    #[test]
    fn test_simd_aggregate_i64_count() {
        let rows = vec![
            Row::new(vec![SqlValue::Integer(10)]),
            Row::new(vec![SqlValue::Integer(20)]),
            Row::new(vec![SqlValue::Integer(30)]),
        ];
        let scan = ColumnarScan::new(&rows);

        let result = simd_aggregate_i64(&scan, 0, AggregateOp::Count, None).unwrap();
        assert_eq!(result, SqlValue::Integer(3));
    }

    #[test]
    fn test_simd_aggregate_i64_with_nulls() {
        let rows = vec![
            Row::new(vec![SqlValue::Integer(10)]),
            Row::new(vec![SqlValue::Null]),
            Row::new(vec![SqlValue::Integer(30)]),
        ];
        let scan = ColumnarScan::new(&rows);

        let result = simd_aggregate_i64(&scan, 0, AggregateOp::Sum, None).unwrap();
        // SUM returns Double to match scalar behavior
        assert!(matches!(result, SqlValue::Double(sum) if (sum - 40.0).abs() < 0.001));

        let count_result = simd_aggregate_i64(&scan, 0, AggregateOp::Count, None).unwrap();
        assert_eq!(count_result, SqlValue::Integer(2));
    }

    #[test]
    fn test_simd_aggregate_i64_with_filter() {
        let rows = vec![
            Row::new(vec![SqlValue::Integer(10)]),
            Row::new(vec![SqlValue::Integer(20)]),
            Row::new(vec![SqlValue::Integer(30)]),
        ];
        let scan = ColumnarScan::new(&rows);
        let filter = vec![true, false, true]; // Include rows 0 and 2

        let result = simd_aggregate_i64(&scan, 0, AggregateOp::Sum, Some(&filter)).unwrap();
        // SUM returns Double to match scalar behavior
        assert!(matches!(result, SqlValue::Double(sum) if (sum - 40.0).abs() < 0.001));
    }

    #[test]
    fn test_simd_aggregate_f64_sum() {
        let rows = vec![
            Row::new(vec![SqlValue::Double(1.5)]),
            Row::new(vec![SqlValue::Double(2.5)]),
            Row::new(vec![SqlValue::Double(3.5)]),
        ];
        let scan = ColumnarScan::new(&rows);

        let result = simd_aggregate_f64(&scan, 0, AggregateOp::Sum, None).unwrap();
        assert!(matches!(result, SqlValue::Double(sum) if (sum - 7.5).abs() < 0.001));
    }

    #[test]
    fn test_simd_aggregate_f64_avg() {
        let rows = vec![
            Row::new(vec![SqlValue::Double(1.0)]),
            Row::new(vec![SqlValue::Double(2.0)]),
            Row::new(vec![SqlValue::Double(3.0)]),
        ];
        let scan = ColumnarScan::new(&rows);

        let result = simd_aggregate_f64(&scan, 0, AggregateOp::Avg, None).unwrap();
        assert!(matches!(result, SqlValue::Double(avg) if (avg - 2.0).abs() < 0.001));
    }

    #[test]
    fn test_simd_aggregate_f64_min_max() {
        let rows = vec![
            Row::new(vec![SqlValue::Double(3.0)]),
            Row::new(vec![SqlValue::Double(1.0)]),
            Row::new(vec![SqlValue::Double(2.0)]),
        ];
        let scan = ColumnarScan::new(&rows);

        let min_result = simd_aggregate_f64(&scan, 0, AggregateOp::Min, None).unwrap();
        assert!(matches!(min_result, SqlValue::Double(min) if (min - 1.0).abs() < 0.001));

        let max_result = simd_aggregate_f64(&scan, 0, AggregateOp::Max, None).unwrap();
        assert!(matches!(max_result, SqlValue::Double(max) if (max - 3.0).abs() < 0.001));
    }

    #[test]
    fn test_simd_aggregate_f64_with_nulls() {
        let rows = vec![
            Row::new(vec![SqlValue::Double(1.5)]),
            Row::new(vec![SqlValue::Null]),
            Row::new(vec![SqlValue::Double(3.5)]),
        ];
        let scan = ColumnarScan::new(&rows);

        let result = simd_aggregate_f64(&scan, 0, AggregateOp::Sum, None).unwrap();
        assert!(matches!(result, SqlValue::Double(sum) if (sum - 5.0).abs() < 0.001));
    }

    #[test]
    fn test_simd_aggregate_f64_with_filter() {
        let rows = vec![
            Row::new(vec![SqlValue::Double(1.5)]),
            Row::new(vec![SqlValue::Double(2.5)]),
            Row::new(vec![SqlValue::Double(3.5)]),
        ];
        let scan = ColumnarScan::new(&rows);
        let filter = vec![true, false, true]; // Include rows 0 and 2

        let result = simd_aggregate_f64(&scan, 0, AggregateOp::Sum, Some(&filter)).unwrap();
        assert!(matches!(result, SqlValue::Double(sum) if (sum - 5.0).abs() < 0.001));
    }

    #[test]
    fn test_simd_aggregate_empty_result() {
        let rows = vec![
            Row::new(vec![SqlValue::Integer(10)]),
            Row::new(vec![SqlValue::Integer(20)]),
        ];
        let scan = ColumnarScan::new(&rows);
        let filter = vec![false, false]; // Filter everything out

        let result = simd_aggregate_i64(&scan, 0, AggregateOp::Sum, Some(&filter)).unwrap();
        assert_eq!(result, SqlValue::Null);

        let count_result = simd_aggregate_i64(&scan, 0, AggregateOp::Count, Some(&filter)).unwrap();
        assert_eq!(count_result, SqlValue::Integer(0));
    }

    #[test]
    fn test_can_use_simd_for_column() {
        // Integer column
        let rows = vec![
            Row::new(vec![SqlValue::Integer(10)]),
            Row::new(vec![SqlValue::Integer(20)]),
        ];
        let scan = ColumnarScan::new(&rows);
        assert_eq!(can_use_simd_for_column(&scan, 0), Some(true));

        // Float column
        let rows = vec![
            Row::new(vec![SqlValue::Double(1.5)]),
            Row::new(vec![SqlValue::Double(2.5)]),
        ];
        let scan = ColumnarScan::new(&rows);
        assert_eq!(can_use_simd_for_column(&scan, 0), Some(false));

        // String column (not SIMD-compatible)
        let rows = vec![
            Row::new(vec![SqlValue::Varchar("hello".to_string())]),
            Row::new(vec![SqlValue::Varchar("world".to_string())]),
        ];
        let scan = ColumnarScan::new(&rows);
        assert_eq!(can_use_simd_for_column(&scan, 0), None);

        // All NULL column
        let rows = vec![
            Row::new(vec![SqlValue::Null]),
            Row::new(vec![SqlValue::Null]),
        ];
        let scan = ColumnarScan::new(&rows);
        assert_eq!(can_use_simd_for_column(&scan, 0), None);
    }
}
