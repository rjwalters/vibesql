//! Hash-based GROUP BY aggregation
//!
//! This module implements efficient hash-based grouping for columnar data,
//! enabling queries like TPC-H Q1 to use the columnar execution path.
//!
//! Two implementations are provided:
//! - `columnar_group_by`: Works on `&[Row]` via `ColumnarScan` (scalar aggregation)
//! - `columnar_group_by_batch`: Works on `ColumnarBatch` with SIMD aggregation (faster)

use ahash::AHashMap;
use vibesql_storage::Row;
use vibesql_types::SqlValue;

use super::{
    super::{
        batch::{ColumnArray, ColumnarBatch},
        scan::ColumnarScan,
    },
    functions::compute_columnar_aggregate_impl,
    AggregateOp,
};
use crate::{
    errors::ExecutorError,
    select::grouping::{GroupKey, GroupKeySpec},
};

// ============================================================================
// Index-based aggregation functions
// ============================================================================
// These functions aggregate over specific row indices instead of scanning
// all rows with a mask. This is significantly faster when groups are small
// relative to total rows (e.g., TPC-H Q1 with 6 groups over 60K rows).
//
// Performance impact: Reduces from O(rows * groups * aggregates) masked scans
// to O(rows * aggregates) direct accesses, providing ~3-5x speedup.

/// Sum i64 values at specific indices (unrolled for vectorization)
#[inline]
fn sum_i64_indexed(values: &[i64], indices: &[usize], nulls: Option<&[bool]>) -> i64 {
    let (mut s0, mut s1, mut s2, mut s3) = (0i64, 0i64, 0i64, 0i64);
    let chunks = indices.len() / 4;

    if let Some(null_mask) = nulls {
        for i in 0..chunks {
            let off = i * 4;
            let (i0, i1, i2, i3) =
                (indices[off], indices[off + 1], indices[off + 2], indices[off + 3]);
            if !null_mask[i0] {
                s0 = s0.wrapping_add(values[i0]);
            }
            if !null_mask[i1] {
                s1 = s1.wrapping_add(values[i1]);
            }
            if !null_mask[i2] {
                s2 = s2.wrapping_add(values[i2]);
            }
            if !null_mask[i3] {
                s3 = s3.wrapping_add(values[i3]);
            }
        }
        let mut sum = s0.wrapping_add(s1).wrapping_add(s2).wrapping_add(s3);
        for &idx in &indices[chunks * 4..] {
            if !null_mask[idx] {
                sum = sum.wrapping_add(values[idx]);
            }
        }
        sum
    } else {
        for i in 0..chunks {
            let off = i * 4;
            s0 = s0.wrapping_add(values[indices[off]]);
            s1 = s1.wrapping_add(values[indices[off + 1]]);
            s2 = s2.wrapping_add(values[indices[off + 2]]);
            s3 = s3.wrapping_add(values[indices[off + 3]]);
        }
        let mut sum = s0.wrapping_add(s1).wrapping_add(s2).wrapping_add(s3);
        for &idx in &indices[chunks * 4..] {
            sum = sum.wrapping_add(values[idx]);
        }
        sum
    }
}

/// Sum f64 values at specific indices (unrolled for vectorization)
#[inline]
fn sum_f64_indexed(values: &[f64], indices: &[usize], nulls: Option<&[bool]>) -> f64 {
    let (mut s0, mut s1, mut s2, mut s3) = (0.0f64, 0.0f64, 0.0f64, 0.0f64);
    let chunks = indices.len() / 4;

    if let Some(null_mask) = nulls {
        for i in 0..chunks {
            let off = i * 4;
            let (i0, i1, i2, i3) =
                (indices[off], indices[off + 1], indices[off + 2], indices[off + 3]);
            if !null_mask[i0] {
                s0 += values[i0];
            }
            if !null_mask[i1] {
                s1 += values[i1];
            }
            if !null_mask[i2] {
                s2 += values[i2];
            }
            if !null_mask[i3] {
                s3 += values[i3];
            }
        }
        let mut sum = s0 + s1 + s2 + s3;
        for &idx in &indices[chunks * 4..] {
            if !null_mask[idx] {
                sum += values[idx];
            }
        }
        sum
    } else {
        for i in 0..chunks {
            let off = i * 4;
            s0 += values[indices[off]];
            s1 += values[indices[off + 1]];
            s2 += values[indices[off + 2]];
            s3 += values[indices[off + 3]];
        }
        let mut sum = s0 + s1 + s2 + s3;
        for &idx in &indices[chunks * 4..] {
            sum += values[idx];
        }
        sum
    }
}

/// Count non-null values at specific indices
#[inline]
fn count_indexed(indices: &[usize], nulls: Option<&[bool]>) -> usize {
    if let Some(null_mask) = nulls {
        indices.iter().filter(|&&idx| !null_mask[idx]).count()
    } else {
        indices.len()
    }
}

/// Min i64 value at specific indices
#[inline]
fn min_i64_indexed(values: &[i64], indices: &[usize], nulls: Option<&[bool]>) -> Option<i64> {
    let mut result = i64::MAX;
    let mut found = false;
    if let Some(null_mask) = nulls {
        for &idx in indices {
            if !null_mask[idx] {
                result = result.min(values[idx]);
                found = true;
            }
        }
    } else {
        for &idx in indices {
            result = result.min(values[idx]);
            found = true;
        }
    }
    if found {
        Some(result)
    } else {
        None
    }
}

/// Max i64 value at specific indices
#[inline]
fn max_i64_indexed(values: &[i64], indices: &[usize], nulls: Option<&[bool]>) -> Option<i64> {
    let mut result = i64::MIN;
    let mut found = false;
    if let Some(null_mask) = nulls {
        for &idx in indices {
            if !null_mask[idx] {
                result = result.max(values[idx]);
                found = true;
            }
        }
    } else {
        for &idx in indices {
            result = result.max(values[idx]);
            found = true;
        }
    }
    if found {
        Some(result)
    } else {
        None
    }
}

/// Min f64 value at specific indices
#[inline]
fn min_f64_indexed(values: &[f64], indices: &[usize], nulls: Option<&[bool]>) -> Option<f64> {
    let mut result = f64::INFINITY;
    let mut found = false;
    if let Some(null_mask) = nulls {
        for &idx in indices {
            if !null_mask[idx] {
                result = result.min(values[idx]);
                found = true;
            }
        }
    } else {
        for &idx in indices {
            result = result.min(values[idx]);
            found = true;
        }
    }
    if found {
        Some(result)
    } else {
        None
    }
}

/// Max f64 value at specific indices
#[inline]
fn max_f64_indexed(values: &[f64], indices: &[usize], nulls: Option<&[bool]>) -> Option<f64> {
    let mut result = f64::NEG_INFINITY;
    let mut found = false;
    if let Some(null_mask) = nulls {
        for &idx in indices {
            if !null_mask[idx] {
                result = result.max(values[idx]);
                found = true;
            }
        }
    } else {
        for &idx in indices {
            result = result.max(values[idx]);
            found = true;
        }
    }
    if found {
        Some(result)
    } else {
        None
    }
}

/// Compute aggregate using indices (O(group_size) instead of O(total_rows))
fn compute_group_aggregate_indexed(
    batch: &ColumnarBatch,
    col_idx: usize,
    op: AggregateOp,
    indices: &[usize],
) -> Result<SqlValue, ExecutorError> {
    let column = batch.column(col_idx).ok_or_else(|| ExecutorError::ColumnarColumnNotFound {
        column_index: col_idx,
        batch_columns: batch.column_count(),
    })?;

    match column {
        ColumnArray::Int64(values, nulls) => {
            let null_slice = nulls.as_ref().map(|v| v.as_slice());
            let count = count_indexed(indices, null_slice);
            if count == 0 {
                return Ok(match op {
                    AggregateOp::Count => SqlValue::Integer(0),
                    _ => SqlValue::Null,
                });
            }

            match op {
                AggregateOp::Sum => {
                    // SQLite's SUM() preserves integer type for integer inputs
                    Ok(SqlValue::Integer(sum_i64_indexed(values, indices, null_slice)))
                }
                AggregateOp::Count => Ok(SqlValue::Integer(count as i64)),
                AggregateOp::Avg => {
                    let sum = sum_i64_indexed(values, indices, null_slice);
                    Ok(SqlValue::Double(sum as f64 / count as f64))
                }
                AggregateOp::Min => min_i64_indexed(values, indices, null_slice)
                    .map(SqlValue::Integer)
                    .ok_or_else(|| ExecutorError::SimdOperationFailed {
                        operation: "MIN".to_string(),
                        reason: "empty group".to_string(),
                    }),
                AggregateOp::Max => max_i64_indexed(values, indices, null_slice)
                    .map(SqlValue::Integer)
                    .ok_or_else(|| ExecutorError::SimdOperationFailed {
                        operation: "MAX".to_string(),
                        reason: "empty group".to_string(),
                    }),
            }
        }

        ColumnArray::Float64(values, nulls) => {
            let null_slice = nulls.as_ref().map(|v| v.as_slice());
            let count = count_indexed(indices, null_slice);
            if count == 0 {
                return Ok(match op {
                    AggregateOp::Count => SqlValue::Integer(0),
                    _ => SqlValue::Null,
                });
            }

            match op {
                AggregateOp::Sum => {
                    // SQLite's SUM() always returns REAL (Numeric)
                    Ok(SqlValue::Numeric(sum_f64_indexed(values, indices, null_slice)))
                }
                AggregateOp::Count => Ok(SqlValue::Integer(count as i64)),
                AggregateOp::Avg => {
                    let sum = sum_f64_indexed(values, indices, null_slice);
                    Ok(SqlValue::Double(sum / count as f64))
                }
                AggregateOp::Min => min_f64_indexed(values, indices, null_slice)
                    .map(SqlValue::Double)
                    .ok_or_else(|| ExecutorError::SimdOperationFailed {
                        operation: "MIN".to_string(),
                        reason: "empty group".to_string(),
                    }),
                AggregateOp::Max => max_f64_indexed(values, indices, null_slice)
                    .map(SqlValue::Double)
                    .ok_or_else(|| ExecutorError::SimdOperationFailed {
                        operation: "MAX".to_string(),
                        reason: "empty group".to_string(),
                    }),
            }
        }

        ColumnArray::Mixed(values) => {
            // Fallback for mixed columns - iterate over indices directly
            // Helper to convert SqlValue to f64
            fn sqlvalue_to_f64(v: &SqlValue) -> Option<f64> {
                match v {
                    SqlValue::Integer(n) => Some(*n as f64),
                    SqlValue::Bigint(n) => Some(*n as f64),
                    SqlValue::Smallint(n) => Some(*n as f64),
                    SqlValue::Float(n) => Some(*n as f64),
                    SqlValue::Double(n) => Some(*n),
                    SqlValue::Numeric(n) => Some(*n),
                    SqlValue::Real(n) => Some(*n as f64),
                    _ => None,
                }
            }

            let count = indices.len();
            if count == 0 {
                return Ok(match op {
                    AggregateOp::Count => SqlValue::Integer(0),
                    _ => SqlValue::Null,
                });
            }

            match op {
                AggregateOp::Count => {
                    let non_null = indices.iter().filter(|&&i| !values[i].is_null()).count();
                    Ok(SqlValue::Integer(non_null as i64))
                }
                AggregateOp::Sum => {
                    let mut sum = 0.0f64;
                    for &idx in indices {
                        if let Some(v) = sqlvalue_to_f64(&values[idx]) {
                            sum += v;
                        }
                    }
                    Ok(SqlValue::Double(sum))
                }
                AggregateOp::Avg => {
                    let mut sum = 0.0f64;
                    let mut cnt = 0usize;
                    for &idx in indices {
                        if let Some(v) = sqlvalue_to_f64(&values[idx]) {
                            sum += v;
                            cnt += 1;
                        }
                    }
                    if cnt > 0 {
                        Ok(SqlValue::Double(sum / cnt as f64))
                    } else {
                        Ok(SqlValue::Null)
                    }
                }
                AggregateOp::Min => {
                    let mut min_val: Option<f64> = None;
                    for &idx in indices {
                        if let Some(v) = sqlvalue_to_f64(&values[idx]) {
                            min_val = Some(min_val.map_or(v, |m| m.min(v)));
                        }
                    }
                    Ok(min_val.map(SqlValue::Double).unwrap_or(SqlValue::Null))
                }
                AggregateOp::Max => {
                    let mut max_val: Option<f64> = None;
                    for &idx in indices {
                        if let Some(v) = sqlvalue_to_f64(&values[idx]) {
                            max_val = Some(max_val.map_or(v, |m| m.max(v)));
                        }
                    }
                    Ok(max_val.map(SqlValue::Double).unwrap_or(SqlValue::Null))
                }
            }
        }

        // String columns - only COUNT is supported
        ColumnArray::String(_values, nulls) => {
            let null_slice = nulls.as_ref().map(|v| v.as_slice());
            let count = count_indexed(indices, null_slice);

            match op {
                AggregateOp::Count => Ok(SqlValue::Integer(count as i64)),
                _ => Err(ExecutorError::UnsupportedExpression(format!(
                    "Aggregate operation {:?} not supported for String column type",
                    op
                ))),
            }
        }

        // FixedString columns - only COUNT is supported
        ColumnArray::FixedString(_values, nulls) => {
            let null_slice = nulls.as_ref().map(|v| v.as_slice());
            let count = count_indexed(indices, null_slice);

            match op {
                AggregateOp::Count => Ok(SqlValue::Integer(count as i64)),
                _ => Err(ExecutorError::UnsupportedExpression(format!(
                    "Aggregate operation {:?} not supported for FixedString column type",
                    op
                ))),
            }
        }

        // Boolean columns - only COUNT is supported
        ColumnArray::Boolean(_values, nulls) => {
            let null_slice = nulls.as_ref().map(|v| v.as_slice());
            let count = count_indexed(indices, null_slice);

            match op {
                AggregateOp::Count => Ok(SqlValue::Integer(count as i64)),
                _ => Err(ExecutorError::UnsupportedExpression(format!(
                    "Aggregate operation {:?} not supported for Boolean column type",
                    op
                ))),
            }
        }

        // Int32 columns - support all aggregate operations
        ColumnArray::Int32(values, nulls) => {
            let null_slice = nulls.as_ref().map(|v| v.as_slice());
            let count = count_indexed(indices, null_slice);
            if count == 0 {
                return Ok(match op {
                    AggregateOp::Count => SqlValue::Integer(0),
                    _ => SqlValue::Null,
                });
            }

            match op {
                AggregateOp::Count => Ok(SqlValue::Integer(count as i64)),
                AggregateOp::Sum => {
                    // SQLite's SUM() preserves integer type for integer inputs
                    let sum: i64 = indices
                        .iter()
                        .filter(|&&i| null_slice.is_none_or(|ns| !ns[i]))
                        .map(|&i| values[i] as i64)
                        .sum();
                    Ok(SqlValue::Integer(sum))
                }
                AggregateOp::Avg => {
                    let sum: i64 = indices
                        .iter()
                        .filter(|&&i| null_slice.is_none_or(|ns| !ns[i]))
                        .map(|&i| values[i] as i64)
                        .sum();
                    Ok(SqlValue::Double(sum as f64 / count as f64))
                }
                AggregateOp::Min => indices
                    .iter()
                    .filter(|&&i| null_slice.is_none_or(|ns| !ns[i]))
                    .map(|&i| values[i])
                    .min()
                    .map(|v| SqlValue::Integer(v as i64))
                    .ok_or_else(|| ExecutorError::SimdOperationFailed {
                        operation: "MIN".to_string(),
                        reason: "empty group".to_string(),
                    }),
                AggregateOp::Max => indices
                    .iter()
                    .filter(|&&i| null_slice.is_none_or(|ns| !ns[i]))
                    .map(|&i| values[i])
                    .max()
                    .map(|v| SqlValue::Integer(v as i64))
                    .ok_or_else(|| ExecutorError::SimdOperationFailed {
                        operation: "MAX".to_string(),
                        reason: "empty group".to_string(),
                    }),
            }
        }

        // Float32 columns - support all aggregate operations
        ColumnArray::Float32(values, nulls) => {
            let null_slice = nulls.as_ref().map(|v| v.as_slice());
            let count = count_indexed(indices, null_slice);
            if count == 0 {
                return Ok(match op {
                    AggregateOp::Count => SqlValue::Integer(0),
                    _ => SqlValue::Null,
                });
            }

            match op {
                AggregateOp::Count => Ok(SqlValue::Integer(count as i64)),
                AggregateOp::Sum => {
                    let sum: f64 = indices
                        .iter()
                        .filter(|&&i| null_slice.is_none_or(|ns| !ns[i]))
                        .map(|&i| values[i] as f64)
                        .sum();
                    Ok(SqlValue::Double(sum))
                }
                AggregateOp::Avg => {
                    let sum: f64 = indices
                        .iter()
                        .filter(|&&i| null_slice.is_none_or(|ns| !ns[i]))
                        .map(|&i| values[i] as f64)
                        .sum();
                    Ok(SqlValue::Double(sum / count as f64))
                }
                AggregateOp::Min => indices
                    .iter()
                    .filter(|&&i| null_slice.is_none_or(|ns| !ns[i]))
                    .map(|&i| values[i])
                    .min_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
                    .map(|v| SqlValue::Double(v as f64))
                    .ok_or_else(|| ExecutorError::SimdOperationFailed {
                        operation: "MIN".to_string(),
                        reason: "empty group".to_string(),
                    }),
                AggregateOp::Max => indices
                    .iter()
                    .filter(|&&i| null_slice.is_none_or(|ns| !ns[i]))
                    .map(|&i| values[i])
                    .max_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal))
                    .map(|v| SqlValue::Double(v as f64))
                    .ok_or_else(|| ExecutorError::SimdOperationFailed {
                        operation: "MAX".to_string(),
                        reason: "empty group".to_string(),
                    }),
            }
        }

        // Date columns - only COUNT is supported (MIN/MAX would need i32-to-Date conversion)
        ColumnArray::Date(_values, nulls) => {
            let null_slice = nulls.as_ref().map(|v| v.as_slice());
            let count = count_indexed(indices, null_slice);

            match op {
                AggregateOp::Count => Ok(SqlValue::Integer(count as i64)),
                _ => Err(ExecutorError::UnsupportedExpression(format!(
                    "Aggregate operation {:?} not supported for Date column type in GROUP BY",
                    op
                ))),
            }
        }

        // Timestamp columns - only COUNT is supported
        ColumnArray::Timestamp(_values, nulls) => {
            let null_slice = nulls.as_ref().map(|v| v.as_slice());
            let count = count_indexed(indices, null_slice);

            match op {
                AggregateOp::Count => Ok(SqlValue::Integer(count as i64)),
                _ => Err(ExecutorError::UnsupportedExpression(format!(
                    "Aggregate operation {:?} not supported for Timestamp column type in GROUP BY",
                    op
                ))),
            }
        }
    }
}

/// Compute aggregates with GROUP BY using columnar execution
///
/// This function implements hash-based grouping on columnar data, enabling
/// TPC-H Q1 and similar queries to use the columnar execution path.
///
/// # Algorithm
///
/// 1. Build hash table mapping group keys → (row indices in that group)
/// 2. For each group, compute aggregates over the grouped rows
/// 3. Return results as rows with (group_key_cols, aggregate_cols)
///
/// # Arguments
///
/// * `rows` - Input rows to group and aggregate
/// * `group_cols` - Indices of columns to group by
/// * `agg_cols` - List of (column_index, aggregate_op) pairs to compute
/// * `filter_bitmap` - Optional filter to apply before grouping
///
/// # Returns
///
/// Vec of Row objects, each containing group key values followed by aggregate results
///
/// # Example
///
/// ```text
/// // SELECT l_returnflag, SUM(l_extendedprice)
/// // FROM lineitem
/// // GROUP BY l_returnflag
///
/// let rows = vec![
///     Row::new(vec![SqlValue::Varchar(arcstr::ArcStr::from("A")), SqlValue::Double(100.0)]),
///     Row::new(vec![SqlValue::Varchar(arcstr::ArcStr::from("B")), SqlValue::Double(200.0)]),
///     Row::new(vec![SqlValue::Varchar(arcstr::ArcStr::from("A")), SqlValue::Double(150.0)]),
/// ];
///
/// let group_cols = vec![0]; // Group by first column (l_returnflag)
/// let agg_cols = vec![(1, AggregateOp::Sum)]; // SUM(l_extendedprice)
///
/// let result = columnar_group_by(&rows, &group_cols, &agg_cols, None)?;
/// // Returns:
/// // Row["A", 250.0]
/// // Row["B", 200.0]
/// ```
pub fn columnar_group_by(
    rows: &[Row],
    group_cols: &[usize],
    agg_cols: &[(usize, AggregateOp)],
    filter_bitmap: Option<&[bool]>,
) -> Result<Vec<Row>, ExecutorError> {
    // Early return for empty input
    if rows.is_empty() {
        return Ok(Vec::new());
    }

    // Create columnar scan for efficient column access
    let scan = ColumnarScan::new(rows);

    // Phase 1: Build hash table mapping group keys to row indices
    // AHashMap<Vec<SqlValue>, Vec<usize>> - faster hashing
    // Key: group key values, Value: indices of rows in that group
    let mut groups: AHashMap<Vec<SqlValue>, Vec<usize>> = AHashMap::new();

    for row_idx in 0..rows.len() {
        // Check filter bitmap
        if let Some(bitmap) = filter_bitmap {
            if !bitmap.get(row_idx).copied().unwrap_or(false) {
                continue;
            }
        }

        // Extract group key values for this row
        let mut group_key = Vec::with_capacity(group_cols.len());
        for &col_idx in group_cols {
            let value =
                scan.row(row_idx).and_then(|row| row.get(col_idx)).unwrap_or(&SqlValue::Null);
            group_key.push(value.clone());
        }

        // Add row index to this group
        groups.entry(group_key).or_default().push(row_idx);
    }

    // Phase 2: Compute aggregates for each group
    let mut result_rows = Vec::with_capacity(groups.len());

    // Reuse a single bitmap buffer to avoid repeated allocations
    // This is much more efficient than allocating rows.len() booleans per group
    let mut group_bitmap = vec![false; rows.len()];

    for (group_key, row_indices) in groups {
        // Set bits for this group's rows
        for &idx in &row_indices {
            group_bitmap[idx] = true;
        }

        // Compute aggregates for this group
        let mut result_values = Vec::with_capacity(group_key.len() + agg_cols.len());

        // First, add group key values
        result_values.extend(group_key);

        // Then, compute each aggregate
        for (col_idx, agg_op) in agg_cols {
            let agg_result =
                compute_columnar_aggregate_impl(&scan, *col_idx, *agg_op, Some(&group_bitmap))?;
            result_values.push(agg_result);
        }

        result_rows.push(Row::new(result_values));

        // Clear bitmap for next group (faster than allocating a new one)
        for &idx in &row_indices {
            group_bitmap[idx] = false;
        }
    }

    // Sort by group key for deterministic ordering (SQLite compatibility).
    // AHashMap iteration order is randomized per process; the row-based path
    // (select/grouping/hash.rs) already sorts by the GROUP BY columns, so the
    // columnar path must match (issue #5291, windowpushd.test 2.x.4.1).
    sort_rows_by_group_key_prefix(&mut result_rows, group_cols.len());

    Ok(result_rows)
}

/// Sort GROUP BY result rows by their leading group-key columns.
///
/// SQLite returns GROUP BY results sorted by the GROUP BY columns (in the
/// absence of an explicit ORDER BY). Result rows are laid out as
/// (group_key_cols..., aggregate_cols...), so comparing the first
/// `group_key_len` values matches the comparator used by the row-based
/// grouping path in `select/grouping/hash.rs`.
fn sort_rows_by_group_key_prefix(rows: &mut [Row], group_key_len: usize) {
    if group_key_len == 0 {
        return;
    }
    rows.sort_by(|a, b| {
        let a_key = &a.values[..group_key_len.min(a.values.len())];
        let b_key = &b.values[..group_key_len.min(b.values.len())];
        a_key.cmp(b_key)
    });
}

/// Compute aggregates with GROUP BY using SIMD-accelerated columnar execution
///
/// This is the high-performance version that works directly on `ColumnarBatch`
/// with typed column arrays and SIMD aggregation. Use this when data is already
/// in columnar format for maximum performance.
///
/// # Algorithm
///
/// 1. Analyze column types and select specialized key strategy
/// 2. Build hash table mapping specialized group keys → row indices
/// 3. For each group, use SIMD masked aggregation on typed arrays
/// 4. Return results as rows with (group_key_cols, aggregate_cols)
///
/// # Performance
///
/// - **Specialized key hashing**: Uses `GroupKey` enum for efficient hashing
///   - `TwoChars(u8, u8)` for TPC-H Q1 (l_returnflag, l_linestatus)
///   - `SingleI64(i64)` for single integer GROUP BY
///   - `TwoI64(i64, i64)` for two integer columns
/// - Uses auto-vectorized SIMD for per-group aggregation (SUM, MIN, MAX)
/// - Avoids row materialization within groups
/// - Direct typed array access (no SqlValue pattern matching in hot path)
/// - Reuses buffers to minimize heap allocations
/// - Provides 3-5x improvement over scalar GROUP BY for TPC-H Q1
///
/// # Arguments
///
/// * `batch` - Input ColumnarBatch to group and aggregate
/// * `group_cols` - Indices of columns to group by
/// * `agg_cols` - List of (column_index, aggregate_op) pairs to compute
///
/// # Returns
///
/// Vec of Row objects, each containing group key values followed by aggregate results
pub fn columnar_group_by_batch(
    batch: &ColumnarBatch,
    group_cols: &[usize],
    agg_cols: &[(usize, AggregateOp)],
) -> Result<Vec<Row>, ExecutorError> {
    // Early return for empty input
    if batch.row_count() == 0 {
        return Ok(Vec::new());
    }

    let row_count = batch.row_count();

    // Analyze column types and select optimal key strategy
    let key_spec = GroupKeySpec::from_columnar_batch(batch, group_cols);

    // Phase 1: Build hash table mapping specialized group keys to row indices
    // Using GroupKey instead of Vec<SqlValue> eliminates per-row allocations
    // and provides efficient hashing for common patterns (e.g., TwoChars for Q1)
    let mut groups: AHashMap<GroupKey, Vec<usize>> = AHashMap::new();

    for row_idx in 0..row_count {
        // Extract group key using specialized strategy (no allocation for primitive keys)
        let group_key = key_spec.extract_key_from_batch(batch, row_idx);

        // Add row index to this group
        groups.entry(group_key).or_default().push(row_idx);
    }

    // Phase 2: Compute aggregates for each group using index-based access
    // This is O(total_rows) instead of O(total_rows * groups) for the masked approach
    let mut result_rows = Vec::with_capacity(groups.len());

    for (group_key, row_indices) in groups {
        // Convert GroupKey back to Vec<SqlValue> for result row
        let mut result_values = key_spec.key_to_values(&group_key);

        // Compute each aggregate using direct index access (no bitmap scans)
        for (col_idx, agg_op) in agg_cols {
            let agg_result =
                compute_group_aggregate_indexed(batch, *col_idx, *agg_op, &row_indices)?;
            result_values.push(agg_result);
        }

        result_rows.push(Row::new(result_values));
    }

    // Sort by group key for deterministic ordering (SQLite compatibility).
    // See sort_rows_by_group_key_prefix and issue #5291.
    sort_rows_by_group_key_prefix(&mut result_rows, group_cols.len());

    Ok(result_rows)
}

#[cfg(test)]
mod batch_tests {
    use super::*;

    fn make_test_batch() -> ColumnarBatch {
        // Create batch with group key and values
        // group_key: [A, B, A, B, A]
        // values:    [10, 20, 30, 40, 50]
        let rows = vec![
            Row::new(vec![SqlValue::Varchar(arcstr::ArcStr::from("A")), SqlValue::Integer(10)]),
            Row::new(vec![SqlValue::Varchar(arcstr::ArcStr::from("B")), SqlValue::Integer(20)]),
            Row::new(vec![SqlValue::Varchar(arcstr::ArcStr::from("A")), SqlValue::Integer(30)]),
            Row::new(vec![SqlValue::Varchar(arcstr::ArcStr::from("B")), SqlValue::Integer(40)]),
            Row::new(vec![SqlValue::Varchar(arcstr::ArcStr::from("A")), SqlValue::Integer(50)]),
        ];
        ColumnarBatch::from_rows(&rows).unwrap()
    }

    #[test]
    fn test_columnar_group_by_batch_sum() {
        let batch = make_test_batch();
        let group_cols = vec![0]; // Group by first column
        let agg_cols = vec![(1, AggregateOp::Sum)]; // SUM(second column)

        let result = columnar_group_by_batch(&batch, &group_cols, &agg_cols).unwrap();

        assert_eq!(result.len(), 2); // Two groups: A and B

        // Sort by group key for deterministic testing
        let mut sorted = result;
        sorted.sort_by(|a, b| a.get(0).unwrap().partial_cmp(b.get(0).unwrap()).unwrap());

        // Group A: 10 + 30 + 50 = 90
        assert_eq!(sorted[0].get(0), Some(&SqlValue::Varchar(arcstr::ArcStr::from("A"))));
        assert_eq!(sorted[0].get(1), Some(&SqlValue::Integer(90)));

        // Group B: 20 + 40 = 60
        assert_eq!(sorted[1].get(0), Some(&SqlValue::Varchar(arcstr::ArcStr::from("B"))));
        assert_eq!(sorted[1].get(1), Some(&SqlValue::Integer(60)));
    }

    #[test]
    fn test_columnar_group_by_batch_avg() {
        let batch = make_test_batch();
        let group_cols = vec![0];
        let agg_cols = vec![(1, AggregateOp::Avg)];

        let result = columnar_group_by_batch(&batch, &group_cols, &agg_cols).unwrap();

        let mut sorted = result;
        sorted.sort_by(|a, b| a.get(0).unwrap().partial_cmp(b.get(0).unwrap()).unwrap());

        // Group A: (10 + 30 + 50) / 3 = 30.0
        assert_eq!(sorted[0].get(0), Some(&SqlValue::Varchar(arcstr::ArcStr::from("A"))));
        if let Some(SqlValue::Double(avg)) = sorted[0].get(1) {
            assert!((avg - 30.0).abs() < 0.001);
        } else {
            panic!("Expected Double for AVG");
        }

        // Group B: (20 + 40) / 2 = 30.0
        assert_eq!(sorted[1].get(0), Some(&SqlValue::Varchar(arcstr::ArcStr::from("B"))));
        if let Some(SqlValue::Double(avg)) = sorted[1].get(1) {
            assert!((avg - 30.0).abs() < 0.001);
        } else {
            panic!("Expected Double for AVG");
        }
    }

    #[test]
    fn test_columnar_group_by_batch_min_max() {
        let batch = make_test_batch();
        let group_cols = vec![0];
        let agg_cols = vec![(1, AggregateOp::Min), (1, AggregateOp::Max)];

        let result = columnar_group_by_batch(&batch, &group_cols, &agg_cols).unwrap();

        let mut sorted = result;
        sorted.sort_by(|a, b| a.get(0).unwrap().partial_cmp(b.get(0).unwrap()).unwrap());

        // Group A: min=10, max=50
        assert_eq!(sorted[0].get(1), Some(&SqlValue::Integer(10)));
        assert_eq!(sorted[0].get(2), Some(&SqlValue::Integer(50)));

        // Group B: min=20, max=40
        assert_eq!(sorted[1].get(1), Some(&SqlValue::Integer(20)));
        assert_eq!(sorted[1].get(2), Some(&SqlValue::Integer(40)));
    }

    #[test]
    fn test_columnar_group_by_batch_count() {
        let batch = make_test_batch();
        let group_cols = vec![0];
        let agg_cols = vec![(1, AggregateOp::Count)];

        let result = columnar_group_by_batch(&batch, &group_cols, &agg_cols).unwrap();

        let mut sorted = result;
        sorted.sort_by(|a, b| a.get(0).unwrap().partial_cmp(b.get(0).unwrap()).unwrap());

        // Group A: 3 rows
        assert_eq!(sorted[0].get(1), Some(&SqlValue::Integer(3)));

        // Group B: 2 rows
        assert_eq!(sorted[1].get(1), Some(&SqlValue::Integer(2)));
    }

    #[test]
    fn test_columnar_group_by_batch_empty() {
        let batch = ColumnarBatch::new(2);
        let group_cols = vec![0];
        let agg_cols = vec![(1, AggregateOp::Sum)];

        let result = columnar_group_by_batch(&batch, &group_cols, &agg_cols).unwrap();
        assert_eq!(result.len(), 0);
    }

    /// Rows with enough distinct group keys to expose hash-iteration-order
    /// nondeterminism (issue #5291). Keys inserted in non-sorted order.
    fn make_unsorted_key_rows() -> Vec<Row> {
        let keys = ["W", "Z", "Q", "A", "M", "X", "B", "Y", "K", "C"];
        let mut rows = Vec::new();
        for (i, key) in keys.iter().enumerate() {
            // Two rows per group
            rows.push(Row::new(vec![
                SqlValue::Varchar(arcstr::ArcStr::from(*key)),
                SqlValue::Integer(i as i64),
            ]));
            rows.push(Row::new(vec![
                SqlValue::Varchar(arcstr::ArcStr::from(*key)),
                SqlValue::Integer((i * 10) as i64),
            ]));
        }
        rows
    }

    fn assert_sorted_by_first_col(result: &[Row]) {
        for pair in result.windows(2) {
            assert!(
                pair[0].values[0] <= pair[1].values[0],
                "result not sorted by group key: {:?} > {:?}",
                pair[0].values[0],
                pair[1].values[0]
            );
        }
    }

    /// Issue #5291: columnar_group_by must emit rows sorted by group key,
    /// deterministically across repeated invocations (AHashMap iteration
    /// order is randomized per map instance).
    #[test]
    fn test_columnar_group_by_deterministic_key_order() {
        let rows = make_unsorted_key_rows();
        let group_cols = vec![0];
        let agg_cols = vec![(1, AggregateOp::Sum)];

        let first = columnar_group_by(&rows, &group_cols, &agg_cols, None).unwrap();
        assert_eq!(first.len(), 10);
        assert_sorted_by_first_col(&first);

        for run in 0..10 {
            let again = columnar_group_by(&rows, &group_cols, &agg_cols, None).unwrap();
            assert_eq!(
                again.iter().map(|r| r.values.to_vec()).collect::<Vec<_>>(),
                first.iter().map(|r| r.values.to_vec()).collect::<Vec<_>>(),
                "run {} produced different row order",
                run
            );
        }
    }

    /// Issue #5291: same determinism guarantee for the SIMD batch path.
    #[test]
    fn test_columnar_group_by_batch_deterministic_key_order() {
        let rows = make_unsorted_key_rows();
        let batch = ColumnarBatch::from_rows(&rows).unwrap();
        let group_cols = vec![0];
        let agg_cols = vec![(1, AggregateOp::Sum)];

        let first = columnar_group_by_batch(&batch, &group_cols, &agg_cols).unwrap();
        assert_eq!(first.len(), 10);
        assert_sorted_by_first_col(&first);

        for run in 0..10 {
            let again = columnar_group_by_batch(&batch, &group_cols, &agg_cols).unwrap();
            assert_eq!(
                again.iter().map(|r| r.values.to_vec()).collect::<Vec<_>>(),
                first.iter().map(|r| r.values.to_vec()).collect::<Vec<_>>(),
                "run {} produced different row order",
                run
            );
        }
    }

    /// Issue #5995: a derived grouping key materialized as a `ColumnArray::Mixed`
    /// (the output of `materialize_derived_column`) groups correctly through the
    /// `GroupKeySpec::Generic` path. NULL keys collapse into a single group and
    /// numeric key values hash/compare via `SqlValue` exactly as the row path's
    /// `Vec<SqlValue>` group-key map does.
    #[test]
    fn test_columnar_group_by_batch_mixed_derived_key() {
        use std::sync::Arc;

        // A base value column (index 0) and a derived Mixed key column (index 1).
        // Derived keys: [2, NULL, 2, NULL, 5, 2] -> groups: {2:[0,2,5], NULL:[1,3], 5:[4]}
        let value_col = ColumnArray::Int64(Arc::new(vec![10, 20, 30, 40, 50, 60]), None);
        let key_col = ColumnArray::Mixed(Arc::new(vec![
            SqlValue::Integer(2),
            SqlValue::Null,
            SqlValue::Integer(2),
            SqlValue::Null,
            SqlValue::Integer(5),
            SqlValue::Integer(2),
        ]));
        let mut batch = ColumnarBatch::new(2);
        batch.add_column(value_col).unwrap();
        batch.add_column(key_col).unwrap();

        let group_cols = vec![1]; // group by the derived Mixed key
        let agg_cols = vec![(0, AggregateOp::Sum)];
        let result = columnar_group_by_batch(&batch, &group_cols, &agg_cols).unwrap();

        // Three groups: key=2 (10+30+60=100), key=5 (50), key=NULL (20+40=60).
        assert_eq!(result.len(), 3, "expected 3 groups (2, 5, NULL)");

        // Find each group by its key value and check the aggregate.
        let find = |k: &SqlValue| -> Option<i64> {
            result.iter().find(|r| &r.values[0] == k).map(|r| match &r.values[1] {
                SqlValue::Integer(v) => *v,
                other => panic!("unexpected agg value: {other:?}"),
            })
        };
        assert_eq!(find(&SqlValue::Integer(2)), Some(100), "group key=2 sum");
        assert_eq!(find(&SqlValue::Integer(5)), Some(50), "group key=5 sum");
        assert_eq!(find(&SqlValue::Null), Some(60), "NULL keys form one group");
    }

    /// Issue #5995: an integer key (`Integer(3)`) and a float key (`Double(3.0)`)
    /// in a derived Mixed column group according to `SqlValue` hash/eq — the same
    /// semantics the row path's group map uses. This asserts the batch path is
    /// consistent with a direct `Vec<SqlValue>` (row-path) grouping of the same
    /// key values, so the two paths never diverge on int-vs-double keys.
    #[test]
    fn test_columnar_group_by_batch_mixed_int_vs_double_key_matches_row_path() {
        use std::sync::Arc;

        let keys = vec![
            SqlValue::Integer(3),
            SqlValue::Double(3.0),
            SqlValue::Integer(3),
            SqlValue::Double(4.0),
        ];
        let vals = vec![1i64, 2, 4, 8];

        // Batch path via Mixed derived key column.
        let value_col = ColumnArray::Int64(Arc::new(vals.clone()), None);
        let key_col = ColumnArray::Mixed(Arc::new(keys.clone()));
        let mut batch = ColumnarBatch::new(2);
        batch.add_column(value_col).unwrap();
        batch.add_column(key_col).unwrap();
        let batch_result = columnar_group_by_batch(&batch, &[1], &[(0, AggregateOp::Sum)]).unwrap();

        // Reference: group the same keys via a plain Vec<SqlValue> map (this is
        // exactly how the row path keys its groups).
        let mut ref_map: AHashMap<Vec<SqlValue>, i64> = AHashMap::new();
        for (k, v) in keys.iter().zip(vals.iter()) {
            *ref_map.entry(vec![k.clone()]).or_insert(0) += v;
        }

        assert_eq!(
            batch_result.len(),
            ref_map.len(),
            "batch group count must match row-path (Vec<SqlValue>) group count"
        );
        for row in &batch_result {
            let key = vec![row.values[0].clone()];
            let expected = ref_map.get(&key).unwrap_or_else(|| {
                panic!("batch produced a group {key:?} the row-path map does not have")
            });
            let got = match &row.values[1] {
                SqlValue::Integer(v) => *v,
                other => panic!("unexpected agg value: {other:?}"),
            };
            assert_eq!(got, *expected, "sum mismatch for key {key:?}");
        }
    }

    /// Multi-column group keys sort lexicographically by the full key prefix.
    #[test]
    fn test_columnar_group_by_multi_key_order() {
        let rows = vec![
            Row::new(vec![
                SqlValue::Varchar(arcstr::ArcStr::from("B")),
                SqlValue::Integer(2),
                SqlValue::Integer(1),
            ]),
            Row::new(vec![
                SqlValue::Varchar(arcstr::ArcStr::from("A")),
                SqlValue::Integer(2),
                SqlValue::Integer(2),
            ]),
            Row::new(vec![
                SqlValue::Varchar(arcstr::ArcStr::from("B")),
                SqlValue::Integer(1),
                SqlValue::Integer(3),
            ]),
            Row::new(vec![
                SqlValue::Varchar(arcstr::ArcStr::from("A")),
                SqlValue::Integer(1),
                SqlValue::Integer(4),
            ]),
        ];
        let group_cols = vec![0, 1];
        let agg_cols = vec![(2, AggregateOp::Sum)];

        let result = columnar_group_by(&rows, &group_cols, &agg_cols, None).unwrap();
        let keys: Vec<(String, i64)> = result
            .iter()
            .map(|r| {
                let s = match &r.values[0] {
                    SqlValue::Varchar(s) => s.to_string(),
                    other => panic!("unexpected key type: {:?}", other),
                };
                let n = match &r.values[1] {
                    SqlValue::Integer(n) => *n,
                    other => panic!("unexpected key type: {:?}", other),
                };
                (s, n)
            })
            .collect();
        assert_eq!(
            keys,
            vec![
                ("A".to_string(), 1),
                ("A".to_string(), 2),
                ("B".to_string(), 1),
                ("B".to_string(), 2),
            ]
        );
    }
}
