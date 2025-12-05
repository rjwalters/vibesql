//! Auto-vectorized columnar hash join implementation
//!
//! This module implements hash join operations using columnar data format and
//! auto-vectorized operations for improved performance over row-based joins.
//!
//! ## Algorithm
//!
//! 1. **Build Phase**: Create hash table from right (build) batch
//!    - Extract key column values
//!    - Build hashmap: key → Vec<row_indices>
//!    - Handle NULLs (NULL keys never match)
//!
//! 2. **Probe Phase**: Scan left (probe) batch using vectorized equality
//!    - For each key value, find all matching rows
//!    - Build list of (left_row, right_row) pairs
//!
//! 3. **Materialize**: Construct output batch from matched pairs
//!    - Combine columns from both sides
//!    - Return columnar batch

use std::sync::Arc;

use super::simd_ops;
use super::{ColumnArray, ColumnarBatch};
use crate::errors::ExecutorError;
use std::collections::HashMap;

// Re-export comparison functions from simd_ops module for consistency

#[inline]
fn simd_eq_i64(values: &[i64], target: i64) -> Vec<bool> {
    simd_ops::eq_i64(values, target)
}

#[inline]
fn simd_eq_f64(values: &[f64], target: f64) -> Vec<bool> {
    simd_ops::eq_f64(values, target)
}

/// Hash table for join operations
///
/// Maps join key values to lists of row indices in the build-side batch.
/// Supports both integer and float keys.
#[derive(Debug)]
enum HashTable {
    Int64(HashMap<i64, Vec<usize>>),
    Float64(HashMap<OrderedFloat, Vec<usize>>),
}

/// Wrapper for f64 that implements Hash and Eq for use in HashMap
///
/// Uses bitwise comparison so that NaN != NaN (SQL semantics)
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
struct OrderedFloat(u64);

impl OrderedFloat {
    fn new(f: f64) -> Self {
        Self(f.to_bits())
    }

    fn to_f64(self) -> f64 {
        f64::from_bits(self.0)
    }
}

impl HashTable {
    /// Build hash table from right batch's key column
    fn build(right: &ColumnarBatch, right_key_col: usize) -> Result<Self, ExecutorError> {
        let key_column =
            right.column(right_key_col).ok_or_else(|| ExecutorError::ColumnarColumnNotFound {
                column_index: right_key_col,
                batch_columns: right.column_count(),
            })?;

        match key_column {
            ColumnArray::Int64(values, nulls) => {
                let mut table = HashMap::new();

                for (row_idx, &value) in values.iter().enumerate() {
                    // Skip NULL keys (NULL != NULL in SQL)
                    if let Some(null_mask) = nulls {
                        if null_mask[row_idx] {
                            continue;
                        }
                    }

                    table.entry(value).or_insert_with(Vec::new).push(row_idx);
                }

                Ok(HashTable::Int64(table))
            }
            ColumnArray::Float64(values, nulls) => {
                let mut table = HashMap::new();

                for (row_idx, &value) in values.iter().enumerate() {
                    // Skip NULL keys and NaN values
                    if let Some(null_mask) = nulls {
                        if null_mask[row_idx] {
                            continue;
                        }
                    }
                    if value.is_nan() {
                        continue; // NaN != NaN in SQL
                    }

                    table.entry(OrderedFloat::new(value)).or_insert_with(Vec::new).push(row_idx);
                }

                Ok(HashTable::Float64(table))
            }
            _ => Err(ExecutorError::UnsupportedArrayType {
                operation: "columnar hash join".to_string(),
                array_type: format!("{:?}", std::mem::discriminant(key_column)),
            }),
        }
    }

    /// Probe hash table with left batch's key column using SIMD
    ///
    /// Returns a list of (left_row_idx, right_row_idx) pairs for all matches
    fn probe_simd(
        &self,
        left: &ColumnarBatch,
        left_key_col: usize,
    ) -> Result<Vec<(usize, usize)>, ExecutorError> {
        let key_column =
            left.column(left_key_col).ok_or_else(|| ExecutorError::ColumnarColumnNotFound {
                column_index: left_key_col,
                batch_columns: left.column_count(),
            })?;

        match (self, key_column) {
            (HashTable::Int64(table), ColumnArray::Int64(left_values, left_nulls)) => {
                Self::probe_i64_simd(table, left_values, left_nulls.as_ref().map(|n| n.as_slice()))
            }
            (HashTable::Float64(table), ColumnArray::Float64(left_values, left_nulls)) => {
                Self::probe_f64_simd(table, left_values, left_nulls.as_ref().map(|n| n.as_slice()))
            }
            _ => Err(ExecutorError::ColumnarTypeMismatch {
                operation: "hash join probe".to_string(),
                left_type: format!("{:?}", std::mem::discriminant(key_column)),
                right_type: None,
            }),
        }
    }

    /// Probe Int64 hash table using SIMD equality
    fn probe_i64_simd(
        table: &HashMap<i64, Vec<usize>>,
        left_values: &[i64],
        left_nulls: Option<&[bool]>,
    ) -> Result<Vec<(usize, usize)>, ExecutorError> {
        let mut matches = Vec::new();

        // Get all unique keys from hash table
        let table_keys: Vec<i64> = table.keys().copied().collect();

        // For each unique key, use SIMD to find all matching rows in left batch
        for &key in &table_keys {
            let equality_mask = simd_eq_i64(left_values, key);

            for (left_row, &is_match) in equality_mask.iter().enumerate() {
                if !is_match {
                    continue;
                }

                // Skip if left key is NULL
                if let Some(null_mask) = left_nulls {
                    if null_mask[left_row] {
                        continue;
                    }
                }

                // Add all right rows that match this key
                if let Some(right_rows) = table.get(&key) {
                    for &right_row in right_rows {
                        matches.push((left_row, right_row));
                    }
                }
            }
        }

        Ok(matches)
    }

    /// Probe Float64 hash table using SIMD equality
    fn probe_f64_simd(
        table: &HashMap<OrderedFloat, Vec<usize>>,
        left_values: &[f64],
        left_nulls: Option<&[bool]>,
    ) -> Result<Vec<(usize, usize)>, ExecutorError> {
        let mut matches = Vec::new();

        // Get all unique keys from hash table (as f64)
        let table_keys: Vec<f64> = table.keys().map(|k| k.to_f64()).collect();

        // For each unique key, use SIMD to find all matching rows in left batch
        for &key in &table_keys {
            let equality_mask = simd_eq_f64(left_values, key);

            for (left_row, &is_match) in equality_mask.iter().enumerate() {
                if !is_match {
                    continue;
                }

                // Skip if left key is NULL or NaN
                if let Some(null_mask) = left_nulls {
                    if null_mask[left_row] {
                        continue;
                    }
                }
                if left_values[left_row].is_nan() {
                    continue;
                }

                // Add all right rows that match this key
                let ordered_key = OrderedFloat::new(key);
                if let Some(right_rows) = table.get(&ordered_key) {
                    for &right_row in right_rows {
                        matches.push((left_row, right_row));
                    }
                }
            }
        }

        Ok(matches)
    }
}

/// Materialize join results into output columnar batch
///
/// Combines columns from left and right batches according to the match pairs.
fn materialize_join_results(
    left: &ColumnarBatch,
    right: &ColumnarBatch,
    matches: Vec<(usize, usize)>,
) -> Result<ColumnarBatch, ExecutorError> {
    let output_row_count = matches.len();
    let output_col_count = left.column_count() + right.column_count();

    let mut output = ColumnarBatch::with_capacity(output_row_count, output_col_count);

    // Process left columns
    for col_idx in 0..left.column_count() {
        let left_col =
            left.column(col_idx).ok_or_else(|| ExecutorError::ColumnarColumnNotFound {
                column_index: col_idx,
                batch_columns: left.column_count(),
            })?;

        let output_col = gather_column(left_col, &matches, |&(left_row, _)| left_row)?;
        output.add_column(output_col)?;
    }

    // Process right columns
    for col_idx in 0..right.column_count() {
        let right_col =
            right.column(col_idx).ok_or_else(|| ExecutorError::ColumnarColumnNotFound {
                column_index: col_idx,
                batch_columns: right.column_count(),
            })?;

        let output_col = gather_column(right_col, &matches, |&(_, right_row)| right_row)?;
        output.add_column(output_col)?;
    }

    Ok(output)
}

/// Gather values from a column based on row index mapping
///
/// Creates a new column by extracting values at specified indices.
fn gather_column<F>(
    source: &ColumnArray,
    indices: &[(usize, usize)],
    index_fn: F,
) -> Result<ColumnArray, ExecutorError>
where
    F: Fn(&(usize, usize)) -> usize,
{
    match source {
        ColumnArray::Int64(values, nulls) => {
            let mut output_values = Vec::with_capacity(indices.len());
            let mut output_nulls =
                if nulls.is_some() { Some(Vec::with_capacity(indices.len())) } else { None };

            for pair in indices {
                let idx = index_fn(pair);
                output_values.push(values[idx]);

                if let Some(ref mut out_nulls) = output_nulls {
                    let is_null = nulls.as_ref().map(|n| n[idx]).unwrap_or(false);
                    out_nulls.push(is_null);
                }
            }

            Ok(ColumnArray::Int64(Arc::new(output_values), output_nulls.map(Arc::new)))
        }
        ColumnArray::Float64(values, nulls) => {
            let mut output_values = Vec::with_capacity(indices.len());
            let mut output_nulls =
                if nulls.is_some() { Some(Vec::with_capacity(indices.len())) } else { None };

            for pair in indices {
                let idx = index_fn(pair);
                output_values.push(values[idx]);

                if let Some(ref mut out_nulls) = output_nulls {
                    let is_null = nulls.as_ref().map(|n| n[idx]).unwrap_or(false);
                    out_nulls.push(is_null);
                }
            }

            Ok(ColumnArray::Float64(Arc::new(output_values), output_nulls.map(Arc::new)))
        }
        ColumnArray::String(values, nulls) => {
            let mut output_values = Vec::with_capacity(indices.len());
            let mut output_nulls =
                if nulls.is_some() { Some(Vec::with_capacity(indices.len())) } else { None };

            for pair in indices {
                let idx = index_fn(pair);
                output_values.push(values[idx].clone());

                if let Some(ref mut out_nulls) = output_nulls {
                    let is_null = nulls.as_ref().map(|n| n[idx]).unwrap_or(false);
                    out_nulls.push(is_null);
                }
            }

            Ok(ColumnArray::String(Arc::new(output_values), output_nulls.map(Arc::new)))
        }
        _ => {
            // For other types, fall back to SqlValue extraction
            let mut output_values = Vec::with_capacity(indices.len());

            for pair in indices {
                let idx = index_fn(pair);
                let value = source.get_value(idx)?;
                output_values.push(value);
            }

            Ok(ColumnArray::Mixed(Arc::new(output_values)))
        }
    }
}

/// Columnar hash join with SIMD-accelerated key comparison (INNER JOIN)
///
/// Performs an INNER JOIN between two columnar batches using SIMD operations
/// for key equality comparison.
///
/// # Arguments
///
/// * `left` - Left (probe-side) batch
/// * `right` - Right (build-side) batch
/// * `left_key_col` - Index of join key column in left batch
/// * `right_key_col` - Index of join key column in right batch
///
/// # Returns
///
/// A new columnar batch containing joined rows. The output schema is:
/// [left_col0, left_col1, ..., right_col0, right_col1, ...]
///
/// # Example
///
/// ```text
/// // Join two batches on their first columns
/// let result = columnar_hash_join_inner(&left_batch, &right_batch, 0, 0)?;
/// ```
///
/// # Performance
///
/// - Best for Int64 and Float64 join keys
/// - Automatically handles NULL keys (NULL != NULL)
pub fn columnar_hash_join_inner(
    left: &ColumnarBatch,
    right: &ColumnarBatch,
    left_key_col: usize,
    right_key_col: usize,
) -> Result<ColumnarBatch, ExecutorError> {
    // Build hash table from right (smaller side typically)
    let hash_table = HashTable::build(right, right_key_col)?;

    // Probe with left using SIMD equality
    let matches = hash_table.probe_simd(left, left_key_col)?;

    // Materialize results
    materialize_join_results(left, right, matches)
}

#[cfg(test)]
mod tests {
    use super::*;
    use vibesql_types::SqlValue;

    fn make_int64_batch(key_values: Vec<i64>, data_values: Vec<i64>) -> ColumnarBatch {
        let row_count = key_values.len();
        let mut batch = ColumnarBatch::with_capacity(row_count, 2);

        batch.add_column(ColumnArray::Int64(Arc::new(key_values), None)).unwrap();
        batch.add_column(ColumnArray::Int64(Arc::new(data_values), None)).unwrap();

        batch
    }

    fn make_float64_batch(key_values: Vec<f64>, data_values: Vec<i64>) -> ColumnarBatch {
        let row_count = key_values.len();
        let mut batch = ColumnarBatch::with_capacity(row_count, 2);

        batch.add_column(ColumnArray::Float64(Arc::new(key_values), None)).unwrap();
        batch.add_column(ColumnArray::Int64(Arc::new(data_values), None)).unwrap();

        batch
    }

    #[test]
    fn test_inner_join_int64_basic() {
        // Left: [(1, 10), (2, 20), (3, 30)]
        let left = make_int64_batch(vec![1, 2, 3], vec![10, 20, 30]);

        // Right: [(2, 200), (3, 300), (4, 400)]
        let right = make_int64_batch(vec![2, 3, 4], vec![200, 300, 400]);

        // Join on column 0 (keys)
        let result = columnar_hash_join_inner(&left, &right, 0, 0).unwrap();

        // Expected: [(2, 20, 2, 200), (3, 30, 3, 300)]
        assert_eq!(result.row_count(), 2);
        assert_eq!(result.column_count(), 4);

        let rows = result.to_rows().unwrap();

        // Row 0: (2, 20, 2, 200) or (3, 30, 3, 300) - order may vary
        // Row 1: (3, 30, 3, 300) or (2, 20, 2, 200)

        // Verify we have the correct matches
        let mut found_2 = false;
        let mut found_3 = false;

        for row in &rows {
            match row.get(0) {
                Some(&SqlValue::Integer(2)) => {
                    assert_eq!(row.get(1), Some(&SqlValue::Integer(20)));
                    assert_eq!(row.get(2), Some(&SqlValue::Integer(2)));
                    assert_eq!(row.get(3), Some(&SqlValue::Integer(200)));
                    found_2 = true;
                }
                Some(&SqlValue::Integer(3)) => {
                    assert_eq!(row.get(1), Some(&SqlValue::Integer(30)));
                    assert_eq!(row.get(2), Some(&SqlValue::Integer(3)));
                    assert_eq!(row.get(3), Some(&SqlValue::Integer(300)));
                    found_3 = true;
                }
                _ => panic!("Unexpected join result"),
            }
        }

        assert!(found_2 && found_3, "Missing expected join matches");
    }

    #[test]
    fn test_inner_join_float64_basic() {
        // Left: [(1.5, 10), (2.5, 20), (3.5, 30)]
        let left = make_float64_batch(vec![1.5, 2.5, 3.5], vec![10, 20, 30]);

        // Right: [(2.5, 200), (3.5, 300), (4.5, 400)]
        let right = make_float64_batch(vec![2.5, 3.5, 4.5], vec![200, 300, 400]);

        let result = columnar_hash_join_inner(&left, &right, 0, 0).unwrap();

        assert_eq!(result.row_count(), 2);
        assert_eq!(result.column_count(), 4);

        let rows = result.to_rows().unwrap();

        let mut found_25 = false;
        let mut found_35 = false;

        for row in &rows {
            match row.get(0) {
                Some(&SqlValue::Double(f)) if (f - 2.5).abs() < 1e-10 => {
                    assert_eq!(row.get(1), Some(&SqlValue::Integer(20)));
                    found_25 = true;
                }
                Some(&SqlValue::Double(f)) if (f - 3.5).abs() < 1e-10 => {
                    assert_eq!(row.get(1), Some(&SqlValue::Integer(30)));
                    found_35 = true;
                }
                _ => {}
            }
        }

        assert!(found_25 && found_35);
    }

    #[test]
    fn test_inner_join_with_nulls() {
        // Left with NULL key: [(1, 10), (NULL, 20), (3, 30)]
        let mut left = ColumnarBatch::with_capacity(3, 2);
        left.add_column(ColumnArray::Int64(
            Arc::new(vec![1, 0, 3]), // 0 is placeholder for NULL
            Some(Arc::new(vec![false, true, false])),
        ))
        .unwrap();
        left.add_column(ColumnArray::Int64(Arc::new(vec![10, 20, 30]), None)).unwrap();

        // Right with NULL key: [(1, 100), (3, 300), (NULL, 400)]
        let mut right = ColumnarBatch::with_capacity(3, 2);
        right
            .add_column(ColumnArray::Int64(
                Arc::new(vec![1, 3, 0]),
                Some(Arc::new(vec![false, false, true])),
            ))
            .unwrap();
        right.add_column(ColumnArray::Int64(Arc::new(vec![100, 300, 400]), None)).unwrap();

        let result = columnar_hash_join_inner(&left, &right, 0, 0).unwrap();

        // NULL keys should not match, so only (1, 10, 1, 100) and (3, 30, 3, 300)
        assert_eq!(result.row_count(), 2);

        let rows = result.to_rows().unwrap();
        for row in &rows {
            assert!(!matches!(row.get(0), Some(&SqlValue::Null)));
            assert!(!matches!(row.get(2), Some(&SqlValue::Null)));
        }
    }

    #[test]
    fn test_inner_join_multi_match() {
        // Left: [(1, 10), (1, 11), (2, 20)]
        let left = make_int64_batch(vec![1, 1, 2], vec![10, 11, 20]);

        // Right: [(1, 100), (1, 101)]
        let right = make_int64_batch(vec![1, 1], vec![100, 101]);

        let result = columnar_hash_join_inner(&left, &right, 0, 0).unwrap();

        // Cartesian product: 2 left rows with key=1 × 2 right rows with key=1 = 4 results
        // Plus 0 for key=2 (no match) = 4 total
        assert_eq!(result.row_count(), 4);
    }

    #[test]
    fn test_inner_join_empty_result() {
        // Left: [(1, 10), (2, 20)]
        let left = make_int64_batch(vec![1, 2], vec![10, 20]);

        // Right: [(3, 300), (4, 400)]
        let right = make_int64_batch(vec![3, 4], vec![300, 400]);

        let result = columnar_hash_join_inner(&left, &right, 0, 0).unwrap();

        // No matching keys
        assert_eq!(result.row_count(), 0);
        assert_eq!(result.column_count(), 4);
    }

    #[test]
    fn test_inner_join_empty_batches() {
        let left = ColumnarBatch::with_capacity(0, 2);
        let right = ColumnarBatch::with_capacity(0, 2);

        // Should not panic on empty batches
        let result = columnar_hash_join_inner(&left, &right, 0, 0);
        assert!(result.is_err()); // No columns to join on
    }
}
