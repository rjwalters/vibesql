//! Gather Operations for Selective Materialization
//!
//! This module provides efficient gather operations for materializing
//! only the columns and rows needed for query output.
//!
//! Parallelization is enabled via the `parallel` feature flag.
//! When the selection vector exceeds the scan threshold (determined by
//! `ParallelConfig`), gather operations use rayon for parallel execution.

use std::sync::Arc;

#[cfg(feature = "parallel")]
use rayon::prelude::*;
use vibesql_storage::Row;
use vibesql_types::SqlValue;

use super::SelectionVector;
#[cfg(feature = "parallel")]
use crate::select::parallel::ParallelConfig;
use crate::{
    errors::ExecutorError,
    select::columnar::{ColumnArray, ColumnarBatch},
};

/// Gather specific columns from a columnar batch using a selection vector
///
/// This is the key function for late materialization output. It only
/// materializes the columns needed for the final result.
///
/// # Arguments
///
/// * `batch` - Source columnar batch
/// * `selection` - Which rows to include
/// * `column_indices` - Which columns to include
///
/// # Returns
///
/// A vector of rows containing only the selected columns for selected rows.
///
/// # Example
///
/// ```text
/// // From 10 columns, only materialize columns 0 and 3
/// let result = gather_columns(&batch, &selection, &[0, 3])?;
/// ```
pub fn gather_columns(
    batch: &ColumnarBatch,
    selection: &SelectionVector,
    column_indices: &[usize],
) -> Result<Vec<Row>, ExecutorError> {
    let mut rows = Vec::with_capacity(selection.len());

    for idx in selection.iter() {
        let row_idx = idx as usize;
        let mut values = Vec::with_capacity(column_indices.len());

        for &col_idx in column_indices {
            let value = batch.get_value(row_idx, col_idx)?;
            values.push(value);
        }

        rows.push(Row::new(values));
    }

    Ok(rows)
}

/// Gather a single column from a columnar batch using a selection vector
///
/// Optimized path for extracting a single column, useful for:
/// - Join key extraction
/// - Aggregation input
/// - Single-column projections
///
/// # Arguments
///
/// * `batch` - Source columnar batch
/// * `selection` - Which rows to include
/// * `column_idx` - Which column to extract
///
/// # Returns
///
/// A vector of SqlValues for the selected column and rows.
pub fn gather_single_column(
    batch: &ColumnarBatch,
    selection: &SelectionVector,
    column_idx: usize,
) -> Result<Vec<SqlValue>, ExecutorError> {
    let column = batch.column(column_idx).ok_or_else(|| ExecutorError::ColumnarColumnNotFound {
        column_index: column_idx,
        batch_columns: batch.column_count(),
    })?;

    gather_from_column_array(column, selection)
}

/// Gather values from a ColumnArray using a selection vector
///
/// This is the lowest-level gather operation, working directly on
/// typed column arrays for maximum efficiency.
pub fn gather_from_column_array(
    column: &ColumnArray,
    selection: &SelectionVector,
) -> Result<Vec<SqlValue>, ExecutorError> {
    let mut values = Vec::with_capacity(selection.len());

    for idx in selection.iter() {
        let value = column.get_value(idx as usize)?;
        values.push(value);
    }

    Ok(values)
}

/// Gather into a new ColumnArray (staying in columnar format)
///
/// This is useful when the result needs to stay columnar for
/// further vectorized processing.
pub fn gather_column_array(
    column: &ColumnArray,
    selection: &SelectionVector,
) -> Result<ColumnArray, ExecutorError> {
    match column {
        ColumnArray::Int64(values, nulls) => {
            let gathered: Vec<i64> = selection.iter().map(|idx| values[idx as usize]).collect();
            let gathered_nulls = nulls
                .as_ref()
                .map(|n| Arc::new(selection.iter().map(|idx| n[idx as usize]).collect::<Vec<_>>()));
            Ok(ColumnArray::Int64(Arc::new(gathered), gathered_nulls))
        }

        ColumnArray::Int32(values, nulls) => {
            let gathered: Vec<i32> = selection.iter().map(|idx| values[idx as usize]).collect();
            let gathered_nulls = nulls
                .as_ref()
                .map(|n| Arc::new(selection.iter().map(|idx| n[idx as usize]).collect::<Vec<_>>()));
            Ok(ColumnArray::Int32(Arc::new(gathered), gathered_nulls))
        }

        ColumnArray::Float64(values, nulls) => {
            let gathered: Vec<f64> = selection.iter().map(|idx| values[idx as usize]).collect();
            let gathered_nulls = nulls
                .as_ref()
                .map(|n| Arc::new(selection.iter().map(|idx| n[idx as usize]).collect::<Vec<_>>()));
            Ok(ColumnArray::Float64(Arc::new(gathered), gathered_nulls))
        }

        ColumnArray::Float32(values, nulls) => {
            let gathered: Vec<f32> = selection.iter().map(|idx| values[idx as usize]).collect();
            let gathered_nulls = nulls
                .as_ref()
                .map(|n| Arc::new(selection.iter().map(|idx| n[idx as usize]).collect::<Vec<_>>()));
            Ok(ColumnArray::Float32(Arc::new(gathered), gathered_nulls))
        }

        ColumnArray::String(values, nulls) => {
            let gathered: Vec<std::sync::Arc<str>> =
                selection.iter().map(|idx| values[idx as usize].clone()).collect();
            let gathered_nulls = nulls
                .as_ref()
                .map(|n| Arc::new(selection.iter().map(|idx| n[idx as usize]).collect::<Vec<_>>()));
            Ok(ColumnArray::String(Arc::new(gathered), gathered_nulls))
        }

        ColumnArray::FixedString(values, nulls) => {
            let gathered: Vec<std::sync::Arc<str>> =
                selection.iter().map(|idx| values[idx as usize].clone()).collect();
            let gathered_nulls = nulls
                .as_ref()
                .map(|n| Arc::new(selection.iter().map(|idx| n[idx as usize]).collect::<Vec<_>>()));
            Ok(ColumnArray::FixedString(Arc::new(gathered), gathered_nulls))
        }

        ColumnArray::Date(values, nulls) => {
            let gathered: Vec<i32> = selection.iter().map(|idx| values[idx as usize]).collect();
            let gathered_nulls = nulls
                .as_ref()
                .map(|n| Arc::new(selection.iter().map(|idx| n[idx as usize]).collect::<Vec<_>>()));
            Ok(ColumnArray::Date(Arc::new(gathered), gathered_nulls))
        }

        ColumnArray::Timestamp(values, nulls) => {
            let gathered: Vec<i64> = selection.iter().map(|idx| values[idx as usize]).collect();
            let gathered_nulls = nulls
                .as_ref()
                .map(|n| Arc::new(selection.iter().map(|idx| n[idx as usize]).collect::<Vec<_>>()));
            Ok(ColumnArray::Timestamp(Arc::new(gathered), gathered_nulls))
        }

        ColumnArray::Boolean(values, nulls) => {
            let gathered: Vec<u8> = selection.iter().map(|idx| values[idx as usize]).collect();
            let gathered_nulls = nulls
                .as_ref()
                .map(|n| Arc::new(selection.iter().map(|idx| n[idx as usize]).collect::<Vec<_>>()));
            Ok(ColumnArray::Boolean(Arc::new(gathered), gathered_nulls))
        }

        ColumnArray::Mixed(values) => {
            let gathered: Vec<SqlValue> =
                selection.iter().map(|idx| values[idx as usize].clone()).collect();
            Ok(ColumnArray::Mixed(Arc::new(gathered)))
        }
    }
}

/// Gather a columnar batch with selection applied to all columns
///
/// Creates a new ColumnarBatch containing only the selected rows.
/// Parallelizes column gathering when selection size exceeds the scan threshold.
pub fn gather_batch(
    batch: &ColumnarBatch,
    selection: &SelectionVector,
) -> Result<ColumnarBatch, ExecutorError> {
    #[cfg(feature = "parallel")]
    {
        let config = ParallelConfig::global();
        // Parallelize when we have enough rows AND multiple columns
        if batch.column_count() > 1 && config.should_parallelize_scan(selection.len()) {
            // Collect column references first (can't fail)
            let column_refs: Vec<_> =
                (0..batch.column_count()).map(|col_idx| batch.column(col_idx)).collect();

            // Check all columns exist before parallel processing
            for (col_idx, col_opt) in column_refs.iter().enumerate() {
                if col_opt.is_none() {
                    return Err(ExecutorError::ColumnarColumnNotFound {
                        column_index: col_idx,
                        batch_columns: batch.column_count(),
                    });
                }
            }

            // Parallel gather each column
            let results: Vec<Result<ColumnArray, ExecutorError>> = column_refs
                .par_iter()
                .map(|col_opt| gather_column_array(col_opt.as_ref().unwrap(), selection))
                .collect();

            // Collect results, propagating any errors
            let columns: Result<Vec<_>, _> = results.into_iter().collect();
            return ColumnarBatch::from_columns(columns?, batch.column_names().map(|n| n.to_vec()));
        }
    }

    // Sequential fallback (always used when parallel feature disabled)
    let mut columns = Vec::with_capacity(batch.column_count());

    for col_idx in 0..batch.column_count() {
        let column =
            batch.column(col_idx).ok_or_else(|| ExecutorError::ColumnarColumnNotFound {
                column_index: col_idx,
                batch_columns: batch.column_count(),
            })?;

        let gathered = gather_column_array(column, selection)?;
        columns.push(gathered);
    }

    ColumnarBatch::from_columns(columns, batch.column_names().map(|n| n.to_vec()))
}

/// Gather specific columns from a batch, staying columnar
/// Parallelizes column gathering when selection size exceeds the scan threshold.
pub fn gather_batch_columns(
    batch: &ColumnarBatch,
    selection: &SelectionVector,
    column_indices: &[usize],
) -> Result<ColumnarBatch, ExecutorError> {
    #[cfg(feature = "parallel")]
    {
        let config = ParallelConfig::global();
        // Parallelize when we have enough rows AND multiple columns
        if column_indices.len() > 1 && config.should_parallelize_scan(selection.len()) {
            // Collect column references and validate first
            let column_refs: Vec<_> =
                column_indices.iter().map(|&col_idx| (col_idx, batch.column(col_idx))).collect();

            // Check all columns exist before parallel processing
            for &(col_idx, ref col_opt) in &column_refs {
                if col_opt.is_none() {
                    return Err(ExecutorError::ColumnarColumnNotFound {
                        column_index: col_idx,
                        batch_columns: batch.column_count(),
                    });
                }
            }

            // Parallel gather each column
            let results: Vec<Result<ColumnArray, ExecutorError>> = column_refs
                .par_iter()
                .map(|(_, col_opt)| gather_column_array(col_opt.as_ref().unwrap(), selection))
                .collect();

            // Collect results
            let columns: Result<Vec<_>, _> = results.into_iter().collect();

            // Build names sequentially (fast and order-dependent)
            let names = batch.column_names().map(|all_names| {
                column_indices
                    .iter()
                    .filter_map(|&col_idx| {
                        if col_idx < all_names.len() {
                            Some(all_names[col_idx].clone())
                        } else {
                            None
                        }
                    })
                    .collect()
            });

            return ColumnarBatch::from_columns(columns?, names);
        }
    }

    // Sequential fallback
    let mut columns = Vec::with_capacity(column_indices.len());
    let mut names = batch.column_names().map(|_| Vec::with_capacity(column_indices.len()));

    for &col_idx in column_indices {
        let column =
            batch.column(col_idx).ok_or_else(|| ExecutorError::ColumnarColumnNotFound {
                column_index: col_idx,
                batch_columns: batch.column_count(),
            })?;

        let gathered = gather_column_array(column, selection)?;
        columns.push(gathered);

        if let Some(ref mut name_vec) = names {
            if let Some(all_names) = batch.column_names() {
                if col_idx < all_names.len() {
                    name_vec.push(all_names[col_idx].clone());
                }
            }
        }
    }

    ColumnarBatch::from_columns(columns, names)
}

/// Gather from row-based data
///
/// Fallback for when data is not columnar.
/// Parallelizes row cloning when selection size exceeds the scan threshold.
pub fn gather_rows(rows: &[Row], selection: &SelectionVector) -> Vec<Row> {
    #[cfg(feature = "parallel")]
    {
        let config = ParallelConfig::global();
        if config.should_parallelize_scan(selection.len()) {
            return selection.indices().par_iter().map(|&idx| rows[idx as usize].clone()).collect();
        }
    }

    // Sequential fallback
    selection.iter().map(|idx| rows[idx as usize].clone()).collect()
}

/// Gather specific columns from row-based data
/// Parallelizes row construction when selection size exceeds the scan threshold.
pub fn gather_row_columns(
    rows: &[Row],
    selection: &SelectionVector,
    column_indices: &[usize],
) -> Vec<Row> {
    #[cfg(feature = "parallel")]
    {
        let config = ParallelConfig::global();
        if config.should_parallelize_scan(selection.len()) {
            return selection
                .indices()
                .par_iter()
                .map(|&idx| {
                    let source_row = &rows[idx as usize];
                    let values: Vec<SqlValue> = column_indices
                        .iter()
                        .map(|&col_idx| source_row.get(col_idx).cloned().unwrap_or(SqlValue::Null))
                        .collect();
                    Row::new(values)
                })
                .collect();
        }
    }

    // Sequential fallback
    selection
        .iter()
        .map(|idx| {
            let source_row = &rows[idx as usize];
            let values: Vec<SqlValue> = column_indices
                .iter()
                .map(|&col_idx| source_row.get(col_idx).cloned().unwrap_or(SqlValue::Null))
                .collect();
            Row::new(values)
        })
        .collect()
}

/// Specialized gather for join output
///
/// Combines columns from left and right batches based on join indices.
/// Uses u32::MAX as a marker for NULL (outer join unmatched rows).
/// Parallelizes row construction when index count exceeds the scan threshold.
pub fn gather_join_output(
    left_batch: &ColumnarBatch,
    right_batch: &ColumnarBatch,
    left_indices: &[u32],
    right_indices: &[u32],
    left_columns: Option<&[usize]>,
    right_columns: Option<&[usize]>,
) -> Result<Vec<Row>, ExecutorError> {
    let left_cols = left_columns
        .map(|c| c.to_vec())
        .unwrap_or_else(|| (0..left_batch.column_count()).collect());
    let right_cols = right_columns
        .map(|c| c.to_vec())
        .unwrap_or_else(|| (0..right_batch.column_count()).collect());

    let total_cols = left_cols.len() + right_cols.len();

    #[cfg(feature = "parallel")]
    {
        let config = ParallelConfig::global();
        if config.should_parallelize_scan(left_indices.len()) {
            // Parallel row construction
            let results: Vec<Result<Row, ExecutorError>> = left_indices
                .par_iter()
                .zip(right_indices.par_iter())
                .map(|(&left_idx, &right_idx)| {
                    let mut values = Vec::with_capacity(total_cols);

                    // Gather left columns
                    if left_idx == u32::MAX {
                        // NULL row for RIGHT OUTER join
                        for _ in 0..left_cols.len() {
                            values.push(SqlValue::Null);
                        }
                    } else {
                        for &col_idx in &left_cols {
                            values.push(left_batch.get_value(left_idx as usize, col_idx)?);
                        }
                    }

                    // Gather right columns
                    if right_idx == u32::MAX {
                        // NULL row for LEFT OUTER join
                        for _ in 0..right_cols.len() {
                            values.push(SqlValue::Null);
                        }
                    } else {
                        for &col_idx in &right_cols {
                            values.push(right_batch.get_value(right_idx as usize, col_idx)?);
                        }
                    }

                    Ok(Row::new(values))
                })
                .collect();

            // Collect results, propagating any errors
            return results.into_iter().collect();
        }
    }

    // Sequential fallback
    let mut rows = Vec::with_capacity(left_indices.len());

    for (&left_idx, &right_idx) in left_indices.iter().zip(right_indices) {
        let mut values = Vec::with_capacity(total_cols);

        // Gather left columns
        if left_idx == u32::MAX {
            // NULL row for RIGHT OUTER join
            for _ in 0..left_cols.len() {
                values.push(SqlValue::Null);
            }
        } else {
            for &col_idx in &left_cols {
                values.push(left_batch.get_value(left_idx as usize, col_idx)?);
            }
        }

        // Gather right columns
        if right_idx == u32::MAX {
            // NULL row for LEFT OUTER join
            for _ in 0..right_cols.len() {
                values.push(SqlValue::Null);
            }
        } else {
            for &col_idx in &right_cols {
                values.push(right_batch.get_value(right_idx as usize, col_idx)?);
            }
        }

        rows.push(Row::new(values));
    }

    Ok(rows)
}

/// Estimate memory savings from late materialization
///
/// Returns (early_materialization_bytes, late_materialization_bytes, savings_ratio)
pub fn estimate_savings(
    total_rows: usize,
    selected_rows: usize,
    total_columns: usize,
    projected_columns: usize,
    avg_value_bytes: usize,
) -> (usize, usize, f64) {
    // Early materialization: all rows × all columns
    let early = total_rows * total_columns * avg_value_bytes;

    // Late materialization: indices + selected rows × projected columns
    let index_overhead = selected_rows * std::mem::size_of::<u32>();
    let late = index_overhead + (selected_rows * projected_columns * avg_value_bytes);

    let savings = if early > 0 { 1.0 - (late as f64 / early as f64) } else { 0.0 };

    (early, late, savings)
}

#[cfg(test)]
mod gather_tests {
    use super::*;

    fn sample_batch() -> ColumnarBatch {
        let columns = vec![
            ColumnArray::Int64(Arc::new(vec![1, 2, 3, 4, 5]), None),
            ColumnArray::String(
                Arc::new(vec![
                    "Alice".into(),
                    "Bob".into(),
                    "Carol".into(),
                    "Dave".into(),
                    "Eve".into(),
                ]),
                None,
            ),
            ColumnArray::Float64(Arc::new(vec![10.0, 20.0, 30.0, 40.0, 50.0]), None),
        ];
        ColumnarBatch::from_columns(columns, Some(vec!["id".into(), "name".into(), "score".into()]))
            .unwrap()
    }

    #[test]
    fn test_gather_columns() {
        let batch = sample_batch();
        let selection = SelectionVector::from_indices(vec![0, 2, 4]);

        // Gather only columns 0 and 2 (id, score)
        let rows = gather_columns(&batch, &selection, &[0, 2]).unwrap();

        assert_eq!(rows.len(), 3);
        assert_eq!(rows[0].len(), 2);
        assert_eq!(rows[0].get(0), Some(&SqlValue::Integer(1)));
        assert_eq!(rows[0].get(1), Some(&SqlValue::Double(10.0)));
        assert_eq!(rows[2].get(0), Some(&SqlValue::Integer(5)));
    }

    #[test]
    fn test_gather_single_column() {
        let batch = sample_batch();
        let selection = SelectionVector::from_indices(vec![1, 3]);

        let names = gather_single_column(&batch, &selection, 1).unwrap();

        assert_eq!(names.len(), 2);
        assert_eq!(names[0], SqlValue::Varchar(arcstr::ArcStr::from("Bob")));
        assert_eq!(names[1], SqlValue::Varchar(arcstr::ArcStr::from("Dave")));
    }

    #[test]
    fn test_gather_column_array() {
        let batch = sample_batch();
        let selection = SelectionVector::from_indices(vec![0, 2, 4]);

        let int_col = batch.column(0).unwrap();
        let gathered = gather_column_array(int_col, &selection).unwrap();

        if let ColumnArray::Int64(values, _) = gathered {
            assert_eq!(&*values, &[1, 3, 5]);
        } else {
            panic!("Expected Int64 column");
        }
    }

    #[test]
    fn test_gather_batch() {
        let batch = sample_batch();
        let selection = SelectionVector::from_indices(vec![1, 3]);

        let gathered = gather_batch(&batch, &selection).unwrap();

        assert_eq!(gathered.row_count(), 2);
        assert_eq!(gathered.column_count(), 3);
        assert_eq!(gathered.get_value(0, 0).unwrap(), SqlValue::Integer(2));
        assert_eq!(gathered.get_value(1, 0).unwrap(), SqlValue::Integer(4));
    }

    #[test]
    fn test_gather_batch_columns() {
        let batch = sample_batch();
        let selection = SelectionVector::from_indices(vec![0, 4]);

        let gathered = gather_batch_columns(&batch, &selection, &[1]).unwrap();

        assert_eq!(gathered.row_count(), 2);
        assert_eq!(gathered.column_count(), 1);
        assert_eq!(
            gathered.get_value(0, 0).unwrap(),
            SqlValue::Varchar(arcstr::ArcStr::from("Alice"))
        );
        assert_eq!(
            gathered.get_value(1, 0).unwrap(),
            SqlValue::Varchar(arcstr::ArcStr::from("Eve"))
        );
    }

    #[test]
    fn test_gather_rows() {
        let rows = vec![
            Row::new(vec![SqlValue::Integer(1)]),
            Row::new(vec![SqlValue::Integer(2)]),
            Row::new(vec![SqlValue::Integer(3)]),
        ];
        let selection = SelectionVector::from_indices(vec![0, 2]);

        let gathered = gather_rows(&rows, &selection);

        assert_eq!(gathered.len(), 2);
        assert_eq!(gathered[0].get(0), Some(&SqlValue::Integer(1)));
        assert_eq!(gathered[1].get(0), Some(&SqlValue::Integer(3)));
    }

    #[test]
    fn test_gather_join_output() {
        let left = ColumnarBatch::from_columns(
            vec![
                ColumnArray::Int64(Arc::new(vec![1, 2]), None),
                ColumnArray::String(Arc::new(vec!["A".into(), "B".into()]), None),
            ],
            None,
        )
        .unwrap();

        let right = ColumnarBatch::from_columns(
            vec![
                ColumnArray::Int64(Arc::new(vec![1, 1]), None),
                ColumnArray::String(Arc::new(vec!["X".into(), "Y".into()]), None),
            ],
            None,
        )
        .unwrap();

        let left_indices = vec![0, 0, 1];
        let right_indices = vec![0, 1, u32::MAX]; // Last is left outer NULL

        let rows =
            gather_join_output(&left, &right, &left_indices, &right_indices, None, None).unwrap();

        assert_eq!(rows.len(), 3);

        // First join: (1, A, 1, X)
        assert_eq!(rows[0].get(0), Some(&SqlValue::Integer(1)));
        assert_eq!(rows[0].get(2), Some(&SqlValue::Integer(1)));

        // Left outer null: (2, B, NULL, NULL)
        assert_eq!(rows[2].get(0), Some(&SqlValue::Integer(2)));
        assert_eq!(rows[2].get(2), Some(&SqlValue::Null));
    }

    #[test]
    fn test_estimate_savings() {
        // 1M rows, 10 columns, 1% selectivity, 4 projected columns
        let (early, late, savings) = estimate_savings(1_000_000, 10_000, 10, 4, 32);

        // Early: 1M × 10 × 32 = 320MB
        assert_eq!(early, 320_000_000);

        // Late: 10K × 4 bytes (indices) + 10K × 4 × 32 = 40KB + 1.28MB ≈ 1.32MB
        assert!(late < 2_000_000);

        // Savings should be ~99%
        assert!(savings > 0.95);
    }
}
