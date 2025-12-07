//! Output construction for columnar hash joins
//!
//! This module provides gather functions that construct result batches
//! from join indices.

use std::sync::Arc;

use crate::errors::ExecutorError;
use crate::select::columnar::{ColumnArray, ColumnarBatch};

/// Result of a columnar hash join probe phase
pub struct JoinIndices {
    /// Indices into left (probe) batch
    pub left_indices: Vec<u32>,
    /// Indices into right (build) batch
    pub right_indices: Vec<u32>,
}

/// Result indices for LEFT OUTER join
pub struct LeftOuterJoinIndices {
    /// Indices into left (probe) batch - always valid
    pub left_indices: Vec<u32>,
    /// Indices into right (build) batch - u32::MAX means no match (NULL row)
    pub right_indices: Vec<u32>,
}

/// Result indices for RIGHT OUTER join
pub struct RightOuterJoinIndices {
    /// Indices into left (probe) batch - u32::MAX means no match (NULL row)
    pub left_indices: Vec<u32>,
    /// Indices into right (build) batch - always valid
    pub right_indices: Vec<u32>,
}

/// Gather columns from both batches based on join indices
pub(crate) fn gather_join_result(
    left_batch: &ColumnarBatch,
    right_batch: &ColumnarBatch,
    indices: &JoinIndices,
) -> Result<ColumnarBatch, ExecutorError> {
    let _result_count = indices.left_indices.len();

    // Gather left columns
    let mut result_columns = Vec::new();
    for col_idx in 0..left_batch.column_count() {
        let column = left_batch.column(col_idx).unwrap();
        let gathered = gather_column(column, &indices.left_indices)?;
        result_columns.push(gathered);
    }

    // Gather right columns
    for col_idx in 0..right_batch.column_count() {
        let column = right_batch.column(col_idx).unwrap();
        let gathered = gather_column(column, &indices.right_indices)?;
        result_columns.push(gathered);
    }

    // Combine column names
    let column_names = match (left_batch.column_names(), right_batch.column_names()) {
        (Some(left_names), Some(right_names)) => {
            let mut names = left_names.to_vec();
            names.extend(right_names.iter().cloned());
            Some(names)
        }
        _ => None,
    };

    ColumnarBatch::from_columns(result_columns, column_names)
}

/// Gather result columns for LEFT OUTER join with NULL handling
pub(crate) fn gather_left_outer_result(
    left_batch: &ColumnarBatch,
    right_batch: &ColumnarBatch,
    indices: &LeftOuterJoinIndices,
) -> Result<ColumnarBatch, ExecutorError> {
    let mut result_columns = Vec::new();

    // Gather left columns (all indices are valid)
    for col_idx in 0..left_batch.column_count() {
        let column = left_batch.column(col_idx).unwrap();
        let gathered = gather_column(column, &indices.left_indices)?;
        result_columns.push(gathered);
    }

    // Gather right columns with NULL handling for unmatched rows
    for col_idx in 0..right_batch.column_count() {
        let column = right_batch.column(col_idx).unwrap();
        let gathered = gather_column_with_nulls(column, &indices.right_indices)?;
        result_columns.push(gathered);
    }

    // Combine column names
    let column_names = match (left_batch.column_names(), right_batch.column_names()) {
        (Some(left_names), Some(right_names)) => {
            let mut names = left_names.to_vec();
            names.extend(right_names.iter().cloned());
            Some(names)
        }
        _ => None,
    };

    ColumnarBatch::from_columns(result_columns, column_names)
}

/// Gather result columns for RIGHT OUTER join with NULL handling
pub(crate) fn gather_right_outer_result(
    left_batch: &ColumnarBatch,
    right_batch: &ColumnarBatch,
    indices: &RightOuterJoinIndices,
) -> Result<ColumnarBatch, ExecutorError> {
    let mut result_columns = Vec::new();

    // Gather left columns with NULL handling for unmatched rows
    for col_idx in 0..left_batch.column_count() {
        let column = left_batch.column(col_idx).unwrap();
        let gathered = gather_column_with_nulls(column, &indices.left_indices)?;
        result_columns.push(gathered);
    }

    // Gather right columns (all indices are valid)
    for col_idx in 0..right_batch.column_count() {
        let column = right_batch.column(col_idx).unwrap();
        let gathered = gather_column(column, &indices.right_indices)?;
        result_columns.push(gathered);
    }

    // Combine column names
    let column_names = match (left_batch.column_names(), right_batch.column_names()) {
        (Some(left_names), Some(right_names)) => {
            let mut names = left_names.to_vec();
            names.extend(right_names.iter().cloned());
            Some(names)
        }
        _ => None,
    };

    ColumnarBatch::from_columns(result_columns, column_names)
}

/// Gather values from a column based on indices
pub(crate) fn gather_column(
    column: &ColumnArray,
    indices: &[u32],
) -> Result<ColumnArray, ExecutorError> {
    match column {
        ColumnArray::Int64(values, nulls) => {
            let gathered: Vec<i64> = indices.iter().map(|&idx| values[idx as usize]).collect();
            let gathered_nulls = nulls
                .as_ref()
                .map(|n| Arc::new(indices.iter().map(|&idx| n[idx as usize]).collect()));
            Ok(ColumnArray::Int64(Arc::new(gathered), gathered_nulls))
        }
        ColumnArray::Int32(values, nulls) => {
            let gathered: Vec<i32> = indices.iter().map(|&idx| values[idx as usize]).collect();
            let gathered_nulls = nulls
                .as_ref()
                .map(|n| Arc::new(indices.iter().map(|&idx| n[idx as usize]).collect()));
            Ok(ColumnArray::Int32(Arc::new(gathered), gathered_nulls))
        }
        ColumnArray::Float64(values, nulls) => {
            let gathered: Vec<f64> = indices.iter().map(|&idx| values[idx as usize]).collect();
            let gathered_nulls = nulls
                .as_ref()
                .map(|n| Arc::new(indices.iter().map(|&idx| n[idx as usize]).collect()));
            Ok(ColumnArray::Float64(Arc::new(gathered), gathered_nulls))
        }
        ColumnArray::Float32(values, nulls) => {
            let gathered: Vec<f32> = indices.iter().map(|&idx| values[idx as usize]).collect();
            let gathered_nulls = nulls
                .as_ref()
                .map(|n| Arc::new(indices.iter().map(|&idx| n[idx as usize]).collect()));
            Ok(ColumnArray::Float32(Arc::new(gathered), gathered_nulls))
        }
        ColumnArray::String(values, nulls) => {
            let gathered: Vec<std::sync::Arc<str>> =
                indices.iter().map(|&idx| values[idx as usize].clone()).collect();
            let gathered_nulls = nulls
                .as_ref()
                .map(|n| Arc::new(indices.iter().map(|&idx| n[idx as usize]).collect()));
            Ok(ColumnArray::String(Arc::new(gathered), gathered_nulls))
        }
        ColumnArray::FixedString(values, nulls) => {
            let gathered: Vec<std::sync::Arc<str>> =
                indices.iter().map(|&idx| values[idx as usize].clone()).collect();
            let gathered_nulls = nulls
                .as_ref()
                .map(|n| Arc::new(indices.iter().map(|&idx| n[idx as usize]).collect()));
            Ok(ColumnArray::FixedString(Arc::new(gathered), gathered_nulls))
        }
        ColumnArray::Date(values, nulls) => {
            let gathered: Vec<i32> = indices.iter().map(|&idx| values[idx as usize]).collect();
            let gathered_nulls = nulls
                .as_ref()
                .map(|n| Arc::new(indices.iter().map(|&idx| n[idx as usize]).collect()));
            Ok(ColumnArray::Date(Arc::new(gathered), gathered_nulls))
        }
        ColumnArray::Timestamp(values, nulls) => {
            let gathered: Vec<i64> = indices.iter().map(|&idx| values[idx as usize]).collect();
            let gathered_nulls = nulls
                .as_ref()
                .map(|n| Arc::new(indices.iter().map(|&idx| n[idx as usize]).collect()));
            Ok(ColumnArray::Timestamp(Arc::new(gathered), gathered_nulls))
        }
        ColumnArray::Boolean(values, nulls) => {
            let gathered: Vec<u8> = indices.iter().map(|&idx| values[idx as usize]).collect();
            let gathered_nulls = nulls
                .as_ref()
                .map(|n| Arc::new(indices.iter().map(|&idx| n[idx as usize]).collect()));
            Ok(ColumnArray::Boolean(Arc::new(gathered), gathered_nulls))
        }
        ColumnArray::Mixed(values) => {
            let gathered: Vec<vibesql_types::SqlValue> =
                indices.iter().map(|&idx| values[idx as usize].clone()).collect();
            Ok(ColumnArray::Mixed(Arc::new(gathered)))
        }
    }
}

/// Gather values from a column with NULL handling for outer joins
///
/// u32::MAX indices are converted to NULL values
pub(crate) fn gather_column_with_nulls(
    column: &ColumnArray,
    indices: &[u32],
) -> Result<ColumnArray, ExecutorError> {
    match column {
        ColumnArray::Int64(values, _existing_nulls) => {
            let mut gathered = Vec::with_capacity(indices.len());
            let mut nulls = Vec::with_capacity(indices.len());

            for &idx in indices {
                if idx == u32::MAX {
                    gathered.push(0); // placeholder
                    nulls.push(true);
                } else {
                    gathered.push(values[idx as usize]);
                    nulls.push(false);
                }
            }

            Ok(ColumnArray::Int64(Arc::new(gathered), Some(Arc::new(nulls))))
        }
        ColumnArray::Int32(values, _existing_nulls) => {
            let mut gathered = Vec::with_capacity(indices.len());
            let mut nulls = Vec::with_capacity(indices.len());

            for &idx in indices {
                if idx == u32::MAX {
                    gathered.push(0);
                    nulls.push(true);
                } else {
                    gathered.push(values[idx as usize]);
                    nulls.push(false);
                }
            }

            Ok(ColumnArray::Int32(Arc::new(gathered), Some(Arc::new(nulls))))
        }
        ColumnArray::Float64(values, _existing_nulls) => {
            let mut gathered = Vec::with_capacity(indices.len());
            let mut nulls = Vec::with_capacity(indices.len());

            for &idx in indices {
                if idx == u32::MAX {
                    gathered.push(0.0);
                    nulls.push(true);
                } else {
                    gathered.push(values[idx as usize]);
                    nulls.push(false);
                }
            }

            Ok(ColumnArray::Float64(Arc::new(gathered), Some(Arc::new(nulls))))
        }
        ColumnArray::Float32(values, _existing_nulls) => {
            let mut gathered = Vec::with_capacity(indices.len());
            let mut nulls = Vec::with_capacity(indices.len());

            for &idx in indices {
                if idx == u32::MAX {
                    gathered.push(0.0);
                    nulls.push(true);
                } else {
                    gathered.push(values[idx as usize]);
                    nulls.push(false);
                }
            }

            Ok(ColumnArray::Float32(Arc::new(gathered), Some(Arc::new(nulls))))
        }
        ColumnArray::String(values, _existing_nulls) => {
            let mut gathered: Vec<std::sync::Arc<str>> = Vec::with_capacity(indices.len());
            let mut nulls = Vec::with_capacity(indices.len());

            for &idx in indices {
                if idx == u32::MAX {
                    gathered.push(Arc::from(""));
                    nulls.push(true);
                } else {
                    gathered.push(values[idx as usize].clone());
                    nulls.push(false);
                }
            }

            Ok(ColumnArray::String(Arc::new(gathered), Some(Arc::new(nulls))))
        }
        ColumnArray::FixedString(values, _existing_nulls) => {
            let mut gathered: Vec<std::sync::Arc<str>> = Vec::with_capacity(indices.len());
            let mut nulls = Vec::with_capacity(indices.len());

            for &idx in indices {
                if idx == u32::MAX {
                    gathered.push(Arc::from(""));
                    nulls.push(true);
                } else {
                    gathered.push(values[idx as usize].clone());
                    nulls.push(false);
                }
            }

            Ok(ColumnArray::FixedString(Arc::new(gathered), Some(Arc::new(nulls))))
        }
        ColumnArray::Date(values, _existing_nulls) => {
            let mut gathered = Vec::with_capacity(indices.len());
            let mut nulls = Vec::with_capacity(indices.len());

            for &idx in indices {
                if idx == u32::MAX {
                    gathered.push(0);
                    nulls.push(true);
                } else {
                    gathered.push(values[idx as usize]);
                    nulls.push(false);
                }
            }

            Ok(ColumnArray::Date(Arc::new(gathered), Some(Arc::new(nulls))))
        }
        ColumnArray::Timestamp(values, _existing_nulls) => {
            let mut gathered = Vec::with_capacity(indices.len());
            let mut nulls = Vec::with_capacity(indices.len());

            for &idx in indices {
                if idx == u32::MAX {
                    gathered.push(0);
                    nulls.push(true);
                } else {
                    gathered.push(values[idx as usize]);
                    nulls.push(false);
                }
            }

            Ok(ColumnArray::Timestamp(Arc::new(gathered), Some(Arc::new(nulls))))
        }
        ColumnArray::Boolean(values, _existing_nulls) => {
            let mut gathered = Vec::with_capacity(indices.len());
            let mut nulls = Vec::with_capacity(indices.len());

            for &idx in indices {
                if idx == u32::MAX {
                    gathered.push(0);
                    nulls.push(true);
                } else {
                    gathered.push(values[idx as usize]);
                    nulls.push(false);
                }
            }

            Ok(ColumnArray::Boolean(Arc::new(gathered), Some(Arc::new(nulls))))
        }
        ColumnArray::Mixed(values) => {
            let gathered: Vec<vibesql_types::SqlValue> = indices
                .iter()
                .map(|&idx| {
                    if idx == u32::MAX {
                        vibesql_types::SqlValue::Null
                    } else {
                        values[idx as usize].clone()
                    }
                })
                .collect();
            Ok(ColumnArray::Mixed(Arc::new(gathered)))
        }
    }
}
