//! Filter mask application for columnar batches
//!
//! This module provides functions to apply boolean filter masks to columnar batches,
//! filtering out rows where the mask is false.

use std::sync::Arc;

use super::super::batch::{ColumnArray, ColumnarBatch};
use crate::errors::ExecutorError;
use vibesql_types::SqlValue;

/// Apply a filter mask to a batch, keeping only rows where mask[i] == true
pub fn apply_filter_mask(
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
        let column =
            batch.column(col_idx).ok_or_else(|| ExecutorError::ColumnarColumnNotFound {
                column_index: col_idx,
                batch_columns: batch.column_count(),
            })?;

        let new_column = filter_column(column, mask)?;
        new_columns.push(new_column);
    }

    ColumnarBatch::from_columns(new_columns, batch.column_names().map(|names| names.to_vec()))
}

/// Filter a single column based on the mask
fn filter_column(column: &ColumnArray, mask: &[bool]) -> Result<ColumnArray, ExecutorError> {
    match column {
        ColumnArray::Int64(values, nulls) => {
            let new_values: Vec<i64> = values
                .iter()
                .zip(mask.iter())
                .filter_map(|(&v, &keep)| if keep { Some(v) } else { None })
                .collect();

            let new_nulls = nulls.as_ref().map(|null_mask| {
                Arc::new(
                    null_mask
                        .iter()
                        .zip(mask.iter())
                        .filter_map(|(&n, &keep)| if keep { Some(n) } else { None })
                        .collect::<Vec<bool>>(),
                )
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
                Arc::new(
                    null_mask
                        .iter()
                        .zip(mask.iter())
                        .filter_map(|(&n, &keep)| if keep { Some(n) } else { None })
                        .collect::<Vec<bool>>(),
                )
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
                Arc::new(
                    null_mask
                        .iter()
                        .zip(mask.iter())
                        .filter_map(|(&n, &keep)| if keep { Some(n) } else { None })
                        .collect::<Vec<bool>>(),
                )
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
                Arc::new(
                    null_mask
                        .iter()
                        .zip(mask.iter())
                        .filter_map(|(&n, &keep)| if keep { Some(n) } else { None })
                        .collect::<Vec<bool>>(),
                )
            });

            Ok(ColumnArray::Float32(Arc::new(new_values), new_nulls))
        }

        ColumnArray::String(values, nulls) => {
            let new_values: Vec<Arc<str>> = values
                .iter()
                .zip(mask.iter())
                .filter_map(|(v, &keep)| if keep { Some(v.clone()) } else { None })
                .collect();

            let new_nulls = nulls.as_ref().map(|null_mask| {
                Arc::new(
                    null_mask
                        .iter()
                        .zip(mask.iter())
                        .filter_map(|(&n, &keep)| if keep { Some(n) } else { None })
                        .collect::<Vec<bool>>(),
                )
            });

            Ok(ColumnArray::String(Arc::new(new_values), new_nulls))
        }

        ColumnArray::FixedString(values, nulls) => {
            let new_values: Vec<Arc<str>> = values
                .iter()
                .zip(mask.iter())
                .filter_map(|(v, &keep)| if keep { Some(v.clone()) } else { None })
                .collect();

            let new_nulls = nulls.as_ref().map(|null_mask| {
                Arc::new(
                    null_mask
                        .iter()
                        .zip(mask.iter())
                        .filter_map(|(&n, &keep)| if keep { Some(n) } else { None })
                        .collect::<Vec<bool>>(),
                )
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
                Arc::new(
                    null_mask
                        .iter()
                        .zip(mask.iter())
                        .filter_map(|(&n, &keep)| if keep { Some(n) } else { None })
                        .collect::<Vec<bool>>(),
                )
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
                Arc::new(
                    null_mask
                        .iter()
                        .zip(mask.iter())
                        .filter_map(|(&n, &keep)| if keep { Some(n) } else { None })
                        .collect::<Vec<bool>>(),
                )
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
                Arc::new(
                    null_mask
                        .iter()
                        .zip(mask.iter())
                        .filter_map(|(&n, &keep)| if keep { Some(n) } else { None })
                        .collect::<Vec<bool>>(),
                )
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
