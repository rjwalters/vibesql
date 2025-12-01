//! Batch manipulation operations
//!
//! This module contains methods for accessing and manipulating
//! `ColumnarBatch` and `ColumnArray` instances.

#![allow(clippy::needless_range_loop)]

use crate::errors::ExecutorError;
use vibesql_storage::Row;
use vibesql_types::{DataType, SqlValue};

use super::types::{ColumnArray, ColumnarBatch};

impl ColumnarBatch {
    /// Get the number of rows in this batch
    pub fn row_count(&self) -> usize {
        self.row_count
    }

    /// Get the number of columns in this batch
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    /// Get a reference to a column array
    pub fn column(&self, index: usize) -> Option<&ColumnArray> {
        self.columns.get(index)
    }

    /// Get a mutable reference to a column array
    pub fn column_mut(&mut self, index: usize) -> Option<&mut ColumnArray> {
        self.columns.get_mut(index)
    }

    /// Add a column to the batch
    pub fn add_column(&mut self, column: ColumnArray) -> Result<(), ExecutorError> {
        // Verify column has correct length
        let col_len = column.len();
        if self.row_count > 0 && col_len != self.row_count {
            return Err(ExecutorError::ColumnarLengthMismatch {
                context: "add_column".to_string(),
                expected: self.row_count,
                actual: col_len,
            });
        }

        if self.row_count == 0 {
            self.row_count = col_len;
        }

        self.columns.push(column);
        Ok(())
    }

    /// Set column names (for debugging)
    pub fn set_column_names(&mut self, names: Vec<String>) {
        self.column_names = Some(names);
    }

    /// Get column names
    pub fn column_names(&self) -> Option<&[String]> {
        self.column_names.as_deref()
    }

    /// Get column index by name
    pub fn column_index_by_name(&self, name: &str) -> Option<usize> {
        self.column_names.as_ref()?.iter().position(|n| n == name)
    }

    /// Get a value at a specific (row, column) position
    pub fn get_value(&self, row_idx: usize, col_idx: usize) -> Result<SqlValue, ExecutorError> {
        let column = self
            .column(col_idx)
            .ok_or(ExecutorError::ColumnarColumnNotFound {
                column_index: col_idx,
                batch_columns: self.columns.len(),
            })?;
        column.get_value(row_idx)
    }

    /// Convert columnar batch back to row-oriented storage
    pub fn to_rows(&self) -> Result<Vec<Row>, ExecutorError> {
        let mut rows = Vec::with_capacity(self.row_count);

        for row_idx in 0..self.row_count {
            let mut values = Vec::with_capacity(self.columns.len());

            for column in &self.columns {
                let value = column.get_value(row_idx)?;
                values.push(value);
            }

            rows.push(Row::new(values));
        }

        Ok(rows)
    }
}

impl ColumnArray {
    /// Get the number of values in this column
    pub fn len(&self) -> usize {
        match self {
            Self::Int64(v, _) => v.len(),
            Self::Int32(v, _) => v.len(),
            Self::Float64(v, _) => v.len(),
            Self::Float32(v, _) => v.len(),
            Self::String(v, _) => v.len(),
            Self::FixedString(v, _) => v.len(),
            Self::Date(v, _) => v.len(),
            Self::Timestamp(v, _) => v.len(),
            Self::Boolean(v, _) => v.len(),
            Self::Mixed(v) => v.len(),
        }
    }

    /// Check if column is empty
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Get a value at the specified index as SqlValue
    pub fn get_value(&self, index: usize) -> Result<SqlValue, ExecutorError> {
        match self {
            Self::Int64(values, nulls) => {
                if let Some(null_mask) = nulls {
                    if null_mask.get(index).copied().unwrap_or(false) {
                        return Ok(SqlValue::Null);
                    }
                }
                values
                    .get(index)
                    .map(|v| SqlValue::Integer(*v))
                    .ok_or(ExecutorError::ColumnIndexOutOfBounds { index })
            }

            Self::Float64(values, nulls) => {
                if let Some(null_mask) = nulls {
                    if null_mask.get(index).copied().unwrap_or(false) {
                        return Ok(SqlValue::Null);
                    }
                }
                values
                    .get(index)
                    .map(|v| SqlValue::Double(*v))
                    .ok_or(ExecutorError::ColumnIndexOutOfBounds { index })
            }

            Self::String(values, nulls) => {
                if let Some(null_mask) = nulls {
                    if null_mask.get(index).copied().unwrap_or(false) {
                        return Ok(SqlValue::Null);
                    }
                }
                values
                    .get(index)
                    .map(|v| SqlValue::Varchar(v.clone()))
                    .ok_or(ExecutorError::ColumnIndexOutOfBounds { index })
            }

            Self::Boolean(values, nulls) => {
                if let Some(null_mask) = nulls {
                    if null_mask.get(index).copied().unwrap_or(false) {
                        return Ok(SqlValue::Null);
                    }
                }
                values
                    .get(index)
                    .map(|v| SqlValue::Boolean(*v != 0))
                    .ok_or(ExecutorError::ColumnIndexOutOfBounds { index })
            }

            Self::Mixed(values) => values
                .get(index)
                .cloned()
                .ok_or(ExecutorError::ColumnIndexOutOfBounds { index }),

            _ => Err(ExecutorError::UnsupportedArrayType {
                operation: "get_value".to_string(),
                array_type: format!("{:?}", std::mem::discriminant(self)),
            }),
        }
    }

    /// Get the data type of this column
    pub fn data_type(&self) -> DataType {
        match self {
            Self::Int64(_, _) => DataType::Integer,
            Self::Int32(_, _) => DataType::Integer,
            Self::Float64(_, _) => DataType::DoublePrecision,
            Self::Float32(_, _) => DataType::Real,
            Self::String(_, _) => DataType::Varchar { max_length: None },
            Self::FixedString(_, _) => DataType::Character { length: 255 },
            Self::Date(_, _) => DataType::Date,
            Self::Timestamp(_, _) => DataType::Timestamp { with_timezone: false },
            Self::Boolean(_, _) => DataType::Boolean,
            Self::Mixed(_) => DataType::Varchar { max_length: None }, // fallback
        }
    }

    /// Get raw i64 slice (for SIMD operations)
    pub fn as_i64(&self) -> Option<(&[i64], Option<&[bool]>)> {
        match self {
            Self::Int64(values, nulls) => {
                Some((values.as_slice(), nulls.as_ref().map(|n| n.as_slice())))
            }
            _ => None,
        }
    }

    /// Get raw f64 slice (for SIMD operations)
    pub fn as_f64(&self) -> Option<(&[f64], Option<&[bool]>)> {
        match self {
            Self::Float64(values, nulls) => {
                Some((values.as_slice(), nulls.as_ref().map(|n| n.as_slice())))
            }
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_batch_to_rows_roundtrip() {
        let original_rows = vec![
            Row::new(vec![SqlValue::Integer(1), SqlValue::Double(10.5)]),
            Row::new(vec![SqlValue::Integer(2), SqlValue::Double(20.5)]),
        ];

        let batch = ColumnarBatch::from_rows(&original_rows).unwrap();
        let converted_rows = batch.to_rows().unwrap();

        assert_eq!(converted_rows.len(), original_rows.len());
        for (original, converted) in original_rows.iter().zip(converted_rows.iter()) {
            assert_eq!(original.len(), converted.len());
            for i in 0..original.len() {
                assert_eq!(original.get(i), converted.get(i));
            }
        }
    }

    #[test]
    fn test_simd_column_access() {
        let rows = vec![
            Row::new(vec![SqlValue::Integer(1), SqlValue::Double(10.5)]),
            Row::new(vec![SqlValue::Integer(2), SqlValue::Double(20.5)]),
            Row::new(vec![SqlValue::Integer(3), SqlValue::Double(30.5)]),
        ];

        let batch = ColumnarBatch::from_rows(&rows).unwrap();

        // Access i64 column for SIMD
        let col0 = batch.column(0).unwrap();
        if let Some((values, nulls)) = col0.as_i64() {
            assert_eq!(values, &[1, 2, 3]);
            assert!(nulls.is_none());
        } else {
            panic!("Expected i64 slice");
        }

        // Access f64 column for SIMD
        let col1 = batch.column(1).unwrap();
        if let Some((values, nulls)) = col1.as_f64() {
            assert_eq!(values, &[10.5, 20.5, 30.5]);
            assert!(nulls.is_none());
        } else {
            panic!("Expected f64 slice");
        }
    }
}
