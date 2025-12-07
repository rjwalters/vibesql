//! Batch construction and building logic
//!
//! This module contains methods for creating `ColumnarBatch` instances
//! from various sources like rows and column arrays.

use std::sync::Arc;

use crate::errors::ExecutorError;
use vibesql_storage::Row;
use vibesql_types::SqlValue;

use super::types::{ColumnArray, ColumnType, ColumnarBatch};

impl ColumnarBatch {
    /// Create a new empty columnar batch
    pub fn new(column_count: usize) -> Self {
        Self { row_count: 0, columns: Vec::with_capacity(column_count), column_names: None }
    }

    /// Create a columnar batch with specified capacity
    pub fn with_capacity(_row_count: usize, column_count: usize) -> Self {
        Self { row_count: 0, columns: Vec::with_capacity(column_count), column_names: None }
    }

    /// Create an empty batch with the specified number of columns
    pub fn empty(column_count: usize) -> Result<Self, ExecutorError> {
        Ok(Self {
            row_count: 0,
            columns: vec![ColumnArray::Mixed(Arc::new(vec![])); column_count],
            column_names: None,
        })
    }

    /// Create a batch from a list of columns
    pub fn from_columns(
        columns: Vec<ColumnArray>,
        column_names: Option<Vec<String>>,
    ) -> Result<Self, ExecutorError> {
        if columns.is_empty() {
            return Ok(Self { row_count: 0, columns, column_names });
        }

        // Verify all columns have the same length
        let row_count = columns[0].len();
        for (idx, column) in columns.iter().enumerate() {
            if column.len() != row_count {
                return Err(ExecutorError::ColumnarLengthMismatch {
                    context: format!("from_columns (column {})", idx),
                    expected: row_count,
                    actual: column.len(),
                });
            }
        }

        Ok(Self { row_count, columns, column_names })
    }

    /// Convert from row-oriented storage to columnar batch
    ///
    /// This analyzes the first row to infer column types, then materializes
    /// all values into type-specialized column arrays.
    pub fn from_rows(rows: &[Row]) -> Result<Self, ExecutorError> {
        if rows.is_empty() {
            return Ok(Self::new(0));
        }

        let row_count = rows.len();
        let column_count = rows[0].len();

        // Infer column types from first row
        let column_types = Self::infer_column_types(&rows[0]);

        // Create column arrays
        let mut columns = Vec::with_capacity(column_count);

        for (col_idx, col_type) in column_types.iter().enumerate() {
            let column = Self::extract_column(rows, col_idx, col_type)?;
            columns.push(column);
        }

        Ok(Self { row_count, columns, column_names: None })
    }

    /// Extract a single column from rows into a typed array
    pub(crate) fn extract_column(
        rows: &[Row],
        col_idx: usize,
        col_type: &ColumnType,
    ) -> Result<ColumnArray, ExecutorError> {
        match col_type {
            ColumnType::Int64 => {
                let mut values = Vec::with_capacity(rows.len());
                let mut nulls = Vec::with_capacity(rows.len());
                let mut has_nulls = false;

                for row in rows {
                    match row.get(col_idx) {
                        Some(SqlValue::Integer(v)) => {
                            values.push(*v);
                            nulls.push(false);
                        }
                        Some(SqlValue::Null) => {
                            values.push(0); // placeholder
                            nulls.push(true);
                            has_nulls = true;
                        }
                        Some(other) => {
                            return Err(ExecutorError::ColumnarTypeMismatch {
                                operation: "extract_column".to_string(),
                                left_type: "Integer".to_string(),
                                right_type: Some(format!("{:?}", other)),
                            });
                        }
                        None => {
                            values.push(0);
                            nulls.push(true);
                            has_nulls = true;
                        }
                    }
                }

                Ok(ColumnArray::Int64(
                    Arc::new(values),
                    if has_nulls { Some(Arc::new(nulls)) } else { None },
                ))
            }

            ColumnType::Float64 => {
                let mut values = Vec::with_capacity(rows.len());
                let mut nulls = Vec::with_capacity(rows.len());
                let mut has_nulls = false;

                for row in rows {
                    match row.get(col_idx) {
                        Some(SqlValue::Double(v)) => {
                            values.push(*v);
                            nulls.push(false);
                        }
                        Some(SqlValue::Null) => {
                            values.push(0.0); // placeholder
                            nulls.push(true);
                            has_nulls = true;
                        }
                        Some(other) => {
                            return Err(ExecutorError::ColumnarTypeMismatch {
                                operation: "extract_column".to_string(),
                                left_type: "Double".to_string(),
                                right_type: Some(format!("{:?}", other)),
                            });
                        }
                        None => {
                            values.push(0.0);
                            nulls.push(true);
                            has_nulls = true;
                        }
                    }
                }

                Ok(ColumnArray::Float64(
                    Arc::new(values),
                    if has_nulls { Some(Arc::new(nulls)) } else { None },
                ))
            }

            ColumnType::String => {
                let mut values = Vec::with_capacity(rows.len());
                let mut nulls = Vec::with_capacity(rows.len());
                let mut has_nulls = false;

                for row in rows {
                    match row.get(col_idx) {
                        Some(SqlValue::Varchar(v)) => {
                            values.push(Arc::from(v.as_str()));
                            nulls.push(false);
                        }
                        Some(SqlValue::Null) => {
                            values.push(Arc::from("")); // placeholder
                            nulls.push(true);
                            has_nulls = true;
                        }
                        Some(other) => {
                            return Err(ExecutorError::ColumnarTypeMismatch {
                                operation: "extract_column".to_string(),
                                left_type: "Varchar".to_string(),
                                right_type: Some(format!("{:?}", other)),
                            });
                        }
                        None => {
                            values.push(Arc::from(""));
                            nulls.push(true);
                            has_nulls = true;
                        }
                    }
                }

                Ok(ColumnArray::String(
                    Arc::new(values),
                    if has_nulls { Some(Arc::new(nulls)) } else { None },
                ))
            }

            ColumnType::Date | ColumnType::Mixed => {
                // Store dates and mixed types as Mixed (fallback for non-SIMD types)
                let mut values = Vec::with_capacity(rows.len());

                for row in rows {
                    let value = row.get(col_idx).cloned().unwrap_or(SqlValue::Null);
                    values.push(value);
                }

                Ok(ColumnArray::Mixed(Arc::new(values)))
            }

            ColumnType::Boolean => {
                let mut values = Vec::with_capacity(rows.len());
                let mut nulls = Vec::with_capacity(rows.len());
                let mut has_nulls = false;

                for row in rows {
                    match row.get(col_idx) {
                        Some(SqlValue::Boolean(b)) => {
                            values.push(if *b { 1 } else { 0 });
                            nulls.push(false);
                        }
                        Some(SqlValue::Null) => {
                            values.push(0); // placeholder
                            nulls.push(true);
                            has_nulls = true;
                        }
                        Some(other) => {
                            return Err(ExecutorError::ColumnarTypeMismatch {
                                operation: "extract_column".to_string(),
                                left_type: "Boolean".to_string(),
                                right_type: Some(format!("{:?}", other)),
                            });
                        }
                        None => {
                            values.push(0);
                            nulls.push(true);
                            has_nulls = true;
                        }
                    }
                }

                Ok(ColumnArray::Boolean(
                    Arc::new(values),
                    if has_nulls { Some(Arc::new(nulls)) } else { None },
                ))
            }
        }
    }

    /// Infer column types from the first row
    pub(crate) fn infer_column_types(first_row: &Row) -> Vec<ColumnType> {
        let mut types = Vec::with_capacity(first_row.len());

        for i in 0..first_row.len() {
            let col_type = match first_row.get(i) {
                Some(SqlValue::Integer(_)) => ColumnType::Int64,
                Some(SqlValue::Double(_)) => ColumnType::Float64,
                Some(SqlValue::Varchar(_)) => ColumnType::String,
                Some(SqlValue::Date(_)) => ColumnType::Date,
                Some(SqlValue::Boolean(_)) => ColumnType::Boolean,
                _ => ColumnType::Mixed,
            };
            types.push(col_type);
        }

        types
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_columnar_batch_creation() {
        let rows = vec![
            Row::new(vec![
                SqlValue::Integer(1),
                SqlValue::Double(10.5),
                SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
            ]),
            Row::new(vec![
                SqlValue::Integer(2),
                SqlValue::Double(20.5),
                SqlValue::Varchar(arcstr::ArcStr::from("Bob")),
            ]),
            Row::new(vec![
                SqlValue::Integer(3),
                SqlValue::Double(30.5),
                SqlValue::Varchar(arcstr::ArcStr::from("Charlie")),
            ]),
        ];

        let batch = ColumnarBatch::from_rows(&rows).unwrap();

        assert_eq!(batch.row_count(), 3);
        assert_eq!(batch.column_count(), 3);

        // Check column 0 (integers)
        let col0 = batch.column(0).unwrap();
        if let ColumnArray::Int64(values, nulls) = col0 {
            assert_eq!(values.as_slice(), &[1, 2, 3]);
            assert!(nulls.is_none());
        } else {
            panic!("Expected Int64 column");
        }

        // Check column 1 (doubles)
        let col1 = batch.column(1).unwrap();
        if let ColumnArray::Float64(values, nulls) = col1 {
            assert_eq!(values.as_slice(), &[10.5, 20.5, 30.5]);
            assert!(nulls.is_none());
        } else {
            panic!("Expected Float64 column");
        }

        // Check column 2 (strings)
        let col2 = batch.column(2).unwrap();
        if let ColumnArray::String(values, nulls) = col2 {
            let str_refs: Vec<&str> = values.iter().map(|s| s.as_ref()).collect();
            assert_eq!(str_refs, vec!["Alice", "Bob", "Charlie"]);
            assert!(nulls.is_none());
        } else {
            panic!("Expected String column");
        }
    }

    #[test]
    fn test_columnar_batch_with_nulls() {
        let rows = vec![
            Row::new(vec![SqlValue::Integer(1), SqlValue::Double(10.0)]),
            Row::new(vec![SqlValue::Null, SqlValue::Double(20.0)]),
            Row::new(vec![SqlValue::Integer(3), SqlValue::Null]),
        ];

        let batch = ColumnarBatch::from_rows(&rows).unwrap();

        // Check column 0 (with NULL)
        let col0 = batch.column(0).unwrap();
        if let ColumnArray::Int64(values, Some(nulls)) = col0 {
            assert_eq!(values.len(), 3);
            assert_eq!(nulls.as_slice(), &[false, true, false]);
        } else {
            panic!("Expected Int64 column with nulls");
        }

        // Check column 1 (with NULL)
        let col1 = batch.column(1).unwrap();
        if let ColumnArray::Float64(values, Some(nulls)) = col1 {
            assert_eq!(values.len(), 3);
            assert_eq!(nulls.as_slice(), &[false, false, true]);
        } else {
            panic!("Expected Float64 column with nulls");
        }
    }
}
