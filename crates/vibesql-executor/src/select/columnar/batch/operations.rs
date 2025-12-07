//! Batch manipulation operations
//!
//! This module contains methods for accessing and manipulating
//! `ColumnarBatch` and `ColumnArray` instances.

#![allow(clippy::needless_range_loop)]

use std::sync::Arc;

use ahash::AHashSet;

use crate::errors::ExecutorError;
use vibesql_storage::Row;
use vibesql_types::{DataType, Date, SqlValue, Time, Timestamp};

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
        let column = self.column(col_idx).ok_or(ExecutorError::ColumnarColumnNotFound {
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

    /// Deduplicate rows in the batch, returning a new batch with unique rows only
    ///
    /// Uses hash-based deduplication on all columns, preserving insertion order.
    /// This implements DISTINCT semantics: NULL == NULL for uniqueness purposes.
    ///
    /// # Performance
    ///
    /// - O(n) time complexity where n is the number of rows
    /// - Uses AHashSet for efficient duplicate detection
    /// - Preserves the first occurrence of each unique row combination
    ///
    /// # Example
    ///
    /// ```text
    /// // Original batch:
    /// // [1, "A"], [2, "B"], [1, "A"], [3, "C"]
    ///
    /// // After deduplicate():
    /// // [1, "A"], [2, "B"], [3, "C"]
    /// ```
    pub fn deduplicate(&self) -> Result<Self, ExecutorError> {
        if self.row_count == 0 {
            return Ok(self.clone());
        }

        // Track which row indices to keep (first occurrence of each unique row)
        let mut seen: AHashSet<Vec<SqlValue>> = AHashSet::with_capacity(self.row_count);
        let mut keep_indices: Vec<usize> = Vec::with_capacity(self.row_count);

        for row_idx in 0..self.row_count {
            // Extract all column values for this row as a key
            let mut row_key = Vec::with_capacity(self.columns.len());
            for col in &self.columns {
                let value = col.get_value(row_idx)?;
                row_key.push(value);
            }

            // If this row hasn't been seen, keep it
            if seen.insert(row_key) {
                keep_indices.push(row_idx);
            }
        }

        // If no duplicates found, return self unchanged
        if keep_indices.len() == self.row_count {
            return Ok(self.clone());
        }

        log::debug!(
            "Columnar deduplicate: {} rows -> {} unique rows",
            self.row_count,
            keep_indices.len()
        );

        // Build new batch with only the unique rows
        self.select_rows(&keep_indices)
    }

    /// Select specific rows from the batch by index, returning a new batch
    ///
    /// # Arguments
    ///
    /// * `indices` - Row indices to select (must be valid for this batch)
    ///
    /// # Returns
    ///
    /// A new ColumnarBatch containing only the selected rows
    pub fn select_rows(&self, indices: &[usize]) -> Result<Self, ExecutorError> {
        if indices.is_empty() {
            return Self::empty(self.columns.len());
        }

        let new_row_count = indices.len();
        let mut new_columns = Vec::with_capacity(self.columns.len());

        for column in &self.columns {
            let new_column = column.select_rows(indices)?;
            new_columns.push(new_column);
        }

        Ok(Self {
            row_count: new_row_count,
            columns: new_columns,
            column_names: self.column_names.clone(),
        })
    }
}

impl ColumnArray {
    /// Select specific rows from the column by index, returning a new column
    fn select_rows(&self, indices: &[usize]) -> Result<Self, ExecutorError> {
        match self {
            Self::Int64(values, nulls) => {
                let new_values: Vec<i64> = indices.iter().map(|&i| values[i]).collect();
                let new_nulls = nulls
                    .as_ref()
                    .map(|n| Arc::new(indices.iter().map(|&i| n[i]).collect::<Vec<_>>()));
                Ok(Self::Int64(Arc::new(new_values), new_nulls))
            }
            Self::Int32(values, nulls) => {
                let new_values: Vec<i32> = indices.iter().map(|&i| values[i]).collect();
                let new_nulls = nulls
                    .as_ref()
                    .map(|n| Arc::new(indices.iter().map(|&i| n[i]).collect::<Vec<_>>()));
                Ok(Self::Int32(Arc::new(new_values), new_nulls))
            }
            Self::Float64(values, nulls) => {
                let new_values: Vec<f64> = indices.iter().map(|&i| values[i]).collect();
                let new_nulls = nulls
                    .as_ref()
                    .map(|n| Arc::new(indices.iter().map(|&i| n[i]).collect::<Vec<_>>()));
                Ok(Self::Float64(Arc::new(new_values), new_nulls))
            }
            Self::Float32(values, nulls) => {
                let new_values: Vec<f32> = indices.iter().map(|&i| values[i]).collect();
                let new_nulls = nulls
                    .as_ref()
                    .map(|n| Arc::new(indices.iter().map(|&i| n[i]).collect::<Vec<_>>()));
                Ok(Self::Float32(Arc::new(new_values), new_nulls))
            }
            Self::String(values, nulls) => {
                let new_values: Vec<Arc<str>> = indices.iter().map(|&i| values[i].clone()).collect();
                let new_nulls = nulls
                    .as_ref()
                    .map(|n| Arc::new(indices.iter().map(|&i| n[i]).collect::<Vec<_>>()));
                Ok(Self::String(Arc::new(new_values), new_nulls))
            }
            Self::FixedString(values, nulls) => {
                let new_values: Vec<Arc<str>> = indices.iter().map(|&i| values[i].clone()).collect();
                let new_nulls = nulls
                    .as_ref()
                    .map(|n| Arc::new(indices.iter().map(|&i| n[i]).collect::<Vec<_>>()));
                Ok(Self::FixedString(Arc::new(new_values), new_nulls))
            }
            Self::Date(values, nulls) => {
                let new_values: Vec<i32> = indices.iter().map(|&i| values[i]).collect();
                let new_nulls = nulls
                    .as_ref()
                    .map(|n| Arc::new(indices.iter().map(|&i| n[i]).collect::<Vec<_>>()));
                Ok(Self::Date(Arc::new(new_values), new_nulls))
            }
            Self::Timestamp(values, nulls) => {
                let new_values: Vec<i64> = indices.iter().map(|&i| values[i]).collect();
                let new_nulls = nulls
                    .as_ref()
                    .map(|n| Arc::new(indices.iter().map(|&i| n[i]).collect::<Vec<_>>()));
                Ok(Self::Timestamp(Arc::new(new_values), new_nulls))
            }
            Self::Boolean(values, nulls) => {
                let new_values: Vec<u8> = indices.iter().map(|&i| values[i]).collect();
                let new_nulls = nulls
                    .as_ref()
                    .map(|n| Arc::new(indices.iter().map(|&i| n[i]).collect::<Vec<_>>()));
                Ok(Self::Boolean(Arc::new(new_values), new_nulls))
            }
            Self::Mixed(values) => {
                let new_values: Vec<SqlValue> =
                    indices.iter().map(|&i| values[i].clone()).collect();
                Ok(Self::Mixed(Arc::new(new_values)))
            }
        }
    }

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
                    .map(|v| SqlValue::Varchar(arcstr::ArcStr::from(v.as_ref())))
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

            Self::Mixed(values) => {
                values.get(index).cloned().ok_or(ExecutorError::ColumnIndexOutOfBounds { index })
            }

            Self::Int32(values, nulls) => {
                if let Some(null_mask) = nulls {
                    if null_mask.get(index).copied().unwrap_or(false) {
                        return Ok(SqlValue::Null);
                    }
                }
                values
                    .get(index)
                    .map(|v| SqlValue::Integer(*v as i64))
                    .ok_or(ExecutorError::ColumnIndexOutOfBounds { index })
            }

            Self::Float32(values, nulls) => {
                if let Some(null_mask) = nulls {
                    if null_mask.get(index).copied().unwrap_or(false) {
                        return Ok(SqlValue::Null);
                    }
                }
                values
                    .get(index)
                    .map(|v| SqlValue::Real(*v))
                    .ok_or(ExecutorError::ColumnIndexOutOfBounds { index })
            }

            Self::FixedString(values, nulls) => {
                if let Some(null_mask) = nulls {
                    if null_mask.get(index).copied().unwrap_or(false) {
                        return Ok(SqlValue::Null);
                    }
                }
                values
                    .get(index)
                    .map(|v| SqlValue::Character(arcstr::ArcStr::from(v.as_ref())))
                    .ok_or(ExecutorError::ColumnIndexOutOfBounds { index })
            }

            Self::Date(values, nulls) => {
                if let Some(null_mask) = nulls {
                    if null_mask.get(index).copied().unwrap_or(false) {
                        return Ok(SqlValue::Null);
                    }
                }
                values
                    .get(index)
                    .map(|v| SqlValue::Date(days_since_epoch_to_date(*v)))
                    .ok_or(ExecutorError::ColumnIndexOutOfBounds { index })
            }

            Self::Timestamp(values, nulls) => {
                if let Some(null_mask) = nulls {
                    if null_mask.get(index).copied().unwrap_or(false) {
                        return Ok(SqlValue::Null);
                    }
                }
                values
                    .get(index)
                    .map(|v| SqlValue::Timestamp(microseconds_to_timestamp(*v)))
                    .ok_or(ExecutorError::ColumnIndexOutOfBounds { index })
            }
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

/// Convert days since Unix epoch to Date
fn days_since_epoch_to_date(days: i32) -> Date {
    // Simplified conversion: start from 1970-01-01 and count forward
    let mut year = 1970;
    let mut remaining_days = days;

    // Handle years
    loop {
        let year_days =
            if year % 4 == 0 && (year % 100 != 0 || year % 400 == 0) { 366 } else { 365 };
        if remaining_days < year_days {
            break;
        }
        remaining_days -= year_days;
        year += 1;
    }

    // Handle months
    let is_leap = year % 4 == 0 && (year % 100 != 0 || year % 400 == 0);
    let month_lengths = if is_leap {
        [31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    } else {
        [31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31]
    };

    let mut month = 1;
    for &days_in_month in &month_lengths {
        if remaining_days < days_in_month {
            break;
        }
        remaining_days -= days_in_month;
        month += 1;
    }

    let day = remaining_days + 1;

    Date::new(year, month as u8, day as u8).unwrap_or_else(|_| Date::new(1970, 1, 1).unwrap())
}

/// Convert microseconds since Unix epoch to Timestamp
fn microseconds_to_timestamp(micros: i64) -> Timestamp {
    let days = (micros / 86_400_000_000) as i32;
    let remaining_micros = micros % 86_400_000_000;

    let date = days_since_epoch_to_date(days);

    let hours = (remaining_micros / 3_600_000_000) as u8;
    let remaining_micros = remaining_micros % 3_600_000_000;
    let minutes = (remaining_micros / 60_000_000) as u8;
    let remaining_micros = remaining_micros % 60_000_000;
    let seconds = (remaining_micros / 1_000_000) as u8;
    let nanoseconds = ((remaining_micros % 1_000_000) * 1_000) as u32;

    let time = Time::new(hours, minutes, seconds, nanoseconds)
        .unwrap_or_else(|_| Time::new(0, 0, 0, 0).unwrap());

    Timestamp::new(date, time)
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

    #[test]
    fn test_deduplicate_with_duplicates() {
        // Create batch with duplicate rows
        let rows = vec![
            Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("A"))]),
            Row::new(vec![SqlValue::Integer(2), SqlValue::Varchar(arcstr::ArcStr::from("B"))]),
            Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("A"))]), // duplicate
            Row::new(vec![SqlValue::Integer(3), SqlValue::Varchar(arcstr::ArcStr::from("C"))]),
            Row::new(vec![SqlValue::Integer(2), SqlValue::Varchar(arcstr::ArcStr::from("B"))]), // duplicate
        ];

        let batch = ColumnarBatch::from_rows(&rows).unwrap();
        assert_eq!(batch.row_count(), 5);

        let deduped = batch.deduplicate().unwrap();
        assert_eq!(deduped.row_count(), 3);

        // Verify the first occurrences are kept in order
        let result_rows = deduped.to_rows().unwrap();
        assert_eq!(result_rows[0].get(0), Some(&SqlValue::Integer(1)));
        assert_eq!(result_rows[0].get(1), Some(&SqlValue::Varchar(arcstr::ArcStr::from("A"))));
        assert_eq!(result_rows[1].get(0), Some(&SqlValue::Integer(2)));
        assert_eq!(result_rows[1].get(1), Some(&SqlValue::Varchar(arcstr::ArcStr::from("B"))));
        assert_eq!(result_rows[2].get(0), Some(&SqlValue::Integer(3)));
        assert_eq!(result_rows[2].get(1), Some(&SqlValue::Varchar(arcstr::ArcStr::from("C"))));
    }

    #[test]
    fn test_deduplicate_no_duplicates() {
        // All unique rows
        let rows = vec![
            Row::new(vec![SqlValue::Integer(1)]),
            Row::new(vec![SqlValue::Integer(2)]),
            Row::new(vec![SqlValue::Integer(3)]),
        ];

        let batch = ColumnarBatch::from_rows(&rows).unwrap();
        let deduped = batch.deduplicate().unwrap();

        assert_eq!(deduped.row_count(), 3);
    }

    #[test]
    fn test_deduplicate_with_nulls() {
        // Test NULL handling: NULL == NULL for DISTINCT purposes
        let rows = vec![
            Row::new(vec![SqlValue::Null, SqlValue::Varchar(arcstr::ArcStr::from("A"))]),
            Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("B"))]),
            Row::new(vec![SqlValue::Null, SqlValue::Varchar(arcstr::ArcStr::from("A"))]), // duplicate
        ];

        let batch = ColumnarBatch::from_rows(&rows).unwrap();
        let deduped = batch.deduplicate().unwrap();

        assert_eq!(deduped.row_count(), 2);
    }

    #[test]
    fn test_deduplicate_empty_batch() {
        let batch = ColumnarBatch::new(2);
        let deduped = batch.deduplicate().unwrap();
        assert_eq!(deduped.row_count(), 0);
    }

    #[test]
    fn test_select_rows() {
        let rows = vec![
            Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("A"))]),
            Row::new(vec![SqlValue::Integer(2), SqlValue::Varchar(arcstr::ArcStr::from("B"))]),
            Row::new(vec![SqlValue::Integer(3), SqlValue::Varchar(arcstr::ArcStr::from("C"))]),
            Row::new(vec![SqlValue::Integer(4), SqlValue::Varchar(arcstr::ArcStr::from("D"))]),
        ];

        let batch = ColumnarBatch::from_rows(&rows).unwrap();

        // Select rows 0, 2 (indices)
        let selected = batch.select_rows(&[0, 2]).unwrap();
        assert_eq!(selected.row_count(), 2);

        let result_rows = selected.to_rows().unwrap();
        assert_eq!(result_rows[0].get(0), Some(&SqlValue::Integer(1)));
        assert_eq!(result_rows[1].get(0), Some(&SqlValue::Integer(3)));
    }
}
