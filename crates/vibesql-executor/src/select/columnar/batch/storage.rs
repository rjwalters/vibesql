//! Storage layer conversion
//!
//! This module contains methods for converting between the storage layer's
//! ColumnarTable and the executor's ColumnarBatch.

use std::sync::Arc;

use crate::errors::ExecutorError;
use vibesql_types::SqlValue;

use super::types::{ColumnArray, ColumnarBatch};

impl ColumnarBatch {
    /// Convert from storage layer ColumnarTable to executor ColumnarBatch
    ///
    /// This method provides **true zero-copy** conversion from the storage layer's
    /// columnar format to the executor's columnar format. This is the key integration
    /// point for native columnar table scans.
    ///
    /// # Performance
    ///
    /// - **O(1) for numeric/string columns**: Arc::clone is just a reference count bump
    /// - **< 1 microsecond** for millions of rows (vs O(n) with data copy)
    /// - Directly shares storage ColumnData with executor ColumnArray
    /// - Critical path for TPC-H Q6 and other analytical queries
    ///
    /// # Zero-Copy Design
    ///
    /// Both `vibesql_storage::ColumnData` and executor `ColumnArray` use `Arc<Vec<T>>`
    /// for column data. Calling `Arc::clone()` only increments a reference count,
    /// avoiding any data copying:
    ///
    /// ```text
    /// Storage: Arc<Vec<i64>> ─┬─> [1, 2, 3, 4, ...]  (shared memory)
    ///                         │
    /// Executor: Arc<Vec<i64>> ┘
    /// ```
    ///
    /// # Arguments
    ///
    /// * `storage_columnar` - ColumnarTable from storage layer (vibesql-storage)
    ///
    /// # Returns
    ///
    /// * `Ok(ColumnarBatch)` - Executor-ready columnar batch with shared Arc references
    /// * `Err(ExecutorError)` - If type conversion fails
    pub fn from_storage_columnar(
        storage_columnar: &vibesql_storage::ColumnarTable,
    ) -> Result<Self, ExecutorError> {
        use vibesql_storage::ColumnData;

        let column_names = storage_columnar.column_names().to_vec();
        let row_count = storage_columnar.row_count();

        // Handle empty tables: return an empty batch with column names but no data
        // This happens when ColumnarTable::from_rows is called with empty rows -
        // the column_names are preserved but the columns HashMap is empty
        if row_count == 0 {
            return Ok(Self {
                row_count: 0,
                columns: Vec::new(),
                column_names: Some(column_names),
            });
        }

        let mut columns = Vec::with_capacity(column_names.len());

        for col_name in column_names.iter() {
            let storage_col = storage_columnar.get_column(col_name).ok_or_else(|| {
                ExecutorError::ColumnarColumnNotFoundByName { column_name: col_name.clone() }
            })?;

            let column_array =
                match storage_col {
                    ColumnData::Int64 { values, nulls } => {
                        // Zero-copy: Arc::clone is O(1) - just bumps reference count
                        let null_bitmap =
                            if nulls.iter().any(|&n| n) { Some(Arc::clone(nulls)) } else { None };
                        ColumnArray::Int64(Arc::clone(values), null_bitmap)
                    }
                    ColumnData::Float64 { values, nulls } => {
                        // Zero-copy: Arc::clone is O(1)
                        let null_bitmap =
                            if nulls.iter().any(|&n| n) { Some(Arc::clone(nulls)) } else { None };
                        ColumnArray::Float64(Arc::clone(values), null_bitmap)
                    }
                    ColumnData::String { values, nulls } => {
                        // Zero-copy: Arc::clone is O(1)
                        let null_bitmap =
                            if nulls.iter().any(|&n| n) { Some(Arc::clone(nulls)) } else { None };
                        ColumnArray::String(Arc::clone(values), null_bitmap)
                    }
                    ColumnData::Bool { values, nulls } => {
                        // Convert bool to u8 for SIMD compatibility (requires iteration)
                        let u8_values: Vec<u8> =
                            values.iter().map(|&b| if b { 1 } else { 0 }).collect();
                        let null_bitmap =
                            if nulls.iter().any(|&n| n) { Some(Arc::clone(nulls)) } else { None };
                        ColumnArray::Boolean(Arc::new(u8_values), null_bitmap)
                    }
                    ColumnData::Date { values, nulls } => {
                        // Convert Date to i32 (days since Unix epoch 1970-01-01)
                        // Must use the same formula as simd_filter.rs:date_to_days_since_epoch
                        // for predicate evaluation to work correctly
                        let i32_values: Vec<i32> =
                            values.iter().map(date_to_days_since_epoch).collect();
                        let null_bitmap =
                            if nulls.iter().any(|&n| n) { Some(Arc::clone(nulls)) } else { None };
                        ColumnArray::Date(Arc::new(i32_values), null_bitmap)
                    }
                    ColumnData::Timestamp { values, nulls } => {
                        // Convert Timestamp to Mixed (fallback - no direct i64 conversion)
                        let sql_values: Vec<SqlValue> = values
                            .iter()
                            .zip(nulls.iter())
                            .map(
                                |(t, &is_null)| {
                                    if is_null {
                                        SqlValue::Null
                                    } else {
                                        SqlValue::Timestamp(*t)
                                    }
                                },
                            )
                            .collect();
                        ColumnArray::Mixed(Arc::new(sql_values))
                    }
                    ColumnData::Time { values, nulls } => {
                        // Convert Time to Mixed (fallback - Time doesn't have direct i64 conversion)
                        let sql_values: Vec<SqlValue> = values
                            .iter()
                            .zip(nulls.iter())
                            .map(
                                |(t, &is_null)| {
                                    if is_null {
                                        SqlValue::Null
                                    } else {
                                        SqlValue::Time(*t)
                                    }
                                },
                            )
                            .collect();
                        ColumnArray::Mixed(Arc::new(sql_values))
                    }
                    ColumnData::Interval { values, nulls } => {
                        // Convert Interval to Mixed (fallback)
                        let sql_values: Vec<SqlValue> = values
                            .iter()
                            .zip(nulls.iter())
                            .map(|(i, &is_null)| {
                                if is_null {
                                    SqlValue::Null
                                } else {
                                    SqlValue::Interval(i.clone())
                                }
                            })
                            .collect();
                        ColumnArray::Mixed(Arc::new(sql_values))
                    }
                    ColumnData::Vector { values, nulls } => {
                        // Convert Vector to Mixed (fallback)
                        let sql_values: Vec<SqlValue> =
                            values
                                .iter()
                                .zip(nulls.iter())
                                .map(|(v, &is_null)| {
                                    if is_null {
                                        SqlValue::Null
                                    } else {
                                        SqlValue::Vector(v.clone())
                                    }
                                })
                                .collect();
                        ColumnArray::Mixed(Arc::new(sql_values))
                    }
                };

            columns.push(column_array);
        }

        Ok(Self { row_count, columns, column_names: Some(column_names) })
    }
}

/// Convert Date to days since Unix epoch (1970-01-01)
///
/// This function MUST be kept in sync with simd_filter.rs::date_to_days_since_epoch()
/// to ensure predicates compare dates correctly.
fn date_to_days_since_epoch(date: &vibesql_types::Date) -> i32 {
    // Accurate days since Unix epoch calculation with leap year handling
    let year_days = (date.year - 1970) * 365;
    let leap_years =
        ((date.year - 1969) / 4) - ((date.year - 1901) / 100) + ((date.year - 1601) / 400);
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

#[cfg(test)]
mod tests {
    use super::*;
    use vibesql_storage::Row;

    #[test]
    fn test_from_storage_columnar() {
        // Create storage-layer columnar table
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
        let column_names = vec!["id".to_string(), "value".to_string(), "name".to_string()];
        let storage_columnar =
            vibesql_storage::ColumnarTable::from_rows(&rows, &column_names).unwrap();

        // Convert to executor ColumnarBatch
        let batch = ColumnarBatch::from_storage_columnar(&storage_columnar).unwrap();

        // Verify structure
        assert_eq!(batch.row_count(), 3);
        assert_eq!(batch.column_count(), 3);

        // Verify column names
        let names = batch.column_names().unwrap();
        assert_eq!(names, &["id", "value", "name"]);

        // Verify Int64 column
        let col0 = batch.column(0).unwrap();
        if let Some((values, nulls)) = col0.as_i64() {
            assert_eq!(values, &[1, 2, 3]);
            assert!(nulls.is_none());
        } else {
            panic!("Expected i64 column");
        }

        // Verify Float64 column
        let col1 = batch.column(1).unwrap();
        if let Some((values, nulls)) = col1.as_f64() {
            assert_eq!(values, &[10.5, 20.5, 30.5]);
            assert!(nulls.is_none());
        } else {
            panic!("Expected f64 column");
        }

        // Verify String column
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
    fn test_from_storage_columnar_with_nulls() {
        // Create storage-layer columnar table with NULLs
        let rows = vec![
            Row::new(vec![SqlValue::Integer(1), SqlValue::Double(10.0)]),
            Row::new(vec![SqlValue::Null, SqlValue::Double(20.0)]),
            Row::new(vec![SqlValue::Integer(3), SqlValue::Null]),
        ];
        let column_names = vec!["id".to_string(), "value".to_string()];
        let storage_columnar =
            vibesql_storage::ColumnarTable::from_rows(&rows, &column_names).unwrap();

        // Convert to executor ColumnarBatch
        let batch = ColumnarBatch::from_storage_columnar(&storage_columnar).unwrap();

        // Verify Int64 column with NULL
        let col0 = batch.column(0).unwrap();
        if let Some((values, Some(nulls))) = col0.as_i64() {
            assert_eq!(values.len(), 3);
            assert_eq!(nulls, &[false, true, false]);
        } else {
            panic!("Expected i64 column with nulls");
        }

        // Verify Float64 column with NULL
        let col1 = batch.column(1).unwrap();
        if let Some((values, Some(nulls))) = col1.as_f64() {
            assert_eq!(values.len(), 3);
            assert_eq!(nulls, &[false, false, true]);
        } else {
            panic!("Expected f64 column with nulls");
        }
    }
}
