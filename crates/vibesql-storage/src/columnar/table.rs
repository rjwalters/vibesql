//! Columnar table storage.
//!
//! This module provides the `ColumnarTable` struct for storing data in
//! column-oriented format for analytical query performance.

use std::collections::HashMap;

use crate::Row;

use super::builder::ColumnBuilder;
use super::data::ColumnData;
use super::types::ColumnTypeClass;

/// Columnar table storage
///
/// Stores data in column-oriented format for analytical query performance.
/// Each column is stored as a typed vector with a separate NULL bitmap.
///
/// # Example
///
/// ```text
/// use vibesql_storage::{Row, ColumnarTable};
/// use vibesql_types::SqlValue;
///
/// // Create rows
/// let rows = vec![
///     Row::new(vec![SqlValue::Integer(1), SqlValue::Double(3.14)]),
///     Row::new(vec![SqlValue::Integer(2), SqlValue::Double(2.71)]),
/// ];
///
/// // Convert to columnar
/// let column_names = vec!["id".to_string(), "value".to_string()];
/// let columnar = ColumnarTable::from_rows(&rows, &column_names).unwrap();
///
/// // Access column data
/// assert_eq!(columnar.row_count(), 2);
/// assert_eq!(columnar.column_count(), 2);
/// ```
#[derive(Debug, Clone)]
pub struct ColumnarTable {
    /// Column data indexed by column name
    columns: HashMap<String, ColumnData>,
    /// Column names in order (for iteration)
    column_names: Vec<String>,
    /// Number of rows
    row_count: usize,
}

impl ColumnarTable {
    /// Create a new empty columnar table
    pub fn new() -> Self {
        ColumnarTable { columns: HashMap::new(), column_names: Vec::new(), row_count: 0 }
    }

    /// Convert row-oriented data to columnar format (optimized single-pass)
    ///
    /// # Arguments
    /// * `rows` - Vector of rows to convert
    /// * `column_names` - Names of columns in order
    ///
    /// # Returns
    /// * `Ok(ColumnarTable)` on success
    /// * `Err(String)` if column count mismatch or incompatible types
    ///
    /// # Performance
    /// O(n * m) single pass through all data
    pub fn from_rows(rows: &[Row], column_names: &[String]) -> Result<Self, String> {
        if rows.is_empty() {
            return Ok(ColumnarTable {
                columns: HashMap::new(),
                column_names: column_names.to_vec(),
                row_count: 0,
            });
        }

        let row_count = rows.len();
        let col_count = column_names.len();

        // Validate first row column count
        if rows[0].len() != col_count {
            return Err(format!("Row 0 has {} columns, expected {}", rows[0].len(), col_count));
        }

        // Infer column types from first non-null value in each column
        let col_types: Vec<_> = (0..col_count)
            .map(|col_idx| {
                rows.iter()
                    .filter_map(|row| row.get(col_idx))
                    .find(|v| !v.is_null())
                    .map(ColumnTypeClass::from_sql_value)
                    .unwrap_or(ColumnTypeClass::Null)
            })
            .collect();

        // Pre-allocate column storage based on inferred types
        let mut column_builders: Vec<ColumnBuilder> =
            col_types.iter().map(|t| ColumnBuilder::new(*t, row_count)).collect();

        // Single pass through rows - distribute values to columns
        for (row_idx, row) in rows.iter().enumerate() {
            if row.len() != col_count {
                return Err(format!(
                    "Row {} has {} columns, expected {}",
                    row_idx,
                    row.len(),
                    col_count
                ));
            }

            for (col_idx, value) in row.values.iter().enumerate() {
                column_builders[col_idx].push(value)?;
            }
        }

        // Build final columns HashMap - consume builders
        let mut columns = HashMap::with_capacity(col_count);
        for (col_name, builder) in column_names.iter().zip(column_builders.into_iter()) {
            columns.insert(col_name.clone(), builder.build());
        }

        Ok(ColumnarTable { columns, column_names: column_names.to_vec(), row_count })
    }

    /// Create a ColumnarTable from a slice of row references
    ///
    /// This is useful when you have filtered rows (e.g., skipping deleted rows)
    /// and want to avoid cloning the entire row just to pass to from_rows.
    ///
    /// # Arguments
    /// * `rows` - Slice of row references to convert
    /// * `column_names` - Column names for the table schema
    ///
    /// # Returns
    /// * `Ok(ColumnarTable)` on success
    /// * `Err(String)` if column count mismatch or incompatible types
    pub fn from_row_refs(rows: &[&Row], column_names: &[String]) -> Result<Self, String> {
        if rows.is_empty() {
            return Ok(ColumnarTable {
                columns: HashMap::new(),
                column_names: column_names.to_vec(),
                row_count: 0,
            });
        }

        let row_count = rows.len();
        let col_count = column_names.len();

        // Validate first row column count
        if rows[0].len() != col_count {
            return Err(format!("Row 0 has {} columns, expected {}", rows[0].len(), col_count));
        }

        // Infer column types from first non-null value in each column
        let col_types: Vec<_> = (0..col_count)
            .map(|col_idx| {
                rows.iter()
                    .filter_map(|row| row.get(col_idx))
                    .find(|v| !v.is_null())
                    .map(ColumnTypeClass::from_sql_value)
                    .unwrap_or(ColumnTypeClass::Null)
            })
            .collect();

        // Pre-allocate column storage based on inferred types
        let mut column_builders: Vec<ColumnBuilder> =
            col_types.iter().map(|t| ColumnBuilder::new(*t, row_count)).collect();

        // Single pass through rows - distribute values to columns
        for (row_idx, row) in rows.iter().enumerate() {
            if row.len() != col_count {
                return Err(format!(
                    "Row {} has {} columns, expected {}",
                    row_idx,
                    row.len(),
                    col_count
                ));
            }

            for (col_idx, value) in row.values.iter().enumerate() {
                column_builders[col_idx].push(value)?;
            }
        }

        // Build final columns HashMap - consume builders
        let mut columns = HashMap::with_capacity(col_count);
        for (col_name, builder) in column_names.iter().zip(column_builders.into_iter()) {
            columns.insert(col_name.clone(), builder.build());
        }

        Ok(ColumnarTable { columns, column_names: column_names.to_vec(), row_count })
    }

    /// Convert columnar data back to row-oriented format
    ///
    /// # Returns
    /// Vector of rows reconstructed from columnar data
    ///
    /// # Performance
    /// O(n * m) where n = rows, m = columns
    pub fn to_rows(&self) -> Vec<Row> {
        let mut rows = Vec::with_capacity(self.row_count);

        for row_idx in 0..self.row_count {
            let mut values = Vec::with_capacity(self.column_names.len());

            for col_name in &self.column_names {
                if let Some(column) = self.columns.get(col_name) {
                    values.push(column.get(row_idx));
                } else {
                    values.push(vibesql_types::SqlValue::Null);
                }
            }

            rows.push(Row::new(values));
        }

        rows
    }

    /// Get the number of rows
    pub fn row_count(&self) -> usize {
        self.row_count
    }

    /// Get the number of columns
    pub fn column_count(&self) -> usize {
        self.column_names.len()
    }

    /// Get column data by name
    pub fn get_column(&self, name: &str) -> Option<&ColumnData> {
        self.columns.get(name)
    }

    /// Get all column names
    pub fn column_names(&self) -> &[String] {
        &self.column_names
    }

    /// Estimate the memory size of this columnar table in bytes
    ///
    /// This is used for memory budgeting in the columnar cache.
    /// The estimate includes:
    /// - All column data (via ColumnData::size_in_bytes)
    /// - HashMap overhead for columns
    /// - Vec overhead for column_names
    /// - String storage for column names
    pub fn size_in_bytes(&self) -> usize {
        const VEC_OVERHEAD: usize = 3 * std::mem::size_of::<usize>();
        // HashMap has ~48 bytes base overhead plus per-bucket overhead
        const HASHMAP_BASE_OVERHEAD: usize = 48;
        const HASHMAP_ENTRY_OVERHEAD: usize = 8; // approximate per-entry overhead

        let columns_size: usize = self.columns.values().map(|c| c.size_in_bytes()).sum();

        let column_names_size: usize =
            self.column_names.iter().map(|s| std::mem::size_of::<String>() + s.capacity()).sum();

        // HashMap keys (column names stored again)
        let hashmap_keys_size: usize =
            self.columns.keys().map(|s| std::mem::size_of::<String>() + s.capacity()).sum();

        std::mem::size_of::<Self>() // Base struct size
            + columns_size
            + HASHMAP_BASE_OVERHEAD
            + self.columns.len() * HASHMAP_ENTRY_OVERHEAD
            + hashmap_keys_size
            + VEC_OVERHEAD
            + column_names_size
    }
}

impl Default for ColumnarTable {
    fn default() -> Self {
        Self::new()
    }
}
