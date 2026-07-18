// ============================================================================
// Direct Point Lookup API (Performance Optimization)
// ============================================================================
//
// This module provides high-performance point lookup methods that bypass
// SQL parsing for direct primary key access.

use super::Database;
use crate::{Row, StorageError};

impl Database {
    // ============================================================================
    // Direct Point Lookup API (Performance Optimization)
    // ============================================================================

    /// Get a row by primary key value - bypasses SQL parsing for maximum performance
    ///
    /// This method provides O(1) point lookups directly using the primary key index,
    /// completely bypassing SQL parsing and the query execution pipeline.
    ///
    /// # Arguments
    /// * `table_name` - Name of the table
    /// * `pk_value` - Primary key value to look up
    ///
    /// # Returns
    /// * `Ok(Some(&Row))` - The row if found
    /// * `Ok(None)` - If no row matches the primary key
    /// * `Err(StorageError)` - If table doesn't exist or has no primary key
    ///
    /// # Performance
    /// This is ~100-300x faster than executing a SQL point SELECT query because it:
    /// - Skips SQL parsing (~300µs)
    /// - Skips query planning and optimization
    /// - Uses direct HashMap lookup on the PK index
    ///
    /// # Example
    /// ```text
    /// let row = db.get_row_by_pk("users", &SqlValue::Integer(42))?;
    /// if let Some(row) = row {
    ///     let name = &row.values[1];
    /// }
    /// ```
    pub fn get_row_by_pk(
        &self,
        table_name: &str,
        pk_value: &vibesql_types::SqlValue,
    ) -> Result<Option<&Row>, StorageError> {
        // Phase 1 of #6199: record the point lookup (measurement only).
        self.record_point_lookup(table_name);

        let table = self
            .get_table(table_name)
            .ok_or_else(|| StorageError::TableNotFound(table_name.to_string()))?;

        let pk_index = table.primary_key_index().ok_or_else(|| {
            StorageError::Other(format!("Table '{}' has no primary key", table_name))
        })?;

        // Look up the row index using the PK value
        let key = vec![pk_value.clone()];
        if let Some(&row_index) = pk_index.get(&key) {
            let rows = table.scan();
            if row_index < rows.len() {
                return Ok(Some(&rows[row_index]));
            }
        }

        Ok(None)
    }

    /// Get a specific column value by primary key - bypasses SQL parsing for maximum performance
    ///
    /// This is even faster than `get_row_by_pk` when you only need one column value,
    /// as it avoids returning the entire row.
    ///
    /// # Arguments
    /// * `table_name` - Name of the table
    /// * `pk_value` - Primary key value to look up
    /// * `column_index` - Index of the column to retrieve (0-based)
    ///
    /// # Returns
    /// * `Ok(Some(&SqlValue))` - The column value if found
    /// * `Ok(None)` - If no row matches the primary key
    /// * `Err(StorageError)` - If table doesn't exist or column index is out of bounds
    pub fn get_column_by_pk(
        &self,
        table_name: &str,
        pk_value: &vibesql_types::SqlValue,
        column_index: usize,
    ) -> Result<Option<&vibesql_types::SqlValue>, StorageError> {
        // Phase 1 of #6199: record the point lookup (measurement only).
        self.record_point_lookup(table_name);

        let table = self
            .get_table(table_name)
            .ok_or_else(|| StorageError::TableNotFound(table_name.to_string()))?;

        // Validate column index
        if column_index >= table.schema.columns.len() {
            return Err(StorageError::Other(format!(
                "Column index {} out of bounds for table '{}' with {} columns",
                column_index,
                table_name,
                table.schema.columns.len()
            )));
        }

        let pk_index = table.primary_key_index().ok_or_else(|| {
            StorageError::Other(format!("Table '{}' has no primary key", table_name))
        })?;

        // Look up the row index using the PK value
        let key = vec![pk_value.clone()];
        if let Some(&row_index) = pk_index.get(&key) {
            let rows = table.scan();
            if row_index < rows.len() {
                return Ok(rows[row_index].values.get(column_index));
            }
        }

        Ok(None)
    }

    /// Get a row by composite primary key - for tables with multi-column primary keys
    ///
    /// # Arguments
    /// * `table_name` - Name of the table
    /// * `pk_values` - Primary key values in column order
    ///
    /// # Returns
    /// * `Ok(Some(&Row))` - The row if found
    /// * `Ok(None)` - If no row matches the primary key
    /// * `Err(StorageError)` - If table doesn't exist or has no primary key
    pub fn get_row_by_composite_pk(
        &self,
        table_name: &str,
        pk_values: &[vibesql_types::SqlValue],
    ) -> Result<Option<&Row>, StorageError> {
        // Phase 1 of #6199: record the point lookup (measurement only).
        self.record_point_lookup(table_name);

        let table = self
            .get_table(table_name)
            .ok_or_else(|| StorageError::TableNotFound(table_name.to_string()))?;

        let pk_index = table.primary_key_index().ok_or_else(|| {
            StorageError::Other(format!("Table '{}' has no primary key", table_name))
        })?;

        // Look up the row index using the composite PK
        let key: Vec<vibesql_types::SqlValue> = pk_values.to_vec();
        if let Some(&row_index) = pk_index.get(&key) {
            let rows = table.scan();
            if row_index < rows.len() {
                return Ok(Some(&rows[row_index]));
            }
        }

        Ok(None)
    }
}
