// ============================================================================
// Table Operations API
// ============================================================================
//
// This module provides table management methods for the Database struct.
// Includes create, drop, insert, update operations.

use super::transactions::TransactionChange;
use super::Database;
use crate::change_events::ChangeEvent;
use crate::wal::WalOp;
use crate::{Row, StorageError, Table};

impl Database {
    // ============================================================================
    // Table Operations
    // ============================================================================

    /// Create a table
    pub fn create_table(
        &mut self,
        schema: vibesql_catalog::TableSchema,
    ) -> Result<(), StorageError> {
        let table_name = schema.name.clone();

        self.operations.create_table(&mut self.catalog, schema.clone())?;

        // Normalize table name for storage (matches catalog normalization)
        let normalized_table_name = if self.catalog.is_case_sensitive_identifiers() {
            table_name.clone()
        } else {
            table_name.to_uppercase()
        };

        let current_schema = &self.catalog.get_current_schema();
        let qualified_name = format!("{}.{}", current_schema, normalized_table_name);

        // Assign table ID and emit WAL entry for persistence
        let table_id = self.next_table_id();

        // Serialize schema for WAL (use a simple binary format)
        let schema_data = serialize_table_schema(&schema);

        self.emit_wal_op(WalOp::CreateTable {
            table_id,
            table_name: qualified_name.clone(),
            schema_data,
        });

        let table = Table::new(schema);
        self.tables.insert(qualified_name, table);

        Ok(())
    }

    /// Get a table for reading
    pub fn get_table(&self, name: &str) -> Option<&Table> {
        // Try the name as-is first (for delimited identifiers)
        if let Some(table) = self.tables.get(name) {
            return Some(table);
        }

        // Try uppercase normalization (for unquoted identifiers from the parser)
        let uppercase_name = name.to_uppercase();
        if uppercase_name != name {
            if let Some(table) = self.tables.get(&uppercase_name) {
                return Some(table);
            }
        }

        // Try lowercase normalization (for case-insensitive matching when table
        // was created with lowercase but query uses uppercase identifiers)
        let lowercase_name = name.to_lowercase();
        if lowercase_name != name && lowercase_name != uppercase_name {
            if let Some(table) = self.tables.get(&lowercase_name) {
                return Some(table);
            }
        }

        // Try with schema qualification
        if !name.contains('.') {
            let current_schema = &self.catalog.get_current_schema();

            // Try as-is with schema prefix
            let qualified_name_original = format!("{}.{}", current_schema, name);
            if let Some(table) = self.tables.get(&qualified_name_original) {
                return Some(table);
            }

            // Try uppercase with schema prefix
            let qualified_name_uppercase = format!("{}.{}", current_schema, uppercase_name);
            if qualified_name_uppercase != qualified_name_original {
                if let Some(table) = self.tables.get(&qualified_name_uppercase) {
                    return Some(table);
                }
            }

            // Try lowercase with schema prefix
            let qualified_name_lowercase = format!("{}.{}", current_schema, lowercase_name);
            if qualified_name_lowercase != qualified_name_original
                && qualified_name_lowercase != qualified_name_uppercase
            {
                return self.tables.get(&qualified_name_lowercase);
            }
        }

        None
    }

    /// Get a table for writing
    pub fn get_table_mut(&mut self, name: &str) -> Option<&mut Table> {
        // Try the name as-is first (for delimited identifiers)
        if self.tables.contains_key(name) {
            return self.tables.get_mut(name);
        }

        // Try uppercase normalization (for unquoted identifiers from the parser)
        let uppercase_name = name.to_uppercase();
        if uppercase_name != name && self.tables.contains_key(&uppercase_name) {
            return self.tables.get_mut(&uppercase_name);
        }

        // Try lowercase normalization (for case-insensitive matching when table
        // was created with lowercase but query uses uppercase identifiers)
        let lowercase_name = name.to_lowercase();
        if lowercase_name != name
            && lowercase_name != uppercase_name
            && self.tables.contains_key(&lowercase_name)
        {
            return self.tables.get_mut(&lowercase_name);
        }

        // Try with schema qualification
        if !name.contains('.') {
            let current_schema = &self.catalog.get_current_schema().to_string();

            // Try as-is with schema prefix
            let qualified_name_original = format!("{}.{}", current_schema, name);
            if self.tables.contains_key(&qualified_name_original) {
                return self.tables.get_mut(&qualified_name_original);
            }

            // Try uppercase with schema prefix
            let qualified_name_uppercase = format!("{}.{}", current_schema, uppercase_name);
            if qualified_name_uppercase != qualified_name_original
                && self.tables.contains_key(&qualified_name_uppercase)
            {
                return self.tables.get_mut(&qualified_name_uppercase);
            }

            // Try lowercase with schema prefix
            let qualified_name_lowercase = format!("{}.{}", current_schema, lowercase_name);
            if qualified_name_lowercase != qualified_name_original
                && qualified_name_lowercase != qualified_name_uppercase
                && self.tables.contains_key(&qualified_name_lowercase)
            {
                return self.tables.get_mut(&qualified_name_lowercase);
            }
        }

        None
    }

    /// Drop a table
    pub fn drop_table(&mut self, name: &str) -> Result<(), StorageError> {
        // Emit WAL entry for persistence before dropping
        self.emit_wal_op(WalOp::DropTable {
            table_id: self.table_name_to_id(name),
            table_name: name.to_string(),
        });

        // Invalidate columnar cache before dropping
        self.columnar_cache.invalidate(name);
        self.operations.drop_table(&mut self.catalog, &mut self.tables, name)
    }

    /// Insert a row into a table
    pub fn insert_row(&mut self, table_name: &str, row: Row) -> Result<(), StorageError> {
        let row_index =
            self.operations.insert_row(&self.catalog, &mut self.tables, table_name, row.clone())?;

        self.record_change(TransactionChange::Insert {
            table_name: table_name.to_string(),
            row: row.clone(),
        });

        // Emit WAL entry for persistence
        self.emit_wal_op(WalOp::Insert {
            table_id: self.table_name_to_id(table_name),
            row_id: row_index as u64,
            values: row.values.to_vec(),
        });

        // Broadcast change event to subscribers
        self.broadcast_change(ChangeEvent::Insert {
            table_name: table_name.to_string(),
            row_index,
        });

        // Invalidate columnar cache for this table
        self.columnar_cache.invalidate(table_name);

        Ok(())
    }

    /// Insert multiple rows into a table in a single batch
    ///
    /// This method is optimized for bulk data loading and provides significant
    /// performance improvements over repeated `insert_row` calls:
    ///
    /// - **Pre-allocation**: Vector capacity reserved upfront
    /// - **Batch validation**: All rows validated before any insertion
    /// - **Deferred index rebuild**: Indexes rebuilt once after all inserts
    /// - **Single cache invalidation**: Columnar cache invalidated once at end
    ///
    /// # Arguments
    ///
    /// * `table_name` - Name of the table to insert into
    /// * `rows` - Vector of rows to insert
    ///
    /// # Returns
    ///
    /// * `Ok(usize)` - Number of rows successfully inserted
    /// * `Err(StorageError)` - If validation fails (no rows inserted on error)
    ///
    /// # Performance
    ///
    /// For large batches (1000+ rows), expect 10-50x speedup vs single-row inserts.
    ///
    /// # Example
    ///
    /// ```text
    /// let rows = vec![
    ///     Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))]),
    ///     Row::new(vec![SqlValue::Integer(2), SqlValue::Varchar(arcstr::ArcStr::from("Bob"))]),
    /// ];
    /// let count = db.insert_rows_batch("users", rows)?;
    /// ```
    pub fn insert_rows_batch(
        &mut self,
        table_name: &str,
        rows: Vec<Row>,
    ) -> Result<usize, StorageError> {
        if rows.is_empty() {
            return Ok(0);
        }

        let row_indices = self.operations.insert_rows_batch(
            &self.catalog,
            &mut self.tables,
            table_name,
            rows.clone(),
        )?;

        let table_id = self.table_name_to_id(table_name);

        // Record changes for transaction management, emit WAL entries, and broadcast events
        for (row, &row_index) in rows.into_iter().zip(row_indices.iter()) {
            self.record_change(TransactionChange::Insert {
                table_name: table_name.to_string(),
                row: row.clone(),
            });

            // Emit WAL entry for persistence
            self.emit_wal_op(WalOp::Insert {
                table_id,
                row_id: row_index as u64,
                values: row.values.to_vec(),
            });

            // Broadcast change event to subscribers
            self.broadcast_change(ChangeEvent::Insert {
                table_name: table_name.to_string(),
                row_index,
            });
        }

        // Invalidate columnar cache for this table
        self.columnar_cache.invalidate(table_name);

        Ok(row_indices.len())
    }

    /// Insert rows from an iterator in a streaming fashion
    ///
    /// This method is optimized for very large datasets that may not fit
    /// in memory all at once. Rows are processed in configurable batch sizes,
    /// balancing memory usage with performance.
    ///
    /// # Arguments
    ///
    /// * `table_name` - Name of the table to insert into
    /// * `rows` - Iterator yielding rows to insert
    /// * `batch_size` - Number of rows per batch (0 defaults to 1000)
    ///
    /// # Returns
    ///
    /// * `Ok(usize)` - Total number of rows successfully inserted
    /// * `Err(StorageError)` - If any batch fails validation
    ///
    /// # Note
    ///
    /// Unlike `insert_rows_batch`, this method commits rows batch-by-batch.
    /// A failure partway through will leave previously committed batches
    /// in the table. Use `insert_rows_batch` for all-or-nothing semantics.
    ///
    /// # Example
    ///
    /// ```text
    /// // Stream 100K rows in batches of 5000
    /// let rows = (0..100_000).map(|i| Row::new(vec![SqlValue::Integer(i)]));
    /// let count = db.insert_rows_iter("numbers", rows, 5000)?;
    /// ```
    pub fn insert_rows_iter<I>(
        &mut self,
        table_name: &str,
        rows: I,
        batch_size: usize,
    ) -> Result<usize, StorageError>
    where
        I: Iterator<Item = Row>,
    {
        let batch_size = if batch_size == 0 { 1000 } else { batch_size };
        let mut total_inserted = 0;
        let mut batch = Vec::with_capacity(batch_size);

        for row in rows {
            batch.push(row);

            if batch.len() >= batch_size {
                let count = self.insert_rows_batch(table_name, std::mem::take(&mut batch))?;
                total_inserted += count;
                batch = Vec::with_capacity(batch_size);
            }
        }

        // Insert any remaining rows
        if !batch.is_empty() {
            let count = self.insert_rows_batch(table_name, batch)?;
            total_inserted += count;
        }

        Ok(total_inserted)
    }

    /// Update a single row by primary key value (direct API, no SQL parsing)
    ///
    /// This method provides a high-performance update path that bypasses SQL parsing,
    /// making it suitable for benchmarking and performance-critical code paths.
    ///
    /// # Arguments
    ///
    /// * `table_name` - Name of the table
    /// * `pk_value` - Primary key value to match (single column PK only)
    /// * `column_updates` - List of (column_name, new_value) pairs to update
    ///
    /// # Returns
    ///
    /// * `Ok(true)` - Row was found and updated
    /// * `Ok(false)` - Row was not found (no error)
    /// * `Err(StorageError)` - Table not found, column not found, or constraint violation
    ///
    /// # Example
    ///
    /// ```text
    /// // Update column 'name' for row with id=5
    /// let updated = db.update_row_by_pk(
    ///     "users",
    ///     SqlValue::Integer(5),
    ///     vec![("name", SqlValue::Varchar(arcstr::ArcStr::from("Alice")))],
    /// )?;
    /// ```
    pub fn update_row_by_pk(
        &mut self,
        table_name: &str,
        pk_value: vibesql_types::SqlValue,
        column_updates: Vec<(&str, vibesql_types::SqlValue)>,
    ) -> Result<bool, StorageError> {
        // First phase: read data (immutable borrow)
        let (row_index, old_row, schema, resolved_name) = {
            // Get table using existing lookup logic (handles schema prefixes)
            let table = self
                .get_table(table_name)
                .ok_or_else(|| StorageError::TableNotFound(table_name.to_string()))?;

            // Look up row by PK
            let pk_index = table
                .primary_key_index()
                .ok_or_else(|| StorageError::Other("Table has no primary key index".to_string()))?;

            let row_index = match pk_index.get(&vec![pk_value.clone()]) {
                Some(&idx) => idx,
                None => return Ok(false), // Row not found
            };

            // Get old row and schema
            let old_row = table.scan()[row_index].clone();
            let schema = table.schema.clone();
            let resolved_name = schema.name.clone();

            (row_index, old_row, schema, resolved_name)
        };

        // Second phase: apply updates
        let mut new_row = old_row.clone();
        let mut changed_columns = std::collections::HashSet::new();

        for (col_name, new_value) in &column_updates {
            let col_index =
                schema.get_column_index(col_name).ok_or_else(|| StorageError::ColumnNotFound {
                    column_name: col_name.to_string(),
                    table_name: resolved_name.clone(),
                })?;

            // Check NOT NULL constraint
            let column = &schema.columns[col_index];
            if !column.nullable && *new_value == vibesql_types::SqlValue::Null {
                return Err(StorageError::NullConstraintViolation { column: col_name.to_string() });
            }

            new_row.set(col_index, new_value.clone())?;
            changed_columns.insert(col_index);
        }

        // Third phase: write data (mutable borrow)
        let table_mut = self.get_table_mut(table_name).unwrap();
        table_mut.update_row_selective(row_index, new_row.clone(), &changed_columns)?;

        // Update user-defined indexes (pass changed_columns to skip unaffected indexes)
        self.operations.update_indexes_for_update(
            &self.catalog,
            &resolved_name,
            &old_row,
            &new_row,
            row_index,
            Some(&changed_columns),
        );

        // Emit WAL entry for persistence
        self.emit_wal_op(WalOp::Update {
            table_id: self.table_name_to_id(&resolved_name),
            row_id: row_index as u64,
            old_values: old_row.values.to_vec(),
            new_values: new_row.values.to_vec(),
        });

        // Broadcast change event to subscribers
        self.broadcast_change(ChangeEvent::Update { table_name: resolved_name.clone(), row_index });

        // Invalidate columnar cache
        self.columnar_cache.invalidate(&resolved_name);

        Ok(true)
    }

    /// List all table names
    pub fn list_tables(&self) -> Vec<String> {
        self.catalog.list_tables()
    }
}

/// Serialize a TableSchema to bytes for WAL storage
///
/// Uses a simple format: JSON serialization of the schema.
/// This is for WAL recovery purposes and doesn't need to be maximally efficient.
pub(super) fn serialize_table_schema(schema: &vibesql_catalog::TableSchema) -> Vec<u8> {
    // Simple approach: serialize the table name and column info as text
    // Format: table_name\0col1_name\0col1_type\0nullable\0...
    let mut data = Vec::new();

    // Write table name
    data.extend_from_slice(schema.name.as_bytes());
    data.push(0);

    // Write column count
    data.extend_from_slice(&(schema.columns.len() as u32).to_le_bytes());

    // Write each column
    for col in &schema.columns {
        // Column name
        data.extend_from_slice(col.name.as_bytes());
        data.push(0);

        // Data type (as debug string for simplicity)
        let type_str = format!("{:?}", col.data_type);
        data.extend_from_slice(type_str.as_bytes());
        data.push(0);

        // Nullable flag
        data.push(if col.nullable { 1 } else { 0 });
    }

    data
}
