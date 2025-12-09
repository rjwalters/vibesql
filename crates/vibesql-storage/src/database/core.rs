// ============================================================================
// Database - Coordinates between focused modules
// ============================================================================

use super::lifecycle::Lifecycle;
use super::metadata::Metadata;
use super::operations::Operations;
use super::transactions::TransactionChange;
use crate::change_events::{ChangeEvent, ChangeEventReceiver, ChangeEventSender};
use crate::columnar_cache::ColumnarCache;
use crate::wal::{DurabilityMode, PersistenceEngine, TransactionDurability, WalOp};
use crate::{QueryBufferPool, Row, StorageError, Table};
use std::collections::HashMap;

#[allow(unused_imports)]
use std::sync::Arc;

pub use super::operations::SpatialIndexMetadata as ExportedSpatialIndexMetadata;

/// In-memory database - manages catalog and tables through focused modules
#[derive(Debug)]
pub struct Database {
    /// Public catalog access for backward compatibility
    pub catalog: vibesql_catalog::Catalog,
    pub(super) lifecycle: Lifecycle,
    pub(super) metadata: Metadata,
    pub(super) operations: Operations,
    pub tables: HashMap<String, Table>,
    /// SQL compatibility mode (MySQL, SQLite, etc.)
    pub(super) sql_mode: vibesql_types::SqlMode,
    /// Buffer pool for reducing query execution allocations
    pub(super) query_buffer_pool: QueryBufferPool,
    /// LRU cache for columnar table representations
    /// Shared via Arc to allow cloning without duplicating cache data
    pub(super) columnar_cache: Arc<ColumnarCache>,
    /// Optional broadcast channel for change event notifications
    /// Enables reactive subscriptions when enabled
    pub(super) change_sender: Option<ChangeEventSender>,
    /// Last generated AUTO_INCREMENT value for LAST_INSERT_ROWID()
    /// Tracks the most recent auto-generated ID from INSERT operations
    pub(super) last_insert_rowid: i64,
    /// Optional persistence engine for WAL-based async persistence
    /// Enables durable storage when enabled
    pub(super) persistence_engine: Option<PersistenceEngine>,
    /// Next table ID to assign (for WAL table_id tracking)
    pub(super) next_table_id: u32,
}

impl Database {
    // ============================================================================
    // Transaction Management
    // ============================================================================

    /// Record a change in the current transaction (if any)
    pub fn record_change(&mut self, change: TransactionChange) {
        self.lifecycle.transaction_manager_mut().record_change(change);
    }

    /// Begin a new transaction
    pub fn begin_transaction(&mut self) -> Result<(), StorageError> {
        self.begin_transaction_with_durability(TransactionDurability::Default)
    }

    /// Begin a new transaction with a specific durability hint
    ///
    /// The durability hint controls how the transaction's changes are persisted.
    /// See [`TransactionDurability`] for available options.
    pub fn begin_transaction_with_durability(
        &mut self,
        durability: TransactionDurability,
    ) -> Result<(), StorageError> {
        let catalog = &self.catalog.clone();
        self.lifecycle
            .transaction_manager_mut()
            .begin_transaction_with_durability(catalog, &self.tables, durability)?;

        // Emit WAL entry for persistence
        if let Some(txn_id) = self.transaction_id() {
            self.emit_wal_op(WalOp::TxnBegin { txn_id });
        }

        Ok(())
    }

    /// Commit the current transaction
    pub fn commit_transaction(&mut self) -> Result<(), StorageError> {
        // Get transaction ID and durability hint before committing (they will be cleared after)
        let txn_id = self.transaction_id();
        let durability_hint = self.lifecycle.transaction_manager().get_durability();

        self.lifecycle.transaction_manager_mut().commit_transaction()?;

        // Emit WAL entry for persistence
        if let Some(txn_id) = txn_id {
            self.emit_wal_op(WalOp::TxnCommit { txn_id });
        }

        // Apply durability-based sync at commit time
        if let Some(hint) = durability_hint {
            let db_mode = self
                .persistence_engine
                .as_ref()
                .map(|e| e.durability_mode())
                .unwrap_or(DurabilityMode::Lazy);

            let resolved_mode = hint.resolve(db_mode);
            if resolved_mode.sync_on_commit() {
                self.sync_persistence()?;
            }
        }

        Ok(())
    }

    /// Rollback the current transaction
    pub fn rollback_transaction(&mut self) -> Result<(), StorageError> {
        // Get transaction ID before rolling back (it will be cleared after)
        let txn_id = self.transaction_id();

        self.lifecycle.perform_rollback(&mut self.catalog, &mut self.tables)?;

        // Emit WAL entry for persistence
        if let Some(txn_id) = txn_id {
            self.emit_wal_op(WalOp::TxnRollback { txn_id });
        }

        Ok(())
    }

    /// Check if we're currently in a transaction
    pub fn in_transaction(&self) -> bool {
        self.lifecycle.transaction_manager().in_transaction()
    }

    /// Get current transaction ID (for debugging)
    pub fn transaction_id(&self) -> Option<u64> {
        self.lifecycle.transaction_manager().transaction_id()
    }

    /// Create a savepoint within the current transaction
    pub fn create_savepoint(&mut self, name: String) -> Result<(), StorageError> {
        self.lifecycle.transaction_manager_mut().create_savepoint(name)
    }

    /// Rollback to a named savepoint
    pub fn rollback_to_savepoint(&mut self, name: String) -> Result<(), StorageError> {
        let changes_to_undo =
            self.lifecycle.transaction_manager_mut().rollback_to_savepoint(name)?;

        for change in changes_to_undo.into_iter().rev() {
            self.undo_change(change)?;
        }

        Ok(())
    }

    /// Undo a single transaction change
    fn undo_change(&mut self, change: TransactionChange) -> Result<(), StorageError> {
        match change {
            TransactionChange::Insert { table_name, row } => {
                let table = self
                    .get_table_mut(&table_name)
                    .ok_or_else(|| StorageError::TableNotFound(table_name.clone()))?;
                table.remove_row(&row)?;
            }
            TransactionChange::Update { table_name, old_row, new_row: _ } => {
                let table = self
                    .get_table_mut(&table_name)
                    .ok_or_else(|| StorageError::TableNotFound(table_name.clone()))?;
                table.remove_row(&old_row)?;
                table.insert(old_row)?;
            }
            TransactionChange::Delete { table_name, row } => {
                let table = self
                    .get_table_mut(&table_name)
                    .ok_or_else(|| StorageError::TableNotFound(table_name.clone()))?;
                table.insert(row)?;
            }
        }
        Ok(())
    }

    /// Release (destroy) a named savepoint
    pub fn release_savepoint(&mut self, name: String) -> Result<(), StorageError> {
        self.lifecycle.transaction_manager_mut().release_savepoint(name)
    }

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

        self.record_change(TransactionChange::Insert { table_name: table_name.to_string(), row: row.clone() });

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

    // ============================================================================
    // Query Buffer Pool
    // ============================================================================

    /// Get a reference to the query buffer pool for reusing allocations
    pub fn query_buffer_pool(&self) -> &QueryBufferPool {
        &self.query_buffer_pool
    }

    // ============================================================================
    // Procedure/Function Body Cache Methods (Phase 6 Performance)
    // ============================================================================

    /// Get cached procedure body or cache it on first access
    pub fn get_cached_procedure_body(
        &mut self,
        name: &str,
    ) -> Result<&vibesql_catalog::ProcedureBody, StorageError> {
        if self.metadata.get_cached_procedure_body(name).is_none() {
            let procedure = &self.catalog.get_procedure(name).ok_or_else(|| {
                StorageError::CatalogError(format!("Procedure '{}' not found", name))
            })?;

            self.metadata.cache_procedure_body(name.to_string(), procedure.body.clone());
        }

        Ok(self.metadata.get_cached_procedure_body(name).unwrap())
    }

    /// Invalidate cached procedure body (call when procedure is dropped or replaced)
    pub fn invalidate_procedure_cache(&mut self, name: &str) {
        self.metadata.invalidate_procedure_cache(name);
    }

    /// Clear all cached procedure/function bodies
    pub fn clear_routine_cache(&mut self) {
        self.metadata.clear_routine_cache();
    }

    // NOTE: Columnar cache methods (get_columnar, invalidate_columnar_cache, clear_columnar_cache,
    // columnar_cache_stats, etc.) are defined in cache.rs to keep cache concerns separated from core
    // database logic.

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

    // ============================================================================
    // Change Event Broadcasting (Reactive Subscriptions)
    // ============================================================================

    /// Enable change event broadcasting
    ///
    /// Creates a broadcast channel for notifying subscribers when data changes.
    /// Returns a receiver for the channel.
    ///
    /// # Arguments
    /// * `capacity` - Maximum number of events to buffer before old events are overwritten
    ///
    /// # Example
    /// ```text
    /// let mut db = Database::new();
    /// let mut rx = db.enable_change_events(1024);
    ///
    /// // Insert some data
    /// db.insert_row("users", row)?;
    ///
    /// // Receive change events
    /// for event in rx.recv_all() {
    ///     println!("Change: {:?}", event);
    /// }
    /// ```
    pub fn enable_change_events(&mut self, capacity: usize) -> ChangeEventReceiver {
        let (sender, receiver) = crate::change_events::channel(capacity);
        self.change_sender = Some(sender);
        receiver
    }

    /// Subscribe to change events
    ///
    /// Returns a new receiver for change events if broadcasting is enabled,
    /// or None if `enable_change_events()` has not been called.
    ///
    /// # Example
    /// ```text
    /// // Enable broadcasting
    /// db.enable_change_events(1024);
    ///
    /// // Create additional subscribers
    /// let rx1 = db.subscribe_changes().unwrap();
    /// let rx2 = db.subscribe_changes().unwrap();
    /// ```
    pub fn subscribe_changes(&self) -> Option<ChangeEventReceiver> {
        self.change_sender.as_ref().map(|s| s.subscribe())
    }

    /// Check if change event broadcasting is enabled
    pub fn change_events_enabled(&self) -> bool {
        self.change_sender.is_some()
    }

    /// Broadcast a change event to all subscribers (internal use)
    pub(super) fn broadcast_change(&self, event: ChangeEvent) {
        if let Some(sender) = &self.change_sender {
            let _ = sender.send(event);
        }
    }

    /// Notify subscribers of an update event
    ///
    /// This should be called by the executor after successfully updating a row.
    /// The storage layer broadcasts the event to any subscribers.
    ///
    /// # Arguments
    /// * `table_name` - Name of the table that was modified
    /// * `row_index` - Index of the row that was updated
    pub fn notify_update(&self, table_name: &str, row_index: usize) {
        self.broadcast_change(ChangeEvent::Update {
            table_name: table_name.to_string(),
            row_index,
        });
    }

    /// Notify subscribers of a delete event
    ///
    /// This should be called by the executor after successfully deleting rows.
    /// The storage layer broadcasts the event to any subscribers.
    ///
    /// # Arguments
    /// * `table_name` - Name of the table that was modified
    /// * `row_indices` - Indices of rows that were deleted (before deletion)
    pub fn notify_deletes(&self, table_name: &str, row_indices: &[usize]) {
        for &row_index in row_indices {
            self.broadcast_change(ChangeEvent::Delete {
                table_name: table_name.to_string(),
                row_index,
            });
        }
    }

    // ============================================================================
    // WAL Persistence Support
    // ============================================================================

    /// Enable WAL-based async persistence
    ///
    /// Creates a persistence engine that writes changes to a WAL file in the background.
    /// All subsequent DML and DDL operations will be logged to the WAL for durability.
    ///
    /// # Arguments
    /// * `engine` - A pre-configured PersistenceEngine instance
    ///
    /// # Example
    /// ```text
    /// use vibesql_storage::{Database, PersistenceEngine, PersistenceConfig};
    ///
    /// let mut db = Database::new();
    /// let engine = PersistenceEngine::new("/path/to/wal.log", PersistenceConfig::default())?;
    /// db.enable_persistence(engine);
    /// ```
    pub fn enable_persistence(&mut self, engine: PersistenceEngine) {
        self.persistence_engine = Some(engine);
    }

    /// Check if WAL persistence is enabled
    pub fn persistence_enabled(&self) -> bool {
        self.persistence_engine.is_some()
    }

    /// Get persistence statistics (if enabled)
    pub fn persistence_stats(&self) -> Option<crate::wal::PersistenceStats> {
        self.persistence_engine.as_ref().map(|e| e.stats())
    }

    /// Emit a WAL operation to the persistence engine (if enabled)
    ///
    /// This is a no-op if persistence is not enabled, providing zero overhead
    /// when WAL is disabled.
    pub(super) fn emit_wal_op(&self, op: WalOp) {
        if let Some(engine) = &self.persistence_engine {
            if let Err(e) = engine.send(op) {
                log::error!("Failed to emit WAL op: {}", e);
            }
        }
    }

    /// Get the next table ID and increment the counter
    pub(super) fn next_table_id(&mut self) -> u32 {
        let id = self.next_table_id;
        self.next_table_id += 1;
        id
    }

    /// Compute a table ID from table name using hash (for consistent mapping)
    ///
    /// This is used when we don't have a monotonic table ID assigned at creation time,
    /// such as for tables created before WAL was enabled.
    pub(super) fn table_name_to_id(&self, name: &str) -> u32 {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        name.hash(&mut hasher);
        hasher.finish() as u32
    }

    /// Sync all pending WAL entries to disk
    ///
    /// Blocks until all pending entries have been written and flushed.
    /// This is useful for ensuring durability before returning to the user.
    pub fn sync_persistence(&self) -> Result<(), StorageError> {
        if let Some(engine) = &self.persistence_engine {
            engine.sync()
        } else {
            Ok(())
        }
    }

    /// Emit a WAL delete entry for persistence
    ///
    /// Called by the DELETE executor before rows are removed.
    /// Captures old_values for recovery replay.
    pub fn emit_wal_delete(
        &self,
        table_name: &str,
        row_id: u64,
        old_values: Vec<vibesql_types::SqlValue>,
    ) {
        self.emit_wal_op(WalOp::Delete {
            table_id: self.table_name_to_id(table_name),
            row_id,
            old_values,
        });
    }

    /// Emit a WAL create index entry for persistence
    ///
    /// Called by the CREATE INDEX executor after index is created.
    pub fn emit_wal_create_index(
        &self,
        index_id: u32,
        index_name: &str,
        table_name: &str,
        column_indices: Vec<u32>,
        is_unique: bool,
    ) {
        self.emit_wal_op(WalOp::CreateIndex {
            index_id,
            index_name: index_name.to_string(),
            table_id: self.table_name_to_id(table_name),
            column_indices,
            is_unique,
        });
    }

    /// Emit a WAL drop index entry for persistence
    ///
    /// Called by the DROP INDEX executor before index is dropped.
    pub fn emit_wal_drop_index(&self, index_id: u32, index_name: &str) {
        self.emit_wal_op(WalOp::DropIndex { index_id, index_name: index_name.to_string() });
    }

    // ============================================================================
    // AUTO_INCREMENT / LAST_INSERT_ROWID Support
    // ============================================================================

    /// Get the last auto-generated ID from an INSERT operation
    ///
    /// Returns the most recent value generated by AUTO_INCREMENT during an INSERT.
    /// This is used to implement LAST_INSERT_ROWID() and LAST_INSERT_ID() functions.
    ///
    /// Returns 0 if no auto-generated values have been produced yet.
    ///
    /// # Example
    /// ```text
    /// // Create table with AUTO_INCREMENT
    /// db.execute("CREATE TABLE users (id INT AUTO_INCREMENT PRIMARY KEY, name VARCHAR(100))")?;
    ///
    /// // Insert a row (ID is auto-generated)
    /// db.execute("INSERT INTO users (name) VALUES ('Alice')")?;
    ///
    /// // Get the generated ID
    /// let id = db.last_insert_rowid();
    /// assert_eq!(id, 1);
    /// ```
    pub fn last_insert_rowid(&self) -> i64 {
        self.last_insert_rowid
    }

    /// Set the last auto-generated ID
    ///
    /// This is called internally by the INSERT executor when a sequence value
    /// is generated for an AUTO_INCREMENT column.
    ///
    /// For multi-row inserts, this will be the ID of the *first* row inserted
    /// (following MySQL semantics for batch inserts).
    pub fn set_last_insert_rowid(&mut self, id: i64) {
        self.last_insert_rowid = id;
    }
}

/// Serialize a TableSchema to bytes for WAL storage
///
/// Uses a simple format: JSON serialization of the schema.
/// This is for WAL recovery purposes and doesn't need to be maximally efficient.
fn serialize_table_schema(schema: &vibesql_catalog::TableSchema) -> Vec<u8> {
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

#[cfg(test)]
mod tests {
    use super::*;
    use vibesql_types::{MySqlModeFlags, SqlMode, SqlValue};

    #[test]
    fn test_set_sql_mode_changes_mode() {
        let mut db = Database::new();

        // Default is MySQL (for SQLLogicTest compatibility - dolthub corpus was regenerated against MySQL 8.x)
        assert!(matches!(db.sql_mode(), SqlMode::MySQL { .. }));

        // Change to SQLite
        db.set_sql_mode(SqlMode::SQLite);
        assert!(matches!(db.sql_mode(), SqlMode::SQLite));

        // Change back to MySQL
        db.set_sql_mode(SqlMode::MySQL { flags: MySqlModeFlags::default() });
        assert!(matches!(db.sql_mode(), SqlMode::MySQL { .. }));
    }

    #[test]
    fn test_set_sql_mode_updates_session_variable() {
        let mut db = Database::new();

        // Set to SQLite mode
        db.set_sql_mode(SqlMode::SQLite);

        // Check session variable reflects the change
        let sql_mode_var = db.get_session_variable("SQL_MODE");
        assert!(sql_mode_var.is_some());
        if let Some(SqlValue::Varchar(mode_str)) = sql_mode_var {
            assert_eq!(mode_str.as_str(), "SQLITE");
        } else {
            panic!("Expected SQL_MODE to be a Varchar");
        }
    }

    #[test]
    fn test_set_sql_mode_mysql_with_flags() {
        let mut db = Database::new();

        // Set MySQL with specific flags
        db.set_sql_mode(SqlMode::MySQL {
            flags: MySqlModeFlags {
                pipes_as_concat: true,
                ansi_quotes: true,
                strict_mode: true,
                sqlite_division_semantics: false,
            },
        });

        // Check session variable contains the flags
        let sql_mode_var = db.get_session_variable("SQL_MODE");
        assert!(sql_mode_var.is_some());
        if let Some(SqlValue::Varchar(mode_str)) = sql_mode_var {
            assert!(mode_str.contains("STRICT_TRANS_TABLES"));
            assert!(mode_str.contains("PIPES_AS_CONCAT"));
            assert!(mode_str.contains("ANSI_QUOTES"));
        } else {
            panic!("Expected SQL_MODE to be a Varchar");
        }
    }

    #[test]
    fn test_set_sql_mode_mysql_default_flags() {
        let mut db = Database::new();

        // Set MySQL with default flags (all false)
        db.set_sql_mode(SqlMode::MySQL { flags: MySqlModeFlags::default() });

        // Check session variable has default MySQL modes
        let sql_mode_var = db.get_session_variable("SQL_MODE");
        assert!(sql_mode_var.is_some());
        if let Some(SqlValue::Varchar(mode_str)) = sql_mode_var {
            // Default should include common MySQL defaults
            assert!(
                mode_str.contains("NO_ZERO_IN_DATE") || mode_str.contains("NO_ENGINE_SUBSTITUTION")
            );
        } else {
            panic!("Expected SQL_MODE to be a Varchar");
        }
    }

    #[test]
    fn test_sql_mode_affects_subsequent_queries() {
        let mut db = Database::new();

        // Start in MySQL mode (default for SQLLogicTest compatibility)
        assert!(matches!(db.sql_mode(), SqlMode::MySQL { .. }));

        // Switch to SQLite
        db.set_sql_mode(SqlMode::SQLite);

        // Verify the mode changed
        let mode = db.sql_mode();
        assert!(matches!(mode, SqlMode::SQLite));
    }

    // ============================================================================
    // Change Event Tests
    // ============================================================================

    #[test]
    fn test_change_events_disabled_by_default() {
        let db = Database::new();
        assert!(!db.change_events_enabled());
        assert!(db.subscribe_changes().is_none());
    }

    #[test]
    fn test_enable_change_events() {
        let mut db = Database::new();
        let _rx = db.enable_change_events(16);
        assert!(db.change_events_enabled());
        assert!(db.subscribe_changes().is_some());
    }

    #[test]
    fn test_insert_emits_change_event() {
        use vibesql_catalog::{ColumnSchema, TableSchema};
        use vibesql_types::DataType;

        let mut db = Database::new();
        let mut rx = db.enable_change_events(16);

        // Create a simple table
        let schema = TableSchema::new(
            "users".to_string(),
            vec![
                ColumnSchema::new("id".to_string(), DataType::Integer, false),
                ColumnSchema::new(
                    "name".to_string(),
                    DataType::Varchar { max_length: Some(50) },
                    false,
                ),
            ],
        );
        db.create_table(schema).unwrap();

        // Insert a row
        let row =
            crate::Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))]);
        db.insert_row("users", row).unwrap();

        // Verify change event was emitted
        let events = rx.recv_all();
        assert_eq!(events.len(), 1);
        match &events[0] {
            ChangeEvent::Insert { table_name, row_index } => {
                assert_eq!(*row_index, 0);
                // Table name will be "users" as passed to insert_row
                assert_eq!(table_name, "users");
            }
            _ => panic!("Expected Insert event, got {:?}", events[0]),
        }
    }

    #[test]
    fn test_batch_insert_emits_multiple_events() {
        use vibesql_catalog::{ColumnSchema, TableSchema};
        use vibesql_types::DataType;

        let mut db = Database::new();
        let mut rx = db.enable_change_events(16);

        // Create a simple table
        let schema = TableSchema::new(
            "products".to_string(),
            vec![
                ColumnSchema::new("id".to_string(), DataType::Integer, false),
                ColumnSchema::new(
                    "name".to_string(),
                    DataType::Varchar { max_length: Some(50) },
                    false,
                ),
            ],
        );
        db.create_table(schema).unwrap();

        // Insert batch of rows
        let rows = vec![
            crate::Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Product A"))]),
            crate::Row::new(vec![SqlValue::Integer(2), SqlValue::Varchar(arcstr::ArcStr::from("Product B"))]),
            crate::Row::new(vec![SqlValue::Integer(3), SqlValue::Varchar(arcstr::ArcStr::from("Product C"))]),
        ];
        db.insert_rows_batch("products", rows).unwrap();

        // Verify 3 change events were emitted
        let events = rx.recv_all();
        assert_eq!(events.len(), 3);
        for (i, event) in events.iter().enumerate() {
            assert!(matches!(event, ChangeEvent::Insert { row_index, .. } if *row_index == i));
        }
    }

    #[test]
    fn test_update_emits_change_event() {
        use vibesql_catalog::{ColumnSchema, TableSchema};
        use vibesql_types::DataType;

        let mut db = Database::new();

        // Create table with primary key
        let schema = TableSchema::with_primary_key(
            "users".to_string(),
            vec![
                ColumnSchema::new("id".to_string(), DataType::Integer, false),
                ColumnSchema::new(
                    "name".to_string(),
                    DataType::Varchar { max_length: Some(50) },
                    false,
                ),
            ],
            vec!["id".to_string()],
        );
        db.create_table(schema).unwrap();

        // Insert a row
        let row =
            crate::Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))]);
        db.insert_row("users", row).unwrap();

        // Now enable change events and update
        let mut rx = db.enable_change_events(16);

        db.update_row_by_pk(
            "users",
            SqlValue::Integer(1),
            vec![("name", SqlValue::Varchar(arcstr::ArcStr::from("Alice Smith")))],
        )
        .unwrap();

        // Verify update event was emitted
        let events = rx.recv_all();
        assert_eq!(events.len(), 1);
        assert!(matches!(&events[0], ChangeEvent::Update { row_index: 0, .. }));
    }

    #[test]
    fn test_multiple_subscribers() {
        use vibesql_catalog::{ColumnSchema, TableSchema};
        use vibesql_types::DataType;

        let mut db = Database::new();
        let mut rx1 = db.enable_change_events(16);
        let mut rx2 = db.subscribe_changes().unwrap();

        // Create table and insert
        let schema = TableSchema::new(
            "test".to_string(),
            vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
        );
        db.create_table(schema).unwrap();

        let row = crate::Row::new(vec![SqlValue::Integer(1)]);
        db.insert_row("test", row).unwrap();

        // Both receivers should get the event
        assert_eq!(rx1.recv_all().len(), 1);
        assert_eq!(rx2.recv_all().len(), 1);
    }

    #[test]
    fn test_no_panic_on_lagged_receiver() {
        use vibesql_catalog::{ColumnSchema, TableSchema};
        use vibesql_types::DataType;

        let mut db = Database::new();
        let _rx = db.enable_change_events(2); // Very small buffer

        // Create table
        let schema = TableSchema::new(
            "test".to_string(),
            vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
        );
        db.create_table(schema).unwrap();

        // Insert more rows than buffer can hold
        for i in 0..10 {
            let row = crate::Row::new(vec![SqlValue::Integer(i)]);
            db.insert_row("test", row).unwrap();
        }
        // Should not panic - lagged receivers are handled gracefully
    }

    #[test]
    fn test_notify_deletes() {
        let mut db = Database::new();
        let mut rx = db.enable_change_events(16);

        // Directly call notify_deletes (since DELETE is handled by executor)
        db.notify_deletes("users", &[0, 2, 5]);

        let events = rx.recv_all();
        assert_eq!(events.len(), 3);
        assert!(
            matches!(&events[0], ChangeEvent::Delete { table_name, row_index: 0 } if table_name == "users")
        );
        assert!(
            matches!(&events[1], ChangeEvent::Delete { table_name, row_index: 2 } if table_name == "users")
        );
        assert!(
            matches!(&events[2], ChangeEvent::Delete { table_name, row_index: 5 } if table_name == "users")
        );
    }

    #[test]
    fn test_notify_update() {
        let mut db = Database::new();
        let mut rx = db.enable_change_events(16);

        // Directly call notify_update
        db.notify_update("products", 42);

        let events = rx.recv_all();
        assert_eq!(events.len(), 1);
        assert!(
            matches!(&events[0], ChangeEvent::Update { table_name, row_index: 42 } if table_name == "products")
        );
    }

    // ============================================================================
    // WAL Persistence Tests
    // ============================================================================

    #[test]
    fn test_persistence_disabled_by_default() {
        let db = Database::new();
        assert!(!db.persistence_enabled());
        assert!(db.persistence_stats().is_none());
    }

    #[test]
    fn test_enable_persistence() {
        use std::io::Cursor;
        use crate::wal::{PersistenceConfig, PersistenceEngine};

        let mut db = Database::new();
        assert!(!db.persistence_enabled());

        // Create a persistence engine with an in-memory writer
        let buf = Vec::new();
        let cursor = Cursor::new(buf);
        let engine = PersistenceEngine::with_writer(cursor, PersistenceConfig::default()).unwrap();

        db.enable_persistence(engine);
        assert!(db.persistence_enabled());
        assert!(db.persistence_stats().is_some());
    }

    #[test]
    fn test_persistence_emits_insert_entries() {
        use std::io::Cursor;
        use vibesql_catalog::{ColumnSchema, TableSchema};
        use vibesql_types::DataType;
        use crate::wal::{PersistenceConfig, PersistenceEngine};

        let mut db = Database::new();

        // Enable persistence
        let buf = Vec::new();
        let cursor = Cursor::new(buf);
        let engine = PersistenceEngine::with_writer(cursor, PersistenceConfig::default()).unwrap();
        db.enable_persistence(engine);

        // Create a table
        let schema = TableSchema::new(
            "users".to_string(),
            vec![
                ColumnSchema::new("id".to_string(), DataType::Integer, false),
                ColumnSchema::new("name".to_string(), DataType::Varchar { max_length: Some(50) }, false),
            ],
        );
        db.create_table(schema).unwrap();

        // Insert rows
        let row1 = crate::Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("Alice"))]);
        let row2 = crate::Row::new(vec![SqlValue::Integer(2), SqlValue::Varchar(arcstr::ArcStr::from("Bob"))]);
        db.insert_row("users", row1).unwrap();
        db.insert_row("users", row2).unwrap();

        // Check stats
        let stats = db.persistence_stats().unwrap();
        // CreateTable + 2 Inserts = 3 entries
        assert_eq!(stats.entries_sent, 3);
    }

    #[test]
    fn test_persistence_emits_transaction_entries() {
        use std::io::Cursor;
        use vibesql_catalog::{ColumnSchema, TableSchema};
        use vibesql_types::DataType;
        use crate::wal::{PersistenceConfig, PersistenceEngine};

        let mut db = Database::new();

        // Enable persistence
        let buf = Vec::new();
        let cursor = Cursor::new(buf);
        let engine = PersistenceEngine::with_writer(cursor, PersistenceConfig::default()).unwrap();
        db.enable_persistence(engine);

        // Create a table
        let schema = TableSchema::new(
            "test".to_string(),
            vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
        );
        db.create_table(schema).unwrap();

        // Start transaction
        db.begin_transaction().unwrap();

        // Insert
        let row = crate::Row::new(vec![SqlValue::Integer(1)]);
        db.insert_row("test", row).unwrap();

        // Commit
        db.commit_transaction().unwrap();

        // Check stats: CreateTable + TxnBegin + Insert + TxnCommit = 4
        let stats = db.persistence_stats().unwrap();
        assert_eq!(stats.entries_sent, 4);
    }

    #[test]
    fn test_sync_persistence_no_op_when_disabled() {
        let db = Database::new();
        // Should not error when persistence is disabled
        assert!(db.sync_persistence().is_ok());
    }

    #[test]
    fn test_emit_wal_delete() {
        use std::io::Cursor;
        use crate::wal::{PersistenceConfig, PersistenceEngine};

        let mut db = Database::new();

        // Enable persistence
        let buf = Vec::new();
        let cursor = Cursor::new(buf);
        let engine = PersistenceEngine::with_writer(cursor, PersistenceConfig::default()).unwrap();
        db.enable_persistence(engine);

        // Emit delete entries
        db.emit_wal_delete("users", 0, vec![SqlValue::Integer(1)]);
        db.emit_wal_delete("users", 1, vec![SqlValue::Integer(2)]);

        // Check stats
        let stats = db.persistence_stats().unwrap();
        assert_eq!(stats.entries_sent, 2);
    }

    #[test]
    fn test_emit_wal_create_index() {
        use std::io::Cursor;
        use crate::wal::{PersistenceConfig, PersistenceEngine};

        let mut db = Database::new();

        // Enable persistence
        let buf = Vec::new();
        let cursor = Cursor::new(buf);
        let engine = PersistenceEngine::with_writer(cursor, PersistenceConfig::default()).unwrap();
        db.enable_persistence(engine);

        // Emit create index entry
        db.emit_wal_create_index(1, "idx_users_email", "users", vec![1], false);

        // Check stats
        let stats = db.persistence_stats().unwrap();
        assert_eq!(stats.entries_sent, 1);
    }

    #[test]
    fn test_emit_wal_drop_index() {
        use std::io::Cursor;
        use crate::wal::{PersistenceConfig, PersistenceEngine};

        let mut db = Database::new();

        // Enable persistence
        let buf = Vec::new();
        let cursor = Cursor::new(buf);
        let engine = PersistenceEngine::with_writer(cursor, PersistenceConfig::default()).unwrap();
        db.enable_persistence(engine);

        // Emit drop index entry
        db.emit_wal_drop_index(1, "idx_users_email");

        // Check stats
        let stats = db.persistence_stats().unwrap();
        assert_eq!(stats.entries_sent, 1);
    }

    #[test]
    fn test_emit_wal_no_op_when_disabled() {
        let db = Database::new();

        // These should be no-ops when persistence is disabled (no panic)
        db.emit_wal_delete("users", 0, vec![SqlValue::Integer(1)]);
        db.emit_wal_create_index(1, "idx", "table", vec![0], false);
        db.emit_wal_drop_index(1, "idx");

        // Persistence stats should still be None
        assert!(db.persistence_stats().is_none());
    }

    // ============================================================================
    // Transaction Durability Hint Tests
    // ============================================================================

    #[test]
    fn test_begin_transaction_with_default_durability() {
        use crate::wal::TransactionDurability;

        let mut db = Database::new();

        db.begin_transaction_with_durability(TransactionDurability::Default).unwrap();
        assert!(db.in_transaction());

        // Verify the durability hint is stored
        let durability = db.lifecycle.transaction_manager().get_durability();
        assert_eq!(durability, Some(TransactionDurability::Default));

        db.rollback_transaction().unwrap();
    }

    #[test]
    fn test_begin_transaction_with_force_durable() {
        use crate::wal::TransactionDurability;

        let mut db = Database::new();

        db.begin_transaction_with_durability(TransactionDurability::ForceDurable).unwrap();
        assert!(db.in_transaction());

        // Verify the durability hint is stored
        let durability = db.lifecycle.transaction_manager().get_durability();
        assert_eq!(durability, Some(TransactionDurability::ForceDurable));

        db.rollback_transaction().unwrap();
    }

    #[test]
    fn test_begin_transaction_with_allow_lazy() {
        use crate::wal::TransactionDurability;

        let mut db = Database::new();

        db.begin_transaction_with_durability(TransactionDurability::AllowLazy).unwrap();
        assert!(db.in_transaction());

        // Verify the durability hint is stored
        let durability = db.lifecycle.transaction_manager().get_durability();
        assert_eq!(durability, Some(TransactionDurability::AllowLazy));

        db.rollback_transaction().unwrap();
    }

    #[test]
    fn test_begin_transaction_with_force_volatile() {
        use crate::wal::TransactionDurability;

        let mut db = Database::new();

        db.begin_transaction_with_durability(TransactionDurability::ForceVolatile).unwrap();
        assert!(db.in_transaction());

        // Verify the durability hint is stored
        let durability = db.lifecycle.transaction_manager().get_durability();
        assert_eq!(durability, Some(TransactionDurability::ForceVolatile));

        db.rollback_transaction().unwrap();
    }

    #[test]
    fn test_durability_hint_cleared_on_commit() {
        use crate::wal::TransactionDurability;

        let mut db = Database::new();

        db.begin_transaction_with_durability(TransactionDurability::ForceDurable).unwrap();
        assert!(db.in_transaction());

        db.commit_transaction().unwrap();
        assert!(!db.in_transaction());

        // Durability hint should be None after commit
        let durability = db.lifecycle.transaction_manager().get_durability();
        assert_eq!(durability, None);
    }

    #[test]
    fn test_durability_hint_cleared_on_rollback() {
        use crate::wal::TransactionDurability;

        let mut db = Database::new();

        db.begin_transaction_with_durability(TransactionDurability::ForceDurable).unwrap();
        assert!(db.in_transaction());

        db.rollback_transaction().unwrap();
        assert!(!db.in_transaction());

        // Durability hint should be None after rollback
        let durability = db.lifecycle.transaction_manager().get_durability();
        assert_eq!(durability, None);
    }

    #[test]
    fn test_force_durable_triggers_sync() {
        use std::io::Cursor;
        use vibesql_catalog::{ColumnSchema, TableSchema};
        use vibesql_types::DataType;
        use crate::wal::{PersistenceConfig, PersistenceEngine, TransactionDurability};

        let mut db = Database::new();

        // Enable persistence in lazy mode (default - no sync on commit)
        let buf = Vec::new();
        let cursor = Cursor::new(buf);
        let engine = PersistenceEngine::with_writer(cursor, PersistenceConfig::lazy()).unwrap();
        db.enable_persistence(engine);

        // Create a table
        let schema = TableSchema::new(
            "test".to_string(),
            vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
        );
        db.create_table(schema).unwrap();

        // Begin transaction with ForceDurable hint
        db.begin_transaction_with_durability(TransactionDurability::ForceDurable).unwrap();

        // Insert a row
        let row = crate::Row::new(vec![SqlValue::Integer(1)]);
        db.insert_row("test", row).unwrap();

        // Commit - should trigger sync because ForceDurable overrides lazy mode
        db.commit_transaction().unwrap();

        // Check stats - explicit_flushes should have been triggered by sync
        let stats = db.persistence_stats().unwrap();
        assert!(stats.explicit_flushes >= 1, "ForceDurable should trigger an explicit flush on commit");
    }

    #[test]
    fn test_default_durability_respects_lazy_mode() {
        use std::io::Cursor;
        use vibesql_catalog::{ColumnSchema, TableSchema};
        use vibesql_types::DataType;
        use crate::wal::{PersistenceConfig, PersistenceEngine, TransactionDurability};

        let mut db = Database::new();

        // Enable persistence in lazy mode
        let buf = Vec::new();
        let cursor = Cursor::new(buf);
        let engine = PersistenceEngine::with_writer(cursor, PersistenceConfig::lazy()).unwrap();
        db.enable_persistence(engine);

        // Create a table
        let schema = TableSchema::new(
            "test".to_string(),
            vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
        );
        db.create_table(schema).unwrap();

        // Get initial stats after table creation
        let initial_stats = db.persistence_stats().unwrap();
        let initial_explicit_flushes = initial_stats.explicit_flushes;

        // Begin transaction with Default hint (should follow lazy mode - no sync on commit)
        db.begin_transaction_with_durability(TransactionDurability::Default).unwrap();

        // Insert a row
        let row = crate::Row::new(vec![SqlValue::Integer(1)]);
        db.insert_row("test", row).unwrap();

        // Commit - should NOT trigger sync in lazy mode with default durability
        db.commit_transaction().unwrap();

        // Check stats - no new explicit_flushes should have been triggered
        let final_stats = db.persistence_stats().unwrap();
        assert_eq!(
            final_stats.explicit_flushes,
            initial_explicit_flushes,
            "Default durability in lazy mode should not trigger explicit flush on commit"
        );
    }

    #[test]
    fn test_durability_hint_no_panic_without_persistence() {
        use crate::wal::TransactionDurability;

        // Create database WITHOUT persistence enabled
        let mut db = Database::new();

        // Begin transaction with ForceDurable hint
        db.begin_transaction_with_durability(TransactionDurability::ForceDurable).unwrap();
        assert!(db.in_transaction());

        // Commit should not panic even though ForceDurable requests sync
        // (sync is a no-op when persistence is not enabled)
        db.commit_transaction().unwrap();
        assert!(!db.in_transaction());
    }

    #[test]
    fn test_allow_lazy_downgrades_durable_mode() {
        use std::io::Cursor;
        use vibesql_catalog::{ColumnSchema, TableSchema};
        use vibesql_types::DataType;
        use crate::wal::{PersistenceConfig, PersistenceEngine, TransactionDurability};

        let mut db = Database::new();

        // Enable persistence in durable mode (sync on every commit by default)
        let buf = Vec::new();
        let cursor = Cursor::new(buf);
        let engine = PersistenceEngine::with_writer(cursor, PersistenceConfig::durable()).unwrap();
        db.enable_persistence(engine);

        // Create a table
        let schema = TableSchema::new(
            "test".to_string(),
            vec![ColumnSchema::new("id".to_string(), DataType::Integer, false)],
        );
        db.create_table(schema).unwrap();

        // Get initial stats after table creation
        let initial_stats = db.persistence_stats().unwrap();
        let initial_explicit_flushes = initial_stats.explicit_flushes;

        // Begin transaction with AllowLazy hint (should downgrade durable to lazy)
        db.begin_transaction_with_durability(TransactionDurability::AllowLazy).unwrap();

        // Insert a row
        let row = crate::Row::new(vec![SqlValue::Integer(1)]);
        db.insert_row("test", row).unwrap();

        // Commit - AllowLazy should prevent sync even in durable mode
        db.commit_transaction().unwrap();

        // Check stats - no new explicit_flushes should have been triggered
        let final_stats = db.persistence_stats().unwrap();
        assert_eq!(
            final_stats.explicit_flushes,
            initial_explicit_flushes,
            "AllowLazy should downgrade durable mode and not trigger explicit flush on commit"
        );
    }
}
