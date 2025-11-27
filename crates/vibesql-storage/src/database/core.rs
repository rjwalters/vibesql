// ============================================================================
// Database - Coordinates between focused modules
// ============================================================================

use super::lifecycle::Lifecycle;
use super::metadata::Metadata;
use super::operations::Operations;
use super::transactions::TransactionChange;
use crate::columnar_cache::ColumnarCache;
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
        let catalog = &self.catalog.clone();
        self.lifecycle
            .transaction_manager_mut()
            .begin_transaction(catalog, &self.tables)
    }

    /// Commit the current transaction
    pub fn commit_transaction(&mut self) -> Result<(), StorageError> {
        self.lifecycle.transaction_manager_mut().commit_transaction()
    }

    /// Rollback the current transaction
    pub fn rollback_transaction(&mut self) -> Result<(), StorageError> {
        self.lifecycle.perform_rollback(&mut self.catalog, &mut self.tables)
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
        let changes_to_undo = self.lifecycle.transaction_manager_mut().rollback_to_savepoint(name)?;

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

        self.operations
            .create_table(&mut self.catalog, schema.clone())?;

        // Normalize table name for storage (matches catalog normalization)
        let normalized_table_name = if self.catalog.is_case_sensitive_identifiers() {
            table_name.clone()
        } else {
            table_name.to_uppercase()
        };

        let current_schema = &self.catalog.get_current_schema();
        let qualified_name = format!("{}.{}", current_schema, normalized_table_name);

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
        let normalized_name = name.to_uppercase();
        if normalized_name != name {
            if let Some(table) = self.tables.get(&normalized_name) {
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
            let qualified_name_normalized = format!("{}.{}", current_schema, normalized_name);
            if qualified_name_normalized != qualified_name_original {
                return self.tables.get(&qualified_name_normalized);
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
        let normalized_name = name.to_uppercase();
        if normalized_name != name && self.tables.contains_key(&normalized_name) {
            return self.tables.get_mut(&normalized_name);
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
            let qualified_name_normalized = format!("{}.{}", current_schema, normalized_name);
            if qualified_name_normalized != qualified_name_original {
                return self.tables.get_mut(&qualified_name_normalized);
            }
        }

        None
    }

    /// Drop a table
    pub fn drop_table(&mut self, name: &str) -> Result<(), StorageError> {
        // Invalidate columnar cache before dropping
        self.columnar_cache.invalidate(name);
        self.operations.drop_table(&mut self.catalog, &mut self.tables, name)
    }

    /// Insert a row into a table
    pub fn insert_row(&mut self, table_name: &str, row: Row) -> Result<(), StorageError> {
        let _row_index = self.operations.insert_row(
            &self.catalog,
            &mut self.tables,
            table_name,
            row.clone(),
        )?;

        self.record_change(TransactionChange::Insert {
            table_name: table_name.to_string(),
            row,
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
    /// ```rust,ignore
    /// let rows = vec![
    ///     Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar("Alice".into())]),
    ///     Row::new(vec![SqlValue::Integer(2), SqlValue::Varchar("Bob".into())]),
    /// ];
    /// let count = db.insert_rows_batch("users", rows)?;
    /// ```
    pub fn insert_rows_batch(&mut self, table_name: &str, rows: Vec<Row>) -> Result<usize, StorageError> {
        if rows.is_empty() {
            return Ok(0);
        }

        let row_indices = self.operations.insert_rows_batch(
            &self.catalog,
            &mut self.tables,
            table_name,
            rows.clone(),
        )?;

        // Record changes for transaction management
        for row in rows {
            self.record_change(TransactionChange::Insert {
                table_name: table_name.to_string(),
                row,
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
    /// ```rust,ignore
    /// // Stream 100K rows in batches of 5000
    /// let rows = (0..100_000).map(|i| Row::new(vec![SqlValue::Integer(i)]));
    /// let count = db.insert_rows_iter("numbers", rows, 5000)?;
    /// ```
    pub fn insert_rows_iter<I>(&mut self, table_name: &str, rows: I, batch_size: usize) -> Result<usize, StorageError>
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use vibesql_types::{MySqlModeFlags, SqlMode, SqlValue};

    #[test]
    fn test_set_sql_mode_changes_mode() {
        let mut db = Database::new();

        // Default is SQLite (for SQLLogicTest compatibility)
        assert!(matches!(db.sql_mode(), SqlMode::SQLite));

        // Change to MySQL
        db.set_sql_mode(SqlMode::MySQL {
            flags: MySqlModeFlags::default(),
        });
        assert!(matches!(db.sql_mode(), SqlMode::MySQL { .. }));

        // Change back to SQLite
        db.set_sql_mode(SqlMode::SQLite);
        assert!(matches!(db.sql_mode(), SqlMode::SQLite));
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
            assert_eq!(mode_str, "SQLITE");
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
        db.set_sql_mode(SqlMode::MySQL {
            flags: MySqlModeFlags::default(),
        });

        // Check session variable has default MySQL modes
        let sql_mode_var = db.get_session_variable("SQL_MODE");
        assert!(sql_mode_var.is_some());
        if let Some(SqlValue::Varchar(mode_str)) = sql_mode_var {
            // Default should include common MySQL defaults
            assert!(mode_str.contains("NO_ZERO_IN_DATE") || mode_str.contains("NO_ENGINE_SUBSTITUTION"));
        } else {
            panic!("Expected SQL_MODE to be a Varchar");
        }
    }

    #[test]
    fn test_sql_mode_affects_subsequent_queries() {
        let mut db = Database::new();

        // Start in SQLite mode (default for SQLLogicTest compatibility)
        assert!(matches!(db.sql_mode(), SqlMode::SQLite));

        // Switch to MySQL
        db.set_sql_mode(SqlMode::MySQL {
            flags: MySqlModeFlags::default(),
        });

        // Verify the mode is available for query execution
        let mode = db.sql_mode();
        assert!(matches!(mode, SqlMode::MySQL { .. }));
    }
}
