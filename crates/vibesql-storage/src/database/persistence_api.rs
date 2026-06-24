// ============================================================================
// WAL Persistence API
// ============================================================================
//
// This module provides WAL (Write-Ahead Log) persistence methods for the
// Database struct. Enables durable storage through async persistence.

use super::Database;
use crate::wal::{PersistenceEngine, WalOp};
use crate::StorageError;

impl Database {
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

    /// Get the next WAL log-sequence-number the engine will assign (if enabled).
    ///
    /// Returns `None` when persistence is not enabled. Used by the CLI to stamp
    /// a checkpoint at the current LSN so the WAL can be truncated up to it.
    pub fn persistence_next_lsn(&self) -> Option<crate::wal::Lsn> {
        self.persistence_engine.as_ref().map(|e| e.next_lsn())
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
            table_name: table_name.to_string(),
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

    // ============================================================================
    // changes() Support (Row Modification Count)
    // ============================================================================

    /// Get the number of rows changed by the last INSERT/UPDATE/DELETE statement
    ///
    /// Returns the count of rows affected by the most recent DML operation.
    /// This is used to implement the SQLite changes() function.
    ///
    /// Returns 0 if no DML operations have been performed yet.
    ///
    /// # Example
    /// ```text
    /// // Insert multiple rows
    /// db.execute("INSERT INTO users (name) VALUES ('Alice'), ('Bob'), ('Carol')")?;
    ///
    /// // Get the number of rows inserted
    /// let changes = db.last_changes_count();
    /// assert_eq!(changes, 3);
    ///
    /// // Delete some rows
    /// db.execute("DELETE FROM users WHERE name = 'Alice'")?;
    /// assert_eq!(db.last_changes_count(), 1);
    /// ```
    pub fn last_changes_count(&self) -> usize {
        self.last_changes_count
    }

    /// Set the number of rows changed by the last DML statement
    ///
    /// This is called internally by INSERT, UPDATE, and DELETE executors
    /// after completing their operations.
    pub fn set_last_changes_count(&mut self, count: usize) {
        self.last_changes_count = count;
    }

    // ============================================================================
    // total_changes() Support (Cumulative Row Modification Count)
    // ============================================================================

    /// Get the total number of rows changed since the database connection was opened
    ///
    /// Returns the cumulative count of rows affected by all INSERT, UPDATE, and DELETE
    /// operations since the database was created. This is used to implement the
    /// SQLite total_changes() function.
    ///
    /// Returns 0 for a new database connection.
    ///
    /// # Example
    /// ```text
    /// // Insert rows
    /// db.execute("INSERT INTO users (name) VALUES ('Alice'), ('Bob')")?;
    /// assert_eq!(db.last_changes_count(), 2);  // Last operation: 2 rows
    ///
    /// // Delete a row
    /// db.execute("DELETE FROM users WHERE name = 'Alice'")?;
    /// assert_eq!(db.last_changes_count(), 1);  // Last operation: 1 row
    ///
    /// // Total changes accumulates
    /// assert_eq!(db.total_changes_count(), 3); // 2 + 1 = 3 rows total
    /// ```
    pub fn total_changes_count(&self) -> usize {
        self.total_changes_count
    }

    /// Increment the total changes count by the specified amount
    ///
    /// This is called internally by INSERT, UPDATE, and DELETE executors
    /// after completing their operations, in addition to set_last_changes_count().
    pub fn increment_total_changes_count(&mut self, count: usize) {
        self.total_changes_count += count;
    }

    // ============================================================================
    // sqlite_search_count Support (TCL Test Compatibility)
    // ============================================================================

    /// Get the current search count
    ///
    /// Returns the number of rows examined during query execution.
    /// This is used to implement sqlite_search_count() for TCL test compatibility.
    ///
    /// In SQLite, this tracks "MoveTo" and "Next" VDBE operations.
    /// In VibeSQL, this tracks rows read during table/index scans.
    ///
    /// # Example
    /// ```text
    /// // Reset before query
    /// db.reset_search_count();
    ///
    /// // Execute query...
    /// db.execute("SELECT * FROM users WHERE id = 1")?;
    ///
    /// // Get count of rows examined
    /// let count = db.search_count();
    /// ```
    pub fn search_count(&self) -> u64 {
        self.search_count.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Reset the search count to zero
    ///
    /// Call this before executing a query to measure how many rows
    /// were examined by that specific query.
    pub fn reset_search_count(&self) {
        self.search_count.store(0, std::sync::atomic::Ordering::Relaxed);
    }

    /// Increment the search count by a specified amount
    ///
    /// Called internally by the executor when rows are examined during
    /// table scans, index scans, or other row-reading operations.
    ///
    /// # Arguments
    /// * `count` - Number of rows examined (typically 1 for row-by-row, or batch size for columnar)
    pub fn increment_search_count(&self, count: u64) {
        self.search_count.fetch_add(count, std::sync::atomic::Ordering::Relaxed);
    }
}
