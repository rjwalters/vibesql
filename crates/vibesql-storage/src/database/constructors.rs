// ============================================================================
// Database Constructors and Initialization
// ============================================================================

#![allow(clippy::clone_on_copy)]

use super::config::{DatabaseConfig, DEFAULT_COLUMNAR_CACHE_BUDGET};
use super::lifecycle::Lifecycle;
use super::metadata::Metadata;
use super::operations::Operations;
use crate::columnar_cache::ColumnarCache;
use crate::QueryBufferPool;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use super::core::Database;

impl Clone for Database {
    fn clone(&self) -> Self {
        Database {
            catalog: self.catalog.clone(),
            lifecycle: self.lifecycle.clone(),
            metadata: self.metadata.clone(),
            operations: self.operations.clone(),
            tables: self.tables.clone(),
            sql_mode: self.sql_mode.clone(),
            query_buffer_pool: self.query_buffer_pool.clone(),
            // Clone creates a new cache with same config but empty data
            // This is intentional - cloned databases shouldn't share cache state
            columnar_cache: Arc::new(ColumnarCache::new(self.columnar_cache.max_memory())),
            // Clone does not inherit change event sender - cloned databases are independent
            change_sender: None,
            // Clone resets last_insert_rowid - each database instance tracks independently
            last_insert_rowid: 0,
            // Clone does not inherit persistence engine - cloned databases are independent
            persistence_engine: None,
            // Preserve table ID counter for consistency
            next_table_id: self.next_table_id,
        }
    }
}

impl Database {
    /// Create a new empty database
    ///
    /// Note: Security is disabled by default for backward compatibility with existing code.
    /// Call `enable_security()` to turn on access control enforcement.
    pub fn new() -> Self {
        Database {
            catalog: vibesql_catalog::Catalog::new(),
            lifecycle: Lifecycle::new(),
            metadata: Metadata::new(),
            operations: Operations::new(),
            tables: HashMap::new(),
            sql_mode: vibesql_types::SqlMode::default(),
            query_buffer_pool: QueryBufferPool::new(),
            columnar_cache: Arc::new(ColumnarCache::new(DEFAULT_COLUMNAR_CACHE_BUDGET)),
            change_sender: None,
            last_insert_rowid: 0,
            persistence_engine: None,
            next_table_id: 1,
        }
    }

    /// Create a new database with a specific storage path
    ///
    /// The provided path will be used as the root directory for database files.
    /// Index files will be stored in `<path>/data/indexes/`.
    ///
    /// # Example
    /// ```rust
    /// use std::path::PathBuf;
    /// use vibesql_storage::Database;
    ///
    /// let db = Database::with_path(PathBuf::from("/var/lib/myapp/db"));
    /// // Index files will be stored in /var/lib/myapp/db/data/indexes/
    /// ```
    pub fn with_path(path: PathBuf) -> Self {
        let mut db = Self::new();
        db.operations.set_database_path(path.join("data"));
        db
    }

    /// Create a new database with a specific configuration
    ///
    /// Allows setting memory budgets, disk budgets, and spill policy for adaptive
    /// index management.
    ///
    /// # Example
    /// ```rust
    /// use vibesql_storage::{Database, DatabaseConfig};
    ///
    /// // Browser environment with limited memory
    /// let db = Database::with_config(DatabaseConfig::browser_default());
    ///
    /// // Server environment with abundant memory
    /// let db = Database::with_config(DatabaseConfig::server_default());
    /// ```
    pub fn with_config(config: DatabaseConfig) -> Self {
        let columnar_cache_budget = config.columnar_cache_budget;
        let mut db = Self::new();
        db.sql_mode = config.sql_mode.clone();
        db.columnar_cache = Arc::new(ColumnarCache::new(columnar_cache_budget));
        db.operations.set_config(config);
        db
    }

    /// Create a new database with both path and configuration
    ///
    /// # Example
    /// ```rust
    /// use std::path::PathBuf;
    /// use vibesql_storage::{Database, DatabaseConfig};
    ///
    /// let db = Database::with_path_and_config(
    ///     PathBuf::from("/var/lib/myapp/db"),
    ///     DatabaseConfig::server_default()
    /// );
    /// ```
    pub fn with_path_and_config(path: PathBuf, config: DatabaseConfig) -> Self {
        let columnar_cache_budget = config.columnar_cache_budget;
        let mut db = Self::new();
        db.sql_mode = config.sql_mode.clone();
        db.columnar_cache = Arc::new(ColumnarCache::new(columnar_cache_budget));
        db.operations.set_database_path(path.join("data"));
        db.operations.set_config(config);
        db
    }

    /// Create a new database with both path and configuration (async version for WASM)
    ///
    /// This async version is required for WASM to properly initialize OPFS storage
    /// without blocking the event loop.
    ///
    /// # Example
    /// ```rust,no_run
    /// # use std::path::PathBuf;
    /// # use vibesql_storage::{Database, DatabaseConfig};
    /// # async fn example() {
    /// let db = Database::with_path_and_config_async(
    ///     PathBuf::from("/vibesql-data"),
    ///     DatabaseConfig::browser_default()
    /// ).await.unwrap();
    /// # }
    /// ```
    #[cfg(target_arch = "wasm32")]
    pub async fn with_path_and_config_async(
        path: PathBuf,
        config: DatabaseConfig,
    ) -> Result<Self, crate::StorageError> {
        let columnar_cache_budget = config.columnar_cache_budget;
        let mut db = Self::new();
        db.sql_mode = config.sql_mode.clone();
        db.columnar_cache = Arc::new(ColumnarCache::new(columnar_cache_budget));
        db.operations.set_database_path(path.join("data"));
        db.operations.set_config(config);

        // Initialize OPFS storage asynchronously
        db.operations.init_opfs_async().await?;

        Ok(db)
    }

    /// Reset the database to empty state (more efficient than creating a new instance).
    ///
    /// Clears all tables, resets catalog to default state, and clears all indexes and transactions.
    /// Useful for test scenarios where you need to reuse a Database instance.
    /// Preserves database configuration (path, storage backend, memory budgets) across resets.
    /// Note: Persistence engine is preserved (WAL remains active if enabled).
    pub fn reset(&mut self) {
        self.catalog = vibesql_catalog::Catalog::new();
        self.lifecycle.reset();
        self.metadata = Metadata::new();

        // Reset operations in place to preserve database_path, storage backend, and config
        self.operations.reset();

        self.tables.clear();

        // Clear the columnar cache
        self.columnar_cache.clear();

        // Reset table ID counter
        self.next_table_id = 1;
    }
}

impl Default for Database {
    fn default() -> Self {
        Self::new()
    }
}
