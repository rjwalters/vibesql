// ============================================================================
// Database - Core struct definition and coordination
// ============================================================================
//
// The Database struct is the main entry point for database operations.
// Methods are organized into focused modules:
// - transaction_api.rs: Transaction management (begin, commit, rollback, savepoints)
// - table_api.rs: Table operations (create, drop, insert, update)
// - point_lookup.rs: High-performance point lookups by primary key
// - change_events_api.rs: Reactive change event broadcasting
// - persistence_api.rs: WAL persistence and AUTO_INCREMENT support
// - cache.rs: Columnar cache management
// - session.rs: Session variables and SQL mode
// - constructors.rs: Database creation and configuration

use std::collections::HashMap;
#[allow(unused_imports)]
use std::sync::atomic::{AtomicU64, Ordering};
#[allow(unused_imports)]
use std::sync::Arc;

pub use super::operations::SpatialIndexMetadata as ExportedSpatialIndexMetadata;
use super::{lifecycle::Lifecycle, metadata::Metadata, operations::Operations};
use crate::{
    change_events::ChangeEventSender, columnar_cache::ColumnarCache, wal::PersistenceEngine,
    QueryBufferPool, Table,
};

/// In-memory database - manages catalog and tables through focused modules
///
/// The Database struct coordinates between multiple internal modules to provide
/// a complete database implementation. Each aspect of database functionality
/// is organized into a focused module:
///
/// - **Transaction management**: `begin_transaction()`, `commit_transaction()`,
///   `rollback_transaction()`, `create_savepoint()`, `rollback_to_savepoint()`
/// - **Table operations**: `create_table()`, `drop_table()`, `get_table()`,
///   `insert_row()`, `insert_rows_batch()`, `update_row_by_pk()`
/// - **Point lookups**: `get_row_by_pk()`, `get_column_by_pk()`,
///   `get_row_by_composite_pk()`
/// - **Change events**: `enable_change_events()`, `subscribe_changes()`,
///   `notify_update()`, `notify_deletes()`
/// - **Persistence**: `enable_persistence()`, `sync_persistence()`,
///   `last_insert_rowid()`
/// - **Caching**: `get_columnar()`, `invalidate_columnar_cache()`,
///   `columnar_cache_stats()`
/// - **Session**: `set_sql_mode()`, `sql_mode()`, `get_session_variable()`,
///   `set_session_variable()`
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
    /// Number of rows changed by the last INSERT/UPDATE/DELETE statement
    /// Used by the changes() function for SQLite compatibility
    pub(super) last_changes_count: usize,
    /// Total number of rows changed since the database connection was opened
    /// Used by the total_changes() function for SQLite compatibility
    pub(super) total_changes_count: usize,
    /// Search count for sqlite_search_count() compatibility
    /// Tracks the number of rows examined during query execution
    /// Used by SQLite TCL tests to verify query optimization behavior
    pub(super) search_count: AtomicU64,
    /// Optional persistence engine for WAL-based async persistence
    /// Enables durable storage when enabled
    pub(super) persistence_engine: Option<PersistenceEngine>,
    /// Next table ID to assign (for WAL table_id tracking)
    pub(super) next_table_id: u32,
    /// Reserved rowids for REPLACE operations (SQLite semantics)
    /// During REPLACE, the rowid for the new row is allocated BEFORE firing
    /// BEFORE DELETE triggers. Any INSERT within those triggers that tries
    /// to allocate the same rowid will fail with a UNIQUE constraint violation.
    /// Maps table name to (reserved_rowid, is_explicit).
    /// - is_explicit: true if the rowid comes from an explicit INTEGER PRIMARY KEY value
    ///   in the REPLACE statement, false if it's auto-allocated.
    pub(super) reserved_rowids: HashMap<String, (u64, bool)>,
}

impl Database {
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
    ) -> Result<&vibesql_catalog::ProcedureBody, crate::StorageError> {
        if self.metadata.get_cached_procedure_body(name).is_none() {
            let procedure = &self.catalog.get_procedure(name).ok_or_else(|| {
                crate::StorageError::CatalogError(format!("Procedure '{}' not found", name))
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

    /// Capture the copy-on-write `Operations` rollback snapshot before a
    /// mutation, if a transaction is active (#5419).
    ///
    /// This is the **single chokepoint** for index/spatial-index mutation:
    /// every code path that mutates `self.operations` must call this first
    /// (it is invoked implicitly by [`Self::operations_mut`]). The lazy
    /// `Operations` rollback snapshot (see
    /// [`TransactionManager::ensure_operations_snapshot`]) is taken before
    /// the *first* mutation inside a transaction, so it captures committed,
    /// pre-mutation state and ROLLBACK restores it exactly — preserving the
    /// #5413 correctness fix while skipping the clone entirely for
    /// read-only transactions. Outside a transaction it is a cheap no-op.
    ///
    /// `ensure_operations_snapshot` borrows `self.operations` immutably and
    /// `self.lifecycle` mutably; these are disjoint fields, so the call
    /// compiles even though both are reached through `self`.
    ///
    /// [`TransactionManager::ensure_operations_snapshot`]: crate::database::transactions::TransactionManager::ensure_operations_snapshot
    #[inline]
    pub(super) fn snapshot_operations_for_mutation(&mut self) {
        let before = self.lifecycle.transaction_manager().operations_snapshot_clones();
        self.lifecycle.transaction_manager_mut().ensure_operations_snapshot(&self.operations);
        let after = self.lifecycle.transaction_manager().operations_snapshot_clones();

        // #5425: A *fresh* snapshot was just taken for this transaction (the
        // clone counter advanced), meaning this is the first index mutation of
        // the transaction. The copy-on-write snapshot restores in-memory index
        // state on ROLLBACK, but it shares the *same* `Arc<Mutex<BTreeIndex>>`
        // as any spilled (disk-backed) index, so shallow-clone restore is a
        // no-op for those. Arm a per-tree undo-log now so ROLLBACK can reverse
        // disk-backed mutations and COMMIT can discard the log.
        if after != before {
            self.operations.begin_disk_undo_logging();
        }
    }

    // NOTE: Other method groups are defined in their respective modules:
    // - Transaction methods: transaction_api.rs
    // - Table methods: table_api.rs
    // - Point lookup methods: point_lookup.rs
    // - Change event methods: change_events_api.rs
    // - Persistence methods: persistence_api.rs
    // - Cache methods: cache.rs
    // - Session methods: session.rs
    // - Constructor: constructors.rs
}
