// ============================================================================
// Transaction Management
// ============================================================================

use std::collections::HashMap;

use crate::{wal::TransactionDurability, Row, StorageError, Table};

/// A single change made during a transaction
#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum TransactionChange {
    Insert { table_name: String, row: Row },
    Update { table_name: String, old_row: Row, new_row: Row },
    Delete { table_name: String, row: Row },
}

/// A foreign-key violation that has been deferred until COMMIT.
///
/// Phase C2 of #5085 introduces this queue: when a FK constraint is
/// `DEFERRABLE INITIALLY DEFERRED` (or the session has
/// `PRAGMA defer_foreign_keys=ON`), child-side INSERT/UPDATE/DELETE
/// validators push a `DeferredFkViolation` instead of failing eagerly.
/// The queue is drained at COMMIT time and each entry is re-checked
/// against the current parent state; commit fails if any violation
/// still holds.
#[derive(Debug, Clone)]
pub struct DeferredFkViolation {
    /// The child table whose row may violate the FK.
    pub child_table: String,
    /// Index of the FK constraint within `child_table`'s schema.
    pub fk_index: usize,
    /// The child row values at the time the violation was queued.
    /// Stored as a flat `Vec<SqlValue>` (rather than `Row`) because the
    /// commit-time re-check only needs the raw FK column values.
    pub child_row: Vec<vibesql_types::SqlValue>,
    /// True when the source operation was a DELETE/UPDATE on the parent
    /// (referential-action queue entry). False when the source was an
    /// INSERT/UPDATE on the child (existence-check queue entry). The
    /// commit-time drain treats these symmetrically: parent-side entries
    /// look for *any* surviving child row that still references missing
    /// parent values; child-side entries look up the child row by FK
    /// values. Phase C2 only emits child-side entries — parent-side
    /// queueing remains immediate (RESTRICT/NO ACTION) and is left to
    /// Phase C3.
    pub kind: DeferredFkViolationKind,
}

/// Origin of a deferred FK violation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeferredFkViolationKind {
    /// Child INSERT or UPDATE created a row whose FK columns do not
    /// (yet) match any parent row.
    ChildInsertOrUpdate,
}

/// A savepoint within a transaction
#[derive(Debug, Clone)]
pub struct Savepoint {
    pub name: String,
    /// Index in the changes vector where this savepoint was created
    pub snapshot_index: usize,
    /// Index in the deferred FK violation queue where this savepoint was
    /// created. ROLLBACK TO truncates the queue back to this index so
    /// that violations queued after the savepoint are discarded.
    pub deferred_fk_snapshot_index: usize,
}

/// Transaction state
#[derive(Debug, Clone)]
#[allow(clippy::large_enum_variant)]
pub enum TransactionState {
    /// No active transaction
    None,
    /// Transaction is active
    Active {
        /// Transaction ID for debugging
        id: u64,
        /// Original catalog snapshot for full rollback
        original_catalog: vibesql_catalog::Catalog,
        /// Original table snapshots for full rollback
        original_tables: HashMap<String, Table>,
        /// Stack of savepoints (newest at end)
        savepoints: Vec<Savepoint>,
        /// All changes made since transaction start (for incremental undo)
        changes: Vec<TransactionChange>,
        /// Durability hint for this transaction
        durability: TransactionDurability,
        /// Queue of deferred FK violations to re-check at COMMIT.
        ///
        /// See [`DeferredFkViolation`]. ROLLBACK clears this implicitly
        /// by replacing the entire transaction state; `ROLLBACK TO`
        /// truncates back to the savepoint's `deferred_fk_snapshot_index`.
        deferred_fk_violations: Vec<DeferredFkViolation>,
    },
}

/// Transaction manager - handles all transaction lifecycle and savepoint operations
#[derive(Debug, Clone)]
pub struct TransactionManager {
    /// Current transaction state
    transaction_state: TransactionState,
    /// Next transaction ID
    next_transaction_id: u64,
}

impl TransactionManager {
    /// Create a new transaction manager
    pub fn new() -> Self {
        TransactionManager { transaction_state: TransactionState::None, next_transaction_id: 1 }
    }

    /// Record a change in the current transaction (if any)
    pub fn record_change(&mut self, change: TransactionChange) {
        if let TransactionState::Active { changes, .. } = &mut self.transaction_state {
            changes.push(change);
        }
    }

    /// Begin a new transaction with default durability
    pub fn begin_transaction(
        &mut self,
        catalog: &vibesql_catalog::Catalog,
        tables: &HashMap<String, Table>,
    ) -> Result<(), StorageError> {
        self.begin_transaction_with_durability(catalog, tables, TransactionDurability::Default)
    }

    /// Begin a new transaction with a specific durability hint
    pub fn begin_transaction_with_durability(
        &mut self,
        catalog: &vibesql_catalog::Catalog,
        tables: &HashMap<String, Table>,
        durability: TransactionDurability,
    ) -> Result<(), StorageError> {
        match self.transaction_state {
            TransactionState::None => {
                // Create snapshots of catalog and all current tables
                let original_catalog = catalog.clone();
                let original_tables = tables.clone();

                let transaction_id = self.next_transaction_id;
                self.next_transaction_id += 1;

                self.transaction_state = TransactionState::Active {
                    id: transaction_id,
                    original_catalog,
                    original_tables,
                    savepoints: Vec::new(),
                    changes: Vec::new(),
                    durability,
                    deferred_fk_violations: Vec::new(),
                };
                Ok(())
            }
            TransactionState::Active { .. } => {
                Err(StorageError::TransactionError("Transaction already active".to_string()))
            }
        }
    }

    /// Commit the current transaction
    pub fn commit_transaction(&mut self) -> Result<(), StorageError> {
        match self.transaction_state {
            TransactionState::None => {
                Err(StorageError::TransactionError("No active transaction to commit".to_string()))
            }
            TransactionState::Active { .. } => {
                // Transaction committed - just clear the state
                // Changes are already in the tables
                self.transaction_state = TransactionState::None;
                Ok(())
            }
        }
    }

    /// Rollback the current transaction
    pub fn rollback_transaction(
        &mut self,
        catalog: &mut vibesql_catalog::Catalog,
        tables: &mut HashMap<String, Table>,
    ) -> Result<(), StorageError> {
        match &self.transaction_state {
            TransactionState::None => {
                Err(StorageError::TransactionError("No active transaction to rollback".to_string()))
            }
            TransactionState::Active { original_catalog, original_tables, .. } => {
                // Restore catalog and all tables from snapshots
                *catalog = original_catalog.clone();
                *tables = original_tables.clone();
                self.transaction_state = TransactionState::None;
                Ok(())
            }
        }
    }

    /// Check if we're currently in a transaction
    pub fn in_transaction(&self) -> bool {
        matches!(self.transaction_state, TransactionState::Active { .. })
    }

    /// Get current transaction ID (for debugging)
    pub fn transaction_id(&self) -> Option<u64> {
        match &self.transaction_state {
            TransactionState::Active { id, .. } => Some(*id),
            TransactionState::None => None,
        }
    }

    /// Get the durability hint for the current transaction (if any)
    pub fn get_durability(&self) -> Option<TransactionDurability> {
        match &self.transaction_state {
            TransactionState::Active { durability, .. } => Some(*durability),
            TransactionState::None => None,
        }
    }

    /// Create a savepoint within the current transaction
    pub fn create_savepoint(&mut self, name: String) -> Result<(), StorageError> {
        match &mut self.transaction_state {
            TransactionState::None => {
                Err(StorageError::TransactionError("No active transaction".to_string()))
            }
            TransactionState::Active { savepoints, changes, deferred_fk_violations, .. } => {
                let savepoint = Savepoint {
                    name,
                    snapshot_index: changes.len(),
                    deferred_fk_snapshot_index: deferred_fk_violations.len(),
                };
                savepoints.push(savepoint);
                Ok(())
            }
        }
    }

    /// Rollback to a named savepoint - returns the changes that need to be undone
    pub fn rollback_to_savepoint(
        &mut self,
        name: String,
    ) -> Result<Vec<TransactionChange>, StorageError> {
        match &mut self.transaction_state {
            TransactionState::None => {
                Err(StorageError::TransactionError("No active transaction".to_string()))
            }
            TransactionState::Active { savepoints, changes, deferred_fk_violations, .. } => {
                // Find the savepoint
                let savepoint_idx =
                    savepoints.iter().position(|sp| sp.name == name).ok_or_else(|| {
                        StorageError::TransactionError(format!("Savepoint '{}' not found", name))
                    })?;

                let snapshot_index = savepoints[savepoint_idx].snapshot_index;
                let deferred_fk_snapshot_index =
                    savepoints[savepoint_idx].deferred_fk_snapshot_index;

                // Collect changes to undo (from snapshot_index to end)
                let changes_to_undo: Vec<_> = changes.drain(snapshot_index..).collect();

                // Discard deferred FK violations queued after the savepoint.
                // Any violation pushed since the savepoint is rolled back along
                // with the underlying child mutation.
                deferred_fk_violations.truncate(deferred_fk_snapshot_index);

                // Destroy later savepoints
                savepoints.truncate(savepoint_idx + 1);

                Ok(changes_to_undo)
            }
        }
    }

    /// Push a deferred FK violation onto the active transaction's queue.
    ///
    /// Silently ignores the call when no transaction is active — the
    /// session-level `defer_foreign_keys=ON` semantics only apply inside
    /// an explicit transaction.
    pub fn queue_deferred_fk_violation(&mut self, violation: DeferredFkViolation) {
        if let TransactionState::Active { deferred_fk_violations, .. } = &mut self.transaction_state
        {
            deferred_fk_violations.push(violation);
        }
    }

    /// Drain the deferred FK violation queue, returning the queued
    /// entries. Used at COMMIT time to re-validate violations against
    /// current parent state.
    pub fn take_deferred_fk_violations(&mut self) -> Vec<DeferredFkViolation> {
        match &mut self.transaction_state {
            TransactionState::Active { deferred_fk_violations, .. } => {
                std::mem::take(deferred_fk_violations)
            }
            TransactionState::None => Vec::new(),
        }
    }

    /// Read-only access to the deferred FK violation queue (for status
    /// pragmas and debugging). Returns an empty slice when no
    /// transaction is active.
    pub fn deferred_fk_violations(&self) -> &[DeferredFkViolation] {
        match &self.transaction_state {
            TransactionState::Active { deferred_fk_violations, .. } => {
                deferred_fk_violations.as_slice()
            }
            TransactionState::None => &[],
        }
    }

    /// Release (destroy) a named savepoint
    pub fn release_savepoint(&mut self, name: String) -> Result<(), StorageError> {
        match &mut self.transaction_state {
            TransactionState::None => {
                Err(StorageError::TransactionError("No active transaction".to_string()))
            }
            TransactionState::Active { savepoints, .. } => {
                let savepoint_idx =
                    savepoints.iter().position(|sp| sp.name == name).ok_or_else(|| {
                        StorageError::TransactionError(format!("Savepoint '{}' not found", name))
                    })?;

                // Remove the savepoint
                savepoints.remove(savepoint_idx);

                Ok(())
            }
        }
    }
}

impl Default for TransactionManager {
    fn default() -> Self {
        Self::new()
    }
}
