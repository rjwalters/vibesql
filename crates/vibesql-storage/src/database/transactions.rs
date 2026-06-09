// ============================================================================
// Transaction Management
// ============================================================================

use std::collections::HashMap;

use crate::{mvcc::TxnSnapshot, row::TxnId, wal::TransactionDurability, Row, StorageError, Table};

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
        /// MVCC snapshot captured at `BEGIN` (Phase 1b of #5136).
        ///
        /// Captured once at transaction start and held for the entire
        /// transaction lifetime. All reads within the transaction
        /// consult the same snapshot, giving snapshot-isolation
        /// semantics independent of concurrent commits.
        ///
        /// **Phase 1b note:** this field is captured but **not yet
        /// consulted by any read path**. Phase 1d wires the
        /// `Table::scan_visible(&snapshot)` boundary. Until then this
        /// field is dead state at runtime — kept here so Phase 1c can
        /// stamp `xmin = id` and Phase 1d has the snapshot to read with.
        ///
        /// See [`crate::mvcc::TxnSnapshot`] for the predicate contract.
        snapshot: TxnSnapshot,
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

                // Capture the MVCC snapshot **before** publishing the
                // new transaction id. The `in_progress` set is computed
                // from currently-active transactions (just this one
                // would be self, which we deliberately exclude — a
                // transaction always sees its own writes). Under the
                // current single-writer model, in_progress is always
                // empty at BEGIN; this will need to change when Raft +
                // multi-writer arrives in later phases.
                let snapshot = Self::capture_snapshot(transaction_id);

                self.transaction_state = TransactionState::Active {
                    id: transaction_id,
                    original_catalog,
                    original_tables,
                    savepoints: Vec::new(),
                    changes: Vec::new(),
                    durability,
                    deferred_fk_violations: Vec::new(),
                    snapshot,
                };
                Ok(())
            }
            TransactionState::Active { .. } => {
                Err(StorageError::TransactionError("Transaction already active".to_string()))
            }
        }
    }

    /// Build the MVCC snapshot for a transaction with id `txn_id`.
    ///
    /// **Phase 1b semantics (single-writer):** there are no other
    /// concurrently-active transactions to put in `in_progress`. The
    /// snapshot is:
    ///
    /// - `xmin_active = txn_id` — the next-to-allocate id (one past the
    ///   high-water mark from the caller's perspective). This makes the
    ///   `xmax > xmin_active` clause in [`Row::visible_to`] only true
    ///   for transactions that started *after* us, which is the correct
    ///   behavior with no concurrent peers.
    /// - `xmax_committed = txn_id - 1` — every transaction allocated
    ///   before us has, by the single-writer invariant, already finished
    ///   (committed or rolled back). For the rolled-back case, the
    ///   rollback path replaces tables wholesale, so no `xmin = rolled_back_id`
    ///   row exists in storage and the predicate correctness is preserved.
    /// - `in_progress = ∅` — no concurrent transactions.
    ///
    /// **Future (multi-writer / Raft):** when concurrent transactions
    /// become possible, this method becomes the chokepoint where the
    /// `in_progress` set is populated from the active-transactions
    /// registry. The caller signature does not need to change.
    ///
    /// [`Row::visible_to`]: crate::Row::visible_to
    fn capture_snapshot(txn_id: TxnId) -> TxnSnapshot {
        // For txn_id = 1 (the first ever), xmax_committed = 0 means
        // "only pre-MVCC rows are visible" — exactly right.
        let xmax_committed = txn_id.saturating_sub(1);
        TxnSnapshot::new(txn_id, xmax_committed, std::collections::HashSet::new())
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

    /// Get the MVCC snapshot for the current transaction (if any).
    ///
    /// Returns the snapshot captured at `BEGIN` time. The same snapshot
    /// is returned on every call within a transaction — capture is
    /// per-transaction, not per-statement.
    ///
    /// **Phase 1b note:** no callers consume this yet. Phase 1d will
    /// thread it through the scan path so SELECT/JOIN/subquery reads
    /// get snapshot-isolation semantics.
    ///
    /// See [`crate::mvcc::TxnSnapshot`].
    pub fn current_snapshot(&self) -> Option<&TxnSnapshot> {
        match &self.transaction_state {
            TransactionState::Active { snapshot, .. } => Some(snapshot),
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

#[cfg(test)]
mod snapshot_capture_tests {
    //! Tests for MVCC snapshot capture (Phase 1b of #5136).
    //!
    //! These tests cover the contract that `begin_transaction` captures
    //! a [`TxnSnapshot`] exactly once, that the snapshot reflects the
    //! current `next_transaction_id` watermark, and that multiple reads
    //! within the same transaction see the same snapshot.

    use super::*;

    fn empty_catalog_and_tables() -> (vibesql_catalog::Catalog, HashMap<String, Table>) {
        (vibesql_catalog::Catalog::new(), HashMap::new())
    }

    #[test]
    fn first_transaction_snapshot_has_xmax_committed_zero() {
        // First-ever transaction: xmax_committed should be 0 (only
        // pre-MVCC sentinel rows are visible to it), xmin_active should
        // equal our own id (1).
        let (catalog, tables) = empty_catalog_and_tables();
        let mut mgr = TransactionManager::new();
        mgr.begin_transaction(&catalog, &tables).unwrap();

        let snap = mgr.current_snapshot().expect("snapshot present after BEGIN");
        assert_eq!(snap.xmin_active, 1);
        assert_eq!(snap.xmax_committed, 0);
        assert!(snap.in_progress.is_empty());
    }

    #[test]
    fn second_transaction_snapshot_sees_first_as_committed() {
        // Second transaction's snapshot should include the first txn's
        // id in the "committed" range (xmax_committed >= 1).
        let (catalog, tables) = empty_catalog_and_tables();
        let mut mgr = TransactionManager::new();

        mgr.begin_transaction(&catalog, &tables).unwrap();
        mgr.commit_transaction().unwrap();

        mgr.begin_transaction(&catalog, &tables).unwrap();
        let snap = mgr.current_snapshot().expect("second snapshot present");
        assert_eq!(snap.xmin_active, 2);
        assert_eq!(snap.xmax_committed, 1);
        assert!(snap.in_progress.is_empty());
    }

    #[test]
    fn snapshot_stable_across_multiple_reads_within_transaction() {
        // The snapshot is captured once at BEGIN; subsequent reads must
        // return the same data. This is the snapshot-isolation contract.
        let (catalog, tables) = empty_catalog_and_tables();
        let mut mgr = TransactionManager::new();
        mgr.begin_transaction(&catalog, &tables).unwrap();

        let snap1 = mgr.current_snapshot().cloned().unwrap();
        let snap2 = mgr.current_snapshot().cloned().unwrap();
        let snap3 = mgr.current_snapshot().cloned().unwrap();

        assert_eq!(snap1.xmin_active, snap2.xmin_active);
        assert_eq!(snap1.xmin_active, snap3.xmin_active);
        assert_eq!(snap1.xmax_committed, snap2.xmax_committed);
        assert_eq!(snap1.xmax_committed, snap3.xmax_committed);
        assert_eq!(snap1.in_progress, snap2.in_progress);
        assert_eq!(snap1.in_progress, snap3.in_progress);
    }

    #[test]
    fn no_snapshot_outside_transaction() {
        let mgr = TransactionManager::new();
        assert!(mgr.current_snapshot().is_none());
    }

    #[test]
    fn rolled_back_transaction_still_advances_watermark() {
        // Even rolled-back transactions consume a TxnId, so the next
        // transaction's snapshot xmax_committed includes them. The
        // rollback path replaces tables wholesale, so no rows stamped
        // with the rolled-back id remain, preserving predicate
        // correctness.
        let (mut catalog, mut tables) = empty_catalog_and_tables();
        let mut mgr = TransactionManager::new();

        mgr.begin_transaction(&catalog, &tables).unwrap();
        mgr.rollback_transaction(&mut catalog, &mut tables).unwrap();

        mgr.begin_transaction(&catalog, &tables).unwrap();
        let snap = mgr.current_snapshot().unwrap();
        // First txn id was 1 (rolled back). Second txn id is 2.
        assert_eq!(snap.xmin_active, 2);
        assert_eq!(snap.xmax_committed, 1);
    }

    #[test]
    fn snapshot_visibility_integration() {
        // End-to-end sanity check: a row stamped with the *current*
        // active transaction's id (Phase 1c will do this) should NOT
        // be visible to that transaction's own snapshot under this
        // predicate — that's intentional. Phase 1c/1d will introduce a
        // separate "is my own write" check that bypasses snapshot
        // visibility. Documenting that gap here.
        let (catalog, tables) = empty_catalog_and_tables();
        let mut mgr = TransactionManager::new();
        mgr.begin_transaction(&catalog, &tables).unwrap();
        let my_id = mgr.transaction_id().unwrap();
        let snap = mgr.current_snapshot().unwrap();

        // A row stamped with our own id: my_id > xmax_committed
        // (== my_id - 1), so visible_to returns false. Phase 1c's
        // "see my own writes" path will be a separate clause.
        let mut my_row = crate::Row::new(vec![vibesql_types::SqlValue::Integer(1)]);
        my_row.xmin = my_id;
        assert!(
            !my_row.visible_to(snap),
            "Phase 1b predicate intentionally does not show transactions \
             their own writes; Phase 1c will add a separate clause for that"
        );
    }
}
