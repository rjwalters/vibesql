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

                // Capture the MVCC snapshot for this transaction. The
                // `in_progress` set is computed from currently-active
                // transactions; the txn deliberately treats *itself* as
                // committed (see [`Self::capture_snapshot`] for the
                // #5207 self-write widening). Under the current
                // single-writer model, in_progress is always empty at
                // BEGIN; this will need to change when Raft + multi-
                // writer arrives in later phases.
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
    /// **Single-writer semantics with #5207 self-write widening:** there
    /// are no other concurrently-active transactions to put in
    /// `in_progress`. The snapshot is:
    ///
    /// - `xmin_active = txn_id + 1` — one past the snapshot's own
    ///   transaction id. Any row whose `xmax` value falls in
    ///   `(xmin_active, ∞)` was deleted by a *later* transaction (one
    ///   that hadn't yet been allocated when our snapshot was taken).
    ///   Under the single-writer model this set is currently empty, but
    ///   the bookkeeping keeps the `xmax > xmin_active` clause in
    ///   [`Row::visible_to`] semantically correct.
    /// - `xmax_committed = txn_id` — every transaction id allocated so
    ///   far, **including our own**, is treated as committed-as-of this
    ///   snapshot. This is the #5207 "see your own writes" widening:
    ///   a row stamped with `xmin = txn_id` (the active transaction's
    ///   own write) passes `is_committed(self)` and is therefore visible
    ///   to subsequent reads within the same transaction. The same
    ///   widening makes prior-transaction writes visible too — which is
    ///   the correct snapshot-isolation behavior in single-writer
    ///   (every "prior" transaction has already finished by the time
    ///   the next one starts).
    /// - `in_progress = ∅` — no concurrent transactions.
    ///
    /// **Future (multi-writer / Raft):** when concurrent transactions
    /// become possible, this method becomes the chokepoint where the
    /// `in_progress` set is populated from the active-transactions
    /// registry. Transactions that started after this one but committed
    /// before this one's reads must be invisible under SI; concurrent
    /// transactions go in `in_progress`. The signature does not change.
    ///
    /// [`Row::visible_to`]: crate::Row::visible_to
    fn capture_snapshot(txn_id: TxnId) -> TxnSnapshot {
        // #5207: widen `xmax_committed` to include the txn's own id so
        // self-writes (xmin = txn_id) are visible. Under single-writer
        // this is safe because there are no concurrent peers. Multi-
        // writer will need to model the "still-running peers" set in
        // `in_progress` instead.
        TxnSnapshot::new(
            txn_id.saturating_add(1),
            txn_id,
            std::collections::HashSet::new(),
        )
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
    /// Phase 1d (#5151) wires this into the SELECT scan boundary so
    /// reads inside a transaction observe a stable snapshot-isolation
    /// view of the database.
    ///
    /// See [`crate::mvcc::TxnSnapshot`].
    pub fn current_snapshot(&self) -> Option<&TxnSnapshot> {
        match &self.transaction_state {
            TransactionState::Active { snapshot, .. } => Some(snapshot),
            TransactionState::None => None,
        }
    }

    /// Capture a fresh "commit-time" MVCC snapshot.
    ///
    /// Phase 1d (#5151) FK deferred-replay coordination: when a deferred
    /// FK violation is re-checked at COMMIT, the read must reflect both
    /// the committing transaction's own writes **and** any other
    /// transactions that have committed between BEGIN and COMMIT. The
    /// BEGIN-time snapshot does *not* see writes committed after it,
    /// which is exactly the wrong semantics for FK enforcement (we want
    /// the latest visible parent state, not the BEGIN-time snapshot).
    ///
    /// This helper synthesizes a snapshot where every transaction id
    /// allocated so far is considered committed (under the single-writer
    /// model). The committing transaction's own writes pass `is_committed`
    /// because the committing transaction's `xmin` is `<= next_transaction_id - 1`.
    ///
    /// Under future multi-writer / Raft, this helper becomes the chokepoint
    /// where the "still-in-progress" set is consulted; the caller signature
    /// does not change.
    pub fn capture_commit_time_snapshot(&self) -> TxnSnapshot {
        let next_id = self.next_transaction_id;
        // xmin_active = next_id ⇒ no row's xmax can exceed it under SI,
        // matching `capture_snapshot`'s convention.
        // xmax_committed = next_id - 1 ⇒ every id allocated so far is
        // committed-as-of this snapshot (single-writer invariant).
        TxnSnapshot::new(next_id, next_id.saturating_sub(1), std::collections::HashSet::new())
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
    fn first_transaction_snapshot_includes_self_as_committed() {
        // #5207: the BEGIN-time snapshot must treat its own txn id as
        // committed so self-writes (xmin = txn_id) are visible to the
        // txn's own reads. For the first-ever transaction (txn_id = 1):
        //   - xmax_committed = 1 (self counts as committed)
        //   - xmin_active = 2 (one past self)
        let (catalog, tables) = empty_catalog_and_tables();
        let mut mgr = TransactionManager::new();
        mgr.begin_transaction(&catalog, &tables).unwrap();

        let snap = mgr.current_snapshot().expect("snapshot present after BEGIN");
        assert_eq!(snap.xmin_active, 2);
        assert_eq!(snap.xmax_committed, 1);
        assert!(snap.in_progress.is_empty());
    }

    #[test]
    fn second_transaction_snapshot_sees_first_and_self_as_committed() {
        // Second transaction's snapshot should include both the first
        // txn's id AND its own id in the "committed" range.
        let (catalog, tables) = empty_catalog_and_tables();
        let mut mgr = TransactionManager::new();

        mgr.begin_transaction(&catalog, &tables).unwrap();
        mgr.commit_transaction().unwrap();

        mgr.begin_transaction(&catalog, &tables).unwrap();
        let snap = mgr.current_snapshot().expect("second snapshot present");
        // #5207: xmax_committed = self (= 2), xmin_active = self + 1 (= 3)
        assert_eq!(snap.xmin_active, 3);
        assert_eq!(snap.xmax_committed, 2);
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
        // #5207: xmax_committed = self (= 2), xmin_active = self + 1 (= 3).
        assert_eq!(snap.xmin_active, 3);
        assert_eq!(snap.xmax_committed, 2);
    }

    #[test]
    fn snapshot_visibility_integration_self_writes_visible() {
        // #5207: the BEGIN-time snapshot now treats the active txn's
        // own id as committed, so a row stamped with that txn's id is
        // visible to the transaction's own reads. This is the
        // "see-your-own-writes" invariant.
        let (catalog, tables) = empty_catalog_and_tables();
        let mut mgr = TransactionManager::new();
        mgr.begin_transaction(&catalog, &tables).unwrap();
        let my_id = mgr.transaction_id().unwrap();
        let snap = mgr.current_snapshot().unwrap();

        // A row stamped with our own id: my_id == xmax_committed under
        // the #5207-widened snapshot, so visible_to returns true.
        let mut my_row = crate::Row::new(vec![vibesql_types::SqlValue::Integer(1)]);
        my_row.xmin = my_id;
        assert!(
            my_row.visible_to(snap),
            "#5207 widening: a txn's own writes (xmin = self) must be visible"
        );
    }
}
