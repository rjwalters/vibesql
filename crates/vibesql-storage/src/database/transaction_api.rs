// ============================================================================
// Transaction Management API
// ============================================================================
//
// This module provides transaction management methods for the Database struct.
// Methods are implemented via an impl block on the Database type.

use super::transactions::{DeferredFkViolation, TransactionChange};
use super::Database;
use crate::mvcc::TxnSnapshot;
use crate::wal::{DurabilityMode, TransactionDurability, WalOp};
use crate::StorageError;

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
        self.lifecycle.transaction_manager_mut().begin_transaction_with_durability(
            catalog,
            &self.tables,
            durability,
        )?;

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

        // SQLite: PRAGMA defer_foreign_keys is automatically reset to OFF at
        // every COMMIT (R-21752-26913, fkey6-1.10.1). This is a session-level
        // flag, so reset it here regardless of FK enforcement state.
        self.set_defer_foreign_keys(false);

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

        self.lifecycle.perform_rollback(
            &mut self.catalog,
            &mut self.tables,
            &mut self.operations,
        )?;

        // SQLite: PRAGMA defer_foreign_keys is automatically reset to OFF at
        // every COMMIT or ROLLBACK (R-21752-26913, fkey6-1.10.1).
        self.set_defer_foreign_keys(false);

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

    /// Number of `Operations` (IndexManager) deep-clones taken for
    /// transaction rollback snapshots since this database was created
    /// (copy-on-write instrumentation, #5419).
    ///
    /// Read-only transactions — including the per-read scratch transaction
    /// used by replicated read-your-own-writes — never mutate the index
    /// manager, so they never trigger the lazy clone and leave this counter
    /// unchanged. A transaction that mutates an index increments it exactly
    /// once. Exposed so tests can assert deterministically (no timing) that
    /// the read-only path avoids the deep clone.
    ///
    /// See [`crate::database::transactions::TransactionManager::ensure_operations_snapshot`].
    pub fn operations_snapshot_clones(&self) -> u64 {
        self.lifecycle.transaction_manager().operations_snapshot_clones()
    }

    /// Get current transaction ID (for debugging)
    pub fn transaction_id(&self) -> Option<u64> {
        self.lifecycle.transaction_manager().transaction_id()
    }

    /// Get the MVCC snapshot for the current transaction (if any).
    ///
    /// Returns `Some(&TxnSnapshot)` when a transaction is active, or
    /// `None` for auto-commit reads. Phase 1d of #5136 wires this into
    /// the SELECT scan boundary so transactional reads observe a stable
    /// snapshot-isolation view.
    ///
    /// See [`crate::mvcc::TxnSnapshot`].
    pub fn current_snapshot(&self) -> Option<&TxnSnapshot> {
        self.lifecycle.transaction_manager().current_snapshot()
    }

    /// Capture a fresh "commit-time" MVCC snapshot.
    ///
    /// Phase 1d (#5151) helper for FK deferred-replay coordination —
    /// see [`crate::database::transactions::TransactionManager::capture_commit_time_snapshot`].
    pub fn capture_commit_time_snapshot(&self) -> TxnSnapshot {
        self.lifecycle.transaction_manager().capture_commit_time_snapshot()
    }

    /// Compute the MVCC garbage-collection horizon.
    ///
    /// See [`crate::database::transactions::TransactionManager::compute_gc_horizon`].
    /// Mostly useful for tests and diagnostics; production callers
    /// should prefer [`Self::vacuum_mvcc`], which uses this horizon
    /// internally.
    pub fn compute_gc_horizon(&self) -> crate::row::TxnId {
        self.lifecycle.transaction_manager().compute_gc_horizon()
    }

    /// Pin the MVCC GC horizon at its current value (Raft Phase B1, #5199).
    ///
    /// See [`TransactionManager::pin_gc_horizon`]: while the returned pin
    /// is held, [`Self::vacuum_mvcc`] cannot reclaim row versions visible
    /// at pin time. Used by `vibesql-consensus` to keep a Raft snapshot
    /// build's view of the database stable without occupying the
    /// single-writer transaction slot.
    ///
    /// [`TransactionManager::pin_gc_horizon`]: crate::database::transactions::TransactionManager::pin_gc_horizon
    pub fn pin_gc_horizon(&mut self) -> u64 {
        self.lifecycle.transaction_manager_mut().pin_gc_horizon()
    }

    /// Release a GC horizon pin acquired with [`Self::pin_gc_horizon`].
    /// Releasing an unknown (or already-released) pin id is a no-op.
    pub fn release_gc_horizon(&mut self, pin_id: u64) {
        self.lifecycle.transaction_manager_mut().release_gc_horizon(pin_id);
    }

    /// Override the next transaction id (Raft Phase B1, #5199).
    ///
    /// Replication-apply use only — see
    /// [`TransactionManager::set_next_txn_id`] for the full caller
    /// contract (must be outside a transaction; must never move the
    /// allocator backwards past stamped rows). This is how the
    /// replicated state machine derives the MVCC commit timestamp from
    /// the Raft log index.
    ///
    /// [`TransactionManager::set_next_txn_id`]: crate::database::transactions::TransactionManager::set_next_txn_id
    pub fn set_next_txn_id(&mut self, id: crate::row::TxnId) -> Result<(), StorageError> {
        self.lifecycle.transaction_manager_mut().set_next_txn_id(id)
    }

    /// MVCC vacuum: reclaim space from row versions whose deletion is
    /// provably invisible to every active and future transaction.
    ///
    /// # Phase 1d follow-up (#5208)
    ///
    /// This is the "minimum viable" MVCC GC: a single, on-demand sweep
    /// across all tables. It is **not** automatic and does **not**
    /// integrate with WAL checkpoints or background tasks — those are
    /// tracked as follow-up work in #5208 itself.
    ///
    /// # Semantics
    ///
    /// 1. Compute the GC horizon (see
    ///    [`TransactionManager::compute_gc_horizon`]).
    /// 2. For each table, call [`Table::gc_old_versions(horizon)`].
    /// 3. If any table reclaimed rows, rebuild its user-defined indexes
    ///    (B-tree indexes managed at the Database level) to keep them
    ///    consistent with the now-compacted row vector.
    ///
    /// # Refused while a transaction is active
    ///
    /// Returns [`StorageError::TransactionError`] when called inside an
    /// active transaction. v1 deliberately does **not** mix GC with
    /// in-flight writes: doing so safely requires deferring the
    /// underlying bitmap-delete on UPDATE/DELETE so the GC sweep can
    /// observe the tombstones, which is a larger change than this
    /// follow-up. Callers that want to run GC mid-transaction should
    /// COMMIT first.
    ///
    /// # Off-state (`mvcc_enabled` feature OFF)
    ///
    /// Returns `Ok(0)` immediately. No row in the off-state carries an
    /// MVCC tombstone, so there is nothing to reclaim. The API is
    /// stable across feature configurations.
    ///
    /// # Returns
    ///
    /// The total number of row versions reclaimed across all tables.
    ///
    /// # Example
    ///
    /// ```text
    /// // After a long-running write workload:
    /// let reclaimed = db.vacuum_mvcc()?;
    /// eprintln!("Reclaimed {reclaimed} old row versions");
    /// ```
    ///
    /// [`TransactionManager::compute_gc_horizon`]: crate::database::transactions::TransactionManager::compute_gc_horizon
    /// [`Table::gc_old_versions(horizon)`]: crate::Table::gc_old_versions
    pub fn vacuum_mvcc(&mut self) -> Result<usize, StorageError> {
        if self.in_transaction() {
            return Err(StorageError::TransactionError(
                "vacuum_mvcc cannot run while a transaction is active; COMMIT first".to_string(),
            ));
        }

        // Off-state fast path: no MVCC stamping ever happened, so nothing
        // to reclaim. Keeps the iteration overhead off the hot path when
        // the feature is compiled out.
        if !crate::mvcc::mvcc_enabled() {
            return Ok(0);
        }

        let horizon = self.lifecycle.transaction_manager().compute_gc_horizon();

        let table_names: Vec<String> = self.tables.keys().cloned().collect();
        let mut total_reclaimed = 0usize;

        for name in table_names {
            let reclaimed = {
                let table = match self.tables.get_mut(&name) {
                    Some(t) => t,
                    None => continue,
                };
                table.gc_old_versions(horizon)
            };
            if reclaimed > 0 {
                total_reclaimed += reclaimed;
                // GC may have triggered compaction, which shuffles physical
                // row indices. Database-level user indexes must be rebuilt
                // to track the new positions — same pattern as the DELETE
                // path's `if delete_result.compacted` branch.
                self.rebuild_indexes(&name);
                self.invalidate_columnar_cache(&name);
            }
        }

        Ok(total_reclaimed)
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
    // Deferred FK Violation Queue (Phase C2 of #5085)
    // ============================================================================

    /// Push a deferred FK violation onto the active transaction's queue.
    ///
    /// Called by FK validators in the executor when a constraint is
    /// `DEFERRABLE INITIALLY DEFERRED` or the session has
    /// `PRAGMA defer_foreign_keys=ON`. The COMMIT path drains the queue
    /// and re-checks each violation against the current parent state.
    pub fn queue_deferred_fk_violation(&mut self, violation: DeferredFkViolation) {
        self.lifecycle.transaction_manager_mut().queue_deferred_fk_violation(violation);
    }

    /// Drain the deferred FK violation queue for COMMIT-time
    /// re-validation. The executor calls this just before
    /// [`commit_transaction`] and aborts the commit (rolling back) if
    /// any violation still holds against the current parent state.
    pub fn take_deferred_fk_violations(&mut self) -> Vec<DeferredFkViolation> {
        self.lifecycle.transaction_manager_mut().take_deferred_fk_violations()
    }

    /// Read-only view of the deferred FK violation queue. Returns an
    /// empty slice when no transaction is active. Used by the
    /// `sqlite3_db_status DBSTATUS_DEFERRED_FKS` PRAGMA bridge.
    pub fn deferred_fk_violations(&self) -> &[DeferredFkViolation] {
        self.lifecycle.transaction_manager().deferred_fk_violations()
    }
}
