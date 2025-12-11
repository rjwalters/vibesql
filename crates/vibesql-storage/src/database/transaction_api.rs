// ============================================================================
// Transaction Management API
// ============================================================================
//
// This module provides transaction management methods for the Database struct.
// Methods are implemented via an impl block on the Database type.

use super::transactions::TransactionChange;
use super::Database;
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
}
