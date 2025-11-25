//! Automatic INSERT batching for test data loading optimization.
//!
//! This module manages implicit transactions to batch consecutive INSERT statements,
//! which significantly improves performance when loading test data. The batching is
//! transparent to the tests and commits automatically when a non-INSERT statement is
//! encountered.

use super::super::execution::TestError;
use vibesql_storage::Database;

/// INSERT batching manager for optimizing test data loads.
pub struct BatchingManager {
    pub enabled: bool,
    pub in_implicit_transaction: bool,
    pub implicit_transaction_insert_count: usize,
    pub verbose: bool,
}

impl BatchingManager {
    /// Create a new batching manager.
    pub fn new(enabled: bool, verbose: bool) -> Self {
        Self {
            enabled,
            in_implicit_transaction: false,
            implicit_transaction_insert_count: 0,
            verbose,
        }
    }

    /// Begin an implicit transaction for INSERT batching if not already in one.
    pub fn begin_if_needed(&mut self, db: &mut Database) -> Result<(), TestError> {
        if self.enabled && !db.in_transaction() && !self.in_implicit_transaction {
            db.begin_transaction()
                .map_err(|e| TestError::Execution(format!("Transaction error: {:?}", e)))?;
            self.in_implicit_transaction = true;
            self.implicit_transaction_insert_count = 0;
        }
        Ok(())
    }

    /// Track an INSERT statement in the current batch.
    pub fn record_insert(&mut self) {
        if self.enabled && self.in_implicit_transaction {
            self.implicit_transaction_insert_count += 1;
        }
    }

    /// Commit any pending implicit transaction from INSERT batching.
    pub fn commit_if_needed(&mut self, db: &mut Database) -> Result<(), TestError> {
        if self.in_implicit_transaction {
            if self.verbose && self.implicit_transaction_insert_count > 1 {
                eprintln!(
                    "  [INSERT Batching] Committing implicit transaction ({} INSERTs batched)",
                    self.implicit_transaction_insert_count
                );
            }
            db.commit_transaction()
                .map_err(|e| TestError::Execution(format!("Transaction error: {:?}", e)))?;
            self.in_implicit_transaction = false;
            self.implicit_transaction_insert_count = 0;
        }
        Ok(())
    }

    /// Rollback any implicit transaction (used for explicit ROLLBACK statements).
    pub fn rollback_if_needed(&mut self) {
        if self.in_implicit_transaction {
            self.in_implicit_transaction = false;
            self.implicit_transaction_insert_count = 0;
        }
    }
}
