//! Transaction isolation support for server sessions.
//!
//! This module provides transaction state management for individual sessions,
//! enabling READ COMMITTED isolation level for shared databases.
//!
//! # Architecture
//!
//! When multiple sessions share a database via `DatabaseRegistry`, each session
//! needs its own transaction state to provide proper isolation:
//!
//! - **READ COMMITTED**: Uncommitted changes in one transaction are NOT visible
//!   to other sessions. Only committed changes propagate to other sessions.
//!
//! # Implementation
//!
//! We use a copy-on-write approach:
//! 1. On BEGIN: Create a snapshot of affected tables as writes occur
//! 2. During transaction: Writes go to the session's local buffer
//! 3. On COMMIT: Atomically merge buffer changes into shared database
//! 4. On ROLLBACK: Discard the buffer (no changes to shared database)
//!
//! This provides READ COMMITTED semantics where:
//! - Other sessions always read committed data
//! - A session in a transaction sees its own uncommitted changes
//! - Committed changes become visible to all sessions

use std::collections::HashMap;
use vibesql_storage::Row;

/// A change made during a transaction that needs to be applied on commit.
#[derive(Debug, Clone, PartialEq)]
#[allow(clippy::large_enum_variant)]
pub enum TransactionChange {
    /// A row was inserted
    Insert {
        table_name: String,
        row: Row,
    },
    /// A row was updated (old values for rollback reference)
    Update {
        table_name: String,
        row_index: usize,
        old_row: Row,
        new_row: Row,
    },
    /// A row was deleted
    Delete {
        table_name: String,
        row_index: usize,
        row: Row,
    },
    /// A table was created
    CreateTable {
        table_name: String,
    },
    /// A table was dropped
    DropTable {
        table_name: String,
    },
    /// An index was created
    CreateIndex {
        index_name: String,
        table_name: String,
    },
    /// An index was dropped
    DropIndex {
        index_name: String,
    },
}

/// Transaction state for a session.
///
/// Tracks uncommitted changes during an active transaction, providing
/// READ COMMITTED isolation when multiple sessions share a database.
#[derive(Debug)]
pub struct TransactionState {
    /// Transaction ID (monotonically increasing)
    pub id: u64,
    /// Whether we're in an active transaction block
    pub active: bool,
    /// Changes made during this transaction (in order)
    changes: Vec<TransactionChange>,
    /// Inserted rows indexed by table name (for reads during transaction)
    inserted_rows: HashMap<String, Vec<Row>>,
    /// Deleted row indices indexed by table name (to filter from reads)
    deleted_indices: HashMap<String, Vec<usize>>,
    /// Updated rows: table_name -> (row_index -> new_row)
    updated_rows: HashMap<String, HashMap<usize, Row>>,
}

impl TransactionState {
    /// Create a new transaction state with the given ID.
    pub fn new(id: u64) -> Self {
        Self {
            id,
            active: true,
            changes: Vec::new(),
            inserted_rows: HashMap::new(),
            deleted_indices: HashMap::new(),
            updated_rows: HashMap::new(),
        }
    }

    /// Record an insert operation.
    pub fn record_insert(&mut self, table_name: String, row: Row) {
        self.changes.push(TransactionChange::Insert {
            table_name: table_name.clone(),
            row: row.clone(),
        });
        self.inserted_rows.entry(table_name).or_default().push(row);
    }

    /// Record an update operation.
    pub fn record_update(
        &mut self,
        table_name: String,
        row_index: usize,
        old_row: Row,
        new_row: Row,
    ) {
        self.changes.push(TransactionChange::Update {
            table_name: table_name.clone(),
            row_index,
            old_row,
            new_row: new_row.clone(),
        });
        self.updated_rows
            .entry(table_name)
            .or_default()
            .insert(row_index, new_row);
    }

    /// Record a delete operation.
    pub fn record_delete(&mut self, table_name: String, row_index: usize, row: Row) {
        self.changes.push(TransactionChange::Delete {
            table_name: table_name.clone(),
            row_index,
            row,
        });
        self.deleted_indices.entry(table_name).or_default().push(row_index);
    }

    /// Record a table creation.
    pub fn record_create_table(&mut self, table_name: String) {
        self.changes.push(TransactionChange::CreateTable { table_name });
    }

    /// Record a table drop.
    pub fn record_drop_table(&mut self, table_name: String) {
        self.changes.push(TransactionChange::DropTable { table_name });
    }

    /// Record an index creation.
    pub fn record_create_index(&mut self, index_name: String, table_name: String) {
        self.changes.push(TransactionChange::CreateIndex { index_name, table_name });
    }

    /// Record an index drop.
    pub fn record_drop_index(&mut self, index_name: String) {
        self.changes.push(TransactionChange::DropIndex { index_name });
    }

    /// Get rows inserted in this transaction for a table.
    pub fn get_inserted_rows(&self, table_name: &str) -> Option<&Vec<Row>> {
        self.inserted_rows.get(table_name)
    }

    /// Get indices of rows deleted in this transaction for a table.
    pub fn get_deleted_indices(&self, table_name: &str) -> Option<&Vec<usize>> {
        self.deleted_indices.get(table_name)
    }

    /// Get updated rows for a table (index -> new_row).
    pub fn get_updated_rows(&self, table_name: &str) -> Option<&HashMap<usize, Row>> {
        self.updated_rows.get(table_name)
    }

    /// Check if a row at a given index was deleted in this transaction.
    pub fn is_deleted(&self, table_name: &str, row_index: usize) -> bool {
        self.deleted_indices
            .get(table_name)
            .is_some_and(|indices| indices.contains(&row_index))
    }

    /// Get the updated version of a row if it was updated in this transaction.
    pub fn get_updated_row(&self, table_name: &str, row_index: usize) -> Option<&Row> {
        self.updated_rows
            .get(table_name)
            .and_then(|updates| updates.get(&row_index))
    }

    /// Consume the transaction state and return all changes for commit.
    pub fn take_changes(self) -> Vec<TransactionChange> {
        self.changes
    }

    /// Get all changes (for inspection without consuming).
    pub fn changes(&self) -> &[TransactionChange] {
        &self.changes
    }

    /// Check if there are any uncommitted changes.
    pub fn has_changes(&self) -> bool {
        !self.changes.is_empty()
    }

    /// Clear all changes (for rollback).
    pub fn clear(&mut self) {
        self.changes.clear();
        self.inserted_rows.clear();
        self.deleted_indices.clear();
        self.updated_rows.clear();
    }
}

/// Manager for session transaction state.
///
/// Each session has its own `SessionTransactionManager` to track
/// its transaction state independently of other sessions.
#[derive(Debug, Default)]
pub struct SessionTransactionManager {
    /// Current transaction state (None if no active transaction)
    current: Option<TransactionState>,
    /// Next transaction ID to assign
    next_id: u64,
}

impl SessionTransactionManager {
    /// Create a new session transaction manager.
    pub fn new() -> Self {
        Self { current: None, next_id: 1 }
    }

    /// Begin a new transaction.
    ///
    /// Returns an error if a transaction is already active.
    pub fn begin(&mut self) -> Result<u64, TransactionError> {
        if self.current.is_some() {
            return Err(TransactionError::AlreadyInTransaction);
        }

        let id = self.next_id;
        self.next_id += 1;
        self.current = Some(TransactionState::new(id));
        Ok(id)
    }

    /// Commit the current transaction.
    ///
    /// Returns the changes to be applied to the shared database.
    pub fn commit(&mut self) -> Result<Vec<TransactionChange>, TransactionError> {
        let state = self.current.take().ok_or(TransactionError::NoActiveTransaction)?;
        Ok(state.take_changes())
    }

    /// Rollback the current transaction.
    ///
    /// Discards all uncommitted changes.
    pub fn rollback(&mut self) -> Result<(), TransactionError> {
        self.current.take().ok_or(TransactionError::NoActiveTransaction)?;
        Ok(())
    }

    /// Check if a transaction is currently active.
    pub fn in_transaction(&self) -> bool {
        self.current.as_ref().is_some_and(|s| s.active)
    }

    /// Get the current transaction ID, if any.
    pub fn transaction_id(&self) -> Option<u64> {
        self.current.as_ref().map(|s| s.id)
    }

    /// Get mutable access to the current transaction state.
    pub fn current_mut(&mut self) -> Option<&mut TransactionState> {
        self.current.as_mut()
    }

    /// Get read access to the current transaction state.
    pub fn current(&self) -> Option<&TransactionState> {
        self.current.as_ref()
    }

    /// Record an insert in the current transaction.
    ///
    /// No-op if not in a transaction.
    pub fn record_insert(&mut self, table_name: String, row: Row) {
        if let Some(state) = &mut self.current {
            state.record_insert(table_name, row);
        }
    }

    /// Record an update in the current transaction.
    ///
    /// No-op if not in a transaction.
    pub fn record_update(
        &mut self,
        table_name: String,
        row_index: usize,
        old_row: Row,
        new_row: Row,
    ) {
        if let Some(state) = &mut self.current {
            state.record_update(table_name, row_index, old_row, new_row);
        }
    }

    /// Record a delete in the current transaction.
    ///
    /// No-op if not in a transaction.
    pub fn record_delete(&mut self, table_name: String, row_index: usize, row: Row) {
        if let Some(state) = &mut self.current {
            state.record_delete(table_name, row_index, row);
        }
    }
}

/// Errors that can occur during transaction management.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TransactionError {
    /// Attempted to begin a transaction when one is already active.
    AlreadyInTransaction,
    /// Attempted to commit/rollback when no transaction is active.
    NoActiveTransaction,
    /// A conflict was detected during commit.
    CommitConflict(String),
}

impl std::fmt::Display for TransactionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TransactionError::AlreadyInTransaction => {
                write!(f, "Transaction already in progress")
            }
            TransactionError::NoActiveTransaction => {
                write!(f, "No transaction in progress")
            }
            TransactionError::CommitConflict(msg) => {
                write!(f, "Commit conflict: {}", msg)
            }
        }
    }
}

impl std::error::Error for TransactionError {}

#[cfg(test)]
mod tests {
    use super::*;
    use vibesql_types::SqlValue;

    fn make_row(values: Vec<SqlValue>) -> Row {
        Row::new(values)
    }

    #[test]
    fn test_begin_transaction() {
        let mut mgr = SessionTransactionManager::new();

        assert!(!mgr.in_transaction());
        assert_eq!(mgr.transaction_id(), None);

        let id = mgr.begin().unwrap();
        assert_eq!(id, 1);
        assert!(mgr.in_transaction());
        assert_eq!(mgr.transaction_id(), Some(1));
    }

    #[test]
    fn test_double_begin_fails() {
        let mut mgr = SessionTransactionManager::new();

        mgr.begin().unwrap();
        let result = mgr.begin();
        assert_eq!(result, Err(TransactionError::AlreadyInTransaction));
    }

    #[test]
    fn test_commit_without_transaction_fails() {
        let mut mgr = SessionTransactionManager::new();

        let result = mgr.commit();
        assert_eq!(result, Err(TransactionError::NoActiveTransaction));
    }

    #[test]
    fn test_rollback_without_transaction_fails() {
        let mut mgr = SessionTransactionManager::new();

        let result = mgr.rollback();
        assert_eq!(result, Err(TransactionError::NoActiveTransaction));
    }

    #[test]
    fn test_record_insert() {
        let mut mgr = SessionTransactionManager::new();
        mgr.begin().unwrap();

        let row = make_row(vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("test"))]);
        mgr.record_insert("users".to_string(), row.clone());

        let state = mgr.current().unwrap();
        assert!(state.has_changes());

        let inserted = state.get_inserted_rows("users").unwrap();
        assert_eq!(inserted.len(), 1);
        assert_eq!(inserted[0].values, row.values);
    }

    #[test]
    fn test_record_delete() {
        let mut mgr = SessionTransactionManager::new();
        mgr.begin().unwrap();

        let row = make_row(vec![SqlValue::Integer(1)]);
        mgr.record_delete("users".to_string(), 5, row);

        let state = mgr.current().unwrap();
        assert!(state.is_deleted("users", 5));
        assert!(!state.is_deleted("users", 6));
        assert!(!state.is_deleted("other_table", 5));
    }

    #[test]
    fn test_record_update() {
        let mut mgr = SessionTransactionManager::new();
        mgr.begin().unwrap();

        let old_row = make_row(vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("old"))]);
        let new_row = make_row(vec![SqlValue::Integer(1), SqlValue::Varchar(arcstr::ArcStr::from("new"))]);
        mgr.record_update("users".to_string(), 3, old_row, new_row.clone());

        let state = mgr.current().unwrap();
        let updated = state.get_updated_row("users", 3).unwrap();
        assert_eq!(updated.values, new_row.values);
        assert!(state.get_updated_row("users", 4).is_none());
    }

    #[test]
    fn test_commit_returns_changes() {
        let mut mgr = SessionTransactionManager::new();
        mgr.begin().unwrap();

        let row1 = make_row(vec![SqlValue::Integer(1)]);
        let row2 = make_row(vec![SqlValue::Integer(2)]);
        mgr.record_insert("users".to_string(), row1);
        mgr.record_insert("users".to_string(), row2);

        let changes = mgr.commit().unwrap();
        assert_eq!(changes.len(), 2);
        assert!(!mgr.in_transaction());
    }

    #[test]
    fn test_rollback_discards_changes() {
        let mut mgr = SessionTransactionManager::new();
        mgr.begin().unwrap();

        let row = make_row(vec![SqlValue::Integer(1)]);
        mgr.record_insert("users".to_string(), row);

        mgr.rollback().unwrap();
        assert!(!mgr.in_transaction());

        // Can start a new transaction after rollback
        mgr.begin().unwrap();
        assert!(mgr.in_transaction());
        assert_eq!(mgr.transaction_id(), Some(2)); // ID incremented
    }

    #[test]
    fn test_transaction_id_increments() {
        let mut mgr = SessionTransactionManager::new();

        let id1 = mgr.begin().unwrap();
        mgr.commit().unwrap();

        let id2 = mgr.begin().unwrap();
        mgr.rollback().unwrap();

        let id3 = mgr.begin().unwrap();

        assert_eq!(id1, 1);
        assert_eq!(id2, 2);
        assert_eq!(id3, 3);
    }

    #[test]
    fn test_no_op_when_not_in_transaction() {
        let mut mgr = SessionTransactionManager::new();

        // These should not panic and should be no-ops
        let row = make_row(vec![SqlValue::Integer(1)]);
        mgr.record_insert("users".to_string(), row.clone());
        mgr.record_delete("users".to_string(), 0, row.clone());
        mgr.record_update("users".to_string(), 0, row.clone(), row);

        // Should not be in transaction
        assert!(!mgr.in_transaction());
    }
}
