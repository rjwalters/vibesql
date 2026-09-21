//! Per-session mirror of the storage layer's transaction-active flag.
//!
//! Real transaction semantics — BEGIN/COMMIT/ROLLBACK, SAVEPOINT/RELEASE, and
//! the snapshot-based undo used for rollback — live in the storage layer
//! ([`vibesql_storage::TransactionManager`]). This module owns nothing of that.
//!
//! What it owns is a **session-local copy of "am I inside a transaction?"**,
//! plus a monotonically increasing id for the current transaction. The wire
//! protocol needs that flag synchronously to fill in the `ReadyForQuery`
//! transaction-status indicator after every command, at a point where the
//! async lock guarding the shared `Database` is not held (see
//! `connection/query.rs`), so the session keeps its own copy rather than
//! reading `Database::in_transaction()` directly.
//!
//! Because it is a copy, it can drift. `BEGIN`/`COMMIT`/`ROLLBACK` go through
//! [`SessionTransactionManager::begin`] / [`commit`] / [`rollback`], but
//! `SAVEPOINT` outside an explicit `BEGIN` (and the matching outermost
//! `RELEASE`) implicitly opens/closes a transaction at the `Database` level
//! without touching this manager. [`SessionTransactionManager::sync_active`]
//! exists to force the copy back into agreement with the storage layer after
//! those operations (#6654).
//!
//! [`commit`]: SessionTransactionManager::commit
//! [`rollback`]: SessionTransactionManager::rollback

/// Tracks whether a session is currently inside a transaction.
///
/// Each session owns one of these; it is a mirror of the storage layer's
/// transaction state, not a source of truth (see the module docs).
#[derive(Debug, Default)]
pub struct SessionTransactionManager {
    /// Id of the active transaction (`None` when no transaction is active)
    active_id: Option<u64>,
    /// Next transaction ID to assign
    next_id: u64,
}

impl SessionTransactionManager {
    /// Create a new session transaction manager.
    pub fn new() -> Self {
        Self { active_id: None, next_id: 1 }
    }

    /// Begin a new transaction.
    ///
    /// Returns an error if a transaction is already active.
    pub fn begin(&mut self) -> Result<u64, TransactionError> {
        if self.active_id.is_some() {
            return Err(TransactionError::AlreadyInTransaction);
        }

        let id = self.next_id;
        self.next_id += 1;
        self.active_id = Some(id);
        Ok(id)
    }

    /// Commit the current transaction.
    ///
    /// Only clears this session's active flag — the storage layer performs the
    /// actual commit.
    pub fn commit(&mut self) -> Result<(), TransactionError> {
        self.active_id.take().ok_or(TransactionError::NoActiveTransaction)?;
        Ok(())
    }

    /// Rollback the current transaction.
    ///
    /// Only clears this session's active flag — the storage layer performs the
    /// actual rollback.
    pub fn rollback(&mut self) -> Result<(), TransactionError> {
        self.active_id.take().ok_or(TransactionError::NoActiveTransaction)?;
        Ok(())
    }

    /// Check if a transaction is currently active.
    pub fn in_transaction(&self) -> bool {
        self.active_id.is_some()
    }

    /// Get the current transaction ID, if any.
    pub fn transaction_id(&self) -> Option<u64> {
        self.active_id
    }

    /// Force the manager's active flag to match the storage layer's actual
    /// transaction state.
    ///
    /// `SAVEPOINT` (outside an explicit `BEGIN`) and the matching outermost
    /// `RELEASE` implicitly open/close a transaction at the `Database` level
    /// (SQLite autocommit semantics — see `vibesql_executor::SavepointExecutor`
    /// / `ReleaseSavepointExecutor`), bypassing this session's `begin()` /
    /// `commit()` / `rollback()`. Without this, the session's `in_transaction()`
    /// — which the wire protocol reports as the `ReadyForQuery` transaction
    /// status — would silently drift from the real storage-layer state
    /// (#6654). `active` should be `Database::in_transaction()` right after
    /// the savepoint operation runs; a no-op when already in sync.
    pub fn sync_active(&mut self, active: bool) {
        match (active, self.active_id.is_some()) {
            (true, false) => {
                let id = self.next_id;
                self.next_id += 1;
                self.active_id = Some(id);
            }
            (false, true) => {
                self.active_id = None;
            }
            _ => {}
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
        }
    }
}

impl std::error::Error for TransactionError {}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn test_commit_clears_in_transaction() {
        let mut mgr = SessionTransactionManager::new();
        mgr.begin().unwrap();
        assert!(mgr.in_transaction());

        mgr.commit().unwrap();
        assert!(!mgr.in_transaction());
        assert_eq!(mgr.transaction_id(), None);
    }

    #[test]
    fn test_rollback_clears_in_transaction() {
        let mut mgr = SessionTransactionManager::new();
        mgr.begin().unwrap();

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
    fn test_sync_active_opens_implicit_transaction() {
        let mut mgr = SessionTransactionManager::new();

        // SAVEPOINT outside BEGIN implicitly opened a transaction at the
        // storage layer; sync_active(true) must reflect that here too.
        assert!(!mgr.in_transaction());
        mgr.sync_active(true);
        assert!(mgr.in_transaction());
    }

    #[test]
    fn test_sync_active_closes_implicit_transaction() {
        let mut mgr = SessionTransactionManager::new();
        mgr.sync_active(true);
        assert!(mgr.in_transaction());

        // Outermost RELEASE auto-committed the implicit transaction.
        mgr.sync_active(false);
        assert!(!mgr.in_transaction());
    }

    #[test]
    fn test_sync_active_is_noop_when_already_in_sync() {
        let mut mgr = SessionTransactionManager::new();

        mgr.sync_active(false);
        assert!(!mgr.in_transaction());

        let id = mgr.begin().unwrap();
        mgr.sync_active(true);
        assert_eq!(mgr.transaction_id(), Some(id));
    }
}
