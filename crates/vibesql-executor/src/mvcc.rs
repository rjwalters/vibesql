//! Executor-side MVCC helpers.
//!
//! # Phase 1d of #5136 (+ follow-up #5207)
//!
//! Phase 1d wires the storage-layer `Row::visible_to(&snapshot)` predicate
//! into the SELECT scan boundary and the FK deferred-replay coordinator.
//! This module centralizes the "which snapshot should this read use?"
//! decision so individual scan paths don't have to repeat it.
//!
//! Follow-up #5207 closes the two visibility gaps left open by the
//! initial Phase 1d PR — see the module-level note on [`read_snapshot`].
//!
//! ## When MVCC is off (default)
//!
//! With the `mvcc_enabled` feature OFF (the default), [`read_snapshot`]
//! returns a default snapshot and the storage-layer scan helpers
//! (`Table::scan_visible_vec`, `Table::scan_visible`) ignore the snapshot
//! entirely. Behavior is bit-for-bit identical to pre-MVCC reads.
//!
//! ## When MVCC is on
//!
//! With the feature ON, [`read_snapshot`] returns:
//!
//! - **Inside a transaction**: the transaction's BEGIN-time snapshot (so reads see a stable
//!   snapshot-isolation view across the txn). Self-writes pass `is_committed(self)` because the
//!   BEGIN-time snapshot is widened by [`TransactionManager::capture_snapshot`] to set
//!   `xmax_committed = txn_id` (see the follow-up #5207 fix).
//! - **Outside a transaction (autocommit)**: a freshly-captured commit-time snapshot, treating
//!   every transaction id allocated so far as committed. This is the autocommit-snapshot-widening
//!   fix from #5207: autocommit reads see writes from every txn that has already committed,
//!   including the transactional inserts that stamped `xmin = txn_id`.
//!
//! [`TransactionManager::capture_snapshot`]:
//!     vibesql_storage::database::transactions::TransactionManager::capture_snapshot

use vibesql_storage::{Database, TxnSnapshot};

/// Return the snapshot to use for reads.
///
/// - In a transaction: returns the transaction's BEGIN-time snapshot (so reads see a stable
///   snapshot-isolation view).
/// - Outside a transaction (autocommit): returns a freshly-captured commit-time snapshot via
///   [`Database::capture_commit_time_snapshot`], which treats every already-allocated transaction
///   id as committed.
///
/// # Why autocommit synthesizes a commit-time snapshot
///
/// Each autocommit statement is, semantically, its own one-statement
/// transaction. Statement N+1 must see writes that statement N
/// committed. Under Phase 1c's stamping rules, transactional writes
/// carry `xmin = txn_id > 0`, and the pre-fix empty snapshot
/// (`xmax_committed = 0`) made them invisible. By synthesizing a
/// commit-time snapshot at every autocommit read, we observe everything
/// that has finished committing so far — exactly the "see your own
/// writes" semantics autocommit needs.
///
/// With the `mvcc_enabled` feature OFF, the storage-layer visibility
/// filter is a no-op, so the returned value is informationally
/// equivalent regardless. With the feature ON, this is the canonical
/// way to thread the right snapshot into scan code.
#[inline]
pub fn read_snapshot(db: &Database) -> TxnSnapshot {
    match db.current_snapshot() {
        Some(snap) => snap.clone(),
        None => db.capture_commit_time_snapshot(),
    }
}
