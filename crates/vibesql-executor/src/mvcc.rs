//! Executor-side MVCC helpers.
//!
//! # Phase 1d of #5136
//!
//! Phase 1d wires the storage-layer `Row::visible_to(&snapshot)` predicate
//! into the SELECT scan boundary and the FK deferred-replay coordinator.
//! This module centralizes the "which snapshot should this read use?"
//! decision so individual scan paths don't have to repeat it.
//!
//! ## When MVCC is off (default)
//!
//! With the `mvcc_enabled` feature OFF (the default), [`read_snapshot`]
//! returns [`TxnSnapshot::empty`] and the storage-layer scan helpers
//! (`Table::scan_visible_vec`, `Table::scan_visible`) ignore the snapshot
//! entirely. Behavior is bit-for-bit identical to pre-MVCC reads.
//!
//! ## When MVCC is on
//!
//! With the feature ON, [`read_snapshot`] returns the active transaction's
//! BEGIN-time snapshot when in a transaction, or [`TxnSnapshot::empty`]
//! for auto-commit reads.
//!
//! Empty-snapshot semantics under MVCC ON deserve a note: an empty
//! snapshot has `xmax_committed = 0`, which means only rows with
//! `xmin = PRE_MVCC_TXN_ID` (= 0) are visible. Under Phase 1c's write
//! semantics, autocommit inserts also keep the pre-MVCC sentinel (no
//! txn id to stamp with), so this remains consistent: autocommit reads
//! see autocommit writes plus pre-MVCC data. The complete story for
//! "autocommit reads should see all committed transactional writes too"
//! is deferred — see the Phase 1d follow-up issues for autocommit
//! snapshot widening.

use vibesql_storage::{Database, TxnSnapshot};

/// Return the snapshot to use for reads.
///
/// - In a transaction: returns the transaction's BEGIN-time snapshot
///   (so reads see a stable snapshot-isolation view).
/// - Outside a transaction (auto-commit): returns [`TxnSnapshot::empty`].
///
/// With the `mvcc_enabled` feature OFF, the storage-layer visibility
/// filter is a no-op, so the returned value is informationally
/// equivalent regardless. With the feature ON, this is the canonical
/// way to thread the right snapshot into scan code.
#[inline]
pub fn read_snapshot(db: &Database) -> TxnSnapshot {
    db.current_snapshot().cloned().unwrap_or_else(TxnSnapshot::empty)
}
