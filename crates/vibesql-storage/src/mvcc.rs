//! MVCC (Multi-Version Concurrency Control) snapshot machinery.
//!
//! # Phase 1b of #5136
//!
//! This module is the second of four sequential PRs decomposing MVCC
//! Phase 1 from the umbrella issue #5136:
//!
//! - **Phase 1a** ([PR #5142]) — added `xmin`/`xmax` fields to [`Row`] and the v6→v7 persistence
//!   shim. Inert at the executor level.
//! - **Phase 1b** (this PR, #5149) — introduces [`TxnSnapshot`] and the [`Row::visible_to`]
//!   predicate. Snapshot capture is wired into `TransactionState::Active`, but **nothing reads it
//!   yet**. The predicate is pure-function and has no callers in this PR; Phase 1c and Phase 1d
//!   wire up the writer and reader respectively.
//! - **Phase 1c** (#5150, future) — write-path `xmin`/`xmax` stamping on INSERT/UPDATE/DELETE.
//! - **Phase 1d** (#5151, future) — read-path visibility filtering on SELECT scans; cross-snapshot
//!   FK deferred-replay coordination; flips the `mvcc_enabled` feature flag on.
//!
//! # API surface (kept minimal on purpose)
//!
//! - [`TxnSnapshot`] — a captured view of which transactions are committed-as-of-then. Plain data,
//!   `Clone + Debug`.
//! - [`TxnSnapshot::empty`] — the "everything pre-MVCC is visible, nothing else is" snapshot used
//!   by non-transactional reads and as the default in transactions that haven't yet captured one.
//! - [`Row::visible_to`] — `&self, &TxnSnapshot -> bool`. The single visibility predicate.
//!
//! Phase 1c/1d should not need to expand this surface — they only need to
//! call `Row::visible_to(snapshot)` from the scan boundary and to pass the
//! `TransactionState::Active.snapshot` field into reads.
//!
//! # Visibility predicate contract
//!
//! A row `r` with `(r.xmin, r.xmax)` is **visible to** a snapshot `s`
//! iff all three of these hold:
//!
//! 1. `r.xmin <= s.xmax_committed` — the row's creator committed before our snapshot was taken (or
//!    `r.xmin` is the pre-MVCC sentinel `0`, which is `<= s.xmax_committed` for any non-zero
//!    snapshot).
//! 2. `r.xmin` is **not** in `s.in_progress` — the row's creator was still mid-flight when our
//!    snapshot was taken, so under snapshot isolation we don't see it.
//! 3. Either `r.xmax` is `None` (still live), OR `r.xmax > s.xmin_active` (deletion happened after
//!    the oldest still-running txn at snapshot time, so it can't have been committed before our
//!    snapshot), OR `r.xmax` is in `s.in_progress` (the deleter was still mid-flight, so the delete
//!    isn't yet visible to us).
//!
//! The third clause is intentionally optimistic: it errs on the side of
//! **showing** rows that *might* have been deleted by a concurrent
//! transaction. Snapshot isolation forbids the opposite (hiding rows
//! that were definitely live at snapshot time), but allows showing rows
//! that have been concurrently deleted. Phase 1c's commit path is what
//! resolves the "definitely committed" status; this predicate operates
//! purely on the snapshot data it was given.
//!
//! # Abort handling
//!
//! This phase does **not** model transaction abort/rollback in the
//! snapshot data. A snapshot only carries information about
//! committed-or-still-running transactions. If a transaction aborts,
//! Phase 1c is responsible for either:
//!
//! - Reverting the `xmin`/`xmax` stamps it made (current `rollback_transaction` already restores
//!   `original_tables` from snapshot, which discards any stamping), OR
//! - Adding an `aborted: HashSet<TxnId>` field to [`TxnSnapshot`] and extending the predicate with
//!   an "ignore writes from aborted txns" clause.
//!
//! The current snapshot-isolation semantics — combined with
//! `TransactionManager::rollback_transaction` restoring full table
//! snapshots — make the "revert on abort" path the simpler choice; Phase
//! 1c will document its final decision.
//!
//! [PR #5142]: https://github.com/rjwalters/vibesql/pull/5142
//! [`Row`]: crate::Row
//! [`Row::visible_to`]: crate::Row::visible_to

use std::collections::HashSet;

use crate::row::TxnId;

/// A point-in-time snapshot of the set of transactions visible to a
/// reader under snapshot isolation.
///
/// A `TxnSnapshot` is captured **once per transaction at `BEGIN`** and
/// held for the lifetime of that transaction (see
/// [`TransactionState::Active`]). All reads within the transaction
/// consult the same snapshot, so a transaction sees a stable view of the
/// database regardless of concurrent commits.
///
/// # Fields
///
/// - [`xmin_active`](Self::xmin_active) — the lowest [`TxnId`] that was **still running** at
///   snapshot time. Any row whose `xmax > xmin_active` was deleted by a transaction that started
///   after our snapshot's oldest concurrent peer, and thus may not yet have been committed when we
///   took our snapshot.
/// - [`xmax_committed`](Self::xmax_committed) — the largest [`TxnId`] that had **already
///   committed** at snapshot time. Rows whose `xmin > xmax_committed` were definitely created after
///   our snapshot and are invisible.
/// - [`in_progress`](Self::in_progress) — the set of transactions that were still running at
///   snapshot time. Writes by these transactions are invisible to us regardless of where their
///   `TxnId` falls relative to `xmin_active` / `xmax_committed`.
///
/// # Phase 1b note
///
/// This struct is intentionally `Clone` (cheap modulo the `HashSet`) so
/// `TransactionState::Active` can hold it by value without lifetime
/// gymnastics. The `in_progress` set is typically small (one per
/// concurrent active transaction); in single-writer workloads it is
/// usually empty.
///
/// [`TxnId`]: crate::row::TxnId
/// [`TransactionState::Active`]: crate::database::TransactionState
#[derive(Debug, Clone, Default)]
pub struct TxnSnapshot {
    /// Lowest `TxnId` of any transaction that was still running when
    /// this snapshot was captured. A row with `xmax > xmin_active` was
    /// deleted by a transaction that didn't exist (or wasn't yet
    /// committed) when our snapshot was taken, so the deletion is not
    /// yet visible to us.
    ///
    /// If no transactions were active at capture time, this is set to
    /// the next [`TxnId`] that *would* be allocated (i.e., one past the
    /// high-water mark), making the `xmax > xmin_active` clause always
    /// fail — which is the correct behavior: with no concurrent
    /// transactions, any deletion we see is definitely committed.
    pub xmin_active: TxnId,
    /// Largest `TxnId` that had committed at snapshot time. Rows with
    /// `xmin > xmax_committed` were created after our snapshot and are
    /// invisible.
    ///
    /// Note that the pre-MVCC sentinel ([`PRE_MVCC_TXN_ID`] = 0) is `<=`
    /// any `xmax_committed >= 0`, so legacy rows are always visible.
    ///
    /// [`PRE_MVCC_TXN_ID`]: crate::row::PRE_MVCC_TXN_ID
    pub xmax_committed: TxnId,
    /// Set of transactions that were still running at snapshot time.
    /// Writes (and deletions) by these transactions are invisible to us
    /// under snapshot isolation, even if their `TxnId` is `<=`
    /// `xmax_committed` (which can happen if they had started before
    /// some transaction we *can* see).
    ///
    /// This set never contains the snapshot's own transaction id: a
    /// transaction always sees its own writes (currently handled at the
    /// catalog/table layer; Phase 1c will reconcile this with the
    /// `xmin == self.txn_id` case if it changes).
    pub in_progress: HashSet<TxnId>,
}

impl TxnSnapshot {
    /// Create a snapshot in which **no MVCC versioning is active**.
    ///
    /// This is the snapshot used by:
    ///
    /// - Auto-commit reads (no enclosing transaction).
    /// - Pre-MVCC code paths (the `mvcc_enabled` feature is off).
    /// - As a placeholder default before a real snapshot is captured.
    ///
    /// With this snapshot, only rows stamped with
    /// [`PRE_MVCC_TXN_ID`](crate::row::PRE_MVCC_TXN_ID) (== `0`) and a
    /// `None` `xmax` are visible. The `xmax_committed = 0` field makes
    /// every non-sentinel `xmin` fail the visibility check, which is
    /// safe-by-default — code that hasn't been migrated to capture real
    /// snapshots will see exactly the rows it saw before Phase 1.
    pub fn empty() -> Self {
        TxnSnapshot { xmin_active: 0, xmax_committed: 0, in_progress: HashSet::new() }
    }

    /// Create a snapshot from the explicit components.
    ///
    /// Intended for tests and for the transaction manager's capture
    /// logic; production code should prefer the manager-driven helpers
    /// in `database::transactions`.
    pub fn new(xmin_active: TxnId, xmax_committed: TxnId, in_progress: HashSet<TxnId>) -> Self {
        TxnSnapshot { xmin_active, xmax_committed, in_progress }
    }

    /// Returns `true` if `txn` was committed-as-of this snapshot.
    ///
    /// A transaction is considered committed iff:
    /// - its `TxnId` is `<= xmax_committed`, AND
    /// - it is not in the `in_progress` set.
    ///
    /// The pre-MVCC sentinel ([`PRE_MVCC_TXN_ID`](crate::row::PRE_MVCC_TXN_ID)
    /// = 0) is always considered committed.
    #[inline]
    pub fn is_committed(&self, txn: TxnId) -> bool {
        if txn == crate::row::PRE_MVCC_TXN_ID {
            return true;
        }
        txn <= self.xmax_committed && !self.in_progress.contains(&txn)
    }

    /// Returns `true` if `txn` was still running at snapshot time.
    #[inline]
    pub fn is_in_progress(&self, txn: TxnId) -> bool {
        self.in_progress.contains(&txn)
    }
}

// ============================================================================
// Write-path stamping helpers (Phase 1c of #5136)
// ============================================================================
//
// These helpers are the single source of truth for "should this write stamp
// xmin/xmax with the current transaction id?". The decision is gated on the
// `mvcc_enabled` feature flag:
//
// - Feature OFF (default): all helpers are no-ops. Rows pass through with their constructor
//   defaults (xmin = PRE_MVCC_TXN_ID, xmax = None). This preserves bit-for-bit pre-MVCC behavior.
// - Feature ON: when called with `Some(txn_id)`, the helpers stamp the row with the active
//   transaction id. When called with `None` (autocommit path with no active transaction), the row
//   keeps the pre-MVCC sentinel. Phase 1d will revisit autocommit semantics; this is the
//   conservative choice for Phase 1c.
//
// The helpers operate by `&mut Row` so the caller can choose where they
// fit into the write pipeline (validators may run before or after
// stamping; the stamp is purely additive to existing row state).

use crate::row::Row;

/// Stamp `row.xmin = txn_id` (when MVCC is enabled and a transaction is
/// active). For INSERT / UPDATE-new-version writes.
///
/// With the `mvcc_enabled` feature OFF this is a no-op — the row keeps
/// whatever `xmin` it came in with, which for executor-produced rows is
/// always [`PRE_MVCC_TXN_ID`](crate::row::PRE_MVCC_TXN_ID).
///
/// With the feature ON and `txn_id = Some(t)`, the row's xmin field is
/// set to `t`. With the feature ON and `txn_id = None` (no active
/// transaction at write time — e.g. autocommit) the row is left with
/// the pre-MVCC sentinel; Phase 1d will revisit autocommit semantics.
#[inline]
pub fn stamp_xmin_for_write(row: &mut Row, txn_id: Option<TxnId>) {
    #[cfg(feature = "mvcc_enabled")]
    {
        if let Some(id) = txn_id {
            row.xmin = id;
        }
    }
    #[cfg(not(feature = "mvcc_enabled"))]
    {
        // Suppress unused-variable warnings in the off-state.
        let _ = (row, txn_id);
    }
}

/// Stamp `row.xmax = Some(txn_id)` (when MVCC is enabled and a
/// transaction is active). For UPDATE-old-version and DELETE writes.
///
/// With the `mvcc_enabled` feature OFF this is a no-op — the row keeps
/// `xmax = None`, matching the pre-MVCC contract that "live rows have no
/// deleter."
///
/// With the feature ON and `txn_id = Some(t)`, the row's xmax field is
/// set to `Some(t)`. The physical storage layer continues to remove or
/// overwrite the row as before; the stamp is purely additive metadata
/// that Phase 1d's visibility filter will consult. With the feature ON
/// and `txn_id = None` (autocommit), the row is left with `xmax = None`.
#[inline]
pub fn stamp_xmax_for_write(row: &mut Row, txn_id: Option<TxnId>) {
    #[cfg(feature = "mvcc_enabled")]
    {
        if let Some(id) = txn_id {
            row.xmax = Some(id);
        }
    }
    #[cfg(not(feature = "mvcc_enabled"))]
    {
        let _ = (row, txn_id);
    }
}

/// Returns true when the `mvcc_enabled` feature is compiled in.
///
/// This is a compile-time constant exposed as a runtime fn so callers
/// outside this crate can branch on feature state without re-declaring
/// the `#[cfg]` themselves.
#[inline]
pub const fn mvcc_enabled() -> bool {
    cfg!(feature = "mvcc_enabled")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_snapshot_treats_only_pre_mvcc_as_committed() {
        let s = TxnSnapshot::empty();
        assert!(s.is_committed(0));
        assert!(!s.is_committed(1));
        assert!(!s.is_committed(100));
        assert!(!s.is_in_progress(0));
        assert!(!s.is_in_progress(1));
    }

    #[test]
    fn is_committed_pre_mvcc_sentinel_is_always_visible() {
        let mut in_progress = HashSet::new();
        in_progress.insert(0); // Even if someone weirdly puts 0 in in_progress,
                               // the sentinel rule wins.
        let s = TxnSnapshot::new(1, 50, in_progress);
        assert!(s.is_committed(0));
    }

    #[test]
    fn is_committed_respects_in_progress_set() {
        let mut in_progress = HashSet::new();
        in_progress.insert(5);
        let s = TxnSnapshot::new(5, 10, in_progress);
        assert!(s.is_committed(3), "txn 3 < xmax_committed and not in_progress");
        assert!(!s.is_committed(5), "txn 5 is in_progress");
        assert!(s.is_committed(10), "txn 10 == xmax_committed and not in_progress");
        assert!(!s.is_committed(11), "txn 11 > xmax_committed");
    }

    #[test]
    fn snapshot_clones_cheaply() {
        let mut in_progress = HashSet::new();
        in_progress.insert(7);
        in_progress.insert(8);
        let s = TxnSnapshot::new(7, 12, in_progress);
        let s2 = s.clone();
        assert_eq!(s.xmin_active, s2.xmin_active);
        assert_eq!(s.xmax_committed, s2.xmax_committed);
        assert_eq!(s.in_progress, s2.in_progress);
    }

    // ========================================================================
    // Write-path stamping helpers (Phase 1c of #5136)
    // ========================================================================
    //
    // These tests check the contract documented on `stamp_xmin_for_write` and
    // `stamp_xmax_for_write`: both helpers are no-ops without the feature
    // flag, and stamp the row's MVCC fields when the flag is on AND a txn
    // id is supplied.

    use vibesql_types::SqlValue;

    use crate::row::PRE_MVCC_TXN_ID;

    fn fresh_row() -> Row {
        Row::new(vec![SqlValue::Integer(42)])
    }

    #[test]
    fn stamp_xmin_with_none_is_noop() {
        let mut r = fresh_row();
        stamp_xmin_for_write(&mut r, None);
        assert_eq!(r.xmin, PRE_MVCC_TXN_ID);
    }

    #[test]
    fn stamp_xmax_with_none_is_noop() {
        let mut r = fresh_row();
        stamp_xmax_for_write(&mut r, None);
        assert_eq!(r.xmax, None);
    }

    #[cfg(not(feature = "mvcc_enabled"))]
    #[test]
    fn stamp_xmin_off_state_is_noop_even_with_some() {
        // With feature OFF, even Some(t) must be ignored — this preserves
        // bit-for-bit pre-MVCC row construction.
        let mut r = fresh_row();
        stamp_xmin_for_write(&mut r, Some(7));
        assert_eq!(r.xmin, PRE_MVCC_TXN_ID, "xmin must remain pre-MVCC sentinel");
    }

    #[cfg(not(feature = "mvcc_enabled"))]
    #[test]
    fn stamp_xmax_off_state_is_noop_even_with_some() {
        let mut r = fresh_row();
        stamp_xmax_for_write(&mut r, Some(7));
        assert_eq!(r.xmax, None, "xmax must remain None");
    }

    #[cfg(feature = "mvcc_enabled")]
    #[test]
    fn stamp_xmin_on_state_writes_txn_id() {
        let mut r = fresh_row();
        stamp_xmin_for_write(&mut r, Some(42));
        assert_eq!(r.xmin, 42);
        assert_eq!(r.xmax, None, "stamping xmin must not touch xmax");
    }

    #[cfg(feature = "mvcc_enabled")]
    #[test]
    fn stamp_xmax_on_state_writes_txn_id() {
        let mut r = fresh_row();
        stamp_xmax_for_write(&mut r, Some(99));
        assert_eq!(r.xmax, Some(99));
        assert_eq!(r.xmin, PRE_MVCC_TXN_ID, "stamping xmax must not touch xmin");
    }

    #[cfg(feature = "mvcc_enabled")]
    #[test]
    fn stamp_both_xmin_and_xmax_independently() {
        // Models the UPDATE old-version case: caller may want both fields
        // stamped on the same row (e.g. for a two-version replay buffer).
        let mut r = fresh_row();
        stamp_xmin_for_write(&mut r, Some(3));
        stamp_xmax_for_write(&mut r, Some(7));
        assert_eq!(r.xmin, 3);
        assert_eq!(r.xmax, Some(7));
    }

    #[test]
    fn mvcc_enabled_const_matches_feature() {
        // Sanity check: the const fn agrees with the cfg() the rest of the
        // module uses. Keeps the helper from drifting if the feature name
        // changes.
        #[cfg(feature = "mvcc_enabled")]
        assert!(mvcc_enabled());
        #[cfg(not(feature = "mvcc_enabled"))]
        assert!(!mvcc_enabled());
    }
}
