// ============================================================================
// MVCC Vacuum Tests (#5208 — MVCC Phase 1d follow-up)
// ============================================================================
//
// Tests for the on-demand MVCC garbage-collection API exposed at the
// `Database::vacuum_mvcc` level. These tests cover both feature states
// — the off-state must be a strict no-op (returning 0), and the on-state
// must reclaim old versions while preserving query correctness.

use vibesql_catalog::{ColumnSchema, TableSchema};
use vibesql_types::{DataType, SqlValue};

use crate::database::Database;
use crate::row::Row;

fn make_users_table(db: &mut Database) {
    let columns = vec![
        ColumnSchema::new("id".to_string(), DataType::Integer, false),
        ColumnSchema::new("name".to_string(), DataType::Varchar { max_length: Some(50) }, true),
    ];
    let schema = TableSchema::with_primary_key(
        "users".to_string(),
        columns,
        vec!["id".to_string()],
    );
    db.create_table(schema).unwrap();
}

fn user_row(id: i64, name: &str) -> Row {
    Row::from_vec(vec![SqlValue::Integer(id), SqlValue::Varchar(arcstr::ArcStr::from(name))])
}

#[test]
fn vacuum_mvcc_on_empty_database_returns_zero() {
    // Trivial smoke test: a brand-new database has nothing to GC,
    // regardless of feature state.
    let mut db = Database::new();
    let reclaimed = db.vacuum_mvcc().unwrap();
    assert_eq!(reclaimed, 0);
}

#[test]
fn vacuum_mvcc_with_no_stamped_tombstones_is_a_noop() {
    // Off-state contract: when no rows have xmax stamped (which is the
    // case in the off-state — the executor never stamps), vacuum is a
    // no-op. Run on a populated table and verify the row count is
    // unchanged.
    let mut db = Database::new();
    make_users_table(&mut db);
    db.insert_row("users", user_row(1, "Alice")).unwrap();
    db.insert_row("users", user_row(2, "Bob")).unwrap();
    db.insert_row("users", user_row(3, "Charlie")).unwrap();

    let reclaimed = db.vacuum_mvcc().unwrap();
    assert_eq!(reclaimed, 0);

    let table = db.get_table("users").unwrap();
    assert_eq!(table.row_count(), 3);
}

#[test]
fn vacuum_mvcc_refuses_to_run_inside_transaction() {
    // v1 contract: vacuum cannot run while a transaction is active.
    let mut db = Database::new();
    make_users_table(&mut db);
    db.begin_transaction().unwrap();
    let err = db.vacuum_mvcc().expect_err("vacuum must refuse mid-transaction");
    let msg = format!("{err}");
    assert!(
        msg.contains("vacuum_mvcc"),
        "error message should mention vacuum_mvcc, got: {msg}"
    );
    // Clean up so we don't leak the active txn into subsequent tests.
    db.rollback_transaction().unwrap();
}

#[cfg(feature = "mvcc_enabled")]
#[test]
fn vacuum_mvcc_reclaims_committed_deletions() {
    // With MVCC on, perform an autocommit-style INSERT + DELETE pair
    // outside any explicit transaction. The DELETE path stamps xmax
    // (when mvcc_enabled) which the GC sweep can then reclaim.
    //
    // Note: under the current single-writer write-path, DELETE both
    // stamps xmax AND bitmap-deletes the row, so the row is already
    // out of `scan_live_vec` results. The GC sweep is still
    // exercising the right primitive — it finds the stamped row in
    // the deletion bitmap, but the test below uses a hand-stamped
    // row to give the sweep something to physically reclaim.
    let mut db = Database::new();
    make_users_table(&mut db);
    db.insert_row("users", user_row(1, "Alice")).unwrap();
    db.insert_row("users", user_row(2, "Bob")).unwrap();
    db.insert_row("users", user_row(3, "Charlie")).unwrap();

    // Stamp row 1 with a committed xmax directly on the table layer
    // to simulate a deferred-tombstone state without depending on
    // the executor's specific write-path semantics.
    {
        let table = db.get_table_mut("users").unwrap();
        table.stamp_row_xmax_inplace(1, 1);
    }

    // No active transaction, so horizon = next_transaction_id = 1.
    // Row 1 has xmax = 1, which is NOT < 1, so it should NOT be
    // reclaimed yet.
    assert_eq!(db.compute_gc_horizon(), 1);
    let reclaimed = db.vacuum_mvcc().unwrap();
    assert_eq!(reclaimed, 0, "row at horizon boundary must be retained");

    // Now begin + commit a no-op txn to advance the watermark, then
    // GC: horizon = 2, row 1's xmax = 1 < 2, so it's reclaimable.
    db.begin_transaction().unwrap();
    db.commit_transaction().unwrap();
    assert_eq!(db.compute_gc_horizon(), 2);

    let reclaimed = db.vacuum_mvcc().unwrap();
    assert_eq!(reclaimed, 1, "row stamped before horizon must be reclaimed");

    // Other rows should still be present.
    let table = db.get_table("users").unwrap();
    assert_eq!(table.row_count(), 2);
}

#[cfg(feature = "mvcc_enabled")]
#[test]
fn vacuum_mvcc_preserves_subsequent_query_correctness() {
    // After vacuum, the table must remain queryable and the indexes
    // must still resolve PK lookups correctly. This is the primary
    // safety property: GC must not break correctness for subsequent
    // reads.
    let mut db = Database::new();
    make_users_table(&mut db);
    db.insert_row("users", user_row(1, "Alice")).unwrap();
    db.insert_row("users", user_row(2, "Bob")).unwrap();
    db.insert_row("users", user_row(3, "Charlie")).unwrap();
    db.insert_row("users", user_row(4, "Dave")).unwrap();

    // Tombstone rows 0 and 2 directly.
    {
        let table = db.get_table_mut("users").unwrap();
        table.stamp_row_xmax_inplace(0, 1);
        table.stamp_row_xmax_inplace(2, 1);
    }

    // Advance horizon past 1.
    db.begin_transaction().unwrap();
    db.commit_transaction().unwrap();

    let reclaimed = db.vacuum_mvcc().unwrap();
    assert_eq!(reclaimed, 2);

    // The surviving rows (id = 2, id = 4) must remain queryable via
    // their PK and produce the expected values.
    let table = db.get_table("users").unwrap();
    assert_eq!(table.row_count(), 2);

    // Use the PK index to look up surviving rows.
    let pk_index =
        table.primary_key_index().expect("PK index must survive GC compaction");
    assert!(pk_index.contains_key(&vec![SqlValue::Integer(2)]));
    assert!(pk_index.contains_key(&vec![SqlValue::Integer(4)]));
    assert!(
        !pk_index.contains_key(&vec![SqlValue::Integer(1)]),
        "reclaimed row's PK must be gone from index"
    );
    assert!(
        !pk_index.contains_key(&vec![SqlValue::Integer(3)]),
        "reclaimed row's PK must be gone from index"
    );
}

// ============================================================================
// GC horizon pins (Raft Phase B1, #5199)
// ============================================================================
//
// A horizon pin holds `compute_gc_horizon` back exactly like an active
// transaction would, but without occupying the single-writer transaction
// slot. The consensus layer acquires one for the duration of a Raft
// snapshot build (see `vibesql-consensus`'s `SnapshotHorizonPin`).

#[test]
fn gc_horizon_pin_holds_horizon_and_release_restores_it() {
    // Feature-independent: the pin operates on `compute_gc_horizon`
    // arithmetic, which exists in both feature states.
    let mut db = Database::new();
    assert_eq!(db.compute_gc_horizon(), 1);

    let pin = db.pin_gc_horizon();

    // Advance the allocator watermark with two no-op transactions.
    for _ in 0..2 {
        db.begin_transaction().unwrap();
        db.commit_transaction().unwrap();
    }
    // Unpinned, the horizon would now be next_transaction_id = 3; the
    // pin holds it at the value captured at acquire time.
    assert_eq!(db.compute_gc_horizon(), 1, "pin must hold the horizon back");

    db.release_gc_horizon(pin);
    assert_eq!(db.compute_gc_horizon(), 3, "release must let the horizon advance");

    // Releasing an unknown pin is a no-op.
    db.release_gc_horizon(9999);
    assert_eq!(db.compute_gc_horizon(), 3);
}

#[test]
fn gc_horizon_pins_combine_to_the_minimum() {
    let mut db = Database::new();
    let early_pin = db.pin_gc_horizon(); // pinned at 1

    db.begin_transaction().unwrap();
    db.commit_transaction().unwrap();
    let late_pin = db.pin_gc_horizon(); // pinned at min(2, early pin 1) = 1

    db.begin_transaction().unwrap();
    db.commit_transaction().unwrap();
    assert_eq!(db.compute_gc_horizon(), 1, "lowest pin wins");

    db.release_gc_horizon(early_pin);
    // The late pin captured the horizon *while the early pin was held*,
    // so it pinned the still-held-back value.
    assert_eq!(db.compute_gc_horizon(), 1);

    db.release_gc_horizon(late_pin);
    assert_eq!(db.compute_gc_horizon(), 3);
}

#[test]
fn gc_horizon_pin_does_not_block_transactions() {
    // Unlike an active transaction, a pin leaves the single-writer
    // transaction slot free: begin/commit while pinned must work. This
    // is the property that lets a Raft snapshot build coexist with
    // applying committed log entries (Phase B1, #5199).
    let mut db = Database::new();
    make_users_table(&mut db);

    let pin = db.pin_gc_horizon();
    db.begin_transaction().unwrap();
    db.insert_row("users", user_row(1, "Alice")).unwrap();
    db.commit_transaction().unwrap();
    db.release_gc_horizon(pin);

    assert_eq!(db.get_table("users").unwrap().row_count(), 1);
}

#[cfg(feature = "mvcc_enabled")]
#[test]
fn vacuum_mvcc_held_back_by_gc_horizon_pin() {
    // The end-to-end property the consensus layer relies on: while a
    // pin is held, `vacuum_mvcc` must not reclaim row versions that
    // were visible when the pin was acquired — even though the
    // allocator watermark has advanced past their tombstones.
    let mut db = Database::new();
    make_users_table(&mut db);
    db.insert_row("users", user_row(1, "Alice")).unwrap();
    db.insert_row("users", user_row(2, "Bob")).unwrap();

    // Tombstone row 0 with a committed xmax, as in
    // `vacuum_mvcc_reclaims_committed_deletions`.
    {
        let table = db.get_table_mut("users").unwrap();
        table.stamp_row_xmax_inplace(0, 1);
    }

    // Pin while the horizon is still 1 (nothing reclaimable yet).
    let pin = db.pin_gc_horizon();

    // Advance the watermark past the tombstone.
    db.begin_transaction().unwrap();
    db.commit_transaction().unwrap();

    // Pinned: the sweep must reclaim nothing.
    assert_eq!(db.vacuum_mvcc().unwrap(), 0, "pinned horizon must block reclamation");
    assert_eq!(db.get_table("users").unwrap().row_count(), 2);

    // Released: the tombstoned version becomes reclaimable.
    db.release_gc_horizon(pin);
    assert_eq!(db.vacuum_mvcc().unwrap(), 1, "released pin must allow reclamation");
    assert_eq!(db.get_table("users").unwrap().row_count(), 1);
}

// ============================================================================
// Replication txn-id override (Raft Phase B1, #5199)
// ============================================================================

#[test]
fn set_next_txn_id_controls_allocation_and_rejects_active_txn() {
    let mut db = Database::new();

    db.set_next_txn_id(42).unwrap();
    db.begin_transaction().unwrap();
    assert_eq!(db.transaction_id(), Some(42), "next BEGIN must use the overridden id");

    // The id of an in-flight transaction cannot change.
    let err = db.set_next_txn_id(99).expect_err("must refuse inside a transaction");
    assert!(format!("{err}").contains("set_next_txn_id"), "unexpected error: {err}");

    db.commit_transaction().unwrap();
    db.set_next_txn_id(99).unwrap();
    db.begin_transaction().unwrap();
    assert_eq!(db.transaction_id(), Some(99));
    db.rollback_transaction().unwrap();
}

#[cfg(feature = "mvcc_enabled")]
#[test]
fn vacuum_mvcc_horizon_held_back_by_active_transaction() {
    // Even when there's a stamped tombstone that would otherwise be
    // reclaimable, an active transaction holds the GC horizon back so
    // that transaction's snapshot remains stable.
    //
    // Setup: insert 3 rows in txn 1, commit. Stamp row 0's xmax = 1
    // (simulating a committed delete). Then begin txn 2 and try to
    // vacuum from outside? No — vacuum refuses inside a txn.
    //
    // Instead we check `compute_gc_horizon` while a txn is active to
    // confirm the horizon is held back (the vacuum API itself
    // wouldn't run here).
    let mut db = Database::new();
    make_users_table(&mut db);
    db.insert_row("users", user_row(1, "Alice")).unwrap();

    {
        let table = db.get_table_mut("users").unwrap();
        table.stamp_row_xmax_inplace(0, 1);
    }

    // Without an active txn: horizon = next_id (= 1 — no committed
    // txns yet), so the stamp at xmax = 1 is NOT reclaimable.
    assert_eq!(db.compute_gc_horizon(), 1);

    // Begin a transaction: the horizon must now reflect this txn's
    // xmin_active, which is held BACK from advancing past it.
    db.begin_transaction().unwrap();
    let horizon_during_txn = db.compute_gc_horizon();
    let snap = db.current_snapshot().unwrap();
    assert_eq!(horizon_during_txn, snap.xmin_active);

    db.commit_transaction().unwrap();
}
