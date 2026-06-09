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
