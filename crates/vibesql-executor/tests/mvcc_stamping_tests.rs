//! Integration tests for Phase 1c (#5150 of #5136) write-path MVCC stamping.
//!
//! These tests cover the contract documented on [`vibesql_storage::mvcc`]'s
//! Phase 1c helpers (`stamp_xmin_for_write`, `stamp_xmax_for_write`):
//!
//! - With the `mvcc_enabled` feature OFF (default): INSERT, UPDATE and
//!   DELETE produce rows with the pre-MVCC sentinel
//!   (`xmin = PRE_MVCC_TXN_ID = 0`, `xmax = None`). Bit-for-bit pre-MVCC
//!   behavior.
//! - With the feature ON and an active transaction: INSERT stamps
//!   `xmin = current_txn_id`; UPDATE stamps `xmin = current_txn_id` on
//!   the in-place new row; DELETE stamps `xmax = current_txn_id` on the
//!   bitmap-deleted row (still observable via `Table::scan()`).
//!
//! The feature-ON assertions are wrapped in `#[cfg(feature = "mvcc_enabled")]`
//! so the file compiles and runs cleanly under both `cargo test` (off-state)
//! and `cargo test --features mvcc_enabled` (on-state).
//!
//! Run with:
//! ```text
//! cargo test -p vibesql-executor --test mvcc_stamping_tests
//! cargo test -p vibesql-executor --test mvcc_stamping_tests --features mvcc_enabled
//! ```

use vibesql_ast::Statement;
use vibesql_executor::{CreateTableExecutor, DeleteExecutor, InsertExecutor, UpdateExecutor};
use vibesql_parser::Parser;
use vibesql_storage::{Database, Row, PRE_MVCC_TXN_ID};
use vibesql_types::SqlValue;

/// Execute one SQL statement against `db`. Limited to the statement kinds
/// our stamping tests need.
fn exec(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).expect("parse failed");
    match stmt {
        Statement::CreateTable(s) => {
            CreateTableExecutor::execute(&s, db).expect("CREATE failed");
        }
        Statement::Insert(s) => {
            InsertExecutor::execute(db, &s).expect("INSERT failed");
        }
        Statement::Update(s) => {
            UpdateExecutor::execute(&s, db).expect("UPDATE failed");
        }
        Statement::Delete(s) => {
            DeleteExecutor::execute(&s, db).expect("DELETE failed");
        }
        other => panic!("unsupported statement in test helper: {:?}", other),
    }
}

/// Build a fresh database with a single `users(id INTEGER PRIMARY KEY, name VARCHAR)`
/// table containing a few rows.
fn db_with_users() -> Database {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(50))");
    exec(&mut db, "INSERT INTO users VALUES (1, 'Alice')");
    exec(&mut db, "INSERT INTO users VALUES (2, 'Bob')");
    exec(&mut db, "INSERT INTO users VALUES (3, 'Charlie')");
    db
}

/// Helper: pull the raw rows slice from `users` (includes bitmap-deleted
/// rows so we can inspect tombstone stamps after DELETE).
fn raw_rows<'a>(db: &'a Database, table: &str) -> &'a [Row] {
    db.get_table(table).expect("table missing").scan()
}

// ============================================================================
// Off-state (no feature flag) tests — always compiled.
//
// These assert the "bit-for-bit pre-MVCC" contract: regardless of whether
// the feature is on or off, autocommit writes outside an explicit
// transaction must produce rows with the pre-MVCC sentinel. Phase 1c's
// txn-id stamping only kicks in when a transaction is active.
// ============================================================================

#[test]
fn insert_outside_txn_uses_pre_mvcc_sentinel() {
    let db = db_with_users();
    for row in raw_rows(&db, "USERS") {
        assert_eq!(
            row.xmin, PRE_MVCC_TXN_ID,
            "autocommit INSERT must produce xmin = PRE_MVCC_TXN_ID regardless of feature flag"
        );
        assert_eq!(row.xmax, None, "fresh INSERT must produce xmax = None");
    }
}

#[test]
fn update_outside_txn_keeps_pre_mvcc_sentinel_on_new_version() {
    let mut db = db_with_users();
    exec(&mut db, "UPDATE users SET name = 'Alice2' WHERE id = 1");

    // Find the (now-updated) row for id=1 and inspect it. The fast path
    // overwrites the row in place, so we look up by primary key value.
    let updated = raw_rows(&db, "USERS")
        .iter()
        .find(|r| matches!(r.values.first(), Some(SqlValue::Integer(1))))
        .expect("id=1 row should still exist");
    assert_eq!(
        updated.xmin, PRE_MVCC_TXN_ID,
        "autocommit UPDATE must leave xmin = PRE_MVCC_TXN_ID outside a transaction"
    );
    assert_eq!(updated.xmax, None, "live updated row must have xmax = None");
}

#[test]
fn delete_outside_txn_keeps_pre_mvcc_xmax() {
    let mut db = db_with_users();
    exec(&mut db, "DELETE FROM users WHERE id = 2");

    // After DELETE, the row remains in the raw rows slice (bitmap deleted),
    // so we can still inspect its xmax. Outside a transaction, we expect
    // it to remain None (pre-MVCC behavior).
    let deleted_row = raw_rows(&db, "USERS")
        .iter()
        .find(|r| matches!(r.values.first(), Some(SqlValue::Integer(2))))
        .expect("bitmap-deleted row id=2 should still be in raw rows");
    assert_eq!(
        deleted_row.xmax, None,
        "autocommit DELETE outside a transaction must leave xmax = None"
    );
}

// ============================================================================
// Off-state (feature OFF only) tests — assert the "no stamping at all" path.
//
// Even inside an explicit transaction, with the feature OFF we must NOT
// stamp xmin/xmax — the v7 storage format keeps working but visibility
// semantics are unchanged from pre-MVCC.
// ============================================================================

#[cfg(not(feature = "mvcc_enabled"))]
#[test]
fn insert_in_txn_off_state_uses_pre_mvcc_sentinel() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(50))");
    db.begin_transaction().unwrap();
    exec(&mut db, "INSERT INTO users VALUES (1, 'Alice')");
    let xmin_after_insert = raw_rows(&db, "USERS")[0].xmin;
    db.commit_transaction().unwrap();

    assert_eq!(
        xmin_after_insert, PRE_MVCC_TXN_ID,
        "with mvcc_enabled OFF, INSERT inside a txn must leave xmin = PRE_MVCC_TXN_ID"
    );
}

#[cfg(not(feature = "mvcc_enabled"))]
#[test]
fn delete_in_txn_off_state_keeps_xmax_none() {
    let mut db = db_with_users();
    db.begin_transaction().unwrap();
    exec(&mut db, "DELETE FROM users WHERE id = 1");

    let row = raw_rows(&db, "USERS")
        .iter()
        .find(|r| matches!(r.values.first(), Some(SqlValue::Integer(1))))
        .expect("bitmap-deleted row should still be in raw rows");
    let xmax = row.xmax;
    db.commit_transaction().unwrap();

    assert_eq!(xmax, None, "with mvcc_enabled OFF, DELETE must leave xmax = None even inside a txn");
}

#[cfg(not(feature = "mvcc_enabled"))]
#[test]
fn update_in_txn_off_state_keeps_pre_mvcc_xmin() {
    let mut db = db_with_users();
    db.begin_transaction().unwrap();
    exec(&mut db, "UPDATE users SET name = 'Alice2' WHERE id = 1");

    let updated = raw_rows(&db, "USERS")
        .iter()
        .find(|r| matches!(r.values.first(), Some(SqlValue::Integer(1))))
        .cloned()
        .expect("id=1 row should still exist");
    db.commit_transaction().unwrap();

    assert_eq!(
        updated.xmin, PRE_MVCC_TXN_ID,
        "with mvcc_enabled OFF, UPDATE must leave xmin = PRE_MVCC_TXN_ID even inside a txn"
    );
}

// ============================================================================
// On-state (feature ON only) tests — assert the actual stamping behavior.
//
// These are the new Phase 1c invariants the issue asks for. Each test
// runs inside an explicit transaction so a real txn id is in scope.
// ============================================================================

#[cfg(feature = "mvcc_enabled")]
#[test]
fn insert_in_txn_stamps_xmin_with_current_txn_id() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(50))");

    db.begin_transaction().unwrap();
    let txn_id = db.transaction_id().expect("txn id must exist inside BEGIN");
    exec(&mut db, "INSERT INTO users VALUES (1, 'Alice')");
    exec(&mut db, "INSERT INTO users VALUES (2, 'Bob')");

    let rows = raw_rows(&db, "USERS");
    assert_eq!(rows.len(), 2, "two rows inserted");
    for row in rows {
        assert_eq!(
            row.xmin, txn_id,
            "every newly INSERTed row must carry xmin = current_txn_id"
        );
        assert_eq!(row.xmax, None, "new rows must have xmax = None");
    }

    db.commit_transaction().unwrap();
}

#[cfg(feature = "mvcc_enabled")]
#[test]
fn update_in_txn_stamps_xmin_on_new_version() {
    // Set up rows in an initial transaction so they have a known xmin.
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(50))");
    db.begin_transaction().unwrap();
    let inserter_txn = db.transaction_id().unwrap();
    exec(&mut db, "INSERT INTO users VALUES (1, 'Alice')");
    db.commit_transaction().unwrap();

    // Now UPDATE in a second transaction. The new row version should be
    // stamped with the *updater's* txn id, not the inserter's.
    db.begin_transaction().unwrap();
    let updater_txn = db.transaction_id().unwrap();
    assert_ne!(inserter_txn, updater_txn, "second txn id should differ");
    exec(&mut db, "UPDATE users SET name = 'Alice2' WHERE id = 1");

    let updated = raw_rows(&db, "USERS")
        .iter()
        .find(|r| matches!(r.values.first(), Some(SqlValue::Integer(1))))
        .cloned()
        .expect("id=1 row must exist after update");

    db.commit_transaction().unwrap();

    assert_eq!(
        updated.xmin, updater_txn,
        "UPDATE must stamp the new row version's xmin with the UPDATER's txn id"
    );
    assert_eq!(updated.xmax, None, "live new version must have xmax = None");
    assert_eq!(
        updated.values[1],
        SqlValue::Varchar(arcstr::ArcStr::from("Alice2")),
        "sanity: the update did change the value"
    );
}

#[cfg(feature = "mvcc_enabled")]
#[test]
fn delete_in_txn_stamps_xmax_on_tombstone() {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(50))");

    db.begin_transaction().unwrap();
    let inserter_txn = db.transaction_id().unwrap();
    exec(&mut db, "INSERT INTO users VALUES (1, 'Alice')");
    exec(&mut db, "INSERT INTO users VALUES (2, 'Bob')");
    db.commit_transaction().unwrap();

    // Now DELETE in a second transaction. The bitmap-deleted row should
    // have xmax stamped with the deleter's txn id, observable via
    // `Table::scan()` which returns ALL rows including bitmap-deleted.
    db.begin_transaction().unwrap();
    let deleter_txn = db.transaction_id().unwrap();
    assert_ne!(inserter_txn, deleter_txn);
    exec(&mut db, "DELETE FROM users WHERE id = 1");

    let tombstone = raw_rows(&db, "USERS")
        .iter()
        .find(|r| matches!(r.values.first(), Some(SqlValue::Integer(1))))
        .cloned()
        .expect("bitmap-deleted row id=1 should still be in raw rows");

    db.commit_transaction().unwrap();

    assert_eq!(
        tombstone.xmin, inserter_txn,
        "deleted row's xmin must still be the original inserter's txn id"
    );
    assert_eq!(
        tombstone.xmax,
        Some(deleter_txn),
        "DELETE must stamp xmax = current_txn_id on the bitmap-deleted row"
    );
}

#[cfg(feature = "mvcc_enabled")]
#[test]
fn batch_insert_in_txn_stamps_all_rows_with_same_xmin() {
    // INSERT ... VALUES (...), (...), (...) goes through the batch path
    // (`insert_rows_batch`). Each row must get the same xmin.
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(50))");
    db.begin_transaction().unwrap();
    let txn_id = db.transaction_id().unwrap();
    exec(
        &mut db,
        "INSERT INTO users VALUES (1, 'a'), (2, 'b'), (3, 'c'), (4, 'd'), (5, 'e')",
    );

    let rows: Vec<Row> = raw_rows(&db, "USERS").to_vec();
    db.commit_transaction().unwrap();

    assert_eq!(rows.len(), 5);
    for row in &rows {
        assert_eq!(
            row.xmin, txn_id,
            "every row in a batch INSERT must carry the same xmin = current_txn_id"
        );
    }
}

#[cfg(feature = "mvcc_enabled")]
#[test]
fn autocommit_insert_with_feature_on_uses_pre_mvcc_sentinel() {
    // Phase 1c conservative choice: with no active transaction, we leave
    // the pre-MVCC sentinel. Documented in the helper contract; this test
    // pins that behavior so it doesn't drift.
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(50))");
    exec(&mut db, "INSERT INTO users VALUES (1, 'Alice')");

    let row = &raw_rows(&db, "USERS")[0];
    assert_eq!(
        row.xmin, PRE_MVCC_TXN_ID,
        "autocommit INSERT (no active txn) keeps the pre-MVCC sentinel even with feature on"
    );
}

#[cfg(feature = "mvcc_enabled")]
#[test]
fn rollback_discards_stamped_rows() {
    // The existing rollback path restores `original_tables` wholesale,
    // so any xmin/xmax stamping done by the aborted transaction is
    // discarded along with the rows themselves. This test pins that
    // behavior — the snapshot-isolation predicate relies on it (see
    // mvcc.rs::writer_aborted_modeling_note).
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(50))");

    db.begin_transaction().unwrap();
    exec(&mut db, "INSERT INTO users VALUES (1, 'Alice')");
    assert_eq!(raw_rows(&db, "USERS").len(), 1);
    db.rollback_transaction().unwrap();

    assert_eq!(
        raw_rows(&db, "USERS").len(),
        0,
        "rollback must discard all rows the aborted txn inserted, stamps and all"
    );
}

#[cfg(feature = "mvcc_enabled")]
#[test]
fn second_transaction_sees_committed_rows_with_first_xmin() {
    // After the first transaction commits, its txn id is "committed
    // forever" — Phase 1b's TxnSnapshot::is_committed treats it as
    // visible. Pin that rows stamped in txn 1 retain their xmin after
    // txn 2 starts and commits its own changes.
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(50))");

    db.begin_transaction().unwrap();
    let txn1 = db.transaction_id().unwrap();
    exec(&mut db, "INSERT INTO users VALUES (1, 'Alice')");
    db.commit_transaction().unwrap();

    db.begin_transaction().unwrap();
    let txn2 = db.transaction_id().unwrap();
    assert!(txn2 > txn1);
    exec(&mut db, "INSERT INTO users VALUES (2, 'Bob')");
    db.commit_transaction().unwrap();

    let rows = raw_rows(&db, "USERS");
    let alice = rows
        .iter()
        .find(|r| matches!(r.values.first(), Some(SqlValue::Integer(1))))
        .unwrap();
    let bob = rows
        .iter()
        .find(|r| matches!(r.values.first(), Some(SqlValue::Integer(2))))
        .unwrap();
    assert_eq!(alice.xmin, txn1, "Alice's xmin should still reflect the inserter");
    assert_eq!(bob.xmin, txn2, "Bob's xmin should reflect his own inserter");
}
