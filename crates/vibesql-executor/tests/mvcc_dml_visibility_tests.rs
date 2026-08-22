//! Integration tests for Phase 1d follow-up (#5205 of #5136):
//! extending `Row::visible_to` filtering to the read sites embedded in
//! INSERT / UPDATE / DELETE.
//!
//! The contract these tests pin down:
//!
//! - **FK parent-existence checks** (INSERT/UPDATE): when MVCC is ON, a parent row that is
//!   tombstoned from our snapshot's perspective must not be treated as a valid parent for a new
//!   child row.
//! - **FK child-reference scans** (DELETE/UPDATE): when MVCC is ON, a child row that has been
//!   tombstoned must not be treated as a referencing row that would block parent deletion / parent
//!   key update.
//! - **UPDATE/DELETE row selection**: the WHERE-clause scan + PK fast path must honor MVCC
//!   visibility. A row that has been deleted (and tombstoned) must not be re-picked by a subsequent
//!   UPDATE/DELETE.
//!
//! Off-state (`mvcc_enabled` OFF): the visibility predicate collapses to
//! the deletion-bitmap check, so every test must continue to pass with
//! identical semantics. On-state contracts that *require* the
//! visibility filter to fire are gated behind
//! `#[cfg(feature = "mvcc_enabled")]`.
//!
//! Run with:
//! ```text
//! cargo test -p vibesql-executor --test mvcc_dml_visibility_tests
//! cargo test -p vibesql-executor --test mvcc_dml_visibility_tests --features mvcc_enabled
//! ```

use vibesql_ast::{SelectStmt, Statement};
use vibesql_executor::{
    BeginTransactionExecutor, CommitExecutor, CreateTableExecutor, DeleteExecutor, InsertExecutor,
    SelectExecutor, UpdateExecutor,
};
use vibesql_parser::Parser;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

// ============================================================================
// Helpers
// ============================================================================

fn exec(db: &mut Database, sql: &str) {
    let stmt = Parser::parse_sql(sql).expect("parse failed");
    match stmt {
        Statement::CreateTable(s) => {
            CreateTableExecutor::execute(&s, db).expect("CREATE TABLE failed");
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
        Statement::BeginTransaction(s) => {
            BeginTransactionExecutor::execute(&s, db).expect("BEGIN failed");
        }
        Statement::Commit(s) => {
            CommitExecutor::execute(&s, db).expect("COMMIT failed");
        }
        other => panic!("unsupported statement in test helper: {:?}", other),
    }
}

fn enable_fks(db: &mut Database) {
    db.set_foreign_keys_enabled(true);
}

fn try_exec(db: &mut Database, sql: &str) -> Result<(), String> {
    let stmt = Parser::parse_sql(sql).map_err(|e| format!("{:?}", e))?;
    match stmt {
        Statement::Insert(s) => {
            InsertExecutor::execute(db, &s).map(|_| ()).map_err(|e| format!("{:?}", e))
        }
        Statement::Update(s) => {
            UpdateExecutor::execute(&s, db).map(|_| ()).map_err(|e| format!("{:?}", e))
        }
        Statement::Delete(s) => {
            DeleteExecutor::execute(&s, db).map(|_| ()).map_err(|e| format!("{:?}", e))
        }
        other => panic!("try_exec only supports INSERT/UPDATE/DELETE, got {:?}", other),
    }
}

fn select(db: &mut Database, sql: &str) -> Vec<Vec<SqlValue>> {
    let stmt = Parser::parse_sql(sql).expect("parse failed");
    let select_stmt: SelectStmt = match stmt {
        Statement::Select(s) => *s,
        other => panic!("expected SELECT, got {:?}", other),
    };
    let executor = SelectExecutor::new(db);
    let rows = executor.execute(&select_stmt).expect("SELECT failed");
    rows.into_iter().map(|r| r.values.to_vec()).collect()
}

// ============================================================================
// Off-state baseline: each of these tests must pass without the feature
// flag too, because off-state semantics are by contract bit-for-bit
// identical to pre-MVCC.
// ============================================================================

#[test]
fn insert_fk_parent_existing_row_passes() {
    // Smoke: a child INSERT with a matching pre-MVCC parent must succeed.
    let mut db = Database::new();
    enable_fks(&mut db);
    exec(&mut db, "CREATE TABLE parent (id INTEGER PRIMARY KEY, name TEXT)");
    exec(&mut db, "CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))");
    exec(&mut db, "INSERT INTO parent VALUES (1, 'alpha')");
    try_exec(&mut db, "INSERT INTO child VALUES (10, 1)").expect("child insert should succeed");
    let rows = select(&mut db, "SELECT id FROM child WHERE pid = 1");
    assert_eq!(rows.len(), 1);
}

#[test]
fn insert_fk_parent_missing_row_fails() {
    // Smoke: a child INSERT against a missing parent must fail.
    let mut db = Database::new();
    enable_fks(&mut db);
    exec(&mut db, "CREATE TABLE parent (id INTEGER PRIMARY KEY, name TEXT)");
    exec(&mut db, "CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))");
    let err = try_exec(&mut db, "INSERT INTO child VALUES (10, 99)")
        .expect_err("missing parent must reject child");
    assert!(err.contains("FOREIGN KEY"), "got error: {err}");
}

#[test]
fn delete_parent_with_no_children_succeeds() {
    // Smoke: parent DELETE with no referencing child rows must succeed.
    let mut db = Database::new();
    enable_fks(&mut db);
    exec(&mut db, "CREATE TABLE parent (id INTEGER PRIMARY KEY, name TEXT)");
    exec(&mut db, "CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))");
    exec(&mut db, "INSERT INTO parent VALUES (1, 'alpha')");
    try_exec(&mut db, "DELETE FROM parent WHERE id = 1").expect("delete should succeed");
    let rows = select(&mut db, "SELECT COUNT(*) FROM parent");
    assert_eq!(rows[0][0], SqlValue::Integer(0));
}

#[test]
fn delete_parent_with_orphan_child_blocked() {
    // Smoke: parent DELETE with a referencing child row must fail
    // (NO ACTION is the default and not deferred outside a txn).
    let mut db = Database::new();
    enable_fks(&mut db);
    exec(&mut db, "CREATE TABLE parent (id INTEGER PRIMARY KEY, name TEXT)");
    exec(&mut db, "CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))");
    exec(&mut db, "INSERT INTO parent VALUES (1, 'alpha')");
    exec(&mut db, "INSERT INTO child VALUES (10, 1)");
    let err = try_exec(&mut db, "DELETE FROM parent WHERE id = 1")
        .expect_err("referenced parent must not be deletable");
    assert!(err.contains("FOREIGN KEY"), "got error: {err}");
}

#[test]
fn delete_parent_succeeds_after_child_deleted() {
    // After deleting the child row, the parent must become deletable.
    // The DELETE child-reference scan must treat the tombstoned child
    // as "absent" for FK purposes — this is the off-state regression
    // guard.
    let mut db = Database::new();
    enable_fks(&mut db);
    exec(&mut db, "CREATE TABLE parent (id INTEGER PRIMARY KEY, name TEXT)");
    exec(&mut db, "CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))");
    exec(&mut db, "INSERT INTO parent VALUES (1, 'alpha')");
    exec(&mut db, "INSERT INTO child VALUES (10, 1)");
    exec(&mut db, "DELETE FROM child WHERE id = 10");
    try_exec(&mut db, "DELETE FROM parent WHERE id = 1")
        .expect("parent should be deletable after child gone");
}

#[test]
fn update_picks_only_live_rows() {
    // Smoke: UPDATE WHERE matching a row that was already deleted
    // returns zero rows updated (the row is tombstoned).
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE items (id INTEGER PRIMARY KEY, qty INTEGER)");
    exec(&mut db, "INSERT INTO items VALUES (1, 10)");
    exec(&mut db, "INSERT INTO items VALUES (2, 20)");
    exec(&mut db, "DELETE FROM items WHERE id = 1");
    try_exec(&mut db, "UPDATE items SET qty = 99 WHERE id = 1").expect("update zero rows is ok");
    // Row 2 still at its original value.
    let rows = select(&mut db, "SELECT qty FROM items WHERE id = 2");
    assert_eq!(rows[0][0], SqlValue::Integer(20));
    let count = select(&mut db, "SELECT COUNT(*) FROM items");
    assert_eq!(count[0][0], SqlValue::Integer(1));
}

#[test]
fn delete_picks_only_live_rows() {
    // Smoke: DELETE WHERE matching a row that was already deleted is a
    // no-op.
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE items (id INTEGER PRIMARY KEY, qty INTEGER)");
    exec(&mut db, "INSERT INTO items VALUES (1, 10)");
    exec(&mut db, "DELETE FROM items WHERE id = 1");
    try_exec(&mut db, "DELETE FROM items WHERE id = 1").expect("second delete is no-op");
    let count = select(&mut db, "SELECT COUNT(*) FROM items");
    assert_eq!(count[0][0], SqlValue::Integer(0));
}

#[test]
fn cascade_delete_in_autocommit_still_works() {
    // ON DELETE CASCADE must continue to wipe children in autocommit
    // mode under both feature states (off-state regression guard for
    // the cascade path's scan_live → scan_visible switch).
    let mut db = Database::new();
    enable_fks(&mut db);
    exec(&mut db, "CREATE TABLE parent (id INTEGER PRIMARY KEY)");
    exec(
        &mut db,
        "CREATE TABLE child (
            id INTEGER PRIMARY KEY,
            pid INTEGER REFERENCES parent(id) ON DELETE CASCADE
        )",
    );
    exec(&mut db, "INSERT INTO parent VALUES (1)");
    exec(&mut db, "INSERT INTO child VALUES (10, 1)");
    exec(&mut db, "INSERT INTO child VALUES (11, 1)");
    try_exec(&mut db, "DELETE FROM parent WHERE id = 1").expect("cascade delete should succeed");
    let count = select(&mut db, "SELECT COUNT(*) FROM child");
    assert_eq!(count[0][0], SqlValue::Integer(0));
}

#[test]
fn set_null_in_autocommit_still_works() {
    // ON DELETE SET NULL must wipe child FK columns (off-state guard
    // for the set_null scan_live → scan_visible switch).
    let mut db = Database::new();
    enable_fks(&mut db);
    exec(&mut db, "CREATE TABLE parent (id INTEGER PRIMARY KEY)");
    exec(
        &mut db,
        "CREATE TABLE child (
            id INTEGER PRIMARY KEY,
            pid INTEGER REFERENCES parent(id) ON DELETE SET NULL
        )",
    );
    exec(&mut db, "INSERT INTO parent VALUES (1)");
    exec(&mut db, "INSERT INTO child VALUES (10, 1)");
    try_exec(&mut db, "DELETE FROM parent WHERE id = 1").expect("set-null delete should succeed");
    let rows = select(&mut db, "SELECT pid FROM child WHERE id = 10");
    assert_eq!(rows[0][0], SqlValue::Null);
}

// ============================================================================
// On-state: visibility filter actually fires inside DML.
//
// Acceptance criterion from the issue body:
//   "With mvcc_enabled ON: integration test demonstrating an UPDATE
//    inside a transaction respects snapshot-isolation when computing
//    its WHERE clause."
// ============================================================================

#[cfg(feature = "mvcc_enabled")]
#[test]
fn update_in_txn_where_clause_sees_pre_mvcc_rows() {
    // Pre-MVCC rows (xmin = sentinel) must remain selectable by a
    // WHERE clause from inside a transaction — the visibility filter
    // must not drop them on the floor.
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE items (id INTEGER PRIMARY KEY, qty INTEGER)");
    for i in 1..=5 {
        exec(&mut db, &format!("INSERT INTO items VALUES ({i}, {i})"));
    }

    exec(&mut db, "BEGIN");
    try_exec(&mut db, "UPDATE items SET qty = 100 WHERE id BETWEEN 2 AND 4")
        .expect("update should succeed");
    // Reads inside the same txn see the updated rows (#5223 widening).
    let rows = select(&mut db, "SELECT qty FROM items WHERE id = 3");
    assert_eq!(rows[0][0], SqlValue::Integer(100));
    exec(&mut db, "COMMIT");

    let rows = select(&mut db, "SELECT qty FROM items WHERE id = 3");
    assert_eq!(rows[0][0], SqlValue::Integer(100));
}

#[cfg(feature = "mvcc_enabled")]
#[test]
fn update_in_txn_skips_already_deleted_row() {
    // Inside a transaction, an UPDATE WHERE that targets a row deleted
    // earlier in the same txn must affect zero rows — see-your-own-deletes
    // semantics under the widened BEGIN snapshot (#5223).
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE items (id INTEGER PRIMARY KEY, qty INTEGER)");
    exec(&mut db, "INSERT INTO items VALUES (1, 10)");
    exec(&mut db, "INSERT INTO items VALUES (2, 20)");

    exec(&mut db, "BEGIN");
    exec(&mut db, "DELETE FROM items WHERE id = 1");
    // The deleted row must not be selectable for UPDATE anymore.
    try_exec(&mut db, "UPDATE items SET qty = 99 WHERE id = 1").expect("zero-row update ok");
    let rows = select(&mut db, "SELECT COUNT(*) FROM items WHERE qty = 99");
    assert_eq!(rows[0][0], SqlValue::Integer(0), "no row should have qty=99");
    exec(&mut db, "COMMIT");

    let rows = select(&mut db, "SELECT COUNT(*) FROM items");
    assert_eq!(rows[0][0], SqlValue::Integer(1));
}

#[cfg(feature = "mvcc_enabled")]
#[test]
fn delete_in_txn_skips_already_deleted_row() {
    // Same contract as the UPDATE case but for the DELETE path's PK
    // fast path + WHERE scan.
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE items (id INTEGER PRIMARY KEY, qty INTEGER)");
    exec(&mut db, "INSERT INTO items VALUES (1, 10)");
    exec(&mut db, "INSERT INTO items VALUES (2, 20)");

    exec(&mut db, "BEGIN");
    exec(&mut db, "DELETE FROM items WHERE id = 1");
    // The deleted row's PK must not be re-selected by a second DELETE
    // (no error, just zero rows affected).
    try_exec(&mut db, "DELETE FROM items WHERE id = 1").expect("idempotent delete is ok");
    exec(&mut db, "COMMIT");

    let rows = select(&mut db, "SELECT COUNT(*) FROM items");
    assert_eq!(rows[0][0], SqlValue::Integer(1));
}

#[cfg(feature = "mvcc_enabled")]
#[test]
fn fk_parent_existence_check_in_txn_sees_self_insert() {
    // A child INSERT inside the same transaction must see a parent
    // INSERTed earlier in the same transaction — the FK parent-existence
    // scan flows through the BEGIN-time snapshot widened in #5223.
    let mut db = Database::new();
    enable_fks(&mut db);
    exec(&mut db, "CREATE TABLE parent (id INTEGER PRIMARY KEY)");
    exec(&mut db, "CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))");

    exec(&mut db, "BEGIN");
    exec(&mut db, "INSERT INTO parent VALUES (42)");
    try_exec(&mut db, "INSERT INTO child VALUES (1, 42)")
        .expect("child should see parent inserted in same txn");
    exec(&mut db, "COMMIT");

    let rows = select(&mut db, "SELECT pid FROM child WHERE id = 1");
    assert_eq!(rows[0][0], SqlValue::Integer(42));
}

#[cfg(feature = "mvcc_enabled")]
#[test]
fn fk_parent_existence_check_after_self_delete() {
    // Inside a transaction: INSERT a parent, DELETE it, then attempt to
    // INSERT a child referencing the (now-tombstoned) parent. The child
    // INSERT must fail because the parent is no longer visible to the
    // txn's own snapshot.
    let mut db = Database::new();
    enable_fks(&mut db);
    exec(&mut db, "CREATE TABLE parent (id INTEGER PRIMARY KEY)");
    exec(&mut db, "CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))");

    exec(&mut db, "BEGIN");
    exec(&mut db, "INSERT INTO parent VALUES (7)");
    exec(&mut db, "DELETE FROM parent WHERE id = 7");
    // The parent row is now tombstoned from this txn's snapshot.
    let err = try_exec(&mut db, "INSERT INTO child VALUES (1, 7)")
        .expect_err("child must not see tombstoned parent");
    assert!(err.contains("FOREIGN KEY"), "got error: {err}");
    // Cleanup: rollback to keep the test self-contained.
    exec(&mut db, "COMMIT");
}

#[cfg(feature = "mvcc_enabled")]
#[test]
fn delete_parent_in_txn_after_child_deleted_in_same_txn() {
    // Inside a transaction: DELETE the only child row, then DELETE the
    // parent. The parent's NO ACTION child-reference scan must treat
    // the tombstoned child as absent under the widened BEGIN snapshot,
    // so the parent DELETE succeeds.
    let mut db = Database::new();
    enable_fks(&mut db);
    exec(&mut db, "CREATE TABLE parent (id INTEGER PRIMARY KEY)");
    exec(&mut db, "CREATE TABLE child (id INTEGER PRIMARY KEY, pid INTEGER REFERENCES parent(id))");
    exec(&mut db, "INSERT INTO parent VALUES (1)");
    exec(&mut db, "INSERT INTO child VALUES (10, 1)");

    exec(&mut db, "BEGIN");
    exec(&mut db, "DELETE FROM child WHERE id = 10");
    try_exec(&mut db, "DELETE FROM parent WHERE id = 1")
        .expect("parent must be deletable after child gone in same txn");
    exec(&mut db, "COMMIT");

    let pcount = select(&mut db, "SELECT COUNT(*) FROM parent");
    assert_eq!(pcount[0][0], SqlValue::Integer(0));
}
