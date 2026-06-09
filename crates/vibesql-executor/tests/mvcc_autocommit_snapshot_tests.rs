//! Integration tests for Phase 1d follow-up #5207 — autocommit snapshot
//! widening (see-your-own-writes).
//!
//! Phase 1d's initial PR (#5209) made the in-transaction BEGIN-time
//! snapshot stable across reads (correct for repeatable-read). It also
//! left two gaps that this follow-up closes:
//!
//! 1. **Autocommit sees prior transactional commits**: with the
//!    pre-1207 `read_snapshot` returning `TxnSnapshot::empty()` for
//!    autocommit reads (`xmax_committed = 0`), MVCC-stamped rows from
//!    *committed* transactions were invisible to autocommit reads. The
//!    fix synthesizes a commit-time-style snapshot at every autocommit
//!    read so reads see everything that has committed so far.
//! 2. **A transaction sees its own writes**: with the pre-1207 BEGIN
//!    snapshot setting `xmax_committed = txn_id - 1`, a row stamped
//!    with `xmin = txn_id` failed the `xmin <= xmax_committed` clause.
//!    The fix widens the BEGIN-time snapshot to set
//!    `xmax_committed = txn_id` so self-writes pass `is_committed(self)`.
//!
//! # Why these tests use tables without a PRIMARY KEY
//!
//! Phase 1d's `try_primary_key_lookup` fast path (in `select/scan/table.rs`)
//! reads via the PK index and goes through `table.get_row()`, which does
//! NOT consult MVCC visibility — that's the gap tracked in follow-up
//! issue #5204 (Index/PK lookup visibility). To assert the *autocommit-
//! snapshot* invariant the tests must take the full-scan code path. So
//! these tests deliberately avoid `INTEGER PRIMARY KEY` and use a
//! non-keyed `id INTEGER` column instead.
//!
//! Run with:
//! ```text
//! cargo test -p vibesql-executor --test mvcc_autocommit_snapshot_tests
//! cargo test -p vibesql-executor --test mvcc_autocommit_snapshot_tests \
//!     --features mvcc_enabled
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
// Helpers (kept local to this file — the read-path tests have their own
// equivalents; duplicating these here keeps the test file self-contained).
// ============================================================================

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
        Statement::BeginTransaction(s) => {
            BeginTransactionExecutor::execute(&s, db).expect("BEGIN failed");
        }
        Statement::Commit(s) => {
            CommitExecutor::execute(&s, db).expect("COMMIT failed");
        }
        other => panic!("unsupported statement in test helper: {:?}", other),
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

/// Build a fresh database with a single non-keyed
/// `accounts(id INTEGER, balance INTEGER)` table containing the row
/// `(1, 100)`. The absence of a PRIMARY KEY is deliberate — see the
/// module doc for why.
fn db_with_accounts() -> Database {
    let mut db = Database::new();
    exec(&mut db, "CREATE TABLE accounts (id INTEGER, balance INTEGER)");
    exec(&mut db, "INSERT INTO accounts VALUES (1, 100)");
    db
}

fn integer(v: &SqlValue) -> Option<i64> {
    match v {
        SqlValue::Integer(x) => Some(*x),
        _ => None,
    }
}

// ============================================================================
// Acceptance: autocommit SELECT sees rows committed by previous transactions.
//
// This is the autocommit-snapshot-widening invariant. Under the on-state
// the row is stamped with `xmin = txn_id`, and the autocommit reader's
// snapshot must treat that txn id as committed.
// ============================================================================

#[test]
fn autocommit_select_sees_row_committed_by_prior_transaction() {
    let mut db = db_with_accounts();

    exec(&mut db, "BEGIN");
    exec(&mut db, "INSERT INTO accounts VALUES (2, 200)");
    exec(&mut db, "COMMIT");

    // The autocommit reader must observe the row committed by the
    // prior transaction. Under MVCC ON the row carries `xmin = txn_id`,
    // and the read snapshot must include that txn id as committed.
    let rows = select(&mut db, "SELECT id, balance FROM accounts");
    assert_eq!(rows.len(), 2, "autocommit SELECT must see both rows after committed txn");
    // Sort for deterministic comparison (no PK ordering on this table).
    let mut sorted: Vec<_> = rows.iter().map(|r| (integer(&r[0]), integer(&r[1]))).collect();
    sorted.sort();
    assert_eq!(sorted, vec![(Some(1), Some(100)), (Some(2), Some(200))]);
}

#[test]
fn autocommit_select_sees_update_committed_by_prior_transaction() {
    let mut db = db_with_accounts();

    exec(&mut db, "BEGIN");
    exec(&mut db, "UPDATE accounts SET balance = 250 WHERE id = 1");
    exec(&mut db, "COMMIT");

    // The autocommit reader must observe the updated value. The new
    // row version is stamped with the txn id; the snapshot must see it.
    let rows = select(&mut db, "SELECT id, balance FROM accounts");
    assert_eq!(rows.len(), 1);
    assert_eq!(integer(&rows[0][0]), Some(1));
    assert_eq!(integer(&rows[0][1]), Some(250));
}

#[test]
fn autocommit_select_sees_delete_committed_by_prior_transaction() {
    let mut db = db_with_accounts();
    exec(&mut db, "INSERT INTO accounts VALUES (2, 200)");

    exec(&mut db, "BEGIN");
    exec(&mut db, "DELETE FROM accounts WHERE id = 1");
    exec(&mut db, "COMMIT");

    // The autocommit reader must observe the deletion.
    let rows = select(&mut db, "SELECT id, balance FROM accounts");
    assert_eq!(rows.len(), 1, "deleted row must not be visible to subsequent autocommit reads");
    assert_eq!(integer(&rows[0][0]), Some(2));
    assert_eq!(integer(&rows[0][1]), Some(200));
}

// ============================================================================
// Acceptance: a transaction sees its own INSERT/UPDATE inside the same txn.
//
// This is the in-transaction see-your-own-writes invariant. Under the
// pre-1207 BEGIN-time snapshot (`xmax_committed = txn_id - 1`), a row
// stamped with `xmin = txn_id` failed the visibility check. The fix is
// to widen the BEGIN-time snapshot to `xmax_committed = txn_id`.
// ============================================================================

#[test]
fn in_transaction_sees_own_insert() {
    let mut db = db_with_accounts();

    exec(&mut db, "BEGIN");
    exec(&mut db, "INSERT INTO accounts VALUES (2, 200)");

    let rows = select(&mut db, "SELECT id, balance FROM accounts");
    assert_eq!(rows.len(), 2, "txn must see its own INSERT");
    let mut sorted: Vec<_> = rows.iter().map(|r| (integer(&r[0]), integer(&r[1]))).collect();
    sorted.sort();
    assert_eq!(sorted, vec![(Some(1), Some(100)), (Some(2), Some(200))]);

    exec(&mut db, "COMMIT");
}

#[test]
fn in_transaction_sees_own_update() {
    let mut db = db_with_accounts();

    exec(&mut db, "BEGIN");
    exec(&mut db, "UPDATE accounts SET balance = 150 WHERE id = 1");

    let rows = select(&mut db, "SELECT id, balance FROM accounts");
    assert_eq!(rows.len(), 1, "txn must see exactly one row (the updated one)");
    assert_eq!(integer(&rows[0][1]), Some(150), "txn must observe the new value");

    exec(&mut db, "COMMIT");
}

#[test]
fn in_transaction_sees_own_insert_followed_by_update() {
    let mut db = db_with_accounts();

    exec(&mut db, "BEGIN");
    exec(&mut db, "INSERT INTO accounts VALUES (2, 200)");
    exec(&mut db, "UPDATE accounts SET balance = 250 WHERE id = 2");

    let rows = select(&mut db, "SELECT id, balance FROM accounts WHERE id = 2");
    assert_eq!(rows.len(), 1);
    assert_eq!(integer(&rows[0][1]), Some(250));

    exec(&mut db, "COMMIT");

    // After commit, autocommit reader still sees the row.
    let rows = select(&mut db, "SELECT id, balance FROM accounts WHERE id = 2");
    assert_eq!(rows.len(), 1);
    assert_eq!(integer(&rows[0][1]), Some(250));
}

#[test]
fn in_transaction_sees_own_delete() {
    let mut db = db_with_accounts();
    exec(&mut db, "INSERT INTO accounts VALUES (2, 200)");

    exec(&mut db, "BEGIN");
    exec(&mut db, "DELETE FROM accounts WHERE id = 1");

    let rows = select(&mut db, "SELECT id, balance FROM accounts");
    assert_eq!(rows.len(), 1, "txn must observe its own DELETE");
    assert_eq!(integer(&rows[0][0]), Some(2));

    exec(&mut db, "COMMIT");

    // After commit, autocommit reader also observes the delete.
    let rows = select(&mut db, "SELECT id, balance FROM accounts");
    assert_eq!(rows.len(), 1);
    assert_eq!(integer(&rows[0][0]), Some(2));
}

// ============================================================================
// Acceptance: multi-statement autocommit "see-your-own-writes" — each
// autocommit statement is its own transaction, and statement N+1 must
// see writes from statement N.
// ============================================================================

#[test]
fn multi_statement_autocommit_sees_prior_writes() {
    let mut db = db_with_accounts();

    // Each of these runs as its own autocommit "statement-transaction".
    exec(&mut db, "INSERT INTO accounts VALUES (2, 200)");
    exec(&mut db, "INSERT INTO accounts VALUES (3, 300)");

    let rows = select(&mut db, "SELECT id, balance FROM accounts");
    assert_eq!(rows.len(), 3, "all three autocommit inserts must be visible");
    let mut sorted: Vec<_> = rows.iter().map(|r| integer(&r[0])).collect();
    sorted.sort();
    assert_eq!(sorted, vec![Some(1), Some(2), Some(3)]);
}

#[test]
fn multi_statement_autocommit_sees_updates_to_pre_mvcc_row() {
    let mut db = db_with_accounts();

    // First autocommit statement updates the pre-MVCC row. Under MVCC
    // ON the autocommit UPDATE path may or may not stamp; the
    // visibility predicate must still let the later SELECT see the
    // current value.
    exec(&mut db, "UPDATE accounts SET balance = 999 WHERE id = 1");

    let rows = select(&mut db, "SELECT balance FROM accounts");
    assert_eq!(rows.len(), 1);
    assert_eq!(integer(&rows[0][0]), Some(999), "autocommit SELECT must observe autocommit UPDATE");
}

// ============================================================================
// Foundation invariant (regression guard) — pre-MVCC rows remain
// visible after the widening fix. Equivalent to the test in
// mvcc_read_path_tests.rs but reasserted here so a future change can't
// quietly regress this file in isolation.
// ============================================================================

#[test]
fn autocommit_select_sees_pre_mvcc_rows_after_widening() {
    let mut db = db_with_accounts();
    let rows = select(&mut db, "SELECT balance FROM accounts");
    assert_eq!(rows.len(), 1);
    assert_eq!(integer(&rows[0][0]), Some(100));
}
