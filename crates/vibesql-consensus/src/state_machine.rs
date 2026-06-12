//! The MVCC-backed replicated state machine (Raft Phase B1, #5199, PR 1).
//!
//! This module is where the replication layer stops echoing bytes and
//! starts applying real database transactions: [`TxnEntry`] is the log
//! entry type replicated through a [`ConsensusBackend`], and
//! [`VibesqlStateMachine`] applies committed entries to a real
//! [`Database`] with MVCC commit timestamps derived from the log index.
//!
//! # Entry representation: deterministic statement batch
//!
//! A [`TxnEntry`] carries **one committed transaction** as an ordered
//! batch of SQL write statements. One entry per transaction makes atomic
//! apply trivial (the whole batch applies in a single storage
//! transaction, all-or-nothing) and rules out half-applied transactions
//! by construction — there is no `TxnBegin@10 / TxnCommit@12`
//! interleaving in the Raft log.
//!
//! The curator re-scope on #5199 preferred a *write-set/effects* form (a
//! batch of row-level mutations) over re-executing SQL text. That form
//! is not implementable against today's storage layer without new
//! machinery, so PR 1 ships the statement-batch form:
//!
//! - The crash-recovery WAL replay (`vibesql-storage::wal::recovery`)
//!   that the issue proposed reusing as the apply function is a **stub**
//!   for DML — `apply_op` counts Insert/Update/Delete entries but does
//!   not apply them (it has no `table_id → table` resolution).
//! - The only write-set the storage layer captures per transaction is
//!   `TransactionChange` (kept for savepoint undo). It records DML only
//!   — no DDL — and replaying it would bypass the executor's index and
//!   constraint maintenance, which has no row-level "apply with index
//!   upkeep" entry point today.
//!
//! **Tradeoff and follow-up**: statement batches are deterministic only
//! if the statements are (no `random()`, `CURRENT_TIMESTAMP`, …). On a
//! single node (this PR) every entry is applied exactly once by one
//! machine, so nothing can diverge; for multi-node (PR 2) and beyond,
//! either the proposer must freeze non-deterministic values into
//! literals before proposing, or the storage layer must grow a real
//! write-set capture + apply pair (effects form). That follow-up is
//! filed as #5377.
//!
//! # commit_ts = log index
//!
//! ADR-0004 (single Raft group, no HLC): the MVCC commit timestamp is
//! the Raft apply order. Concretely, the transaction that applies log
//! entry `N` runs with `TxnId = N` — [`VibesqlStateMachine::apply`]
//! calls [`Database::set_next_txn_id`]`(N)` immediately before `BEGIN`,
//! so every `xmin`/`xmax` stamped by entry `N` equals `N`. The mapping
//! survives snapshot install + log replay (the allocator is re-seeded
//! from the entry index on every apply) and failed entries (a rolled-
//! back entry's id is not left behind in any row).
//!
//! Indices here are the **dense application indices** of the
//! [`ConsensusBackend`] contract (1-based, application entries only) —
//! the same values `propose` resolves with.
//!
//! # Idempotent apply
//!
//! `apply(index, entry)` with `index <= last_applied` is a no-op
//! returning [`ApplyOutcome::AlreadyApplied`]. This is what makes
//! snapshot-install + log-replay overlap safe: a recovering node may
//! replay log entries at or below its installed snapshot's index, and
//! they must not double-apply. Gaps (`index > last_applied + 1`) are a
//! protocol violation and fail loudly.
//!
//! # Snapshot codec
//!
//! Snapshots serialize the **database state** using the binary
//! persistence format from `vibesql-storage::persistence::binary`
//! (header + catalog + data sections, vbsql v7+, which carries per-row
//! `xmin`/`xmax`) — this swaps the echo machine's JSON payload seam
//! exactly where Phase A4 left it (`crate::snapshot`). Builds hold the
//! MVCC vacuum horizon through the real [`SnapshotHorizonPin`]
//! implementation ([`VibesqlStateMachine::horizon_pin`]), wired to
//! [`Database::pin_gc_horizon`]'s holdback in the storage layer.
//!
//! [`ConsensusBackend`]: crate::ConsensusBackend
//! [`Database`]: vibesql_storage::Database
//! [`Database::set_next_txn_id`]: vibesql_storage::Database::set_next_txn_id
//! [`Database::pin_gc_horizon`]: vibesql_storage::Database::pin_gc_horizon

use std::sync::{Arc, Mutex, MutexGuard};

use serde::{Deserialize, Serialize};
use vibesql_ast::Statement;
use vibesql_storage::persistence::binary::{
    read_catalog_v, read_data, read_header, write_catalog, write_data, write_header,
};
use vibesql_storage::Database;
use vibesql_types::SqlValue;

use crate::backend::{ConsensusError, LogIndex, Result, Snapshot};
use crate::snapshot::SnapshotHorizonPin;

// ---------------------------------------------------------------------------
// The replicated entry type
// ---------------------------------------------------------------------------

/// One committed transaction, replicated as a single Raft log entry.
///
/// See the module docs for the representation decision (deterministic
/// statement batch) and its tradeoffs. The serde encoding (JSON at the
/// adapter boundary, like every other entry type) is the payload behind
/// the backend's opaque `Vec<u8>`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TxnEntry {
    /// The transaction's write statements, in execution order. Applied
    /// atomically: all statements commit together or none do.
    pub statements: Vec<String>,
}

impl TxnEntry {
    /// Entry for a single-statement (autocommit-style) transaction.
    pub fn single(sql: impl Into<String>) -> Self {
        Self { statements: vec![sql.into()] }
    }

    /// Entry for a multi-statement transaction.
    pub fn batch<S: Into<String>>(statements: impl IntoIterator<Item = S>) -> Self {
        Self { statements: statements.into_iter().map(Into::into).collect() }
    }
}

/// Result of applying one [`TxnEntry`] to the state machine.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ApplyOutcome {
    /// The entry's transaction committed; `rows_affected` sums the DML
    /// row counts across its statements.
    Applied {
        /// Total rows affected by the entry's statements.
        rows_affected: usize,
    },
    /// The entry's transaction was rolled back (a statement failed).
    /// The entry still consumed its log index — every replica rejects
    /// it identically, so the rejection is part of the state machine
    /// history, not an apply error.
    Rejected {
        /// The failing statement's error message.
        reason: String,
    },
    /// `index <= last_applied`: this entry's effects are already in the
    /// state (snapshot-install + log-replay overlap). Nothing was done.
    AlreadyApplied,
}

// ---------------------------------------------------------------------------
// The state machine
// ---------------------------------------------------------------------------

#[derive(Debug)]
struct MachineInner {
    db: Database,
    /// Dense application index of the last applied entry (`0` = none).
    last_applied: LogIndex,
}

/// The replicated state machine: a real [`Database`] plus the apply-side
/// bookkeeping (`last_applied`, idempotence, commit_ts derivation).
///
/// Cloning shares the underlying state (it is a handle), matching the
/// convention of the other state machines in this crate. PR 1 drives it
/// through [`ReplicatedDb`](crate::ReplicatedDb) over
/// [`SingleNodeBackend`](crate::SingleNodeBackend); PR 2 mounts it
/// behind openraft's `RaftStateMachine` for multi-node replication.
#[derive(Debug, Clone)]
pub struct VibesqlStateMachine {
    inner: Arc<Mutex<MachineInner>>,
}

impl Default for VibesqlStateMachine {
    fn default() -> Self {
        Self::new()
    }
}

impl VibesqlStateMachine {
    /// A fresh state machine over an empty in-memory database.
    pub fn new() -> Self {
        Self { inner: Arc::new(Mutex::new(MachineInner { db: Database::new(), last_applied: 0 })) }
    }

    /// Rebuild a state machine from a snapshot produced by
    /// [`snapshot`](Self::snapshot): decodes the database state and
    /// resumes `last_applied` at the snapshot's
    /// [`last_included_index`](Snapshot::last_included_index).
    pub fn from_snapshot(snapshot: &Snapshot) -> Result<Self> {
        let machine = Self::new();
        machine.install_snapshot(snapshot)?;
        Ok(machine)
    }

    fn lock(&self) -> MutexGuard<'_, MachineInner> {
        self.inner.lock().expect("replicated state machine mutex poisoned")
    }

    /// Dense application index of the last applied entry (`0` if none).
    pub fn last_applied(&self) -> LogIndex {
        self.lock().last_applied
    }

    /// Apply one committed log entry.
    ///
    /// - `index <= last_applied`: idempotent no-op
    ///   ([`ApplyOutcome::AlreadyApplied`]).
    /// - `index == last_applied + 1`: the entry's statements run in a
    ///   single storage transaction whose `TxnId` — and therefore every
    ///   MVCC stamp it writes — is `index` (commit_ts = log index). A
    ///   statement failure rolls the whole transaction back
    ///   ([`ApplyOutcome::Rejected`]); the entry still consumes its
    ///   index, because every replica rejects it identically.
    /// - `index > last_applied + 1`: a gap — protocol violation, loud
    ///   error.
    pub fn apply(&self, index: LogIndex, entry: &TxnEntry) -> Result<ApplyOutcome> {
        let mut inner = self.lock();
        if index == 0 {
            return Err(ConsensusError::Backend(
                "log index 0 is the 'nothing committed' sentinel; entries start at 1".to_string(),
            ));
        }
        if index <= inner.last_applied {
            return Ok(ApplyOutcome::AlreadyApplied);
        }
        if index != inner.last_applied + 1 {
            return Err(ConsensusError::Backend(format!(
                "apply gap: entry index {index} but last applied is {} (entries must apply in \
                 dense log order)",
                inner.last_applied
            )));
        }

        // commit_ts = log index: the transaction applying entry N is
        // stamped TxnId = N (ADR-0004's "apply index = commit order").
        inner
            .db
            .set_next_txn_id(index)
            .map_err(|e| ConsensusError::Backend(format!("failed to seed commit_ts: {e}")))?;
        inner
            .db
            .begin_transaction()
            .map_err(|e| ConsensusError::Backend(format!("failed to begin apply txn: {e}")))?;
        debug_assert_eq!(
            inner.db.transaction_id(),
            Some(index),
            "apply transaction id must equal the log index"
        );

        let mut rows_affected = 0usize;
        let mut failure: Option<String> = None;
        for sql in &entry.statements {
            match execute_write_statement(&mut inner.db, sql) {
                Ok(n) => rows_affected += n,
                Err(reason) => {
                    failure = Some(reason);
                    break;
                }
            }
        }

        let outcome = match failure {
            None => {
                // COMMIT through the executor so deferred-FK re-validation
                // (which can itself abort the transaction) behaves exactly
                // as it would for a local session.
                match vibesql_executor::CommitExecutor::execute(
                    &vibesql_ast::CommitStmt,
                    &mut inner.db,
                ) {
                    Ok(_) => ApplyOutcome::Applied { rows_affected },
                    Err(e) => {
                        // CommitExecutor rolls back on deferred-FK failure;
                        // make sure no transaction is left open either way.
                        if inner.db.in_transaction() {
                            let _ = inner.db.rollback_transaction();
                        }
                        ApplyOutcome::Rejected { reason: e.to_string() }
                    }
                }
            }
            Some(reason) => {
                inner.db.rollback_transaction().map_err(|e| {
                    ConsensusError::Backend(format!("failed to roll back rejected entry: {e}"))
                })?;
                ApplyOutcome::Rejected { reason }
            }
        };

        // The entry consumed its index regardless of outcome: a rejected
        // entry is rejected identically on every replica.
        inner.last_applied = index;
        Ok(outcome)
    }

    /// Run a read-only SELECT against the applied state, returning row
    /// values. Reads are local (not replicated); this is the query
    /// surface the PR 1 tests assert convergence with.
    pub fn query(&self, sql: &str) -> Result<Vec<Vec<SqlValue>>> {
        let inner = self.lock();
        let statement = vibesql_parser::parse_with_arena_fallback(sql)
            .map_err(|e| ConsensusError::Backend(format!("parse error: {e}")))?;
        let Statement::Select(select) = statement else {
            return Err(ConsensusError::Backend(format!(
                "query() only accepts SELECT statements, got: {sql}"
            )));
        };
        let rows = vibesql_executor::SelectExecutor::new(&inner.db)
            .execute(&select)
            .map_err(|e| ConsensusError::Backend(format!("query failed: {e}")))?;
        Ok(rows.into_iter().map(|r| r.values.to_vec()).collect())
    }

    /// Capture a snapshot of the applied state: the database serialized
    /// with the binary persistence format, covering everything up to
    /// [`last_applied`](Self::last_applied).
    ///
    /// The MVCC vacuum horizon is pinned (via
    /// [`horizon_pin`](Self::horizon_pin) →
    /// [`Database::pin_gc_horizon`]) before the state is read and
    /// released only after the blob is built, so `vacuum_mvcc` cannot
    /// reclaim row versions out from under the build.
    ///
    /// [`Database::pin_gc_horizon`]: vibesql_storage::Database::pin_gc_horizon
    pub fn snapshot(&self) -> Result<Snapshot> {
        // Acquire the pin through the same seam openraft's snapshot
        // builder uses (Phase A4's SnapshotHorizonPin), now backed by the
        // real storage-layer holdback.
        let _horizon_pin = self.horizon_pin().acquire();

        let inner = self.lock();
        let data = serialize_database(&inner.db)?;
        Ok(Snapshot { last_included_index: inner.last_applied, data })
        // `_horizon_pin` drops here, releasing the vacuum horizon.
    }

    /// Replace this machine's state from a snapshot produced by
    /// [`snapshot`](Self::snapshot).
    ///
    /// The payload is fully decoded **before** any mutation (a corrupt
    /// snapshot must not half-install). After install, the transaction
    /// id allocator resumes at `last_included_index + 1`, so MVCC
    /// visibility for the restored rows (stamped with commit timestamps
    /// `<= last_included_index`) is correct and the next applied entry
    /// keeps the commit_ts = log index mapping.
    pub fn install_snapshot(&self, snapshot: &Snapshot) -> Result<()> {
        let mut db = deserialize_database(&snapshot.data)?;
        db.set_next_txn_id(snapshot.last_included_index + 1)
            .map_err(|e| ConsensusError::SnapshotCodec(format!("failed to seed commit_ts: {e}")))?;
        let mut inner = self.lock();
        inner.db = db;
        inner.last_applied = snapshot.last_included_index;
        Ok(())
    }

    /// MVCC vacuum on the applied state — see
    /// [`Database::vacuum_mvcc`](vibesql_storage::Database::vacuum_mvcc).
    /// Returns the number of row versions reclaimed.
    pub fn vacuum(&self) -> Result<usize> {
        let mut inner = self.lock();
        inner.db.vacuum_mvcc().map_err(|e| ConsensusError::Backend(format!("vacuum failed: {e}")))
    }

    /// The [`SnapshotHorizonPin`] over this machine's database: acquiring
    /// it holds the MVCC vacuum horizon (the active-transaction-holdback
    /// generalization in `vibesql-storage`) until the guard drops. This
    /// is the real implementation of the seam Phase A4 introduced with a
    /// no-op; PR 2 hands it to the openraft snapshot builder.
    pub(crate) fn horizon_pin(&self) -> MvccHorizonPin {
        MvccHorizonPin { inner: Arc::clone(&self.inner) }
    }

    /// Direct read access to the underlying database, for tests that
    /// assert storage-level invariants (MVCC stamps, table contents).
    /// Only the `mvcc_enabled`-gated tests need it.
    #[cfg(all(test, feature = "mvcc_enabled"))]
    pub(crate) fn with_db<R>(&self, f: impl FnOnce(&Database) -> R) -> R {
        f(&self.lock().db)
    }

    /// Direct mutable access to the underlying database — test
    /// instrumentation only (e.g. hand-stamping tombstones the way the
    /// storage crate's vacuum tests do, since the current write path
    /// bitmap-deletes rather than deferring tombstones).
    #[cfg(all(test, feature = "mvcc_enabled"))]
    pub(crate) fn with_db_mut<R>(&self, f: impl FnOnce(&mut Database) -> R) -> R {
        f(&mut self.lock().db)
    }
}

// ---------------------------------------------------------------------------
// The MVCC vacuum-horizon pin (the real SnapshotHorizonPin)
// ---------------------------------------------------------------------------

/// [`SnapshotHorizonPin`] backed by the storage layer's GC-horizon
/// holdback ([`Database::pin_gc_horizon`]): the Phase B1 replacement for
/// the echo machine's `NoopHorizonPin`. Acquiring registers the snapshot
/// build alongside the active-transaction holdback in
/// `vibesql-storage::database::transaction_api`; dropping the guard
/// releases it.
///
/// [`Database::pin_gc_horizon`]: vibesql_storage::Database::pin_gc_horizon
#[derive(Debug)]
pub(crate) struct MvccHorizonPin {
    inner: Arc<Mutex<MachineInner>>,
}

struct MvccHorizonGuard {
    inner: Arc<Mutex<MachineInner>>,
    pin_id: u64,
}

impl Drop for MvccHorizonGuard {
    fn drop(&mut self) {
        if let Ok(mut inner) = self.inner.lock() {
            inner.db.release_gc_horizon(self.pin_id);
        }
    }
}

impl SnapshotHorizonPin for MvccHorizonPin {
    fn acquire(&self) -> Box<dyn Send> {
        let pin_id = {
            let mut inner = self.inner.lock().expect("replicated state machine mutex poisoned");
            inner.db.pin_gc_horizon()
        };
        Box::new(MvccHorizonGuard { inner: Arc::clone(&self.inner), pin_id })
    }
}

// ---------------------------------------------------------------------------
// Snapshot codec: the database state in the binary persistence format
// ---------------------------------------------------------------------------

/// Serialize the database with the binary persistence format
/// (`vibesql-storage::persistence::binary`: header + catalog + data;
/// vbsql v7+ carries per-row `xmin`/`xmax`, so MVCC stamps survive the
/// roundtrip).
fn serialize_database(db: &Database) -> Result<Vec<u8>> {
    let codec = |e: vibesql_storage::StorageError| ConsensusError::SnapshotCodec(e.to_string());
    let mut buf = Vec::new();
    write_header(&mut buf).map_err(codec)?;
    write_catalog(&mut buf, db).map_err(codec)?;
    write_data(&mut buf, db).map_err(codec)?;
    Ok(buf)
}

/// Decode a snapshot payload back into a database. Fails loudly (and
/// without side effects) on a corrupt payload.
fn deserialize_database(data: &[u8]) -> Result<Database> {
    let codec = |e: vibesql_storage::StorageError| ConsensusError::SnapshotCodec(e.to_string());
    let mut reader = data;
    let version = read_header(&mut reader).map_err(codec)?;
    let mut db = read_catalog_v(&mut reader, version).map_err(codec)?;
    read_data(&mut reader, &mut db, version).map_err(codec)?;
    Ok(db)
}

// ---------------------------------------------------------------------------
// Statement dispatch
// ---------------------------------------------------------------------------

/// Parse and execute one write statement inside the apply transaction,
/// returning its affected-row count.
///
/// The supported set covers the replicated write surface: DML
/// (INSERT/UPDATE/DELETE) and the core DDL (CREATE/DROP TABLE,
/// CREATE/DROP INDEX, CREATE/DROP VIEW). Reads are not replicated, and
/// transaction control lives at the entry boundary (the whole entry IS
/// the transaction), so SELECT/BEGIN/COMMIT/ROLLBACK inside an entry are
/// rejected — deterministically, on every replica.
fn execute_write_statement(
    db: &mut Database,
    sql: &str,
) -> std::result::Result<usize, String> {
    let statement = vibesql_parser::parse_with_arena_fallback(sql)
        .map_err(|e| format!("parse error in replicated statement: {e}"))?;
    match &statement {
        Statement::Insert(stmt) => vibesql_executor::InsertExecutor::execute(db, stmt)
            .map_err(|e| e.to_string()),
        Statement::Update(stmt) => vibesql_executor::UpdateExecutor::execute(stmt, db)
            .map_err(|e| e.to_string()),
        Statement::Delete(stmt) => vibesql_executor::DeleteExecutor::execute(stmt, db)
            .map_err(|e| e.to_string()),
        Statement::CreateTable(stmt) => vibesql_executor::CreateTableExecutor::execute(stmt, db)
            .map(|_| 0)
            .map_err(|e| e.to_string()),
        Statement::CreateIndex(stmt) => vibesql_executor::CreateIndexExecutor::execute(stmt, db)
            .map(|_| 0)
            .map_err(|e| e.to_string()),
        Statement::CreateView(stmt) => {
            vibesql_executor::advanced_objects::execute_create_view(stmt, db)
                .map(|_| 0)
                .map_err(|e| e.to_string())
        }
        Statement::DropTable(stmt) => vibesql_executor::DropTableExecutor::execute(stmt, db)
            .map(|_| 0)
            .map_err(|e| e.to_string()),
        Statement::DropIndex(stmt) => vibesql_executor::DropIndexExecutor::execute(stmt, db)
            .map(|_| 0)
            .map_err(|e| e.to_string()),
        Statement::DropView(stmt) => {
            vibesql_executor::advanced_objects::execute_drop_view(stmt, db)
                .map(|_| 0)
                .map_err(|e| e.to_string())
        }
        Statement::BeginTransaction(_) | Statement::Commit(_) | Statement::Rollback(_) => {
            Err("transaction control is not allowed inside a replicated entry: the entry itself \
                 is the transaction (one entry per committed transaction)"
                .to_string())
        }
        Statement::Select(_) => Err(
            "SELECT is not allowed inside a replicated entry: reads are local, not replicated"
                .to_string(),
        ),
        other => Err(format!(
            "statement is not supported in a replicated entry (Raft Phase B1, PR 1): {other:?}"
        )),
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn create_users(machine: &VibesqlStateMachine, index: LogIndex) {
        let outcome = machine
            .apply(
                index,
                &TxnEntry::single("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"),
            )
            .unwrap();
        assert_eq!(outcome, ApplyOutcome::Applied { rows_affected: 0 });
    }

    fn names(machine: &VibesqlStateMachine) -> Vec<String> {
        machine
            .query("SELECT name FROM users ORDER BY id")
            .unwrap()
            .into_iter()
            .map(|row| row[0].to_string())
            .collect()
    }

    #[test]
    fn entry_serde_roundtrips() {
        let entry = TxnEntry::batch(["INSERT INTO t VALUES (1)", "UPDATE t SET x = 2"]);
        let bytes = serde_json::to_vec(&entry).unwrap();
        assert_eq!(serde_json::from_slice::<TxnEntry>(&bytes).unwrap(), entry);
    }

    #[test]
    fn apply_insert_update_delete_roundtrip() {
        let machine = VibesqlStateMachine::new();
        create_users(&machine, 1);

        let outcome = machine
            .apply(
                2,
                &TxnEntry::batch([
                    "INSERT INTO users VALUES (1, 'alice')",
                    "INSERT INTO users VALUES (2, 'bob')",
                    "INSERT INTO users VALUES (3, 'carol')",
                ]),
            )
            .unwrap();
        assert_eq!(outcome, ApplyOutcome::Applied { rows_affected: 3 });

        let outcome = machine
            .apply(3, &TxnEntry::single("UPDATE users SET name = 'bobby' WHERE id = 2"))
            .unwrap();
        assert_eq!(outcome, ApplyOutcome::Applied { rows_affected: 1 });

        let outcome =
            machine.apply(4, &TxnEntry::single("DELETE FROM users WHERE id = 1")).unwrap();
        assert_eq!(outcome, ApplyOutcome::Applied { rows_affected: 1 });

        assert_eq!(names(&machine), vec!["bobby", "carol"]);
        assert_eq!(machine.last_applied(), 4);
    }

    /// commit_ts = log index: every MVCC stamp written by entry `N`
    /// carries `xmin = N`. Only observable with the MVCC feature on
    /// (stamping is compiled out otherwise).
    #[cfg(feature = "mvcc_enabled")]
    #[test]
    fn apply_stamps_commit_ts_with_the_log_index() {
        let machine = VibesqlStateMachine::new();
        create_users(&machine, 1);
        machine.apply(2, &TxnEntry::single("INSERT INTO users VALUES (1, 'alice')")).unwrap();
        machine.apply(3, &TxnEntry::single("INSERT INTO users VALUES (2, 'bob')")).unwrap();

        machine.with_db(|db| {
            let table = db.get_table("users").unwrap();
            let xmins: Vec<u64> = table.scan().iter().map(|row| row.xmin).collect();
            assert_eq!(xmins, vec![2, 3], "xmin must equal the applying entry's log index");
        });
    }

    #[test]
    fn apply_is_idempotent_per_index() {
        let machine = VibesqlStateMachine::new();
        create_users(&machine, 1);
        let entry = TxnEntry::single("INSERT INTO users VALUES (1, 'alice')");
        assert_eq!(
            machine.apply(2, &entry).unwrap(),
            ApplyOutcome::Applied { rows_affected: 1 }
        );

        // Re-applying the same index must not duplicate effects — this is
        // what makes snapshot+log-replay overlap safe.
        assert_eq!(machine.apply(2, &entry).unwrap(), ApplyOutcome::AlreadyApplied);
        assert_eq!(machine.apply(1, &entry).unwrap(), ApplyOutcome::AlreadyApplied);
        assert_eq!(names(&machine), vec!["alice"]);
        assert_eq!(machine.last_applied(), 2);
    }

    #[test]
    fn apply_gap_is_a_loud_error() {
        let machine = VibesqlStateMachine::new();
        create_users(&machine, 1);
        let err = machine
            .apply(3, &TxnEntry::single("INSERT INTO users VALUES (1, 'alice')"))
            .unwrap_err();
        assert!(format!("{err}").contains("apply gap"), "unexpected error: {err}");
        // Index 0 is the "nothing" sentinel.
        machine.apply(0, &TxnEntry::single("INSERT INTO users VALUES (1, 'x')")).unwrap_err();
        assert_eq!(machine.last_applied(), 1);
    }

    /// Multi-statement entries are atomic: a failing statement rolls the
    /// whole entry back, but the entry still consumes its index (every
    /// replica rejects it identically).
    #[test]
    fn rejected_entry_applies_nothing_and_still_consumes_its_index() {
        let machine = VibesqlStateMachine::new();
        create_users(&machine, 1);
        machine.apply(2, &TxnEntry::single("INSERT INTO users VALUES (1, 'alice')")).unwrap();

        let outcome = machine
            .apply(
                3,
                &TxnEntry::batch([
                    "INSERT INTO users VALUES (2, 'bob')",
                    // Duplicate primary key: fails after the first insert
                    // succeeded inside the txn.
                    "INSERT INTO users VALUES (1, 'dupe')",
                ]),
            )
            .unwrap();
        assert!(
            matches!(outcome, ApplyOutcome::Rejected { .. }),
            "constraint violation must reject the entry, got: {outcome:?}"
        );

        // All-or-nothing: bob must not have survived the rollback.
        assert_eq!(names(&machine), vec!["alice"]);
        // The index is consumed; the next entry continues after it.
        assert_eq!(machine.last_applied(), 3);
        machine.apply(4, &TxnEntry::single("INSERT INTO users VALUES (3, 'carol')")).unwrap();
        assert_eq!(names(&machine), vec!["alice", "carol"]);
    }

    #[test]
    fn transaction_control_and_reads_inside_an_entry_are_rejected() {
        let machine = VibesqlStateMachine::new();
        create_users(&machine, 1);
        for (index, sql) in [(2, "BEGIN"), (3, "COMMIT"), (4, "SELECT * FROM users")] {
            let outcome = machine.apply(index, &TxnEntry::single(sql)).unwrap();
            assert!(
                matches!(outcome, ApplyOutcome::Rejected { .. }),
                "{sql} inside an entry must be rejected, got: {outcome:?}"
            );
        }
    }

    #[test]
    fn snapshot_install_roundtrip_restores_state_and_index() {
        let machine = VibesqlStateMachine::new();
        create_users(&machine, 1);
        machine.apply(2, &TxnEntry::single("INSERT INTO users VALUES (1, 'alice')")).unwrap();
        machine.apply(3, &TxnEntry::single("INSERT INTO users VALUES (2, 'bob')")).unwrap();

        let snapshot = machine.snapshot().unwrap();
        assert_eq!(snapshot.last_included_index, 3);

        let restored = VibesqlStateMachine::from_snapshot(&snapshot).unwrap();
        assert_eq!(restored.last_applied(), 3);
        assert_eq!(names(&restored), names(&machine));

        // The restored machine continues the log where the snapshot left
        // off — and keeps the commit_ts = log index mapping.
        restored.apply(4, &TxnEntry::single("INSERT INTO users VALUES (3, 'carol')")).unwrap();
        assert_eq!(names(&restored), vec!["alice", "bob", "carol"]);
        #[cfg(feature = "mvcc_enabled")]
        restored.with_db(|db| {
            let table = db.get_table("users").unwrap();
            let carol =
                table.scan().iter().find(|r| r.values[0] == SqlValue::Integer(3)).unwrap();
            assert_eq!(carol.xmin, 4, "post-install applies keep commit_ts = log index");
        });
    }

    #[test]
    fn corrupt_snapshot_is_rejected_without_mutation() {
        let machine = VibesqlStateMachine::new();
        create_users(&machine, 1);
        machine.apply(2, &TxnEntry::single("INSERT INTO users VALUES (1, 'alice')")).unwrap();

        let err = machine
            .install_snapshot(&Snapshot {
                last_included_index: 9,
                data: b"definitely not a vbsql payload".to_vec(),
            })
            .unwrap_err();
        assert!(matches!(err, ConsensusError::SnapshotCodec(_)), "unexpected error: {err}");

        // The failed install must not have touched the state.
        assert_eq!(machine.last_applied(), 2);
        assert_eq!(names(&machine), vec!["alice"]);
    }

    /// Hand-stamp a committed tombstone (`xmax`) on a live row, the same
    /// way the storage crate's vacuum tests do: today's executor DELETE
    /// path bitmap-deletes immediately (and `gc_old_versions` skips
    /// bitmap-deleted rows), so vacuum-reclaimable garbage must be
    /// simulated via a deferred tombstone.
    #[cfg(feature = "mvcc_enabled")]
    fn tombstone_users_row(machine: &VibesqlStateMachine, row_idx: usize, xmax: u64) {
        machine.with_db_mut(|db| {
            db.get_table_mut("users").unwrap().stamp_row_xmax_inplace(row_idx, xmax);
        });
    }

    /// The real SnapshotHorizonPin: while the pin a snapshot build
    /// acquires is held, `vacuum_mvcc` cannot reclaim row versions the
    /// build might still read; releasing it un-blocks reclamation. This
    /// replaces Phase A4's recording-pin assertion with one against the
    /// actual storage-layer holdback.
    #[cfg(feature = "mvcc_enabled")]
    #[test]
    fn horizon_pin_holds_vacuum_back_until_released() {
        let machine = VibesqlStateMachine::new();
        create_users(&machine, 1);
        machine.apply(2, &TxnEntry::single("INSERT INTO users VALUES (1, 'alice')")).unwrap();

        // Acquire the pin exactly as `snapshot()` does (same code path).
        // The horizon is pinned at 3 (= next commit_ts).
        let pin = machine.horizon_pin().acquire();

        // A "transaction" at commit_ts 3 tombstones alice's version, and
        // a later entry advances the allocator watermark past it — so the
        // version would be reclaimable if not for the pin.
        tombstone_users_row(&machine, 0, 3);
        machine.apply(3, &TxnEntry::single("INSERT INTO users VALUES (2, 'bob')")).unwrap();

        assert_eq!(
            machine.vacuum().unwrap(),
            0,
            "vacuum must reclaim nothing while the snapshot pin is held"
        );

        drop(pin);
        assert_eq!(
            machine.vacuum().unwrap(),
            1,
            "vacuum must reclaim the dead version once the pin is released"
        );
    }

    /// `snapshot()` itself acquires and releases the pin: after a build
    /// completes, vacuum proceeds normally (nothing leaks pinned).
    #[cfg(feature = "mvcc_enabled")]
    #[test]
    fn snapshot_releases_the_horizon_pin_when_done() {
        let machine = VibesqlStateMachine::new();
        create_users(&machine, 1);
        machine.apply(2, &TxnEntry::single("INSERT INTO users VALUES (1, 'alice')")).unwrap();
        tombstone_users_row(&machine, 0, 3);
        machine.apply(3, &TxnEntry::single("INSERT INTO users VALUES (2, 'bob')")).unwrap();

        let snapshot = machine.snapshot().unwrap();
        assert_eq!(snapshot.last_included_index, 3);

        // The build's pin was released with the build; the dead version
        // is reclaimable again.
        assert_eq!(machine.vacuum().unwrap(), 1, "the build must not leak its horizon pin");
    }
}
