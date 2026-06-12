//! [`ReplicatedDb`]: the PR 1 propose path — run transactions through
//! consensus on a single node (Raft Phase B1, #5199).
//!
//! This couples a [`ConsensusBackend`] whose entries are [`TxnEntry`]
//! with a [`VibesqlStateMachine`]: writes are proposed as whole-
//! transaction entries, and committed entries are applied — in dense log
//! order — to the real database, with commit_ts = log index. Reads run
//! locally against the applied state.
//!
//! This is a **test-facing harness**, not server wiring (deliberately:
//! server/CLI integration is out of scope for PR 1). With
//! [`SingleNodeBackend`](crate::SingleNodeBackend), `propose` commits
//! immediately and the harness applies the committed entry before the
//! write returns, so a session can read its own write — the same
//! "propose resolves post-apply" contract the openraft state machine
//! gives for free. PR 2 mounts [`VibesqlStateMachine`] behind openraft's
//! `RaftStateMachine` so apply happens *inside* the backend on every
//! voter; the harness's catch-up loop then becomes the recovery/replay
//! path only.

use vibesql_types::SqlValue;

use crate::backend::{ConsensusBackend, LogIndex, Result, Snapshot};
use crate::state_machine::{ApplyOutcome, TxnEntry, VibesqlStateMachine};

/// A database whose writes flow through a consensus backend.
///
/// See the module docs for scope (PR 1 test harness). The interesting
/// invariants all live in [`VibesqlStateMachine`]; this type contributes
/// the propose → read-committed → apply plumbing and the catch-up loop.
#[derive(Debug)]
pub struct ReplicatedDb<B> {
    backend: B,
    machine: VibesqlStateMachine,
}

impl<B> ReplicatedDb<B>
where
    B: ConsensusBackend<Entry = TxnEntry>,
{
    /// Couple `backend` with a fresh (empty) state machine.
    pub fn new(backend: B) -> Self {
        Self { backend, machine: VibesqlStateMachine::new() }
    }

    /// Couple `backend` with an existing state machine — e.g. one
    /// restored from a snapshot, after which [`catch_up_to`]
    /// (or the next write) replays only the log suffix above the
    /// snapshot (entries at or below it are skipped by the machine's
    /// idempotence).
    ///
    /// [`catch_up_to`]: Self::catch_up_to
    pub fn with_machine(backend: B, machine: VibesqlStateMachine) -> Self {
        Self { backend, machine }
    }

    /// The state machine applying this node's committed entries.
    pub fn machine(&self) -> &VibesqlStateMachine {
        &self.machine
    }

    /// The consensus backend sequencing this node's writes.
    pub fn backend(&self) -> &B {
        &self.backend
    }

    /// Execute one autocommit write statement through consensus.
    pub async fn execute_replicated(&self, sql: &str) -> Result<(LogIndex, ApplyOutcome)> {
        self.execute_replicated_txn(&[sql]).await
    }

    /// Execute a multi-statement transaction through consensus: the
    /// whole batch is proposed as **one** [`TxnEntry`] (one log entry
    /// per committed transaction) and applied atomically with
    /// commit_ts = the entry's log index.
    ///
    /// Returns the entry's log index and its apply outcome. A rejected
    /// entry (failed statement, rolled back) is an `Ok` with
    /// [`ApplyOutcome::Rejected`] — the rejection consumed a log index
    /// and is part of the replicated history.
    pub async fn execute_replicated_txn(
        &self,
        statements: &[&str],
    ) -> Result<(LogIndex, ApplyOutcome)> {
        let entry = TxnEntry::batch(statements.iter().copied());
        let index = self.backend.propose(entry).await?;
        let outcome = self.catch_up_to(index).await?;
        Ok((index, outcome))
    }

    /// Apply every committed entry up to and including `index`, in dense
    /// log order, returning the outcome at `index`. Entries the machine
    /// has already applied (`<= last_applied`) are skipped by its
    /// idempotence check, which is what makes this safe to run on top of
    /// an installed snapshot that overlaps the log.
    pub async fn catch_up_to(&self, index: LogIndex) -> Result<ApplyOutcome> {
        let mut outcome = ApplyOutcome::AlreadyApplied;
        let first_pending = self.machine.last_applied() + 1;
        for idx in first_pending..=index {
            let entry = self.backend.read_committed(idx).await?;
            outcome = self.machine.apply(idx, &entry)?;
        }
        Ok(outcome)
    }

    /// Run a read-only SELECT against the applied state (reads are
    /// local, not replicated).
    pub fn query(&self, sql: &str) -> Result<Vec<Vec<SqlValue>>> {
        self.machine.query(sql)
    }

    /// Snapshot the applied database state (binary persistence codec,
    /// vacuum horizon pinned for the build) — see
    /// [`VibesqlStateMachine::snapshot`].
    pub fn snapshot(&self) -> Result<Snapshot> {
        self.machine.snapshot()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::single_node::SingleNodeBackend;
    use crate::ConsensusError;

    fn replicated() -> ReplicatedDb<SingleNodeBackend<TxnEntry>> {
        ReplicatedDb::new(SingleNodeBackend::new())
    }

    async fn seed_users(db: &ReplicatedDb<SingleNodeBackend<TxnEntry>>) {
        let (index, outcome) = db
            .execute_replicated("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
            .await
            .unwrap();
        assert_eq!(index, 1);
        assert_eq!(outcome, ApplyOutcome::Applied { rows_affected: 0 });
    }

    fn names(db: &ReplicatedDb<SingleNodeBackend<TxnEntry>>) -> Vec<String> {
        db.query("SELECT name FROM users ORDER BY id")
            .unwrap()
            .into_iter()
            .map(|row| row[0].to_string())
            .collect()
    }

    /// The PR 1 acceptance roundtrip: INSERT/UPDATE/DELETE through the
    /// single-node backend, data visible afterwards, commit_ts = log
    /// index (asserted at the MVCC level when the feature is on).
    #[tokio::test]
    async fn propose_apply_roundtrip_through_single_node_backend() {
        let db = replicated();
        seed_users(&db).await;

        let (index, outcome) =
            db.execute_replicated("INSERT INTO users VALUES (1, 'alice')").await.unwrap();
        assert_eq!(index, 2);
        assert_eq!(outcome, ApplyOutcome::Applied { rows_affected: 1 });

        let (index, _) =
            db.execute_replicated("INSERT INTO users VALUES (2, 'bob')").await.unwrap();
        assert_eq!(index, 3);

        let (index, outcome) = db
            .execute_replicated("UPDATE users SET name = 'amelia' WHERE id = 1")
            .await
            .unwrap();
        assert_eq!(index, 4);
        assert_eq!(outcome, ApplyOutcome::Applied { rows_affected: 1 });

        let (index, outcome) =
            db.execute_replicated("DELETE FROM users WHERE id = 2").await.unwrap();
        assert_eq!(index, 5);
        assert_eq!(outcome, ApplyOutcome::Applied { rows_affected: 1 });

        // The session reads its own writes immediately after propose.
        assert_eq!(names(&db), vec!["amelia"]);
        assert_eq!(db.machine().last_applied(), 5);

        // commit_ts = log index, observable in the MVCC stamps.
        #[cfg(feature = "mvcc_enabled")]
        db.machine().with_db(|inner| {
            let table = inner.get_table("users").unwrap();
            let amelia = table
                .scan()
                .iter()
                .find(|r| r.values[0] == SqlValue::Integer(1) && r.xmax.is_none())
                .expect("amelia's live version");
            assert_eq!(amelia.xmin, 4, "UPDATE's new version is stamped with its log index");
        });
    }

    /// A whole transaction is one log entry; it applies all-or-nothing.
    #[tokio::test]
    async fn multi_statement_txn_is_one_entry_and_atomic() {
        let db = replicated();
        seed_users(&db).await;

        let (index, outcome) = db
            .execute_replicated_txn(&[
                "INSERT INTO users VALUES (1, 'alice')",
                "INSERT INTO users VALUES (2, 'bob')",
            ])
            .await
            .unwrap();
        assert_eq!(index, 2, "the whole batch consumed exactly one log index");
        assert_eq!(outcome, ApplyOutcome::Applied { rows_affected: 2 });

        // A failing batch rolls back entirely but still consumes its
        // index; the history converges on every replica.
        let (index, outcome) = db
            .execute_replicated_txn(&[
                "INSERT INTO users VALUES (3, 'carol')",
                "INSERT INTO users VALUES (1, 'dupe')", // PK violation
            ])
            .await
            .unwrap();
        assert_eq!(index, 3);
        assert!(matches!(outcome, ApplyOutcome::Rejected { .. }), "got: {outcome:?}");
        assert_eq!(names(&db), vec!["alice", "bob"], "rejected entry must apply nothing");

        let (index, _) =
            db.execute_replicated("INSERT INTO users VALUES (4, 'dave')").await.unwrap();
        assert_eq!(index, 4, "log numbering continues past the rejected entry");
        assert_eq!(names(&db), vec!["alice", "bob", "dave"]);
    }

    /// Snapshot + log-replay overlap: a fresh machine restored from a
    /// snapshot at index 3 replays the full log 1..=5; entries 1–3 are
    /// skipped by idempotence, 4–5 apply, and the result is identical to
    /// the original.
    #[tokio::test]
    async fn snapshot_install_plus_log_replay_overlap_converges() {
        let db = replicated();
        seed_users(&db).await;
        db.execute_replicated("INSERT INTO users VALUES (1, 'alice')").await.unwrap();
        db.execute_replicated("INSERT INTO users VALUES (2, 'bob')").await.unwrap();

        let snapshot = db.snapshot().unwrap();
        assert_eq!(snapshot.last_included_index, 3);

        db.execute_replicated("UPDATE users SET name = 'bobby' WHERE id = 2").await.unwrap();
        db.execute_replicated("INSERT INTO users VALUES (3, 'carol')").await.unwrap();

        // "Recover" a second node: install the snapshot, then replay the
        // whole committed log against the same backend log.
        let machine = VibesqlStateMachine::from_snapshot(&snapshot).unwrap();
        assert_eq!(machine.last_applied(), 3);
        let recovered = ReplicatedDb::with_machine(clone_log(&db).await, machine);
        recovered.catch_up_to(5).await.unwrap();

        assert_eq!(recovered.machine().last_applied(), 5);
        assert_eq!(names(&recovered), names(&db));
        assert_eq!(names(&recovered), vec!["alice", "bobby", "carol"]);
    }

    /// Rebuild a `SingleNodeBackend` holding the same committed log (the
    /// loopback backend is in-memory; a "second node" shares the log by
    /// re-proposing the committed prefix).
    async fn clone_log(
        db: &ReplicatedDb<SingleNodeBackend<TxnEntry>>,
    ) -> SingleNodeBackend<TxnEntry> {
        let source = db.backend();
        let target = SingleNodeBackend::new();
        for idx in 1..=source.last_index() {
            let entry = source.read_committed(idx).await.unwrap();
            target.propose(entry).await.unwrap();
        }
        target
    }

    /// Replaying an already-applied prefix is harmless (idempotence at
    /// the harness level): catch_up_to with a stale index does nothing.
    #[tokio::test]
    async fn catch_up_is_idempotent() {
        let db = replicated();
        seed_users(&db).await;
        db.execute_replicated("INSERT INTO users VALUES (1, 'alice')").await.unwrap();

        for _ in 0..3 {
            let outcome = db.catch_up_to(2).await.unwrap();
            assert_eq!(outcome, ApplyOutcome::AlreadyApplied);
        }
        assert_eq!(names(&db), vec!["alice"]);
    }

    /// Catching up to an uncommitted index is a NotCommitted error from
    /// the backend, surfaced unchanged.
    #[tokio::test]
    async fn catch_up_beyond_committed_prefix_fails() {
        let db = replicated();
        seed_users(&db).await;
        let err = db.catch_up_to(7).await.unwrap_err();
        assert!(matches!(err, ConsensusError::NotCommitted(2)), "got: {err:?}");
    }

    /// TCL Priority-1-style smoke through the replicated path. The TCL
    /// harness itself drives the CLI binary, which is not wired to
    /// consensus in PR 1 (server/CLI integration is out of scope), so
    /// this exercises a representative set of P1 statement shapes —
    /// CREATE TABLE / INSERT / UPDATE with WHERE / DELETE with WHERE /
    /// CREATE INDEX / aggregates / ORDER BY / JOIN — end-to-end through
    /// propose + apply instead. The full `make test-tcl` baseline runs
    /// against the unreplicated engine, which this PR leaves untouched.
    #[tokio::test]
    async fn tcl_p1_style_smoke_through_replication() {
        let db = replicated();

        db.execute_replicated("CREATE TABLE t1 (a INTEGER PRIMARY KEY, b INTEGER, c TEXT)")
            .await
            .unwrap();
        for (a, b, c) in [(1, 10, "one"), (2, 20, "two"), (3, 30, "three"), (4, 40, "four")] {
            let (_, outcome) = db
                .execute_replicated(&format!("INSERT INTO t1 VALUES ({a}, {b}, '{c}')"))
                .await
                .unwrap();
            assert_eq!(outcome, ApplyOutcome::Applied { rows_affected: 1 });
        }
        db.execute_replicated("CREATE INDEX i1 ON t1(b)").await.unwrap();
        db.execute_replicated("UPDATE t1 SET b = b + 1 WHERE a > 2").await.unwrap();
        db.execute_replicated("DELETE FROM t1 WHERE a = 1").await.unwrap();

        db.execute_replicated("CREATE TABLE t2 (a INTEGER, d TEXT)").await.unwrap();
        db.execute_replicated_txn(&[
            "INSERT INTO t2 VALUES (2, 'zwei')",
            "INSERT INTO t2 VALUES (3, 'drei')",
        ])
        .await
        .unwrap();

        // WHERE + ORDER BY
        let rows = db.query("SELECT a, b FROM t1 WHERE b >= 20 ORDER BY b DESC").unwrap();
        assert_eq!(
            rows,
            vec![
                vec![SqlValue::Integer(4), SqlValue::Integer(41)],
                vec![SqlValue::Integer(3), SqlValue::Integer(31)],
                vec![SqlValue::Integer(2), SqlValue::Integer(20)],
            ]
        );

        // Aggregates
        let rows = db.query("SELECT count(*), sum(b) FROM t1").unwrap();
        assert_eq!(rows, vec![vec![SqlValue::Integer(3), SqlValue::Integer(92)]]);

        // JOIN
        let rows = db
            .query("SELECT t1.c, t2.d FROM t1 JOIN t2 ON t1.a = t2.a ORDER BY t1.a")
            .unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][1].to_string(), "zwei");
        assert_eq!(rows[1][1].to_string(), "drei");

        assert_eq!(db.machine().last_applied(), db.backend().last_index());
    }
}
