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

use crate::{
    backend::{ConsensusBackend, ConsensusError, LogIndex, Result, Snapshot},
    state_machine::{ApplyOutcome, QueryResult, TxnEntry, VibesqlStateMachine},
};

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
    /// Non-deterministic expressions (`random()`, `CURRENT_TIMESTAMP`,
    /// `datetime('now')`, …) are **frozen here, at propose time**
    /// (#5377): each call site is evaluated once and the drawn values
    /// travel in the entry, so every replica applies identical
    /// statements. Statements whose nondeterminism cannot be frozen
    /// (per-row `random()`, session-state functions, volatile calls in
    /// subqueries/CTEs/`INSERT … SELECT`) fail the propose with a
    /// [`ConsensusError::Backend`] error before consuming a log index.
    ///
    /// Returns the entry's log index and its apply outcome. A rejected
    /// entry (failed statement, rolled back) is an `Ok` with
    /// [`ApplyOutcome::Rejected`] — the rejection consumed a log index
    /// and is part of the replicated history.
    pub async fn execute_replicated_txn(
        &self,
        statements: &[&str],
    ) -> Result<(LogIndex, ApplyOutcome)> {
        let mut frozen = Vec::with_capacity(statements.len());
        let mut frozen_defaults = Vec::with_capacity(statements.len());
        for sql in statements {
            // Freeze positional volatile sites and — against this
            // node's applied schema (#5381) — the volatile column
            // DEFAULTs the statement fires.
            let (values, defaults) = self.machine.freeze_for_propose(sql).map_err(|e| {
                ConsensusError::Backend(format!("statement cannot be replicated: {e}"))
            })?;
            frozen.push(values);
            frozen_defaults.push(defaults);
        }
        let entry =
            TxnEntry::frozen_batch(statements.iter().map(|s| s.to_string()).collect(), frozen)
                .with_frozen_defaults(frozen_defaults);
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
    pub fn query(&self, sql: &str) -> Result<QueryResult> {
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
    use vibesql_types::SqlValue;

    use super::*;
    use crate::{single_node::SingleNodeBackend, ConsensusError};

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
            .rows
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

        let (index, outcome) =
            db.execute_replicated("UPDATE users SET name = 'amelia' WHERE id = 1").await.unwrap();
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

    /// The #5377 acceptance roundtrip: non-deterministic expressions
    /// are frozen at propose time, the logged entry carries the drawn
    /// values, the applied rows equal those values, and a second
    /// machine replaying the identical log converges exactly —
    /// including `random()`, `randomblob()`, `datetime('now')`, and
    /// `CURRENT_TIMESTAMP` in both INSERT (exactly-once) and UPDATE
    /// (per-row, clock) positions.
    #[tokio::test]
    async fn nondeterministic_sql_freezes_at_propose_and_converges() {
        let db = replicated();
        db.execute_replicated(
            "CREATE TABLE t (id INTEGER PRIMARY KEY, r INTEGER, b BLOB, ts TIMESTAMP)",
        )
        .await
        .unwrap();

        let (index, outcome) = db
            .execute_replicated(
                "INSERT INTO t VALUES (1, random(), randomblob(8), CURRENT_TIMESTAMP)",
            )
            .await
            .unwrap();
        assert_eq!(outcome, ApplyOutcome::Applied { rows_affected: 1 });
        db.execute_replicated("INSERT INTO t (id, ts) VALUES (2, datetime('now'))").await.unwrap();
        let (_, outcome) = db
            .execute_replicated("UPDATE t SET ts = CURRENT_TIMESTAMP WHERE id = 2")
            .await
            .unwrap();
        assert_eq!(outcome, ApplyOutcome::Applied { rows_affected: 1 });

        // The clock aliases and arity-dependent clock reads from the PR
        // #5382 review (curdate/curtime/1-arg age/CAST(TIME AS
        // TIMESTAMP)) freeze at propose like the rest.
        let (alias_index, outcome) = db
            .execute_replicated(
                "CREATE TABLE clk (id INTEGER PRIMARY KEY, d DATE, t TIME, a TEXT, c TIMESTAMP)",
            )
            .await
            .unwrap();
        assert_eq!(outcome, ApplyOutcome::Applied { rows_affected: 0 });
        let (alias_index, outcome) = db
            .execute_replicated(
                "INSERT INTO clk VALUES (1, curdate(), curtime(), age(DATE '2020-01-02'), \
                 CAST(TIME '12:34:56' AS TIMESTAMP))",
            )
            .await
            .unwrap_or_else(|e| {
                panic!("clock aliases must freeze at propose: {e} ({alias_index})")
            });
        assert_eq!(outcome, ApplyOutcome::Applied { rows_affected: 1 });
        let alias_entry = db.backend().read_committed(alias_index).await.unwrap();
        assert_eq!(
            alias_entry.frozen[0].len(),
            4,
            "curdate, curtime, age, CAST(TIME AS TIMESTAMP) must all freeze"
        );
        let rows = db.query("SELECT d, t, a, c FROM clk WHERE id = 1").unwrap();
        for (got, frozen) in rows.rows[0].iter().zip(&alias_entry.frozen[0]) {
            assert_eq!(
                *got,
                SqlValue::from(frozen.clone()),
                "applied value must equal the proposer-frozen value"
            );
        }

        // The logged entry carries the proposer-drawn values, and the
        // applied row equals them (the determinism proof: apply did not
        // re-evaluate the volatile calls).
        let entry = db.backend().read_committed(index).await.unwrap();
        assert_eq!(entry.frozen.len(), 1);
        assert_eq!(entry.frozen[0].len(), 3, "random, randomblob, CURRENT_TIMESTAMP");
        let rows = db.query("SELECT r, b FROM t WHERE id = 1").unwrap();
        assert_eq!(rows.rows[0][0], SqlValue::from(entry.frozen[0][0].clone()));
        assert_eq!(rows.rows[0][1], SqlValue::from(entry.frozen[0][1].clone()));

        // A second machine replaying the same log converges exactly.
        let recovered = ReplicatedDb::new(clone_log(&db).await);
        recovered.catch_up_to(db.backend().last_index()).await.unwrap();
        assert_eq!(
            recovered.query("SELECT id, r, b, ts FROM t ORDER BY id").unwrap(),
            db.query("SELECT id, r, b, ts FROM t ORDER BY id").unwrap(),
            "replicas must converge on identical rows despite non-deterministic SQL"
        );
        assert_eq!(
            recovered.query("SELECT id, d, t, a, c FROM clk ORDER BY id").unwrap(),
            db.query("SELECT id, d, t, a, c FROM clk ORDER BY id").unwrap(),
            "clock-alias rows must converge (curdate/curtime/age/CAST)"
        );

        // ... and replaying the already-applied log again changes nothing
        // (idempotence with frozen entries).
        recovered.catch_up_to(db.backend().last_index()).await.unwrap();
        assert_eq!(
            recovered.query("SELECT id, r, b, ts FROM t ORDER BY id").unwrap(),
            db.query("SELECT id, r, b, ts FROM t ORDER BY id").unwrap()
        );
    }

    /// Statements whose nondeterminism cannot be frozen fail at propose
    /// time, before consuming a log index.
    #[tokio::test]
    async fn unfreezable_nondeterminism_is_rejected_at_propose() {
        let db = replicated();
        seed_users(&db).await;
        for sql in [
            // Per-row volatile: freezing would change semantics.
            "UPDATE users SET name = random()",
            "DELETE FROM users WHERE random() % 2 = 0",
            // Session state is not replicated state.
            "INSERT INTO users VALUES (1, last_insert_rowid())",
            // Data-dependent evaluation count.
            "INSERT INTO users SELECT id + 1, random() FROM users",
        ] {
            let err = db.execute_replicated(sql).await.unwrap_err();
            assert!(
                matches!(err, ConsensusError::Backend(_)),
                "{sql} must fail at propose, got: {err:?}"
            );
        }
        assert_eq!(db.backend().last_index(), 1, "no log index may be consumed");
    }

    /// #5406: the propose path resolves TIME→TIMESTAMP cast operands in a
    /// query body against the leader's applied catalog. A provably-non-TIME
    /// operand (an other-table TEXT column inside `INSERT … SELECT`) is
    /// *accepted* — the schema-free propose validator used to over-reject
    /// it — and replays identically on a second machine; a genuinely-TIME
    /// operand is rejected at propose before consuming a log index.
    #[tokio::test]
    async fn query_body_time_cast_disposition_is_schema_resolved_at_propose() {
        let db = replicated();
        db.execute_replicated("CREATE TABLE sched (id INTEGER PRIMARY KEY, ts TIMESTAMP)")
            .await
            .unwrap();
        db.execute_replicated("CREATE TABLE src (id INTEGER PRIMARY KEY, txt TEXT, tm TIME)")
            .await
            .unwrap();
        db.execute_replicated(
            "INSERT INTO src (id, txt, tm) VALUES (1, '2026-06-13 09:00:00', '09:00:00')",
        )
        .await
        .unwrap();

        // Provably-non-TIME other-table column → accepted at propose.
        let (_, outcome) = db
            .execute_replicated(
                "INSERT INTO sched (id, ts) SELECT id, CAST(src.txt AS TIMESTAMP) FROM src",
            )
            .await
            .expect("provably-non-TIME query-body cast must be accepted at propose (#5406)");
        assert_eq!(outcome, ApplyOutcome::Applied { rows_affected: 1 });

        // Genuinely-TIME other-table column → rejected at propose, no index.
        let before = db.backend().last_index();
        let err = db
            .execute_replicated(
                "INSERT INTO sched (id, ts) SELECT id, CAST(src.tm AS TIMESTAMP) FROM src",
            )
            .await
            .unwrap_err();
        assert!(matches!(err, ConsensusError::Backend(_)), "got: {err:?}");
        assert_eq!(db.backend().last_index(), before, "rejected cast consumes no log index");

        // A second machine replaying the committed log converges exactly —
        // the accepted cast applied deterministically.
        let recovered = ReplicatedDb::new(clone_log(&db).await);
        recovered.catch_up_to(db.backend().last_index()).await.unwrap();
        assert_eq!(
            recovered.query("SELECT id, ts FROM sched ORDER BY id").unwrap(),
            db.query("SELECT id, ts FROM sched ORDER BY id").unwrap(),
            "replicas must converge on the accepted cast's row"
        );
    }

    /// A rejected entry (constraint violation) rejects identically on a
    /// second machine replaying the log: rejection is part of the
    /// deterministic history.
    #[tokio::test]
    async fn rejected_entries_replay_identically_on_a_second_machine() {
        let db = replicated();
        seed_users(&db).await;
        let mut outcomes = Vec::new();
        for sql in [
            "INSERT INTO users VALUES (1, 'alice')",
            "INSERT INTO users VALUES (1, 'dupe')", // PK violation: rejected
            "INSERT INTO users VALUES (2, 'bob')",
        ] {
            let (_, outcome) = db.execute_replicated(sql).await.unwrap();
            outcomes.push(outcome);
        }
        assert!(matches!(outcomes[1], ApplyOutcome::Rejected { .. }));

        let backend = clone_log(&db).await;
        let machine = VibesqlStateMachine::new();
        for idx in 1..=backend.last_index() {
            let entry = backend.read_committed(idx).await.unwrap();
            let outcome = machine.apply(idx, &entry).unwrap();
            if idx > 1 {
                assert_eq!(
                    outcome,
                    outcomes[(idx - 2) as usize],
                    "entry {idx} must produce the same outcome on the second machine"
                );
            }
        }
        assert_eq!(
            machine.query("SELECT name FROM users ORDER BY id").unwrap(),
            db.query("SELECT name FROM users ORDER BY id").unwrap()
        );
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
            rows.rows,
            vec![
                vec![SqlValue::Integer(4), SqlValue::Integer(41)],
                vec![SqlValue::Integer(3), SqlValue::Integer(31)],
                vec![SqlValue::Integer(2), SqlValue::Integer(20)],
            ]
        );

        // Aggregates
        let rows = db.query("SELECT count(*), sum(b) FROM t1").unwrap();
        assert_eq!(rows.rows, vec![vec![SqlValue::Integer(3), SqlValue::Integer(92)]]);

        // JOIN
        let rows =
            db.query("SELECT t1.c, t2.d FROM t1 JOIN t2 ON t1.a = t2.a ORDER BY t1.a").unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0][1].to_string(), "zwei");
        assert_eq!(rows[1][1].to_string(), "drei");

        assert_eq!(db.machine().last_applied(), db.backend().last_index());
    }

    /// #5381 acceptance: a multi-row INSERT that omits a
    /// `DEFAULT CURRENT_TIMESTAMP` column freezes one drawn value **per
    /// row** at propose time (matching the executor's per-row
    /// `apply_default_values`); the logged entry carries those values,
    /// the applied rows equal them, and a second machine replaying the
    /// identical log converges exactly — the determinism proof that
    /// apply never re-read the clock.
    #[tokio::test]
    async fn multi_row_volatile_default_freezes_per_row_and_converges() {
        let db = replicated();
        db.execute_replicated(
            "CREATE TABLE events (id INTEGER PRIMARY KEY, \
             ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
        )
        .await
        .unwrap();

        // Three rows, column omitted: the DEFAULT fires for each.
        let (index, outcome) =
            db.execute_replicated("INSERT INTO events (id) VALUES (1), (2), (3)").await.unwrap();
        assert_eq!(outcome, ApplyOutcome::Applied { rows_affected: 3 });

        let entry = db.backend().read_committed(index).await.unwrap();
        assert_eq!(
            entry.frozen_defaults.len(),
            1,
            "the entry carries one statement's frozen DEFAULT list"
        );
        assert_eq!(
            entry.frozen_defaults[0].len(),
            3,
            "one frozen CURRENT_TIMESTAMP value per firing row"
        );

        // The applied rows equal the proposer-frozen values (apply did
        // not re-evaluate the clock).
        let rows = db.query("SELECT ts FROM events ORDER BY id").unwrap();
        for (got, frozen) in rows.iter().zip(&entry.frozen_defaults[0]) {
            assert_eq!(got[0], SqlValue::from(frozen.clone()));
        }

        // A second machine replaying the same committed log converges
        // exactly — every replica stamps the proposer's clock, not its
        // own.
        let recovered = ReplicatedDb::new(clone_log(&db).await);
        recovered.catch_up_to(db.backend().last_index()).await.unwrap();
        assert_eq!(
            recovered.query("SELECT id, ts FROM events ORDER BY id").unwrap(),
            db.query("SELECT id, ts FROM events ORDER BY id").unwrap(),
            "replicas must converge despite the volatile DEFAULT"
        );
    }

    /// #5381: an explicit-column INSERT that supplies `DEFAULT` (or
    /// `NULL`, which VibeSQL coerces to the DEFAULT) for a volatile
    /// column freezes the value, while an explicitly supplied literal is
    /// left untouched (its DEFAULT never fires).
    #[tokio::test]
    async fn explicit_default_and_null_freeze_supplied_literal_does_not() {
        let db = replicated();
        db.execute_replicated(
            "CREATE TABLE events (id INTEGER PRIMARY KEY, \
             ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
        )
        .await
        .unwrap();

        // row 1: explicit DEFAULT keyword fires; row 2: explicit NULL
        // fires; row 3: explicit literal does not.
        let (index, outcome) = db
            .execute_replicated(
                "INSERT INTO events (id, ts) VALUES \
                 (1, DEFAULT), (2, NULL), (3, '2020-01-01 00:00:00')",
            )
            .await
            .unwrap();
        assert_eq!(outcome, ApplyOutcome::Applied { rows_affected: 3 });

        let entry = db.backend().read_committed(index).await.unwrap();
        assert_eq!(
            entry.frozen_defaults[0].len(),
            2,
            "only the DEFAULT and NULL cells fire; the supplied literal does not"
        );
        // The supplied literal is preserved verbatim (not overwritten by
        // a frozen DEFAULT value).
        let supplied = db.query("SELECT ts FROM events WHERE id = 3").unwrap();
        assert_eq!(supplied[0][0].to_string(), "2020-01-01 00:00:00");
        for frozen in &entry.frozen_defaults[0] {
            assert_ne!(
                supplied[0][0],
                SqlValue::from(frozen.clone()),
                "the explicitly-supplied literal must not equal a frozen DEFAULT value"
            );
        }

        let recovered = ReplicatedDb::new(clone_log(&db).await);
        recovered.catch_up_to(db.backend().last_index()).await.unwrap();
        assert_eq!(
            recovered.query("SELECT id, ts FROM events ORDER BY id").unwrap(),
            db.query("SELECT id, ts FROM events ORDER BY id").unwrap()
        );
    }

    /// #5381: `UPDATE … SET col = DEFAULT` for a volatile-DEFAULT column
    /// freezes one value at propose time and converges on a second
    /// machine.
    #[tokio::test]
    async fn update_set_default_freezes_and_converges() {
        let db = replicated();
        db.execute_replicated(
            "CREATE TABLE events (id INTEGER PRIMARY KEY, \
             ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
        )
        .await
        .unwrap();
        db.execute_replicated("INSERT INTO events VALUES (1, '2000-01-01 00:00:00')")
            .await
            .unwrap();

        let (index, outcome) =
            db.execute_replicated("UPDATE events SET ts = DEFAULT WHERE id = 1").await.unwrap();
        assert_eq!(outcome, ApplyOutcome::Applied { rows_affected: 1 });

        let entry = db.backend().read_committed(index).await.unwrap();
        assert_eq!(entry.frozen_defaults[0].len(), 1, "the SET = DEFAULT site freezes one value");
        assert_eq!(
            db.query("SELECT ts FROM events WHERE id = 1").unwrap().rows,
            vec![vec![SqlValue::from(entry.frozen_defaults[0][0].clone())]]
        );

        let recovered = ReplicatedDb::new(clone_log(&db).await);
        recovered.catch_up_to(db.backend().last_index()).await.unwrap();
        assert_eq!(
            recovered.query("SELECT id, ts FROM events ORDER BY id").unwrap(),
            db.query("SELECT id, ts FROM events ORDER BY id").unwrap()
        );
    }

    /// #5381: `INSERT … SELECT` into a table with a volatile DEFAULT
    /// cannot be frozen (row count / NULL-ness not statically known) and
    /// is rejected at propose before consuming a log index.
    #[tokio::test]
    async fn insert_select_into_volatile_default_table_is_rejected_at_propose() {
        let db = replicated();
        db.execute_replicated("CREATE TABLE src (id INTEGER PRIMARY KEY)").await.unwrap();
        db.execute_replicated(
            "CREATE TABLE events (id INTEGER PRIMARY KEY, \
             ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
        )
        .await
        .unwrap();
        let before = db.backend().last_index();
        let err =
            db.execute_replicated("INSERT INTO events (id) SELECT id FROM src").await.unwrap_err();
        assert!(matches!(err, ConsensusError::Backend(_)), "got: {err:?}");
        assert_eq!(db.backend().last_index(), before, "no log index may be consumed");
    }

    /// #5381 defense-in-depth (the #5377 behavior, preserved): an entry
    /// that fires a volatile DEFAULT but carries **no** frozen values —
    /// e.g. proposed by pre-#5381 code via `TxnEntry::single`, or around
    /// the freeze pass — is rejected deterministically at apply on every
    /// replica.
    #[tokio::test]
    async fn old_entry_firing_volatile_default_is_rejected_at_apply() {
        let a = VibesqlStateMachine::new();
        let b = VibesqlStateMachine::new();
        let ddl = TxnEntry::single(
            "CREATE TABLE events (id INTEGER PRIMARY KEY, ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
        );
        a.apply(1, &ddl).unwrap();
        b.apply(1, &ddl).unwrap();

        // A bare entry (no frozen_defaults) firing the DEFAULT.
        let entry = TxnEntry::single("INSERT INTO events (id) VALUES (1)");
        let outcome_a = a.apply(2, &entry).unwrap();
        let outcome_b = b.apply(2, &entry).unwrap();
        assert!(
            matches!(&outcome_a, ApplyOutcome::Rejected { reason } if reason.contains("DEFAULT")),
            "an unfrozen volatile DEFAULT must be rejected, got: {outcome_a:?}"
        );
        assert_eq!(outcome_a, outcome_b, "rejection must be identical on every replica");
        assert!(a.query("SELECT * FROM events").unwrap().is_empty(), "nothing may have applied");
    }

    /// #5381: a `CREATE TRIGGER` whose body contains a volatile call is
    /// rejected at propose — it could never be frozen because the body
    /// re-executes per firing row at apply time.
    #[tokio::test]
    async fn volatile_bodied_create_trigger_is_rejected_at_propose() {
        let db = replicated();
        db.execute_replicated("CREATE TABLE t (id INTEGER PRIMARY KEY, r INTEGER)").await.unwrap();
        db.execute_replicated("CREATE TABLE log (id INTEGER PRIMARY KEY, r INTEGER)")
            .await
            .unwrap();
        let before = db.backend().last_index();
        let err = db
            .execute_replicated(
                "CREATE TRIGGER trg AFTER INSERT ON t \
                 BEGIN INSERT INTO log VALUES (NEW.id, random()); END",
            )
            .await
            .unwrap_err();
        assert!(matches!(err, ConsensusError::Backend(_)), "got: {err:?}");
        assert_eq!(db.backend().last_index(), before, "rejected trigger consumes no log index");
    }

    /// #5381: a non-volatile `CREATE TRIGGER` is admitted, and when its
    /// triggering INSERT replicates, the body fires deterministically —
    /// the audit row converges on a second machine (insert → trigger →
    /// audit-row).
    #[tokio::test]
    async fn nonvolatile_trigger_fires_identically_on_both_machines() {
        let db = replicated();
        db.execute_replicated("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)").await.unwrap();
        // The audit columns are non-IPK: inserting `NEW.col` into an
        // INTEGER PRIMARY KEY column of a trigger body's target table is
        // a separate, pre-existing executor limitation (see the #5381 PR
        // follow-on), orthogonal to the replication determinism this
        // test asserts.
        db.execute_replicated("CREATE TABLE audit (k INTEGER, v INTEGER)").await.unwrap();
        db.execute_replicated(
            "CREATE TRIGGER trg AFTER INSERT ON t \
             BEGIN INSERT INTO audit (k, v) VALUES (NEW.id, NEW.v); END",
        )
        .await
        .unwrap();

        // The triggering INSERT is what replicates; the body re-executes
        // at apply on every replica against replicated state only.
        let (_, outcome) = db.execute_replicated("INSERT INTO t VALUES (1, 42)").await.unwrap();
        assert_eq!(outcome, ApplyOutcome::Applied { rows_affected: 1 });
        assert_eq!(
            db.query("SELECT k, v FROM audit ORDER BY k").unwrap().rows,
            vec![vec![SqlValue::Integer(1), SqlValue::Integer(42)]],
            "the trigger body fired and wrote the audit row from NEW"
        );

        let recovered = ReplicatedDb::new(clone_log(&db).await);
        recovered.catch_up_to(db.backend().last_index()).await.unwrap();
        assert_eq!(
            recovered.query("SELECT k, v FROM audit ORDER BY k").unwrap(),
            db.query("SELECT k, v FROM audit ORDER BY k").unwrap(),
            "the audit row produced by the trigger must converge on both machines"
        );
    }

    /// #5381: a `CREATE TRIGGER` whose body INSERT would fire a volatile
    /// DEFAULT of *its* target table is rejected at CREATE time — the
    /// classic `audit(…, ts DEFAULT CURRENT_TIMESTAMP)` hazard the static
    /// validator cannot see (it needs the catalog).
    #[tokio::test]
    async fn trigger_body_firing_volatile_default_is_rejected() {
        let db = replicated();
        db.execute_replicated("CREATE TABLE t (id INTEGER PRIMARY KEY)").await.unwrap();
        db.execute_replicated(
            "CREATE TABLE audit (id INTEGER PRIMARY KEY, ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
        )
        .await
        .unwrap();
        // The body is statically volatile-free, so it passes propose and
        // is rejected at apply by the schema-dependent body audit
        // (`trigger_body_schema_violation`): an `Ok(Rejected)` outcome.
        let (_, outcome) = db
            .execute_replicated(
                "CREATE TRIGGER trg AFTER INSERT ON t \
                 BEGIN INSERT INTO audit (id) VALUES (NEW.id); END",
            )
            .await
            .unwrap();
        assert!(
            matches!(&outcome, ApplyOutcome::Rejected { reason }
                if reason.contains("DEFAULT") && reason.contains("trigger 'trg'")),
            "the trigger body firing a volatile DEFAULT must be rejected, got: {outcome:?}"
        );
        // The trigger was not created.
        db.execute_replicated("INSERT INTO t VALUES (1)").await.unwrap();
        assert!(db.query("SELECT * FROM audit").unwrap().is_empty());
    }

    /// #5381: DROP TRIGGER is admitted on the replicated surface.
    #[tokio::test]
    async fn drop_trigger_is_admitted() {
        let db = replicated();
        db.execute_replicated("CREATE TABLE t (id INTEGER PRIMARY KEY, v INTEGER)").await.unwrap();
        db.execute_replicated("CREATE TABLE audit (id INTEGER PRIMARY KEY, v INTEGER)")
            .await
            .unwrap();
        db.execute_replicated(
            "CREATE TRIGGER trg AFTER INSERT ON t \
             BEGIN INSERT INTO audit VALUES (NEW.id, NEW.v); END",
        )
        .await
        .unwrap();
        let (_, outcome) = db.execute_replicated("DROP TRIGGER trg").await.unwrap();
        assert_eq!(outcome, ApplyOutcome::Applied { rows_affected: 0 });

        // With the trigger gone, an INSERT no longer writes the audit row.
        db.execute_replicated("INSERT INTO t VALUES (1, 5)").await.unwrap();
        assert!(db.query("SELECT * FROM audit").unwrap().is_empty());
    }
}
