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
//! - The crash-recovery WAL replay (`vibesql-storage::wal::recovery`) that the issue proposed
//!   reusing as the apply function is a **stub** for DML — `apply_op` counts Insert/Update/Delete
//!   entries but does not apply them (it has no `table_id → table` resolution).
//! - The only write-set the storage layer captures per transaction is `TransactionChange` (kept for
//!   savepoint undo). It records DML only — no DDL — and replaying it would bypass the executor's
//!   index and constraint maintenance, which has no row-level "apply with index upkeep" entry point
//!   today.
//!
//! **Determinism (#5377)**: statement batches are deterministic only if
//! the statements are — so non-deterministic expressions are **frozen
//! at propose time**. The proposer evaluates `random()`,
//! `CURRENT_TIMESTAMP`, `datetime('now')`, … once and ships the drawn
//! values in [`TxnEntry::frozen`]; the apply path splices them back in
//! as literals before execution (`crate::freeze`). As defense in depth
//! the apply path *rejects* (deterministically, on every replica) any
//! entry that still contains an unfrozen non-deterministic site — the
//! state machine structurally cannot evaluate volatile SQL. A frozen-
//! value count mismatch (proposer/applier version skew) is treated as
//! fatal instead ([`ConsensusError::FatalApply`]), because recording a
//! local-only failure as a rejection would silently diverge replicas.
//!
//! **Failure classification** (judge note on PR #5378): a statement
//! failure inside apply is recorded as [`ApplyOutcome::Rejected`] only
//! when the failure is a deterministic function of the entry and the
//! replicated state (constraint violations, type errors, parse errors,
//! …) — by state induction every replica rejects it identically.
//! Failures that may be local to this node (storage errors, memory /
//! row / join / recursion limits, query timeouts) abort the apply with
//! [`ConsensusError::FatalApply`] **without consuming the index**: the
//! node must halt and resync (snapshot + replay) rather than record an
//! outcome other replicas may not reproduce.
//!
//! **Session context** (audited in #5392): statements run against a
//! bare [`Database`] with no session state, and that is load-bearing.
//! The session-scoped knobs that could change write-statement *semantics*
//! (`sql_mode`, `foreign_keys_enabled` / `defer_foreign_keys`,
//! `case_sensitive_like`, `reverse_unordered_selects` — which flips the
//! row order an `INSERT ... SELECT` (no `ORDER BY`) materializes, hence
//! the rowids it assigns — roles/security, session variables — see
//! `vibesql-storage::database::session`) all sit at their defaults on
//! every machine, because the replicated statement surface has no way
//! to mutate them: PRAGMA/SET/GRANT-style statements are rejected by
//! the dispatch below, and snapshots serialize only catalog + data.
//! Session functions that *read* such state (`last_insert_rowid()`,
//! `changes()`, `total_changes()`) are rejected by the freeze pass. The
//! server's replicated session (`vibesql-server::session`) likewise
//! never threads session settings into a write: the only `SET`s it
//! honors are the read-consistency knobs (`vibesql_read_consistency`,
//! `vibesql_max_staleness_ms`, `vibesql_ryw_wait_ms`), which are local
//! read-routing state and never enter a `TxnEntry`. This invariant is
//! pinned by the guard tests
//! `state_machine_session_settings_are_at_safe_write_semantics_defaults`
//! and `session_state_mutations_are_rejected_at_the_replicated_boundary`
//! below: if a future change moves the machine's default session state,
//! or lets a session-state mutation enter the replicated write surface,
//! the guard fails. Any setting that must travel with a write has to be
//! captured in [`TxnEntry`] and materialized deterministically — do not
//! execute session statements against the machine's database.
//!
//! The effects-form write-set (row-level mutations instead of SQL text)
//! remains the preferred long-term representation; see #5199 for why it
//! is not implementable against today's storage layer.
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
use crate::freeze::{self, FrozenValue, SubstituteError};
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TxnEntry {
    /// The transaction's write statements, in execution order. Applied
    /// atomically: all statements commit together or none do.
    pub statements: Vec<String>,
    /// Proposer-evaluated values for each statement's non-deterministic
    /// call sites (`random()`, `CURRENT_TIMESTAMP`, …), parallel to
    /// [`statements`](Self::statements); `frozen[i]` holds statement
    /// `i`'s values in the deterministic traversal order of
    /// [`crate::freeze`]. Empty inner vectors (and, via
    /// `#[serde(default)]`, entries encoded before #5377) mean "no
    /// non-deterministic sites" — the apply path then *rejects* any
    /// statement found to contain one.
    #[serde(default)]
    pub frozen: Vec<Vec<FrozenValue>>,
    /// Proposer-evaluated values for the non-deterministic column
    /// DEFAULTs each statement fires (#5381), parallel to
    /// [`statements`](Self::statements): `frozen_defaults[i]` holds
    /// statement `i`'s values in the deterministic site order of
    /// [`crate::freeze::substitute_volatile_defaults`] (schema column
    /// order, rows within each column, then `SET`-style assignments).
    ///
    /// Serde-compatible in both directions: entries encoded before
    /// #5381 decode as empty (the apply path then *rejects* any
    /// statement that fires a volatile DEFAULT — the #5377 behavior),
    /// and an all-empty list is normalized away by
    /// [`with_frozen_defaults`](Self::with_frozen_defaults) +
    /// `skip_serializing_if`, so DEFAULT-free entries keep the pre-#5381
    /// byte encoding.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub frozen_defaults: Vec<Vec<FrozenValue>>,
    /// The proposing leader's wall clock at propose time, in
    /// milliseconds since the UNIX epoch (Raft Phase B2, #5200, PR 2).
    ///
    /// **Bookkeeping only** — never consulted by the apply path's SQL
    /// execution (it is not an HLC and assigns no visibility): replicas
    /// record the stamp of the last applied stamped entry so a
    /// bounded-staleness read can prove "my applied state is no staler
    /// than `now - stamp`". Serde-compatible in both directions:
    /// entries encoded before #5200 decode as `None` (unknown
    /// staleness — bounded reads refuse until a stamped entry arrives),
    /// and `None` skips serialization so unstamped entries keep the
    /// pre-#5200 byte encoding.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub leader_time_ms: Option<u64>,
}

impl TxnEntry {
    /// Entry for a single-statement (autocommit-style) transaction.
    /// No frozen values: the statement must be deterministic, or the
    /// apply path will reject it. Use [`crate::freeze::freeze_statement`]
    /// (as [`ReplicatedDb`](crate::ReplicatedDb) does) to replicate
    /// non-deterministic SQL.
    pub fn single(sql: impl Into<String>) -> Self {
        Self {
            statements: vec![sql.into()],
            frozen: Vec::new(),
            frozen_defaults: Vec::new(),
            leader_time_ms: None,
        }
    }

    /// Entry for a multi-statement transaction (no frozen values — see
    /// [`single`](Self::single)).
    pub fn batch<S: Into<String>>(statements: impl IntoIterator<Item = S>) -> Self {
        Self {
            statements: statements.into_iter().map(Into::into).collect(),
            frozen: Vec::new(),
            frozen_defaults: Vec::new(),
            leader_time_ms: None,
        }
    }

    /// Entry whose non-deterministic call sites were frozen at propose
    /// time: `frozen[i]` carries the evaluated values for
    /// `statements[i]`.
    pub fn frozen_batch(statements: Vec<String>, frozen: Vec<Vec<FrozenValue>>) -> Self {
        debug_assert_eq!(statements.len(), frozen.len());
        Self { statements, frozen, frozen_defaults: Vec::new(), leader_time_ms: None }
    }

    /// Attach proposer-frozen DEFAULT values (#5381):
    /// `frozen_defaults[i]` carries the values for the
    /// non-deterministic column DEFAULTs `statements[i]` fires. An
    /// all-empty list is normalized to empty so DEFAULT-free entries
    /// keep the pre-#5381 byte encoding.
    pub fn with_frozen_defaults(mut self, frozen_defaults: Vec<Vec<FrozenValue>>) -> Self {
        debug_assert!(
            frozen_defaults.is_empty() || frozen_defaults.len() == self.statements.len()
        );
        self.frozen_defaults = if frozen_defaults.iter().all(Vec::is_empty) {
            Vec::new()
        } else {
            frozen_defaults
        };
        self
    }

    /// Stamp this entry with the proposing leader's wall clock (ms since
    /// the UNIX epoch) — see [`leader_time_ms`](Self::leader_time_ms).
    pub fn stamp_leader_time(mut self, ms: u64) -> Self {
        self.leader_time_ms = Some(ms);
        self
    }
}

/// Result of applying one [`TxnEntry`] to the state machine.
///
/// Serializable because the openraft mount (PR 2 of #5199,
/// `crate::mvcc_raft`) returns it as the engine-level apply response
/// (`R` of the raft type config), which openraft requires to be serde-
/// capable. It never travels between nodes — `client_write` responses
/// are local.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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
/// behind openraft's `RaftStateMachine` for multi-node replication —
/// see `crate::mvcc_raft` ([`MvccRaftNode`](crate::MvccRaftNode)).
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
        static NO_FROZEN: &[FrozenValue] = &[];
        for (i, sql) in entry.statements.iter().enumerate() {
            let frozen = entry.frozen.get(i).map(Vec::as_slice).unwrap_or(NO_FROZEN);
            let frozen_defaults =
                entry.frozen_defaults.get(i).map(Vec::as_slice).unwrap_or(NO_FROZEN);
            match execute_write_statement(&mut inner.db, sql, frozen, frozen_defaults) {
                Ok(n) => rows_affected += n,
                Err(StatementFailure::Rejected(reason)) => {
                    failure = Some(reason);
                    break;
                }
                Err(StatementFailure::Fatal(reason)) => {
                    // A possibly node-local failure: roll back, do NOT
                    // consume the index, and surface a fatal error — the
                    // node must halt and resync rather than record an
                    // outcome other replicas may not reproduce.
                    let _ = inner.db.rollback_transaction();
                    return Err(ConsensusError::FatalApply(format!(
                        "entry {index}, statement {i}: {reason}"
                    )));
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
                        match classify_executor_error(&e) {
                            FailureClass::Deterministic => {
                                ApplyOutcome::Rejected { reason: e.to_string() }
                            }
                            FailureClass::PossiblyLocal => {
                                return Err(ConsensusError::FatalApply(format!(
                                    "entry {index}, COMMIT: {e}"
                                )));
                            }
                        }
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

    /// Propose-side freeze with schema access (#5381): freeze the
    /// statement's positional non-deterministic sites **and** the
    /// non-deterministic column DEFAULTs it fires, resolved against
    /// this machine's applied state — on the leader, that *is* the
    /// replicated schema. Returns `(positional, defaults)` for
    /// [`TxnEntry::frozen_batch`] /
    /// [`TxnEntry::with_frozen_defaults`].
    ///
    /// A statement whose target table is not in the applied state
    /// freezes no DEFAULT values (e.g. the table is created earlier in
    /// the same entry); if it turns out to fire a volatile DEFAULT at
    /// apply time, the apply path rejects it deterministically.
    pub(crate) fn freeze_for_propose(
        &self,
        sql: &str,
    ) -> std::result::Result<(Vec<FrozenValue>, Vec<FrozenValue>), freeze::FreezeError> {
        let schema: Option<vibesql_catalog::TableSchema> =
            vibesql_parser::parse_with_arena_fallback(sql).ok().and_then(|statement| {
                let inner = self.lock();
                match &statement {
                    Statement::Insert(stmt) => {
                        lookup_table_schema(&inner.db, stmt.schema_name.as_deref(), &stmt.table_name)
                            .cloned()
                    }
                    Statement::Update(stmt) => {
                        lookup_table_schema(&inner.db, None, &stmt.table_name).cloned()
                    }
                    _ => None,
                }
            });
        freeze::freeze_statement_with_schema(sql, schema.as_ref())
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

    /// Speculatively read inside an open replicated transaction so the
    /// session observes its **own** buffered (not-yet-committed) writes —
    /// full mid-transaction read-your-own-writes (#5401).
    ///
    /// `entry` carries the session's buffered write statements with the
    /// values they were frozen with at buffer time
    /// ([`freeze_for_propose`](Self::freeze_for_propose)) — *the very same
    /// `TxnEntry` that will be proposed authoritatively at `COMMIT`*. This
    /// replays the frozen statements into a **discardable scratch
    /// transaction** on the leader's applied state (using the same
    /// per-statement path [`apply`](Self::apply) uses, seeded with the
    /// same `commit_ts = last_applied + 1` so MVCC visibility of the
    /// replayed rows matches what the committed entry will produce), runs
    /// `select_sql` against that in-flight state, then **rolls the scratch
    /// transaction back**. Nothing is committed here:
    ///
    /// - The authoritative state change happens exactly once, via the consensus entry at `COMMIT`
    ///   (`propose_entry` → `apply`). The scratch transaction is always rolled back, so this path
    ///   never double-applies.
    /// - Because the replay reuses `entry`'s already-frozen values (not a fresh freeze), a mid-txn
    ///   read of a `random()` / `now()` write sees exactly the value the committed row will carry —
    ///   freeze happens once, at buffer time, and is reused both here and at propose.
    ///
    /// A buffered statement that fails deterministically (e.g. a
    /// constraint violation) surfaces here as a [`ConsensusError`] — the
    /// same failure the client would see at `COMMIT` — leaving the applied
    /// state untouched.
    pub fn speculative_query(
        &self,
        entry: &TxnEntry,
        select_sql: &str,
    ) -> Result<Vec<Vec<SqlValue>>> {
        let select = match vibesql_parser::parse_with_arena_fallback(select_sql)
            .map_err(|e| ConsensusError::Backend(format!("parse error: {e}")))?
        {
            Statement::Select(select) => select,
            other => {
                return Err(ConsensusError::Backend(format!(
                    "speculative_query() only accepts SELECT statements, got: {other:?}"
                )))
            }
        };

        let mut inner = self.lock();
        let scratch_ts = inner.last_applied + 1;

        // Seed + open the scratch transaction exactly as `apply` does, so
        // the replayed rows get the commit_ts they would get if this entry
        // were the next committed one — keeping mid-txn reads consistent
        // with the eventual committed state.
        inner.db.set_next_txn_id(scratch_ts).map_err(|e| {
            ConsensusError::Backend(format!("failed to seed speculative commit_ts: {e}"))
        })?;
        inner.db.begin_transaction().map_err(|e| {
            ConsensusError::Backend(format!("failed to begin speculative txn: {e}"))
        })?;

        // Replay the frozen buffer, then read, capturing any failure so we
        // can always roll the scratch transaction back before returning.
        let result = (|| {
            static NO_FROZEN: &[FrozenValue] = &[];
            for (i, sql) in entry.statements.iter().enumerate() {
                let frozen = entry.frozen.get(i).map(Vec::as_slice).unwrap_or(NO_FROZEN);
                let frozen_defaults =
                    entry.frozen_defaults.get(i).map(Vec::as_slice).unwrap_or(NO_FROZEN);
                execute_write_statement(&mut inner.db, sql, frozen, frozen_defaults).map_err(
                    |e| match e {
                        StatementFailure::Rejected(reason) | StatementFailure::Fatal(reason) => {
                            ConsensusError::Backend(format!(
                                "speculative replay of a buffered statement failed: {reason}"
                            ))
                        }
                    },
                )?;
            }
            vibesql_executor::SelectExecutor::new(&inner.db)
                .execute(&select)
                .map(|rows| rows.into_iter().map(|r| r.values.to_vec()).collect::<Vec<_>>())
                .map_err(|e| {
                    ConsensusError::Backend(format!("speculative query failed: {e}"))
                })
        })();

        // Discard the scratch transaction unconditionally: the
        // authoritative write happens only through the consensus entry at
        // COMMIT. A rollback failure after a successful read is itself
        // fatal bookkeeping — surface it rather than leak an open txn.
        if let Err(rollback_err) = inner.db.rollback_transaction() {
            return Err(ConsensusError::Backend(format!(
                "failed to discard speculative txn: {rollback_err}"
            )));
        }

        result
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
        let db = deserialize_database(&snapshot.data)?;
        self.install_decoded(db, snapshot.last_included_index)
    }

    /// Install an already-decoded database as this machine's state, with
    /// `last_applied` resuming at `last_included_index`. The openraft
    /// mount (`crate::mvcc_raft`) uses this split so a received snapshot
    /// can be fully validated (decoded) **before** it is durably
    /// persisted, and persisted before the machine mutates — a corrupt
    /// stream must neither half-install nor leave a corrupt file behind.
    pub(crate) fn install_decoded(
        &self,
        mut db: Database,
        last_included_index: LogIndex,
    ) -> Result<()> {
        db.set_next_txn_id(last_included_index + 1)
            .map_err(|e| ConsensusError::SnapshotCodec(format!("failed to seed commit_ts: {e}")))?;
        let mut inner = self.lock();
        inner.db = db;
        inner.last_applied = last_included_index;
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

    /// Read-only access to the underlying database for the session-context
    /// guard test (#5392), which asserts the replicated state machine's
    /// write-semantics-affecting session settings sit at their documented
    /// defaults. Available without the `mvcc_enabled` feature (the guard is
    /// about session state, not MVCC stamps).
    #[cfg(test)]
    pub(crate) fn inspect_db<R>(&self, f: impl FnOnce(&Database) -> R) -> R {
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
pub(crate) fn deserialize_database(data: &[u8]) -> Result<Database> {
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

/// One statement's failure inside apply, split by determinism (#5377,
/// judge note on PR #5378).
enum StatementFailure {
    /// Deterministic function of the entry and the replicated state:
    /// every replica fails identically, so the entry is recorded as
    /// [`ApplyOutcome::Rejected`] (and still consumes its index).
    Rejected(String),
    /// Possibly local to this node (resource limits, storage failure,
    /// frozen-entry version skew): surfaced as
    /// [`ConsensusError::FatalApply`] without consuming the index.
    Fatal(String),
}

/// How to record an executor error during apply.
enum FailureClass {
    /// Deterministic by state induction → reject the entry identically
    /// on every replica.
    Deterministic,
    /// May depend on node-local resources/configuration → fatal.
    PossiblyLocal,
}

/// Classify an executor error for the apply path.
///
/// Resource-class failures (storage errors, wall-clock timeouts, and
/// the memory/row/join/recursion guard rails, whose thresholds are
/// node-environment-dependent — e.g. `MAX_MEMORY_BYTES` differs by
/// target) may not reproduce on other replicas, so they must halt the
/// node instead of rejecting the entry. Misclassifying a deterministic
/// error as `PossiblyLocal` is safe (every replica halts identically —
/// an outage, not divergence); the reverse is not, so doubt resolves to
/// `PossiblyLocal`.
fn classify_executor_error(e: &vibesql_executor::ExecutorError) -> FailureClass {
    use vibesql_executor::ExecutorError as E;
    match e {
        E::StorageError(_)
        | E::QueryTimeoutExceeded { .. }
        | E::MemoryLimitExceeded { .. }
        | E::RowLimitExceeded { .. }
        | E::JoinTableLimitExceeded { .. }
        | E::RecursionLimitExceeded { .. } => FailureClass::PossiblyLocal,
        _ => FailureClass::Deterministic,
    }
}

fn run_executor<T>(
    result: std::result::Result<T, vibesql_executor::ExecutorError>,
) -> std::result::Result<T, StatementFailure> {
    result.map_err(|e| match classify_executor_error(&e) {
        FailureClass::Deterministic => StatementFailure::Rejected(e.to_string()),
        FailureClass::PossiblyLocal => StatementFailure::Fatal(e.to_string()),
    })
}

/// Parse and execute one write statement inside the apply transaction,
/// returning its affected-row count.
///
/// Before execution, the statement's non-deterministic call sites are
/// replaced with the entry's frozen values
/// ([`freeze::substitute_statement`]), and the non-deterministic column
/// DEFAULTs it fires are replaced with the entry's frozen DEFAULT
/// values ([`freeze::substitute_volatile_defaults`], #5381). A
/// statement with unfrozen volatile sites — or one firing a volatile
/// DEFAULT without frozen values — is rejected deterministically, so
/// the state machine never evaluates volatile SQL.
///
/// The supported set covers the replicated write surface: DML
/// (INSERT/UPDATE/DELETE) and the core DDL (CREATE/DROP TABLE,
/// CREATE/DROP INDEX, CREATE/DROP VIEW, CREATE/DROP TRIGGER). Reads are
/// not replicated, and transaction control lives at the entry boundary
/// (the whole entry IS the transaction), so SELECT/BEGIN/COMMIT/ROLLBACK
/// inside an entry are rejected — deterministically, on every replica.
fn execute_write_statement(
    db: &mut Database,
    sql: &str,
    frozen: &[FrozenValue],
    frozen_defaults: &[FrozenValue],
) -> std::result::Result<usize, StatementFailure> {
    let statement = vibesql_parser::parse_with_arena_fallback(sql).map_err(|e| {
        StatementFailure::Rejected(format!("parse error in replicated statement: {e}"))
    })?;
    let statement = freeze::substitute_statement(statement, frozen).map_err(|e| match e {
        SubstituteError::UnfrozenVolatile(_) => StatementFailure::Rejected(e.to_string()),
        SubstituteError::FrozenSiteMismatch(_) => StatementFailure::Fatal(e.to_string()),
    })?;

    // Schema-dependent nondeterminism the positional freeze pass cannot
    // see (#5381). The schema is replicated state, so everything below
    // is identical on every replica.
    //
    // 1. Non-deterministic column DEFAULTs the statement fires: splice
    //    in the proposer-frozen values (`frozen_defaults`); reject
    //    deterministically when the sites and values disagree (entries
    //    proposed around / before DEFAULT freezing, or a schema change
    //    between propose and apply).
    let target_schema: Option<vibesql_catalog::TableSchema> = match &statement {
        Statement::Insert(stmt) => {
            lookup_table_schema(db, stmt.schema_name.as_deref(), &stmt.table_name).cloned()
        }
        Statement::Update(stmt) => lookup_table_schema(db, None, &stmt.table_name).cloned(),
        Statement::Delete(stmt) => lookup_table_schema(db, None, &stmt.table_name).cloned(),
        _ => None,
    };
    let can_fire_defaults = matches!(statement, Statement::Insert(_) | Statement::Update(_))
        && target_schema.is_some();
    let statement = if can_fire_defaults {
        let schema = target_schema.as_ref().expect("checked above");
        freeze::substitute_volatile_defaults(statement, schema, frozen_defaults)
            .map_err(StatementFailure::Rejected)?
    } else {
        if !frozen_defaults.is_empty() {
            // Deterministic: every replica resolves the same statement
            // against the same replicated catalog (a missing table here
            // means it was dropped between propose and apply).
            return Err(StatementFailure::Rejected(format!(
                "entry carries {} frozen DEFAULT value(s) but the statement cannot fire a \
                 column DEFAULT against the current replicated schema (table dropped between \
                 propose and apply?); retry the statement (#5381)",
                frozen_defaults.len()
            )));
        }
        statement
    };

    // 2. `CAST(<TIME column> AS TIMESTAMP)` (SQL:1999 stamps the
    //    current date per row at apply time): reject against the
    //    replicated target-table schema.
    if let Some(schema) = &target_schema {
        if let Some(reason) = freeze::time_cast_violation(&statement, schema) {
            return Err(StatementFailure::Rejected(reason));
        }
    }

    // 2b. The #5398 residual: TIME→TIMESTAMP casts whose operand is not
    //     the target-table TIME column — an other-table column or a
    //     computed TIME expression, in a SET/WHERE root or inside an
    //     `INSERT … SELECT` body / subquery. The target-schema audit in
    //     (2) only resolves the target table and does not walk query
    //     bodies; this conservative, schema-free backstop rejects any
    //     such cast not provably non-TIME, on every replica alike. (A
    //     properly frozen entry never reaches here with a live cast —
    //     literal TIME operands substitute to a TIMESTAMP literal.)
    if let Some(reason) = freeze::time_cast_query_violation(&statement) {
        return Err(StatementFailure::Rejected(reason));
    }

    // 3. Trigger-body backstop (#5381): this DML may fire triggers
    //    whose bodies were audited at CREATE time against a schema that
    //    later DDL may have invalidated (drop + recreate with a
    //    volatile DEFAULT); re-audit them — transitively through
    //    trigger cascades — against the current replicated catalog.
    let trigger_target: Option<&str> = match &statement {
        Statement::Insert(stmt) => Some(&stmt.table_name),
        Statement::Update(stmt) => Some(&stmt.table_name),
        Statement::Delete(stmt) => Some(&stmt.table_name),
        _ => None,
    };
    if let Some(table_name) = trigger_target {
        if let Some(reason) = freeze::dml_trigger_violation(db, table_name) {
            return Err(StatementFailure::Rejected(reason));
        }
    }

    match &statement {
        Statement::Insert(stmt) => {
            run_executor(vibesql_executor::InsertExecutor::execute(db, stmt))
        }
        Statement::Update(stmt) => {
            run_executor(vibesql_executor::UpdateExecutor::execute(stmt, db))
        }
        Statement::Delete(stmt) => {
            run_executor(vibesql_executor::DeleteExecutor::execute(stmt, db))
        }
        Statement::CreateTable(stmt) => {
            run_executor(vibesql_executor::CreateTableExecutor::execute(stmt, db).map(|_| 0))
        }
        Statement::CreateIndex(stmt) => {
            run_executor(vibesql_executor::CreateIndexExecutor::execute(stmt, db).map(|_| 0))
        }
        Statement::CreateView(stmt) => run_executor(
            vibesql_executor::advanced_objects::execute_create_view(stmt, db).map(|_| 0),
        ),
        Statement::DropTable(stmt) => {
            run_executor(vibesql_executor::DropTableExecutor::execute(stmt, db).map(|_| 0))
        }
        Statement::DropIndex(stmt) => {
            run_executor(vibesql_executor::DropIndexExecutor::execute(stmt, db).map(|_| 0))
        }
        Statement::DropView(stmt) => {
            run_executor(vibesql_executor::advanced_objects::execute_drop_view(stmt, db).map(|_| 0))
        }
        Statement::CreateTrigger(stmt) => {
            // The static volatility validation already ran (inside
            // `substitute_statement`); the schema-dependent body audit
            // (volatile DEFAULTs / TIME casts fired by body DML) needs
            // the replicated catalog and runs here (#5381).
            if let Some(reason) = freeze::trigger_body_schema_violation(stmt, db) {
                return Err(StatementFailure::Rejected(reason));
            }
            // Preserve the original SQL text so the trigger survives
            // SQL-dump persistence round-trips.
            run_executor(
                vibesql_executor::TriggerExecutor::create_trigger_with_sql(db, stmt, Some(sql))
                    .map(|_| 0),
            )
        }
        Statement::DropTrigger(stmt) => {
            run_executor(vibesql_executor::TriggerExecutor::drop_trigger(db, stmt).map(|_| 0))
        }
        Statement::BeginTransaction(_) | Statement::Commit(_) | Statement::Rollback(_) => {
            Err(StatementFailure::Rejected(
                "transaction control is not allowed inside a replicated entry: the entry itself \
                 is the transaction (one entry per committed transaction)"
                    .to_string(),
            ))
        }
        Statement::Select(_) => Err(StatementFailure::Rejected(
            "SELECT is not allowed inside a replicated entry: reads are local, not replicated"
                .to_string(),
        )),
        other => Err(StatementFailure::Rejected(format!(
            "statement is not supported in a replicated entry (Raft Phase B1, PR 1): {other:?}"
        ))),
    }
}

/// Best-effort schema lookup for the DEFAULT audit. Tries the
/// schema-qualified storage name first, then the bare table name; a
/// miss falls through to the executor, which fails (or resolves)
/// deterministically.
fn lookup_table_schema<'a>(
    db: &'a Database,
    schema_name: Option<&str>,
    table_name: &str,
) -> Option<&'a vibesql_catalog::TableSchema> {
    if let Some(schema_name) = schema_name {
        if let Some(table) = db.get_table(&format!("{schema_name}.{table_name}")) {
            return Some(&table.schema);
        }
    }
    db.get_table(table_name).map(|t| &t.schema)
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

        let entry = TxnEntry::frozen_batch(
            vec!["INSERT INTO t VALUES (random())".to_string()],
            vec![vec![FrozenValue::Integer(7)]],
        );
        let bytes = serde_json::to_vec(&entry).unwrap();
        assert_eq!(serde_json::from_slice::<TxnEntry>(&bytes).unwrap(), entry);

        let entry = TxnEntry::single("INSERT INTO t VALUES (1)").stamp_leader_time(1_700_000_000);
        let bytes = serde_json::to_vec(&entry).unwrap();
        assert_eq!(serde_json::from_slice::<TxnEntry>(&bytes).unwrap(), entry);
    }

    /// Entries encoded before #5377 (no `frozen` field) / #5200 (no
    /// `leader_time_ms` field) still decode: unknown staleness, no
    /// frozen sites.
    #[test]
    fn entry_decode_without_frozen_field_is_backward_compatible() {
        let json = br#"{"statements":["INSERT INTO t VALUES (1)"]}"#;
        let entry: TxnEntry = serde_json::from_slice(json).unwrap();
        assert_eq!(entry.statements.len(), 1);
        assert!(entry.frozen.is_empty());
        assert_eq!(entry.leader_time_ms, None);
    }

    /// An unstamped entry keeps the pre-#5200 byte encoding (the stamp
    /// field is skipped, not serialized as null), so proposers that do
    /// not stamp emit entries old appliers decode unchanged.
    #[test]
    fn unstamped_entry_keeps_pre_5200_encoding() {
        let entry = TxnEntry::single("INSERT INTO t VALUES (1)");
        let json = serde_json::to_string(&entry).unwrap();
        assert!(
            !json.contains("leader_time_ms"),
            "unstamped entries must not mention the stamp field, got: {json}"
        );
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

    /// Speculative reads (#5401) see the buffer's own writes but never
    /// commit them: after a `speculative_query`, the applied state is
    /// unchanged (no double-apply), and the entry can still be applied
    /// authoritatively to land the same rows.
    #[test]
    fn speculative_query_sees_buffer_without_applying_it() {
        let machine = VibesqlStateMachine::new();
        create_users(&machine, 1);
        machine.apply(2, &TxnEntry::single("INSERT INTO users VALUES (1, 'alice')")).unwrap();

        let buffer = TxnEntry::batch([
            "INSERT INTO users VALUES (2, 'bob')",
            "INSERT INTO users VALUES (3, 'carol')",
        ]);

        // The speculative read sees the committed row plus the buffered ones.
        let rows = machine.speculative_query(&buffer, "SELECT name FROM users ORDER BY id").unwrap();
        let seen: Vec<String> = rows.into_iter().map(|r| r[0].to_string()).collect();
        assert_eq!(seen, vec!["alice", "bob", "carol"], "read-your-own-writes");

        // Nothing was committed: applied state still has only the committed
        // row, and last_applied is unchanged (no double-apply).
        assert_eq!(names(&machine), vec!["alice"], "speculative read must not commit");
        assert_eq!(machine.last_applied(), 2);

        // The same buffer, applied authoritatively, lands exactly those rows.
        machine.apply(3, &buffer).unwrap();
        assert_eq!(names(&machine), vec!["alice", "bob", "carol"]);
        assert_eq!(machine.last_applied(), 3);
    }

    /// A buffered statement that fails deterministically surfaces at
    /// speculative read time (the same failure the client gets at COMMIT)
    /// and leaves the applied state untouched.
    #[test]
    fn speculative_query_surfaces_buffer_failure_and_rolls_back() {
        let machine = VibesqlStateMachine::new();
        create_users(&machine, 1);
        machine.apply(2, &TxnEntry::single("INSERT INTO users VALUES (1, 'alice')")).unwrap();

        // Duplicate primary key inside the buffer.
        let buffer = TxnEntry::batch(["INSERT INTO users VALUES (1, 'dup')"]);
        let err =
            machine.speculative_query(&buffer, "SELECT name FROM users").unwrap_err();
        assert!(format!("{err}").contains("speculative replay"), "unexpected error: {err}");

        // Applied state untouched; no open transaction left behind.
        assert_eq!(names(&machine), vec!["alice"]);
        assert_eq!(machine.last_applied(), 2);
        // A subsequent apply still works (the scratch txn was rolled back).
        machine.apply(3, &TxnEntry::single("INSERT INTO users VALUES (2, 'bob')")).unwrap();
        assert_eq!(names(&machine), vec!["alice", "bob"]);
    }

    /// Regression for #5413: a speculative read of a buffered PK insert
    /// must leave the leader's PK/UNIQUE index bit-for-bit identical to
    /// before the read. Previously the scratch transaction's rollback
    /// restored `catalog`/`tables` but **not** the `IndexManager`, so the
    /// replayed PK key survived rollback and a later legitimate apply
    /// reusing that PK was wrongly `Rejected` cluster-wide.
    #[test]
    fn speculative_query_leaves_pk_index_clean_for_later_apply() {
        let machine = VibesqlStateMachine::new();
        create_users(&machine, 1);

        // Buffer a brand-new PK (id = 2) and read through the speculative
        // path. The read sees its own write...
        let buffer = TxnEntry::batch(["INSERT INTO users VALUES (2, 'spec')"]);
        let rows =
            machine.speculative_query(&buffer, "SELECT name FROM users ORDER BY id").unwrap();
        let seen: Vec<String> = rows.into_iter().map(|r| r[0].to_string()).collect();
        assert_eq!(seen, vec!["spec"], "RYW: speculative read sees the buffered PK");

        // ...but committed state stays empty and last_applied is unchanged.
        assert!(names(&machine).is_empty(), "speculative read must not commit");
        assert_eq!(machine.last_applied(), 1);

        // A FRESH entry committing the same PK must now land `Applied` —
        // the rolled-back speculative key must not leak into the index and
        // trigger a spurious UNIQUE rejection.
        let outcome =
            machine.apply(2, &TxnEntry::single("INSERT INTO users VALUES (2, 'real')")).unwrap();
        assert_eq!(
            outcome,
            ApplyOutcome::Applied { rows_affected: 1 },
            "fresh PK reuse after a rolled-back speculative read must apply, not reject"
        );
        assert_eq!(names(&machine), vec!["real"]);
        assert_eq!(machine.last_applied(), 2);
    }

    /// Regression for #5413: read-your-own-writes must be idempotent.
    /// Two consecutive speculative reads after one buffered PK insert must
    /// both succeed and return the same single row. Previously the first
    /// read's scratch INSERT key leaked into the index, so the second
    /// read's replay failed its own `UNIQUE constraint` check.
    #[test]
    fn two_speculative_reads_of_one_buffered_pk_are_idempotent() {
        let machine = VibesqlStateMachine::new();
        create_users(&machine, 1);

        let buffer = TxnEntry::batch(["INSERT INTO users VALUES (2, 'spec')"]);

        let first =
            machine.speculative_query(&buffer, "SELECT name FROM users ORDER BY id").unwrap();
        let first_seen: Vec<String> = first.into_iter().map(|r| r[0].to_string()).collect();
        assert_eq!(first_seen, vec!["spec"], "first speculative read");

        // The second read replays the same buffer into a fresh scratch txn;
        // it must not see a stale key from the first read's rollback.
        let second =
            machine.speculative_query(&buffer, "SELECT name FROM users ORDER BY id").unwrap();
        let second_seen: Vec<String> = second.into_iter().map(|r| r[0].to_string()).collect();
        assert_eq!(second_seen, vec!["spec"], "second speculative read must match the first");

        assert!(names(&machine).is_empty(), "neither speculative read commits");
        assert_eq!(machine.last_applied(), 1);
    }

    /// #5419: a speculative read whose buffer contains no index-mutating
    /// statements must NOT deep-clone the `Operations` (IndexManager)
    /// rollback snapshot. Before #5419 the snapshot was eager, so the
    /// scratch transaction every `speculative_query` opens cloned the
    /// entire index manager per read — O(index-keys) per mid-txn read on a
    /// replicated session. With copy-on-write the read-only scratch txn
    /// triggers zero clones. Asserted deterministically via the clone
    /// counter (no timing).
    #[test]
    fn read_only_speculative_query_does_not_clone_operations_snapshot() {
        let machine = VibesqlStateMachine::new();
        // Seed a committed row so the index manager is non-empty (an eager
        // clone would be observable work).
        create_users(&machine, 1);
        machine.apply(2, &TxnEntry::single("INSERT INTO users VALUES (1, 'alice')")).unwrap();

        let before = machine.inspect_db(|db| db.operations_snapshot_clones());

        // Empty buffer => the scratch transaction only reads; it never
        // mutates an index, so the COW snapshot is never taken.
        let empty: [&str; 0] = [];
        let rows = machine
            .speculative_query(&TxnEntry::batch(empty), "SELECT name FROM users ORDER BY id")
            .unwrap();
        let seen: Vec<String> = rows.into_iter().map(|r| r[0].to_string()).collect();
        assert_eq!(seen, vec!["alice"], "read-only speculative read sees committed state");

        let after = machine.inspect_db(|db| db.operations_snapshot_clones());
        assert_eq!(
            after, before,
            "a read-only speculative query must not deep-clone the Operations snapshot (#5419)"
        );
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
        assert_eq!(machine.apply(2, &entry).unwrap(), ApplyOutcome::Applied { rows_affected: 1 });

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

    /// Defense in depth (#5377): an entry containing an unfrozen
    /// non-deterministic call site is *rejected* — deterministically,
    /// with the same outcome on a second machine — instead of being
    /// evaluated at apply time (which would diverge replicas).
    #[test]
    fn unfrozen_volatile_entries_are_rejected_identically_everywhere() {
        let entries = [
            TxnEntry::single("CREATE TABLE t (id INTEGER PRIMARY KEY, r INTEGER)"),
            TxnEntry::single("INSERT INTO t VALUES (1, random())"),
            TxnEntry::single("INSERT INTO t SELECT id + 1, random() FROM t"),
            TxnEntry::single("INSERT INTO t VALUES (2, last_insert_rowid())"),
            TxnEntry::single("INSERT INTO t VALUES (3, 30)"),
        ];
        let a = VibesqlStateMachine::new();
        let b = VibesqlStateMachine::new();
        for (i, entry) in entries.iter().enumerate() {
            let index = (i + 1) as LogIndex;
            let outcome_a = a.apply(index, entry).unwrap();
            let outcome_b = b.apply(index, entry).unwrap();
            assert_eq!(outcome_a, outcome_b, "entry {index} must apply identically");
        }
        for (index, rejected) in [(2, true), (3, true), (4, true), (5, false)] {
            // (replay onto a third machine to inspect each outcome)
            let c = VibesqlStateMachine::new();
            let mut outcome = ApplyOutcome::AlreadyApplied;
            for i in 1..=index {
                outcome = c.apply(i as LogIndex, &entries[i - 1]).unwrap();
            }
            if rejected {
                assert!(
                    matches!(&outcome, ApplyOutcome::Rejected { reason }
                        if reason.contains("frozen") || reason.contains("session state")
                            || reason.contains("non-deterministic")),
                    "entry {index} must be rejected as unfrozen, got: {outcome:?}"
                );
            } else {
                assert_eq!(outcome, ApplyOutcome::Applied { rows_affected: 1 });
            }
        }
        assert_eq!(a.query("SELECT r FROM t").unwrap(), vec![vec![SqlValue::Integer(30)]]);
    }

    /// A frozen-value/site-count mismatch indicates proposer/applier
    /// version skew: fatal ([`ConsensusError::FatalApply`]), and the
    /// entry's index is NOT consumed — the node halts rather than
    /// recording an outcome other replicas may not reproduce.
    #[test]
    fn frozen_count_mismatch_is_fatal_and_does_not_consume_the_index() {
        let machine = VibesqlStateMachine::new();
        machine
            .apply(1, &TxnEntry::single("CREATE TABLE t (id INTEGER PRIMARY KEY, r INTEGER)"))
            .unwrap();

        let statements = vec!["INSERT INTO t VALUES (1, random())".to_string()];
        let skewed = TxnEntry::frozen_batch(
            statements.clone(),
            vec![vec![FrozenValue::Integer(7), FrozenValue::Integer(8)]],
        );
        let err = machine.apply(2, &skewed).unwrap_err();
        assert!(matches!(err, ConsensusError::FatalApply(_)), "got: {err}");
        assert_eq!(machine.last_applied(), 1, "a fatal apply must not consume the index");

        // After "recovery", the correctly-frozen entry applies at the
        // same index, and the stored row equals the frozen value.
        let frozen = TxnEntry::frozen_batch(statements, vec![vec![FrozenValue::Integer(7)]]);
        assert_eq!(machine.apply(2, &frozen).unwrap(), ApplyOutcome::Applied { rows_affected: 1 });
        assert_eq!(
            machine.query("SELECT r FROM t WHERE id = 1").unwrap(),
            vec![vec![SqlValue::Integer(7)]]
        );
    }

    /// A non-deterministic column DEFAULT (`DEFAULT CURRENT_TIMESTAMP`)
    /// is fine as DDL, but an INSERT that would *fire* it at apply time
    /// is rejected deterministically; supplying the value explicitly
    /// applies normally.
    #[test]
    fn inserts_firing_a_volatile_default_are_rejected() {
        let machine = VibesqlStateMachine::new();
        let outcome = machine
            .apply(
                1,
                &TxnEntry::single(
                    "CREATE TABLE events (id INTEGER PRIMARY KEY, \
                     ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
                ),
            )
            .unwrap();
        assert_eq!(outcome, ApplyOutcome::Applied { rows_affected: 0 });

        let outcome =
            machine.apply(2, &TxnEntry::single("INSERT INTO events (id) VALUES (1)")).unwrap();
        assert!(
            matches!(&outcome, ApplyOutcome::Rejected { reason } if reason.contains("DEFAULT")),
            "firing a volatile default must reject the entry, got: {outcome:?}"
        );

        let outcome = machine
            .apply(3, &TxnEntry::single("INSERT INTO events VALUES (1, '2026-06-11 12:00:00')"))
            .unwrap();
        assert_eq!(outcome, ApplyOutcome::Applied { rows_affected: 1 });
    }

    /// The PR #5382 review probes: unfrozen `curdate()`/`curtime()`/
    /// 1-arg `age()` entries previously slipped past both the freeze
    /// pass and the apply-side validation (the volatility list missed
    /// them) and were applied with the apply-time wall clock. They must
    /// now be rejected — identically on a second machine.
    #[test]
    fn unfrozen_clock_alias_entries_are_rejected_not_applied() {
        let a = VibesqlStateMachine::new();
        let b = VibesqlStateMachine::new();
        let ddl = TxnEntry::single("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)");
        a.apply(1, &ddl).unwrap();
        b.apply(1, &ddl).unwrap();
        for (i, sql) in [
            "INSERT INTO t VALUES (1, curdate())",
            "INSERT INTO t VALUES (2, curtime())",
            "INSERT INTO t VALUES (3, age(DATE '2020-01-02'))",
            "INSERT INTO t VALUES (4, CAST(TIME '12:34:56' AS TIMESTAMP))",
        ]
        .iter()
        .enumerate()
        {
            let entry = TxnEntry::single(*sql);
            let index = (i + 2) as LogIndex;
            let outcome = a.apply(index, &entry).unwrap();
            assert!(
                matches!(&outcome, ApplyOutcome::Rejected { reason }
                    if reason.contains("frozen") || reason.contains("non-deterministic")),
                "{sql} must be rejected as unfrozen, got: {outcome:?}"
            );
            assert_eq!(outcome, b.apply(index, &entry).unwrap(), "{sql} must reject identically");
        }
        assert!(a.query("SELECT * FROM t").unwrap().is_empty(), "nothing may have applied");
    }

    /// `CAST(<TIME column> AS TIMESTAMP)` stamps the current date per
    /// row at apply time (SQL:1999) — invisible to the positional freeze
    /// pass, so the apply path rejects it against the replicated schema
    /// (like the DEFAULT audit). Deterministic casts keep working.
    #[test]
    fn time_column_casts_to_timestamp_are_rejected() {
        let machine = VibesqlStateMachine::new();
        machine
            .apply(
                1,
                &TxnEntry::single("CREATE TABLE sched (id INTEGER PRIMARY KEY, t TIME, s TEXT)"),
            )
            .unwrap();
        machine
            .apply(2, &TxnEntry::single("INSERT INTO sched VALUES (1, '12:34:56', 'x')"))
            .unwrap();

        for (index, sql) in [
            (3, "UPDATE sched SET s = CAST(t AS TIMESTAMP) WHERE id = 1"),
            (4, "DELETE FROM sched WHERE CAST(t AS TIMESTAMP) < '2026-01-01 00:00:00'"),
        ] {
            let outcome = machine.apply(index, &TxnEntry::single(sql)).unwrap();
            assert!(
                matches!(&outcome, ApplyOutcome::Rejected { reason }
                    if reason.contains("TIME column")),
                "{sql} must be rejected, got: {outcome:?}"
            );
        }

        // A deterministic cast of a TEXT column still applies.
        let outcome = machine
            .apply(5, &TxnEntry::single("UPDATE sched SET s = CAST(id AS TEXT) WHERE id = 1"))
            .unwrap();
        assert_eq!(outcome, ApplyOutcome::Applied { rows_affected: 1 });
    }

    /// The #5398 residual: `CAST(<TIME> AS TIMESTAMP)` over an operand
    /// that is neither a literal nor the *target* table's TIME column —
    /// an other-table TIME column or a computed TIME expression. Before,
    /// these slipped past both the literal-only propose freeze and the
    /// target-schema apply audit and silently stamped the apply-time
    /// date, diverging across replicas. Now they are rejected
    /// deterministically — identically on a second machine (the
    /// acceptance criterion's convergence check).
    #[test]
    fn residual_time_casts_are_rejected_identically_on_every_node() {
        let a = VibesqlStateMachine::new();
        let b = VibesqlStateMachine::new();
        let setup = [
            (1, "CREATE TABLE sched (id INTEGER PRIMARY KEY, t TIME, s TEXT)"),
            (2, "CREATE TABLE src (id INTEGER PRIMARY KEY, t TIME)"),
            (3, "INSERT INTO sched VALUES (1, '12:34:56', 'x')"),
            (4, "INSERT INTO src VALUES (1, '09:00:00')"),
        ];
        for (i, sql) in setup {
            let entry = TxnEntry::single(sql);
            assert_eq!(a.apply(i, &entry).unwrap(), b.apply(i, &entry).unwrap(), "{sql}");
        }

        // The residual cases, each a per-row apply-time clock read.
        for (i, sql) in [
            // Other-table TIME column inside an UPDATE … FROM-style SET
            // (caught by the apply-side type guard).
            (5, "UPDATE sched SET s = CAST(src.t AS TIMESTAMP) WHERE id = 1"),
            // Computed TIME expression in a SET.
            (6, "UPDATE sched SET s = CAST(time(t) AS TIMESTAMP) WHERE id = 1"),
            // Other-table TIME column inside INSERT … SELECT (caught by
            // the propose-side query-bearing guard; rejected at apply when
            // the entry was crafted without a freeze pass, as here).
            (7, "INSERT INTO sched (id, s) SELECT 2, CAST(src.t AS TIMESTAMP) FROM src"),
        ] {
            let entry = TxnEntry::single(sql);
            let outcome = a.apply(i, &entry).unwrap();
            assert!(
                matches!(&outcome, ApplyOutcome::Rejected { .. }),
                "{sql} must be rejected as a residual TIME cast, got: {outcome:?}"
            );
            assert_eq!(outcome, b.apply(i, &entry).unwrap(), "{sql} must reject identically");
        }

        // Convergence: both machines applied exactly the setup rows and
        // nothing from the rejected residual entries.
        let rows_a = a.query("SELECT * FROM sched").unwrap();
        assert_eq!(rows_a, b.query("SELECT * FROM sched").unwrap());
        assert_eq!(rows_a.len(), 1, "only the single setup row may remain; no residual cast applied");
    }

    /// The deterministic-vs-resource error split (judge note on PR
    /// #5378): resource-class executor errors halt the node; logic
    /// errors reject the entry.
    #[test]
    fn executor_error_classification_splits_resource_from_deterministic() {
        use vibesql_executor::ExecutorError as E;
        let possibly_local = [
            E::StorageError("disk error".to_string()),
            E::QueryTimeoutExceeded { elapsed_seconds: 301, max_seconds: 300 },
            E::RowLimitExceeded { rows_processed: 11, max_rows: 10 },
            E::MemoryLimitExceeded { used_bytes: 11, max_bytes: 10 },
            E::JoinTableLimitExceeded { table_count: 65, max_tables: 64 },
            E::RecursionLimitExceeded {
                message: "too deep".to_string(),
                call_stack: vec![],
                max_depth: 100,
            },
        ];
        for e in &possibly_local {
            assert!(
                matches!(classify_executor_error(e), FailureClass::PossiblyLocal),
                "{e:?} must be classified as possibly local (fatal)"
            );
        }
        let deterministic = [
            E::ConstraintViolation("UNIQUE constraint failed".to_string()),
            E::TableNotFound("t".to_string()),
            E::DivisionByZero,
            E::IntegerOverflow,
        ];
        for e in &deterministic {
            assert!(
                matches!(classify_executor_error(e), FailureClass::Deterministic),
                "{e:?} must be classified as deterministic (reject)"
            );
        }
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
            let carol = table.scan().iter().find(|r| r.values[0] == SqlValue::Integer(3)).unwrap();
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

    // -----------------------------------------------------------------------
    // Session-context capture guard (#5392)
    // -----------------------------------------------------------------------
    //
    // The replicated state machine executes every entry against a bare
    // `Database` with no session context (module docs, "Session context").
    // That is only safe while every session-scoped setting that could change
    // a *write*'s semantics sits at the same fixed default on every replica.
    //
    // The audit (#5392) enumerated the write-semantics-affecting settings the
    // storage `Database` carries — `vibesql-storage::database::session`:
    //
    //   - `foreign_keys_enabled`  (whether FK constraints are enforced on DML)
    //   - `defer_foreign_keys`    (whether FK checks defer to COMMIT)
    //   - `case_sensitive_like`   (changes which rows a LIKE predicate selects,
    //                              so it changes UPDATE/DELETE *effects*, not
    //                              just SELECT presentation)
    //   - `reverse_unordered_selects`
    //                             (reverses the output order of an unordered
    //                              SELECT — so an `INSERT ... SELECT` with no
    //                              `ORDER BY` materializes rows in the opposite
    //                              order and assigns rowids accordingly; a write
    //                              effect, not just SELECT presentation)
    //   - `sql_mode`              (MySQL vs SQLite type coercion / division /
    //                              REPLACE semantics)
    //   - generic `session_variables` read by SQL functions
    //   - role / security enforcement (`is_security_enabled`)
    //
    // `full_column_names` / `short_column_names` were considered and *excluded*:
    // they are read only by SELECT column-naming (`select::executor::columns`),
    // never by any DML/DDL/SELECT-materialization path, so they affect result
    // presentation only — never a write's effects.
    //
    // and concluded all are write-semantics-neutral *as deployed* because the
    // replicated surface cannot mutate them: PRAGMA / SET / GRANT statements
    // are rejected by `execute_write_statement`'s dispatch, and snapshots
    // serialize only catalog + data (not session vars), so a recovering
    // replica's machine also boots at defaults. The two tests below are the
    // regression guard: they fail loudly if a future change either (a) shifts
    // the state machine's default session state, or (b) lets a session-state
    // mutation (PRAGMA/SET) enter the replicated write surface unmaterialized.

    /// The state machine's database boots at the documented write-semantics
    /// defaults. If a future change moves any of these off-default, the
    /// replicated-determinism implications must be re-audited (and the
    /// setting either captured into `TxnEntry` or kept off the machine).
    #[test]
    fn state_machine_session_settings_are_at_safe_write_semantics_defaults() {
        let machine = VibesqlStateMachine::new();
        machine.inspect_db(|db| {
            assert!(
                !db.foreign_keys_enabled(),
                "FK enforcement must default OFF on the replicated machine (#5392)"
            );
            assert!(
                !db.defer_foreign_keys(),
                "FK deferral must default OFF on the replicated machine (#5392)"
            );
            assert!(
                !db.case_sensitive_like(),
                "case_sensitive_like must default OFF on the replicated machine (#5392)"
            );
            assert!(
                !db.reverse_unordered_selects(),
                "reverse_unordered_selects must default OFF on the replicated machine (#5392)"
            );
            assert!(
                matches!(db.sql_mode(), vibesql_types::SqlMode::MySQL { .. }),
                "sql_mode must default to MySQL on the replicated machine (#5392); got {:?}",
                db.sql_mode()
            );
        });

        // A snapshot roundtrip must not be able to smuggle non-default
        // session state into a recovering replica (snapshots serialize
        // catalog + data only).
        let snapshot = machine.snapshot().unwrap();
        let restored = VibesqlStateMachine::from_snapshot(&snapshot).unwrap();
        restored.inspect_db(|db| {
            assert!(!db.foreign_keys_enabled());
            assert!(!db.defer_foreign_keys());
            assert!(!db.case_sensitive_like());
            assert!(!db.reverse_unordered_selects());
            assert!(matches!(db.sql_mode(), vibesql_types::SqlMode::MySQL { .. }));
        });
    }

    /// Session-state mutations must never reach the replicated executor
    /// unmaterialized: a PRAGMA or SET inside an entry is rejected (so it
    /// cannot flip `foreign_keys` / `case_sensitive_like` / `sql_mode` on the
    /// state machine), and — proving the guard bites — the setting it would
    /// have changed is observably untouched afterward.
    #[test]
    fn session_state_mutations_are_rejected_at_the_replicated_boundary() {
        let machine = VibesqlStateMachine::new();
        create_users(&machine, 1);

        let mut index = 1;
        for sql in [
            "PRAGMA foreign_keys = ON",
            "PRAGMA case_sensitive_like = ON",
            "SET sql_mode = 'SQLITE'",
            "SET foreign_keys = 1",
            "SET @my_var = 7",
        ] {
            index += 1;
            let outcome = machine.apply(index, &TxnEntry::single(sql)).unwrap();
            assert!(
                matches!(outcome, ApplyOutcome::Rejected { .. }),
                "session-state mutation `{sql}` must be rejected at the replicated boundary, \
                 not silently applied against the state machine's session (#5392); got: \
                 {outcome:?}"
            );
            // The rejected entry consumed its index (every replica rejects it
            // identically) but must not have moved the session setting.
            machine.inspect_db(|db| {
                assert!(!db.foreign_keys_enabled(), "{sql} must not enable FK enforcement");
                assert!(!db.case_sensitive_like(), "{sql} must not flip case_sensitive_like");
                assert!(
                    matches!(db.sql_mode(), vibesql_types::SqlMode::MySQL { .. }),
                    "{sql} must not change sql_mode"
                );
            });
        }
        assert_eq!(machine.last_applied(), index);
    }
}
