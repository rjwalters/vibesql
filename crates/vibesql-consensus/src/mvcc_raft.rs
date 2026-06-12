//! The MVCC state machine mounted inside openraft, plus [`MvccRaftNode`]:
//! a cluster member whose replicated writes apply to a real database
//! (Raft Phase B1, #5199, PR 2 of 2).
//!
//! PR 1 built [`VibesqlStateMachine`] — apply, idempotence, commit_ts =
//! log index, the MVCC snapshot codec, and the vacuum-horizon pin — and
//! drove it *outside* the engine through `ReplicatedDb::catch_up_to`.
//! This module mounts that machine **inside** openraft as a real
//! [`RaftStateMachine`], so apply happens in the engine's state-machine
//! worker on every voter: a leader's `client_write` resolves only after
//! the entry is committed on a quorum and applied locally, and followers
//! apply the same entries in the same order without any external
//! catch-up loop.
//!
//! # Index mapping
//!
//! openraft's raw log interleaves application entries with protocol
//! entries (membership, new-leader blanks). [`VibesqlStateMachine`]
//! numbers **application entries only** (dense, 1-based — the indices
//! its commit timestamps are derived from), so [`MvccStateMachine`]
//! tracks both: the raw raft `last_applied` (what openraft resumes
//! from) and the machine's dense index (computed as
//! `machine.last_applied() + 1` for each `Normal` entry). Protocol
//! entries advance only the raw cursor, never the dense one — which is
//! exactly why dense indices (and therefore MVCC `xmin` stamps) are
//! identical on every replica even across leader changes.
//!
//! # Fatal apply = node halt
//!
//! [`ConsensusError::FatalApply`] documents the PR 2 obligation: a
//! possibly-node-local apply failure (resource exhaustion, storage
//! failure, frozen-entry version skew — see `crate::state_machine`)
//! must **halt the node**; recording it as a rejection would silently
//! diverge replicas, and acknowledging it would let openraft advance
//! `last_applied` past an entry whose effects this node does not have.
//! The mount discharges that obligation by returning a [`StorageError`]
//! from [`RaftStateMachine::apply`]: openraft treats storage errors as
//! fatal — the core shuts down without acknowledging the entry, so
//! neither the engine's `last_applied` nor the machine's advances. The
//! reason is retained on the node ([`MvccRaftNode::fatal_reason`]) and
//! embedded in the error openraft logs and returns to in-flight
//! writes. Recovery is operational: restart the node and let it resync
//! via snapshot install + log replay (or fix the version skew the
//! fatal error reported).
//!
//! # Leader-only writes
//!
//! Proposals are accepted only on the leader. A follower (or candidate)
//! fails fast with [`ConsensusError::NotLeader`] carrying its best
//! guess at the leader id, *before* evaluating any nondeterministic
//! expression; even without the fast path, openraft's `client_write`
//! rejects non-leader writes with `ForwardToLeader`, which maps to the
//! same error. Freeze-at-propose (#5377) therefore always runs on the
//! leader: [`MvccRaftNode::execute_replicated_txn`] freezes volatile
//! call sites into the entry before `client_write`, and every replica
//! applies the identical frozen entry.
//!
//! # Snapshots
//!
//! The engine-level snapshot payload is an 8-byte LE **dense index
//! header** followed by the machine's binary database snapshot (the
//! vbsql codec from PR 1). The header carries the dense application
//! index across the wire because openraft's `SnapshotMeta` only knows
//! raw raft log ids; install seeds the machine's dense cursor from it.
//! Builds pin the MVCC vacuum horizon for their whole duration
//! (`VibesqlStateMachine::horizon_pin` → `Database::pin_gc_horizon`),
//! and on durable configurations the blob is persisted via the Phase A4
//! [`SnapshotStore`] **before** it is registered or acknowledged — the
//! same crash-safety order as the echo machine, including the deferred
//! post-install log purge (`DurableLogStore::purge_compacted`).
//!
//! # Read paths (Raft Phase B2, #5200)
//!
//! Four read modes, one guarantee each:
//!
//! - [`MvccRaftNode::query`] — **local, stale-allowed**: runs against
//!   whatever this node has applied, with no leadership check or network
//!   round. Available on every node — including a leader partitioned
//!   into a minority — and may therefore trail the cluster's committed
//!   state arbitrarily.
//! - [`MvccRaftNode::query_linearizable`] — **linearizable**: confirms
//!   this node's leadership through openraft's ReadIndex protocol
//!   ([`Raft::ensure_linearizable`]: one empty-AppendEntries heartbeat
//!   round acknowledged by a quorum, then a wait until the local state
//!   machine reaches the confirmed read index) before running the same
//!   local query. A node that knows it is not the leader fails with
//!   [`ConsensusError::NotLeader`] and a leader hint; a node that still
//!   believes it leads but cannot confirm a quorum (partitioned into a
//!   minority, deposed but not yet aware) fails with
//!   [`ConsensusError::NotLeader`] carrying **no** hint — its own view
//!   is exactly what could not be confirmed, so hinting at itself would
//!   route callers straight back to a possibly-stale node.
//! - [`MvccRaftNode::query_at_least`] — **read-your-writes** (PR 2 of 2,
//!   clock-free): a session carries the **dense application index** its
//!   last write's propose returned as a token; any node — leader or
//!   follower — serves the read once its locally applied dense index
//!   reaches the token (waiting up to the caller's bound, then failing
//!   with [`ConsensusError::ReadTimeout`]). Dense indices are identical
//!   on every replica (see *Index mapping* above) and
//!   [`execute_replicated`](MvccRaftNode::execute_replicated) returns
//!   exactly the dense index its entry consumed, so a token minted by a
//!   leader propose compares like-with-like against any follower's
//!   applied cursor. Exact, no clocks involved.
//! - [`MvccRaftNode::query_bounded_staleness`] — **bounded staleness**
//!   (PR 2 of 2): serves locally iff this node can prove its applied
//!   state is no staler than the caller's bound, refusing otherwise
//!   with [`ConsensusError::StalenessExceeded`] (route to the leader or
//!   retry with a larger bound). A bound of **zero** delegates to
//!   [`query_linearizable`](MvccRaftNode::query_linearizable) — on a
//!   follower that is exactly the "staleness 0 redirects to the leader"
//!   contract.
//!
//! ## Bounded staleness: the leader wall-clock piggyback (PR 2 of 2)
//!
//! openraft 0.9.24's heartbeats are engine-internal empty AppendEntries
//! frames with no application payload, so the leader's clock cannot
//! ride on them without forking the engine. The carrier is therefore
//! the **log itself**: every entry [`MvccRaftNode`] proposes is stamped
//! with the leader's wall clock at propose time
//! ([`TxnEntry::leader_time_ms`] — serde-compatible both ways: pre-#5200
//! entries decode as unstamped, unstamped entries keep the pre-#5200
//! byte encoding). Each replica records the stamp of the freshest
//! stamped entry it has applied, and a bounded read with bound `S` is
//! served iff `now - last_stamp <= S` on the serving node's own clock.
//! The stamp is taken at *propose* time — before replication and apply —
//! so the observed staleness already over-counts replication latency:
//! the sound direction. A node that has never applied a stamped entry
//! (fresh boot, pre-#5200 log prefix, or a snapshot installed onto a
//! fresh node — engine snapshots carry no stamps) treats its staleness
//! as **unknown** and refuses bounded reads until a stamped entry
//! arrives; an unstamped entry advances the applied cursor but retains
//! the previous (older) stamp, which only weakens the freshness claim —
//! also sound.
//!
//! **Idle clusters**: with no writes, the last stamp simply ages, and a
//! perfectly healthy idle follower ends up refusing bounded reads. Two
//! supported answers, chosen per deployment: (a) leave it — bounded
//! reads refuse and callers fall back to leader reads (the default;
//! zero overhead, zero log noise); or (b) enable the app-level
//! **staleness beacon** ([`RaftTuning::staleness_beacon_ms`], off by
//! default): the leader proposes an empty, stamped no-op transaction
//! entry whenever no stamped entry has been applied within the window,
//! keeping idle followers fresh at the cost of one tiny log entry per
//! window (which consumes a dense index / commit_ts like any entry — a
//! no-op transaction, no rows touched).
//!
//! ## Clock skew: what it can and cannot affect
//!
//! The ReadIndex path above is **clock-free**: its correctness rests
//! solely on a quorum acknowledging the leader's term, never on
//! wall-clock or monotonic time, so no amount of clock skew or NTP
//! stepping can make `query_linearizable` return stale data. Clocks
//! influence only *liveness* (heartbeat and election timeouts run on
//! each node's local clock).
//!
//! The **lease fast path** — serving linearizable reads inside a
//! quorum-acked time window without a per-read heartbeat round — is
//! deliberately **not** implemented in this PR. openraft 0.9.24 keeps a
//! leader lease internally, but only to suppress elections (its
//! `docs::protocol::leader_lease`); it exposes no lease-based read
//! policy — `ensure_linearizable` always runs the quorum round
//! (`ReadPolicy::LeaseRead` arrives with openraft 0.10's
//! `Raft::ensure_linearizable(read_policy)`). Hand-rolling lease timing
//! outside the engine would re-introduce the classic lease failure
//! mode — a deposed leader serving reads past lease expiry under clock
//! *rate* drift — so the fast path is deferred to an openraft upgrade
//! (or a follow-on) rather than approximated here. When it lands, the
//! standard requirements apply: lease duration < election timeout minus
//! a safety margin, monotonic clock only, warn on NTP step.
//!
//! Of the PR 2 read modes, [`query_at_least`](MvccRaftNode::query_at_least)
//! is fully clock-free (pure index comparison — skew-immune like the
//! ReadIndex path). [`query_bounded_staleness`](MvccRaftNode::query_bounded_staleness)
//! is the one read mode whose *bound* — not its data integrity, which
//! MVCC snapshots at the applied index guarantee regardless — depends on
//! wall clocks: staleness is measured as `follower_now - leader_stamp`,
//! so leader↔follower clock skew adds (or subtracts) directly. Honest
//! accounting of the real bound a caller gets: **true staleness ≤
//! max_staleness + skew**, where `skew` is how far the follower's clock
//! runs *behind* the leader's (a follower running behind under-measures
//! staleness; one running ahead over-measures and refuses early — only
//! the behind direction weakens the guarantee). Deployments relying on
//! tight bounds therefore assume NTP-disciplined clocks and should
//! budget the expected sync error (and the beacon/heartbeat interval)
//! into `max_staleness`; an NTP step on the *leader* makes stamps jump,
//! which at worst makes followers refuse (backward step ages stamps) or
//! briefly under-measure by the step size (forward step). This is
//! exactly the documented-skew-term bound from the curator's
//! acceptance criteria — bounded, never silently unbounded.
//!
//! Out of scope here (deliberately): server/CLI wiring of replicated
//! writes and reads — PostgreSQL-protocol sessions still execute against
//! the local engine; surfacing `NotLeader` as a SQL error with redirect
//! info, and mapping a per-connection/per-statement `max_staleness_ms`
//! PG option onto
//! [`query_bounded_staleness`](MvccRaftNode::query_bounded_staleness)
//! (plus propose-returned tokens onto
//! [`query_at_least`](MvccRaftNode::query_at_least) for session-level
//! read-your-writes), is follow-on work in #5383
//! (`crates/vibesql-server/src/session.rs`).
//!
//! [`RaftStateMachine`]: openraft::storage::RaftStateMachine
//! [`StorageError`]: openraft::StorageError
//! [`ConsensusError::NotLeader`]: crate::backend::ConsensusError::NotLeader

#[cfg(test)]
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::Cursor;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use openraft::error::{CheckIsLeaderError, ClientWriteError, RaftError};
use openraft::storage::{RaftLogStorage, RaftStateMachine};
#[cfg(test)]
use openraft::ServerState;
use openraft::{
    BasicNode, Entry, EntryPayload, LogId, Raft, RaftSnapshotBuilder, SnapshotMeta, StorageError,
    StorageIOError, StoredMembership,
};
use tokio::sync::watch;

use crate::snapshot::SnapshotHorizonPin as _;

use crate::backend::{ConsensusError, LogIndex, Result, Role, Snapshot};
use crate::cluster_config::ClusterConfig;
use crate::durable::DurableLogStore;
use crate::freeze;
use crate::openraft_backend::{
    current_timestamp_ms, initialize_membership, role_from_state, AppResponse, Bootstrap,
    InMemoryLogStore, RaftTuning, RecoveredDurable, TypeConfig,
};
use crate::snapshot::SnapshotStore;
use crate::state_machine::{deserialize_database, ApplyOutcome, TxnEntry, VibesqlStateMachine};

// ---------------------------------------------------------------------------
// Snapshot payload framing: dense index header + vbsql database blob
// ---------------------------------------------------------------------------

/// Frame the machine's snapshot for the engine: 8-byte LE dense
/// application index, then the binary database payload.
fn encode_mvcc_snapshot(last_included_index: LogIndex, db_payload: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity(8 + db_payload.len());
    buf.extend_from_slice(&last_included_index.to_le_bytes());
    buf.extend_from_slice(db_payload);
    buf
}

/// Split an engine-level snapshot payload back into `(dense index,
/// database payload)`. The database payload itself is validated by the
/// machine's decoder before anything mutates.
fn decode_mvcc_snapshot(data: &[u8]) -> std::result::Result<(LogIndex, &[u8]), String> {
    if data.len() < 8 {
        return Err(format!(
            "MVCC snapshot payload is {} bytes — smaller than its 8-byte dense-index header",
            data.len()
        ));
    }
    let dense = LogIndex::from_le_bytes(data[..8].try_into().expect("8-byte slice"));
    Ok((dense, &data[8..]))
}

// ---------------------------------------------------------------------------
// The mounted state machine
// ---------------------------------------------------------------------------

/// A registered engine-level snapshot (framed payload + raft meta).
#[derive(Debug, Clone)]
struct StoredMvccSnapshot {
    meta: SnapshotMeta<u64, BasicNode>,
    /// Framed payload (dense-index header + vbsql blob).
    data: Vec<u8>,
}

/// The applied-state stamp broadcast by the mounted machine after every
/// apply and snapshot install (Raft Phase B2, #5200, PR 2 bookkeeping):
/// what the staleness-bounded and read-your-writes read paths watch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
struct AppliedStamp {
    /// Dense application index of the last applied entry (mirrors
    /// [`VibesqlStateMachine::last_applied`]; `0` = nothing applied).
    dense: LogIndex,
    /// Leader wall clock (ms since the UNIX epoch, taken at propose
    /// time) of the freshest *stamped* entry applied. `None` until one
    /// arrives — fresh node, pre-#5200 log prefix, or a snapshot
    /// installed onto a fresh node (engine snapshots carry no stamps).
    /// Unstamped entries advance `dense` but retain the previous stamp:
    /// a weaker (older) freshness claim is always sound.
    leader_time_ms: Option<u64>,
}

/// Raft-level bookkeeping the engine needs alongside the database state.
#[derive(Debug, Default)]
struct MvccSmInner {
    /// Raw raft log id of the last applied entry (protocol entries
    /// included) — what [`RaftStateMachine::applied_state`] reports.
    last_applied: Option<LogId<u64>>,
    last_membership: StoredMembership<u64, BasicNode>,
    /// Monotonic snapshot counter (uniquified with a timestamp in the
    /// snapshot id, since this counter restarts with the process).
    snapshot_seq: u64,
    current_snapshot: Option<StoredMvccSnapshot>,
}

/// [`VibesqlStateMachine`] mounted as openraft's [`RaftStateMachine`]
/// (+ [`RaftSnapshotBuilder`]). See the module docs for the index
/// mapping, the fatal-apply halt, and the snapshot framing.
///
/// Cloning shares the underlying state (it is a handle), as openraft
/// expects from `get_snapshot_builder`. The `inner` mutex is held across
/// each whole apply batch *and* the snapshot capture, so a concurrently
/// built snapshot can never observe a raft meta / database pair from two
/// different instants (lock order is always `inner` → machine; nothing
/// takes them in the reverse order).
#[derive(Debug, Clone)]
pub(crate) struct MvccStateMachine {
    machine: VibesqlStateMachine,
    inner: Arc<Mutex<MvccSmInner>>,
    /// Broadcasts the applied dense index + freshest leader-clock stamp
    /// after every apply / install, so the PR 2 read paths
    /// (`query_at_least`, `query_bounded_staleness`) can wait/decide
    /// without polling the machine. Updated under the `inner` lock,
    /// which serializes applies — values are monotone in `dense`.
    applied: Arc<watch::Sender<AppliedStamp>>,
    /// Durable persistence for built/installed snapshots (`None` for
    /// in-memory configurations).
    store: Option<Arc<SnapshotStore>>,
    /// Durable raft log, for the deferred post-install purge record
    /// (see `DurableLogStore::purge_compacted`).
    log_store: Option<DurableLogStore>,
    /// The reason this node halted (a fatal apply), if it did. Never
    /// cleared: a halted node must be restarted to resync.
    fatal: Arc<Mutex<Option<String>>>,
    /// Test-only failure injection: the next apply of the entry at this
    /// **dense** index fails fatally (as if the executor had hit a
    /// node-local resource error), exercising the halt path on one node
    /// without poisoning the replicated log for its peers.
    #[cfg(test)]
    inject_fatal_at: Arc<Mutex<Option<LogIndex>>>,
}

impl MvccStateMachine {
    /// A fresh machine over an empty in-memory database, with in-memory
    /// snapshots.
    fn volatile() -> Self {
        Self {
            machine: VibesqlStateMachine::new(),
            inner: Arc::default(),
            applied: Arc::new(watch::Sender::new(AppliedStamp::default())),
            store: None,
            log_store: None,
            fatal: Arc::default(),
            #[cfg(test)]
            inject_fatal_at: Arc::default(),
        }
    }

    /// A fresh machine that persists built/installed snapshots through
    /// `store` and records deferred install purges in `log_store`.
    fn durable(store: Arc<SnapshotStore>, log_store: DurableLogStore) -> Self {
        Self { store: Some(store), log_store: Some(log_store), ..Self::volatile() }
    }

    fn lock(&self) -> MutexGuard<'_, MvccSmInner> {
        self.inner.lock().expect("mvcc raft state machine mutex poisoned")
    }

    /// Seed this (fresh) machine from a durable snapshot recovered off
    /// disk, before the Raft core starts: database state, dense index,
    /// raw `last_applied`, and membership all resume from the snapshot,
    /// and openraft then replays only the log suffix above
    /// `meta.last_log_id`.
    fn seed_from_loaded(&self, meta: SnapshotMeta<u64, BasicNode>, data: Vec<u8>) -> Result<()> {
        let (dense, db_payload) =
            decode_mvcc_snapshot(&data).map_err(ConsensusError::SnapshotCodec)?;
        let db = deserialize_database(db_payload)?;
        let mut inner = self.lock();
        self.machine.install_decoded(db, dense)?;
        inner.last_applied = meta.last_log_id;
        inner.last_membership = meta.last_membership.clone();
        inner.current_snapshot = Some(StoredMvccSnapshot { meta, data });
        // Snapshots carry no leader-clock stamp: the dense cursor jumps,
        // the previous stamp (typically none — this runs on a fresh
        // machine) is retained as a sound lower bound on freshness.
        self.applied.send_modify(|s| s.dense = dense);
        Ok(())
    }

    /// Record the fatal reason and build the [`StorageError`] that halts
    /// the node: openraft treats a storage error from `apply` as fatal —
    /// the core shuts down *without* acknowledging the entry, so
    /// `last_applied` (engine and machine alike) never advances past it.
    /// The reason survives on the handle for operators/tests
    /// ([`MvccRaftNode::fatal_reason`]) and rides inside the error the
    /// engine logs and returns to in-flight `client_write`s.
    fn halt(&self, log_id: LogId<u64>, reason: String) -> StorageError<u64> {
        let reason = format!(
            "halting this node: fatal apply at raft log id {log_id} — {reason} (restart the \
             node to resync via snapshot install + log replay; recording this as a rejection \
             would silently diverge replicas)"
        );
        let mut fatal = self.fatal.lock().expect("fatal reason mutex poisoned");
        if fatal.is_none() {
            *fatal = Some(reason.clone());
        }
        StorageError::IO { source: StorageIOError::apply(log_id, &std::io::Error::other(reason)) }
    }

    fn fatal_reason(&self) -> Option<String> {
        self.fatal.lock().expect("fatal reason mutex poisoned").clone()
    }
}

impl RaftSnapshotBuilder<TypeConfig> for MvccStateMachine {
    async fn build_snapshot(
        &mut self,
    ) -> std::result::Result<openraft::Snapshot<TypeConfig>, StorageError<u64>> {
        // Pin the MVCC vacuum horizon for the whole build — acquired
        // before any state is read, released (guard drop) only after the
        // blob is built, persisted, and registered, so `vacuum_mvcc`
        // cannot reclaim row versions out from under it. (The machine's
        // own `snapshot()` briefly takes a nested pin too; pins stack.)
        let _horizon_pin = self.machine.horizon_pin().acquire();

        // Capture the raft meta and the database state atomically: the
        // `inner` lock is held across both (apply also holds it across
        // each batch), so the dense index inside `data` always matches
        // `meta.last_log_id`.
        let (meta, data) = {
            let mut inner = self.lock();
            let snapshot = self.machine.snapshot().map_err(|e| StorageError::IO {
                source: StorageIOError::write_state_machine(&std::io::Error::other(e.to_string())),
            })?;
            let data = encode_mvcc_snapshot(snapshot.last_included_index, &snapshot.data);
            inner.snapshot_seq += 1;
            let meta = SnapshotMeta {
                last_log_id: inner.last_applied,
                last_membership: inner.last_membership.clone(),
                snapshot_id: format!(
                    "mvcc-{}-{}-{}",
                    inner.last_applied.map_or(0, |id| id.index),
                    inner.snapshot_seq,
                    current_timestamp_ms(),
                ),
            };
            (meta, data)
        };

        // Persist BEFORE registering: openraft must never see (and purge
        // against) a snapshot that is not yet durable.
        if let Some(store) = &self.store {
            store.save(&meta, &data).map_err(|e| StorageError::IO {
                source: StorageIOError::write_snapshot(Some(meta.signature()), &e),
            })?;
        }

        self.lock().current_snapshot =
            Some(StoredMvccSnapshot { meta: meta.clone(), data: data.clone() });

        Ok(openraft::Snapshot { meta, snapshot: Box::new(Cursor::new(data)) })
        // `_horizon_pin` drops here: the horizon was held across read,
        // persistence, and registration.
    }
}

impl RaftStateMachine<TypeConfig> for MvccStateMachine {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> std::result::Result<
        (Option<LogId<u64>>, StoredMembership<u64, BasicNode>),
        StorageError<u64>,
    > {
        let inner = self.lock();
        Ok((inner.last_applied, inner.last_membership.clone()))
    }

    async fn apply<I>(
        &mut self,
        entries: I,
    ) -> std::result::Result<Vec<AppResponse>, StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
        I::IntoIter: Send,
    {
        // Held across the whole batch so a concurrent snapshot build
        // never sees raft meta and database state from two different
        // instants. No `.await` happens under the guard.
        let mut inner = self.lock();
        let mut responses = Vec::new();
        for entry in entries {
            let response = match entry.payload {
                // Protocol entries advance only the raw cursor: they do
                // not consume a dense application index, which is what
                // keeps dense indices (= MVCC commit timestamps)
                // identical on every replica across leader changes.
                EntryPayload::Blank => {
                    inner.last_applied = Some(entry.log_id);
                    AppResponse::Protocol
                }
                EntryPayload::Membership(membership) => {
                    inner.last_applied = Some(entry.log_id);
                    inner.last_membership = StoredMembership::new(Some(entry.log_id), membership);
                    AppResponse::Protocol
                }
                EntryPayload::Normal(data) => {
                    let txn: TxnEntry = match serde_json::from_slice(&data) {
                        Ok(txn) => txn,
                        // Undecodable payloads indicate proposer/applier
                        // version skew, the same class as a frozen-site
                        // mismatch: halt rather than guess (halting
                        // everywhere is an outage; guessing wrong is
                        // divergence).
                        Err(e) => {
                            return Err(self.halt(
                                entry.log_id,
                                format!("replicated entry payload failed to decode: {e}"),
                            ))
                        }
                    };
                    let dense = self.machine.last_applied() + 1;

                    #[cfg(test)]
                    {
                        let mut inject =
                            self.inject_fatal_at.lock().expect("fatal injection mutex poisoned");
                        if *inject == Some(dense) {
                            *inject = None;
                            return Err(self.halt(
                                entry.log_id,
                                format!(
                                    "injected node-local apply failure at dense index {dense} \
                                     (test hook)"
                                ),
                            ));
                        }
                    }

                    match self.machine.apply(dense, &txn) {
                        Ok(outcome) => {
                            inner.last_applied = Some(entry.log_id);
                            // Broadcast for the PR 2 read paths: the new
                            // dense cursor, plus the entry's leader-clock
                            // stamp when it has one (an unstamped entry
                            // retains the previous, older stamp — a
                            // weaker freshness claim, always sound). A
                            // rejected entry counts too: it consumed its
                            // index on every replica, proving this
                            // node's state is exactly as fresh.
                            self.applied.send_modify(|s| {
                                s.dense = dense;
                                if txn.leader_time_ms.is_some() {
                                    s.leader_time_ms = txn.leader_time_ms;
                                }
                            });
                            AppResponse::Mvcc { index: dense, outcome }
                        }
                        // FatalApply (possibly node-local failure) and
                        // Backend (gap/protocol violation) alike: the
                        // machine did not consume the index, and neither
                        // may openraft — halt.
                        Err(e) => {
                            return Err(self.halt(entry.log_id, format!("dense index {dense}: {e}")))
                        }
                    }
                }
            };
            responses.push(response);
        }
        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> std::result::Result<Box<Cursor<Vec<u8>>>, StorageError<u64>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    /// Replace this node's database from a received snapshot. The whole
    /// payload is decoded and validated **before** anything is persisted
    /// or mutated (a corrupt stream must neither half-install nor leave
    /// a corrupt durable file), and on durable configurations the blob
    /// is persisted — and the deferred post-install log purge recorded —
    /// **before** the install is acknowledged, exactly like the echo
    /// machine (Phase A4 crash-safety order).
    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> std::result::Result<(), StorageError<u64>> {
        let data = snapshot.into_inner();
        let codec_err = |e: String| StorageError::IO {
            source: StorageIOError::write_snapshot(
                Some(meta.signature()),
                &std::io::Error::new(std::io::ErrorKind::InvalidData, e),
            ),
        };
        let (dense, db_payload) = decode_mvcc_snapshot(&data).map_err(codec_err)?;
        let db = deserialize_database(db_payload).map_err(|e| codec_err(e.to_string()))?;

        if let Some(store) = &self.store {
            store.save(meta, &data).map_err(|e| StorageError::IO {
                source: StorageIOError::write_snapshot(Some(meta.signature()), &e),
            })?;
            // Record the log purge openraft issued before this snapshot
            // was durable (deferred by the log store; see the echo
            // machine's install for the full rationale and the
            // crash-window repair in `RecoveredDurable::open`).
            if let (Some(log_store), Some(last)) = (&self.log_store, meta.last_log_id) {
                log_store.purge_compacted(last).map_err(|e| StorageError::IO {
                    source: StorageIOError::write_snapshot(Some(meta.signature()), &e),
                })?;
            }
        }

        let mut inner = self.lock();
        self.machine.install_decoded(db, dense).map_err(|e| codec_err(e.to_string()))?;
        inner.last_applied = meta.last_log_id;
        inner.last_membership = meta.last_membership.clone();
        inner.current_snapshot = Some(StoredMvccSnapshot { meta: meta.clone(), data });
        // As in `seed_from_loaded`: dense cursor jumps, no new stamp —
        // staleness stays unknown (or keeps its older lower bound) until
        // the next stamped entry applies. Bounded reads refuse until
        // then; `query_at_least` waiters see the jump immediately.
        self.applied.send_modify(|s| s.dense = dense);
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> std::result::Result<Option<openraft::Snapshot<TypeConfig>>, StorageError<u64>> {
        let inner = self.lock();
        Ok(inner.current_snapshot.as_ref().map(|s| openraft::Snapshot {
            meta: s.meta.clone(),
            snapshot: Box::new(Cursor::new(s.data.clone())),
        }))
    }
}

// ---------------------------------------------------------------------------
// The node
// ---------------------------------------------------------------------------

/// One voter of a Raft cluster whose state machine is a real MVCC
/// database: replicated writes flow leader → quorum → apply-on-every-
/// voter, with commit_ts = dense log index (Raft Phase B1, #5199, PR 2).
///
/// Writes ([`execute_replicated`](Self::execute_replicated) /
/// [`execute_replicated_txn`](Self::execute_replicated_txn)) are
/// accepted only on the leader — anywhere else they fail fast with
/// [`ConsensusError::NotLeader`] and a leader hint — and resolve only
/// after the entry is committed and applied locally, so a session can
/// read its own write. Reads come in two modes (Raft Phase B2, #5200,
/// PR 1): [`query`](Self::query) is local and stale-allowed;
/// [`query_linearizable`](Self::query_linearizable) confirms leadership
/// with a quorum first (see the module docs' read-path section).
///
/// This is the multi-node successor of the PR 1 harness
/// (`ReplicatedDb`): same entry type, same freeze-at-propose, same
/// machine — but apply happens inside openraft's state-machine worker
/// instead of an external catch-up loop. Server/CLI wiring is follow-on
/// work (see the module docs).
pub struct MvccRaftNode {
    raft: Raft<TypeConfig>,
    sm: MvccStateMachine,
    metrics: tokio::sync::watch::Receiver<openraft::RaftMetrics<u64, BasicNode>>,
    /// Accept-loop task of the TCP transport (`None` for channel-network
    /// configurations). Aborted on shutdown and on drop.
    listener_task: Option<tokio::task::JoinHandle<()>>,
    /// App-level staleness-beacon task (`None` unless
    /// [`RaftTuning::staleness_beacon_ms`] is non-zero) — see the module
    /// docs' bounded-staleness section. Aborted on shutdown and on drop.
    beacon_task: Option<tokio::task::JoinHandle<()>>,
}

impl Drop for MvccRaftNode {
    fn drop(&mut self) {
        if let Some(task) = &self.listener_task {
            task.abort();
        }
        if let Some(task) = &self.beacon_task {
            task.abort();
        }
    }
}

impl Debug for MvccRaftNode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MvccRaftNode")
            .field("role", &self.role())
            .field("last_applied", &self.last_applied())
            .finish_non_exhaustive()
    }
}

impl MvccRaftNode {
    /// Boot one voter of a **TCP-connected MVCC cluster** with an
    /// in-memory Raft log. Prefer
    /// [`join_tcp_cluster_with_data_dir`](Self::join_tcp_cluster_with_data_dir)
    /// anywhere a restart must not forget the log or vote.
    ///
    /// Like the echo backend's cluster constructors, this does **not**
    /// wait for an election: the caller awaits leadership (e.g. by
    /// polling [`role`](Self::role) / [`current_leader`](Self::current_leader)
    /// with a bounded timeout). Must be called from within a tokio
    /// runtime.
    pub async fn join_tcp_cluster(node_id: u64, config: &ClusterConfig) -> Result<Self> {
        Self::join_tcp_cluster_tuned(node_id, config, RaftTuning::default()).await
    }

    /// Like [`join_tcp_cluster`](Self::join_tcp_cluster), with explicit
    /// [`RaftTuning`] (snapshot/retention/transfer knobs and the Phase
    /// B2 staleness beacon).
    pub async fn join_tcp_cluster_tuned(
        node_id: u64,
        config: &ClusterConfig,
        tuning: RaftTuning,
    ) -> Result<Self> {
        Self::join_tcp(
            node_id,
            config,
            InMemoryLogStore::default(),
            MvccStateMachine::volatile(),
            Bootstrap::Initialize,
            tuning,
        )
        .await
    }

    /// Like [`join_tcp_cluster`](Self::join_tcp_cluster), but the node's
    /// Raft log, vote, and snapshots are persisted under `dir` (created
    /// if absent) and recovered on restart — snapshot first, then log
    /// replay, with the same loss/tampering cross-checks as the echo
    /// backend.
    pub async fn join_tcp_cluster_with_data_dir(
        node_id: u64,
        config: &ClusterConfig,
        dir: impl AsRef<Path>,
    ) -> Result<Self> {
        Self::join_tcp_cluster_with_data_dir_tuned(node_id, config, dir, RaftTuning::default())
            .await
    }

    /// Like
    /// [`join_tcp_cluster_with_data_dir`](Self::join_tcp_cluster_with_data_dir),
    /// with explicit snapshot/retention/transfer tuning — see
    /// [`RaftTuning`].
    pub async fn join_tcp_cluster_with_data_dir_tuned(
        node_id: u64,
        config: &ClusterConfig,
        dir: impl AsRef<Path>,
        tuning: RaftTuning,
    ) -> Result<Self> {
        let (log_store, sm, bootstrap) = Self::open_durable(dir.as_ref())?;
        Self::join_tcp(node_id, config, log_store, sm, bootstrap, tuning).await
    }

    /// Open (or create) the durable artifacts under `dir` and seed a
    /// state machine from the latest durable snapshot, if any.
    fn open_durable(dir: &Path) -> Result<(DurableLogStore, MvccStateMachine, Bootstrap)> {
        let raw = RecoveredDurable::open(dir)?;
        let sm = MvccStateMachine::durable(Arc::clone(&raw.snapshot_store), raw.log_store.clone());
        if let Some(loaded) = raw.loaded {
            sm.seed_from_loaded(loaded.meta, loaded.data).map_err(|e| {
                ConsensusError::Backend(format!(
                    "failed to recover durable MVCC snapshot in {}: {e}",
                    dir.display()
                ))
            })?;
        }
        // Cluster members do not wait for local replay: convergence is a
        // cluster-level outcome the caller awaits.
        let bootstrap = if raw.log_has_state {
            Bootstrap::Recover { last_log_index: None }
        } else {
            Bootstrap::Initialize
        };
        Ok((raw.log_store, sm, bootstrap))
    }

    async fn join_tcp<LS>(
        node_id: u64,
        config: &ClusterConfig,
        log_store: LS,
        sm: MvccStateMachine,
        bootstrap: Bootstrap,
        tuning: RaftTuning,
    ) -> Result<Self>
    where
        LS: RaftLogStorage<TypeConfig>,
    {
        let listen_addr = config.addr(node_id).ok_or_else(|| {
            ConsensusError::Config(format!("node {node_id} is not in the cluster config"))
        })?;
        // Bind before booting the core so a peer that initialized first
        // can reach this node as soon as its Raft exists.
        let listener = crate::tcp::bind_listener(listen_addr).await.map_err(|e| {
            ConsensusError::Backend(format!(
                "failed to bind consensus listener on {listen_addr}: {e}"
            ))
        })?;

        let network = crate::tcp::TcpNetworkFactory::new(config);
        let mut node = Self::boot(node_id, network, log_store, sm, tuning).await?;
        node.listener_task = Some(crate::tcp::spawn_listener(node.raft.clone(), listener));

        initialize_membership(&node.raft, config.membership(), bootstrap).await?;
        Ok(node)
    }

    /// Spawn the Raft core over `sm`: storage + state machine + network,
    /// no membership yet.
    async fn boot<LS, NF>(
        node_id: u64,
        network: NF,
        log_store: LS,
        sm: MvccStateMachine,
        tuning: RaftTuning,
    ) -> Result<Self>
    where
        LS: RaftLogStorage<TypeConfig>,
        NF: openraft::RaftNetworkFactory<TypeConfig>,
    {
        let config = Arc::new(tuning.raft_config()?);
        let raft = Raft::new(node_id, config, network, log_store, sm.clone())
            .await
            .map_err(|e| ConsensusError::Backend(format!("failed to start raft core: {e}")))?;
        let metrics = raft.metrics();
        let beacon_task = (tuning.staleness_beacon_ms > 0).then(|| {
            spawn_staleness_beacon(
                raft.clone(),
                metrics.clone(),
                Arc::clone(&sm.applied),
                tuning.staleness_beacon_ms,
            )
        });
        Ok(Self { raft, sm, metrics, listener_task: None, beacon_task })
    }

    /// Execute one autocommit write statement through consensus. See
    /// [`execute_replicated_txn`](Self::execute_replicated_txn).
    pub async fn execute_replicated(&self, sql: &str) -> Result<(LogIndex, ApplyOutcome)> {
        self.execute_replicated_txn(&[sql]).await
    }

    /// Execute a multi-statement transaction through consensus: the
    /// whole batch is proposed as **one** [`TxnEntry`] (one log entry
    /// per committed transaction) and applied atomically on every voter
    /// with commit_ts = the entry's dense log index.
    ///
    /// Non-deterministic expressions are **frozen here, at propose
    /// time, on the leader** (#5377); statements whose nondeterminism
    /// cannot be frozen fail before consuming a log index. On a
    /// non-leader this fails fast with [`ConsensusError::NotLeader`]
    /// (leader hint attached when known) before evaluating anything.
    ///
    /// Resolves once the entry is committed on a quorum **and applied
    /// locally** — the caller can immediately read its own write. A
    /// rejected entry (failed statement, rolled back identically on
    /// every replica) is an `Ok` with [`ApplyOutcome::Rejected`].
    pub async fn execute_replicated_txn(
        &self,
        statements: &[&str],
    ) -> Result<(LogIndex, ApplyOutcome)> {
        // Fast-path leader check: don't evaluate volatile expressions
        // (freeze draws random values / reads the clock) on a node whose
        // proposal is going to be refused anyway. Races are harmless —
        // `client_write` itself enforces leadership authoritatively.
        if self.role() != Role::Leader {
            return Err(ConsensusError::NotLeader { leader_hint: self.current_leader() });
        }
        let mut frozen = Vec::with_capacity(statements.len());
        for sql in statements {
            frozen.push(freeze::freeze_statement(sql).map_err(|e| {
                ConsensusError::Backend(format!("statement cannot be replicated: {e}"))
            })?);
        }
        let entry =
            TxnEntry::frozen_batch(statements.iter().map(|s| s.to_string()).collect(), frozen);
        self.propose_entry(entry).await
    }

    /// Propose a pre-built entry. Internal: the public surface freezes
    /// statements first (tests use this to exercise the apply-side
    /// defenses with hand-crafted entries). Every entry proposed here is
    /// stamped with this node's wall clock — the leader-clock piggyback
    /// the bounded-staleness read path measures against (Raft Phase B2,
    /// #5200, PR 2; see [`TxnEntry::leader_time_ms`]).
    async fn propose_entry(&self, entry: TxnEntry) -> Result<(LogIndex, ApplyOutcome)> {
        let entry = entry.stamp_leader_time(current_timestamp_ms());
        let payload =
            serde_json::to_vec(&entry).map_err(|e| ConsensusError::Backend(e.to_string()))?;
        let response =
            self.raft.client_write(payload).await.map_err(|e| self.map_write_error(e))?;
        match response.data {
            AppResponse::Mvcc { index, outcome } => Ok((index, outcome)),
            other => Err(ConsensusError::Backend(format!(
                "client write resolved with an unexpected apply response: {other:?}"
            ))),
        }
    }

    fn map_write_error(
        &self,
        e: RaftError<u64, ClientWriteError<u64, BasicNode>>,
    ) -> ConsensusError {
        match e {
            RaftError::APIError(ClientWriteError::ForwardToLeader(forward)) => {
                ConsensusError::NotLeader { leader_hint: forward.leader_id }
            }
            other => {
                // A halted node surfaces its fatal reason instead of the
                // engine's shutdown noise: the write failed because this
                // node must not apply anything further.
                if let Some(reason) = self.fatal_reason() {
                    ConsensusError::FatalApply(reason)
                } else {
                    ConsensusError::Backend(other.to_string())
                }
            }
        }
    }

    /// Run a read-only SELECT against the locally applied state —
    /// **local, stale-allowed** (see [`VibesqlStateMachine::query`]).
    ///
    /// No leadership check, no network round: this is available on every
    /// node, including a leader partitioned into a minority, and may
    /// trail the cluster's committed state arbitrarily. For reads that
    /// must observe every committed write, use
    /// [`query_linearizable`](Self::query_linearizable).
    pub fn query(&self, sql: &str) -> Result<Vec<Vec<vibesql_types::SqlValue>>> {
        self.sm.machine.query(sql)
    }

    /// Run a read-only SELECT with **linearizable** semantics (Raft
    /// Phase B2, #5200, PR 1): openraft's ReadIndex protocol
    /// ([`Raft::ensure_linearizable`]) first confirms this node's
    /// leadership with a quorum heartbeat round and waits for the local
    /// state machine to reach the confirmed read index; only then does
    /// the query run locally. The whole path is clock-free — see the
    /// module docs' read-path section, including why the lease fast path
    /// is deferred. Costs one heartbeat round-trip per read (bounded by
    /// the heartbeat-interval RPC TTL), which is still far cheaper than
    /// the full log-append round a replicated write pays.
    ///
    /// Fails with [`ConsensusError::NotLeader`]:
    /// - carrying a leader hint when this node knows it is not the
    ///   leader (route the read there);
    /// - carrying **no** hint when this node still believes it leads but
    ///   could not confirm a quorum (e.g. partitioned into a minority) —
    ///   its own view is exactly what could not be confirmed, so hinting
    ///   at itself would route callers back to a possibly-stale node.
    pub async fn query_linearizable(&self, sql: &str) -> Result<Vec<Vec<vibesql_types::SqlValue>>> {
        self.raft.ensure_linearizable().await.map_err(|e| self.map_read_error(e))?;
        self.sm.machine.query(sql)
    }

    /// Map a failed leadership confirmation onto the adapter error,
    /// mirroring [`map_write_error`](Self::map_write_error).
    fn map_read_error(
        &self,
        e: RaftError<u64, CheckIsLeaderError<u64, BasicNode>>,
    ) -> ConsensusError {
        match e {
            RaftError::APIError(CheckIsLeaderError::ForwardToLeader(forward)) => {
                ConsensusError::NotLeader { leader_hint: forward.leader_id }
            }
            // Still nominally the leader, but a quorum did not answer the
            // confirmation heartbeat: serving the read could violate
            // linearizability (a majority may have elected someone else).
            // Deliberately no hint — this node's own leadership is what
            // could not be confirmed.
            RaftError::APIError(CheckIsLeaderError::QuorumNotEnough(_)) => {
                ConsensusError::NotLeader { leader_hint: None }
            }
            other => {
                // A halted node surfaces its fatal reason instead of the
                // engine's shutdown noise (same as the write path).
                if let Some(reason) = self.fatal_reason() {
                    ConsensusError::FatalApply(reason)
                } else {
                    ConsensusError::Backend(other.to_string())
                }
            }
        }
    }

    /// Run a read-only SELECT with **read-your-writes** semantics (Raft
    /// Phase B2, #5200, PR 2): wait — up to `wait` — until this node's
    /// locally applied **dense application index** reaches `min_index`,
    /// then read locally. Clock-free and exact: the token is the dense
    /// index a write's propose returned
    /// ([`execute_replicated`](Self::execute_replicated) /
    /// [`execute_replicated_txn`](Self::execute_replicated_txn)), and
    /// dense indices are identical on every replica (see the module
    /// docs' index-mapping section), so *write on the leader → carry the
    /// returned index → read on ANY node with the token* always observes
    /// the write. Works on leaders and followers alike; `min_index = 0`
    /// degenerates to a plain local [`query`](Self::query).
    ///
    /// Fails with [`ConsensusError::ReadTimeout`] (carrying this node's
    /// applied index and a best-effort leader hint) if the node does not
    /// catch up within `wait` — e.g. a partitioned or badly lagging
    /// follower. The session can then retry here, or route the read to
    /// the leader, which has applied every index it ever returned.
    pub async fn query_at_least(
        &self,
        min_index: LogIndex,
        sql: &str,
        wait: Duration,
    ) -> Result<Vec<Vec<vibesql_types::SqlValue>>> {
        let mut rx = self.sm.applied.subscribe();
        // A closed channel (the inner `Err`) cannot happen while `self`
        // is alive — the sender lives on `self.sm` — so it folds into
        // the timeout arm rather than inventing an unreachable error.
        let caught_up = matches!(
            tokio::time::timeout(wait, rx.wait_for(|s| s.dense >= min_index)).await,
            Ok(Ok(_))
        );
        if caught_up {
            self.sm.machine.query(sql)
        } else {
            Err(ConsensusError::ReadTimeout {
                required: min_index,
                applied: self.sm.applied.borrow().dense,
                leader_hint: self.current_leader(),
            })
        }
    }

    /// Run a read-only SELECT with **bounded staleness** (Raft Phase B2,
    /// #5200, PR 2): serve locally iff this node can prove its applied
    /// state is no staler than `max_staleness` — i.e. the leader-clock
    /// stamp of the freshest stamped entry it has applied is within the
    /// bound of this node's current wall clock (see the module docs'
    /// bounded-staleness section for the carrier design, and the
    /// clock-skew section for the honest bound: true staleness ≤
    /// `max_staleness` + leader↔follower skew). Works on any node; a
    /// leader gets no special treatment — a deposed or partitioned
    /// ex-leader's stamps age exactly like a follower's, so it refuses
    /// rather than serve provably-stale data.
    ///
    /// A bound of **zero** delegates to
    /// [`query_linearizable`](Self::query_linearizable): the leader
    /// serves it linearizably, a follower fails with
    /// [`ConsensusError::NotLeader`] and a leader hint — the
    /// "staleness 0 redirects to the leader" contract.
    ///
    /// Otherwise fails with [`ConsensusError::StalenessExceeded`] when
    /// the bound cannot be proven: the observed staleness exceeds it, or
    /// no stamped entry has been applied yet (`observed_ms: None` —
    /// fresh node, pre-#5200 log prefix, snapshot install onto a fresh
    /// node, or an idle cluster without the
    /// [`RaftTuning::staleness_beacon_ms`] beacon). The error carries a
    /// leader hint for redirect routing — except when this node itself
    /// believes it leads, since stale stamps on a "leader" are evidence
    /// it may be deposed, and hinting at itself would route callers
    /// straight back (the same discipline as
    /// [`query_linearizable`](Self::query_linearizable)'s quorum-failure
    /// arm).
    ///
    /// The server's per-connection / per-statement `max_staleness_ms` PG
    /// option (#5383) maps directly onto this method's `max_staleness`.
    pub async fn query_bounded_staleness(
        &self,
        max_staleness: Duration,
        sql: &str,
    ) -> Result<Vec<Vec<vibesql_types::SqlValue>>> {
        if max_staleness.is_zero() {
            return self.query_linearizable(sql).await;
        }
        let max_staleness_ms = u64::try_from(max_staleness.as_millis()).unwrap_or(u64::MAX);
        let stamp = self.sm.applied.borrow().leader_time_ms;
        let observed_ms = stamp.map(|t| current_timestamp_ms().saturating_sub(t));
        match observed_ms {
            Some(observed) if observed <= max_staleness_ms => self.sm.machine.query(sql),
            _ => Err(ConsensusError::StalenessExceeded {
                observed_ms,
                max_staleness_ms,
                leader_hint: match self.role() {
                    Role::Leader => None,
                    _ => self.current_leader(),
                },
            }),
        }
    }

    /// This node's current role in the consensus group.
    pub fn role(&self) -> Role {
        role_from_state(self.metrics.borrow().state)
    }

    /// The node this one currently believes leads the cluster (`None`
    /// until it has heard from — or become — a leader).
    pub fn current_leader(&self) -> Option<u64> {
        self.metrics.borrow().current_leader
    }

    /// Dense application index of the last entry applied to the local
    /// database (`0` if none) — also the MVCC commit timestamp of the
    /// most recent committed transaction.
    pub fn last_applied(&self) -> LogIndex {
        self.sm.machine.last_applied()
    }

    /// The reason this node halted on a fatal apply, if it did. A halted
    /// node acknowledges nothing further; restart it to resync via
    /// snapshot install + log replay.
    pub fn fatal_reason(&self) -> Option<String> {
        self.sm.fatal_reason()
    }

    /// Raw raft index of the last snapshot this node knows of (built
    /// locally or installed from the leader); `None` until one exists.
    pub fn snapshot_log_index(&self) -> Option<u64> {
        self.metrics.borrow().snapshot.map(|id| id.index)
    }

    /// Raw raft index through which this node's log has been purged
    /// (`None` until the first purge). Never exceeds the durable
    /// snapshot.
    pub fn purged_log_index(&self) -> Option<u64> {
        self.metrics.borrow().purged.map(|id| id.index)
    }

    /// Capture a snapshot through the engine's real pipeline (trigger →
    /// builder → registration), returning the database-level artifact:
    /// `last_included_index` is the **dense** application index and
    /// `data` the machine's binary database payload — the same shape
    /// [`VibesqlStateMachine::snapshot`] produces, so
    /// [`VibesqlStateMachine::from_snapshot`] can restore it.
    pub async fn snapshot(&self) -> Result<Snapshot> {
        let Some(target) = self.metrics.borrow().last_applied else {
            // Nothing applied (not even membership): nothing for the
            // engine to build at. Unreachable through the public
            // constructors once the cluster has initialized.
            return self.sm.machine.snapshot();
        };

        self.raft
            .trigger()
            .snapshot()
            .await
            .map_err(|e| ConsensusError::Backend(format!("failed to trigger snapshot: {e}")))?;
        self.raft
            .wait(Some(Duration::from_secs(10)))
            .metrics(move |m| m.snapshot >= Some(target), "snapshot build")
            .await
            .map_err(|e| ConsensusError::Backend(format!("snapshot build did not settle: {e}")))?;

        let inner = self.sm.lock();
        let stored = inner.current_snapshot.as_ref().ok_or_else(|| {
            ConsensusError::Backend(
                "snapshot build settled but no snapshot is registered".to_string(),
            )
        })?;
        let (dense, db_payload) =
            decode_mvcc_snapshot(&stored.data).map_err(ConsensusError::SnapshotCodec)?;
        Ok(Snapshot { last_included_index: dense, data: db_payload.to_vec() })
    }

    /// Gracefully shut down the Raft core (and, for a TCP-clustered
    /// node, its listener and inbound connections).
    pub async fn shutdown(&self) -> Result<()> {
        if let Some(task) = &self.listener_task {
            task.abort();
        }
        if let Some(task) = &self.beacon_task {
            task.abort();
        }
        self.raft.shutdown().await.map_err(|e| ConsensusError::Backend(e.to_string()))
    }
}

/// The app-level staleness beacon (Raft Phase B2, #5200, PR 2 — see the
/// module docs' bounded-staleness section, idle-cluster paragraph):
/// every `interval_ms`, a node that believes it leads checks whether any
/// stamped entry was applied within the window and, if not, proposes an
/// empty no-op [`TxnEntry`] stamped with its wall clock. On a healthy
/// idle cluster a follower therefore sees a fresh stamp at least every
/// ~2×`interval_ms` (one window to notice + one to land).
///
/// Deliberately fire-and-forget: a `NotLeader` race (deposed between the
/// role check and the propose), an engine shutdown, or any other write
/// failure is a non-event — the beacon retries next tick, and bounded
/// reads simply refuse in the meantime, which is the safe direction.
fn spawn_staleness_beacon(
    raft: Raft<TypeConfig>,
    metrics: tokio::sync::watch::Receiver<openraft::RaftMetrics<u64, BasicNode>>,
    applied: Arc<watch::Sender<AppliedStamp>>,
    interval_ms: u64,
) -> tokio::task::JoinHandle<()> {
    tokio::task::spawn(async move {
        let interval = Duration::from_millis(interval_ms);
        loop {
            tokio::time::sleep(interval).await;
            if role_from_state(metrics.borrow().state) != Role::Leader {
                continue;
            }
            let fresh_enough = applied
                .borrow()
                .leader_time_ms
                .is_some_and(|t| current_timestamp_ms().saturating_sub(t) < interval_ms);
            if fresh_enough {
                continue;
            }
            let entry =
                TxnEntry::batch(Vec::<String>::new()).stamp_leader_time(current_timestamp_ms());
            let Ok(payload) = serde_json::to_vec(&entry) else { continue };
            let _ = raft.client_write(payload).await;
        }
    })
}

#[cfg(test)]
impl MvccRaftNode {
    /// Boot one member of an in-process channel-network cluster
    /// (mirrors `OpenraftBackend::join_channel_cluster`).
    pub(crate) async fn join_channel_cluster<LS>(
        node_id: u64,
        members: &BTreeMap<u64, BasicNode>,
        router: &crate::network::ChannelRouter,
        log_store: LS,
        sm: MvccStateMachine,
        bootstrap: Bootstrap,
        tuning: RaftTuning,
    ) -> Result<Self>
    where
        LS: RaftLogStorage<TypeConfig>,
    {
        let network = crate::network::ChannelNetworkFactory::new(router.clone());
        let node = Self::boot(node_id, network, log_store, sm, tuning).await?;
        // Register the inbound RPC loop *before* initializing, so peers
        // that initialized first can already reach this node.
        router.register(node_id, node.raft.clone());
        initialize_membership(&node.raft, members.clone(), bootstrap).await?;
        Ok(node)
    }

    /// A single-voter node with everything in memory, elected leader
    /// before this returns — the cheapest way to exercise the mount
    /// itself (apply plumbing, outcome surfacing, snapshot artifact).
    pub(crate) async fn single_node() -> Result<Self> {
        let node = Self::boot(
            1,
            crate::openraft_backend::NoopNetworkFactory,
            InMemoryLogStore::default(),
            MvccStateMachine::volatile(),
            RaftTuning::default(),
        )
        .await?;
        let members = BTreeMap::from([(1, BasicNode::default())]);
        initialize_membership(&node.raft, members, Bootstrap::Initialize).await?;
        node.raft
            .wait(Some(Duration::from_secs(10)))
            .state(ServerState::Leader, "single-node leader election")
            .await
            .map_err(|e| ConsensusError::Backend(format!("leader election did not settle: {e}")))?;
        Ok(node)
    }

    /// The underlying MVCC machine, for storage-level assertions (only
    /// the `mvcc_enabled`-gated tests inspect MVCC stamps).
    #[cfg(feature = "mvcc_enabled")]
    pub(crate) fn machine(&self) -> &VibesqlStateMachine {
        &self.sm.machine
    }

    /// Arm the test-only fatal injection: the apply of the entry at this
    /// **dense** index fails as if a node-local resource error occurred.
    pub(crate) fn inject_fatal_at(&self, dense_index: LogIndex) {
        *self.sm.inject_fatal_at.lock().expect("fatal injection mutex poisoned") =
            Some(dense_index);
    }

    /// Raw raft index of the last applied entry per the **state
    /// machine's own** bookkeeping (not the metrics channel) — what the
    /// engine would resume from.
    pub(crate) fn sm_raft_last_applied(&self) -> Option<u64> {
        self.sm.lock().last_applied.map(|id| id.index)
    }
}

// ---------------------------------------------------------------------------
// Tests: the Phase B1 PR 2 multi-node acceptance scenarios
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use tempfile::TempDir;

    use super::*;
    use crate::freeze::FrozenValue;
    use crate::network::ChannelRouter;
    use vibesql_types::SqlValue;

    /// Upper bound for any single cluster-level wait (election, catch-up,
    /// purge). Bounded polls only — every wait fails loudly, never hangs.
    const WAIT_TIMEOUT: Duration = Duration::from_secs(10);
    const POLL_INTERVAL: Duration = Duration::from_millis(20);

    /// Where a node keeps its Raft log + vote (and, when durable, its
    /// snapshots) between `kill` and `restore`.
    enum NodeStorage {
        /// Shared handle to the killed node's in-memory store — models a
        /// process that lost its (volatile) database but kept its log.
        InMemory(InMemoryLogStore),
        /// Data directory reopened on restore (durable log + snapshots).
        Durable(TempDir),
    }

    struct TestNode {
        /// `None` while the node is killed.
        node: Option<MvccRaftNode>,
        storage: NodeStorage,
    }

    /// An in-process MVCC Raft cluster wired by the channel transport.
    struct MvccCluster {
        router: ChannelRouter,
        members: BTreeMap<u64, BasicNode>,
        nodes: BTreeMap<u64, TestNode>,
        tuning: RaftTuning,
    }

    impl MvccCluster {
        /// Boot `n` voters with in-memory logs and fresh databases.
        async fn new(n: u64) -> Self {
            Self::build(n, false, RaftTuning::default()).await
        }

        /// Boot `n` voters each persisting log + snapshots in a tempdir.
        async fn new_durable(n: u64, tuning: RaftTuning) -> Self {
            Self::build(n, true, tuning).await
        }

        async fn build(n: u64, durable: bool, tuning: RaftTuning) -> Self {
            let router = ChannelRouter::default();
            let members: BTreeMap<u64, BasicNode> =
                (1..=n).map(|id| (id, BasicNode::default())).collect();

            let mut nodes = BTreeMap::new();
            for id in 1..=n {
                let node = if durable {
                    let dir = TempDir::new().expect("create tempdir for durable node");
                    let (log_store, sm, bootstrap) =
                        MvccRaftNode::open_durable(dir.path()).expect("open durable storage");
                    let node = MvccRaftNode::join_channel_cluster(
                        id, &members, &router, log_store, sm, bootstrap, tuning,
                    )
                    .await
                    .expect("boot durable mvcc node");
                    TestNode { node: Some(node), storage: NodeStorage::Durable(dir) }
                } else {
                    let store = InMemoryLogStore::default();
                    let node = MvccRaftNode::join_channel_cluster(
                        id,
                        &members,
                        &router,
                        store.clone(),
                        MvccStateMachine::volatile(),
                        Bootstrap::Initialize,
                        tuning,
                    )
                    .await
                    .expect("boot mvcc node");
                    TestNode { node: Some(node), storage: NodeStorage::InMemory(store) }
                };
                nodes.insert(id, node);
            }

            Self { router, members, nodes, tuning }
        }

        fn node(&self, id: u64) -> &MvccRaftNode {
            self.nodes
                .get(&id)
                .and_then(|n| n.node.as_ref())
                .unwrap_or_else(|| panic!("node {id} is killed or unknown"))
        }

        fn live_ids(&self) -> Vec<u64> {
            self.nodes.iter().filter(|(_, n)| n.node.is_some()).map(|(id, _)| *id).collect()
        }

        /// "Crash" a node: cut it off from the network and stop its core.
        /// Shutdown errors are tolerated — a node that already halted on a
        /// fatal apply has a dead core by design.
        async fn kill(&mut self, id: u64) {
            self.router.deregister(id);
            let node = self
                .nodes
                .get_mut(&id)
                .and_then(|n| n.node.take())
                .unwrap_or_else(|| panic!("node {id} is already killed or unknown"));
            let _ = node.shutdown().await;
        }

        /// Restart a killed node from its retained storage with a **fresh
        /// database**: in-memory nodes replay the kept log from scratch;
        /// durable nodes recover snapshot-first then replay the suffix.
        async fn restore(&mut self, id: u64) {
            let entry = self.nodes.get_mut(&id).expect("unknown node");
            assert!(entry.node.is_none(), "node {id} is not killed");
            let node = match &entry.storage {
                NodeStorage::InMemory(store) => {
                    MvccRaftNode::join_channel_cluster(
                        id,
                        &self.members,
                        &self.router,
                        store.clone(),
                        MvccStateMachine::volatile(),
                        Bootstrap::Recover { last_log_index: None },
                        self.tuning,
                    )
                    .await
                }
                NodeStorage::Durable(dir) => {
                    let (log_store, sm, bootstrap) =
                        MvccRaftNode::open_durable(dir.path()).expect("reopen durable storage");
                    MvccRaftNode::join_channel_cluster(
                        id,
                        &self.members,
                        &self.router,
                        log_store,
                        sm,
                        bootstrap,
                        self.tuning,
                    )
                    .await
                }
            };
            entry.node = Some(node.expect("restore killed node"));
        }

        async fn wait_for_leader(&self) -> u64 {
            self.wait_until("a leader to be elected", || {
                self.live_ids().into_iter().find(|id| self.node(*id).role() == Role::Leader)
            })
            .await
        }

        /// Wait until node `id` has applied the **dense** index `idx`.
        async fn wait_for_apply(&self, id: u64, idx: LogIndex) {
            self.wait_until(&format!("node {id} to apply dense index {idx}"), || {
                (self.node(id).last_applied() >= idx).then_some(())
            })
            .await;
        }

        /// Execute on whoever currently leads, re-resolving leadership if
        /// an election moved it mid-call.
        async fn execute_on_leader(&self, sql: &str) -> (LogIndex, ApplyOutcome) {
            let deadline = tokio::time::Instant::now() + WAIT_TIMEOUT;
            loop {
                let leader = self.wait_for_leader().await;
                match self.node(leader).execute_replicated(sql).await {
                    Ok(result) => return result,
                    Err(ConsensusError::NotLeader { .. }) => {
                        assert!(
                            tokio::time::Instant::now() < deadline,
                            "timed out executing {sql:?}: leadership never settled"
                        );
                    }
                    Err(other) => panic!("execute of {sql:?} failed: {other:?}"),
                }
            }
        }

        /// Assert every live node returns identical rows for `sql`.
        fn assert_converged(&self, sql: &str) -> Vec<Vec<SqlValue>> {
            let ids = self.live_ids();
            let reference = self.node(ids[0]).query(sql).expect("query reference node");
            for id in &ids[1..] {
                assert_eq!(
                    self.node(*id).query(sql).expect("query node"),
                    reference,
                    "node {id} diverged from node {} on {sql:?}",
                    ids[0]
                );
            }
            reference
        }

        async fn wait_until<T>(&self, what: &str, mut probe: impl FnMut() -> Option<T>) -> T {
            let deadline = tokio::time::Instant::now() + WAIT_TIMEOUT;
            loop {
                if let Some(value) = probe() {
                    return value;
                }
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "timed out after {WAIT_TIMEOUT:?} waiting for {what}"
                );
                tokio::time::sleep(POLL_INTERVAL).await;
            }
        }
    }

    fn follower_of(cluster: &MvccCluster, leader: u64) -> u64 {
        cluster
            .live_ids()
            .into_iter()
            .find(|id| *id != leader)
            .expect("cluster has at least one follower")
    }

    /// The mount itself, on a single voter: writes flow client_write →
    /// commit → apply-inside-the-engine; outcomes (incl. deterministic
    /// rejection) surface through the response; the snapshot artifact is
    /// the machine's own codec.
    #[tokio::test]
    async fn single_node_mount_applies_rejects_and_snapshots() {
        let node = MvccRaftNode::single_node().await.unwrap();

        let (idx, outcome) = node
            .execute_replicated("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)")
            .await
            .unwrap();
        assert_eq!((idx, outcome), (1, ApplyOutcome::Applied { rows_affected: 0 }));

        let (idx, outcome) =
            node.execute_replicated("INSERT INTO users VALUES (1, 'alice')").await.unwrap();
        assert_eq!((idx, outcome), (2, ApplyOutcome::Applied { rows_affected: 1 }));

        // The session reads its own write immediately after the propose.
        assert_eq!(
            node.query("SELECT name FROM users").unwrap(),
            vec![vec![SqlValue::Varchar("alice".into())]]
        );

        // A deterministic failure is a *rejected entry*, not an error: it
        // consumed dense index 3 and surfaced through the apply response.
        let (idx, outcome) =
            node.execute_replicated("INSERT INTO users VALUES (1, 'dupe')").await.unwrap();
        assert_eq!(idx, 3);
        assert!(matches!(outcome, ApplyOutcome::Rejected { .. }), "got: {outcome:?}");
        assert_eq!(node.last_applied(), 3);

        node.execute_replicated("INSERT INTO users VALUES (2, 'bob')").await.unwrap();

        // The engine-built snapshot artifact restores into a standalone
        // machine byte-identically.
        let snapshot = node.snapshot().await.unwrap();
        assert_eq!(snapshot.last_included_index, 4);
        let restored = VibesqlStateMachine::from_snapshot(&snapshot).unwrap();
        assert_eq!(
            restored.query("SELECT id, name FROM users ORDER BY id").unwrap(),
            node.query("SELECT id, name FROM users ORDER BY id").unwrap()
        );
        assert_eq!(restored.last_applied(), 4);
    }

    /// 3-node acceptance: leader INSERT/UPDATE/DELETE replicate to every
    /// voter — same rows, and (with MVCC on) the same `xmin` stamps,
    /// because commit_ts = dense log index on every replica.
    #[tokio::test]
    async fn three_node_dml_converges_with_identical_commit_timestamps() {
        let cluster = MvccCluster::new(3).await;

        for (expected_idx, sql) in [
            (1, "CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)"),
            (2, "INSERT INTO users VALUES (1, 'alice')"),
            (3, "INSERT INTO users VALUES (2, 'bob')"),
            (4, "UPDATE users SET name = 'bobby' WHERE id = 2"),
            (5, "DELETE FROM users WHERE id = 1"),
        ] {
            let (idx, outcome) = cluster.execute_on_leader(sql).await;
            assert_eq!(idx, expected_idx, "{sql}");
            assert!(matches!(outcome, ApplyOutcome::Applied { .. }), "{sql}: {outcome:?}");
        }

        for id in 1..=3 {
            cluster.wait_for_apply(id, 5).await;
        }
        let rows = cluster.assert_converged("SELECT id, name FROM users ORDER BY id");
        assert_eq!(rows, vec![vec![SqlValue::Integer(2), SqlValue::Varchar("bobby".into())]]);

        // commit_ts = dense log index, identical on every node: bobby's
        // live version was written by entry 4 everywhere.
        #[cfg(feature = "mvcc_enabled")]
        for id in 1..=3 {
            cluster.node(id).machine().with_db(|db| {
                let table = db.get_table("users").unwrap();
                let bobby = table
                    .scan()
                    .iter()
                    .find(|r| r.values[0] == SqlValue::Integer(2) && r.xmax.is_none())
                    .expect("bobby's live version")
                    .xmin;
                assert_eq!(bobby, 4, "node {id}: xmin must equal the entry's dense log index");
            });
        }
    }

    /// Freeze determinism multi-node (#5377 end-to-end): volatile
    /// expressions are evaluated once on the leader and every voter
    /// stores the identical drawn values.
    #[tokio::test]
    async fn nondeterministic_sql_converges_identically_on_all_nodes() {
        let cluster = MvccCluster::new(3).await;

        cluster
            .execute_on_leader(
                "CREATE TABLE t (id INTEGER PRIMARY KEY, r INTEGER, b BLOB, ts TIMESTAMP, d DATE)",
            )
            .await;
        let (_, outcome) = cluster
            .execute_on_leader(
                "INSERT INTO t VALUES (1, random(), randomblob(8), CURRENT_TIMESTAMP, curdate())",
            )
            .await;
        assert_eq!(outcome, ApplyOutcome::Applied { rows_affected: 1 });
        let (idx, outcome) =
            cluster.execute_on_leader("INSERT INTO t (id, ts) VALUES (2, datetime('now'))").await;
        assert_eq!(outcome, ApplyOutcome::Applied { rows_affected: 1 });

        for id in 1..=3 {
            cluster.wait_for_apply(id, idx).await;
        }
        let rows = cluster.assert_converged("SELECT id, r, b, ts, d FROM t ORDER BY id");
        assert_eq!(rows.len(), 2);
        assert_ne!(rows[0][1], SqlValue::Null, "random() must have been frozen to a value");
        assert_ne!(rows[0][3], SqlValue::Null, "CURRENT_TIMESTAMP must have been frozen");
    }

    /// A deterministic statement failure (constraint violation) is a
    /// *rejected entry*: it consumes its dense index on every voter, the
    /// rejection is identical everywhere, and the cluster keeps going.
    #[tokio::test]
    async fn deterministic_rejection_is_identical_on_every_node() {
        let cluster = MvccCluster::new(3).await;
        cluster.execute_on_leader("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)").await;
        cluster.execute_on_leader("INSERT INTO users VALUES (1, 'alice')").await;

        let (idx, outcome) =
            cluster.execute_on_leader("INSERT INTO users VALUES (1, 'dupe')").await;
        assert_eq!(idx, 3);
        assert!(matches!(outcome, ApplyOutcome::Rejected { .. }), "got: {outcome:?}");

        // Every voter consumed the rejected entry's index...
        for id in 1..=3 {
            cluster.wait_for_apply(id, 3).await;
        }
        // ...and the log keeps numbering past it.
        let (idx, outcome) =
            cluster.execute_on_leader("INSERT INTO users VALUES (2, 'carol')").await;
        assert_eq!(idx, 4);
        assert_eq!(outcome, ApplyOutcome::Applied { rows_affected: 1 });

        for id in 1..=3 {
            cluster.wait_for_apply(id, 4).await;
            assert_eq!(cluster.node(id).last_applied(), 4, "node {id}");
        }
        let rows = cluster.assert_converged("SELECT id, name FROM users ORDER BY id");
        assert_eq!(rows.len(), 2, "the rejected entry must have applied nothing anywhere");
    }

    /// Kill the leader mid-stream: the survivors elect a new leader and
    /// writes continue (dense indices stay contiguous — the new leader's
    /// blank protocol entry consumes no dense index, so commit timestamps
    /// stay aligned across replicas); the killed node restores and
    /// converges via log replay.
    #[tokio::test]
    async fn killing_the_leader_keeps_writes_flowing_and_restored_node_converges() {
        let mut cluster = MvccCluster::new(3).await;
        let leader = cluster.wait_for_leader().await;

        cluster.execute_on_leader("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)").await;
        cluster.execute_on_leader("INSERT INTO users VALUES (1, 'alice')").await;
        for id in 1..=3 {
            cluster.wait_for_apply(id, 2).await;
        }

        cluster.kill(leader).await;
        let new_leader = cluster.wait_for_leader().await;
        assert_ne!(new_leader, leader, "the killed node cannot lead");

        let (idx, _) = cluster.execute_on_leader("INSERT INTO users VALUES (2, 'bob')").await;
        assert_eq!(idx, 3, "dense indices stay contiguous across the failover");
        let (idx, _) = cluster.execute_on_leader("INSERT INTO users VALUES (3, 'carol')").await;
        assert_eq!(idx, 4);

        cluster.restore(leader).await;
        cluster.wait_for_apply(leader, 4).await;
        let rows = cluster.assert_converged("SELECT id, name FROM users ORDER BY id");
        assert_eq!(rows.len(), 3);

        // The restored node replayed the full log into a fresh database:
        // its commit timestamps match everyone else's.
        #[cfg(feature = "mvcc_enabled")]
        cluster.node(leader).machine().with_db(|db| {
            let table = db.get_table("users").unwrap();
            let carol = table
                .scan()
                .iter()
                .find(|r| r.values[0] == SqlValue::Integer(3) && r.xmax.is_none())
                .expect("carol's live version")
                .xmin;
            assert_eq!(carol, 4, "replayed write keeps commit_ts = dense log index");
        });
    }

    /// Leader-only writes: a follower fails fast with `NotLeader` and a
    /// hint at who leads, before consuming any log index (and before
    /// evaluating any volatile expression).
    #[tokio::test]
    async fn follower_writes_are_rejected_with_a_leader_hint() {
        let cluster = MvccCluster::new(3).await;
        let leader = cluster.wait_for_leader().await;
        let follower = follower_of(&cluster, leader);

        // Wait until the follower has heard from the leader, so the hint
        // is deterministic rather than `None`.
        cluster
            .wait_until("the follower to learn who leads", || {
                (cluster.node(follower).current_leader() == Some(leader)).then_some(())
            })
            .await;

        let err = cluster
            .node(follower)
            .execute_replicated("INSERT INTO nowhere VALUES (random())")
            .await
            .unwrap_err();
        match err {
            ConsensusError::NotLeader { leader_hint } => assert_eq!(leader_hint, Some(leader)),
            other => panic!("expected NotLeader with a hint, got: {other:?}"),
        }
        for id in 1..=3 {
            assert_eq!(cluster.node(id).last_applied(), 0, "no log index may be consumed");
        }
    }

    /// The FatalApply → halt obligation, end-to-end through the real
    /// fatal path: an entry with a frozen-site/value count mismatch
    /// (proposer/applier version skew) halts every node that applies it
    /// — openraft never advances `last_applied` past it, nothing is
    /// recorded as a rejection, and the fatal reason is retained.
    #[tokio::test]
    async fn poisoned_entry_halts_nodes_without_consuming_the_index() {
        let cluster = MvccCluster::new(3).await;
        cluster.execute_on_leader("CREATE TABLE t (id INTEGER PRIMARY KEY, r INTEGER)").await;
        for id in 1..=3 {
            cluster.wait_for_apply(id, 1).await;
        }
        let leader = cluster.wait_for_leader().await;
        let raft_before = cluster.node(leader).sm_raft_last_applied();

        // Version-skewed entry: one volatile site, two frozen values.
        let poisoned = TxnEntry::frozen_batch(
            vec!["INSERT INTO t VALUES (1, random())".to_string()],
            vec![vec![FrozenValue::Integer(7), FrozenValue::Integer(8)]],
        );
        let err = cluster.node(leader).propose_entry(poisoned).await.unwrap_err();
        assert!(
            matches!(err, ConsensusError::FatalApply(_)),
            "the write must surface the halt, got: {err:?}"
        );

        // The leader halted at apply; the surviving followers commit the
        // entry (it reached a quorum), elect a new leader among
        // themselves, and halt the same way when *they* apply it. The
        // whole cluster stops — an outage, never divergence.
        for id in 1..=3 {
            cluster
                .wait_until(&format!("node {id} to halt on the poisoned entry"), || {
                    cluster.node(id).fatal_reason().map(|_| ())
                })
                .await;
        }
        for id in 1..=3 {
            assert_eq!(
                cluster.node(id).last_applied(),
                1,
                "node {id}: a fatal apply must not consume the dense index"
            );
            let reason = cluster.node(id).fatal_reason().unwrap();
            assert!(reason.contains("halting this node"), "node {id}: {reason}");
        }
        assert_eq!(
            cluster.node(leader).sm_raft_last_applied(),
            raft_before,
            "openraft must not advance last_applied past the fatal entry"
        );

        // A halted node acknowledges nothing further.
        let err = cluster
            .node(leader)
            .execute_replicated("INSERT INTO t VALUES (2, 2)")
            .await
            .unwrap_err();
        assert!(
            matches!(err, ConsensusError::FatalApply(_) | ConsensusError::NotLeader { .. }),
            "writes after the halt must fail, got: {err:?}"
        );
    }

    /// A *node-local* fatal failure (injected) halts only the node that
    /// hit it; the quorum keeps committing. Restarting the halted node
    /// resyncs it via log replay — the documented recovery path.
    #[tokio::test]
    async fn injected_fatal_halts_one_node_and_restart_resyncs_it() {
        let mut cluster = MvccCluster::new(3).await;
        cluster.execute_on_leader("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)").await;
        for id in 1..=3 {
            cluster.wait_for_apply(id, 1).await;
        }
        let leader = cluster.wait_for_leader().await;
        let victim = follower_of(&cluster, leader);
        cluster.node(victim).inject_fatal_at(2);

        // The quorum (leader + the other follower) applies fine.
        let (idx, outcome) = cluster.execute_on_leader("INSERT INTO t VALUES (1, 'alive')").await;
        assert_eq!(idx, 2);
        assert_eq!(outcome, ApplyOutcome::Applied { rows_affected: 1 });

        cluster
            .wait_until("the victim to halt on the injected failure", || {
                cluster.node(victim).fatal_reason().map(|_| ())
            })
            .await;
        assert_eq!(
            cluster.node(victim).last_applied(),
            1,
            "the halted node must not have applied past the fatal entry"
        );

        // Writes keep flowing on the healthy majority.
        let (idx, _) = cluster.execute_on_leader("INSERT INTO t VALUES (2, 'still alive')").await;
        assert_eq!(idx, 3);

        // Restart the halted node: fresh database, kept log → full
        // replay → convergence (snapshot+replay is exercised separately).
        cluster.kill(victim).await;
        cluster.restore(victim).await;
        cluster.wait_for_apply(victim, 3).await;
        assert!(cluster.node(victim).fatal_reason().is_none(), "restarted node is healthy");
        let rows = cluster.assert_converged("SELECT id, v FROM t ORDER BY id");
        assert_eq!(rows.len(), 2);
    }

    /// The snapshot path end-to-end on durable nodes: policy-driven MVCC
    /// snapshots + log purge run past a dead follower; the restored
    /// follower heals via chunked MVCC snapshot install (its gap is
    /// purged, so log replay alone cannot), converges, and a subsequent
    /// restart recovers cleanly from its own durable snapshot.
    #[tokio::test]
    async fn purged_follower_heals_via_mvcc_snapshot_install_and_restarts_cleanly() {
        let tuning =
            RaftTuning { snapshot_after_entries: 4, keep_in_log: 1, ..RaftTuning::default() };
        let mut cluster = MvccCluster::new_durable(3, tuning).await;

        cluster.execute_on_leader("CREATE TABLE kv (id INTEGER PRIMARY KEY, v TEXT)").await;
        cluster.execute_on_leader("INSERT INTO kv VALUES (1, 'one')").await;
        for id in 1..=3 {
            cluster.wait_for_apply(id, 2).await;
        }

        let leader = cluster.wait_for_leader().await;
        let follower = follower_of(&cluster, leader);
        let follower_raw =
            cluster.node(follower).sm_raft_last_applied().expect("follower applied something");
        cluster.kill(follower).await;

        // Push enough writes for the snapshot/purge policy to discard the
        // dead follower's catch-up gap.
        let mut last_dense = 0;
        for i in 2..=12u64 {
            let (idx, outcome) =
                cluster.execute_on_leader(&format!("INSERT INTO kv VALUES ({i}, 'v{i}')")).await;
            assert!(matches!(outcome, ApplyOutcome::Applied { .. }));
            last_dense = idx;
        }
        cluster
            .wait_until("the leader to purge past the dead follower", || {
                let leader = cluster
                    .live_ids()
                    .into_iter()
                    .find(|id| cluster.node(*id).role() == Role::Leader)?;
                cluster
                    .node(leader)
                    .purged_log_index()
                    .is_some_and(|purged| purged > follower_raw)
                    .then_some(())
            })
            .await;

        // The restored follower cannot be backfilled by log replay (its
        // gap is purged): it must heal via MVCC snapshot install.
        cluster.restore(follower).await;
        cluster.wait_for_apply(follower, last_dense).await;
        assert!(
            cluster.node(follower).snapshot_log_index().is_some_and(|snap| snap > follower_raw),
            "the follower must have installed a snapshot covering its purged gap"
        );
        cluster.assert_converged("SELECT id, v FROM kv ORDER BY id");

        // Post-install writes keep the commit_ts mapping on the healed
        // node (snapshot install seeded the dense cursor + allocator).
        let (idx, _) = cluster.execute_on_leader("INSERT INTO kv VALUES (99, 'post')").await;
        cluster.wait_for_apply(follower, idx).await;
        #[cfg(feature = "mvcc_enabled")]
        cluster.node(follower).machine().with_db(|db| {
            let table = db.get_table("kv").unwrap();
            let post = table
                .scan()
                .iter()
                .find(|r| r.values[0] == SqlValue::Integer(99))
                .expect("post-install row")
                .xmin;
            assert_eq!(post, idx, "post-install applies keep commit_ts = dense log index");
        });

        // Subsequent restart is clean: the follower recovers from its own
        // durable snapshot + log suffix and converges again.
        cluster.kill(follower).await;
        cluster.restore(follower).await;
        cluster.wait_for_apply(follower, idx).await;
        cluster.assert_converged("SELECT id, v FROM kv ORDER BY id");
    }

    // -----------------------------------------------------------------
    // Raft Phase B2, PR 1 (#5200): linearizable leader reads
    // (the partition/fencing acceptance runs over real sockets in
    // tests/leader_reads.rs; these cover the read path's plumbing)
    // -----------------------------------------------------------------

    /// Read-your-writes through the linearizable path on a single voter
    /// (quorum of one): a just-acknowledged write is visible, and the
    /// linearizable result matches the local read exactly.
    #[tokio::test]
    async fn single_node_linearizable_read_sees_own_write() {
        let node = MvccRaftNode::single_node().await.unwrap();
        node.execute_replicated("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)").await.unwrap();
        node.execute_replicated("INSERT INTO t VALUES (1, 'one')").await.unwrap();

        let rows = node.query_linearizable("SELECT id, v FROM t").await.unwrap();
        assert_eq!(rows, vec![vec![SqlValue::Integer(1), SqlValue::Varchar("one".into())]]);
        assert_eq!(rows, node.query("SELECT id, v FROM t").unwrap());
    }

    /// A follower never serves a linearizable read: it redirects with a
    /// leader hint, while its plain local read keeps working
    /// (stale-allowed by contract).
    #[tokio::test]
    async fn linearizable_read_on_follower_redirects_to_the_leader() {
        let cluster = MvccCluster::new(3).await;
        let (idx, _) = cluster.execute_on_leader("CREATE TABLE t (id INTEGER PRIMARY KEY)").await;

        let leader = cluster.wait_for_leader().await;
        let follower = follower_of(&cluster, leader);
        // Wait until the follower has heard from the leader, so the hint
        // is deterministic rather than `None`.
        cluster
            .wait_until("the follower to learn who leads", || {
                (cluster.node(follower).current_leader() == Some(leader)).then_some(())
            })
            .await;

        let err = cluster.node(follower).query_linearizable("SELECT id FROM t").await.unwrap_err();
        match err {
            ConsensusError::NotLeader { leader_hint } => assert_eq!(leader_hint, Some(leader)),
            other => panic!("expected NotLeader with a hint, got: {other:?}"),
        }

        // The stale-allowed local read stays available on the follower.
        cluster.wait_for_apply(follower, idx).await;
        assert_eq!(
            cluster.node(follower).query("SELECT id FROM t").unwrap(),
            Vec::<Vec<SqlValue>>::new()
        );

        // And the leader serves the linearizable read it just confirmed.
        let rows = cluster.node(leader).query_linearizable("SELECT id FROM t").await.unwrap();
        assert!(rows.is_empty(), "no rows inserted yet, got: {rows:?}");
    }

    // -----------------------------------------------------------------
    // Raft Phase B2, PR 2 (#5200): read-your-writes tokens +
    // bounded-staleness follower reads (the partition scenarios run
    // over real sockets in tests/follower_reads.rs; these cover the
    // plumbing and the mixed-version / idle-cluster contracts)
    // -----------------------------------------------------------------

    /// The read-your-writes contract, clock-free: write on the leader,
    /// carry the returned dense index as the session token, and
    /// `query_at_least` on ANY node observes the write — no manual
    /// apply-waiting by the caller.
    #[tokio::test]
    async fn read_your_writes_token_serves_on_every_node() {
        let cluster = MvccCluster::new(3).await;
        cluster.execute_on_leader("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)").await;
        let (token, _) = cluster.execute_on_leader("INSERT INTO t VALUES (1, 'one')").await;

        for id in 1..=3 {
            let rows = cluster
                .node(id)
                .query_at_least(token, "SELECT id, v FROM t", WAIT_TIMEOUT)
                .await
                .unwrap_or_else(|e| panic!("node {id} must serve the token read: {e:?}"));
            assert_eq!(rows, vec![vec![SqlValue::Integer(1), SqlValue::Varchar("one".into())]]);
        }
    }

    /// A token this node cannot reach within the caller's bound is a
    /// typed, loud timeout — applied cursor and leader hint attached —
    /// never a hang and never a stale serve.
    #[tokio::test]
    async fn query_at_least_times_out_with_a_typed_error() {
        let cluster = MvccCluster::new(3).await;
        let (token, _) = cluster.execute_on_leader("CREATE TABLE t (id INTEGER PRIMARY KEY)").await;
        let leader = cluster.wait_for_leader().await;
        let follower = follower_of(&cluster, leader);
        cluster.wait_for_apply(follower, token).await;

        let err = cluster
            .node(follower)
            .query_at_least(token + 100, "SELECT id FROM t", Duration::from_millis(100))
            .await
            .unwrap_err();
        match err {
            ConsensusError::ReadTimeout { required, applied, .. } => {
                assert_eq!(required, token + 100);
                assert!(applied < required, "applied {applied} must trail the token");
            }
            other => panic!("expected ReadTimeout, got: {other:?}"),
        }
    }

    /// Unknown staleness refuses: a fresh node (only protocol entries
    /// applied — no stamped entry yet) must not serve bounded reads, and
    /// a node that itself believes it leads must not hint at itself. The
    /// first stamped write then makes bounded reads serviceable.
    #[tokio::test]
    async fn bounded_staleness_refuses_until_a_stamped_entry_applies() {
        let node = MvccRaftNode::single_node().await.unwrap();

        let err =
            node.query_bounded_staleness(Duration::from_secs(10), "SELECT 1").await.unwrap_err();
        match err {
            ConsensusError::StalenessExceeded { observed_ms, leader_hint, .. } => {
                assert_eq!(observed_ms, None, "no stamped entry applied → unknown staleness");
                assert_eq!(leader_hint, None, "a leader must not hint at itself");
            }
            other => panic!("expected StalenessExceeded, got: {other:?}"),
        }

        node.execute_replicated("CREATE TABLE t (id INTEGER PRIMARY KEY)").await.unwrap();
        let rows = node
            .query_bounded_staleness(Duration::from_secs(10), "SELECT id FROM t")
            .await
            .expect("a just-stamped node serves bounded reads");
        assert!(rows.is_empty());
    }

    /// A zero bound is the linearizable path: the leader serves it (with
    /// full ReadIndex confirmation), so `max_staleness = 0` never serves
    /// from an unconfirmed local state.
    #[tokio::test]
    async fn bounded_staleness_zero_is_linearizable_on_the_leader() {
        let node = MvccRaftNode::single_node().await.unwrap();
        node.execute_replicated("CREATE TABLE t (id INTEGER PRIMARY KEY)").await.unwrap();
        node.execute_replicated("INSERT INTO t VALUES (7)").await.unwrap();

        let rows = node.query_bounded_staleness(Duration::ZERO, "SELECT id FROM t").await.unwrap();
        assert_eq!(rows, vec![vec![SqlValue::Integer(7)]]);
    }

    /// Pre-#5200 (unstamped) entries through the real mount: the dense
    /// cursor advances — read-your-writes tokens keep working — but
    /// staleness stays unknown, so bounded reads refuse until the first
    /// stamped entry arrives (the documented mixed-version behavior).
    #[tokio::test]
    async fn unstamped_entry_advances_tokens_but_not_staleness() {
        let node = MvccRaftNode::single_node().await.unwrap();

        // Bypass the (stamping) propose path: an old proposer's entry.
        let old_format =
            serde_json::to_vec(&TxnEntry::single("CREATE TABLE t (id INTEGER PRIMARY KEY)"))
                .unwrap();
        node.raft.client_write(old_format).await.unwrap();

        // Token reads work — clock-free, index-based.
        let rows = node.query_at_least(1, "SELECT id FROM t", WAIT_TIMEOUT).await.unwrap();
        assert!(rows.is_empty());

        // Bounded reads refuse — staleness unknown.
        let err = node
            .query_bounded_staleness(Duration::from_secs(10), "SELECT id FROM t")
            .await
            .unwrap_err();
        assert!(
            matches!(err, ConsensusError::StalenessExceeded { observed_ms: None, .. }),
            "got: {err:?}"
        );

        // A stamped entry restores bounded-read service.
        node.execute_replicated("INSERT INTO t VALUES (1)").await.unwrap();
        node.query_bounded_staleness(Duration::from_secs(10), "SELECT id FROM t")
            .await
            .expect("a stamped entry restores bounded reads");
    }

    /// Idle aging without the beacon (the default): once the last stamp
    /// is older than the caller's bound, a healthy but idle follower
    /// refuses bounded reads — with a leader hint for redirect routing —
    /// while its token/local reads keep serving (those are clock-free).
    #[tokio::test]
    async fn idle_cluster_without_beacon_ages_out_of_bounded_reads() {
        let cluster = MvccCluster::new(3).await;
        cluster.execute_on_leader("CREATE TABLE t (id INTEGER PRIMARY KEY)").await;
        let (token, _) = cluster.execute_on_leader("INSERT INTO t VALUES (1)").await;
        let leader = cluster.wait_for_leader().await;
        let follower = follower_of(&cluster, leader);
        cluster.wait_for_apply(follower, token).await;
        cluster
            .wait_until("the follower to learn who leads", || {
                (cluster.node(follower).current_leader() == Some(leader)).then_some(())
            })
            .await;

        // No writes from here on: the stamp can only age. Poll until the
        // 150ms bound is exceeded (bounded — fails loudly, never hangs).
        let bound = Duration::from_millis(150);
        let deadline = tokio::time::Instant::now() + WAIT_TIMEOUT;
        loop {
            match cluster.node(follower).query_bounded_staleness(bound, "SELECT id FROM t").await {
                Err(ConsensusError::StalenessExceeded {
                    observed_ms: Some(observed),
                    max_staleness_ms,
                    leader_hint,
                }) => {
                    assert!(observed > max_staleness_ms, "{observed} vs {max_staleness_ms}");
                    assert_eq!(leader_hint, Some(leader), "refusals must carry the redirect");
                    break;
                }
                Ok(_) => {} // still within the bound — keep aging
                Err(other) => panic!("expected StalenessExceeded, got: {other:?}"),
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for the idle follower to age out of the bound"
            );
            tokio::time::sleep(POLL_INTERVAL).await;
        }

        // The clock-free paths are unaffected by idleness.
        let rows = cluster
            .node(follower)
            .query_at_least(token, "SELECT id FROM t", WAIT_TIMEOUT)
            .await
            .unwrap();
        assert_eq!(rows, vec![vec![SqlValue::Integer(1)]]);
    }

    /// The opt-in staleness beacon keeps an IDLE cluster's bounded reads
    /// alive: with no application writes for many beacon windows, a
    /// follower still proves freshness within a 200ms bound, because the
    /// leader keeps proposing stamped no-op entries (which consume dense
    /// indices like any entry).
    #[tokio::test]
    async fn staleness_beacon_keeps_idle_cluster_bounded_reads_fresh() {
        let tuning = RaftTuning { staleness_beacon_ms: 50, ..RaftTuning::default() };
        let cluster = MvccCluster::build(3, false, tuning).await;
        cluster.execute_on_leader("CREATE TABLE t (id INTEGER PRIMARY KEY)").await;
        let (token, _) = cluster.execute_on_leader("INSERT INTO t VALUES (1)").await;
        let leader = cluster.wait_for_leader().await;
        let follower = follower_of(&cluster, leader);
        cluster.wait_for_apply(follower, token).await;

        // Go idle for far longer than the 200ms bound the last real
        // write's stamp could cover on its own.
        tokio::time::sleep(Duration::from_millis(400)).await;

        // Poll (bounded) until a bounded read succeeds: without the
        // beacon the stamp could only age further, so success here
        // proves a beacon-refreshed stamp reached the follower.
        let bound = Duration::from_millis(200);
        let deadline = tokio::time::Instant::now() + WAIT_TIMEOUT;
        loop {
            match cluster.node(follower).query_bounded_staleness(bound, "SELECT id FROM t").await {
                Ok(rows) => {
                    assert_eq!(rows, vec![vec![SqlValue::Integer(1)]]);
                    break;
                }
                Err(ConsensusError::StalenessExceeded { .. }) => {} // beacon not landed yet
                Err(other) => panic!("expected rows or StalenessExceeded, got: {other:?}"),
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out waiting for the beacon to refresh the idle follower"
            );
            tokio::time::sleep(POLL_INTERVAL).await;
        }

        // Beacons are real (no-op) entries: the dense cursor moved past
        // the last application write on every replica identically.
        assert!(
            cluster.node(follower).last_applied() > token,
            "beacon entries must have consumed dense indices"
        );
    }
}
