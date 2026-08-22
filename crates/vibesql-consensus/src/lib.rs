//! Consensus adapter layer for VibeSQL WAN replication.
//!
//! This crate is the stable seam between VibeSQL and whatever consensus
//! engine drives replication. [ADR-0004] selects `openraft` running as a
//! **single Raft group replicating the whole database** (the
//! rqlite/dqlite/Turso model), but consumers of this crate depend only on
//! the [`ConsensusBackend`] trait — never on the underlying library — so the
//! engine (or topology) can change without rewriting them.
//!
//! What this crate provides today (Phase A1 + Phase A2):
//!
//! - [`ConsensusBackend`]: the engine-agnostic adapter trait (propose / read_committed / snapshot /
//!   role).
//! - [`SingleNodeBackend`]: an in-memory loopback implementation that commits every proposal
//!   immediately, so dev and unit tests can exercise the trait without standing up a multi-node
//!   cluster.
//! - [`OpenraftBackend`]: the real engine, wired as a single-node Raft group (#5196). Proposals
//!   flow through openraft's actual append → commit → apply pipeline. [`OpenraftBackend::new`]
//!   keeps the Raft log in memory; [`OpenraftBackend::with_data_dir`] persists the log and vote on
//!   disk (WAL-style framed records, fsynced before acknowledgment) and recovers them on restart,
//!   tolerating torn trailing writes.
//!
//! All backend configurations pass the same behavioral conformance suite
//! (the `conformance` test module), so consumers written against the trait
//! behave identically on any of them.
//!
//! Phase A3 (#5197) adds **multi-node replication**:
//!
//! - PR 1: a channel-based implementation of openraft's network traits (the `network` module) plus
//!   an in-process cluster harness with kill/restore failure injection (the `cluster` module). Both
//!   are test-only.
//! - PR 2: the **TCP transport** (the `tcp` module — length-prefixed frames on a dedicated
//!   consensus port, default [`DEFAULT_CONSENSUS_PORT`]), static membership via [`ClusterConfig`]
//!   (`cluster.toml`), and the cluster-level constructors [`OpenraftBackend::join_tcp_cluster`] /
//!   [`OpenraftBackend::join_tcp_cluster_with_data_dir`]. The `tcp_cluster` integration test (run
//!   by `make test-cluster`) exercises election, failover, restart catch-up, minority partitions,
//!   and torn/garbage frames over real sockets.
//!
//! Phase A4 (#5198), PR 1, makes snapshots real on a single node:
//!
//! - [`ConsensusBackend::snapshot`] on [`OpenraftBackend`] flows through openraft's
//!   `RaftSnapshotBuilder` (the `snapshot` module holds the payload codec and the durable
//!   [`SnapshotStore`](snapshot) machinery).
//! - Durable configurations persist each built/installed snapshot to the data directory (tmp/rename
//!   atomic, CRC-framed) and recover **snapshot first, then log replay**; a corrupt snapshot file
//!   fails recovery loudly rather than silently starting fresh.
//! - Snapshot builds pin the MVCC vacuum horizon through a hook (`SnapshotHorizonPin`) that is a
//!   no-op until Phase B1 wires the real state machine.
//! - Log purge is storage-enforced to never exceed the last durable snapshot's index.
//!
//! Phase A4, PR 2, completes snapshots across the network:
//!
//! - **Snapshot transfer**: a lagging node whose gap was purged on the leader is healed by chunked
//!   snapshot streaming over the existing `InstallSnapshot` RPC leg (design in the `tcp` module
//!   docs); the frame limit bounds a chunk, never the snapshot.
//! - **Purge policy** ([`RaftTuning`]): automatic snapshots every N entries and automatic purge
//!   keeping a catch-up tail, exposed on the durable constructors
//!   ([`OpenraftBackend::with_data_dir_tuned`] /
//!   [`OpenraftBackend::join_tcp_cluster_with_data_dir_tuned`]). Purge compacts `raft.log`,
//!   physically reclaiming the purged prefix.
//! - **Interrupted-install recovery**: a follower that crashes between durably installing a
//!   snapshot and recording the follow-up log purge restarts cleanly (recovery repairs the log from
//!   the snapshot); a snapshot next to a log with no state at all still fails loudly.
//!
//! Phase B1 (#5199), PR 1, applies committed entries to **real storage**:
//!
//! - [`TxnEntry`]: the replicated entry type — one committed transaction per log entry, as a
//!   deterministic SQL statement batch (see the `state_machine` module docs for why the
//!   statement-batch form was chosen over a row-level write-set in PR 1, and the tradeoff).
//! - [`VibesqlStateMachine`]: applies committed entries to a real `vibesql-storage` database with
//!   **commit_ts = log index** (the transaction applying entry `N` is stamped `TxnId = N`, per
//!   ADR-0004's "apply index = commit order"); apply is idempotent per index, so snapshot-install +
//!   log-replay overlap is safe. Its snapshots serialize the database state with the binary
//!   persistence codec, and builds pin the MVCC vacuum horizon through the real storage-layer
//!   holdback (`Database::pin_gc_horizon`) — the `SnapshotHorizonPin` seam from Phase A4, no longer
//!   a no-op.
//! - [`ReplicatedDb`]: the PR 1 propose path (test harness, not server wiring) — whole transactions
//!   proposed through a [`ConsensusBackend`] and applied on commit.
//!
//! The generic byte-echo configurations above remain unchanged (and keep
//! their conformance suite); the MVCC machine is a new configuration on
//! top of them.
//!
//! Phase B1, PR 2, mounts that machine **inside** openraft (the
//! `mvcc_raft` module):
//!
//! - [`MvccRaftNode`]: one voter of a multi-node cluster whose state machine is the MVCC database —
//!   apply happens in the engine's state-machine worker on every voter, with commit_ts = dense log
//!   index surviving leader changes (protocol entries consume no dense index).
//! - **Leader-only writes**: proposals anywhere but the leader fail fast with
//!   [`ConsensusError::NotLeader`] + leader hint; freeze-at-propose (#5377) always runs on the
//!   leader.
//! - **Fatal apply = node halt**: a possibly-node-local apply failure surfaces as a storage error
//!   to openraft, which shuts the node down *without* acknowledging the entry — `last_applied`
//!   never advances past it (the [`ConsensusError::FatalApply`] obligation).
//! - **MVCC snapshots across the network**: chunked install of the binary database payload heals
//!   followers that fell behind a purge, with the Phase A4 durability order (persist before
//!   acknowledge).
//!
//! Phase B2 (#5200) adds the **read paths** on [`MvccRaftNode`] (see the
//! `mvcc_raft` module docs' read-path sections for the full contracts):
//!
//! - PR 1: `query_linearizable` — openraft's ReadIndex protocol (clock-free quorum confirmation),
//!   with stale-leader fencing and no-self-hint discipline; plain `query` stays
//!   local/stale-allowed.
//! - PR 2: `query_at_least` — clock-free **read-your-writes** via the dense application index a
//!   propose returns, served on any node once it catches up; and `query_bounded_staleness` —
//!   follower reads bounded by the leader wall-clock stamp piggybacked on every proposed entry
//!   ([`TxnEntry::leader_time_ms`], serde-compatible both ways), refusing whenever the bound cannot
//!   be proven. Idle clusters either age out of bounded reads (default) or stay fresh via the
//!   opt-in staleness beacon ([`RaftTuning::staleness_beacon_ms`]).
//!
//! Deliberately **not** here yet (later Raft phases): TLS on the
//! consensus port, production wiring into `vibesql-server` (PostgreSQL
//! sessions do not yet route writes through [`MvccRaftNode`]; surfacing
//! `NotLeader` as a SQL error and mapping the `max_staleness_ms` PG
//! option onto `query_bounded_staleness` is follow-on work, #5383), the
//! lease fast path for linearizable reads (needs openraft 0.10's
//! `ReadPolicy::LeaseRead`), and membership changes.
//!
//! Note on retention: the Raft log purge gated above is orthogonal to the
//! pre-existing data-WAL checkpoint/truncation in `vibesql-storage::wal`
//! (LSN-based, `DEFAULT_SAFETY_BUFFER`). On a replicated node the **Raft
//! log is authoritative** (per the A2/B1 direction); the data WAL remains a
//! local durability detail of the storage engine.
//!
//! [ADR-0004]: https://github.com/rjwalters/vibesql/blob/main/docs/decisions/0004-consensus-library.md

mod backend;
#[cfg(test)]
mod cluster;
mod cluster_config;
#[cfg(test)]
mod conformance;
mod durable;
mod freeze;
mod mvcc_raft;
#[cfg(test)]
mod network;
mod openraft_backend;
mod replicated;
mod single_node;
mod snapshot;
mod state_machine;
mod tcp;

pub use backend::{ConsensusBackend, ConsensusError, LogIndex, Result, Role, Snapshot};
pub use cluster_config::{ClusterConfig, DEFAULT_CONSENSUS_PORT};
pub use freeze::{freeze_statement, FreezeError, FrozenValue, SubstituteError};
pub use mvcc_raft::MvccRaftNode;
pub use openraft_backend::{OpenraftBackend, RaftTuning};
pub use replicated::ReplicatedDb;
pub use single_node::SingleNodeBackend;
pub use state_machine::{ApplyOutcome, QueryResult, TxnEntry, VibesqlStateMachine};
