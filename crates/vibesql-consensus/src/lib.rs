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
//! - [`ConsensusBackend`]: the engine-agnostic adapter trait
//!   (propose / read_committed / snapshot / role).
//! - [`SingleNodeBackend`]: an in-memory loopback implementation that
//!   commits every proposal immediately, so dev and unit tests can exercise
//!   the trait without standing up a multi-node cluster.
//! - [`OpenraftBackend`]: the real engine, wired as a single-node Raft
//!   group (#5196). Proposals flow through openraft's actual
//!   append → commit → apply pipeline. [`OpenraftBackend::new`] keeps the
//!   Raft log in memory; [`OpenraftBackend::with_data_dir`] persists the
//!   log and vote on disk (WAL-style framed records, fsynced before
//!   acknowledgment) and recovers them on restart, tolerating torn trailing
//!   writes.
//!
//! All backend configurations pass the same behavioral conformance suite
//! (the `conformance` test module), so consumers written against the trait
//! behave identically on any of them.
//!
//! Phase A3 (#5197) adds **multi-node replication**:
//!
//! - PR 1: a channel-based implementation of openraft's network traits (the
//!   `network` module) plus an in-process cluster harness with kill/restore
//!   failure injection (the `cluster` module). Both are test-only.
//! - PR 2: the **TCP transport** (the `tcp` module — length-prefixed frames
//!   on a dedicated consensus port, default [`DEFAULT_CONSENSUS_PORT`]),
//!   static membership via [`ClusterConfig`] (`cluster.toml`), and the
//!   cluster-level constructors
//!   [`OpenraftBackend::join_tcp_cluster`] /
//!   [`OpenraftBackend::join_tcp_cluster_with_data_dir`]. The
//!   `tcp_cluster` integration test (run by `make test-cluster`) exercises
//!   election, failover, restart catch-up, minority partitions, and
//!   torn/garbage frames over real sockets.
//!
//! Phase A4 (#5198), PR 1, makes snapshots real on a single node:
//!
//! - [`ConsensusBackend::snapshot`] on [`OpenraftBackend`] flows through
//!   openraft's `RaftSnapshotBuilder` (the `snapshot` module holds the
//!   payload codec and the durable [`SnapshotStore`](snapshot) machinery).
//! - Durable configurations persist each built/installed snapshot to the
//!   data directory (tmp/rename atomic, CRC-framed) and recover **snapshot
//!   first, then log replay**; a corrupt snapshot file fails recovery
//!   loudly rather than silently starting fresh.
//! - Snapshot builds pin the MVCC vacuum horizon through a hook
//!   (`SnapshotHorizonPin`) that is a no-op until Phase B1 wires the real
//!   state machine.
//! - Log purge is storage-enforced to never exceed the last durable
//!   snapshot's index.
//!
//! Phase A4, PR 2, completes snapshots across the network:
//!
//! - **Snapshot transfer**: a lagging node whose gap was purged on the
//!   leader is healed by chunked snapshot streaming over the existing
//!   `InstallSnapshot` RPC leg (design in the `tcp` module docs); the frame
//!   limit bounds a chunk, never the snapshot.
//! - **Purge policy** ([`RaftTuning`]): automatic snapshots every N
//!   entries and automatic purge keeping a catch-up tail, exposed on the
//!   durable constructors
//!   ([`OpenraftBackend::with_data_dir_tuned`] /
//!   [`OpenraftBackend::join_tcp_cluster_with_data_dir_tuned`]). Purge
//!   compacts `raft.log`, physically reclaiming the purged prefix.
//! - **Interrupted-install recovery**: a follower that crashes between
//!   durably installing a snapshot and recording the follow-up log purge
//!   restarts cleanly (recovery repairs the log from the snapshot); a
//!   snapshot next to a log with no state at all still fails loudly.
//!
//! Deliberately **not** here yet (later Raft phases): applying entries
//! to VibeSQL storage (Phase B1), TLS on the consensus port, production
//! wiring into `vibesql-server`, and membership changes.
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
#[cfg(test)]
mod network;
mod openraft_backend;
mod single_node;
mod snapshot;
mod tcp;

pub use backend::{ConsensusBackend, ConsensusError, LogIndex, Result, Role, Snapshot};
pub use cluster_config::{ClusterConfig, DEFAULT_CONSENSUS_PORT};
pub use openraft_backend::{OpenraftBackend, RaftTuning};
pub use single_node::SingleNodeBackend;
