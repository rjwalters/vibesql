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
//! Phase A3 (PR 1 of #5197) adds **in-process multi-node replication for
//! tests**: a channel-based implementation of openraft's network traits
//! (the `network` module) plus a cluster harness with kill/restore failure
//! injection (the `cluster` module). Both are test-only; nothing about them
//! is public API.
//!
//! Deliberately **not** here yet (later Raft phases): a real TCP transport
//! and process-level clusters (Phase A3, PR 2), snapshot transfer and
//! truncation *policy* (Phase A4; the storage-level truncate/purge hooks
//! exist), applying entries to VibeSQL storage (Phase B1), and membership
//! changes.
//!
//! [ADR-0004]: https://github.com/rjwalters/vibesql/blob/main/docs/decisions/0004-consensus-library.md

mod backend;
#[cfg(test)]
mod cluster;
#[cfg(test)]
mod conformance;
mod durable;
#[cfg(test)]
mod network;
mod openraft_backend;
mod single_node;

pub use backend::{ConsensusBackend, ConsensusError, LogIndex, Result, Role, Snapshot};
pub use openraft_backend::OpenraftBackend;
pub use single_node::SingleNodeBackend;
