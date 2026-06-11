//! Consensus adapter layer for VibeSQL WAN replication.
//!
//! This crate is the stable seam between VibeSQL and whatever consensus
//! engine drives replication. [ADR-0004] selects `openraft` running as a
//! **single Raft group replicating the whole database** (the
//! rqlite/dqlite/Turso model), but consumers of this crate depend only on
//! the [`ConsensusBackend`] trait — never on the underlying library — so the
//! engine (or topology) can change without rewriting them.
//!
//! Phase A1 scaffolding (this crate, today):
//!
//! - [`ConsensusBackend`]: the engine-agnostic adapter trait
//!   (propose / read_committed / snapshot / role).
//! - [`SingleNodeBackend`]: an in-memory loopback implementation that
//!   commits every proposal immediately, so dev and unit tests can exercise
//!   the trait without standing up a multi-node cluster.
//!
//! Deliberately **not** here yet (later Raft phases): the `openraft`
//! dependency, multi-node replication, log persistence wired to the WAL,
//! membership changes, and any RPC/network code.
//!
//! [ADR-0004]: https://github.com/rjwalters/vibesql/blob/main/docs/decisions/0004-consensus-library.md

mod backend;
mod single_node;

pub use backend::{ConsensusBackend, ConsensusError, LogIndex, Result, Role, Snapshot};
pub use single_node::SingleNodeBackend;
