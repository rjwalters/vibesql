//! The engine-agnostic [`ConsensusBackend`] adapter trait and its companion
//! types. See ADR-0004 for the full design rationale.

use serde::de::DeserializeOwned;
use serde::Serialize;

/// Index of an entry in the replicated log.
///
/// Indices are **1-based**, following the Raft convention where index `0`
/// means "no entry" (the state before anything has been committed).
pub type LogIndex = u64;

/// Raft-style role of the local node within the consensus group.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum Role {
    /// This node sequences writes; proposals are accepted here.
    Leader,
    /// This node replicates the leader's log.
    Follower,
    /// This node is campaigning for leadership.
    Candidate,
}

/// An opaque snapshot of the replicated state machine.
///
/// The byte encoding of `data` is backend-specific and intentionally not
/// part of this contract; real snapshot encoding is decided when WAL wiring
/// lands (later Raft phases).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    /// The last log index whose effects are included in this snapshot.
    /// Entries at or below this index may be truncated from the log.
    pub last_included_index: LogIndex,
    /// Backend-specific serialized state.
    pub data: Vec<u8>,
}

/// Errors surfaced by a [`ConsensusBackend`].
#[derive(Debug, thiserror::Error)]
pub enum ConsensusError {
    /// The local node is not the leader; proposals must be retried against
    /// the leader (hint is the backend's best guess at its node id, if any).
    #[error("not the leader of the consensus group (leader hint: {leader_hint:?})")]
    NotLeader {
        /// Best-effort identifier of the current leader, if known.
        leader_hint: Option<u64>,
    },
    /// The requested log index has not been committed (or never existed).
    #[error("log index {0} is not committed")]
    NotCommitted(LogIndex),
    /// Snapshot serialization or deserialization failed.
    #[error("snapshot encode/decode failed: {0}")]
    SnapshotCodec(String),
    /// Catch-all for backend-internal failures.
    #[error("consensus backend error: {0}")]
    Backend(String),
}

/// Convenience alias used throughout this crate.
pub type Result<T> = std::result::Result<T, ConsensusError>;

/// Engine-agnostic consensus adapter.
///
/// Consumers (replication, server) depend on this trait, never on the
/// underlying consensus library, so the engine — `openraft` per ADR-0004 —
/// or even the replication topology can be swapped without rewriting them.
///
/// Native `async fn` in traits is used deliberately (stable since Rust
/// 1.75), and the `async_fn_in_trait` lint stays allowed. Phase A2 resolved
/// the question this lint flags: wiring the first real backend
/// (`OpenraftBackend`, openraft 0.9) required **no** `Send` bounds on the
/// returned futures and no dyn dispatch — openraft's `Raft` handle methods
/// already return `Send` futures, and current consumers await these methods
/// from generic (static-dispatch) contexts. If a later phase needs to hold a
/// backend behind `dyn` or `tokio::spawn` a future returned by a generic
/// `B: ConsensusBackend`, the anticipated fallback from ADR-0004 still
/// applies: switch these methods to explicit `impl Future + Send` returns or
/// boxed futures at that point.
#[allow(async_fn_in_trait)]
pub trait ConsensusBackend: Send + Sync {
    /// The log entry type replicated through consensus (e.g. a serialized
    /// transaction or write batch).
    type Entry: Serialize + DeserializeOwned + Send;

    /// Propose an entry for replication. Resolves with the entry's log
    /// index once the entry is **committed** (durable on a quorum).
    ///
    /// Returns [`ConsensusError::NotLeader`] if this node cannot sequence
    /// writes.
    async fn propose(&self, entry: Self::Entry) -> Result<LogIndex>;

    /// Read back a committed entry by log index.
    ///
    /// Returns [`ConsensusError::NotCommitted`] if `idx` is `0`, beyond the
    /// committed prefix, or otherwise unavailable.
    async fn read_committed(&self, idx: LogIndex) -> Result<Self::Entry>;

    /// Capture a snapshot of the committed state, suitable for log
    /// truncation and follower catch-up.
    async fn snapshot(&self) -> Result<Snapshot>;

    /// The current role of this node in the consensus group.
    fn role(&self) -> Role;
}
