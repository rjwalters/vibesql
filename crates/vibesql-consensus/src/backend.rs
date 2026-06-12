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
    /// A read-your-writes read (Raft Phase B2, #5200, PR 2:
    /// `MvccRaftNode::query_at_least`) gave up waiting: this node's
    /// applied state had not reached the session token within the
    /// caller's bound. The local state is intact — the caller may retry
    /// here later, or route the read to the leader (`leader_hint`, when
    /// known), which by construction has applied every index it ever
    /// returned from a propose.
    #[error(
        "read-your-writes wait expired: this node has applied index {applied}, the session \
         token requires {required} (leader hint: {leader_hint:?})"
    )]
    ReadTimeout {
        /// The session token: the dense application index the read must
        /// observe (as returned by the write's propose).
        required: LogIndex,
        /// The dense application index this node had reached when the
        /// wait expired.
        applied: LogIndex,
        /// Best-effort identifier of the current leader, if known.
        leader_hint: Option<u64>,
    },
    /// A bounded-staleness read (Raft Phase B2, #5200, PR 2:
    /// `MvccRaftNode::query_bounded_staleness`) was refused because this
    /// node could not prove its applied state is no staler than the
    /// caller's bound. Route the read to the leader (`leader_hint`, when
    /// known) or retry with a larger bound.
    #[error(
        "bounded-staleness read refused: observed staleness {observed_ms:?}ms exceeds the \
         {max_staleness_ms}ms bound (None = unknown — no leader-stamped entry applied yet; \
         leader hint: {leader_hint:?})"
    )]
    StalenessExceeded {
        /// Milliseconds elapsed (per this node's clock) since the
        /// leader-clock stamp of the last stamped entry this node
        /// applied; `None` when no stamped entry has been applied yet
        /// (fresh node, pre-#5200 log prefix, or just after a snapshot
        /// install on a fresh node) — unknown staleness is treated as
        /// unbounded.
        observed_ms: Option<u64>,
        /// The caller's staleness bound, in milliseconds.
        max_staleness_ms: u64,
        /// Best-effort identifier of the current leader, if known.
        /// Deliberately `None` when the refusing node itself believes it
        /// leads: stale stamps on a "leader" suggest it has been deposed
        /// or partitioned, and hinting at itself would route callers
        /// straight back.
        leader_hint: Option<u64>,
    },
    /// Snapshot serialization or deserialization failed.
    #[error("snapshot encode/decode failed: {0}")]
    SnapshotCodec(String),
    /// The cluster configuration is invalid or could not be loaded.
    #[error("cluster configuration error: {0}")]
    Config(String),
    /// Applying a committed entry failed for a reason that may be local
    /// to this node — resource exhaustion, a storage failure, or a
    /// frozen-entry mismatch suggesting proposer/applier version skew
    /// (#5377). The state machine did **not** consume the entry's index.
    ///
    /// Unlike a deterministic statement failure (which every replica
    /// records identically as a rejected entry), this class of failure
    /// may not reproduce on other replicas: recording it as a rejection
    /// would silently diverge the cluster. The node must treat it as
    /// fatal — halt and recover via snapshot install + log replay.
    #[error("fatal apply failure (halt this node and resync; do not record a rejection): {0}")]
    FatalApply(String),
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
