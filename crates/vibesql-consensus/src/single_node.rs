//! [`SingleNodeBackend`]: an in-process loopback implementation of
//! [`ConsensusBackend`] that commits every proposal immediately.
//!
//! This exists so dev and unit tests can exercise the adapter trait without
//! standing up a multi-node cluster (see ADR-0004, "Single-Node Loopback
//! Plan"). It is **not** a replication mechanism: there is exactly one node,
//! it is always the leader, and the log lives in memory.

use std::sync::Mutex;

use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::backend::{ConsensusBackend, ConsensusError, LogIndex, Result, Role, Snapshot};

/// Single-node, in-memory [`ConsensusBackend`].
///
/// Proposals are appended to a `Vec`-backed log under a mutex and are
/// considered committed as soon as `propose` returns. [`Role::Leader`] is
/// always reported.
///
/// Snapshots encode the entire log as JSON. This is a stand-in format for
/// tests only; real snapshot encoding is decided when WAL wiring lands.
#[derive(Debug)]
pub struct SingleNodeBackend<E> {
    /// Committed log entries; entry `i` of the vec holds log index `i + 1`
    /// (log indices are 1-based per Raft convention).
    log: Mutex<Vec<E>>,
}

impl<E> SingleNodeBackend<E> {
    /// Create a backend with an empty log.
    pub fn new() -> Self {
        Self { log: Mutex::new(Vec::new()) }
    }

    /// The index of the most recently committed entry (`0` if the log is
    /// empty).
    pub fn last_index(&self) -> LogIndex {
        self.log.lock().expect("consensus log mutex poisoned").len() as LogIndex
    }
}

impl<E: DeserializeOwned> SingleNodeBackend<E> {
    /// Rebuild a backend from a snapshot previously produced by
    /// [`ConsensusBackend::snapshot`] on a `SingleNodeBackend`.
    ///
    /// This is an inherent method rather than part of the trait: snapshot
    /// *installation* hooks belong to later Raft phases, but the loopback
    /// backend needs a way to prove snapshot roundtrips in unit tests.
    pub fn from_snapshot(snapshot: &Snapshot) -> Result<Self> {
        let log: Vec<E> = serde_json::from_slice(&snapshot.data)
            .map_err(|e| ConsensusError::SnapshotCodec(e.to_string()))?;
        if log.len() as LogIndex != snapshot.last_included_index {
            return Err(ConsensusError::SnapshotCodec(format!(
                "snapshot claims last_included_index {} but contains {} entries",
                snapshot.last_included_index,
                log.len()
            )));
        }
        Ok(Self { log: Mutex::new(log) })
    }
}

impl<E> Default for SingleNodeBackend<E> {
    fn default() -> Self {
        Self::new()
    }
}

impl<E> ConsensusBackend for SingleNodeBackend<E>
where
    E: Serialize + DeserializeOwned + Clone + Send,
{
    type Entry = E;

    async fn propose(&self, entry: E) -> Result<LogIndex> {
        let mut log = self.log.lock().expect("consensus log mutex poisoned");
        log.push(entry);
        Ok(log.len() as LogIndex)
    }

    async fn read_committed(&self, idx: LogIndex) -> Result<E> {
        let log = self.log.lock().expect("consensus log mutex poisoned");
        if idx == 0 || idx > log.len() as LogIndex {
            return Err(ConsensusError::NotCommitted(idx));
        }
        Ok(log[(idx - 1) as usize].clone())
    }

    async fn snapshot(&self) -> Result<Snapshot> {
        let log = self.log.lock().expect("consensus log mutex poisoned");
        let data =
            serde_json::to_vec(&*log).map_err(|e| ConsensusError::SnapshotCodec(e.to_string()))?;
        Ok(Snapshot { last_included_index: log.len() as LogIndex, data })
    }

    fn role(&self) -> Role {
        Role::Leader
    }
}

// Behavioral tests for this backend live in the shared conformance suite
// (`crate::conformance`), which runs the same test bodies against every
// `ConsensusBackend` implementation.
