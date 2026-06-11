//! [`OpenraftBackend`]: a [`ConsensusBackend`] implementation driven by the
//! `openraft` engine selected in ADR-0004.
//!
//! Raft Phase A2, PR 1 scope (see issue #5196): a **single-node, in-memory**
//! configuration. The Raft log and state machine live in memory; durability
//! (log + vote persistence under the database directory, crash recovery) is
//! PR 2. Multi-node networking is Phase A3 — the network factory here is a
//! no-op that is never exercised because a single-voter cluster sends no
//! RPCs.
//!
//! ## Log index mapping
//!
//! openraft's raw log interleaves application entries with protocol entries
//! (the membership entry written by [`Raft::initialize`] and the blank entry
//! a new leader appends). The [`ConsensusBackend`] contract instead exposes a
//! dense, 1-based index over **application entries only**, matching
//! [`SingleNodeBackend`](crate::SingleNodeBackend). The state machine assigns
//! that application index as each `Normal` entry is applied and returns it as
//! the client-write response, so `propose` resolves with the same indices the
//! loopback backend would produce.
//!
//! [`ConsensusBackend`]: crate::ConsensusBackend

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::Cursor;
use std::marker::PhantomData;
use std::ops::RangeBounds;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use openraft::error::{ClientWriteError, InstallSnapshotError, RPCError, RaftError, Unreachable};
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::storage::{LogFlushed, RaftLogStorage, RaftStateMachine};
use openraft::{
    BasicNode, Config, Entry, EntryPayload, LogId, LogState, Raft, RaftLogReader,
    RaftSnapshotBuilder, ServerState, SnapshotMeta, StorageError, StoredMembership,
    Vote,
};
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::backend::{ConsensusBackend, ConsensusError, LogIndex, Result, Role, Snapshot};

openraft::declare_raft_types!(
    /// Raft type configuration for VibeSQL consensus.
    ///
    /// The application payload (`D`) is an opaque byte buffer: the
    /// [`ConsensusBackend`] entry type is serialized at the adapter boundary
    /// so openraft types never leak to consumers (ADR-0004). The response
    /// (`R`) is the dense application log index assigned by the state
    /// machine (`0` for protocol entries, which carry no app payload).
    pub(crate) TypeConfig:
        D = Vec<u8>,
        R = u64,
);

/// The fixed node id of the single-voter cluster.
const NODE_ID: u64 = 1;

// ---------------------------------------------------------------------------
// In-memory Raft log storage
// ---------------------------------------------------------------------------

#[derive(Debug, Default)]
struct LogStoreInner {
    /// The last log id purged from the log (snapshot replaced it).
    last_purged_log_id: Option<LogId<u64>>,
    /// Raft log entries keyed by raw raft index.
    log: BTreeMap<u64, Entry<TypeConfig>>,
    /// Last committed log id saved by openraft (optional API).
    committed: Option<LogId<u64>>,
    /// Persistent vote state (term + voted_for). In-memory in PR 1; PR 2
    /// moves this to disk, because losing it across restarts breaks
    /// election safety.
    vote: Option<Vote<u64>>,
}

/// In-memory [`RaftLogStorage`] for the single-node Phase A2 configuration.
///
/// Cloning shares the underlying store (it is a handle), which is what
/// openraft expects from `get_log_reader`.
#[derive(Debug, Clone, Default)]
struct InMemoryLogStore {
    inner: Arc<Mutex<LogStoreInner>>,
}

impl InMemoryLogStore {
    fn lock(&self) -> std::sync::MutexGuard<'_, LogStoreInner> {
        self.inner.lock().expect("raft log store mutex poisoned")
    }
}

impl RaftLogReader<TypeConfig> for InMemoryLogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> std::result::Result<Vec<Entry<TypeConfig>>, StorageError<u64>> {
        let inner = self.lock();
        Ok(inner.log.range(range).map(|(_, entry)| entry.clone()).collect())
    }
}

impl RaftLogStorage<TypeConfig> for InMemoryLogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> std::result::Result<LogState<TypeConfig>, StorageError<u64>> {
        let inner = self.lock();
        let last_log_id =
            inner.log.values().next_back().map(|e| e.log_id).or(inner.last_purged_log_id);
        Ok(LogState { last_purged_log_id: inner.last_purged_log_id, last_log_id })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<u64>) -> std::result::Result<(), StorageError<u64>> {
        self.lock().vote = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> std::result::Result<Option<Vote<u64>>, StorageError<u64>> {
        Ok(self.lock().vote)
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<u64>>,
    ) -> std::result::Result<(), StorageError<u64>> {
        self.lock().committed = committed;
        Ok(())
    }

    async fn read_committed(
        &mut self,
    ) -> std::result::Result<Option<LogId<u64>>, StorageError<u64>> {
        Ok(self.lock().committed)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<TypeConfig>,
    ) -> std::result::Result<(), StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
        I::IntoIter: Send,
    {
        {
            let mut inner = self.lock();
            for entry in entries {
                inner.log.insert(entry.log_id.index, entry);
            }
        }
        // In-memory storage: entries are "flushed" the moment they are
        // inserted. PR 2 calls this only after an fsync of the on-disk log.
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<u64>) -> std::result::Result<(), StorageError<u64>> {
        let mut inner = self.lock();
        inner.log.split_off(&log_id.index);
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<u64>) -> std::result::Result<(), StorageError<u64>> {
        let mut inner = self.lock();
        inner.last_purged_log_id = Some(log_id);
        inner.log = inner.log.split_off(&(log_id.index + 1));
        Ok(())
    }
}

// ---------------------------------------------------------------------------
// In-memory state machine
// ---------------------------------------------------------------------------

/// A snapshot held by the state machine.
#[derive(Debug, Clone)]
struct StoredSnapshot {
    meta: SnapshotMeta<u64, BasicNode>,
    data: Vec<u8>,
}

#[derive(Debug, Default)]
struct StateMachineInner {
    last_applied: Option<LogId<u64>>,
    last_membership: StoredMembership<u64, BasicNode>,
    /// Applied application entries (serialized payloads). Position `i`
    /// holds application log index `i + 1` (1-based, Raft convention).
    entries: Vec<Vec<u8>>,
    /// Monotonic snapshot id counter.
    snapshot_seq: u64,
    current_snapshot: Option<StoredSnapshot>,
}

/// In-memory [`RaftStateMachine`] that records applied application entries.
///
/// "State" here is simply the ordered list of applied payloads: VibeSQL's
/// real state machine (applying write-sets to storage) is Phase B1 work.
/// Cloning shares the underlying state (it is a handle).
#[derive(Debug, Clone, Default)]
struct InMemoryStateMachine {
    inner: Arc<Mutex<StateMachineInner>>,
}

impl InMemoryStateMachine {
    fn lock(&self) -> std::sync::MutexGuard<'_, StateMachineInner> {
        self.inner.lock().expect("raft state machine mutex poisoned")
    }
}

impl RaftSnapshotBuilder<TypeConfig> for InMemoryStateMachine {
    async fn build_snapshot(
        &mut self,
    ) -> std::result::Result<openraft::Snapshot<TypeConfig>, StorageError<u64>> {
        let mut inner = self.lock();
        let data = serde_json::to_vec(&inner.entries).map_err(|e| {
            StorageError::IO {
                source: openraft::StorageIOError::write_state_machine(&e),
            }
        })?;

        inner.snapshot_seq += 1;
        let meta = SnapshotMeta {
            last_log_id: inner.last_applied,
            last_membership: inner.last_membership.clone(),
            snapshot_id: format!("snapshot-{}", inner.snapshot_seq),
        };
        inner.current_snapshot = Some(StoredSnapshot { meta: meta.clone(), data: data.clone() });

        Ok(openraft::Snapshot { meta, snapshot: Box::new(Cursor::new(data)) })
    }
}

impl RaftStateMachine<TypeConfig> for InMemoryStateMachine {
    type SnapshotBuilder = Self;

    async fn applied_state(
        &mut self,
    ) -> std::result::Result<(Option<LogId<u64>>, StoredMembership<u64, BasicNode>), StorageError<u64>>
    {
        let inner = self.lock();
        Ok((inner.last_applied, inner.last_membership.clone()))
    }

    async fn apply<I>(&mut self, entries: I) -> std::result::Result<Vec<u64>, StorageError<u64>>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + Send,
        I::IntoIter: Send,
    {
        let mut inner = self.lock();
        let mut responses = Vec::new();
        for entry in entries {
            inner.last_applied = Some(entry.log_id);
            let response = match entry.payload {
                // Protocol entries carry no application payload; they do not
                // consume an application log index.
                EntryPayload::Blank => 0,
                EntryPayload::Membership(membership) => {
                    inner.last_membership = StoredMembership::new(Some(entry.log_id), membership);
                    0
                }
                EntryPayload::Normal(data) => {
                    inner.entries.push(data);
                    inner.entries.len() as u64
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

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> std::result::Result<(), StorageError<u64>> {
        let data = snapshot.into_inner();
        let entries: Vec<Vec<u8>> = serde_json::from_slice(&data).map_err(|e| {
            StorageError::IO {
                source: openraft::StorageIOError::write_state_machine(&e),
            }
        })?;

        let mut inner = self.lock();
        inner.entries = entries;
        inner.last_applied = meta.last_log_id;
        inner.last_membership = meta.last_membership.clone();
        inner.current_snapshot = Some(StoredSnapshot { meta: meta.clone(), data });
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
// No-op network (single voter sends no RPCs)
// ---------------------------------------------------------------------------

/// Network stub for the single-node configuration.
///
/// A single-voter cluster never replicates to peers, so these methods are
/// unreachable in practice; they return [`Unreachable`] (rather than
/// panicking) for defense in depth. Phase A3 replaces this with a real
/// transport.
#[derive(Debug, Default)]
struct NoopNetwork;

fn unreachable_rpc<E: std::error::Error>() -> RPCError<u64, BasicNode, E> {
    RPCError::Unreachable(Unreachable::new(&std::io::Error::other(
        "single-node consensus group has no peers (network lands in Raft Phase A3)",
    )))
}

impl openraft::RaftNetwork<TypeConfig> for NoopNetwork {
    async fn append_entries(
        &mut self,
        _rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> std::result::Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>>
    {
        Err(unreachable_rpc())
    }

    async fn install_snapshot(
        &mut self,
        _rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> std::result::Result<
        InstallSnapshotResponse<u64>,
        RPCError<u64, BasicNode, RaftError<u64, InstallSnapshotError>>,
    > {
        Err(unreachable_rpc())
    }

    async fn vote(
        &mut self,
        _rpc: VoteRequest<u64>,
        _option: RPCOption,
    ) -> std::result::Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        Err(unreachable_rpc())
    }
}

#[derive(Debug, Default)]
struct NoopNetworkFactory;

impl openraft::RaftNetworkFactory<TypeConfig> for NoopNetworkFactory {
    type Network = NoopNetwork;

    async fn new_client(&mut self, _target: u64, _node: &BasicNode) -> Self::Network {
        NoopNetwork
    }
}

// ---------------------------------------------------------------------------
// The backend
// ---------------------------------------------------------------------------

/// [`ConsensusBackend`] backed by `openraft` (ADR-0004), running a
/// single-node, in-memory configuration (Raft Phase A2, PR 1).
///
/// The node initializes itself as the sole voter of the consensus group and
/// immediately elects itself leader, so `propose` succeeds locally while
/// still flowing through openraft's real append → commit → apply pipeline.
/// Entries are serialized with `serde_json` at this boundary so openraft
/// types never appear in the public API.
pub struct OpenraftBackend<E> {
    raft: Raft<TypeConfig>,
    state_machine: InMemoryStateMachine,
    metrics: tokio::sync::watch::Receiver<openraft::RaftMetrics<u64, BasicNode>>,
    /// `fn() -> E` keeps the backend `Send + Sync` independent of `E` while
    /// still tying the entry type to the instance.
    _entry: PhantomData<fn() -> E>,
}

impl<E> Debug for OpenraftBackend<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpenraftBackend")
            .field("role", &self.current_role())
            .finish_non_exhaustive()
    }
}

impl<E> OpenraftBackend<E> {
    /// Create a backend with an empty log and wait until the single-voter
    /// cluster has elected this node leader.
    ///
    /// Must be called from within a tokio runtime (openraft spawns its core
    /// tasks on it).
    pub async fn new() -> Result<Self> {
        Self::with_seeded_entries(Vec::new()).await
    }

    /// The application log index of the most recently applied entry (`0` if
    /// nothing has been committed). Mirrors
    /// [`SingleNodeBackend::last_index`](crate::SingleNodeBackend::last_index).
    pub fn last_index(&self) -> LogIndex {
        self.state_machine.lock().entries.len() as LogIndex
    }

    /// Gracefully shut down the underlying Raft core tasks.
    pub async fn shutdown(&self) -> Result<()> {
        self.raft.shutdown().await.map_err(|e| ConsensusError::Backend(e.to_string()))
    }

    fn current_role(&self) -> Role {
        match self.metrics.borrow().state {
            ServerState::Leader => Role::Leader,
            ServerState::Candidate => Role::Candidate,
            // `Learner` (non-voting) and `Shutdown` have no Role equivalent
            // yet; report Follower, the closest "not sequencing writes"
            // state. Revisit if consumers need the distinction.
            ServerState::Follower | ServerState::Learner | ServerState::Shutdown => Role::Follower,
        }
    }

    async fn with_seeded_entries(entries: Vec<Vec<u8>>) -> Result<Self> {
        // Short election timeouts keep single-node startup snappy; with one
        // voter there is no contention to back off from.
        let config = Config {
            heartbeat_interval: 100,
            election_timeout_min: 150,
            election_timeout_max: 300,
            ..Default::default()
        };
        let config =
            Arc::new(config.validate().map_err(|e| ConsensusError::Backend(e.to_string()))?);

        let log_store = InMemoryLogStore::default();
        let state_machine = InMemoryStateMachine::default();
        // Seed the state machine before the Raft core starts so restored
        // entries keep their application log indices and new proposals
        // continue numbering after them.
        state_machine.lock().entries = entries;

        let raft = Raft::new(
            NODE_ID,
            config,
            NoopNetworkFactory,
            log_store,
            state_machine.clone(),
        )
        .await
        .map_err(|e| ConsensusError::Backend(format!("failed to start raft core: {e}")))?;

        let mut members = BTreeMap::new();
        members.insert(NODE_ID, BasicNode::default());
        raft.initialize(members)
            .await
            .map_err(|e| ConsensusError::Backend(format!("failed to initialize cluster: {e}")))?;

        // A single voter elects itself; wait for leadership so `propose`
        // never races the initial election.
        raft.wait(Some(Duration::from_secs(10)))
            .state(ServerState::Leader, "single-node leader election")
            .await
            .map_err(|e| ConsensusError::Backend(format!("leader election did not settle: {e}")))?;

        let metrics = raft.metrics();
        Ok(Self { raft, state_machine, metrics, _entry: PhantomData })
    }
}

impl<E: DeserializeOwned> OpenraftBackend<E> {
    /// Rebuild a backend from a snapshot previously produced by
    /// [`ConsensusBackend::snapshot`] on an `OpenraftBackend`.
    ///
    /// Like [`SingleNodeBackend::from_snapshot`](crate::SingleNodeBackend::from_snapshot)
    /// this is an inherent method: snapshot *installation* hooks join the
    /// trait in later Raft phases. The restored entries seed the state
    /// machine; the Raft log itself starts fresh (durable log recovery is
    /// PR 2 of Phase A2).
    pub async fn from_snapshot(snapshot: &Snapshot) -> Result<Self> {
        let entries: Vec<Vec<u8>> = serde_json::from_slice(&snapshot.data)
            .map_err(|e| ConsensusError::SnapshotCodec(e.to_string()))?;
        if entries.len() as LogIndex != snapshot.last_included_index {
            return Err(ConsensusError::SnapshotCodec(format!(
                "snapshot claims last_included_index {} but contains {} entries",
                snapshot.last_included_index,
                entries.len()
            )));
        }
        // Validate that every payload decodes as `E` so a corrupt snapshot
        // fails here, not on a later read.
        for payload in &entries {
            serde_json::from_slice::<E>(payload)
                .map_err(|e| ConsensusError::SnapshotCodec(e.to_string()))?;
        }
        Self::with_seeded_entries(entries).await
    }
}

impl<E> ConsensusBackend for OpenraftBackend<E>
where
    E: Serialize + DeserializeOwned + Send,
{
    type Entry = E;

    async fn propose(&self, entry: E) -> Result<LogIndex> {
        let payload =
            serde_json::to_vec(&entry).map_err(|e| ConsensusError::Backend(e.to_string()))?;

        let response = self.raft.client_write(payload).await.map_err(|e| match e {
            RaftError::APIError(ClientWriteError::ForwardToLeader(forward)) => {
                ConsensusError::NotLeader { leader_hint: forward.leader_id }
            }
            other => ConsensusError::Backend(other.to_string()),
        })?;

        // `data` is the dense application index assigned by the state
        // machine; 0 would mean a protocol entry, which client_write never
        // produces.
        debug_assert!(response.data > 0, "client write applied as a protocol entry");
        Ok(response.data)
    }

    async fn read_committed(&self, idx: LogIndex) -> Result<E> {
        // `client_write` resolves only after apply, so the state machine's
        // applied entries are the committed prefix. With a single voter
        // there is no other node that could have advanced the commit index.
        let payload = {
            let inner = self.state_machine.lock();
            if idx == 0 || idx > inner.entries.len() as LogIndex {
                return Err(ConsensusError::NotCommitted(idx));
            }
            inner.entries[(idx - 1) as usize].clone()
        };
        serde_json::from_slice(&payload).map_err(|e| ConsensusError::Backend(e.to_string()))
    }

    async fn snapshot(&self) -> Result<Snapshot> {
        let inner = self.state_machine.lock();
        let data = serde_json::to_vec(&inner.entries)
            .map_err(|e| ConsensusError::SnapshotCodec(e.to_string()))?;
        Ok(Snapshot { last_included_index: inner.entries.len() as LogIndex, data })
    }

    fn role(&self) -> Role {
        self.current_role()
    }
}
