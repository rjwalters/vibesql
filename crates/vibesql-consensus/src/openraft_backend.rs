//! [`OpenraftBackend`]: a [`ConsensusBackend`] implementation driven by the
//! `openraft` engine selected in ADR-0004.
//!
//! Raft Phase A2 scope (see issue #5196): a **single-node** configuration in
//! two flavors. [`OpenraftBackend::new`] keeps the Raft log in memory (PR 1);
//! [`OpenraftBackend::with_data_dir`] persists the log and vote on disk via
//! [`DurableLogStore`] and recovers them on restart (PR 2). The state machine
//! is in-memory in both — applying entries to VibeSQL storage is Phase B1.
//!
//! Raft Phase A3 (PR 1 of #5197) adds `join_channel_cluster` (test-only):
//! the same backend booted as one voter of an in-process multi-node cluster
//! whose RPCs travel over the channel transport in `crate::network`. PR 2
//! adds the production-shaped equivalents over real sockets —
//! [`join_tcp_cluster`](OpenraftBackend::join_tcp_cluster) /
//! [`join_tcp_cluster_with_data_dir`](OpenraftBackend::join_tcp_cluster_with_data_dir)
//! — wiring the TCP transport in `crate::tcp` to a static
//! [`ClusterConfig`]. The single-node constructors keep the no-op network
//! factory, which is never exercised because a single-voter cluster sends
//! no RPCs.
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
use std::path::Path;
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
    RaftSnapshotBuilder, ServerState, SnapshotMeta, StorageError, StoredMembership, Vote,
};
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::backend::{ConsensusBackend, ConsensusError, LogIndex, Result, Role, Snapshot};
use crate::cluster_config::ClusterConfig;
use crate::durable::DurableLogStore;

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
    /// Vote state (term + voted_for). In-memory here, so it does not survive
    /// restarts — acceptable only because this store is for tests/dev. The
    /// durable configuration ([`DurableLogStore`]) persists it, because
    /// losing it across restarts breaks election safety.
    vote: Option<Vote<u64>>,
}

/// In-memory [`RaftLogStorage`] for the single-node Phase A2 configuration
/// (and, via the shared handle, for the in-process cluster tests of Phase
/// A3: a "restarted" node reopens the same store, which is how it keeps its
/// log and vote across the restart).
///
/// Cloning shares the underlying store (it is a handle), which is what
/// openraft expects from `get_log_reader`.
#[derive(Debug, Clone, Default)]
pub(crate) struct InMemoryLogStore {
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

    async fn get_log_state(
        &mut self,
    ) -> std::result::Result<LogState<TypeConfig>, StorageError<u64>> {
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
        // inserted. The durable store ([`DurableLogStore`]) invokes this only
        // after an fsync of the on-disk log.
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
        let data = serde_json::to_vec(&inner.entries).map_err(|e| StorageError::IO {
            source: openraft::StorageIOError::write_state_machine(&e),
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
    ) -> std::result::Result<
        (Option<LogId<u64>>, StoredMembership<u64, BasicNode>),
        StorageError<u64>,
    > {
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
            StorageError::IO { source: openraft::StorageIOError::write_state_machine(&e) }
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
/// panicking) for defense in depth. Multi-node configurations use a real
/// transport instead: the in-process channel network in `crate::network`
/// (Phase A3, PR 1; test-only), with TCP following in PR 2.
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

/// How a starting backend obtains its membership.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Bootstrap {
    /// Fresh log: form the cluster by writing the membership entry
    /// ([`Raft::initialize`]).
    Initialize,
    /// Prior Raft state was recovered (from disk, or from a kept-alive
    /// [`InMemoryLogStore`] in cluster tests): the membership entry (and
    /// vote) are already in the log, so re-running `initialize` would be
    /// rejected by openraft. `last_log_index` is the raw index of the last
    /// recovered entry; single-node startup waits until the state machine
    /// has re-applied up to it so reads reflect pre-restart state (cluster
    /// restarts pass `None` and let the test harness await convergence
    /// instead).
    Recover { last_log_index: Option<u64> },
}

/// [`ConsensusBackend`] backed by `openraft` (ADR-0004), running a
/// single-node configuration (Raft Phase A2).
///
/// The node initializes itself as the sole voter of the consensus group and
/// immediately elects itself leader, so `propose` succeeds locally while
/// still flowing through openraft's real append → commit → apply pipeline.
/// Entries are serialized with `serde_json` at this boundary so openraft
/// types never appear in the public API.
///
/// [`new`](Self::new) keeps the Raft log in memory;
/// [`with_data_dir`](Self::with_data_dir) persists log + vote on disk and
/// recovers them on restart.
pub struct OpenraftBackend<E> {
    raft: Raft<TypeConfig>,
    state_machine: InMemoryStateMachine,
    metrics: tokio::sync::watch::Receiver<openraft::RaftMetrics<u64, BasicNode>>,
    /// Accept-loop task of the TCP transport (`None` for the single-node
    /// and channel-network configurations). Aborted on [`shutdown`] and on
    /// drop; the loop owns its accepted connections through a `JoinSet`, so
    /// aborting it also severs every inbound socket — a dropped backend
    /// looks like a crashed process to its peers.
    ///
    /// [`shutdown`]: Self::shutdown
    listener_task: Option<tokio::task::JoinHandle<()>>,
    /// `fn() -> E` keeps the backend `Send + Sync` independent of `E` while
    /// still tying the entry type to the instance.
    _entry: PhantomData<fn() -> E>,
}

impl<E> Drop for OpenraftBackend<E> {
    fn drop(&mut self) {
        if let Some(task) = &self.listener_task {
            task.abort();
        }
    }
}

impl<E> Debug for OpenraftBackend<E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OpenraftBackend")
            .field("role", &self.current_role())
            .finish_non_exhaustive()
    }
}

impl<E> OpenraftBackend<E> {
    /// Create a backend with an empty **in-memory** log and wait until the
    /// single-voter cluster has elected this node leader. Nothing survives a
    /// restart; use [`with_data_dir`](Self::with_data_dir) for durability.
    ///
    /// Must be called from within a tokio runtime (openraft spawns its core
    /// tasks on it).
    pub async fn new() -> Result<Self> {
        Self::start(InMemoryLogStore::default(), Vec::new(), Bootstrap::Initialize).await
    }

    /// Create a backend whose Raft log and vote are **persisted on disk**
    /// under `dir` (created if absent), in `raft.log` (Raft Phase A2, PR 2).
    ///
    /// If `dir` already contains Raft state, it is recovered before this
    /// returns: log entries are re-applied to the state machine (so
    /// `read_committed` immediately reflects pre-restart commits) and the
    /// persisted vote keeps election safety — the restarted node resumes at
    /// its prior term and never votes twice in a term it already voted in.
    /// A partially written trailing record (torn write) is discarded during
    /// recovery, not treated as an error.
    ///
    /// Must be called from within a tokio runtime.
    pub async fn with_data_dir(dir: impl AsRef<Path>) -> Result<Self> {
        Self::start_durable(dir.as_ref(), None).await
    }

    /// The application log index of the most recently applied entry (`0` if
    /// nothing has been committed). Mirrors
    /// [`SingleNodeBackend::last_index`](crate::SingleNodeBackend::last_index).
    pub fn last_index(&self) -> LogIndex {
        self.state_machine.lock().entries.len() as LogIndex
    }

    /// Gracefully shut down the underlying Raft core tasks (and, for a
    /// TCP-clustered node, its listener and every inbound connection).
    pub async fn shutdown(&self) -> Result<()> {
        // Abort the listener first so no new inbound RPC is dispatched into
        // a core that is going down; peers see dead sockets and retry.
        if let Some(task) = &self.listener_task {
            task.abort();
        }
        self.raft.shutdown().await.map_err(|e| ConsensusError::Backend(e.to_string()))
    }

    /// The node this backend currently believes leads the cluster, per its
    /// metrics (`None` until it has heard from — or become — a leader).
    ///
    /// Callers that get [`ConsensusError::NotLeader`] without a hint can
    /// poll this to discover where to retry; B2 (#5200) builds leader-aware
    /// routing on it.
    pub fn current_leader(&self) -> Option<u64> {
        self.metrics.borrow().current_leader
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

    #[cfg(test)]
    pub(crate) fn current_term(&self) -> u64 {
        self.metrics.borrow().current_term
    }

    /// Open (or create) the durable log under `dir` and start the node.
    ///
    /// `restore` carries state-machine entries decoded from a snapshot; it
    /// must only be combined with a *fresh* data directory, since seeding a
    /// snapshot next to recovered log state would produce two competing
    /// histories.
    async fn start_durable(dir: &Path, restore: Option<Vec<Vec<u8>>>) -> Result<Self> {
        let log_store = DurableLogStore::open(dir).map_err(|e| {
            ConsensusError::Backend(format!(
                "failed to open durable raft log in {}: {e}",
                dir.display()
            ))
        })?;
        let (has_state, last_log_index) = log_store.recovery_summary();
        if has_state && restore.is_some() {
            return Err(ConsensusError::Backend(format!(
                "cannot restore a snapshot into {}: it already contains raft state",
                dir.display()
            )));
        }
        let bootstrap =
            if has_state { Bootstrap::Recover { last_log_index } } else { Bootstrap::Initialize };
        Self::start(log_store, restore.unwrap_or_default(), bootstrap).await
    }

    /// Spawn the Raft core: storage + state machine + network, no membership
    /// yet. Shared by the single-node constructors and the in-process
    /// cluster constructor; the caller decides how membership is established
    /// (initialize vs. recovered log) and what to wait for.
    async fn boot<LS, NF>(
        node_id: u64,
        network: NF,
        log_store: LS,
        entries: Vec<Vec<u8>>,
    ) -> Result<Self>
    where
        LS: RaftLogStorage<TypeConfig>,
        NF: openraft::RaftNetworkFactory<TypeConfig>,
    {
        // Short timeouts keep single-node startup and test elections snappy;
        // the 4x gap between heartbeat and the minimum election timeout
        // keeps healthy multi-node clusters from triggering spurious
        // elections.
        let config = Config {
            heartbeat_interval: 50,
            election_timeout_min: 200,
            election_timeout_max: 400,
            ..Default::default()
        };
        let config =
            Arc::new(config.validate().map_err(|e| ConsensusError::Backend(e.to_string()))?);

        let state_machine = InMemoryStateMachine::default();
        // Seed the state machine before the Raft core starts so restored
        // entries keep their application log indices and new proposals
        // continue numbering after them.
        state_machine.lock().entries = entries;

        let raft = Raft::new(node_id, config, network, log_store, state_machine.clone())
            .await
            .map_err(|e| ConsensusError::Backend(format!("failed to start raft core: {e}")))?;

        let metrics = raft.metrics();
        Ok(Self { raft, state_machine, metrics, listener_task: None, _entry: PhantomData })
    }

    /// With [`Bootstrap::Initialize`], write the static membership into the
    /// fresh log ([`Raft::initialize`]); with [`Bootstrap::Recover`] do
    /// nothing (the membership entry and vote are already in the recovered
    /// log, and re-running `initialize` would be rejected).
    ///
    /// openraft documents that multiple nodes initializing with the *same*
    /// membership is safe; a node that already voted for (or received the
    /// membership entry from) a faster peer rejects its own initialize with
    /// `NotAllowed`. Both paths converge on the same membership, so that
    /// rejection is tolerated.
    async fn establish_membership(
        &self,
        members: BTreeMap<u64, BasicNode>,
        bootstrap: Bootstrap,
    ) -> Result<()> {
        if !matches!(bootstrap, Bootstrap::Initialize) {
            return Ok(());
        }
        match self.raft.initialize(members).await {
            Ok(()) | Err(RaftError::APIError(openraft::error::InitializeError::NotAllowed(_))) => {
                Ok(())
            }
            Err(e) => Err(ConsensusError::Backend(format!("failed to initialize cluster: {e}"))),
        }
    }

    /// Boot one voter of a **TCP-connected cluster** with an in-memory Raft
    /// log (Raft Phase A3, PR 2 of #5197). Prefer
    /// [`join_tcp_cluster_with_data_dir`](Self::join_tcp_cluster_with_data_dir)
    /// anywhere a restart must not forget the log or vote.
    ///
    /// Binds this node's listener at its own `config` address (consensus
    /// port convention: [`crate::DEFAULT_CONSENSUS_PORT`]) and dials peers
    /// at theirs. Like `join_channel_cluster`, this does **not** wait for an
    /// election: which node wins is a cluster-level outcome the caller
    /// awaits (e.g. by polling [`role`](ConsensusBackend::role) /
    /// [`current_leader`](Self::current_leader) with a bounded timeout).
    ///
    /// Must be called from within a tokio runtime.
    pub async fn join_tcp_cluster(node_id: u64, config: &ClusterConfig) -> Result<Self> {
        Self::join_tcp(node_id, config, InMemoryLogStore::default(), Bootstrap::Initialize).await
    }

    /// Like [`join_tcp_cluster`](Self::join_tcp_cluster), but the node's
    /// Raft log and vote are persisted under `dir` (created if absent) and
    /// recovered on restart — a restarted node rebinds its address, rejoins
    /// the cluster with its pre-crash log and vote, and catches up via log
    /// replication.
    pub async fn join_tcp_cluster_with_data_dir(
        node_id: u64,
        config: &ClusterConfig,
        dir: impl AsRef<Path>,
    ) -> Result<Self> {
        let dir = dir.as_ref();
        let log_store = DurableLogStore::open(dir).map_err(|e| {
            ConsensusError::Backend(format!(
                "failed to open durable raft log in {}: {e}",
                dir.display()
            ))
        })?;
        let (has_state, _) = log_store.recovery_summary();
        // Cluster members do not wait for local replay (`last_log_index:
        // None`): convergence is a cluster-level outcome the caller awaits.
        let bootstrap = if has_state {
            Bootstrap::Recover { last_log_index: None }
        } else {
            Bootstrap::Initialize
        };
        Self::join_tcp(node_id, config, log_store, bootstrap).await
    }

    async fn join_tcp<LS>(
        node_id: u64,
        config: &ClusterConfig,
        log_store: LS,
        bootstrap: Bootstrap,
    ) -> Result<Self>
    where
        LS: RaftLogStorage<TypeConfig>,
    {
        let listen_addr = config.addr(node_id).ok_or_else(|| {
            ConsensusError::Config(format!("node {node_id} is not in the cluster config"))
        })?;
        // Bind before booting the core so a peer that initialized first can
        // reach this node as soon as its Raft exists.
        let listener = crate::tcp::bind_listener(listen_addr).await.map_err(|e| {
            ConsensusError::Backend(format!(
                "failed to bind consensus listener on {listen_addr}: {e}"
            ))
        })?;

        let network = crate::tcp::TcpNetworkFactory::new(config);
        let mut backend = Self::boot(node_id, network, log_store, Vec::new()).await?;
        backend.listener_task = Some(crate::tcp::spawn_listener(backend.raft.clone(), listener));

        backend.establish_membership(config.membership(), bootstrap).await?;
        Ok(backend)
    }

    async fn start<LS>(log_store: LS, entries: Vec<Vec<u8>>, bootstrap: Bootstrap) -> Result<Self>
    where
        LS: RaftLogStorage<TypeConfig>,
    {
        let backend = Self::boot(NODE_ID, NoopNetworkFactory, log_store, entries).await?;

        if matches!(bootstrap, Bootstrap::Initialize) {
            let mut members = BTreeMap::new();
            members.insert(NODE_ID, BasicNode::default());
            backend.raft.initialize(members).await.map_err(|e| {
                ConsensusError::Backend(format!("failed to initialize cluster: {e}"))
            })?;
        }
        // When recovering, the membership entry and vote are already in the
        // recovered log (`Raft::new` read them); openraft restores a node
        // whose persisted vote marks it leader straight back into
        // leadership at the same term.

        // A single voter elects itself; wait for leadership so `propose`
        // never races the initial election.
        backend
            .raft
            .wait(Some(Duration::from_secs(10)))
            .state(ServerState::Leader, "single-node leader election")
            .await
            .map_err(|e| ConsensusError::Backend(format!("leader election did not settle: {e}")))?;

        if let Bootstrap::Recover { last_log_index: Some(last) } = bootstrap {
            // The state machine is in-memory (until Phase B1) and therefore
            // starts empty on every boot; wait for the recovered log to be
            // re-applied so `read_committed` and `last_index` reflect
            // pre-restart state before the constructor returns.
            backend
                .raft
                .wait(Some(Duration::from_secs(10)))
                .metrics(
                    move |m| m.last_applied.map_or(0, |id| id.index) >= last,
                    "recovered log entries re-applied",
                )
                .await
                .map_err(|e| {
                    ConsensusError::Backend(format!("recovered log replay did not settle: {e}"))
                })?;
        }

        Ok(backend)
    }
}

#[cfg(test)]
impl<E> OpenraftBackend<E> {
    /// Boot one member of an **in-process multi-node cluster** whose RPCs
    /// are routed through `router` (Raft Phase A3, PR 1 of #5197).
    ///
    /// `members` is the full static membership of the cluster (every node
    /// passes the same map; dynamic add/remove is a later issue). With
    /// [`Bootstrap::Initialize`] the node writes that membership into its
    /// fresh log; with [`Bootstrap::Recover`] the membership is already in
    /// the (kept-alive or on-disk) log being reopened.
    ///
    /// Unlike the single-node constructors this does **not** wait for an
    /// election or for log replay: which node wins, and when a restarted
    /// node has caught up, are cluster-level outcomes the test harness
    /// awaits explicitly (with bounded timeouts).
    pub(crate) async fn join_channel_cluster<LS>(
        node_id: u64,
        members: &BTreeMap<u64, BasicNode>,
        router: &crate::network::ChannelRouter,
        log_store: LS,
        bootstrap: Bootstrap,
    ) -> Result<Self>
    where
        LS: RaftLogStorage<TypeConfig>,
    {
        let network = crate::network::ChannelNetworkFactory::new(router.clone());
        let backend = Self::boot(node_id, network, log_store, Vec::new()).await?;

        // Register the inbound RPC loop *before* initializing, so peers that
        // initialized first can already send this node vote/append RPCs.
        router.register(node_id, backend.raft.clone());

        backend.establish_membership(members.clone(), bootstrap).await?;
        Ok(backend)
    }
}

impl<E: DeserializeOwned> OpenraftBackend<E> {
    /// Rebuild a backend from a snapshot previously produced by
    /// [`ConsensusBackend::snapshot`] on an `OpenraftBackend`.
    ///
    /// Like [`SingleNodeBackend::from_snapshot`](crate::SingleNodeBackend::from_snapshot)
    /// this is an inherent method: snapshot *installation* hooks join the
    /// trait in later Raft phases. The restored entries seed the state
    /// machine; the Raft log itself starts fresh (and in-memory — see
    /// [`from_snapshot_with_data_dir`](Self::from_snapshot_with_data_dir)
    /// for a durable restore).
    pub async fn from_snapshot(snapshot: &Snapshot) -> Result<Self> {
        let entries = Self::decode_snapshot(snapshot)?;
        Self::start(InMemoryLogStore::default(), entries, Bootstrap::Initialize).await
    }

    /// Like [`from_snapshot`](Self::from_snapshot), but the restored node
    /// persists its Raft log and vote under `dir` (Raft Phase A2, PR 2).
    ///
    /// `dir` must not already contain Raft state: a snapshot seeded next to a
    /// recovered log would be two competing histories, so that case is
    /// rejected.
    pub async fn from_snapshot_with_data_dir(
        snapshot: &Snapshot,
        dir: impl AsRef<Path>,
    ) -> Result<Self> {
        let entries = Self::decode_snapshot(snapshot)?;
        Self::start_durable(dir.as_ref(), Some(entries)).await
    }

    /// Decode and validate a [`Snapshot`] produced by
    /// [`ConsensusBackend::snapshot`] into state-machine entries.
    fn decode_snapshot(snapshot: &Snapshot) -> Result<Vec<Vec<u8>>> {
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
        Ok(entries)
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

#[cfg(test)]
mod tests {
    use super::*;

    /// `shutdown` stops the Raft core cleanly: subsequent proposals fail
    /// fast (rather than hanging), while reads of already-applied state
    /// still succeed. (Judge follow-up from PR #5351.)
    #[tokio::test]
    async fn shutdown_stops_the_raft_core() {
        let backend = OpenraftBackend::<String>::new().await.unwrap();
        backend.propose("before".to_string()).await.unwrap();

        backend.shutdown().await.unwrap();

        let err = backend.propose("after".to_string()).await.unwrap_err();
        assert!(
            matches!(err, ConsensusError::Backend(_)),
            "propose after shutdown should fail with a backend error, got: {err:?}"
        );

        // Applied state is served from the state machine, not the core.
        assert_eq!(backend.read_committed(1).await.unwrap(), "before");
        assert_eq!(backend.last_index(), 1);
    }
}
