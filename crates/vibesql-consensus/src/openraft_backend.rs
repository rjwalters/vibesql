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
//! Raft Phase A4 (PR 1 of #5198) replaces the snapshot stand-ins with the
//! real pipeline: [`ConsensusBackend::snapshot`] flows through openraft's
//! [`RaftSnapshotBuilder`], builds pin the MVCC vacuum horizon
//! (`crate::snapshot::SnapshotHorizonPin`; a no-op until B1), durable
//! configurations persist snapshots to the data directory and recover from
//! them (snapshot first, then log replay), and log purge is storage-enforced
//! to never exceed the durable snapshot. Network snapshot transfer and the
//! purge *policy* are PR 2.
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
use crate::snapshot::{
    decode_payload, encode_payload, NoopHorizonPin, SnapshotHorizonPin, SnapshotStore,
};

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
        // No durable-snapshot check here, unlike `DurableLogStore::purge`:
        // this store is for tests/dev only, and its log is exactly as
        // volatile as the in-memory snapshots that justify purging it — a
        // restart loses both, so there is no acknowledged-durability gap for
        // the Phase A4 safety rule to protect.
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
    /// Number of application entries covered by `data` (the dense
    /// [`LogIndex`] the [`ConsensusBackend::snapshot`] artifact reports).
    app_index: u64,
}

#[derive(Debug, Default)]
struct StateMachineInner {
    last_applied: Option<LogId<u64>>,
    last_membership: StoredMembership<u64, BasicNode>,
    /// Applied application entries (serialized payloads). Position `i`
    /// holds application log index `i + 1` (1-based, Raft convention).
    entries: Vec<Vec<u8>>,
    /// Monotonic snapshot counter (uniquified with a timestamp in the
    /// snapshot id, since this counter restarts with the process).
    snapshot_seq: u64,
    current_snapshot: Option<StoredSnapshot>,
}

/// In-memory [`RaftStateMachine`] that records applied application entries.
///
/// "State" here is simply the ordered list of applied payloads: VibeSQL's
/// real state machine (applying write-sets to storage) is Phase B1 work
/// (#5199), and the snapshot machinery below is deliberately generic over
/// that state so B1 only swaps the payload codec and the horizon pin.
/// Cloning shares the underlying state (it is a handle).
///
/// With a [`SnapshotStore`] attached ([`durable`](Self::durable)), built and
/// installed snapshots are persisted to the data directory **before** they
/// are registered, so openraft never learns of — and purge can never run
/// against — a snapshot that would not survive a crash.
#[derive(Debug, Clone)]
struct InMemoryStateMachine {
    inner: Arc<Mutex<StateMachineInner>>,
    /// Durable persistence for built/installed snapshots (`None` for the
    /// in-memory configurations, whose snapshots are exactly as volatile as
    /// the rest of their state).
    store: Option<Arc<SnapshotStore>>,
    /// MVCC vacuum-horizon pin held across snapshot builds. A no-op until
    /// Phase B1 (#5199) wires the MVCC state machine — see
    /// [`SnapshotHorizonPin`].
    pin: Arc<dyn SnapshotHorizonPin>,
}

impl Default for InMemoryStateMachine {
    fn default() -> Self {
        Self::volatile()
    }
}

impl InMemoryStateMachine {
    /// A state machine whose snapshots live (and die) in memory.
    fn volatile() -> Self {
        Self { inner: Arc::default(), store: None, pin: Arc::new(NoopHorizonPin) }
    }

    /// A state machine that persists its snapshots through `store`.
    fn durable(store: Arc<SnapshotStore>) -> Self {
        Self { inner: Arc::default(), store: Some(store), pin: Arc::new(NoopHorizonPin) }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, StateMachineInner> {
        self.inner.lock().expect("raft state machine mutex poisoned")
    }

    /// Seed this (fresh) machine from a snapshot recovered off disk, before
    /// the Raft core starts: applied state, `last_applied`, and membership
    /// all resume from the snapshot, and openraft then replays only the log
    /// suffix above `meta.last_log_id`.
    fn seed_from_snapshot(
        &self,
        meta: SnapshotMeta<u64, BasicNode>,
        data: Vec<u8>,
    ) -> std::io::Result<()> {
        let entries = decode_payload(&data).map_err(|e| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("durable raft snapshot payload failed to decode: {e}"),
            )
        })?;
        let mut inner = self.lock();
        inner.last_applied = meta.last_log_id;
        inner.last_membership = meta.last_membership.clone();
        inner.current_snapshot =
            Some(StoredSnapshot { meta, app_index: entries.len() as u64, data });
        inner.entries = entries;
        Ok(())
    }

    /// Seed this (fresh) machine with entries restored from a
    /// [`ConsensusBackend::snapshot`] artifact — the "restore a backup into
    /// a NEW consensus group" path ([`OpenraftBackend::from_snapshot`]).
    ///
    /// Unlike [`install_snapshot`](RaftStateMachine::install_snapshot), the
    /// artifact carries no raft meta (the new group starts its own log from
    /// scratch), so `last_applied` stays `None` and the persisted seed
    /// snapshot legalizes no log purge. With a durable store the seed is
    /// persisted before this returns, so the restored state now survives
    /// restarts of the restored node.
    fn restore_seed(&self, entries: Vec<Vec<u8>>) -> std::io::Result<()> {
        let data = encode_payload(&entries).map_err(std::io::Error::other)?;
        let meta = SnapshotMeta {
            last_log_id: None,
            last_membership: StoredMembership::default(),
            snapshot_id: format!("restore-{}", current_timestamp_ms()),
        };
        if let Some(store) = &self.store {
            store.save(&meta, &data)?;
        }
        let mut inner = self.lock();
        inner.current_snapshot =
            Some(StoredSnapshot { meta, app_index: entries.len() as u64, data });
        inner.entries = entries;
        Ok(())
    }

    /// `(application index, payload blob)` of the current snapshot, for the
    /// [`ConsensusBackend::snapshot`] artifact.
    fn current_snapshot_artifact(&self) -> Option<(u64, Vec<u8>)> {
        self.lock().current_snapshot.as_ref().map(|s| (s.app_index, s.data.clone()))
    }
}

fn current_timestamp_ms() -> u64 {
    use std::time::{SystemTime, UNIX_EPOCH};
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as u64).unwrap_or(0)
}

impl RaftSnapshotBuilder<TypeConfig> for InMemoryStateMachine {
    async fn build_snapshot(
        &mut self,
    ) -> std::result::Result<openraft::Snapshot<TypeConfig>, StorageError<u64>> {
        // Pin the MVCC vacuum horizon for the whole build: the snapshot is a
        // consistent view at `last_applied`, so versions visible at that
        // index must not be reclaimed until the blob is built, durable, and
        // registered. (No-op for the echo machine; #5199 connects this to
        // the active-transaction holdback in vibesql-storage's
        // transaction_api.)
        let _horizon_pin = self.pin.acquire();

        // Capture state and meta atomically under the state-machine lock.
        let (meta, data, app_index) = {
            let mut inner = self.lock();
            let data = encode_payload(&inner.entries).map_err(|e| StorageError::IO {
                source: openraft::StorageIOError::write_state_machine(&e),
            })?;
            inner.snapshot_seq += 1;
            let meta = SnapshotMeta {
                last_log_id: inner.last_applied,
                last_membership: inner.last_membership.clone(),
                snapshot_id: format!(
                    "snapshot-{}-{}-{}",
                    inner.last_applied.map_or(0, |id| id.index),
                    inner.snapshot_seq,
                    current_timestamp_ms(),
                ),
            };
            (meta, data, inner.entries.len() as u64)
        };

        // Persist BEFORE registering: openraft must never see (and purge
        // against) a snapshot that is not yet durable.
        if let Some(store) = &self.store {
            store.save(&meta, &data).map_err(|e| StorageError::IO {
                source: openraft::StorageIOError::write_snapshot(Some(meta.signature()), &e),
            })?;
        }

        self.lock().current_snapshot =
            Some(StoredSnapshot { meta: meta.clone(), data: data.clone(), app_index });

        Ok(openraft::Snapshot { meta, snapshot: Box::new(Cursor::new(data)) })
        // `_horizon_pin` drops here: the horizon was held across read,
        // persistence, and registration.
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

    /// Replace the state machine's contents from a received snapshot:
    /// applied state, `last_applied`, and membership all jump to the
    /// snapshot's view. Decode is validated **before** any mutation (a
    /// corrupt snapshot must not half-install), and with a durable store the
    /// blob is persisted **before** the install is acknowledged (a follower
    /// must not confirm an install it could forget across a crash).
    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> std::result::Result<(), StorageError<u64>> {
        let data = snapshot.into_inner();
        let entries = decode_payload(&data).map_err(|e| StorageError::IO {
            source: openraft::StorageIOError::write_snapshot(Some(meta.signature()), &e),
        })?;

        if let Some(store) = &self.store {
            store.save(meta, &data).map_err(|e| StorageError::IO {
                source: openraft::StorageIOError::write_snapshot(Some(meta.signature()), &e),
            })?;
        }

        let mut inner = self.lock();
        inner.last_applied = meta.last_log_id;
        inner.last_membership = meta.last_membership.clone();
        inner.current_snapshot =
            Some(StoredSnapshot { meta: meta.clone(), app_index: entries.len() as u64, data });
        inner.entries = entries;
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

/// Everything recovered from a data directory: the durable log store, plus a
/// state machine already seeded from the latest durable snapshot (Raft Phase
/// A4, PR 1 of #5198). Recovery order is **snapshot first, then log
/// replay**: the state machine resumes at the snapshot's `last_applied`, and
/// openraft re-applies only the log suffix above it.
struct DurableStorage {
    log_store: DurableLogStore,
    state_machine: InMemoryStateMachine,
    /// The raft *log* holds prior state (vote / entries / purge watermark).
    /// Drives the initialize-vs-recover decision: only a membership entry in
    /// the log makes re-running `Raft::initialize` illegal.
    log_has_state: bool,
    /// Any prior state at all, including a durable snapshot. Drives the
    /// "cannot restore a snapshot into a stateful directory" rejection.
    has_any_state: bool,
    /// Last raw raft log index (entries or purge watermark), for the
    /// single-node recovery-replay wait.
    last_log_index: Option<u64>,
}

impl DurableStorage {
    fn open(dir: &Path) -> Result<Self> {
        let (snapshot_store, loaded) = SnapshotStore::open(dir).map_err(|e| {
            ConsensusError::Backend(format!(
                "failed to open durable raft snapshot in {}: {e}",
                dir.display()
            ))
        })?;
        let log_store = DurableLogStore::open(dir, snapshot_store.watermark()).map_err(|e| {
            ConsensusError::Backend(format!(
                "failed to open durable raft log in {}: {e}",
                dir.display()
            ))
        })?;
        let (log_has_state, last_log_index) = log_store.recovery_summary();
        let snapshot_index = loaded.as_ref().and_then(|s| s.meta.last_log_id).map(|id| id.index);

        // Cross-checks between the two durable artifacts; both are loud
        // errors because proceeding would silently lose acknowledged state.
        if let Some(purged) = log_store.last_purged_index() {
            if snapshot_index.is_none_or(|covered| covered < purged) {
                return Err(ConsensusError::Backend(format!(
                    "raft log in {} is purged through index {purged}, but the durable snapshot \
                     covers only {snapshot_index:?}; entries below the purge point are \
                     unrecoverable — refusing to start",
                    dir.display()
                )));
            }
        }
        if let Some(snapshot_index) = snapshot_index {
            if last_log_index.is_none_or(|last| last < snapshot_index) {
                return Err(ConsensusError::Backend(format!(
                    "durable snapshot in {} covers raft index {snapshot_index}, but the raft \
                     log ends at {last_log_index:?}; the log has lost acknowledged state — \
                     refusing to start",
                    dir.display()
                )));
            }
        }

        let state_machine = InMemoryStateMachine::durable(Arc::clone(&snapshot_store));
        let mut has_any_state = log_has_state;
        if let Some(loaded) = loaded {
            state_machine.seed_from_snapshot(loaded.meta, loaded.data).map_err(|e| {
                ConsensusError::Backend(format!(
                    "failed to recover durable raft snapshot in {}: {e}",
                    dir.display()
                ))
            })?;
            has_any_state = true;
        }

        Ok(Self { log_store, state_machine, log_has_state, has_any_state, last_log_index })
    }
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
        Self::start(
            InMemoryLogStore::default(),
            InMemoryStateMachine::volatile(),
            Bootstrap::Initialize,
        )
        .await
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
    /// snapshot next to recovered state would produce two competing
    /// histories. The seed is persisted as a durable snapshot before the
    /// node starts, so restored state survives later restarts.
    async fn start_durable(dir: &Path, restore: Option<Vec<Vec<u8>>>) -> Result<Self> {
        let storage = DurableStorage::open(dir)?;
        if storage.has_any_state && restore.is_some() {
            return Err(ConsensusError::Backend(format!(
                "cannot restore a snapshot into {}: it already contains raft state",
                dir.display()
            )));
        }
        if let Some(entries) = restore {
            storage.state_machine.restore_seed(entries).map_err(|e| {
                ConsensusError::Backend(format!(
                    "failed to persist the restored snapshot in {}: {e}",
                    dir.display()
                ))
            })?;
        }
        let bootstrap = if storage.log_has_state {
            Bootstrap::Recover { last_log_index: storage.last_log_index }
        } else {
            Bootstrap::Initialize
        };
        Self::start(storage.log_store, storage.state_machine, bootstrap).await
    }

    /// Spawn the Raft core: storage + state machine + network, no membership
    /// yet. Shared by the single-node constructors and the in-process
    /// cluster constructor; the caller decides how membership is established
    /// (initialize vs. recovered log) and what to wait for.
    async fn boot<LS, NF>(
        node_id: u64,
        network: NF,
        log_store: LS,
        state_machine: InMemoryStateMachine,
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

        // The state machine arrives pre-seeded (from a durable snapshot or a
        // restore artifact) so recovered entries keep their application log
        // indices and new proposals continue numbering after them.
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
        Self::join_tcp(
            node_id,
            config,
            InMemoryLogStore::default(),
            InMemoryStateMachine::volatile(),
            Bootstrap::Initialize,
        )
        .await
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
        let storage = DurableStorage::open(dir)?;
        // Cluster members do not wait for local replay (`last_log_index:
        // None`): convergence is a cluster-level outcome the caller awaits.
        let bootstrap = if storage.log_has_state {
            Bootstrap::Recover { last_log_index: None }
        } else {
            Bootstrap::Initialize
        };
        Self::join_tcp(node_id, config, storage.log_store, storage.state_machine, bootstrap).await
    }

    async fn join_tcp<LS>(
        node_id: u64,
        config: &ClusterConfig,
        log_store: LS,
        state_machine: InMemoryStateMachine,
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
        let mut backend = Self::boot(node_id, network, log_store, state_machine).await?;
        backend.listener_task = Some(crate::tcp::spawn_listener(backend.raft.clone(), listener));

        backend.establish_membership(config.membership(), bootstrap).await?;
        Ok(backend)
    }

    async fn start<LS>(
        log_store: LS,
        state_machine: InMemoryStateMachine,
        bootstrap: Bootstrap,
    ) -> Result<Self>
    where
        LS: RaftLogStorage<TypeConfig>,
    {
        let backend = Self::boot(NODE_ID, NoopNetworkFactory, log_store, state_machine).await?;

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
            // The state machine resumes from the durable snapshot (or empty,
            // if none exists); wait for the recovered log suffix to be
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
        let backend =
            Self::boot(node_id, network, log_store, InMemoryStateMachine::volatile()).await?;

        // Register the inbound RPC loop *before* initializing, so peers that
        // initialized first can already send this node vote/append RPCs.
        router.register(node_id, backend.raft.clone());

        backend.establish_membership(members.clone(), bootstrap).await?;
        Ok(backend)
    }
}

impl<E: DeserializeOwned> OpenraftBackend<E> {
    /// Rebuild a backend from a snapshot previously produced by
    /// [`ConsensusBackend::snapshot`] on an `OpenraftBackend` — restoring a
    /// backup into a **new** consensus group with a fresh log.
    ///
    /// This is distinct from openraft's
    /// [`install_snapshot`](RaftStateMachine::install_snapshot), which
    /// replaces a *replica's* state within an existing group (carrying the
    /// group's raft meta); both paths share the same payload codec and
    /// state-machine seeding (`crate::snapshot`), so there is one snapshot
    /// mechanism with two entry points. The restored entries seed the state
    /// machine; the Raft log itself starts fresh (and in-memory — see
    /// [`from_snapshot_with_data_dir`](Self::from_snapshot_with_data_dir)
    /// for a durable restore).
    pub async fn from_snapshot(snapshot: &Snapshot) -> Result<Self> {
        let entries = Self::decode_snapshot(snapshot)?;
        let state_machine = InMemoryStateMachine::volatile();
        state_machine
            .restore_seed(entries)
            .map_err(|e| ConsensusError::SnapshotCodec(e.to_string()))?;
        Self::start(InMemoryLogStore::default(), state_machine, Bootstrap::Initialize).await
    }

    /// Like [`from_snapshot`](Self::from_snapshot), but the restored node
    /// persists its Raft log and vote under `dir` (Raft Phase A2, PR 2) and,
    /// since Phase A4, the restored entries themselves as a durable seed
    /// snapshot — so the restored state also survives restarts.
    ///
    /// `dir` must not already contain Raft state (log *or* snapshot): a
    /// restore seeded next to recovered state would be two competing
    /// histories, so that case is rejected.
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
        let entries = decode_payload(&snapshot.data)
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

    /// Capture a snapshot through openraft's real snapshot pipeline (Raft
    /// Phase A4): trigger a build, wait for the engine to run
    /// [`RaftSnapshotBuilder::build_snapshot`] (which pins the vacuum
    /// horizon and — on durable configurations — persists the blob before
    /// registering it), then hand back the registered artifact. The Phase A2
    /// shortcut that serialized the state machine directly, bypassing the
    /// builder, is gone (judge follow-up from PR #5351).
    async fn snapshot(&self) -> Result<Snapshot> {
        let Some(target) = self.metrics.borrow().last_applied else {
            // Nothing applied yet (not even a membership entry): there is no
            // log id for openraft to build a snapshot at, and nothing for
            // the builder to capture. Unreachable through the public
            // constructors, which all wait for membership to apply.
            return Ok(Snapshot {
                last_included_index: 0,
                data: encode_payload(&[])
                    .map_err(|e| ConsensusError::SnapshotCodec(e.to_string()))?,
            });
        };

        self.raft
            .trigger()
            .snapshot()
            .await
            .map_err(|e| ConsensusError::Backend(format!("failed to trigger snapshot: {e}")))?;

        // The trigger returns once accepted, not once built: wait until the
        // engine reports a snapshot covering everything applied at the time
        // of this call. (If such a snapshot already exists, the engine may
        // skip the build; the artifact is identical either way.)
        self.raft
            .wait(Some(Duration::from_secs(10)))
            .metrics(move |m| m.snapshot >= Some(target), "snapshot build")
            .await
            .map_err(|e| ConsensusError::Backend(format!("snapshot build did not settle: {e}")))?;

        let (last_included_index, data) =
            self.state_machine.current_snapshot_artifact().ok_or_else(|| {
                ConsensusError::Backend(
                    "snapshot build settled but no snapshot is registered".to_string(),
                )
            })?;
        Ok(Snapshot { last_included_index, data })
    }

    fn role(&self) -> Role {
        self.current_role()
    }
}

#[cfg(test)]
mod tests {
    use openraft::CommittedLeaderId;
    use tempfile::TempDir;

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

    // -----------------------------------------------------------------------
    // Raft Phase A4, PR 1 (#5198): snapshot builder, durable snapshots,
    // vacuum-horizon pin, purge safety.
    // -----------------------------------------------------------------------

    /// [`SnapshotHorizonPin`] that records acquire/release ordering, and
    /// (optionally) whether the snapshot file was already durable when the
    /// pin was released — proving the pin is held across the whole build,
    /// persistence included.
    #[derive(Debug, Default)]
    struct RecordingPin {
        events: Arc<Mutex<Vec<String>>>,
        expect_file: Option<std::path::PathBuf>,
    }

    struct RecordingGuard {
        events: Arc<Mutex<Vec<String>>>,
        expect_file: Option<std::path::PathBuf>,
    }

    impl Drop for RecordingGuard {
        fn drop(&mut self) {
            let event = match &self.expect_file {
                None => "released",
                Some(path) if path.exists() => "released-after-durable",
                Some(_) => "released-before-durable",
            };
            self.events.lock().unwrap().push(event.to_string());
        }
    }

    impl SnapshotHorizonPin for RecordingPin {
        fn acquire(&self) -> Box<dyn Send> {
            self.events.lock().unwrap().push("acquired".to_string());
            Box::new(RecordingGuard {
                events: Arc::clone(&self.events),
                expect_file: self.expect_file.clone(),
            })
        }
    }

    fn events_of(events: &Arc<Mutex<Vec<String>>>) -> Vec<String> {
        events.lock().unwrap().clone()
    }

    /// Build on one node, install into a fresh state machine: applied state,
    /// `last_applied`, and membership all come out identical.
    #[tokio::test]
    async fn snapshot_build_install_roundtrip_restores_state_and_meta() {
        let backend = OpenraftBackend::<String>::new().await.unwrap();
        for i in 1..=3u64 {
            backend.propose(format!("entry-{i}")).await.unwrap();
        }
        // Build + register through the engine's real snapshot pipeline.
        backend.snapshot().await.unwrap();

        let mut source = backend.state_machine.clone();
        let built = source.get_current_snapshot().await.unwrap().expect("snapshot registered");

        let mut target = InMemoryStateMachine::volatile();
        target.install_snapshot(&built.meta, built.snapshot).await.unwrap();

        let (source_applied, source_membership) = source.applied_state().await.unwrap();
        let (target_applied, target_membership) = target.applied_state().await.unwrap();
        assert!(source_applied.is_some(), "source has applied entries");
        assert_eq!(source_applied, target_applied);
        assert_eq!(source_membership, target_membership);
        assert_eq!(source.lock().entries, target.lock().entries);
        assert_eq!(target.lock().entries.len(), 3);
    }

    /// A corrupt snapshot must fail the install loudly and leave the state
    /// machine untouched — never half-install.
    #[tokio::test]
    async fn install_of_a_corrupt_snapshot_is_rejected_without_mutation() {
        let machine = InMemoryStateMachine::volatile();
        machine.lock().entries = vec![b"keep me".to_vec()];

        let meta = SnapshotMeta {
            last_log_id: Some(LogId::new(CommittedLeaderId::new(1, 1), 9)),
            last_membership: StoredMembership::default(),
            snapshot_id: "bogus".to_string(),
        };
        let mut handle = machine.clone();
        handle
            .install_snapshot(&meta, Box::new(Cursor::new(b"not a payload".to_vec())))
            .await
            .unwrap_err();

        let inner = machine.lock();
        assert_eq!(inner.entries, vec![b"keep me".to_vec()]);
        assert_eq!(inner.last_applied, None, "meta must not be applied either");
    }

    /// `ConsensusBackend::snapshot` no longer bypasses the snapshot builder
    /// (judge follow-up from PR #5351): the builder runs, and with it the
    /// vacuum-horizon pin, acquired before and released after the build.
    #[tokio::test]
    async fn trait_snapshot_runs_the_builder_and_pins_the_horizon() {
        let events = Arc::new(Mutex::new(Vec::new()));
        let mut machine = InMemoryStateMachine::volatile();
        machine.pin = Arc::new(RecordingPin { events: Arc::clone(&events), expect_file: None });

        let backend = OpenraftBackend::<String>::start(
            InMemoryLogStore::default(),
            machine,
            Bootstrap::Initialize,
        )
        .await
        .unwrap();
        backend.propose("entry-1".to_string()).await.unwrap();
        assert!(events_of(&events).is_empty(), "no build before snapshot() is called");

        let snapshot = backend.snapshot().await.unwrap();
        assert_eq!(snapshot.last_included_index, 1);
        assert_eq!(events_of(&events), vec!["acquired".to_string(), "released".to_string()]);
    }

    /// On a durable configuration, the pin is held until the snapshot file
    /// is on disk: the guard observes the durable file at release time.
    #[tokio::test]
    async fn horizon_pin_is_held_until_the_snapshot_is_durable() {
        let dir = TempDir::new().unwrap();
        let (store, _) = SnapshotStore::open(dir.path()).unwrap();
        let events = Arc::new(Mutex::new(Vec::new()));
        let expect_file = dir.path().join("snapshot-3.bin");

        let mut machine = InMemoryStateMachine::durable(store);
        machine.pin = Arc::new(RecordingPin {
            events: Arc::clone(&events),
            expect_file: Some(expect_file.clone()),
        });
        {
            let mut inner = machine.lock();
            inner.entries = vec![b"a".to_vec(), b"b".to_vec()];
            inner.last_applied = Some(LogId::new(CommittedLeaderId::new(1, 1), 3));
        }

        let built = machine.build_snapshot().await.unwrap();
        assert_eq!(built.meta.last_log_id.unwrap().index, 3);
        assert!(expect_file.exists(), "snapshot persisted to the data dir");
        assert_eq!(
            events_of(&events),
            vec!["acquired".to_string(), "released-after-durable".to_string()]
        );
    }

    /// The full Phase A4 PR 1 story on one durable node: build a snapshot,
    /// purge the log up to it (legal — at the durable snapshot), write past
    /// it, restart. Recovery loads the snapshot first, then replays the log
    /// suffix; the purged prefix is only recoverable through the snapshot.
    #[tokio::test]
    async fn durable_recovery_loads_snapshot_then_replays_log() {
        let dir = TempDir::new().unwrap();
        {
            let backend = OpenraftBackend::<String>::with_data_dir(dir.path()).await.unwrap();
            for i in 1..=3u64 {
                backend.propose(format!("entry-{i}")).await.unwrap();
            }
            let snapshot = backend.snapshot().await.unwrap();
            assert_eq!(snapshot.last_included_index, 3);

            // Purge the log up to the durable snapshot. (Triggering purge is
            // manual here; *when* to purge — the policy — is PR 2.)
            let snapshot_log_id = backend.metrics.borrow().snapshot.expect("snapshot built");
            backend.raft.trigger().purge_log(snapshot_log_id.index).await.unwrap();
            backend
                .raft
                .wait(Some(Duration::from_secs(10)))
                .metrics(move |m| m.purged >= Some(snapshot_log_id), "log purged")
                .await
                .unwrap();

            // Write past the purge point so recovery must stitch
            // snapshot + log suffix.
            for i in 4..=5u64 {
                backend.propose(format!("entry-{i}")).await.unwrap();
            }
            backend.shutdown().await.unwrap();
        }

        let backend = OpenraftBackend::<String>::with_data_dir(dir.path()).await.unwrap();
        assert_eq!(backend.last_index(), 5);
        for i in 1..=5u64 {
            assert_eq!(backend.read_committed(i).await.unwrap(), format!("entry-{i}"));
        }
        // Numbering continues after the stitched prefix.
        assert_eq!(backend.propose("entry-6".to_string()).await.unwrap(), 6);
    }

    /// A corrupt durable snapshot fails recovery loudly — never a silent
    /// fresh start (the log may be purged up to that snapshot).
    #[tokio::test]
    async fn corrupt_durable_snapshot_fails_recovery_loudly() {
        let dir = TempDir::new().unwrap();
        {
            let backend = OpenraftBackend::<String>::with_data_dir(dir.path()).await.unwrap();
            backend.propose("entry-1".to_string()).await.unwrap();
            backend.snapshot().await.unwrap();
            backend.shutdown().await.unwrap();
        }

        let snapshot_path = std::fs::read_dir(dir.path())
            .unwrap()
            .flatten()
            .map(|e| e.path())
            .find(|p| {
                p.file_name().is_some_and(|n| {
                    let n = n.to_string_lossy();
                    n.starts_with("snapshot-") && n.ends_with(".bin")
                })
            })
            .expect("a durable snapshot file exists");
        let mut buf = std::fs::read(&snapshot_path).unwrap();
        let last = buf.len() - 1;
        buf[last] ^= 0xFF;
        std::fs::write(&snapshot_path, &buf).unwrap();

        let err = OpenraftBackend::<String>::with_data_dir(dir.path()).await.unwrap_err();
        assert!(
            matches!(err, ConsensusError::Backend(ref msg) if msg.contains("corrupt")),
            "unexpected result: {err:?}"
        );
    }

    /// Restoring a `ConsensusBackend::snapshot` artifact into a durable data
    /// dir persists the restored state as a durable seed snapshot, so it now
    /// survives restarts of the restored node (pre-A4 it lived only in
    /// memory and a restart silently lost it).
    #[tokio::test]
    async fn restored_durable_backend_survives_restart() {
        let snapshot = {
            let source = OpenraftBackend::<String>::new().await.unwrap();
            for i in 1..=3u64 {
                source.propose(format!("entry-{i}")).await.unwrap();
            }
            source.snapshot().await.unwrap()
        };

        let dir = TempDir::new().unwrap();
        {
            let restored =
                OpenraftBackend::<String>::from_snapshot_with_data_dir(&snapshot, dir.path())
                    .await
                    .unwrap();
            assert_eq!(restored.last_index(), 3);
            assert_eq!(restored.propose("entry-4".to_string()).await.unwrap(), 4);
            restored.shutdown().await.unwrap();
        }

        let reopened = OpenraftBackend::<String>::with_data_dir(dir.path()).await.unwrap();
        assert_eq!(reopened.last_index(), 4);
        for i in 1..=4u64 {
            assert_eq!(reopened.read_committed(i).await.unwrap(), format!("entry-{i}"));
        }
    }
}
