//! In-process multi-node cluster harness and deterministic replication
//! tests (Raft Phase A3, PR 1 of issue #5197).
//!
//! [`TestCluster`] boots N `OpenraftBackend` voters in one process, routing
//! their Raft RPCs through the channel transport in [`crate::network`], and
//! exposes the failure-injection primitives the A3 acceptance scenarios
//! need: `kill`, `restore`, `wait_for_leader`, `wait_for_apply`. All waits
//! are bounded polls with explicit timeouts — no bare sleeps as
//! synchronization — so the tests are CI-deterministic.
//!
//! Storage comes in both flavors:
//!
//! - **In-memory** ([`InMemoryLogStore`]): the harness keeps a handle to
//!   each node's store, so a killed node's log and vote survive until
//!   `restore` reboots a node on the same store — modeling a process that
//!   lost its volatile state machine but kept its Raft log.
//! - **Durable** ([`DurableLogStore`]): each node gets a tempdir; `restore`
//!   reopens `raft.log` from disk, exercising the real recovery path from
//!   Phase A2 inside a cluster.
//!
//! Deliberately out of scope here (matching the issue): snapshot-based
//! catch-up (Phase A4, #5198 — these tests stay far below any log-purge
//! threshold so catch-up always happens via plain AppendEntries backfill),
//! TCP transport and process-level clusters (PR 2), and dynamic membership.

use std::collections::BTreeMap;
use std::time::Duration;

use openraft::BasicNode;
use tempfile::TempDir;

use crate::durable::DurableLogStore;
use crate::network::ChannelRouter;
use crate::openraft_backend::{Bootstrap, InMemoryLogStore};
use crate::{ConsensusBackend, ConsensusError, LogIndex, OpenraftBackend, Role};

/// Upper bound for any single cluster-level wait (election, catch-up).
/// Generous next to the 200–400ms election timeouts so CI machines under
/// load do not flake, but every wait still fails loudly instead of hanging.
const WAIT_TIMEOUT: Duration = Duration::from_secs(10);

/// Poll interval inside bounded waits.
const POLL_INTERVAL: Duration = Duration::from_millis(20);

/// Where a node keeps its Raft log + vote between `kill` and `restore`.
enum NodeStorage {
    /// Shared handle to the killed node's in-memory store.
    InMemory(InMemoryLogStore),
    /// Data directory whose `raft.log` is reopened on restore.
    Durable(TempDir),
}

struct TestNode {
    /// `None` while the node is killed.
    backend: Option<OpenraftBackend<String>>,
    storage: NodeStorage,
}

/// An in-process Raft cluster of [`OpenraftBackend`] voters wired together
/// by the channel transport.
struct TestCluster {
    router: ChannelRouter,
    /// The static membership every node was initialized with.
    members: BTreeMap<u64, BasicNode>,
    nodes: BTreeMap<u64, TestNode>,
}

impl TestCluster {
    /// Boot `n` voters (ids `1..=n`) with in-memory Raft logs and initialize
    /// the n-voter membership.
    async fn new(n: u64) -> Self {
        Self::build(n, false).await
    }

    /// Like [`new`](Self::new), but every node persists its Raft log + vote
    /// in its own temporary data directory.
    async fn new_durable(n: u64) -> Self {
        Self::build(n, true).await
    }

    async fn build(n: u64, durable: bool) -> Self {
        let router = ChannelRouter::default();
        let members: BTreeMap<u64, BasicNode> =
            (1..=n).map(|id| (id, BasicNode::default())).collect();

        let mut nodes = BTreeMap::new();
        for id in 1..=n {
            let node = if durable {
                let dir = TempDir::new().expect("create tempdir for durable raft log");
                let store = DurableLogStore::open(dir.path()).expect("open durable raft log");
                let backend = OpenraftBackend::join_channel_cluster(
                    id,
                    &members,
                    &router,
                    store,
                    Bootstrap::Initialize,
                )
                .await
                .expect("boot durable cluster node");
                TestNode { backend: Some(backend), storage: NodeStorage::Durable(dir) }
            } else {
                let store = InMemoryLogStore::default();
                let backend = OpenraftBackend::join_channel_cluster(
                    id,
                    &members,
                    &router,
                    store.clone(),
                    Bootstrap::Initialize,
                )
                .await
                .expect("boot cluster node");
                TestNode { backend: Some(backend), storage: NodeStorage::InMemory(store) }
            };
            nodes.insert(id, node);
        }

        Self { router, members, nodes }
    }

    /// The backend of a live node. Panics if `id` is killed or unknown.
    fn node(&self, id: u64) -> &OpenraftBackend<String> {
        self.nodes
            .get(&id)
            .and_then(|n| n.backend.as_ref())
            .unwrap_or_else(|| panic!("node {id} is killed or unknown"))
    }

    /// Ids of nodes that are currently alive.
    fn live_ids(&self) -> Vec<u64> {
        self.nodes.iter().filter(|(_, n)| n.backend.is_some()).map(|(id, _)| *id).collect()
    }

    /// "Crash" a node: cut it off from the network (peers see
    /// `Unreachable`, like a dead socket) and stop its Raft core. Its log
    /// and vote remain in [`NodeStorage`] so it can be [`restore`]d.
    ///
    /// [`restore`]: Self::restore
    async fn kill(&mut self, id: u64) {
        // Deregister first so peers stop reaching a half-shut-down core.
        self.router.deregister(id);
        let backend = self
            .nodes
            .get_mut(&id)
            .and_then(|n| n.backend.take())
            .unwrap_or_else(|| panic!("node {id} is already killed or unknown"));
        backend.shutdown().await.expect("shut down killed node's raft core");
    }

    /// Restart a killed node from its retained storage. The node rejoins
    /// with its pre-kill log and vote and catches up via log replication;
    /// callers await convergence with [`wait_for_apply`](Self::wait_for_apply).
    async fn restore(&mut self, id: u64) {
        let node = self.nodes.get_mut(&id).expect("unknown node");
        assert!(node.backend.is_none(), "node {id} is not killed");

        // The reopened log already contains the membership entry (and the
        // node's vote), so this is a recovery boot: re-running `initialize`
        // would be rejected. `last_log_index: None` because a cluster member
        // does not need to finish local replay before returning — the test
        // awaits cluster-level convergence instead.
        let bootstrap = Bootstrap::Recover { last_log_index: None };
        let backend = match &node.storage {
            NodeStorage::InMemory(store) => {
                OpenraftBackend::join_channel_cluster(
                    id,
                    &self.members,
                    &self.router,
                    store.clone(),
                    bootstrap,
                )
                .await
            }
            NodeStorage::Durable(dir) => {
                let store = DurableLogStore::open(dir.path()).expect("reopen durable raft log");
                OpenraftBackend::join_channel_cluster(
                    id,
                    &self.members,
                    &self.router,
                    store,
                    bootstrap,
                )
                .await
            }
        };
        node.backend = Some(backend.expect("restore killed node"));
    }

    /// Wait (bounded) until some live node reports itself leader; returns
    /// its id.
    async fn wait_for_leader(&self) -> u64 {
        self.wait_until("a leader to be elected", || {
            self.live_ids().into_iter().find(|id| self.node(*id).role() == Role::Leader)
        })
        .await
    }

    /// Wait (bounded) until node `id` has applied the application log entry
    /// at `idx` (i.e. `last_index() >= idx`).
    async fn wait_for_apply(&self, id: u64, idx: LogIndex) {
        self.wait_until(&format!("node {id} to apply log index {idx}"), || {
            (self.node(id).last_index() >= idx).then_some(())
        })
        .await;
    }

    /// Poll `probe` until it yields a value, panicking after
    /// [`WAIT_TIMEOUT`]. The single bounded-wait primitive every
    /// cluster-level synchronization goes through.
    async fn wait_until<T>(&self, what: &str, mut probe: impl FnMut() -> Option<T>) -> T {
        let deadline = tokio::time::Instant::now() + WAIT_TIMEOUT;
        loop {
            if let Some(value) = probe() {
                return value;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "timed out after {WAIT_TIMEOUT:?} waiting for {what}"
            );
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }
}

/// Pick a live node that is not `leader`.
fn follower_of(cluster: &TestCluster, leader: u64) -> u64 {
    cluster
        .live_ids()
        .into_iter()
        .find(|id| *id != leader)
        .expect("cluster has at least one follower")
}

// ---------------------------------------------------------------------------
// A3 acceptance scenarios (in-process / PR 1)
// ---------------------------------------------------------------------------

/// Cold start: three voters elect exactly one leader, a proposal on the
/// leader commits, and all three state machines apply it identically.
#[tokio::test]
async fn boot_elects_leader_and_replicates_to_all() {
    let cluster = TestCluster::new(3).await;
    let leader = cluster.wait_for_leader().await;

    let idx = cluster.node(leader).propose("write-1".to_string()).await.unwrap();
    assert_eq!(idx, 1, "first application entry gets the first dense index");

    for id in 1..=3 {
        cluster.wait_for_apply(id, idx).await;
        assert_eq!(
            cluster.node(id).read_committed(idx).await.unwrap(),
            "write-1",
            "node {id} applied a different value"
        );
    }

    // With replication settled and heartbeats healthy, leadership is
    // unique: exactly the elected node reports `Role::Leader`.
    let leaders: Vec<u64> = (1..=3).filter(|id| cluster.node(*id).role() == Role::Leader).collect();
    assert_eq!(leaders, vec![leader]);
}

/// Kill the leader: the two survivors elect a new leader and writes resume,
/// committing on the remaining majority.
#[tokio::test]
async fn killing_the_leader_elects_a_new_one_and_writes_resume() {
    let mut cluster = TestCluster::new(3).await;
    let leader = cluster.wait_for_leader().await;

    let idx = cluster.node(leader).propose("before-failover".to_string()).await.unwrap();
    for id in cluster.live_ids() {
        cluster.wait_for_apply(id, idx).await;
    }

    cluster.kill(leader).await;

    let new_leader = cluster.wait_for_leader().await;
    assert_ne!(new_leader, leader, "the killed node cannot be the new leader");

    let idx2 = cluster.node(new_leader).propose("after-failover".to_string()).await.unwrap();
    assert_eq!(idx2, 2, "application indices stay dense across the failover");

    for id in cluster.live_ids() {
        cluster.wait_for_apply(id, idx2).await;
        assert_eq!(cluster.node(id).read_committed(idx2).await.unwrap(), "after-failover");
    }
}

/// A killed follower misses writes, then catches up via AppendEntries
/// backfill after being restored, converging on the full history. (The
/// entry count stays far below any snapshot/purge threshold on purpose:
/// snapshot-based catch-up is Phase A4, #5198.)
#[tokio::test]
async fn restored_follower_catches_up_via_log_replication() {
    let mut cluster = TestCluster::new(3).await;
    let leader = cluster.wait_for_leader().await;

    for i in 1..=5u64 {
        let idx = cluster.node(leader).propose(format!("entry-{i}")).await.unwrap();
        assert_eq!(idx, i);
    }
    let follower = follower_of(&cluster, leader);
    cluster.wait_for_apply(follower, 5).await;

    cluster.kill(follower).await;

    // The two survivors are still a majority: writes keep committing.
    for i in 6..=10u64 {
        let idx = cluster.node(leader).propose(format!("entry-{i}")).await.unwrap();
        assert_eq!(idx, i);
    }

    cluster.restore(follower).await;
    cluster.wait_for_apply(follower, 10).await;

    for i in 1..=10u64 {
        assert_eq!(
            cluster.node(follower).read_committed(i).await.unwrap(),
            format!("entry-{i}"),
            "restored follower diverged at index {i}"
        );
    }
}

/// Proposing on a follower surfaces as a **typed error** —
/// [`ConsensusError::NotLeader`] with the follower's best guess at the
/// leader id — rather than auto-forwarding to the leader.
///
/// Decision (A3 scope question): no transparent forwarding. The typed error
/// keeps the adapter thin and the failure mode explicit; the caller (or the
/// SQL layer above it) retries against `leader_hint`. Leader-aware routing
/// and read semantics are revisited in B2 (#5200).
#[tokio::test]
async fn propose_on_follower_surfaces_not_leader_with_hint() {
    let cluster = TestCluster::new(3).await;
    let leader = cluster.wait_for_leader().await;
    let follower = follower_of(&cluster, leader);

    // Wait until the follower has heard from the leader, so the hint it
    // attaches is deterministic rather than `None`.
    cluster
        .wait_until("the follower to learn who leads", || {
            (cluster.node(follower).current_leader() == Some(leader)).then_some(())
        })
        .await;

    let err = cluster.node(follower).propose("not here".to_string()).await.unwrap_err();
    match err {
        ConsensusError::NotLeader { leader_hint } => assert_eq!(leader_hint, Some(leader)),
        other => panic!("expected NotLeader with a hint, got: {other:?}"),
    }
}

/// Durable variant of kill/restore: a node persisting its Raft log on disk
/// is killed, recovers its pre-kill log + vote from `raft.log` on restart,
/// and rejoins the cluster to catch up on the writes it missed.
#[tokio::test]
async fn durable_node_recovers_from_disk_and_rejoins() {
    let mut cluster = TestCluster::new_durable(3).await;
    let leader = cluster.wait_for_leader().await;

    for i in 1..=3u64 {
        let idx = cluster.node(leader).propose(format!("entry-{i}")).await.unwrap();
        assert_eq!(idx, i);
    }
    let follower = follower_of(&cluster, leader);
    cluster.wait_for_apply(follower, 3).await;

    cluster.kill(follower).await;

    for i in 4..=5u64 {
        let idx = cluster.node(leader).propose(format!("entry-{i}")).await.unwrap();
        assert_eq!(idx, i);
    }

    // Restore reopens the node's raft.log from its data directory: entries
    // 1..=3 (and the vote) come back from disk, 4..=5 arrive via
    // replication.
    cluster.restore(follower).await;
    cluster.wait_for_apply(follower, 5).await;

    for i in 1..=5u64 {
        assert_eq!(
            cluster.node(follower).read_committed(i).await.unwrap(),
            format!("entry-{i}"),
            "durable node diverged at index {i}"
        );
    }
}
