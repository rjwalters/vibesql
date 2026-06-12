//! Process-level TCP cluster tests (Raft Phase A3, PR 2 of issue #5197) —
//! the suite behind `make test-cluster`.
//!
//! Boots 3-voter [`OpenraftBackend`] clusters whose Raft RPCs travel over
//! the real TCP transport (`crate::tcp` — length-prefixed frames), with
//! each node persisting its Raft log in its own tempdir, and exercises the
//! A3 acceptance scenarios end-to-end: election, replication, leader kill →
//! re-election, restart → rebind + catch-up, minority partition, and
//! garbage on the wire — plus the A4 PR 2 scenarios (#5198): policy-driven
//! snapshot + purge, lagging-follower rejoin via chunked snapshot install,
//! and post-install restart liveness.
//!
//! ## Port strategy
//!
//! Nothing here hardcodes the 5433 port convention: every cluster binds OS-
//! assigned ephemeral ports (`127.0.0.1:0`, all reservations held
//! simultaneously, then released just before the nodes rebind them), so
//! parallel tests and busy CI runners cannot collide. Ephemeral allocation
//! is cyclic, so a just-released port is not handed out again within a test
//! run, and the consensus listener binds with `SO_REUSEADDR`, so a
//! restarted node can rebind its own port immediately.
//!
//! ## Partitions
//!
//! True partitions need per-edge severing — a live node's outbound traffic
//! must die too, which neither killing the node nor the channel network's
//! inbound-queue gating models (see `src/cluster.rs`). Here every directed
//! edge runs through a tiny TCP [`Proxy`], and each node's *local*
//! [`ClusterConfig`] dials peers through its own outgoing proxies (local
//! dial addresses are exactly why the transport does not put addresses in
//! the replicated membership). Severing a proxy kills its live connections
//! and refuses new ones.
//!
//! All synchronization is bounded polling ([`TcpTestCluster::wait_until`],
//! 10s deadline) — no bare sleeps; every wait fails loudly instead of
//! hanging (PR 1's convention).

use std::collections::BTreeMap;
use std::net::TcpListener as StdTcpListener;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

use vibesql_consensus::{
    ClusterConfig, ConsensusBackend, ConsensusError, LogIndex, MvccRaftNode, OpenraftBackend,
    RaftTuning, Role,
};

/// Upper bound for any single cluster-level wait (election, catch-up).
const WAIT_TIMEOUT: Duration = Duration::from_secs(10);

/// Poll interval inside bounded waits.
const POLL_INTERVAL: Duration = Duration::from_millis(20);

/// Reserve `n` distinct localhost addresses with OS-assigned ephemeral
/// ports, keyed by node id `1..=n`. All reservations are held at once (so
/// they are guaranteed distinct) and released on return, just before the
/// cluster's nodes bind them for real.
fn free_localhost_addrs(n: u64) -> BTreeMap<u64, String> {
    let listeners: Vec<(u64, StdTcpListener)> = (1..=n)
        .map(|id| (id, StdTcpListener::bind("127.0.0.1:0").expect("reserve ephemeral port")))
        .collect();
    listeners
        .iter()
        .map(|(id, listener)| {
            (*id, listener.local_addr().expect("reserved port address").to_string())
        })
        .collect()
}

// ---------------------------------------------------------------------------
// Per-edge TCP proxy (partition injection)
// ---------------------------------------------------------------------------

/// A severable TCP forwarder for one directed cluster edge.
///
/// While connected, accepted connections are forwarded byte-for-byte to the
/// target. [`sever`](Self::sever) kills every live forwarded connection
/// (both endpoints see a dead socket) and makes the proxy accept-then-drop
/// new ones, so dialers get a connection that dies on first use — openraft
/// maps either to `Unreachable` and backs off, exactly like a real
/// partition.
struct Proxy {
    /// Address the source node dials instead of the target's real address.
    addr: String,
    enabled: Arc<AtomicBool>,
    /// Live forwarder tasks; aborted (sockets dropped) on sever.
    conns: Arc<Mutex<Vec<JoinHandle<()>>>>,
    accept_task: JoinHandle<()>,
}

impl Proxy {
    async fn spawn(target: String) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind proxy listener");
        let addr = listener.local_addr().expect("proxy address").to_string();
        let enabled = Arc::new(AtomicBool::new(true));
        let conns: Arc<Mutex<Vec<JoinHandle<()>>>> = Arc::default();

        let accept_task = tokio::spawn({
            let enabled = Arc::clone(&enabled);
            let conns = Arc::clone(&conns);
            async move {
                loop {
                    let Ok((mut inbound, _)) = listener.accept().await else {
                        tokio::time::sleep(POLL_INTERVAL).await;
                        continue;
                    };
                    if !enabled.load(Ordering::SeqCst) {
                        continue; // dropping `inbound` hangs up on the dialer
                    }
                    let target = target.clone();
                    let forwarder = tokio::spawn(async move {
                        let Ok(mut outbound) = TcpStream::connect(&target).await else { return };
                        let _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await;
                    });
                    let mut conns = conns.lock().expect("proxy connection list poisoned");
                    conns.retain(|handle| !handle.is_finished());
                    conns.push(forwarder);
                }
            }
        });

        Self { addr, enabled, conns, accept_task }
    }

    /// Cut the edge: live connections die, new ones are accepted and
    /// immediately dropped.
    fn sever(&self) {
        self.enabled.store(false, Ordering::SeqCst);
        let mut conns = self.conns.lock().expect("proxy connection list poisoned");
        for handle in conns.drain(..) {
            handle.abort();
        }
    }

    /// Reconnect the edge for new connections.
    fn reconnect(&self) {
        self.enabled.store(true, Ordering::SeqCst);
    }
}

impl Drop for Proxy {
    fn drop(&mut self) {
        self.accept_task.abort();
        self.sever();
    }
}

// ---------------------------------------------------------------------------
// Cluster harness
// ---------------------------------------------------------------------------

struct TestNode {
    /// `None` while the node is killed. `Arc` so a test can hold the
    /// backend across `&mut self` harness calls (e.g. await a pending
    /// propose on a partitioned node).
    backend: Option<Arc<OpenraftBackend<String>>>,
    /// This node's local view of the cluster (its own real address, peers
    /// possibly behind per-edge proxies).
    config: ClusterConfig,
    /// Data directory holding the node's durable Raft log across restarts.
    dir: TempDir,
    /// The node's real listen address (where raw-socket tests dial it).
    listen_addr: String,
}

/// A 3-voter TCP cluster on localhost ephemeral ports, every node with a
/// durable Raft log in its own tempdir.
struct TcpTestCluster {
    nodes: BTreeMap<u64, TestNode>,
    /// Directed-edge proxies; empty unless built with
    /// [`boot_partitionable`](Self::boot_partitionable).
    proxies: BTreeMap<(u64, u64), Proxy>,
    /// Snapshot/retention tuning every node (and every restore) runs with.
    tuning: RaftTuning,
}

impl TcpTestCluster {
    /// Boot `n` voters (ids `1..=n`) dialing each other directly.
    async fn boot(n: u64) -> Self {
        Self::build(n, false, RaftTuning::default()).await
    }

    /// Like [`boot`](Self::boot), with explicit snapshot/retention tuning
    /// (Raft Phase A4, PR 2: snapshot-transfer and purge-policy scenarios).
    async fn boot_tuned(n: u64, tuning: RaftTuning) -> Self {
        Self::build(n, false, tuning).await
    }

    /// Like [`boot`](Self::boot), but every directed edge runs through a
    /// severable [`Proxy`], enabling [`partition`](Self::partition).
    async fn boot_partitionable(n: u64) -> Self {
        Self::build(n, true, RaftTuning::default()).await
    }

    async fn build(n: u64, with_proxies: bool, tuning: RaftTuning) -> Self {
        let addrs = free_localhost_addrs(n);

        let mut proxies = BTreeMap::new();
        if with_proxies {
            for a in 1..=n {
                for b in 1..=n {
                    if a != b {
                        proxies.insert((a, b), Proxy::spawn(addrs[&b].clone()).await);
                    }
                }
            }
        }

        let mut nodes = BTreeMap::new();
        for id in 1..=n {
            // Node `id`'s view: its own real address (it binds it), every
            // peer behind this node's outgoing proxy when partitionable.
            let view = (1..=n).map(|peer| {
                let addr = if peer == id || !with_proxies {
                    addrs[&peer].clone()
                } else {
                    proxies[&(id, peer)].addr.clone()
                };
                (peer, addr)
            });
            let config = ClusterConfig::new(view).expect("valid cluster config");
            let dir = TempDir::new().expect("create node data directory");
            let backend = OpenraftBackend::join_tcp_cluster_with_data_dir_tuned(
                id,
                &config,
                dir.path(),
                tuning,
            )
            .await
            .expect("boot tcp cluster node");
            nodes.insert(
                id,
                TestNode {
                    backend: Some(Arc::new(backend)),
                    config,
                    dir,
                    listen_addr: addrs[&id].clone(),
                },
            );
        }

        Self { nodes, proxies, tuning }
    }

    /// The backend of a live node. Panics if `id` is killed or unknown.
    fn node(&self, id: u64) -> &Arc<OpenraftBackend<String>> {
        self.nodes
            .get(&id)
            .and_then(|n| n.backend.as_ref())
            .unwrap_or_else(|| panic!("node {id} is killed or unknown"))
    }

    /// The real listen address of node `id`.
    fn listen_addr(&self, id: u64) -> &str {
        &self.nodes[&id].listen_addr
    }

    /// Ids of nodes that are currently alive.
    fn live_ids(&self) -> Vec<u64> {
        self.nodes.iter().filter(|(_, n)| n.backend.is_some()).map(|(id, _)| *id).collect()
    }

    /// "Crash" a node: stop its Raft core and abort its listener, so every
    /// socket it owns — inbound and outbound — dies, exactly like a killed
    /// process. Its data directory survives for [`restore`](Self::restore).
    async fn kill(&mut self, id: u64) {
        let backend = self
            .nodes
            .get_mut(&id)
            .and_then(|n| n.backend.take())
            .unwrap_or_else(|| panic!("node {id} is already killed or unknown"));
        backend.shutdown().await.expect("shut down killed node's raft core");
        // Dropping the (last) Arc also aborts the listener task via Drop,
        // tearing down all of the node's accepted connections.
    }

    /// Restart a killed node: reopen its durable Raft log and rebind its
    /// address. Bind is retried briefly because the killed node's listener
    /// task is aborted asynchronously by the runtime.
    async fn restore(&mut self, id: u64) {
        let tuning = self.tuning;
        let node = self.nodes.get_mut(&id).expect("unknown node");
        assert!(node.backend.is_none(), "node {id} is not killed");

        let deadline = tokio::time::Instant::now() + WAIT_TIMEOUT;
        let backend = loop {
            match OpenraftBackend::join_tcp_cluster_with_data_dir_tuned(
                id,
                &node.config,
                node.dir.path(),
                tuning,
            )
            .await
            {
                Ok(backend) => break backend,
                Err(e) => {
                    assert!(
                        tokio::time::Instant::now() < deadline,
                        "timed out restoring node {id}: {e}"
                    );
                    tokio::time::sleep(POLL_INTERVAL).await;
                }
            }
        };
        node.backend = Some(Arc::new(backend));
    }

    /// Sever every directed edge between `minority` and the rest of the
    /// cluster, in both directions. Panics unless built partitionable.
    fn partition(&self, minority: &[u64]) {
        assert!(!self.proxies.is_empty(), "cluster was not built partitionable");
        for (&(a, b), proxy) in &self.proxies {
            if minority.contains(&a) != minority.contains(&b) {
                proxy.sever();
            }
        }
    }

    /// Reconnect every severed edge.
    fn heal(&self) {
        for proxy in self.proxies.values() {
            proxy.reconnect();
        }
    }

    /// Wait (bounded) until some live node reports itself leader.
    async fn wait_for_leader(&self) -> u64 {
        self.wait_for_leader_among(&self.live_ids()).await
    }

    /// Wait (bounded) until one of `ids` reports itself leader **and**
    /// every node in `ids` acknowledges it; returns the leader.
    ///
    /// Requiring agreement (not just a `Role::Leader` claim) matters over
    /// TCP: nodes boot staggered enough that early elections can be
    /// superseded, leaving a node briefly claiming a leadership it already
    /// lost. Once every polled node has heard from the same leader,
    /// heartbeats are flowing and the claim is settled.
    async fn wait_for_leader_among(&self, ids: &[u64]) -> u64 {
        self.wait_until(&format!("an acknowledged leader among {ids:?}"), || {
            let leader = ids.iter().copied().find(|id| self.node(*id).role() == Role::Leader)?;
            ids.iter().all(|id| self.node(*id).current_leader() == Some(leader)).then_some(leader)
        })
        .await
    }

    /// Wait (bounded) until node `id` has applied the application log entry
    /// at `idx`.
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
fn follower_of(cluster: &TcpTestCluster, leader: u64) -> u64 {
    cluster
        .live_ids()
        .into_iter()
        .find(|id| *id != leader)
        .expect("cluster has at least one follower")
}

// ---------------------------------------------------------------------------
// A3 acceptance scenarios (process-level / PR 2)
// ---------------------------------------------------------------------------

/// Cold start over real sockets: three voters elect exactly one leader, a
/// proposal commits, and all three state machines apply it identically.
#[tokio::test]
async fn boots_elects_and_replicates_over_tcp() {
    let cluster = TcpTestCluster::boot(3).await;
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

    let leaders: Vec<u64> = (1..=3).filter(|id| cluster.node(*id).role() == Role::Leader).collect();
    assert_eq!(leaders, vec![leader], "leadership must be unique once settled");
}

/// Kill the leader — its listener and every socket it owns die, like a
/// crashed process — and the two survivors elect a new leader; writes
/// resume on the remaining majority.
#[tokio::test]
async fn killing_the_leader_severs_its_sockets_and_a_new_leader_emerges() {
    let mut cluster = TcpTestCluster::boot(3).await;
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

/// A killed follower misses writes, then restarts: it reopens its durable
/// Raft log, rebinds its address (`SO_REUSEADDR`), reconnects, and catches
/// up via AppendEntries backfill to the full history.
#[tokio::test]
async fn restarted_node_rebinds_reconnects_and_catches_up() {
    let mut cluster = TcpTestCluster::boot(3).await;
    let leader = cluster.wait_for_leader().await;

    for i in 1..=3u64 {
        let idx = cluster.node(leader).propose(format!("entry-{i}")).await.unwrap();
        assert_eq!(idx, i);
    }
    let follower = follower_of(&cluster, leader);
    cluster.wait_for_apply(follower, 3).await;

    cluster.kill(follower).await;

    // The two survivors are still a majority: writes keep committing.
    for i in 4..=6u64 {
        let idx = cluster.node(leader).propose(format!("entry-{i}")).await.unwrap();
        assert_eq!(idx, i);
    }

    cluster.restore(follower).await;
    cluster.wait_for_apply(follower, 6).await;

    for i in 1..=6u64 {
        assert_eq!(
            cluster.node(follower).read_committed(i).await.unwrap(),
            format!("entry-{i}"),
            "restored follower diverged at index {i}"
        );
    }
}

/// The minority-partition acceptance criterion from #5197, 2-1 split with
/// the leader on the minority side:
///
/// - the majority elects a new leader and keeps committing writes;
/// - a write proposed on the partitioned old leader can never commit, and
///   surfaces as the **typed** [`ConsensusError::NotLeader`] (with the new
///   leader as hint) once the heal lets the old leader learn it was
///   deposed — openraft fails the pending proposal when the new leader's
///   log truncates the uncommitted entry;
/// - after the heal, everyone converges on the majority history and the
///   rejected write appears nowhere.
#[tokio::test]
async fn minority_partition_rejects_writes_while_the_majority_continues() {
    let cluster = TcpTestCluster::boot_partitionable(3).await;
    let old_leader = cluster.wait_for_leader().await;

    let idx = cluster.node(old_leader).propose("before-partition".to_string()).await.unwrap();
    for id in 1..=3 {
        cluster.wait_for_apply(id, idx).await;
    }

    // 2-1 split: the leader alone on the minority side, both directions
    // severed.
    cluster.partition(&[old_leader]);

    // A write proposed on the minority leader cannot reach quorum; it stays
    // pending until the node learns it was deposed (openraft has no
    // check-quorum step-down, so the rejection arrives at heal time).
    let minority = Arc::clone(cluster.node(old_leader));
    let pending = tokio::spawn(async move { minority.propose("minority-write".to_string()).await });

    // Majority side: a new leader emerges and writes keep committing.
    let majority: Vec<u64> = (1..=3).filter(|id| *id != old_leader).collect();
    let new_leader = cluster.wait_for_leader_among(&majority).await;
    let idx2 = cluster.node(new_leader).propose("majority-write".to_string()).await.unwrap();
    assert_eq!(idx2, 2, "the minority write must not have consumed an index");
    for id in &majority {
        cluster.wait_for_apply(*id, idx2).await;
        assert_eq!(cluster.node(*id).read_committed(idx2).await.unwrap(), "majority-write");
    }

    cluster.heal();

    // The deposed leader truncates its uncommitted entry when the new
    // leader's log reaches it; the pending write surfaces NotLeader with
    // the new leader as hint.
    let result = tokio::time::timeout(WAIT_TIMEOUT, pending)
        .await
        .expect("the minority write should resolve after the partition heals")
        .expect("the propose task must not panic");
    match result {
        Err(ConsensusError::NotLeader { leader_hint }) => assert_eq!(
            leader_hint,
            Some(new_leader),
            "the rejection should point at the leader that deposed the entry"
        ),
        other => panic!("minority write must surface NotLeader, got: {other:?}"),
    }

    // Everyone converges on the majority history; the rejected write
    // appears nowhere.
    cluster.wait_for_apply(old_leader, idx2).await;
    for id in 1..=3 {
        assert_eq!(cluster.node(id).read_committed(idx).await.unwrap(), "before-partition");
        assert_eq!(cluster.node(id).read_committed(idx2).await.unwrap(), "majority-write");
    }

    // And the stepped-down old leader now rejects writes outright, hinting
    // at the new leader.
    cluster
        .wait_until("the old leader to step down and learn who leads", || {
            (cluster.node(old_leader).role() == Role::Follower
                && cluster.node(old_leader).current_leader() == Some(new_leader))
            .then_some(())
        })
        .await;
    let err = cluster.node(old_leader).propose("still rejected".to_string()).await.unwrap_err();
    match err {
        ConsensusError::NotLeader { leader_hint } => assert_eq!(leader_hint, Some(new_leader)),
        other => panic!("expected NotLeader from the deposed leader, got: {other:?}"),
    }
}

/// Read until the server hangs up; panics if it answers or stays open.
async fn assert_connection_closed(mut stream: TcpStream) {
    let mut buf = [0u8; 16];
    match tokio::time::timeout(WAIT_TIMEOUT, stream.read(&mut buf)).await {
        Ok(Ok(0)) | Ok(Err(_)) => {} // clean close or reset — both fine
        Ok(Ok(n)) => panic!("server answered {n} bytes instead of dropping the connection"),
        Err(_) => panic!("server kept the connection open after a malformed frame"),
    }
}

/// Garbage on the wire — an undecodable payload, an absurd length prefix,
/// and a torn frame — gets the offending connection dropped (no panic, no
/// allocation blow-up) while the cluster keeps replicating.
#[tokio::test]
async fn garbage_frames_drop_the_connection_without_disrupting_the_cluster() {
    let cluster = TcpTestCluster::boot(3).await;
    let leader = cluster.wait_for_leader().await;

    let idx = cluster.node(leader).propose("before-garbage".to_string()).await.unwrap();
    for id in 1..=3 {
        cluster.wait_for_apply(id, idx).await;
    }

    let victim = follower_of(&cluster, leader);
    let addr = cluster.listen_addr(victim).to_string();

    // Well-framed garbage payload: decode fails, connection dropped.
    let mut stream = TcpStream::connect(&addr).await.unwrap();
    stream.write_all(&7u32.to_le_bytes()).await.unwrap();
    stream.write_all(b"garbage").await.unwrap();
    assert_connection_closed(stream).await;

    // Absurd length prefix: rejected before allocation, connection dropped.
    let mut stream = TcpStream::connect(&addr).await.unwrap();
    stream.write_all(&u32::MAX.to_le_bytes()).await.unwrap();
    assert_connection_closed(stream).await;

    // Torn frame: promise 100 bytes, deliver 10, hang up. The server's
    // read fails and the connection just ends.
    let mut stream = TcpStream::connect(&addr).await.unwrap();
    stream.write_all(&100u32.to_le_bytes()).await.unwrap();
    stream.write_all(b"only ten b").await.unwrap();
    drop(stream);

    // The cluster keeps replicating as if nothing happened.
    let idx2 = cluster.node(leader).propose("after-garbage".to_string()).await.unwrap();
    for id in 1..=3 {
        cluster.wait_for_apply(id, idx2).await;
        assert_eq!(cluster.node(id).read_committed(idx2).await.unwrap(), "after-garbage");
    }
}

// ---------------------------------------------------------------------------
// A4 acceptance scenarios (snapshot transfer + purge policy, PR 2 of #5198)
// ---------------------------------------------------------------------------

/// Tuning that makes the A4 scenarios cheap to trigger: snapshot every 8
/// log entries, purge everything snapshotted (no catch-up tail) — so a
/// follower that misses more than a handful of writes can only be healed by
/// snapshot transfer, never by log replay.
fn aggressive_purge_tuning() -> RaftTuning {
    RaftTuning { snapshot_after_entries: 8, keep_in_log: 0, ..RaftTuning::default() }
}

/// Drive the lagging-follower-rejoin-via-snapshot flow and return the
/// converged cluster:
///
/// 1. 3 voters; a follower applies the first 3 entries and is killed.
/// 2. The survivors commit `entries` total writes of `payload_bytes` each;
///    the policy snapshots and purges the leader's log **past** everything
///    the dead follower ever held (its log ended around raw index 5; we
///    wait for the purge watermark to clear raw index 10).
/// 3. The follower restores. Its gap is purged on the leader, so log
///    replay cannot heal it — catch-up must go through openraft's chunked
///    snapshot install over the TCP `InstallSnapshot` leg.
/// 4. The follower converges on the full history, byte-identical.
async fn rejoin_lagging_follower_via_snapshot(
    tuning: RaftTuning,
    entries: u64,
    payload_bytes: usize,
    convergence_timeout: Duration,
) -> (TcpTestCluster, u64, u64) {
    let value = {
        let payload = "x".repeat(payload_bytes);
        move |i: u64| format!("entry-{i}-{payload}")
    };

    let mut cluster = TcpTestCluster::boot_tuned(3, tuning).await;
    let leader = cluster.wait_for_leader().await;

    for i in 1..=3u64 {
        cluster.node(leader).propose(value(i)).await.unwrap();
    }
    let follower = follower_of(&cluster, leader);
    cluster.wait_for_apply(follower, 3).await;
    cluster.kill(follower).await;

    for i in 4..=entries {
        cluster.node(leader).propose(value(i)).await.unwrap();
    }
    cluster
        .wait_until("the leader's policy purge to pass the dead follower's position", || {
            (cluster.node(leader).purged_log_index() >= Some(10)).then_some(())
        })
        .await;

    cluster.restore(follower).await;
    // Bounded wait with a scenario-specific deadline (the stress variant
    // moves a lot more data than WAIT_TIMEOUT is sized for).
    let deadline = tokio::time::Instant::now() + convergence_timeout;
    while cluster.node(follower).last_index() < entries {
        assert!(
            tokio::time::Instant::now() < deadline,
            "follower did not converge to {entries} entries within {convergence_timeout:?} \
             (at {})",
            cluster.node(follower).last_index()
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }

    for i in 1..=entries {
        assert_eq!(
            cluster.node(follower).read_committed(i).await.unwrap(),
            value(i),
            "rejoined follower diverged at index {i}"
        );
    }
    // Supporting evidence that the heal was a snapshot install: the
    // follower now knows a snapshot covering the purged prefix it could
    // never have replayed.
    assert!(
        cluster.node(follower).snapshot_log_index() >= Some(10),
        "rejoined follower should hold a snapshot covering the purged prefix, has {:?}",
        cluster.node(follower).snapshot_log_index()
    );

    (cluster, leader, follower)
}

/// The A4 rejoin acceptance criterion plus the #5369 judge's PR 2 liveness
/// requirement, end-to-end: a follower whose gap was purged heals via
/// snapshot install, converges, and then **survives a subsequent
/// kill/restart** — recovery from a data directory whose durable snapshot
/// arrived over the network (snapshot first, then whatever log tail exists)
/// comes back cleanly and stays current.
#[tokio::test]
async fn lagging_follower_rejoins_via_snapshot_and_survives_restart() {
    let (mut cluster, leader, follower) =
        rejoin_lagging_follower_via_snapshot(aggressive_purge_tuning(), 30, 16, WAIT_TIMEOUT).await;

    cluster.kill(follower).await;
    cluster.restore(follower).await;

    let idx = cluster.node(leader).propose("after-second-restart".to_string()).await.unwrap();
    assert_eq!(idx, 31, "application indices stay dense across install + restart");
    cluster.wait_for_apply(follower, idx).await;
    assert_eq!(cluster.node(follower).read_committed(idx).await.unwrap(), "after-second-restart");
    // And the pre-restart history is still all there.
    for i in [1u64, 10, 30] {
        assert!(cluster.node(follower).read_committed(i).await.is_ok(), "lost index {i}");
    }
}

/// Snapshot transfer is *chunked*: with 4 KiB chunks, the ~250 KiB snapshot
/// blob this builds (30 entries x 2 KiB payload, JSON-encoded) streams as
/// dozens of bounded frames. This is the CI-sane proof that the transport's
/// 64 MiB frame limit bounds a chunk, never the snapshot — see
/// `large_snapshot_transfer_stress` for the beyond-64 MiB variant.
#[tokio::test]
async fn snapshot_transfer_spans_many_chunks() {
    let tuning = RaftTuning { snapshot_chunk_bytes: 4096, ..aggressive_purge_tuning() };
    rejoin_lagging_follower_via_snapshot(tuning, 30, 2048, WAIT_TIMEOUT).await;
}

/// Env-tunable large-snapshot variant for manual / nightly runs (curator
/// acceptance criterion on #5198): set `VIBESQL_SNAPSHOT_STRESS_MIB` to the
/// desired snapshot-blob size in MiB — e.g. `80` pushes the blob past the
/// 64 MiB frame limit that capped a single InstallSnapshot frame before
/// chunking (judge follow-up from PR #5368) — and the rejoin still heals
/// through bounded default-size (3 MiB) chunks.
#[tokio::test]
async fn large_snapshot_transfer_stress() {
    let Ok(mib) = std::env::var("VIBESQL_SNAPSHOT_STRESS_MIB") else {
        eprintln!("skipping large-snapshot stress: set VIBESQL_SNAPSHOT_STRESS_MIB (e.g. 80)");
        return;
    };
    let mib: usize = mib.parse().expect("VIBESQL_SNAPSHOT_STRESS_MIB must be a number of MiB");

    // The snapshot blob JSON-encodes each payload byte as ~4 wire bytes, so
    // size the raw payload at a quarter of the requested blob size, spread
    // over 64 entries.
    let entries = 64u64;
    let payload_bytes = mib * 1024 * 1024 / 4 / entries as usize;
    rejoin_lagging_follower_via_snapshot(
        aggressive_purge_tuning(),
        entries,
        payload_bytes,
        Duration::from_secs(300),
    )
    .await;
}

// ---------------------------------------------------------------------------
// Raft Phase B1, PR 2 (#5199): the MVCC state machine over real sockets
// ---------------------------------------------------------------------------

/// Bounded poll shared by the MVCC test below (the `TcpTestCluster`
/// methods are bound to the echo backend).
async fn wait_for<T>(what: &str, mut probe: impl FnMut() -> Option<T>) -> T {
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

/// Drive one write through whoever currently leads the MVCC cluster,
/// tolerating elections mid-test.
async fn execute_on_mvcc_leader(
    nodes: &BTreeMap<u64, MvccRaftNode>,
    sql: &str,
) -> (LogIndex, vibesql_consensus::ApplyOutcome) {
    let deadline = tokio::time::Instant::now() + WAIT_TIMEOUT;
    loop {
        let leader = wait_for("a leader to be elected", || {
            nodes.iter().find(|(_, n)| n.role() == Role::Leader).map(|(id, _)| *id)
        })
        .await;
        match nodes[&leader].execute_replicated(sql).await {
            Ok(result) => return result,
            Err(ConsensusError::NotLeader { .. }) => {
                assert!(
                    tokio::time::Instant::now() < deadline,
                    "timed out executing {sql:?}: leadership never settled"
                );
            }
            Err(other) => panic!("execute of {sql:?} failed: {other:?}"),
        }
    }
}

/// A 3-voter **MVCC** cluster over the TCP transport: replicated DML —
/// including nondeterministic SQL frozen at propose — converges on every
/// node; a follower write fails fast with `NotLeader` + leader hint.
/// (The full failure-injection matrix for the MVCC machine runs on the
/// in-process channel transport in `src/mvcc_raft.rs`; this proves the
/// mount end-to-end over real sockets.)
#[tokio::test]
async fn mvcc_three_node_cluster_replicates_over_tcp() {
    let addrs = free_localhost_addrs(3);
    let config = ClusterConfig::new(addrs).expect("valid cluster config");

    let mut nodes: BTreeMap<u64, MvccRaftNode> = BTreeMap::new();
    for id in 1..=3 {
        nodes.insert(
            id,
            MvccRaftNode::join_tcp_cluster(id, &config).await.expect("boot mvcc tcp node"),
        );
    }

    let statements = [
        "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT, r INTEGER, d DATE)",
        "INSERT INTO t VALUES (1, 'one', random(), curdate())",
        "INSERT INTO t VALUES (2, 'two', random(), curdate())",
        "UPDATE t SET v = 'TWO' WHERE id = 2",
        "DELETE FROM t WHERE id = 1",
    ];
    let mut last_dense = 0;
    for (i, sql) in statements.iter().enumerate() {
        let (idx, outcome) = execute_on_mvcc_leader(&nodes, sql).await;
        assert_eq!(idx, (i + 1) as LogIndex, "{sql}");
        assert!(
            matches!(outcome, vibesql_consensus::ApplyOutcome::Applied { .. }),
            "{sql}: {outcome:?}"
        );
        last_dense = idx;
    }

    // Every voter applies the same log — identical rows everywhere,
    // including the frozen random()/curdate() values.
    for (id, node) in &nodes {
        let id = *id;
        wait_for(&format!("node {id} to apply dense index {last_dense}"), || {
            (node.last_applied() >= last_dense).then_some(())
        })
        .await;
    }
    let reference = nodes[&1].query("SELECT id, v, r, d FROM t ORDER BY id").unwrap();
    assert_eq!(reference.len(), 1, "one live row after the DELETE");
    for id in 2..=3 {
        assert_eq!(
            nodes[&id].query("SELECT id, v, r, d FROM t ORDER BY id").unwrap(),
            reference,
            "node {id} diverged"
        );
    }

    // Leader-only writes: a follower fails fast with a leader hint.
    let leader = wait_for("a settled leader", || {
        nodes.iter().find(|(_, n)| n.role() == Role::Leader).map(|(id, _)| *id)
    })
    .await;
    let follower = (1..=3).find(|id| *id != leader).expect("a follower exists");
    wait_for("the follower to learn who leads", || {
        (nodes[&follower].current_leader() == Some(leader)).then_some(())
    })
    .await;
    let err = nodes[&follower]
        .execute_replicated("INSERT INTO t VALUES (3, 'nope', random(), curdate())")
        .await
        .unwrap_err();
    match err {
        ConsensusError::NotLeader { leader_hint } => assert_eq!(leader_hint, Some(leader)),
        other => panic!("expected NotLeader with a hint, got: {other:?}"),
    }

    for node in nodes.values() {
        node.shutdown().await.expect("clean shutdown");
    }
}
