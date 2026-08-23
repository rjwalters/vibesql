//! Linearizable leader reads over real sockets (Raft Phase B2, #5200,
//! PR 1 of 2) — the stale-leader fencing acceptance.
//!
//! Boots 3-voter [`MvccRaftNode`] clusters whose Raft RPCs travel over the
//! TCP transport, with every directed edge optionally behind a severable
//! TCP proxy (the partition machinery introduced for #5197's minority-
//! partition test), and proves the B2 PR 1 read-path contract end-to-end:
//!
//! - a leader serves `query_linearizable` and sees its own writes;
//! - a follower redirects linearizable reads with a leader hint while its plain `query` keeps
//!   serving (stale-allowed by contract);
//! - **fencing**: a leader partitioned into the minority FAILS `query_linearizable` (it cannot
//!   confirm a quorum) while the majority's new leader serves both writes and linearizable reads;
//!   the minority leader's plain local read stays available but stale; after the heal the deposed
//!   leader steps down and redirects at the real leader.
//!
//! The `Proxy` here intentionally duplicates the one in
//! `tests/tcp_cluster.rs`: integration-test binaries are separate crates,
//! and keeping this file self-contained also keeps it conflict-free with
//! parallel work on the echo-cluster suite (#5385).
//!
//! All synchronization is bounded polling (10s deadline) — no bare
//! sleeps; every wait fails loudly instead of hanging.

use std::{
    collections::BTreeMap,
    net::TcpListener as StdTcpListener,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use tokio::{
    net::{TcpListener, TcpStream},
    task::JoinHandle,
};
use vibesql_consensus::{
    ApplyOutcome, ClusterConfig, ConsensusError, LogIndex, MvccRaftNode, QueryResult, RaftTuning,
    Role,
};
use vibesql_types::SqlValue;

/// Upper bound for any single cluster-level wait (election, catch-up,
/// fencing rejection).
const WAIT_TIMEOUT: Duration = Duration::from_secs(10);

/// Poll interval inside bounded waits.
const POLL_INTERVAL: Duration = Duration::from_millis(20);

/// Bind `n` distinct localhost listeners on OS-assigned ephemeral ports,
/// keyed by node id `1..=n`. The listeners are returned **still bound** so
/// the cluster boots each node directly onto its own socket
/// ([`MvccRaftNode::join_tcp_cluster_with_listener`]) — no port is ever
/// released and rebound, closing the reserve-then-rebind window that races
/// parallel CI for "Address already in use" (#5507).
fn bound_localhost_listeners(n: u64) -> BTreeMap<u64, StdTcpListener> {
    (1..=n)
        .map(|id| (id, StdTcpListener::bind("127.0.0.1:0").expect("bind ephemeral port")))
        .collect()
}

// ---------------------------------------------------------------------------
// Per-edge TCP proxy (partition injection) — see tests/tcp_cluster.rs
// ---------------------------------------------------------------------------

/// A severable TCP forwarder for one directed cluster edge.
///
/// While connected, accepted connections are forwarded byte-for-byte to
/// the target. [`sever`](Self::sever) kills every live forwarded
/// connection (both endpoints see a dead socket) and makes the proxy
/// accept-then-drop new ones, so dialers get a connection that dies on
/// first use — openraft maps either to `Unreachable` and backs off,
/// exactly like a real partition.
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
// MVCC cluster harness
// ---------------------------------------------------------------------------

/// A 3-voter MVCC cluster over the TCP transport, optionally with every
/// directed edge behind a severable [`Proxy`].
struct MvccTcpCluster {
    nodes: BTreeMap<u64, MvccRaftNode>,
    /// Directed-edge proxies; empty unless built with
    /// [`boot_partitionable`](Self::boot_partitionable).
    proxies: BTreeMap<(u64, u64), Proxy>,
}

impl MvccTcpCluster {
    /// Boot `n` voters (ids `1..=n`) dialing each other directly.
    async fn boot(n: u64) -> Self {
        Self::build(n, false).await
    }

    /// Like [`boot`](Self::boot), but every directed edge runs through a
    /// severable [`Proxy`], enabling [`partition`](Self::partition).
    async fn boot_partitionable(n: u64) -> Self {
        Self::build(n, true).await
    }

    async fn build(n: u64, with_proxies: bool) -> Self {
        // Bind every node's listener up front and keep the sockets; each
        // node boots directly onto its own bound listener, so no port is
        // freed and rebound (no port-collision race under parallel CI).
        let mut listeners = bound_localhost_listeners(n);
        let addrs: BTreeMap<u64, String> = listeners
            .iter()
            .map(|(id, l)| (*id, l.local_addr().expect("bound port address").to_string()))
            .collect();

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
            // Node `id`'s view: its own real bound address, every peer
            // behind this node's outgoing proxy when partitionable.
            let view = (1..=n).map(|peer| {
                let addr = if peer == id || !with_proxies {
                    addrs[&peer].clone()
                } else {
                    proxies[&(id, peer)].addr.clone()
                };
                (peer, addr)
            });
            let config = ClusterConfig::new(view).expect("valid cluster config");
            let listener = listeners.remove(&id).expect("a bound listener per node");
            let node = MvccRaftNode::join_tcp_cluster_with_listener(
                id,
                &config,
                listener,
                RaftTuning::default(),
            )
            .await
            .expect("boot mvcc tcp node");
            nodes.insert(id, node);
        }

        Self { nodes, proxies }
    }

    fn node(&self, id: u64) -> &MvccRaftNode {
        &self.nodes[&id]
    }

    fn ids(&self) -> Vec<u64> {
        self.nodes.keys().copied().collect()
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

    /// Wait (bounded) until one of `ids` reports itself leader **and**
    /// every node in `ids` acknowledges it; returns the leader. Requiring
    /// agreement matters over TCP — see tcp_cluster.rs for the rationale.
    async fn wait_for_leader_among(&self, ids: &[u64]) -> u64 {
        self.wait_until(&format!("an acknowledged leader among {ids:?}"), || {
            let leader = ids.iter().copied().find(|id| self.node(*id).role() == Role::Leader)?;
            ids.iter().all(|id| self.node(*id).current_leader() == Some(leader)).then_some(leader)
        })
        .await
    }

    /// Wait (bounded) until node `id` has applied the dense index `idx`.
    async fn wait_for_apply(&self, id: u64, idx: LogIndex) {
        self.wait_until(&format!("node {id} to apply dense index {idx}"), || {
            (self.node(id).last_applied() >= idx).then_some(())
        })
        .await;
    }

    /// Execute one replicated write through whoever currently leads among
    /// `ids`, re-resolving leadership if an election moves it mid-call.
    /// Returns the node that committed the write plus the entry's dense
    /// index and outcome.
    async fn execute_on_leader_among(
        &self,
        ids: &[u64],
        sql: &str,
    ) -> (u64, LogIndex, ApplyOutcome) {
        let deadline = tokio::time::Instant::now() + WAIT_TIMEOUT;
        loop {
            let leader = self.wait_for_leader_among(ids).await;
            match self.node(leader).execute_replicated(sql).await {
                Ok((idx, outcome)) => return (leader, idx, outcome),
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

    /// Poll `probe` until it yields a value, panicking after
    /// [`WAIT_TIMEOUT`].
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

    async fn shutdown(self) {
        for node in self.nodes.values() {
            node.shutdown().await.expect("clean shutdown");
        }
    }
}

/// Run `query_linearizable` on node `id` with a loud bound: fencing and
/// redirects must resolve within [`WAIT_TIMEOUT`], never hang.
async fn linearizable(
    cluster: &MvccTcpCluster,
    id: u64,
    sql: &str,
) -> Result<QueryResult, ConsensusError> {
    tokio::time::timeout(WAIT_TIMEOUT, cluster.node(id).query_linearizable(sql))
        .await
        .expect("query_linearizable must resolve within the bounded wait")
}

// ---------------------------------------------------------------------------
// B2 PR 1 scenarios
// ---------------------------------------------------------------------------

/// Healthy-cluster contract: the leader's linearizable read sees its own
/// just-acknowledged write; a follower redirects linearizable reads with
/// a leader hint while its plain local read keeps serving (stale-allowed
/// by contract).
#[tokio::test]
async fn leader_serves_linearizable_reads_and_followers_redirect() {
    let cluster = MvccTcpCluster::boot(3).await;
    let all = cluster.ids();

    let (_, idx, outcome) = cluster
        .execute_on_leader_among(&all, "CREATE TABLE accounts (id INTEGER PRIMARY KEY, v TEXT)")
        .await;
    assert_eq!((idx, outcome), (1, ApplyOutcome::Applied { rows_affected: 0 }));

    // Read-your-writes: insert through the leader and immediately read
    // linearizably on the same node — the row must be visible.
    let (leader, idx, outcome) =
        cluster.execute_on_leader_among(&all, "INSERT INTO accounts VALUES (1, 'one')").await;
    assert_eq!((idx, outcome), (2, ApplyOutcome::Applied { rows_affected: 1 }));
    let rows = linearizable(&cluster, leader, "SELECT id, v FROM accounts")
        .await
        .expect("the node that just acknowledged the write must serve a linearizable read");
    assert_eq!(rows.rows, vec![vec![SqlValue::Integer(1), SqlValue::Varchar("one".into())]]);

    // A follower that knows who leads redirects with that hint.
    let follower = all.iter().copied().find(|id| *id != leader).expect("a follower exists");
    cluster
        .wait_until("the follower to learn who leads", || {
            (cluster.node(follower).current_leader() == Some(leader)).then_some(())
        })
        .await;
    match linearizable(&cluster, follower, "SELECT id, v FROM accounts").await {
        Err(ConsensusError::NotLeader { leader_hint }) => assert_eq!(leader_hint, Some(leader)),
        other => panic!("a follower must redirect linearizable reads, got: {other:?}"),
    }

    // ...while its plain local read keeps working once it has applied.
    cluster.wait_for_apply(follower, idx).await;
    assert_eq!(cluster.node(follower).query("SELECT id, v FROM accounts").unwrap(), rows);

    cluster.shutdown().await;
}

/// The B2 PR 1 fencing acceptance (curator criterion on #5200): a leader
/// partitioned into the minority must FAIL linearizable reads — it cannot
/// confirm a quorum — while the majority's new leader serves both writes
/// and linearizable reads. The minority leader's plain local read stays
/// available but stale (documented stale-allowed). After the heal, the
/// deposed leader steps down, converges, and redirects linearizable reads
/// at the real leader.
#[tokio::test]
async fn partitioned_stale_leader_is_fenced_from_linearizable_reads() {
    let cluster = MvccTcpCluster::boot_partitionable(3).await;
    let all = cluster.ids();

    // Seed history and pin down the pre-partition leader: the node that
    // just committed a write is the (confirmed) leader.
    cluster.execute_on_leader_among(&all, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)").await;
    let (old_leader, idx, outcome) =
        cluster.execute_on_leader_among(&all, "INSERT INTO t VALUES (1, 'one')").await;
    assert_eq!((idx, outcome), (2, ApplyOutcome::Applied { rows_affected: 1 }));
    for id in &all {
        cluster.wait_for_apply(*id, idx).await;
    }

    // Pre-partition sanity: the leader serves linearizable reads.
    let seeded = vec![vec![SqlValue::Integer(1), SqlValue::Varchar("one".into())]];
    assert_eq!(
        linearizable(&cluster, old_leader, "SELECT id, v FROM t ORDER BY id").await.unwrap().rows,
        seeded
    );

    // 2-1 split with the leader alone on the minority side.
    cluster.partition(&[old_leader]);

    // FENCING: the partitioned leader cannot confirm a quorum, so the
    // linearizable read fails — with no hint, because its own leadership
    // is exactly what could not be confirmed (hinting at itself would
    // route callers back to a stale node).
    match linearizable(&cluster, old_leader, "SELECT id, v FROM t").await {
        Err(ConsensusError::NotLeader { leader_hint }) => assert_eq!(
            leader_hint, None,
            "a quorum-less leader must not hint at anyone (least of all itself)"
        ),
        other => panic!("the partitioned leader must be fenced, got: {other:?}"),
    }

    // ...while its plain local read stays available (stale-allowed).
    assert_eq!(
        cluster.node(old_leader).query("SELECT id, v FROM t ORDER BY id").unwrap().rows,
        seeded,
        "the minority leader's local read must keep serving the stale prefix"
    );

    // Majority side: a new leader emerges and serves writes AND
    // linearizable reads.
    let majority: Vec<u64> = all.iter().copied().filter(|id| *id != old_leader).collect();
    let (new_leader, idx2, outcome) =
        cluster.execute_on_leader_among(&majority, "INSERT INTO t VALUES (2, 'two')").await;
    assert_ne!(new_leader, old_leader);
    assert_eq!(
        (idx2, outcome),
        (3, ApplyOutcome::Applied { rows_affected: 1 }),
        "the fenced reads must not have consumed a log index"
    );
    let both = vec![
        vec![SqlValue::Integer(1), SqlValue::Varchar("one".into())],
        vec![SqlValue::Integer(2), SqlValue::Varchar("two".into())],
    ];
    assert_eq!(
        linearizable(&cluster, new_leader, "SELECT id, v FROM t ORDER BY id").await.unwrap().rows,
        both
    );

    // The minority leader is provably stale now (it cannot have applied
    // the majority write) — its local read still shows only the prefix,
    // and the linearizable path still refuses to serve it.
    assert_eq!(
        cluster.node(old_leader).query("SELECT id, v FROM t ORDER BY id").unwrap().rows,
        seeded
    );
    assert!(
        matches!(
            linearizable(&cluster, old_leader, "SELECT id, v FROM t").await,
            Err(ConsensusError::NotLeader { leader_hint: None })
        ),
        "the stale leader must stay fenced for as long as the partition lasts"
    );

    cluster.heal();

    // The deposed leader steps down once the new leader's heartbeats
    // reach it, learns who leads, and converges on the majority history.
    cluster
        .wait_until("the old leader to step down and learn who leads", || {
            (cluster.node(old_leader).role() == Role::Follower
                && cluster.node(old_leader).current_leader() == Some(new_leader))
            .then_some(())
        })
        .await;
    cluster.wait_for_apply(old_leader, idx2).await;

    // Now it redirects linearizable reads at the real leader...
    match linearizable(&cluster, old_leader, "SELECT id, v FROM t").await {
        Err(ConsensusError::NotLeader { leader_hint }) => assert_eq!(leader_hint, Some(new_leader)),
        other => panic!("the deposed leader must redirect, got: {other:?}"),
    }

    // ...the leader serves them, and everyone's local state converged.
    assert_eq!(
        linearizable(&cluster, new_leader, "SELECT id, v FROM t ORDER BY id").await.unwrap().rows,
        both
    );
    for id in &all {
        assert_eq!(
            cluster.node(*id).query("SELECT id, v FROM t ORDER BY id").unwrap().rows,
            both,
            "node {id} diverged after the heal"
        );
    }

    cluster.shutdown().await;
}
