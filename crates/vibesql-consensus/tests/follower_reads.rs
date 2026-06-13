//! Follower reads over real sockets (Raft Phase B2, #5200, PR 2 of 2):
//! read-your-writes session tokens + bounded-staleness reads.
//!
//! Boots 3-voter [`MvccRaftNode`] clusters whose Raft RPCs travel over the
//! TCP transport, with every directed edge optionally behind a severable
//! TCP proxy (the partition machinery from #5197/#5200 PR 1), and proves
//! the B2 PR 2 read-path contract end-to-end:
//!
//! - **read-your-writes**: write on the leader, carry the returned dense
//!   index as the session token — `query_at_least` on ANY node observes
//!   the write, and a partitioned (lagging) follower blocks until the
//!   heal lets it catch up rather than serving early;
//! - **bounded staleness**: a fresh follower serves within the bound; a
//!   `max_staleness = 0` read on a follower redirects to the leader; a
//!   PARTITIONED follower refuses once its last leader-clock stamp ages
//!   past the bound — provably stale data is never served within the
//!   claimed bound — and resumes service after the heal.
//!
//! The `Proxy` here intentionally duplicates the one in
//! `tests/tcp_cluster.rs` / `tests/leader_reads.rs`: integration-test
//! binaries are separate crates, and keeping each file self-contained
//! keeps them conflict-free under parallel work.
//!
//! All synchronization is bounded polling (10s deadline) — no bare
//! sleeps on the critical path; every wait fails loudly instead of
//! hanging.

use std::collections::BTreeMap;
use std::net::TcpListener as StdTcpListener;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

use vibesql_consensus::{
    ApplyOutcome, ClusterConfig, ConsensusError, LogIndex, MvccRaftNode, RaftTuning, Role,
};
use vibesql_types::SqlValue;

/// Upper bound for any single cluster-level wait (election, catch-up,
/// staleness aging).
const WAIT_TIMEOUT: Duration = Duration::from_secs(10);

/// Poll interval inside bounded waits.
const POLL_INTERVAL: Duration = Duration::from_millis(20);

/// Reserve `n` distinct localhost addresses with OS-assigned ephemeral
/// ports, keyed by node id `1..=n` (same strategy as tcp_cluster.rs).
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
// Per-edge TCP proxy (partition injection) — see tests/tcp_cluster.rs
// ---------------------------------------------------------------------------

/// A severable TCP forwarder for one directed cluster edge.
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
/// directed edge behind a severable [`Proxy`], and with explicit
/// [`RaftTuning`] (the staleness-beacon tests need it).
struct MvccTcpCluster {
    nodes: BTreeMap<u64, MvccRaftNode>,
    /// Directed-edge proxies; empty unless built partitionable.
    proxies: BTreeMap<(u64, u64), Proxy>,
}

impl MvccTcpCluster {
    /// Boot `n` voters (ids `1..=n`) dialing each other directly.
    async fn boot(n: u64) -> Self {
        Self::build(n, false, RaftTuning::default()).await
    }

    /// Like [`boot`](Self::boot), but every directed edge runs through a
    /// severable [`Proxy`], enabling [`partition`](Self::partition).
    async fn boot_partitionable(n: u64) -> Self {
        Self::build(n, true, RaftTuning::default()).await
    }

    /// Partitionable cluster with explicit tuning (staleness beacon on).
    async fn boot_partitionable_tuned(n: u64, tuning: RaftTuning) -> Self {
        Self::build(n, true, tuning).await
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
            let node = MvccRaftNode::join_tcp_cluster_tuned(id, &config, tuning)
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
    /// every node in `ids` acknowledges it; returns the leader.
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

// ---------------------------------------------------------------------------
// B2 PR 2 scenarios
// ---------------------------------------------------------------------------

/// Read-your-writes via the session token, end-to-end over TCP: write on
/// the leader, carry the returned dense index, and `query_at_least` on
/// EVERY node — followers included — observes the write, with no manual
/// apply-waiting by the caller. A `0` token serves immediately; a token
/// from the future fails with a typed timeout rather than serving early.
#[tokio::test]
async fn read_your_writes_token_serves_on_any_node() {
    let cluster = MvccTcpCluster::boot(3).await;
    let all = cluster.ids();

    cluster
        .execute_on_leader_among(&all, "CREATE TABLE accounts (id INTEGER PRIMARY KEY, v TEXT)")
        .await;
    let (leader, token, outcome) =
        cluster.execute_on_leader_among(&all, "INSERT INTO accounts VALUES (1, 'one')").await;
    assert_eq!(outcome, ApplyOutcome::Applied { rows_affected: 1 });

    let expected = vec![vec![SqlValue::Integer(1), SqlValue::Varchar("one".into())]];
    for id in &all {
        let rows = cluster
            .node(*id)
            .query_at_least(token, "SELECT id, v FROM accounts", WAIT_TIMEOUT)
            .await
            .unwrap_or_else(|e| panic!("node {id} must serve the token read: {e:?}"));
        assert_eq!(rows.rows, expected, "node {id}");
    }

    // Token 0 degenerates to a plain local read.
    let follower = all.iter().copied().find(|id| *id != leader).expect("a follower exists");
    let rows = cluster
        .node(follower)
        .query_at_least(0, "SELECT id, v FROM accounts", WAIT_TIMEOUT)
        .await
        .unwrap();
    assert_eq!(rows.rows, expected);

    // A token beyond anything committed must NOT serve — typed timeout.
    let err = cluster
        .node(follower)
        .query_at_least(token + 100, "SELECT id FROM accounts", Duration::from_millis(200))
        .await
        .unwrap_err();
    match err {
        ConsensusError::ReadTimeout { required, applied, .. } => {
            assert_eq!(required, token + 100);
            assert!(applied < required, "applied {applied} must trail the token");
        }
        other => panic!("expected ReadTimeout, got: {other:?}"),
    }

    cluster.shutdown().await;
}

/// A lagging (partitioned) follower holds a token read rather than
/// serving early: the bounded wait fails with `ReadTimeout` while the
/// partition lasts, and the SAME token read succeeds after the heal —
/// observing the write it was a receipt for.
#[tokio::test]
async fn token_read_on_lagging_follower_blocks_until_catch_up() {
    let cluster = MvccTcpCluster::boot_partitionable(3).await;
    let all = cluster.ids();

    let (_, idx, _) = cluster
        .execute_on_leader_among(&all, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)")
        .await;
    for id in &all {
        cluster.wait_for_apply(*id, idx).await;
    }
    let leader = cluster.wait_for_leader_among(&all).await;
    let lagging = all.iter().copied().find(|id| *id != leader).expect("a follower exists");

    // Cut the follower off, then commit a write it cannot see.
    cluster.partition(&[lagging]);
    let majority: Vec<u64> = all.iter().copied().filter(|id| *id != lagging).collect();
    let (_, token, outcome) =
        cluster.execute_on_leader_among(&majority, "INSERT INTO t VALUES (1, 'one')").await;
    assert_eq!(outcome, ApplyOutcome::Applied { rows_affected: 1 });

    // The partitioned follower must NOT serve the token read: it waits,
    // then fails loudly with its applied cursor still short of the token.
    let err = cluster
        .node(lagging)
        .query_at_least(token, "SELECT id, v FROM t", Duration::from_millis(500))
        .await
        .unwrap_err();
    match err {
        ConsensusError::ReadTimeout { required, applied, .. } => {
            assert_eq!(required, token);
            assert!(applied < token, "the partitioned follower cannot have applied the write");
        }
        other => panic!("expected ReadTimeout on the partitioned follower, got: {other:?}"),
    }

    // Heal: the same token read now blocks only until catch-up, then
    // serves the write it was a receipt for.
    cluster.heal();
    let rows = cluster
        .node(lagging)
        .query_at_least(token, "SELECT id, v FROM t", WAIT_TIMEOUT)
        .await
        .expect("the healed follower must catch up and serve the token read");
    assert_eq!(rows.rows, vec![vec![SqlValue::Integer(1), SqlValue::Varchar("one".into())]]);

    cluster.shutdown().await;
}

/// Bounded staleness on a healthy cluster: a fresh follower serves
/// within a generous bound; `max_staleness = 0` on a follower redirects
/// to the leader (the curator's "staleness=0 → redirect" criterion)
/// while the leader serves it linearizably.
#[tokio::test]
async fn fresh_follower_serves_bounded_reads_and_zero_redirects() {
    let cluster = MvccTcpCluster::boot(3).await;
    let all = cluster.ids();

    cluster.execute_on_leader_among(&all, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)").await;
    let (leader, idx, _) =
        cluster.execute_on_leader_among(&all, "INSERT INTO t VALUES (1, 'one')").await;
    let follower = all.iter().copied().find(|id| *id != leader).expect("a follower exists");
    cluster.wait_for_apply(follower, idx).await;
    cluster
        .wait_until("the follower to learn who leads", || {
            (cluster.node(follower).current_leader() == Some(leader)).then_some(())
        })
        .await;

    let expected = vec![vec![SqlValue::Integer(1), SqlValue::Varchar("one".into())]];

    // Fresh follower, generous bound: serves the local snapshot.
    let rows = cluster
        .node(follower)
        .query_bounded_staleness(Duration::from_secs(10), "SELECT id, v FROM t")
        .await
        .expect("a freshly-applied follower must serve within a 10s bound");
    assert_eq!(rows.rows, expected);

    // staleness = 0 on a follower: redirect to the leader.
    match cluster.node(follower).query_bounded_staleness(Duration::ZERO, "SELECT id FROM t").await {
        Err(ConsensusError::NotLeader { leader_hint }) => assert_eq!(leader_hint, Some(leader)),
        other => panic!("staleness=0 on a follower must redirect, got: {other:?}"),
    }

    // staleness = 0 on the leader: served, linearizably.
    let rows = cluster
        .node(leader)
        .query_bounded_staleness(Duration::ZERO, "SELECT id, v FROM t")
        .await
        .expect("the leader serves staleness=0 through the linearizable path");
    assert_eq!(rows.rows, expected);

    cluster.shutdown().await;
}

/// The bounded-staleness acceptance on a PARTITIONED follower: its last
/// leader-clock stamp ages, so once `max_staleness` elapses it refuses —
/// and while it still serves (inside the bound), it serves only the
/// pre-partition prefix, never claiming freshness it does not have. The
/// staleness beacon keeps the healthy side fresh through the idle
/// stretch and restores the follower's service after the heal.
#[tokio::test]
async fn partitioned_follower_refuses_bounded_reads_once_the_bound_elapses() {
    let tuning = RaftTuning { staleness_beacon_ms: 100, ..RaftTuning::default() };
    let cluster = MvccTcpCluster::boot_partitionable_tuned(3, tuning).await;
    let all = cluster.ids();

    cluster.execute_on_leader_among(&all, "CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)").await;
    let (leader, idx, _) =
        cluster.execute_on_leader_among(&all, "INSERT INTO t VALUES (1, 'one')").await;
    for id in &all {
        cluster.wait_for_apply(*id, idx).await;
    }
    let follower = all.iter().copied().find(|id| *id != leader).expect("a follower exists");
    let seeded = vec![vec![SqlValue::Integer(1), SqlValue::Varchar("one".into())]];

    // Cut the follower off, then commit a write it can never see.
    cluster.partition(&[follower]);
    let majority: Vec<u64> = all.iter().copied().filter(|id| *id != follower).collect();
    let (_, majority_idx, _) =
        cluster.execute_on_leader_among(&majority, "INSERT INTO t VALUES (2, 'two')").await;

    // Poll the partitioned follower with a 750ms bound: any read it still
    // serves must be the pre-partition prefix (it physically cannot have
    // the majority write), and once its stamp ages past the bound it must
    // refuse — provably stale data is never served within the bound.
    let bound = Duration::from_millis(750);
    let deadline = tokio::time::Instant::now() + WAIT_TIMEOUT;
    loop {
        match cluster
            .node(follower)
            .query_bounded_staleness(bound, "SELECT id, v FROM t ORDER BY id")
            .await
        {
            Ok(rows) => {
                assert_eq!(
                    rows.rows, seeded,
                    "a bounded read served inside the bound must be the pre-partition prefix"
                );
            }
            Err(ConsensusError::StalenessExceeded { observed_ms, max_staleness_ms, .. }) => {
                let observed = observed_ms.expect("the follower applied stamped entries");
                assert!(observed > max_staleness_ms, "{observed} vs {max_staleness_ms}");
                break;
            }
            Err(other) => panic!("expected rows or StalenessExceeded, got: {other:?}"),
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for the partitioned follower to age out of the bound"
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }

    // It stays refused for as long as the partition lasts.
    assert!(
        matches!(
            cluster.node(follower).query_bounded_staleness(bound, "SELECT id FROM t").await,
            Err(ConsensusError::StalenessExceeded { .. })
        ),
        "the partitioned follower must keep refusing"
    );

    // Heal: the follower catches up (read-your-writes token from the
    // majority write proves it) and — thanks to the beacon — proves
    // freshness again, now serving BOTH rows within the bound.
    cluster.heal();
    let both = vec![
        vec![SqlValue::Integer(1), SqlValue::Varchar("one".into())],
        vec![SqlValue::Integer(2), SqlValue::Varchar("two".into())],
    ];
    let rows = cluster
        .node(follower)
        .query_at_least(majority_idx, "SELECT id, v FROM t ORDER BY id", WAIT_TIMEOUT)
        .await
        .expect("the healed follower must catch up to the majority write");
    assert_eq!(rows.rows, both);
    let deadline = tokio::time::Instant::now() + WAIT_TIMEOUT;
    loop {
        match cluster
            .node(follower)
            .query_bounded_staleness(bound, "SELECT id, v FROM t ORDER BY id")
            .await
        {
            Ok(rows) => {
                assert_eq!(rows.rows, both);
                break;
            }
            Err(ConsensusError::StalenessExceeded { .. }) => {} // beacon not landed yet
            Err(other) => panic!("expected rows or StalenessExceeded, got: {other:?}"),
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for the healed follower to prove freshness again"
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }

    cluster.shutdown().await;
}
