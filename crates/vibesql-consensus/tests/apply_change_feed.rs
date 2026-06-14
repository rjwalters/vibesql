//! Apply-path change feed over a real TCP cluster (#5422).
//!
//! These tests prove the foundation of consensus-coherent HTTP SSE
//! subscriptions: every node — leader **and** follower — emits a
//! [`ChangeEvent`](vibesql_storage::ChangeEvent) as it applies a committed
//! entry, so a subscriber tapping a node's apply-path feed
//! ([`MvccRaftNode::subscribe_changes`]) observes committed changes regardless
//! of which node it is connected to. This is the channel the server's
//! replicated subscription loop drains; testing it here at the consensus level
//! exercises the cross-node "write on leader A, observed on follower B" path
//! without an HTTP streaming client (the SSE wiring is covered in
//! `vibesql-server`).

use std::collections::BTreeMap;
use std::net::TcpListener as StdTcpListener;
use std::time::Duration;

use vibesql_consensus::{ClusterConfig, ConsensusError, LogIndex, MvccRaftNode, RaftTuning, Role};
use vibesql_storage::{change_events::RecvError, ChangeEventReceiver};
use vibesql_types::SqlValue;

const WAIT_TIMEOUT: Duration = Duration::from_secs(10);
const POLL_INTERVAL: Duration = Duration::from_millis(20);

/// Bind `n` distinct localhost listeners on OS-assigned ephemeral ports,
/// returned **still bound** so each node boots directly onto its own socket
/// — no port is freed and rebound (no "Address already in use" race under
/// parallel CI, #5507).
fn bound_localhost_listeners(n: u64) -> BTreeMap<u64, StdTcpListener> {
    (1..=n)
        .map(|id| (id, StdTcpListener::bind("127.0.0.1:0").expect("bind ephemeral port")))
        .collect()
}

/// A minimal `n`-voter MVCC cluster over TCP (no partition proxies).
struct Cluster {
    nodes: BTreeMap<u64, MvccRaftNode>,
}

impl Cluster {
    async fn boot(n: u64) -> Self {
        let mut listeners = bound_localhost_listeners(n);
        let addrs: BTreeMap<u64, String> = listeners
            .iter()
            .map(|(id, l)| (*id, l.local_addr().expect("bound port address").to_string()))
            .collect();
        let mut nodes = BTreeMap::new();
        for id in 1..=n {
            let view = (1..=n).map(|peer| (peer, addrs[&peer].clone()));
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
        Self { nodes }
    }

    fn node(&self, id: u64) -> &MvccRaftNode {
        &self.nodes[&id]
    }

    fn ids(&self) -> Vec<u64> {
        self.nodes.keys().copied().collect()
    }

    async fn wait_for_leader(&self) -> u64 {
        self.wait_until("an acknowledged leader", || {
            let ids = self.ids();
            let leader = ids.iter().copied().find(|id| self.node(*id).role() == Role::Leader)?;
            ids.iter().all(|id| self.node(*id).current_leader() == Some(leader)).then_some(leader)
        })
        .await
    }

    async fn wait_for_apply(&self, id: u64, idx: LogIndex) {
        self.wait_until(&format!("node {id} to apply dense index {idx}"), || {
            (self.node(id).last_applied() >= idx).then_some(())
        })
        .await;
    }

    /// Execute one replicated write through whoever currently leads.
    async fn execute(&self, sql: &str) -> (u64, LogIndex) {
        let deadline = tokio::time::Instant::now() + WAIT_TIMEOUT;
        loop {
            let leader = self.wait_for_leader().await;
            match self.node(leader).execute_replicated(sql).await {
                Ok((idx, _outcome)) => return (leader, idx),
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

/// Drain a change feed (bounded) until it yields an event mentioning `table`,
/// returning the matching event. Panics on timeout or a closed feed.
async fn wait_for_table_change(rx: &mut ChangeEventReceiver, table: &str) {
    let deadline = tokio::time::Instant::now() + WAIT_TIMEOUT;
    loop {
        match rx.try_recv() {
            Ok(event) => {
                if event.table_name().eq_ignore_ascii_case(table) {
                    return;
                }
            }
            Err(RecvError::Empty) => {}
            Err(RecvError::Lagged(_)) => {
                // Lagging is acceptable; keep draining toward the latest.
            }
            Err(RecvError::Closed) => panic!("apply-path change feed closed unexpectedly"),
        }
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for a change event on table {table:?}"
        );
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// The core cross-node subscription contract: a subscriber tapping a
/// **follower's** apply-path feed observes a change committed by a write on the
/// **leader**, and the follower's applied state reflects that write. This is
/// the consensus-level equivalent of "an SSE subscriber on node B sees a change
/// committed via a write on leader A".
#[tokio::test]
async fn follower_apply_feed_observes_leader_write() {
    let cluster = Cluster::boot(3).await;

    let (_, idx) =
        cluster.execute("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)").await;
    for id in cluster.ids() {
        cluster.wait_for_apply(id, idx).await;
    }

    let leader = cluster.wait_for_leader().await;
    let follower = cluster.ids().into_iter().find(|id| *id != leader).expect("a follower exists");

    // Subscribe to the follower's apply-path feed BEFORE the write (the feed
    // carries no backlog, mirroring the standalone subscription contract).
    let mut feed = cluster
        .node(follower)
        .subscribe_changes()
        .expect("apply-path change feed must be available on a follower");

    // Write on the leader; the follower applies it via replication.
    let (write_leader, write_idx) =
        cluster.execute("INSERT INTO users VALUES (1, 'alice')").await;
    assert_eq!(write_leader, leader, "leadership should not have moved");
    cluster.wait_for_apply(follower, write_idx).await;

    // The follower's feed fired for the table as it applied the entry.
    wait_for_table_change(&mut feed, "users").await;

    // And re-querying the follower's applied state (what the subscription loop
    // does) returns the committed row.
    let rows = cluster.node(follower).query("SELECT id, name FROM users").expect("local read");
    assert_eq!(
        rows.rows,
        vec![vec![SqlValue::Integer(1), SqlValue::Varchar("alice".into())]],
        "the follower must observe the committed write"
    );

    cluster.shutdown().await;
}

/// The leader's own apply-path feed observes the leader's committed writes:
/// applying an entry on the node that proposed it still emits change events, so
/// a subscriber connected to the leader sees its own committed writes.
#[tokio::test]
async fn leader_apply_feed_observes_own_write() {
    let cluster = Cluster::boot(3).await;

    let (_, idx) =
        cluster.execute("CREATE TABLE t (id INTEGER PRIMARY KEY, v TEXT)").await;
    for id in cluster.ids() {
        cluster.wait_for_apply(id, idx).await;
    }

    let leader = cluster.wait_for_leader().await;
    let mut feed = cluster
        .node(leader)
        .subscribe_changes()
        .expect("apply-path change feed must be available on the leader");

    let (_, write_idx) = cluster.execute("INSERT INTO t VALUES (7, 'seven')").await;
    cluster.wait_for_apply(leader, write_idx).await;

    wait_for_table_change(&mut feed, "t").await;

    let rows = cluster.node(leader).query("SELECT id, v FROM t").expect("local read");
    assert_eq!(rows.rows, vec![vec![SqlValue::Integer(7), SqlValue::Varchar("seven".into())]]);

    cluster.shutdown().await;
}
