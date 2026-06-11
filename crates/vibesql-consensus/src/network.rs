//! In-process channel transport for multi-node openraft clusters
//! (Raft Phase A3, PR 1 of issue #5197).
//!
//! This module implements openraft 0.9's pluggable network layer —
//! [`RaftNetwork`] / [`RaftNetworkFactory`] — over tokio channels, so several
//! `Raft` instances can form a real cluster inside one process. That makes
//! leader-election, failover, and catch-up tests fast and deterministic
//! enough for CI, independent of sockets (the curator re-scope on #5197).
//! The TCP transport that implements the same traits over real connections
//! is PR 2.
//!
//! ## Topology
//!
//! A [`ChannelRouter`] is the shared "switchboard": it maps node ids to the
//! sending half of a per-node mpsc queue. Each node, once its `Raft` handle
//! exists, [registers](ChannelRouter::register) with the router, which spawns
//! a server loop draining that node's queue and feeding each RPC into the
//! local `Raft` (`Raft::append_entries` / `Raft::vote` /
//! `Raft::install_snapshot` — exactly what a socket listener will do in
//! PR 2). On the client side, [`ChannelNetwork`] resolves the target's queue
//! through the router *per RPC*, so connectivity changes take effect
//! immediately.
//!
//! ## Failure injection
//!
//! "Killing" a node is [`ChannelRouter::deregister`]: subsequent RPCs to it
//! fail with [`Unreachable`], which openraft treats like a dead socket
//! (replication backs off and retries). Restoring a node is registering a
//! freshly booted `Raft` under the same id. The cluster harness in
//! `crate::cluster` builds `kill`/`restore` on these primitives.
//!
//! ## Ordering
//!
//! openraft expects RPCs between a (source, target) pair to arrive in send
//! order. Each replication stream sends one RPC at a time and the server
//! loop handles requests sequentially, so the per-pair order is preserved —
//! same guarantee a TCP connection gives.

use tokio::sync::{mpsc, oneshot};

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use openraft::error::{InstallSnapshotError, RPCError, RaftError, RemoteError, Unreachable};
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{BasicNode, Raft, RaftNetwork, RaftNetworkFactory};

use crate::openraft_backend::TypeConfig;

/// One Raft RPC traveling from a peer to a node's server loop, paired with
/// the oneshot on which the (local-`Raft`-produced) response travels back.
enum RaftRequest {
    AppendEntries(
        AppendEntriesRequest<TypeConfig>,
        oneshot::Sender<Result<AppendEntriesResponse<u64>, RaftError<u64>>>,
    ),
    Vote(VoteRequest<u64>, oneshot::Sender<Result<VoteResponse<u64>, RaftError<u64>>>),
    InstallSnapshot(
        InstallSnapshotRequest<TypeConfig>,
        oneshot::Sender<Result<InstallSnapshotResponse<u64>, RaftError<u64, InstallSnapshotError>>>,
    ),
}

/// Shared switchboard mapping node ids to their inbound RPC queues.
///
/// Cloning shares the switchboard (it is a handle); every node of one test
/// cluster holds a clone of the same router.
#[derive(Clone, Default)]
pub(crate) struct ChannelRouter {
    inner: Arc<Mutex<HashMap<u64, mpsc::UnboundedSender<RaftRequest>>>>,
}

impl ChannelRouter {
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<u64, mpsc::UnboundedSender<RaftRequest>>> {
        self.inner.lock().expect("channel router mutex poisoned")
    }

    /// Make `raft` reachable as `node_id`: spawns the server loop that feeds
    /// this node's inbound RPCs into it.
    ///
    /// Must be called before peers need to reach the node (i.e. before the
    /// cluster is initialized). Registering an id twice replaces the previous
    /// registration, which also ends the superseded server loop.
    pub(crate) fn register(&self, node_id: u64, raft: Raft<TypeConfig>) {
        let (tx, rx) = mpsc::unbounded_channel();
        self.lock().insert(node_id, tx);
        // The loop ends (recv -> None) once this node is deregistered and
        // any in-flight sender clones are dropped, so no JoinHandle to keep.
        tokio::spawn(serve(raft, rx));
    }

    /// Cut a node off: peers' RPCs to `node_id` now fail with
    /// [`Unreachable`], and its server loop winds down.
    pub(crate) fn deregister(&self, node_id: u64) {
        self.lock().remove(&node_id);
    }

    fn sender(&self, node_id: u64) -> Option<mpsc::UnboundedSender<RaftRequest>> {
        self.lock().get(&node_id).cloned()
    }
}

/// Server loop: drain a node's inbound queue, dispatching each RPC to the
/// local `Raft` exactly as a socket listener would.
async fn serve(raft: Raft<TypeConfig>, mut rx: mpsc::UnboundedReceiver<RaftRequest>) {
    while let Some(request) = rx.recv().await {
        // A dropped response receiver just means the caller gave up
        // (e.g. its node was killed mid-RPC); nothing to do.
        match request {
            RaftRequest::AppendEntries(rpc, tx) => {
                let _ = tx.send(raft.append_entries(rpc).await);
            }
            RaftRequest::Vote(rpc, tx) => {
                let _ = tx.send(raft.vote(rpc).await);
            }
            RaftRequest::InstallSnapshot(rpc, tx) => {
                let _ = tx.send(raft.install_snapshot(rpc).await);
            }
        }
    }
}

/// Factory handed to `Raft::new`; produces one [`ChannelNetwork`] per
/// (source, target) replication stream.
pub(crate) struct ChannelNetworkFactory {
    router: ChannelRouter,
}

impl ChannelNetworkFactory {
    pub(crate) fn new(router: ChannelRouter) -> Self {
        Self { router }
    }
}

impl RaftNetworkFactory<TypeConfig> for ChannelNetworkFactory {
    type Network = ChannelNetwork;

    async fn new_client(&mut self, target: u64, _node: &BasicNode) -> Self::Network {
        ChannelNetwork { target, router: self.router.clone() }
    }
}

/// Client side of the channel transport: sends RPCs to one target node.
pub(crate) struct ChannelNetwork {
    target: u64,
    router: ChannelRouter,
}

/// The error a client sees when the target is deregistered (killed) or its
/// server loop dropped the request/response mid-flight.
fn unreachable<E: std::error::Error>(target: u64) -> RPCError<u64, BasicNode, E> {
    RPCError::Unreachable(Unreachable::new(&std::io::Error::other(format!(
        "node {target} is not reachable (killed or not yet registered)"
    ))))
}

impl ChannelNetwork {
    /// Send one request and await its response. `make` wraps the rpc and the
    /// response oneshot into the right [`RaftRequest`] variant.
    async fn call<R, E>(
        &self,
        make: impl FnOnce(oneshot::Sender<Result<R, E>>) -> RaftRequest,
    ) -> Result<R, RPCError<u64, BasicNode, E>>
    where
        E: std::error::Error,
    {
        let Some(tx) = self.router.sender(self.target) else {
            return Err(unreachable(self.target));
        };
        let (response_tx, response_rx) = oneshot::channel();
        if tx.send(make(response_tx)).is_err() {
            // Deregistered between lookup and send.
            return Err(unreachable(self.target));
        }
        match response_rx.await {
            Ok(Ok(response)) => Ok(response),
            // The remote Raft rejected/failed the RPC; report it as the
            // remote's error, as a socket transport would.
            Ok(Err(raft_error)) => {
                Err(RPCError::RemoteError(RemoteError::new(self.target, raft_error)))
            }
            // Server loop dropped the oneshot (node killed mid-RPC).
            Err(_) => Err(unreachable(self.target)),
        }
    }
}

impl RaftNetwork<TypeConfig> for ChannelNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        self.call(|tx| RaftRequest::AppendEntries(rpc, tx)).await
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<u64>,
        RPCError<u64, BasicNode, RaftError<u64, InstallSnapshotError>>,
    > {
        self.call(|tx| RaftRequest::InstallSnapshot(rpc, tx)).await
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        _option: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        self.call(|tx| RaftRequest::Vote(rpc, tx)).await
    }
}
