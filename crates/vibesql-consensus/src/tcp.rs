//! TCP transport for multi-node openraft clusters
//! (Raft Phase A3, PR 2 of issue #5197).
//!
//! This is the socket implementation of openraft 0.9's pluggable network
//! layer — the same [`RaftNetwork`] / [`RaftNetworkFactory`] traits the
//! in-process channel transport (`crate::network`, PR 1) implements, so a
//! cluster of real processes behaves exactly like the deterministic test
//! cluster, RPC for RPC.
//!
//! ## Wire format
//!
//! Length-prefixed frames, mirroring the durable Raft log's framing
//! convention (`crate::durable`) minus the CRC — TCP already checksums the
//! stream:
//!
//! ```text
//! ┌──────────────────────────────┐
//! │ [len: u32 LE][payload bytes] │
//! └──────────────────────────────┘
//! ```
//!
//! The payload is a `serde_json`-encoded [`TcpRequest`] (client → server)
//! or [`TcpResponse`] (server → client). JSON keeps this crate's existing
//! codec convention (entries, snapshots, and the durable log all serialize
//! through serde_json) and adds no dependency; the framing is
//! codec-agnostic, so swapping in a binary codec later only changes the
//! payload encoder. A frame longer than [`MAX_FRAME_LEN`] is rejected
//! before allocation, so a torn or malicious length prefix cannot OOM the
//! process.
//!
//! ## Connection model
//!
//! Client side: one lazily-established outbound connection per
//! [`TcpNetwork`] instance, i.e. per (source, target) replication stream —
//! openraft awaits each RPC on a stream before issuing the next (verified
//! against the vendored 0.9.24 in PR 1), so a strict request/response
//! lockstep per connection is sufficient; no pipelining or correlation ids.
//! Any failure — connect refused, IO error, decode error, or openraft's
//! hard TTL expiring — tears the connection down and surfaces
//! [`Unreachable`]; openraft backs off and retries, and the next RPC dials
//! fresh. That *is* the reconnect-on-failure story: reconnection needs no
//! bookkeeping because every RPC re-establishes on demand. The connection
//! is taken out of `self` for the duration of an exchange and only put back
//! after a complete round-trip, so a cancelled (timed-out) RPC can never
//! leave a half-written frame on a reused stream.
//!
//! Dial addresses come from the node's **local** [`ClusterConfig`], not
//! from the replicated membership — see `crate::cluster_config` for why.
//!
//! ## Snapshot transfer (Raft Phase A4, PR 2 of #5198)
//!
//! Snapshots ride the same `InstallSnapshot` leg, **chunked by openraft**:
//! the leader's replication core calls `RaftNetwork::full_snapshot`, whose
//! default implementation (`openraft::network::snapshot_transport::Chunked`,
//! verified against the vendored 0.9.24) slices the snapshot blob into
//! [`Config::snapshot_max_chunk_size`]-byte pieces and sends each as one
//! `InstallSnapshotRequest` through [`RaftNetwork::install_snapshot`] — one
//! frame per chunk. The follower side reassembles inside
//! [`Raft::install_snapshot`] (offset-checked streaming; a mismatch resets
//! the transfer) and installs the completed snapshot through the state
//! machine, which persists it durably *before* acknowledging.
//!
//! Consequently [`MAX_FRAME_LEN`] bounds the **chunk**, never the snapshot:
//! a snapshot of any size transfers as a sequence of bounded frames. The
//! chunk size is exposed as `RaftTuning::snapshot_chunk_bytes` and validated
//! against [`MAX_SNAPSHOT_CHUNK_BYTES`] at construction with a typed
//! [`ConsensusError::Config`], so a chunk that could overflow a frame is a
//! loud configuration error instead of a silent `Unreachable` retry loop
//! (judge follow-up from PR #5368).
//!
//! [`Config::snapshot_max_chunk_size`]: openraft::Config::snapshot_max_chunk_size
//! [`Raft::install_snapshot`]: openraft::Raft::install_snapshot
//! [`ConsensusError::Config`]: crate::ConsensusError::Config
//!
//! Server side: [`spawn_listener`] accepts connections and serves each on
//! its own task, dispatching inbound RPCs into the local [`Raft`] exactly
//! like the channel transport's serve loop (`Raft::append_entries` /
//! `Raft::vote` / `Raft::install_snapshot`), sequentially per connection. A
//! malformed or torn inbound frame drops that connection (the peer
//! reconnects); it never panics the node. The accept loop owns the
//! per-connection tasks through a [`JoinSet`], so aborting the listener
//! task — which [`OpenraftBackend::shutdown`] and `Drop` do — also severs
//! every accepted connection, making a dropped backend look like a crashed
//! process to its peers.
//!
//! [`ClusterConfig`]: crate::cluster_config::ClusterConfig
//! [`OpenraftBackend::shutdown`]: crate::OpenraftBackend::shutdown

use std::collections::BTreeMap;
use std::io;
use std::sync::Arc;
use std::time::Duration;

use openraft::error::{InstallSnapshotError, RPCError, RaftError, RemoteError, Unreachable};
use openraft::network::RPCOption;
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{BasicNode, Raft, RaftNetwork, RaftNetworkFactory};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpSocket, TcpStream};
use tokio::task::{JoinHandle, JoinSet};

use crate::cluster_config::ClusterConfig;
use crate::openraft_backend::TypeConfig;

/// Upper bound on a single frame's payload. Far above any AppendEntries
/// batch this phase produces and above any snapshot *chunk* (snapshots are
/// chunked by openraft — see the module docs — with the chunk size capped
/// at [`MAX_SNAPSHOT_CHUNK_BYTES`], so this never bounds total snapshot
/// size), but small enough that a garbage length prefix cannot trigger a
/// huge allocation.
const MAX_FRAME_LEN: u32 = 64 * 1024 * 1024;

/// Upper bound on `RaftTuning::snapshot_chunk_bytes`, derived from
/// [`MAX_FRAME_LEN`]: the frame carries the chunk JSON-encoded, and
/// serde_json renders a `Vec<u8>` as an array of decimal numbers — worst
/// case 4 bytes on the wire per snapshot byte (`255,`). 12 MiB of chunk is
/// at most 48 MiB of JSON, leaving a 16 MiB margin for the request envelope
/// (vote, meta, offset) inside the 64 MiB frame limit.
pub(crate) const MAX_SNAPSHOT_CHUNK_BYTES: u64 = 12 * 1024 * 1024;

/// Backoff after a failed `accept` (e.g. EMFILE) before trying again.
const ACCEPT_RETRY_DELAY: Duration = Duration::from_millis(50);

// ---------------------------------------------------------------------------
// Wire messages
// ---------------------------------------------------------------------------

/// A Raft RPC traveling client → server. One frame each.
#[derive(Serialize, Deserialize)]
enum TcpRequest {
    AppendEntries(AppendEntriesRequest<TypeConfig>),
    Vote(VoteRequest<u64>),
    InstallSnapshot(InstallSnapshotRequest<TypeConfig>),
}

/// The matching response traveling server → client: the local `Raft`'s
/// verbatim result, including its error — mirroring how the channel
/// transport ships `Result<_, RaftError>` back over its oneshot.
#[derive(Serialize, Deserialize)]
enum TcpResponse {
    AppendEntries(Result<AppendEntriesResponse<u64>, RaftError<u64>>),
    Vote(Result<VoteResponse<u64>, RaftError<u64>>),
    InstallSnapshot(Result<InstallSnapshotResponse<u64>, RaftError<u64, InstallSnapshotError>>),
}

// ---------------------------------------------------------------------------
// Framing
// ---------------------------------------------------------------------------

/// Write one `[len:u32 LE][payload]` frame.
async fn write_frame<W>(writer: &mut W, payload: &[u8]) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let len =
        u32::try_from(payload.len()).ok().filter(|len| *len <= MAX_FRAME_LEN).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidData,
                format!("frame of {} bytes exceeds the {MAX_FRAME_LEN}-byte limit", payload.len()),
            )
        })?;
    writer.write_all(&len.to_le_bytes()).await?;
    writer.write_all(payload).await?;
    writer.flush().await
}

/// Read one `[len:u32 LE][payload]` frame, rejecting oversized lengths
/// before allocating.
async fn read_frame<R>(reader: &mut R) -> io::Result<Vec<u8>>
where
    R: AsyncRead + Unpin,
{
    let mut len = [0u8; 4];
    reader.read_exact(&mut len).await?;
    let len = u32::from_le_bytes(len);
    if len > MAX_FRAME_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!("frame length {len} exceeds the {MAX_FRAME_LEN}-byte limit"),
        ));
    }
    let mut payload = vec![0u8; len as usize];
    reader.read_exact(&mut payload).await?;
    Ok(payload)
}

// ---------------------------------------------------------------------------
// Server side
// ---------------------------------------------------------------------------

/// Bind the consensus listener for `addr` (`host:port`, resolved if it is a
/// hostname), with `SO_REUSEADDR` so a restarted node can rebind its port
/// while connections from its previous life linger in TIME_WAIT.
pub(crate) async fn bind_listener(addr: &str) -> io::Result<TcpListener> {
    let resolved = tokio::net::lookup_host(addr)
        .await?
        .next()
        .ok_or_else(|| io::Error::other(format!("{addr} resolved to no addresses")))?;
    let socket = if resolved.is_ipv4() { TcpSocket::new_v4() } else { TcpSocket::new_v6() }?;
    socket.set_reuseaddr(true)?;
    socket.bind(resolved)?;
    socket.listen(1024)
}

/// Adopt an already-bound [`std::net::TcpListener`] as the consensus
/// listener, switching it to non-blocking for the tokio reactor.
///
/// This is the no-gap counterpart to [`bind_listener`]: a caller can bind a
/// port (e.g. `127.0.0.1:0` to let the OS pick), read back the real bound
/// address, wire peers to it, and only *then* hand the very same socket here
/// — so nothing ever rebinds a freed port. Tests use it (via the
/// `join_tcp_cluster_with_listener` constructors) to spin up clusters on
/// ephemeral ports without the reserve-then-rebind window that otherwise
/// races parallel runs for "Address already in use".
pub(crate) fn adopt_listener(listener: std::net::TcpListener) -> io::Result<TcpListener> {
    listener.set_nonblocking(true)?;
    TcpListener::from_std(listener)
}

/// Spawn the accept loop feeding inbound RPCs into the local `Raft`.
///
/// Aborting the returned handle ends the loop **and** every accepted
/// connection (the loop owns them via a [`JoinSet`]), so peers see dead
/// sockets — the crash semantics the failover tests rely on.
pub(crate) fn spawn_listener(raft: Raft<TypeConfig>, listener: TcpListener) -> JoinHandle<()> {
    tokio::spawn(async move {
        let mut connections = JoinSet::new();
        loop {
            tokio::select! {
                accepted = listener.accept() => match accepted {
                    Ok((stream, _peer)) => {
                        let _ = stream.set_nodelay(true);
                        connections.spawn(serve_connection(raft.clone(), stream));
                    }
                    // Transient accept failure (e.g. fd exhaustion): back
                    // off briefly instead of spinning.
                    Err(_) => tokio::time::sleep(ACCEPT_RETRY_DELAY).await,
                },
                // Reap finished connection tasks so the set stays small.
                Some(_) = connections.join_next(), if !connections.is_empty() => {}
            }
        }
    })
}

/// Serve one inbound connection: read a request frame, dispatch it into the
/// local `Raft`, write the response frame; repeat. Any framing, decode, or
/// IO problem ends the connection (the peer reconnects) — never a panic.
async fn serve_connection(raft: Raft<TypeConfig>, mut stream: TcpStream) {
    loop {
        let Ok(payload) = read_frame(&mut stream).await else { return };
        let Ok(request) = serde_json::from_slice::<TcpRequest>(&payload) else { return };

        let response = match request {
            TcpRequest::AppendEntries(rpc) => {
                TcpResponse::AppendEntries(raft.append_entries(rpc).await)
            }
            TcpRequest::Vote(rpc) => TcpResponse::Vote(raft.vote(rpc).await),
            TcpRequest::InstallSnapshot(rpc) => {
                TcpResponse::InstallSnapshot(raft.install_snapshot(rpc).await)
            }
        };

        let Ok(bytes) = serde_json::to_vec(&response) else { return };
        if write_frame(&mut stream, &bytes).await.is_err() {
            return;
        }
    }
}

// ---------------------------------------------------------------------------
// Client side
// ---------------------------------------------------------------------------

/// Factory handed to `Raft::new`; produces one [`TcpNetwork`] per
/// (source, target) replication stream, dialing by the **local** cluster
/// config's view of each peer.
pub(crate) struct TcpNetworkFactory {
    dial_addrs: Arc<BTreeMap<u64, String>>,
}

impl TcpNetworkFactory {
    pub(crate) fn new(config: &ClusterConfig) -> Self {
        Self { dial_addrs: Arc::new(config.dial_addrs()) }
    }
}

impl RaftNetworkFactory<TypeConfig> for TcpNetworkFactory {
    type Network = TcpNetwork;

    async fn new_client(&mut self, target: u64, _node: &BasicNode) -> Self::Network {
        TcpNetwork { target, addr: self.dial_addrs.get(&target).cloned(), stream: None }
    }
}

/// Client half of the TCP transport: sends RPCs to one target node over a
/// lazily-(re)established connection.
pub(crate) struct TcpNetwork {
    target: u64,
    /// Dial address from the local cluster config; `None` if the target is
    /// not in it (every RPC then fails as unreachable).
    addr: Option<String>,
    /// The live connection, if the previous exchange completed cleanly.
    stream: Option<TcpStream>,
}

/// The transport-level error openraft treats as "back off and retry".
fn unreachable<E: std::error::Error>(message: String) -> RPCError<u64, BasicNode, E> {
    RPCError::Unreachable(Unreachable::new(&io::Error::other(message)))
}

impl TcpNetwork {
    /// One framed request/response round-trip, bounded by openraft's hard
    /// TTL for the RPC. Every failure mode (connect, IO, decode, timeout)
    /// drops the connection and maps to [`Unreachable`].
    async fn exchange<E>(
        &mut self,
        request: &TcpRequest,
        option: RPCOption,
    ) -> Result<TcpResponse, RPCError<u64, BasicNode, E>>
    where
        E: std::error::Error,
    {
        let target = self.target;
        match tokio::time::timeout(option.hard_ttl(), self.round_trip(request)).await {
            Ok(Ok(response)) => Ok(response),
            Ok(Err(e)) => Err(unreachable(format!("rpc to node {target} failed: {e}"))),
            Err(_) => Err(unreachable(format!(
                "rpc to node {target} exceeded its {:?} ttl",
                option.hard_ttl()
            ))),
        }
    }

    /// Connect if needed, write the request frame, read the response frame.
    ///
    /// The stream is **taken out** of `self` for the duration and only put
    /// back after a complete round-trip, so cancellation (the TTL above, or
    /// openraft dropping the future) cannot leave a desynchronized
    /// connection behind — the next RPC simply dials fresh.
    async fn round_trip(&mut self, request: &TcpRequest) -> io::Result<TcpResponse> {
        let payload = serde_json::to_vec(request).map_err(io::Error::other)?;

        let mut stream = match self.stream.take() {
            Some(stream) => stream,
            None => {
                let addr = self.addr.as_deref().ok_or_else(|| {
                    io::Error::other(format!(
                        "node {} is not in the local cluster config",
                        self.target
                    ))
                })?;
                let stream = TcpStream::connect(addr).await?;
                let _ = stream.set_nodelay(true);
                stream
            }
        };

        write_frame(&mut stream, &payload).await?;
        let response = read_frame(&mut stream).await?;
        let response = serde_json::from_slice(&response)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

        self.stream = Some(stream);
        Ok(response)
    }

    /// The server answered with the wrong response variant: a protocol bug,
    /// not a transient fault — but dropping the connection and letting
    /// openraft retry is still the safest reaction.
    fn wrong_variant<E: std::error::Error>(
        &mut self,
        expected: &str,
    ) -> RPCError<u64, BasicNode, E> {
        self.stream = None;
        unreachable(format!(
            "node {} answered an {expected} rpc with a different variant",
            self.target
        ))
    }
}

impl RaftNetwork<TypeConfig> for TcpNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        match self.exchange(&TcpRequest::AppendEntries(rpc), option).await? {
            TcpResponse::AppendEntries(result) => {
                result.map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
            }
            _ => Err(self.wrong_variant("append-entries")),
        }
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<TypeConfig>,
        option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<u64>,
        RPCError<u64, BasicNode, RaftError<u64, InstallSnapshotError>>,
    > {
        match self.exchange(&TcpRequest::InstallSnapshot(rpc), option).await? {
            TcpResponse::InstallSnapshot(result) => {
                result.map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
            }
            _ => Err(self.wrong_variant("install-snapshot")),
        }
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        option: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        match self.exchange(&TcpRequest::Vote(rpc), option).await? {
            TcpResponse::Vote(result) => {
                result.map_err(|e| RPCError::RemoteError(RemoteError::new(self.target, e)))
            }
            _ => Err(self.wrong_variant("vote")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The chunk-size cap keeps every worst-case-encoded snapshot chunk
    /// (4 wire bytes per chunk byte) inside a frame, with at least 16 MiB
    /// of margin for the request envelope.
    #[test]
    fn snapshot_chunk_cap_fits_a_frame_with_margin() {
        assert!(MAX_SNAPSHOT_CHUNK_BYTES * 4 + 16 * 1024 * 1024 <= MAX_FRAME_LEN as u64);
    }

    /// Frames round-trip bytes exactly, back to back.
    #[tokio::test]
    async fn frames_round_trip() {
        let (mut a, mut b) = tokio::io::duplex(1024);

        write_frame(&mut a, b"hello").await.unwrap();
        write_frame(&mut a, b"").await.unwrap();
        write_frame(&mut a, &[0u8, 1, 2, 255]).await.unwrap();

        assert_eq!(read_frame(&mut b).await.unwrap(), b"hello");
        assert_eq!(read_frame(&mut b).await.unwrap(), b"");
        assert_eq!(read_frame(&mut b).await.unwrap(), &[0u8, 1, 2, 255]);
    }

    /// A length prefix beyond the limit is rejected before any allocation,
    /// as `InvalidData` rather than a panic or OOM.
    #[tokio::test]
    async fn read_frame_rejects_oversized_lengths() {
        let (mut a, mut b) = tokio::io::duplex(64);
        a.write_all(&u32::MAX.to_le_bytes()).await.unwrap();

        let err = read_frame(&mut b).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    /// The writer enforces the same limit symmetrically.
    #[tokio::test]
    async fn write_frame_rejects_oversized_payloads() {
        let (mut a, _b) = tokio::io::duplex(64);
        let oversized = vec![0u8; MAX_FRAME_LEN as usize + 1];

        let err = write_frame(&mut a, &oversized).await.unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
    }

    /// A torn frame (length promises more bytes than ever arrive) surfaces
    /// as an IO error once the stream ends.
    #[tokio::test]
    async fn read_frame_errors_on_torn_payload() {
        let (mut a, mut b) = tokio::io::duplex(64);
        a.write_all(&100u32.to_le_bytes()).await.unwrap();
        a.write_all(b"only ten b").await.unwrap();
        drop(a); // EOF before the promised 100 bytes

        assert!(read_frame(&mut b).await.is_err());
    }
}
