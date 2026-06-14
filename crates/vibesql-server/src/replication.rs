//! Replicated-mode wiring: PostgreSQL sessions → [`MvccRaftNode`] (#5383).
//!
//! When `[replication]` is enabled in the server config, startup boots one
//! voter of a Raft cluster ([`ReplicationHandle::start`]) and every wire
//! session routes writes through consensus instead of the local executor
//! ([`crate::session::Session::new_replicated`]). Standalone mode (the
//! default) is untouched.
//!
//! # SQLSTATE mapping
//!
//! Consensus refusals surface as PostgreSQL errors with deliberately
//! chosen SQLSTATEs (see [`SqlError`]):
//!
//! - [`ConsensusError::NotLeader`] → **`25006`** (`read_only_sql_transaction`). This is exactly
//!   what real PostgreSQL returns when a write reaches a hot-standby replica ("cannot execute
//!   INSERT in a read-only transaction"), so PG-aware clients and poolers that already handle
//!   primary/replica routing (libpq `target_session_attrs=read-write`, JDBC
//!   `targetServerType=primary`, pgpool) treat it as "wrong node, find the primary". The error's
//!   DETAIL carries the leader hint (node id + consensus address from this node's `cluster.toml`
//!   view) so a client can redirect without re-probing the whole cluster.
//! - [`ConsensusError::StalenessExceeded`] / [`ConsensusError::ReadTimeout`] → **`57P03`**
//!   (`cannot_connect_now`): the node cannot serve this read *right now* — the same retryable class
//!   PostgreSQL uses while a standby is catching up ("the database system is starting up"). DETAIL
//!   carries the observed staleness / applied-vs-required indices and the leader hint.
//! - [`ConsensusError::FatalApply`] → **`58000`** (`system_error`): this node halted on a fatal
//!   apply and must be restarted to resync; every statement on the halted node fails with the
//!   retained reason rather than silently serving stale reads. The node also reports
//!   `can_serve_writes = false` from [`ReplicationHandle::health_snapshot`] so the `/health`
//!   endpoint returns 503 and load balancers route around it (#5393).
//! - Anything else → **`XX000`** (`internal_error`), matching the server's existing catch-all.
//!
//! Features a replicated session does not support yet return **`0A000`**
//! (`feature_not_supported`) with a pointer to the follow-on issue:
//! cursors, `EXPLAIN`, `ANALYZE`, `VACUUM` (#5393). `PREPARE`/`EXECUTE`
//! statement syntax is supported (the EXECUTE substitutes its literals into
//! the prepared SQL text and routes through the consensus propose path).
//!
//! [`ConsensusError::NotLeader`]: vibesql_consensus::ConsensusError::NotLeader
//! [`ConsensusError::StalenessExceeded`]: vibesql_consensus::ConsensusError::StalenessExceeded
//! [`ConsensusError::ReadTimeout`]: vibesql_consensus::ConsensusError::ReadTimeout
//! [`ConsensusError::FatalApply`]: vibesql_consensus::ConsensusError::FatalApply

use std::{sync::Arc, time::Duration};

use anyhow::Result;
use vibesql_consensus::{
    ApplyOutcome, ClusterConfig, ConsensusError, LogIndex, MvccRaftNode, QueryResult, RaftTuning,
    Role, TxnEntry,
};

use crate::config::ReplicationConfig;

// ---------------------------------------------------------------------------
// SQLSTATE constants (documented in the module docs above)
// ---------------------------------------------------------------------------

/// `read_only_sql_transaction`: writes (or linearizable reads) reached a
/// non-leader node — what PostgreSQL hot standby returns for writes on a
/// replica.
pub const SQLSTATE_NOT_LEADER: &str = "25006";
/// `cannot_connect_now`: this node cannot serve the read right now
/// (staleness bound not provable / read-your-writes wait expired).
pub const SQLSTATE_RETRY: &str = "57P03";
/// `system_error`: this node halted on a fatal apply.
pub const SQLSTATE_FATAL: &str = "58000";
/// `feature_not_supported`: not available in replicated mode (yet).
pub const SQLSTATE_NOT_SUPPORTED: &str = "0A000";
/// `invalid_parameter_value`: bad value for a `SET vibesql_*` setting.
pub const SQLSTATE_INVALID_PARAMETER: &str = "22023";
/// `internal_error`: the server's existing catch-all.
pub const SQLSTATE_INTERNAL: &str = "XX000";

/// A SQL-level error with a definite SQLSTATE and optional PostgreSQL
/// DETAIL / HINT fields, surfaced to the client as a proper
/// `ErrorResponse` instead of the catch-all `XX000`.
///
/// Carried through `anyhow::Error`; the wire layer downcasts to recover
/// the structured fields (see `connection::query` / `connection::extended`).
#[derive(Debug, thiserror::Error)]
#[error("{message}")]
pub struct SqlError {
    /// Five-character SQLSTATE code.
    pub code: &'static str,
    /// Primary human-readable message (`M` field).
    pub message: String,
    /// Optional detail (`D` field).
    pub detail: Option<String>,
    /// Optional hint (`H` field).
    pub hint: Option<String>,
}

impl SqlError {
    /// A bare error with no detail/hint.
    pub fn new(code: &'static str, message: impl Into<String>) -> Self {
        Self { code, message: message.into(), detail: None, hint: None }
    }

    /// `0A000` for a feature a replicated session does not support yet.
    pub fn not_supported(feature: &str, follow_on: &str) -> Self {
        Self {
            code: SQLSTATE_NOT_SUPPORTED,
            message: format!("{feature} is not supported in replicated mode yet"),
            detail: None,
            hint: Some(format!("tracked in {follow_on}")),
        }
    }
}

/// A point-in-time view of a node's consensus health, produced by
/// [`ReplicationHandle::health_snapshot`] for the `/health` endpoint.
#[derive(Debug, Clone)]
pub struct HealthSnapshot {
    /// This node's id within the cluster.
    pub node_id: u64,
    /// This node's current consensus role.
    pub role: Role,
    /// Whether this node can currently accept writes (a healthy leader).
    pub can_serve_writes: bool,
    /// Dense application index of the last locally applied entry.
    pub applied_index: LogIndex,
    /// The node this one currently believes leads the cluster, if known.
    pub leader_id: Option<u64>,
    /// The fatal-apply reason if this node has halted, else `None`.
    pub fatal_reason: Option<String>,
}

/// Lower-case string name of a consensus [`Role`], for JSON/observability.
pub fn role_str(role: Role) -> &'static str {
    match role {
        Role::Leader => "leader",
        Role::Follower => "follower",
        Role::Candidate => "candidate",
    }
}

// ---------------------------------------------------------------------------
// The handle
// ---------------------------------------------------------------------------

/// One booted consensus voter plus the cluster view it was booted from,
/// shared by every session of a replicated server.
///
/// Thin by design: all consensus semantics live on [`MvccRaftNode`]; this
/// adds config validation, leader-hint address resolution, and the
/// consensus-error → [`SqlError`] mapping.
pub struct ReplicationHandle {
    node: MvccRaftNode,
    cluster: ClusterConfig,
    node_id: u64,
}

impl ReplicationHandle {
    /// Boot this server's consensus voter from the `[replication]`
    /// config: loads `cluster.toml`, then joins the cluster (durably
    /// under `data_dir` when configured, in-memory otherwise).
    ///
    /// Does **not** wait for an election — sessions opened before the
    /// cluster elects a leader simply get `NotLeader` errors on writes
    /// until it does, which is the correct degraded behavior.
    pub async fn start(config: &ReplicationConfig) -> Result<Arc<Self>> {
        let cluster_path = config.cluster_config.as_ref().ok_or_else(|| {
            anyhow::anyhow!("replication.enabled requires replication.cluster_config")
        })?;
        let node_id = config
            .node_id
            .ok_or_else(|| anyhow::anyhow!("replication.enabled requires replication.node_id"))?;
        let cluster = ClusterConfig::load(cluster_path)
            .map_err(|e| anyhow::anyhow!("failed to load {}: {e}", cluster_path.display()))?;
        if cluster.addr(node_id).is_none() {
            return Err(anyhow::anyhow!(
                "replication.node_id {node_id} is not a member of {}",
                cluster_path.display()
            ));
        }
        let tuning =
            RaftTuning { staleness_beacon_ms: config.staleness_beacon_ms, ..RaftTuning::default() };
        let node = match &config.data_dir {
            Some(dir) => {
                MvccRaftNode::join_tcp_cluster_with_data_dir_tuned(node_id, &cluster, dir, tuning)
                    .await
            }
            None => MvccRaftNode::join_tcp_cluster_tuned(node_id, &cluster, tuning).await,
        }
        .map_err(|e| anyhow::anyhow!("failed to join consensus cluster: {e}"))?;
        Ok(Arc::new(Self { node, cluster, node_id }))
    }

    /// Wrap an already-booted consensus node as a [`ReplicationHandle`],
    /// bypassing the `cluster.toml` load + address-bind that
    /// [`start`](Self::start) performs.
    ///
    /// This exists for test harnesses that boot consensus voters on
    /// **pre-bound ephemeral listeners**
    /// ([`MvccRaftNode::join_tcp_cluster_with_listener`]) to avoid the
    /// reserve-then-rebind port-collision race under parallel CI (#5507).
    /// Production servers always go through [`start`](Self::start).
    ///
    /// `#[doc(hidden)]` to keep it off the public surface.
    #[doc(hidden)]
    pub fn from_node(node: MvccRaftNode, cluster: ClusterConfig, node_id: u64) -> Arc<Self> {
        Arc::new(Self { node, cluster, node_id })
    }

    /// The underlying consensus node.
    pub fn node(&self) -> &MvccRaftNode {
        &self.node
    }

    /// This node's id in the cluster.
    pub fn node_id(&self) -> u64 {
        self.node_id
    }

    /// Number of members in the cluster config.
    pub fn cluster_size(&self) -> usize {
        self.cluster.len()
    }

    /// This node's current consensus role.
    pub fn role(&self) -> Role {
        self.node.role()
    }

    /// The reason this node halted on a fatal apply, if it did.
    pub fn fatal_reason(&self) -> Option<String> {
        self.node.fatal_reason()
    }

    /// A point-in-time snapshot of this node's consensus health, for the
    /// `/health` endpoint (#5393). `can_serve_writes` is true only on a
    /// healthy leader (a halted leader cannot serve writes); load balancers
    /// route writes to a node reporting `can_serve_writes = true`, and route
    /// around any node that has a `fatal_reason`.
    pub fn health_snapshot(&self) -> HealthSnapshot {
        let fatal_reason = self.node.fatal_reason();
        let role = self.node.role();
        HealthSnapshot {
            node_id: self.node_id,
            role,
            // A halted node serves nothing further, even if it was leader.
            can_serve_writes: role == Role::Leader && fatal_reason.is_none(),
            applied_index: self.node.last_applied(),
            leader_id: self.node.current_leader(),
            fatal_reason,
        }
    }

    /// Execute one autocommit write statement through consensus,
    /// returning the dense log index (the session's read-your-writes
    /// token) and the apply outcome.
    pub async fn execute_write(&self, sql: &str) -> Result<(LogIndex, ApplyOutcome), SqlError> {
        self.node.execute_replicated(sql).await.map_err(|e| self.sql_error("the statement", e))
    }

    /// Propose an interactive transaction's buffered write statements as
    /// **one** consensus entry (#5391): the whole batch becomes a single
    /// `TxnEntry`, applied atomically with `commit_ts` = its dense log
    /// index. Freeze-at-propose runs once, on the leader, at COMMIT.
    /// Returns the entry's log index (the session's read-your-writes
    /// token) and the apply outcome.
    pub async fn execute_txn(
        &self,
        statements: &[&str],
    ) -> Result<(LogIndex, ApplyOutcome), SqlError> {
        self.node
            .execute_replicated_txn(statements)
            .await
            .map_err(|e| self.sql_error("the transaction", e))
    }

    /// Freeze an open transaction's buffered write into a single-statement
    /// [`TxnEntry`] at buffer time (#5401), so the same frozen values feed
    /// both mid-transaction speculative reads and the authoritative propose
    /// at COMMIT. Leader-only; surfaces `NotLeader` otherwise.
    pub fn freeze_buffered_write(&self, sql: &str) -> Result<TxnEntry, SqlError> {
        self.node.freeze_txn_batch(&[sql]).map_err(|e| self.sql_error("the statement", e))
    }

    /// Speculatively read inside an open replicated transaction (#5401):
    /// replay the buffered (already-frozen) `entry` into a discardable
    /// scratch transaction on the leader and run `select_sql` against it,
    /// so the session observes its own uncommitted writes. Nothing is
    /// committed — the authoritative write happens only at COMMIT.
    /// Leader-only.
    pub fn speculative_query(
        &self,
        entry: &TxnEntry,
        select_sql: &str,
    ) -> Result<QueryResult, SqlError> {
        self.node.speculative_query(entry, select_sql).map_err(|e| self.sql_error("the query", e))
    }

    /// Propose an already-frozen [`TxnEntry`] (the merged open-transaction
    /// buffer) as one consensus entry at COMMIT (#5401), without
    /// re-freezing — the committed rows match what the session saw
    /// mid-transaction. Returns the entry's log index and apply outcome.
    pub async fn propose_txn_entry(
        &self,
        entry: TxnEntry,
    ) -> Result<(LogIndex, ApplyOutcome), SqlError> {
        self.node.propose_txn_entry(entry).await.map_err(|e| self.sql_error("the transaction", e))
    }

    /// Local, stale-allowed read (the default read mode).
    pub fn query_local(&self, sql: &str) -> Result<QueryResult, SqlError> {
        self.node.query(sql).map_err(|e| self.sql_error("the query", e))
    }

    /// Resolve a SELECT's output column names against the applied state
    /// **without executing it** — the names-only path for the
    /// extended-protocol `Describe` (#5484). Resolves via the same
    /// `SelectExecutor::resolve_column_names` the standalone `Describe`
    /// uses (so the labels match label-for-label), but materializes no
    /// rows, unlike [`query_local`](Self::query_local) which ran a full
    /// read just to keep its `.columns`.
    pub fn resolve_column_names(&self, sql: &str) -> Result<Vec<String>, SqlError> {
        self.node.resolve_column_names(sql).map_err(|e| self.sql_error("the query", e))
    }

    /// Subscribe to the apply-path change feed (#5422): a
    /// [`ChangeEventReceiver`](vibesql_storage::ChangeEventReceiver) yielding a
    /// [`ChangeEvent`](vibesql_storage::ChangeEvent) per row the consensus state
    /// machine applies, in commit order. Available on every node — apply runs
    /// on all replicas — so a subscriber connected to a follower observes
    /// committed changes as that follower applies them. This is the replicated
    /// counterpart of `Database::subscribe_changes` that drives standalone
    /// subscriptions; the server's replicated subscription loop drains it and
    /// re-runs each subscription's SELECT against the applied state via
    /// [`with_applied_db`](Self::with_applied_db).
    pub fn subscribe_changes(&self) -> Option<vibesql_storage::ChangeEventReceiver> {
        self.node.subscribe_changes()
    }

    /// Run a closure against the applied (committed) database under the state
    /// machine lock (#5422). The replicated subscription loop uses this to
    /// re-execute a subscription's SELECT against committed state. The closure
    /// must not block or `.await` — it holds the apply mutex.
    pub fn with_applied_db<R>(&self, f: impl FnOnce(&vibesql_storage::Database) -> R) -> R {
        self.node.with_applied_db(f)
    }

    /// Resolve the single-column primary key of `table_name` from the
    /// applied replicated catalog (#5420). The HTTP CRUD by-id endpoints
    /// use this to introspect the PK in replicated mode — where the schema
    /// lives in the consensus state machine, not the (empty) local registry
    /// database — then build their `WHERE pk = {id}` SQL and route it
    /// through the replicated session like the collection endpoints.
    /// Returns `None` for an unknown table, no primary key, or a composite
    /// primary key (the by-id endpoints support single-column keys only,
    /// matching the standalone path).
    pub fn primary_key_column(&self, table_name: &str) -> Option<String> {
        self.node.primary_key_column(table_name)
    }

    /// Snapshot every table's schema from the applied replicated catalog
    /// (#5421), keyed by table name. The HTTP GraphQL surface uses this to
    /// build its GraphQL type and relationship model in replicated mode —
    /// where the schema lives in the consensus state machine, not the
    /// (empty) local registry database — exactly as it would read the local
    /// catalog in standalone mode. Local read of the applied catalog: no
    /// leadership check or network round.
    pub fn schema_snapshot(
        &self,
    ) -> std::collections::HashMap<String, vibesql_catalog::TableSchema> {
        self.node.schema_snapshot()
    }

    /// Linearizable read (quorum-confirmed leadership).
    pub async fn query_linearizable(
        &self,
        sql: &str,
    ) -> Result<QueryResult, SqlError> {
        self.node.query_linearizable(sql).await.map_err(|e| self.sql_error("the query", e))
    }

    /// Bounded-staleness read; a zero bound delegates to linearizable.
    pub async fn query_bounded_staleness(
        &self,
        max_staleness: Duration,
        sql: &str,
    ) -> Result<QueryResult, SqlError> {
        self.node
            .query_bounded_staleness(max_staleness, sql)
            .await
            .map_err(|e| self.sql_error("the query", e))
    }

    /// Read-your-writes read against the session's write token.
    pub async fn query_at_least(
        &self,
        min_index: LogIndex,
        sql: &str,
        wait: Duration,
    ) -> Result<QueryResult, SqlError> {
        self.node
            .query_at_least(min_index, sql, wait)
            .await
            .map_err(|e| self.sql_error("the query", e))
    }

    /// Map a consensus refusal onto a [`SqlError`] (codes documented in
    /// the module docs). `context` names what was being executed, for
    /// the message ("the statement", "the query").
    pub fn sql_error(&self, context: &str, e: ConsensusError) -> SqlError {
        map_consensus_error(self.node_id, &self.cluster, context, e)
    }

    /// The [`SqlError`] every statement gets on a halted node.
    pub fn halted_error(&self, reason: String) -> SqlError {
        self.sql_error("the statement", ConsensusError::FatalApply(reason))
    }
}

/// Best-effort human-readable description of a leader hint, resolved
/// against this node's `cluster.toml` view ("node 2 at 10.0.0.2:5433").
fn describe_leader(cluster: &ClusterConfig, hint: Option<u64>) -> Option<String> {
    let id = hint?;
    Some(match cluster.addr(id) {
        Some(addr) => format!("node {id} at {addr} (consensus address)"),
        None => format!("node {id}"),
    })
}

/// The consensus-error → SQLSTATE mapping (free function so it is
/// unit-testable without booting a node; see the module docs for the
/// code choices).
fn map_consensus_error(
    node_id: u64,
    cluster: &ClusterConfig,
    context: &str,
    e: ConsensusError,
) -> SqlError {
    match e {
        ConsensusError::NotLeader { leader_hint } => {
            let leader = describe_leader(cluster, leader_hint);
            SqlError {
                code: SQLSTATE_NOT_LEADER,
                message: format!(
                    "cannot execute {context} on node {node_id}: not the cluster leader"
                ),
                detail: leader.as_ref().map(|l| format!("current leader: {l}")),
                hint: Some(match &leader {
                    Some(l) => format!("redirect to the leader: {l}"),
                    None => "no leader is currently known; retry shortly".to_string(),
                }),
            }
        }
        ConsensusError::StalenessExceeded { observed_ms, max_staleness_ms, leader_hint } => {
            SqlError {
                code: SQLSTATE_RETRY,
                message: format!(
                    "cannot serve {context} within the staleness bound on node {node_id}"
                ),
                detail: Some(match observed_ms {
                    Some(observed) => format!(
                        "observed staleness {observed}ms exceeds the {max_staleness_ms}ms bound"
                    ),
                    None => format!(
                        "staleness is unknown (no leader-stamped entry applied yet); the bound \
                         is {max_staleness_ms}ms"
                    ),
                }),
                hint: Some(match describe_leader(cluster, leader_hint) {
                    Some(l) => format!(
                        "redirect the read to the leader ({l}) or raise vibesql_max_staleness_ms"
                    ),
                    None => "retry with a larger vibesql_max_staleness_ms, or redirect the read \
                             to the leader"
                        .to_string(),
                }),
            }
        }
        ConsensusError::ReadTimeout { required, applied, leader_hint } => SqlError {
            code: SQLSTATE_RETRY,
            message: format!(
                "read-your-writes wait expired on node {node_id}: applied index {applied} has \
                 not reached the session token {required}"
            ),
            detail: Some(format!("required index {required}, applied index {applied}")),
            hint: Some(match describe_leader(cluster, leader_hint) {
                Some(l) => format!("retry here, or redirect the read to the leader ({l})"),
                None => "retry here, or redirect the read to the leader".to_string(),
            }),
        },
        ConsensusError::FatalApply(reason) => SqlError {
            code: SQLSTATE_FATAL,
            message: format!("node {node_id} has halted on a fatal apply error"),
            detail: Some(reason),
            hint: Some("restart the node to resync via snapshot install + log replay".to_string()),
        },
        other => SqlError::new(SQLSTATE_INTERNAL, other.to_string()),
    }
}

impl std::fmt::Debug for ReplicationHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReplicationHandle")
            .field("node_id", &self.node_id)
            .field("cluster_size", &self.cluster.len())
            .field("role", &self.role())
            .finish_non_exhaustive()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn cluster() -> ClusterConfig {
        ClusterConfig::new([
            (1, "10.0.0.1:5433".to_string()),
            (2, "10.0.0.2:5433".to_string()),
            (3, "10.0.0.3:5433".to_string()),
        ])
        .unwrap()
    }

    #[test]
    fn not_leader_maps_to_25006_with_resolved_leader_address() {
        let err = map_consensus_error(
            2,
            &cluster(),
            "the statement",
            ConsensusError::NotLeader { leader_hint: Some(1) },
        );
        assert_eq!(err.code, SQLSTATE_NOT_LEADER);
        assert!(err.message.contains("node 2"), "{}", err.message);
        let detail = err.detail.expect("detail with leader");
        assert!(detail.contains("node 1 at 10.0.0.1:5433"), "{detail}");
        assert!(err.hint.expect("hint").contains("10.0.0.1:5433"));
    }

    #[test]
    fn not_leader_without_hint_says_so() {
        let err = map_consensus_error(
            3,
            &cluster(),
            "the statement",
            ConsensusError::NotLeader { leader_hint: None },
        );
        assert_eq!(err.code, SQLSTATE_NOT_LEADER);
        assert!(err.detail.is_none());
        assert!(err.hint.expect("hint").contains("no leader is currently known"));
    }

    #[test]
    fn staleness_exceeded_maps_to_57p03() {
        let err = map_consensus_error(
            2,
            &cluster(),
            "the query",
            ConsensusError::StalenessExceeded {
                observed_ms: Some(750),
                max_staleness_ms: 500,
                leader_hint: Some(1),
            },
        );
        assert_eq!(err.code, SQLSTATE_RETRY);
        let detail = err.detail.expect("detail");
        assert!(detail.contains("750ms") && detail.contains("500ms"), "{detail}");
        assert!(err.hint.expect("hint").contains("node 1 at 10.0.0.1:5433"));
    }

    #[test]
    fn unknown_staleness_is_explained() {
        let err = map_consensus_error(
            2,
            &cluster(),
            "the query",
            ConsensusError::StalenessExceeded {
                observed_ms: None,
                max_staleness_ms: 500,
                leader_hint: None,
            },
        );
        assert_eq!(err.code, SQLSTATE_RETRY);
        assert!(err.detail.expect("detail").contains("unknown"));
    }

    #[test]
    fn read_timeout_maps_to_57p03_with_indices() {
        let err = map_consensus_error(
            2,
            &cluster(),
            "the query",
            ConsensusError::ReadTimeout { required: 9, applied: 4, leader_hint: Some(1) },
        );
        assert_eq!(err.code, SQLSTATE_RETRY);
        assert!(err.message.contains('9') && err.message.contains('4'), "{}", err.message);
        assert_eq!(err.detail.as_deref(), Some("required index 9, applied index 4"));
    }

    #[test]
    fn fatal_apply_maps_to_58000_with_reason() {
        let err = map_consensus_error(
            1,
            &cluster(),
            "the statement",
            ConsensusError::FatalApply("disk exploded".to_string()),
        );
        assert_eq!(err.code, SQLSTATE_FATAL);
        assert_eq!(err.detail.as_deref(), Some("disk exploded"));
        assert!(err.hint.expect("hint").contains("restart"));
    }

    #[test]
    fn other_errors_fall_back_to_internal() {
        let err = map_consensus_error(
            1,
            &cluster(),
            "the statement",
            ConsensusError::Backend("boom".to_string()),
        );
        assert_eq!(err.code, SQLSTATE_INTERNAL);
        assert!(err.message.contains("boom"));
    }

    #[test]
    fn leader_hint_outside_cluster_config_degrades_gracefully() {
        let err = map_consensus_error(
            1,
            &cluster(),
            "the statement",
            ConsensusError::NotLeader { leader_hint: Some(42) },
        );
        assert!(err.detail.expect("detail").contains("node 42"));
    }

    #[tokio::test]
    async fn start_validates_required_fields() {
        use crate::config::ReplicationConfig;

        // Missing cluster_config.
        let err = ReplicationHandle::start(&ReplicationConfig {
            enabled: true,
            node_id: Some(1),
            ..ReplicationConfig::default()
        })
        .await
        .unwrap_err();
        assert!(err.to_string().contains("cluster_config"), "{err}");

        // Missing node_id.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cluster.toml");
        std::fs::write(&path, "[[node]]\nid = 1\naddr = \"127.0.0.1:0\"\n").unwrap();
        let err = ReplicationHandle::start(&ReplicationConfig {
            enabled: true,
            cluster_config: Some(path.clone()),
            ..ReplicationConfig::default()
        })
        .await
        .unwrap_err();
        assert!(err.to_string().contains("node_id"), "{err}");

        // node_id not a cluster member.
        let err = ReplicationHandle::start(&ReplicationConfig {
            enabled: true,
            cluster_config: Some(path),
            node_id: Some(7),
            ..ReplicationConfig::default()
        })
        .await
        .unwrap_err();
        assert!(err.to_string().contains("not a member"), "{err}");
    }
}
