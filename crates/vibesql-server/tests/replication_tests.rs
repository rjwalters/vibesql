//! Integration tests for replicated mode (#5383): PostgreSQL sessions
//! routing writes through `MvccRaftNode`, read-consistency session
//! settings, and the NOT_LEADER / staleness / unsupported-feature
//! SQLSTATE surface.
//!
//! Two layers are exercised:
//! - **Session-level**: `Session::new_replicated` against real consensus
//!   nodes (single-node and 3-node TCP clusters on ephemeral ports).
//! - **Wire-level**: a full `ConnectionHandler` server in replicated
//!   mode, driven by `tokio-postgres`, asserting the SQLSTATEs and
//!   redirect fields real clients see.

use std::net::TcpListener as StdTcpListener;
use std::sync::{atomic::AtomicUsize, Arc};
use std::time::Duration;

use tokio::net::TcpListener as TokioTcpListener;
use tokio::sync::{broadcast, oneshot};
use tokio_postgres::error::SqlState;
use tokio_postgres::NoTls;

use vibesql_consensus::Role;
use vibesql_server::{
    config::{Config, ReplicationConfig},
    connection::{ConnectionHandler, TableMutationNotification},
    observability::ObservabilityProvider,
    registry::DatabaseRegistry,
    replication::ReplicationHandle,
    ExecutionResult, Session, SharedDatabase, SqlError, SubscriptionManager,
};
use vibesql_storage::Database;

/// Upper bound for any single cluster-level wait (election, catch-up).
const WAIT_TIMEOUT: Duration = Duration::from_secs(10);

/// Poll interval inside bounded waits.
const POLL_INTERVAL: Duration = Duration::from_millis(20);

// ---------------------------------------------------------------------------
// Cluster helpers
// ---------------------------------------------------------------------------

/// Reserve `n` distinct localhost addresses with OS-assigned ephemeral
/// ports (same strategy as the consensus crate's TCP tests: all
/// reservations held at once, released just before the nodes bind them).
fn free_localhost_addrs(n: u64) -> Vec<(u64, String)> {
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

/// Write a `cluster.toml` for the given members and return its path
/// (inside `dir`).
fn write_cluster_toml(dir: &std::path::Path, members: &[(u64, String)]) -> std::path::PathBuf {
    let mut toml = String::new();
    for (id, addr) in members {
        toml.push_str(&format!("[[node]]\nid = {id}\naddr = \"{addr}\"\n\n"));
    }
    let path = dir.join("cluster.toml");
    std::fs::write(&path, toml).expect("write cluster.toml");
    path
}

/// Boot an `n`-node replicated cluster on ephemeral ports, returning the
/// handles (keyed 1..=n in order) and the tempdir holding the config.
async fn boot_cluster(n: u64) -> (Vec<Arc<ReplicationHandle>>, tempfile::TempDir) {
    let dir = tempfile::tempdir().expect("tempdir");
    let members = free_localhost_addrs(n);
    let path = write_cluster_toml(dir.path(), &members);

    let mut handles = Vec::new();
    for (id, _) in &members {
        let config = ReplicationConfig {
            enabled: true,
            cluster_config: Some(path.clone()),
            node_id: Some(*id),
            data_dir: None,
            staleness_beacon_ms: 0,
        };
        handles.push(ReplicationHandle::start(&config).await.expect("boot consensus node"));
    }
    (handles, dir)
}

/// Wait until some node of `handles` is the leader; returns its index.
async fn wait_for_leader(handles: &[Arc<ReplicationHandle>]) -> usize {
    let deadline = tokio::time::Instant::now() + WAIT_TIMEOUT;
    loop {
        if let Some(i) = handles.iter().position(|h| h.role() == Role::Leader) {
            return i;
        }
        assert!(tokio::time::Instant::now() < deadline, "timed out waiting for a leader");
        tokio::time::sleep(POLL_INTERVAL).await;
    }
}

/// A replicated session over a fresh (unused) local shared database.
fn replicated_session(handle: &Arc<ReplicationHandle>) -> Session {
    Session::new_replicated(
        "testdb".to_string(),
        "testuser".to_string(),
        SharedDatabase::new(Database::new()),
        Arc::clone(handle),
    )
}

/// Downcast an execution error to the structured [`SqlError`].
fn sql_error(err: &anyhow::Error) -> &SqlError {
    err.downcast_ref::<SqlError>()
        .unwrap_or_else(|| panic!("expected a structured SqlError, got: {err}"))
}

fn select_rows(result: ExecutionResult) -> Vec<vibesql_server::Row> {
    match result {
        ExecutionResult::Select { rows, .. } => rows,
        other => panic!("expected Select result, got {other:?}"),
    }
}

// ---------------------------------------------------------------------------
// Session-level: single node
// ---------------------------------------------------------------------------

/// Writes route through consensus and every read mode serves on the
/// leader of a single-node cluster.
#[tokio::test]
async fn replicated_session_writes_and_read_modes() {
    let (handles, _dir) = boot_cluster(1).await;
    wait_for_leader(&handles).await;
    let mut session = replicated_session(&handles[0]);

    // DDL + DML route through consensus.
    let result = session.execute("CREATE TABLE users (id INT, name VARCHAR(100))").await.unwrap();
    assert!(matches!(result, ExecutionResult::CreateTable));
    let result = session.execute("INSERT INTO users VALUES (1, 'Alice')").await.unwrap();
    assert!(matches!(result, ExecutionResult::Insert { rows_affected: 1 }));
    let result = session.execute("UPDATE users SET name = 'Bob' WHERE id = 1").await.unwrap();
    assert!(matches!(result, ExecutionResult::Update { rows_affected: 1 }));

    // The write is visible to every read mode (this node applied it
    // before the propose resolved).
    for set in [
        "SET vibesql_read_consistency = 'local'",
        "SET vibesql_read_consistency = 'linearizable'",
        "SET vibesql_read_consistency = 'read_your_writes'",
        "SET vibesql_read_consistency = 'bounded_staleness'",
    ] {
        session.execute(set).await.unwrap();
        if set.contains("bounded_staleness") {
            // A write just landed, so a generous bound is provable.
            session.execute("SET vibesql_max_staleness_ms = 60000").await.unwrap();
        }
        let rows = select_rows(session.execute("SELECT id, name FROM users").await.unwrap());
        assert_eq!(rows.len(), 1, "read mode {set} should see the row");
    }

    // A deterministic statement failure surfaces the executor's error
    // (the entry was rejected identically on every replica).
    let err = session.execute("INSERT INTO no_such_table VALUES (1)").await.unwrap_err();
    assert!(err.downcast_ref::<SqlError>().is_none(), "rejection keeps the executor error: {err}");
}

/// Unsupported features fail with SQLSTATE 0A000 and a follow-on
/// pointer; invalid SET values fail with 22023.
#[tokio::test]
async fn replicated_session_rejects_unsupported_features() {
    let (handles, _dir) = boot_cluster(1).await;
    wait_for_leader(&handles).await;
    let mut session = replicated_session(&handles[0]);

    let err = session.execute("BEGIN").await.unwrap_err();
    let err = sql_error(&err);
    assert_eq!(err.code, "0A000");
    assert!(err.hint.as_deref().unwrap_or_default().contains("#5391"), "{err:?}");

    for (sql, follow_on) in [
        ("PREPARE p FROM 'SELECT 1'", "#5393"),
        ("DECLARE c CURSOR FOR SELECT 1", "#5393"),
        ("EXPLAIN SELECT 1", "#5393"),
        ("ANALYZE", "#5393"),
        ("VACUUM", "#5393"),
    ] {
        let err = session.execute(sql).await.unwrap_err();
        let err = sql_error(&err);
        assert_eq!(err.code, "0A000", "{sql}");
        assert!(err.hint.as_deref().unwrap_or_default().contains(follow_on), "{sql}: {err:?}");
    }

    // Invalid read-consistency value.
    let err = session.execute("SET vibesql_read_consistency = 'psychic'").await.unwrap_err();
    assert_eq!(sql_error(&err).code, "22023");

    // Negative staleness bound.
    let err = session.execute("SET vibesql_max_staleness_ms = -5").await.unwrap_err();
    assert_eq!(sql_error(&err).code, "22023");

    // Unknown SET variables stay a lenient no-op (PG clients SET all
    // sorts of things at startup).
    session.execute("SET application_name = 'tests'").await.unwrap();

    // PRAGMA stays a no-op, as in standalone mode.
    session.execute("PRAGMA journal_mode = WAL").await.unwrap();
}

// ---------------------------------------------------------------------------
// Session-level: 3-node cluster (NOT_LEADER surface)
// ---------------------------------------------------------------------------

/// Writes (and leader-required reads) on a follower fail with SQLSTATE
/// 25006 carrying the leader's identity for client redirect.
#[tokio::test]
async fn follower_rejects_writes_with_redirect_hint() {
    let (handles, _dir) = boot_cluster(3).await;
    let leader = wait_for_leader(&handles).await;

    // Create the table through the leader.
    let mut leader_session = replicated_session(&handles[leader]);
    leader_session.execute("CREATE TABLE t (id INT)").await.unwrap();

    // Pick a follower that already knows who leads (so the redirect
    // hint is populated deterministically).
    let deadline = tokio::time::Instant::now() + WAIT_TIMEOUT;
    let follower = loop {
        if let Some(i) = handles.iter().position(|h| {
            h.role() == Role::Follower && h.node().current_leader().is_some()
        }) {
            break i;
        }
        assert!(tokio::time::Instant::now() < deadline, "timed out waiting for a follower");
        tokio::time::sleep(POLL_INTERVAL).await;
    };
    let leader_id = handles[leader].node_id();

    let mut session = replicated_session(&handles[follower]);

    // Write on the follower: 25006 + leader identity in detail/hint.
    let err = session.execute("INSERT INTO t VALUES (1)").await.unwrap_err();
    let err = sql_error(&err);
    assert_eq!(err.code, "25006");
    let detail = err.detail.as_deref().expect("redirect detail");
    assert!(detail.contains(&format!("node {leader_id}")), "{detail}");
    assert!(err.hint.as_deref().expect("redirect hint").contains("redirect"), "{err:?}");

    // Linearizable read on the follower: same surface.
    session.execute("SET vibesql_read_consistency = 'linearizable'").await.unwrap();
    let err = session.execute("SELECT * FROM t").await.unwrap_err();
    assert_eq!(sql_error(&err).code, "25006");

    // Bounded staleness with a zero bound is the "staleness 0 redirects
    // to the leader" contract.
    session.execute("SET vibesql_read_consistency = 'bounded_staleness'").await.unwrap();
    session.execute("SET vibesql_max_staleness_ms = 0").await.unwrap();
    let err = session.execute("SELECT * FROM t").await.unwrap_err();
    assert_eq!(sql_error(&err).code, "25006");

    // Local reads always serve on the follower, however stale (the
    // follower may or may not have applied the leader's CREATE yet, so
    // accept either a result or a local table-not-found error — what
    // must NOT happen is a leader-redirect error).
    let assert_not_redirected = |r: anyhow::Result<ExecutionResult>, what: &str| {
        if let Err(err) = r {
            if let Some(sql_err) = err.downcast_ref::<SqlError>() {
                assert_ne!(sql_err.code, "25006", "{what} must not redirect: {sql_err:?}");
                assert_ne!(sql_err.code, "57P03", "{what} must not refuse: {sql_err:?}");
            }
        }
    };
    session.execute("SET vibesql_read_consistency = 'local'").await.unwrap();
    assert_not_redirected(session.execute("SELECT * FROM t").await, "local read");

    // Read-your-writes on the follower: with no write token of its own
    // (token 0 degenerates to a local read) the read serves locally.
    session.execute("SET vibesql_read_consistency = 'read_your_writes'").await.unwrap();
    assert_not_redirected(session.execute("SELECT * FROM t").await, "token-0 read");
}

// ---------------------------------------------------------------------------
// Wire-level: full server in replicated mode, driven by tokio-postgres
// ---------------------------------------------------------------------------

/// A wire-protocol test server, optionally in replicated mode.
struct TestServer {
    port: u16,
    shutdown_tx: Option<oneshot::Sender<()>>,
}

impl TestServer {
    async fn start(replication: Option<Arc<ReplicationHandle>>) -> Self {
        let listener =
            TokioTcpListener::bind("127.0.0.1:0").await.expect("bind test server port");
        let port = listener.local_addr().expect("server port").port();
        let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();

        let mut config = Config::default();
        config.auth.method = "trust".to_string();
        config.http.enabled = false;
        let config = Arc::new(config);

        let observability =
            Arc::new(ObservabilityProvider::init(&config.observability).expect("observability"));
        let active_connections = Arc::new(AtomicUsize::new(0));
        let subscription_manager = Arc::new(SubscriptionManager::new());
        let database_registry = DatabaseRegistry::new();
        let (mutation_broadcast_tx, _rx) = broadcast::channel::<TableMutationNotification>(1024);

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = &mut shutdown_rx => break,
                    accept_result = listener.accept() => {
                        let Ok((stream, peer_addr)) = accept_result else { continue };
                        let config = Arc::clone(&config);
                        let observability = Arc::clone(&observability);
                        let active_connections = Arc::clone(&active_connections);
                        let database_registry = database_registry.clone();
                        let subscription_manager = Arc::clone(&subscription_manager);
                        let mutation_broadcast_tx = mutation_broadcast_tx.clone();
                        let replication = replication.clone();
                        tokio::spawn(async move {
                            let mut handler = ConnectionHandler::new(
                                stream,
                                peer_addr,
                                config,
                                observability,
                                None,
                                active_connections,
                                database_registry,
                                subscription_manager,
                                mutation_broadcast_tx,
                            );
                            if let Some(handle) = replication {
                                handler = handler.with_replication(handle);
                            }
                            let _ = handler.handle().await;
                        });
                    }
                }
            }
        });

        TestServer { port, shutdown_tx: Some(shutdown_tx) }
    }

    fn connection_string(&self) -> String {
        format!("host=127.0.0.1 port={} user=test dbname=test", self.port)
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }
}

async fn connect(server: &TestServer) -> tokio_postgres::Client {
    let (client, connection) =
        tokio_postgres::connect(&server.connection_string(), NoTls).await.expect("connect");
    tokio::spawn(async move {
        let _ = connection.await;
    });
    client
}

/// End to end on the leader: a real PostgreSQL client creates a table,
/// writes through consensus, reads it back, and switches read modes.
#[tokio::test]
async fn wire_protocol_replicated_end_to_end() {
    let (handles, _dir) = boot_cluster(1).await;
    wait_for_leader(&handles).await;
    let server = TestServer::start(Some(Arc::clone(&handles[0]))).await;
    let client = connect(&server).await;

    client.simple_query("CREATE TABLE wire_test (id INT, name VARCHAR(100))").await.unwrap();
    client.simple_query("INSERT INTO wire_test VALUES (1, 'Alice')").await.unwrap();
    client.simple_query("SET vibesql_read_consistency = 'linearizable'").await.unwrap();

    let messages = client.simple_query("SELECT id, name FROM wire_test").await.unwrap();
    let rows: Vec<_> = messages
        .iter()
        .filter_map(|m| match m {
            tokio_postgres::SimpleQueryMessage::Row(row) => Some(row),
            _ => None,
        })
        .collect();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get(0), Some("1"));
    assert_eq!(rows[0].get(1), Some("Alice"));

    // The replicated write is in the consensus state machine.
    assert_eq!(
        handles[0].node().query("SELECT COUNT(*) FROM wire_test").unwrap()[0][0].to_string(),
        "1"
    );

    // BEGIN is refused with feature_not_supported.
    let err = client.simple_query("BEGIN").await.unwrap_err();
    let db_err = err.as_db_error().expect("db error");
    assert_eq!(db_err.code(), &SqlState::FEATURE_NOT_SUPPORTED);
}

/// End to end on a follower: a write gets SQLSTATE 25006 with the
/// leader's identity in DETAIL/HINT — the client-redirect contract.
#[tokio::test]
async fn wire_protocol_follower_write_redirects() {
    let (handles, _dir) = boot_cluster(3).await;
    let leader = wait_for_leader(&handles).await;

    // Set up schema through the leader.
    let mut leader_session = replicated_session(&handles[leader]);
    leader_session.execute("CREATE TABLE wire_redirect (id INT)").await.unwrap();
    let leader_id = handles[leader].node_id();

    // Wait for a follower that knows the leader.
    let deadline = tokio::time::Instant::now() + WAIT_TIMEOUT;
    let follower = loop {
        if let Some(i) = handles.iter().position(|h| {
            h.role() == Role::Follower && h.node().current_leader().is_some()
        }) {
            break i;
        }
        assert!(tokio::time::Instant::now() < deadline, "timed out waiting for a follower");
        tokio::time::sleep(POLL_INTERVAL).await;
    };

    let server = TestServer::start(Some(Arc::clone(&handles[follower]))).await;
    let client = connect(&server).await;

    let err = client.simple_query("INSERT INTO wire_redirect VALUES (1)").await.unwrap_err();
    let db_err = err.as_db_error().expect("db error");
    assert_eq!(db_err.code(), &SqlState::READ_ONLY_SQL_TRANSACTION);
    let detail = db_err.detail().expect("redirect detail");
    assert!(detail.contains(&format!("node {leader_id}")), "{detail}");
    assert!(db_err.hint().expect("redirect hint").contains("redirect"));
}

/// Standalone servers are untouched: the same wire flow works without
/// replication, including BEGIN/COMMIT (regression guard for the
/// optional wiring).
#[tokio::test]
async fn wire_protocol_standalone_unaffected() {
    let server = TestServer::start(None).await;
    let client = connect(&server).await;

    client.simple_query("CREATE TABLE standalone_test (id INT)").await.unwrap();
    client.simple_query("BEGIN").await.unwrap();
    client.simple_query("INSERT INTO standalone_test VALUES (1)").await.unwrap();
    client.simple_query("COMMIT").await.unwrap();

    let messages = client.simple_query("SELECT * FROM standalone_test").await.unwrap();
    let rows = messages
        .iter()
        .filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_)))
        .count();
    assert_eq!(rows, 1);
}
