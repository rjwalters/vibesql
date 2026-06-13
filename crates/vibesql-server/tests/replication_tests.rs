//! Integration tests for replicated mode (#5383): PostgreSQL sessions
//! routing writes through `MvccRaftNode`, read-consistency session
//! settings, and the NOT_LEADER / staleness / unsupported-feature
//! SQLSTATE surface.
//!
//! Two layers are exercised:
//! - **Session-level**: `Session::new_replicated` against real consensus nodes (single-node and
//!   3-node TCP clusters on ephemeral ports).
//! - **Wire-level**: a full `ConnectionHandler` server in replicated mode, driven by
//!   `tokio-postgres`, asserting the SQLSTATEs and redirect fields real clients see.

use std::{
    net::TcpListener as StdTcpListener,
    sync::{atomic::AtomicUsize, Arc},
    time::Duration,
};

use tokio::{
    net::TcpListener as TokioTcpListener,
    sync::{broadcast, oneshot},
};
use tokio_postgres::{error::SqlState, NoTls};
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
use vibesql_types::SqlValue;

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

    // BEGIN/COMMIT/ROLLBACK are supported as of #5391 (see the dedicated
    // interactive-transaction tests); PREPARE/EXECUTE syntax is supported
    // as of #5393 (see the dedicated prepared-statement tests). Neither is
    // refused here any longer.

    for (sql, follow_on) in [
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
// Session-level: PREPARE/EXECUTE statement syntax (#5393)
// ---------------------------------------------------------------------------

/// A PREPARE'd INSERT routes its EXECUTE through consensus (one log entry
/// per EXECUTE), exactly like the simple-query write path: the rows land in
/// the replicated state machine and the row count comes back from the apply.
#[tokio::test]
async fn replicated_prepared_insert_routes_through_consensus() {
    let (handles, _dir) = boot_cluster(1).await;
    wait_for_leader(&handles).await;
    let mut session = replicated_session(&handles[0]);

    session.execute("CREATE TABLE users (id INT, name VARCHAR(100))").await.unwrap();

    // PREPARE captures the SQL text; EXECUTE substitutes the literals and
    // proposes through consensus.
    session.execute("PREPARE ins FROM 'INSERT INTO users VALUES (?, ?)'").await.unwrap();
    let applied_before = handles[0].node().last_applied();

    let r = session.execute("EXECUTE ins (1, 'Alice')").await.unwrap();
    assert!(matches!(r, ExecutionResult::Insert { rows_affected: 1 }), "{r:?}");
    let r = session.execute("EXECUTE ins USING 2, 'Bob'").await.unwrap();
    assert!(matches!(r, ExecutionResult::Insert { rows_affected: 1 }), "{r:?}");

    // Each EXECUTE was its own replicated entry.
    assert_eq!(handles[0].node().last_applied(), applied_before + 2);

    // Both rows are in the consensus state machine, with the quoted string
    // preserved (no SQL injection / quote-escaping breakage).
    let rows = handles[0].node().query("SELECT id, name FROM users ORDER BY id").unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[1][1].to_string(), "Bob");

    // DEALLOCATE drops the named statement; a later EXECUTE errors.
    session.execute("DEALLOCATE ins").await.unwrap();
    assert!(session.execute("EXECUTE ins (3, 'Carol')").await.is_err());
}

/// A PREPARE'd SELECT honors the session's read-consistency mode and reads
/// from the replicated state machine (not the empty local database).
#[tokio::test]
async fn replicated_prepared_select_reads_state_machine() {
    let (handles, _dir) = boot_cluster(1).await;
    wait_for_leader(&handles).await;
    let mut session = replicated_session(&handles[0]);

    session.execute("CREATE TABLE t (id INT)").await.unwrap();
    session.execute("INSERT INTO t VALUES (1)").await.unwrap();
    session.execute("INSERT INTO t VALUES (2)").await.unwrap();

    session.execute("PREPARE sel FROM 'SELECT id FROM t WHERE id = ?'").await.unwrap();
    session.execute("SET vibesql_read_consistency = 'linearizable'").await.unwrap();

    let rows = select_rows(session.execute("EXECUTE sel (2)").await.unwrap());
    assert_eq!(rows.len(), 1, "prepared SELECT must read the replicated data");
    assert_eq!(rows[0].values[0].to_string(), "2");
}

/// A PREPARE'd write on a follower is refused with the same NOT_LEADER
/// surface (SQLSTATE 25006 + redirect hint) as a simple-query write — the
/// prepared path is not a bypass.
#[tokio::test]
async fn replicated_prepared_write_on_follower_redirects() {
    let (handles, _dir) = boot_cluster(3).await;
    let leader = wait_for_leader(&handles).await;

    let mut leader_session = replicated_session(&handles[leader]);
    leader_session.execute("CREATE TABLE t (id INT)").await.unwrap();

    let deadline = tokio::time::Instant::now() + WAIT_TIMEOUT;
    let follower = loop {
        if let Some(i) = handles
            .iter()
            .position(|h| h.role() == Role::Follower && h.node().current_leader().is_some())
        {
            break i;
        }
        assert!(tokio::time::Instant::now() < deadline, "timed out waiting for a follower");
        tokio::time::sleep(POLL_INTERVAL).await;
    };

    let mut follower_session = replicated_session(&handles[follower]);
    // The follower may not have applied the leader's CREATE TABLE yet; the
    // propose itself is refused before any local execution regardless.
    follower_session.execute("PREPARE ins FROM 'INSERT INTO t VALUES (?)'").await.unwrap();
    let err = follower_session.execute("EXECUTE ins (1)").await.unwrap_err();
    assert_eq!(sql_error(&err).code, "25006", "prepared write must redirect like a simple write");
}

// ---------------------------------------------------------------------------
// Session-level: health snapshot (#5393)
// ---------------------------------------------------------------------------

/// The leader reports writable; a follower reports not-writable with the
/// leader's id; both expose a monotonic applied index.
#[tokio::test]
async fn health_snapshot_reports_role_and_writability() {
    let (handles, _dir) = boot_cluster(3).await;
    let leader = wait_for_leader(&handles).await;

    let leader_snap = handles[leader].health_snapshot();
    assert_eq!(leader_snap.role, Role::Leader);
    assert!(leader_snap.can_serve_writes, "a healthy leader must be writable");
    assert!(leader_snap.fatal_reason.is_none());

    // Wait for a follower that knows the leader, then check its snapshot.
    let deadline = tokio::time::Instant::now() + WAIT_TIMEOUT;
    let follower = loop {
        if let Some(i) = handles
            .iter()
            .position(|h| h.role() == Role::Follower && h.node().current_leader().is_some())
        {
            break i;
        }
        assert!(tokio::time::Instant::now() < deadline, "timed out waiting for a follower");
        tokio::time::sleep(POLL_INTERVAL).await;
    };
    let follower_snap = handles[follower].health_snapshot();
    assert_eq!(follower_snap.role, Role::Follower);
    assert!(!follower_snap.can_serve_writes, "a follower must not be writable");
    assert_eq!(follower_snap.leader_id, Some(handles[leader].node_id()));
}

// ---------------------------------------------------------------------------
// Session-level: interactive transactions (#5391)
// ---------------------------------------------------------------------------

/// BEGIN ... INSERT ... INSERT ... COMMIT replicates the whole batch as
/// **one** consensus entry: a single log index is consumed and both rows
/// are visible afterward.
#[tokio::test]
async fn replicated_txn_commits_batch_as_one_entry() {
    let (handles, _dir) = boot_cluster(1).await;
    wait_for_leader(&handles).await;
    let mut session = replicated_session(&handles[0]);

    session.execute("CREATE TABLE accounts (id INT, balance INT)").await.unwrap();
    let applied_before = handles[0].node().last_applied();

    // Open transaction: writes buffer and ack optimistically (rows = 0).
    assert!(matches!(session.execute("BEGIN").await.unwrap(), ExecutionResult::Begin));
    let r = session.execute("INSERT INTO accounts VALUES (1, 100)").await.unwrap();
    assert!(matches!(r, ExecutionResult::Insert { rows_affected: 0 }), "{r:?}");
    let r = session.execute("INSERT INTO accounts VALUES (2, 200)").await.unwrap();
    assert!(matches!(r, ExecutionResult::Insert { rows_affected: 0 }), "{r:?}");

    // Nothing is applied yet — the batch is not proposed until COMMIT.
    assert_eq!(handles[0].node().last_applied(), applied_before, "buffer must not apply early");

    assert!(matches!(session.execute("COMMIT").await.unwrap(), ExecutionResult::Commit));

    // Exactly one entry was consumed for the whole transaction.
    assert_eq!(
        handles[0].node().last_applied(),
        applied_before + 1,
        "the transaction must be a single log entry"
    );

    // Both rows replicated atomically.
    let rows = select_rows(session.execute("SELECT id, balance FROM accounts").await.unwrap());
    assert_eq!(rows.len(), 2);
}

/// ROLLBACK discards the buffer without proposing anything: no log index
/// is consumed and none of the buffered writes land.
#[tokio::test]
async fn replicated_txn_rollback_discards_buffer() {
    let (handles, _dir) = boot_cluster(1).await;
    wait_for_leader(&handles).await;
    let mut session = replicated_session(&handles[0]);

    session.execute("CREATE TABLE t (id INT)").await.unwrap();
    let applied_before = handles[0].node().last_applied();

    session.execute("BEGIN").await.unwrap();
    session.execute("INSERT INTO t VALUES (1)").await.unwrap();
    session.execute("INSERT INTO t VALUES (2)").await.unwrap();
    assert!(matches!(session.execute("ROLLBACK").await.unwrap(), ExecutionResult::Rollback));

    // No entry consumed, no rows landed.
    assert_eq!(handles[0].node().last_applied(), applied_before, "rollback must not propose");
    let rows = select_rows(session.execute("SELECT id FROM t").await.unwrap());
    assert!(rows.is_empty(), "rolled-back writes must not be visible");

    // The session is back in autocommit: a write proposes immediately.
    let r = session.execute("INSERT INTO t VALUES (3)").await.unwrap();
    assert!(matches!(r, ExecutionResult::Insert { rows_affected: 1 }), "{r:?}");
    assert_eq!(handles[0].node().last_applied(), applied_before + 1);
}

/// A deterministic statement failure in the batch rejects the whole
/// COMMIT — no partial state is committed.
#[tokio::test]
async fn replicated_txn_deterministic_failure_rejects_whole_batch() {
    let (handles, _dir) = boot_cluster(1).await;
    wait_for_leader(&handles).await;
    let mut session = replicated_session(&handles[0]);

    session.execute("CREATE TABLE t (id INTEGER PRIMARY KEY)").await.unwrap();

    session.execute("BEGIN").await.unwrap();
    session.execute("INSERT INTO t VALUES (1)").await.unwrap();
    // Duplicate primary key: deterministic rejection at apply.
    session.execute("INSERT INTO t VALUES (1)").await.unwrap();

    let err = session.execute("COMMIT").await.unwrap_err();
    // A rejection surfaces the executor error, not a structured SqlError.
    assert!(err.downcast_ref::<SqlError>().is_none(), "rejection keeps the executor error: {err}");

    // No partial state: the first INSERT did not commit either.
    let rows = select_rows(session.execute("SELECT id FROM t").await.unwrap());
    assert!(rows.is_empty(), "a rejected batch must commit nothing: {rows:?}");
}

/// An empty transaction (BEGIN; COMMIT; with no writes) consumes no log
/// index — there is nothing to replicate.
#[tokio::test]
async fn replicated_empty_txn_consumes_no_index() {
    let (handles, _dir) = boot_cluster(1).await;
    wait_for_leader(&handles).await;
    let mut session = replicated_session(&handles[0]);

    let applied_before = handles[0].node().last_applied();
    session.execute("BEGIN").await.unwrap();
    assert!(matches!(session.execute("COMMIT").await.unwrap(), ExecutionResult::Commit));
    assert_eq!(handles[0].node().last_applied(), applied_before, "empty txn must not propose");
}

/// Full mid-transaction read-your-own-writes (#5401): a read after a
/// buffered INSERT sees the session's own uncommitted write, via
/// leader-local speculative replay — without proposing anything until
/// COMMIT.
#[tokio::test]
async fn replicated_txn_read_sees_own_buffered_writes() {
    let (handles, _dir) = boot_cluster(1).await;
    wait_for_leader(&handles).await;
    let mut session = replicated_session(&handles[0]);

    session.execute("CREATE TABLE t (id INT)").await.unwrap();
    session.execute("INSERT INTO t VALUES (1)").await.unwrap();
    let applied_before = handles[0].node().last_applied();

    session.execute("BEGIN").await.unwrap();
    // No buffered writes yet: a read observes committed state.
    let rows = select_rows(session.execute("SELECT id FROM t").await.unwrap());
    assert_eq!(rows.len(), 1, "pre-write read sees committed state");

    // After a buffered INSERT, the read sees the session's own write.
    session.execute("INSERT INTO t VALUES (2)").await.unwrap();
    let rows = select_rows(session.execute("SELECT id FROM t ORDER BY id").await.unwrap());
    assert_eq!(rows.len(), 2, "read-your-own-writes: the buffered INSERT is visible");

    // Nothing was proposed: the speculative read does not consume an index.
    assert_eq!(
        handles[0].node().last_applied(),
        applied_before,
        "speculative reads must not propose"
    );

    // A mid-txn UPDATE then SELECT reflects it.
    session.execute("UPDATE t SET id = 99 WHERE id = 2").await.unwrap();
    let rows = select_rows(session.execute("SELECT id FROM t ORDER BY id").await.unwrap());
    let ids: Vec<_> = rows.iter().map(|r| r.values[0].clone()).collect();
    assert_eq!(ids, vec![SqlValue::Integer(1), SqlValue::Integer(99)], "UPDATE reflected mid-txn");

    session.execute("COMMIT").await.unwrap();
    // Committed state matches what the session saw mid-transaction.
    assert_eq!(handles[0].node().last_applied(), applied_before + 1, "one entry for the txn");
    let rows = select_rows(session.execute("SELECT id FROM t ORDER BY id").await.unwrap());
    let ids: Vec<_> = rows.iter().map(|r| r.values[0].clone()).collect();
    assert_eq!(ids, vec![SqlValue::Integer(1), SqlValue::Integer(99)]);
}

/// SAVEPOINT / ROLLBACK TO truncates the buffered batch to the marker:
/// writes after the savepoint are discarded, the survivors commit as one
/// entry, and the committed state equals what the client saw.
#[tokio::test]
async fn replicated_txn_savepoint_rollback_to_discards_later_writes() {
    let (handles, _dir) = boot_cluster(1).await;
    wait_for_leader(&handles).await;
    let mut session = replicated_session(&handles[0]);

    session.execute("CREATE TABLE t (id INT)").await.unwrap();
    let applied_before = handles[0].node().last_applied();

    session.execute("BEGIN").await.unwrap();
    session.execute("INSERT INTO t VALUES (1)").await.unwrap();
    session.execute("SAVEPOINT s1").await.unwrap();
    session.execute("INSERT INTO t VALUES (2)").await.unwrap();
    session.execute("INSERT INTO t VALUES (3)").await.unwrap();

    // Before ROLLBACK TO, the reads see all three.
    let rows = select_rows(session.execute("SELECT id FROM t ORDER BY id").await.unwrap());
    assert_eq!(rows.len(), 3, "all buffered writes visible before ROLLBACK TO");

    // ROLLBACK TO discards the two writes after the savepoint.
    session.execute("ROLLBACK TO SAVEPOINT s1").await.unwrap();
    let rows = select_rows(session.execute("SELECT id FROM t ORDER BY id").await.unwrap());
    let ids: Vec<_> = rows.iter().map(|r| r.values[0].clone()).collect();
    assert_eq!(ids, vec![SqlValue::Integer(1)], "only the pre-savepoint write survives");

    // COMMIT applies only the survivor, as one entry.
    session.execute("COMMIT").await.unwrap();
    assert_eq!(handles[0].node().last_applied(), applied_before + 1);
    let rows = select_rows(session.execute("SELECT id FROM t ORDER BY id").await.unwrap());
    let ids: Vec<_> = rows.iter().map(|r| r.values[0].clone()).collect();
    assert_eq!(ids, vec![SqlValue::Integer(1)], "committed state matches what the client saw");
}

/// RELEASE SAVEPOINT keeps the buffered writes; they commit with the
/// transaction.
#[tokio::test]
async fn replicated_txn_release_savepoint_keeps_writes() {
    let (handles, _dir) = boot_cluster(1).await;
    wait_for_leader(&handles).await;
    let mut session = replicated_session(&handles[0]);

    session.execute("CREATE TABLE t (id INT)").await.unwrap();

    session.execute("BEGIN").await.unwrap();
    session.execute("INSERT INTO t VALUES (1)").await.unwrap();
    session.execute("SAVEPOINT s1").await.unwrap();
    session.execute("INSERT INTO t VALUES (2)").await.unwrap();
    session.execute("RELEASE SAVEPOINT s1").await.unwrap();

    // ROLLBACK TO a released savepoint is an error.
    let err = session.execute("ROLLBACK TO SAVEPOINT s1").await.unwrap_err();
    assert!(err.to_string().contains("no such savepoint"), "{err}");

    session.execute("COMMIT").await.unwrap();
    let rows = select_rows(session.execute("SELECT id FROM t ORDER BY id").await.unwrap());
    assert_eq!(rows.len(), 2, "released savepoint keeps both writes");
}

/// A volatile write inside a transaction freezes its value once: the
/// mid-transaction read and the committed row carry the same value.
#[tokio::test]
async fn replicated_txn_volatile_write_frozen_value_is_consistent() {
    let (handles, _dir) = boot_cluster(1).await;
    wait_for_leader(&handles).await;
    let mut session = replicated_session(&handles[0]);

    session.execute("CREATE TABLE t (id INT, r INT)").await.unwrap();

    session.execute("BEGIN").await.unwrap();
    session.execute("INSERT INTO t VALUES (1, abs(random()))").await.unwrap();
    // Read the frozen value mid-transaction.
    let rows = select_rows(session.execute("SELECT r FROM t WHERE id = 1").await.unwrap());
    assert_eq!(rows.len(), 1);
    let mid_txn_value = rows[0].values[0].clone();

    session.execute("COMMIT").await.unwrap();
    // The committed row carries the same frozen value.
    let rows = select_rows(session.execute("SELECT r FROM t WHERE id = 1").await.unwrap());
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].values[0], mid_txn_value,
        "the committed volatile value must equal what the session saw mid-transaction"
    );
}

/// SAVEPOINT / ROLLBACK TO / RELEASE outside a transaction are errors.
#[tokio::test]
async fn replicated_savepoint_outside_txn_errors() {
    let (handles, _dir) = boot_cluster(1).await;
    wait_for_leader(&handles).await;
    let mut session = replicated_session(&handles[0]);

    for sql in ["SAVEPOINT s1", "ROLLBACK TO SAVEPOINT s1", "RELEASE SAVEPOINT s1"] {
        let err = session.execute(sql).await.unwrap_err();
        assert!(err.to_string().contains("inside a transaction"), "{sql}: {err}");
    }
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
        if let Some(i) = handles
            .iter()
            .position(|h| h.role() == Role::Follower && h.node().current_leader().is_some())
        {
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

/// A buffered write inside a transaction on a follower fails fast with
/// SQLSTATE 25006 (the leader-redirect contract): freezing a buffered
/// write — like the speculative read path — is leader-only (#5401), so the
/// follower surfaces "not the leader" at the write rather than deferring it
/// to COMMIT. The transaction stays open (nothing was buffered) and a
/// ROLLBACK cleanly closes it.
#[tokio::test]
async fn replicated_txn_buffered_write_on_follower_redirects() {
    let (handles, _dir) = boot_cluster(3).await;
    let leader = wait_for_leader(&handles).await;

    // Create the table through the leader.
    replicated_session(&handles[leader]).execute("CREATE TABLE t (id INT)").await.unwrap();

    // Pick a follower that knows who leads.
    let deadline = tokio::time::Instant::now() + WAIT_TIMEOUT;
    let follower = loop {
        if let Some(i) = handles
            .iter()
            .position(|h| h.role() == Role::Follower && h.node().current_leader().is_some())
        {
            break i;
        }
        assert!(tokio::time::Instant::now() < deadline, "timed out waiting for a follower");
        tokio::time::sleep(POLL_INTERVAL).await;
    };

    let mut session = replicated_session(&handles[follower]);

    // BEGIN succeeds (session-local), but the buffered write fails fast:
    // freezing is leader-only, so the follower surfaces 25006 immediately.
    session.execute("BEGIN").await.unwrap();
    let err = session.execute("INSERT INTO t VALUES (1)").await.unwrap_err();
    assert_eq!(sql_error(&err).code, "25006");

    // The transaction is still open (nothing buffered); ROLLBACK closes it.
    assert!(matches!(session.execute("ROLLBACK").await.unwrap(), ExecutionResult::Rollback));
}

/// A committed transaction replicates atomically to a second node: after
/// COMMIT on the leader, a follower converges to **all** of the
/// transaction's rows (one entry applies all or none).
#[tokio::test]
async fn replicated_txn_replicates_atomically_to_followers() {
    let (handles, _dir) = boot_cluster(3).await;
    let leader = wait_for_leader(&handles).await;
    let mut session = replicated_session(&handles[leader]);

    session.execute("CREATE TABLE kv (id INT, v INT)").await.unwrap();
    session.execute("BEGIN").await.unwrap();
    session.execute("INSERT INTO kv VALUES (1, 10)").await.unwrap();
    session.execute("INSERT INTO kv VALUES (2, 20)").await.unwrap();
    session.execute("INSERT INTO kv VALUES (3, 30)").await.unwrap();
    session.execute("COMMIT").await.unwrap();

    // Every follower converges to all three rows (never a partial 1 or 2).
    let deadline = tokio::time::Instant::now() + WAIT_TIMEOUT;
    for (i, h) in handles.iter().enumerate() {
        if i == leader {
            continue;
        }
        loop {
            let count = h
                .node()
                .query("SELECT COUNT(*) FROM kv")
                .ok()
                .and_then(|r| r.first().and_then(|row| row.first()).map(ToString::to_string));
            if count.as_deref() == Some("3") {
                break;
            }
            // A partial count would mean the batch did not apply atomically.
            assert!(
                matches!(count.as_deref(), None | Some("0") | Some("3")),
                "follower {i} saw a non-atomic count: {count:?}"
            );
            assert!(
                tokio::time::Instant::now() < deadline,
                "follower {i} did not converge to 3 rows (saw {count:?})"
            );
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }
}

/// A transaction with SAVEPOINT + ROLLBACK TO commits only the surviving
/// writes, and a follower converges to exactly that state (#5401): the
/// committed state matches what the client saw, on a second node.
#[tokio::test]
async fn replicated_txn_savepoint_survivors_replicate_to_followers() {
    let (handles, _dir) = boot_cluster(3).await;
    let leader = wait_for_leader(&handles).await;
    let mut session = replicated_session(&handles[leader]);

    session.execute("CREATE TABLE kv (id INT)").await.unwrap();
    session.execute("BEGIN").await.unwrap();
    session.execute("INSERT INTO kv VALUES (1)").await.unwrap();
    session.execute("SAVEPOINT s1").await.unwrap();
    session.execute("INSERT INTO kv VALUES (2)").await.unwrap();
    session.execute("INSERT INTO kv VALUES (3)").await.unwrap();
    session.execute("ROLLBACK TO SAVEPOINT s1").await.unwrap();
    session.execute("COMMIT").await.unwrap();

    // Every follower converges to exactly the one surviving row (id = 1),
    // never the rolled-back 2/3.
    let deadline = tokio::time::Instant::now() + WAIT_TIMEOUT;
    for (i, h) in handles.iter().enumerate() {
        if i == leader {
            continue;
        }
        loop {
            let count = h
                .node()
                .query("SELECT COUNT(*) FROM kv")
                .ok()
                .and_then(|r| r.first().and_then(|row| row.first()).map(ToString::to_string));
            if count.as_deref() == Some("1") {
                // The single surviving row must be id = 1, not a later one.
                let ids = h.node().query("SELECT id FROM kv").expect("query ids");
                assert_eq!(ids.len(), 1);
                assert_eq!(ids[0][0], SqlValue::Integer(1), "follower {i} kept the wrong row");
                break;
            }
            assert!(
                matches!(count.as_deref(), None | Some("0") | Some("1")),
                "follower {i} saw an unexpected count: {count:?}"
            );
            assert!(
                tokio::time::Instant::now() < deadline,
                "follower {i} did not converge to 1 row (saw {count:?})"
            );
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }
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
        let listener = TokioTcpListener::bind("127.0.0.1:0").await.expect("bind test server port");
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

    // A wire-level interactive transaction commits atomically (#5391).
    client.simple_query("BEGIN").await.unwrap();
    client.simple_query("INSERT INTO wire_test VALUES (2, 'Bob')").await.unwrap();
    client.simple_query("INSERT INTO wire_test VALUES (3, 'Carol')").await.unwrap();
    client.simple_query("COMMIT").await.unwrap();
    assert_eq!(
        handles[0].node().query("SELECT COUNT(*) FROM wire_test").unwrap()[0][0].to_string(),
        "3"
    );

    // ROLLBACK discards a buffered write.
    client.simple_query("BEGIN").await.unwrap();
    client.simple_query("INSERT INTO wire_test VALUES (4, 'Dan')").await.unwrap();
    client.simple_query("ROLLBACK").await.unwrap();
    assert_eq!(
        handles[0].node().query("SELECT COUNT(*) FROM wire_test").unwrap()[0][0].to_string(),
        "3"
    );

    // Read-your-own-writes and savepoints work end-to-end (#5401): a read
    // after a buffered INSERT sees it, ROLLBACK TO discards later writes,
    // and only the survivors commit.
    client.simple_query("BEGIN").await.unwrap();
    client.simple_query("INSERT INTO wire_test VALUES (4, 'Dan')").await.unwrap();
    let messages = client.simple_query("SELECT COUNT(*) FROM wire_test").await.unwrap();
    let mid = messages.iter().find_map(|m| match m {
        tokio_postgres::SimpleQueryMessage::Row(row) => row.get(0).map(str::to_string),
        _ => None,
    });
    assert_eq!(mid.as_deref(), Some("4"), "read-your-own-writes: the buffered INSERT is visible");
    client.simple_query("SAVEPOINT s1").await.unwrap();
    client.simple_query("INSERT INTO wire_test VALUES (5, 'Eve')").await.unwrap();
    client.simple_query("ROLLBACK TO SAVEPOINT s1").await.unwrap();
    client.simple_query("COMMIT").await.unwrap();
    // Dan (4) survives; Eve (5) was rolled back to the savepoint.
    assert_eq!(
        handles[0].node().query("SELECT COUNT(*) FROM wire_test").unwrap()[0][0].to_string(),
        "4"
    );
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
        if let Some(i) = handles
            .iter()
            .position(|h| h.role() == Role::Follower && h.node().current_leader().is_some())
        {
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
    let rows =
        messages.iter().filter(|m| matches!(m, tokio_postgres::SimpleQueryMessage::Row(_))).count();
    assert_eq!(rows, 1);
}

/// The PostgreSQL **extended** query protocol (Parse/Bind/Execute, used by
/// `tokio_postgres::Client::execute`/`query`) routes a write through
/// consensus in replicated mode: `handle_execute` runs the bound query via
/// `Session::execute`, which dispatches through the replicated path exactly
/// like the simple-query path. This guards the contract that the extended
/// protocol is not a write bypass (#5393).
///
/// (Bound `$N` parameters are exercised by the simple-query and
/// session-level PREPARE/EXECUTE tests; VibeSQL's extended-protocol
/// Describe does not infer parameter OIDs, so this test drives the
/// Parse/Bind/Execute path the same param-free way the other
/// extended-protocol integration tests do.)
#[tokio::test]
async fn wire_protocol_extended_write_replicates() {
    let (handles, _dir) = boot_cluster(1).await;
    wait_for_leader(&handles).await;
    let server = TestServer::start(Some(Arc::clone(&handles[0]))).await;
    let client = connect(&server).await;

    // `execute`/`query` drive Parse/Bind/Execute (not the simple-query
    // protocol). The write must land in the consensus state machine.
    client.execute("CREATE TABLE ext (id INT, name VARCHAR(100))", &[]).await.unwrap();
    let affected = client.execute("INSERT INTO ext VALUES (1, 'Alice')", &[]).await.unwrap();
    assert_eq!(affected, 1);
    assert_eq!(handles[0].node().query("SELECT COUNT(*) FROM ext").unwrap()[0][0].to_string(), "1");

    // A read over the extended protocol reads it back from the state machine.
    let rows = client.query("SELECT name FROM ext WHERE id = 1", &[]).await.unwrap();
    assert_eq!(rows.len(), 1);
    let name: &str = rows[0].get(0);
    assert_eq!(name, "Alice");
}

/// An extended-protocol write on a follower is refused with SQLSTATE 25006,
/// exactly like the simple-query path — the extended path is not a bypass.
#[tokio::test]
async fn wire_protocol_extended_write_on_follower_redirects() {
    let (handles, _dir) = boot_cluster(3).await;
    let leader = wait_for_leader(&handles).await;

    let mut leader_session = replicated_session(&handles[leader]);
    leader_session.execute("CREATE TABLE ext_redirect (id INT)").await.unwrap();

    let deadline = tokio::time::Instant::now() + WAIT_TIMEOUT;
    let follower = loop {
        if let Some(i) = handles
            .iter()
            .position(|h| h.role() == Role::Follower && h.node().current_leader().is_some())
        {
            break i;
        }
        assert!(tokio::time::Instant::now() < deadline, "timed out waiting for a follower");
        tokio::time::sleep(POLL_INTERVAL).await;
    };

    let server = TestServer::start(Some(Arc::clone(&handles[follower]))).await;
    let client = connect(&server).await;

    let err = client.execute("INSERT INTO ext_redirect VALUES (1)", &[]).await.unwrap_err();
    assert_eq!(err.as_db_error().expect("db error").code(), &SqlState::READ_ONLY_SQL_TRANSACTION);
}

// ---------------------------------------------------------------------------
// HTTP /health endpoint in replicated mode (#5393)
// ---------------------------------------------------------------------------

/// Bind the HTTP router (in replicated mode against `handle`) to an
/// ephemeral port and return its base URL plus a shutdown sender.
async fn start_http(handle: Option<Arc<ReplicationHandle>>) -> (String, oneshot::Sender<()>) {
    use vibesql_server::{http::create_http_router, registry::DatabaseRegistry};
    use vibesql_storage::Database;

    let listener = TokioTcpListener::bind("127.0.0.1:0").await.expect("bind http");
    let addr = listener.local_addr().unwrap();
    let (tx, rx) = oneshot::channel::<()>();

    let db = Arc::new(Database::new());
    let registry = DatabaseRegistry::new();
    let subs = Arc::new(SubscriptionManager::new());
    let app = create_http_router(db, registry, subs, None, handle);

    tokio::spawn(async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(async {
                let _ = rx.await;
            })
            .await
            .ok();
    });

    (format!("http://{addr}"), tx)
}

/// `/health` on the leader is 200 `ok` with role `"leader"` and
/// `can_serve_writes: true`; on a follower it is 503 `unavailable` with
/// role `"follower"` and the leader's id — the load-balancer routing
/// contract.
#[tokio::test]
async fn http_health_reports_leader_and_follower() {
    let (handles, _dir) = boot_cluster(3).await;
    let leader = wait_for_leader(&handles).await;

    // Leader: 200 OK, writable.
    let (base, _tx) = start_http(Some(Arc::clone(&handles[leader]))).await;
    let resp = reqwest::get(format!("{base}/health")).await.unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = serde_json::from_str(&resp.text().await.unwrap()).unwrap();
    assert_eq!(body["status"], "ok");
    assert_eq!(body["replication"]["role"], "leader");
    assert_eq!(body["replication"]["can_serve_writes"], true);

    // Follower: 503, not writable, leader id present.
    let deadline = tokio::time::Instant::now() + WAIT_TIMEOUT;
    let follower = loop {
        if let Some(i) = handles
            .iter()
            .position(|h| h.role() == Role::Follower && h.node().current_leader().is_some())
        {
            break i;
        }
        assert!(tokio::time::Instant::now() < deadline, "timed out waiting for a follower");
        tokio::time::sleep(POLL_INTERVAL).await;
    };
    let (base, _tx2) = start_http(Some(Arc::clone(&handles[follower]))).await;
    let resp = reqwest::get(format!("{base}/health")).await.unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
    let body: serde_json::Value = serde_json::from_str(&resp.text().await.unwrap()).unwrap();
    assert_eq!(body["status"], "unavailable");
    assert_eq!(body["replication"]["role"], "follower");
    assert_eq!(body["replication"]["can_serve_writes"], false);
    assert_eq!(body["replication"]["leader_id"], handles[leader].node_id());
}

/// In replicated mode the HTTP query/CRUD/subscription surface is gated to
/// a clear 503 (it would otherwise execute against the unreplicated local
/// database) — never silent wrong behavior.
#[tokio::test]
async fn http_query_surface_gated_in_replicated_mode() {
    let (handles, _dir) = boot_cluster(1).await;
    wait_for_leader(&handles).await;
    let (base, _tx) = start_http(Some(Arc::clone(&handles[0]))).await;

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base}/api/query"))
        .header("content-type", "application/json")
        .body(r#"{"sql":"SELECT 1"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);

    // CRUD writes are gated too.
    let resp = client
        .post(format!("{base}/api/tables/t/rows"))
        .header("content-type", "application/json")
        .body(r#"{"id":1}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);
}

/// Standalone HTTP is unaffected: `/health` is 200 with no `replication`
/// block and the query surface works (regression guard for the optional
/// wiring).
#[tokio::test]
async fn http_standalone_unaffected() {
    let (base, _tx) = start_http(None).await;
    let resp = reqwest::get(format!("{base}/health")).await.unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = serde_json::from_str(&resp.text().await.unwrap()).unwrap();
    assert_eq!(body["status"], "ok");
    assert!(body.get("replication").is_none() || body["replication"].is_null());

    let client = reqwest::Client::new();
    let resp = client
        .post(format!("{base}/api/query"))
        .header("content-type", "application/json")
        .body(r#"{"sql":"SELECT 1"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
}
