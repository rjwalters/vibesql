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

/// The resolved column names of a SELECT result (#5427 parity checks).
fn select_columns(result: ExecutionResult) -> Vec<String> {
    match result {
        ExecutionResult::Select { columns, .. } => {
            columns.into_iter().map(|c| c.name).collect()
        }
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

/// Replicated SELECT results carry the **real** column names — the same
/// labels the standalone executor produces — not the `col0`, `col1`, …
/// placeholders the consensus read path used to force (#5427). Standalone
/// is the correctness oracle: the replicated read must label its columns
/// identically for simple columns, aliases, expressions, `SELECT *`,
/// aggregates, and joins.
#[tokio::test]
async fn replicated_select_column_names_match_standalone() {
    // Schema + data, applied to both a standalone session and a
    // replicated single-node leader so the two answer the same queries.
    let setup = [
        "CREATE TABLE t (a INT, b INT)",
        "CREATE TABLE u (a INT, c VARCHAR(10))",
        "INSERT INTO t VALUES (1, 10)",
        "INSERT INTO t VALUES (2, 20)",
        "INSERT INTO u VALUES (1, 'one')",
        "INSERT INTO u VALUES (2, 'two')",
    ];

    // The SELECTs whose column labels must match. Each exercises a
    // different name-resolution path.
    let selects = [
        "SELECT a, b FROM t ORDER BY a",            // simple columns
        "SELECT a AS x, b AS y FROM t ORDER BY a",  // aliases
        "SELECT a + 1, b * 2 FROM t ORDER BY a",    // expressions
        "SELECT * FROM t ORDER BY a",               // wildcard expansion
        "SELECT count(*), sum(b) FROM t",           // aggregates
        "SELECT count(*) AS n FROM t",              // aggregate + alias
        "SELECT t.a, u.c FROM t JOIN u ON t.a = u.a ORDER BY t.a", // join
    ];

    // Standalone session is the oracle.
    let mut standalone =
        Session::new("testdb".to_string(), "testuser".to_string(), SharedDatabase::new(Database::new()));
    for sql in setup {
        standalone.execute(sql).await.expect("standalone setup");
    }

    // Replicated leader session.
    let (handles, _dir) = boot_cluster(1).await;
    wait_for_leader(&handles).await;
    let mut replicated = replicated_session(&handles[0]);
    for sql in setup {
        replicated.execute(sql).await.expect("replicated setup");
    }

    for sql in selects {
        let standalone_cols =
            select_columns(standalone.execute(sql).await.expect("standalone select"));
        let replicated_cols =
            select_columns(replicated.execute(sql).await.expect("replicated select"));

        // The placeholders the bug produced must be gone.
        assert!(
            !replicated_cols.iter().any(|name| name.starts_with("col")),
            "{sql}: replicated columns must not be col0/col1 placeholders, got {replicated_cols:?}",
        );
        // And the replicated labels must match the standalone oracle.
        assert_eq!(
            replicated_cols, standalone_cols,
            "{sql}: replicated column names must match standalone",
        );
    }

    // Spot-check the resolved names so a regression in BOTH paths cannot
    // pass silently by agreeing on placeholders.
    let aliased = select_columns(
        replicated
            .execute("SELECT a AS x, b AS y FROM t ORDER BY a")
            .await
            .expect("replicated aliased select"),
    );
    assert_eq!(aliased, vec!["x".to_string(), "y".to_string()]);
    let star =
        select_columns(replicated.execute("SELECT * FROM t ORDER BY a").await.expect("star select"));
    assert_eq!(star, vec!["a".to_string(), "b".to_string()]);
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
// Session-level: EXECUTE of a prepared statement inside a transaction (#5414)
// ---------------------------------------------------------------------------

/// A PREPARE'd INSERT EXECUTE'd inside an open replicated transaction buffers
/// into the batch exactly like a simple-query write: the read-your-own-writes
/// path sees it mid-transaction, nothing is proposed until COMMIT, and the
/// whole transaction lands as a **single** consensus entry.
#[tokio::test]
async fn replicated_txn_prepared_execute_buffers_into_batch() {
    let (handles, _dir) = boot_cluster(1).await;
    wait_for_leader(&handles).await;
    let mut session = replicated_session(&handles[0]);

    session.execute("CREATE TABLE users (id INT, name VARCHAR(100))").await.unwrap();
    session.execute("PREPARE ins FROM 'INSERT INTO users VALUES (?, ?)'").await.unwrap();
    let applied_before = handles[0].node().last_applied();

    session.execute("BEGIN").await.unwrap();
    // A prepared EXECUTE inside the txn acks optimistically (rows = 0) and
    // buffers like a plain INSERT — it is not refused.
    let r = session.execute("EXECUTE ins (1, 'Alice')").await.unwrap();
    assert!(matches!(r, ExecutionResult::Insert { rows_affected: 0 }), "{r:?}");
    let r = session.execute("EXECUTE ins USING 2, 'Bob'").await.unwrap();
    assert!(matches!(r, ExecutionResult::Insert { rows_affected: 0 }), "{r:?}");

    // Nothing applied yet: the buffer is not proposed until COMMIT.
    assert_eq!(handles[0].node().last_applied(), applied_before, "buffer must not apply early");

    // Read-your-own-writes: the buffered prepared INSERTs are visible mid-txn,
    // with the quoted string preserved (no quote-escaping breakage).
    let rows = select_rows(session.execute("SELECT id, name FROM users ORDER BY id").await.unwrap());
    assert_eq!(rows.len(), 2, "read-your-own-writes: buffered prepared INSERTs visible");
    assert_eq!(rows[1].values[1].to_string(), "Bob");

    session.execute("COMMIT").await.unwrap();
    // The whole transaction — both prepared writes — committed as one entry.
    assert_eq!(
        handles[0].node().last_applied(),
        applied_before + 1,
        "the transaction must be a single log entry"
    );
    let rows = handles[0].node().query("SELECT id, name FROM users ORDER BY id").unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[1][1].to_string(), "Bob");
}

/// ROLLBACK discards a prepared EXECUTE buffered inside the transaction: no
/// log index is consumed and the prepared write never lands. The named
/// statement survives (PREPARE is session-local), so a later autocommit
/// EXECUTE still works.
#[tokio::test]
async fn replicated_txn_prepared_execute_rolled_back_discards_write() {
    let (handles, _dir) = boot_cluster(1).await;
    wait_for_leader(&handles).await;
    let mut session = replicated_session(&handles[0]);

    session.execute("CREATE TABLE t (id INT)").await.unwrap();
    session.execute("PREPARE ins FROM 'INSERT INTO t VALUES (?)'").await.unwrap();
    let applied_before = handles[0].node().last_applied();

    session.execute("BEGIN").await.unwrap();
    session.execute("EXECUTE ins (1)").await.unwrap();
    assert!(matches!(session.execute("ROLLBACK").await.unwrap(), ExecutionResult::Rollback));

    // No entry consumed, no row landed.
    assert_eq!(handles[0].node().last_applied(), applied_before, "rollback must not propose");
    let rows = select_rows(session.execute("SELECT id FROM t").await.unwrap());
    assert!(rows.is_empty(), "rolled-back prepared write must not be visible");

    // The named statement survived; an autocommit EXECUTE proposes immediately.
    let r = session.execute("EXECUTE ins (2)").await.unwrap();
    assert!(matches!(r, ExecutionResult::Insert { rows_affected: 1 }), "{r:?}");
    assert_eq!(handles[0].node().last_applied(), applied_before + 1);
}

/// A prepared INSERT of a volatile value (`random()`) EXECUTE'd inside a
/// transaction freezes once at buffer time: the mid-transaction read and the
/// committed row carry the same value — same freeze-at-buffer contract as a
/// simple-query volatile write.
#[tokio::test]
async fn replicated_txn_prepared_execute_volatile_value_is_frozen() {
    let (handles, _dir) = boot_cluster(1).await;
    wait_for_leader(&handles).await;
    let mut session = replicated_session(&handles[0]);

    session.execute("CREATE TABLE t (id INT, r INT)").await.unwrap();
    // The volatile call lives in the prepared text; only `id` is a parameter.
    session.execute("PREPARE ins FROM 'INSERT INTO t VALUES (?, abs(random()))'").await.unwrap();

    session.execute("BEGIN").await.unwrap();
    session.execute("EXECUTE ins (1)").await.unwrap();
    let rows = select_rows(session.execute("SELECT r FROM t WHERE id = 1").await.unwrap());
    assert_eq!(rows.len(), 1);
    let mid_txn_value = rows[0].values[0].clone();

    session.execute("COMMIT").await.unwrap();
    let rows = select_rows(session.execute("SELECT r FROM t WHERE id = 1").await.unwrap());
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].values[0], mid_txn_value,
        "a prepared volatile write must freeze once: committed value == mid-txn value"
    );
}

/// A prepared EXECUTE buffered inside a transaction replicates atomically with
/// the rest of the batch: after COMMIT on the leader, every follower converges
/// to **all** of the transaction's rows (the prepared write is part of the one
/// all-or-nothing entry).
#[tokio::test]
async fn replicated_txn_prepared_execute_replicates_atomically_to_followers() {
    let (handles, _dir) = boot_cluster(3).await;
    let leader = wait_for_leader(&handles).await;
    let mut session = replicated_session(&handles[leader]);

    session.execute("CREATE TABLE kv (id INT, v INT)").await.unwrap();
    session.execute("PREPARE ins FROM 'INSERT INTO kv VALUES (?, ?)'").await.unwrap();
    session.execute("BEGIN").await.unwrap();
    // Mix a prepared EXECUTE with a simple-query write in the same batch.
    session.execute("EXECUTE ins (1, 10)").await.unwrap();
    session.execute("INSERT INTO kv VALUES (2, 20)").await.unwrap();
    session.execute("EXECUTE ins (3, 30)").await.unwrap();
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

/// A prepared EXECUTE buffered inside a transaction on a follower fails fast
/// with SQLSTATE 25006 (the leader-redirect contract): freezing a buffered
/// write is leader-only, so the prepared path is not a bypass. The
/// transaction stays open (nothing buffered) and ROLLBACK cleanly closes it.
#[tokio::test]
async fn replicated_txn_prepared_execute_on_follower_redirects() {
    let (handles, _dir) = boot_cluster(3).await;
    let leader = wait_for_leader(&handles).await;

    replicated_session(&handles[leader]).execute("CREATE TABLE t (id INT)").await.unwrap();

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
    session.execute("PREPARE ins FROM 'INSERT INTO t VALUES (?)'").await.unwrap();

    // BEGIN succeeds (session-local), but the buffered prepared write fails
    // fast: freezing is leader-only, so the follower surfaces 25006.
    session.execute("BEGIN").await.unwrap();
    let err = session.execute("EXECUTE ins (1)").await.unwrap_err();
    assert_eq!(sql_error(&err).code, "25006", "buffered prepared write must redirect like a write");

    // The transaction is still open (nothing buffered); ROLLBACK closes it.
    assert!(matches!(session.execute("ROLLBACK").await.unwrap(), ExecutionResult::Rollback));
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

    // The wire RowDescription carries the real column names, not the
    // col0/col1 placeholders the replicated read path used to force
    // (#5427).
    let col_names: Vec<&str> = rows[0].columns().iter().map(|c| c.name()).collect();
    assert_eq!(col_names, vec!["id", "name"]);

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

/// In replicated mode the HTTP `/api/query` endpoint routes writes through
/// consensus (#5410): a `POST /api/query` INSERT on the leader lands in the
/// consensus state machine and a subsequent SELECT reads it back from there
/// (not the unreplicated local database).
#[tokio::test]
async fn http_query_writes_route_through_consensus() {
    let (handles, _dir) = boot_cluster(1).await;
    wait_for_leader(&handles).await;
    let (base, _tx) = start_http(Some(Arc::clone(&handles[0]))).await;
    let client = reqwest::Client::new();

    let post_sql = |sql: &str| {
        let client = client.clone();
        let base = base.clone();
        let body = serde_json::json!({ "sql": sql }).to_string();
        async move {
            client
                .post(format!("{base}/api/query"))
                .header("content-type", "application/json")
                .body(body)
                .send()
                .await
                .unwrap()
        }
    };

    // DDL + DML route through consensus.
    assert!(post_sql("CREATE TABLE http_t (id INT, name VARCHAR(100))")
        .await
        .status()
        .is_success());
    let resp = post_sql("INSERT INTO http_t VALUES (1, 'Alice')").await;
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    // The write is in the consensus state machine, not the local database.
    assert_eq!(
        handles[0].node().query("SELECT COUNT(*) FROM http_t").unwrap()[0][0].to_string(),
        "1"
    );

    // A SELECT reads it back through the replicated state machine.
    let resp = post_sql("SELECT id, name FROM http_t").await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = serde_json::from_str(&resp.text().await.unwrap()).unwrap();
    assert_eq!(body["row_count"], 1, "the replicated read must see the row");

    // The JSON response carries the real column names, not col0/col1
    // placeholders — a REST/JSON consumer keys on `id`/`name` (#5427).
    assert_eq!(
        body["columns"],
        serde_json::json!(["id", "name"]),
        "replicated HTTP SELECT must return real column names, got {body}",
    );
    // An aliased + expression SELECT resolves the same labels standalone
    // would (alias wins; the expression gets its derived name).
    let resp = post_sql("SELECT id AS pk, id + 1 FROM http_t").await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = serde_json::from_str(&resp.text().await.unwrap()).unwrap();
    let cols = body["columns"].as_array().expect("columns array");
    assert_eq!(cols[0], "pk", "alias must be the JSON key, got {body}");
    assert!(
        !cols[1].as_str().unwrap().starts_with("col"),
        "expression column must not be a col1 placeholder, got {body}",
    );
}

/// A deterministic SQL rejection over `/api/query` in replicated mode (one
/// that every replica rejects identically, e.g. inserting into a missing
/// table) surfaces as HTTP 400 — not a 503/421 consensus refusal (#5410).
#[tokio::test]
async fn http_query_deterministic_rejection_is_400() {
    let (handles, _dir) = boot_cluster(1).await;
    wait_for_leader(&handles).await;
    let (base, _tx) = start_http(Some(Arc::clone(&handles[0]))).await;
    let client = reqwest::Client::new();

    let resp = client
        .post(format!("{base}/api/query"))
        .header("content-type", "application/json")
        .body(r#"{"sql":"INSERT INTO no_such_table VALUES (1)"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::BAD_REQUEST);
}

/// A CRUD collection write (`POST /api/tables/{t}/rows`) routes through
/// consensus in replicated mode (#5410): the row lands in the state machine.
#[tokio::test]
async fn http_crud_create_routes_through_consensus() {
    let (handles, _dir) = boot_cluster(1).await;
    wait_for_leader(&handles).await;
    let (base, _tx) = start_http(Some(Arc::clone(&handles[0]))).await;
    let client = reqwest::Client::new();

    // Schema via /api/query (also replicated).
    let resp = client
        .post(format!("{base}/api/query"))
        .header("content-type", "application/json")
        .body(r#"{"sql":"CREATE TABLE crud_t (id INT, name VARCHAR(100))"}"#)
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // CRUD create proposes through consensus.
    let resp = client
        .post(format!("{base}/api/tables/crud_t/rows"))
        .header("content-type", "application/json")
        .body(r#"{"id":7,"name":"Carol"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::CREATED);

    // The row is in the consensus state machine.
    assert_eq!(
        handles[0].node().query("SELECT name FROM crud_t WHERE id = 7").unwrap()[0][0].to_string(),
        "Carol"
    );
}

/// An HTTP write on a **follower** is refused with `421 Misdirected Request`
/// carrying the leader hint in the `X-VibeSQL-Leader` header — the HTTP
/// equivalent of the wire path's NOT_LEADER redirect (#5410). It must never
/// be executed against the follower's local database.
#[tokio::test]
async fn http_write_on_follower_redirects_with_leader_hint() {
    let (handles, _dir) = boot_cluster(3).await;
    let leader = wait_for_leader(&handles).await;

    // Create the table through the leader.
    replicated_session(&handles[leader]).execute("CREATE TABLE redir (id INT)").await.unwrap();
    let leader_id = handles[leader].node_id();

    // Pick a follower that knows who leads (so the hint is populated).
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

    let (base, _tx) = start_http(Some(Arc::clone(&handles[follower]))).await;
    let client = reqwest::Client::new();

    // /api/query write on the follower → 421 + leader hint header.
    let resp = client
        .post(format!("{base}/api/query"))
        .header("content-type", "application/json")
        .body(r#"{"sql":"INSERT INTO redir VALUES (1)"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::MISDIRECTED_REQUEST);
    let leader_hint = resp
        .headers()
        .get("X-VibeSQL-Leader")
        .expect("leader-hint header")
        .to_str()
        .unwrap()
        .to_string();
    assert!(leader_hint.contains(&format!("node {leader_id}")), "{leader_hint}");
    let body: serde_json::Value = serde_json::from_str(&resp.text().await.unwrap()).unwrap();
    assert_eq!(body["code"], "25006", "the body carries the NOT_LEADER SQLSTATE");

    // CRUD create on the follower → same 421 contract.
    let resp = client
        .post(format!("{base}/api/tables/redir/rows"))
        .header("content-type", "application/json")
        .body(r#"{"id":2}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::MISDIRECTED_REQUEST);
}

/// The CRUD by-id endpoints (GET/PUT/PATCH/DELETE one row) work in replicated
/// mode (#5420): the primary key is resolved from the consensus state machine's
/// catalog (the local registry DB is empty), reads run against the replicated
/// state machine, and writes propose through consensus. A by-id GET returns the
/// row with its real column names (post-#5428).
#[tokio::test]
async fn http_crud_by_id_routes_through_consensus() {
    let (handles, _dir) = boot_cluster(1).await;
    wait_for_leader(&handles).await;
    let (base, _tx) = start_http(Some(Arc::clone(&handles[0]))).await;
    let client = reqwest::Client::new();

    // Schema + seed row via the (already replicated) /api/query endpoint.
    for sql in [
        "CREATE TABLE byid (id INTEGER PRIMARY KEY, name VARCHAR(100))",
        "INSERT INTO byid VALUES (1, 'Alice')",
    ] {
        let resp = client
            .post(format!("{base}/api/query"))
            .header("content-type", "application/json")
            .body(serde_json::json!({ "sql": sql }).to_string())
            .send()
            .await
            .unwrap();
        assert!(resp.status().is_success(), "{sql}: {}", resp.status());
    }

    // GET by id resolves the PK through consensus and reads the row back from
    // the replicated state machine, with real column names (#5428).
    let resp = client.get(format!("{base}/api/tables/byid/rows/1")).send().await.unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = serde_json::from_str(&resp.text().await.unwrap()).unwrap();
    assert_eq!(body["data"]["id"], 1, "by-id GET must read the replicated row, got {body}");
    assert_eq!(body["data"]["name"], "Alice", "real column name (post-#5428), got {body}");

    // GET a missing id → 404.
    let resp = client.get(format!("{base}/api/tables/byid/rows/999")).send().await.unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);

    // PUT by id proposes an UPDATE through consensus.
    let resp = client
        .put(format!("{base}/api/tables/byid/rows/1"))
        .header("content-type", "application/json")
        .body(r#"{"name":"Alicia"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert_eq!(
        handles[0].node().query("SELECT name FROM byid WHERE id = 1").unwrap()[0][0].to_string(),
        "Alicia",
        "the PUT must land in the consensus state machine"
    );

    // PUT a missing id → 404 (zero rows affected).
    let resp = client
        .put(format!("{base}/api/tables/byid/rows/999"))
        .header("content-type", "application/json")
        .body(r#"{"name":"Nobody"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);

    // PATCH by id proposes a partial UPDATE through consensus.
    let resp = client
        .patch(format!("{base}/api/tables/byid/rows/1"))
        .header("content-type", "application/json")
        .body(r#"{"name":"Allie"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert_eq!(
        handles[0].node().query("SELECT name FROM byid WHERE id = 1").unwrap()[0][0].to_string(),
        "Allie",
        "the PATCH must land in the consensus state machine"
    );

    // DELETE by id proposes a DELETE through consensus.
    let resp = client.delete(format!("{base}/api/tables/byid/rows/1")).send().await.unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    assert_eq!(
        handles[0].node().query("SELECT COUNT(*) FROM byid").unwrap()[0][0].to_string(),
        "0",
        "the DELETE must land in the consensus state machine"
    );

    // DELETE a missing id → 404.
    let resp = client.delete(format!("{base}/api/tables/byid/rows/1")).send().await.unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::NOT_FOUND);
}

/// A CRUD by-id write replicates to followers (#5420): PUT/PATCH/DELETE on the
/// leader's HTTP port converge on a second node.
#[tokio::test]
async fn http_crud_by_id_writes_replicate_to_followers() {
    let (handles, _dir) = boot_cluster(3).await;
    let leader = wait_for_leader(&handles).await;
    let (base, _tx) = start_http(Some(Arc::clone(&handles[leader]))).await;
    let client = reqwest::Client::new();

    for sql in [
        "CREATE TABLE byid_repl (id INTEGER PRIMARY KEY, n INT)",
        "INSERT INTO byid_repl VALUES (1, 10)",
    ] {
        let resp = client
            .post(format!("{base}/api/query"))
            .header("content-type", "application/json")
            .body(serde_json::json!({ "sql": sql }).to_string())
            .send()
            .await
            .unwrap();
        assert!(resp.status().is_success(), "{sql}: {}", resp.status());
    }

    // PATCH the row by id on the leader's HTTP port.
    let resp = client
        .patch(format!("{base}/api/tables/byid_repl/rows/1"))
        .header("content-type", "application/json")
        .body(r#"{"n":42}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // Every follower converges to the updated value.
    let deadline = tokio::time::Instant::now() + WAIT_TIMEOUT;
    for (i, h) in handles.iter().enumerate() {
        if i == leader {
            continue;
        }
        loop {
            let n = h
                .node()
                .query("SELECT n FROM byid_repl WHERE id = 1")
                .ok()
                .and_then(|r| r.first().and_then(|row| row.first()).map(ToString::to_string));
            if n.as_deref() == Some("42") {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "follower {i} did not converge to n=42 (saw {n:?})"
            );
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }
}

/// A CRUD by-id write on a **follower** is refused with `421 Misdirected
/// Request` carrying the leader hint — the same redirect contract as the
/// collection endpoints (#5420). It must never execute against the follower's
/// local database (the split-brain invariant).
#[tokio::test]
async fn http_crud_by_id_write_on_follower_redirects_with_leader_hint() {
    let (handles, _dir) = boot_cluster(3).await;
    let leader = wait_for_leader(&handles).await;

    // Create + seed the table through the leader.
    for sql in [
        "CREATE TABLE byid_redir (id INTEGER PRIMARY KEY, name VARCHAR(100))",
        "INSERT INTO byid_redir VALUES (1, 'Alice')",
    ] {
        replicated_session(&handles[leader]).execute(sql).await.unwrap();
    }
    let leader_id = handles[leader].node_id();

    // Pick a follower that knows who leads (so the hint is populated) and that
    // has applied the seed row (so the resolution sees the table).
    let deadline = tokio::time::Instant::now() + WAIT_TIMEOUT;
    let follower = loop {
        if let Some(i) = handles.iter().position(|h| {
            h.role() == Role::Follower
                && h.node().current_leader().is_some()
                && h.primary_key_column("byid_redir").as_deref() == Some("id")
        }) {
            break i;
        }
        assert!(tokio::time::Instant::now() < deadline, "timed out waiting for a follower");
        tokio::time::sleep(POLL_INTERVAL).await;
    };

    let (base, _tx) = start_http(Some(Arc::clone(&handles[follower]))).await;
    let client = reqwest::Client::new();

    // PUT by id on the follower → 421 + leader hint header (NOT_LEADER).
    let resp = client
        .put(format!("{base}/api/tables/byid_redir/rows/1"))
        .header("content-type", "application/json")
        .body(r#"{"name":"Bob"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::MISDIRECTED_REQUEST);
    let leader_hint = resp
        .headers()
        .get("X-VibeSQL-Leader")
        .expect("leader-hint header")
        .to_str()
        .unwrap()
        .to_string();
    assert!(leader_hint.contains(&format!("node {leader_id}")), "{leader_hint}");

    // DELETE by id on the follower → same 421 contract.
    let resp = client.delete(format!("{base}/api/tables/byid_redir/rows/1")).send().await.unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::MISDIRECTED_REQUEST);

    // The follower's local database never executed the write: the row is
    // untouched in the replicated state machine.
    assert_eq!(
        handles[follower].node().query("SELECT name FROM byid_redir WHERE id = 1").unwrap()[0][0]
            .to_string(),
        "Alice",
        "a by-id write on a follower must not mutate state outside consensus"
    );
}

/// An HTTP write replicates to a second node (#5410): after a `POST /api/query`
/// INSERT on the leader's HTTP port, every follower converges to the row.
#[tokio::test]
async fn http_write_replicates_to_followers() {
    let (handles, _dir) = boot_cluster(3).await;
    let leader = wait_for_leader(&handles).await;
    let (base, _tx) = start_http(Some(Arc::clone(&handles[leader]))).await;
    let client = reqwest::Client::new();

    for sql in [
        "CREATE TABLE repl_http (id INT)",
        "INSERT INTO repl_http VALUES (1)",
        "INSERT INTO repl_http VALUES (2)",
    ] {
        let resp = client
            .post(format!("{base}/api/query"))
            .header("content-type", "application/json")
            .body(serde_json::json!({ "sql": sql }).to_string())
            .send()
            .await
            .unwrap();
        assert!(resp.status().is_success(), "{sql}: {}", resp.status());
    }

    // Every follower converges to both rows.
    let deadline = tokio::time::Instant::now() + WAIT_TIMEOUT;
    for (i, h) in handles.iter().enumerate() {
        if i == leader {
            continue;
        }
        loop {
            let count = h
                .node()
                .query("SELECT COUNT(*) FROM repl_http")
                .ok()
                .and_then(|r| r.first().and_then(|row| row.first()).map(ToString::to_string));
            if count.as_deref() == Some("2") {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "follower {i} did not converge to 2 rows (saw {count:?})"
            );
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }
}

/// In replicated mode the subscription and blob-storage surfaces remain gated
/// to a clear 503 (they depend on local-database change-feed/state
/// introspection that is not yet wired through consensus — follow-ons to
/// #5410). They must never silently execute against the local database. The
/// CRUD by-id endpoints (#5420) and the GraphQL endpoint (#5421) are no longer
/// gated — they route through consensus; see `http_crud_by_id_*` and
/// `http_graphql_*` below.
#[tokio::test]
async fn http_unwired_surfaces_still_gated_in_replicated_mode() {
    let (handles, _dir) = boot_cluster(1).await;
    wait_for_leader(&handles).await;
    let (base, _tx) = start_http(Some(Arc::clone(&handles[0]))).await;
    let client = reqwest::Client::new();

    // Subscriptions.
    let resp = client.get(format!("{base}/api/subscribe?query=SELECT%201")).send().await.unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);

    // Blob storage.
    let resp = client.get(format!("{base}/api/storage/anything")).send().await.unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::SERVICE_UNAVAILABLE);

    // GraphQL is no longer gated — an introspection query succeeds (200)
    // against the replicated catalog rather than returning the old 503.
    let resp = client
        .post(format!("{base}/api/graphql"))
        .header("content-type", "application/json")
        .body(r#"{"query":"{ __schema { types { name } } }"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
}

/// In replicated mode a GraphQL **mutation** routes through consensus and a
/// GraphQL **query** reads it back from the replicated state machine (#5421):
/// the inserted row lands in the consensus state machine (not the unreplicated
/// local database) and a subsequent GraphQL query observes it.
#[tokio::test]
async fn http_graphql_mutation_and_query_route_through_consensus() {
    let (handles, _dir) = boot_cluster(1).await;
    wait_for_leader(&handles).await;
    let (base, _tx) = start_http(Some(Arc::clone(&handles[0]))).await;
    let client = reqwest::Client::new();

    let post_graphql = |query: &str| {
        let client = client.clone();
        let base = base.clone();
        let body = serde_json::json!({ "query": query }).to_string();
        async move {
            client
                .post(format!("{base}/api/graphql"))
                .header("content-type", "application/json")
                .body(body)
                .send()
                .await
                .unwrap()
        }
    };

    // Create the table through the SQL surface (DDL has no GraphQL form).
    let resp = client
        .post(format!("{base}/api/query"))
        .header("content-type", "application/json")
        .body(r#"{"sql":"CREATE TABLE gql_t (id INT, name VARCHAR(100))"}"#)
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // GraphQL INSERT mutation → proposes through consensus (200).
    let resp = post_graphql(
        r#"mutation { insertInto(table: "gql_t", values: {"id": 1, "name": "Alice"}) }"#,
    )
    .await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // The write is in the consensus state machine, not the local database.
    assert_eq!(
        handles[0].node().query("SELECT COUNT(*) FROM gql_t").unwrap()[0][0].to_string(),
        "1",
        "a GraphQL mutation must land in the replicated state machine"
    );

    // A GraphQL query reads it back through the replicated state machine.
    let resp = post_graphql(r#"{ gql_t { id name } }"#).await;
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = serde_json::from_str(&resp.text().await.unwrap()).unwrap();
    let rows = body["data"]["data"].as_array().expect("data.data array");
    assert_eq!(rows.len(), 1, "the replicated GraphQL query must see the row: {body}");
    assert_eq!(rows[0]["name"], "Alice", "{body}");
}

/// A GraphQL mutation replicates to every node (#5421): after an INSERT on the
/// leader's HTTP GraphQL port, every follower converges to the row.
#[tokio::test]
async fn http_graphql_mutation_replicates_to_followers() {
    let (handles, _dir) = boot_cluster(3).await;
    let leader = wait_for_leader(&handles).await;
    let (base, _tx) = start_http(Some(Arc::clone(&handles[leader]))).await;
    let client = reqwest::Client::new();

    // Create + seed through the leader's SQL surface.
    let resp = client
        .post(format!("{base}/api/query"))
        .header("content-type", "application/json")
        .body(r#"{"sql":"CREATE TABLE gql_repl (id INT, name VARCHAR(100))"}"#)
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    // GraphQL INSERT mutation on the leader.
    let resp = client
        .post(format!("{base}/api/graphql"))
        .header("content-type", "application/json")
        .body(
            serde_json::json!({
                "query": r#"mutation { insertInto(table: "gql_repl", values: {"id": 7, "name": "Bob"}) }"#
            })
            .to_string(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // Every follower converges to the row.
    let deadline = tokio::time::Instant::now() + WAIT_TIMEOUT;
    for (i, h) in handles.iter().enumerate() {
        if i == leader {
            continue;
        }
        loop {
            let n = h
                .node()
                .query("SELECT name FROM gql_repl WHERE id = 7")
                .ok()
                .and_then(|r| r.first().and_then(|row| row.first()).map(ToString::to_string));
            if n.as_deref() == Some("Bob") {
                break;
            }
            assert!(
                tokio::time::Instant::now() < deadline,
                "follower {i} did not converge to the GraphQL-inserted row (saw {n:?})"
            );
            tokio::time::sleep(POLL_INTERVAL).await;
        }
    }
}

/// A GraphQL mutation on a **follower** is refused with `421 Misdirected
/// Request` carrying the leader hint, and is never executed against the
/// follower's local database (the split-brain invariant, #5421). The redirect
/// contract matches the SQL surfaces; the body is a GraphQL `errors` array.
#[tokio::test]
async fn http_graphql_mutation_on_follower_redirects_with_leader_hint() {
    let (handles, _dir) = boot_cluster(3).await;
    let leader = wait_for_leader(&handles).await;

    // Create + seed the table through the leader.
    for sql in [
        "CREATE TABLE gql_redir (id INT, name VARCHAR(100))",
        "INSERT INTO gql_redir VALUES (1, 'Alice')",
    ] {
        replicated_session(&handles[leader]).execute(sql).await.unwrap();
    }
    let leader_id = handles[leader].node_id();

    // Pick a follower that knows who leads (so the hint is populated) and that
    // has applied the seed row (so introspection sees the table).
    let deadline = tokio::time::Instant::now() + WAIT_TIMEOUT;
    let follower = loop {
        if let Some(i) = handles.iter().position(|h| {
            h.role() == Role::Follower
                && h.node().current_leader().is_some()
                && h.schema_snapshot().contains_key("gql_redir")
        }) {
            break i;
        }
        assert!(tokio::time::Instant::now() < deadline, "timed out waiting for a follower");
        tokio::time::sleep(POLL_INTERVAL).await;
    };

    let (base, _tx) = start_http(Some(Arc::clone(&handles[follower]))).await;
    let client = reqwest::Client::new();

    // GraphQL UPDATE mutation on the follower → 421 + leader hint header.
    let resp = client
        .post(format!("{base}/api/graphql"))
        .header("content-type", "application/json")
        .body(
            serde_json::json!({
                "query": r#"mutation { update(table: "gql_redir", values: {"name": "Bob"}, where: {"id": {"eq": 1}}) }"#
            })
            .to_string(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::MISDIRECTED_REQUEST);
    let leader_hint = resp
        .headers()
        .get("X-VibeSQL-Leader")
        .expect("leader-hint header")
        .to_str()
        .unwrap()
        .to_string();
    assert!(leader_hint.contains(&format!("node {leader_id}")), "{leader_hint}");

    // The response body is a valid GraphQL error envelope.
    let body: serde_json::Value = serde_json::from_str(&resp.text().await.unwrap()).unwrap();
    assert!(body["errors"].is_array(), "expected a GraphQL errors array, got {body}");

    // The follower's local state machine never executed the write.
    assert_eq!(
        handles[follower].node().query("SELECT name FROM gql_redir WHERE id = 1").unwrap()[0][0]
            .to_string(),
        "Alice",
        "a GraphQL mutation on a follower must not mutate state outside consensus"
    );
}

/// A GraphQL query string arg is parameterized, not interpolated, so a value
/// crafted to break out of the SQL string cannot inject (#5421). The malicious
/// value is bound as a literal and simply matches no row.
#[tokio::test]
async fn http_graphql_string_arg_is_injection_safe() {
    let (handles, _dir) = boot_cluster(1).await;
    wait_for_leader(&handles).await;
    let (base, _tx) = start_http(Some(Arc::clone(&handles[0]))).await;
    let client = reqwest::Client::new();

    for sql in [
        "CREATE TABLE gql_inj (id INT, name VARCHAR(100))",
        "INSERT INTO gql_inj VALUES (1, 'Alice')",
    ] {
        let resp = client
            .post(format!("{base}/api/query"))
            .header("content-type", "application/json")
            .body(serde_json::json!({ "sql": sql }).to_string())
            .send()
            .await
            .unwrap();
        assert!(resp.status().is_success(), "{sql}: {}", resp.status());
    }

    // A classic injection payload in a GraphQL `where` string equality arg.
    // If naively interpolated this would become `WHERE name = '' OR '1'='1'`
    // and return every row; parameterized, it matches the literal string and
    // returns nothing.
    let payload = "' OR '1'='1";
    let query = serde_json::json!({
        "query": format!(r#"{{ gql_inj(where: {{"name": {{"eq": "{payload}"}}}}) {{ id name }} }}"#)
    })
    .to_string();
    let resp = client
        .post(format!("{base}/api/graphql"))
        .header("content-type", "application/json")
        .body(query)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = serde_json::from_str(&resp.text().await.unwrap()).unwrap();
    let rows = body["data"]["data"].as_array().expect("data.data array");
    assert!(
        rows.is_empty(),
        "an injection payload must be bound as a literal (match nothing), got {body}"
    );

    // The table is untouched — the real row is still present and queryable.
    assert_eq!(
        handles[0].node().query("SELECT COUNT(*) FROM gql_inj").unwrap()[0][0].to_string(),
        "1"
    );
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

    // GraphQL is unchanged standalone (#5421): a mutation + query round-trip
    // works against the local database (no consensus handle present).
    let resp = client
        .post(format!("{base}/api/query"))
        .header("content-type", "application/json")
        .body(r#"{"sql":"CREATE TABLE gql_std (id INT, name VARCHAR(100))"}"#)
        .send()
        .await
        .unwrap();
    assert!(resp.status().is_success());

    let resp = client
        .post(format!("{base}/api/graphql"))
        .header("content-type", "application/json")
        .body(
            serde_json::json!({
                "query": r#"mutation { insertInto(table: "gql_std", values: {"id": 1, "name": "Alice"}) }"#
            })
            .to_string(),
        )
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    let resp = client
        .post(format!("{base}/api/graphql"))
        .header("content-type", "application/json")
        .body(r#"{"query":"{ gql_std { id name } }"}"#)
        .send()
        .await
        .unwrap();
    assert_eq!(resp.status(), reqwest::StatusCode::OK);
    let body: serde_json::Value = serde_json::from_str(&resp.text().await.unwrap()).unwrap();
    let rows = body["data"]["data"].as_array().expect("data.data array");
    assert_eq!(rows.len(), 1, "standalone GraphQL query must still work: {body}");
    assert_eq!(rows[0]["name"], "Alice", "{body}");
}
