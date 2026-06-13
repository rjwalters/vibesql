//! Tests for the PostgreSQL extended query protocol
//!
//! These tests verify that the server correctly handles Parse, Bind, Describe,
//! Execute, Sync, Flush, and Close messages.

mod common;

use tokio_postgres::NoTls;

use common::start_test_server;

/// Test basic extended query protocol flow: Parse -> Bind -> Execute -> Sync
#[tokio::test]
async fn test_extended_query_basic() {
    let server = start_test_server().await;

    // Connect using tokio-postgres (which uses extended query protocol by default)
    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=postgres dbname=test", server.addr().port()),
        NoTls,
    )
    .await
    .expect("Failed to connect");

    // Spawn connection task
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });

    // Create a table
    client
        .execute("CREATE TABLE test_table (id INTEGER, name TEXT)", &[])
        .await
        .expect("Failed to create table");

    // Insert data
    client
        .execute("INSERT INTO test_table VALUES (1, 'Alice')", &[])
        .await
        .expect("Failed to insert");

    // Query with extended protocol (client.query uses Parse/Bind/Execute/Sync)
    let rows = client.query("SELECT id, name FROM test_table", &[]).await.expect("Failed to query");

    assert_eq!(rows.len(), 1);

    server.shutdown();
}

/// Test extended query with multiple statements
#[tokio::test]
async fn test_extended_query_multiple_statements() {
    let server = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=postgres dbname=test", server.addr().port()),
        NoTls,
    )
    .await
    .expect("Failed to connect");

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });

    // Create a table
    client
        .execute("CREATE TABLE multi_test (id INTEGER, value TEXT)", &[])
        .await
        .expect("Failed to create table");

    // Insert multiple rows
    client
        .execute("INSERT INTO multi_test VALUES (1, 'one')", &[])
        .await
        .expect("Failed to insert");
    client
        .execute("INSERT INTO multi_test VALUES (2, 'two')", &[])
        .await
        .expect("Failed to insert");
    client
        .execute("INSERT INTO multi_test VALUES (3, 'three')", &[])
        .await
        .expect("Failed to insert");

    // Query without parameters
    let rows = client
        .query("SELECT id, value FROM multi_test WHERE id = 1", &[])
        .await
        .expect("Failed to query");

    assert_eq!(rows.len(), 1);

    // Query all rows
    let rows =
        client.query("SELECT id, value FROM multi_test", &[]).await.expect("Failed to query all");

    assert_eq!(rows.len(), 3);

    server.shutdown();
}

/// Test prepared statement reuse
#[tokio::test]
async fn test_prepared_statement_reuse() {
    let server = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=postgres dbname=test", server.addr().port()),
        NoTls,
    )
    .await
    .expect("Failed to connect");

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });

    // Create a table
    client
        .execute("CREATE TABLE prep_test (id INTEGER)", &[])
        .await
        .expect("Failed to create table");

    for i in 1..=5 {
        client
            .execute(&format!("INSERT INTO prep_test VALUES ({})", i), &[])
            .await
            .expect("Failed to insert");
    }

    // Use client.query which internally prepares and executes
    // Run multiple queries to test statement reuse
    for _ in 0..3 {
        let rows = client.query("SELECT id FROM prep_test", &[]).await.expect("Failed to query");
        assert_eq!(rows.len(), 5);
    }

    server.shutdown();
}

/// Test extended protocol error handling
#[tokio::test]
async fn test_extended_protocol_error() {
    let server = start_test_server().await;

    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=postgres dbname=test", server.addr().port()),
        NoTls,
    )
    .await
    .expect("Failed to connect");

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });

    // Query non-existent table should return error
    let result = client.query("SELECT * FROM nonexistent_table", &[]).await;

    assert!(result.is_err(), "Expected error for non-existent table");

    // Connection should still work after error
    client
        .execute("CREATE TABLE after_error (id INTEGER)", &[])
        .await
        .expect("Connection should still work after error");

    server.shutdown();
}

/// Connect a tokio-postgres client and spawn its connection task.
async fn connect_client(server: &common::TestServer) -> tokio_postgres::Client {
    let (client, connection) = tokio_postgres::connect(
        &format!("host=127.0.0.1 port={} user=postgres dbname=test", server.addr().port()),
        NoTls,
    )
    .await
    .expect("Failed to connect");
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Connection error: {}", e);
        }
    });
    client
}

/// Extended-protocol Describe must report the result column names in the
/// `RowDescription` *before* Execute. `tokio_postgres::Client::prepare`
/// issues Parse + Describe and exposes the RowDescription via
/// `Statement::columns()`, so this is a true wire-level assertion (#5429).
///
/// Before the fix, `SELECT *` / `table.*` resolved to `NoData` (no column
/// names) because the Describe path used a static AST that could not expand
/// wildcards. Now Describe expands `*` against the schema, matching what the
/// executed query returns.
#[tokio::test]
async fn test_describe_resolves_wildcard_and_explicit_columns() {
    let server = start_test_server().await;
    let client = connect_client(&server).await;

    client
        .execute("CREATE TABLE t (a INTEGER, b TEXT, c INTEGER)", &[])
        .await
        .expect("create table");

    // SELECT * -> Describe must list the table's actual columns in order.
    let stmt = client.prepare("SELECT * FROM t").await.expect("prepare SELECT *");
    let names: Vec<&str> = stmt.columns().iter().map(|c| c.name()).collect();
    assert_eq!(names, vec!["a", "b", "c"], "SELECT * Describe column names");

    // table.* -> same expansion.
    let stmt = client.prepare("SELECT t.* FROM t").await.expect("prepare SELECT t.*");
    let names: Vec<&str> = stmt.columns().iter().map(|c| c.name()).collect();
    assert_eq!(names, vec!["a", "b", "c"], "SELECT t.* Describe column names");

    // Explicit columns and an alias.
    let stmt =
        client.prepare("SELECT a, b AS x FROM t").await.expect("prepare explicit+alias");
    let names: Vec<&str> = stmt.columns().iter().map(|c| c.name()).collect();
    assert_eq!(names, vec!["a", "x"], "explicit + aliased Describe column names");

    server.shutdown();
}

/// Describe for a join wildcard must expand each table's columns in order:
/// `SELECT t1.*, t2.c` -> t1's columns followed by `c` (#5429).
#[tokio::test]
async fn test_describe_join_wildcard_columns() {
    let server = start_test_server().await;
    let client = connect_client(&server).await;

    client.execute("CREATE TABLE t1 (a INTEGER, b INTEGER)", &[]).await.expect("create t1");
    client.execute("CREATE TABLE t2 (id INTEGER, c TEXT)", &[]).await.expect("create t2");

    let stmt = client
        .prepare("SELECT t1.*, t2.c FROM t1 JOIN t2 ON t1.a = t2.id")
        .await
        .expect("prepare join wildcard");
    let names: Vec<&str> = stmt.columns().iter().map(|c| c.name()).collect();
    assert_eq!(names, vec!["a", "b", "c"], "join wildcard Describe column names");

    server.shutdown();
}

/// The Describe `RowDescription` must agree with what the executed
/// (simple-query / `client.query`) path returns for the same SELECT (#5429,
/// parity with #5428). We compare the prepared statement's column names
/// against the executed result's column names for each query shape.
#[tokio::test]
async fn test_describe_matches_executed_columns() {
    let server = start_test_server().await;
    let client = connect_client(&server).await;

    client
        .execute("CREATE TABLE t (a INTEGER, b TEXT, c INTEGER)", &[])
        .await
        .expect("create table");
    client.execute("INSERT INTO t VALUES (1, 'x', 2)", &[]).await.expect("insert");

    for sql in [
        "SELECT * FROM t",
        "SELECT t.* FROM t",
        "SELECT a, b FROM t",
        "SELECT a AS x, b AS y FROM t",
        "SELECT a + c AS sum FROM t",
    ] {
        // Describe (Parse + Describe) column names.
        let stmt = client.prepare(sql).await.unwrap_or_else(|e| panic!("prepare {sql}: {e}"));
        let described: Vec<String> =
            stmt.columns().iter().map(|c| c.name().to_string()).collect();

        // Executed result column names.
        let rows = client.query(sql, &[]).await.unwrap_or_else(|e| panic!("query {sql}: {e}"));
        let executed: Vec<String> =
            rows[0].columns().iter().map(|c| c.name().to_string()).collect();

        assert_eq!(
            described, executed,
            "Describe vs executed column names disagree for `{sql}`"
        );

        let is_placeholder = |n: &str| {
            n.strip_prefix("col").is_some_and(|rest| {
                !rest.is_empty() && rest.chars().all(|c| c.is_ascii_digit())
            })
        };
        assert!(
            !described.iter().any(|n| is_placeholder(n)),
            "Describe produced placeholder column names for `{sql}`: {described:?}"
        );
    }

    server.shutdown();
}
