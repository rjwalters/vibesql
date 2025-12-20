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
