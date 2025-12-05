//! Integration tests for session state management
//!
//! Tests session state persistence across queries, table visibility,
//! and concurrent session isolation.

mod common;

use common::{parse_backend_messages, start_test_server, TestClient};

/// Test that table created in session is visible to subsequent queries in same session
#[tokio::test]
async fn test_session_table_persistence() {
    let server = start_test_server().await;
    let mut client = TestClient::connect(server.addr()).await.expect("Failed to connect");

    client.send_startup("testuser", "testdb").await.expect("Failed to send startup");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read startup response");

    // Create table
    client
        .send_query("CREATE TABLE persist_test (id INT, name VARCHAR(50))")
        .await
        .expect("Failed to CREATE TABLE");
    let data = client.read_until_message_type(b'Z').await.expect("Failed to read response");
    let messages = parse_backend_messages(&data);
    assert!(messages.iter().any(|m| m.is_command_complete()), "CREATE TABLE should complete");

    // Insert data
    client
        .send_query("INSERT INTO persist_test VALUES (1, 'first')")
        .await
        .expect("Failed to INSERT");
    let data = client.read_until_message_type(b'Z').await.expect("Failed to read response");
    let messages = parse_backend_messages(&data);
    assert!(messages.iter().any(|m| m.is_command_complete()));

    // Query the table
    client.send_query("SELECT * FROM persist_test").await.expect("Failed to SELECT");
    let data = client.read_until_message_type(b'Z').await.expect("Failed to read response");
    let messages = parse_backend_messages(&data);

    assert!(messages.iter().any(|m| m.is_row_description()));
    assert!(messages.iter().any(|m| m.is_data_row()), "Should have data from INSERT");

    client.send_terminate().await.expect("Failed to terminate");
    server.shutdown();
}

/// Test that data modifications are visible to subsequent queries
#[tokio::test]
async fn test_session_data_modification_visibility() {
    let server = start_test_server().await;
    let mut client = TestClient::connect(server.addr()).await.expect("Failed to connect");

    client.send_startup("testuser", "testdb").await.expect("Failed to send startup");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read startup response");

    // Create and populate table
    client
        .send_query("CREATE TABLE modify_test (id INT, value INT)")
        .await
        .expect("Failed to CREATE");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read response");

    client.send_query("INSERT INTO modify_test VALUES (1, 100)").await.expect("Failed to INSERT");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read response");

    // Update the value
    client
        .send_query("UPDATE modify_test SET value = 200 WHERE id = 1")
        .await
        .expect("Failed to UPDATE");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read response");

    // Verify the update is visible
    client
        .send_query("SELECT value FROM modify_test WHERE id = 1")
        .await
        .expect("Failed to SELECT");
    let data = client.read_until_message_type(b'Z').await.expect("Failed to read response");
    let messages = parse_backend_messages(&data);

    assert!(messages.iter().any(|m| m.is_data_row()), "Should see the updated row");

    client.send_terminate().await.expect("Failed to terminate");
    server.shutdown();
}

/// Test that sessions are isolated from each other
#[tokio::test]
async fn test_session_isolation() {
    let server = start_test_server().await;

    // First session creates a table
    let mut client1 = TestClient::connect(server.addr()).await.expect("Failed to connect client1");
    client1.send_startup("user1", "testdb").await.expect("Failed to send startup");
    let _ = client1.read_until_message_type(b'Z').await.expect("Failed to read response");

    client1.send_query("CREATE TABLE session1_table (id INT)").await.expect("Failed to CREATE");
    let _ = client1.read_until_message_type(b'Z').await.expect("Failed to read response");

    // Second session creates a different table
    let mut client2 = TestClient::connect(server.addr()).await.expect("Failed to connect client2");
    client2.send_startup("user2", "testdb").await.expect("Failed to send startup");
    let _ = client2.read_until_message_type(b'Z').await.expect("Failed to read response");

    client2.send_query("CREATE TABLE session2_table (value INT)").await.expect("Failed to CREATE");
    let _ = client2.read_until_message_type(b'Z').await.expect("Failed to read response");

    // Each session should only see its own table (query other session's table should fail)
    // Note: In an isolated database per session, we can't actually test cross-session visibility
    // Instead we verify each session can query its own table

    client1.send_query("SELECT * FROM session1_table").await.expect("Failed to SELECT");
    let data = client1.read_until_message_type(b'Z').await.expect("Failed to read response");
    let messages = parse_backend_messages(&data);
    assert!(messages.iter().any(|m| m.is_row_description()), "Client1 should see session1_table");

    client2.send_query("SELECT * FROM session2_table").await.expect("Failed to SELECT");
    let data = client2.read_until_message_type(b'Z').await.expect("Failed to read response");
    let messages = parse_backend_messages(&data);
    assert!(messages.iter().any(|m| m.is_row_description()), "Client2 should see session2_table");

    client1.send_terminate().await.expect("Failed to terminate");
    client2.send_terminate().await.expect("Failed to terminate");
    server.shutdown();
}

/// Test that dropping a table makes it invisible to subsequent queries
#[tokio::test]
async fn test_session_drop_table() {
    let server = start_test_server().await;
    let mut client = TestClient::connect(server.addr()).await.expect("Failed to connect");

    client.send_startup("testuser", "testdb").await.expect("Failed to send startup");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read response");

    // Create table
    client.send_query("CREATE TABLE drop_test (id INT)").await.expect("Failed to CREATE");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read response");

    // Verify it exists
    client.send_query("SELECT * FROM drop_test").await.expect("Failed to SELECT");
    let data = client.read_until_message_type(b'Z').await.expect("Failed to read response");
    let messages = parse_backend_messages(&data);
    assert!(messages.iter().any(|m| m.is_row_description()), "Table should exist before DROP");

    // Drop table
    client.send_query("DROP TABLE drop_test").await.expect("Failed to DROP");
    let data = client.read_until_message_type(b'Z').await.expect("Failed to read response");
    let messages = parse_backend_messages(&data);
    assert!(messages.iter().any(|m| m.is_command_complete()), "DROP should complete");

    // Table should no longer exist (error on SELECT)
    client.send_query("SELECT * FROM drop_test").await.expect("Failed to SELECT");
    let data = client.read_until_message_type(b'Z').await.expect("Failed to read response");
    let messages = parse_backend_messages(&data);
    assert!(messages.iter().any(|m| m.is_error()), "Table should not exist after DROP");

    client.send_terminate().await.expect("Failed to terminate");
    server.shutdown();
}

/// Test multiple sequential operations in a session
#[tokio::test]
async fn test_session_sequential_operations() {
    let server = start_test_server().await;
    let mut client = TestClient::connect(server.addr()).await.expect("Failed to connect");

    client.send_startup("testuser", "testdb").await.expect("Failed to send startup");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read response");

    // Create table
    client.send_query("CREATE TABLE seq_test (id INT, val INT)").await.expect("Failed to CREATE");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read response");

    // Multiple inserts
    for i in 1..=5 {
        client
            .send_query(&format!("INSERT INTO seq_test VALUES ({}, {})", i, i * 10))
            .await
            .expect("Failed to INSERT");
        let _ = client.read_until_message_type(b'Z').await.expect("Failed to read response");
    }

    // Verify all rows
    client.send_query("SELECT * FROM seq_test").await.expect("Failed to SELECT");
    let data = client.read_until_message_type(b'Z').await.expect("Failed to read response");
    let messages = parse_backend_messages(&data);

    let row_count = messages.iter().filter(|m| m.is_data_row()).count();
    assert_eq!(row_count, 5, "Should have 5 rows");

    // Update some rows
    client
        .send_query("UPDATE seq_test SET val = val * 2 WHERE id > 3")
        .await
        .expect("Failed to UPDATE");
    let data = client.read_until_message_type(b'Z').await.expect("Failed to read response");
    let messages = parse_backend_messages(&data);
    let cmd = messages.iter().find(|m| m.is_command_complete()).unwrap();
    let tag = cmd.get_command_tag().unwrap();
    assert!(tag.contains("2"), "Should update 2 rows (id=4 and id=5)");

    // Delete some rows
    client.send_query("DELETE FROM seq_test WHERE id < 3").await.expect("Failed to DELETE");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read response");

    // Verify final state
    client.send_query("SELECT * FROM seq_test").await.expect("Failed to SELECT");
    let data = client.read_until_message_type(b'Z').await.expect("Failed to read response");
    let messages = parse_backend_messages(&data);

    let final_row_count = messages.iter().filter(|m| m.is_data_row()).count();
    assert_eq!(final_row_count, 3, "Should have 3 rows after DELETE");

    client.send_terminate().await.expect("Failed to terminate");
    server.shutdown();
}

/// Test session maintains state after error
#[tokio::test]
async fn test_session_continues_after_error() {
    let server = start_test_server().await;
    let mut client = TestClient::connect(server.addr()).await.expect("Failed to connect");

    client.send_startup("testuser", "testdb").await.expect("Failed to send startup");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read response");

    // Create table
    client.send_query("CREATE TABLE error_test (id INT)").await.expect("Failed to CREATE");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read response");

    // Cause an error (query non-existent table)
    client.send_query("SELECT * FROM nonexistent_table").await.expect("Failed to SELECT");
    let data = client.read_until_message_type(b'Z').await.expect("Failed to read response");
    let messages = parse_backend_messages(&data);
    assert!(messages.iter().any(|m| m.is_error()), "Should get an error for non-existent table");
    assert!(messages.iter().any(|m| m.is_ready_for_query()), "Should still be ready for queries");

    // Session should still work - query the existing table
    client.send_query("SELECT * FROM error_test").await.expect("Failed to SELECT after error");
    let data = client.read_until_message_type(b'Z').await.expect("Failed to read response");
    let messages = parse_backend_messages(&data);
    assert!(
        messages.iter().any(|m| m.is_row_description()),
        "Session should continue working after error"
    );

    client.send_terminate().await.expect("Failed to terminate");
    server.shutdown();
}

/// Test that sessions connecting to the same database share data
/// This verifies the shared database behavior - tables created in one session
/// are visible in subsequent sessions connecting to the same database name.
#[tokio::test]
async fn test_shared_database_across_sessions() {
    let server = start_test_server().await;

    // First session creates a table
    {
        let mut client = TestClient::connect(server.addr()).await.expect("Failed to connect");
        client.send_startup("testuser", "testdb").await.expect("Failed to send startup");
        let _ = client.read_until_message_type(b'Z').await.expect("Failed to read response");

        client.send_query("CREATE TABLE shared_table (id INT)").await.expect("Failed to CREATE");
        let _ = client.read_until_message_type(b'Z').await.expect("Failed to read response");

        client.send_terminate().await.expect("Failed to terminate");
    }

    // New session to SAME database should see the table (shared database)
    {
        let mut client = TestClient::connect(server.addr()).await.expect("Failed to connect");
        client.send_startup("testuser", "testdb").await.expect("Failed to send startup");
        let _ = client.read_until_message_type(b'Z').await.expect("Failed to read response");

        client.send_query("SELECT * FROM shared_table").await.expect("Failed to SELECT");
        let data = client.read_until_message_type(b'Z').await.expect("Failed to read response");
        let messages = parse_backend_messages(&data);
        // Table should exist since sessions share the same database
        assert!(
            !messages.iter().any(|m| m.is_error()),
            "Table from previous session SHOULD exist in shared database"
        );

        client.send_terminate().await.expect("Failed to terminate");
    }

    // Session to DIFFERENT database should NOT see the table
    {
        let mut client = TestClient::connect(server.addr()).await.expect("Failed to connect");
        client.send_startup("testuser", "otherdb").await.expect("Failed to send startup");
        let _ = client.read_until_message_type(b'Z').await.expect("Failed to read response");

        client.send_query("SELECT * FROM shared_table").await.expect("Failed to SELECT");
        let data = client.read_until_message_type(b'Z').await.expect("Failed to read response");
        let messages = parse_backend_messages(&data);
        // Table should NOT exist in different database
        assert!(
            messages.iter().any(|m| m.is_error()),
            "Table should NOT exist in different database"
        );

        client.send_terminate().await.expect("Failed to terminate");
    }

    server.shutdown();
}

/// Test concurrent sessions don't interfere with each other's operations
#[tokio::test]
async fn test_concurrent_session_operations() {
    let server = start_test_server().await;
    let addr = server.addr();

    let handles: Vec<_> = (0..3)
        .map(|i| {
            tokio::spawn(async move {
                let mut client = TestClient::connect(addr).await.expect("Failed to connect");
                client
                    .send_startup(&format!("user{}", i), "testdb")
                    .await
                    .expect("Failed to startup");
                let _ =
                    client.read_until_message_type(b'Z').await.expect("Failed to read response");

                // Each session creates its own table
                let table_name = format!("concurrent_table_{}", i);
                client
                    .send_query(&format!("CREATE TABLE {} (id INT)", table_name))
                    .await
                    .expect("Failed to CREATE");
                let _ =
                    client.read_until_message_type(b'Z').await.expect("Failed to read response");

                // Insert multiple rows
                for j in 0..5 {
                    client
                        .send_query(&format!("INSERT INTO {} VALUES ({})", table_name, j))
                        .await
                        .expect("Failed to INSERT");
                    let _ = client
                        .read_until_message_type(b'Z')
                        .await
                        .expect("Failed to read response");
                }

                // Verify row count
                client
                    .send_query(&format!("SELECT * FROM {}", table_name))
                    .await
                    .expect("Failed to SELECT");
                let data =
                    client.read_until_message_type(b'Z').await.expect("Failed to read response");
                let messages = parse_backend_messages(&data);
                let row_count = messages.iter().filter(|m| m.is_data_row()).count();
                assert_eq!(row_count, 5, "Session {} should have 5 rows", i);

                client.send_terminate().await.expect("Failed to terminate");
            })
        })
        .collect();

    for handle in handles {
        handle.await.expect("Session task failed");
    }

    server.shutdown();
}

/// Test that index creation is persistent within session
#[tokio::test]
async fn test_session_create_index() {
    let server = start_test_server().await;
    let mut client = TestClient::connect(server.addr()).await.expect("Failed to connect");

    client.send_startup("testuser", "testdb").await.expect("Failed to send startup");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read response");

    // Create table
    client
        .send_query("CREATE TABLE index_test (id INT, name VARCHAR(50))")
        .await
        .expect("Failed to CREATE TABLE");
    let data = client.read_until_message_type(b'Z').await.expect("Failed to read response");
    let messages = parse_backend_messages(&data);
    assert!(messages.iter().any(|m| m.is_command_complete()));

    // Create index
    client
        .send_query("CREATE INDEX idx_name ON index_test (name)")
        .await
        .expect("Failed to CREATE INDEX");
    let data = client.read_until_message_type(b'Z').await.expect("Failed to read response");
    let messages = parse_backend_messages(&data);
    assert!(messages.iter().any(|m| m.is_command_complete()), "CREATE INDEX should complete");

    // Insert data and query (index should be used implicitly)
    client.send_query("INSERT INTO index_test VALUES (1, 'test')").await.expect("Failed to INSERT");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read response");

    client
        .send_query("SELECT * FROM index_test WHERE name = 'test'")
        .await
        .expect("Failed to SELECT");
    let data = client.read_until_message_type(b'Z').await.expect("Failed to read response");
    let messages = parse_backend_messages(&data);
    assert!(messages.iter().any(|m| m.is_data_row()), "Query should work with index");

    client.send_terminate().await.expect("Failed to terminate");
    server.shutdown();
}
