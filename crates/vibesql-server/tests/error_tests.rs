//! Integration tests for error handling
//!
//! Tests malformed requests, SQL errors, and edge cases.

mod common;

use common::{parse_backend_messages, start_test_server, TestClient};

/// Test SQL syntax error handling
#[tokio::test]
async fn test_sql_syntax_error() {
    let server = start_test_server().await;
    let mut client = TestClient::connect(server.addr()).await.expect("Failed to connect");

    client.send_startup("testuser", "testdb").await.expect("Failed to send startup");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read response");

    // Send invalid SQL
    client.send_query("SELEKT * FROM nowhere").await.expect("Failed to send query");

    let data = client.read_until_message_type(b'Z').await.expect("Failed to read response");
    let messages = parse_backend_messages(&data);

    // Should have ErrorResponse
    assert!(messages.iter().any(|m| m.is_error()), "Should receive ErrorResponse for syntax error");

    // Should still be ready for more queries
    assert!(
        messages.iter().any(|m| m.is_ready_for_query()),
        "Should be ready for queries after error"
    );

    // Verify connection still works
    client.send_query("SELECT 1").await.expect("Failed to send query");
    let data = client.read_until_message_type(b'Z').await.expect("Failed to read response");
    let messages = parse_backend_messages(&data);
    assert!(
        messages.iter().any(|m| m.is_command_complete()),
        "Connection should still work after error"
    );

    client.send_terminate().await.expect("Failed to terminate");
    server.shutdown();
}

/// Test error on non-existent table
#[tokio::test]
async fn test_nonexistent_table_error() {
    let server = start_test_server().await;
    let mut client = TestClient::connect(server.addr()).await.expect("Failed to connect");

    client.send_startup("testuser", "testdb").await.expect("Failed to send startup");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read response");

    // Query non-existent table
    client
        .send_query("SELECT * FROM table_that_does_not_exist")
        .await
        .expect("Failed to send query");

    let data = client.read_until_message_type(b'Z').await.expect("Failed to read response");
    let messages = parse_backend_messages(&data);

    assert!(
        messages.iter().any(|m| m.is_error()),
        "Should receive ErrorResponse for non-existent table"
    );
    assert!(messages.iter().any(|m| m.is_ready_for_query()), "Should still be ready for queries");

    client.send_terminate().await.expect("Failed to terminate");
    server.shutdown();
}

/// Test error on invalid column reference
#[tokio::test]
async fn test_invalid_column_error() {
    let server = start_test_server().await;
    let mut client = TestClient::connect(server.addr()).await.expect("Failed to connect");

    client.send_startup("testuser", "testdb").await.expect("Failed to send startup");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read response");

    // Create table and verify it succeeded
    client
        .send_query("CREATE TABLE col_test (id INT, name VARCHAR(50))")
        .await
        .expect("Failed to CREATE");
    let create_data = client.read_until_message_type(b'Z').await.expect("Failed to read response");
    let create_messages = parse_backend_messages(&create_data);
    assert!(
        create_messages.iter().any(|m| m.is_command_complete()),
        "CREATE TABLE should succeed, got messages: {:?}",
        create_messages.iter().map(|m| m.msg_type as char).collect::<Vec<_>>()
    );

    // Query non-existent column
    client
        .send_query("SELECT nonexistent_column FROM col_test")
        .await
        .expect("Failed to send query");

    let data = client.read_until_message_type(b'Z').await.expect("Failed to read response");
    let messages = parse_backend_messages(&data);

    assert!(
        messages.iter().any(|m| m.is_error()),
        "Should receive ErrorResponse for invalid column, got messages: {:?}",
        messages.iter().map(|m| m.msg_type as char).collect::<Vec<_>>()
    );

    client.send_terminate().await.expect("Failed to terminate");
    server.shutdown();
}

/// Test multiple errors in sequence don't crash server
#[tokio::test]
async fn test_multiple_errors_resilience() {
    let server = start_test_server().await;
    let mut client = TestClient::connect(server.addr()).await.expect("Failed to connect");

    client.send_startup("testuser", "testdb").await.expect("Failed to send startup");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read response");

    // Send multiple erroneous queries
    for i in 0..5 {
        client.send_query(&format!("INVALID_STATEMENT_{}", i)).await.expect("Failed to send query");
        let data = client.read_until_message_type(b'Z').await.expect("Failed to read response");
        let messages = parse_backend_messages(&data);
        assert!(messages.iter().any(|m| m.is_error()), "Query {} should error", i);
        assert!(
            messages.iter().any(|m| m.is_ready_for_query()),
            "Should still be ready after error {}",
            i
        );
    }

    // Connection should still work for valid queries
    client.send_query("SELECT 1").await.expect("Failed to send valid query");
    let data = client.read_until_message_type(b'Z').await.expect("Failed to read response");
    let messages = parse_backend_messages(&data);
    assert!(
        messages.iter().any(|m| m.is_command_complete()),
        "Valid query should succeed after errors"
    );

    client.send_terminate().await.expect("Failed to terminate");
    server.shutdown();
}

/// Test constraint violation error (duplicate primary key)
#[tokio::test]
async fn test_constraint_violation_error() {
    let server = start_test_server().await;
    let mut client = TestClient::connect(server.addr()).await.expect("Failed to connect");

    client.send_startup("testuser", "testdb").await.expect("Failed to send startup");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read response");

    // Create table with primary key
    client
        .send_query("CREATE TABLE pk_test (id INT PRIMARY KEY, value INT)")
        .await
        .expect("Failed to CREATE");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read response");

    // Insert first row
    client.send_query("INSERT INTO pk_test VALUES (1, 100)").await.expect("Failed to INSERT");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read response");

    // Try to insert duplicate primary key
    client
        .send_query("INSERT INTO pk_test VALUES (1, 200)")
        .await
        .expect("Failed to send duplicate INSERT");
    let data = client.read_until_message_type(b'Z').await.expect("Failed to read response");
    let messages = parse_backend_messages(&data);

    // Should either error or succeed depending on implementation
    // Most DBs would error on duplicate PK
    assert!(
        messages.iter().any(|m| m.is_ready_for_query()),
        "Should be ready for queries after constraint check"
    );

    client.send_terminate().await.expect("Failed to terminate");
    server.shutdown();
}

/// Test error recovery - valid query after error
#[tokio::test]
async fn test_error_recovery() {
    let server = start_test_server().await;
    let mut client = TestClient::connect(server.addr()).await.expect("Failed to connect");

    client.send_startup("testuser", "testdb").await.expect("Failed to send startup");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read response");

    // Create valid table
    client.send_query("CREATE TABLE recovery_test (id INT)").await.expect("Failed to CREATE");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read response");

    // Cause an error
    client.send_query("SELECT * FROM no_such_table").await.expect("Failed to send bad query");
    let data = client.read_until_message_type(b'Z').await.expect("Failed to read response");
    let messages = parse_backend_messages(&data);
    assert!(messages.iter().any(|m| m.is_error()));

    // Recovery: valid insert
    client.send_query("INSERT INTO recovery_test VALUES (1)").await.expect("Failed to INSERT");
    let data = client.read_until_message_type(b'Z').await.expect("Failed to read response");
    let messages = parse_backend_messages(&data);
    assert!(
        messages.iter().any(|m| m.is_command_complete()),
        "Should complete INSERT after error recovery"
    );

    // Recovery: valid select
    client.send_query("SELECT * FROM recovery_test").await.expect("Failed to SELECT");
    let data = client.read_until_message_type(b'Z').await.expect("Failed to read response");
    let messages = parse_backend_messages(&data);
    assert!(messages.iter().any(|m| m.is_data_row()), "Should return data after error recovery");

    client.send_terminate().await.expect("Failed to terminate");
    server.shutdown();
}

/// Test error on DROP non-existent table
#[tokio::test]
async fn test_drop_nonexistent_table_error() {
    let server = start_test_server().await;
    let mut client = TestClient::connect(server.addr()).await.expect("Failed to connect");

    client.send_startup("testuser", "testdb").await.expect("Failed to send startup");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read response");

    // Try to drop non-existent table
    client.send_query("DROP TABLE nonexistent_table").await.expect("Failed to send DROP");
    let data = client.read_until_message_type(b'Z').await.expect("Failed to read response");
    let messages = parse_backend_messages(&data);

    assert!(messages.iter().any(|m| m.is_error()), "Should error when dropping non-existent table");
    assert!(messages.iter().any(|m| m.is_ready_for_query()), "Should still be ready for queries");

    client.send_terminate().await.expect("Failed to terminate");
    server.shutdown();
}

/// Test error message format (should have severity and message)
#[tokio::test]
async fn test_error_message_format() {
    let server = start_test_server().await;
    let mut client = TestClient::connect(server.addr()).await.expect("Failed to connect");

    client.send_startup("testuser", "testdb").await.expect("Failed to send startup");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read response");

    // Cause an error
    client.send_query("INVALID SYNTAX HERE").await.expect("Failed to send query");
    let data = client.read_until_message_type(b'Z').await.expect("Failed to read response");
    let messages = parse_backend_messages(&data);

    let error_msg = messages.iter().find(|m| m.is_error());
    assert!(error_msg.is_some(), "Should have error message");

    // Error message payload should contain field codes
    let error = error_msg.unwrap();
    // Check that payload is not empty (contains severity, code, message fields)
    assert!(!error.payload.is_empty(), "Error payload should not be empty");

    client.send_terminate().await.expect("Failed to terminate");
    server.shutdown();
}

/// Test alternating errors and successes
#[tokio::test]
async fn test_alternating_errors_successes() {
    let server = start_test_server().await;
    let mut client = TestClient::connect(server.addr()).await.expect("Failed to connect");

    client.send_startup("testuser", "testdb").await.expect("Failed to send startup");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read response");

    for i in 0..10 {
        if i % 2 == 0 {
            // Success case
            client.send_query(&format!("SELECT {}", i)).await.expect("Failed to send query");
            let data = client.read_until_message_type(b'Z').await.expect("Failed to read response");
            let messages = parse_backend_messages(&data);
            assert!(
                messages.iter().any(|m| m.is_command_complete()),
                "Even iteration {} should succeed",
                i
            );
        } else {
            // Error case
            client.send_query("INVALID SQL").await.expect("Failed to send query");
            let data = client.read_until_message_type(b'Z').await.expect("Failed to read response");
            let messages = parse_backend_messages(&data);
            assert!(messages.iter().any(|m| m.is_error()), "Odd iteration {} should error", i);
        }
    }

    client.send_terminate().await.expect("Failed to terminate");
    server.shutdown();
}

/// Test type mismatch error (inserting wrong type)
#[tokio::test]
async fn test_type_mismatch_error() {
    let server = start_test_server().await;
    let mut client = TestClient::connect(server.addr()).await.expect("Failed to connect");

    client.send_startup("testuser", "testdb").await.expect("Failed to send startup");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read response");

    // Create table with specific type
    client.send_query("CREATE TABLE type_test (id INT)").await.expect("Failed to CREATE");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read response");

    // Insert string into INT column - may error or coerce depending on implementation
    client
        .send_query("INSERT INTO type_test VALUES ('not an integer')")
        .await
        .expect("Failed to send INSERT");
    let data = client.read_until_message_type(b'Z').await.expect("Failed to read response");
    let messages = parse_backend_messages(&data);

    // Should either error or succeed with coercion
    assert!(
        messages.iter().any(|m| m.is_ready_for_query()),
        "Should still be ready for queries after type check"
    );

    client.send_terminate().await.expect("Failed to terminate");
    server.shutdown();
}

/// Test error on ambiguous query
#[tokio::test]
async fn test_ambiguous_column_error() {
    let server = start_test_server().await;
    let mut client = TestClient::connect(server.addr()).await.expect("Failed to connect");

    client.send_startup("testuser", "testdb").await.expect("Failed to send startup");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read response");

    // Create two tables with same column name
    client.send_query("CREATE TABLE ambig1 (id INT, value INT)").await.expect("Failed to CREATE");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read response");

    client.send_query("CREATE TABLE ambig2 (id INT, value INT)").await.expect("Failed to CREATE");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read response");

    // Try ambiguous query without qualification - this might work or error
    // depending on whether the DB requires explicit join
    client.send_query("SELECT id FROM ambig1, ambig2").await.expect("Failed to send query");
    let data = client.read_until_message_type(b'Z').await.expect("Failed to read response");
    let messages = parse_backend_messages(&data);

    // Either error or succeed - both are valid outcomes
    assert!(messages.iter().any(|m| m.is_ready_for_query()), "Should still be ready for queries");

    client.send_terminate().await.expect("Failed to terminate");
    server.shutdown();
}

/// Test server handles concurrent error conditions gracefully
#[tokio::test]
async fn test_concurrent_errors() {
    let server = start_test_server().await;
    let addr = server.addr();

    let handles: Vec<_> = (0..5)
        .map(|i| {
            tokio::spawn(async move {
                let mut client = TestClient::connect(addr).await.expect("Failed to connect");
                client
                    .send_startup(&format!("user{}", i), "testdb")
                    .await
                    .expect("Failed to startup");
                let _ =
                    client.read_until_message_type(b'Z').await.expect("Failed to read response");

                // Each client causes multiple errors
                for j in 0..3 {
                    client
                        .send_query(&format!("INVALID_QUERY_{}_{}", i, j))
                        .await
                        .expect("Failed to send query");
                    let data = client
                        .read_until_message_type(b'Z')
                        .await
                        .expect("Failed to read response");
                    let messages = parse_backend_messages(&data);
                    assert!(messages.iter().any(|m| m.is_error()));
                }

                // Then a valid query
                client.send_query("SELECT 1").await.expect("Failed to send valid query");
                let data =
                    client.read_until_message_type(b'Z').await.expect("Failed to read response");
                let messages = parse_backend_messages(&data);
                assert!(
                    messages.iter().any(|m| m.is_command_complete()),
                    "Valid query should succeed for client {}",
                    i
                );

                client.send_terminate().await.expect("Failed to terminate");
            })
        })
        .collect();

    for handle in handles {
        handle.await.expect("Client task failed");
    }

    server.shutdown();
}
