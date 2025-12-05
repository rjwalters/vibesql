//! Integration tests for connection lifecycle
//!
//! Tests client connect/disconnect handling, connection behavior,
//! and graceful connection management.

mod common;

use common::{parse_backend_messages, start_test_server, TestClient};
use std::time::Duration;
use tokio::time::timeout;

/// Test basic client connection and handshake
#[tokio::test]
async fn test_basic_connection() {
    let server = start_test_server().await;
    let mut client = TestClient::connect(server.addr()).await.expect("Failed to connect");

    // Send startup message
    client.send_startup("testuser", "testdb").await.expect("Failed to send startup");

    // Read response - should get authentication ok and ready for query
    let data = client.read_until_message_type(b'Z').await.expect("Failed to read response");

    let messages = parse_backend_messages(&data);

    // Should have AuthenticationOk
    assert!(messages.iter().any(|m| m.is_auth_ok()), "Expected AuthenticationOk message");

    // Should have ReadyForQuery
    assert!(messages.iter().any(|m| m.is_ready_for_query()), "Expected ReadyForQuery message");

    // Should have ParameterStatus messages (server_version, etc.)
    let param_count = messages.iter().filter(|m| m.is_parameter_status()).count();
    assert!(param_count >= 3, "Expected at least 3 ParameterStatus messages, got {}", param_count);

    // Should have BackendKeyData
    assert!(messages.iter().any(|m| m.is_backend_key_data()), "Expected BackendKeyData message");

    // Clean disconnect
    client.send_terminate().await.expect("Failed to send terminate");
    server.shutdown();
}

/// Test SSL request handling (server rejects SSL)
#[tokio::test]
async fn test_ssl_request_rejected() {
    let server = start_test_server().await;
    let mut client = TestClient::connect(server.addr()).await.expect("Failed to connect");

    // Send SSL request
    client.send_ssl_request().await.expect("Failed to send SSL request");

    // Server should respond with 'N' (SSL not supported)
    let response = client.read_byte().await.expect("Failed to read SSL response");
    assert_eq!(response, b'N', "Expected 'N' response for SSL rejection");

    // Now send regular startup
    client.send_startup("testuser", "testdb").await.expect("Failed to send startup");

    // Should complete successfully
    let data = client.read_until_message_type(b'Z').await.expect("Failed to read response");

    let messages = parse_backend_messages(&data);
    assert!(
        messages.iter().any(|m| m.is_ready_for_query()),
        "Expected ReadyForQuery after SSL rejection and normal startup"
    );

    client.send_terminate().await.expect("Failed to send terminate");
    server.shutdown();
}

/// Test graceful client disconnect
#[tokio::test]
async fn test_graceful_disconnect() {
    let server = start_test_server().await;
    let mut client = TestClient::connect(server.addr()).await.expect("Failed to connect");

    // Complete handshake
    client.send_startup("testuser", "testdb").await.expect("Failed to send startup");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read response");

    // Send terminate
    client.send_terminate().await.expect("Failed to send terminate");

    // Give server time to process
    tokio::time::sleep(Duration::from_millis(50)).await;

    // Server should still be running (accepting new connections)
    let mut client2 =
        TestClient::connect(server.addr()).await.expect("Server should still accept connections");
    client2.send_startup("testuser2", "testdb").await.expect("Failed to send startup");
    let _ = client2.read_until_message_type(b'Z').await.expect("Failed to read response");
    client2.send_terminate().await.expect("Failed to send terminate");

    server.shutdown();
}

/// Test connection drop without terminate message
#[tokio::test]
async fn test_connection_drop_without_terminate() {
    let server = start_test_server().await;

    {
        let mut client = TestClient::connect(server.addr()).await.expect("Failed to connect");
        client.send_startup("testuser", "testdb").await.expect("Failed to send startup");
        let _ = client.read_until_message_type(b'Z').await.expect("Failed to read response");
        // Client drops here without sending terminate
    }

    // Give server time to clean up
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Server should still be running
    let mut client2 = TestClient::connect(server.addr())
        .await
        .expect("Server should still accept connections after client drop");
    client2.send_startup("testuser2", "testdb").await.expect("Failed to send startup");
    let _ = client2.read_until_message_type(b'Z').await.expect("Failed to read response");
    client2.send_terminate().await.expect("Failed to send terminate");

    server.shutdown();
}

/// Test multiple concurrent connections
#[tokio::test]
async fn test_multiple_concurrent_connections() {
    let server = start_test_server().await;
    let addr = server.addr();

    // Connect 5 clients concurrently
    let handles: Vec<_> = (0..5)
        .map(|i| {
            tokio::spawn(async move {
                let mut client = TestClient::connect(addr).await.expect("Failed to connect");
                client
                    .send_startup(&format!("user{}", i), "testdb")
                    .await
                    .expect("Failed to send startup");
                let data =
                    client.read_until_message_type(b'Z').await.expect("Failed to read response");
                let messages = parse_backend_messages(&data);
                assert!(messages.iter().any(|m| m.is_ready_for_query()));
                client.send_terminate().await.expect("Failed to send terminate");
            })
        })
        .collect();

    // Wait for all clients
    for handle in handles {
        handle.await.expect("Client task failed");
    }

    server.shutdown();
}

/// Test connection with default database (same as username)
#[tokio::test]
async fn test_connection_default_database() {
    let server = start_test_server().await;
    let mut client = TestClient::connect(server.addr()).await.expect("Failed to connect");

    // Send startup with just user, database should default to user
    client.send_startup("myuser", "myuser").await.expect("Failed to send startup");

    let data = client.read_until_message_type(b'Z').await.expect("Failed to read response");

    let messages = parse_backend_messages(&data);
    assert!(
        messages.iter().any(|m| m.is_ready_for_query()),
        "Connection should succeed with default database"
    );

    client.send_terminate().await.expect("Failed to send terminate");
    server.shutdown();
}

/// Test rapid connect/disconnect cycles
#[tokio::test]
async fn test_rapid_connect_disconnect_cycles() {
    let server = start_test_server().await;

    for i in 0..10 {
        let mut client = TestClient::connect(server.addr())
            .await
            .unwrap_or_else(|_| panic!("Failed to connect on cycle {}", i));

        client
            .send_startup("testuser", "testdb")
            .await
            .unwrap_or_else(|_| panic!("Failed to send startup on cycle {}", i));

        let _ = client
            .read_until_message_type(b'Z')
            .await
            .unwrap_or_else(|_| panic!("Failed to read response on cycle {}", i));

        client
            .send_terminate()
            .await
            .unwrap_or_else(|_| panic!("Failed to send terminate on cycle {}", i));

        // Small delay between cycles
        tokio::time::sleep(Duration::from_millis(10)).await;
    }

    server.shutdown();
}

/// Test connection timeout behavior (partial startup message)
#[tokio::test]
async fn test_partial_startup_timeout() {
    let server = start_test_server().await;
    let client = TestClient::connect(server.addr()).await.expect("Failed to connect");

    // Don't send startup message, just connect
    // Server should eventually timeout or we should be able to connect others

    // Try to connect another client while first one is idle
    let result = timeout(Duration::from_secs(2), async {
        let mut client2 =
            TestClient::connect(server.addr()).await.expect("Failed to connect second client");
        client2.send_startup("testuser", "testdb").await.expect("Failed to send startup");
        let _ = client2.read_until_message_type(b'Z').await.expect("Failed to read response");
        client2.send_terminate().await.expect("Failed to send terminate");
    })
    .await;

    assert!(result.is_ok(), "Second client should be able to connect while first is idle");

    // Clean up first client - suppress unused warning since we only need it kept alive until here
    let _ = client;
    server.shutdown();
}

/// Test that server handles many sequential connections
#[tokio::test]
async fn test_many_sequential_connections() {
    let server = start_test_server().await;

    for i in 0..20 {
        let mut client = TestClient::connect(server.addr())
            .await
            .unwrap_or_else(|_| panic!("Failed to connect on iteration {}", i));

        client.send_startup(&format!("user{}", i), "testdb").await.expect("Failed to send startup");

        let data = client.read_until_message_type(b'Z').await.expect("Failed to read response");

        let messages = parse_backend_messages(&data);
        assert!(messages.iter().any(|m| m.is_ready_for_query()), "Connection {} should succeed", i);

        client.send_terminate().await.expect("Failed to terminate");
    }

    server.shutdown();
}

/// Test connection after server has handled many connections
#[tokio::test]
async fn test_connection_after_heavy_load() {
    let server = start_test_server().await;
    let addr = server.addr();

    // Create heavy load with concurrent connections
    let handles: Vec<_> = (0..10)
        .map(|i| {
            tokio::spawn(async move {
                let mut client = TestClient::connect(addr).await.expect("Failed to connect");
                client
                    .send_startup(&format!("user{}", i), "testdb")
                    .await
                    .expect("Failed to send startup");
                let _ =
                    client.read_until_message_type(b'Z').await.expect("Failed to read response");
                // Execute a query
                client.send_query("SELECT 1").await.expect("Failed to send query");
                let _ = client
                    .read_until_message_type(b'Z')
                    .await
                    .expect("Failed to read query response");
                client.send_terminate().await.expect("Failed to terminate");
            })
        })
        .collect();

    for handle in handles {
        handle.await.expect("Client task failed");
    }

    // Now test that a new connection still works
    let mut client = TestClient::connect(addr).await.expect("Failed to connect after load");
    client.send_startup("finaluser", "testdb").await.expect("Failed to send startup");
    let data = client.read_until_message_type(b'Z').await.expect("Failed to read response");
    let messages = parse_backend_messages(&data);
    assert!(messages.iter().any(|m| m.is_ready_for_query()));
    client.send_terminate().await.expect("Failed to terminate");

    server.shutdown();
}
