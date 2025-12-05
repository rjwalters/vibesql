//! Integration tests for PostgreSQL wire protocol
//!
//! Tests message serialization/deserialization, query handling,
//! and protocol compliance.

mod common;

use common::{parse_backend_messages, start_test_server, TestClient};

/// Test query message roundtrip with simple SELECT
#[tokio::test]
async fn test_simple_query_roundtrip() {
    let server = start_test_server().await;
    let mut client = TestClient::connect(server.addr()).await.expect("Failed to connect");

    // Complete handshake
    client.send_startup("testuser", "testdb").await.expect("Failed to send startup");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read startup response");

    // Send a simple query
    client.send_query("SELECT 1").await.expect("Failed to send query");

    // Read query response
    let data = client.read_until_message_type(b'Z').await.expect("Failed to read query response");
    let messages = parse_backend_messages(&data);

    // Should have RowDescription, DataRow, CommandComplete, ReadyForQuery
    assert!(
        messages.iter().any(|m| m.is_row_description()),
        "Expected RowDescription for SELECT query"
    );
    assert!(messages.iter().any(|m| m.is_data_row()), "Expected DataRow for SELECT query");
    assert!(
        messages.iter().any(|m| m.is_command_complete()),
        "Expected CommandComplete for SELECT query"
    );
    assert!(messages.iter().any(|m| m.is_ready_for_query()), "Expected ReadyForQuery after query");

    // Verify command tag
    let cmd_complete = messages.iter().find(|m| m.is_command_complete()).unwrap();
    let tag = cmd_complete.get_command_tag().expect("Failed to get command tag");
    assert!(tag.starts_with("SELECT"), "Command tag should start with SELECT, got: {}", tag);

    client.send_terminate().await.expect("Failed to send terminate");
    server.shutdown();
}

/// Test empty query response
#[tokio::test]
async fn test_empty_query() {
    let server = start_test_server().await;
    let mut client = TestClient::connect(server.addr()).await.expect("Failed to connect");

    client.send_startup("testuser", "testdb").await.expect("Failed to send startup");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read startup response");

    // Send empty query
    client.send_query("").await.expect("Failed to send empty query");

    let data = client.read_until_message_type(b'Z').await.expect("Failed to read response");
    let messages = parse_backend_messages(&data);

    // Should have EmptyQueryResponse and ReadyForQuery
    assert!(
        messages.iter().any(|m| m.is_empty_query_response()),
        "Expected EmptyQueryResponse for empty query"
    );
    assert!(
        messages.iter().any(|m| m.is_ready_for_query()),
        "Expected ReadyForQuery after empty query"
    );

    client.send_terminate().await.expect("Failed to send terminate");
    server.shutdown();
}

/// Test whitespace-only query (treated as empty)
#[tokio::test]
async fn test_whitespace_query() {
    let server = start_test_server().await;
    let mut client = TestClient::connect(server.addr()).await.expect("Failed to connect");

    client.send_startup("testuser", "testdb").await.expect("Failed to send startup");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read startup response");

    // Send whitespace-only query
    client.send_query("   \t\n  ").await.expect("Failed to send whitespace query");

    let data = client.read_until_message_type(b'Z').await.expect("Failed to read response");
    let messages = parse_backend_messages(&data);

    // Should have EmptyQueryResponse
    assert!(
        messages.iter().any(|m| m.is_empty_query_response()),
        "Expected EmptyQueryResponse for whitespace query"
    );

    client.send_terminate().await.expect("Failed to send terminate");
    server.shutdown();
}

/// Test CREATE TABLE and INSERT roundtrip
#[tokio::test]
async fn test_ddl_and_dml_roundtrip() {
    let server = start_test_server().await;
    let mut client = TestClient::connect(server.addr()).await.expect("Failed to connect");

    client.send_startup("testuser", "testdb").await.expect("Failed to send startup");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read startup response");

    // Create table
    client
        .send_query("CREATE TABLE test_table (id INT, name VARCHAR(100))")
        .await
        .expect("Failed to send CREATE TABLE");
    let data = client.read_until_message_type(b'Z').await.expect("Failed to read CREATE response");
    let messages = parse_backend_messages(&data);
    assert!(
        messages.iter().any(|m| m.is_command_complete()),
        "Expected CommandComplete for CREATE TABLE"
    );

    // Insert data
    client
        .send_query("INSERT INTO test_table VALUES (1, 'test')")
        .await
        .expect("Failed to send INSERT");
    let data = client.read_until_message_type(b'Z').await.expect("Failed to read INSERT response");
    let messages = parse_backend_messages(&data);

    let cmd_complete = messages.iter().find(|m| m.is_command_complete()).unwrap();
    let tag = cmd_complete.get_command_tag().expect("Failed to get command tag");
    assert!(tag.starts_with("INSERT"), "Command tag should be INSERT, got: {}", tag);

    // Select data back
    client.send_query("SELECT * FROM test_table").await.expect("Failed to send SELECT");
    let data = client.read_until_message_type(b'Z').await.expect("Failed to read SELECT response");
    let messages = parse_backend_messages(&data);

    assert!(messages.iter().any(|m| m.is_row_description()));
    assert!(messages.iter().any(|m| m.is_data_row()));

    client.send_terminate().await.expect("Failed to send terminate");
    server.shutdown();
}

/// Test UPDATE command roundtrip
#[tokio::test]
async fn test_update_roundtrip() {
    let server = start_test_server().await;
    let mut client = TestClient::connect(server.addr()).await.expect("Failed to connect");

    client.send_startup("testuser", "testdb").await.expect("Failed to send startup");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read startup response");

    // Create and populate table
    client
        .send_query("CREATE TABLE update_test (id INT, value INT)")
        .await
        .expect("Failed to send CREATE TABLE");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read response");

    client
        .send_query("INSERT INTO update_test VALUES (1, 10)")
        .await
        .expect("Failed to send INSERT");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read response");

    // Update data
    client
        .send_query("UPDATE update_test SET value = 20 WHERE id = 1")
        .await
        .expect("Failed to send UPDATE");
    let data = client.read_until_message_type(b'Z').await.expect("Failed to read UPDATE response");
    let messages = parse_backend_messages(&data);

    let cmd_complete = messages.iter().find(|m| m.is_command_complete()).unwrap();
    let tag = cmd_complete.get_command_tag().expect("Failed to get command tag");
    assert!(tag.starts_with("UPDATE"), "Command tag should be UPDATE, got: {}", tag);

    client.send_terminate().await.expect("Failed to send terminate");
    server.shutdown();
}

/// Test DELETE command roundtrip
#[tokio::test]
async fn test_delete_roundtrip() {
    let server = start_test_server().await;
    let mut client = TestClient::connect(server.addr()).await.expect("Failed to connect");

    client.send_startup("testuser", "testdb").await.expect("Failed to send startup");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read startup response");

    // Create and populate table
    client
        .send_query("CREATE TABLE delete_test (id INT)")
        .await
        .expect("Failed to send CREATE TABLE");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read response");

    client.send_query("INSERT INTO delete_test VALUES (1)").await.expect("Failed to send INSERT");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read response");

    // Delete data
    client.send_query("DELETE FROM delete_test WHERE id = 1").await.expect("Failed to send DELETE");
    let data = client.read_until_message_type(b'Z').await.expect("Failed to read DELETE response");
    let messages = parse_backend_messages(&data);

    let cmd_complete = messages.iter().find(|m| m.is_command_complete()).unwrap();
    let tag = cmd_complete.get_command_tag().expect("Failed to get command tag");
    assert!(tag.starts_with("DELETE"), "Command tag should be DELETE, got: {}", tag);

    client.send_terminate().await.expect("Failed to send terminate");
    server.shutdown();
}

/// Test multiple queries in sequence
#[tokio::test]
async fn test_multiple_queries_sequence() {
    let server = start_test_server().await;
    let mut client = TestClient::connect(server.addr()).await.expect("Failed to connect");

    client.send_startup("testuser", "testdb").await.expect("Failed to send startup");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read startup response");

    // Execute multiple queries in sequence
    for i in 0..5 {
        client.send_query(&format!("SELECT {}", i)).await.expect("Failed to send query");
        let data = client.read_until_message_type(b'Z').await.expect("Failed to read response");
        let messages = parse_backend_messages(&data);
        assert!(
            messages.iter().any(|m| m.is_ready_for_query()),
            "Each query should complete with ReadyForQuery"
        );
    }

    client.send_terminate().await.expect("Failed to send terminate");
    server.shutdown();
}

/// Test ParameterStatus messages have expected parameters
#[tokio::test]
async fn test_parameter_status_messages() {
    let server = start_test_server().await;
    let mut client = TestClient::connect(server.addr()).await.expect("Failed to connect");

    client.send_startup("testuser", "testdb").await.expect("Failed to send startup");
    let data = client.read_until_message_type(b'Z').await.expect("Failed to read startup response");
    let messages = parse_backend_messages(&data);

    // Count ParameterStatus messages
    let param_statuses: Vec<_> = messages.iter().filter(|m| m.is_parameter_status()).collect();

    // Should have several standard parameters
    assert!(
        param_statuses.len() >= 5,
        "Expected at least 5 ParameterStatus messages, got {}",
        param_statuses.len()
    );

    client.send_terminate().await.expect("Failed to send terminate");
    server.shutdown();
}

/// Test BackendKeyData is sent during startup
#[tokio::test]
async fn test_backend_key_data() {
    let server = start_test_server().await;
    let mut client = TestClient::connect(server.addr()).await.expect("Failed to connect");

    client.send_startup("testuser", "testdb").await.expect("Failed to send startup");
    let data = client.read_until_message_type(b'Z').await.expect("Failed to read startup response");
    let messages = parse_backend_messages(&data);

    // Should have exactly one BackendKeyData
    let key_data_count = messages.iter().filter(|m| m.is_backend_key_data()).count();
    assert_eq!(key_data_count, 1, "Expected exactly one BackendKeyData message");

    // BackendKeyData should have process_id and secret_key (8 bytes payload)
    let key_data = messages.iter().find(|m| m.is_backend_key_data()).unwrap();
    assert_eq!(key_data.payload.len(), 8, "BackendKeyData should have 8 bytes payload");

    client.send_terminate().await.expect("Failed to send terminate");
    server.shutdown();
}

/// Test ReadyForQuery status indicator
#[tokio::test]
async fn test_ready_for_query_status() {
    let server = start_test_server().await;
    let mut client = TestClient::connect(server.addr()).await.expect("Failed to connect");

    client.send_startup("testuser", "testdb").await.expect("Failed to send startup");
    let data = client.read_until_message_type(b'Z').await.expect("Failed to read startup response");
    let messages = parse_backend_messages(&data);

    // Check ReadyForQuery status byte
    let rfq = messages.iter().find(|m| m.is_ready_for_query()).unwrap();
    assert_eq!(rfq.payload.len(), 1, "ReadyForQuery should have 1 byte payload");
    assert_eq!(rfq.payload[0], b'I', "Status should be 'I' (idle) after startup");

    client.send_terminate().await.expect("Failed to send terminate");
    server.shutdown();
}

/// Test that RowDescription has correct field count
#[tokio::test]
async fn test_row_description_fields() {
    let server = start_test_server().await;
    let mut client = TestClient::connect(server.addr()).await.expect("Failed to connect");

    client.send_startup("testuser", "testdb").await.expect("Failed to send startup");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read startup response");

    // Create table with known columns
    client
        .send_query("CREATE TABLE field_test (a INT, b INT, c INT)")
        .await
        .expect("Failed to send CREATE");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read response");

    client
        .send_query("INSERT INTO field_test VALUES (1, 2, 3)")
        .await
        .expect("Failed to send INSERT");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read response");

    // Select all columns
    client.send_query("SELECT * FROM field_test").await.expect("Failed to send SELECT");
    let data = client.read_until_message_type(b'Z').await.expect("Failed to read response");
    let messages = parse_backend_messages(&data);

    let row_desc = messages.iter().find(|m| m.is_row_description()).unwrap();
    // Field count is first 2 bytes as i16
    let field_count = i16::from_be_bytes([row_desc.payload[0], row_desc.payload[1]]);
    assert_eq!(field_count, 3, "Expected 3 fields in RowDescription");

    client.send_terminate().await.expect("Failed to send terminate");
    server.shutdown();
}

/// Test DataRow has correct value count matching RowDescription
#[tokio::test]
async fn test_data_row_matches_row_description() {
    let server = start_test_server().await;
    let mut client = TestClient::connect(server.addr()).await.expect("Failed to connect");

    client.send_startup("testuser", "testdb").await.expect("Failed to send startup");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read startup response");

    client
        .send_query("CREATE TABLE match_test (x INT, y INT)")
        .await
        .expect("Failed to send CREATE");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read response");

    client
        .send_query("INSERT INTO match_test VALUES (10, 20)")
        .await
        .expect("Failed to send INSERT");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read response");

    client.send_query("SELECT * FROM match_test").await.expect("Failed to send SELECT");
    let data = client.read_until_message_type(b'Z').await.expect("Failed to read response");
    let messages = parse_backend_messages(&data);

    let row_desc = messages.iter().find(|m| m.is_row_description()).unwrap();
    let data_row = messages.iter().find(|m| m.is_data_row()).unwrap();

    // Get field counts
    let desc_fields = i16::from_be_bytes([row_desc.payload[0], row_desc.payload[1]]);
    let row_fields = i16::from_be_bytes([data_row.payload[0], data_row.payload[1]]);

    assert_eq!(desc_fields, row_fields, "DataRow field count should match RowDescription");

    client.send_terminate().await.expect("Failed to send terminate");
    server.shutdown();
}

/// Test SELECT with no rows returns RowDescription but no DataRow
#[tokio::test]
async fn test_select_no_rows() {
    let server = start_test_server().await;
    let mut client = TestClient::connect(server.addr()).await.expect("Failed to connect");

    client.send_startup("testuser", "testdb").await.expect("Failed to send startup");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read startup response");

    client.send_query("CREATE TABLE empty_test (id INT)").await.expect("Failed to send CREATE");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read response");

    // Select from empty table
    client.send_query("SELECT * FROM empty_test").await.expect("Failed to send SELECT");
    let data = client.read_until_message_type(b'Z').await.expect("Failed to read response");
    let messages = parse_backend_messages(&data);

    // Should have RowDescription but no DataRow
    assert!(
        messages.iter().any(|m| m.is_row_description()),
        "Should have RowDescription even for empty result"
    );
    assert!(!messages.iter().any(|m| m.is_data_row()), "Should not have DataRow for empty result");

    // CommandComplete should say SELECT 0
    let cmd = messages.iter().find(|m| m.is_command_complete()).unwrap();
    let tag = cmd.get_command_tag().unwrap();
    assert_eq!(tag, "SELECT 0", "Command tag should be 'SELECT 0'");

    client.send_terminate().await.expect("Failed to send terminate");
    server.shutdown();
}
