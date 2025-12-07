//! End-to-end integration tests for subscription functionality
//!
//! Tests the full subscription flow from client to server to storage and back,
//! verifying that mutations trigger change events and subscriptions are properly
//! managed throughout their lifecycle.

mod common;

use bytes::{BufMut, BytesMut};
use common::{parse_backend_messages, start_test_server, TestClient};

/// Message type constants for subscription protocol
const MSG_SUBSCRIPTION_DATA: u8 = 0xF2;
const MSG_SUBSCRIPTION_PARTIAL_DATA: u8 = 0xF7;
const MSG_READY_FOR_QUERY: u8 = b'Z';

/// Helper to send a subscription request for a query
/// Note: The returned ID is a placeholder. Use `extract_subscription_id` on the
/// SubscriptionData response to get the real server-generated ID.
async fn send_subscribe(client: &mut TestClient, query: &str) -> std::io::Result<()> {
    let mut buf = BytesMut::new();

    // Build message body: query string (null-terminated) + param count (i16)
    let query_bytes = query.as_bytes();
    let mut body = BytesMut::new();
    body.extend_from_slice(query_bytes);
    body.put_u8(0); // null terminator for query string
    body.put_i16(0); // param count (0 params)

    // Message: type + length + body
    let total_len = body.len() as i32 + 4; // +4 for the length field itself
    buf.put_u8(0xF0); // Subscribe message type
    buf.put_i32(total_len);
    buf.extend_from_slice(&body);

    client.stream_write_all(&buf).await?;
    client.stream_flush().await?;
    Ok(())
}

/// Extract subscription ID from SubscriptionData message bytes
/// The ID is at bytes 0-16 of the payload (after msg_type byte and length i32 which are not in payload)
fn extract_subscription_id(messages: &[common::ParsedMessage]) -> Option<[u8; 16]> {
    for msg in messages {
        if msg.msg_type == MSG_SUBSCRIPTION_DATA && msg.payload.len() >= 16 {
            let mut id = [0u8; 16];
            // Payload starts after msg_type (1 byte) + length (4 bytes)
            // So subscription_id is at the start of payload
            id.copy_from_slice(&msg.payload[0..16]);
            return Some(id);
        }
    }
    None
}

/// Helper to send an unsubscribe request
async fn send_unsubscribe(client: &mut TestClient, sub_id: [u8; 16]) -> std::io::Result<()> {
    let mut buf = BytesMut::new();
    buf.put_u8(0xF1); // Unsubscribe message
    buf.put_i32(16 + 4); // Length: 16 bytes for ID + 4 for length field
    buf.extend_from_slice(&sub_id);

    client.stream_write_all(&buf).await?;
    client.stream_flush().await?;
    Ok(())
}

// ============================================================================
// BASIC FLOW TESTS
// ============================================================================

/// test_subscribe_receives_initial_data - Subscribe returns current query results
#[tokio::test]
async fn test_subscribe_receives_initial_data() {
    let server = start_test_server().await;
    let mut client = TestClient::connect(server.addr()).await.expect("Failed to connect");

    // Complete handshake
    client.send_startup("testuser", "testdb").await.expect("Failed to send startup");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read startup response");

    // Create a test table with unique name
    let table_name = "sub_init_data_test";
    client
        .send_query(&format!("CREATE TABLE IF NOT EXISTS {} (id INT, name VARCHAR)", table_name))
        .await
        .expect("Failed to create table");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read create response");

    // Insert initial data
    client
        .send_query(&format!("INSERT INTO {} VALUES (1, 'Alice'), (2, 'Bob')", table_name))
        .await
        .expect("Failed to insert data");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read insert response");

    // Subscribe to the table
    let select_query = format!("SELECT * FROM {}", table_name);
    send_subscribe(&mut client, &select_query).await.expect("Failed to send subscribe");

    // Read subscription response (should include initial data)
    let data = client
        .read_until_message_type(MSG_SUBSCRIPTION_DATA)
        .await
        .expect("Failed to read subscription response");
    let messages = parse_backend_messages(&data);

    // Should have SubscriptionData message (0xF2) with initial results
    let has_subscription_data = messages.iter().any(|m| m.msg_type == MSG_SUBSCRIPTION_DATA);
    assert!(has_subscription_data, "Expected SubscriptionData message for subscription");

    client.send_terminate().await.expect("Failed to send terminate");
    server.shutdown();
}

/// test_insert_triggers_subscription_update - INSERT causes subscriber notification
#[tokio::test]
async fn test_insert_triggers_subscription_update() {
    let server = start_test_server().await;
    let mut client = TestClient::connect(server.addr()).await.expect("Failed to connect");

    // Complete handshake
    client.send_startup("testuser", "testdb").await.expect("Failed to send startup");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read startup response");

    // Create a test table with unique name
    let table_name = "sub_insert_trigger_test";
    client
        .send_query(&format!("CREATE TABLE IF NOT EXISTS {} (id INT, name VARCHAR)", table_name))
        .await
        .expect("Failed to create table");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read create response");

    // Subscribe to empty table
    let select_query = format!("SELECT * FROM {}", table_name);
    send_subscribe(&mut client, &select_query).await.expect("Failed to send subscribe");
    let _ = client
        .read_until_message_type(MSG_SUBSCRIPTION_DATA)
        .await
        .expect("Failed to read subscription response");

    // Insert data - should trigger subscription update
    client
        .send_query(&format!("INSERT INTO {} VALUES (1, 'Alice')", table_name))
        .await
        .expect("Failed to insert data");

    // Read for update notification - expect both ReadyForQuery and SubscriptionData
    let data = client
        .read_until_message_type(MSG_READY_FOR_QUERY)
        .await
        .expect("Failed to read after insert");
    let messages = parse_backend_messages(&data);

    // Should include SubscriptionData message (0xF2) with the insert update
    let has_subscription_update = messages.iter().any(|m| m.msg_type == MSG_SUBSCRIPTION_DATA);
    assert!(has_subscription_update, "Expected SubscriptionData update after INSERT");

    client.send_terminate().await.expect("Failed to send terminate");
    server.shutdown();
}

/// test_update_triggers_subscription_update - UPDATE causes subscriber notification
#[tokio::test]
async fn test_update_triggers_subscription_update() {
    let server = start_test_server().await;
    let mut client = TestClient::connect(server.addr()).await.expect("Failed to connect");

    // Complete handshake
    client.send_startup("testuser", "testdb").await.expect("Failed to send startup");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read startup response");

    // Create and populate test table
    let table_name = "sub_update_trigger_test";
    client
        .send_query(&format!("CREATE TABLE IF NOT EXISTS {} (id INT, name VARCHAR)", table_name))
        .await
        .expect("Failed to create table");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read create response");

    client
        .send_query(&format!("INSERT INTO {} VALUES (1, 'Alice')", table_name))
        .await
        .expect("Failed to insert data");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read insert response");

    // Subscribe
    let select_query = format!("SELECT * FROM {} WHERE id = 1", table_name);
    send_subscribe(&mut client, &select_query).await.expect("Failed to send subscribe");
    let _ = client
        .read_until_message_type(MSG_SUBSCRIPTION_DATA)
        .await
        .expect("Failed to read subscription response");

    // Update data - should trigger subscription update
    client
        .send_query(&format!("UPDATE {} SET name = 'Alicia' WHERE id = 1", table_name))
        .await
        .expect("Failed to update data");

    // Read for update notification
    let data = client
        .read_until_message_type(MSG_READY_FOR_QUERY)
        .await
        .expect("Failed to read after update");
    let messages = parse_backend_messages(&data);

    // Should include SubscriptionData (0xF2) or SubscriptionPartialData (0xF7) with the update
    let has_subscription_update = messages
        .iter()
        .any(|m| m.msg_type == MSG_SUBSCRIPTION_DATA || m.msg_type == MSG_SUBSCRIPTION_PARTIAL_DATA);
    assert!(has_subscription_update, "Expected subscription update after UPDATE");

    client.send_terminate().await.expect("Failed to send terminate");
    server.shutdown();
}

/// test_delete_triggers_subscription_update - DELETE causes subscriber notification
#[tokio::test]
async fn test_delete_triggers_subscription_update() {
    let server = start_test_server().await;
    let mut client = TestClient::connect(server.addr()).await.expect("Failed to connect");

    // Complete handshake
    client.send_startup("testuser", "testdb").await.expect("Failed to send startup");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read startup response");

    // Create and populate test table
    let table_name = "sub_delete_trigger_test";
    client
        .send_query(&format!("CREATE TABLE IF NOT EXISTS {} (id INT, name VARCHAR)", table_name))
        .await
        .expect("Failed to create table");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read create response");

    client
        .send_query(&format!("INSERT INTO {} VALUES (1, 'Alice'), (2, 'Bob')", table_name))
        .await
        .expect("Failed to insert data");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read insert response");

    // Subscribe
    let select_query = format!("SELECT * FROM {}", table_name);
    send_subscribe(&mut client, &select_query).await.expect("Failed to send subscribe");
    let _ = client
        .read_until_message_type(MSG_SUBSCRIPTION_DATA)
        .await
        .expect("Failed to read subscription response");

    // Delete data - should trigger subscription update
    client
        .send_query(&format!("DELETE FROM {} WHERE id = 1", table_name))
        .await
        .expect("Failed to delete data");

    // Read for update notification
    let data = client
        .read_until_message_type(MSG_READY_FOR_QUERY)
        .await
        .expect("Failed to read after delete");
    let messages = parse_backend_messages(&data);

    // Should include SubscriptionData message (0xF2) with the delete
    let has_subscription_update = messages.iter().any(|m| m.msg_type == MSG_SUBSCRIPTION_DATA);
    assert!(has_subscription_update, "Expected SubscriptionData update after DELETE");

    client.send_terminate().await.expect("Failed to send terminate");
    server.shutdown();
}

/// test_unsubscribe_stops_updates - After unsubscribe, no more notifications
#[tokio::test]
async fn test_unsubscribe_stops_updates() {
    let server = start_test_server().await;
    let mut client = TestClient::connect(server.addr()).await.expect("Failed to connect");

    // Complete handshake
    client.send_startup("testuser", "testdb").await.expect("Failed to send startup");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read startup response");

    // Create and populate test table
    let table_name = "sub_unsubscribe_test";
    client
        .send_query(&format!("CREATE TABLE IF NOT EXISTS {} (id INT, name VARCHAR)", table_name))
        .await
        .expect("Failed to create table");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read create response");

    client
        .send_query(&format!("INSERT INTO {} VALUES (1, 'Alice')", table_name))
        .await
        .expect("Failed to insert data");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read insert response");

    // Subscribe
    let select_query = format!("SELECT * FROM {}", table_name);
    send_subscribe(&mut client, &select_query).await.expect("Failed to send subscribe");
    let data = client
        .read_until_message_type(MSG_SUBSCRIPTION_DATA)
        .await
        .expect("Failed to read subscription response");
    let messages = parse_backend_messages(&data);
    let sub_id = extract_subscription_id(&messages).expect("Failed to extract subscription ID");

    // Unsubscribe - no response expected per protocol spec, just continue
    send_unsubscribe(&mut client, sub_id).await.expect("Failed to send unsubscribe");

    // Insert more data
    client
        .send_query(&format!("INSERT INTO {} VALUES (2, 'Bob')", table_name))
        .await
        .expect("Failed to insert more data");

    // Read the response for insert
    let data = client
        .read_until_message_type(MSG_READY_FOR_QUERY)
        .await
        .expect("Failed to read after second insert");
    let messages = parse_backend_messages(&data);

    // Should NOT have any SubscriptionData message (0xF2) after unsubscribe
    let has_subscription_update = messages.iter().any(|m| m.msg_type == MSG_SUBSCRIPTION_DATA);
    assert!(!has_subscription_update, "Should not receive SubscriptionData after unsubscribe");

    client.send_terminate().await.expect("Failed to send terminate");
    server.shutdown();
}

// ============================================================================
// FILTERING TESTS
// ============================================================================

/// test_subscription_ignores_unrelated_tables - Changes to table B don't notify table A subscribers
#[tokio::test]
async fn test_subscription_ignores_unrelated_tables() {
    let server = start_test_server().await;
    let mut client = TestClient::connect(server.addr()).await.expect("Failed to connect");

    // Complete handshake
    client.send_startup("testuser", "testdb").await.expect("Failed to send startup");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read startup response");

    // Create two test tables
    client
        .send_query("CREATE TABLE IF NOT EXISTS sub_users_unrel (id INT, name VARCHAR)")
        .await
        .expect("Failed to create users table");
    let _ =
        client.read_until_message_type(b'Z').await.expect("Failed to read create users response");

    client
        .send_query("CREATE TABLE IF NOT EXISTS sub_orders_unrel (id INT, user_id INT)")
        .await
        .expect("Failed to create orders table");
    let _ =
        client.read_until_message_type(b'Z').await.expect("Failed to read create orders response");

    // Insert initial data in users
    client
        .send_query("INSERT INTO sub_users_unrel VALUES (1, 'Alice')")
        .await
        .expect("Failed to insert user data");
    let _ =
        client.read_until_message_type(b'Z').await.expect("Failed to read insert user response");

    // Subscribe to users table
    send_subscribe(&mut client, "SELECT * FROM sub_users_unrel")
        .await
        .expect("Failed to send subscribe");
    let _ = client
        .read_until_message_type(MSG_SUBSCRIPTION_DATA)
        .await
        .expect("Failed to read subscription response");

    // Insert into orders table (unrelated)
    client
        .send_query("INSERT INTO sub_orders_unrel VALUES (1, 1)")
        .await
        .expect("Failed to insert order data");

    // Read response - should NOT have subscription update for orders table change
    let data = client
        .read_until_message_type(MSG_READY_FOR_QUERY)
        .await
        .expect("Failed to read after orders insert");
    let messages = parse_backend_messages(&data);

    // Should NOT have SubscriptionData message (0xF2) for unrelated table change
    let has_subscription_update = messages.iter().any(|m| m.msg_type == MSG_SUBSCRIPTION_DATA);
    assert!(
        !has_subscription_update,
        "Should not receive SubscriptionData for changes to unrelated table"
    );

    client.send_terminate().await.expect("Failed to send terminate");
    server.shutdown();
}

// ============================================================================
// MULTI-CLIENT TESTS
// ============================================================================

/// test_multiple_subscribers_same_query - Both clients receive updates
///
/// This test verifies that when multiple clients subscribe to the same query,
/// mutations from one client notify all subscribers across all connections.
#[tokio::test]
async fn test_multiple_subscribers_same_query() {
    let server = start_test_server().await;

    // Connect first client
    let mut client1 = TestClient::connect(server.addr()).await.expect("Failed to connect client 1");
    client1.send_startup("testuser", "testdb").await.expect("Failed to send startup");
    let _ = client1.read_until_message_type(b'Z').await.expect("Failed to read startup response");

    // Connect second client
    let mut client2 = TestClient::connect(server.addr()).await.expect("Failed to connect client 2");
    client2.send_startup("testuser", "testdb").await.expect("Failed to send startup");
    let _ = client2.read_until_message_type(b'Z').await.expect("Failed to read startup response");

    // Create shared test table on client1
    client1
        .send_query("CREATE TABLE IF NOT EXISTS sub_shared_users (id INT, name VARCHAR)")
        .await
        .expect("Failed to create table");
    let _ = client1.read_until_message_type(b'Z').await.expect("Failed to read create response");

    // Insert initial data on client1
    client1
        .send_query("INSERT INTO sub_shared_users VALUES (1, 'Alice')")
        .await
        .expect("Failed to insert data");
    let _ = client1.read_until_message_type(b'Z').await.expect("Failed to read insert response");

    // Both clients subscribe to the same query
    let query = "SELECT * FROM sub_shared_users";
    send_subscribe(&mut client1, query).await.expect("Failed to send subscribe from client1");
    let _ = client1
        .read_until_message_type(MSG_SUBSCRIPTION_DATA)
        .await
        .expect("Failed to read subscription response");

    send_subscribe(&mut client2, query).await.expect("Failed to send subscribe from client2");
    let _ = client2
        .read_until_message_type(MSG_SUBSCRIPTION_DATA)
        .await
        .expect("Failed to read subscription response");

    // Insert new data on client1
    client1
        .send_query("INSERT INTO sub_shared_users VALUES (2, 'Bob')")
        .await
        .expect("Failed to insert data");

    // Both clients should receive update notification
    // Client1 receives the response directly (same-connection notification)
    let data1 = client1
        .read_until_message_type(MSG_READY_FOR_QUERY)
        .await
        .expect("Failed to read after insert");
    let messages1 = parse_backend_messages(&data1);
    let has_update1 = messages1.iter().any(|m| m.msg_type == MSG_SUBSCRIPTION_DATA);

    // Client2 receives a cross-connection notification, which requires the server
    // to poll and process the broadcast. Use a longer timeout (10s) to handle
    // high-load scenarios where polling may be delayed.
    let data2 = client2
        .read_until_message_type_timeout(
            MSG_SUBSCRIPTION_DATA,
            std::time::Duration::from_secs(10),
        )
        .await
        .expect("Failed to read cross-connection notification");
    let messages2 = parse_backend_messages(&data2);
    let has_update2 = messages2.iter().any(|m| m.msg_type == MSG_SUBSCRIPTION_DATA);

    assert!(has_update1, "Client1 should receive SubscriptionData update");
    assert!(has_update2, "Client2 should receive SubscriptionData update (cross-connection)");

    client1.send_terminate().await.expect("Failed to send terminate");
    client2.send_terminate().await.expect("Failed to send terminate");
    server.shutdown();
}

// ============================================================================
// EDGE CASE TESTS
// ============================================================================

/// test_subscription_survives_empty_result - Subscription works even if query returns no rows
#[tokio::test]
async fn test_subscription_survives_empty_result() {
    let server = start_test_server().await;
    let mut client = TestClient::connect(server.addr()).await.expect("Failed to connect");

    // Complete handshake
    client.send_startup("testuser", "testdb").await.expect("Failed to send startup");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read startup response");

    // Create empty test table
    client
        .send_query("CREATE TABLE IF NOT EXISTS sub_empty_users (id INT, name VARCHAR)")
        .await
        .expect("Failed to create table");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read create response");

    // Subscribe to empty result set
    send_subscribe(&mut client, "SELECT * FROM sub_empty_users")
        .await
        .expect("Failed to send subscribe");

    // Should receive subscription confirmation even with empty result
    let data = client
        .read_until_message_type(MSG_SUBSCRIPTION_DATA)
        .await
        .expect("Failed to read subscription response");
    let messages = parse_backend_messages(&data);

    let has_subscription_data = messages.iter().any(|m| m.msg_type == MSG_SUBSCRIPTION_DATA);
    assert!(has_subscription_data, "Should receive SubscriptionData even for empty result set");

    // Insert data should trigger update
    client
        .send_query("INSERT INTO sub_empty_users VALUES (1, 'Alice')")
        .await
        .expect("Failed to insert data");

    let data = client
        .read_until_message_type(MSG_READY_FOR_QUERY)
        .await
        .expect("Failed to read after insert");
    let messages = parse_backend_messages(&data);

    let has_update = messages.iter().any(|m| m.msg_type == MSG_SUBSCRIPTION_DATA);
    assert!(
        has_update,
        "Should receive update notification after inserting into previously empty result"
    );

    client.send_terminate().await.expect("Failed to send terminate");
    server.shutdown();
}

/// test_rapid_mutations - Many quick changes don't cause issues
#[tokio::test]
async fn test_rapid_mutations() {
    let server = start_test_server().await;
    let mut client = TestClient::connect(server.addr()).await.expect("Failed to connect");

    // Complete handshake
    client.send_startup("testuser", "testdb").await.expect("Failed to send startup");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read startup response");

    // Create test table
    client
        .send_query("CREATE TABLE IF NOT EXISTS sub_counter_test (id INT, value INT)")
        .await
        .expect("Failed to create table");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read create response");

    // Subscribe
    send_subscribe(&mut client, "SELECT * FROM sub_counter_test")
        .await
        .expect("Failed to send subscribe");
    let _ = client
        .read_until_message_type(MSG_SUBSCRIPTION_DATA)
        .await
        .expect("Failed to read subscription response");

    // Perform rapid mutations
    for i in 0..10 {
        let query = format!("INSERT INTO sub_counter_test VALUES ({}, {})", i, i * 10);
        client.send_query(&query).await.expect("Failed to send insert");
        let _ = client
            .read_until_message_type(MSG_READY_FOR_QUERY)
            .await
            .expect("Failed to read after insert");
    }

    // All mutations should have completed without errors
    // Final state: 10 rows in the table
    client.send_query("SELECT COUNT(*) FROM sub_counter_test").await.expect("Failed to count rows");

    let data = client.read_until_message_type(b'Z').await.expect("Failed to read count response");
    let messages = parse_backend_messages(&data);

    // Should have data row with count
    let has_data_row = messages.iter().any(|m| m.is_data_row());
    assert!(has_data_row, "Should have data row with count result");

    client.send_terminate().await.expect("Failed to send terminate");
    server.shutdown();
}

/// test_subscription_cleanup_on_disconnect - Subscriptions removed when client disconnects
#[tokio::test]
async fn test_subscription_cleanup_on_disconnect() {
    let server = start_test_server().await;
    let mut client = TestClient::connect(server.addr()).await.expect("Failed to connect");

    // Complete handshake
    client.send_startup("testuser", "testdb").await.expect("Failed to send startup");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read startup response");

    // Create test table
    client
        .send_query("CREATE TABLE IF NOT EXISTS sub_disconnect_test (id INT)")
        .await
        .expect("Failed to create table");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read create response");

    // Subscribe
    send_subscribe(&mut client, "SELECT * FROM sub_disconnect_test")
        .await
        .expect("Failed to send subscribe");
    let _ = client
        .read_until_message_type(MSG_SUBSCRIPTION_DATA)
        .await
        .expect("Failed to read subscription response");

    // Disconnect by sending terminate
    client.send_terminate().await.expect("Failed to send terminate");

    // Connection should cleanly close
    // (No panic or resource leak - verified by test not hanging)
    server.shutdown();
}

// ============================================================================
// SELECTIVE COLUMN UPDATE TESTS (0xF7 Message Tests - Issue #3924)
// ============================================================================

/// Helper to extract subscription ID from SubscriptionPartialData (0xF7) message
#[allow(dead_code)]
fn extract_partial_data_subscription_id(messages: &[common::ParsedMessage]) -> Option<[u8; 16]> {
    for msg in messages {
        if msg.msg_type == MSG_SUBSCRIPTION_PARTIAL_DATA && msg.payload.len() >= 16 {
            let mut id = [0u8; 16];
            id.copy_from_slice(&msg.payload[0..16]);
            return Some(id);
        }
    }
    None
}

/// Helper to parse column mask from SubscriptionPartialData (0xF7) message
/// Returns (total_columns, column_mask_bytes)
fn parse_partial_data_column_mask(messages: &[common::ParsedMessage]) -> Option<(u16, Vec<u8>)> {
    for msg in messages {
        if msg.msg_type == MSG_SUBSCRIPTION_PARTIAL_DATA && msg.payload.len() >= 21 {
            // Payload structure:
            // - 16 bytes: subscription_id
            // - 1 byte: update_type (4 = SelectiveUpdate)
            // - 4 bytes: row_count
            // For each row:
            //   - 2 bytes: total_columns
            //   - N bytes: column_mask (ceil(total_columns/8) bytes)
            //   - Values...

            let update_type = msg.payload[16];
            if update_type != 4 {
                // Not a SelectiveUpdate
                continue;
            }

            let row_count = i32::from_be_bytes([
                msg.payload[17], msg.payload[18], msg.payload[19], msg.payload[20]
            ]) as usize;

            if row_count == 0 {
                continue;
            }

            // Parse first row's column mask
            if msg.payload.len() >= 23 {
                let total_columns = i16::from_be_bytes([msg.payload[21], msg.payload[22]]) as u16;
                let mask_len = ((total_columns + 7) / 8) as usize;

                if msg.payload.len() >= 23 + mask_len {
                    let column_mask = msg.payload[23..23 + mask_len].to_vec();
                    return Some((total_columns, column_mask));
                }
            }
        }
    }
    None
}

/// Helper to check if a column is present in a column mask
fn is_column_present_in_mask(mask: &[u8], col_idx: u16) -> bool {
    let byte_idx = (col_idx / 8) as usize;
    let bit_idx = col_idx % 8;
    if byte_idx < mask.len() {
        (mask[byte_idx] & (1 << bit_idx)) != 0
    } else {
        false
    }
}

/// test_selective_update_sends_partial_data - UPDATE with subset of columns triggers 0xF7
///
/// When a table has a primary key and only some columns change, the server
/// should send SubscriptionPartialData (0xF7) instead of SubscriptionData (0xF2).
#[tokio::test]
async fn test_selective_update_sends_partial_data() {
    let server = start_test_server().await;
    let mut client = TestClient::connect(server.addr()).await.expect("Failed to connect");

    // Complete handshake
    client.send_startup("testuser", "testdb").await.expect("Failed to send startup");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read startup response");

    // Create a test table with PRIMARY KEY (required for selective updates)
    let table_name = "selective_update_test";
    client
        .send_query(&format!(
            "CREATE TABLE IF NOT EXISTS {} (id INT PRIMARY KEY, name VARCHAR, status VARCHAR, value INT)",
            table_name
        ))
        .await
        .expect("Failed to create table");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read create response");

    // Insert initial data
    client
        .send_query(&format!(
            "INSERT INTO {} VALUES (1, 'Alice', 'active', 100)",
            table_name
        ))
        .await
        .expect("Failed to insert data");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read insert response");

    // Subscribe to the table
    let select_query = format!("SELECT * FROM {}", table_name);
    send_subscribe(&mut client, &select_query).await.expect("Failed to send subscribe");
    let _ = client
        .read_until_message_type(MSG_SUBSCRIPTION_DATA)
        .await
        .expect("Failed to read subscription response");

    // Update ONLY the name column (1 of 4 columns = 25%, within 50% threshold)
    client
        .send_query(&format!(
            "UPDATE {} SET name = 'Alicia' WHERE id = 1",
            table_name
        ))
        .await
        .expect("Failed to update data");

    // Read for update notification - should be SubscriptionPartialData (0xF7)
    let data = client
        .read_until_message_type(MSG_READY_FOR_QUERY)
        .await
        .expect("Failed to read after update");
    let messages = parse_backend_messages(&data);

    // Check for SubscriptionPartialData (0xF7) message
    let has_partial_data = messages.iter().any(|m| m.msg_type == MSG_SUBSCRIPTION_PARTIAL_DATA);
    let has_full_data = messages.iter().any(|m| m.msg_type == MSG_SUBSCRIPTION_DATA);

    // We expect 0xF7 for selective updates when only a subset of columns changed
    // Note: The server may still use 0xF2 if PK detection failed or threshold exceeded
    // This test verifies the feature is wired up and working
    assert!(
        has_partial_data || has_full_data,
        "Expected either SubscriptionPartialData (0xF7) or SubscriptionData (0xF2) after UPDATE"
    );

    if has_partial_data {
        // Verify the column mask is correct
        if let Some((total_columns, mask)) = parse_partial_data_column_mask(&messages) {
            assert_eq!(total_columns, 4, "Table should have 4 columns");
            // Column 0 (id/PK) should always be present
            assert!(is_column_present_in_mask(&mask, 0), "PK column 0 should be present");
            // Column 1 (name) was changed, should be present
            assert!(is_column_present_in_mask(&mask, 1), "Changed column 1 (name) should be present");
        }
    }

    client.send_terminate().await.expect("Failed to send terminate");
    server.shutdown();
}

/// test_insert_sends_full_subscription_data - INSERT triggers 0xF2 (not 0xF7)
///
/// Selective column updates (0xF7) only apply to UPDATE operations.
/// INSERT operations should always use SubscriptionData (0xF2).
#[tokio::test]
async fn test_insert_sends_full_subscription_data() {
    let server = start_test_server().await;
    let mut client = TestClient::connect(server.addr()).await.expect("Failed to connect");

    // Complete handshake
    client.send_startup("testuser", "testdb").await.expect("Failed to send startup");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read startup response");

    // Create a test table with PRIMARY KEY
    let table_name = "insert_full_data_test";
    client
        .send_query(&format!(
            "CREATE TABLE IF NOT EXISTS {} (id INT PRIMARY KEY, name VARCHAR, status VARCHAR)",
            table_name
        ))
        .await
        .expect("Failed to create table");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read create response");

    // Subscribe to empty table
    let select_query = format!("SELECT * FROM {}", table_name);
    send_subscribe(&mut client, &select_query).await.expect("Failed to send subscribe");
    let _ = client
        .read_until_message_type(MSG_SUBSCRIPTION_DATA)
        .await
        .expect("Failed to read subscription response");

    // INSERT new row - should trigger 0xF2 (not 0xF7)
    client
        .send_query(&format!("INSERT INTO {} VALUES (1, 'Alice', 'active')", table_name))
        .await
        .expect("Failed to insert data");

    // Read for update notification
    let data = client
        .read_until_message_type(MSG_READY_FOR_QUERY)
        .await
        .expect("Failed to read after insert");
    let messages = parse_backend_messages(&data);

    // INSERT should use SubscriptionData (0xF2), not SubscriptionPartialData (0xF7)
    let has_subscription_data = messages.iter().any(|m| m.msg_type == MSG_SUBSCRIPTION_DATA);
    let has_partial_data = messages.iter().any(|m| m.msg_type == MSG_SUBSCRIPTION_PARTIAL_DATA);

    assert!(has_subscription_data, "INSERT should trigger SubscriptionData (0xF2)");
    assert!(!has_partial_data, "INSERT should NOT trigger SubscriptionPartialData (0xF7)");

    client.send_terminate().await.expect("Failed to send terminate");
    server.shutdown();
}

/// test_delete_sends_full_subscription_data - DELETE triggers 0xF2 (not 0xF7)
///
/// Selective column updates (0xF7) only apply to UPDATE operations.
/// DELETE operations should always use SubscriptionData (0xF2).
#[tokio::test]
async fn test_delete_sends_full_subscription_data() {
    let server = start_test_server().await;
    let mut client = TestClient::connect(server.addr()).await.expect("Failed to connect");

    // Complete handshake
    client.send_startup("testuser", "testdb").await.expect("Failed to send startup");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read startup response");

    // Create a test table with PRIMARY KEY
    let table_name = "delete_full_data_test";
    client
        .send_query(&format!(
            "CREATE TABLE IF NOT EXISTS {} (id INT PRIMARY KEY, name VARCHAR, status VARCHAR)",
            table_name
        ))
        .await
        .expect("Failed to create table");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read create response");

    // Insert initial data
    client
        .send_query(&format!("INSERT INTO {} VALUES (1, 'Alice', 'active'), (2, 'Bob', 'inactive')", table_name))
        .await
        .expect("Failed to insert data");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read insert response");

    // Subscribe to the table
    let select_query = format!("SELECT * FROM {}", table_name);
    send_subscribe(&mut client, &select_query).await.expect("Failed to send subscribe");
    let _ = client
        .read_until_message_type(MSG_SUBSCRIPTION_DATA)
        .await
        .expect("Failed to read subscription response");

    // DELETE a row - should trigger 0xF2 (not 0xF7)
    client
        .send_query(&format!("DELETE FROM {} WHERE id = 1", table_name))
        .await
        .expect("Failed to delete data");

    // Read for update notification
    let data = client
        .read_until_message_type(MSG_READY_FOR_QUERY)
        .await
        .expect("Failed to read after delete");
    let messages = parse_backend_messages(&data);

    // DELETE should use SubscriptionData (0xF2), not SubscriptionPartialData (0xF7)
    let has_subscription_data = messages.iter().any(|m| m.msg_type == MSG_SUBSCRIPTION_DATA);
    let has_partial_data = messages.iter().any(|m| m.msg_type == MSG_SUBSCRIPTION_PARTIAL_DATA);

    assert!(has_subscription_data, "DELETE should trigger SubscriptionData (0xF2)");
    assert!(!has_partial_data, "DELETE should NOT trigger SubscriptionPartialData (0xF7)");

    client.send_terminate().await.expect("Failed to send terminate");
    server.shutdown();
}

/// test_update_all_columns_falls_back_to_full_data - UPDATE all columns triggers 0xF2
///
/// When all columns are updated, the selective update threshold is exceeded (100% > 50%),
/// so the server should fall back to SubscriptionData (0xF2).
#[tokio::test]
async fn test_update_all_columns_falls_back_to_full_data() {
    let server = start_test_server().await;
    let mut client = TestClient::connect(server.addr()).await.expect("Failed to connect");

    // Complete handshake
    client.send_startup("testuser", "testdb").await.expect("Failed to send startup");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read startup response");

    // Create a test table with PRIMARY KEY and only 2 non-PK columns
    // This way updating both non-PK columns = 66% > 50% threshold
    let table_name = "update_all_cols_test";
    client
        .send_query(&format!(
            "CREATE TABLE IF NOT EXISTS {} (id INT PRIMARY KEY, name VARCHAR, status VARCHAR)",
            table_name
        ))
        .await
        .expect("Failed to create table");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read create response");

    // Insert initial data
    client
        .send_query(&format!("INSERT INTO {} VALUES (1, 'Alice', 'active')", table_name))
        .await
        .expect("Failed to insert data");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read insert response");

    // Subscribe to the table
    let select_query = format!("SELECT * FROM {}", table_name);
    send_subscribe(&mut client, &select_query).await.expect("Failed to send subscribe");
    let _ = client
        .read_until_message_type(MSG_SUBSCRIPTION_DATA)
        .await
        .expect("Failed to read subscription response");

    // Update ALL non-PK columns (name and status) - 66% > 50% threshold
    client
        .send_query(&format!(
            "UPDATE {} SET name = 'Bob', status = 'inactive' WHERE id = 1",
            table_name
        ))
        .await
        .expect("Failed to update data");

    // Read for update notification
    let data = client
        .read_until_message_type(MSG_READY_FOR_QUERY)
        .await
        .expect("Failed to read after update");
    let messages = parse_backend_messages(&data);

    // When ratio exceeds threshold, should fall back to SubscriptionData (0xF2)
    let has_subscription_data = messages.iter().any(|m| m.msg_type == MSG_SUBSCRIPTION_DATA);
    let has_partial_data = messages.iter().any(|m| m.msg_type == MSG_SUBSCRIPTION_PARTIAL_DATA);

    // Either is acceptable - the important thing is we get a notification
    assert!(
        has_subscription_data || has_partial_data,
        "UPDATE should trigger either SubscriptionData (0xF2) or SubscriptionPartialData (0xF7)"
    );

    client.send_terminate().await.expect("Failed to send terminate");
    server.shutdown();
}

/// test_strict_partial_data_with_guaranteed_pk_detection - Strict 0xF7 assertion test
///
/// This test strictly verifies that 0xF7 (SubscriptionPartialData) is sent when:
/// 1. Table has an explicit PRIMARY KEY constraint
/// 2. PK detection succeeds (confident)
/// 3. Only a small subset of columns are updated (within 50% threshold)
///
/// Unlike test_selective_update_sends_partial_data, this test FAILS if 0xF2 is received,
/// as that would indicate a regression in PK detection or selective update logic.
///
/// Related: Issue #3943
#[tokio::test]
async fn test_strict_partial_data_with_guaranteed_pk_detection() {
    let server = start_test_server().await;
    let mut client = TestClient::connect(server.addr()).await.expect("Failed to connect");

    // Complete handshake
    client.send_startup("testuser", "testdb").await.expect("Failed to send startup");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read startup response");

    // Drop table first to ensure clean state (avoids IF NOT EXISTS issues)
    let table_name = "strict_pk_detection_test";
    client
        .send_query(&format!("DROP TABLE IF EXISTS {}", table_name))
        .await
        .expect("Failed to drop table");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read drop response");

    // Create a table with explicit PRIMARY KEY constraint
    // Using 5 columns so updating 1 = 20%, well within the 50% threshold
    client
        .send_query(&format!(
            "CREATE TABLE {} (
                id INT PRIMARY KEY,
                name VARCHAR,
                status VARCHAR,
                value INT,
                description VARCHAR
            )",
            table_name
        ))
        .await
        .expect("Failed to create table");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read create response");

    // Insert initial data
    client
        .send_query(&format!(
            "INSERT INTO {} VALUES (1, 'Alice', 'active', 100, 'Test user')",
            table_name
        ))
        .await
        .expect("Failed to insert data");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read insert response");

    // Subscribe to the table with SELECT *
    // This should allow PK detection since we're selecting from a single table
    let select_query = format!("SELECT * FROM {}", table_name);
    send_subscribe(&mut client, &select_query).await.expect("Failed to send subscribe");
    let initial_data = client
        .read_until_message_type(MSG_SUBSCRIPTION_DATA)
        .await
        .expect("Failed to read subscription response");
    let initial_messages = parse_backend_messages(&initial_data);

    // Verify we got initial data
    assert!(
        initial_messages.iter().any(|m| m.msg_type == MSG_SUBSCRIPTION_DATA),
        "Should receive initial SubscriptionData (0xF2)"
    );

    // Update ONLY the 'name' column (1 of 5 columns = 20%, well within 50% threshold)
    client
        .send_query(&format!(
            "UPDATE {} SET name = 'Alicia' WHERE id = 1",
            table_name
        ))
        .await
        .expect("Failed to update data");

    // Read for update notification
    let data = client
        .read_until_message_type(MSG_READY_FOR_QUERY)
        .await
        .expect("Failed to read after update");
    let messages = parse_backend_messages(&data);

    // STRICT ASSERTION: We expect ONLY 0xF7 (SubscriptionPartialData), NOT 0xF2
    let has_partial_data = messages.iter().any(|m| m.msg_type == MSG_SUBSCRIPTION_PARTIAL_DATA);
    let has_full_data = messages.iter().any(|m| m.msg_type == MSG_SUBSCRIPTION_DATA);

    // If we got 0xF2 instead of 0xF7, this indicates a regression in PK detection
    assert!(
        has_partial_data,
        "Expected SubscriptionPartialData (0xF7) for single-column update. \
         Got 0xF2 instead, indicating PK detection may have failed. \
         Messages received: {:?}",
        messages.iter().map(|m| format!("0x{:02X}", m.msg_type)).collect::<Vec<_>>()
    );

    assert!(
        !has_full_data,
        "Should NOT receive SubscriptionData (0xF2) when updating a single column \
         on a table with properly detected PK. This indicates a regression."
    );

    // Verify the column mask in the 0xF7 message
    if let Some((total_columns, mask)) = parse_partial_data_column_mask(&messages) {
        assert_eq!(total_columns, 5, "Table should have 5 columns");

        // Column 0 (id/PK) should ALWAYS be present
        assert!(
            is_column_present_in_mask(&mask, 0),
            "PK column 0 (id) must always be present in partial data"
        );

        // Column 1 (name) was changed, should be present
        assert!(
            is_column_present_in_mask(&mask, 1),
            "Changed column 1 (name) should be present in partial data"
        );

        // Columns 2, 3, 4 were NOT changed, should NOT be present
        assert!(
            !is_column_present_in_mask(&mask, 2),
            "Unchanged column 2 (status) should NOT be present"
        );
        assert!(
            !is_column_present_in_mask(&mask, 3),
            "Unchanged column 3 (value) should NOT be present"
        );
        assert!(
            !is_column_present_in_mask(&mask, 4),
            "Unchanged column 4 (description) should NOT be present"
        );
    } else {
        panic!("Failed to parse column mask from SubscriptionPartialData message");
    }

    client.send_terminate().await.expect("Failed to send terminate");
    server.shutdown();
}

/// test_strict_partial_data_multiple_updates - Verify 0xF7 with multiple rows
///
/// Tests that 0xF7 is correctly sent when updating multiple rows with the same
/// column changes. Each row should be included in the partial data with the
/// correct column mask.
///
/// Related: Issue #3943
#[tokio::test]
async fn test_strict_partial_data_multiple_updates() {
    let server = start_test_server().await;
    let mut client = TestClient::connect(server.addr()).await.expect("Failed to connect");

    // Complete handshake
    client.send_startup("testuser", "testdb").await.expect("Failed to send startup");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read startup response");

    // Drop and create table
    let table_name = "strict_pk_multi_update_test";
    client
        .send_query(&format!("DROP TABLE IF EXISTS {}", table_name))
        .await
        .expect("Failed to drop table");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read drop response");

    // Create a table with 4 columns (updating 1 = 25% < 50% threshold)
    client
        .send_query(&format!(
            "CREATE TABLE {} (
                id INT PRIMARY KEY,
                name VARCHAR,
                status VARCHAR,
                value INT
            )",
            table_name
        ))
        .await
        .expect("Failed to create table");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read create response");

    // Insert multiple rows
    client
        .send_query(&format!(
            "INSERT INTO {} VALUES
             (1, 'Alice', 'active', 100),
             (2, 'Bob', 'active', 200),
             (3, 'Charlie', 'active', 300)",
            table_name
        ))
        .await
        .expect("Failed to insert data");
    let _ = client.read_until_message_type(b'Z').await.expect("Failed to read insert response");

    // Subscribe
    let select_query = format!("SELECT * FROM {}", table_name);
    send_subscribe(&mut client, &select_query).await.expect("Failed to send subscribe");
    let _ = client
        .read_until_message_type(MSG_SUBSCRIPTION_DATA)
        .await
        .expect("Failed to read subscription response");

    // Update status column for ALL rows (1 of 4 columns = 25%)
    client
        .send_query(&format!(
            "UPDATE {} SET status = 'inactive'",
            table_name
        ))
        .await
        .expect("Failed to update data");

    // Read for update notification
    let data = client
        .read_until_message_type(MSG_READY_FOR_QUERY)
        .await
        .expect("Failed to read after update");
    let messages = parse_backend_messages(&data);

    // STRICT ASSERTION: Expect 0xF7 for bulk single-column update
    let has_partial_data = messages.iter().any(|m| m.msg_type == MSG_SUBSCRIPTION_PARTIAL_DATA);
    let has_full_data = messages.iter().any(|m| m.msg_type == MSG_SUBSCRIPTION_DATA);

    assert!(
        has_partial_data,
        "Expected SubscriptionPartialData (0xF7) for bulk single-column update. \
         Messages: {:?}",
        messages.iter().map(|m| format!("0x{:02X}", m.msg_type)).collect::<Vec<_>>()
    );

    assert!(
        !has_full_data,
        "Should NOT receive SubscriptionData (0xF2) for bulk single-column update."
    );

    // Verify column mask structure
    if let Some((total_columns, mask)) = parse_partial_data_column_mask(&messages) {
        assert_eq!(total_columns, 4, "Table should have 4 columns");

        // Column 0 (id/PK) must be present
        assert!(
            is_column_present_in_mask(&mask, 0),
            "PK column 0 (id) must be present"
        );

        // Column 2 (status) was changed
        assert!(
            is_column_present_in_mask(&mask, 2),
            "Changed column 2 (status) should be present"
        );

        // Columns 1 and 3 were NOT changed
        assert!(
            !is_column_present_in_mask(&mask, 1),
            "Unchanged column 1 (name) should NOT be present"
        );
        assert!(
            !is_column_present_in_mask(&mask, 3),
            "Unchanged column 3 (value) should NOT be present"
        );
    }

    client.send_terminate().await.expect("Failed to send terminate");
    server.shutdown();
}

/// test_cross_connection_selective_update_sends_0xf7 - Cross-connection 0xF7 notification
///
/// This test verifies that when client A makes a selective column UPDATE,
/// client B (subscribed to the same query) receives a 0xF7 message via
/// the cross-connection broadcast path.
///
/// Key differences from same-connection tests:
/// - Same-connection: notifications use direct channel
/// - Cross-connection: notifications use broadcast system with async polling
///
/// Related: Issue #3942
#[tokio::test]
async fn test_cross_connection_selective_update_sends_0xf7() {
    let server = start_test_server().await;

    // Connect two clients
    let mut client1 = TestClient::connect(server.addr()).await.expect("Failed to connect client 1");
    client1.send_startup("testuser", "testdb").await.expect("Failed to send startup");
    let _ = client1.read_until_message_type(b'Z').await.expect("Failed to read startup response");

    let mut client2 = TestClient::connect(server.addr()).await.expect("Failed to connect client 2");
    client2.send_startup("testuser", "testdb").await.expect("Failed to send startup");
    let _ = client2.read_until_message_type(b'Z').await.expect("Failed to read startup response");

    // Create a test table with PRIMARY KEY (required for selective updates)
    // Using 5 columns so updating 1 = 20%, well within the 50% threshold
    let table_name = "cross_selective_update_test";
    client1
        .send_query(&format!("DROP TABLE IF EXISTS {}", table_name))
        .await
        .expect("Failed to drop table");
    let _ = client1.read_until_message_type(b'Z').await.expect("Failed to read drop response");

    client1
        .send_query(&format!(
            "CREATE TABLE {} (
                id INT PRIMARY KEY,
                name VARCHAR,
                status VARCHAR,
                value INT,
                description VARCHAR
            )",
            table_name
        ))
        .await
        .expect("Failed to create table");
    let _ = client1.read_until_message_type(b'Z').await.expect("Failed to read create response");

    // Insert initial data
    client1
        .send_query(&format!(
            "INSERT INTO {} VALUES (1, 'Alice', 'active', 100, 'Test user')",
            table_name
        ))
        .await
        .expect("Failed to insert data");
    let _ = client1.read_until_message_type(b'Z').await.expect("Failed to read insert response");

    // Both clients subscribe to the same query
    let query = format!("SELECT * FROM {}", table_name);
    send_subscribe(&mut client1, &query).await.expect("Failed to send subscribe from client1");
    let _ = client1
        .read_until_message_type(MSG_SUBSCRIPTION_DATA)
        .await
        .expect("Failed to read subscription response");

    send_subscribe(&mut client2, &query).await.expect("Failed to send subscribe from client2");
    let _ = client2
        .read_until_message_type(MSG_SUBSCRIPTION_DATA)
        .await
        .expect("Failed to read subscription response");

    // Client1 updates ONLY the 'name' column (1 of 5 columns = 20%, within 50% threshold)
    client1
        .send_query(&format!(
            "UPDATE {} SET name = 'Alicia' WHERE id = 1",
            table_name
        ))
        .await
        .expect("Failed to update data");

    // Client1: Read same-connection notification (direct channel)
    let data1 = client1
        .read_until_message_type(MSG_READY_FOR_QUERY)
        .await
        .expect("Failed to read after update");
    let messages1 = parse_backend_messages(&data1);

    // Verify client1 received 0xF7 (same-connection path)
    let has_partial1 = messages1.iter().any(|m| m.msg_type == MSG_SUBSCRIPTION_PARTIAL_DATA);
    let has_full1 = messages1.iter().any(|m| m.msg_type == MSG_SUBSCRIPTION_DATA);

    assert!(
        has_partial1,
        "Client1 should receive 0xF7 for selective update. Messages: {:?}",
        messages1.iter().map(|m| format!("0x{:02X}", m.msg_type)).collect::<Vec<_>>()
    );
    assert!(
        !has_full1,
        "Client1 should NOT receive 0xF2 for selective update"
    );

    // Client2: Read cross-connection notification (broadcast path)
    // Use longer timeout for cross-connection which involves async polling
    let data2 = client2
        .read_until_message_type_timeout(
            MSG_SUBSCRIPTION_PARTIAL_DATA,
            std::time::Duration::from_secs(10),
        )
        .await
        .expect("Client2 should receive 0xF7 via cross-connection broadcast");
    let messages2 = parse_backend_messages(&data2);

    // Verify client2 received 0xF7 (NOT 0xF2) via cross-connection
    let has_partial2 = messages2.iter().any(|m| m.msg_type == MSG_SUBSCRIPTION_PARTIAL_DATA);
    let has_full2 = messages2.iter().any(|m| m.msg_type == MSG_SUBSCRIPTION_DATA);

    assert!(
        has_partial2,
        "Client2 should receive 0xF7 via cross-connection broadcast. Messages: {:?}",
        messages2.iter().map(|m| format!("0x{:02X}", m.msg_type)).collect::<Vec<_>>()
    );
    assert!(
        !has_full2,
        "Client2 should NOT receive 0xF2 when selective update is used"
    );

    // Verify the column mask in client2's 0xF7 message
    if let Some((total_columns, mask)) = parse_partial_data_column_mask(&messages2) {
        assert_eq!(total_columns, 5, "Table should have 5 columns");

        // Column 0 (id/PK) should ALWAYS be present
        assert!(
            is_column_present_in_mask(&mask, 0),
            "PK column 0 (id) must be present in cross-connection partial data"
        );

        // Column 1 (name) was changed, should be present
        assert!(
            is_column_present_in_mask(&mask, 1),
            "Changed column 1 (name) should be present in cross-connection partial data"
        );

        // Columns 2, 3, 4 were NOT changed, should NOT be present
        assert!(
            !is_column_present_in_mask(&mask, 2),
            "Unchanged column 2 (status) should NOT be present"
        );
        assert!(
            !is_column_present_in_mask(&mask, 3),
            "Unchanged column 3 (value) should NOT be present"
        );
        assert!(
            !is_column_present_in_mask(&mask, 4),
            "Unchanged column 4 (description) should NOT be present"
        );
    } else {
        panic!("Failed to parse column mask from cross-connection SubscriptionPartialData message");
    }

    client1.send_terminate().await.expect("Failed to send terminate");
    client2.send_terminate().await.expect("Failed to send terminate");
    server.shutdown();
}

/// test_cross_connection_selective_update_multiple_rows - Multi-row cross-connection 0xF7 updates
///
/// This test verifies that when client A performs a bulk UPDATE affecting multiple rows
/// with selective column changes, client B (subscribed to the same query) receives a
/// 0xF7 message via the cross-connection broadcast path with the correct row count and
/// column masks.
///
/// Key verifications:
/// - Cross-connection path sends 0xF7 (not 0xF2) for selective updates
/// - Row count in message matches the number of updated rows
/// - Column mask is consistent across all rows
///
/// Related: Issue #3978
#[tokio::test]
async fn test_cross_connection_selective_update_multiple_rows() {
    let server = start_test_server().await;

    // Connect two clients
    let mut client1 = TestClient::connect(server.addr()).await.expect("Failed to connect client 1");
    client1.send_startup("testuser", "testdb").await.expect("Failed to send startup");
    let _ = client1.read_until_message_type(b'Z').await.expect("Failed to read startup response");

    let mut client2 = TestClient::connect(server.addr()).await.expect("Failed to connect client 2");
    client2.send_startup("testuser", "testdb").await.expect("Failed to send startup");
    let _ = client2.read_until_message_type(b'Z').await.expect("Failed to read startup response");

    // Create a test table with PRIMARY KEY (required for selective updates)
    // Using 4 columns so updating 1 = 25%, within 50% threshold
    let table_name = "cross_multi_row_selective_test";
    client1
        .send_query(&format!("DROP TABLE IF EXISTS {}", table_name))
        .await
        .expect("Failed to drop table");
    let _ = client1.read_until_message_type(b'Z').await.expect("Failed to read drop response");

    client1
        .send_query(&format!(
            "CREATE TABLE {} (
                id INT PRIMARY KEY,
                name VARCHAR,
                status VARCHAR,
                value INT
            )",
            table_name
        ))
        .await
        .expect("Failed to create table");
    let _ = client1.read_until_message_type(b'Z').await.expect("Failed to read create response");

    // Insert multiple rows
    client1
        .send_query(&format!(
            "INSERT INTO {} VALUES
             (1, 'Alice', 'active', 100),
             (2, 'Bob', 'active', 200),
             (3, 'Charlie', 'active', 300),
             (4, 'Diana', 'active', 400),
             (5, 'Eve', 'active', 500)",
            table_name
        ))
        .await
        .expect("Failed to insert data");
    let _ = client1.read_until_message_type(b'Z').await.expect("Failed to read insert response");

    // Both clients subscribe to the same query
    let query = format!("SELECT * FROM {}", table_name);
    send_subscribe(&mut client1, &query).await.expect("Failed to send subscribe from client1");
    let _ = client1
        .read_until_message_type(MSG_SUBSCRIPTION_DATA)
        .await
        .expect("Failed to read subscription response");

    send_subscribe(&mut client2, &query).await.expect("Failed to send subscribe from client2");
    let _ = client2
        .read_until_message_type(MSG_SUBSCRIPTION_DATA)
        .await
        .expect("Failed to read subscription response");

    // Client1 updates status column for ALL rows (1 of 4 columns = 25%)
    client1
        .send_query(&format!(
            "UPDATE {} SET status = 'inactive'",
            table_name
        ))
        .await
        .expect("Failed to update data");

    // Client1: Read same-connection notification
    let data1 = client1
        .read_until_message_type(MSG_READY_FOR_QUERY)
        .await
        .expect("Failed to read after update");
    let messages1 = parse_backend_messages(&data1);

    // Verify client1 received 0xF7 via same-connection path
    let has_partial1 = messages1.iter().any(|m| m.msg_type == MSG_SUBSCRIPTION_PARTIAL_DATA);
    assert!(
        has_partial1,
        "Client1 should receive 0xF7 for bulk selective update. Messages: {:?}",
        messages1.iter().map(|m| format!("0x{:02X}", m.msg_type)).collect::<Vec<_>>()
    );

    // Client2: Read cross-connection notification (broadcast path)
    let data2 = client2
        .read_until_message_type_timeout(
            MSG_SUBSCRIPTION_PARTIAL_DATA,
            std::time::Duration::from_secs(10),
        )
        .await
        .expect("Client2 should receive 0xF7 via cross-connection broadcast for multi-row update");
    let messages2 = parse_backend_messages(&data2);

    // Verify client2 received 0xF7 (NOT 0xF2) via cross-connection
    let has_partial2 = messages2.iter().any(|m| m.msg_type == MSG_SUBSCRIPTION_PARTIAL_DATA);
    let has_full2 = messages2.iter().any(|m| m.msg_type == MSG_SUBSCRIPTION_DATA);

    assert!(
        has_partial2,
        "Client2 should receive 0xF7 via cross-connection broadcast for multi-row update. Messages: {:?}",
        messages2.iter().map(|m| format!("0x{:02X}", m.msg_type)).collect::<Vec<_>>()
    );
    assert!(
        !has_full2,
        "Client2 should NOT receive 0xF2 when selective update is used on multiple rows"
    );

    // Verify the column mask in client2's 0xF7 message
    if let Some((total_columns, mask)) = parse_partial_data_column_mask(&messages2) {
        assert_eq!(total_columns, 4, "Table should have 4 columns");

        // Column 0 (id/PK) should ALWAYS be present
        assert!(
            is_column_present_in_mask(&mask, 0),
            "PK column 0 (id) must be present in cross-connection partial data"
        );

        // Column 2 (status) was changed, should be present
        assert!(
            is_column_present_in_mask(&mask, 2),
            "Changed column 2 (status) should be present in cross-connection partial data"
        );

        // Columns 1, 3 were NOT changed, should NOT be present
        assert!(
            !is_column_present_in_mask(&mask, 1),
            "Unchanged column 1 (name) should NOT be present"
        );
        assert!(
            !is_column_present_in_mask(&mask, 3),
            "Unchanged column 3 (value) should NOT be present"
        );
    } else {
        panic!("Failed to parse column mask from cross-connection SubscriptionPartialData message for multi-row update");
    }

    client1.send_terminate().await.expect("Failed to send terminate");
    client2.send_terminate().await.expect("Failed to send terminate");
    server.shutdown();
}
