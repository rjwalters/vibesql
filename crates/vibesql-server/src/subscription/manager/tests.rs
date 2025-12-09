//! Tests for subscription manager functionality.

use std::collections::HashSet;

use tokio::sync::mpsc;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

use super::SubscriptionManager;
use crate::subscription::{
    Subscription, SubscriptionConfig, SubscriptionError, SubscriptionId, SubscriptionUpdate,
};

fn setup_test_db() -> Database {
    let mut db = Database::new();

    // Create test tables
    let create_users = vibesql_parser::Parser::parse_sql(
        "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100), active BOOLEAN)",
    )
    .unwrap();
    if let vibesql_ast::Statement::CreateTable(stmt) = create_users {
        vibesql_executor::CreateTableExecutor::execute(&stmt, &mut db).unwrap();
    }

    let create_orders = vibesql_parser::Parser::parse_sql(
        "CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, amount INT)",
    )
    .unwrap();
    if let vibesql_ast::Statement::CreateTable(stmt) = create_orders {
        vibesql_executor::CreateTableExecutor::execute(&stmt, &mut db).unwrap();
    }

    db
}

#[test]
fn test_subscribe_simple() {
    let manager = SubscriptionManager::new();
    let (tx, _rx) = mpsc::channel(16);

    let result = manager.subscribe("SELECT * FROM users".to_string(), tx);
    assert!(result.is_ok());

    let _id = result.unwrap();
    assert_eq!(manager.subscription_count(), 1);

    // Check table index
    let watched = manager.watched_tables();
    assert_eq!(watched.len(), 1);
    assert!(watched.iter().any(|(t, c)| t == "users" && *c == 1));
}

#[test]
fn test_subscribe_with_join() {
    let manager = SubscriptionManager::new();
    let (tx, _rx) = mpsc::channel(16);

    let result = manager
        .subscribe("SELECT * FROM users u JOIN orders o ON u.id = o.user_id".to_string(), tx);
    assert!(result.is_ok());

    // Should be indexed under both tables
    let watched = manager.watched_tables();
    assert_eq!(watched.len(), 2);
    assert!(watched.iter().any(|(t, _)| t == "users"));
    assert!(watched.iter().any(|(t, _)| t == "orders"));
}

#[test]
fn test_unsubscribe() {
    let manager = SubscriptionManager::new();
    let (tx, _rx) = mpsc::channel(16);

    let id = manager.subscribe("SELECT * FROM users".to_string(), tx).unwrap();
    assert_eq!(manager.subscription_count(), 1);

    manager.unsubscribe(id);
    assert_eq!(manager.subscription_count(), 0);

    // Table index should be empty
    let watched = manager.watched_tables();
    assert!(watched.iter().all(|(_, c)| *c == 0));
}

#[test]
fn test_invalid_query() {
    let manager = SubscriptionManager::new();
    let (tx, _rx) = mpsc::channel(16);

    let result = manager.subscribe("SELECT * FROM".to_string(), tx);
    assert!(result.is_err());
    assert!(matches!(result, Err(SubscriptionError::ParseError(_))));
}

#[test]
fn test_query_without_tables() {
    let manager = SubscriptionManager::new();
    let (tx, _rx) = mpsc::channel(16);

    // SELECT without FROM should fail
    let result = manager.subscribe("SELECT 1 + 1".to_string(), tx);
    assert!(result.is_err());
}

#[tokio::test]
async fn test_handle_change_notifies_subscribers() {
    let manager = SubscriptionManager::new();
    let (tx, mut rx) = mpsc::channel(16);
    let db = setup_test_db();

    // Subscribe to users table
    let _id = manager.subscribe("SELECT * FROM users".to_string(), tx).unwrap();

    // Simulate a change to users table
    manager
        .handle_change(
            vibesql_storage::ChangeEvent::Insert {
                table_name: "users".to_string(),
                row_index: 0,
            },
            &db,
        )
        .await;

    // Should receive a notification (empty result since table is empty)
    let update = rx.try_recv();
    assert!(update.is_ok());

    match update.unwrap() {
        SubscriptionUpdate::Full { rows, .. } => {
            // Table is empty, so no rows
            assert!(rows.is_empty());
        }
        _ => panic!("Expected Full update"),
    }
}

#[tokio::test]
async fn test_handle_change_ignores_unrelated_tables() {
    let manager = SubscriptionManager::new();
    let (tx, mut rx) = mpsc::channel(16);
    let db = setup_test_db();

    // Subscribe to users table
    let _id = manager.subscribe("SELECT * FROM users".to_string(), tx).unwrap();

    // Simulate a change to orders table (not subscribed)
    manager
        .handle_change(
            vibesql_storage::ChangeEvent::Insert {
                table_name: "orders".to_string(),
                row_index: 0,
            },
            &db,
        )
        .await;

    // Should NOT receive a notification
    let update = rx.try_recv();
    assert!(update.is_err()); // Channel should be empty
}

#[tokio::test]
async fn test_send_initial_results() {
    let manager = SubscriptionManager::new();
    let (tx, mut rx) = mpsc::channel(16);
    let mut db = setup_test_db();

    // Insert some data
    let insert =
        vibesql_parser::Parser::parse_sql("INSERT INTO users VALUES (1, 'Alice', TRUE)").unwrap();
    if let vibesql_ast::Statement::Insert(stmt) = insert {
        vibesql_executor::InsertExecutor::execute(&mut db, &stmt).unwrap();
    }

    // Subscribe
    let id = manager.subscribe("SELECT * FROM users".to_string(), tx).unwrap();

    // Send initial results
    manager.send_initial_results(id, &db).await.unwrap();

    // Should receive initial data
    let update = rx.recv().await.unwrap();
    match update {
        SubscriptionUpdate::Full { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].values[0], SqlValue::Integer(1));
        }
        _ => panic!("Expected Full update"),
    }
}

#[tokio::test]
async fn test_results_changed_detection() {
    let manager = SubscriptionManager::new();
    let (tx, mut rx) = mpsc::channel(16);
    let mut db = setup_test_db();

    // Subscribe before any data
    let id = manager.subscribe("SELECT * FROM users".to_string(), tx).unwrap();

    // Send initial (empty) results
    manager.send_initial_results(id, &db).await.unwrap();
    let _ = rx.recv().await; // Consume initial

    // Insert data
    let insert =
        vibesql_parser::Parser::parse_sql("INSERT INTO users VALUES (1, 'Alice', TRUE)").unwrap();
    if let vibesql_ast::Statement::Insert(stmt) = insert {
        vibesql_executor::InsertExecutor::execute(&mut db, &stmt).unwrap();
    }

    // Trigger change notification
    manager
        .handle_change(
            vibesql_storage::ChangeEvent::Insert {
                table_name: "users".to_string(),
                row_index: 0,
            },
            &db,
        )
        .await;

    // Should receive update with new data (as Delta since we have previous results)
    let update = rx.recv().await.unwrap();
    match update {
        SubscriptionUpdate::Delta { inserts, updates, deletes, .. } => {
            // The inserted row should appear as an insert
            assert_eq!(inserts.len(), 1);
            assert!(updates.is_empty());
            assert!(deletes.is_empty());
        }
        SubscriptionUpdate::Full { rows, .. } => {
            // Also acceptable if Full is sent
            assert_eq!(rows.len(), 1);
        }
        _ => panic!("Expected Delta or Full update"),
    }
}

#[tokio::test]
async fn test_no_notification_when_unchanged() {
    let manager = SubscriptionManager::new();
    let (tx, mut rx) = mpsc::channel(16);
    let db = setup_test_db();

    // Subscribe (empty table)
    let id = manager.subscribe("SELECT * FROM users".to_string(), tx).unwrap();

    // Send initial results
    manager.send_initial_results(id, &db).await.unwrap();
    let _ = rx.recv().await; // Consume initial

    // Trigger change (but data didn't actually change since we didn't insert)
    manager
        .handle_change(
            vibesql_storage::ChangeEvent::Insert {
                table_name: "users".to_string(),
                row_index: 0,
            },
            &db,
        )
        .await;

    // Should NOT receive notification (results haven't changed)
    let update = rx.try_recv();
    assert!(update.is_err()); // Channel should be empty
}

#[test]
fn test_multiple_subscriptions_same_table() {
    let manager = SubscriptionManager::new();
    let (tx1, _rx1) = mpsc::channel(16);
    let (tx2, _rx2) = mpsc::channel(16);

    let _id1 = manager.subscribe("SELECT * FROM users".to_string(), tx1).unwrap();
    let _id2 =
        manager.subscribe("SELECT * FROM users WHERE active = TRUE".to_string(), tx2).unwrap();

    assert_eq!(manager.subscription_count(), 2);

    // Both should be indexed under users
    let watched = manager.watched_tables();
    let users_entry = watched.iter().find(|(t, _)| t == "users").unwrap();
    assert_eq!(users_entry.1, 2);
}

#[tokio::test]
async fn test_delta_update_on_insert() {
    let manager = SubscriptionManager::new();
    let (tx, mut rx) = mpsc::channel(16);
    let mut db = setup_test_db();

    // Insert initial data
    let insert =
        vibesql_parser::Parser::parse_sql("INSERT INTO users VALUES (1, 'Alice', TRUE)").unwrap();
    if let vibesql_ast::Statement::Insert(stmt) = insert {
        vibesql_executor::InsertExecutor::execute(&mut db, &stmt).unwrap();
    }

    // Subscribe and get initial results
    let id = manager.subscribe("SELECT * FROM users".to_string(), tx).unwrap();
    manager.send_initial_results(id, &db).await.unwrap();

    // Consume initial Full update
    let initial = rx.recv().await.unwrap();
    match initial {
        SubscriptionUpdate::Full { rows, .. } => {
            assert_eq!(rows.len(), 1);
        }
        _ => panic!("Expected Full update for initial results"),
    }

    // Insert another row
    let insert2 =
        vibesql_parser::Parser::parse_sql("INSERT INTO users VALUES (2, 'Bob', TRUE)").unwrap();
    if let vibesql_ast::Statement::Insert(stmt) = insert2 {
        vibesql_executor::InsertExecutor::execute(&mut db, &stmt).unwrap();
    }

    // Trigger change notification
    manager
        .handle_change(
            vibesql_storage::ChangeEvent::Insert {
                table_name: "users".to_string(),
                row_index: 1,
            },
            &db,
        )
        .await;

    // Should receive a Delta update (not Full)
    let update = rx.recv().await.unwrap();
    match update {
        SubscriptionUpdate::Delta { inserts, updates, deletes, .. } => {
            assert_eq!(inserts.len(), 1);
            assert_eq!(inserts[0].values[0], SqlValue::Integer(2));
            assert!(updates.is_empty());
            assert!(deletes.is_empty());
        }
        SubscriptionUpdate::Full { .. } => {
            panic!("Expected Delta update, got Full");
        }
        _ => panic!("Unexpected update type"),
    }
}

#[tokio::test]
async fn test_delta_update_on_delete() {
    let manager = SubscriptionManager::new();
    let (tx, mut rx) = mpsc::channel(16);
    let mut db = setup_test_db();

    // Insert initial data
    let insert1 =
        vibesql_parser::Parser::parse_sql("INSERT INTO users VALUES (1, 'Alice', TRUE)").unwrap();
    if let vibesql_ast::Statement::Insert(stmt) = insert1 {
        vibesql_executor::InsertExecutor::execute(&mut db, &stmt).unwrap();
    }
    let insert2 =
        vibesql_parser::Parser::parse_sql("INSERT INTO users VALUES (2, 'Bob', TRUE)").unwrap();
    if let vibesql_ast::Statement::Insert(stmt) = insert2 {
        vibesql_executor::InsertExecutor::execute(&mut db, &stmt).unwrap();
    }

    // Subscribe and get initial results
    let id = manager.subscribe("SELECT * FROM users".to_string(), tx).unwrap();
    manager.send_initial_results(id, &db).await.unwrap();

    // Consume initial Full update
    let initial = rx.recv().await.unwrap();
    match initial {
        SubscriptionUpdate::Full { rows, .. } => {
            assert_eq!(rows.len(), 2);
        }
        _ => panic!("Expected Full update for initial results"),
    }

    // Delete a row
    let delete = vibesql_parser::Parser::parse_sql("DELETE FROM users WHERE id = 2").unwrap();
    if let vibesql_ast::Statement::Delete(stmt) = delete {
        vibesql_executor::DeleteExecutor::execute(&stmt, &mut db).unwrap();
    }

    // Trigger change notification
    manager
        .handle_change(
            vibesql_storage::ChangeEvent::Delete {
                table_name: "users".to_string(),
                row_index: 1,
            },
            &db,
        )
        .await;

    // Should receive a Delta update with delete
    let update = rx.recv().await.unwrap();
    match update {
        SubscriptionUpdate::Delta { inserts, updates, deletes, .. } => {
            assert!(inserts.is_empty());
            assert!(updates.is_empty());
            assert_eq!(deletes.len(), 1);
            assert_eq!(deletes[0].values[0], SqlValue::Integer(2));
        }
        SubscriptionUpdate::Full { .. } => {
            panic!("Expected Delta update, got Full");
        }
        _ => panic!("Unexpected update type"),
    }
}

#[test]
fn test_global_limit_exceeded() {
    // Create manager with very low global limit for testing
    let config = SubscriptionConfig {
        max_per_connection: 100,
        max_global: 2,
        max_result_rows: 10000,
        rate_limit_per_second: 100,
        ..Default::default()
    };
    let manager = SubscriptionManager::with_config(config);

    // First two subscriptions should succeed
    let (tx1, _rx1) = mpsc::channel(16);
    let (tx2, _rx2) = mpsc::channel(16);
    let (tx3, _rx3) = mpsc::channel(16);

    manager.subscribe("SELECT * FROM users".to_string(), tx1).unwrap();
    manager.subscribe("SELECT * FROM users WHERE id = 1".to_string(), tx2).unwrap();

    // Third subscription should fail with global limit exceeded
    let result = manager.subscribe("SELECT * FROM users WHERE id = 2".to_string(), tx3);
    assert!(matches!(
        result,
        Err(SubscriptionError::GlobalLimitExceeded { current: 2, max: 2 })
    ));

    // Metrics should reflect the limit exceeded event
    assert_eq!(manager.limit_exceeded_count(), 1);
}

#[tokio::test]
async fn test_result_set_too_large() {
    // Create manager with very low result limit for testing
    let config = SubscriptionConfig {
        max_per_connection: 100,
        max_global: 10000,
        max_result_rows: 0, // No rows allowed
        rate_limit_per_second: 100,
        ..Default::default()
    };
    let manager = SubscriptionManager::with_config(config);
    let mut db = setup_test_db();

    // Insert some data
    let insert =
        vibesql_parser::Parser::parse_sql("INSERT INTO users VALUES (1, 'Alice', TRUE)").unwrap();
    if let vibesql_ast::Statement::Insert(stmt) = insert {
        vibesql_executor::InsertExecutor::execute(&mut db, &stmt).unwrap();
    }

    // Subscribe
    let (tx, _rx) = mpsc::channel(16);
    let id = manager.subscribe("SELECT * FROM users".to_string(), tx).unwrap();

    // Send initial results should fail due to result set too large
    let result = manager.send_initial_results(id, &db).await;
    assert!(matches!(result, Err(SubscriptionError::ResultSetTooLarge { rows: 1, max: 0 })));

    // Metrics should reflect the result set exceeded event
    assert_eq!(manager.result_set_exceeded_count(), 1);
}

#[test]
fn test_retry_policy_backoff_calculation() {
    use crate::subscription::SubscriptionRetryPolicy;

    let policy = SubscriptionRetryPolicy {
        max_retries: 3,
        base_delay_ms: 1000,
        max_delay_ms: 30000,
        backoff_multiplier: 2.0,
    };

    // First retry: 1000ms
    let backoff0 = policy.calculate_backoff(0);
    assert_eq!(backoff0.as_millis(), 1000);

    // Second retry: 2000ms
    let backoff1 = policy.calculate_backoff(1);
    assert_eq!(backoff1.as_millis(), 2000);

    // Third retry: 4000ms
    let backoff2 = policy.calculate_backoff(2);
    assert_eq!(backoff2.as_millis(), 4000);

    // Fourth retry: 8000ms
    let backoff3 = policy.calculate_backoff(3);
    assert_eq!(backoff3.as_millis(), 8000);

    // High attempt should be capped at max_delay_ms
    let backoff10 = policy.calculate_backoff(10);
    assert_eq!(backoff10.as_millis(), 30000);
}

#[test]
fn test_subscription_retry_count_initialization() {
    let (tx, _rx) = mpsc::channel(16);
    let tables: std::collections::HashSet<_> = vec!["users".to_string()].into_iter().collect();

    let sub = Subscription::new("SELECT * FROM users".to_string(), tables, tx);
    assert_eq!(sub.retry_count, 0);
}

#[test]
fn test_subscription_with_custom_policy() {
    use crate::subscription::SubscriptionRetryPolicy;

    let (tx, _rx) = mpsc::channel(16);
    let tables: std::collections::HashSet<_> = vec!["users".to_string()].into_iter().collect();

    let policy = SubscriptionRetryPolicy {
        max_retries: 5,
        base_delay_ms: 500,
        max_delay_ms: 60000,
        backoff_multiplier: 3.0,
    };

    let sub =
        Subscription::with_policy("SELECT * FROM users".to_string(), tables, tx, policy.clone());

    assert_eq!(sub.retry_count, 0);
    assert_eq!(sub.retry_policy.max_retries, 5);
    assert_eq!(sub.retry_policy.base_delay_ms, 500);
}

#[test]
fn test_subscription_backpressure_fields_initialization() {
    let (tx, _rx) = mpsc::channel(16);
    let tables: std::collections::HashSet<_> = vec!["users".to_string()].into_iter().collect();

    let sub = Subscription::new("SELECT * FROM users".to_string(), tables, tx);

    // Verify backpressure tracking fields are initialized
    assert_eq!(sub.updates_sent, 0);
    assert_eq!(sub.updates_dropped, 0);
    assert_eq!(sub.channel_buffer_size, 64); // default
    assert_eq!(sub.slow_consumer_threshold_percent, 80); // default
}

#[test]
fn test_subscription_with_config_backpressure() {
    use crate::subscription::SubscriptionConfig;

    let (tx, _rx) = mpsc::channel(16);
    let tables: std::collections::HashSet<_> = vec!["users".to_string()].into_iter().collect();

    let config = SubscriptionConfig {
        max_per_connection: 100,
        max_global: 10000,
        max_result_rows: 10000,
        rate_limit_per_second: 100,
        channel_buffer_size: 128,
        slow_consumer_threshold_percent: 90,
        selective_updates: Default::default(),
    };

    let sub = Subscription::with_config("SELECT * FROM users".to_string(), tables, tx, &config);

    // Verify config values are applied
    assert_eq!(sub.updates_sent, 0);
    assert_eq!(sub.updates_dropped, 0);
    assert_eq!(sub.channel_buffer_size, 128);
    assert_eq!(sub.slow_consumer_threshold_percent, 90);
}

#[test]
fn test_get_subscription_metrics() {
    let manager = SubscriptionManager::new();
    let (tx, _rx) = mpsc::channel(16);

    let id = manager.subscribe("SELECT * FROM users".to_string(), tx).unwrap();

    // Get metrics for the subscription
    let metrics = manager.get_subscription_metrics(id);
    assert!(metrics.is_some());

    let metrics = metrics.unwrap();
    assert_eq!(metrics.subscription_id, Some(id));
    assert_eq!(metrics.updates_sent, 0);
    assert_eq!(metrics.updates_dropped, 0);
    assert_eq!(metrics.channel_buffer_size, 64);
    assert_eq!(metrics.slow_consumer_threshold_percent, 80);
}

#[test]
fn test_get_subscription_metrics_not_found() {
    let manager = SubscriptionManager::new();

    // Get metrics for a non-existent subscription
    let fake_id = SubscriptionId::new();
    let metrics = manager.get_subscription_metrics(fake_id);
    assert!(metrics.is_none());
}

#[test]
fn test_get_all_metrics() {
    let manager = SubscriptionManager::new();
    let (tx1, _rx1) = mpsc::channel(16);
    let (tx2, _rx2) = mpsc::channel(16);

    manager.subscribe("SELECT * FROM users".to_string(), tx1).unwrap();
    manager.subscribe("SELECT * FROM orders".to_string(), tx2).unwrap();

    // Get all metrics
    let all_metrics = manager.get_all_metrics();
    assert_eq!(all_metrics.len(), 2);

    // Verify each subscription has metrics
    for metrics in all_metrics {
        assert!(metrics.subscription_id.is_some());
        assert_eq!(metrics.updates_sent, 0);
        assert_eq!(metrics.updates_dropped, 0);
    }
}

#[test]
fn test_get_all_metrics_empty() {
    let manager = SubscriptionManager::new();

    // Get all metrics when no subscriptions exist
    let all_metrics = manager.get_all_metrics();
    assert!(all_metrics.is_empty());
}

// ========================================================================
// Tests for connection tracking methods
// ========================================================================

#[test]
fn test_subscribe_for_connection() {
    let manager = SubscriptionManager::new();
    let (tx, _rx) = mpsc::channel(16);

    let connection_id = "conn-1".to_string();
    let wire_id: [u8; 16] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
    let tables: HashSet<String> = ["users".to_string()].into_iter().collect();

    let result = manager.subscribe_for_connection(
        "SELECT * FROM users".to_string(),
        tx,
        connection_id.clone(),
        wire_id,
        tables,
        None, // no filter
    );

    assert!(result.is_ok());
    let id = result.unwrap();

    // Verify subscription was registered
    assert_eq!(manager.subscription_count(), 1);
    assert_eq!(manager.connection_subscription_count(&connection_id), 1);

    // Verify wire ID index works
    let found_id = manager.find_subscription_by_wire_id(&wire_id);
    assert_eq!(found_id, Some(id));
}

#[test]
fn test_subscribe_for_connection_limit_enforcement() {
    use crate::subscription::SubscriptionConfig;

    let config = SubscriptionConfig { max_per_connection: 2, max_global: 10, ..Default::default() };
    let manager = SubscriptionManager::with_config(config);
    let connection_id = "conn-1".to_string();

    // Create 2 subscriptions (at the limit)
    for i in 0..2 {
        let (tx, _rx) = mpsc::channel(16);
        let mut wire_id = [0u8; 16];
        wire_id[0] = i;
        let tables: HashSet<String> = ["users".to_string()].into_iter().collect();

        let result = manager.subscribe_for_connection(
            format!("SELECT {} FROM users", i),
            tx,
            connection_id.clone(),
            wire_id,
            tables,
            None,
        );
        assert!(result.is_ok(), "Subscription {} should succeed", i);
    }

    assert_eq!(manager.connection_subscription_count(&connection_id), 2);

    // Third subscription should fail (connection limit)
    let (tx, _rx) = mpsc::channel(16);
    let wire_id = [2u8; 16];
    let tables: HashSet<String> = ["users".to_string()].into_iter().collect();

    let result = manager.subscribe_for_connection(
        "SELECT 3 FROM users".to_string(),
        tx,
        connection_id.clone(),
        wire_id,
        tables,
        None,
    );

    assert!(result.is_err());
    match result.unwrap_err() {
        crate::subscription::SubscriptionError::ConnectionLimitExceeded { current, max } => {
            assert_eq!(current, 2);
            assert_eq!(max, 2);
        }
        e => panic!("Expected ConnectionLimitExceeded, got {:?}", e),
    }
}

#[test]
fn test_unsubscribe_by_wire_id() {
    let manager = SubscriptionManager::new();
    let (tx, _rx) = mpsc::channel(16);

    let connection_id = "conn-1".to_string();
    let wire_id: [u8; 16] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
    let tables: HashSet<String> = ["users".to_string()].into_iter().collect();

    manager
        .subscribe_for_connection(
            "SELECT * FROM users".to_string(),
            tx,
            connection_id.clone(),
            wire_id,
            tables,
            None,
        )
        .unwrap();

    assert_eq!(manager.subscription_count(), 1);
    assert_eq!(manager.connection_subscription_count(&connection_id), 1);

    // Unsubscribe by wire ID
    manager.unsubscribe_by_wire_id(&wire_id);

    assert_eq!(manager.subscription_count(), 0);
    assert_eq!(manager.connection_subscription_count(&connection_id), 0);

    // Wire ID should no longer be found
    assert!(manager.find_subscription_by_wire_id(&wire_id).is_none());
}

#[test]
fn test_unsubscribe_all_for_connection() {
    let manager = SubscriptionManager::new();
    let connection_id1 = "conn-1".to_string();
    let connection_id2 = "conn-2".to_string();

    // Create subscriptions for connection 1
    for i in 0..3 {
        let (tx, _rx) = mpsc::channel(16);
        let mut wire_id = [0u8; 16];
        wire_id[0] = i;
        let tables: HashSet<String> = ["users".to_string()].into_iter().collect();

        manager
            .subscribe_for_connection(
                format!("SELECT {} FROM users", i),
                tx,
                connection_id1.clone(),
                wire_id,
                tables,
                None,
            )
            .unwrap();
    }

    // Create subscriptions for connection 2
    for i in 0..2 {
        let (tx, _rx) = mpsc::channel(16);
        let mut wire_id = [10u8; 16];
        wire_id[0] = i + 10;
        let tables: HashSet<String> = ["orders".to_string()].into_iter().collect();

        manager
            .subscribe_for_connection(
                format!("SELECT {} FROM orders", i),
                tx,
                connection_id2.clone(),
                wire_id,
                tables,
                None,
            )
            .unwrap();
    }

    assert_eq!(manager.subscription_count(), 5);
    assert_eq!(manager.connection_subscription_count(&connection_id1), 3);
    assert_eq!(manager.connection_subscription_count(&connection_id2), 2);

    // Unsubscribe all for connection 1
    manager.unsubscribe_all_for_connection(&connection_id1);

    assert_eq!(manager.subscription_count(), 2);
    assert_eq!(manager.connection_subscription_count(&connection_id1), 0);
    assert_eq!(manager.connection_subscription_count(&connection_id2), 2);
}

#[test]
fn test_connection_subscription_count() {
    let manager = SubscriptionManager::new();
    let connection_id = "conn-1".to_string();

    // Initially zero
    assert_eq!(manager.connection_subscription_count(&connection_id), 0);

    // Add a subscription
    let (tx, _rx) = mpsc::channel(16);
    let wire_id: [u8; 16] = [1u8; 16];
    let tables: HashSet<String> = ["users".to_string()].into_iter().collect();

    manager
        .subscribe_for_connection(
            "SELECT * FROM users".to_string(),
            tx,
            connection_id.clone(),
            wire_id,
            tables,
            None,
        )
        .unwrap();

    assert_eq!(manager.connection_subscription_count(&connection_id), 1);

    // Non-existent connection should return 0
    assert_eq!(manager.connection_subscription_count("non-existent"), 0);
}

#[test]
fn test_get_affected_subscriptions_for_wire_protocol() {
    let manager = SubscriptionManager::new();
    let connection_id = "conn-1".to_string();

    // Create a subscription for "users" table
    let (tx, _rx) = mpsc::channel(16);
    let wire_id: [u8; 16] = [1u8; 16];
    let tables: HashSet<String> = ["users".to_string()].into_iter().collect();

    manager
        .subscribe_for_connection(
            "SELECT * FROM users".to_string(),
            tx,
            connection_id.clone(),
            wire_id,
            tables,
            None,
        )
        .unwrap();

    // Query for affected subscriptions
    let affected = manager.get_affected_subscriptions_for_wire_protocol("users");
    assert_eq!(affected.len(), 1);
    assert_eq!(affected[0].0, wire_id);
    assert_eq!(affected[0].1, "SELECT * FROM users");

    // Query for non-existent table
    let affected = manager.get_affected_subscriptions_for_wire_protocol("orders");
    assert!(affected.is_empty());
}

#[test]
fn test_update_result_by_wire_id() {
    let manager = SubscriptionManager::new();
    let connection_id = "conn-1".to_string();

    let (tx, _rx) = mpsc::channel(16);
    let wire_id: [u8; 16] = [1u8; 16];
    let tables: HashSet<String> = ["users".to_string()].into_iter().collect();

    manager
        .subscribe_for_connection(
            "SELECT * FROM users".to_string(),
            tx,
            connection_id.clone(),
            wire_id,
            tables,
            None,
        )
        .unwrap();

    // Update the result
    let rows = vec![crate::Row { values: vec![vibesql_types::SqlValue::Integer(42)] }];
    manager.update_result_by_wire_id(&wire_id, 12345, rows.clone());

    // Verify the result was stored
    let affected = manager.get_affected_subscriptions_for_wire_protocol("users");
    assert_eq!(affected.len(), 1);
    assert_eq!(affected[0].2, 12345); // last_result_hash
    assert!(affected[0].3.is_some()); // last_result
    assert_eq!(affected[0].3.as_ref().unwrap().len(), 1);
}

#[test]
fn test_update_pk_columns_with_eligibility() {
    let manager = SubscriptionManager::new();
    let (tx, _rx) = mpsc::channel(16);

    // Create a basic subscription (non-connection based)
    let id = manager.subscribe("SELECT * FROM users".to_string(), tx).unwrap();

    // Initially not selective eligible
    let sub = manager.subscriptions.get(&id).unwrap();
    assert!(!sub.selective_eligible);
    assert_eq!(sub.pk_columns, vec![0]); // Default
    drop(sub);

    // Update PK columns with confident detection
    let newly_eligible = manager.update_pk_columns_with_eligibility(id, vec![0, 1], true);
    assert!(newly_eligible, "Should be newly eligible");

    // Verify changes
    let sub = manager.subscriptions.get(&id).unwrap();
    assert!(sub.selective_eligible);
    assert_eq!(sub.pk_columns, vec![0, 1]);
    drop(sub);

    // Updating again should return false (not newly eligible)
    let newly_eligible2 = manager.update_pk_columns_with_eligibility(id, vec![0, 1], true);
    assert!(!newly_eligible2, "Should not be newly eligible since already was");

    // Unsubscribe should return true (was selective eligible)
    let was_eligible = manager.unsubscribe(id);
    assert!(was_eligible);
}

#[test]
fn test_update_pk_columns_with_eligibility_not_confident() {
    let manager = SubscriptionManager::new();
    let (tx, _rx) = mpsc::channel(16);

    let id = manager.subscribe("SELECT * FROM users".to_string(), tx).unwrap();

    // Update with non-confident detection
    let newly_eligible = manager.update_pk_columns_with_eligibility(id, vec![0, 2], false);
    assert!(!newly_eligible, "Should not be eligible when not confident");

    // Verify PK columns updated but not selective eligible
    let sub = manager.subscriptions.get(&id).unwrap();
    assert!(!sub.selective_eligible);
    assert_eq!(sub.pk_columns, vec![0, 2]);
    drop(sub);

    // Unsubscribe should return false (was not selective eligible)
    let was_eligible = manager.unsubscribe(id);
    assert!(!was_eligible);
}
