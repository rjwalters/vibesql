//! Integration tests for session-level subscription functionality
//!
//! Tests the SessionSubscriptionManager for per-connection subscription tracking.

use std::collections::HashSet;
use vibesql_server::subscription::{
    SessionSubscriptionManager, SubscriptionConfig, TablePkInfo,
};

#[test]
fn test_subscription_manager_basic_subscribe() {
    let mut manager = SessionSubscriptionManager::new();

    let mut deps = HashSet::new();
    deps.insert("users".to_string());

    let id = manager
        .subscribe("SELECT * FROM users".to_string(), vec![], deps, None, TablePkInfo::default())
        .unwrap();

    assert_eq!(manager.subscription_count(), 1);
    assert!(manager.get(&id).is_some());

    let sub = manager.get(&id).unwrap();
    assert_eq!(sub.query, "SELECT * FROM users");
    assert!(sub.params.is_empty());
    assert!(sub.table_dependencies.contains("users"));
}

#[test]
fn test_subscription_manager_with_params() {
    let mut manager = SessionSubscriptionManager::new();

    let mut deps = HashSet::new();
    deps.insert("users".to_string());

    let params = vec![
        Some(b"1".to_vec()),
        Some(b"test".to_vec()),
        None, // NULL parameter
    ];

    let id = manager
        .subscribe(
            "SELECT * FROM users WHERE id = $1 AND name = $2 AND deleted_at IS $3".to_string(),
            params.clone(),
            deps,
            None,
            TablePkInfo::default(),
        )
        .unwrap();

    let sub = manager.get(&id).unwrap();
    assert_eq!(sub.params.len(), 3);
    assert_eq!(sub.params[0], Some(b"1".to_vec()));
    assert_eq!(sub.params[1], Some(b"test".to_vec()));
    assert_eq!(sub.params[2], None);
}

#[test]
fn test_subscription_manager_unsubscribe() {
    let mut manager = SessionSubscriptionManager::new();

    let mut deps = HashSet::new();
    deps.insert("users".to_string());

    let id = manager
        .subscribe("SELECT * FROM users".to_string(), vec![], deps, None, TablePkInfo::default())
        .unwrap();

    assert_eq!(manager.subscription_count(), 1);

    let removed = manager.unsubscribe(&id);
    assert!(removed.is_some());
    assert_eq!(manager.subscription_count(), 0);

    // Second unsubscribe should return None
    let removed_again = manager.unsubscribe(&id);
    assert!(removed_again.is_none());
}

#[test]
fn test_subscription_manager_table_index() {
    let mut manager = SessionSubscriptionManager::new();

    // Subscribe to users table
    let mut deps1 = HashSet::new();
    deps1.insert("users".to_string());
    let id1 = manager
        .subscribe("SELECT * FROM users".to_string(), vec![], deps1, None, TablePkInfo::default())
        .unwrap();

    // Subscribe to users and orders tables
    let mut deps2 = HashSet::new();
    deps2.insert("users".to_string());
    deps2.insert("orders".to_string());
    let id2 = manager
        .subscribe(
            "SELECT * FROM users JOIN orders ON users.id = orders.user_id".to_string(),
            vec![],
            deps2,
            None,
            TablePkInfo::default(),
        )
        .unwrap();

    // Both should be indexed under "users"
    let user_subs = manager.get_subscriptions_for_table("users");
    assert_eq!(user_subs.len(), 2);
    assert!(user_subs.contains(&id1));
    assert!(user_subs.contains(&id2));

    // Only id2 should be under "orders"
    let order_subs = manager.get_subscriptions_for_table("orders");
    assert_eq!(order_subs.len(), 1);
    assert!(order_subs.contains(&id2));

    // Unsubscribe id1, check that "users" still has id2
    manager.unsubscribe(&id1);
    let user_subs = manager.get_subscriptions_for_table("users");
    assert_eq!(user_subs.len(), 1);
    assert!(user_subs.contains(&id2));
}

#[test]
fn test_subscription_manager_clear() {
    let mut manager = SessionSubscriptionManager::new();

    let mut deps = HashSet::new();
    deps.insert("users".to_string());

    manager
        .subscribe(
            "SELECT * FROM users WHERE id = 1".to_string(),
            vec![],
            deps.clone(),
            None,
            TablePkInfo::default(),
        )
        .unwrap();
    manager
        .subscribe(
            "SELECT * FROM users WHERE id = 2".to_string(),
            vec![],
            deps.clone(),
            None,
            TablePkInfo::default(),
        )
        .unwrap();
    manager
        .subscribe(
            "SELECT * FROM users WHERE id = 3".to_string(),
            vec![],
            deps,
            None,
            TablePkInfo::default(),
        )
        .unwrap();

    assert_eq!(manager.subscription_count(), 3);

    manager.clear();

    assert_eq!(manager.subscription_count(), 0);
    assert!(manager.get_subscriptions_for_table("users").is_empty());
}

#[test]
fn test_subscription_manager_unique_ids() {
    // Use a high rate limit config for this test since we create many subscriptions quickly
    let config = SubscriptionConfig {
        max_per_connection: 200,
        max_global: 10000,
        max_result_rows: 10000,
        rate_limit_per_second: 200, // High rate limit for test
        channel_buffer_size: 64,
        slow_consumer_threshold_percent: 80,
    };
    let mut manager = SessionSubscriptionManager::with_config(config);

    let mut deps = HashSet::new();
    deps.insert("test".to_string());

    // Create multiple subscriptions and ensure they get unique IDs
    let mut ids = Vec::new();
    for i in 0..100 {
        let id = manager
            .subscribe(
                format!("SELECT {} FROM test", i),
                vec![],
                deps.clone(),
                None,
                TablePkInfo::default(),
            )
            .unwrap();
        ids.push(id);
    }

    // All IDs should be unique
    let unique_ids: HashSet<[u8; 16]> = ids.iter().copied().collect();
    assert_eq!(unique_ids.len(), 100);
}

#[test]
fn test_subscription_manager_empty_table_dependencies() {
    let mut manager = SessionSubscriptionManager::new();

    // Subscribe with no table dependencies (e.g., SELECT 1)
    let id = manager
        .subscribe("SELECT 1".to_string(), vec![], HashSet::new(), None, TablePkInfo::default())
        .unwrap();

    assert_eq!(manager.subscription_count(), 1);

    let sub = manager.get(&id).unwrap();
    assert!(sub.table_dependencies.is_empty());

    // Unsubscribe should still work
    let removed = manager.unsubscribe(&id);
    assert!(removed.is_some());
    assert_eq!(manager.subscription_count(), 0);
}

#[test]
fn test_subscription_manager_all_subscription_ids() {
    let mut manager = SessionSubscriptionManager::new();

    let deps = HashSet::new();

    let id1 = manager
        .subscribe("SELECT 1".to_string(), vec![], deps.clone(), None, TablePkInfo::default())
        .unwrap();
    let id2 = manager
        .subscribe("SELECT 2".to_string(), vec![], deps.clone(), None, TablePkInfo::default())
        .unwrap();
    let id3 = manager
        .subscribe("SELECT 3".to_string(), vec![], deps, None, TablePkInfo::default())
        .unwrap();

    let all_ids = manager.all_subscription_ids();
    assert_eq!(all_ids.len(), 3);
    assert!(all_ids.contains(&id1));
    assert!(all_ids.contains(&id2));
    assert!(all_ids.contains(&id3));
}
