//! Session-level subscription management
//!
//! This module provides a per-connection subscription manager that tracks
//! subscriptions for a single client session. Unlike the global SubscriptionManager,
//! this is used to track which queries a specific client is subscribed to.

use std::collections::{HashMap, HashSet, VecDeque};
use std::time::Instant;
use uuid::Uuid;

use super::{SubscriptionConfig, SubscriptionError};
use crate::Row;

/// Unique identifier for a subscription (UUID bytes)
pub type SessionSubscriptionId = [u8; 16];

/// Information about an active subscription in a session
#[derive(Debug, Clone)]
pub struct SessionSubscription {
    /// Unique identifier for this subscription
    pub id: SessionSubscriptionId,
    /// The SQL query being subscribed to
    pub query: String,
    /// Parameter values for the query (for parameterized queries)
    pub params: Vec<Option<Vec<u8>>>,
    /// Tables that this subscription depends on (for invalidation)
    pub table_dependencies: HashSet<String>,
    /// Hash of the last result set (for change detection)
    pub last_result_hash: u64,
    /// Last result set (for delta computation)
    /// This stores the previous result to enable computing deltas on change.
    pub last_result: Option<Vec<Row>>,
}

/// Manages subscriptions for a single connection/session
#[derive(Debug)]
pub struct SessionSubscriptionManager {
    /// Active subscriptions by ID
    subscriptions: HashMap<SessionSubscriptionId, SessionSubscription>,
    /// Index from table name to subscription IDs that depend on it
    table_to_subscriptions: HashMap<String, HashSet<SessionSubscriptionId>>,
    /// Configuration for limits and quotas
    config: SubscriptionConfig,
    /// Timestamps of recent subscription creations for rate limiting
    /// Stores timestamps within the last second
    recent_subscriptions: VecDeque<Instant>,
}

impl SessionSubscriptionManager {
    /// Create a new subscription manager with default configuration
    pub fn new() -> Self {
        Self::with_config(SubscriptionConfig::default())
    }

    /// Create a new subscription manager with custom configuration
    pub fn with_config(config: SubscriptionConfig) -> Self {
        Self {
            subscriptions: HashMap::new(),
            table_to_subscriptions: HashMap::new(),
            config,
            recent_subscriptions: VecDeque::new(),
        }
    }

    /// Subscribe to a query with the given table dependencies
    ///
    /// Returns the generated subscription ID on success, or an error if limits are exceeded.
    ///
    /// # Errors
    ///
    /// - `ConnectionLimitExceeded` if this connection has too many subscriptions
    /// - `RateLimited` if subscriptions are being created too quickly
    pub fn subscribe(
        &mut self,
        query: String,
        params: Vec<Option<Vec<u8>>>,
        table_dependencies: HashSet<String>,
    ) -> Result<SessionSubscriptionId, SubscriptionError> {
        // Check per-connection limit
        if self.subscriptions.len() >= self.config.max_per_connection {
            return Err(SubscriptionError::ConnectionLimitExceeded {
                current: self.subscriptions.len(),
                max: self.config.max_per_connection,
            });
        }

        // Check rate limit
        self.check_rate_limit()?;

        let id = generate_subscription_id();

        // Register subscription
        let subscription = SessionSubscription {
            id,
            query,
            params,
            table_dependencies: table_dependencies.clone(),
            last_result_hash: 0,
            last_result: None,
        };

        // Update table -> subscription index (normalize to lowercase for case-insensitive matching)
        for table in &table_dependencies {
            self.table_to_subscriptions.entry(table.to_lowercase()).or_default().insert(id);
        }

        self.subscriptions.insert(id, subscription);

        // Record this subscription for rate limiting
        self.recent_subscriptions.push_back(Instant::now());

        Ok(id)
    }

    /// Check rate limit and return error if exceeded
    fn check_rate_limit(&mut self) -> Result<(), SubscriptionError> {
        let now = Instant::now();
        let one_second_ago = now - std::time::Duration::from_secs(1);

        // Remove old entries (older than 1 second)
        while let Some(front) = self.recent_subscriptions.front() {
            if *front < one_second_ago {
                self.recent_subscriptions.pop_front();
            } else {
                break;
            }
        }

        // Check if we've hit the rate limit
        if self.recent_subscriptions.len() >= self.config.rate_limit_per_second as usize {
            // Calculate retry-after based on oldest entry
            if let Some(oldest) = self.recent_subscriptions.front() {
                let elapsed = now.duration_since(*oldest);
                let retry_after_ms = 1000u64.saturating_sub(elapsed.as_millis() as u64);
                return Err(SubscriptionError::RateLimited { retry_after_ms });
            }
        }

        Ok(())
    }

    /// Unsubscribe from a query
    ///
    /// Returns the subscription if it existed, None otherwise
    pub fn unsubscribe(
        &mut self,
        subscription_id: &SessionSubscriptionId,
    ) -> Option<SessionSubscription> {
        if let Some(subscription) = self.subscriptions.remove(subscription_id) {
            // Clean up table -> subscription index (use lowercase to match how we stored it)
            for table in &subscription.table_dependencies {
                let table_lower = table.to_lowercase();
                if let Some(sub_ids) = self.table_to_subscriptions.get_mut(&table_lower) {
                    sub_ids.remove(subscription_id);
                    if sub_ids.is_empty() {
                        self.table_to_subscriptions.remove(&table_lower);
                    }
                }
            }
            Some(subscription)
        } else {
            None
        }
    }

    /// Get all subscription IDs that depend on a given table (case-insensitive)
    pub fn get_subscriptions_for_table(&self, table: &str) -> Vec<SessionSubscriptionId> {
        self.table_to_subscriptions
            .get(&table.to_lowercase())
            .map(|ids| ids.iter().copied().collect())
            .unwrap_or_default()
    }

    /// Get subscriptions that depend on a given table (case-insensitive)
    ///
    /// Returns an iterator over (subscription_id, subscription) pairs for all
    /// subscriptions that depend on the given table. This is used to notify
    /// subscribers when a table is mutated.
    pub fn get_subscriptions_for_table_with_details(
        &self,
        table: &str,
    ) -> impl Iterator<Item = (&SessionSubscriptionId, &SessionSubscription)> {
        let table_lower = table.to_lowercase();
        self.table_to_subscriptions
            .get(&table_lower)
            .into_iter()
            .flat_map(|ids| {
                ids.iter().filter_map(|id| self.subscriptions.get(id).map(|sub| (id, sub)))
            })
    }

    /// Get a subscription by ID
    #[allow(dead_code)]
    pub fn get(&self, subscription_id: &SessionSubscriptionId) -> Option<&SessionSubscription> {
        self.subscriptions.get(subscription_id)
    }

    /// Get a mutable reference to a subscription by ID
    pub fn get_mut(
        &mut self,
        subscription_id: &SessionSubscriptionId,
    ) -> Option<&mut SessionSubscription> {
        self.subscriptions.get_mut(subscription_id)
    }

    /// Update the stored result for a subscription (for delta computation)
    ///
    /// This should be called after sending subscription data to store the result
    /// for computing deltas on the next update.
    pub fn update_result(
        &mut self,
        subscription_id: &SessionSubscriptionId,
        result_hash: u64,
        result: Vec<Row>,
    ) {
        if let Some(sub) = self.subscriptions.get_mut(subscription_id) {
            sub.last_result_hash = result_hash;
            sub.last_result = Some(result);
        }
    }

    /// Get the number of active subscriptions
    #[allow(dead_code)]
    pub fn subscription_count(&self) -> usize {
        self.subscriptions.len()
    }

    /// Clear all subscriptions (used when connection closes)
    pub fn clear(&mut self) {
        self.subscriptions.clear();
        self.table_to_subscriptions.clear();
        self.recent_subscriptions.clear();
    }

    /// Get all active subscription IDs
    #[allow(dead_code)]
    pub fn all_subscription_ids(&self) -> Vec<SessionSubscriptionId> {
        self.subscriptions.keys().copied().collect()
    }

    /// Get the current configuration
    pub fn config(&self) -> &SubscriptionConfig {
        &self.config
    }
}

impl Default for SessionSubscriptionManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Generate a new unique subscription ID using UUID v4
fn generate_subscription_id() -> SessionSubscriptionId {
    *Uuid::new_v4().as_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_subscribe_and_unsubscribe() {
        let mut manager = SessionSubscriptionManager::new();

        let mut deps = HashSet::new();
        deps.insert("users".to_string());
        deps.insert("orders".to_string());

        let id = manager
            .subscribe(
                "SELECT * FROM users JOIN orders ON users.id = orders.user_id".to_string(),
                vec![],
                deps,
            )
            .unwrap();

        assert_eq!(manager.subscription_count(), 1);
        assert!(manager.get(&id).is_some());

        // Check table index
        let user_subs = manager.get_subscriptions_for_table("users");
        assert_eq!(user_subs.len(), 1);
        assert_eq!(user_subs[0], id);

        let order_subs = manager.get_subscriptions_for_table("orders");
        assert_eq!(order_subs.len(), 1);
        assert_eq!(order_subs[0], id);

        // Unsubscribe
        let removed = manager.unsubscribe(&id);
        assert!(removed.is_some());
        assert_eq!(manager.subscription_count(), 0);

        // Table index should be cleaned up
        let user_subs = manager.get_subscriptions_for_table("users");
        assert!(user_subs.is_empty());
    }

    #[test]
    fn test_multiple_subscriptions_same_table() {
        let mut manager = SessionSubscriptionManager::new();

        let mut deps1 = HashSet::new();
        deps1.insert("users".to_string());

        let mut deps2 = HashSet::new();
        deps2.insert("users".to_string());
        deps2.insert("products".to_string());

        let id1 = manager.subscribe("SELECT * FROM users".to_string(), vec![], deps1).unwrap();
        let id2 = manager
            .subscribe(
                "SELECT * FROM users JOIN products ON users.id = products.seller_id".to_string(),
                vec![],
                deps2,
            )
            .unwrap();

        assert_eq!(manager.subscription_count(), 2);

        // Both subscriptions should be indexed under "users"
        let user_subs = manager.get_subscriptions_for_table("users");
        assert_eq!(user_subs.len(), 2);
        assert!(user_subs.contains(&id1));
        assert!(user_subs.contains(&id2));

        // Only id2 should be under "products"
        let product_subs = manager.get_subscriptions_for_table("products");
        assert_eq!(product_subs.len(), 1);
        assert!(product_subs.contains(&id2));

        // Remove first subscription
        manager.unsubscribe(&id1);
        assert_eq!(manager.subscription_count(), 1);

        // "users" should still have id2
        let user_subs = manager.get_subscriptions_for_table("users");
        assert_eq!(user_subs.len(), 1);
        assert!(user_subs.contains(&id2));
    }

    #[test]
    fn test_clear() {
        let mut manager = SessionSubscriptionManager::new();

        let mut deps = HashSet::new();
        deps.insert("users".to_string());

        manager.subscribe("SELECT * FROM users".to_string(), vec![], deps.clone()).unwrap();
        manager.subscribe("SELECT * FROM users WHERE id = 1".to_string(), vec![], deps).unwrap();

        assert_eq!(manager.subscription_count(), 2);

        manager.clear();

        assert_eq!(manager.subscription_count(), 0);
        assert!(manager.get_subscriptions_for_table("users").is_empty());
    }

    #[test]
    fn test_unsubscribe_nonexistent() {
        let mut manager = SessionSubscriptionManager::new();
        let fake_id = [0u8; 16];

        let result = manager.unsubscribe(&fake_id);
        assert!(result.is_none());
    }

    #[test]
    fn test_connection_limit_exceeded() {
        // Create manager with very low limit for testing
        let config = SubscriptionConfig {
            max_per_connection: 2,
            max_global: 10000,
            max_result_rows: 10000,
            rate_limit_per_second: 100, // High rate limit to not interfere
            ..Default::default()
        };
        let mut manager = SessionSubscriptionManager::with_config(config);

        let mut deps = HashSet::new();
        deps.insert("users".to_string());

        // First two subscriptions should succeed
        manager.subscribe("SELECT * FROM users".to_string(), vec![], deps.clone()).unwrap();
        manager
            .subscribe("SELECT * FROM users WHERE id = 1".to_string(), vec![], deps.clone())
            .unwrap();

        // Third subscription should fail with limit exceeded
        let result =
            manager.subscribe("SELECT * FROM users WHERE id = 2".to_string(), vec![], deps);
        assert!(matches!(
            result,
            Err(SubscriptionError::ConnectionLimitExceeded { current: 2, max: 2 })
        ));
    }

    #[test]
    fn test_rate_limit() {
        // Create manager with very low rate limit for testing
        let config = SubscriptionConfig {
            max_per_connection: 100,
            max_global: 10000,
            max_result_rows: 10000,
            rate_limit_per_second: 2,
            ..Default::default()
        };
        let mut manager = SessionSubscriptionManager::with_config(config);

        let mut deps = HashSet::new();
        deps.insert("users".to_string());

        // First two subscriptions should succeed (rate limit = 2/sec)
        manager.subscribe("SELECT * FROM users".to_string(), vec![], deps.clone()).unwrap();
        manager
            .subscribe("SELECT * FROM users WHERE id = 1".to_string(), vec![], deps.clone())
            .unwrap();

        // Third subscription should be rate limited
        let result =
            manager.subscribe("SELECT * FROM users WHERE id = 2".to_string(), vec![], deps);
        assert!(matches!(result, Err(SubscriptionError::RateLimited { .. })));
    }

    #[test]
    fn test_update_result_stores_data() {
        use vibesql_types::SqlValue;

        let mut manager = SessionSubscriptionManager::new();
        let mut deps = HashSet::new();
        deps.insert("users".to_string());

        let id = manager.subscribe("SELECT * FROM users".to_string(), vec![], deps).unwrap();

        // Initially no result stored
        let sub = manager.get(&id).unwrap();
        assert_eq!(sub.last_result_hash, 0);
        assert!(sub.last_result.is_none());

        // Store a result
        let rows = vec![
            crate::Row {
                values: vec![SqlValue::Integer(1), SqlValue::Varchar("Alice".to_string())],
            },
            crate::Row {
                values: vec![SqlValue::Integer(2), SqlValue::Varchar("Bob".to_string())],
            },
        ];
        manager.update_result(&id, 12345, rows.clone());

        // Verify result is stored
        let sub = manager.get(&id).unwrap();
        assert_eq!(sub.last_result_hash, 12345);
        assert!(sub.last_result.is_some());
        assert_eq!(sub.last_result.as_ref().unwrap().len(), 2);
    }

    #[test]
    fn test_get_mut_allows_modification() {
        let mut manager = SessionSubscriptionManager::new();
        let mut deps = HashSet::new();
        deps.insert("users".to_string());

        let id = manager.subscribe("SELECT * FROM users".to_string(), vec![], deps).unwrap();

        // Get mutable reference and modify
        let sub = manager.get_mut(&id).unwrap();
        sub.last_result_hash = 99999;

        // Verify modification persisted
        let sub = manager.get(&id).unwrap();
        assert_eq!(sub.last_result_hash, 99999);
    }

    #[test]
    fn test_subscription_preserves_result_across_queries() {
        use vibesql_types::SqlValue;

        let mut manager = SessionSubscriptionManager::new();
        let mut deps = HashSet::new();
        deps.insert("users".to_string());

        let id = manager.subscribe("SELECT * FROM users".to_string(), vec![], deps).unwrap();

        // Store initial result
        let initial_rows = vec![crate::Row {
            values: vec![SqlValue::Integer(1), SqlValue::Varchar("Alice".to_string())],
        }];
        manager.update_result(&id, 11111, initial_rows);

        // Verify initial result
        let sub = manager.get(&id).unwrap();
        assert_eq!(sub.last_result_hash, 11111);
        assert_eq!(sub.last_result.as_ref().unwrap().len(), 1);

        // Update with new result
        let new_rows = vec![
            crate::Row {
                values: vec![SqlValue::Integer(1), SqlValue::Varchar("Alice".to_string())],
            },
            crate::Row {
                values: vec![SqlValue::Integer(2), SqlValue::Varchar("Bob".to_string())],
            },
        ];
        manager.update_result(&id, 22222, new_rows);

        // Verify new result
        let sub = manager.get(&id).unwrap();
        assert_eq!(sub.last_result_hash, 22222);
        assert_eq!(sub.last_result.as_ref().unwrap().len(), 2);
    }
}
