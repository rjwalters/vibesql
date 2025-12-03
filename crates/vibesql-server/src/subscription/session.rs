//! Session-level subscription management
//!
//! This module provides a per-connection subscription manager that tracks
//! subscriptions for a single client session. Unlike the global SubscriptionManager,
//! this is used to track which queries a specific client is subscribed to.

use std::collections::{HashMap, HashSet};
use uuid::Uuid;

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
}

/// Manages subscriptions for a single connection/session
#[derive(Debug, Default)]
pub struct SessionSubscriptionManager {
    /// Active subscriptions by ID
    subscriptions: HashMap<SessionSubscriptionId, SessionSubscription>,
    /// Index from table name to subscription IDs that depend on it
    table_to_subscriptions: HashMap<String, HashSet<SessionSubscriptionId>>,
}

impl SessionSubscriptionManager {
    /// Create a new subscription manager
    pub fn new() -> Self {
        Self {
            subscriptions: HashMap::new(),
            table_to_subscriptions: HashMap::new(),
        }
    }

    /// Subscribe to a query with the given table dependencies
    ///
    /// Returns the generated subscription ID
    pub fn subscribe(
        &mut self,
        query: String,
        params: Vec<Option<Vec<u8>>>,
        table_dependencies: HashSet<String>,
    ) -> SessionSubscriptionId {
        let id = generate_subscription_id();

        // Register subscription
        let subscription = SessionSubscription {
            id,
            query,
            params,
            table_dependencies: table_dependencies.clone(),
        };

        // Update table -> subscription index
        for table in &table_dependencies {
            self.table_to_subscriptions
                .entry(table.clone())
                .or_default()
                .insert(id);
        }

        self.subscriptions.insert(id, subscription);
        id
    }

    /// Unsubscribe from a query
    ///
    /// Returns the subscription if it existed, None otherwise
    pub fn unsubscribe(&mut self, subscription_id: &SessionSubscriptionId) -> Option<SessionSubscription> {
        if let Some(subscription) = self.subscriptions.remove(subscription_id) {
            // Clean up table -> subscription index
            for table in &subscription.table_dependencies {
                if let Some(sub_ids) = self.table_to_subscriptions.get_mut(table) {
                    sub_ids.remove(subscription_id);
                    if sub_ids.is_empty() {
                        self.table_to_subscriptions.remove(table);
                    }
                }
            }
            Some(subscription)
        } else {
            None
        }
    }

    /// Get all subscription IDs that depend on a given table
    #[allow(dead_code)]
    pub fn get_subscriptions_for_table(&self, table: &str) -> Vec<SessionSubscriptionId> {
        self.table_to_subscriptions
            .get(table)
            .map(|ids| ids.iter().copied().collect())
            .unwrap_or_default()
    }

    /// Get a subscription by ID
    #[allow(dead_code)]
    pub fn get(&self, subscription_id: &SessionSubscriptionId) -> Option<&SessionSubscription> {
        self.subscriptions.get(subscription_id)
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
    }

    /// Get all active subscription IDs
    #[allow(dead_code)]
    pub fn all_subscription_ids(&self) -> Vec<SessionSubscriptionId> {
        self.subscriptions.keys().copied().collect()
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

        let id = manager.subscribe(
            "SELECT * FROM users JOIN orders ON users.id = orders.user_id".to_string(),
            vec![],
            deps,
        );

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

        let id1 = manager.subscribe("SELECT * FROM users".to_string(), vec![], deps1);
        let id2 = manager.subscribe(
            "SELECT * FROM users JOIN products ON users.id = products.seller_id".to_string(),
            vec![],
            deps2,
        );

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

        manager.subscribe("SELECT * FROM users".to_string(), vec![], deps.clone());
        manager.subscribe("SELECT * FROM users WHERE id = 1".to_string(), vec![], deps);

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
}
