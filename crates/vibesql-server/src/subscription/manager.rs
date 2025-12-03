//! Subscription manager for tracking active query subscriptions within a session

use std::collections::{HashMap, HashSet};
use std::time::Instant;
use uuid::Uuid;

/// Unique identifier for a subscription (16-byte UUID)
pub type SubscriptionId = [u8; 16];

/// A single subscription to a query
#[derive(Debug, Clone)]
pub struct Subscription {
    /// Unique identifier for this subscription
    pub id: SubscriptionId,
    /// The SQL query being subscribed to
    pub query: String,
    /// Optional query parameters
    pub params: Vec<Option<Vec<u8>>>,
    /// Tables this query depends on
    pub table_dependencies: HashSet<String>,
    /// Hash of the last known result (for change detection)
    pub last_result_hash: Option<u64>,
    /// When this subscription was created
    pub created_at: Instant,
}

impl Subscription {
    /// Create a new subscription
    pub fn new(
        query: String,
        params: Vec<Option<Vec<u8>>>,
        table_dependencies: HashSet<String>,
    ) -> Self {
        Self {
            id: generate_subscription_id(),
            query,
            params,
            table_dependencies,
            last_result_hash: None,
            created_at: Instant::now(),
        }
    }
}

/// Manages subscriptions for a single session
#[derive(Debug, Default)]
pub struct SubscriptionManager {
    /// Map from subscription ID to subscription
    subscriptions: HashMap<SubscriptionId, Subscription>,
    /// Map from table name to set of subscription IDs that depend on it
    table_subscriptions: HashMap<String, HashSet<SubscriptionId>>,
}

impl SubscriptionManager {
    /// Create a new, empty subscription manager
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a new subscription and return its ID
    pub fn subscribe(
        &mut self,
        query: String,
        params: Vec<Option<Vec<u8>>>,
        table_dependencies: HashSet<String>,
    ) -> SubscriptionId {
        let subscription = Subscription::new(query, params, table_dependencies.clone());
        let id = subscription.id;

        // Register in table index
        for table in &table_dependencies {
            self.table_subscriptions
                .entry(table.clone())
                .or_insert_with(HashSet::new)
                .insert(id);
        }

        // Store subscription
        self.subscriptions.insert(id, subscription);

        id
    }

    /// Remove a subscription by ID
    pub fn unsubscribe(&mut self, id: &SubscriptionId) -> bool {
        if let Some(subscription) = self.subscriptions.remove(id) {
            // Clean up table index
            for table in &subscription.table_dependencies {
                if let Some(subs) = self.table_subscriptions.get_mut(table) {
                    subs.remove(id);
                    if subs.is_empty() {
                        self.table_subscriptions.remove(table);
                    }
                }
            }
            true
        } else {
            false
        }
    }

    /// Get a subscription by ID
    pub fn get(&self, id: &SubscriptionId) -> Option<&Subscription> {
        self.subscriptions.get(id)
    }

    /// Get a mutable subscription by ID
    pub fn get_mut(&mut self, id: &SubscriptionId) -> Option<&mut Subscription> {
        self.subscriptions.get_mut(id)
    }

    /// Get all subscription IDs that depend on a specific table
    pub fn subscriptions_for_table(&self, table: &str) -> Vec<SubscriptionId> {
        self.table_subscriptions
            .get(table)
            .map(|subs| subs.iter().copied().collect())
            .unwrap_or_default()
    }

    /// Get the total number of active subscriptions
    pub fn count(&self) -> usize {
        self.subscriptions.len()
    }

    /// Get all subscription IDs
    pub fn all_ids(&self) -> Vec<SubscriptionId> {
        self.subscriptions.keys().copied().collect()
    }

    /// Check if a subscription exists
    pub fn exists(&self, id: &SubscriptionId) -> bool {
        self.subscriptions.contains_key(id)
    }
}

/// Generate a new subscription ID (UUID as 16-byte array)
fn generate_subscription_id() -> SubscriptionId {
    let uuid = Uuid::new_v4();
    *uuid.as_bytes()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_subscribe_and_get() {
        let mut manager = SubscriptionManager::new();
        let deps = vec!["users".to_string()].into_iter().collect();
        let id = manager.subscribe("SELECT * FROM users".to_string(), vec![], deps);

        let sub = manager.get(&id).unwrap();
        assert_eq!(sub.query, "SELECT * FROM users");
        assert_eq!(sub.table_dependencies.len(), 1);
    }

    #[test]
    fn test_unsubscribe() {
        let mut manager = SubscriptionManager::new();
        let deps = vec!["users".to_string()].into_iter().collect();
        let id = manager.subscribe("SELECT * FROM users".to_string(), vec![], deps);

        assert!(manager.exists(&id));
        assert!(manager.unsubscribe(&id));
        assert!(!manager.exists(&id));
    }

    #[test]
    fn test_subscriptions_for_table() {
        let mut manager = SubscriptionManager::new();

        // Create subscriptions for different tables
        let deps1: HashSet<String> = vec!["users".to_string()].into_iter().collect();
        let id1 = manager.subscribe("SELECT * FROM users".to_string(), vec![], deps1);

        let deps2: HashSet<String> = vec!["orders".to_string()].into_iter().collect();
        let _id2 = manager.subscribe("SELECT * FROM orders".to_string(), vec![], deps2);

        let deps3: HashSet<String> = vec!["users".to_string(), "orders".to_string()]
            .into_iter()
            .collect();
        let id3 = manager.subscribe(
            "SELECT * FROM users JOIN orders".to_string(),
            vec![],
            deps3,
        );

        // Check users subscriptions
        let user_subs = manager.subscriptions_for_table("users");
        assert_eq!(user_subs.len(), 2);
        assert!(user_subs.contains(&id1));
        assert!(user_subs.contains(&id3));
    }

    #[test]
    fn test_table_index_cleanup() {
        let mut manager = SubscriptionManager::new();
        let deps: HashSet<String> = vec!["users".to_string()].into_iter().collect();
        let id = manager.subscribe("SELECT * FROM users".to_string(), vec![], deps);

        // Table should be in index
        assert!(!manager.table_subscriptions.get("users").unwrap().is_empty());

        // Remove subscription
        manager.unsubscribe(&id);

        // Table should be removed from index
        assert!(!manager.table_subscriptions.contains_key("users"));
    }

    #[test]
    fn test_count() {
        let mut manager = SubscriptionManager::new();
        assert_eq!(manager.count(), 0);

        let deps: HashSet<String> = vec!["users".to_string()].into_iter().collect();
        manager.subscribe("SELECT * FROM users".to_string(), vec![], deps);
        assert_eq!(manager.count(), 1);

        let deps: HashSet<String> = vec!["orders".to_string()].into_iter().collect();
        manager.subscribe("SELECT * FROM orders".to_string(), vec![], deps);
        assert_eq!(manager.count(), 2);
    }

    #[test]
    fn test_unique_ids() {
        let mut manager = SubscriptionManager::new();
        let deps: HashSet<String> = vec!["users".to_string()].into_iter().collect();

        let id1 = manager.subscribe("SELECT * FROM users".to_string(), vec![], deps.clone());
        let id2 = manager.subscribe("SELECT * FROM users".to_string(), vec![], deps);

        assert_ne!(id1, id2);
    }
}
