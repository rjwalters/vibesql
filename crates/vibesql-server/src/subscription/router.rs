//! Change router that routes storage events to affected subscriptions

use super::SubscriptionId;
use std::collections::HashMap;
use tokio::sync::mpsc;
use tracing::warn;
use vibesql_storage::{ChangeEvent, ChangeEventReceiver};

/// Update notification for a subscription
#[derive(Debug, Clone)]
pub struct SubscriptionUpdate {
    /// The subscription ID that was affected
    pub subscription_id: SubscriptionId,
    /// The table that changed
    pub table_name: String,
    /// The type of change
    pub event: ChangeEvent,
}

/// Routes storage change events to affected subscriptions
///
/// This component:
/// 1. Listens for storage `ChangeEvent`s
/// 2. Determines which subscriptions are affected (by table dependency)
/// 3. Sends notifications to those subscriptions
/// 4. Triggers query re-evaluation and delta detection
pub struct ChangeRouter {
    /// Receiver for storage change events
    change_receiver: ChangeEventReceiver,
    /// Map from table name to affected subscription IDs
    table_subscriptions: HashMap<String, Vec<SubscriptionId>>,
    /// Channel to send updates to sessions
    session_senders: HashMap<String, mpsc::Sender<SubscriptionUpdate>>,
    /// Batch size for processing changes (for debouncing rapid changes)
    batch_timeout_ms: u64,
}

impl ChangeRouter {
    /// Create a new change router
    pub fn new(change_receiver: ChangeEventReceiver) -> Self {
        Self {
            change_receiver,
            table_subscriptions: HashMap::new(),
            session_senders: HashMap::new(),
            batch_timeout_ms: 10,
        }
    }

    /// Register a subscription for a table
    ///
    /// When changes occur to this table, the subscription will be notified.
    pub fn register_subscription(&mut self, table: String, subscription_id: SubscriptionId) {
        self.table_subscriptions.entry(table).or_default().push(subscription_id);
    }

    /// Unregister a subscription
    pub fn unregister_subscription(&mut self, table: &str, subscription_id: SubscriptionId) {
        if let Some(subs) = self.table_subscriptions.get_mut(table) {
            subs.retain(|s| s != &subscription_id);
        }
    }

    /// Register a session channel for sending updates
    ///
    /// The provided channel will receive updates for all subscriptions in this session.
    pub fn register_session(
        &mut self,
        session_id: String,
        sender: mpsc::Sender<SubscriptionUpdate>,
    ) {
        self.session_senders.insert(session_id, sender);
    }

    /// Unregister a session channel
    pub fn unregister_session(&mut self, session_id: &str) {
        self.session_senders.remove(session_id);
    }

    /// Run the change router event loop
    ///
    /// This should be spawned as a separate async task. It continuously:
    /// 1. Listens for storage change events
    /// 2. Routes them to affected subscriptions
    /// 3. Sends notifications through session channels
    pub async fn run(&mut self) {
        loop {
            // Receive all available changes (batching for efficiency)
            let events = self.change_receiver.recv_all();

            // Process each change event
            for event in events {
                self.process_change(&event).await;
            }

            // Small delay to batch rapid changes
            tokio::time::sleep(tokio::time::Duration::from_millis(self.batch_timeout_ms)).await;
        }
    }

    /// Process a single change event
    ///
    /// Determines which subscriptions are affected and sends notifications.
    async fn process_change(&mut self, event: &ChangeEvent) {
        let table = event.table_name();

        // Find all subscriptions affected by this table
        if let Some(subscription_ids) = self.table_subscriptions.get(table) {
            for subscription_id in subscription_ids.iter().copied() {
                let update = SubscriptionUpdate {
                    subscription_id,
                    table_name: table.to_string(),
                    event: event.clone(),
                };

                // Send to all registered session channels
                // Note: In a real implementation, we'd know which session owns which subscription
                for (session_id, sender) in &self.session_senders {
                    // Use try_send with backpressure detection
                    match sender.try_send(update.clone()) {
                        Ok(()) => {
                            // Successfully sent
                        }
                        Err(mpsc::error::TrySendError::Full(_)) => {
                            warn!(
                                subscription_id = %subscription_id,
                                session_id = %session_id,
                                table = %table,
                                "Session channel full, dropping update. \
                                 Consider increasing channel buffer size or client is consuming too slowly."
                            );
                        }
                        Err(mpsc::error::TrySendError::Closed(_)) => {
                            // Session disconnected, will be cleaned up
                        }
                    }
                }
            }
        }
    }

    /// Get the number of subscriptions registered for a table
    pub fn subscription_count_for_table(&self, table: &str) -> usize {
        self.table_subscriptions.get(table).map(|subs| subs.len()).unwrap_or(0)
    }

    /// Get total number of registered subscriptions
    pub fn total_subscription_count(&self) -> usize {
        self.table_subscriptions.values().map(|v| v.len()).sum()
    }

    /// Set the batch timeout in milliseconds
    pub fn set_batch_timeout(&mut self, ms: u64) {
        self.batch_timeout_ms = ms;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vibesql_storage::change_event_channel;

    #[tokio::test]
    async fn test_register_subscription() {
        let (_sender, receiver) = change_event_channel(16);
        let mut router = ChangeRouter::new(receiver);

        let sub_id = SubscriptionId::new();
        router.register_subscription("users".to_string(), sub_id);

        assert_eq!(router.subscription_count_for_table("users"), 1);
    }

    #[tokio::test]
    async fn test_unregister_subscription() {
        let (_sender, receiver) = change_event_channel(16);
        let mut router = ChangeRouter::new(receiver);

        let sub_id = SubscriptionId::new();
        router.register_subscription("users".to_string(), sub_id);
        assert_eq!(router.subscription_count_for_table("users"), 1);

        router.unregister_subscription("users", sub_id);
        assert_eq!(router.subscription_count_for_table("users"), 0);
    }

    #[tokio::test]
    async fn test_multiple_subscriptions_same_table() {
        let (_sender, receiver) = change_event_channel(16);
        let mut router = ChangeRouter::new(receiver);

        let sub1 = SubscriptionId::new();
        let sub2 = SubscriptionId::new();

        router.register_subscription("users".to_string(), sub1);
        router.register_subscription("users".to_string(), sub2);

        assert_eq!(router.subscription_count_for_table("users"), 2);
    }

    #[tokio::test]
    async fn test_different_tables() {
        let (_sender, receiver) = change_event_channel(16);
        let mut router = ChangeRouter::new(receiver);

        let sub1 = SubscriptionId::new();
        let sub2 = SubscriptionId::new();

        router.register_subscription("users".to_string(), sub1);
        router.register_subscription("orders".to_string(), sub2);

        assert_eq!(router.subscription_count_for_table("users"), 1);
        assert_eq!(router.subscription_count_for_table("orders"), 1);
        assert_eq!(router.total_subscription_count(), 2);
    }

    #[tokio::test]
    async fn test_session_registration() {
        let (_sender, receiver) = change_event_channel(16);
        let mut router = ChangeRouter::new(receiver);

        let (tx, _rx) = mpsc::channel(16);
        router.register_session("session1".to_string(), tx);

        assert_eq!(router.session_senders.len(), 1);
    }

    #[tokio::test]
    async fn test_session_unregistration() {
        let (_sender, receiver) = change_event_channel(16);
        let mut router = ChangeRouter::new(receiver);

        let (tx, _rx) = mpsc::channel(16);
        router.register_session("session1".to_string(), tx);
        assert_eq!(router.session_senders.len(), 1);

        router.unregister_session("session1");
        assert_eq!(router.session_senders.len(), 0);
    }

    #[tokio::test]
    async fn test_process_change_event() {
        let (sender, receiver) = change_event_channel(16);
        let mut router = ChangeRouter::new(receiver);

        let sub_id = SubscriptionId::new();
        router.register_subscription("users".to_string(), sub_id);

        let (tx, mut rx) = mpsc::channel(16);
        router.register_session("session1".to_string(), tx);

        // Send a change event
        let event = ChangeEvent::Insert { table_name: "users".to_string(), row_index: 0 };
        sender.send(event);

        // Process the change
        let events = router.change_receiver.recv_all();
        for evt in events {
            router.process_change(&evt).await;
        }

        // Should have received an update
        let update = rx.try_recv();
        assert!(update.is_ok());
        if let Ok(upd) = update {
            assert_eq!(upd.subscription_id, sub_id);
            assert_eq!(upd.table_name, "users");
        }
    }
}
