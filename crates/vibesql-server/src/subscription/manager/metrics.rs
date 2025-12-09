//! Metrics and configuration accessors for subscription management.

use std::sync::atomic::Ordering;

use super::SubscriptionManager;
use crate::subscription::{SubscriptionConfig, SubscriptionId, SubscriptionMetrics};

impl SubscriptionManager {
    /// Get the number of active subscriptions
    pub fn subscription_count(&self) -> usize {
        self.subscriptions.len()
    }

    /// Get the tables being watched and their subscription counts
    pub fn watched_tables(&self) -> Vec<(String, usize)> {
        self.table_index.iter().map(|entry| (entry.key().clone(), entry.value().len())).collect()
    }

    /// Get the current configuration
    pub fn config(&self) -> &SubscriptionConfig {
        &self.config
    }

    /// Get the number of times the global limit was exceeded (for metrics)
    pub fn limit_exceeded_count(&self) -> usize {
        self.limit_exceeded_count.load(Ordering::Relaxed)
    }

    /// Get the number of times a result set was too large (for metrics)
    pub fn result_set_exceeded_count(&self) -> usize {
        self.result_set_exceeded_count.load(Ordering::Relaxed)
    }

    /// Get metrics for a specific subscription
    ///
    /// Returns metrics including updates sent, dropped, and channel health.
    /// Returns None if the subscription doesn't exist.
    pub fn get_subscription_metrics(&self, id: SubscriptionId) -> Option<SubscriptionMetrics> {
        self.subscriptions.get(&id).map(|sub| SubscriptionMetrics {
            subscription_id: Some(sub.id),
            updates_sent: sub.updates_sent,
            updates_dropped: sub.updates_dropped,
            channel_buffer_size: sub.channel_buffer_size,
            channel_capacity: sub.notify_tx.capacity(),
            slow_consumer_threshold_percent: sub.slow_consumer_threshold_percent,
        })
    }

    /// Get metrics for all active subscriptions
    ///
    /// Returns a vector of metrics for all subscriptions, useful for
    /// monitoring and alerting on subscription health.
    pub fn get_all_metrics(&self) -> Vec<SubscriptionMetrics> {
        self.subscriptions
            .iter()
            .map(|entry| {
                let sub = entry.value();
                SubscriptionMetrics {
                    subscription_id: Some(sub.id),
                    updates_sent: sub.updates_sent,
                    updates_dropped: sub.updates_dropped,
                    channel_buffer_size: sub.channel_buffer_size,
                    channel_capacity: sub.notify_tx.capacity(),
                    slow_consumer_threshold_percent: sub.slow_consumer_threshold_percent,
                }
            })
            .collect()
    }
}
