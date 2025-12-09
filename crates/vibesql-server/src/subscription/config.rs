//! Subscription configuration types
//!
//! This module provides configuration structures for controlling subscription
//! behavior, including limits, quotas, backpressure, and retry policies.

use std::time::Duration;

use serde::{Deserialize, Serialize};

use super::selective::SelectiveColumnConfig;

// ============================================================================
// Subscription Configuration
// ============================================================================

/// Configuration for subscription limits, quotas, and backpressure
///
/// Provides configurable limits to prevent resource exhaustion attacks
/// and ensure fair resource sharing between clients.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscriptionConfig {
    /// Maximum subscriptions per connection (default: 100)
    ///
    /// Prevents a single client from creating too many subscriptions
    /// and monopolizing server resources.
    #[serde(default = "default_max_per_connection")]
    pub max_per_connection: usize,

    /// Maximum subscriptions globally across all connections (default: 10,000)
    ///
    /// Sets an upper bound on total subscriptions to ensure predictable
    /// memory usage and performance.
    #[serde(default = "default_max_global")]
    pub max_global: usize,

    /// Maximum result set size per subscription in rows (default: 10,000)
    ///
    /// Limits memory usage per subscription by capping the number of rows
    /// that can be returned.
    #[serde(default = "default_max_result_rows")]
    pub max_result_rows: usize,

    /// Rate limit: subscriptions per second per connection (default: 10)
    ///
    /// Prevents rapid subscription creation that could degrade performance.
    #[serde(default = "default_rate_limit_per_second")]
    pub rate_limit_per_second: u32,

    /// Channel buffer size per subscription (default: 64)
    /// Larger values reduce chance of drops but use more memory.
    /// Smaller values detect slow consumers faster.
    #[serde(default = "default_channel_buffer_size")]
    pub channel_buffer_size: usize,

    /// Slow consumer threshold as percentage of buffer full (default: 80)
    /// When channel depth exceeds this percentage, warn about slow consumer
    #[serde(default = "default_slow_consumer_threshold_percent")]
    pub slow_consumer_threshold_percent: u8,

    /// Configuration for selective column updates
    ///
    /// Controls when the server sends partial row updates (only changed columns)
    /// instead of full rows, reducing bandwidth for wide tables with few changes.
    #[serde(default)]
    pub selective_updates: SelectiveColumnConfig,
}

fn default_max_per_connection() -> usize {
    100
}

fn default_max_global() -> usize {
    10_000
}

fn default_max_result_rows() -> usize {
    10_000
}

fn default_rate_limit_per_second() -> u32 {
    10
}

fn default_channel_buffer_size() -> usize {
    64
}

fn default_slow_consumer_threshold_percent() -> u8 {
    80
}

impl Default for SubscriptionConfig {
    fn default() -> Self {
        Self {
            max_per_connection: default_max_per_connection(),
            max_global: default_max_global(),
            max_result_rows: default_max_result_rows(),
            rate_limit_per_second: default_rate_limit_per_second(),
            channel_buffer_size: default_channel_buffer_size(),
            slow_consumer_threshold_percent: default_slow_consumer_threshold_percent(),
            selective_updates: SelectiveColumnConfig::default(),
        }
    }
}

// ============================================================================
// Retry Policy
// ============================================================================

/// Configuration for subscription query retry behavior
///
/// When a subscription query fails during re-execution, it may be automatically
/// retried with exponential backoff if the error is classified as transient.
#[derive(Debug, Clone, PartialEq)]
pub struct SubscriptionRetryPolicy {
    /// Maximum number of retry attempts after initial failure
    ///
    /// Default: 3
    /// Once retries are exhausted, the subscription enters a failed state
    /// and the error is sent to the client.
    pub max_retries: u32,

    /// Base delay for the first retry in milliseconds
    ///
    /// Default: 1000 (1 second)
    /// Used as the starting point for exponential backoff calculation.
    pub base_delay_ms: u64,

    /// Maximum delay between retries in milliseconds
    ///
    /// Default: 30000 (30 seconds)
    /// Exponential backoff is capped at this duration to prevent excessive delays.
    pub max_delay_ms: u64,

    /// Multiplier for exponential backoff
    ///
    /// Default: 2.0
    /// Delay for retry N = base_delay * (multiplier ^ N), capped at max_delay
    pub backoff_multiplier: f64,
}

impl Default for SubscriptionRetryPolicy {
    fn default() -> Self {
        Self { max_retries: 3, base_delay_ms: 1000, max_delay_ms: 30000, backoff_multiplier: 2.0 }
    }
}

impl SubscriptionRetryPolicy {
    /// Calculate the backoff delay for a given retry attempt
    ///
    /// # Arguments
    ///
    /// * `attempt` - The retry attempt number (0-indexed, so first retry is 0)
    ///
    /// # Returns
    ///
    /// Duration to wait before the next retry
    pub(crate) fn calculate_backoff(&self, attempt: u32) -> Duration {
        let backoff_ms = self.base_delay_ms as f64 * self.backoff_multiplier.powi(attempt as i32);

        let capped_ms = backoff_ms.min(self.max_delay_ms as f64);
        Duration::from_millis(capped_ms as u64)
    }
}
