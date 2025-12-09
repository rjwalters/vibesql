//! Core subscription types
//!
//! This module defines the fundamental types for the subscription system:
//! - `SubscriptionId`: Unique identifier for subscriptions
//! - `Subscription`: Individual subscription with query and notification channel
//! - `SubscriptionMetrics`: Metrics for monitoring subscription health
//! - `SubscriptionUpdate`: Update notifications sent to subscribers
//! - `SubscriptionError`: Errors that can occur during subscription operations

use std::collections::HashSet;

use tokio::sync::mpsc;

use super::config::{SubscriptionConfig, SubscriptionRetryPolicy};
use super::delta::PartialRowDelta;
use super::selective::SelectiveColumnConfig;

// ============================================================================
// Subscription ID
// ============================================================================

/// Unique subscription identifier
///
/// Each subscription is assigned a unique ID when created. This ID is used
/// to track the subscription throughout its lifecycle and to unsubscribe.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SubscriptionId(u64);

impl SubscriptionId {
    /// Create a new unique subscription ID
    ///
    /// Uses an atomic counter to ensure uniqueness across all threads.
    pub fn new() -> Self {
        use std::sync::atomic::{AtomicU64, Ordering};
        static COUNTER: AtomicU64 = AtomicU64::new(1);
        Self(COUNTER.fetch_add(1, Ordering::Relaxed))
    }

    /// Get the raw ID value (for debugging/logging)
    pub fn as_u64(&self) -> u64 {
        self.0
    }
}

impl Default for SubscriptionId {
    fn default() -> Self {
        Self::new()
    }
}

impl std::fmt::Display for SubscriptionId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "sub-{}", self.0)
    }
}

// ============================================================================
// Subscription Metrics
// ============================================================================

/// Metrics for a single subscription
///
/// Provides observability into subscription health and backpressure.
#[derive(Debug, Clone, Default)]
pub struct SubscriptionMetrics {
    /// Subscription ID
    pub subscription_id: Option<SubscriptionId>,
    /// Total updates successfully sent to this subscription
    pub updates_sent: u64,
    /// Total updates dropped due to channel being full
    pub updates_dropped: u64,
    /// Configured channel buffer size
    pub channel_buffer_size: usize,
    /// Current channel capacity (available slots)
    pub channel_capacity: usize,
    /// Slow consumer threshold percentage
    pub slow_consumer_threshold_percent: u8,
}

// ============================================================================
// Subscription
// ============================================================================

/// A single query subscription
///
/// Tracks the query, its table dependencies, and the channel for sending updates.
#[derive(Debug)]
pub struct Subscription {
    /// Unique identifier for this subscription
    pub id: SubscriptionId,
    /// The SQL query being monitored
    pub query: String,
    /// Tables this query depends on (extracted from AST)
    pub tables: HashSet<String>,
    /// Hash of the last result set (for change detection)
    pub last_result_hash: u64,
    /// Last result set (for delta computation)
    /// This stores the previous result to enable computing deltas on change.
    pub last_result: Option<Vec<crate::Row>>,
    /// Channel to send updates to the subscriber
    pub notify_tx: mpsc::Sender<SubscriptionUpdate>,
    /// Retry policy for handling transient errors
    pub retry_policy: SubscriptionRetryPolicy,
    /// Current retry attempt count (resets on successful execution)
    pub retry_count: u32,
    /// Total updates sent to this subscription
    pub updates_sent: u64,
    /// Total updates dropped due to channel being full
    pub updates_dropped: u64,
    /// Buffer size for the subscription channel
    pub channel_buffer_size: usize,
    /// Slow consumer threshold percentage
    pub slow_consumer_threshold_percent: u8,
    /// Optional connection/session ID that owns this subscription
    /// Used for connection-level subscription tracking and cleanup
    pub connection_id: Option<String>,
    /// Optional wire protocol subscription ID (UUID bytes)
    /// Used to bridge between wire protocol IDs and internal SubscriptionId
    pub wire_subscription_id: Option<[u8; 16]>,
    /// Optional filter expression (SQL WHERE clause) to apply to updates
    /// Only rows matching the filter will be included in subscription updates
    pub filter: Option<String>,
    /// Primary key column indices in the result set
    /// Used for selective column updates to always include PK columns
    /// Default: [0] (assumes first column is PK if not detected)
    pub pk_columns: Vec<usize>,
    /// Whether this subscription is eligible for selective column updates
    /// True when PK columns were confidently detected
    pub selective_eligible: bool,
    /// Per-subscription override for selective column update configuration
    /// If set, this overrides the server-level selective_updates config for this subscription
    pub selective_updates_override: Option<SelectiveColumnConfig>,
}

impl Subscription {
    /// Create a new subscription
    pub fn new(
        query: String,
        tables: HashSet<String>,
        notify_tx: mpsc::Sender<SubscriptionUpdate>,
    ) -> Self {
        Self::with_policy(query, tables, notify_tx, SubscriptionRetryPolicy::default())
    }

    /// Create a new subscription with a custom retry policy
    pub fn with_policy(
        query: String,
        tables: HashSet<String>,
        notify_tx: mpsc::Sender<SubscriptionUpdate>,
        retry_policy: SubscriptionRetryPolicy,
    ) -> Self {
        Self {
            id: SubscriptionId::new(),
            query,
            tables,
            last_result_hash: 0,
            last_result: None,
            notify_tx,
            retry_policy,
            retry_count: 0,
            updates_sent: 0,
            updates_dropped: 0,
            channel_buffer_size: 64, // default buffer size
            slow_consumer_threshold_percent: 80,
            connection_id: None,
            wire_subscription_id: None,
            filter: None,
            pk_columns: vec![0], // default: assume first column is PK
            selective_eligible: false,
            selective_updates_override: None,
        }
    }

    /// Create a new subscription with custom configuration
    pub fn with_config(
        query: String,
        tables: HashSet<String>,
        notify_tx: mpsc::Sender<SubscriptionUpdate>,
        config: &SubscriptionConfig,
    ) -> Self {
        Self {
            id: SubscriptionId::new(),
            query,
            tables,
            last_result_hash: 0,
            last_result: None,
            notify_tx,
            retry_policy: SubscriptionRetryPolicy::default(),
            retry_count: 0,
            updates_sent: 0,
            updates_dropped: 0,
            channel_buffer_size: config.channel_buffer_size,
            slow_consumer_threshold_percent: config.slow_consumer_threshold_percent,
            connection_id: None,
            wire_subscription_id: None,
            filter: None,
            pk_columns: vec![0], // default: assume first column is PK
            selective_eligible: false,
            selective_updates_override: None,
        }
    }

    /// Create a new subscription for a specific connection (wire protocol)
    ///
    /// This associates the subscription with a connection ID for tracking
    /// and cleanup when the connection closes.
    pub fn for_connection(
        query: String,
        tables: HashSet<String>,
        notify_tx: mpsc::Sender<SubscriptionUpdate>,
        connection_id: String,
        wire_subscription_id: [u8; 16],
        filter: Option<String>,
        config: &SubscriptionConfig,
    ) -> Self {
        Self::for_connection_with_pk(
            query,
            tables,
            notify_tx,
            connection_id,
            wire_subscription_id,
            filter,
            config,
            vec![0], // default: assume first column is PK
        )
    }

    /// Create a new subscription for a specific connection with custom PK columns
    ///
    /// This associates the subscription with a connection ID for tracking
    /// and cleanup when the connection closes. It also allows specifying
    /// which columns are primary keys for selective column updates.
    #[allow(clippy::too_many_arguments)]
    pub fn for_connection_with_pk(
        query: String,
        tables: HashSet<String>,
        notify_tx: mpsc::Sender<SubscriptionUpdate>,
        connection_id: String,
        wire_subscription_id: [u8; 16],
        filter: Option<String>,
        config: &SubscriptionConfig,
        pk_columns: Vec<usize>,
    ) -> Self {
        Self {
            id: SubscriptionId::new(),
            query,
            tables,
            last_result_hash: 0,
            last_result: None,
            notify_tx,
            retry_policy: SubscriptionRetryPolicy::default(),
            retry_count: 0,
            updates_sent: 0,
            updates_dropped: 0,
            channel_buffer_size: config.channel_buffer_size,
            slow_consumer_threshold_percent: config.slow_consumer_threshold_percent,
            connection_id: Some(connection_id),
            wire_subscription_id: Some(wire_subscription_id),
            filter,
            pk_columns,
            selective_eligible: false,
            selective_updates_override: None,
        }
    }

    /// Set the primary key columns for this subscription
    ///
    /// Used after detection to update the subscription with actual PK columns.
    pub fn set_pk_columns(&mut self, pk_columns: Vec<usize>) {
        self.pk_columns = pk_columns;
    }

    /// Set both PK columns and selective eligibility
    ///
    /// Used after PK detection to update the subscription.
    /// Returns true if the subscription is newly marked as selective-eligible.
    pub fn set_pk_columns_with_eligibility(
        &mut self,
        pk_columns: Vec<usize>,
        confident: bool,
    ) -> bool {
        self.pk_columns = pk_columns;
        let was_eligible = self.selective_eligible;
        self.selective_eligible = confident;
        // Return true if newly eligible (wasn't before, is now)
        !was_eligible && confident
    }

    /// Set per-subscription selective updates override
    ///
    /// Allows clients to override server-level selective update thresholds
    /// on a per-subscription basis.
    pub fn set_selective_updates_override(&mut self, config: SelectiveColumnConfig) {
        self.selective_updates_override = Some(config);
    }

    /// Clear the selective updates override (use server defaults)
    pub fn clear_selective_updates_override(&mut self) {
        self.selective_updates_override = None;
    }

    /// Get the effective selective column update config for this subscription
    ///
    /// Returns the per-subscription override if set, otherwise creates a config
    /// from the server-level config with this subscription's PK columns.
    pub fn get_effective_selective_config(
        &self,
        server_config: &SelectiveColumnConfig,
    ) -> SelectiveColumnConfig {
        match &self.selective_updates_override {
            Some(override_config) => {
                // Use override but ensure pk_columns is always from the subscription
                override_config.with_pk_columns(self.pk_columns.clone())
            }
            None => {
                // Use server config with this subscription's pk_columns
                server_config.with_pk_columns(self.pk_columns.clone())
            }
        }
    }
}

// ============================================================================
// Subscription Update
// ============================================================================

/// Update notification sent to subscribers
///
/// When a subscription's results change, an update is sent through the
/// subscription's notification channel.
#[derive(Debug, Clone)]
pub enum SubscriptionUpdate {
    /// Full result set (initial subscription or major change)
    ///
    /// Contains all rows matching the query. This is sent when:
    /// - A new subscription is created (initial results)
    /// - The results have changed and delta calculation isn't available
    Full {
        /// The subscription ID this update is for
        subscription_id: SubscriptionId,
        /// All rows in the result set
        rows: Vec<crate::Row>,
    },

    /// Incremental delta update
    ///
    /// Contains only the changes since the last update. More efficient
    /// for large result sets with small changes. Sent when the change
    /// can be expressed as a set of inserts, updates, and deletes.
    Delta {
        /// The subscription ID this update is for
        subscription_id: SubscriptionId,
        /// Newly inserted rows (in new result, not in previous)
        inserts: Vec<crate::Row>,
        /// Updated rows (old value, new value) - rows with same identity but different content
        updates: Vec<(crate::Row, crate::Row)>,
        /// Deleted rows (in previous result, not in new)
        deletes: Vec<crate::Row>,
    },

    /// Query execution error
    ///
    /// Sent when the subscription query fails to execute, typically due to
    /// schema changes that invalidate the query.
    Error {
        /// The subscription ID this update is for
        subscription_id: SubscriptionId,
        /// Error message describing what went wrong
        message: String,
    },

    /// Partial row updates (selective column updates)
    ///
    /// Sent when a subscription is eligible for selective column updates and
    /// only a subset of columns have changed. Contains only the changed columns
    /// plus the primary key columns, reducing bandwidth for wide tables.
    ///
    /// This is more efficient than Delta for tables with many columns where
    /// only a few columns change at a time.
    Partial {
        /// The subscription ID this update is for
        subscription_id: SubscriptionId,
        /// Partial row updates, each containing only changed columns + PK columns
        updates: Vec<PartialRowDelta>,
    },
}

impl SubscriptionUpdate {
    /// Get the subscription ID this update is for
    pub fn subscription_id(&self) -> SubscriptionId {
        match self {
            SubscriptionUpdate::Full { subscription_id, .. } => *subscription_id,
            SubscriptionUpdate::Delta { subscription_id, .. } => *subscription_id,
            SubscriptionUpdate::Error { subscription_id, .. } => *subscription_id,
            SubscriptionUpdate::Partial { subscription_id, .. } => *subscription_id,
        }
    }
}

// ============================================================================
// Subscription Error
// ============================================================================

/// Errors that can occur during subscription operations
#[derive(Debug, thiserror::Error)]
pub enum SubscriptionError {
    /// Failed to parse the subscription query
    #[error("Failed to parse query: {0}")]
    ParseError(String),

    /// The query references unknown tables
    #[error("Query references unknown table: {0}")]
    UnknownTable(String),

    /// The subscription was not found
    #[error("Subscription not found: {0}")]
    NotFound(SubscriptionId),

    /// Failed to send notification to subscriber
    #[error("Failed to send notification: channel closed")]
    ChannelClosed,

    /// Per-connection subscription limit exceeded
    #[error("Connection limit exceeded: {current} subscriptions (max: {max})")]
    ConnectionLimitExceeded {
        /// Current number of subscriptions for this connection
        current: usize,
        /// Maximum allowed subscriptions per connection
        max: usize,
    },

    /// Global subscription limit exceeded
    #[error("Global limit exceeded: {current} subscriptions (max: {max})")]
    GlobalLimitExceeded {
        /// Current total subscriptions across all connections
        current: usize,
        /// Maximum allowed subscriptions globally
        max: usize,
    },

    /// Result set too large for subscription
    #[error("Result set too large: {rows} rows (max: {max})")]
    ResultSetTooLarge {
        /// Number of rows in the result set
        rows: usize,
        /// Maximum allowed rows per subscription
        max: usize,
    },

    /// Rate limit exceeded for subscription creation
    #[error("Rate limited: retry after {retry_after_ms}ms")]
    RateLimited {
        /// Milliseconds to wait before retrying
        retry_after_ms: u64,
    },
}
