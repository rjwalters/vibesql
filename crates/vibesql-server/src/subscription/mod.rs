//! Query subscription management for real-time reactive updates
//!
//! This module provides the infrastructure for tracking active query subscriptions,
//! receiving change events from the storage layer, and determining which subscriptions
//! need to be notified when data changes.
//!
//! # Overview
//!
//! The subscription system allows clients to register queries for real-time updates.
//! When the underlying data changes, subscriptions are automatically re-evaluated
//! and clients are notified if their results have changed.
//!
//! # Architecture
//!
//! - [`SubscriptionId`]: Unique identifier for each subscription
//! - [`Subscription`]: Individual subscription with query and notification channel
//! - [`SubscriptionManager`]: Central manager tracking all subscriptions
//! - [`SubscriptionUpdate`]: Update notifications sent to subscribers
//! - [`ChangeEvent`]: Events from the storage layer indicating data changes
//!
//! # Example
//!
//! ```ignore
//! use vibesql_server::subscription::{SubscriptionManager, ChangeEvent};
//! use tokio::sync::mpsc;
//!
//! let manager = SubscriptionManager::new();
//! let (tx, mut rx) = mpsc::channel(16);
//!
//! // Subscribe to a query
//! let id = manager.subscribe("SELECT * FROM users WHERE active = true".to_string(), tx)?;
//!
//! // When data changes, the manager checks affected subscriptions
//! manager.handle_change(ChangeEvent::Insert {
//!     table_name: "users".to_string(),
//!     row_id: 42,
//! }).await;
//!
//! // Subscriber receives update if results changed
//! if let Some(update) = rx.recv().await {
//!     println!("Results updated: {:?}", update);
//! }
//! ```

mod manager;
mod router;
pub mod session;
mod table_dependencies;
mod table_extract;
pub mod error;

use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

pub use manager::SubscriptionManager;
pub use router::{ChangeRouter, SubscriptionUpdate as RouterUpdate};
pub use session::{SessionSubscription, SessionSubscriptionId, SessionSubscriptionManager};
pub use table_dependencies::extract_table_dependencies;
pub use table_extract::extract_table_refs;
pub use error::{SubscriptionErrorKind, classify_error, classify_error_str};

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
        }
    }
}

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
        Self {
            max_retries: 3,
            base_delay_ms: 1000,
            max_delay_ms: 30000,
            backoff_multiplier: 2.0,
        }
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
    fn calculate_backoff(&self, attempt: u32) -> Duration {
        let backoff_ms = self.base_delay_ms as f64
            * self.backoff_multiplier.powi(attempt as i32);

        let capped_ms = backoff_ms.min(self.max_delay_ms as f64);
        Duration::from_millis(capped_ms as u64)
    }
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
        /// All rows in the result set
        rows: Vec<crate::Row>,
    },

    /// Incremental delta update
    ///
    /// Contains only the changes since the last update. More efficient
    /// for large result sets with small changes. Sent when the change
    /// can be expressed as a set of inserts, updates, and deletes.
    Delta {
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
        /// Error message describing what went wrong
        message: String,
    },
}

// ============================================================================
// Change Event
// ============================================================================
// Note: ChangeEvent is imported from vibesql_storage and re-exported at the
// crate level for consistency. This ensures the server uses the same event
// type that the storage layer emits.

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

// ============================================================================
// Result Hashing
// ============================================================================

/// Compute a hash of result rows for change detection
///
/// This function hashes the row contents to detect changes without
/// storing the full result set. When the hash changes, we know the
/// results have changed and need to notify subscribers.
pub fn hash_rows(rows: &[crate::Row]) -> u64 {
    use std::collections::hash_map::DefaultHasher;

    let mut hasher = DefaultHasher::new();

    // Hash the number of rows first
    rows.len().hash(&mut hasher);

    // Hash each row's values
    for row in rows {
        for value in &row.values {
            // Hash the SqlValue - using debug format as a simple approach
            // In production, you'd implement proper hashing for SqlValue
            format!("{:?}", value).hash(&mut hasher);
        }
    }

    hasher.finish()
}

/// Compute a hash for a single row (for delta computation)
fn hash_row(row: &crate::Row) -> u64 {
    use std::collections::hash_map::DefaultHasher;

    let mut hasher = DefaultHasher::new();
    for value in &row.values {
        value.hash(&mut hasher);
    }
    hasher.finish()
}

/// Compute delta between old and new result sets
///
/// This function compares two result sets and produces a delta update
/// containing the inserts, updates, and deletes needed to transform
/// the old result into the new result.
///
/// # Algorithm
///
/// Uses row hashing to efficiently detect changes:
/// - Rows in new but not in old are inserts
/// - Rows in old but not in new are deletes
/// - Updates are not detected in this implementation (would appear as delete + insert)
///
/// For proper update detection, primary key information would be needed.
///
/// # Returns
///
/// Returns `Some(SubscriptionUpdate::Delta)` if there are changes,
/// or `None` if the result sets are identical.
pub fn compute_delta(
    old: &[crate::Row],
    new: &[crate::Row],
) -> Option<SubscriptionUpdate> {
    use std::collections::HashMap;

    // Build hash maps for efficient lookup
    // Map from row hash -> (count, row reference)
    // We use count to handle duplicate rows correctly
    let mut old_map: HashMap<u64, Vec<&crate::Row>> = HashMap::new();
    for row in old {
        let hash = hash_row(row);
        old_map.entry(hash).or_default().push(row);
    }

    let mut new_map: HashMap<u64, Vec<&crate::Row>> = HashMap::new();
    for row in new {
        let hash = hash_row(row);
        new_map.entry(hash).or_default().push(row);
    }

    let mut inserts = Vec::new();
    let mut deletes = Vec::new();

    // Find inserts: rows in new but not in old (or with higher count in new)
    for (hash, new_rows) in &new_map {
        let old_rows = old_map.get(hash).map(|v| v.as_slice()).unwrap_or(&[]);

        // For each row in new that exceeds the count in old, it's an insert
        if new_rows.len() > old_rows.len() {
            for row in new_rows.iter().skip(old_rows.len()) {
                inserts.push((*row).clone());
            }
        }
    }

    // Find deletes: rows in old but not in new (or with higher count in old)
    for (hash, old_rows) in &old_map {
        let new_rows = new_map.get(hash).map(|v| v.as_slice()).unwrap_or(&[]);

        // For each row in old that exceeds the count in new, it's a delete
        if old_rows.len() > new_rows.len() {
            for row in old_rows.iter().skip(new_rows.len()) {
                deletes.push((*row).clone());
            }
        }
    }

    // If no changes, return None
    if inserts.is_empty() && deletes.is_empty() {
        return None;
    }

    // Updates are not detected in this implementation
    // A row update would appear as a delete of the old row + insert of the new row
    // This is semantically correct, just not optimal for clients that could patch in place
    let updates = Vec::new();

    Some(SubscriptionUpdate::Delta {
        inserts,
        updates,
        deletes,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_subscription_id_uniqueness() {
        let id1 = SubscriptionId::new();
        let id2 = SubscriptionId::new();
        let id3 = SubscriptionId::new();

        assert_ne!(id1, id2);
        assert_ne!(id2, id3);
        assert_ne!(id1, id3);
    }

    #[test]
    fn test_subscription_id_display() {
        let id = SubscriptionId(42);
        assert_eq!(format!("{}", id), "sub-42");
    }



    #[test]
    fn test_hash_rows_empty() {
        let rows: Vec<crate::Row> = vec![];
        let hash = hash_rows(&rows);
        // Empty rows should produce a consistent hash
        assert_eq!(hash, hash_rows(&[]));
    }

    #[test]
    fn test_hash_rows_different_content() {
        use vibesql_types::SqlValue;

        let rows1 = vec![crate::Row {
            values: vec![SqlValue::Integer(1), SqlValue::Varchar("hello".to_string())],
        }];

        let rows2 = vec![crate::Row {
            values: vec![SqlValue::Integer(2), SqlValue::Varchar("hello".to_string())],
        }];

        let hash1 = hash_rows(&rows1);
        let hash2 = hash_rows(&rows2);

        // Different content should produce different hashes
        assert_ne!(hash1, hash2);
    }

    #[test]
    fn test_hash_rows_same_content() {
        use vibesql_types::SqlValue;

        let rows1 = vec![crate::Row {
            values: vec![SqlValue::Integer(42), SqlValue::Varchar("test".to_string())],
        }];

        let rows2 = vec![crate::Row {
            values: vec![SqlValue::Integer(42), SqlValue::Varchar("test".to_string())],
        }];

        let hash1 = hash_rows(&rows1);
        let hash2 = hash_rows(&rows2);

        // Same content should produce same hash
        assert_eq!(hash1, hash2);
    }

    // ========================================================================
    // Tests for compute_delta
    // ========================================================================

    #[test]
    fn test_compute_delta_no_changes() {
        use vibesql_types::SqlValue;

        let rows = vec![
            crate::Row {
                values: vec![SqlValue::Integer(1), SqlValue::Varchar("Alice".to_string())],
            },
            crate::Row {
                values: vec![SqlValue::Integer(2), SqlValue::Varchar("Bob".to_string())],
            },
        ];

        // Same old and new should return None
        let delta = compute_delta(&rows, &rows);
        assert!(delta.is_none());
    }

    #[test]
    fn test_compute_delta_single_insert() {
        use vibesql_types::SqlValue;

        let old = vec![crate::Row {
            values: vec![SqlValue::Integer(1), SqlValue::Varchar("Alice".to_string())],
        }];

        let new = vec![
            crate::Row {
                values: vec![SqlValue::Integer(1), SqlValue::Varchar("Alice".to_string())],
            },
            crate::Row {
                values: vec![SqlValue::Integer(2), SqlValue::Varchar("Bob".to_string())],
            },
        ];

        let delta = compute_delta(&old, &new);
        assert!(delta.is_some());

        match delta.unwrap() {
            SubscriptionUpdate::Delta {
                inserts,
                updates,
                deletes,
            } => {
                assert_eq!(inserts.len(), 1);
                assert_eq!(inserts[0].values[0], SqlValue::Integer(2));
                assert_eq!(
                    inserts[0].values[1],
                    SqlValue::Varchar("Bob".to_string())
                );
                assert!(updates.is_empty());
                assert!(deletes.is_empty());
            }
            _ => panic!("Expected Delta update"),
        }
    }

    #[test]
    fn test_compute_delta_single_delete() {
        use vibesql_types::SqlValue;

        let old = vec![
            crate::Row {
                values: vec![SqlValue::Integer(1), SqlValue::Varchar("Alice".to_string())],
            },
            crate::Row {
                values: vec![SqlValue::Integer(2), SqlValue::Varchar("Bob".to_string())],
            },
        ];

        let new = vec![crate::Row {
            values: vec![SqlValue::Integer(1), SqlValue::Varchar("Alice".to_string())],
        }];

        let delta = compute_delta(&old, &new);
        assert!(delta.is_some());

        match delta.unwrap() {
            SubscriptionUpdate::Delta {
                inserts,
                updates,
                deletes,
            } => {
                assert!(inserts.is_empty());
                assert!(updates.is_empty());
                assert_eq!(deletes.len(), 1);
                assert_eq!(deletes[0].values[0], SqlValue::Integer(2));
            }
            _ => panic!("Expected Delta update"),
        }
    }

    #[test]
    fn test_compute_delta_insert_and_delete() {
        use vibesql_types::SqlValue;

        let old = vec![crate::Row {
            values: vec![SqlValue::Integer(1), SqlValue::Varchar("Alice".to_string())],
        }];

        let new = vec![crate::Row {
            values: vec![SqlValue::Integer(2), SqlValue::Varchar("Bob".to_string())],
        }];

        let delta = compute_delta(&old, &new);
        assert!(delta.is_some());

        match delta.unwrap() {
            SubscriptionUpdate::Delta {
                inserts,
                updates,
                deletes,
            } => {
                assert_eq!(inserts.len(), 1);
                assert_eq!(deletes.len(), 1);
                assert!(updates.is_empty());
                // The old row was deleted, new row was inserted
                assert_eq!(inserts[0].values[0], SqlValue::Integer(2));
                assert_eq!(deletes[0].values[0], SqlValue::Integer(1));
            }
            _ => panic!("Expected Delta update"),
        }
    }

    #[test]
    fn test_compute_delta_empty_to_rows() {
        use vibesql_types::SqlValue;

        let old: Vec<crate::Row> = vec![];
        let new = vec![
            crate::Row {
                values: vec![SqlValue::Integer(1), SqlValue::Varchar("Alice".to_string())],
            },
            crate::Row {
                values: vec![SqlValue::Integer(2), SqlValue::Varchar("Bob".to_string())],
            },
        ];

        let delta = compute_delta(&old, &new);
        assert!(delta.is_some());

        match delta.unwrap() {
            SubscriptionUpdate::Delta {
                inserts,
                updates,
                deletes,
            } => {
                assert_eq!(inserts.len(), 2);
                assert!(updates.is_empty());
                assert!(deletes.is_empty());
            }
            _ => panic!("Expected Delta update"),
        }
    }

    #[test]
    fn test_compute_delta_rows_to_empty() {
        use vibesql_types::SqlValue;

        let old = vec![
            crate::Row {
                values: vec![SqlValue::Integer(1), SqlValue::Varchar("Alice".to_string())],
            },
            crate::Row {
                values: vec![SqlValue::Integer(2), SqlValue::Varchar("Bob".to_string())],
            },
        ];
        let new: Vec<crate::Row> = vec![];

        let delta = compute_delta(&old, &new);
        assert!(delta.is_some());

        match delta.unwrap() {
            SubscriptionUpdate::Delta {
                inserts,
                updates,
                deletes,
            } => {
                assert!(inserts.is_empty());
                assert!(updates.is_empty());
                assert_eq!(deletes.len(), 2);
            }
            _ => panic!("Expected Delta update"),
        }
    }

    #[test]
    fn test_compute_delta_duplicate_rows() {
        use vibesql_types::SqlValue;

        // Test handling of duplicate rows
        let old = vec![
            crate::Row {
                values: vec![SqlValue::Integer(1)],
            },
            crate::Row {
                values: vec![SqlValue::Integer(1)],
            },
        ];

        let new = vec![
            crate::Row {
                values: vec![SqlValue::Integer(1)],
            },
            crate::Row {
                values: vec![SqlValue::Integer(1)],
            },
            crate::Row {
                values: vec![SqlValue::Integer(1)],
            },
        ];

        let delta = compute_delta(&old, &new);
        assert!(delta.is_some());

        match delta.unwrap() {
            SubscriptionUpdate::Delta {
                inserts,
                updates,
                deletes,
            } => {
                // One additional duplicate row was inserted
                assert_eq!(inserts.len(), 1);
                assert!(updates.is_empty());
                assert!(deletes.is_empty());
            }
            _ => panic!("Expected Delta update"),
        }
    }
}
