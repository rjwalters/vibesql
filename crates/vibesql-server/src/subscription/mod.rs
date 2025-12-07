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

pub mod error;
pub mod filter;
mod manager;
pub mod pk_detector;
mod router;
pub mod session;
mod table_dependencies;
mod table_extract;

use std::collections::HashSet;
use std::hash::{Hash, Hasher};
use std::time::Duration;

use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;

pub use error::{classify_error, classify_error_str, SubscriptionErrorKind};
pub use manager::SubscriptionManager;
pub use pk_detector::{detect_pk_columns, detect_pk_columns_from_stmt, PkDetectionResult};
pub use router::{ChangeRouter, SubscriptionUpdate as RouterUpdate};
pub use session::{SessionSubscription, SessionSubscriptionId, TablePkInfo};
pub use table_dependencies::extract_table_dependencies;
pub use table_extract::extract_table_refs;
// SubscriptionMetrics is defined inline in this module and exported directly

// Re-export selective column update types (defined later in this file)
// SelectiveColumnConfig, ColumnDiff, compute_column_diff, should_use_selective_update, create_partial_row_update

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
    fn calculate_backoff(&self, attempt: u32) -> Duration {
        let backoff_ms = self.base_delay_ms as f64 * self.backoff_multiplier.powi(attempt as i32);

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
    /// Configuration for selective column updates
    /// Per-subscription overrides for server-level config
    pub selective_updates: SelectiveColumnConfig,
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
            selective_updates: SelectiveColumnConfig::default(),
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
            selective_updates: SelectiveColumnConfig::default(),
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
            selective_updates: SelectiveColumnConfig::default(),
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
}

// ============================================================================
// Partial Row Delta (for selective column updates)
// ============================================================================

/// A partial row update containing only changed columns plus primary key columns
///
/// Used for efficient updates when only a subset of columns have changed.
/// The `column_indices` field indicates which columns are present in `values`.
#[derive(Debug, Clone)]
pub struct PartialRowDelta {
    /// Indices of columns that are included in this partial update
    /// (primary key columns + changed columns, sorted)
    pub column_indices: Vec<usize>,
    /// Old values for the included columns
    pub old_values: Vec<vibesql_types::SqlValue>,
    /// New values for the included columns
    pub new_values: Vec<vibesql_types::SqlValue>,
}

impl PartialRowDelta {
    /// Create a new partial row delta from old and new rows
    ///
    /// # Arguments
    /// * `old_row` - The previous row values
    /// * `new_row` - The current row values
    /// * `pk_columns` - Primary key column indices (always included)
    ///
    /// # Returns
    /// * `Some(PartialRowDelta)` if the rows differ
    /// * `None` if the rows are identical
    pub fn from_rows(
        old_row: &crate::Row,
        new_row: &crate::Row,
        pk_columns: &[usize],
    ) -> Option<Self> {
        if old_row.values.len() != new_row.values.len() {
            return None;
        }

        // Find changed columns
        let mut changed_columns = Vec::new();
        for (idx, (old_val, new_val)) in
            old_row.values.iter().zip(new_row.values.iter()).enumerate()
        {
            if old_val != new_val {
                changed_columns.push(idx);
            }
        }

        // If no columns changed, return None
        if changed_columns.is_empty() {
            return None;
        }

        // Build included columns: PK columns + changed columns, sorted
        let mut column_indices: Vec<usize> = pk_columns.to_vec();
        for &idx in &changed_columns {
            if !column_indices.contains(&idx) {
                column_indices.push(idx);
            }
        }
        column_indices.sort_unstable();

        // Extract values for included columns
        let old_values: Vec<vibesql_types::SqlValue> =
            column_indices.iter().map(|&idx| old_row.values[idx].clone()).collect();
        let new_values: Vec<vibesql_types::SqlValue> =
            column_indices.iter().map(|&idx| new_row.values[idx].clone()).collect();

        Some(Self { column_indices, old_values, new_values })
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
/// For proper update detection, use `compute_delta_with_pk()` with primary key information.
///
/// # Returns
///
/// Returns `Some(SubscriptionUpdate::Delta)` if there are changes,
/// or `None` if the result sets are identical.
pub fn compute_delta(
    subscription_id: SubscriptionId,
    old: &[crate::Row],
    new: &[crate::Row],
) -> Option<SubscriptionUpdate> {
    // Delegate to PK-based implementation with empty pk_columns for backward compatibility
    compute_delta_with_pk(subscription_id, old, new, &[])
}

/// Compute delta between old and new result sets using primary key columns
///
/// This function compares two result sets and produces a delta update
/// containing the inserts, updates, and deletes needed to transform
/// the old result into the new result.
///
/// # Algorithm
///
/// When `pk_columns` is provided and non-empty:
/// - Builds a lookup map of old rows indexed by their PK values
/// - For each new row, looks up by PK to determine if it's an INSERT or UPDATE
/// - Rows in old but not in new (by PK) are DELETEs
/// - Rows with same PK but different content are UPDATEs
///
/// When `pk_columns` is empty, falls back to hash-based matching:
/// - Rows in new but not in old are inserts
/// - Rows in old but not in new are deletes
/// - Updates appear as delete + insert pairs
///
/// # Arguments
///
/// * `subscription_id` - The subscription ID for the delta update
/// * `old` - Previous result set rows
/// * `new` - Current result set rows
/// * `pk_columns` - Indices of primary key columns in the result set
///
/// # Returns
///
/// Returns `Some(SubscriptionUpdate::Delta)` if there are changes,
/// or `None` if the result sets are identical.
pub fn compute_delta_with_pk(
    subscription_id: SubscriptionId,
    old: &[crate::Row],
    new: &[crate::Row],
    pk_columns: &[usize],
) -> Option<SubscriptionUpdate> {
    use std::collections::HashMap;

    // If no PK columns provided, use hash-based matching
    if pk_columns.is_empty() {
        return compute_delta_hash_based(subscription_id, old, new);
    }

    // Validate PK columns are within bounds for both old and new rows
    let valid_pk = old.iter().chain(new.iter()).all(|row| {
        pk_columns.iter().all(|&idx| idx < row.values.len())
    });

    if !valid_pk {
        // Fall back to hash-based if PK columns are out of bounds
        return compute_delta_hash_based(subscription_id, old, new);
    }

    // Build a lookup map of old rows indexed by PK values
    // Key: PK values as a vector, Value: list of (index, row) for handling duplicates
    let mut old_by_pk: HashMap<Vec<&vibesql_types::SqlValue>, Vec<(usize, &crate::Row)>> =
        HashMap::new();
    for (idx, row) in old.iter().enumerate() {
        let pk_values: Vec<&vibesql_types::SqlValue> =
            pk_columns.iter().map(|&i| &row.values[i]).collect();
        old_by_pk.entry(pk_values).or_default().push((idx, row));
    }

    let mut inserts = Vec::new();
    let mut updates: Vec<(crate::Row, crate::Row)> = Vec::new();
    let mut matched_old_indices = std::collections::HashSet::new();

    // Process each new row
    for new_row in new {
        let pk_values: Vec<&vibesql_types::SqlValue> =
            pk_columns.iter().map(|&i| &new_row.values[i]).collect();

        if let Some(old_rows) = old_by_pk.get_mut(&pk_values) {
            // Found matching PK in old - check if it's an update or unchanged
            if let Some((old_idx, old_row)) = old_rows.pop() {
                matched_old_indices.insert(old_idx);

                // Compare full row content to detect changes
                if old_row.values != new_row.values {
                    // Content differs - this is an UPDATE
                    updates.push((old_row.clone(), new_row.clone()));
                }
                // If content is identical, row is unchanged - no action needed
            } else {
                // No more old rows with this PK - treat as insert
                // (handles case where new has more duplicates than old)
                inserts.push(new_row.clone());
            }
        } else {
            // No matching PK in old - this is an INSERT
            inserts.push(new_row.clone());
        }
    }

    // Find deletes: old rows that weren't matched
    let deletes: Vec<crate::Row> = old
        .iter()
        .enumerate()
        .filter(|(idx, _)| !matched_old_indices.contains(idx))
        .map(|(_, row)| row.clone())
        .collect();

    // If no changes, return None
    if inserts.is_empty() && updates.is_empty() && deletes.is_empty() {
        return None;
    }

    Some(SubscriptionUpdate::Delta { subscription_id, inserts, updates, deletes })
}

/// Hash-based delta computation (original algorithm)
///
/// This is the fallback when PK columns are not available.
fn compute_delta_hash_based(
    subscription_id: SubscriptionId,
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

    // Updates are not detected in hash-based mode
    // A row update would appear as a delete of the old row + insert of the new row
    let updates = Vec::new();

    Some(SubscriptionUpdate::Delta { subscription_id, inserts, updates, deletes })
}

// ============================================================================
// Selective Column Updates
// ============================================================================

/// Configuration for selective column updates
///
/// This config controls when selective column updates (0xF7 messages) are used
/// instead of full row updates. Selective updates only send changed columns
/// plus primary key columns, reducing bandwidth for wide tables with few changes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SelectiveColumnConfig {
    /// Enable selective column updates
    #[serde(default = "default_selective_enabled")]
    pub enabled: bool,
    /// Column indices that are primary key columns (always included)
    /// This is per-subscription and not configurable via config file
    #[serde(skip)]
    pub pk_columns: Vec<usize>,
    /// Minimum columns that must change to use selective update
    /// If fewer columns change, send full row instead
    #[serde(default = "default_min_changed_columns")]
    pub min_changed_columns: usize,
    /// Maximum ratio of changed columns before falling back to full row
    /// E.g., 0.5 means if >50% of columns changed, send full row instead
    #[serde(default = "default_max_changed_columns_ratio")]
    pub max_changed_columns_ratio: f64,
}

fn default_selective_enabled() -> bool {
    true
}

fn default_min_changed_columns() -> usize {
    1
}

fn default_max_changed_columns_ratio() -> f64 {
    0.5
}

impl Default for SelectiveColumnConfig {
    fn default() -> Self {
        Self {
            enabled: default_selective_enabled(),
            pk_columns: vec![0], // Assume first column is PK by default
            min_changed_columns: default_min_changed_columns(),
            max_changed_columns_ratio: default_max_changed_columns_ratio(),
        }
    }
}

/// Result of column-level diff computation
#[derive(Debug, Clone)]
pub struct ColumnDiff {
    /// Indices of columns that changed
    pub changed_columns: Vec<usize>,
    /// Indices of columns to include (PK + changed)
    pub included_columns: Vec<usize>,
}

/// Compute which columns differ between two rows
///
/// # Arguments
/// * `old_row` - The previous row values
/// * `new_row` - The current row values
/// * `pk_columns` - Indices of primary key columns (always included even if unchanged)
///
/// # Returns
/// * `Some(ColumnDiff)` if rows have same column count and some columns differ
/// * `None` if rows have different column counts or are identical
pub fn compute_column_diff(
    old_row: &crate::Row,
    new_row: &crate::Row,
    pk_columns: &[usize],
) -> Option<ColumnDiff> {
    // Rows must have same number of columns
    if old_row.values.len() != new_row.values.len() {
        return None;
    }

    let mut changed_columns = Vec::new();

    // Compare each column
    for (idx, (old_val, new_val)) in old_row.values.iter().zip(new_row.values.iter()).enumerate() {
        if old_val != new_val {
            changed_columns.push(idx);
        }
    }

    // If no columns changed, return None
    if changed_columns.is_empty() {
        return None;
    }

    // Build included columns: PK columns + changed columns
    let mut included_columns: Vec<usize> = pk_columns.to_vec();
    for &idx in &changed_columns {
        if !included_columns.contains(&idx) {
            included_columns.push(idx);
        }
    }
    included_columns.sort_unstable();

    Some(ColumnDiff { changed_columns, included_columns })
}

/// Determine if selective update should be used based on configuration
///
/// Returns true if:
/// - Selective updates are enabled
/// - Number of changed columns meets minimum threshold
/// - Changed column ratio doesn't exceed maximum
pub fn should_use_selective_update(
    diff: &ColumnDiff,
    total_columns: usize,
    config: &SelectiveColumnConfig,
) -> bool {
    if !config.enabled {
        return false;
    }

    // Check minimum changed columns
    if diff.changed_columns.len() < config.min_changed_columns {
        return false;
    }

    // Check maximum ratio
    let changed_ratio = diff.changed_columns.len() as f64 / total_columns as f64;
    if changed_ratio > config.max_changed_columns_ratio {
        return false;
    }

    true
}

pub fn should_use_selective_update_with_metrics(
    diff: &ColumnDiff,
    total_columns: usize,
    config: &SelectiveColumnConfig,
    metrics: Option<&crate::observability::metrics::ServerMetrics>,
) -> bool {
    if !config.enabled {
        if let Some(m) = metrics {
            m.record_partial_update_fallback("disabled");
        }
        return false;
    }

    // Check minimum changed columns
    if diff.changed_columns.len() < config.min_changed_columns {
        return false;
    }

    // Check maximum ratio
    let changed_ratio = diff.changed_columns.len() as f64 / total_columns as f64;
    if changed_ratio > config.max_changed_columns_ratio {
        if let Some(m) = metrics {
            m.record_partial_update_fallback("threshold_exceeded");
        }
        return false;
    }

    true
}

/// Create a partial row update from old and new rows
///
/// # Arguments
/// * `old_row` - The previous row values (wire format)
/// * `new_row` - The current row values (wire format)
/// * `pk_columns` - Primary key column indices
/// * `config` - Selective column configuration
///
/// # Returns
/// * `Some(PartialRowUpdate)` if selective update should be used
/// * `None` if full row should be sent instead
pub fn create_partial_row_update(
    old_row: &[Option<Vec<u8>>],
    new_row: &[Option<Vec<u8>>],
    pk_columns: &[usize],
    config: &SelectiveColumnConfig,
) -> Option<crate::protocol::messages::PartialRowUpdate> {
    // Rows must have same number of columns
    if old_row.len() != new_row.len() {
        return None;
    }

    let total_columns = new_row.len();
    let mut changed_columns = Vec::new();

    // Compare each column
    for (idx, (old_val, new_val)) in old_row.iter().zip(new_row.iter()).enumerate() {
        if old_val != new_val {
            changed_columns.push(idx);
        }
    }

    // If no columns changed, return None
    if changed_columns.is_empty() {
        return None;
    }

    // Check if we should use selective update
    let changed_ratio = changed_columns.len() as f64 / total_columns as f64;
    if !config.enabled || changed_ratio > config.max_changed_columns_ratio {
        return None;
    }

    // Build included columns: PK columns + changed columns, sorted
    let mut included_columns: Vec<usize> = pk_columns.to_vec();
    for &idx in &changed_columns {
        if !included_columns.contains(&idx) {
            included_columns.push(idx);
        }
    }
    included_columns.sort_unstable();

    // Extract values for included columns
    let values: Vec<Option<Vec<u8>>> =
        included_columns.iter().map(|&idx| new_row[idx].clone()).collect();

    // Convert to u16 for protocol
    let present_columns: Vec<u16> = included_columns.iter().map(|&idx| idx as u16).collect();

    Some(crate::protocol::messages::PartialRowUpdate::new(
        total_columns as u16,
        &present_columns,
        values,
    ))
}

/// Create a partial row update from old and new rows with metrics recording
///
/// # Arguments
/// * `old_row` - The previous row values (wire format)
/// * `new_row` - The current row values (wire format)
/// * `pk_columns` - Primary key column indices
/// * `config` - Selective column configuration
/// * `metrics` - Optional metrics for recording fallback reasons
///
/// # Returns
/// * `Some(PartialRowUpdate)` if selective update should be used
/// * `None` if full row should be sent instead
pub fn create_partial_row_update_with_metrics(
    old_row: &[Option<Vec<u8>>],
    new_row: &[Option<Vec<u8>>],
    pk_columns: &[usize],
    config: &SelectiveColumnConfig,
    metrics: Option<&crate::observability::metrics::ServerMetrics>,
) -> Option<crate::protocol::messages::PartialRowUpdate> {
    // Rows must have same number of columns
    if old_row.len() != new_row.len() {
        if let Some(m) = metrics {
            m.record_partial_update_fallback("row_count_mismatch");
        }
        return None;
    }

    let total_columns = new_row.len();
    let mut changed_columns = Vec::new();

    // Compare each column
    for (idx, (old_val, new_val)) in old_row.iter().zip(new_row.iter()).enumerate() {
        if old_val != new_val {
            changed_columns.push(idx);
        }
    }

    // If no columns changed, return None
    if changed_columns.is_empty() {
        if let Some(m) = metrics {
            m.record_partial_update_fallback("no_changes");
        }
        return None;
    }

    // Check if we should use selective update
    let changed_ratio = changed_columns.len() as f64 / total_columns as f64;
    if !config.enabled || changed_ratio > config.max_changed_columns_ratio {
        if let Some(m) = metrics {
            if !config.enabled {
                m.record_partial_update_fallback("disabled");
            } else {
                m.record_partial_update_fallback("threshold_exceeded");
            }
        }
        return None;
    }

    // Build included columns: PK columns + changed columns, sorted
    let mut included_columns: Vec<usize> = pk_columns.to_vec();
    for &idx in &changed_columns {
        if !included_columns.contains(&idx) {
            included_columns.push(idx);
        }
    }
    included_columns.sort_unstable();

    // Extract values for included columns
    let values: Vec<Option<Vec<u8>>> =
        included_columns.iter().map(|&idx| new_row[idx].clone()).collect();

    // Convert to u16 for protocol
    let present_columns: Vec<u16> = included_columns.iter().map(|&idx| idx as u16).collect();

    Some(crate::protocol::messages::PartialRowUpdate::new(
        total_columns as u16,
        &present_columns,
        values,
    ))
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
            values: vec![SqlValue::Integer(1), SqlValue::Varchar(std::sync::Arc::from("hello"))],
        }];

        let rows2 = vec![crate::Row {
            values: vec![SqlValue::Integer(2), SqlValue::Varchar(std::sync::Arc::from("hello"))],
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
            values: vec![SqlValue::Integer(42), SqlValue::Varchar(std::sync::Arc::from("test"))],
        }];

        let rows2 = vec![crate::Row {
            values: vec![SqlValue::Integer(42), SqlValue::Varchar(std::sync::Arc::from("test"))],
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
                values: vec![SqlValue::Integer(1), SqlValue::Varchar(std::sync::Arc::from("Alice"))],
            },
            crate::Row { values: vec![SqlValue::Integer(2), SqlValue::Varchar(std::sync::Arc::from("Bob"))] },
        ];

        // Same old and new should return None
        let test_id = SubscriptionId::new();
        let delta = compute_delta(test_id, &rows, &rows);
        assert!(delta.is_none());
    }

    #[test]
    fn test_compute_delta_single_insert() {
        use vibesql_types::SqlValue;

        let old = vec![crate::Row {
            values: vec![SqlValue::Integer(1), SqlValue::Varchar(std::sync::Arc::from("Alice"))],
        }];

        let new = vec![
            crate::Row {
                values: vec![SqlValue::Integer(1), SqlValue::Varchar(std::sync::Arc::from("Alice"))],
            },
            crate::Row { values: vec![SqlValue::Integer(2), SqlValue::Varchar(std::sync::Arc::from("Bob"))] },
        ];

        let test_id = SubscriptionId::new();
        let delta = compute_delta(test_id, &old, &new);
        assert!(delta.is_some());

        match delta.unwrap() {
            SubscriptionUpdate::Delta { inserts, updates, deletes, .. } => {
                assert_eq!(inserts.len(), 1);
                assert_eq!(inserts[0].values[0], SqlValue::Integer(2));
                assert_eq!(inserts[0].values[1], SqlValue::Varchar(std::sync::Arc::from("Bob")));
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
                values: vec![SqlValue::Integer(1), SqlValue::Varchar(std::sync::Arc::from("Alice"))],
            },
            crate::Row { values: vec![SqlValue::Integer(2), SqlValue::Varchar(std::sync::Arc::from("Bob"))] },
        ];

        let new = vec![crate::Row {
            values: vec![SqlValue::Integer(1), SqlValue::Varchar(std::sync::Arc::from("Alice"))],
        }];

        let test_id = SubscriptionId::new();
        let delta = compute_delta(test_id, &old, &new);
        assert!(delta.is_some());

        match delta.unwrap() {
            SubscriptionUpdate::Delta { inserts, updates, deletes, .. } => {
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
            values: vec![SqlValue::Integer(1), SqlValue::Varchar(std::sync::Arc::from("Alice"))],
        }];

        let new = vec![crate::Row {
            values: vec![SqlValue::Integer(2), SqlValue::Varchar(std::sync::Arc::from("Bob"))],
        }];

        let test_id = SubscriptionId::new();
        let delta = compute_delta(test_id, &old, &new);
        assert!(delta.is_some());

        match delta.unwrap() {
            SubscriptionUpdate::Delta { inserts, updates, deletes, .. } => {
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
                values: vec![SqlValue::Integer(1), SqlValue::Varchar(std::sync::Arc::from("Alice"))],
            },
            crate::Row { values: vec![SqlValue::Integer(2), SqlValue::Varchar(std::sync::Arc::from("Bob"))] },
        ];

        let test_id = SubscriptionId::new();
        let delta = compute_delta(test_id, &old, &new);
        assert!(delta.is_some());

        match delta.unwrap() {
            SubscriptionUpdate::Delta { inserts, updates, deletes, .. } => {
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
                values: vec![SqlValue::Integer(1), SqlValue::Varchar(std::sync::Arc::from("Alice"))],
            },
            crate::Row { values: vec![SqlValue::Integer(2), SqlValue::Varchar(std::sync::Arc::from("Bob"))] },
        ];
        let new: Vec<crate::Row> = vec![];

        let test_id = SubscriptionId::new();
        let delta = compute_delta(test_id, &old, &new);
        assert!(delta.is_some());

        match delta.unwrap() {
            SubscriptionUpdate::Delta { inserts, updates, deletes, .. } => {
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
            crate::Row { values: vec![SqlValue::Integer(1)] },
            crate::Row { values: vec![SqlValue::Integer(1)] },
        ];

        let new = vec![
            crate::Row { values: vec![SqlValue::Integer(1)] },
            crate::Row { values: vec![SqlValue::Integer(1)] },
            crate::Row { values: vec![SqlValue::Integer(1)] },
        ];

        let test_id = SubscriptionId::new();
        let delta = compute_delta(test_id, &old, &new);
        assert!(delta.is_some());

        match delta.unwrap() {
            SubscriptionUpdate::Delta { inserts, updates, deletes, .. } => {
                // One additional duplicate row was inserted
                assert_eq!(inserts.len(), 1);
                assert!(updates.is_empty());
                assert!(deletes.is_empty());
            }
            _ => panic!("Expected Delta update"),
        }
    }

    // ========================================================================
    // Tests for PK-based Delta Computation
    // ========================================================================

    #[test]
    fn test_compute_delta_with_pk_detects_update() {
        use vibesql_types::SqlValue;

        // Same PK (1), different name value - should be detected as UPDATE
        let old = vec![crate::Row {
            values: vec![SqlValue::Integer(1), SqlValue::Varchar(Arc::from("Alice"))],
        }];

        let new = vec![crate::Row {
            values: vec![SqlValue::Integer(1), SqlValue::Varchar(Arc::from("Bob"))],
        }];

        let test_id = SubscriptionId::new();
        let pk_columns = vec![0]; // First column is PK
        let delta = compute_delta_with_pk(test_id, &old, &new, &pk_columns);
        assert!(delta.is_some());

        match delta.unwrap() {
            SubscriptionUpdate::Delta { inserts, updates, deletes, .. } => {
                // With PK matching, this should be an UPDATE, not insert+delete
                assert!(inserts.is_empty());
                assert_eq!(updates.len(), 1);
                assert!(deletes.is_empty());

                // Verify the update contains old and new row
                let (old_row, new_row) = &updates[0];
                assert_eq!(old_row.values[0], SqlValue::Integer(1));
                assert_eq!(old_row.values[1], SqlValue::Varchar(Arc::from("Alice")));
                assert_eq!(new_row.values[0], SqlValue::Integer(1));
                assert_eq!(new_row.values[1], SqlValue::Varchar(Arc::from("Bob")));
            }
            _ => panic!("Expected Delta update"),
        }
    }

    #[test]
    fn test_compute_delta_with_pk_insert_and_delete() {
        use vibesql_types::SqlValue;

        // Different PKs - should be insert + delete
        let old = vec![crate::Row {
            values: vec![SqlValue::Integer(1), SqlValue::Varchar(Arc::from("Alice"))],
        }];

        let new = vec![crate::Row {
            values: vec![SqlValue::Integer(2), SqlValue::Varchar(Arc::from("Bob"))],
        }];

        let test_id = SubscriptionId::new();
        let pk_columns = vec![0];
        let delta = compute_delta_with_pk(test_id, &old, &new, &pk_columns);
        assert!(delta.is_some());

        match delta.unwrap() {
            SubscriptionUpdate::Delta { inserts, updates, deletes, .. } => {
                assert_eq!(inserts.len(), 1);
                assert!(updates.is_empty());
                assert_eq!(deletes.len(), 1);
                assert_eq!(inserts[0].values[0], SqlValue::Integer(2));
                assert_eq!(deletes[0].values[0], SqlValue::Integer(1));
            }
            _ => panic!("Expected Delta update"),
        }
    }

    #[test]
    fn test_compute_delta_with_pk_no_changes() {
        use vibesql_types::SqlValue;

        let rows = vec![
            crate::Row {
                values: vec![SqlValue::Integer(1), SqlValue::Varchar(Arc::from("Alice"))],
            },
            crate::Row { values: vec![SqlValue::Integer(2), SqlValue::Varchar(Arc::from("Bob"))] },
        ];

        let test_id = SubscriptionId::new();
        let pk_columns = vec![0];
        let delta = compute_delta_with_pk(test_id, &rows, &rows, &pk_columns);
        assert!(delta.is_none());
    }

    #[test]
    fn test_compute_delta_with_pk_multiple_updates() {
        use vibesql_types::SqlValue;

        // Multiple rows with updates
        let old = vec![
            crate::Row {
                values: vec![SqlValue::Integer(1), SqlValue::Varchar(Arc::from("Alice"))],
            },
            crate::Row { values: vec![SqlValue::Integer(2), SqlValue::Varchar(Arc::from("Bob"))] },
        ];

        let new = vec![
            crate::Row {
                values: vec![SqlValue::Integer(1), SqlValue::Varchar(Arc::from("ALICE"))],
            },
            crate::Row { values: vec![SqlValue::Integer(2), SqlValue::Varchar(Arc::from("BOB"))] },
        ];

        let test_id = SubscriptionId::new();
        let pk_columns = vec![0];
        let delta = compute_delta_with_pk(test_id, &old, &new, &pk_columns);
        assert!(delta.is_some());

        match delta.unwrap() {
            SubscriptionUpdate::Delta { inserts, updates, deletes, .. } => {
                assert!(inserts.is_empty());
                assert_eq!(updates.len(), 2);
                assert!(deletes.is_empty());
            }
            _ => panic!("Expected Delta update"),
        }
    }

    #[test]
    fn test_compute_delta_with_pk_composite_pk() {
        use vibesql_types::SqlValue;

        // Composite PK (order_id, user_id)
        let old = vec![crate::Row {
            values: vec![
                SqlValue::Integer(1),
                SqlValue::Integer(100),
                SqlValue::Varchar(Arc::from("pending")),
            ],
        }];

        let new = vec![crate::Row {
            values: vec![
                SqlValue::Integer(1),
                SqlValue::Integer(100),
                SqlValue::Varchar(Arc::from("shipped")),
            ],
        }];

        let test_id = SubscriptionId::new();
        let pk_columns = vec![0, 1]; // Composite PK
        let delta = compute_delta_with_pk(test_id, &old, &new, &pk_columns);
        assert!(delta.is_some());

        match delta.unwrap() {
            SubscriptionUpdate::Delta { inserts, updates, deletes, .. } => {
                assert!(inserts.is_empty());
                assert_eq!(updates.len(), 1);
                assert!(deletes.is_empty());

                let (_, new_row) = &updates[0];
                assert_eq!(new_row.values[2], SqlValue::Varchar(Arc::from("shipped")));
            }
            _ => panic!("Expected Delta update"),
        }
    }

    #[test]
    fn test_compute_delta_with_pk_empty_fallback() {
        use vibesql_types::SqlValue;

        // With empty pk_columns, should fall back to hash-based and detect as insert+delete
        let old = vec![crate::Row {
            values: vec![SqlValue::Integer(1), SqlValue::Varchar(Arc::from("Alice"))],
        }];

        let new = vec![crate::Row {
            values: vec![SqlValue::Integer(1), SqlValue::Varchar(Arc::from("Bob"))],
        }];

        let test_id = SubscriptionId::new();
        let delta = compute_delta_with_pk(test_id, &old, &new, &[]);
        assert!(delta.is_some());

        match delta.unwrap() {
            SubscriptionUpdate::Delta { inserts, updates, deletes, .. } => {
                // Hash-based: different content = insert + delete, no update detection
                assert_eq!(inserts.len(), 1);
                assert!(updates.is_empty());
                assert_eq!(deletes.len(), 1);
            }
            _ => panic!("Expected Delta update"),
        }
    }

    #[test]
    fn test_compute_delta_with_pk_mixed_operations() {
        use vibesql_types::SqlValue;

        // Mix of insert, update, and delete
        let old = vec![
            crate::Row {
                values: vec![SqlValue::Integer(1), SqlValue::Varchar(Arc::from("Alice"))],
            },
            crate::Row { values: vec![SqlValue::Integer(2), SqlValue::Varchar(Arc::from("Bob"))] },
            crate::Row {
                values: vec![SqlValue::Integer(3), SqlValue::Varchar(Arc::from("Charlie"))],
            },
        ];

        let new = vec![
            crate::Row {
                values: vec![SqlValue::Integer(1), SqlValue::Varchar(Arc::from("ALICE"))],
            }, // Update
            // Row 2 deleted
            crate::Row {
                values: vec![SqlValue::Integer(3), SqlValue::Varchar(Arc::from("Charlie"))],
            }, // Unchanged
            crate::Row {
                values: vec![SqlValue::Integer(4), SqlValue::Varchar(Arc::from("Diana"))],
            }, // Insert
        ];

        let test_id = SubscriptionId::new();
        let pk_columns = vec![0];
        let delta = compute_delta_with_pk(test_id, &old, &new, &pk_columns);
        assert!(delta.is_some());

        match delta.unwrap() {
            SubscriptionUpdate::Delta { inserts, updates, deletes, .. } => {
                assert_eq!(inserts.len(), 1);
                assert_eq!(updates.len(), 1);
                assert_eq!(deletes.len(), 1);

                // Verify insert
                assert_eq!(inserts[0].values[0], SqlValue::Integer(4));

                // Verify update
                let (old_row, new_row) = &updates[0];
                assert_eq!(old_row.values[1], SqlValue::Varchar(Arc::from("Alice")));
                assert_eq!(new_row.values[1], SqlValue::Varchar(Arc::from("ALICE")));

                // Verify delete
                assert_eq!(deletes[0].values[0], SqlValue::Integer(2));
            }
            _ => panic!("Expected Delta update"),
        }
    }

    #[test]
    fn test_compute_delta_with_pk_out_of_bounds_fallback() {
        use vibesql_types::SqlValue;

        // PK column index out of bounds - should fall back to hash-based
        let old = vec![crate::Row {
            values: vec![SqlValue::Integer(1), SqlValue::Varchar(Arc::from("Alice"))],
        }];

        let new = vec![crate::Row {
            values: vec![SqlValue::Integer(1), SqlValue::Varchar(Arc::from("Bob"))],
        }];

        let test_id = SubscriptionId::new();
        let pk_columns = vec![5]; // Out of bounds
        let delta = compute_delta_with_pk(test_id, &old, &new, &pk_columns);
        assert!(delta.is_some());

        // Should fall back to hash-based matching
        match delta.unwrap() {
            SubscriptionUpdate::Delta { inserts, updates, deletes, .. } => {
                assert_eq!(inserts.len(), 1);
                assert!(updates.is_empty());
                assert_eq!(deletes.len(), 1);
            }
            _ => panic!("Expected Delta update"),
        }
    }

    // ========================================================================
    // Tests for Selective Column Updates
    // ========================================================================

    #[test]
    fn test_compute_column_diff_no_changes() {
        use vibesql_types::SqlValue;

        let old = crate::Row {
            values: vec![SqlValue::Integer(1), SqlValue::Varchar(Arc::from("Alice"))],
        };
        let new = crate::Row {
            values: vec![SqlValue::Integer(1), SqlValue::Varchar(Arc::from("Alice"))],
        };

        let diff = compute_column_diff(&old, &new, &[0]);
        assert!(diff.is_none());
    }

    #[test]
    fn test_compute_column_diff_single_column_change() {
        use vibesql_types::SqlValue;

        let old = crate::Row {
            values: vec![SqlValue::Integer(1), SqlValue::Varchar(Arc::from("Alice"))],
        };
        let new = crate::Row {
            values: vec![SqlValue::Integer(1), SqlValue::Varchar(Arc::from("Bob"))],
        };

        let diff = compute_column_diff(&old, &new, &[0]).unwrap();
        assert_eq!(diff.changed_columns, vec![1]);
        // Included columns should be PK (0) + changed (1)
        assert_eq!(diff.included_columns, vec![0, 1]);
    }

    #[test]
    fn test_compute_column_diff_multiple_columns_change() {
        use vibesql_types::SqlValue;

        let old = crate::Row {
            values: vec![
                SqlValue::Integer(1),
                SqlValue::Varchar(Arc::from("Alice")),
                SqlValue::Integer(100),
                SqlValue::Varchar(Arc::from("active")),
            ],
        };
        let new = crate::Row {
            values: vec![
                SqlValue::Integer(1),
                SqlValue::Varchar(Arc::from("Bob")),
                SqlValue::Integer(100),
                SqlValue::Varchar(Arc::from("inactive")),
            ],
        };

        let diff = compute_column_diff(&old, &new, &[0]).unwrap();
        assert_eq!(diff.changed_columns, vec![1, 3]);
        // Included columns should be PK (0) + changed (1, 3)
        assert_eq!(diff.included_columns, vec![0, 1, 3]);
    }

    #[test]
    fn test_compute_column_diff_pk_column_changed() {
        use vibesql_types::SqlValue;

        let old = crate::Row {
            values: vec![SqlValue::Integer(1), SqlValue::Varchar(Arc::from("Alice"))],
        };
        let new = crate::Row {
            values: vec![SqlValue::Integer(2), SqlValue::Varchar(Arc::from("Alice"))],
        };

        let diff = compute_column_diff(&old, &new, &[0]).unwrap();
        assert_eq!(diff.changed_columns, vec![0]);
        // PK is already changed, so included = just [0]
        assert_eq!(diff.included_columns, vec![0]);
    }

    #[test]
    fn test_compute_column_diff_null_handling() {
        use vibesql_types::SqlValue;

        let old = crate::Row {
            values: vec![SqlValue::Integer(1), SqlValue::Varchar(Arc::from("Alice"))],
        };
        let new = crate::Row { values: vec![SqlValue::Integer(1), SqlValue::Null] };

        let diff = compute_column_diff(&old, &new, &[0]).unwrap();
        assert_eq!(diff.changed_columns, vec![1]);
        assert_eq!(diff.included_columns, vec![0, 1]);
    }

    #[test]
    fn test_should_use_selective_update_enabled() {
        let diff = ColumnDiff { changed_columns: vec![1], included_columns: vec![0, 1] };

        let config =
            SelectiveColumnConfig { enabled: true, pk_columns: vec![0], ..Default::default() };

        assert!(should_use_selective_update(&diff, 10, &config));
    }

    #[test]
    fn test_should_use_selective_update_disabled() {
        let diff = ColumnDiff { changed_columns: vec![1], included_columns: vec![0, 1] };

        let config =
            SelectiveColumnConfig { enabled: false, pk_columns: vec![0], ..Default::default() };

        assert!(!should_use_selective_update(&diff, 10, &config));
    }

    #[test]
    fn test_should_use_selective_update_too_many_changes() {
        // 6 columns changed out of 10 = 60%, exceeds 50% threshold
        let diff = ColumnDiff {
            changed_columns: vec![1, 2, 3, 4, 5, 6],
            included_columns: vec![0, 1, 2, 3, 4, 5, 6],
        };

        let config = SelectiveColumnConfig {
            enabled: true,
            pk_columns: vec![0],
            max_changed_columns_ratio: 0.5,
            ..Default::default()
        };

        assert!(!should_use_selective_update(&diff, 10, &config));
    }

    #[test]
    fn test_create_partial_row_update() {
        let old_row =
            vec![Some(b"1".to_vec()), Some(b"Alice".to_vec()), Some(b"100".to_vec())];
        let new_row =
            vec![Some(b"1".to_vec()), Some(b"Bob".to_vec()), Some(b"100".to_vec())];

        let config = SelectiveColumnConfig {
            enabled: true,
            pk_columns: vec![0],
            max_changed_columns_ratio: 0.5,
            ..Default::default()
        };

        let partial = create_partial_row_update(&old_row, &new_row, &[0], &config).unwrap();

        assert_eq!(partial.total_columns, 3);
        // Should include columns 0 (PK) and 1 (changed)
        assert!(partial.is_column_present(0));
        assert!(partial.is_column_present(1));
        assert!(!partial.is_column_present(2));
        assert_eq!(partial.present_column_count(), 2);
        // Values should be the new values for included columns
        assert_eq!(partial.values.len(), 2);
        assert_eq!(partial.values[0], Some(b"1".to_vec()));
        assert_eq!(partial.values[1], Some(b"Bob".to_vec()));
    }

    #[test]
    fn test_create_partial_row_update_null_change() {
        let old_row = vec![Some(b"1".to_vec()), Some(b"Alice".to_vec())];
        let new_row = vec![Some(b"1".to_vec()), None];

        let config =
            SelectiveColumnConfig { enabled: true, pk_columns: vec![0], ..Default::default() };

        let partial = create_partial_row_update(&old_row, &new_row, &[0], &config).unwrap();

        assert_eq!(partial.total_columns, 2);
        assert!(partial.is_column_present(0));
        assert!(partial.is_column_present(1));
        assert_eq!(partial.values.len(), 2);
        assert_eq!(partial.values[0], Some(b"1".to_vec()));
        assert_eq!(partial.values[1], None); // NULL value
    }

    #[test]
    fn test_create_partial_row_update_no_changes() {
        let old_row = vec![Some(b"1".to_vec()), Some(b"Alice".to_vec())];
        let new_row = vec![Some(b"1".to_vec()), Some(b"Alice".to_vec())];

        let config =
            SelectiveColumnConfig { enabled: true, pk_columns: vec![0], ..Default::default() };

        let partial = create_partial_row_update(&old_row, &new_row, &[0], &config);
        assert!(partial.is_none());
    }

    #[test]
    fn test_partial_row_update_column_mask() {
        use crate::protocol::messages::PartialRowUpdate;

        // Test with 10 columns, columns 0, 3, 7 present
        let partial = PartialRowUpdate::new(
            10,
            &[0, 3, 7],
            vec![Some(b"a".to_vec()), Some(b"b".to_vec()), Some(b"c".to_vec())],
        );

        assert_eq!(partial.total_columns, 10);
        assert_eq!(partial.column_mask.len(), 2); // ceil(10/8) = 2 bytes

        // Check column presence
        assert!(partial.is_column_present(0));
        assert!(!partial.is_column_present(1));
        assert!(!partial.is_column_present(2));
        assert!(partial.is_column_present(3));
        assert!(!partial.is_column_present(4));
        assert!(!partial.is_column_present(5));
        assert!(!partial.is_column_present(6));
        assert!(partial.is_column_present(7));
        assert!(!partial.is_column_present(8));
        assert!(!partial.is_column_present(9));
        assert!(!partial.is_column_present(10)); // Out of range

        assert_eq!(partial.present_column_count(), 3);
    }

    #[test]
    fn test_delta_updates_produce_partial_row_updates() {
        use vibesql_types::SqlValue;

        // Test that delta computation with updates can produce partial row updates
        // This verifies the integration between compute_delta_with_pk and create_partial_row_update

        let test_id = SubscriptionId::new();

        // Create old and new rows where only one column changes
        // Row format: [id, name, balance]
        // id=1: name unchanged, balance changes from 100 to 150
        let old = vec![crate::Row {
            values: vec![
                SqlValue::Integer(1),
                SqlValue::Varchar(Arc::from("Alice")),
                SqlValue::Integer(100),
            ],
        }];
        let new = vec![crate::Row {
            values: vec![
                SqlValue::Integer(1),
                SqlValue::Varchar(Arc::from("Alice")),
                SqlValue::Integer(150),
            ],
        }];

        let pk_columns = vec![0]; // First column is PK
        let delta = compute_delta_with_pk(test_id, &old, &new, &pk_columns);

        // Verify we got an update (not delete+insert)
        if let SubscriptionUpdate::Delta { updates, inserts, deletes, .. } = delta.unwrap() {
            assert!(inserts.is_empty(), "Should not have inserts");
            assert!(deletes.is_empty(), "Should not have deletes");
            assert_eq!(updates.len(), 1, "Should have one update");

            // Now verify that create_partial_row_update works with this update
            let (old_row, new_row) = &updates[0];

            // Convert to wire format (as connection.rs does)
            let old_wire: Vec<Option<Vec<u8>>> =
                old_row.values.iter().map(|v| Some(v.to_string().as_bytes().to_vec())).collect();
            let new_wire: Vec<Option<Vec<u8>>> =
                new_row.values.iter().map(|v| Some(v.to_string().as_bytes().to_vec())).collect();

            let config =
                SelectiveColumnConfig { enabled: true, pk_columns: vec![0], ..Default::default() };

            let partial = create_partial_row_update(&old_wire, &new_wire, &[0], &config);

            // Should produce a partial update since only 1 of 3 columns changed
            assert!(partial.is_some(), "Should produce partial row update");

            let partial = partial.unwrap();
            assert_eq!(partial.total_columns, 3);

            // Should include PK (column 0) and changed column (column 2)
            assert!(partial.is_column_present(0), "PK column should be present");
            assert!(!partial.is_column_present(1), "Unchanged column should not be present");
            assert!(partial.is_column_present(2), "Changed column should be present");

            // Verify values
            assert_eq!(partial.values.len(), 2); // PK + changed column
            assert_eq!(partial.values[0], Some(b"1".to_vec())); // PK value
            assert_eq!(partial.values[1], Some(b"150".to_vec())); // New balance
        } else {
            panic!("Expected Delta, got something else");
        }
    }

    #[test]
    fn test_delta_updates_fallback_to_full_row_when_too_many_changes() {
        use vibesql_types::SqlValue;

        // Test that when too many columns change, create_partial_row_update returns None

        let test_id = SubscriptionId::new();

        // Create old and new rows where most non-PK columns change
        // Row format: [id, name, email]
        let old = vec![crate::Row {
            values: vec![
                SqlValue::Integer(1),
                SqlValue::Varchar(Arc::from("Alice")),
                SqlValue::Varchar(Arc::from("alice@old.com")),
            ],
        }];
        let new = vec![crate::Row {
            values: vec![
                SqlValue::Integer(1),
                SqlValue::Varchar(Arc::from("Bob")),
                SqlValue::Varchar(Arc::from("bob@new.com")),
            ],
        }];

        let pk_columns = vec![0];
        let delta = compute_delta_with_pk(test_id, &old, &new, &pk_columns);

        if let SubscriptionUpdate::Delta { updates, .. } = delta.unwrap() {
            assert_eq!(updates.len(), 1);

            let (old_row, new_row) = &updates[0];

            let old_wire: Vec<Option<Vec<u8>>> =
                old_row.values.iter().map(|v| Some(v.to_string().as_bytes().to_vec())).collect();
            let new_wire: Vec<Option<Vec<u8>>> =
                new_row.values.iter().map(|v| Some(v.to_string().as_bytes().to_vec())).collect();

            // Use config with low threshold (max 30% of columns can change)
            let config = SelectiveColumnConfig {
                enabled: true,
                pk_columns: vec![0],
                max_changed_columns_ratio: 0.3,
                ..Default::default()
            };

            // 2 of 3 columns changed (66%), which exceeds 30% threshold
            let partial = create_partial_row_update(&old_wire, &new_wire, &[0], &config);

            // Should NOT produce partial update due to too many changes
            assert!(partial.is_none(), "Should fall back to full row when too many columns change");
        } else {
            panic!("Expected Delta");
        }
    }

    // ========================================================================
    // Additional Tests for Selective Column Updates (Issue #3924)
    // ========================================================================

    #[test]
    fn test_should_use_selective_update_below_min_changed_columns() {
        // Only 1 column changed, but min_changed_columns is 2
        let diff = ColumnDiff { changed_columns: vec![1], included_columns: vec![0, 1] };

        let config = SelectiveColumnConfig {
            enabled: true,
            pk_columns: vec![0],
            min_changed_columns: 2, // Require at least 2 columns to change
            max_changed_columns_ratio: 0.5,
        };

        // Should return false because only 1 column changed
        assert!(!should_use_selective_update(&diff, 10, &config));
    }

    #[test]
    fn test_should_use_selective_update_at_min_changed_columns() {
        // Exactly 2 columns changed, min_changed_columns is 2
        let diff = ColumnDiff { changed_columns: vec![1, 2], included_columns: vec![0, 1, 2] };

        let config = SelectiveColumnConfig {
            enabled: true,
            pk_columns: vec![0],
            min_changed_columns: 2, // Require at least 2 columns to change
            max_changed_columns_ratio: 0.5,
        };

        // Should return true because exactly min_changed_columns changed (2 of 10 = 20%)
        assert!(should_use_selective_update(&diff, 10, &config));
    }

    #[test]
    fn test_should_use_selective_update_at_max_ratio() {
        // 5 of 10 columns changed = 50%, exactly at max_changed_columns_ratio
        let diff = ColumnDiff {
            changed_columns: vec![1, 2, 3, 4, 5],
            included_columns: vec![0, 1, 2, 3, 4, 5],
        };

        let config = SelectiveColumnConfig {
            enabled: true,
            pk_columns: vec![0],
            min_changed_columns: 1,
            max_changed_columns_ratio: 0.5, // Allow up to 50%
        };

        // Should return true because exactly at threshold (not over)
        assert!(should_use_selective_update(&diff, 10, &config));
    }

    #[test]
    fn test_create_partial_row_update_all_columns_changed() {
        // All 3 columns change - should fall back (ratio = 100% > 50%)
        let old_row = vec![Some(b"1".to_vec()), Some(b"Alice".to_vec()), Some(b"100".to_vec())];
        let new_row = vec![Some(b"2".to_vec()), Some(b"Bob".to_vec()), Some(b"200".to_vec())];

        let config = SelectiveColumnConfig {
            enabled: true,
            pk_columns: vec![0],
            min_changed_columns: 1,
            max_changed_columns_ratio: 0.5,
        };

        // Should return None because all columns changed (100% > 50%)
        let partial = create_partial_row_update(&old_row, &new_row, &[0], &config);
        assert!(partial.is_none());
    }

    #[test]
    fn test_create_partial_row_update_empty_pk_columns() {
        // Empty PK columns - should still work, just won't include extra columns
        let old_row = vec![Some(b"1".to_vec()), Some(b"Alice".to_vec()), Some(b"100".to_vec())];
        let new_row = vec![Some(b"1".to_vec()), Some(b"Bob".to_vec()), Some(b"100".to_vec())];

        let config = SelectiveColumnConfig {
            enabled: true,
            pk_columns: vec![],
            min_changed_columns: 1,
            max_changed_columns_ratio: 0.5,
        };

        // Should work - only column 1 changed (33% < 50%)
        let partial = create_partial_row_update(&old_row, &new_row, &[], &config).unwrap();

        assert_eq!(partial.total_columns, 3);
        // Only column 1 is present (no PK to force-include)
        assert!(!partial.is_column_present(0));
        assert!(partial.is_column_present(1));
        assert!(!partial.is_column_present(2));
        assert_eq!(partial.present_column_count(), 1);
        assert_eq!(partial.values.len(), 1);
        assert_eq!(partial.values[0], Some(b"Bob".to_vec()));
    }

    #[test]
    fn test_create_partial_row_update_disabled_returns_none() {
        let old_row = vec![Some(b"1".to_vec()), Some(b"Alice".to_vec())];
        let new_row = vec![Some(b"1".to_vec()), Some(b"Bob".to_vec())];

        let config = SelectiveColumnConfig {
            enabled: false, // Disabled
            pk_columns: vec![0],
            min_changed_columns: 1,
            max_changed_columns_ratio: 0.5,
        };

        // Should return None because selective updates are disabled
        let partial = create_partial_row_update(&old_row, &new_row, &[0], &config);
        assert!(partial.is_none());
    }

    #[test]
    fn test_create_partial_row_update_different_row_lengths() {
        let old_row = vec![Some(b"1".to_vec()), Some(b"Alice".to_vec())];
        let new_row = vec![Some(b"1".to_vec()), Some(b"Bob".to_vec()), Some(b"extra".to_vec())];

        let config =
            SelectiveColumnConfig { enabled: true, pk_columns: vec![0], ..Default::default() };

        // Should return None because row lengths differ
        let partial = create_partial_row_update(&old_row, &new_row, &[0], &config);
        assert!(partial.is_none());
    }

    #[test]
    fn test_compute_column_diff_different_row_lengths() {
        use vibesql_types::SqlValue;

        let old = crate::Row { values: vec![SqlValue::Integer(1), SqlValue::Varchar(Arc::from("Alice"))] };
        let new = crate::Row {
            values: vec![
                SqlValue::Integer(1),
                SqlValue::Varchar(Arc::from("Alice")),
                SqlValue::Integer(100),
            ],
        };

        // Should return None because row lengths differ
        let diff = compute_column_diff(&old, &new, &[0]);
        assert!(diff.is_none());
    }

    #[test]
    fn test_compute_column_diff_composite_pk() {
        use vibesql_types::SqlValue;

        let old = crate::Row {
            values: vec![
                SqlValue::Integer(1),   // PK col 0
                SqlValue::Integer(100), // PK col 1
                SqlValue::Varchar(Arc::from("Alice")),
                SqlValue::Integer(50),
            ],
        };
        let new = crate::Row {
            values: vec![
                SqlValue::Integer(1),   // PK col 0 unchanged
                SqlValue::Integer(100), // PK col 1 unchanged
                SqlValue::Varchar(Arc::from("Bob")), // Changed
                SqlValue::Integer(50), // Unchanged
            ],
        };

        // Composite PK: columns 0 and 1
        let diff = compute_column_diff(&old, &new, &[0, 1]).unwrap();
        assert_eq!(diff.changed_columns, vec![2]); // Only column 2 changed
        // Included columns should be PK (0, 1) + changed (2)
        assert_eq!(diff.included_columns, vec![0, 1, 2]);
    }

    #[test]
    fn test_create_partial_row_update_composite_pk() {
        let old_row = vec![
            Some(b"1".to_vec()),     // PK col 0
            Some(b"100".to_vec()),   // PK col 1
            Some(b"Alice".to_vec()), // Data
            Some(b"50".to_vec()),    // Data
        ];
        let new_row = vec![
            Some(b"1".to_vec()),   // PK col 0 unchanged
            Some(b"100".to_vec()), // PK col 1 unchanged
            Some(b"Bob".to_vec()), // Changed
            Some(b"50".to_vec()),  // Unchanged
        ];

        let config = SelectiveColumnConfig {
            enabled: true,
            pk_columns: vec![0, 1],
            min_changed_columns: 1,
            max_changed_columns_ratio: 0.5,
        };

        let partial = create_partial_row_update(&old_row, &new_row, &[0, 1], &config).unwrap();

        assert_eq!(partial.total_columns, 4);
        // Columns 0, 1 (PK) and 2 (changed) should be present
        assert!(partial.is_column_present(0));
        assert!(partial.is_column_present(1));
        assert!(partial.is_column_present(2));
        assert!(!partial.is_column_present(3));
        assert_eq!(partial.present_column_count(), 3);
    }

    #[test]
    fn test_partial_row_update_large_column_count() {
        use crate::protocol::messages::PartialRowUpdate;

        // Test with 20 columns (requires 3 bytes for column mask)
        let partial = PartialRowUpdate::new(
            20,
            &[0, 7, 8, 15, 16], // Spread across multiple bytes
            vec![
                Some(b"a".to_vec()),
                Some(b"b".to_vec()),
                Some(b"c".to_vec()),
                Some(b"d".to_vec()),
                Some(b"e".to_vec()),
            ],
        );

        assert_eq!(partial.total_columns, 20);
        assert_eq!(partial.column_mask.len(), 3); // ceil(20/8) = 3 bytes

        // Check column presence across bytes
        assert!(partial.is_column_present(0));  // Byte 0, bit 0
        assert!(partial.is_column_present(7));  // Byte 0, bit 7
        assert!(partial.is_column_present(8));  // Byte 1, bit 0
        assert!(partial.is_column_present(15)); // Byte 1, bit 7
        assert!(partial.is_column_present(16)); // Byte 2, bit 0
        assert!(!partial.is_column_present(19)); // Not present
        assert_eq!(partial.present_column_count(), 5);
    }

    #[test]
    fn test_compute_column_diff_null_to_value() {
        use vibesql_types::SqlValue;

        // Test NULL -> value transition
        let old = crate::Row { values: vec![SqlValue::Integer(1), SqlValue::Null] };
        let new = crate::Row {
            values: vec![SqlValue::Integer(1), SqlValue::Varchar(Arc::from("Alice"))],
        };

        let diff = compute_column_diff(&old, &new, &[0]).unwrap();
        assert_eq!(diff.changed_columns, vec![1]);
        assert_eq!(diff.included_columns, vec![0, 1]);
    }

    #[test]
    fn test_compute_column_diff_value_to_null() {
        use vibesql_types::SqlValue;

        // Test value -> NULL transition
        let old = crate::Row {
            values: vec![SqlValue::Integer(1), SqlValue::Varchar(Arc::from("Alice"))],
        };
        let new = crate::Row { values: vec![SqlValue::Integer(1), SqlValue::Null] };

        let diff = compute_column_diff(&old, &new, &[0]).unwrap();
        assert_eq!(diff.changed_columns, vec![1]);
        assert_eq!(diff.included_columns, vec![0, 1]);
    }

    #[test]
    fn test_create_partial_row_update_null_to_value() {
        let old_row = vec![Some(b"1".to_vec()), None]; // NULL in column 1
        let new_row = vec![Some(b"1".to_vec()), Some(b"Alice".to_vec())];

        let config =
            SelectiveColumnConfig { enabled: true, pk_columns: vec![0], ..Default::default() };

        let partial = create_partial_row_update(&old_row, &new_row, &[0], &config).unwrap();

        assert_eq!(partial.total_columns, 2);
        assert!(partial.is_column_present(0));
        assert!(partial.is_column_present(1));
        assert_eq!(partial.values.len(), 2);
        assert_eq!(partial.values[0], Some(b"1".to_vec()));
        assert_eq!(partial.values[1], Some(b"Alice".to_vec())); // Changed from NULL
    }

    // ========================================================================
    // Tests for PartialRowDelta
    // ========================================================================

    #[test]
    fn test_partial_row_delta_from_rows_single_column_change() {
        use vibesql_types::SqlValue;

        let old_row = crate::Row {
            values: vec![
                SqlValue::Integer(1),
                SqlValue::Varchar("Alice".to_string()),
                SqlValue::Integer(100),
            ],
        };
        let new_row = crate::Row {
            values: vec![
                SqlValue::Integer(1),
                SqlValue::Varchar("Alice".to_string()),
                SqlValue::Integer(150),
            ],
        };

        let pk_columns = vec![0];
        let delta = PartialRowDelta::from_rows(&old_row, &new_row, &pk_columns);

        assert!(delta.is_some());
        let delta = delta.unwrap();

        // Should include PK (0) + changed column (2)
        assert_eq!(delta.column_indices, vec![0, 2]);
        assert_eq!(delta.old_values, vec![SqlValue::Integer(1), SqlValue::Integer(100)]);
        assert_eq!(delta.new_values, vec![SqlValue::Integer(1), SqlValue::Integer(150)]);
    }

    #[test]
    fn test_partial_row_delta_from_rows_multiple_column_changes() {
        use vibesql_types::SqlValue;

        let old_row = crate::Row {
            values: vec![
                SqlValue::Integer(1),
                SqlValue::Varchar("Alice".to_string()),
                SqlValue::Integer(100),
                SqlValue::Varchar("active".to_string()),
            ],
        };
        let new_row = crate::Row {
            values: vec![
                SqlValue::Integer(1),
                SqlValue::Varchar("Bob".to_string()),
                SqlValue::Integer(100),
                SqlValue::Varchar("inactive".to_string()),
            ],
        };

        let pk_columns = vec![0];
        let delta = PartialRowDelta::from_rows(&old_row, &new_row, &pk_columns);

        assert!(delta.is_some());
        let delta = delta.unwrap();

        // Should include PK (0) + changed columns (1, 3)
        assert_eq!(delta.column_indices, vec![0, 1, 3]);
        assert_eq!(
            delta.old_values,
            vec![
                SqlValue::Integer(1),
                SqlValue::Varchar("Alice".to_string()),
                SqlValue::Varchar("active".to_string())
            ]
        );
        assert_eq!(
            delta.new_values,
            vec![
                SqlValue::Integer(1),
                SqlValue::Varchar("Bob".to_string()),
                SqlValue::Varchar("inactive".to_string())
            ]
        );
    }

    #[test]
    fn test_partial_row_delta_from_rows_no_changes() {
        use vibesql_types::SqlValue;

        let row = crate::Row {
            values: vec![SqlValue::Integer(1), SqlValue::Varchar("Alice".to_string())],
        };

        let pk_columns = vec![0];
        let delta = PartialRowDelta::from_rows(&row, &row, &pk_columns);

        assert!(delta.is_none(), "Should return None when rows are identical");
    }

    #[test]
    fn test_partial_row_delta_from_rows_pk_column_changed() {
        use vibesql_types::SqlValue;

        let old_row = crate::Row {
            values: vec![SqlValue::Integer(1), SqlValue::Varchar("Alice".to_string())],
        };
        let new_row = crate::Row {
            values: vec![SqlValue::Integer(2), SqlValue::Varchar("Alice".to_string())],
        };

        let pk_columns = vec![0];
        let delta = PartialRowDelta::from_rows(&old_row, &new_row, &pk_columns);

        assert!(delta.is_some());
        let delta = delta.unwrap();

        // PK column changed, should only include column 0
        assert_eq!(delta.column_indices, vec![0]);
        assert_eq!(delta.old_values, vec![SqlValue::Integer(1)]);
        assert_eq!(delta.new_values, vec![SqlValue::Integer(2)]);
    }

    #[test]
    fn test_partial_row_delta_from_rows_null_handling() {
        use vibesql_types::SqlValue;

        let old_row = crate::Row {
            values: vec![SqlValue::Integer(1), SqlValue::Varchar("Alice".to_string())],
        };
        let new_row = crate::Row { values: vec![SqlValue::Integer(1), SqlValue::Null] };

        let pk_columns = vec![0];
        let delta = PartialRowDelta::from_rows(&old_row, &new_row, &pk_columns);

        assert!(delta.is_some());
        let delta = delta.unwrap();

        // Should include PK (0) + changed column (1)
        assert_eq!(delta.column_indices, vec![0, 1]);
        assert_eq!(delta.new_values, vec![SqlValue::Integer(1), SqlValue::Null]);
    }

    #[test]
    fn test_partial_row_delta_from_rows_composite_pk() {
        use vibesql_types::SqlValue;

        let old_row = crate::Row {
            values: vec![
                SqlValue::Integer(1),
                SqlValue::Integer(100),
                SqlValue::Varchar("old".to_string()),
            ],
        };
        let new_row = crate::Row {
            values: vec![
                SqlValue::Integer(1),
                SqlValue::Integer(100),
                SqlValue::Varchar("new".to_string()),
            ],
        };

        let pk_columns = vec![0, 1]; // Composite PK
        let delta = PartialRowDelta::from_rows(&old_row, &new_row, &pk_columns);

        assert!(delta.is_some());
        let delta = delta.unwrap();

        // Should include PK columns (0, 1) + changed column (2)
        assert_eq!(delta.column_indices, vec![0, 1, 2]);
        assert_eq!(
            delta.old_values,
            vec![
                SqlValue::Integer(1),
                SqlValue::Integer(100),
                SqlValue::Varchar("old".to_string())
            ]
        );
        assert_eq!(
            delta.new_values,
            vec![
                SqlValue::Integer(1),
                SqlValue::Integer(100),
                SqlValue::Varchar("new".to_string())
            ]
        );
    }

    #[test]
    fn test_partial_row_delta_from_rows_different_column_count() {
        use vibesql_types::SqlValue;

        let old_row = crate::Row {
            values: vec![SqlValue::Integer(1), SqlValue::Varchar("Alice".to_string())],
        };
        let new_row = crate::Row {
            values: vec![
                SqlValue::Integer(1),
                SqlValue::Varchar("Alice".to_string()),
                SqlValue::Integer(100),
            ],
        };

        let pk_columns = vec![0];
        let delta = PartialRowDelta::from_rows(&old_row, &new_row, &pk_columns);

        assert!(delta.is_none(), "Should return None when column counts differ");
    }

    #[test]
    fn test_subscription_update_partial_subscription_id() {
        let test_id = SubscriptionId::new();
        let update = SubscriptionUpdate::Partial { subscription_id: test_id, updates: vec![] };

        assert_eq!(update.subscription_id(), test_id);
    }
}
