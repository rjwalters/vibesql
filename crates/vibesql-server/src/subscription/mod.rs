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
pub mod session;
mod table_dependencies;
mod table_extract;

use std::collections::HashSet;
use std::hash::{Hash, Hasher};

use tokio::sync::mpsc;

pub use manager::SubscriptionManager;
pub use session::{SessionSubscription, SessionSubscriptionId, SessionSubscriptionManager};
pub use table_dependencies::extract_table_dependencies;
pub use table_extract::extract_table_refs;

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
    /// Channel to send updates to the subscriber
    pub notify_tx: mpsc::Sender<SubscriptionUpdate>,
}

impl Subscription {
    /// Create a new subscription
    pub fn new(
        query: String,
        tables: HashSet<String>,
        notify_tx: mpsc::Sender<SubscriptionUpdate>,
    ) -> Self {
        Self {
            id: SubscriptionId::new(),
            query,
            tables,
            last_result_hash: 0,
            notify_tx,
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

    /// Incremental delta (future optimization)
    ///
    /// Contains only the changes since the last update. More efficient
    /// for large result sets with small changes.
    #[allow(dead_code)]
    Delta {
        /// Newly inserted rows
        inserts: Vec<crate::Row>,
        /// Updated rows (old value, new value)
        updates: Vec<(crate::Row, crate::Row)>,
        /// Deleted rows
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

/// Event from the storage layer indicating data has changed
///
/// These events are produced by the storage layer (Phase 1) and consumed
/// by the SubscriptionManager to determine which subscriptions need updates.
#[derive(Debug, Clone)]
pub enum ChangeEvent {
    /// A row was inserted into a table
    Insert {
        /// Name of the affected table
        table_name: String,
        /// ID of the inserted row (for future delta tracking)
        row_id: u64,
    },

    /// A row was updated in a table
    Update {
        /// Name of the affected table
        table_name: String,
        /// ID of the updated row
        row_id: u64,
    },

    /// A row was deleted from a table
    Delete {
        /// Name of the affected table
        table_name: String,
        /// ID of the deleted row
        row_id: u64,
    },

    /// A table was truncated (all rows deleted)
    Truncate {
        /// Name of the truncated table
        table_name: String,
    },

    /// A table was dropped
    DropTable {
        /// Name of the dropped table
        table_name: String,
    },
}

impl ChangeEvent {
    /// Get the name of the affected table
    pub fn table_name(&self) -> &str {
        match self {
            ChangeEvent::Insert { table_name, .. }
            | ChangeEvent::Update { table_name, .. }
            | ChangeEvent::Delete { table_name, .. }
            | ChangeEvent::Truncate { table_name }
            | ChangeEvent::DropTable { table_name } => table_name,
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
    fn test_change_event_table_name() {
        let insert = ChangeEvent::Insert {
            table_name: "users".to_string(),
            row_id: 1,
        };
        assert_eq!(insert.table_name(), "users");

        let update = ChangeEvent::Update {
            table_name: "orders".to_string(),
            row_id: 2,
        };
        assert_eq!(update.table_name(), "orders");

        let delete = ChangeEvent::Delete {
            table_name: "products".to_string(),
            row_id: 3,
        };
        assert_eq!(delete.table_name(), "products");

        let truncate = ChangeEvent::Truncate {
            table_name: "logs".to_string(),
        };
        assert_eq!(truncate.table_name(), "logs");

        let drop = ChangeEvent::DropTable {
            table_name: "temp".to_string(),
        };
        assert_eq!(drop.table_name(), "temp");
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
}
