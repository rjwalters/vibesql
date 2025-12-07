//! Session-level subscription types
//!
//! This module provides types used for per-connection subscription tracking:
//! - `SessionSubscriptionId` - Unique identifier for a subscription (UUID bytes)
//! - `SessionSubscription` - Information about an active subscription
//! - `TablePkInfo` - Primary key column indices for selective updates
//!
//! For subscription management, use `SubscriptionManager` with connection tracking:
//! - `subscribe_for_connection()` - Subscribe for a specific connection
//! - `unsubscribe_by_wire_id()` - Unsubscribe by wire protocol ID
//! - `unsubscribe_all_for_connection()` - Clean up all subscriptions for a connection
//! - `connection_subscription_count()` - Get subscription count for a connection

use std::collections::{HashMap, HashSet};

use crate::Row;

/// Unique identifier for a subscription (UUID bytes)
pub type SessionSubscriptionId = [u8; 16];

/// Primary key column indices for a table.
///
/// Used for selective column updates: when sending subscription updates,
/// PK columns must always be included so clients can identify which rows changed.
#[derive(Debug, Clone, Default)]
pub struct TablePkInfo {
    /// Maps table name (lowercase) to its primary key column indices.
    /// `None` means the table has no primary key defined.
    /// `Some(vec![])` means PK info couldn't be determined (fallback to first column).
    pub pk_indices: HashMap<String, Option<Vec<usize>>>,
}

impl TablePkInfo {
    /// Create an empty TablePkInfo
    pub fn new() -> Self {
        Self { pk_indices: HashMap::new() }
    }

    /// Add PK info for a table
    pub fn add_table(&mut self, table_name: &str, pk_indices: Option<Vec<usize>>) {
        self.pk_indices.insert(table_name.to_lowercase(), pk_indices);
    }

    /// Get PK column indices for a table.
    /// Returns None if the table has no PK or wasn't found.
    /// Falls back to `vec![0]` (first column) if PK info is unavailable.
    pub fn get_pk_columns(&self, table_name: &str) -> Vec<usize> {
        match self.pk_indices.get(&table_name.to_lowercase()) {
            Some(Some(indices)) if !indices.is_empty() => indices.clone(),
            _ => vec![0], // Fallback: assume first column is PK
        }
    }

    /// Check if we have explicit PK info for a table (not using fallback)
    pub fn has_explicit_pk(&self, table_name: &str) -> bool {
        matches!(
            self.pk_indices.get(&table_name.to_lowercase()),
            Some(Some(indices)) if !indices.is_empty()
        )
    }
}

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
    /// Whether updates are currently paused for this subscription
    pub paused: bool,
    /// Optional filter expression (SQL WHERE clause) for filtering updates.
    /// If present, only rows matching the filter are sent to the client.
    pub filter: Option<String>,
    /// Primary key column indices for each table in the subscription.
    /// Used for selective column updates to identify rows.
    pub pk_info: TablePkInfo,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pk_info_fallback_to_first_column() {
        let pk_info = TablePkInfo::new();

        // Table not in PK info should fallback to first column
        assert_eq!(pk_info.get_pk_columns("unknown_table"), vec![0]);
        assert!(!pk_info.has_explicit_pk("unknown_table"));
    }

    #[test]
    fn test_pk_info_no_pk_defined() {
        let mut pk_info = TablePkInfo::new();
        pk_info.add_table("no_pk_table", None); // Table has no PK

        // Should still fallback to first column
        assert_eq!(pk_info.get_pk_columns("no_pk_table"), vec![0]);
        assert!(!pk_info.has_explicit_pk("no_pk_table"));
    }

    #[test]
    fn test_pk_info_case_insensitive() {
        let mut pk_info = TablePkInfo::new();
        pk_info.add_table("Users", Some(vec![0]));

        // Should work regardless of case
        assert_eq!(pk_info.get_pk_columns("users"), vec![0]);
        assert_eq!(pk_info.get_pk_columns("USERS"), vec![0]);
        assert_eq!(pk_info.get_pk_columns("Users"), vec![0]);
        assert!(pk_info.has_explicit_pk("users"));
        assert!(pk_info.has_explicit_pk("USERS"));
    }

    #[test]
    fn test_pk_info_explicit_indices() {
        let mut pk_info = TablePkInfo::new();
        pk_info.add_table("users", Some(vec![0]));
        pk_info.add_table("order_items", Some(vec![0, 1])); // Composite key

        assert!(pk_info.has_explicit_pk("users"));
        assert_eq!(pk_info.get_pk_columns("users"), vec![0]);

        assert!(pk_info.has_explicit_pk("order_items"));
        assert_eq!(pk_info.get_pk_columns("order_items"), vec![0, 1]);
    }

    #[test]
    fn test_pk_info_non_first_column() {
        let mut pk_info = TablePkInfo::new();
        // PK is at index 2, not first column
        pk_info.add_table("products", Some(vec![2]));

        assert!(pk_info.has_explicit_pk("products"));
        assert_eq!(pk_info.get_pk_columns("products"), vec![2]);
    }
}
