//! Subscription lifecycle management: subscribe and unsubscribe operations.

use std::collections::HashSet;
use std::sync::atomic::Ordering;

use tokio::sync::mpsc;
use tracing::debug;

use super::SubscriptionManager;
use crate::subscription::{
    extract_table_refs, Subscription, SubscriptionError, SubscriptionId, SubscriptionUpdate,
};

impl SubscriptionManager {
    /// Create a new subscription for a query
    ///
    /// Parses the query to extract table dependencies and registers the
    /// subscription for notifications.
    ///
    /// # Arguments
    ///
    /// * `query` - SQL query to monitor
    /// * `notify_tx` - Channel to send updates to the subscriber
    ///
    /// # Returns
    ///
    /// The subscription ID on success, or an error if parsing fails or limits exceeded
    ///
    /// # Errors
    ///
    /// - `ParseError` if the query cannot be parsed or references no tables
    /// - `GlobalLimitExceeded` if the global subscription limit is reached
    ///
    /// # Example
    ///
    /// ```text
    /// let manager = SubscriptionManager::new();
    /// let (tx, mut rx) = mpsc::channel(16);
    ///
    /// let id = manager.subscribe("SELECT * FROM users".to_string(), tx)?;
    /// println!("Subscribed with ID: {}", id);
    /// ```
    pub fn subscribe(
        &self,
        query: String,
        notify_tx: mpsc::Sender<SubscriptionUpdate>,
    ) -> Result<SubscriptionId, SubscriptionError> {
        // Atomically reserve a slot to prevent TOCTOU race condition
        // Use compare-and-swap loop to safely increment the counter
        loop {
            let current_count = self.subscription_count_atomic.load(Ordering::Acquire);
            if current_count >= self.config.max_global {
                self.limit_exceeded_count.fetch_add(1, Ordering::Relaxed);
                return Err(SubscriptionError::GlobalLimitExceeded {
                    current: current_count,
                    max: self.config.max_global,
                });
            }

            // Try to atomically increment the count
            match self.subscription_count_atomic.compare_exchange(
                current_count,
                current_count + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,     // Successfully reserved a slot
                Err(_) => continue, // Another thread changed the count, retry
            }
        }

        // Parse query and extract table dependencies
        let tables = match self.extract_tables(&query) {
            Ok(tables) => tables,
            Err(e) => {
                // Release the reserved slot on parse error
                self.subscription_count_atomic.fetch_sub(1, Ordering::Release);
                return Err(e);
            }
        };

        if tables.is_empty() {
            // Release the reserved slot
            self.subscription_count_atomic.fetch_sub(1, Ordering::Release);
            return Err(SubscriptionError::ParseError(
                "Query must reference at least one table".to_string(),
            ));
        }

        // Create subscription with default channel buffer size
        let subscription = Subscription::new(query.clone(), tables.clone(), notify_tx);
        let id = subscription.id;

        debug!(
            subscription_id = %id,
            tables = ?tables,
            "Creating new subscription"
        );

        // Register subscription
        self.subscriptions.insert(id, subscription);

        // Index by tables
        for table in tables {
            self.table_index.entry(table).or_default().insert(id);
        }

        Ok(id)
    }

    /// Create a new subscription for a specific connection (wire protocol)
    ///
    /// This is the primary method for wire protocol subscriptions. It:
    /// - Checks both global and per-connection limits
    /// - Associates the subscription with a connection ID for cleanup
    /// - Stores the wire protocol UUID for lookup
    ///
    /// # Arguments
    ///
    /// * `query` - SQL query to monitor
    /// * `notify_tx` - Channel to send updates to the subscriber
    /// * `connection_id` - The connection/session ID that owns this subscription
    /// * `wire_subscription_id` - The wire protocol UUID for this subscription
    /// * `table_dependencies` - Pre-extracted table dependencies (from AST parsing)
    ///
    /// # Returns
    ///
    /// The subscription ID on success, or an error if limits exceeded
    ///
    /// # Errors
    ///
    /// - `GlobalLimitExceeded` if the global subscription limit is reached
    /// - `ConnectionLimitExceeded` if the per-connection limit is reached
    pub fn subscribe_for_connection(
        &self,
        query: String,
        notify_tx: mpsc::Sender<SubscriptionUpdate>,
        connection_id: String,
        wire_subscription_id: [u8; 16],
        table_dependencies: HashSet<String>,
        filter: Option<String>,
    ) -> Result<SubscriptionId, SubscriptionError> {
        // Check per-connection limit first
        let conn_count = self
            .connection_subscription_counts
            .entry(connection_id.clone())
            .or_insert_with(|| std::sync::atomic::AtomicUsize::new(0));

        // Use CAS loop for per-connection limit check
        loop {
            let current_conn_count = conn_count.load(Ordering::Acquire);
            if current_conn_count >= self.config.max_per_connection {
                return Err(SubscriptionError::ConnectionLimitExceeded {
                    current: current_conn_count,
                    max: self.config.max_per_connection,
                });
            }

            // Try to atomically increment the per-connection count
            match conn_count.compare_exchange(
                current_conn_count,
                current_conn_count + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(_) => continue,
            }
        }

        // Atomically reserve a global slot to prevent TOCTOU race condition
        loop {
            let current_count = self.subscription_count_atomic.load(Ordering::Acquire);
            if current_count >= self.config.max_global {
                // Release the per-connection slot we reserved
                conn_count.fetch_sub(1, Ordering::Release);
                self.limit_exceeded_count.fetch_add(1, Ordering::Relaxed);
                return Err(SubscriptionError::GlobalLimitExceeded {
                    current: current_count,
                    max: self.config.max_global,
                });
            }

            // Try to atomically increment the count
            match self.subscription_count_atomic.compare_exchange(
                current_count,
                current_count + 1,
                Ordering::AcqRel,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(_) => continue,
            }
        }

        // Create subscription with connection tracking
        let subscription = Subscription::for_connection(
            query.clone(),
            table_dependencies.clone(),
            notify_tx,
            connection_id.clone(),
            wire_subscription_id,
            filter,
            &self.config,
        );
        let id = subscription.id;

        debug!(
            subscription_id = %id,
            connection_id = %connection_id,
            tables = ?table_dependencies,
            "Creating new subscription for connection"
        );

        // Register subscription
        self.subscriptions.insert(id, subscription);

        // Index by tables (lowercase for case-insensitive matching)
        for table in table_dependencies {
            self.table_index.entry(table.to_lowercase()).or_default().insert(id);
        }

        // Index by connection
        self.connection_index.entry(connection_id).or_default().insert(id);

        // Index by wire ID
        self.wire_id_index.insert(wire_subscription_id, id);

        Ok(id)
    }

    /// Remove a subscription
    ///
    /// Unregisters the subscription and removes it from all indexes.
    ///
    /// # Arguments
    ///
    /// * `id` - The subscription ID to remove
    ///
    /// # Returns
    ///
    /// `true` if the removed subscription was selective-eligible, `false` otherwise
    pub fn unsubscribe(&self, id: SubscriptionId) -> bool {
        if let Some((_, subscription)) = self.subscriptions.remove(&id) {
            debug!(subscription_id = %id, "Removing subscription");

            let was_selective_eligible = subscription.selective_eligible;

            // Decrement the atomic counter
            self.subscription_count_atomic.fetch_sub(1, Ordering::Release);

            // Remove from table index
            for table in &subscription.tables {
                if let Some(mut ids) = self.table_index.get_mut(table) {
                    ids.remove(&id);
                }
            }

            // Remove from connection index if present
            if let Some(ref conn_id) = subscription.connection_id {
                if let Some(mut ids) = self.connection_index.get_mut(conn_id) {
                    ids.remove(&id);
                }
                // Decrement per-connection count
                if let Some(count) = self.connection_subscription_counts.get(conn_id) {
                    count.fetch_sub(1, Ordering::Release);
                }
            }

            // Remove from wire ID index if present
            if let Some(wire_id) = subscription.wire_subscription_id {
                self.wire_id_index.remove(&wire_id);
            }

            return was_selective_eligible;
        }
        false
    }

    /// Remove a subscription by its wire protocol ID
    ///
    /// This is used by wire protocol clients that use UUID-based subscription IDs.
    ///
    /// # Arguments
    ///
    /// * `wire_id` - The wire protocol subscription ID (UUID bytes)
    ///
    /// # Returns
    ///
    /// `true` if the removed subscription was selective-eligible, `false` otherwise.
    /// Returns `false` if the subscription was not found.
    pub fn unsubscribe_by_wire_id(&self, wire_id: &[u8; 16]) -> bool {
        if let Some((_, id)) = self.wire_id_index.remove(wire_id) {
            self.unsubscribe(id)
        } else {
            false
        }
    }

    /// Remove all subscriptions for a connection
    ///
    /// This should be called when a connection closes to clean up all its
    /// subscriptions. This is important for wire protocol connections.
    ///
    /// # Arguments
    ///
    /// * `connection_id` - The connection ID to clean up
    ///
    /// # Returns
    ///
    /// A tuple of (total_removed, selective_eligible_removed)
    pub fn unsubscribe_all_for_connection(&self, connection_id: &str) -> (usize, usize) {
        let subscription_ids: Vec<SubscriptionId> = if let Some((_, ids)) =
            self.connection_index.remove(connection_id)
        {
            ids.into_iter().collect()
        } else {
            return (0, 0);
        };

        let count = subscription_ids.len();
        debug!(
            connection_id = %connection_id,
            subscription_count = count,
            "Removing all subscriptions for connection"
        );

        let mut selective_eligible_count = 0;
        for id in subscription_ids {
            // Note: unsubscribe will try to remove from connection_index again,
            // but it will be a no-op since we already removed it
            if self.unsubscribe(id) {
                selective_eligible_count += 1;
            }
        }

        // Clean up the per-connection count entry
        self.connection_subscription_counts.remove(connection_id);

        (count, selective_eligible_count)
    }

    /// Extract table references from a query
    pub(crate) fn extract_tables(&self, query: &str) -> Result<HashSet<String>, SubscriptionError> {
        let stmt = vibesql_parser::Parser::parse_sql(query)
            .map_err(|e| SubscriptionError::ParseError(e.to_string()))?;
        Ok(extract_table_refs(&stmt))
    }
}
