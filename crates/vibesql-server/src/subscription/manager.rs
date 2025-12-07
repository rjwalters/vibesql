//! Subscription manager for tracking and notifying query subscriptions
//!
//! The SubscriptionManager is the central component of the subscription system.
//! It maintains the registry of active subscriptions, indexes them by table
//! dependencies, and handles change event notifications.

use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use dashmap::DashMap;
use tokio::sync::mpsc;
use tracing::{debug, trace, warn};
use vibesql_storage::change_events::RecvError;
use vibesql_storage::Database;

use super::{
    classify_error_str, compute_delta_with_pk, extract_table_refs, hash_rows, Subscription,
    SubscriptionConfig, SubscriptionError, SubscriptionErrorKind, SubscriptionId,
    SubscriptionUpdate,
};

// ============================================================================
// Subscription Manager
// ============================================================================

/// Manager for query subscriptions
///
/// Tracks all active subscriptions, indexes them by table dependencies,
/// and handles notifications when data changes.
///
/// # Thread Safety
///
/// The manager uses `DashMap` for lock-free concurrent access to subscriptions.
/// Multiple threads can subscribe, unsubscribe, and process change events
/// concurrently without explicit locking.
///
/// # Performance
///
/// The manager uses a table-based index to quickly find subscriptions affected
/// by a change event. This allows O(1) lookup of subscriptions by table name,
/// rather than scanning all subscriptions.
///
/// # Connection Tracking
///
/// The manager supports tracking subscriptions by connection/session ID.
/// This enables efficient cleanup when a connection closes, and allows
/// both HTTP SSE and wire protocol clients to use the same subscription
/// infrastructure.
pub struct SubscriptionManager {
    /// All active subscriptions, indexed by ID
    subscriptions: DashMap<SubscriptionId, Subscription>,

    /// Index: table_name -> subscription IDs that depend on it
    /// This enables fast lookup of affected subscriptions when a table changes
    table_index: DashMap<String, HashSet<SubscriptionId>>,

    /// Index: connection_id -> subscription IDs belonging to that connection
    /// Used for connection-level subscription tracking and cleanup
    connection_index: DashMap<String, HashSet<SubscriptionId>>,

    /// Index: wire_subscription_id -> internal SubscriptionId
    /// Used to bridge wire protocol UUIDs to internal u64 IDs
    wire_id_index: DashMap<[u8; 16], SubscriptionId>,

    /// Configuration for limits and quotas
    config: SubscriptionConfig,

    /// Counter for global limit exceeded events (for metrics)
    limit_exceeded_count: AtomicUsize,

    /// Counter for result set too large events (for metrics)
    result_set_exceeded_count: AtomicUsize,

    /// Atomic counter for current subscription count (for lock-free limit checking)
    subscription_count_atomic: AtomicUsize,

    /// Per-connection subscription counts (for per-connection limit enforcement)
    connection_subscription_counts: DashMap<String, AtomicUsize>,
}

impl SubscriptionManager {
    /// Create a new subscription manager with default configuration
    pub fn new() -> Self {
        Self::with_config(SubscriptionConfig::default())
    }

    /// Create a new subscription manager with custom configuration
    pub fn with_config(config: SubscriptionConfig) -> Self {
        Self {
            subscriptions: DashMap::new(),
            table_index: DashMap::new(),
            connection_index: DashMap::new(),
            wire_id_index: DashMap::new(),
            config,
            limit_exceeded_count: AtomicUsize::new(0),
            result_set_exceeded_count: AtomicUsize::new(0),
            subscription_count_atomic: AtomicUsize::new(0),
            connection_subscription_counts: DashMap::new(),
        }
    }

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
    /// ```ignore
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
            .or_insert_with(|| AtomicUsize::new(0));

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

    /// Find a subscription by its wire protocol ID
    ///
    /// # Arguments
    ///
    /// * `wire_id` - The wire protocol subscription ID (UUID bytes)
    ///
    /// # Returns
    ///
    /// The internal subscription ID if found
    pub fn find_subscription_by_wire_id(&self, wire_id: &[u8; 16]) -> Option<SubscriptionId> {
        self.wire_id_index.get(wire_id).map(|r| *r)
    }

    /// Get the subscription count for a specific connection
    ///
    /// # Arguments
    ///
    /// * `connection_id` - The connection ID to check
    ///
    /// # Returns
    ///
    /// Number of subscriptions for this connection
    pub fn connection_subscription_count(&self, connection_id: &str) -> usize {
        self.connection_subscription_counts
            .get(connection_id)
            .map(|c| c.load(Ordering::Acquire))
            .unwrap_or(0)
    }

    /// Get affected subscription details for wire protocol
    ///
    /// This method finds all subscriptions that depend on the given table and returns
    /// their details needed for wire protocol notifications.
    ///
    /// # Arguments
    ///
    /// * `table` - The table name to find affected subscriptions for
    ///
    /// # Returns
    ///
    /// Vector of (wire_subscription_id, query, last_result_hash, last_result) for
    /// each affected subscription that has a wire ID.
    pub fn get_affected_subscriptions_for_wire_protocol(
        &self,
        table: &str,
    ) -> Vec<([u8; 16], String, u64, Option<Vec<crate::Row>>)> {
        let table_lower = table.to_lowercase();
        let subscription_ids: Vec<SubscriptionId> = self
            .table_index
            .get(&table_lower)
            .map(|ids| ids.iter().copied().collect())
            .unwrap_or_default();

        subscription_ids
            .into_iter()
            .filter_map(|id| {
                self.subscriptions.get(&id).and_then(|sub| {
                    sub.wire_subscription_id.map(|wire_id| {
                        (wire_id, sub.query.clone(), sub.last_result_hash, sub.last_result.clone())
                    })
                })
            })
            .collect()
    }

    /// Get affected subscription details for wire protocol filtered by connection
    ///
    /// This method finds subscriptions for a specific connection that depend on the
    /// given table and returns their details needed for wire protocol notifications.
    ///
    /// # Arguments
    ///
    /// * `table` - The table name to find affected subscriptions for
    /// * `connection_id` - The connection ID to filter by
    ///
    /// # Returns
    ///
    /// Vector of (wire_subscription_id, query, last_result_hash, last_result, filter) for
    /// each affected subscription that belongs to the specified connection.
    pub fn get_affected_subscriptions_for_connection(
        &self,
        table: &str,
        connection_id: &str,
    ) -> Vec<([u8; 16], String, u64, Option<Vec<crate::Row>>, Option<String>)> {
        let table_lower = table.to_lowercase();
        let subscription_ids: Vec<SubscriptionId> = self
            .table_index
            .get(&table_lower)
            .map(|ids| ids.iter().copied().collect())
            .unwrap_or_default();

        subscription_ids
            .into_iter()
            .filter_map(|id| {
                self.subscriptions.get(&id).and_then(|sub| {
                    // Only include subscriptions that belong to this connection
                    if sub.connection_id.as_deref() == Some(connection_id) {
                        sub.wire_subscription_id.map(|wire_id| {
                            (
                                wire_id,
                                sub.query.clone(),
                                sub.last_result_hash,
                                sub.last_result.clone(),
                                sub.filter.clone(),
                            )
                        })
                    } else {
                        None
                    }
                })
            })
            .collect()
    }

    /// Update the stored result for a subscription by wire ID
    ///
    /// This is used by wire protocol to store results for delta computation.
    ///
    /// # Arguments
    ///
    /// * `wire_id` - The wire protocol subscription ID (UUID bytes)
    /// * `result_hash` - Hash of the new result set
    /// * `result` - The new result set
    pub fn update_result_by_wire_id(
        &self,
        wire_id: &[u8; 16],
        result_hash: u64,
        result: Vec<crate::Row>,
    ) {
        if let Some(id) = self.wire_id_index.get(wire_id).map(|r| *r) {
            if let Some(mut sub) = self.subscriptions.get_mut(&id) {
                sub.last_result_hash = result_hash;
                sub.last_result = Some(result);
            }
        }
    }

    /// Update the primary key columns for a subscription by wire ID
    ///
    /// This is called after PK detection to set the actual PK column indices
    /// for selective column updates.
    pub fn update_pk_columns_by_wire_id(&self, wire_id: &[u8; 16], pk_columns: Vec<usize>) {
        if let Some(id) = self.wire_id_index.get(wire_id).map(|r| *r) {
            if let Some(mut sub) = self.subscriptions.get_mut(&id) {
                sub.pk_columns = pk_columns;
            }
        }
    }

    /// Update PK columns with eligibility tracking
    ///
    /// Sets the PK columns and marks whether the subscription is eligible
    /// for selective column updates (based on confident PK detection).
    ///
    /// Returns `true` if the subscription is newly marked as selective-eligible.
    pub fn update_pk_columns_with_eligibility_by_wire_id(
        &self,
        wire_id: &[u8; 16],
        pk_columns: Vec<usize>,
        confident: bool,
    ) -> bool {
        if let Some(id) = self.wire_id_index.get(wire_id).map(|r| *r) {
            if let Some(mut sub) = self.subscriptions.get_mut(&id) {
                return sub.set_pk_columns_with_eligibility(pk_columns, confident);
            }
        }
        false
    }

    /// Get the primary key columns for a subscription by wire ID
    ///
    /// Returns the PK column indices, or default [0] if not found.
    pub fn get_pk_columns_by_wire_id(&self, wire_id: &[u8; 16]) -> Vec<usize> {
        if let Some(id) = self.wire_id_index.get(wire_id).map(|r| *r) {
            if let Some(sub) = self.subscriptions.get(&id) {
                return sub.pk_columns.clone();
            }
        }
        vec![0] // default
    }

    /// Get the number of active subscriptions
    pub fn subscription_count(&self) -> usize {
        self.subscriptions.len()
    }

    /// Get the tables being watched and their subscription counts
    pub fn watched_tables(&self) -> Vec<(String, usize)> {
        self.table_index.iter().map(|entry| (entry.key().clone(), entry.value().len())).collect()
    }

    /// Find all subscriptions affected by a change to a given table
    ///
    /// This is the core lookup operation for fanout during change handling.
    /// Uses the table index for O(1) lookup of the subscription ID set.
    ///
    /// # Arguments
    ///
    /// * `table_name` - The table that changed
    ///
    /// # Returns
    ///
    /// Vector of subscription IDs that depend on this table
    pub fn find_affected_subscriptions(&self, table_name: &str) -> Vec<SubscriptionId> {
        let table = table_name.to_lowercase();
        self.table_index.get(&table).map(|ids| ids.iter().copied().collect()).unwrap_or_default()
    }

    /// Handle a change event from the storage layer
    ///
    /// Finds all subscriptions affected by the change and checks if their
    /// results have changed. Sends notifications for changed results.
    ///
    /// # Arguments
    ///
    /// * `event` - The change event to process (from storage layer)
    /// * `db` - Database to re-execute queries against
    pub async fn handle_change(&self, event: vibesql_storage::ChangeEvent, db: &Database) {
        let table = event.table_name();

        trace!(
            table = %table,
            event = ?event,
            "Processing change event from storage"
        );

        // Find subscriptions affected by this table
        let affected_ids = self.find_affected_subscriptions(table);

        if affected_ids.is_empty() {
            trace!(table = %table, "No subscriptions affected");
            return;
        }

        debug!(
            table = %table,
            affected_count = affected_ids.len(),
            "Found affected subscriptions"
        );

        // Check each affected subscription
        for id in affected_ids {
            self.check_and_notify(id, db).await;
        }
    }

    /// Check a subscription and notify if results changed
    ///
    /// This method re-executes the subscription query, computes the delta
    /// from the previous result, and sends either a Delta or Full update
    /// to the subscriber.
    async fn check_and_notify(&self, id: SubscriptionId, db: &Database) {
        // Get mutable reference to subscription
        let mut sub_ref = match self.subscriptions.get_mut(&id) {
            Some(sub) => sub,
            None => {
                trace!(subscription_id = %id, "Subscription not found (may have been removed)");
                return;
            }
        };

        let subscription = sub_ref.value_mut();

        // Try to execute with retry logic
        self.execute_with_retry(subscription, db, id).await;
    }

    /// Execute query with retry logic for transient errors
    async fn execute_with_retry(
        &self,
        subscription: &mut Subscription,
        db: &Database,
        id: SubscriptionId,
    ) {
        loop {
            // Parse and execute the query
            let result = self.execute_subscription_query(subscription, db, id).await;

            match result {
                Ok(rows) => {
                    // Successful execution - reset retry count
                    subscription.retry_count = 0;

                    // Convert to Row format
                    let result_rows: Vec<crate::Row> =
                        rows.iter().map(|r| crate::Row { values: r.values.clone() }).collect();

                    // Hash results for comparison
                    let new_hash = hash_rows(&result_rows);

                    if new_hash != subscription.last_result_hash {
                        debug!(
                            subscription_id = %id,
                            old_hash = subscription.last_result_hash,
                            new_hash = new_hash,
                            row_count = result_rows.len(),
                            "Results changed, notifying subscriber"
                        );

                        // Determine whether to send Delta or Full update
                        let update = if let Some(ref old_rows) = subscription.last_result {
                            // We have previous results - compute delta using PK columns
                            if let Some(delta) = compute_delta_with_pk(
                                id,
                                old_rows,
                                &result_rows,
                                &subscription.pk_columns,
                            ) {
                                // Log delta statistics
                                if let SubscriptionUpdate::Delta {
                                    ref inserts,
                                    ref updates,
                                    ref deletes,
                                    ..
                                } = delta
                                {
                                    debug!(
                                        subscription_id = %id,
                                        inserts = inserts.len(),
                                        updates = updates.len(),
                                        deletes = deletes.len(),
                                        "Sending delta update"
                                    );
                                }
                                delta
                            } else {
                                // No delta (shouldn't happen if hash changed, but be safe)
                                SubscriptionUpdate::Full { subscription_id: id, rows: result_rows.clone() }
                            }
                        } else {
                            // No previous results - send full (first update after initial)
                            debug!(
                                subscription_id = %id,
                                "No previous result, sending full update"
                            );
                            SubscriptionUpdate::Full { subscription_id: id, rows: result_rows.clone() }
                        };

                        // Update stored state
                        subscription.last_result_hash = new_hash;
                        subscription.last_result = Some(result_rows);

                        // Check for slow consumer before sending
                        let capacity = subscription.notify_tx.capacity();
                        let max_capacity = subscription.notify_tx.max_capacity();
                        let used = max_capacity.saturating_sub(capacity);
                        let usage_percent =
                            if max_capacity > 0 { (used * 100) / max_capacity } else { 0 };

                        if usage_percent >= subscription.slow_consumer_threshold_percent as usize {
                            warn!(
                                subscription_id = %id,
                                used = used,
                                max_capacity = max_capacity,
                                usage_percent = usage_percent,
                                threshold = subscription.slow_consumer_threshold_percent,
                                "Slow consumer detected: subscription channel is {}% full. \
                                 Consider increasing channel_buffer_size or client is consuming too slowly.",
                                usage_percent
                            );
                        }

                        // Use try_send for non-blocking send with backpressure detection
                        match subscription.notify_tx.try_send(update) {
                            Ok(()) => {
                                subscription.updates_sent += 1;
                                trace!(
                                    subscription_id = %id,
                                    updates_sent = subscription.updates_sent,
                                    "Update sent successfully"
                                );
                            }
                            Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                                subscription.updates_dropped += 1;
                                warn!(
                                    subscription_id = %id,
                                    updates_dropped = subscription.updates_dropped,
                                    channel_buffer_size = subscription.channel_buffer_size,
                                    "Subscription channel full, dropping update. \
                                     Consider increasing channel_buffer_size in SubscriptionConfig \
                                     or ensure client is consuming updates faster."
                                );
                            }
                            Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => {
                                trace!(
                                    subscription_id = %id,
                                    "Notification channel closed, subscription will be cleaned up"
                                );
                            }
                        }
                    } else {
                        trace!(
                            subscription_id = %id,
                            "Results unchanged, no notification needed"
                        );
                    }
                    return;
                }
                Err(error_msg) => {
                    // Classify the error to determine retry strategy
                    let error_kind = classify_error_str(&error_msg);

                    match error_kind {
                        SubscriptionErrorKind::Permanent => {
                            // Permanent error - don't retry, notify subscriber and stop
                            debug!(
                                subscription_id = %id,
                                error = %error_msg,
                                "Permanent error, not retrying"
                            );
                            let _ = subscription
                                .notify_tx
                                .send(SubscriptionUpdate::Error {
                                    subscription_id: id,
                                    message: format!(
                                        "Query execution failed: {} (error will not be retried)",
                                        error_msg
                                    ),
                                })
                                .await;
                            return;
                        }
                        SubscriptionErrorKind::Transient | SubscriptionErrorKind::Unknown => {
                            // Transient error - may retry
                            subscription.retry_count += 1;

                            if subscription.retry_count > subscription.retry_policy.max_retries {
                                // Exceeded max retries - circuit breaker
                                warn!(
                                    subscription_id = %id,
                                    retry_count = subscription.retry_count,
                                    max_retries = subscription.retry_policy.max_retries,
                                    error = %error_msg,
                                    "Subscription failed after max retries"
                                );
                                let _ = subscription
                                    .notify_tx
                                    .send(SubscriptionUpdate::Error {
                                        subscription_id: id,
                                        message: format!(
                                            "Subscription failed after {} retries: {}",
                                            subscription.retry_policy.max_retries, error_msg
                                        ),
                                    })
                                    .await;
                                return;
                            }

                            // Calculate backoff and retry
                            let backoff = subscription
                                .retry_policy
                                .calculate_backoff(subscription.retry_count - 1);

                            warn!(
                                subscription_id = %id,
                                retry_attempt = subscription.retry_count,
                                backoff_ms = backoff.as_millis(),
                                error_kind = %error_kind,
                                error = %error_msg,
                                "Retrying subscription query after transient error"
                            );

                            tokio::time::sleep(backoff).await;
                            // Loop continues to retry
                        }
                    }
                }
            }
        }
    }

    /// Execute the subscription query and return rows or error message
    async fn execute_subscription_query(
        &self,
        subscription: &Subscription,
        db: &Database,
        id: SubscriptionId,
    ) -> Result<Vec<vibesql_storage::Row>, String> {
        // Re-execute the query
        let executor = vibesql_executor::SelectExecutor::new(db);

        // Parse and execute the query
        match vibesql_parser::Parser::parse_sql(&subscription.query) {
            Ok(vibesql_ast::Statement::Select(select)) => {
                executor.execute(&select).map_err(|e| e.to_string())
            }
            Ok(_) => {
                // Not a SELECT - shouldn't happen for subscriptions
                warn!(
                    subscription_id = %id,
                    "Subscription query is not a SELECT"
                );
                Err("Subscription query is not a SELECT".to_string())
            }
            Err(e) => Err(format!("Failed to parse query: {}", e)),
        }
    }

    /// Run the subscription manager event loop
    ///
    /// Listens for change events from the storage layer and processes them.
    /// This method runs indefinitely until the change channel is closed.
    ///
    /// # Arguments
    ///
    /// * `db` - Database reference for re-executing subscription queries
    ///
    /// # Note
    ///
    /// This method should be spawned as a tokio task at server startup using `tokio::spawn`.
    /// It will poll the change receiver and handle events until closed.
    pub async fn run_event_loop(
        &self,
        mut change_rx: vibesql_storage::ChangeEventReceiver,
        db: Arc<Database>,
    ) {
        loop {
            match change_rx.try_recv() {
                Ok(event) => {
                    self.handle_change(event, &db).await;
                }
                Err(RecvError::Lagged(n)) => {
                    warn!(lagged_count = n, "SubscriptionManager lagged behind change events");
                }
                Err(RecvError::Closed) => {
                    debug!("Change event channel closed, stopping subscription manager");
                    break;
                }
                Err(RecvError::Empty) => {
                    // No events available, yield to other tasks
                    tokio::task::yield_now().await;
                }
            }
        }
    }

    /// Extract table references from a query
    fn extract_tables(&self, query: &str) -> Result<HashSet<String>, SubscriptionError> {
        let stmt = vibesql_parser::Parser::parse_sql(query)
            .map_err(|e| SubscriptionError::ParseError(e.to_string()))?;
        Ok(extract_table_refs(&stmt))
    }

    /// Send initial results to a new subscriber
    ///
    /// Executes the query and sends the initial results. This should be called
    /// right after subscribing to provide immediate data. The initial results
    /// are always sent as a Full update.
    ///
    /// # Errors
    ///
    /// - `NotFound` if the subscription doesn't exist
    /// - `ParseError` if the query fails to execute
    /// - `ResultSetTooLarge` if the result set exceeds the configured limit
    /// - `ChannelClosed` if the notification channel is closed
    pub async fn send_initial_results(
        &self,
        id: SubscriptionId,
        db: &Database,
    ) -> Result<(), SubscriptionError> {
        let mut sub_ref = self.subscriptions.get_mut(&id).ok_or(SubscriptionError::NotFound(id))?;

        let subscription = sub_ref.value_mut();

        // Execute the query
        let executor = vibesql_executor::SelectExecutor::new(db);
        let stmt = vibesql_parser::Parser::parse_sql(&subscription.query)
            .map_err(|e| SubscriptionError::ParseError(e.to_string()))?;

        let rows = match stmt {
            vibesql_ast::Statement::Select(select) => executor
                .execute(&select)
                .map_err(|e| SubscriptionError::ParseError(e.to_string()))?,
            _ => return Err(SubscriptionError::ParseError("Not a SELECT query".to_string())),
        };

        // Check result set size limit
        if rows.len() > self.config.max_result_rows {
            self.result_set_exceeded_count.fetch_add(1, Ordering::Relaxed);
            return Err(SubscriptionError::ResultSetTooLarge {
                rows: rows.len(),
                max: self.config.max_result_rows,
            });
        }

        // Convert to Row format
        let result_rows: Vec<crate::Row> =
            rows.iter().map(|r| crate::Row { values: r.values.clone() }).collect();

        // Update hash and store result for delta computation
        subscription.last_result_hash = hash_rows(&result_rows);
        subscription.last_result = Some(result_rows.clone());

        // Send initial results (always Full for initial)
        subscription
            .notify_tx
            .send(SubscriptionUpdate::Full { subscription_id: id, rows: result_rows })
            .await
            .map_err(|_| SubscriptionError::ChannelClosed)?;

        Ok(())
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
    pub fn get_subscription_metrics(
        &self,
        id: SubscriptionId,
    ) -> Option<super::SubscriptionMetrics> {
        self.subscriptions.get(&id).map(|sub| super::SubscriptionMetrics {
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
    pub fn get_all_metrics(&self) -> Vec<super::SubscriptionMetrics> {
        self.subscriptions
            .iter()
            .map(|entry| {
                let sub = entry.value();
                super::SubscriptionMetrics {
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

impl Default for SubscriptionManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::subscription::SubscriptionConfig;
    use vibesql_types::SqlValue;

    fn setup_test_db() -> Database {
        let mut db = Database::new();

        // Create test tables
        let create_users = vibesql_parser::Parser::parse_sql(
            "CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(100), active BOOLEAN)",
        )
        .unwrap();
        if let vibesql_ast::Statement::CreateTable(stmt) = create_users {
            vibesql_executor::CreateTableExecutor::execute(&stmt, &mut db).unwrap();
        }

        let create_orders = vibesql_parser::Parser::parse_sql(
            "CREATE TABLE orders (id INT PRIMARY KEY, user_id INT, amount INT)",
        )
        .unwrap();
        if let vibesql_ast::Statement::CreateTable(stmt) = create_orders {
            vibesql_executor::CreateTableExecutor::execute(&stmt, &mut db).unwrap();
        }

        db
    }

    #[test]
    fn test_subscribe_simple() {
        let manager = SubscriptionManager::new();
        let (tx, _rx) = mpsc::channel(16);

        let result = manager.subscribe("SELECT * FROM users".to_string(), tx);
        assert!(result.is_ok());

        let _id = result.unwrap();
        assert_eq!(manager.subscription_count(), 1);

        // Check table index
        let watched = manager.watched_tables();
        assert_eq!(watched.len(), 1);
        assert!(watched.iter().any(|(t, c)| t == "users" && *c == 1));
    }

    #[test]
    fn test_subscribe_with_join() {
        let manager = SubscriptionManager::new();
        let (tx, _rx) = mpsc::channel(16);

        let result = manager
            .subscribe("SELECT * FROM users u JOIN orders o ON u.id = o.user_id".to_string(), tx);
        assert!(result.is_ok());

        // Should be indexed under both tables
        let watched = manager.watched_tables();
        assert_eq!(watched.len(), 2);
        assert!(watched.iter().any(|(t, _)| t == "users"));
        assert!(watched.iter().any(|(t, _)| t == "orders"));
    }

    #[test]
    fn test_unsubscribe() {
        let manager = SubscriptionManager::new();
        let (tx, _rx) = mpsc::channel(16);

        let id = manager.subscribe("SELECT * FROM users".to_string(), tx).unwrap();
        assert_eq!(manager.subscription_count(), 1);

        manager.unsubscribe(id);
        assert_eq!(manager.subscription_count(), 0);

        // Table index should be empty
        let watched = manager.watched_tables();
        assert!(watched.iter().all(|(_, c)| *c == 0));
    }

    #[test]
    fn test_invalid_query() {
        let manager = SubscriptionManager::new();
        let (tx, _rx) = mpsc::channel(16);

        let result = manager.subscribe("SELECT * FROM".to_string(), tx);
        assert!(result.is_err());
        assert!(matches!(result, Err(SubscriptionError::ParseError(_))));
    }

    #[test]
    fn test_query_without_tables() {
        let manager = SubscriptionManager::new();
        let (tx, _rx) = mpsc::channel(16);

        // SELECT without FROM should fail
        let result = manager.subscribe("SELECT 1 + 1".to_string(), tx);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_handle_change_notifies_subscribers() {
        let manager = SubscriptionManager::new();
        let (tx, mut rx) = mpsc::channel(16);
        let db = setup_test_db();

        // Subscribe to users table
        let _id = manager.subscribe("SELECT * FROM users".to_string(), tx).unwrap();

        // Simulate a change to users table
        manager
            .handle_change(
                vibesql_storage::ChangeEvent::Insert {
                    table_name: "users".to_string(),
                    row_index: 0,
                },
                &db,
            )
            .await;

        // Should receive a notification (empty result since table is empty)
        let update = rx.try_recv();
        assert!(update.is_ok());

        match update.unwrap() {
            SubscriptionUpdate::Full { rows, .. } => {
                // Table is empty, so no rows
                assert!(rows.is_empty());
            }
            _ => panic!("Expected Full update"),
        }
    }

    #[tokio::test]
    async fn test_handle_change_ignores_unrelated_tables() {
        let manager = SubscriptionManager::new();
        let (tx, mut rx) = mpsc::channel(16);
        let db = setup_test_db();

        // Subscribe to users table
        let _id = manager.subscribe("SELECT * FROM users".to_string(), tx).unwrap();

        // Simulate a change to orders table (not subscribed)
        manager
            .handle_change(
                vibesql_storage::ChangeEvent::Insert {
                    table_name: "orders".to_string(),
                    row_index: 0,
                },
                &db,
            )
            .await;

        // Should NOT receive a notification
        let update = rx.try_recv();
        assert!(update.is_err()); // Channel should be empty
    }

    #[tokio::test]
    async fn test_send_initial_results() {
        let manager = SubscriptionManager::new();
        let (tx, mut rx) = mpsc::channel(16);
        let mut db = setup_test_db();

        // Insert some data
        let insert =
            vibesql_parser::Parser::parse_sql("INSERT INTO users VALUES (1, 'Alice', TRUE)")
                .unwrap();
        if let vibesql_ast::Statement::Insert(stmt) = insert {
            vibesql_executor::InsertExecutor::execute(&mut db, &stmt).unwrap();
        }

        // Subscribe
        let id = manager.subscribe("SELECT * FROM users".to_string(), tx).unwrap();

        // Send initial results
        manager.send_initial_results(id, &db).await.unwrap();

        // Should receive initial data
        let update = rx.recv().await.unwrap();
        match update {
            SubscriptionUpdate::Full { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].values[0], SqlValue::Integer(1));
            }
            _ => panic!("Expected Full update"),
        }
    }

    #[tokio::test]
    async fn test_results_changed_detection() {
        let manager = SubscriptionManager::new();
        let (tx, mut rx) = mpsc::channel(16);
        let mut db = setup_test_db();

        // Subscribe before any data
        let id = manager.subscribe("SELECT * FROM users".to_string(), tx).unwrap();

        // Send initial (empty) results
        manager.send_initial_results(id, &db).await.unwrap();
        let _ = rx.recv().await; // Consume initial

        // Insert data
        let insert =
            vibesql_parser::Parser::parse_sql("INSERT INTO users VALUES (1, 'Alice', TRUE)")
                .unwrap();
        if let vibesql_ast::Statement::Insert(stmt) = insert {
            vibesql_executor::InsertExecutor::execute(&mut db, &stmt).unwrap();
        }

        // Trigger change notification
        manager
            .handle_change(
                vibesql_storage::ChangeEvent::Insert {
                    table_name: "users".to_string(),
                    row_index: 0,
                },
                &db,
            )
            .await;

        // Should receive update with new data (as Delta since we have previous results)
        let update = rx.recv().await.unwrap();
        match update {
            SubscriptionUpdate::Delta { inserts, updates, deletes, .. } => {
                // The inserted row should appear as an insert
                assert_eq!(inserts.len(), 1);
                assert!(updates.is_empty());
                assert!(deletes.is_empty());
            }
            SubscriptionUpdate::Full { rows, .. } => {
                // Also acceptable if Full is sent
                assert_eq!(rows.len(), 1);
            }
            _ => panic!("Expected Delta or Full update"),
        }
    }

    #[tokio::test]
    async fn test_no_notification_when_unchanged() {
        let manager = SubscriptionManager::new();
        let (tx, mut rx) = mpsc::channel(16);
        let db = setup_test_db();

        // Subscribe (empty table)
        let id = manager.subscribe("SELECT * FROM users".to_string(), tx).unwrap();

        // Send initial results
        manager.send_initial_results(id, &db).await.unwrap();
        let _ = rx.recv().await; // Consume initial

        // Trigger change (but data didn't actually change since we didn't insert)
        manager
            .handle_change(
                vibesql_storage::ChangeEvent::Insert {
                    table_name: "users".to_string(),
                    row_index: 0,
                },
                &db,
            )
            .await;

        // Should NOT receive notification (results haven't changed)
        let update = rx.try_recv();
        assert!(update.is_err()); // Channel should be empty
    }

    #[test]
    fn test_multiple_subscriptions_same_table() {
        let manager = SubscriptionManager::new();
        let (tx1, _rx1) = mpsc::channel(16);
        let (tx2, _rx2) = mpsc::channel(16);

        let _id1 = manager.subscribe("SELECT * FROM users".to_string(), tx1).unwrap();
        let _id2 =
            manager.subscribe("SELECT * FROM users WHERE active = TRUE".to_string(), tx2).unwrap();

        assert_eq!(manager.subscription_count(), 2);

        // Both should be indexed under users
        let watched = manager.watched_tables();
        let users_entry = watched.iter().find(|(t, _)| t == "users").unwrap();
        assert_eq!(users_entry.1, 2);
    }

    #[tokio::test]
    async fn test_delta_update_on_insert() {
        let manager = SubscriptionManager::new();
        let (tx, mut rx) = mpsc::channel(16);
        let mut db = setup_test_db();

        // Insert initial data
        let insert =
            vibesql_parser::Parser::parse_sql("INSERT INTO users VALUES (1, 'Alice', TRUE)")
                .unwrap();
        if let vibesql_ast::Statement::Insert(stmt) = insert {
            vibesql_executor::InsertExecutor::execute(&mut db, &stmt).unwrap();
        }

        // Subscribe and get initial results
        let id = manager.subscribe("SELECT * FROM users".to_string(), tx).unwrap();
        manager.send_initial_results(id, &db).await.unwrap();

        // Consume initial Full update
        let initial = rx.recv().await.unwrap();
        match initial {
            SubscriptionUpdate::Full { rows, .. } => {
                assert_eq!(rows.len(), 1);
            }
            _ => panic!("Expected Full update for initial results"),
        }

        // Insert another row
        let insert2 =
            vibesql_parser::Parser::parse_sql("INSERT INTO users VALUES (2, 'Bob', TRUE)").unwrap();
        if let vibesql_ast::Statement::Insert(stmt) = insert2 {
            vibesql_executor::InsertExecutor::execute(&mut db, &stmt).unwrap();
        }

        // Trigger change notification
        manager
            .handle_change(
                vibesql_storage::ChangeEvent::Insert {
                    table_name: "users".to_string(),
                    row_index: 1,
                },
                &db,
            )
            .await;

        // Should receive a Delta update (not Full)
        let update = rx.recv().await.unwrap();
        match update {
            SubscriptionUpdate::Delta { inserts, updates, deletes, .. } => {
                assert_eq!(inserts.len(), 1);
                assert_eq!(inserts[0].values[0], SqlValue::Integer(2));
                assert!(updates.is_empty());
                assert!(deletes.is_empty());
            }
            SubscriptionUpdate::Full { .. } => {
                panic!("Expected Delta update, got Full");
            }
            _ => panic!("Unexpected update type"),
        }
    }

    #[tokio::test]
    async fn test_delta_update_on_delete() {
        let manager = SubscriptionManager::new();
        let (tx, mut rx) = mpsc::channel(16);
        let mut db = setup_test_db();

        // Insert initial data
        let insert1 =
            vibesql_parser::Parser::parse_sql("INSERT INTO users VALUES (1, 'Alice', TRUE)")
                .unwrap();
        if let vibesql_ast::Statement::Insert(stmt) = insert1 {
            vibesql_executor::InsertExecutor::execute(&mut db, &stmt).unwrap();
        }
        let insert2 =
            vibesql_parser::Parser::parse_sql("INSERT INTO users VALUES (2, 'Bob', TRUE)").unwrap();
        if let vibesql_ast::Statement::Insert(stmt) = insert2 {
            vibesql_executor::InsertExecutor::execute(&mut db, &stmt).unwrap();
        }

        // Subscribe and get initial results
        let id = manager.subscribe("SELECT * FROM users".to_string(), tx).unwrap();
        manager.send_initial_results(id, &db).await.unwrap();

        // Consume initial Full update
        let initial = rx.recv().await.unwrap();
        match initial {
            SubscriptionUpdate::Full { rows, .. } => {
                assert_eq!(rows.len(), 2);
            }
            _ => panic!("Expected Full update for initial results"),
        }

        // Delete a row
        let delete = vibesql_parser::Parser::parse_sql("DELETE FROM users WHERE id = 2").unwrap();
        if let vibesql_ast::Statement::Delete(stmt) = delete {
            vibesql_executor::DeleteExecutor::execute(&stmt, &mut db).unwrap();
        }

        // Trigger change notification
        manager
            .handle_change(
                vibesql_storage::ChangeEvent::Delete {
                    table_name: "users".to_string(),
                    row_index: 1,
                },
                &db,
            )
            .await;

        // Should receive a Delta update with delete
        let update = rx.recv().await.unwrap();
        match update {
            SubscriptionUpdate::Delta { inserts, updates, deletes, .. } => {
                assert!(inserts.is_empty());
                assert!(updates.is_empty());
                assert_eq!(deletes.len(), 1);
                assert_eq!(deletes[0].values[0], SqlValue::Integer(2));
            }
            SubscriptionUpdate::Full { .. } => {
                panic!("Expected Delta update, got Full");
            }
            _ => panic!("Unexpected update type"),
        }
    }

    #[test]
    fn test_global_limit_exceeded() {
        // Create manager with very low global limit for testing
        let config = SubscriptionConfig {
            max_per_connection: 100,
            max_global: 2,
            max_result_rows: 10000,
            rate_limit_per_second: 100,
            ..Default::default()
        };
        let manager = SubscriptionManager::with_config(config);

        // First two subscriptions should succeed
        let (tx1, _rx1) = mpsc::channel(16);
        let (tx2, _rx2) = mpsc::channel(16);
        let (tx3, _rx3) = mpsc::channel(16);

        manager.subscribe("SELECT * FROM users".to_string(), tx1).unwrap();
        manager.subscribe("SELECT * FROM users WHERE id = 1".to_string(), tx2).unwrap();

        // Third subscription should fail with global limit exceeded
        let result = manager.subscribe("SELECT * FROM users WHERE id = 2".to_string(), tx3);
        assert!(matches!(
            result,
            Err(SubscriptionError::GlobalLimitExceeded { current: 2, max: 2 })
        ));

        // Metrics should reflect the limit exceeded event
        assert_eq!(manager.limit_exceeded_count(), 1);
    }

    #[tokio::test]
    async fn test_result_set_too_large() {
        // Create manager with very low result limit for testing
        let config = SubscriptionConfig {
            max_per_connection: 100,
            max_global: 10000,
            max_result_rows: 0, // No rows allowed
            rate_limit_per_second: 100,
            ..Default::default()
        };
        let manager = SubscriptionManager::with_config(config);
        let mut db = setup_test_db();

        // Insert some data
        let insert =
            vibesql_parser::Parser::parse_sql("INSERT INTO users VALUES (1, 'Alice', TRUE)")
                .unwrap();
        if let vibesql_ast::Statement::Insert(stmt) = insert {
            vibesql_executor::InsertExecutor::execute(&mut db, &stmt).unwrap();
        }

        // Subscribe
        let (tx, _rx) = mpsc::channel(16);
        let id = manager.subscribe("SELECT * FROM users".to_string(), tx).unwrap();

        // Send initial results should fail due to result set too large
        let result = manager.send_initial_results(id, &db).await;
        assert!(matches!(result, Err(SubscriptionError::ResultSetTooLarge { rows: 1, max: 0 })));

        // Metrics should reflect the result set exceeded event
        assert_eq!(manager.result_set_exceeded_count(), 1);
    }

    #[test]
    fn test_retry_policy_backoff_calculation() {
        use crate::subscription::SubscriptionRetryPolicy;

        let policy = SubscriptionRetryPolicy {
            max_retries: 3,
            base_delay_ms: 1000,
            max_delay_ms: 30000,
            backoff_multiplier: 2.0,
        };

        // First retry: 1000ms
        let backoff0 = policy.calculate_backoff(0);
        assert_eq!(backoff0.as_millis(), 1000);

        // Second retry: 2000ms
        let backoff1 = policy.calculate_backoff(1);
        assert_eq!(backoff1.as_millis(), 2000);

        // Third retry: 4000ms
        let backoff2 = policy.calculate_backoff(2);
        assert_eq!(backoff2.as_millis(), 4000);

        // Fourth retry: 8000ms
        let backoff3 = policy.calculate_backoff(3);
        assert_eq!(backoff3.as_millis(), 8000);

        // High attempt should be capped at max_delay_ms
        let backoff10 = policy.calculate_backoff(10);
        assert_eq!(backoff10.as_millis(), 30000);
    }

    #[test]
    fn test_subscription_retry_count_initialization() {
        let (tx, _rx) = mpsc::channel(16);
        let tables: std::collections::HashSet<_> = vec!["users".to_string()].into_iter().collect();

        let sub = Subscription::new("SELECT * FROM users".to_string(), tables, tx);
        assert_eq!(sub.retry_count, 0);
    }

    #[test]
    fn test_subscription_with_custom_policy() {
        use crate::subscription::SubscriptionRetryPolicy;

        let (tx, _rx) = mpsc::channel(16);
        let tables: std::collections::HashSet<_> = vec!["users".to_string()].into_iter().collect();

        let policy = SubscriptionRetryPolicy {
            max_retries: 5,
            base_delay_ms: 500,
            max_delay_ms: 60000,
            backoff_multiplier: 3.0,
        };

        let sub = Subscription::with_policy(
            "SELECT * FROM users".to_string(),
            tables,
            tx,
            policy.clone(),
        );

        assert_eq!(sub.retry_count, 0);
        assert_eq!(sub.retry_policy.max_retries, 5);
        assert_eq!(sub.retry_policy.base_delay_ms, 500);
    }

    #[test]
    fn test_subscription_backpressure_fields_initialization() {
        let (tx, _rx) = mpsc::channel(16);
        let tables: std::collections::HashSet<_> = vec!["users".to_string()].into_iter().collect();

        let sub = Subscription::new("SELECT * FROM users".to_string(), tables, tx);

        // Verify backpressure tracking fields are initialized
        assert_eq!(sub.updates_sent, 0);
        assert_eq!(sub.updates_dropped, 0);
        assert_eq!(sub.channel_buffer_size, 64); // default
        assert_eq!(sub.slow_consumer_threshold_percent, 80); // default
    }

    #[test]
    fn test_subscription_with_config_backpressure() {
        use crate::subscription::SubscriptionConfig;

        let (tx, _rx) = mpsc::channel(16);
        let tables: std::collections::HashSet<_> = vec!["users".to_string()].into_iter().collect();

        let config = SubscriptionConfig {
            max_per_connection: 100,
            max_global: 10000,
            max_result_rows: 10000,
            rate_limit_per_second: 100,
            channel_buffer_size: 128,
            slow_consumer_threshold_percent: 90,
            selective_updates: Default::default(),
        };

        let sub = Subscription::with_config("SELECT * FROM users".to_string(), tables, tx, &config);

        // Verify config values are applied
        assert_eq!(sub.updates_sent, 0);
        assert_eq!(sub.updates_dropped, 0);
        assert_eq!(sub.channel_buffer_size, 128);
        assert_eq!(sub.slow_consumer_threshold_percent, 90);
    }

    #[test]
    fn test_get_subscription_metrics() {
        let manager = SubscriptionManager::new();
        let (tx, _rx) = mpsc::channel(16);

        let id = manager.subscribe("SELECT * FROM users".to_string(), tx).unwrap();

        // Get metrics for the subscription
        let metrics = manager.get_subscription_metrics(id);
        assert!(metrics.is_some());

        let metrics = metrics.unwrap();
        assert_eq!(metrics.subscription_id, Some(id));
        assert_eq!(metrics.updates_sent, 0);
        assert_eq!(metrics.updates_dropped, 0);
        assert_eq!(metrics.channel_buffer_size, 64);
        assert_eq!(metrics.slow_consumer_threshold_percent, 80);
    }

    #[test]
    fn test_get_subscription_metrics_not_found() {
        let manager = SubscriptionManager::new();

        // Get metrics for a non-existent subscription
        let fake_id = SubscriptionId::new();
        let metrics = manager.get_subscription_metrics(fake_id);
        assert!(metrics.is_none());
    }

    #[test]
    fn test_get_all_metrics() {
        let manager = SubscriptionManager::new();
        let (tx1, _rx1) = mpsc::channel(16);
        let (tx2, _rx2) = mpsc::channel(16);

        manager.subscribe("SELECT * FROM users".to_string(), tx1).unwrap();
        manager.subscribe("SELECT * FROM orders".to_string(), tx2).unwrap();

        // Get all metrics
        let all_metrics = manager.get_all_metrics();
        assert_eq!(all_metrics.len(), 2);

        // Verify each subscription has metrics
        for metrics in all_metrics {
            assert!(metrics.subscription_id.is_some());
            assert_eq!(metrics.updates_sent, 0);
            assert_eq!(metrics.updates_dropped, 0);
        }
    }

    #[test]
    fn test_get_all_metrics_empty() {
        let manager = SubscriptionManager::new();

        // Get all metrics when no subscriptions exist
        let all_metrics = manager.get_all_metrics();
        assert!(all_metrics.is_empty());
    }

    // ========================================================================
    // Tests for connection tracking methods
    // ========================================================================

    #[test]
    fn test_subscribe_for_connection() {
        let manager = SubscriptionManager::new();
        let (tx, _rx) = mpsc::channel(16);

        let connection_id = "conn-1".to_string();
        let wire_id: [u8; 16] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
        let tables: HashSet<String> = ["users".to_string()].into_iter().collect();

        let result = manager.subscribe_for_connection(
            "SELECT * FROM users".to_string(),
            tx,
            connection_id.clone(),
            wire_id,
            tables,
            None, // no filter
        );

        assert!(result.is_ok());
        let id = result.unwrap();

        // Verify subscription was registered
        assert_eq!(manager.subscription_count(), 1);
        assert_eq!(manager.connection_subscription_count(&connection_id), 1);

        // Verify wire ID index works
        let found_id = manager.find_subscription_by_wire_id(&wire_id);
        assert_eq!(found_id, Some(id));
    }

    #[test]
    fn test_subscribe_for_connection_limit_enforcement() {
        use crate::subscription::SubscriptionConfig;

        let config = SubscriptionConfig {
            max_per_connection: 2,
            max_global: 10,
            ..Default::default()
        };
        let manager = SubscriptionManager::with_config(config);
        let connection_id = "conn-1".to_string();

        // Create 2 subscriptions (at the limit)
        for i in 0..2 {
            let (tx, _rx) = mpsc::channel(16);
            let mut wire_id = [0u8; 16];
            wire_id[0] = i;
            let tables: HashSet<String> = ["users".to_string()].into_iter().collect();

            let result = manager.subscribe_for_connection(
                format!("SELECT {} FROM users", i),
                tx,
                connection_id.clone(),
                wire_id,
                tables,
                None,
            );
            assert!(result.is_ok(), "Subscription {} should succeed", i);
        }

        assert_eq!(manager.connection_subscription_count(&connection_id), 2);

        // Third subscription should fail (connection limit)
        let (tx, _rx) = mpsc::channel(16);
        let wire_id = [2u8; 16];
        let tables: HashSet<String> = ["users".to_string()].into_iter().collect();

        let result = manager.subscribe_for_connection(
            "SELECT 3 FROM users".to_string(),
            tx,
            connection_id.clone(),
            wire_id,
            tables,
            None,
        );

        assert!(result.is_err());
        match result.unwrap_err() {
            crate::subscription::SubscriptionError::ConnectionLimitExceeded { current, max } => {
                assert_eq!(current, 2);
                assert_eq!(max, 2);
            }
            e => panic!("Expected ConnectionLimitExceeded, got {:?}", e),
        }
    }

    #[test]
    fn test_unsubscribe_by_wire_id() {
        let manager = SubscriptionManager::new();
        let (tx, _rx) = mpsc::channel(16);

        let connection_id = "conn-1".to_string();
        let wire_id: [u8; 16] = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16];
        let tables: HashSet<String> = ["users".to_string()].into_iter().collect();

        manager
            .subscribe_for_connection(
                "SELECT * FROM users".to_string(),
                tx,
                connection_id.clone(),
                wire_id,
                tables,
                None,
            )
            .unwrap();

        assert_eq!(manager.subscription_count(), 1);
        assert_eq!(manager.connection_subscription_count(&connection_id), 1);

        // Unsubscribe by wire ID
        manager.unsubscribe_by_wire_id(&wire_id);

        assert_eq!(manager.subscription_count(), 0);
        assert_eq!(manager.connection_subscription_count(&connection_id), 0);

        // Wire ID should no longer be found
        assert!(manager.find_subscription_by_wire_id(&wire_id).is_none());
    }

    #[test]
    fn test_unsubscribe_all_for_connection() {
        let manager = SubscriptionManager::new();
        let connection_id1 = "conn-1".to_string();
        let connection_id2 = "conn-2".to_string();

        // Create subscriptions for connection 1
        for i in 0..3 {
            let (tx, _rx) = mpsc::channel(16);
            let mut wire_id = [0u8; 16];
            wire_id[0] = i;
            let tables: HashSet<String> = ["users".to_string()].into_iter().collect();

            manager
                .subscribe_for_connection(
                    format!("SELECT {} FROM users", i),
                    tx,
                    connection_id1.clone(),
                    wire_id,
                    tables,
                    None,
                )
                .unwrap();
        }

        // Create subscriptions for connection 2
        for i in 0..2 {
            let (tx, _rx) = mpsc::channel(16);
            let mut wire_id = [10u8; 16];
            wire_id[0] = i + 10;
            let tables: HashSet<String> = ["orders".to_string()].into_iter().collect();

            manager
                .subscribe_for_connection(
                    format!("SELECT {} FROM orders", i),
                    tx,
                    connection_id2.clone(),
                    wire_id,
                    tables,
                    None,
                )
                .unwrap();
        }

        assert_eq!(manager.subscription_count(), 5);
        assert_eq!(manager.connection_subscription_count(&connection_id1), 3);
        assert_eq!(manager.connection_subscription_count(&connection_id2), 2);

        // Unsubscribe all for connection 1
        manager.unsubscribe_all_for_connection(&connection_id1);

        assert_eq!(manager.subscription_count(), 2);
        assert_eq!(manager.connection_subscription_count(&connection_id1), 0);
        assert_eq!(manager.connection_subscription_count(&connection_id2), 2);
    }

    #[test]
    fn test_connection_subscription_count() {
        let manager = SubscriptionManager::new();
        let connection_id = "conn-1".to_string();

        // Initially zero
        assert_eq!(manager.connection_subscription_count(&connection_id), 0);

        // Add a subscription
        let (tx, _rx) = mpsc::channel(16);
        let wire_id: [u8; 16] = [1u8; 16];
        let tables: HashSet<String> = ["users".to_string()].into_iter().collect();

        manager
            .subscribe_for_connection(
                "SELECT * FROM users".to_string(),
                tx,
                connection_id.clone(),
                wire_id,
                tables,
                None,
            )
            .unwrap();

        assert_eq!(manager.connection_subscription_count(&connection_id), 1);

        // Non-existent connection should return 0
        assert_eq!(manager.connection_subscription_count("non-existent"), 0);
    }

    #[test]
    fn test_get_affected_subscriptions_for_wire_protocol() {
        let manager = SubscriptionManager::new();
        let connection_id = "conn-1".to_string();

        // Create a subscription for "users" table
        let (tx, _rx) = mpsc::channel(16);
        let wire_id: [u8; 16] = [1u8; 16];
        let tables: HashSet<String> = ["users".to_string()].into_iter().collect();

        manager
            .subscribe_for_connection(
                "SELECT * FROM users".to_string(),
                tx,
                connection_id.clone(),
                wire_id,
                tables,
                None,
            )
            .unwrap();

        // Query for affected subscriptions
        let affected = manager.get_affected_subscriptions_for_wire_protocol("users");
        assert_eq!(affected.len(), 1);
        assert_eq!(affected[0].0, wire_id);
        assert_eq!(affected[0].1, "SELECT * FROM users");

        // Query for non-existent table
        let affected = manager.get_affected_subscriptions_for_wire_protocol("orders");
        assert!(affected.is_empty());
    }

    #[test]
    fn test_update_result_by_wire_id() {
        let manager = SubscriptionManager::new();
        let connection_id = "conn-1".to_string();

        let (tx, _rx) = mpsc::channel(16);
        let wire_id: [u8; 16] = [1u8; 16];
        let tables: HashSet<String> = ["users".to_string()].into_iter().collect();

        manager
            .subscribe_for_connection(
                "SELECT * FROM users".to_string(),
                tx,
                connection_id.clone(),
                wire_id,
                tables,
                None,
            )
            .unwrap();

        // Update the result
        let rows = vec![crate::Row { values: vec![vibesql_types::SqlValue::Integer(42)] }];
        manager.update_result_by_wire_id(&wire_id, 12345, rows.clone());

        // Verify the result was stored
        let affected = manager.get_affected_subscriptions_for_wire_protocol("users");
        assert_eq!(affected.len(), 1);
        assert_eq!(affected[0].2, 12345); // last_result_hash
        assert!(affected[0].3.is_some()); // last_result
        assert_eq!(affected[0].3.as_ref().unwrap().len(), 1);
    }
}
