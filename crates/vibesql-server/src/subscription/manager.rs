//! Subscription manager for tracking and notifying query subscriptions
//!
//! The SubscriptionManager is the central component of the subscription system.
//! It maintains the registry of active subscriptions, indexes them by table
//! dependencies, and handles change event notifications.

use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use dashmap::DashMap;
use tokio::sync::mpsc;
use tracing::{debug, trace, warn};
use vibesql_storage::Database;
use vibesql_storage::change_events::RecvError;

use super::{
    compute_delta, extract_table_refs, hash_rows, Subscription, SubscriptionConfig, SubscriptionError, SubscriptionId,
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
pub struct SubscriptionManager {
    /// All active subscriptions, indexed by ID
    subscriptions: DashMap<SubscriptionId, Subscription>,

    /// Index: table_name -> subscription IDs that depend on it
    /// This enables fast lookup of affected subscriptions when a table changes
    table_index: DashMap<String, HashSet<SubscriptionId>>,

    /// Configuration for limits and quotas
    config: SubscriptionConfig,

    /// Counter for limit exceeded events (for metrics)
    limit_exceeded_count: AtomicUsize,
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
            config,
            limit_exceeded_count: AtomicUsize::new(0),
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
        // Check global limit before creating subscription
        let current_count = self.subscriptions.len();
        if current_count >= self.config.max_global {
            self.limit_exceeded_count.fetch_add(1, Ordering::Relaxed);
            return Err(SubscriptionError::GlobalLimitExceeded {
                current: current_count,
                max: self.config.max_global,
            });
        }

        // Parse query and extract table dependencies
        let tables = self.extract_tables(&query)?;

        if tables.is_empty() {
            return Err(SubscriptionError::ParseError(
                "Query must reference at least one table".to_string(),
            ));
        }

        // Create subscription
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
            self.table_index
                .entry(table)
                .or_default()
                .insert(id);
        }

        Ok(id)
    }

    /// Remove a subscription
    ///
    /// Unregisters the subscription and removes it from all table indexes.
    ///
    /// # Arguments
    ///
    /// * `id` - The subscription ID to remove
    pub fn unsubscribe(&self, id: SubscriptionId) {
        if let Some((_, subscription)) = self.subscriptions.remove(&id) {
            debug!(subscription_id = %id, "Removing subscription");

            // Remove from table index
            for table in &subscription.tables {
                if let Some(mut ids) = self.table_index.get_mut(table) {
                    ids.remove(&id);
                }
            }
        }
    }

    /// Get the number of active subscriptions
    pub fn subscription_count(&self) -> usize {
        self.subscriptions.len()
    }

    /// Get the tables being watched and their subscription counts
    pub fn watched_tables(&self) -> Vec<(String, usize)> {
        self.table_index
            .iter()
            .map(|entry| (entry.key().clone(), entry.value().len()))
            .collect()
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
        self.table_index
            .get(&table)
            .map(|ids| ids.iter().copied().collect())
            .unwrap_or_default()
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

        // Re-execute the query
        let executor = vibesql_executor::SelectExecutor::new(db);

        // Parse and execute the query
        let result = match vibesql_parser::Parser::parse_sql(&subscription.query) {
            Ok(vibesql_ast::Statement::Select(select)) => executor.execute(&select),
            Ok(_) => {
                // Not a SELECT - shouldn't happen for subscriptions
                warn!(
                    subscription_id = %id,
                    "Subscription query is not a SELECT"
                );
                return;
            }
            Err(e) => {
                // Query parse error - notify subscriber
                let _ = subscription
                    .notify_tx
                    .send(SubscriptionUpdate::Error {
                        message: format!("Failed to parse query: {}", e),
                    })
                    .await;
                return;
            }
        };

        match result {
            Ok(rows) => {
                // Convert to Row format
                let result_rows: Vec<crate::Row> = rows
                    .iter()
                    .map(|r| crate::Row {
                        values: r.values.clone(),
                    })
                    .collect();

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
                        // We have previous results - compute delta
                        if let Some(delta) = compute_delta(old_rows, &result_rows) {
                            // Log delta statistics
                            if let SubscriptionUpdate::Delta {
                                ref inserts,
                                ref updates,
                                ref deletes,
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
                            SubscriptionUpdate::Full {
                                rows: result_rows.clone(),
                            }
                        }
                    } else {
                        // No previous results - send full (first update after initial)
                        debug!(
                            subscription_id = %id,
                            "No previous result, sending full update"
                        );
                        SubscriptionUpdate::Full {
                            rows: result_rows.clone(),
                        }
                    };

                    // Update stored state
                    subscription.last_result_hash = new_hash;
                    subscription.last_result = Some(result_rows);

                    // Send update - ignore errors (channel may be closed)
                    if subscription.notify_tx.send(update).await.is_err() {
                        trace!(
                            subscription_id = %id,
                            "Notification channel closed, subscription will be cleaned up"
                        );
                    }
                } else {
                    trace!(
                        subscription_id = %id,
                        "Results unchanged, no notification needed"
                    );
                }
            }
            Err(e) => {
                // Query execution error - notify subscriber
                let _ = subscription
                    .notify_tx
                    .send(SubscriptionUpdate::Error {
                        message: format!("Query execution failed: {}", e),
                    })
                    .await;
            }
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
    pub async fn run_event_loop(&self, mut change_rx: vibesql_storage::ChangeEventReceiver, db: Arc<Database>) {
        loop {
            match change_rx.try_recv() {
                Ok(event) => {
                    self.handle_change(event, &db).await;
                }
                Err(RecvError::Lagged(n)) => {
                    warn!(
                        lagged_count = n,
                        "SubscriptionManager lagged behind change events"
                    );
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
        let mut sub_ref = self
            .subscriptions
            .get_mut(&id)
            .ok_or(SubscriptionError::NotFound(id))?;

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
            return Err(SubscriptionError::ResultSetTooLarge {
                rows: rows.len(),
                max: self.config.max_result_rows,
            });
        }

        // Convert to Row format
        let result_rows: Vec<crate::Row> = rows
            .iter()
            .map(|r| crate::Row {
                values: r.values.clone(),
            })
            .collect();

        // Update hash and store result for delta computation
        subscription.last_result_hash = hash_rows(&result_rows);
        subscription.last_result = Some(result_rows.clone());

        // Send initial results (always Full for initial)
        subscription
            .notify_tx
            .send(SubscriptionUpdate::Full { rows: result_rows })
            .await
            .map_err(|_| SubscriptionError::ChannelClosed)?;

        Ok(())
    }

    /// Get the current configuration
    pub fn config(&self) -> &SubscriptionConfig {
        &self.config
    }

    /// Get the number of times a limit was exceeded (for metrics)
    pub fn limit_exceeded_count(&self) -> usize {
        self.limit_exceeded_count.load(Ordering::Relaxed)
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

        let result = manager.subscribe(
            "SELECT * FROM users u JOIN orders o ON u.id = o.user_id".to_string(),
            tx,
        );
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

        let id = manager
            .subscribe("SELECT * FROM users".to_string(), tx)
            .unwrap();
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
        let _id = manager
            .subscribe("SELECT * FROM users".to_string(), tx)
            .unwrap();

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
            SubscriptionUpdate::Full { rows } => {
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
        let _id = manager
            .subscribe("SELECT * FROM users".to_string(), tx)
            .unwrap();

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
        let insert = vibesql_parser::Parser::parse_sql("INSERT INTO users VALUES (1, 'Alice', TRUE)")
            .unwrap();
        if let vibesql_ast::Statement::Insert(stmt) = insert {
            vibesql_executor::InsertExecutor::execute(&mut db, &stmt).unwrap();
        }

        // Subscribe
        let id = manager
            .subscribe("SELECT * FROM users".to_string(), tx)
            .unwrap();

        // Send initial results
        manager.send_initial_results(id, &db).await.unwrap();

        // Should receive initial data
        let update = rx.recv().await.unwrap();
        match update {
            SubscriptionUpdate::Full { rows } => {
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
        let id = manager
            .subscribe("SELECT * FROM users".to_string(), tx)
            .unwrap();

        // Send initial (empty) results
        manager.send_initial_results(id, &db).await.unwrap();
        let _ = rx.recv().await; // Consume initial

        // Insert data
        let insert = vibesql_parser::Parser::parse_sql("INSERT INTO users VALUES (1, 'Alice', TRUE)")
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
            SubscriptionUpdate::Delta {
                inserts,
                updates,
                deletes,
            } => {
                // The inserted row should appear as an insert
                assert_eq!(inserts.len(), 1);
                assert!(updates.is_empty());
                assert!(deletes.is_empty());
            }
            SubscriptionUpdate::Full { rows } => {
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
        let id = manager
            .subscribe("SELECT * FROM users".to_string(), tx)
            .unwrap();

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

        let _id1 = manager
            .subscribe("SELECT * FROM users".to_string(), tx1)
            .unwrap();
        let _id2 = manager
            .subscribe("SELECT * FROM users WHERE active = TRUE".to_string(), tx2)
            .unwrap();

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
        let insert = vibesql_parser::Parser::parse_sql("INSERT INTO users VALUES (1, 'Alice', TRUE)")
            .unwrap();
        if let vibesql_ast::Statement::Insert(stmt) = insert {
            vibesql_executor::InsertExecutor::execute(&mut db, &stmt).unwrap();
        }

        // Subscribe and get initial results
        let id = manager
            .subscribe("SELECT * FROM users".to_string(), tx)
            .unwrap();
        manager.send_initial_results(id, &db).await.unwrap();

        // Consume initial Full update
        let initial = rx.recv().await.unwrap();
        match initial {
            SubscriptionUpdate::Full { rows } => {
                assert_eq!(rows.len(), 1);
            }
            _ => panic!("Expected Full update for initial results"),
        }

        // Insert another row
        let insert2 = vibesql_parser::Parser::parse_sql("INSERT INTO users VALUES (2, 'Bob', TRUE)")
            .unwrap();
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
            SubscriptionUpdate::Delta {
                inserts,
                updates,
                deletes,
            } => {
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
        let insert1 = vibesql_parser::Parser::parse_sql("INSERT INTO users VALUES (1, 'Alice', TRUE)")
            .unwrap();
        if let vibesql_ast::Statement::Insert(stmt) = insert1 {
            vibesql_executor::InsertExecutor::execute(&mut db, &stmt).unwrap();
        }
        let insert2 = vibesql_parser::Parser::parse_sql("INSERT INTO users VALUES (2, 'Bob', TRUE)")
            .unwrap();
        if let vibesql_ast::Statement::Insert(stmt) = insert2 {
            vibesql_executor::InsertExecutor::execute(&mut db, &stmt).unwrap();
        }

        // Subscribe and get initial results
        let id = manager
            .subscribe("SELECT * FROM users".to_string(), tx)
            .unwrap();
        manager.send_initial_results(id, &db).await.unwrap();

        // Consume initial Full update
        let initial = rx.recv().await.unwrap();
        match initial {
            SubscriptionUpdate::Full { rows } => {
                assert_eq!(rows.len(), 2);
            }
            _ => panic!("Expected Full update for initial results"),
        }

        // Delete a row
        let delete = vibesql_parser::Parser::parse_sql("DELETE FROM users WHERE id = 2")
            .unwrap();
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
        assert!(matches!(result, Err(SubscriptionError::GlobalLimitExceeded { current: 2, max: 2 })));

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
        };
        let manager = SubscriptionManager::with_config(config);
        let mut db = setup_test_db();

        // Insert some data
        let insert = vibesql_parser::Parser::parse_sql("INSERT INTO users VALUES (1, 'Alice', TRUE)")
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
    }
}
