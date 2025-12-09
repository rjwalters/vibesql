//! Change event handling and notification for subscriptions.

use std::sync::Arc;

use tracing::{debug, trace, warn};
use vibesql_storage::change_events::RecvError;
use vibesql_storage::Database;

use super::SubscriptionManager;
use crate::subscription::SubscriptionId;

impl SubscriptionManager {
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
}
