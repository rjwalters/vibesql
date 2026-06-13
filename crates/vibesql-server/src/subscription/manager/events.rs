//! Change event handling and notification for subscriptions.

use std::{
    collections::HashSet,
    sync::{atomic::Ordering, Arc},
};

use tracing::{debug, trace, warn};
use vibesql_storage::{change_events::RecvError, Database};

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

    /// Handle a change event in **replicated mode** (#5422).
    ///
    /// Identical fanout to [`handle_change`](Self::handle_change), but each
    /// affected subscription's SELECT is re-executed against the consensus
    /// **applied** state machine via `query_fn` (which the server backs with
    /// `ReplicationHandle::with_applied_db`) instead of a local `Database`. The
    /// (sync) notification — delta computation, selective-column handling,
    /// slow-consumer detection — is shared with the standalone path via
    /// [`notify_with_rows`](Self::notify_with_rows).
    pub async fn handle_change_replicated<F>(&self, event: vibesql_storage::ChangeEvent, query_fn: &F)
    where
        F: Fn(&str) -> Result<Vec<vibesql_storage::Row>, String>,
    {
        let table = event.table_name();
        trace!(table = %table, event = ?event, "Processing replicated apply-path change event");

        let affected_ids = self.find_affected_subscriptions(table);
        if affected_ids.is_empty() {
            return;
        }

        for id in affected_ids {
            // Clone the query string out before re-querying so the state
            // machine lock (held inside `query_fn`) never overlaps the DashMap
            // mutable borrow used for notification.
            let query = match self.subscriptions.get(&id) {
                Some(sub) => sub.query.clone(),
                None => continue,
            };

            match query_fn(&query) {
                Ok(rows) => {
                    if let Some(mut sub_ref) = self.subscriptions.get_mut(&id) {
                        self.notify_with_rows(sub_ref.value_mut(), id, rows);
                    }
                }
                Err(error_msg) => {
                    warn!(
                        subscription_id = %id,
                        error = %error_msg,
                        "Replicated subscription query failed; skipping this change"
                    );
                }
            }
        }
    }

    /// Handle a **batch** of apply-path change events in replicated mode,
    /// re-querying each affected subscription **at most once** (#5456).
    ///
    /// This is the coalesced counterpart to
    /// [`handle_change_replicated`](Self::handle_change_replicated). A single
    /// committed write (or a burst of committed entries) emits one
    /// [`ChangeEvent`](vibesql_storage::ChangeEvent) per mutated row, so the
    /// apply-path feed produces many events back-to-back. Re-querying a
    /// subscription once per event is correct but redundant: the result is the
    /// same applied state whether we re-query after the first changed row or the
    /// last. Here we collect the union of affected subscription IDs across all
    /// `events`, then re-query each once.
    ///
    /// Correctness is preserved exactly: every subscription affected by *any*
    /// event in the batch is re-queried against the applied state and diffed via
    /// [`notify_with_rows`](Self::notify_with_rows), so no change is missed and
    /// none is over-delivered — identical end state to the per-event path, just
    /// fewer re-queries. The count of saved re-queries is recorded for metrics.
    pub async fn handle_changes_coalesced<F>(
        &self,
        events: &[vibesql_storage::ChangeEvent],
        query_fn: &F,
    ) where
        F: Fn(&str) -> Result<Vec<vibesql_storage::Row>, String>,
    {
        if events.is_empty() {
            return;
        }

        // Union of affected subscription IDs across every event in the batch.
        // `seen` dedupes so each subscription is re-queried once; `total_hits`
        // counts how many event→subscription matches occurred so we can report
        // how many re-queries coalescing saved.
        let mut affected: Vec<SubscriptionId> = Vec::new();
        let mut seen: HashSet<SubscriptionId> = HashSet::new();
        let mut total_hits: usize = 0;

        for event in events {
            for id in self.find_affected_subscriptions(event.table_name()) {
                total_hits += 1;
                if seen.insert(id) {
                    affected.push(id);
                }
            }
        }

        if affected.is_empty() {
            return;
        }

        // Re-queries saved = (matches that would each trigger a re-query) minus
        // (the unique re-queries we actually run).
        let saved = total_hits.saturating_sub(affected.len());
        if saved > 0 {
            self.replicated_requeries_coalesced.fetch_add(saved, Ordering::Relaxed);
            trace!(
                batch_events = events.len(),
                unique_subscriptions = affected.len(),
                requeries_saved = saved,
                "Coalesced replicated apply-path change events"
            );
        }

        for id in affected {
            let query = match self.subscriptions.get(&id) {
                Some(sub) => sub.query.clone(),
                None => continue,
            };

            match query_fn(&query) {
                Ok(rows) => {
                    if let Some(mut sub_ref) = self.subscriptions.get_mut(&id) {
                        self.notify_with_rows(sub_ref.value_mut(), id, rows);
                    }
                }
                Err(error_msg) => {
                    warn!(
                        subscription_id = %id,
                        error = %error_msg,
                        "Replicated subscription query failed; skipping this change batch"
                    );
                }
            }
        }
    }

    /// Run the **replicated** subscription event loop (#5422).
    ///
    /// Drains the consensus apply-path change feed (`change_rx`, from
    /// `ReplicationHandle::subscribe_changes`) and re-checks affected
    /// subscriptions against the applied state machine using `query_fn`. Runs
    /// until the feed closes — which happens when the state machine is replaced
    /// by a snapshot install or the node shuts down.
    ///
    /// When [`SubscriptionConfig::replicated_coalesce`] is enabled (the
    /// default), the loop drains every event currently available and re-queries
    /// each affected subscription **at most once per batch** (#5456) — a fan-out
    /// optimization that collapses the per-row events of a committed write into
    /// a single re-query per subscription. With it disabled, the loop re-queries
    /// strictly once per event. Both modes deliver identical updates; only the
    /// number of redundant re-queries differs.
    pub async fn run_replicated_event_loop<F>(
        &self,
        mut change_rx: vibesql_storage::ChangeEventReceiver,
        query_fn: F,
    ) where
        F: Fn(&str) -> Result<Vec<vibesql_storage::Row>, String>,
    {
        let coalesce = self.config.replicated_coalesce;
        loop {
            match change_rx.try_recv() {
                Ok(event) => {
                    if coalesce {
                        // Drain the rest of the currently-available burst, then
                        // re-query each affected subscription once for the batch.
                        let mut batch = vec![event];
                        let closed = loop {
                            match change_rx.try_recv() {
                                Ok(next) => batch.push(next),
                                Err(RecvError::Lagged(n)) => {
                                    warn!(
                                        lagged_count = n,
                                        "Replicated SubscriptionManager lagged behind \
                                         apply-path change events"
                                    );
                                    // Keep draining; re-query rebuilds full state anyway.
                                }
                                Err(RecvError::Empty) => break false,
                                Err(RecvError::Closed) => break true,
                            }
                        };
                        self.handle_changes_coalesced(&batch, &query_fn).await;
                        if closed {
                            debug!(
                                "Apply-path change feed closed (snapshot install or shutdown); \
                                 stopping replicated subscription loop"
                            );
                            break;
                        }
                    } else {
                        self.handle_change_replicated(event, &query_fn).await;
                    }
                }
                Err(RecvError::Lagged(n)) => {
                    warn!(
                        lagged_count = n,
                        "Replicated SubscriptionManager lagged behind apply-path change events"
                    );
                }
                Err(RecvError::Closed) => {
                    debug!(
                        "Apply-path change feed closed (snapshot install or shutdown); stopping \
                         replicated subscription loop"
                    );
                    break;
                }
                Err(RecvError::Empty) => {
                    tokio::task::yield_now().await;
                }
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
