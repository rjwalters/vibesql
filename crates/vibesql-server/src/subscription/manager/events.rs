//! Change event handling and notification for subscriptions.

use std::{
    collections::HashMap,
    sync::{atomic::Ordering, Arc},
};

use tracing::{debug, trace, warn};
use vibesql_storage::{change_events::RecvError, Database};

use super::SubscriptionManager;
use crate::subscription::{pk_prune::PkPruner, SubscriptionId};

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

    /// Decide whether `query` (a subscription's SELECT) must be re-queried given
    /// the changed primary-key identities in `events` (#5472).
    ///
    /// This is the conservative pruning predicate. It returns `true` (re-query)
    /// unless it can *prove* that **every** event's primary key cannot satisfy
    /// the subscription's `WHERE` filter:
    ///
    /// - If any event lacks a PK identity (`pk() == None` — e.g. composite keys or emission sites
    ///   without row data), we cannot reason and re-query.
    /// - The filter is analyzed (once) against the PK column name carried by the events; if it is
    ///   not a pure single-PK-column predicate the analyzer reports `Unanalyzable` and we re-query.
    /// - For an `Insert`, the new PK must be unable to match; for a `Delete`, the old PK; for an
    ///   `Update`, **both** the old and new PK (a row moving into or out of the set is a real
    ///   change). If any of these *could* match, we re-query.
    ///
    /// Returns `true` to re-query, `false` to safely skip. The caller increments
    /// the prune metric on a `false` result.
    fn subscription_needs_requery(query: &str, events: &[&vibesql_storage::ChangeEvent]) -> bool {
        use vibesql_storage::ChangeEvent;

        // Analyze the filter lazily, against the PK column name the events
        // carry. Built on first event that has a PK; reused for the rest.
        let mut pruner: Option<PkPruner> = None;

        for event in events {
            // No PK identity attached → cannot reason → must re-query.
            let pk = match event.pk() {
                Some(pk) => pk,
                None => return true,
            };

            // (Re)build the analyzer for this PK column if needed. All events on
            // a given table carry the same PK column, so this is built once.
            let analyzed = pruner.get_or_insert_with(|| PkPruner::analyze(query, &pk.column));

            let could_match = match event {
                ChangeEvent::Insert { .. } => analyzed.pk_might_match(&pk.value),
                ChangeEvent::Delete { .. } => analyzed.pk_might_match(&pk.value),
                ChangeEvent::Update { .. } => {
                    // Consider BOTH the pre-image (`value`) and the post-image
                    // (`new_value`, defaulting to `value` when the PK did not
                    // change). Re-query if EITHER could match.
                    let old_match = analyzed.pk_might_match(&pk.value);
                    let new_pk = pk.new_value.as_ref().unwrap_or(&pk.value);
                    let new_match = analyzed.pk_might_match(new_pk);
                    old_match || new_match
                }
            };

            if could_match {
                // At least one changed row could affect this subscription.
                return true;
            }
        }

        // Every event's PK provably cannot satisfy the filter → safe to skip.
        false
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
    pub async fn handle_change_replicated<F>(
        &self,
        event: vibesql_storage::ChangeEvent,
        query_fn: &F,
    ) where
        F: Fn(&str) -> Result<Vec<vibesql_storage::Row>, String>,
    {
        let table = event.table_name();
        trace!(table = %table, event = ?event, "Processing replicated apply-path change event");

        let affected_ids = self.find_affected_subscriptions(table);
        if affected_ids.is_empty() {
            return;
        }

        let relevant = [&event];
        for id in affected_ids {
            // Clone the query string out before re-querying so the state
            // machine lock (held inside `query_fn`) never overlaps the DashMap
            // mutable borrow used for notification.
            let query = match self.subscriptions.get(&id) {
                Some(sub) => sub.query.clone(),
                None => continue,
            };

            // PK pruning (#5472): skip the re-query when the changed PK provably
            // cannot satisfy this subscription's WHERE filter.
            if !Self::subscription_needs_requery(&query, &relevant) {
                self.replicated_requeries_pruned.fetch_add(1, Ordering::Relaxed);
                trace!(
                    subscription_id = %id,
                    "Pruned replicated re-query: changed PK cannot match the subscription filter"
                );
                continue;
            }

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

        // Union of affected subscription IDs across every event in the batch,
        // recording for each the slice of events that touched a table it depends
        // on (needed for PK pruning). `affected` preserves first-seen order;
        // `total_hits` counts event→subscription matches so we can report how
        // many re-queries coalescing alone saved.
        let mut affected: Vec<SubscriptionId> = Vec::new();
        let mut per_sub: HashMap<SubscriptionId, Vec<&vibesql_storage::ChangeEvent>> =
            HashMap::new();
        let mut total_hits: usize = 0;

        for event in events {
            for id in self.find_affected_subscriptions(event.table_name()) {
                total_hits += 1;
                let entry = per_sub.entry(id);
                if matches!(entry, std::collections::hash_map::Entry::Vacant(_)) {
                    affected.push(id);
                }
                entry.or_default().push(event);
            }
        }

        if affected.is_empty() {
            return;
        }

        // Re-queries saved by coalescing = (matches that would each trigger a
        // re-query) minus (the unique re-queries we would otherwise run).
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

            // PK pruning (#5472): skip the re-query entirely when none of this
            // subscription's relevant changes could satisfy its WHERE filter.
            let relevant = per_sub.get(&id).map(Vec::as_slice).unwrap_or(&[]);
            if !Self::subscription_needs_requery(&query, relevant) {
                self.replicated_requeries_pruned.fetch_add(1, Ordering::Relaxed);
                trace!(
                    subscription_id = %id,
                    "Pruned replicated re-query: changed PK(s) cannot match the subscription filter"
                );
                continue;
            }

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
