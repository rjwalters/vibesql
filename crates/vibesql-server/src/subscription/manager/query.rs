//! Query execution and retry logic for subscriptions.

use std::sync::atomic::Ordering;

use tracing::{debug, trace, warn};
use vibesql_storage::Database;

use super::SubscriptionManager;
use crate::subscription::{
    classify_error_str, compute_delta_with_pk, hash_rows, PartialRowDelta, Subscription,
    SubscriptionError, SubscriptionErrorKind, SubscriptionId, SubscriptionUpdate,
};

impl SubscriptionManager {
    /// Execute query with retry logic for transient errors
    pub(crate) async fn execute_with_retry(
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
                    // Successful execution - reset retry count, then compute the
                    // delta against the prior result and notify (shared with the
                    // replicated apply-path loop via `notify_with_rows`).
                    subscription.retry_count = 0;
                    self.notify_with_rows(subscription, id, rows);
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

    /// Compute the update (Full / Delta / Partial) from freshly executed
    /// `rows` against the subscription's prior result and notify the subscriber
    /// if the result changed.
    ///
    /// This is the notification half of [`execute_with_retry`], factored out so
    /// the **replicated** apply-path subscription loop (#5422) can reuse the
    /// exact same delta/selective-column/slow-consumer logic. It is synchronous
    /// (uses `try_send`), so the caller may invoke it after running the query —
    /// in replicated mode, after re-querying the applied state machine — without
    /// holding any database lock across an `.await`.
    pub(crate) fn notify_with_rows(
        &self,
        subscription: &mut Subscription,
        id: SubscriptionId,
        rows: Vec<vibesql_storage::Row>,
    ) {
        // Convert to Row format
        let result_rows: Vec<crate::Row> =
            rows.iter().map(|r| crate::Row { values: r.values.to_vec() }).collect();

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

            // Determine whether to send Delta, Partial, or Full update
            let update = if let Some(ref old_rows) = subscription.last_result {
                // We have previous results - compute delta using PK columns
                if let Some(delta) =
                    compute_delta_with_pk(id, old_rows, &result_rows, &subscription.pk_columns)
                {
                    // Check if we can use Partial updates (selective column updates)
                    // Conditions:
                    // 1. Subscription is selective_eligible (confident PK detection)
                    // 2. Delta has only updates (no inserts or deletes)
                    // 3. Updates exist
                    if let SubscriptionUpdate::Delta { ref inserts, ref updates, ref deletes, .. } =
                        delta
                    {
                        if subscription.selective_eligible
                            && inserts.is_empty()
                            && deletes.is_empty()
                            && !updates.is_empty()
                        {
                            // Convert to Partial updates
                            let partial_updates: Vec<PartialRowDelta> = updates
                                .iter()
                                .filter_map(|(old_row, new_row)| {
                                    PartialRowDelta::from_rows(
                                        old_row,
                                        new_row,
                                        &subscription.pk_columns,
                                    )
                                })
                                .collect();

                            if !partial_updates.is_empty() {
                                debug!(
                                    subscription_id = %id,
                                    partial_updates = partial_updates.len(),
                                    "Sending partial update (selective columns)"
                                );
                                SubscriptionUpdate::Partial {
                                    subscription_id: id,
                                    updates: partial_updates,
                                }
                            } else {
                                // Fall back to delta if partial conversion failed
                                debug!(
                                    subscription_id = %id,
                                    updates = updates.len(),
                                    "Sending delta update (partial conversion failed)"
                                );
                                delta
                            }
                        } else {
                            // Log delta statistics and send as-is
                            debug!(
                                subscription_id = %id,
                                inserts = inserts.len(),
                                updates = updates.len(),
                                deletes = deletes.len(),
                                selective_eligible = subscription.selective_eligible,
                                "Sending delta update"
                            );
                            delta
                        }
                    } else {
                        delta
                    }
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
            let usage_percent = if max_capacity > 0 { (used * 100) / max_capacity } else { 0 };

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
    }

    /// Execute the subscription query and return rows or error message
    pub(crate) async fn execute_subscription_query(
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

    /// Synchronously execute a subscription's SELECT against an arbitrary
    /// database, returning its rows (#5422). Unlike
    /// [`execute_subscription_query`](Self::execute_subscription_query) this is
    /// `async`-free, so the replicated apply-path loop can call it inside
    /// [`ReplicationHandle::with_applied_db`] (which holds the state machine
    /// lock and forbids `.await`). The notification half runs afterwards via
    /// [`notify_with_rows`](Self::notify_with_rows), outside the lock.
    pub fn execute_query_against(
        query: &str,
        db: &Database,
    ) -> Result<Vec<vibesql_storage::Row>, String> {
        let executor = vibesql_executor::SelectExecutor::new(db);
        match vibesql_parser::Parser::parse_sql(query) {
            Ok(vibesql_ast::Statement::Select(select)) => {
                executor.execute(&select).map_err(|e| e.to_string())
            }
            Ok(_) => Err("Subscription query is not a SELECT".to_string()),
            Err(e) => Err(format!("Failed to parse query: {}", e)),
        }
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
            rows.iter().map(|r| crate::Row { values: r.values.to_vec() }).collect();

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

    /// Seed a subscription's initial result from rows the caller already
    /// executed, and send them as the initial Full update (#5422).
    ///
    /// The replicated SSE handler executes a subscription's initial SELECT
    /// against the **applied** consensus state itself (via the
    /// `ReplicationHandle` read path), then hands the rows here. This mirrors
    /// [`send_initial_results`](Self::send_initial_results) but sources the rows
    /// from the consensus state machine instead of a local `Database`, so the
    /// initial snapshot is consistent with the apply-path change feed that
    /// subsequently drives updates.
    pub async fn prime_initial_result(
        &self,
        id: SubscriptionId,
        rows: Vec<crate::Row>,
    ) -> Result<(), SubscriptionError> {
        let mut sub_ref = self.subscriptions.get_mut(&id).ok_or(SubscriptionError::NotFound(id))?;
        let subscription = sub_ref.value_mut();

        if rows.len() > self.config.max_result_rows {
            self.result_set_exceeded_count.fetch_add(1, Ordering::Relaxed);
            return Err(SubscriptionError::ResultSetTooLarge {
                rows: rows.len(),
                max: self.config.max_result_rows,
            });
        }

        subscription.last_result_hash = hash_rows(&rows);
        subscription.last_result = Some(rows.clone());

        subscription
            .notify_tx
            .send(SubscriptionUpdate::Full { subscription_id: id, rows })
            .await
            .map_err(|_| SubscriptionError::ChannelClosed)?;

        Ok(())
    }
}
