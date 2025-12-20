//! Subscription handling for real-time query updates
//!
//! This module handles subscription registration, update notifications,
//! and cross-connection subscription management.

use std::collections::HashSet;
use std::sync::Arc;

use anyhow::Result;
use bytes::BytesMut;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::broadcast;
use tracing::debug;
use vibesql_executor::cache::table_extractor;

use crate::{
    config::Config,
    observability::ObservabilityProvider,
    protocol::{SelectiveUpdatesConfig, SubscriptionUpdateType},
    session::{ExecutionResult, Session},
    subscription::{
        detect_pk_columns_from_stmt, extract_table_refs, filter::SubscriptionFilter, hash_rows,
        SelectiveColumnConfig, SubscriptionManager,
    },
    Row,
};

use super::protocol::{send_subscription_data, send_subscription_error};
use super::updates::send_subscription_update;
use super::TableMutationNotification;

/// Handle a subscription request
///
/// Parses the query, extracts table dependencies, executes the query,
/// registers the subscription, and sends the initial data to the client.
///
/// # Arguments
///
/// * `query` - The SQL SELECT query to subscribe to
/// * `_params` - Parameter values for parameterized queries (unused for now)
/// * `filter` - Optional filter expression (SQL WHERE clause) to apply to updates
#[allow(clippy::too_many_arguments)]
pub async fn handle_subscribe(
    session: &mut Option<Session>,
    config: &Arc<Config>,
    observability: &Arc<ObservabilityProvider>,
    subscription_manager: &Arc<SubscriptionManager>,
    connection_id: &str,
    write_half: &mut OwnedWriteHalf,
    write_buf: &mut BytesMut,
    query: &str,
    _params: Vec<Option<Vec<u8>>>,
    filter: Option<String>,
    selective_updates_config: Option<SelectiveUpdatesConfig>,
) -> Result<()> {
    let session = session.as_mut().ok_or_else(|| anyhow::anyhow!("No session"))?;

    // Parse the query to extract table dependencies
    let parsed = match vibesql_parser::Parser::parse_sql(query) {
        Ok(stmt) => stmt,
        Err(e) => {
            // Send subscription error with a dummy subscription ID (query failed before
            // registration)
            let error_id = [0u8; 16];
            send_subscription_error(
                write_half,
                write_buf,
                &error_id,
                &format!("Parse error: {}", e),
            )
            .await?;
            return Ok(());
        }
    };

    // Validate the filter expression if provided
    if let Some(ref filter_str) = filter {
        if let Err(e) = vibesql_parser::arena_parser::parse_expression_to_owned(filter_str) {
            let error_id = [0u8; 16];
            send_subscription_error(
                write_half,
                write_buf,
                &error_id,
                &format!("Filter parse error: {}", e),
            )
            .await?;
            return Ok(());
        }
    }

    // Extract table dependencies from the query
    let table_dependencies = table_extractor::extract_tables_from_statement(&parsed);

    // Detect primary key columns for selective updates
    // This enables bandwidth-efficient delta updates by knowing which columns identify rows
    let pk_detection = {
        let db = session.shared_database().read().await;
        detect_pk_columns_from_stmt(&parsed, &db)
    };
    if pk_detection.confident {
        debug!(
            "PK detection confident for subscription: pk_columns={:?}, tables={:?}",
            pk_detection.pk_column_indices, pk_detection.tables
        );
    } else {
        debug!(
            "PK detection not confident for subscription: reason={}, pk_columns={:?}, tables={:?}, query={}",
            pk_detection.reason.map(|r| r.to_string()).unwrap_or_else(|| "unknown".to_string()),
            pk_detection.pk_column_indices,
            pk_detection.tables,
            query
        );
    }

    // Record PK detection metrics
    if let Some(metrics) = observability.metrics() {
        if pk_detection.confident {
            metrics.record_pk_detection("confident", None);
        } else {
            // Determine reason for non-confidence based on detection results
            let reason = if pk_detection.tables.is_empty() {
                "no_table"
            } else if pk_detection.tables.len() > 1 {
                "join_query"
            } else if pk_detection.pk_column_indices == vec![0] {
                // Default fallback - could be multiple reasons
                "pk_not_in_result"
            } else {
                "unknown"
            };
            metrics.record_pk_detection("not_confident", Some(reason));
        }
    }

    // Generate a wire subscription ID (UUID) for the wire protocol
    let wire_subscription_id = *uuid::Uuid::new_v4().as_bytes();

    // Create a dummy channel - wire protocol sends data directly through TCP socket,
    // not through the subscription manager's channel-based notification system
    let (notify_tx, _notify_rx) = tokio::sync::mpsc::channel(1);

    // Register the subscription with the global subscription manager
    if let Err(e) = subscription_manager.subscribe_for_connection(
        query.to_string(),
        notify_tx,
        connection_id.to_string(),
        wire_subscription_id,
        table_dependencies.clone(),
        filter.clone(),
    ) {
        // Send subscription error with a dummy subscription ID (subscription failed before
        // registration)
        let error_id = [0u8; 16];
        send_subscription_error(write_half, write_buf, &error_id, &format!("{}", e)).await?;
        return Ok(());
    }

    // Track the new subscription in metrics
    if let Some(metrics) = observability.metrics() {
        metrics.increment_subscriptions_active();
    }

    // Store detected PK columns in the subscription for selective updates
    // Track selective-eligible subscriptions in metrics
    let newly_eligible = subscription_manager.update_pk_columns_with_eligibility_by_wire_id(
        &wire_subscription_id,
        pk_detection.pk_column_indices.clone(),
        pk_detection.confident,
    );
    if newly_eligible {
        if let Some(metrics) = observability.metrics() {
            metrics.increment_selective_eligible();
        }
    }

    // Apply per-subscription selective updates override if provided
    if let Some(wire_config) = selective_updates_config {
        // Convert wire protocol config to SelectiveColumnConfig
        // Merge with server defaults for any unspecified fields
        let server_config = &config.subscriptions.selective_updates;

        let override_config = SelectiveColumnConfig {
            enabled: wire_config.enabled.unwrap_or(server_config.enabled),
            pk_columns: pk_detection.pk_column_indices.clone(), // Use detected PK columns
            min_changed_columns: wire_config
                .min_changed_columns
                .unwrap_or(server_config.min_changed_columns),
            max_changed_columns_ratio: wire_config
                .max_changed_columns_ratio
                .unwrap_or(server_config.max_changed_columns_ratio),
        };

        subscription_manager
            .set_selective_updates_override_by_wire_id(&wire_subscription_id, override_config);
    }

    // Execute the query to get initial data
    match session.execute(query).await {
        Ok(ExecutionResult::Select { rows, columns }) => {
            // Build filter if present
            let filter_opt = filter.as_ref().and_then(|f| {
                let col_names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
                SubscriptionFilter::new(f, &col_names).ok()
            });

            // Filter rows if filter is present, then convert to Row format
            let result_rows: Vec<Row> = if let Some(ref flt) = filter_opt {
                rows.iter()
                    .filter(|row| flt.matches(&row.values))
                    .map(|r| Row { values: r.values.clone() })
                    .collect()
            } else {
                rows.iter().map(|r| Row { values: r.values.clone() }).collect()
            };

            // Compute hash and store result for future delta computation
            let result_hash = hash_rows(&result_rows);
            subscription_manager.update_result_by_wire_id(
                &wire_subscription_id,
                result_hash,
                result_rows.clone(),
            );

            // Convert rows to wire format
            let wire_rows: Vec<Vec<Option<Vec<u8>>>> = result_rows
                .iter()
                .map(|row| {
                    row.values.iter().map(|v| Some(v.to_string().as_bytes().to_vec())).collect()
                })
                .collect();

            // Send initial subscription data
            send_subscription_data(
                write_half,
                write_buf,
                observability,
                &wire_subscription_id,
                SubscriptionUpdateType::Full,
                wire_rows,
            )
            .await?;
        }
        Ok(_) => {
            // Non-SELECT query - send error and remove subscription
            let was_selective_eligible =
                subscription_manager.unsubscribe_by_wire_id(&wire_subscription_id);
            if was_selective_eligible {
                if let Some(metrics) = observability.metrics() {
                    metrics.decrement_selective_eligible();
                }
            }
            send_subscription_error(
                write_half,
                write_buf,
                &wire_subscription_id,
                "Only SELECT queries can be subscribed to",
            )
            .await?;
        }
        Err(e) => {
            // Query execution failed - remove subscription and send error
            let was_selective_eligible =
                subscription_manager.unsubscribe_by_wire_id(&wire_subscription_id);
            if was_selective_eligible {
                if let Some(metrics) = observability.metrics() {
                    metrics.decrement_selective_eligible();
                }
            }
            send_subscription_error(
                write_half,
                write_buf,
                &wire_subscription_id,
                &format!("Execution error: {}", e),
            )
            .await?;
        }
    }

    Ok(())
}

/// Notify affected subscriptions after a mutation (INSERT/UPDATE/DELETE)
///
/// This method parses the mutation query to extract the affected table,
/// finds all subscriptions that depend on that table, re-executes their
/// queries, and sends updated results to the client.
/// Supports delta updates to reduce network bandwidth.
/// Supports optional filtering expressions to send only matching rows.
#[allow(clippy::type_complexity, clippy::too_many_arguments)]
pub async fn notify_affected_subscriptions(
    session: &mut Session,
    config: &Arc<Config>,
    observability: &Arc<ObservabilityProvider>,
    subscription_manager: &Arc<SubscriptionManager>,
    connection_id: &str,
    write_half: &mut OwnedWriteHalf,
    write_buf: &mut BytesMut,
    mutation_query: &str,
) {
    // Parse the mutation query to extract affected tables
    let affected_tables = match vibesql_parser::Parser::parse_sql(mutation_query) {
        Ok(stmt) => extract_table_refs(&stmt),
        Err(e) => {
            debug!("Failed to parse mutation query for subscription update: {}", e);
            return;
        }
    };

    if affected_tables.is_empty() {
        return;
    }

    // Collect subscriptions for THIS connection that need updating
    let subscriptions_to_update: Vec<([u8; 16], String, u64, Option<Vec<Row>>, Option<String>)> =
        affected_tables
            .iter()
            .flat_map(|table| {
                subscription_manager.get_affected_subscriptions_for_connection(table, connection_id)
            })
            .collect();

    if subscriptions_to_update.is_empty() {
        return;
    }

    // De-duplicate subscriptions (a subscription may depend on multiple affected tables)
    let mut seen = std::collections::HashSet::new();
    let unique_subscriptions: Vec<_> =
        subscriptions_to_update.into_iter().filter(|(id, _, _, _, _)| seen.insert(*id)).collect();

    debug!(
        "Notifying {} subscriptions after mutation affecting tables: {:?}",
        unique_subscriptions.len(),
        affected_tables
    );

    // Re-execute each subscription query and send updates
    for (subscription_id, query, last_hash, last_result, filter) in unique_subscriptions {
        send_subscription_update(
            session,
            config,
            observability,
            subscription_manager,
            write_half,
            write_buf,
            &subscription_id,
            &query,
            last_hash,
            last_result,
            filter,
            "Same-connection",
        )
        .await;
    }
}

/// Handle a cross-connection notification about table mutations
///
/// When another connection mutates tables, this method is called to
/// check if any of our subscriptions are affected and send updates.
/// This method supports delta updates to reduce network bandwidth when
/// only a small portion of the result set has changed.
/// Supports optional filtering expressions to send only matching rows.
#[allow(clippy::type_complexity, clippy::too_many_arguments)]
pub async fn handle_cross_connection_notification(
    session: &mut Option<Session>,
    config: &Arc<Config>,
    observability: &Arc<ObservabilityProvider>,
    subscription_manager: &Arc<SubscriptionManager>,
    connection_id: &str,
    write_half: &mut OwnedWriteHalf,
    write_buf: &mut BytesMut,
    affected_tables: &HashSet<String>,
) {
    let session = match session.as_mut() {
        Some(s) => s,
        None => return,
    };

    // Collect subscriptions for THIS connection that need updating
    let subscriptions_to_update: Vec<([u8; 16], String, u64, Option<Vec<Row>>, Option<String>)> =
        affected_tables
            .iter()
            .flat_map(|table| {
                subscription_manager.get_affected_subscriptions_for_connection(table, connection_id)
            })
            .collect();

    if subscriptions_to_update.is_empty() {
        return;
    }

    // De-duplicate subscriptions (a subscription may depend on multiple affected tables)
    let mut seen = std::collections::HashSet::new();
    let unique_subscriptions: Vec<_> =
        subscriptions_to_update.into_iter().filter(|(id, _, _, _, _)| seen.insert(*id)).collect();

    debug!(
        "Cross-connection notification: notifying {} subscriptions for tables: {:?}",
        unique_subscriptions.len(),
        affected_tables
    );

    // Re-execute each subscription query and send updates
    for (subscription_id, query, last_hash, last_result, filter) in unique_subscriptions {
        send_subscription_update(
            session,
            config,
            observability,
            subscription_manager,
            write_half,
            write_buf,
            &subscription_id,
            &query,
            last_hash,
            last_result,
            filter,
            "Cross-connection",
        )
        .await;
    }
}

/// Broadcast a mutation event to all connections
///
/// This is called after a mutation (INSERT/UPDATE/DELETE) is executed to notify
/// other connections that may have subscriptions on the affected tables.
pub fn broadcast_mutation(
    mutation_broadcast_tx: &broadcast::Sender<TableMutationNotification>,
    mutation_query: &str,
) {
    // Parse the mutation query to extract affected tables
    let affected_tables = match vibesql_parser::Parser::parse_sql(mutation_query) {
        Ok(stmt) => extract_table_refs(&stmt),
        Err(e) => {
            debug!("Failed to parse mutation query for broadcast: {}", e);
            return;
        }
    };

    if affected_tables.is_empty() {
        return;
    }

    debug!("Broadcasting mutation affecting tables: {:?}", affected_tables);

    // Broadcast the notification to all connections
    // Note: This is fire-and-forget. If the channel is full or has no receivers,
    // it's okay - we've already notified our own connection's subscriptions.
    let notification = TableMutationNotification { affected_tables };
    if let Err(e) = mutation_broadcast_tx.send(notification) {
        // No receivers or channel issue - this is fine, just log at debug level
        debug!("Failed to broadcast mutation notification: {}", e);
    }
}
