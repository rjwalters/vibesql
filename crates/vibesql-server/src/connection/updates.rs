//! Subscription update sending logic
//!
//! This module handles sending subscription updates (delta and full) to clients.

use std::sync::Arc;

use anyhow::Result;
use bytes::BytesMut;
use tokio::net::tcp::OwnedWriteHalf;
use tracing::{debug, warn};

use crate::{
    config::Config,
    observability::ObservabilityProvider,
    protocol::SubscriptionUpdateType,
    session::{ExecutionResult, Session},
    subscription::{
        compute_delta_with_pk, filter::SubscriptionFilter, hash_rows, SubscriptionId,
        SubscriptionManager, SubscriptionUpdate,
    },
    Row,
};

use super::protocol::{send_subscription_data, send_subscription_error};
use super::selective::{rows_to_wire_format, try_send_selective_updates};

/// Send a subscription update (either delta or full)
#[allow(clippy::too_many_arguments)]
pub async fn send_subscription_update(
    session: &mut Session,
    config: &Arc<Config>,
    observability: &Arc<ObservabilityProvider>,
    subscription_manager: &Arc<SubscriptionManager>,
    write_half: &mut OwnedWriteHalf,
    write_buf: &mut BytesMut,
    subscription_id: &[u8; 16],
    query: &str,
    last_hash: u64,
    last_result: Option<Vec<Row>>,
    filter: Option<String>,
    log_prefix: &str,
) {
    match session.execute(query).await {
        Ok(ExecutionResult::Select { rows, columns }) => {
            // Build filter if present
            let filter_opt = filter.as_ref().and_then(|f| {
                let col_names: Vec<String> = columns.iter().map(|c| c.name.clone()).collect();
                SubscriptionFilter::new(f, &col_names).ok()
            });

            // Filter rows if filter is present, then convert to Row format
            let new_rows: Vec<Row> = if let Some(ref flt) = filter_opt {
                rows.iter()
                    .filter(|row| flt.matches(&row.values))
                    .map(|r| Row { values: r.values.clone() })
                    .collect()
            } else {
                rows.iter().map(|r| Row { values: r.values.clone() }).collect()
            };

            // Compute hash for change detection
            let new_hash = hash_rows(&new_rows);

            // Skip if results haven't changed
            if new_hash == last_hash {
                debug!(
                    "{} update: results unchanged for subscription {:?}",
                    log_prefix, subscription_id
                );
                return;
            }

            // Determine whether to send delta or full update
            if let Some(ref old_rows) = last_result {
                // First, try selective column updates (0xF7) using effective config
                if try_send_selective_updates(
                    subscription_manager,
                    config,
                    observability,
                    write_half,
                    write_buf,
                    subscription_id,
                    old_rows,
                    &new_rows,
                )
                .await
                {
                    // Selective updates sent successfully - update stored result
                    subscription_manager.update_result_by_wire_id(subscription_id, new_hash, new_rows);
                    return;
                }

                // Fall back to delta updates using PK columns
                let pk_columns = subscription_manager.get_pk_columns_by_wire_id(subscription_id);
                if let Some(delta) = compute_delta_with_pk(
                    SubscriptionId::default(),
                    old_rows,
                    &new_rows,
                    &pk_columns,
                ) {
                    // Send delta updates
                    if let Err(e) = send_delta_updates(
                        config,
                        observability,
                        subscription_manager,
                        write_half,
                        write_buf,
                        subscription_id,
                        &delta,
                    )
                    .await
                    {
                        warn!("Failed to send {} delta update: {}", log_prefix, e);
                    }

                    // Log delta statistics
                    if let SubscriptionUpdate::Delta {
                        ref inserts,
                        ref updates,
                        ref deletes,
                        ..
                    } = delta
                    {
                        debug!(
                            "{} delta update sent: {} inserts, {} updates, {} deletes for subscription {:?}",
                            log_prefix,
                            inserts.len(),
                            updates.len(),
                            deletes.len(),
                            subscription_id
                        );
                    }
                } else {
                    // No delta computed (shouldn't happen if hash changed)
                    // Fall back to full update
                    let wire_rows = rows_to_wire_format(&new_rows);
                    if let Err(e) = send_subscription_data(
                        write_half,
                        write_buf,
                        observability,
                        subscription_id,
                        SubscriptionUpdateType::Full,
                        wire_rows,
                    )
                    .await
                    {
                        warn!("Failed to send {} full update: {}", log_prefix, e);
                    }
                }
            } else {
                // No previous results - send full update
                debug!(
                    "{} update: no previous result, sending full update for subscription {:?}",
                    log_prefix, subscription_id
                );
                let wire_rows = rows_to_wire_format(&new_rows);
                if let Err(e) = send_subscription_data(
                    write_half,
                    write_buf,
                    observability,
                    subscription_id,
                    SubscriptionUpdateType::Full,
                    wire_rows,
                )
                .await
                {
                    warn!("Failed to send {} full update: {}", log_prefix, e);
                }
            }

            // Update stored result for next delta computation
            subscription_manager.update_result_by_wire_id(subscription_id, new_hash, new_rows);
        }
        Ok(_) => {
            // Non-SELECT result - shouldn't happen for a subscription query
            warn!("Subscription query returned non-SELECT result");
        }
        Err(e) => {
            // Query failed - send error to subscriber
            if let Err(send_err) =
                send_subscription_error(write_half, write_buf, subscription_id, &format!("Query error: {}", e))
                    .await
            {
                warn!("Failed to send subscription error: {}", send_err);
            }
        }
    }
}

/// Send delta updates to a subscription
///
/// The wire protocol sends separate messages for inserts, updates, and deletes.
/// For UPDATE operations, we use PartialRowUpdate to send only changed columns
/// plus PK columns, reducing wire traffic for wide tables.
pub async fn send_delta_updates(
    config: &Arc<Config>,
    observability: &Arc<ObservabilityProvider>,
    subscription_manager: &Arc<SubscriptionManager>,
    write_half: &mut OwnedWriteHalf,
    write_buf: &mut BytesMut,
    subscription_id: &[u8; 16],
    delta: &SubscriptionUpdate,
) -> Result<()> {
    if let SubscriptionUpdate::Delta { inserts, updates, deletes, .. } = delta {
        // Send deletes first (so clients can remove before adding)
        if !deletes.is_empty() {
            let wire_rows = rows_to_wire_format(deletes);
            send_subscription_data(
                write_half,
                write_buf,
                observability,
                subscription_id,
                SubscriptionUpdateType::DeltaDelete,
                wire_rows,
            )
            .await?;
        }

        // Send updates using partial row format when beneficial
        if !updates.is_empty() {
            // Get effective selective config for this subscription
            // Uses per-subscription override if set, otherwise falls back to server config
            let selective_config = subscription_manager.get_effective_selective_config_by_wire_id(
                subscription_id,
                &config.subscriptions.selective_updates,
            );
            let pk_columns = selective_config.pk_columns.clone();

            // Separate updates into partial and full based on threshold
            let mut partial_updates: Vec<crate::protocol::PartialRowUpdate> = Vec::new();
            let mut full_updates: Vec<Vec<Option<Vec<u8>>>> = Vec::new();

            for (old_row, new_row) in updates {
                // Convert rows to wire format
                let old_wire: Vec<Option<Vec<u8>>> = old_row
                    .values
                    .iter()
                    .map(|v| Some(v.to_string().as_bytes().to_vec()))
                    .collect();
                let new_wire: Vec<Option<Vec<u8>>> = new_row
                    .values
                    .iter()
                    .map(|v| Some(v.to_string().as_bytes().to_vec()))
                    .collect();

                // Try to create a partial update
                if let Some(partial) = crate::subscription::create_partial_row_update(
                    &old_wire,
                    &new_wire,
                    &pk_columns,
                    &selective_config,
                ) {
                    partial_updates.push(partial);
                } else {
                    // Fall back to full row update
                    full_updates.push(new_wire);
                }
            }

            // Send partial updates via SubscriptionPartialData (0xF7)
            if !partial_updates.is_empty() {
                super::protocol::send_subscription_partial_data(
                    write_half,
                    write_buf,
                    observability,
                    subscription_id,
                    partial_updates,
                )
                .await?;
            }

            // Send any full updates via regular DeltaUpdate
            if !full_updates.is_empty() {
                send_subscription_data(
                    write_half,
                    write_buf,
                    observability,
                    subscription_id,
                    SubscriptionUpdateType::DeltaUpdate,
                    full_updates,
                )
                .await?;
            }
        }

        // Send inserts last
        if !inserts.is_empty() {
            let wire_rows = rows_to_wire_format(inserts);
            send_subscription_data(
                write_half,
                write_buf,
                observability,
                subscription_id,
                SubscriptionUpdateType::DeltaInsert,
                wire_rows,
            )
            .await?;
        }
    }
    Ok(())
}
