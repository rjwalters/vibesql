//! Selective column update handling
//!
//! This module handles selective column updates (0xF7 message format), which send
//! only changed columns plus primary key columns to reduce wire traffic for
//! subscription updates.

use std::collections::HashMap;
use std::sync::Arc;

use bytes::BytesMut;
use tokio::net::tcp::OwnedWriteHalf;
use tracing::debug;

use crate::{
    config::Config,
    observability::ObservabilityProvider,
    subscription::{create_partial_row_update, SubscriptionManager},
    Row,
};

use super::protocol::send_subscription_partial_data;

/// Convert rows to wire format for sending over the protocol
pub fn rows_to_wire_format(rows: &[Row]) -> Vec<Vec<Option<Vec<u8>>>> {
    rows.iter()
        .map(|row| row.values.iter().map(|v| Some(v.to_string().as_bytes().to_vec())).collect())
        .collect()
}

/// Try to send selective column updates (0xF7) for row updates
///
/// Returns `true` if selective updates were sent, `false` if caller should
/// fall back to regular updates.
///
/// Selective updates are used when:
/// - Config has selective updates enabled
/// - Row counts match (updates only, not inserts/deletes)
/// - Rows can be matched by primary key
/// - Changed columns ratio is within threshold
pub async fn try_send_selective_updates(
    subscription_manager: &Arc<SubscriptionManager>,
    config: &Arc<Config>,
    observability: &Arc<ObservabilityProvider>,
    write_half: &mut OwnedWriteHalf,
    write_buf: &mut BytesMut,
    subscription_id: &[u8; 16],
    old_rows: &[Row],
    new_rows: &[Row],
) -> bool {
    // Get effective selective config (uses per-subscription override if set)
    let selective_config = subscription_manager.get_effective_selective_config_by_wire_id(
        subscription_id,
        &config.subscriptions.selective_updates,
    );

    // Check if selective updates are enabled in effective config
    if !selective_config.enabled {
        debug!(
            "Selective update skipped for subscription {:?}: disabled in config",
            subscription_id
        );
        if let Some(metrics) = observability.metrics() {
            metrics.record_partial_update_fallback("disabled");
            metrics.record_selective_update_decision("sent_full", Some("disabled"));
        }
        return false;
    }

    // Row counts must match for selective updates (no inserts/deletes)
    if old_rows.len() != new_rows.len() {
        debug!(
            "Selective update skipped for subscription {:?}: row count mismatch (old={}, new={})",
            subscription_id,
            old_rows.len(),
            new_rows.len()
        );
        if let Some(metrics) = observability.metrics() {
            metrics.record_partial_update_fallback("row_count_mismatch");
            metrics.record_selective_update_decision("sent_full", Some("row_count_mismatch"));
        }
        return false;
    }

    if old_rows.is_empty() {
        return false;
    }

    let pk_columns = &selective_config.pk_columns;

    // Convert rows to wire format for comparison
    let old_wire: Vec<Vec<Option<Vec<u8>>>> = rows_to_wire_format(old_rows);
    let new_wire: Vec<Vec<Option<Vec<u8>>>> = rows_to_wire_format(new_rows);

    // Build a map from PK values to row index for old rows
    let mut pk_to_old_idx: HashMap<Vec<Option<Vec<u8>>>, usize> = HashMap::new();
    for (idx, row) in old_wire.iter().enumerate() {
        let pk_values: Vec<Option<Vec<u8>>> =
            pk_columns.iter().filter_map(|&col| row.get(col).cloned()).collect();
        pk_to_old_idx.insert(pk_values, idx);
    }

    // Try to create partial row updates for each new row
    let mut partial_updates = Vec::new();
    let mut threshold_exceeded_count = 0u64;
    for new_row in &new_wire {
        // Extract PK from new row
        let pk_values: Vec<Option<Vec<u8>>> =
            pk_columns.iter().filter_map(|&col| new_row.get(col).cloned()).collect();

        // Find matching old row by PK
        if let Some(&old_idx) = pk_to_old_idx.get(&pk_values) {
            let old_row = &old_wire[old_idx];

            // Try to create a partial row update
            if let Some(partial) =
                create_partial_row_update(old_row, new_row, pk_columns, &selective_config)
            {
                // Record column ratio for successful partial updates
                let changed_count =
                    old_row.iter().zip(new_row.iter()).filter(|(o, n)| o != n).count();
                if let Some(metrics) = observability.metrics() {
                    metrics.record_selective_update_column_ratio(changed_count, new_row.len());
                }
                partial_updates.push(partial);
            } else {
                // Check if this was due to threshold exceeded (too many columns changed)
                let changed_count =
                    old_row.iter().zip(new_row.iter()).filter(|(o, n)| o != n).count();
                if changed_count > 0 {
                    let ratio = changed_count as f64 / new_row.len() as f64;
                    // Record column ratio for analysis (helps tuning threshold)
                    if let Some(metrics) = observability.metrics() {
                        metrics.record_selective_update_column_ratio(changed_count, new_row.len());
                    }
                    if ratio > selective_config.max_changed_columns_ratio {
                        threshold_exceeded_count += 1;
                    }
                }
                continue;
            }
        } else {
            // Can't find matching old row - this is an insert, not an update
            // Fall back to regular updates
            debug!(
                "Selective update skipped for subscription {:?}: cannot match row by PK (pk_columns={:?})",
                subscription_id,
                pk_columns
            );
            if let Some(metrics) = observability.metrics() {
                metrics.record_partial_update_fallback("pk_mismatch");
                metrics.record_selective_update_decision("sent_full", Some("pk_mismatch"));
            }
            return false;
        }
    }

    // Record threshold exceeded fallbacks if any
    if threshold_exceeded_count > 0 {
        debug!(
            "Selective update: {} rows exceeded change threshold for subscription {:?}",
            threshold_exceeded_count, subscription_id
        );
        if let Some(metrics) = observability.metrics() {
            for _ in 0..threshold_exceeded_count {
                metrics.record_partial_update_fallback("threshold_exceeded");
                metrics.record_selective_update_decision("sent_full", Some("threshold_exceeded"));
            }
        }
    }

    // If no partial updates were generated, nothing changed
    if partial_updates.is_empty() {
        debug!(
            "Selective update skipped for subscription {:?}: no column changes detected",
            subscription_id
        );
        if let Some(metrics) = observability.metrics() {
            metrics.record_partial_update_fallback("no_changes");
            metrics.record_selective_update_decision("skipped", Some("no_changes"));
        }
        return false;
    }

    // Calculate and record metrics before sending
    if let Some(metrics) = observability.metrics() {
        let total_columns = if !new_wire.is_empty() { new_wire[0].len() as u64 } else { 0 };
        let mut total_columns_sent: u64 = 0;
        let mut total_bytes_full: u64 = 0;
        let mut total_bytes_partial: u64 = 0;

        for (partial, new_row) in partial_updates.iter().zip(new_wire.iter()) {
            // Count columns sent in this partial update
            total_columns_sent += partial.present_column_count() as u64;

            // Estimate bytes for full row vs partial update
            let full_row_bytes: u64 = new_row
                .iter()
                .map(|v| v.as_ref().map(|b| b.len() as u64).unwrap_or(0) + 4) // value + length prefix
                .sum();
            let partial_bytes: u64 = partial
                .values
                .iter()
                .map(|v| v.as_ref().map(|b| b.len() as u64).unwrap_or(0) + 4)
                .sum::<u64>()
                + partial.column_mask.len() as u64
                + 2; // mask + total_columns header

            total_bytes_full += full_row_bytes;
            total_bytes_partial += partial_bytes;
        }

        // Record column efficiency metrics
        let total_possible = total_columns * partial_updates.len() as u64;
        metrics.record_selective_update_columns(total_columns_sent, total_possible);

        // Record bytes saved
        if total_bytes_full > total_bytes_partial {
            metrics.record_partial_update_bytes_saved(total_bytes_full - total_bytes_partial);
        }

        // Record successful selective update decision for each partial update
        for _ in 0..partial_updates.len() {
            metrics.record_selective_update_decision("sent_partial", None);
        }
    }

    // Send the partial updates
    if let Err(e) =
        send_subscription_partial_data(write_half, write_buf, observability, subscription_id, partial_updates).await
    {
        tracing::warn!("Failed to send selective updates: {}", e);
        return false;
    }

    // Record successful partial update sent
    if let Some(metrics) = observability.metrics() {
        metrics.record_partial_update_sent();
    }

    debug!("Sent selective column update (0xF7) for subscription {:?}", subscription_id);
    true
}
