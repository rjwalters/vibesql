//! Query execution handling
//!
//! This module handles SQL query execution, including tracking metrics,
//! handling mutations that affect subscriptions, and managing transaction status.

use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use bytes::BytesMut;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::broadcast;
use tracing::error;

use crate::{
    config::Config,
    observability::ObservabilityProvider,
    protocol::TransactionStatus,
    session::{ExecutionResult, Session},
    subscription::SubscriptionManager,
};

use super::protocol::{send_error_response, send_empty_query_response, send_query_result, send_ready_for_query};
use super::subscription::{broadcast_mutation, notify_affected_subscriptions};
use super::TableMutationNotification;

/// Execute a SQL query
#[allow(clippy::too_many_arguments)]
pub async fn execute_query(
    session: &mut Option<Session>,
    config: &Arc<Config>,
    observability: &Arc<ObservabilityProvider>,
    subscription_manager: &Arc<SubscriptionManager>,
    mutation_broadcast_tx: &broadcast::Sender<TableMutationNotification>,
    connection_id: &str,
    write_half: &mut OwnedWriteHalf,
    write_buf: &mut BytesMut,
    query: &str,
) -> Result<()> {
    let session = session.as_mut().ok_or_else(|| anyhow::anyhow!("No session"))?;

    // Handle empty query
    if query.trim().is_empty() {
        send_empty_query_response(write_half, write_buf).await?;
        let txn_status = get_transaction_status(Some(session));
        send_ready_for_query(write_half, write_buf, txn_status).await?;
        return Ok(());
    }

    // Track query execution time
    let query_start = Instant::now();

    // Execute query (now async due to shared database locking)
    match session.execute(query).await {
        Ok(result) => {
            let query_duration = query_start.elapsed();
            let stmt_type = result.statement_type();
            let rows_affected = result.rows_affected();

            // Record metrics
            if let Some(metrics) = observability.metrics() {
                metrics.record_query(query_duration, stmt_type, true, rows_affected);
            }

            // Check if this was a mutation that might affect subscriptions
            let is_mutation = matches!(
                &result,
                ExecutionResult::Insert { .. }
                    | ExecutionResult::Update { .. }
                    | ExecutionResult::Delete { .. }
            );

            send_query_result(write_half, write_buf, result).await?;

            // Notify affected subscriptions after mutations
            if is_mutation {
                // First, notify local subscriptions (same connection)
                notify_affected_subscriptions(
                    session,
                    config,
                    observability,
                    subscription_manager,
                    connection_id,
                    write_half,
                    write_buf,
                    query,
                )
                .await;

                // Then, broadcast to other connections for cross-connection notifications
                broadcast_mutation(mutation_broadcast_tx, query);
            }

            // Return appropriate transaction status
            let txn_status = get_transaction_status(Some(session));
            send_ready_for_query(write_half, write_buf, txn_status).await?;
            Ok(())
        }

        Err(e) => {
            error!("Query error: {}", e);

            // Record error metric
            if let Some(metrics) = observability.metrics() {
                metrics.record_query_error("execution_error", None);
            }

            send_error_response(write_half, write_buf, &format!("{}", e)).await?;

            // If in transaction and error occurred, report failed transaction state
            let txn_status = if session.in_transaction() {
                TransactionStatus::FailedTransaction
            } else {
                TransactionStatus::Idle
            };
            send_ready_for_query(write_half, write_buf, txn_status).await?;
            Ok(())
        }
    }
}

/// Get the current transaction status for the session
pub fn get_transaction_status(session: Option<&Session>) -> TransactionStatus {
    if session.is_some_and(|s| s.in_transaction()) {
        TransactionStatus::InTransaction
    } else {
        TransactionStatus::Idle
    }
}
