//! Protocol message sending helpers
//!
//! This module provides helper functions for encoding and sending PostgreSQL
//! wire protocol messages to the client.

use std::{collections::HashMap, sync::Arc};

use anyhow::Result;
use bytes::BytesMut;
use tokio::net::tcp::OwnedWriteHalf;

use super::io::flush_write_buffer;
use crate::{
    observability::ObservabilityProvider,
    protocol::{
        BackendMessage, FieldDescription, PartialRowUpdate, SubscriptionUpdateType,
        TransactionStatus,
    },
    session::ExecutionResult,
};

/// Send parameter status message
pub async fn send_parameter_status(
    write_half: &mut OwnedWriteHalf,
    write_buf: &mut BytesMut,
    name: &str,
    value: &str,
) -> Result<()> {
    BackendMessage::ParameterStatus { name: name.to_string(), value: value.to_string() }
        .encode(write_buf);
    flush_write_buffer(write_half, write_buf).await
}

/// Send backend key data
pub async fn send_backend_key_data(
    write_half: &mut OwnedWriteHalf,
    write_buf: &mut BytesMut,
) -> Result<()> {
    BackendMessage::BackendKeyData {
        process_id: std::process::id() as i32,
        secret_key: 12345, // TODO: Generate random secret
    }
    .encode(write_buf);
    flush_write_buffer(write_half, write_buf).await
}

/// Send ready for query message
pub async fn send_ready_for_query(
    write_half: &mut OwnedWriteHalf,
    write_buf: &mut BytesMut,
    status: TransactionStatus,
) -> Result<()> {
    BackendMessage::ReadyForQuery { status }.encode(write_buf);
    flush_write_buffer(write_half, write_buf).await
}

/// Send command complete message
pub async fn send_command_complete(
    write_half: &mut OwnedWriteHalf,
    write_buf: &mut BytesMut,
    tag: &str,
) -> Result<()> {
    BackendMessage::CommandComplete { tag: tag.to_string() }.encode(write_buf);
    flush_write_buffer(write_half, write_buf).await
}

/// Send error response message
pub async fn send_error_response(
    write_half: &mut OwnedWriteHalf,
    write_buf: &mut BytesMut,
    message: &str,
) -> Result<()> {
    let mut fields = HashMap::new();
    fields.insert(b'S', "ERROR".to_string());
    fields.insert(b'C', "XX000".to_string()); // internal_error
    fields.insert(b'M', message.to_string());

    BackendMessage::ErrorResponse { fields }.encode(write_buf);
    flush_write_buffer(write_half, write_buf).await
}

/// Send an execution error, preserving a structured [`SqlError`]'s
/// SQLSTATE / detail / hint when the error carries one (#5383: NotLeader
/// redirects, staleness refusals, fatal-apply halts); anything else keeps
/// the existing `XX000` catch-all.
///
/// [`SqlError`]: crate::replication::SqlError
pub async fn send_execution_error(
    write_half: &mut OwnedWriteHalf,
    write_buf: &mut BytesMut,
    error: &anyhow::Error,
) -> Result<()> {
    let Some(sql_error) = error.downcast_ref::<crate::replication::SqlError>() else {
        return send_error_response(write_half, write_buf, &format!("{}", error)).await;
    };

    let mut fields = HashMap::new();
    fields.insert(b'S', "ERROR".to_string());
    fields.insert(b'V', "ERROR".to_string());
    fields.insert(b'C', sql_error.code.to_string());
    fields.insert(b'M', sql_error.message.clone());
    if let Some(detail) = &sql_error.detail {
        fields.insert(b'D', detail.clone());
    }
    if let Some(hint) = &sql_error.hint {
        fields.insert(b'H', hint.clone());
    }

    BackendMessage::ErrorResponse { fields }.encode(write_buf);
    flush_write_buffer(write_half, write_buf).await
}

/// Send empty query response message
pub async fn send_empty_query_response(
    write_half: &mut OwnedWriteHalf,
    write_buf: &mut BytesMut,
) -> Result<()> {
    BackendMessage::EmptyQueryResponse.encode(write_buf);
    flush_write_buffer(write_half, write_buf).await
}

/// Send subscription data message (initial results or updates)
pub async fn send_subscription_data(
    write_half: &mut OwnedWriteHalf,
    write_buf: &mut BytesMut,
    observability: &Arc<ObservabilityProvider>,
    subscription_id: &[u8; 16],
    update_type: SubscriptionUpdateType,
    rows: Vec<Vec<Option<Vec<u8>>>>,
) -> Result<()> {
    // Record subscription update metrics
    if let Some(metrics) = observability.metrics() {
        let type_str = match update_type {
            SubscriptionUpdateType::Full => "full",
            SubscriptionUpdateType::DeltaInsert => "delta_insert",
            SubscriptionUpdateType::DeltaUpdate => "delta_update",
            SubscriptionUpdateType::DeltaDelete => "delta_delete",
            SubscriptionUpdateType::SelectiveUpdate => "selective",
        };
        metrics.record_subscription_update(type_str, rows.len() as u64);

        // Record full update sent for efficiency stats
        if matches!(update_type, SubscriptionUpdateType::Full) {
            metrics.record_full_update_sent();
        }
    }

    BackendMessage::SubscriptionData { subscription_id: *subscription_id, update_type, rows }
        .encode(write_buf);
    flush_write_buffer(write_half, write_buf).await
}

/// Send subscription partial data message (for selective column updates)
///
/// Uses the SubscriptionPartialData (0xF7) message format to send only
/// changed columns plus primary key columns, reducing wire traffic.
pub async fn send_subscription_partial_data(
    write_half: &mut OwnedWriteHalf,
    write_buf: &mut BytesMut,
    observability: &Arc<ObservabilityProvider>,
    subscription_id: &[u8; 16],
    rows: Vec<PartialRowUpdate>,
) -> Result<()> {
    // Record subscription update metrics
    if let Some(metrics) = observability.metrics() {
        metrics.record_subscription_update("selective", rows.len() as u64);
    }

    BackendMessage::SubscriptionPartialData { subscription_id: *subscription_id, rows }
        .encode(write_buf);
    flush_write_buffer(write_half, write_buf).await
}

/// Send subscription error message
pub async fn send_subscription_error(
    write_half: &mut OwnedWriteHalf,
    write_buf: &mut BytesMut,
    subscription_id: &[u8; 16],
    message: &str,
) -> Result<()> {
    BackendMessage::SubscriptionError {
        subscription_id: *subscription_id,
        message: message.to_string(),
    }
    .encode(write_buf);
    flush_write_buffer(write_half, write_buf).await
}

/// Send query result to client
pub async fn send_query_result(
    write_half: &mut OwnedWriteHalf,
    write_buf: &mut BytesMut,
    result: ExecutionResult,
) -> Result<()> {
    match result {
        ExecutionResult::Select { rows, columns } => {
            // Send row description
            let fields: Vec<FieldDescription> = columns
                .iter()
                .enumerate()
                .map(|(i, col)| FieldDescription {
                    name: col.name.clone(),
                    table_oid: 0,
                    column_attr_number: i as i16,
                    data_type_oid: 25,  // TEXT type
                    data_type_size: -1, // Variable length
                    type_modifier: -1,
                    format_code: 0, // Text format
                })
                .collect();

            // Encode row description (no flush yet - batch with data rows)
            BackendMessage::RowDescription { fields }.encode(write_buf);

            // Save row count before consuming
            let row_count = rows.len();

            // Encode all data rows into the buffer without flushing
            for row in rows {
                let values: Vec<Option<Vec<u8>>> = row
                    .values
                    .iter()
                    .map(|v: &vibesql_types::SqlValue| Some(v.to_string().as_bytes().to_vec()))
                    .collect();

                BackendMessage::DataRow { values }.encode(write_buf);
            }

            // Encode command complete
            BackendMessage::CommandComplete { tag: format!("SELECT {}", row_count) }
                .encode(write_buf);

            // Single flush for the entire result set
            flush_write_buffer(write_half, write_buf).await?;
        }

        ExecutionResult::Insert { rows_affected } => {
            send_command_complete(write_half, write_buf, &format!("INSERT 0 {}", rows_affected))
                .await?;
        }

        ExecutionResult::Update { rows_affected } => {
            send_command_complete(write_half, write_buf, &format!("UPDATE {}", rows_affected))
                .await?;
        }

        ExecutionResult::Delete { rows_affected } => {
            send_command_complete(write_half, write_buf, &format!("DELETE {}", rows_affected))
                .await?;
        }

        ExecutionResult::CreateTable
        | ExecutionResult::CreateIndex
        | ExecutionResult::CreateView => {
            send_command_complete(write_half, write_buf, "CREATE TABLE").await?;
        }

        ExecutionResult::DropTable | ExecutionResult::DropIndex | ExecutionResult::DropView => {
            send_command_complete(write_half, write_buf, "DROP TABLE").await?;
        }

        ExecutionResult::Analyze { tables_analyzed } => {
            send_command_complete(write_half, write_buf, &format!("ANALYZE {}", tables_analyzed))
                .await?;
        }

        ExecutionResult::Other { message } => {
            send_command_complete(write_half, write_buf, &message).await?;
        }

        ExecutionResult::Prepare { statement_name } => {
            send_command_complete(write_half, write_buf, &format!("PREPARE {}", statement_name))
                .await?;
        }

        ExecutionResult::Deallocate { statement_name } => {
            send_command_complete(write_half, write_buf, &format!("DEALLOCATE {}", statement_name))
                .await?;
        }

        ExecutionResult::DeclareCursor { cursor_name } => {
            send_command_complete(
                write_half,
                write_buf,
                &format!("DECLARE CURSOR {}", cursor_name),
            )
            .await?;
        }

        ExecutionResult::OpenCursor { cursor_name } => {
            send_command_complete(write_half, write_buf, &format!("OPEN {}", cursor_name)).await?;
        }

        ExecutionResult::Fetch { rows, columns } => {
            // Send row description
            let fields: Vec<FieldDescription> = columns
                .iter()
                .enumerate()
                .map(|(i, col)| FieldDescription {
                    name: col.name.clone(),
                    table_oid: 0,
                    column_attr_number: i as i16,
                    data_type_oid: 25,  // TEXT type
                    data_type_size: -1, // Variable length
                    type_modifier: -1,
                    format_code: 0, // Text format
                })
                .collect();

            // Encode row description (no flush yet - batch with data rows)
            BackendMessage::RowDescription { fields }.encode(write_buf);

            // Save row count before consuming
            let row_count = rows.len();

            // Encode all data rows into the buffer without flushing
            for row in rows {
                let values: Vec<Option<Vec<u8>>> = row
                    .values
                    .iter()
                    .map(|v: &vibesql_types::SqlValue| Some(v.to_string().as_bytes().to_vec()))
                    .collect();

                BackendMessage::DataRow { values }.encode(write_buf);
            }

            // Encode command complete
            BackendMessage::CommandComplete { tag: format!("FETCH {}", row_count) }
                .encode(write_buf);

            // Single flush for the entire result set
            flush_write_buffer(write_half, write_buf).await?;
        }

        ExecutionResult::CloseCursor { cursor_name } => {
            send_command_complete(write_half, write_buf, &format!("CLOSE {}", cursor_name)).await?;
        }

        ExecutionResult::Begin => {
            send_command_complete(write_half, write_buf, "BEGIN").await?;
        }

        ExecutionResult::Commit => {
            send_command_complete(write_half, write_buf, "COMMIT").await?;
        }

        ExecutionResult::Rollback => {
            send_command_complete(write_half, write_buf, "ROLLBACK").await?;
        }

        ExecutionResult::Explain { plan_text, columns } => {
            // Send row description
            let fields: Vec<FieldDescription> = columns
                .iter()
                .enumerate()
                .map(|(i, col)| FieldDescription {
                    name: col.name.clone(),
                    table_oid: 0,
                    column_attr_number: i as i16,
                    data_type_oid: 25,  // TEXT type
                    data_type_size: -1, // Variable length
                    type_modifier: -1,
                    format_code: 0, // Text format
                })
                .collect();

            BackendMessage::RowDescription { fields }.encode(write_buf);

            // Split the plan text into lines and send each as a row
            let lines: Vec<&str> = plan_text.lines().collect();
            let row_count = lines.len();

            for line in lines {
                let values: Vec<Option<Vec<u8>>> = vec![Some(line.as_bytes().to_vec())];
                BackendMessage::DataRow { values }.encode(write_buf);
            }

            BackendMessage::CommandComplete { tag: format!("EXPLAIN {}", row_count) }
                .encode(write_buf);

            flush_write_buffer(write_half, write_buf).await?;
        }
    }

    Ok(())
}
