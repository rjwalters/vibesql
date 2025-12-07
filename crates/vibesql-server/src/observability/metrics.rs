use opentelemetry::metrics::{Counter, Histogram, Meter};
use opentelemetry::KeyValue;
use std::time::Duration;

/// Server metrics collection
#[derive(Clone)]
pub struct ServerMetrics {
    // Connection metrics
    connections_total: Counter<u64>,
    #[allow(dead_code)]
    connection_errors_total: Counter<u64>,
    connection_duration: Histogram<f64>,

    // Query metrics
    queries_total: Counter<u64>,
    query_duration: Histogram<f64>,
    query_errors_total: Counter<u64>,
    query_rows_affected: Histogram<u64>,

    // Protocol metrics
    #[allow(dead_code)]
    messages_received_total: Counter<u64>,
    #[allow(dead_code)]
    messages_sent_total: Counter<u64>,
    #[allow(dead_code)]
    bytes_received_total: Counter<u64>,
    #[allow(dead_code)]
    bytes_sent_total: Counter<u64>,

    // Subscription metrics
    subscription_updates_total: Counter<u64>,
    selective_update_columns_sent: Histogram<u64>,
    selective_update_changed_ratio: Histogram<f64>,
}

impl ServerMetrics {
    /// Create new server metrics
    pub fn new(meter: &Meter) -> Self {
        // Connection metrics
        let connections_total = meter
            .u64_counter("vibesql_server_connections_total")
            .with_description("Total connections accepted")
            .with_unit("{connection}")
            .build();

        let connection_errors_total = meter
            .u64_counter("vibesql_server_connection_errors_total")
            .with_description("Connection failures by error type")
            .with_unit("{error}")
            .build();

        let connection_duration = meter
            .f64_histogram("vibesql_server_connection_duration_seconds")
            .with_description("Connection lifetime distribution")
            .with_unit("s")
            .build();

        // Query metrics
        let queries_total = meter
            .u64_counter("vibesql_server_queries_total")
            .with_description("Queries executed by statement type")
            .with_unit("{query}")
            .build();

        let query_duration = meter
            .f64_histogram("vibesql_server_query_duration_seconds")
            .with_description("Query execution latency")
            .with_unit("s")
            .build();

        let query_errors_total = meter
            .u64_counter("vibesql_server_query_errors_total")
            .with_description("Query errors by error type")
            .with_unit("{error}")
            .build();

        let query_rows_affected = meter
            .u64_histogram("vibesql_server_query_rows_affected")
            .with_description("Rows affected distribution")
            .with_unit("{row}")
            .build();

        // Protocol metrics
        let messages_received_total = meter
            .u64_counter("vibesql_server_messages_received_total")
            .with_description("PostgreSQL protocol messages received")
            .with_unit("{message}")
            .build();

        let messages_sent_total = meter
            .u64_counter("vibesql_server_messages_sent_total")
            .with_description("PostgreSQL protocol messages sent")
            .with_unit("{message}")
            .build();

        let bytes_received_total = meter
            .u64_counter("vibesql_server_bytes_received_total")
            .with_description("Total bytes received")
            .with_unit("By")
            .build();

        let bytes_sent_total = meter
            .u64_counter("vibesql_server_bytes_sent_total")
            .with_description("Total bytes sent")
            .with_unit("By")
            .build();

        // Subscription metrics
        let subscription_updates_total = meter
            .u64_counter("vibesql_subscription_updates_total")
            .with_description("Subscription updates sent by type (full, delta_insert, delta_update, delta_delete, selective)")
            .with_unit("{update}")
            .build();

        let selective_update_columns_sent = meter
            .u64_histogram("vibesql_selective_update_columns_sent")
            .with_description("Number of columns sent in selective updates")
            .with_unit("{column}")
            .build();

        let selective_update_changed_ratio = meter
            .f64_histogram("vibesql_selective_update_changed_ratio")
            .with_description("Ratio of changed columns to total columns in selective updates (0.0-1.0)")
            .with_unit("1")
            .build();

        Self {
            connections_total,
            connection_errors_total,
            connection_duration,
            queries_total,
            query_duration,
            query_errors_total,
            query_rows_affected,
            messages_received_total,
            messages_sent_total,
            bytes_received_total,
            bytes_sent_total,
            subscription_updates_total,
            selective_update_columns_sent,
            selective_update_changed_ratio,
        }
    }

    // Connection metrics methods

    /// Record a new connection
    pub fn record_connection(&self) {
        self.connections_total.add(1, &[]);
    }

    /// Record a connection error
    #[allow(dead_code)]
    pub fn record_connection_error(&self, error_type: &str) {
        self.connection_errors_total.add(1, &[KeyValue::new("error_type", error_type.to_string())]);
    }

    /// Record connection duration
    pub fn record_connection_duration(&self, duration: Duration) {
        self.connection_duration.record(duration.as_secs_f64(), &[]);
    }

    // Query metrics methods

    /// Record a query execution
    pub fn record_query(
        &self,
        duration: Duration,
        stmt_type: &str,
        success: bool,
        rows_affected: u64,
    ) {
        let attributes = vec![
            KeyValue::new("statement_type", stmt_type.to_string()),
            KeyValue::new("success", success),
        ];

        self.query_duration.record(duration.as_secs_f64(), &attributes);
        self.queries_total.add(1, &attributes);

        if success {
            self.query_rows_affected.record(rows_affected, &attributes);
        }
    }

    /// Record a query error
    pub fn record_query_error(&self, error_type: &str, stmt_type: Option<&str>) {
        let mut attributes = vec![KeyValue::new("error_type", error_type.to_string())];

        if let Some(stmt) = stmt_type {
            attributes.push(KeyValue::new("statement_type", stmt.to_string()));
        }

        self.query_errors_total.add(1, &attributes);
    }

    // Protocol metrics methods

    /// Record a received message
    #[allow(dead_code)]
    pub fn record_message_received(&self, message_type: &str) {
        self.messages_received_total
            .add(1, &[KeyValue::new("message_type", message_type.to_string())]);
    }

    /// Record a sent message
    #[allow(dead_code)]
    pub fn record_message_sent(&self, message_type: &str) {
        self.messages_sent_total.add(1, &[KeyValue::new("message_type", message_type.to_string())]);
    }

    /// Record bytes received
    #[allow(dead_code)]
    pub fn record_bytes_received(&self, bytes: u64) {
        self.bytes_received_total.add(bytes, &[]);
    }

    /// Record bytes sent
    #[allow(dead_code)]
    pub fn record_bytes_sent(&self, bytes: u64) {
        self.bytes_sent_total.add(bytes, &[]);
    }

    // Subscription metrics methods

    /// Record a subscription update
    ///
    /// # Arguments
    /// * `update_type` - The type of update: "full", "delta_insert", "delta_update", "delta_delete", or "selective"
    /// * `row_count` - Number of rows in the update
    pub fn record_subscription_update(&self, update_type: &str, row_count: u64) {
        self.subscription_updates_total.add(
            1,
            &[
                KeyValue::new("type", update_type.to_string()),
                KeyValue::new("row_count", row_count as i64),
            ],
        );
    }

    /// Record selective update column statistics
    ///
    /// # Arguments
    /// * `columns_sent` - Number of columns included in the selective update
    /// * `total_columns` - Total number of columns in the full row
    pub fn record_selective_update_columns(&self, columns_sent: u64, total_columns: u64) {
        self.selective_update_columns_sent.record(columns_sent, &[]);

        if total_columns > 0 {
            let ratio = columns_sent as f64 / total_columns as f64;
            self.selective_update_changed_ratio.record(ratio, &[]);
        }
    }
}
