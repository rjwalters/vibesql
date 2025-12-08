use opentelemetry::metrics::{Counter, Gauge, Histogram, Meter};
use opentelemetry::KeyValue;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
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
    subscriptions_active: Gauge<u64>,
    subscriptions_active_count: Arc<AtomicU64>,
    subscriptions_selective_eligible: Gauge<u64>,
    subscriptions_selective_eligible_count: Arc<AtomicU64>,

    // Partial update efficiency metrics
    partial_update_fallbacks_total: Counter<u64>,
    partial_update_bytes_saved: Histogram<u64>,
    selective_update_bytes_saved_total: Counter<u64>,
    partial_update_efficiency: Gauge<f64>,
    partial_update_efficiency_numerator: Arc<AtomicU64>,
    partial_update_efficiency_denominator: Arc<AtomicU64>,

    // Selective update eligibility breakdown metrics
    pk_detection_total: Counter<u64>,
    selective_update_decisions_total: Counter<u64>,
    selective_update_column_ratio: Histogram<f64>,

    // Trackable counters for HTTP stats endpoint (OpenTelemetry metrics don't expose values)
    fallback_disabled_count: Arc<AtomicU64>,
    fallback_threshold_exceeded_count: Arc<AtomicU64>,
    fallback_row_count_mismatch_count: Arc<AtomicU64>,
    fallback_pk_mismatch_count: Arc<AtomicU64>,
    fallback_no_changes_count: Arc<AtomicU64>,
    total_bytes_saved_count: Arc<AtomicU64>,
    partial_updates_sent_count: Arc<AtomicU64>,
    full_updates_sent_count: Arc<AtomicU64>,
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

        let subscriptions_active = meter
            .u64_gauge("vibesql_subscriptions_active")
            .with_description("Total number of active subscriptions")
            .with_unit("{subscription}")
            .build();
        let subscriptions_active_count = Arc::new(AtomicU64::new(0));

        let subscriptions_selective_eligible = meter
            .u64_gauge("vibesql_subscriptions_selective_eligible")
            .with_description("Active subscriptions eligible for selective column updates")
            .with_unit("{subscription}")
            .build();
        let subscriptions_selective_eligible_count = Arc::new(AtomicU64::new(0));

        // Partial update efficiency metrics
        let partial_update_fallbacks_total = meter
            .u64_counter("vibesql_partial_update_fallbacks_total")
            .with_description("Times partial update was skipped, by reason (threshold_exceeded, disabled, row_count_mismatch, pk_mismatch, no_changes)")
            .with_unit("{fallback}")
            .build();

        let partial_update_bytes_saved = meter
            .u64_histogram("vibesql_partial_update_bytes_saved")
            .with_description("Estimated bytes saved per partial update compared to full row update")
            .with_unit("By")
            .build();

        let selective_update_bytes_saved_total = meter
            .u64_counter("vibesql_selective_update_bytes_saved_total")
            .with_description("Total bytes saved by using selective column updates instead of full row updates")
            .with_unit("By")
            .build();

        let partial_update_efficiency = meter
            .f64_gauge("vibesql_partial_update_efficiency")
            .with_description("Rolling average of column efficiency (columns_sent / total_columns) for partial updates")
            .with_unit("1")
            .build();
        let partial_update_efficiency_numerator = Arc::new(AtomicU64::new(0));
        let partial_update_efficiency_denominator = Arc::new(AtomicU64::new(0));

        // Selective update eligibility breakdown metrics
        let pk_detection_total = meter
            .u64_counter("vibesql_subscription_pk_detection_total")
            .with_description("Primary key detection outcomes during subscription registration")
            .with_unit("{detection}")
            .build();

        let selective_update_decisions_total = meter
            .u64_counter("vibesql_subscription_selective_update_decisions_total")
            .with_description("Selective update decisions (sent_partial, sent_full, skipped)")
            .with_unit("{decision}")
            .build();

        let selective_update_column_ratio = meter
            .f64_histogram("vibesql_subscription_selective_update_column_ratio")
            .with_description("Ratio of changed columns to total columns when evaluating selective updates (0.0-1.0)")
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
            subscriptions_active,
            subscriptions_active_count,
            subscriptions_selective_eligible,
            subscriptions_selective_eligible_count,
            partial_update_fallbacks_total,
            partial_update_bytes_saved,
            selective_update_bytes_saved_total,
            partial_update_efficiency,
            partial_update_efficiency_numerator,
            partial_update_efficiency_denominator,
            pk_detection_total,
            selective_update_decisions_total,
            selective_update_column_ratio,
            // Initialize trackable counters for HTTP stats endpoint
            fallback_disabled_count: Arc::new(AtomicU64::new(0)),
            fallback_threshold_exceeded_count: Arc::new(AtomicU64::new(0)),
            fallback_row_count_mismatch_count: Arc::new(AtomicU64::new(0)),
            fallback_pk_mismatch_count: Arc::new(AtomicU64::new(0)),
            fallback_no_changes_count: Arc::new(AtomicU64::new(0)),
            total_bytes_saved_count: Arc::new(AtomicU64::new(0)),
            partial_updates_sent_count: Arc::new(AtomicU64::new(0)),
            full_updates_sent_count: Arc::new(AtomicU64::new(0)),
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

    /// Increment the count of active subscriptions
    ///
    /// Called when a subscription is registered.
    pub fn increment_subscriptions_active(&self) {
        let new_value = self.subscriptions_active_count.fetch_add(1, Ordering::Relaxed) + 1;
        self.subscriptions_active.record(new_value, &[]);
    }

    /// Decrement the count of active subscriptions
    ///
    /// Called when a subscription is unregistered.
    pub fn decrement_subscriptions_active(&self) {
        let new_value = self.subscriptions_active_count.fetch_sub(1, Ordering::Relaxed) - 1;
        self.subscriptions_active.record(new_value, &[]);
    }

    /// Get the current count of active subscriptions
    pub fn subscriptions_active_count(&self) -> u64 {
        self.subscriptions_active_count.load(Ordering::Relaxed)
    }

    /// Increment the count of selective-eligible subscriptions
    ///
    /// Called when a subscription is registered with successfully detected PK columns.
    pub fn increment_selective_eligible(&self) {
        let new_value = self.subscriptions_selective_eligible_count.fetch_add(1, Ordering::Relaxed) + 1;
        self.subscriptions_selective_eligible.record(new_value, &[]);
    }

    /// Decrement the count of selective-eligible subscriptions
    ///
    /// Called when a selective-eligible subscription is unregistered.
    pub fn decrement_selective_eligible(&self) {
        let new_value = self.subscriptions_selective_eligible_count.fetch_sub(1, Ordering::Relaxed) - 1;
        self.subscriptions_selective_eligible.record(new_value, &[]);
    }

    /// Get the current count of selective-eligible subscriptions
    pub fn selective_eligible_count(&self) -> u64 {
        self.subscriptions_selective_eligible_count.load(Ordering::Relaxed)
    }

    // Partial update efficiency metrics methods

    /// Record a partial update fallback (when partial update was skipped)
    ///
    /// # Arguments
    /// * `reason` - The reason for fallback: "threshold_exceeded", "disabled",
    ///   "row_count_mismatch", "pk_mismatch", "no_changes"
    pub fn record_partial_update_fallback(&self, reason: &str) {
        self.partial_update_fallbacks_total
            .add(1, &[KeyValue::new("reason", reason.to_string())]);

        // Also increment trackable counter for HTTP stats endpoint
        match reason {
            "disabled" => {
                self.fallback_disabled_count.fetch_add(1, Ordering::Relaxed);
            }
            "threshold_exceeded" => {
                self.fallback_threshold_exceeded_count.fetch_add(1, Ordering::Relaxed);
            }
            "row_count_mismatch" => {
                self.fallback_row_count_mismatch_count.fetch_add(1, Ordering::Relaxed);
            }
            "pk_mismatch" => {
                self.fallback_pk_mismatch_count.fetch_add(1, Ordering::Relaxed);
            }
            "no_changes" => {
                self.fallback_no_changes_count.fetch_add(1, Ordering::Relaxed);
            }
            _ => {}
        }
    }

    /// Record bytes saved by a partial update compared to full row update
    ///
    /// # Arguments
    /// * `bytes_saved` - Estimated bytes saved (full_row_size - partial_update_size)
    pub fn record_partial_update_bytes_saved(&self, bytes_saved: u64) {
        // Record per-update histogram for distribution analysis
        self.partial_update_bytes_saved.record(bytes_saved, &[]);
        // Increment counter for cumulative tracking via OpenTelemetry
        self.selective_update_bytes_saved_total.add(bytes_saved, &[]);
        // Also track cumulative bytes saved for HTTP stats endpoint
        self.total_bytes_saved_count.fetch_add(bytes_saved, Ordering::Relaxed);
    }

    /// Record that a partial update was sent
    pub fn record_partial_update_sent(&self) {
        self.partial_updates_sent_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Record that a full update was sent (fallback from partial)
    pub fn record_full_update_sent(&self) {
        self.full_updates_sent_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Record selective update columns and update efficiency gauge
    ///
    /// This method records both the column-level statistics and updates
    /// the rolling efficiency gauge.
    ///
    /// # Arguments
    /// * `columns_sent` - Number of columns included in the selective update
    /// * `total_columns` - Total number of columns in the full row
    pub fn record_selective_update_columns(&self, columns_sent: u64, total_columns: u64) {
        self.selective_update_columns_sent.record(columns_sent, &[]);

        if total_columns > 0 {
            let ratio = columns_sent as f64 / total_columns as f64;
            self.selective_update_changed_ratio.record(ratio, &[]);

            // Update rolling efficiency (columns saved ratio = 1 - columns_sent/total)
            // We track cumulative columns_sent and total_columns to compute average
            self.partial_update_efficiency_numerator.fetch_add(columns_sent, Ordering::Relaxed);
            self.partial_update_efficiency_denominator.fetch_add(total_columns, Ordering::Relaxed);

            // Compute and record rolling average efficiency
            let total_sent = self.partial_update_efficiency_numerator.load(Ordering::Relaxed);
            let total_possible = self.partial_update_efficiency_denominator.load(Ordering::Relaxed);
            if total_possible > 0 {
                // Efficiency = ratio of columns NOT sent (i.e., bandwidth saved)
                let efficiency = 1.0 - (total_sent as f64 / total_possible as f64);
                self.partial_update_efficiency.record(efficiency, &[]);
            }
        }
    }

    /// Get the current partial update efficiency (columns saved ratio)
    pub fn partial_update_efficiency(&self) -> f64 {
        let total_sent = self.partial_update_efficiency_numerator.load(Ordering::Relaxed);
        let total_possible = self.partial_update_efficiency_denominator.load(Ordering::Relaxed);
        if total_possible > 0 {
            1.0 - (total_sent as f64 / total_possible as f64)
        } else {
            0.0
        }
    }

    // Selective update eligibility breakdown metrics methods

    /// Record a primary key detection outcome during subscription registration
    ///
    /// # Arguments
    /// * `result` - Detection result: "confident" or "not_confident"
    /// * `reason` - For not_confident results: "parse_error", "no_table", "no_pk",
    ///   "pk_not_in_result", "join_query", "subquery", "set_operation"
    pub fn record_pk_detection(&self, result: &str, reason: Option<&str>) {
        let mut attributes = vec![KeyValue::new("result", result.to_string())];
        if let Some(r) = reason {
            attributes.push(KeyValue::new("reason", r.to_string()));
        }
        self.pk_detection_total.add(1, &attributes);
    }

    /// Record a selective update decision
    ///
    /// # Arguments
    /// * `decision` - The decision made: "sent_partial", "sent_full", or "skipped"
    /// * `reason` - Optional reason for the decision (e.g., "threshold_exceeded", "disabled")
    pub fn record_selective_update_decision(&self, decision: &str, reason: Option<&str>) {
        let mut attributes = vec![KeyValue::new("decision", decision.to_string())];
        if let Some(r) = reason {
            attributes.push(KeyValue::new("reason", r.to_string()));
        }
        self.selective_update_decisions_total.add(1, &attributes);
    }

    /// Record the column ratio when evaluating selective updates
    ///
    /// This histogram tracks the ratio of changed columns to total columns,
    /// helping operators understand if threshold tuning would improve selective update rates.
    ///
    /// # Arguments
    /// * `changed_columns` - Number of columns that changed
    /// * `total_columns` - Total number of columns in the row
    pub fn record_selective_update_column_ratio(&self, changed_columns: usize, total_columns: usize) {
        if total_columns > 0 {
            let ratio = changed_columns as f64 / total_columns as f64;
            self.selective_update_column_ratio.record(ratio, &[]);
        }
    }

    /// Get subscription efficiency stats
    ///
    /// Returns efficiency metrics for subscription partial updates, including
    /// fallback reasons, bytes saved, and update counts.
    pub fn get_efficiency_stats(&self) -> crate::http::types::SubscriptionEfficiencyStats {
        crate::http::types::SubscriptionEfficiencyStats {
            partial_update_efficiency: self.partial_update_efficiency(),
            total_bytes_saved: self.total_bytes_saved_count.load(Ordering::Relaxed),
            fallbacks: crate::http::types::PartialUpdateFallbacks {
                disabled: self.fallback_disabled_count.load(Ordering::Relaxed),
                threshold_exceeded: self.fallback_threshold_exceeded_count.load(Ordering::Relaxed),
                row_count_mismatch: self.fallback_row_count_mismatch_count.load(Ordering::Relaxed),
                pk_mismatch: self.fallback_pk_mismatch_count.load(Ordering::Relaxed),
                no_changes: self.fallback_no_changes_count.load(Ordering::Relaxed),
            },
            partial_updates_sent: self.partial_updates_sent_count.load(Ordering::Relaxed),
            full_updates_sent: self.full_updates_sent_count.load(Ordering::Relaxed),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::global;

    fn create_test_metrics() -> ServerMetrics {
        let meter = global::meter("test_meter");
        ServerMetrics::new(&meter)
    }

    #[test]
    fn test_subscriptions_active_increment_decrement() {
        let metrics = create_test_metrics();

        // Initially zero
        assert_eq!(metrics.subscriptions_active_count(), 0);

        // Increment
        metrics.increment_subscriptions_active();
        assert_eq!(metrics.subscriptions_active_count(), 1);

        // Increment again
        metrics.increment_subscriptions_active();
        assert_eq!(metrics.subscriptions_active_count(), 2);

        // Decrement
        metrics.decrement_subscriptions_active();
        assert_eq!(metrics.subscriptions_active_count(), 1);

        // Decrement again
        metrics.decrement_subscriptions_active();
        assert_eq!(metrics.subscriptions_active_count(), 0);
    }

    #[test]
    fn test_selective_eligible_increment_decrement() {
        let metrics = create_test_metrics();

        // Initially zero
        assert_eq!(metrics.selective_eligible_count(), 0);

        // Increment
        metrics.increment_selective_eligible();
        assert_eq!(metrics.selective_eligible_count(), 1);

        // Increment again
        metrics.increment_selective_eligible();
        assert_eq!(metrics.selective_eligible_count(), 2);

        // Decrement
        metrics.decrement_selective_eligible();
        assert_eq!(metrics.selective_eligible_count(), 1);

        // Decrement again
        metrics.decrement_selective_eligible();
        assert_eq!(metrics.selective_eligible_count(), 0);
    }

    #[test]
    fn test_selective_eligible_clone() {
        let metrics1 = create_test_metrics();

        // Increment on first instance
        metrics1.increment_selective_eligible();
        assert_eq!(metrics1.selective_eligible_count(), 1);

        // Clone and check shared state
        let metrics2 = metrics1.clone();
        assert_eq!(metrics2.selective_eligible_count(), 1);

        // Increment on clone, check both see it
        metrics2.increment_selective_eligible();
        assert_eq!(metrics1.selective_eligible_count(), 2);
        assert_eq!(metrics2.selective_eligible_count(), 2);
    }

    #[test]
    fn test_partial_update_efficiency_calculation() {
        let metrics = create_test_metrics();

        // Initially zero efficiency (no data)
        assert_eq!(metrics.partial_update_efficiency(), 0.0);

        // Record some selective update columns
        // 3 columns sent out of 10 total = 70% efficiency (columns saved)
        metrics.record_selective_update_columns(3, 10);
        let efficiency = metrics.partial_update_efficiency();
        assert!((efficiency - 0.7).abs() < 0.001, "Expected ~0.7, got {}", efficiency);

        // Record more: 5 columns sent out of 10 total
        // Cumulative: 8 sent out of 20 = 60% efficiency
        metrics.record_selective_update_columns(5, 10);
        let efficiency = metrics.partial_update_efficiency();
        assert!((efficiency - 0.6).abs() < 0.001, "Expected ~0.6, got {}", efficiency);
    }

    #[test]
    fn test_partial_update_efficiency_shared_across_clones() {
        let metrics1 = create_test_metrics();

        // Record on first instance
        metrics1.record_selective_update_columns(2, 10);
        let eff1 = metrics1.partial_update_efficiency();

        // Clone and verify shared state
        let metrics2 = metrics1.clone();
        let eff2 = metrics2.partial_update_efficiency();
        assert!((eff1 - eff2).abs() < 0.001, "Clones should share efficiency state");

        // Record on clone, both should see updated value
        metrics2.record_selective_update_columns(2, 10);
        // Cumulative: 4 sent out of 20 = 80% efficiency
        let eff_after = metrics1.partial_update_efficiency();
        assert!((eff_after - 0.8).abs() < 0.001, "Expected ~0.8, got {}", eff_after);
    }

    #[test]
    fn test_partial_update_fallback_recording() {
        // This test verifies the method exists and can be called without panicking.
        // The actual counter value is tracked in OpenTelemetry and harder to verify.
        let metrics = create_test_metrics();

        // Should not panic when recording various fallback reasons
        metrics.record_partial_update_fallback("disabled");
        metrics.record_partial_update_fallback("threshold_exceeded");
        metrics.record_partial_update_fallback("row_count_mismatch");
        metrics.record_partial_update_fallback("pk_mismatch");
        metrics.record_partial_update_fallback("no_changes");
    }

    #[test]
    fn test_partial_update_bytes_saved_recording() {
        // This test verifies the method exists and can be called without panicking.
        // The method records to three places:
        // 1. partial_update_bytes_saved histogram (per-update distribution)
        // 2. selective_update_bytes_saved_total counter (cumulative OpenTelemetry metric)
        // 3. total_bytes_saved_count AtomicU64 (for HTTP stats endpoint)
        let metrics = create_test_metrics();

        // Should not panic when recording bytes saved
        metrics.record_partial_update_bytes_saved(100);
        metrics.record_partial_update_bytes_saved(500);
        metrics.record_partial_update_bytes_saved(0);
    }

    #[test]
    fn test_pk_detection_recording() {
        // This test verifies the method exists and can be called without panicking.
        let metrics = create_test_metrics();

        // Test confident detection
        metrics.record_pk_detection("confident", None);

        // Test not confident detections with various reasons
        metrics.record_pk_detection("not_confident", Some("parse_error"));
        metrics.record_pk_detection("not_confident", Some("no_table"));
        metrics.record_pk_detection("not_confident", Some("no_pk"));
        metrics.record_pk_detection("not_confident", Some("pk_not_in_result"));
        metrics.record_pk_detection("not_confident", Some("join_query"));
        metrics.record_pk_detection("not_confident", Some("subquery"));
        metrics.record_pk_detection("not_confident", Some("set_operation"));
    }

    #[test]
    fn test_selective_update_decision_recording() {
        // This test verifies the method exists and can be called without panicking.
        let metrics = create_test_metrics();

        // Test various decisions
        metrics.record_selective_update_decision("sent_partial", None);
        metrics.record_selective_update_decision("sent_full", Some("threshold_exceeded"));
        metrics.record_selective_update_decision("sent_full", Some("disabled"));
        metrics.record_selective_update_decision("skipped", Some("no_changes"));
        metrics.record_selective_update_decision("skipped", Some("row_count_mismatch"));
    }

    #[test]
    fn test_selective_update_column_ratio_recording() {
        // This test verifies the method exists and can be called without panicking.
        let metrics = create_test_metrics();

        // Test various ratios
        metrics.record_selective_update_column_ratio(1, 10); // 10%
        metrics.record_selective_update_column_ratio(5, 10); // 50%
        metrics.record_selective_update_column_ratio(10, 10); // 100%

        // Edge case: zero total columns should not record
        metrics.record_selective_update_column_ratio(0, 0);
    }

    #[test]
    fn test_get_efficiency_stats_tracks_fallbacks() {
        let metrics = create_test_metrics();

        // Record some fallbacks
        metrics.record_partial_update_fallback("disabled");
        metrics.record_partial_update_fallback("disabled");
        metrics.record_partial_update_fallback("threshold_exceeded");
        metrics.record_partial_update_fallback("row_count_mismatch");
        metrics.record_partial_update_fallback("pk_mismatch");
        metrics.record_partial_update_fallback("no_changes");
        metrics.record_partial_update_fallback("no_changes");
        metrics.record_partial_update_fallback("no_changes");

        let stats = metrics.get_efficiency_stats();
        assert_eq!(stats.fallbacks.disabled, 2);
        assert_eq!(stats.fallbacks.threshold_exceeded, 1);
        assert_eq!(stats.fallbacks.row_count_mismatch, 1);
        assert_eq!(stats.fallbacks.pk_mismatch, 1);
        assert_eq!(stats.fallbacks.no_changes, 3);
    }

    #[test]
    fn test_get_efficiency_stats_tracks_bytes_saved() {
        let metrics = create_test_metrics();

        // Record some bytes saved
        metrics.record_partial_update_bytes_saved(100);
        metrics.record_partial_update_bytes_saved(500);
        metrics.record_partial_update_bytes_saved(400);

        let stats = metrics.get_efficiency_stats();
        assert_eq!(stats.total_bytes_saved, 1000);
    }

    #[test]
    fn test_get_efficiency_stats_tracks_update_counts() {
        let metrics = create_test_metrics();

        // Record some updates
        metrics.record_partial_update_sent();
        metrics.record_partial_update_sent();
        metrics.record_partial_update_sent();
        metrics.record_full_update_sent();
        metrics.record_full_update_sent();

        let stats = metrics.get_efficiency_stats();
        assert_eq!(stats.partial_updates_sent, 3);
        assert_eq!(stats.full_updates_sent, 2);
    }

    #[test]
    fn test_get_efficiency_stats_includes_efficiency() {
        let metrics = create_test_metrics();

        // Record some selective updates: 3 columns sent out of 10 = 70% efficiency
        metrics.record_selective_update_columns(3, 10);

        let stats = metrics.get_efficiency_stats();
        assert!((stats.partial_update_efficiency - 0.7).abs() < 0.001);
    }
}
