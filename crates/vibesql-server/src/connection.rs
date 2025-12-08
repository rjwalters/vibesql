use crate::auth::PasswordStore;
use crate::config::Config;
use crate::observability::ObservabilityProvider;
use crate::protocol::{
    BackendMessage, FieldDescription, FrontendMessage, SelectiveUpdatesConfig, SubscriptionUpdateType, TransactionStatus,
};
use crate::registry::DatabaseRegistry;
use crate::session::{ExecutionResult, Session};
use crate::protocol::PartialRowUpdate;
use crate::subscription::{
    compute_delta_with_pk, create_partial_row_update, detect_pk_columns_from_stmt,
    extract_table_refs, filter::SubscriptionFilter, hash_rows, SelectiveColumnConfig,
    SubscriptionId, SubscriptionManager, SubscriptionUpdate,
};
use crate::Row;
use anyhow::Result;
use bytes::BytesMut;
use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::TcpStream;
use tokio::sync::broadcast;
use tracing::{debug, error, info, warn};
use vibesql_executor::cache::table_extractor;

/// Notification sent when a mutation affects tables
/// This is broadcast to all connections so they can notify their subscriptions
#[derive(Debug, Clone)]
pub struct TableMutationNotification {
    /// Tables that were affected by the mutation
    pub affected_tables: HashSet<String>,
}

/// Connection handler for a single client
pub struct ConnectionHandler {
    /// Read half of the TCP stream (split for async select! usage)
    read_half: OwnedReadHalf,
    /// Write half of the TCP stream (split for async select! usage)
    write_half: OwnedWriteHalf,
    peer_addr: SocketAddr,
    config: Arc<Config>,
    observability: Arc<ObservabilityProvider>,
    password_store: Option<Arc<PasswordStore>>,
    read_buf: BytesMut,
    write_buf: BytesMut,
    session: Option<Session>,
    connection_start: Instant,
    active_connections: Arc<AtomicUsize>,
    /// Database registry for shared database instances across connections
    database_registry: DatabaseRegistry,
    /// Unique identifier for this connection (for subscription tracking)
    connection_id: String,
    /// Global subscription manager for processing storage change events and tracking subscriptions
    subscription_manager: Arc<SubscriptionManager>,
    /// Broadcast sender for notifying other connections about mutations
    mutation_broadcast_tx: broadcast::Sender<TableMutationNotification>,
    /// Broadcast receiver for receiving mutation notifications from other connections
    mutation_broadcast_rx: broadcast::Receiver<TableMutationNotification>,
}

/// Result of handling a client message
enum ClientMessageResult {
    /// Continue processing messages
    Continue,
    /// Client requested termination
    Terminate,
}

impl ConnectionHandler {
    /// Create a new connection handler
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        stream: TcpStream,
        peer_addr: SocketAddr,
        config: Arc<Config>,
        observability: Arc<ObservabilityProvider>,
        password_store: Option<Arc<PasswordStore>>,
        active_connections: Arc<AtomicUsize>,
        database_registry: DatabaseRegistry,
        subscription_manager: Arc<SubscriptionManager>,
        mutation_broadcast_tx: broadcast::Sender<TableMutationNotification>,
    ) -> Self {
        // Split the TCP stream for async select! usage
        // This allows us to wait on both client messages and broadcast notifications simultaneously
        let (read_half, write_half) = stream.into_split();

        // Subscribe to the broadcast channel to receive notifications from other connections
        let mutation_broadcast_rx = mutation_broadcast_tx.subscribe();

        // Generate a unique connection ID for subscription tracking
        let connection_id = uuid::Uuid::new_v4().to_string();

        Self {
            read_half,
            write_half,
            peer_addr,
            config,
            observability,
            password_store,
            read_buf: BytesMut::with_capacity(8192),
            write_buf: BytesMut::with_capacity(8192),
            session: None,
            connection_start: Instant::now(),
            active_connections,
            database_registry,
            connection_id,
            subscription_manager,
            mutation_broadcast_tx,
            mutation_broadcast_rx,
        }
    }

    /// Handle the connection
    pub async fn handle(&mut self) -> Result<()> {
        // Perform startup handshake
        self.startup_handshake().await?;

        // Process queries
        self.process_queries().await?;

        Ok(())
    }

    /// Perform the PostgreSQL startup handshake
    async fn startup_handshake(&mut self) -> Result<()> {
        debug!("Starting handshake with {}", self.peer_addr);

        // Read startup message
        self.read_message().await?;

        let startup_msg = FrontendMessage::decode_startup(&mut self.read_buf)?;

        match startup_msg {
            Some(FrontendMessage::SSLRequest) => {
                debug!("Received SSL request");
                // We don't support SSL yet, send 'N'
                self.write_half.write_u8(b'N').await?;
                self.write_half.flush().await?;

                // Read actual startup message after SSL rejection
                self.read_buf.clear();
                self.read_message().await?;

                let startup_msg = FrontendMessage::decode_startup(&mut self.read_buf)?;
                self.handle_startup(startup_msg).await?;
            }

            Some(msg) => {
                self.handle_startup(Some(msg)).await?;
            }

            None => {
                return Err(anyhow::anyhow!("No startup message received"));
            }
        }

        Ok(())
    }

    /// Handle startup message and authentication
    async fn handle_startup(&mut self, msg: Option<FrontendMessage>) -> Result<()> {
        match msg {
            Some(FrontendMessage::Startup { protocol_version, params }) => {
                debug!("Startup: version={}, params={:?}", protocol_version, params);

                let user = params.get("user").cloned().unwrap_or_else(|| "postgres".to_string());
                let database = params.get("database").cloned().unwrap_or_else(|| user.clone());

                // Perform authentication
                self.authenticate(&user).await?;

                // Get or create shared database from registry
                let shared_db = self.database_registry.get_or_create(&database).await;

                // Create session with shared database
                self.session = Some(Session::new(database.clone(), user.clone(), shared_db));

                info!("User '{}' connected to database '{}'", user, database);

                // Send startup complete messages
                self.send_parameter_status("server_version", "14.0 (VibeSQL)").await?;
                self.send_parameter_status("server_encoding", "UTF8").await?;
                self.send_parameter_status("client_encoding", "UTF8").await?;
                self.send_parameter_status("DateStyle", "ISO, MDY").await?;
                self.send_parameter_status("TimeZone", "UTC").await?;

                // Send backend key data (for cancel requests)
                self.send_backend_key_data().await?;

                // Send ready for query
                self.send_ready_for_query(TransactionStatus::Idle).await?;

                Ok(())
            }

            _ => Err(anyhow::anyhow!("Invalid startup message")),
        }
    }

    /// Authenticate the user
    async fn authenticate(&mut self, user: &str) -> Result<()> {
        match self.config.auth.method.as_str() {
            "trust" => {
                // Trust authentication - no password required
                debug!("Using trust authentication for user '{}'", user);
                self.send_authentication_ok().await?;
                Ok(())
            }

            "password" => {
                // Cleartext password authentication
                debug!("Requesting cleartext password for user '{}'", user);
                self.send_cleartext_password_request().await?;

                // Read password response
                self.read_message().await?;
                let msg = FrontendMessage::decode(&mut self.read_buf)?;

                match msg {
                    Some(FrontendMessage::Password { password }) => {
                        debug!("Received password from user '{}'", user);

                        if let Some(ref store) = self.password_store {
                            if store.verify_cleartext(user, &password) {
                                info!("User '{}' authenticated successfully", user);
                                self.send_authentication_ok().await?;
                                Ok(())
                            } else {
                                error!("Authentication failed for user '{}'", user);
                                Err(anyhow::anyhow!("Authentication failed"))
                            }
                        } else {
                            error!("No password store configured");
                            Err(anyhow::anyhow!("Authentication not configured"))
                        }
                    }
                    _ => {
                        error!("Expected password message, got: {:?}", msg);
                        Err(anyhow::anyhow!("Expected password message"))
                    }
                }
            }

            "md5" => {
                // MD5 password authentication
                debug!("Requesting MD5 password for user '{}'", user);

                // Generate random salt
                use rand::Rng;
                let salt: [u8; 4] = rand::rng().random();

                self.send_md5_password_request(&salt).await?;

                // Read password response
                self.read_message().await?;
                let msg = FrontendMessage::decode(&mut self.read_buf)?;

                match msg {
                    Some(FrontendMessage::Password { password }) => {
                        debug!("Received MD5 password response from user '{}'", user);

                        if let Some(ref store) = self.password_store {
                            if store.verify_md5(user, &password, &salt) {
                                info!("User '{}' authenticated successfully (MD5)", user);
                                self.send_authentication_ok().await?;
                                Ok(())
                            } else {
                                error!("MD5 authentication failed for user '{}'", user);
                                Err(anyhow::anyhow!("Authentication failed"))
                            }
                        } else {
                            error!("No password store configured");
                            Err(anyhow::anyhow!("Authentication not configured"))
                        }
                    }
                    _ => {
                        error!("Expected password message, got: {:?}", msg);
                        Err(anyhow::anyhow!("Expected password message"))
                    }
                }
            }

            "scram-sha-256" => {
                // SCRAM-SHA-256 not yet implemented
                error!("SCRAM-SHA-256 authentication not yet implemented");
                Err(anyhow::anyhow!("SCRAM-SHA-256 not implemented"))
            }

            _ => {
                error!("Unsupported authentication method: {}", self.config.auth.method);
                Err(anyhow::anyhow!("Unsupported authentication method"))
            }
        }
    }

    /// Process queries from the client
    ///
    /// This method handles both:
    /// 1. Client messages from the TCP stream
    /// 2. Broadcast notifications from other connections about table mutations
    ///
    /// This enables cross-connection subscription notifications: when connection A
    /// mutates a table, connection B's subscriptions on that table are notified.
    ///
    /// Uses `tokio::select!` to wait on both sources simultaneously with near-zero
    /// latency, avoiding the previous 100ms polling approach.
    async fn process_queries(&mut self) -> Result<()> {
        loop {
            // First, process any complete messages already in the buffer
            // This handles cases where multiple messages arrived in a single TCP read
            while let Some(msg) = FrontendMessage::decode(&mut self.read_buf)? {
                match self.handle_client_message(msg).await? {
                    ClientMessageResult::Continue => {}
                    ClientMessageResult::Terminate => {
                        let (total, selective_eligible) = self.subscription_manager
                            .unsubscribe_all_for_connection(&self.connection_id);
                        if let Some(metrics) = self.observability.metrics() {
                            for _ in 0..total {
                                metrics.decrement_subscriptions_active();
                            }
                            for _ in 0..selective_eligible {
                                metrics.decrement_selective_eligible();
                            }
                        }
                        return Ok(());
                    }
                }
            }

            // No complete message in buffer - wait for either:
            // 1. More data from the client TCP stream
            // 2. Broadcast notifications from other connections
            //
            // Using select! provides near-zero latency for cross-connection notifications
            // compared to the previous 100ms timeout polling approach.
            tokio::select! {
                biased;  // Prioritize broadcast notifications for lower latency

                // Check for cross-connection mutation notifications
                notification = self.mutation_broadcast_rx.recv() => {
                    match notification {
                        Ok(n) => {
                            if self.subscription_manager.connection_subscription_count(&self.connection_id) > 0 {
                                self.handle_cross_connection_notification(&n.affected_tables).await;
                            }
                        }
                        Err(broadcast::error::RecvError::Lagged(n)) => {
                            debug!("Missed {} broadcast notifications (lagged)", n);
                        }
                        Err(broadcast::error::RecvError::Closed) => {
                            warn!("Mutation broadcast channel closed");
                        }
                    }
                }

                // Read more data from the client
                read_result = self.read_half.read_buf(&mut self.read_buf) => {
                    match read_result {
                        Ok(0) => {
                            // Connection closed by client
                            debug!("Connection closed by client");
                            break;
                        }
                        Ok(_) => {
                            // Data received - loop back to decode and process messages
                        }
                        Err(e) => {
                            return Err(e.into());
                        }
                    }
                }
            }
        }

        // Clean up subscriptions when connection closes
        let (total, selective_eligible) = self.subscription_manager
            .unsubscribe_all_for_connection(&self.connection_id);
        if let Some(metrics) = self.observability.metrics() {
            for _ in 0..total {
                metrics.decrement_subscriptions_active();
            }
            for _ in 0..selective_eligible {
                metrics.decrement_selective_eligible();
            }
        }

        Ok(())
    }

    /// Handle a single client message
    async fn handle_client_message(&mut self, msg: FrontendMessage) -> Result<ClientMessageResult> {
        match msg {
            FrontendMessage::Query { query } => {
                debug!("Query: {}", query);
                self.execute_query(&query).await?;
                Ok(ClientMessageResult::Continue)
            }

            FrontendMessage::Subscribe { query, params, filter, selective_updates_config } => {
                debug!("Subscribe: {} (filter: {:?}, selective_config: {:?})", query, filter, selective_updates_config);
                self.handle_subscribe(&query, params, filter, selective_updates_config).await?;
                Ok(ClientMessageResult::Continue)
            }

            FrontendMessage::Unsubscribe { subscription_id } => {
                debug!("Unsubscribe: {:?}", subscription_id);
                let was_selective_eligible = self.subscription_manager.unsubscribe_by_wire_id(&subscription_id);
                if was_selective_eligible {
                    if let Some(metrics) = self.observability.metrics() {
                        metrics.decrement_selective_eligible();
                    }
                }
                // No response needed per protocol spec
                Ok(ClientMessageResult::Continue)
            }

            FrontendMessage::Terminate => {
                debug!("Client requested termination");
                Ok(ClientMessageResult::Terminate)
            }

            msg => {
                warn!("Unexpected message: {:?}", msg);
                Ok(ClientMessageResult::Continue)
            }
        }
    }

    /// Handle a cross-connection notification about table mutations
    ///
    /// When another connection mutates tables, this method is called to
    /// check if any of our subscriptions are affected and send updates.
    /// This method supports delta updates to reduce network bandwidth when
    /// only a small portion of the result set has changed.
    /// Supports optional filtering expressions to send only matching rows.
    #[allow(clippy::type_complexity)]
    async fn handle_cross_connection_notification(&mut self, affected_tables: &HashSet<String>) {
        // Collect subscriptions for THIS connection that need updating
        let subscriptions_to_update: Vec<([u8; 16], String, u64, Option<Vec<Row>>, Option<String>)> =
            affected_tables
                .iter()
                .flat_map(|table| {
                    self.subscription_manager
                        .get_affected_subscriptions_for_connection(table, &self.connection_id)
                })
                .collect();

        if subscriptions_to_update.is_empty() {
            return;
        }

        // De-duplicate subscriptions (a subscription may depend on multiple affected tables)
        let mut seen = std::collections::HashSet::new();
        let unique_subscriptions: Vec<_> = subscriptions_to_update
            .into_iter()
            .filter(|(id, _, _, _, _)| seen.insert(*id))
            .collect();

        debug!(
            "Cross-connection notification: notifying {} subscriptions for tables: {:?}",
            unique_subscriptions.len(),
            affected_tables
        );

        // Re-execute each subscription query and send updates
        for (subscription_id, query, last_hash, last_result, filter) in unique_subscriptions {
            if let Some(session) = &mut self.session {
                match session.execute(&query).await {
                    Ok(ExecutionResult::Select { rows, columns }) => {
                        // Build filter if present
                        let filter_opt = filter.as_ref().and_then(|f| {
                            let col_names: Vec<String> =
                                columns.iter().map(|c| c.name.clone()).collect();
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
                                "Cross-connection update: results unchanged for subscription {:?}",
                                subscription_id
                            );
                            continue;
                        }

                        // Determine whether to send delta or full update
                        if let Some(ref old_rows) = last_result {
                            // First, try selective column updates (0xF7) using effective config
                            if self
                                .try_send_selective_updates(
                                    &subscription_id,
                                    old_rows,
                                    &new_rows,
                                )
                                .await
                            {
                                // Selective updates sent successfully - update stored result
                                self.subscription_manager.update_result_by_wire_id(
                                    &subscription_id,
                                    new_hash,
                                    new_rows,
                                );
                                continue;
                            }

                            // Fall back to delta updates using PK columns
                            let pk_columns = self
                                .subscription_manager
                                .get_pk_columns_by_wire_id(&subscription_id);
                            if let Some(delta) = compute_delta_with_pk(
                                SubscriptionId::default(),
                                old_rows,
                                &new_rows,
                                &pk_columns,
                            ) {
                                // Send delta updates
                                if let Err(e) =
                                    self.send_delta_updates(&subscription_id, &delta).await
                                {
                                    warn!(
                                        "Failed to send cross-connection delta update: {}",
                                        e
                                    );
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
                                        "Cross-connection delta update sent: {} inserts, {} updates, {} deletes for subscription {:?}",
                                        inserts.len(),
                                        updates.len(),
                                        deletes.len(),
                                        subscription_id
                                    );
                                }
                            } else {
                                // No delta computed (shouldn't happen if hash changed)
                                // Fall back to full update
                                let wire_rows = Self::rows_to_wire_format(&new_rows);
                                if let Err(e) = self
                                    .send_subscription_data(
                                        &subscription_id,
                                        SubscriptionUpdateType::Full,
                                        wire_rows,
                                    )
                                    .await
                                {
                                    warn!("Failed to send cross-connection full update: {}", e);
                                }
                            }
                        } else {
                            // No previous results - send full update
                            debug!(
                                "Cross-connection update: no previous result, sending full update for subscription {:?}",
                                subscription_id
                            );
                            let wire_rows = Self::rows_to_wire_format(&new_rows);
                            if let Err(e) = self
                                .send_subscription_data(
                                    &subscription_id,
                                    SubscriptionUpdateType::Full,
                                    wire_rows,
                                )
                                .await
                            {
                                warn!("Failed to send cross-connection full update: {}", e);
                            }
                        }

                        // Update stored result for next delta computation
                        self.subscription_manager.update_result_by_wire_id(
                            &subscription_id,
                            new_hash,
                            new_rows,
                        );
                    }
                    Ok(_) => {
                        // Non-SELECT result - shouldn't happen for a subscription query
                        warn!("Subscription query returned non-SELECT result");
                    }
                    Err(e) => {
                        // Query failed - send error to subscriber
                        if let Err(send_err) = self
                            .send_subscription_error(&subscription_id, &format!("Query error: {}", e))
                            .await
                        {
                            warn!("Failed to send subscription error: {}", send_err);
                        }
                    }
                }
            }
        }
    }

    /// Send delta updates to a subscription
    ///
    /// The wire protocol sends separate messages for inserts, updates, and deletes.
    /// For UPDATE operations, we use PartialRowUpdate to send only changed columns
    /// plus PK columns, reducing wire traffic for wide tables.
    async fn send_delta_updates(
        &mut self,
        subscription_id: &[u8; 16],
        delta: &SubscriptionUpdate,
    ) -> Result<()> {
        if let SubscriptionUpdate::Delta { inserts, updates, deletes, .. } = delta {
            // Send deletes first (so clients can remove before adding)
            if !deletes.is_empty() {
                let wire_rows = Self::rows_to_wire_format(deletes);
                self.send_subscription_data(
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
                let config = self.subscription_manager.get_effective_selective_config_by_wire_id(
                    subscription_id,
                    &self.config.subscriptions.selective_updates,
                );
                let pk_columns = config.pk_columns.clone();

                // Separate updates into partial and full based on threshold
                let mut partial_updates: Vec<PartialRowUpdate> = Vec::new();
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
                    if let Some(partial) =
                        create_partial_row_update(&old_wire, &new_wire, &pk_columns, &config)
                    {
                        partial_updates.push(partial);
                    } else {
                        // Fall back to full row update
                        full_updates.push(new_wire);
                    }
                }

                // Send partial updates via SubscriptionPartialData (0xF7)
                if !partial_updates.is_empty() {
                    self.send_subscription_partial_data(subscription_id, partial_updates).await?;
                }

                // Send any full updates via regular DeltaUpdate
                if !full_updates.is_empty() {
                    self.send_subscription_data(
                        subscription_id,
                        SubscriptionUpdateType::DeltaUpdate,
                        full_updates,
                    )
                    .await?;
                }
            }

            // Send inserts last
            if !inserts.is_empty() {
                let wire_rows = Self::rows_to_wire_format(inserts);
                self.send_subscription_data(
                    subscription_id,
                    SubscriptionUpdateType::DeltaInsert,
                    wire_rows,
                )
                .await?;
            }
        }
        Ok(())
    }

    /// Convert rows to wire format for sending over the protocol
    fn rows_to_wire_format(rows: &[Row]) -> Vec<Vec<Option<Vec<u8>>>> {
        rows.iter()
            .map(|row| {
                row.values.iter().map(|v| Some(v.to_string().as_bytes().to_vec())).collect()
            })
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
    async fn try_send_selective_updates(
        &mut self,
        subscription_id: &[u8; 16],
        old_rows: &[Row],
        new_rows: &[Row],
    ) -> bool {
        // Get effective selective config (uses per-subscription override if set)
        let selective_config = self.subscription_manager.get_effective_selective_config_by_wire_id(
            subscription_id,
            &self.config.subscriptions.selective_updates,
        );

        // Check if selective updates are enabled in effective config
        if !selective_config.enabled {
            debug!(
                "Selective update skipped for subscription {:?}: disabled in config",
                subscription_id
            );
            if let Some(metrics) = self.observability.metrics() {
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
            if let Some(metrics) = self.observability.metrics() {
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
        let old_wire: Vec<Vec<Option<Vec<u8>>>> = Self::rows_to_wire_format(old_rows);
        let new_wire: Vec<Vec<Option<Vec<u8>>>> = Self::rows_to_wire_format(new_rows);

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
                    let changed_count = old_row
                        .iter()
                        .zip(new_row.iter())
                        .filter(|(o, n)| o != n)
                        .count();
                    if let Some(metrics) = self.observability.metrics() {
                        metrics.record_selective_update_column_ratio(changed_count, new_row.len());
                    }
                    partial_updates.push(partial);
                } else {
                    // Check if this was due to threshold exceeded (too many columns changed)
                    let changed_count = old_row
                        .iter()
                        .zip(new_row.iter())
                        .filter(|(o, n)| o != n)
                        .count();
                    if changed_count > 0 {
                        let ratio = changed_count as f64 / new_row.len() as f64;
                        // Record column ratio for analysis (helps tuning threshold)
                        if let Some(metrics) = self.observability.metrics() {
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
                if let Some(metrics) = self.observability.metrics() {
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
                threshold_exceeded_count,
                subscription_id
            );
            if let Some(metrics) = self.observability.metrics() {
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
            if let Some(metrics) = self.observability.metrics() {
                metrics.record_partial_update_fallback("no_changes");
                metrics.record_selective_update_decision("skipped", Some("no_changes"));
            }
            return false;
        }

        // Calculate and record metrics before sending
        if let Some(metrics) = self.observability.metrics() {
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
        if let Err(e) = self.send_subscription_partial_data(subscription_id, partial_updates).await
        {
            warn!("Failed to send selective updates: {}", e);
            return false;
        }

        // Record successful partial update sent
        if let Some(metrics) = self.observability.metrics() {
            metrics.record_partial_update_sent();
        }

        debug!(
            "Sent selective column update (0xF7) for subscription {:?}",
            subscription_id
        );
        true
    }

    /// Execute a SQL query
    async fn execute_query(&mut self, query: &str) -> Result<()> {
        let session = self.session.as_mut().ok_or_else(|| anyhow::anyhow!("No session"))?;

        // Handle empty query
        if query.trim().is_empty() {
            self.send_empty_query_response().await?;
            let txn_status = self.get_transaction_status();
            self.send_ready_for_query(txn_status).await?;
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
                if let Some(metrics) = self.observability.metrics() {
                    metrics.record_query(query_duration, stmt_type, true, rows_affected);
                }

                // Check if this was a mutation that might affect subscriptions
                let is_mutation = matches!(
                    &result,
                    ExecutionResult::Insert { .. }
                        | ExecutionResult::Update { .. }
                        | ExecutionResult::Delete { .. }
                );

                self.send_query_result(result).await?;

                // Notify affected subscriptions after mutations
                if is_mutation {
                    // First, notify local subscriptions (same connection)
                    self.notify_affected_subscriptions(query).await;

                    // Then, broadcast to other connections for cross-connection notifications
                    self.broadcast_mutation(query);
                }

                // Return appropriate transaction status
                let txn_status = self.get_transaction_status();
                self.send_ready_for_query(txn_status).await?;
                Ok(())
            }

            Err(e) => {
                error!("Query error: {}", e);

                // Record error metric
                if let Some(metrics) = self.observability.metrics() {
                    metrics.record_query_error("execution_error", None);
                }

                self.send_error_response(&format!("{}", e)).await?;

                // If in transaction and error occurred, report failed transaction state
                let txn_status = if self.session.as_ref().is_some_and(|s| s.in_transaction()) {
                    TransactionStatus::FailedTransaction
                } else {
                    TransactionStatus::Idle
                };
                self.send_ready_for_query(txn_status).await?;
                Ok(())
            }
        }
    }

    /// Get the current transaction status for the session
    fn get_transaction_status(&self) -> TransactionStatus {
        if self.session.as_ref().is_some_and(|s| s.in_transaction()) {
            TransactionStatus::InTransaction
        } else {
            TransactionStatus::Idle
        }
    }

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
    async fn handle_subscribe(
        &mut self,
        query: &str,
        _params: Vec<Option<Vec<u8>>>,
        filter: Option<String>,
        selective_updates_config: Option<SelectiveUpdatesConfig>,
    ) -> Result<()> {
        let session = self.session.as_mut().ok_or_else(|| anyhow::anyhow!("No session"))?;

        // Parse the query to extract table dependencies
        let parsed = match vibesql_parser::Parser::parse_sql(query) {
            Ok(stmt) => stmt,
            Err(e) => {
                // Send subscription error with a dummy subscription ID (query failed before registration)
                let error_id = [0u8; 16];
                self.send_subscription_error(&error_id, &format!("Parse error: {}", e)).await?;
                return Ok(());
            }
        };

        // Validate the filter expression if provided
        if let Some(ref filter_str) = filter {
            if let Err(e) = vibesql_parser::arena_parser::parse_expression_to_owned(filter_str) {
                let error_id = [0u8; 16];
                self.send_subscription_error(&error_id, &format!("Filter parse error: {}", e))
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
                pk_detection.pk_column_indices,
                pk_detection.tables
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
        if let Some(metrics) = self.observability.metrics() {
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
        if let Err(e) = self.subscription_manager.subscribe_for_connection(
            query.to_string(),
            notify_tx,
            self.connection_id.clone(),
            wire_subscription_id,
            table_dependencies.clone(),
            filter.clone(),
        ) {
            // Send subscription error with a dummy subscription ID (subscription failed before registration)
            let error_id = [0u8; 16];
            self.send_subscription_error(&error_id, &format!("{}", e)).await?;
            return Ok(());
        }

        // Track the new subscription in metrics
        if let Some(metrics) = self.observability.metrics() {
            metrics.increment_subscriptions_active();
        }

        // Store detected PK columns in the subscription for selective updates
        // Track selective-eligible subscriptions in metrics
        let newly_eligible = self.subscription_manager.update_pk_columns_with_eligibility_by_wire_id(
            &wire_subscription_id,
            pk_detection.pk_column_indices.clone(),
            pk_detection.confident,
        );
        if newly_eligible {
            if let Some(metrics) = self.observability.metrics() {
                metrics.increment_selective_eligible();
            }
        }

        // Apply per-subscription selective updates override if provided
        if let Some(wire_config) = selective_updates_config {
            // Convert wire protocol config to SelectiveColumnConfig
            // Merge with server defaults for any unspecified fields
            let server_config = &self.config.subscriptions.selective_updates;

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

            self.subscription_manager.set_selective_updates_override_by_wire_id(
                &wire_subscription_id,
                override_config,
            );
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
                self.subscription_manager.update_result_by_wire_id(
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
                self.send_subscription_data(
                    &wire_subscription_id,
                    SubscriptionUpdateType::Full,
                    wire_rows,
                )
                .await?;
            }
            Ok(_) => {
                // Non-SELECT query - send error and remove subscription
                let was_selective_eligible = self.subscription_manager.unsubscribe_by_wire_id(&wire_subscription_id);
                if was_selective_eligible {
                    if let Some(metrics) = self.observability.metrics() {
                        metrics.decrement_selective_eligible();
                    }
                }
                self.send_subscription_error(
                    &wire_subscription_id,
                    "Only SELECT queries can be subscribed to",
                )
                .await?;
            }
            Err(e) => {
                // Query execution failed - remove subscription and send error
                let was_selective_eligible = self.subscription_manager.unsubscribe_by_wire_id(&wire_subscription_id);
                if was_selective_eligible {
                    if let Some(metrics) = self.observability.metrics() {
                        metrics.decrement_selective_eligible();
                    }
                }
                self.send_subscription_error(&wire_subscription_id, &format!("Execution error: {}", e))
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
    #[allow(clippy::type_complexity)]
    async fn notify_affected_subscriptions(&mut self, mutation_query: &str) {
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
                    self.subscription_manager
                        .get_affected_subscriptions_for_connection(table, &self.connection_id)
                })
                .collect();

        if subscriptions_to_update.is_empty() {
            return;
        }

        // De-duplicate subscriptions (a subscription may depend on multiple affected tables)
        let mut seen = std::collections::HashSet::new();
        let unique_subscriptions: Vec<_> = subscriptions_to_update
            .into_iter()
            .filter(|(id, _, _, _, _)| seen.insert(*id))
            .collect();

        debug!(
            "Notifying {} subscriptions after mutation affecting tables: {:?}",
            unique_subscriptions.len(),
            affected_tables
        );

        // Re-execute each subscription query and send updates
        for (subscription_id, query, last_hash, last_result, filter) in unique_subscriptions {
            if let Some(session) = &mut self.session {
                match session.execute(&query).await {
                    Ok(ExecutionResult::Select { rows, columns }) => {
                        // Build filter if present
                        let filter_opt = filter.as_ref().and_then(|f| {
                            let col_names: Vec<String> =
                                columns.iter().map(|c| c.name.clone()).collect();
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
                                "Same-connection update: results unchanged for subscription {:?}",
                                subscription_id
                            );
                            continue;
                        }

                        // Determine whether to send delta or full update
                        if let Some(ref old_rows) = last_result {
                            // First, try selective column updates (0xF7) using effective config
                            if self
                                .try_send_selective_updates(
                                    &subscription_id,
                                    old_rows,
                                    &new_rows,
                                )
                                .await
                            {
                                // Selective updates sent successfully - update stored result
                                self.subscription_manager.update_result_by_wire_id(
                                    &subscription_id,
                                    new_hash,
                                    new_rows,
                                );
                                continue;
                            }

                            // Fall back to delta updates using PK columns
                            let pk_columns = self
                                .subscription_manager
                                .get_pk_columns_by_wire_id(&subscription_id);
                            if let Some(delta) = compute_delta_with_pk(
                                SubscriptionId::default(),
                                old_rows,
                                &new_rows,
                                &pk_columns,
                            ) {
                                // Send delta updates
                                if let Err(e) =
                                    self.send_delta_updates(&subscription_id, &delta).await
                                {
                                    warn!("Failed to send same-connection delta update: {}", e);
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
                                        "Same-connection delta update sent: {} inserts, {} updates, {} deletes for subscription {:?}",
                                        inserts.len(),
                                        updates.len(),
                                        deletes.len(),
                                        subscription_id
                                    );
                                }
                            } else {
                                // No delta computed - send full update
                                let wire_rows = Self::rows_to_wire_format(&new_rows);
                                if let Err(e) = self
                                    .send_subscription_data(
                                        &subscription_id,
                                        SubscriptionUpdateType::Full,
                                        wire_rows,
                                    )
                                    .await
                                {
                                    warn!("Failed to send same-connection full update: {}", e);
                                }
                            }
                        } else {
                            // No previous results - send full update
                            let wire_rows = Self::rows_to_wire_format(&new_rows);
                            if let Err(e) = self
                                .send_subscription_data(
                                    &subscription_id,
                                    SubscriptionUpdateType::Full,
                                    wire_rows,
                                )
                                .await
                            {
                                warn!("Failed to send same-connection full update: {}", e);
                            }
                        }

                        // Update stored result for next delta computation
                        self.subscription_manager.update_result_by_wire_id(
                            &subscription_id,
                            new_hash,
                            new_rows,
                        );
                    }
                    Ok(_) => {
                        // Non-SELECT result - shouldn't happen for a subscription query
                        warn!("Subscription query returned non-SELECT result");
                    }
                    Err(e) => {
                        // Query failed - send error to subscriber
                        if let Err(send_err) = self
                            .send_subscription_error(&subscription_id, &format!("Query error: {}", e))
                            .await
                        {
                            warn!("Failed to send subscription error: {}", send_err);
                        }
                    }
                }
            }
        }
    }

    /// Broadcast a mutation event to all connections
    ///
    /// This is called after a mutation (INSERT/UPDATE/DELETE) is executed to notify
    /// other connections that may have subscriptions on the affected tables.
    fn broadcast_mutation(&self, mutation_query: &str) {
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
        if let Err(e) = self.mutation_broadcast_tx.send(notification) {
            // No receivers or channel issue - this is fine, just log at debug level
            debug!("Failed to broadcast mutation notification: {}", e);
        }
    }

    /// Send query result to client
    async fn send_query_result(&mut self, result: ExecutionResult) -> Result<()> {
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

                self.send_row_description(fields).await?;

                // Save row count before consuming
                let row_count = rows.len();

                // Send data rows
                for row in rows {
                    let values: Vec<Option<Vec<u8>>> = row
                        .values
                        .iter()
                        .map(|v: &vibesql_types::SqlValue| Some(v.to_string().as_bytes().to_vec()))
                        .collect();

                    self.send_data_row(values).await?;
                }

                // Send command complete
                self.send_command_complete(&format!("SELECT {}", row_count)).await?;
            }

            ExecutionResult::Insert { rows_affected } => {
                self.send_command_complete(&format!("INSERT 0 {}", rows_affected)).await?;
            }

            ExecutionResult::Update { rows_affected } => {
                self.send_command_complete(&format!("UPDATE {}", rows_affected)).await?;
            }

            ExecutionResult::Delete { rows_affected } => {
                self.send_command_complete(&format!("DELETE {}", rows_affected)).await?;
            }

            ExecutionResult::CreateTable
            | ExecutionResult::CreateIndex
            | ExecutionResult::CreateView => {
                self.send_command_complete("CREATE TABLE").await?;
            }

            ExecutionResult::DropTable | ExecutionResult::DropIndex | ExecutionResult::DropView => {
                self.send_command_complete("DROP TABLE").await?;
            }

            ExecutionResult::Analyze { tables_analyzed } => {
                self.send_command_complete(&format!("ANALYZE {}", tables_analyzed)).await?;
            }

            ExecutionResult::Other { message } => {
                self.send_command_complete(&message).await?;
            }

            ExecutionResult::Prepare { statement_name } => {
                self.send_command_complete(&format!("PREPARE {}", statement_name)).await?;
            }

            ExecutionResult::Deallocate { statement_name } => {
                self.send_command_complete(&format!("DEALLOCATE {}", statement_name)).await?;
            }

            ExecutionResult::DeclareCursor { cursor_name } => {
                self.send_command_complete(&format!("DECLARE CURSOR {}", cursor_name)).await?;
            }

            ExecutionResult::OpenCursor { cursor_name } => {
                self.send_command_complete(&format!("OPEN {}", cursor_name)).await?;
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

                self.send_row_description(fields).await?;

                // Save row count before consuming
                let row_count = rows.len();

                // Send data rows
                for row in rows {
                    let values: Vec<Option<Vec<u8>>> = row
                        .values
                        .iter()
                        .map(|v: &vibesql_types::SqlValue| Some(v.to_string().as_bytes().to_vec()))
                        .collect();

                    self.send_data_row(values).await?;
                }

                // Send command complete
                self.send_command_complete(&format!("FETCH {}", row_count)).await?;
            }

            ExecutionResult::CloseCursor { cursor_name } => {
                self.send_command_complete(&format!("CLOSE {}", cursor_name)).await?;
            }

            ExecutionResult::Begin => {
                self.send_command_complete("BEGIN").await?;
            }

            ExecutionResult::Commit => {
                self.send_command_complete("COMMIT").await?;
            }

            ExecutionResult::Rollback => {
                self.send_command_complete("ROLLBACK").await?;
            }
        }

        Ok(())
    }

    // Message sending methods

    async fn send_authentication_ok(&mut self) -> Result<()> {
        BackendMessage::AuthenticationOk.encode(&mut self.write_buf);
        self.flush_write_buffer().await
    }

    async fn send_cleartext_password_request(&mut self) -> Result<()> {
        BackendMessage::AuthenticationCleartextPassword.encode(&mut self.write_buf);
        self.flush_write_buffer().await
    }

    async fn send_md5_password_request(&mut self, salt: &[u8; 4]) -> Result<()> {
        BackendMessage::AuthenticationMD5Password { salt: *salt }.encode(&mut self.write_buf);
        self.flush_write_buffer().await
    }

    async fn send_parameter_status(&mut self, name: &str, value: &str) -> Result<()> {
        BackendMessage::ParameterStatus { name: name.to_string(), value: value.to_string() }
            .encode(&mut self.write_buf);
        self.flush_write_buffer().await
    }

    async fn send_backend_key_data(&mut self) -> Result<()> {
        BackendMessage::BackendKeyData {
            process_id: std::process::id() as i32,
            secret_key: 12345, // TODO: Generate random secret
        }
        .encode(&mut self.write_buf);
        self.flush_write_buffer().await
    }

    async fn send_ready_for_query(&mut self, status: TransactionStatus) -> Result<()> {
        BackendMessage::ReadyForQuery { status }.encode(&mut self.write_buf);
        self.flush_write_buffer().await
    }

    async fn send_row_description(&mut self, fields: Vec<FieldDescription>) -> Result<()> {
        BackendMessage::RowDescription { fields }.encode(&mut self.write_buf);
        self.flush_write_buffer().await
    }

    async fn send_data_row(&mut self, values: Vec<Option<Vec<u8>>>) -> Result<()> {
        BackendMessage::DataRow { values }.encode(&mut self.write_buf);
        self.flush_write_buffer().await
    }

    async fn send_command_complete(&mut self, tag: &str) -> Result<()> {
        BackendMessage::CommandComplete { tag: tag.to_string() }.encode(&mut self.write_buf);
        self.flush_write_buffer().await
    }

    async fn send_error_response(&mut self, message: &str) -> Result<()> {
        let mut fields = HashMap::new();
        fields.insert(b'S', "ERROR".to_string());
        fields.insert(b'C', "XX000".to_string()); // internal_error
        fields.insert(b'M', message.to_string());

        BackendMessage::ErrorResponse { fields }.encode(&mut self.write_buf);
        self.flush_write_buffer().await
    }

    async fn send_empty_query_response(&mut self) -> Result<()> {
        BackendMessage::EmptyQueryResponse.encode(&mut self.write_buf);
        self.flush_write_buffer().await
    }

    /// Send subscription data message (initial results or updates)
    async fn send_subscription_data(
        &mut self,
        subscription_id: &[u8; 16],
        update_type: SubscriptionUpdateType,
        rows: Vec<Vec<Option<Vec<u8>>>>,
    ) -> Result<()> {
        // Record subscription update metrics
        if let Some(metrics) = self.observability.metrics() {
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
            .encode(&mut self.write_buf);
        self.flush_write_buffer().await
    }

    /// Send subscription partial data message (for selective column updates)
    ///
    /// Uses the SubscriptionPartialData (0xF7) message format to send only
    /// changed columns plus primary key columns, reducing wire traffic.
    async fn send_subscription_partial_data(
        &mut self,
        subscription_id: &[u8; 16],
        rows: Vec<PartialRowUpdate>,
    ) -> Result<()> {
        // Record subscription update metrics
        if let Some(metrics) = self.observability.metrics() {
            metrics.record_subscription_update("selective", rows.len() as u64);
        }

        BackendMessage::SubscriptionPartialData { subscription_id: *subscription_id, rows }
            .encode(&mut self.write_buf);
        self.flush_write_buffer().await
    }

    /// Send subscription error message
    async fn send_subscription_error(
        &mut self,
        subscription_id: &[u8; 16],
        message: &str,
    ) -> Result<()> {
        BackendMessage::SubscriptionError {
            subscription_id: *subscription_id,
            message: message.to_string(),
        }
        .encode(&mut self.write_buf);
        self.flush_write_buffer().await
    }

    // I/O methods

    async fn read_message(&mut self) -> Result<()> {
        let n = self.read_half.read_buf(&mut self.read_buf).await?;
        if n == 0 {
            return Err(anyhow::anyhow!("Connection closed"));
        }
        Ok(())
    }

    async fn flush_write_buffer(&mut self) -> Result<()> {
        self.write_half.write_all(&self.write_buf).await?;
        self.write_half.flush().await?;
        self.write_buf.clear();
        Ok(())
    }
}

impl Drop for ConnectionHandler {
    fn drop(&mut self) {
        // Decrement active connection count
        self.active_connections.fetch_sub(1, Ordering::AcqRel);

        // Record connection duration when connection closes
        if let Some(metrics) = self.observability.metrics() {
            metrics.record_connection_duration(self.connection_start.elapsed());
        }
    }
}
