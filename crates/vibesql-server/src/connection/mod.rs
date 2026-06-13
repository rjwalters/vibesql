//! Connection handling for PostgreSQL wire protocol
//!
//! This module provides the core connection handling logic for VibeSQL's PostgreSQL
//! wire protocol implementation. It handles:
//!
//! - Authentication (trust, password, MD5)
//! - Query processing
//! - Subscription management for real-time updates
//! - Cross-connection notifications via broadcast channels
//!
//! # Architecture
//!
//! The connection handler is split into focused modules:
//!
//! - `auth`: Authentication methods (trust, password, MD5)
//! - `io`: Low-level I/O operations
//! - `protocol`: Protocol message encoding/sending
//! - `query`: Query execution and result handling
//! - `selective`: Selective column update logic
//! - `subscription`: Subscription registration and updates
//! - `updates`: Subscription update sending logic

mod auth;
mod extended;
mod io;
mod protocol;
mod query;
mod selective;
mod subscription;
mod updates;

use std::{
    collections::HashSet,
    net::SocketAddr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Instant,
};

use anyhow::Result;
use bytes::BytesMut;
use tokio::{
    io::AsyncWriteExt,
    net::{
        tcp::{OwnedReadHalf, OwnedWriteHalf},
        TcpStream,
    },
    sync::broadcast,
};
use tracing::{debug, info, warn};

use crate::{
    auth::PasswordStore,
    config::Config,
    observability::ObservabilityProvider,
    protocol::{FrontendMessage, TransactionStatus},
    registry::DatabaseRegistry,
    session::Session,
    subscription::SubscriptionManager,
};

use self::extended::{
    handle_bind, handle_close, handle_describe, handle_execute, handle_flush, handle_parse,
    handle_sync, ExtendedQueryState,
};
use self::io::read_message;
use self::protocol::{send_backend_key_data, send_parameter_status, send_ready_for_query};
use self::query::execute_query;
use self::subscription::{handle_cross_connection_notification, handle_subscribe};

/// Notification sent when a mutation affects tables
/// This is broadcast to all connections so they can notify their subscriptions
#[derive(Debug, Clone)]
pub struct TableMutationNotification {
    /// Tables that were affected by the mutation
    pub affected_tables: HashSet<String>,
}

/// Result of handling a client message
enum ClientMessageResult {
    /// Continue processing messages
    Continue,
    /// Client requested termination
    Terminate,
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
    /// Extended query protocol state (prepared statements and portals)
    extended_state: ExtendedQueryState,
    /// Replicated-mode handle (#5383): when set, sessions route their
    /// statements through consensus (`Session::new_replicated`).
    replication: Option<Arc<crate::replication::ReplicationHandle>>,
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
            extended_state: ExtendedQueryState::new(),
            replication: None,
        }
    }

    /// Route this connection's sessions through consensus (#5383). The
    /// builder form keeps every existing standalone call site untouched.
    pub fn with_replication(
        mut self,
        replication: Arc<crate::replication::ReplicationHandle>,
    ) -> Self {
        self.replication = Some(replication);
        self
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
        read_message(&mut self.read_half, &mut self.read_buf).await?;

        let startup_msg = FrontendMessage::decode_startup(&mut self.read_buf)?;

        match startup_msg {
            Some(FrontendMessage::SSLRequest) => {
                debug!("Received SSL request");
                // We don't support SSL yet, send 'N'
                self.write_half.write_u8(b'N').await?;
                self.write_half.flush().await?;

                // Read actual startup message after SSL rejection
                self.read_buf.clear();
                read_message(&mut self.read_half, &mut self.read_buf).await?;

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
                auth::authenticate(
                    &self.config,
                    &self.password_store,
                    &mut self.read_half,
                    &mut self.write_half,
                    &mut self.read_buf,
                    &mut self.write_buf,
                    &user,
                )
                .await?;

                // Get or create shared database from registry
                let shared_db = self.database_registry.get_or_create(&database).await;

                // Create session with shared database; in replicated
                // mode the session routes statements through consensus
                // instead (#5383).
                self.session = Some(match &self.replication {
                    Some(handle) => Session::new_replicated(
                        database.clone(),
                        user.clone(),
                        shared_db,
                        Arc::clone(handle),
                    ),
                    None => Session::new(database.clone(), user.clone(), shared_db),
                });

                info!("User '{}' connected to database '{}'", user, database);

                // Send startup complete messages
                send_parameter_status(
                    &mut self.write_half,
                    &mut self.write_buf,
                    "server_version",
                    "14.0 (VibeSQL)",
                )
                .await?;
                send_parameter_status(
                    &mut self.write_half,
                    &mut self.write_buf,
                    "server_encoding",
                    "UTF8",
                )
                .await?;
                send_parameter_status(
                    &mut self.write_half,
                    &mut self.write_buf,
                    "client_encoding",
                    "UTF8",
                )
                .await?;
                send_parameter_status(
                    &mut self.write_half,
                    &mut self.write_buf,
                    "DateStyle",
                    "ISO, MDY",
                )
                .await?;
                send_parameter_status(&mut self.write_half, &mut self.write_buf, "TimeZone", "UTC")
                    .await?;

                // Send backend key data (for cancel requests)
                send_backend_key_data(&mut self.write_half, &mut self.write_buf).await?;

                // Send ready for query
                send_ready_for_query(
                    &mut self.write_half,
                    &mut self.write_buf,
                    TransactionStatus::Idle,
                )
                .await?;

                Ok(())
            }

            _ => Err(anyhow::anyhow!("Invalid startup message")),
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
                        let (total, selective_eligible) = self
                            .subscription_manager
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
                                handle_cross_connection_notification(
                                    &mut self.session,
                                    &self.config,
                                    &self.observability,
                                    &self.subscription_manager,
                                    &self.connection_id,
                                    &mut self.write_half,
                                    &mut self.write_buf,
                                    &n.affected_tables,
                                ).await;
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
                read_result = tokio::io::AsyncReadExt::read_buf(&mut self.read_half, &mut self.read_buf) => {
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
        let (total, selective_eligible) =
            self.subscription_manager.unsubscribe_all_for_connection(&self.connection_id);
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
                execute_query(
                    &mut self.session,
                    &self.config,
                    &self.observability,
                    &self.subscription_manager,
                    &self.mutation_broadcast_tx,
                    &self.connection_id,
                    &mut self.write_half,
                    &mut self.write_buf,
                    &query,
                )
                .await?;
                Ok(ClientMessageResult::Continue)
            }

            // =================================================================
            // Extended Query Protocol Messages
            // =================================================================
            FrontendMessage::Parse { name, query, param_types } => {
                handle_parse(
                    &mut self.session,
                    &mut self.extended_state,
                    &mut self.write_half,
                    &mut self.write_buf,
                    name,
                    query,
                    param_types,
                )
                .await?;
                Ok(ClientMessageResult::Continue)
            }

            FrontendMessage::Bind {
                portal,
                statement,
                param_formats,
                param_values,
                result_formats,
            } => {
                handle_bind(
                    &mut self.session,
                    &mut self.extended_state,
                    &mut self.write_half,
                    &mut self.write_buf,
                    portal,
                    statement,
                    param_formats,
                    param_values,
                    result_formats,
                )
                .await?;
                Ok(ClientMessageResult::Continue)
            }

            FrontendMessage::Describe { target_type, name } => {
                handle_describe(
                    &self.session,
                    &self.extended_state,
                    &mut self.write_half,
                    &mut self.write_buf,
                    target_type,
                    name,
                )
                .await?;
                Ok(ClientMessageResult::Continue)
            }

            FrontendMessage::Execute { portal, max_rows } => {
                handle_execute(
                    &mut self.session,
                    &self.extended_state,
                    &mut self.write_half,
                    &mut self.write_buf,
                    portal,
                    max_rows,
                )
                .await?;
                Ok(ClientMessageResult::Continue)
            }

            FrontendMessage::Sync => {
                handle_sync(&mut self.write_half, &mut self.write_buf).await?;
                Ok(ClientMessageResult::Continue)
            }

            FrontendMessage::Flush => {
                handle_flush(&mut self.write_half).await?;
                Ok(ClientMessageResult::Continue)
            }

            FrontendMessage::Close { target_type, name } => {
                handle_close(
                    &mut self.extended_state,
                    &mut self.write_half,
                    &mut self.write_buf,
                    target_type,
                    name,
                )
                .await?;
                Ok(ClientMessageResult::Continue)
            }

            // =================================================================
            // Subscription Protocol Messages
            // =================================================================
            FrontendMessage::Subscribe { query, params, filter, selective_updates_config } => {
                debug!(
                    "Subscribe: {} (filter: {:?}, selective_config: {:?})",
                    query, filter, selective_updates_config
                );
                handle_subscribe(
                    &mut self.session,
                    &self.config,
                    &self.observability,
                    &self.subscription_manager,
                    &self.connection_id,
                    &mut self.write_half,
                    &mut self.write_buf,
                    &query,
                    params,
                    filter,
                    selective_updates_config,
                )
                .await?;
                Ok(ClientMessageResult::Continue)
            }

            FrontendMessage::Unsubscribe { subscription_id } => {
                debug!("Unsubscribe: {:?}", subscription_id);
                let was_selective_eligible =
                    self.subscription_manager.unsubscribe_by_wire_id(&subscription_id);
                if was_selective_eligible {
                    if let Some(metrics) = self.observability.metrics() {
                        metrics.decrement_selective_eligible();
                    }
                }
                // No response needed per protocol spec
                Ok(ClientMessageResult::Continue)
            }

            FrontendMessage::SubscriptionPause { subscription_id } => {
                debug!("SubscriptionPause: {:?}", subscription_id);
                // TODO: Implement pause functionality
                Ok(ClientMessageResult::Continue)
            }

            FrontendMessage::SubscriptionResume { subscription_id } => {
                debug!("SubscriptionResume: {:?}", subscription_id);
                // TODO: Implement resume functionality
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
