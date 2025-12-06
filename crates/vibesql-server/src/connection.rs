use crate::auth::PasswordStore;
use crate::config::Config;
use crate::observability::ObservabilityProvider;
use crate::protocol::{
    BackendMessage, FieldDescription, FrontendMessage, SubscriptionUpdateType, TransactionStatus,
};
use crate::registry::DatabaseRegistry;
use crate::session::{ExecutionResult, Session};
use crate::subscription::{extract_table_refs, SessionSubscriptionManager, SubscriptionManager};
use anyhow::Result;
use bytes::BytesMut;
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tracing::{debug, error, info, warn};
use vibesql_executor::cache::table_extractor;

/// Connection handler for a single client
pub struct ConnectionHandler {
    stream: TcpStream,
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
    /// Session-level subscription manager for real-time query subscriptions
    subscription_manager: SessionSubscriptionManager,
    /// Global subscription manager for processing storage change events
    #[allow(dead_code)]
    global_subscription_manager: Arc<SubscriptionManager>,
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
        global_subscription_manager: Arc<SubscriptionManager>,
    ) -> Self {
        Self {
            stream,
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
            subscription_manager: SessionSubscriptionManager::new(),
            global_subscription_manager,
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
                self.stream.write_u8(b'N').await?;
                self.stream.flush().await?;

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
    async fn process_queries(&mut self) -> Result<()> {
        loop {
            // Read a complete message, waiting for more data if needed
            let msg = match self.read_complete_message().await {
                Ok(msg) => msg,
                Err(e) => {
                    // Check if this is a clean connection close
                    let err_str = e.to_string();
                    if err_str.contains("Connection closed") {
                        debug!("Connection closed by client");
                        break;
                    }
                    return Err(e);
                }
            };

            match msg {
                FrontendMessage::Query { query } => {
                    debug!("Query: {}", query);
                    self.execute_query(&query).await?;
                }

                FrontendMessage::Subscribe { query, params } => {
                    debug!("Subscribe: {}", query);
                    self.handle_subscribe(&query, params).await?;
                }

                FrontendMessage::Unsubscribe { subscription_id } => {
                    debug!("Unsubscribe: {:?}", subscription_id);
                    self.subscription_manager.unsubscribe(&subscription_id);
                    // No response needed per protocol spec
                }

                FrontendMessage::Terminate => {
                    debug!("Client requested termination");
                    break;
                }

                msg => {
                    warn!("Unexpected message: {:?}", msg);
                }
            }
        }

        // Clean up subscriptions when connection closes
        self.subscription_manager.clear();

        Ok(())
    }

    /// Read a complete frontend message, looping until enough data is available
    ///
    /// This is critical for proper PostgreSQL wire protocol handling. TCP may deliver
    /// messages in fragments, so we must continue reading until we have a complete
    /// message. Previously, partial reads were incorrectly interpreted as connection
    /// closure, causing connections to drop after ~150-190 queries.
    async fn read_complete_message(&mut self) -> Result<FrontendMessage> {
        loop {
            // Try to decode a message from the existing buffer
            if let Some(msg) = FrontendMessage::decode(&mut self.read_buf)? {
                return Ok(msg);
            }

            // Need more data - read from the stream
            self.read_message().await?;
        }
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
                    self.notify_affected_subscriptions(query).await;
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
    async fn handle_subscribe(&mut self, query: &str, params: Vec<Option<Vec<u8>>>) -> Result<()> {
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

        // Extract table dependencies from the query
        let table_dependencies = table_extractor::extract_tables_from_statement(&parsed);

        // Register the subscription first (to get the ID)
        let subscription_id = match self.subscription_manager.subscribe(
            query.to_string(),
            params,
            table_dependencies,
        ) {
            Ok(id) => id,
            Err(e) => {
                // Send subscription error with a dummy subscription ID (subscription failed before registration)
                let error_id = [0u8; 16];
                self.send_subscription_error(&error_id, &format!("{}", e)).await?;
                return Ok(());
            }
        };

        // Execute the query to get initial data
        match session.execute(query).await {
            Ok(ExecutionResult::Select { rows, .. }) => {
                // Convert rows to wire format
                let wire_rows: Vec<Vec<Option<Vec<u8>>>> = rows
                    .iter()
                    .map(|row| {
                        row.values.iter().map(|v| Some(v.to_string().as_bytes().to_vec())).collect()
                    })
                    .collect();

                // Send initial subscription data
                self.send_subscription_data(
                    &subscription_id,
                    SubscriptionUpdateType::Full,
                    wire_rows,
                )
                .await?;
            }
            Ok(_) => {
                // Non-SELECT query - send error and remove subscription
                self.subscription_manager.unsubscribe(&subscription_id);
                self.send_subscription_error(
                    &subscription_id,
                    "Only SELECT queries can be subscribed to",
                )
                .await?;
            }
            Err(e) => {
                // Query execution failed - remove subscription and send error
                self.subscription_manager.unsubscribe(&subscription_id);
                self.send_subscription_error(&subscription_id, &format!("Execution error: {}", e))
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

        // Collect subscriptions that need updating
        // We collect (subscription_id, query) pairs to avoid borrowing issues
        let subscriptions_to_update: Vec<([u8; 16], String)> = affected_tables
            .iter()
            .flat_map(|table| {
                self.subscription_manager
                    .get_subscriptions_for_table_with_details(table)
                    .map(|(id, sub)| (*id, sub.query.clone()))
                    .collect::<Vec<_>>()
            })
            .collect();

        if subscriptions_to_update.is_empty() {
            return;
        }

        // De-duplicate subscriptions (a subscription may depend on multiple affected tables)
        let mut seen = std::collections::HashSet::new();
        let unique_subscriptions: Vec<_> = subscriptions_to_update
            .into_iter()
            .filter(|(id, _)| seen.insert(*id))
            .collect();

        debug!(
            "Notifying {} subscriptions after mutation affecting tables: {:?}",
            unique_subscriptions.len(),
            affected_tables
        );

        // Re-execute each subscription query and send updates
        for (subscription_id, query) in unique_subscriptions {
            if let Some(session) = &mut self.session {
                match session.execute(&query).await {
                    Ok(ExecutionResult::Select { rows, .. }) => {
                        // Convert rows to wire format
                        let wire_rows: Vec<Vec<Option<Vec<u8>>>> = rows
                            .iter()
                            .map(|row| {
                                row.values
                                    .iter()
                                    .map(|v| Some(v.to_string().as_bytes().to_vec()))
                                    .collect()
                            })
                            .collect();

                        // Send full update (could optimize to send delta in the future)
                        if let Err(e) = self
                            .send_subscription_data(
                                &subscription_id,
                                SubscriptionUpdateType::Full,
                                wire_rows,
                            )
                            .await
                        {
                            warn!("Failed to send subscription update: {}", e);
                        }
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
        BackendMessage::SubscriptionData { subscription_id: *subscription_id, update_type, rows }
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
        let n = self.stream.read_buf(&mut self.read_buf).await?;
        if n == 0 {
            return Err(anyhow::anyhow!("Connection closed"));
        }
        Ok(())
    }

    async fn flush_write_buffer(&mut self) -> Result<()> {
        self.stream.write_all(&self.write_buf).await?;
        self.stream.flush().await?;
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
