use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use anyhow::Result;
use bytes::BytesMut;
use tokio::{io::AsyncWriteExt, net::TcpListener, sync::broadcast};
use tracing::{error, info, warn};
use vibesql_server::{
    auth::PasswordStore,
    config::Config,
    connection::{ConnectionHandler, TableMutationNotification},
    http::create_http_router,
    observability::ObservabilityProvider,
    protocol::BackendMessage,
    registry::DatabaseRegistry,
    replication::ReplicationHandle,
    subscription::SubscriptionManager,
};
use vibesql_storage::Database;

#[tokio::main]
async fn main() -> Result<()> {
    // Load configuration first (needed for observability setup)
    let config = Config::load().unwrap_or_else(|e| {
        eprintln!("Warning: Could not load config file: {}", e);
        eprintln!("Using default configuration");
        Config::default()
    });

    // Initialize observability (this sets up tracing subscriber if configured)
    let observability = ObservabilityProvider::init(&config.observability)?;

    // Initialize basic tracing if observability didn't set it up
    if !config.observability.enabled || !config.observability.logs.bridge_tracing {
        tracing_subscriber::fmt()
            .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(
                |_| tracing_subscriber::EnvFilter::new(config.logging.level.to_lowercase()),
            ))
            .try_init()
            .ok(); // Ignore error if already initialized
    }

    info!("Starting VibeSQL Server v{}", env!("CARGO_PKG_VERSION"));
    info!("Configuration:");
    info!("  PostgreSQL Wire Protocol:");
    info!("    Host: {}", config.server.host);
    info!("    Port: {}", config.server.port);
    info!("    Max connections: {}", config.server.max_connections);
    info!("    SSL enabled: {}", config.server.ssl_enabled);
    info!("  HTTP REST API:");
    info!("    Enabled: {}", config.http.enabled);
    if config.http.enabled {
        info!("    Host: {}", config.http.host);
        info!("    Port: {}", config.http.port);
    }
    info!("  Auth method: {}", config.auth.method);
    info!("  Observability enabled: {}", config.observability.enabled);
    info!("  Replication enabled: {}", config.replication.enabled);

    // Boot the consensus voter when replicated mode is configured
    // (#5383). Sessions then route writes through it; standalone mode
    // (the default) is untouched.
    let replication = if config.replication.enabled {
        let handle = ReplicationHandle::start(&config.replication).await?;
        info!(
            "Replicated mode: node {} of a {}-node cluster (writes route through consensus)",
            handle.node_id(),
            handle.cluster_size(),
        );
        Some(handle)
    } else {
        None
    };

    // Load password store if password file is configured
    let password_store = if let Some(ref password_file) = config.auth.password_file {
        info!("Loading password file: {:?}", password_file);
        match PasswordStore::load_from_file(password_file) {
            Ok(store) => {
                info!("Password file loaded successfully");
                Some(Arc::new(store))
            }
            Err(e) => {
                error!("Failed to load password file: {}", e);
                if config.auth.method != "trust" {
                    return Err(e);
                }
                None
            }
        }
    } else {
        if config.auth.method != "trust" {
            error!("Password file not configured, but auth method is '{}'", config.auth.method);
            return Err(anyhow::anyhow!(
                "Password file required for '{}' authentication method",
                config.auth.method
            ));
        }
        None
    };

    // Bind to address
    let addr: SocketAddr = format!("{}:{}", config.server.host, config.server.port)
        .parse()
        .expect("Invalid server address");

    let listener = TcpListener::bind(&addr).await?;
    info!("Server listening on {}", addr);

    // Share configuration and observability across handlers
    let config = Arc::new(config);
    let observability = Arc::new(observability);

    // Track active connections
    let active_connections = Arc::new(AtomicUsize::new(0));

    // Create a shared database registry for wire protocol connections
    // Each database name maps to a shared Database instance
    let database_registry = DatabaseRegistry::new();

    // Create a broadcast channel for cross-connection subscription notifications
    // When one connection mutates data, other connections with subscriptions on
    // the affected tables need to be notified. The channel capacity should be
    // large enough to handle bursts of mutations without dropping messages.
    let (mutation_broadcast_tx, _mutation_broadcast_rx) =
        broadcast::channel::<TableMutationNotification>(1024);

    // Create a shared database for HTTP API and subscriptions
    // TODO: Consider integrating HTTP database into the registry
    let mut db = Database::new();
    let change_rx = db.enable_change_events(1024);
    let db = Arc::new(db);

    // Create the global subscription manager
    let subscription_manager = Arc::new(SubscriptionManager::new());
    let subscription_manager_for_handler = Arc::clone(&subscription_manager);

    // Spawn the subscription manager event loop in a background task.
    //
    // Standalone mode drives subscriptions from the local HTTP `Database`'s
    // change stream. Replicated mode (#5422) instead drives them from the
    // consensus **apply-path** change feed: every node — leader or follower —
    // emits a change event as it applies a committed entry, and the loop
    // re-runs affected subscriptions against the applied state machine. This is
    // what lets a subscriber on any node observe committed writes (the local
    // HTTP `Database` receives no replicated writes, so its change stream would
    // never fire). On a snapshot install the feed closes; the loop exits and is
    // restarted with a fresh receiver.
    let db_for_subscription_task = Arc::clone(&db);
    let subscription_manager_for_loop = Arc::clone(&subscription_manager);
    let replication_for_subs = replication.clone();
    tokio::spawn(async move {
        match replication_for_subs {
            Some(handle) => {
                info!("Starting replicated (apply-path) subscription event loop");
                loop {
                    let Some(change_rx) = handle.subscribe_changes() else {
                        warn!("Apply-path change feed unavailable; subscriptions disabled");
                        break;
                    };
                    let handle_for_query = Arc::clone(&handle);
                    let query_fn = move |query: &str| {
                        handle_for_query.with_applied_db(|db| {
                            SubscriptionManager::execute_query_against(query, db)
                        })
                    };
                    subscription_manager_for_loop
                        .run_replicated_event_loop(change_rx, query_fn)
                        .await;
                    // The feed closed (snapshot install or shutdown). Re-subscribe
                    // to pick up the new state machine; if it is gone, stop.
                    info!("Apply-path change feed closed; re-subscribing");
                }
            }
            None => {
                info!("Starting subscription manager event loop");
                subscription_manager_for_loop
                    .run_event_loop(change_rx, db_for_subscription_task)
                    .await;
            }
        }
        info!("Subscription manager event loop stopped");
    });

    // Spawn HTTP server if enabled
    if config.http.enabled {
        let http_addr: SocketAddr = format!("{}:{}", config.http.host, config.http.port)
            .parse()
            .expect("Invalid HTTP address");
        let db_for_http = Arc::clone(&db);
        let subscription_manager_for_http = Arc::clone(&subscription_manager);
        // Share the database registry with the HTTP server
        let registry_for_http = database_registry.clone();

        let metrics_for_http = observability.metrics().cloned();
        let replication_for_http = replication.clone();
        tokio::spawn(async move {
            let app = create_http_router(
                db_for_http,
                registry_for_http,
                subscription_manager_for_http,
                metrics_for_http,
                replication_for_http,
            );
            let listener = tokio::net::TcpListener::bind(&http_addr)
                .await
                .expect("Failed to bind HTTP server");

            info!("HTTP REST API listening on http://{}", http_addr);

            axum::serve(listener, app).await.expect("HTTP server error");
        });
    }

    loop {
        // Accept new connections
        match listener.accept().await {
            Ok((mut stream, peer_addr)) => {
                info!("New connection from {}", peer_addr);

                // Check if we've reached the connection limit using compare_exchange
                let max_conns = config.server.max_connections;
                let mut current = active_connections.load(Ordering::Acquire);

                loop {
                    if current >= max_conns {
                        // At limit - reject connection
                        warn!(
                            "Connection limit reached ({}/{}), rejecting connection from {}",
                            current, max_conns, peer_addr
                        );

                        // Send PostgreSQL error response (53300 = too_many_connections)
                        let mut buf = BytesMut::new();
                        let mut fields = HashMap::new();
                        fields.insert(b'S', "FATAL".to_string());
                        fields.insert(b'V', "FATAL".to_string());
                        fields.insert(b'C', "53300".to_string());
                        fields.insert(
                            b'M',
                            format!(
                                "sorry, too many clients already (max_connections={})",
                                max_conns
                            ),
                        );
                        BackendMessage::ErrorResponse { fields }.encode(&mut buf);

                        // Try to send error and close connection
                        if let Err(e) = stream.write_all(&buf).await {
                            error!("Failed to send rejection error to {}: {}", peer_addr, e);
                        }
                        let _ = stream.shutdown().await;
                        break;
                    }

                    // Try to atomically increment the counter
                    match active_connections.compare_exchange_weak(
                        current,
                        current + 1,
                        Ordering::AcqRel,
                        Ordering::Acquire,
                    ) {
                        Ok(_) => {
                            // Successfully incremented - proceed with connection
                            let config = Arc::clone(&config);
                            let observability = Arc::clone(&observability);
                            let password_store = password_store.clone();
                            let active_connections = Arc::clone(&active_connections);
                            let subscription_manager =
                                Arc::clone(&subscription_manager_for_handler);

                            // Record connection metric
                            if let Some(metrics) = observability.metrics() {
                                metrics.record_connection();
                            }

                            // Clone the database registry for this connection
                            let database_registry = database_registry.clone();

                            // Clone the mutation broadcast sender for this connection
                            let mutation_broadcast_tx = mutation_broadcast_tx.clone();

                            // Clone the replication handle for this connection
                            let replication = replication.clone();

                            // Spawn a new task for each connection
                            tokio::spawn(async move {
                                let mut handler = ConnectionHandler::new(
                                    stream,
                                    peer_addr,
                                    config,
                                    observability,
                                    password_store,
                                    active_connections,
                                    database_registry,
                                    subscription_manager,
                                    mutation_broadcast_tx,
                                );
                                if let Some(handle) = replication {
                                    handler = handler.with_replication(handle);
                                }
                                if let Err(e) = handler.handle().await {
                                    error!("Connection error from {}: {}", peer_addr, e);
                                }
                                info!("Connection closed: {}", peer_addr);
                            });
                            break;
                        }
                        Err(new_current) => {
                            // Another thread changed the value, retry
                            current = new_current;
                        }
                    }
                }
            }
            Err(e) => {
                error!("Failed to accept connection: {}", e);
            }
        }
    }
}
