use anyhow::Result;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::{error, info};

use vibesql_server::auth::PasswordStore;
use vibesql_server::config::Config;
use vibesql_server::connection::ConnectionHandler;
use vibesql_server::observability::ObservabilityProvider;

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
            .with_env_filter(
                tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| {
                        tracing_subscriber::EnvFilter::new(config.logging.level.to_lowercase())
                    }),
            )
            .try_init()
            .ok(); // Ignore error if already initialized
    }

    info!("Starting VibeSQL Server v{}", env!("CARGO_PKG_VERSION"));
    info!("Configuration:");
    info!("  Host: {}", config.server.host);
    info!("  Port: {}", config.server.port);
    info!("  Max connections: {}", config.server.max_connections);
    info!("  SSL enabled: {}", config.server.ssl_enabled);
    info!("  Auth method: {}", config.auth.method);
    info!("  Observability enabled: {}", config.observability.enabled);

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

    loop {
        // Accept new connections
        match listener.accept().await {
            Ok((stream, peer_addr)) => {
                info!("New connection from {}", peer_addr);

                let config = Arc::clone(&config);
                let observability = Arc::clone(&observability);
                let password_store = password_store.clone();

                // Record connection metric
                if let Some(metrics) = observability.metrics() {
                    metrics.record_connection();
                }

                // Spawn a new task for each connection
                tokio::spawn(async move {
                    let mut handler =
                        ConnectionHandler::new(stream, peer_addr, config, observability, password_store);
                    if let Err(e) = handler.handle().await {
                        error!("Connection error from {}: {}", peer_addr, e);
                    }
                    info!("Connection closed: {}", peer_addr);
                });
            }
            Err(e) => {
                error!("Failed to accept connection: {}", e);
            }
        }
    }
}
