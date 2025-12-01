//! Common test utilities for vibesql-server integration tests

use bytes::{BufMut, BytesMut};
use std::net::SocketAddr;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;

use vibesql_server::auth::PasswordStore;
use vibesql_server::config::{AuthConfig, Config, LoggingConfig, ServerConfig};
use vibesql_server::connection::ConnectionHandler;
use vibesql_server::observability::{ObservabilityConfig, ObservabilityProvider};

/// Test server handle - holds the shutdown channel and address
pub struct TestServer {
    pub addr: SocketAddr,
    shutdown_tx: Option<oneshot::Sender<()>>,
}

impl TestServer {
    /// Get the server address
    pub fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Shutdown the server
    pub fn shutdown(mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown_tx.take() {
            let _ = tx.send(());
        }
    }
}

/// Start a test server with trust authentication on a random port
pub async fn start_test_server() -> TestServer {
    start_test_server_with_config(test_config()).await
}

/// Start a test server with the given configuration
pub async fn start_test_server_with_config(mut config: Config) -> TestServer {
    // Bind to a random available port
    let listener = TcpListener::bind("127.0.0.1:0").await.expect("Failed to bind");
    let addr = listener.local_addr().expect("Failed to get local address");

    // Update config with actual port
    config.server.port = addr.port();
    config.server.host = "127.0.0.1".to_string();

    let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();

    let config = Arc::new(config);

    // Initialize observability (disabled for tests)
    let observability = Arc::new(
        ObservabilityProvider::init(&config.observability)
            .expect("Failed to init observability"),
    );

    // Load password store if configured
    let password_store = if let Some(ref password_file) = config.auth.password_file {
        Some(Arc::new(
            PasswordStore::load_from_file(password_file)
                .expect("Failed to load password file"),
        ))
    } else {
        None
    };

    // Track active connections
    let active_connections = Arc::new(AtomicUsize::new(0));

    // Spawn server task
    tokio::spawn(async move {
        loop {
            tokio::select! {
                result = listener.accept() => {
                    match result {
                        Ok((stream, peer_addr)) => {
                            let config = Arc::clone(&config);
                            let observability = Arc::clone(&observability);
                            let password_store = password_store.clone();
                            let active_connections = Arc::clone(&active_connections);

                            tokio::spawn(async move {
                                let mut handler = ConnectionHandler::new(
                                    stream,
                                    peer_addr,
                                    config,
                                    observability,
                                    password_store,
                                    active_connections,
                                );
                                let _ = handler.handle().await;
                            });
                        }
                        Err(_) => break,
                    }
                }
                _ = &mut shutdown_rx => {
                    break;
                }
            }
        }
    });

    TestServer {
        addr,
        shutdown_tx: Some(shutdown_tx),
    }
}

/// Create a test configuration with trust authentication
pub fn test_config() -> Config {
    Config {
        server: ServerConfig {
            host: "127.0.0.1".to_string(),
            port: 0, // Will be assigned
            max_connections: 10,
            ssl_enabled: false,
            ssl_cert: None,
            ssl_key: None,
        },
        auth: AuthConfig {
            method: "trust".to_string(),
            password_file: None,
        },
        logging: LoggingConfig {
            level: "error".to_string(), // Quiet for tests
            file: None,
        },
        observability: ObservabilityConfig::default(),
    }
}

/// Create a test configuration with password authentication
pub fn test_config_with_password(password_file: std::path::PathBuf) -> Config {
    let mut config = test_config();
    config.auth.method = "password".to_string();
    config.auth.password_file = Some(password_file);
    config
}

/// Test client for connecting to the server
pub struct TestClient {
    stream: TcpStream,
    read_buf: BytesMut,
    write_buf: BytesMut,
}

impl TestClient {
    /// Connect to a test server
    pub async fn connect(addr: SocketAddr) -> std::io::Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        Ok(Self {
            stream,
            read_buf: BytesMut::with_capacity(8192),
            write_buf: BytesMut::with_capacity(8192),
        })
    }

    /// Send startup message
    pub async fn send_startup(
        &mut self,
        user: &str,
        database: &str,
    ) -> std::io::Result<()> {
        self.write_buf.clear();

        // Build startup message
        let mut params = BytesMut::new();
        put_cstring(&mut params, "user");
        put_cstring(&mut params, user);
        put_cstring(&mut params, "database");
        put_cstring(&mut params, database);
        params.put_u8(0); // End of parameters

        // Protocol version (3.0) = 196608
        let protocol_version: i32 = 196608;

        // Total length: 4 (length) + 4 (version) + params
        let len = 4 + 4 + params.len();

        self.write_buf.put_i32(len as i32);
        self.write_buf.put_i32(protocol_version);
        self.write_buf.extend_from_slice(&params);

        self.stream.write_all(&self.write_buf).await?;
        self.stream.flush().await?;
        Ok(())
    }

    /// Send SSL request
    pub async fn send_ssl_request(&mut self) -> std::io::Result<()> {
        self.write_buf.clear();

        // SSL request message: length (8) + SSL magic number
        self.write_buf.put_i32(8);
        self.write_buf.put_i32(80877103); // SSL request code

        self.stream.write_all(&self.write_buf).await?;
        self.stream.flush().await?;
        Ok(())
    }

    /// Send a password message
    pub async fn send_password(&mut self, password: &str) -> std::io::Result<()> {
        self.write_buf.clear();

        let password_bytes = password.as_bytes();
        let len = 4 + password_bytes.len() + 1; // length + password + null

        self.write_buf.put_u8(b'p'); // Password message
        self.write_buf.put_i32(len as i32);
        self.write_buf.extend_from_slice(password_bytes);
        self.write_buf.put_u8(0); // Null terminator

        self.stream.write_all(&self.write_buf).await?;
        self.stream.flush().await?;
        Ok(())
    }

    /// Send a query message
    pub async fn send_query(&mut self, query: &str) -> std::io::Result<()> {
        self.write_buf.clear();

        let query_bytes = query.as_bytes();
        let len = 4 + query_bytes.len() + 1; // length + query + null

        self.write_buf.put_u8(b'Q'); // Query message
        self.write_buf.put_i32(len as i32);
        self.write_buf.extend_from_slice(query_bytes);
        self.write_buf.put_u8(0); // Null terminator

        self.stream.write_all(&self.write_buf).await?;
        self.stream.flush().await?;
        Ok(())
    }

    /// Send a terminate message
    pub async fn send_terminate(&mut self) -> std::io::Result<()> {
        self.write_buf.clear();

        self.write_buf.put_u8(b'X'); // Terminate
        self.write_buf.put_i32(4); // Length

        self.stream.write_all(&self.write_buf).await?;
        self.stream.flush().await?;
        Ok(())
    }

    /// Read bytes from the server and return the data
    pub async fn read_response(&mut self) -> std::io::Result<Vec<u8>> {
        self.read_buf.clear();
        let n = self.stream.read_buf(&mut self.read_buf).await?;
        if n == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "Connection closed",
            ));
        }
        Ok(self.read_buf.to_vec())
    }

    /// Read until we get a specific message type
    pub async fn read_until_message_type(&mut self, msg_type: u8) -> std::io::Result<Vec<u8>> {
        let mut all_data = Vec::new();
        loop {
            let n = self.stream.read_buf(&mut self.read_buf).await?;
            if n == 0 {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "Connection closed",
                ));
            }

            all_data.extend_from_slice(&self.read_buf);
            self.read_buf.clear();

            // Check if we have the target message
            if contains_message_type(&all_data, msg_type) {
                return Ok(all_data);
            }
        }
    }

    /// Read a single byte (for SSL response)
    pub async fn read_byte(&mut self) -> std::io::Result<u8> {
        let mut buf = [0u8; 1];
        self.stream.read_exact(&mut buf).await?;
        Ok(buf[0])
    }

    /// Get raw access to the read buffer
    pub fn read_buffer(&self) -> &BytesMut {
        &self.read_buf
    }

    /// Check if connection is still open by attempting a zero-byte read
    pub async fn is_connected(&mut self) -> bool {
        let mut buf = [0u8; 1];
        match self.stream.try_read(&mut buf) {
            Ok(0) => false, // Connection closed
            Ok(_) => true,  // Data available (shouldn't happen in this context)
            Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => true, // Connection open
            Err(_) => false, // Other error, assume closed
        }
    }
}

/// Helper to put a null-terminated C string
fn put_cstring(buf: &mut BytesMut, s: &str) {
    buf.extend_from_slice(s.as_bytes());
    buf.put_u8(0);
}

/// Check if data contains a message of a specific type
fn contains_message_type(data: &[u8], msg_type: u8) -> bool {
    let mut pos = 0;
    while pos < data.len() {
        if data[pos] == msg_type {
            return true;
        }
        // Skip to next message
        if pos + 5 <= data.len() {
            let len = i32::from_be_bytes([data[pos + 1], data[pos + 2], data[pos + 3], data[pos + 4]]) as usize;
            pos += 1 + len;
        } else {
            break;
        }
    }
    false
}

/// Parse backend messages from raw bytes
pub fn parse_backend_messages(data: &[u8]) -> Vec<ParsedMessage> {
    let mut messages = Vec::new();
    let mut pos = 0;

    while pos < data.len() {
        if pos + 5 > data.len() {
            break;
        }

        let msg_type = data[pos];
        let len = i32::from_be_bytes([data[pos + 1], data[pos + 2], data[pos + 3], data[pos + 4]]) as usize;

        if pos + 1 + len > data.len() {
            break;
        }

        let payload = &data[pos + 5..pos + 1 + len];
        messages.push(ParsedMessage {
            msg_type,
            payload: payload.to_vec(),
        });

        pos += 1 + len;
    }

    messages
}

/// A parsed backend message
#[derive(Debug, Clone)]
pub struct ParsedMessage {
    pub msg_type: u8,
    pub payload: Vec<u8>,
}

impl ParsedMessage {
    /// Check if this is an AuthenticationOk message
    pub fn is_auth_ok(&self) -> bool {
        self.msg_type == b'R' && self.payload.len() >= 4 && i32::from_be_bytes([self.payload[0], self.payload[1], self.payload[2], self.payload[3]]) == 0
    }

    /// Check if this is a ReadyForQuery message
    pub fn is_ready_for_query(&self) -> bool {
        self.msg_type == b'Z'
    }

    /// Check if this is an ErrorResponse message
    pub fn is_error(&self) -> bool {
        self.msg_type == b'E'
    }

    /// Check if this is a RowDescription message
    pub fn is_row_description(&self) -> bool {
        self.msg_type == b'T'
    }

    /// Check if this is a DataRow message
    pub fn is_data_row(&self) -> bool {
        self.msg_type == b'D'
    }

    /// Check if this is a CommandComplete message
    pub fn is_command_complete(&self) -> bool {
        self.msg_type == b'C'
    }

    /// Check if this is a ParameterStatus message
    pub fn is_parameter_status(&self) -> bool {
        self.msg_type == b'S'
    }

    /// Check if this is a BackendKeyData message
    pub fn is_backend_key_data(&self) -> bool {
        self.msg_type == b'K'
    }

    /// Check if this is an EmptyQueryResponse message
    pub fn is_empty_query_response(&self) -> bool {
        self.msg_type == b'I'
    }

    /// Check if this is an AuthenticationCleartextPassword request
    pub fn is_cleartext_password_request(&self) -> bool {
        self.msg_type == b'R' && self.payload.len() >= 4 && i32::from_be_bytes([self.payload[0], self.payload[1], self.payload[2], self.payload[3]]) == 3
    }

    /// Check if this is an AuthenticationMD5Password request
    pub fn is_md5_password_request(&self) -> bool {
        self.msg_type == b'R' && self.payload.len() >= 4 && i32::from_be_bytes([self.payload[0], self.payload[1], self.payload[2], self.payload[3]]) == 5
    }

    /// Get MD5 salt if this is an MD5 password request
    pub fn get_md5_salt(&self) -> Option<[u8; 4]> {
        if self.is_md5_password_request() && self.payload.len() >= 8 {
            let mut salt = [0u8; 4];
            salt.copy_from_slice(&self.payload[4..8]);
            Some(salt)
        } else {
            None
        }
    }

    /// Get command tag if this is a CommandComplete message
    pub fn get_command_tag(&self) -> Option<String> {
        if self.is_command_complete() {
            // Find null terminator
            let null_pos = self.payload.iter().position(|&b| b == 0)?;
            String::from_utf8(self.payload[..null_pos].to_vec()).ok()
        } else {
            None
        }
    }
}
