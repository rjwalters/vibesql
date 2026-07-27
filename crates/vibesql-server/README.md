# vibesql-server

Network server with PostgreSQL wire protocol for VibeSQL database.

## Overview

`vibesql-server` provides a network server that accepts remote client connections using the PostgreSQL wire protocol. This enables VibeSQL to be used as a standalone database server, compatible with existing PostgreSQL clients and drivers.

## Features

- **PostgreSQL Wire Protocol**: Compatible with PostgreSQL clients (psql, JDBC, ODBC, etc.)
- **Async I/O**: Built with Tokio for high-performance concurrent connections
- **Authentication**: Support for trust, password, MD5, and SCRAM-SHA-256 authentication
- **Session Management**: Per-connection database sessions with transaction support
- **Configuration**: TOML-based configuration for server settings

## Installation

```bash
cargo build --release -p vibesql-server
```

## Usage

### Starting the Server

```bash
# Start with default configuration
vibesql-server

# Start with custom configuration file
vibesql-server --config /path/to/vibesql-server.toml
```

### Configuration

Create a configuration file `vibesql-server.toml`:

```toml
[server]
host = "0.0.0.0"
port = 5432
max_connections = 100
ssl_enabled = false

[auth]
method = "trust"  # Options: trust, password, md5, scram-sha-256

[logging]
level = "info"  # Options: trace, debug, info, warn, error
```

The server searches for configuration files in:
1. `./vibesql-server.toml` (current directory)
2. `~/.config/vibesql/vibesql-server.toml`
3. `/etc/vibesql/vibesql-server.toml`

## Connecting with PostgreSQL Clients

### psql

```bash
psql -h localhost -p 5432 -U myuser -d mydb
```

### Python (psycopg2)

```python
import psycopg2

conn = psycopg2.connect(
    host="localhost",
    port=5432,
    user="myuser",
    database="mydb"
)

cursor = conn.cursor()
cursor.execute("SELECT * FROM users")
rows = cursor.fetchall()
```

### JDBC

```java
String url = "jdbc:postgresql://localhost:5432/mydb";
Connection conn = DriverManager.getConnection(url, "myuser", "");
```

## Architecture

### Connection Flow

```
Client → TCP Connection → SSL Negotiation (optional)
       → Startup Handshake
       → Authentication
       → Query Processing Loop
       → Termination
```

### PostgreSQL Protocol Implementation

**Implemented:**
- Phase 1: Basic TCP server
- Phase 2: Startup handshake and authentication
- Phase 3: Simple query protocol (Query message)
- Row description and data row messages
- Error handling and responses

**TODO:**
- Advanced authentication (SCRAM-SHA-256)
- COPY protocol
- NOTIFY/LISTEN
- Connection pooling

## Development

### Testing

```bash
# Run unit tests
cargo test -p vibesql-server

# Run with logging
RUST_LOG=debug cargo run -p vibesql-server

# Test with psql
psql -h localhost -p 5432 -c "SELECT 1"
```

### Protocol Documentation

- [PostgreSQL Protocol](https://www.postgresql.org/docs/current/protocol.html)
- [Message Flow](https://www.postgresql.org/docs/current/protocol-flow.html)
- [Message Formats](https://www.postgresql.org/docs/current/protocol-message-formats.html)

## Performance

The server uses:
- Tokio for async I/O
- Per-connection tasks for concurrency
- Buffer pooling for efficient message handling
- Zero-copy message parsing where possible

## License

Licensed under the same terms as VibeSQL (MIT OR Apache-2.0).

## Related

- **#2155**: ODBC/JDBC drivers (depends on this server)
- **vibesql-executor**: Query execution engine used by this server
- **vibesql-cli**: Command-line client for VibeSQL
