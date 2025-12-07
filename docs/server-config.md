# VibeSQL Server Configuration

This document describes the configuration options for the VibeSQL server (`vibesql-server`).

## Configuration File

The server looks for configuration in the following locations (in order):

1. `vibesql-server.toml` in the current directory
2. `~/.config/vibesql/vibesql-server.toml`
3. `/etc/vibesql/vibesql-server.toml`

If no configuration file is found, the server will fail to start.

## Configuration Format

The configuration file uses [TOML](https://toml.io) format.

## Complete Example

```toml
[server]
host = "0.0.0.0"
port = 5432
max_connections = 100
ssl_enabled = false
# ssl_cert = "/path/to/cert.pem"
# ssl_key = "/path/to/key.pem"

[auth]
method = "trust"
# password_file = "/path/to/passwords"

[logging]
level = "info"
# file = "/var/log/vibesql/server.log"

[http]
enabled = true
host = "0.0.0.0"
port = 8080

[http.auth]
enabled = false
methods = ["api_key", "basic"]

[http.auth.api_keys]
keys = []

[http.auth.jwt]
secret = ""
issuer = "vibesql"
audience = "vibesql-api"
expiration_secs = 3600

[observability]
# See docs/observability/metrics.md for details

[subscriptions]
max_per_connection = 100
max_global = 10000
max_result_rows = 10000
rate_limit_per_second = 10
channel_buffer_size = 64
slow_consumer_threshold_percent = 80

[subscriptions.selective_updates]
enabled = true
min_changed_columns = 1
max_changed_columns_ratio = 0.5
```

## Configuration Sections

### Server (`[server]`)

Core server settings.

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `host` | string | `"0.0.0.0"` | Host address to bind to |
| `port` | integer | `5432` | Port to listen on (PostgreSQL wire protocol) |
| `max_connections` | integer | `100` | Maximum concurrent connections |
| `ssl_enabled` | boolean | `false` | Enable SSL/TLS encryption |
| `ssl_cert` | string | - | Path to SSL certificate file (required if SSL enabled) |
| `ssl_key` | string | - | Path to SSL private key file (required if SSL enabled) |

### Authentication (`[auth]`)

PostgreSQL wire protocol authentication settings.

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `method` | string | `"trust"` | Authentication method: `trust`, `password`, `md5`, `scram-sha-256` |
| `password_file` | string | - | Path to password file (for file-based auth) |

### Logging (`[logging]`)

Logging configuration.

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `level` | string | `"info"` | Log level: `trace`, `debug`, `info`, `warn`, `error` |
| `file` | string | - | Path to log file (logs to stderr if not set) |

### HTTP API (`[http]`)

REST API and Server-Sent Events configuration.

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `enabled` | boolean | `true` | Enable HTTP REST API |
| `host` | string | `"0.0.0.0"` | HTTP server host |
| `port` | integer | `8080` | HTTP server port |

#### HTTP Authentication (`[http.auth]`)

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `enabled` | boolean | `false` | Enable authentication for HTTP API |
| `methods` | array | `["api_key", "basic"]` | Allowed auth methods: `api_key`, `basic`, `jwt` |

See [HTTP API documentation](http-api.md) for more details.

### Subscriptions (`[subscriptions]`)

Real-time subscription configuration for managing resource limits and behavior.

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `max_per_connection` | integer | `100` | Maximum subscriptions per connection |
| `max_global` | integer | `10000` | Maximum total subscriptions across all connections |
| `max_result_rows` | integer | `10000` | Maximum rows per subscription result set |
| `rate_limit_per_second` | integer | `10` | Maximum subscription creations per second per connection |
| `channel_buffer_size` | integer | `64` | Update channel buffer size per subscription |
| `slow_consumer_threshold_percent` | integer | `80` | Buffer fill percentage that triggers slow consumer warnings |

#### Selective Updates (`[subscriptions.selective_updates]`)

Controls when the server sends partial row updates (only changed columns) instead of full rows. This optimization reduces bandwidth for wide tables where only a few columns change frequently.

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `enabled` | boolean | `true` | Enable selective column updates |
| `min_changed_columns` | integer | `1` | Minimum columns that must change to use selective update |
| `max_changed_columns_ratio` | float | `0.5` | Maximum ratio of changed columns before falling back to full row |

**How selective updates work:**

When a subscription detects row updates, the server compares the old and new row values:

1. If `enabled` is `false`, always send full rows
2. If fewer than `min_changed_columns` columns changed, send full row
3. If the ratio of changed columns exceeds `max_changed_columns_ratio`, send full row
4. Otherwise, send only the changed columns (plus primary key columns for row identification)

**Use cases:**

- **Disable for debugging**: Set `enabled = false` to always see full row updates, making it easier to debug subscription behavior

- **Wide tables (many columns)**: Lower `max_changed_columns_ratio` (e.g., `0.3`) to be more aggressive about using selective updates, since bandwidth savings are larger

- **Narrow tables (few columns)**: Consider disabling or using a higher ratio, as the overhead of partial updates may not be worth the bandwidth savings

**Example configurations:**

```toml
# Disable selective updates (for debugging)
[subscriptions.selective_updates]
enabled = false

# Aggressive selective updates for wide tables
[subscriptions.selective_updates]
enabled = true
min_changed_columns = 1
max_changed_columns_ratio = 0.3

# Conservative selective updates
[subscriptions.selective_updates]
enabled = true
min_changed_columns = 2
max_changed_columns_ratio = 0.7
```

## Minimal Configuration

For development or testing, a minimal configuration:

```toml
[server]
host = "127.0.0.1"
port = 5432
max_connections = 10
ssl_enabled = false

[auth]
method = "trust"

[logging]
level = "debug"
```

## Production Configuration

Recommended settings for production deployments:

```toml
[server]
host = "0.0.0.0"
port = 5432
max_connections = 500
ssl_enabled = true
ssl_cert = "/etc/vibesql/server.crt"
ssl_key = "/etc/vibesql/server.key"

[auth]
method = "scram-sha-256"
password_file = "/etc/vibesql/passwords"

[logging]
level = "info"
file = "/var/log/vibesql/server.log"

[http]
enabled = true
host = "0.0.0.0"
port = 8080

[http.auth]
enabled = true
methods = ["jwt"]

[http.auth.jwt]
secret = "your-secret-key-here"
issuer = "vibesql"
audience = "vibesql-api"
expiration_secs = 3600

[subscriptions]
max_per_connection = 50
max_global = 5000
max_result_rows = 10000
rate_limit_per_second = 5
channel_buffer_size = 128
slow_consumer_threshold_percent = 70

[subscriptions.selective_updates]
enabled = true
min_changed_columns = 1
max_changed_columns_ratio = 0.5
```

## Environment Variables

Configuration values can also be set via environment variables with the `VIBESQL_` prefix:

| Environment Variable | Config Path |
|---------------------|-------------|
| `VIBESQL_SERVER_HOST` | `server.host` |
| `VIBESQL_SERVER_PORT` | `server.port` |
| `VIBESQL_LOG_LEVEL` | `logging.level` |

Environment variables take precedence over configuration file values.

## Related Documentation

- [HTTP API](http-api.md) - REST API and SSE endpoints
- [Wire Protocol](../crates/vibesql-server/PROTOCOL.md) - PostgreSQL protocol extensions
- [Observability Metrics](observability/metrics.md) - Prometheus metrics reference
