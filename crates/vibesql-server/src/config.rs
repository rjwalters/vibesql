use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::env;
use std::fs;
use std::path::PathBuf;

use crate::observability::ObservabilityConfig;
use crate::subscription::SubscriptionConfig;

/// Server configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub server: ServerConfig,
    pub auth: AuthConfig,
    pub logging: LoggingConfig,
    #[serde(default)]
    pub http: HttpConfig,
    #[serde(default)]
    pub observability: ObservabilityConfig,
    #[serde(default)]
    pub subscriptions: SubscriptionConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    /// Host to bind to (default: 0.0.0.0)
    pub host: String,
    /// Port to listen on (default: 5432)
    pub port: u16,
    /// Maximum concurrent connections (default: 100)
    pub max_connections: usize,
    /// Enable SSL/TLS (default: false)
    pub ssl_enabled: bool,
    /// SSL certificate file path
    pub ssl_cert: Option<PathBuf>,
    /// SSL key file path
    pub ssl_key: Option<PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AuthConfig {
    /// Authentication method: trust, password, md5, scram-sha-256
    pub method: String,
    /// Password file path (for file-based auth)
    pub password_file: Option<PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    /// Log level: trace, debug, info, warn, error
    pub level: String,
    /// Log file path (optional)
    pub file: Option<PathBuf>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpConfig {
    /// Enable HTTP REST API (default: true)
    pub enabled: bool,
    /// HTTP server host (default: 0.0.0.0)
    pub host: String,
    /// HTTP server port (default: 8080)
    pub port: u16,
    /// HTTP authentication configuration
    #[serde(default)]
    pub auth: HttpAuthConfig,
}

impl Default for HttpConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            host: "0.0.0.0".to_string(),
            port: 8080,
            auth: HttpAuthConfig::default(),
        }
    }
}

/// HTTP API authentication configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpAuthConfig {
    /// Enable authentication for HTTP API (default: false for backward compatibility)
    pub enabled: bool,
    /// Allowed authentication methods: api_key, basic, jwt
    pub methods: Vec<HttpAuthMethod>,
    /// API key configuration
    #[serde(default)]
    pub api_keys: ApiKeyConfig,
    /// JWT configuration
    #[serde(default)]
    pub jwt: JwtConfig,
}

impl Default for HttpAuthConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            methods: vec![HttpAuthMethod::ApiKey, HttpAuthMethod::Basic],
            api_keys: ApiKeyConfig::default(),
            jwt: JwtConfig::default(),
        }
    }
}

/// Supported HTTP authentication methods
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum HttpAuthMethod {
    /// API key authentication via Bearer token
    ApiKey,
    /// Basic HTTP authentication
    Basic,
    /// JWT authentication
    Jwt,
}

/// API key configuration
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ApiKeyConfig {
    /// List of valid API keys
    #[serde(default)]
    pub keys: Vec<String>,
}

/// JWT configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JwtConfig {
    /// Secret key for JWT signing/verification (HS256)
    #[serde(default)]
    pub secret: String,
    /// Expected issuer (iss claim)
    #[serde(default)]
    pub issuer: Option<String>,
    /// Expected audience (aud claim)
    #[serde(default)]
    pub audience: Option<String>,
    /// Token expiration time in seconds (default: 3600 = 1 hour)
    #[serde(default = "default_jwt_expiration")]
    pub expiration_secs: u64,
}

fn default_jwt_expiration() -> u64 {
    3600
}

impl Default for JwtConfig {
    fn default() -> Self {
        Self {
            secret: String::new(),
            issuer: Some("vibesql".to_string()),
            audience: Some("vibesql-api".to_string()),
            expiration_secs: default_jwt_expiration(),
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            server: ServerConfig {
                host: "0.0.0.0".to_string(),
                port: 5432,
                max_connections: 100,
                ssl_enabled: false,
                ssl_cert: None,
                ssl_key: None,
            },
            auth: AuthConfig { method: "trust".to_string(), password_file: None },
            logging: LoggingConfig { level: "info".to_string(), file: None },
            http: HttpConfig::default(),
            observability: ObservabilityConfig::default(),
            subscriptions: SubscriptionConfig::default(),
        }
    }
}

impl Config {
    /// Load configuration from file
    /// Searches for vibesql-server.toml in:
    /// 1. Current directory
    /// 2. $HOME/.config/vibesql/
    /// 3. /etc/vibesql/
    pub fn load() -> Result<Self> {
        let config_paths = vec![
            PathBuf::from("vibesql-server.toml"),
            dirs::config_dir()
                .map(|p| p.join("vibesql").join("vibesql-server.toml"))
                .unwrap_or_default(),
            PathBuf::from("/etc/vibesql/vibesql-server.toml"),
        ];

        for path in config_paths {
            if path.exists() {
                let contents = fs::read_to_string(&path)?;
                let mut config: Config = toml::from_str(&contents)?;
                config.apply_env_overrides();
                return Ok(config);
            }
        }

        // No config file found, return error
        Err(anyhow::anyhow!("No configuration file found"))
    }

    /// Load configuration from specific file
    #[allow(dead_code)]
    pub fn load_from(path: &PathBuf) -> Result<Self> {
        let contents = fs::read_to_string(path)?;
        let mut config: Config = toml::from_str(&contents)?;
        config.apply_env_overrides();
        Ok(config)
    }

    /// Apply environment variable overrides to the configuration.
    ///
    /// Environment variables with the `VIBESQL_` prefix override configuration file values.
    /// This follows the precedence: environment variable > config file > default.
    ///
    /// # Supported Environment Variables
    ///
    /// | Environment Variable | Config Path |
    /// |---------------------|-------------|
    /// | `VIBESQL_SERVER_HOST` | `server.host` |
    /// | `VIBESQL_SERVER_PORT` | `server.port` |
    /// | `VIBESQL_SERVER_MAX_CONNECTIONS` | `server.max_connections` |
    /// | `VIBESQL_SERVER_SSL_ENABLED` | `server.ssl_enabled` |
    /// | `VIBESQL_SERVER_SSL_CERT` | `server.ssl_cert` |
    /// | `VIBESQL_SERVER_SSL_KEY` | `server.ssl_key` |
    /// | `VIBESQL_AUTH_METHOD` | `auth.method` |
    /// | `VIBESQL_AUTH_PASSWORD_FILE` | `auth.password_file` |
    /// | `VIBESQL_LOG_LEVEL` | `logging.level` |
    /// | `VIBESQL_LOG_FILE` | `logging.file` |
    /// | `VIBESQL_HTTP_ENABLED` | `http.enabled` |
    /// | `VIBESQL_HTTP_HOST` | `http.host` |
    /// | `VIBESQL_HTTP_PORT` | `http.port` |
    /// | `VIBESQL_HTTP_AUTH_ENABLED` | `http.auth.enabled` |
    /// | `VIBESQL_HTTP_AUTH_METHODS` | `http.auth.methods` |
    /// | `VIBESQL_HTTP_AUTH_API_KEYS` | `http.auth.api_keys.keys` |
    /// | `VIBESQL_HTTP_AUTH_JWT_SECRET` | `http.auth.jwt.secret` |
    /// | `VIBESQL_HTTP_AUTH_JWT_ISSUER` | `http.auth.jwt.issuer` |
    /// | `VIBESQL_HTTP_AUTH_JWT_AUDIENCE` | `http.auth.jwt.audience` |
    /// | `VIBESQL_HTTP_AUTH_JWT_EXPIRATION` | `http.auth.jwt.expiration_secs` |
    pub fn apply_env_overrides(&mut self) {
        // Server configuration
        if let Ok(val) = env::var("VIBESQL_SERVER_HOST") {
            self.server.host = val;
        }
        if let Ok(val) = env::var("VIBESQL_SERVER_PORT") {
            if let Ok(port) = val.parse() {
                self.server.port = port;
            }
        }
        if let Ok(val) = env::var("VIBESQL_SERVER_MAX_CONNECTIONS") {
            if let Ok(max_conn) = val.parse() {
                self.server.max_connections = max_conn;
            }
        }
        if let Ok(val) = env::var("VIBESQL_SERVER_SSL_ENABLED") {
            self.server.ssl_enabled = parse_bool(&val);
        }
        if let Ok(val) = env::var("VIBESQL_SERVER_SSL_CERT") {
            self.server.ssl_cert = Some(PathBuf::from(val));
        }
        if let Ok(val) = env::var("VIBESQL_SERVER_SSL_KEY") {
            self.server.ssl_key = Some(PathBuf::from(val));
        }

        // Auth configuration
        if let Ok(val) = env::var("VIBESQL_AUTH_METHOD") {
            self.auth.method = val;
        }
        if let Ok(val) = env::var("VIBESQL_AUTH_PASSWORD_FILE") {
            self.auth.password_file = Some(PathBuf::from(val));
        }

        // Logging configuration
        if let Ok(val) = env::var("VIBESQL_LOG_LEVEL") {
            self.logging.level = val;
        }
        if let Ok(val) = env::var("VIBESQL_LOG_FILE") {
            self.logging.file = Some(PathBuf::from(val));
        }

        // HTTP configuration
        if let Ok(val) = env::var("VIBESQL_HTTP_ENABLED") {
            self.http.enabled = parse_bool(&val);
        }
        if let Ok(val) = env::var("VIBESQL_HTTP_HOST") {
            self.http.host = val;
        }
        if let Ok(val) = env::var("VIBESQL_HTTP_PORT") {
            if let Ok(port) = val.parse() {
                self.http.port = port;
            }
        }

        // HTTP Auth configuration
        if let Ok(val) = env::var("VIBESQL_HTTP_AUTH_ENABLED") {
            self.http.auth.enabled = parse_bool(&val);
        }
        if let Ok(val) = env::var("VIBESQL_HTTP_AUTH_METHODS") {
            let methods: Vec<HttpAuthMethod> = val
                .split(',')
                .filter_map(|s| match s.trim().to_lowercase().as_str() {
                    "api_key" => Some(HttpAuthMethod::ApiKey),
                    "basic" => Some(HttpAuthMethod::Basic),
                    "jwt" => Some(HttpAuthMethod::Jwt),
                    _ => None,
                })
                .collect();
            if !methods.is_empty() {
                self.http.auth.methods = methods;
            }
        }
        if let Ok(val) = env::var("VIBESQL_HTTP_AUTH_API_KEYS") {
            let keys: Vec<String> = val
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            if !keys.is_empty() {
                self.http.auth.api_keys.keys = keys;
            }
        }

        // JWT configuration
        if let Ok(val) = env::var("VIBESQL_HTTP_AUTH_JWT_SECRET") {
            self.http.auth.jwt.secret = val;
        }
        if let Ok(val) = env::var("VIBESQL_HTTP_AUTH_JWT_ISSUER") {
            self.http.auth.jwt.issuer = Some(val);
        }
        if let Ok(val) = env::var("VIBESQL_HTTP_AUTH_JWT_AUDIENCE") {
            self.http.auth.jwt.audience = Some(val);
        }
        if let Ok(val) = env::var("VIBESQL_HTTP_AUTH_JWT_EXPIRATION") {
            if let Ok(secs) = val.parse() {
                self.http.auth.jwt.expiration_secs = secs;
            }
        }
    }
}

/// Parse a string value as a boolean.
/// Accepts "true", "1", "yes", "on" as true (case-insensitive).
/// All other values are considered false.
fn parse_bool(val: &str) -> bool {
    matches!(val.to_lowercase().as_str(), "true" | "1" | "yes" | "on")
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    // Environment variable tests must be serialized to avoid interference
    static ENV_TEST_MUTEX: Mutex<()> = Mutex::new(());

    #[test]
    fn test_default_config() {
        let config = Config::default();
        assert_eq!(config.server.host, "0.0.0.0");
        assert_eq!(config.server.port, 5432);
        assert_eq!(config.server.max_connections, 100);
        assert!(!config.server.ssl_enabled);
        assert_eq!(config.auth.method, "trust");
    }

    #[test]
    fn test_config_serialization() {
        let config = Config::default();
        let toml_str = toml::to_string(&config).unwrap();
        let deserialized: Config = toml::from_str(&toml_str).unwrap();
        assert_eq!(config.server.port, deserialized.server.port);
    }

    #[test]
    fn test_selective_updates_config_defaults() {
        let config = Config::default();
        assert!(config.subscriptions.selective_updates.enabled);
        assert_eq!(config.subscriptions.selective_updates.min_changed_columns, 1);
        assert!((config.subscriptions.selective_updates.max_changed_columns_ratio - 0.5).abs() < 0.001);
    }

    #[test]
    fn test_selective_updates_config_from_toml() {
        let toml_str = r#"
[server]
host = "0.0.0.0"
port = 5432
max_connections = 100
ssl_enabled = false

[auth]
method = "trust"

[logging]
level = "info"

[subscriptions.selective_updates]
enabled = false
min_changed_columns = 2
max_changed_columns_ratio = 0.75
"#;
        let config: Config = toml::from_str(toml_str).unwrap();
        assert!(!config.subscriptions.selective_updates.enabled);
        assert_eq!(config.subscriptions.selective_updates.min_changed_columns, 2);
        assert!((config.subscriptions.selective_updates.max_changed_columns_ratio - 0.75).abs() < 0.001);
    }

    #[test]
    fn test_selective_updates_config_partial_override() {
        // Test that partial config falls back to defaults for unspecified fields
        let toml_str = r#"
[server]
host = "0.0.0.0"
port = 5432
max_connections = 100
ssl_enabled = false

[auth]
method = "trust"

[logging]
level = "info"

[subscriptions.selective_updates]
enabled = false
"#;
        let config: Config = toml::from_str(toml_str).unwrap();
        assert!(!config.subscriptions.selective_updates.enabled);
        // Other fields should use defaults
        assert_eq!(config.subscriptions.selective_updates.min_changed_columns, 1);
        assert!((config.subscriptions.selective_updates.max_changed_columns_ratio - 0.5).abs() < 0.001);
    }

    #[test]
    fn test_selective_updates_config_serialization() {
        let config = Config::default();
        let toml_str = toml::to_string(&config).unwrap();
        let deserialized: Config = toml::from_str(&toml_str).unwrap();

        assert_eq!(
            config.subscriptions.selective_updates.enabled,
            deserialized.subscriptions.selective_updates.enabled
        );
        assert_eq!(
            config.subscriptions.selective_updates.min_changed_columns,
            deserialized.subscriptions.selective_updates.min_changed_columns
        );
        assert!(
            (config.subscriptions.selective_updates.max_changed_columns_ratio
                - deserialized.subscriptions.selective_updates.max_changed_columns_ratio).abs() < 0.001
        );
    }

    #[test]
    fn test_parse_bool() {
        assert!(parse_bool("true"));
        assert!(parse_bool("TRUE"));
        assert!(parse_bool("True"));
        assert!(parse_bool("1"));
        assert!(parse_bool("yes"));
        assert!(parse_bool("YES"));
        assert!(parse_bool("on"));
        assert!(parse_bool("ON"));

        assert!(!parse_bool("false"));
        assert!(!parse_bool("0"));
        assert!(!parse_bool("no"));
        assert!(!parse_bool("off"));
        assert!(!parse_bool(""));
        assert!(!parse_bool("invalid"));
    }

    #[test]
    fn test_env_override_server_host() {
        let _lock = ENV_TEST_MUTEX.lock().unwrap();
        let mut config = Config::default();
        assert_eq!(config.server.host, "0.0.0.0");

        env::set_var("VIBESQL_SERVER_HOST", "127.0.0.1");
        config.apply_env_overrides();
        assert_eq!(config.server.host, "127.0.0.1");
        env::remove_var("VIBESQL_SERVER_HOST");
    }

    #[test]
    fn test_env_override_server_port() {
        let _lock = ENV_TEST_MUTEX.lock().unwrap();
        let mut config = Config::default();
        assert_eq!(config.server.port, 5432);

        env::set_var("VIBESQL_SERVER_PORT", "5433");
        config.apply_env_overrides();
        assert_eq!(config.server.port, 5433);
        env::remove_var("VIBESQL_SERVER_PORT");
    }

    #[test]
    fn test_env_override_server_port_invalid() {
        let _lock = ENV_TEST_MUTEX.lock().unwrap();
        let mut config = Config::default();
        assert_eq!(config.server.port, 5432);

        // Invalid port should be ignored
        env::set_var("VIBESQL_SERVER_PORT", "not_a_number");
        config.apply_env_overrides();
        assert_eq!(config.server.port, 5432);
        env::remove_var("VIBESQL_SERVER_PORT");
    }

    #[test]
    fn test_env_override_max_connections() {
        let _lock = ENV_TEST_MUTEX.lock().unwrap();
        let mut config = Config::default();
        assert_eq!(config.server.max_connections, 100);

        env::set_var("VIBESQL_SERVER_MAX_CONNECTIONS", "500");
        config.apply_env_overrides();
        assert_eq!(config.server.max_connections, 500);
        env::remove_var("VIBESQL_SERVER_MAX_CONNECTIONS");
    }

    #[test]
    fn test_env_override_ssl_enabled() {
        let _lock = ENV_TEST_MUTEX.lock().unwrap();
        let mut config = Config::default();
        assert!(!config.server.ssl_enabled);

        env::set_var("VIBESQL_SERVER_SSL_ENABLED", "true");
        config.apply_env_overrides();
        assert!(config.server.ssl_enabled);
        env::remove_var("VIBESQL_SERVER_SSL_ENABLED");
    }

    #[test]
    fn test_env_override_ssl_cert_and_key() {
        let _lock = ENV_TEST_MUTEX.lock().unwrap();
        let mut config = Config::default();
        assert!(config.server.ssl_cert.is_none());
        assert!(config.server.ssl_key.is_none());

        env::set_var("VIBESQL_SERVER_SSL_CERT", "/path/to/cert.pem");
        env::set_var("VIBESQL_SERVER_SSL_KEY", "/path/to/key.pem");
        config.apply_env_overrides();
        assert_eq!(config.server.ssl_cert, Some(PathBuf::from("/path/to/cert.pem")));
        assert_eq!(config.server.ssl_key, Some(PathBuf::from("/path/to/key.pem")));
        env::remove_var("VIBESQL_SERVER_SSL_CERT");
        env::remove_var("VIBESQL_SERVER_SSL_KEY");
    }

    #[test]
    fn test_env_override_auth_method() {
        let _lock = ENV_TEST_MUTEX.lock().unwrap();
        let mut config = Config::default();
        assert_eq!(config.auth.method, "trust");

        env::set_var("VIBESQL_AUTH_METHOD", "scram-sha-256");
        config.apply_env_overrides();
        assert_eq!(config.auth.method, "scram-sha-256");
        env::remove_var("VIBESQL_AUTH_METHOD");
    }

    #[test]
    fn test_env_override_auth_password_file() {
        let _lock = ENV_TEST_MUTEX.lock().unwrap();
        let mut config = Config::default();
        assert!(config.auth.password_file.is_none());

        env::set_var("VIBESQL_AUTH_PASSWORD_FILE", "/etc/vibesql/passwords");
        config.apply_env_overrides();
        assert_eq!(config.auth.password_file, Some(PathBuf::from("/etc/vibesql/passwords")));
        env::remove_var("VIBESQL_AUTH_PASSWORD_FILE");
    }

    #[test]
    fn test_env_override_log_level() {
        let _lock = ENV_TEST_MUTEX.lock().unwrap();
        let mut config = Config::default();
        assert_eq!(config.logging.level, "info");

        env::set_var("VIBESQL_LOG_LEVEL", "debug");
        config.apply_env_overrides();
        assert_eq!(config.logging.level, "debug");
        env::remove_var("VIBESQL_LOG_LEVEL");
    }

    #[test]
    fn test_env_override_log_file() {
        let _lock = ENV_TEST_MUTEX.lock().unwrap();
        let mut config = Config::default();
        assert!(config.logging.file.is_none());

        env::set_var("VIBESQL_LOG_FILE", "/var/log/vibesql/server.log");
        config.apply_env_overrides();
        assert_eq!(config.logging.file, Some(PathBuf::from("/var/log/vibesql/server.log")));
        env::remove_var("VIBESQL_LOG_FILE");
    }

    #[test]
    fn test_env_override_http_enabled() {
        let _lock = ENV_TEST_MUTEX.lock().unwrap();
        let mut config = Config::default();
        assert!(config.http.enabled);

        env::set_var("VIBESQL_HTTP_ENABLED", "false");
        config.apply_env_overrides();
        assert!(!config.http.enabled);
        env::remove_var("VIBESQL_HTTP_ENABLED");
    }

    #[test]
    fn test_env_override_http_host() {
        let _lock = ENV_TEST_MUTEX.lock().unwrap();
        let mut config = Config::default();
        assert_eq!(config.http.host, "0.0.0.0");

        env::set_var("VIBESQL_HTTP_HOST", "localhost");
        config.apply_env_overrides();
        assert_eq!(config.http.host, "localhost");
        env::remove_var("VIBESQL_HTTP_HOST");
    }

    #[test]
    fn test_env_override_http_port() {
        let _lock = ENV_TEST_MUTEX.lock().unwrap();
        let mut config = Config::default();
        assert_eq!(config.http.port, 8080);

        env::set_var("VIBESQL_HTTP_PORT", "9090");
        config.apply_env_overrides();
        assert_eq!(config.http.port, 9090);
        env::remove_var("VIBESQL_HTTP_PORT");
    }

    #[test]
    fn test_env_override_multiple_values() {
        let _lock = ENV_TEST_MUTEX.lock().unwrap();
        let mut config = Config::default();

        env::set_var("VIBESQL_SERVER_HOST", "192.168.1.1");
        env::set_var("VIBESQL_SERVER_PORT", "5433");
        env::set_var("VIBESQL_AUTH_METHOD", "md5");
        env::set_var("VIBESQL_LOG_LEVEL", "warn");
        env::set_var("VIBESQL_HTTP_PORT", "8081");

        config.apply_env_overrides();

        assert_eq!(config.server.host, "192.168.1.1");
        assert_eq!(config.server.port, 5433);
        assert_eq!(config.auth.method, "md5");
        assert_eq!(config.logging.level, "warn");
        assert_eq!(config.http.port, 8081);

        env::remove_var("VIBESQL_SERVER_HOST");
        env::remove_var("VIBESQL_SERVER_PORT");
        env::remove_var("VIBESQL_AUTH_METHOD");
        env::remove_var("VIBESQL_LOG_LEVEL");
        env::remove_var("VIBESQL_HTTP_PORT");
    }

    #[test]
    fn test_env_override_http_auth_enabled() {
        let _lock = ENV_TEST_MUTEX.lock().unwrap();
        let mut config = Config::default();
        assert!(!config.http.auth.enabled);

        env::set_var("VIBESQL_HTTP_AUTH_ENABLED", "true");
        config.apply_env_overrides();
        assert!(config.http.auth.enabled);
        env::remove_var("VIBESQL_HTTP_AUTH_ENABLED");
    }

    #[test]
    fn test_env_override_http_auth_enabled_false() {
        let _lock = ENV_TEST_MUTEX.lock().unwrap();
        let mut config = Config::default();
        config.http.auth.enabled = true;

        env::set_var("VIBESQL_HTTP_AUTH_ENABLED", "false");
        config.apply_env_overrides();
        assert!(!config.http.auth.enabled);
        env::remove_var("VIBESQL_HTTP_AUTH_ENABLED");
    }

    #[test]
    fn test_env_override_http_auth_methods() {
        let _lock = ENV_TEST_MUTEX.lock().unwrap();
        let mut config = Config::default();
        // Default methods are api_key and basic
        assert_eq!(config.http.auth.methods.len(), 2);
        assert!(config.http.auth.methods.contains(&HttpAuthMethod::ApiKey));
        assert!(config.http.auth.methods.contains(&HttpAuthMethod::Basic));

        env::set_var("VIBESQL_HTTP_AUTH_METHODS", "jwt,api_key");
        config.apply_env_overrides();
        assert_eq!(config.http.auth.methods.len(), 2);
        assert!(config.http.auth.methods.contains(&HttpAuthMethod::Jwt));
        assert!(config.http.auth.methods.contains(&HttpAuthMethod::ApiKey));
        assert!(!config.http.auth.methods.contains(&HttpAuthMethod::Basic));
        env::remove_var("VIBESQL_HTTP_AUTH_METHODS");
    }

    #[test]
    fn test_env_override_http_auth_methods_case_insensitive() {
        let _lock = ENV_TEST_MUTEX.lock().unwrap();
        let mut config = Config::default();

        env::set_var("VIBESQL_HTTP_AUTH_METHODS", "JWT, API_KEY, BASIC");
        config.apply_env_overrides();
        assert_eq!(config.http.auth.methods.len(), 3);
        assert!(config.http.auth.methods.contains(&HttpAuthMethod::Jwt));
        assert!(config.http.auth.methods.contains(&HttpAuthMethod::ApiKey));
        assert!(config.http.auth.methods.contains(&HttpAuthMethod::Basic));
        env::remove_var("VIBESQL_HTTP_AUTH_METHODS");
    }

    #[test]
    fn test_env_override_http_auth_methods_invalid_ignored() {
        let _lock = ENV_TEST_MUTEX.lock().unwrap();
        let mut config = Config::default();

        // Invalid methods should be silently ignored
        env::set_var("VIBESQL_HTTP_AUTH_METHODS", "jwt,invalid_method,basic,unknown");
        config.apply_env_overrides();
        assert_eq!(config.http.auth.methods.len(), 2);
        assert!(config.http.auth.methods.contains(&HttpAuthMethod::Jwt));
        assert!(config.http.auth.methods.contains(&HttpAuthMethod::Basic));
        env::remove_var("VIBESQL_HTTP_AUTH_METHODS");
    }

    #[test]
    fn test_env_override_http_auth_methods_empty_preserves_default() {
        let _lock = ENV_TEST_MUTEX.lock().unwrap();
        let mut config = Config::default();
        let original_methods = config.http.auth.methods.clone();

        // Empty value should preserve default
        env::set_var("VIBESQL_HTTP_AUTH_METHODS", "");
        config.apply_env_overrides();
        assert_eq!(config.http.auth.methods, original_methods);
        env::remove_var("VIBESQL_HTTP_AUTH_METHODS");
    }

    #[test]
    fn test_env_override_http_auth_methods_all_invalid_preserves_default() {
        let _lock = ENV_TEST_MUTEX.lock().unwrap();
        let mut config = Config::default();
        let original_methods = config.http.auth.methods.clone();

        // All invalid values should preserve default
        env::set_var("VIBESQL_HTTP_AUTH_METHODS", "invalid,unknown,bad");
        config.apply_env_overrides();
        assert_eq!(config.http.auth.methods, original_methods);
        env::remove_var("VIBESQL_HTTP_AUTH_METHODS");
    }

    #[test]
    fn test_env_override_http_auth_api_keys() {
        let _lock = ENV_TEST_MUTEX.lock().unwrap();
        let mut config = Config::default();
        assert!(config.http.auth.api_keys.keys.is_empty());

        env::set_var("VIBESQL_HTTP_AUTH_API_KEYS", "key1,key2,key3");
        config.apply_env_overrides();
        assert_eq!(config.http.auth.api_keys.keys.len(), 3);
        assert!(config.http.auth.api_keys.keys.contains(&"key1".to_string()));
        assert!(config.http.auth.api_keys.keys.contains(&"key2".to_string()));
        assert!(config.http.auth.api_keys.keys.contains(&"key3".to_string()));
        env::remove_var("VIBESQL_HTTP_AUTH_API_KEYS");
    }

    #[test]
    fn test_env_override_http_auth_api_keys_trims_whitespace() {
        let _lock = ENV_TEST_MUTEX.lock().unwrap();
        let mut config = Config::default();

        env::set_var("VIBESQL_HTTP_AUTH_API_KEYS", " key1 , key2 , key3 ");
        config.apply_env_overrides();
        assert_eq!(config.http.auth.api_keys.keys.len(), 3);
        assert!(config.http.auth.api_keys.keys.contains(&"key1".to_string()));
        assert!(config.http.auth.api_keys.keys.contains(&"key2".to_string()));
        assert!(config.http.auth.api_keys.keys.contains(&"key3".to_string()));
        env::remove_var("VIBESQL_HTTP_AUTH_API_KEYS");
    }

    #[test]
    fn test_env_override_http_auth_api_keys_empty_values_ignored() {
        let _lock = ENV_TEST_MUTEX.lock().unwrap();
        let mut config = Config::default();

        // Empty values between commas should be ignored
        env::set_var("VIBESQL_HTTP_AUTH_API_KEYS", "key1,,key2,  ,key3");
        config.apply_env_overrides();
        assert_eq!(config.http.auth.api_keys.keys.len(), 3);
        assert!(config.http.auth.api_keys.keys.contains(&"key1".to_string()));
        assert!(config.http.auth.api_keys.keys.contains(&"key2".to_string()));
        assert!(config.http.auth.api_keys.keys.contains(&"key3".to_string()));
        env::remove_var("VIBESQL_HTTP_AUTH_API_KEYS");
    }

    #[test]
    fn test_env_override_http_auth_api_keys_empty_preserves_default() {
        let _lock = ENV_TEST_MUTEX.lock().unwrap();
        let mut config = Config::default();
        // Pre-populate with a key
        config.http.auth.api_keys.keys = vec!["original-key".to_string()];

        // Empty value should preserve existing keys
        env::set_var("VIBESQL_HTTP_AUTH_API_KEYS", "");
        config.apply_env_overrides();
        assert_eq!(config.http.auth.api_keys.keys, vec!["original-key".to_string()]);
        env::remove_var("VIBESQL_HTTP_AUTH_API_KEYS");
    }

    #[test]
    fn test_env_override_http_auth_api_keys_single_key() {
        let _lock = ENV_TEST_MUTEX.lock().unwrap();
        let mut config = Config::default();

        env::set_var("VIBESQL_HTTP_AUTH_API_KEYS", "single-key");
        config.apply_env_overrides();
        assert_eq!(config.http.auth.api_keys.keys.len(), 1);
        assert_eq!(config.http.auth.api_keys.keys[0], "single-key");
        env::remove_var("VIBESQL_HTTP_AUTH_API_KEYS");
    }

    #[test]
    fn test_env_override_jwt_secret() {
        let _lock = ENV_TEST_MUTEX.lock().unwrap();
        let mut config = Config::default();
        assert_eq!(config.http.auth.jwt.secret, "");

        env::set_var("VIBESQL_HTTP_AUTH_JWT_SECRET", "my-super-secret-key");
        config.apply_env_overrides();
        assert_eq!(config.http.auth.jwt.secret, "my-super-secret-key");
        env::remove_var("VIBESQL_HTTP_AUTH_JWT_SECRET");
    }

    #[test]
    fn test_env_override_jwt_issuer() {
        let _lock = ENV_TEST_MUTEX.lock().unwrap();
        let mut config = Config::default();
        assert_eq!(config.http.auth.jwt.issuer, Some("vibesql".to_string()));

        env::set_var("VIBESQL_HTTP_AUTH_JWT_ISSUER", "custom-issuer");
        config.apply_env_overrides();
        assert_eq!(config.http.auth.jwt.issuer, Some("custom-issuer".to_string()));
        env::remove_var("VIBESQL_HTTP_AUTH_JWT_ISSUER");
    }

    #[test]
    fn test_env_override_jwt_audience() {
        let _lock = ENV_TEST_MUTEX.lock().unwrap();
        let mut config = Config::default();
        assert_eq!(config.http.auth.jwt.audience, Some("vibesql-api".to_string()));

        env::set_var("VIBESQL_HTTP_AUTH_JWT_AUDIENCE", "custom-audience");
        config.apply_env_overrides();
        assert_eq!(config.http.auth.jwt.audience, Some("custom-audience".to_string()));
        env::remove_var("VIBESQL_HTTP_AUTH_JWT_AUDIENCE");
    }

    #[test]
    fn test_env_override_jwt_expiration() {
        let _lock = ENV_TEST_MUTEX.lock().unwrap();
        let mut config = Config::default();
        assert_eq!(config.http.auth.jwt.expiration_secs, 3600);

        env::set_var("VIBESQL_HTTP_AUTH_JWT_EXPIRATION", "7200");
        config.apply_env_overrides();
        assert_eq!(config.http.auth.jwt.expiration_secs, 7200);
        env::remove_var("VIBESQL_HTTP_AUTH_JWT_EXPIRATION");
    }

    #[test]
    fn test_env_override_jwt_expiration_invalid() {
        let _lock = ENV_TEST_MUTEX.lock().unwrap();
        let mut config = Config::default();
        assert_eq!(config.http.auth.jwt.expiration_secs, 3600);

        // Invalid expiration should be ignored
        env::set_var("VIBESQL_HTTP_AUTH_JWT_EXPIRATION", "not_a_number");
        config.apply_env_overrides();
        assert_eq!(config.http.auth.jwt.expiration_secs, 3600);
        env::remove_var("VIBESQL_HTTP_AUTH_JWT_EXPIRATION");
    }
}
