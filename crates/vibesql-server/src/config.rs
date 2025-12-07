use anyhow::Result;
use serde::{Deserialize, Serialize};
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
                let config: Config = toml::from_str(&contents)?;
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
        let config: Config = toml::from_str(&contents)?;
        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
