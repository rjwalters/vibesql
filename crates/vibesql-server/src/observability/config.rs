use serde::{Deserialize, Serialize};

/// Observability configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ObservabilityConfig {
    /// Enable observability (default: false)
    #[serde(default)]
    pub enabled: bool,

    /// Observability level: TRACE, DEBUG, INFO, WARN, ERROR (default: INFO)
    #[serde(default = "default_level")]
    pub level: String,

    /// OTLP exporter configuration
    #[serde(default)]
    pub otlp: OtlpConfig,

    /// Metrics configuration
    #[serde(default)]
    pub metrics: MetricsConfig,

    /// Traces configuration
    #[serde(default)]
    pub traces: TracesConfig,

    /// Logs configuration
    #[serde(default)]
    pub logs: LogsConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OtlpConfig {
    /// OTLP endpoint (default: http://localhost:4317)
    #[serde(default = "default_otlp_endpoint")]
    pub endpoint: String,

    /// Protocol: grpc or http (default: grpc)
    #[serde(default = "default_otlp_protocol")]
    pub protocol: String,

    /// Timeout in seconds (default: 10)
    #[serde(default = "default_otlp_timeout")]
    pub timeout_seconds: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsConfig {
    /// Enable metrics export (default: true)
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Export interval in seconds (default: 60)
    #[serde(default = "default_metrics_interval")]
    pub export_interval_seconds: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TracesConfig {
    /// Enable traces export (default: true)
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Sampler type: always_on, always_off, parent_based_always_on, trace_id_ratio
    #[serde(default = "default_sampler")]
    pub sampler: String,

    /// Sampling ratio for trace_id_ratio sampler (0.0-1.0, default: 1.0)
    #[serde(default = "default_sampling_ratio")]
    pub sampling_ratio: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogsConfig {
    /// Enable logs export (default: true)
    #[serde(default = "default_true")]
    pub enabled: bool,

    /// Bridge existing tracing logs to OpenTelemetry (default: true)
    #[serde(default = "default_true")]
    pub bridge_tracing: bool,
}

// Default value functions
fn default_level() -> String {
    "INFO".to_string()
}

fn default_otlp_endpoint() -> String {
    "http://localhost:4317".to_string()
}

fn default_otlp_protocol() -> String {
    "grpc".to_string()
}

fn default_otlp_timeout() -> u64 {
    10
}

fn default_true() -> bool {
    true
}

fn default_metrics_interval() -> u64 {
    60
}

fn default_sampler() -> String {
    "parent_based_always_on".to_string()
}

fn default_sampling_ratio() -> f64 {
    1.0
}

impl Default for ObservabilityConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            level: default_level(),
            otlp: OtlpConfig::default(),
            metrics: MetricsConfig::default(),
            traces: TracesConfig::default(),
            logs: LogsConfig::default(),
        }
    }
}

impl Default for OtlpConfig {
    fn default() -> Self {
        Self {
            endpoint: default_otlp_endpoint(),
            protocol: default_otlp_protocol(),
            timeout_seconds: default_otlp_timeout(),
        }
    }
}

impl Default for MetricsConfig {
    fn default() -> Self {
        Self { enabled: default_true(), export_interval_seconds: default_metrics_interval() }
    }
}

impl Default for TracesConfig {
    fn default() -> Self {
        Self {
            enabled: default_true(),
            sampler: default_sampler(),
            sampling_ratio: default_sampling_ratio(),
        }
    }
}

impl Default for LogsConfig {
    fn default() -> Self {
        Self { enabled: default_true(), bridge_tracing: default_true() }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_config() {
        let config = ObservabilityConfig::default();
        assert!(!config.enabled);
        assert_eq!(config.level, "INFO");
        assert_eq!(config.otlp.endpoint, "http://localhost:4317");
        assert_eq!(config.otlp.protocol, "grpc");
        assert!(config.metrics.enabled);
        assert!(config.traces.enabled);
        assert!(config.logs.enabled);
    }

    #[test]
    fn test_config_serialization() {
        let config = ObservabilityConfig::default();
        let toml_str = toml::to_string(&config).unwrap();
        let deserialized: ObservabilityConfig = toml::from_str(&toml_str).unwrap();
        assert_eq!(config.level, deserialized.level);
    }
}
