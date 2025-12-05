use super::config::ObservabilityConfig;
use super::metrics::ServerMetrics;
use anyhow::{Context, Result};
use opentelemetry::global;
use opentelemetry::trace::TracerProvider as _;
use opentelemetry::KeyValue;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::trace::{Sampler, SdkTracerProvider};
use opentelemetry_sdk::Resource;
use std::time::Duration;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

/// OpenTelemetry observability provider
pub struct ObservabilityProvider {
    #[allow(dead_code)]
    meter_provider: Option<SdkMeterProvider>,
    #[allow(dead_code)]
    tracer_provider: Option<SdkTracerProvider>,
    metrics: Option<ServerMetrics>,
}

impl ObservabilityProvider {
    /// Initialize observability with the given configuration
    pub fn init(config: &ObservabilityConfig) -> Result<Self> {
        if !config.enabled {
            return Ok(Self { meter_provider: None, tracer_provider: None, metrics: None });
        }

        let resource = Resource::builder()
            .with_service_name("vibesql-server")
            .with_attribute(KeyValue::new("service.version", env!("CARGO_PKG_VERSION")))
            .build();

        // Initialize metrics if enabled
        let (meter_provider, metrics) = if config.metrics.enabled {
            let exporter = opentelemetry_otlp::MetricExporter::builder()
                .with_tonic()
                .with_endpoint(&config.otlp.endpoint)
                .with_timeout(Duration::from_secs(config.otlp.timeout_seconds))
                .build()
                .context("Failed to create OTLP metrics exporter")?;

            let reader = opentelemetry_sdk::metrics::PeriodicReader::builder(exporter)
                .with_interval(Duration::from_secs(config.metrics.export_interval_seconds))
                .build();

            let provider = SdkMeterProvider::builder()
                .with_reader(reader)
                .with_resource(resource.clone())
                .build();

            // Set global meter provider
            global::set_meter_provider(provider.clone());

            // Get meter from global provider
            let meter = global::meter("vibesql-server");
            let metrics = ServerMetrics::new(&meter);

            (Some(provider), Some(metrics))
        } else {
            (None, None)
        };

        // Initialize tracing if enabled
        let tracer_provider = if config.traces.enabled {
            let sampler = match config.traces.sampler.as_str() {
                "always_on" => Sampler::AlwaysOn,
                "always_off" => Sampler::AlwaysOff,
                "parent_based_always_on" => Sampler::ParentBased(Box::new(Sampler::AlwaysOn)),
                "trace_id_ratio" => Sampler::TraceIdRatioBased(config.traces.sampling_ratio),
                _ => {
                    tracing::warn!(
                        "Unknown sampler '{}', using parent_based_always_on",
                        config.traces.sampler
                    );
                    Sampler::ParentBased(Box::new(Sampler::AlwaysOn))
                }
            };

            let exporter = opentelemetry_otlp::SpanExporter::builder()
                .with_tonic()
                .with_endpoint(&config.otlp.endpoint)
                .with_timeout(Duration::from_secs(config.otlp.timeout_seconds))
                .build()
                .context("Failed to create OTLP trace exporter")?;

            let provider = SdkTracerProvider::builder()
                .with_batch_exporter(exporter)
                .with_sampler(sampler)
                .with_resource(resource.clone())
                .build();

            // Set global tracer provider
            global::set_tracer_provider(provider.clone());

            // Initialize tracing subscriber with OpenTelemetry layer
            if config.logs.bridge_tracing {
                let tracer = provider.tracer("vibesql-server");
                let telemetry = tracing_opentelemetry::layer().with_tracer(tracer);

                let env_filter = tracing_subscriber::EnvFilter::try_from_default_env()
                    .unwrap_or_else(|_| {
                        tracing_subscriber::EnvFilter::new(config.level.to_lowercase())
                    });

                tracing_subscriber::registry()
                    .with(env_filter)
                    .with(telemetry)
                    .with(tracing_subscriber::fmt::layer())
                    .try_init()
                    .ok(); // Ignore error if already initialized
            }

            Some(provider)
        } else {
            None
        };

        Ok(Self { meter_provider, tracer_provider, metrics })
    }

    /// Get server metrics (if enabled)
    pub fn metrics(&self) -> Option<&ServerMetrics> {
        self.metrics.as_ref()
    }

    /// Shutdown the observability provider
    ///
    /// This should be called before the application exits to ensure all telemetry
    /// data is flushed to the OTLP endpoint.
    #[allow(dead_code)]
    pub fn shutdown(mut self) -> Result<()> {
        if let Some(provider) = self.meter_provider.take() {
            provider.shutdown().context("Failed to shutdown meter provider")?;
        }

        if let Some(provider) = self.tracer_provider.take() {
            provider.shutdown().context("Failed to shutdown tracer provider")?;
        }

        Ok(())
    }
}
