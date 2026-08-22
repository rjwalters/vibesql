//! Morsel configuration and adaptive sizing.
//!
//! This module provides configuration for morsel-driven execution, including
//! per-operation morsel sizes and adaptive sizing based on row width and selectivity.

use vibesql_types::DataType;

use super::morsel_debug_enabled;

/// Configuration for morsel-driven execution.
///
/// Supports per-operation morsel sizes based on benchmark data showing that
/// different operations perform optimally with different morsel sizes:
///
/// - **Filter/Aggregate**: Smaller sizes (2K) improve cache locality
/// - **GROUP BY**: Smaller sizes (2K) benefit from faster hash table merges
/// - **Join**: Medium sizes (4K) balance hash table operations
/// - **Sort**: Larger sizes (8K) improve merge phase efficiency
/// - **Scan**: Larger sizes (8K) optimize sequential I/O
///
/// DuckDB uses 2048 for SIMD vectorization alignment (32 x 64-byte AVX-512 elements).
#[derive(Debug, Clone)]
pub struct MorselConfig {
    /// Default morsel size (used when no operation-specific size applies)
    pub morsel_size: usize,
    /// Morsel size for filter operations (default: 2048)
    pub filter_size: usize,
    /// Morsel size for GROUP BY operations (default: 2048)
    pub group_by_size: usize,
    /// Morsel size for hash join build phase (default: 4096)
    pub join_build_size: usize,
    /// Morsel size for hash join probe phase (default: 4096)
    pub join_probe_size: usize,
    /// Morsel size for sort operations (default: 8192)
    pub sort_size: usize,
    /// Morsel size for scan/materialize operations (default: 8192)
    pub scan_size: usize,
    /// Morsel size for aggregate operations (default: 2048)
    pub aggregate_size: usize,
}

/// Target cache size for morsel data (2MB = typical L3 cache slice)
pub(super) const TARGET_CACHE_BYTES: usize = 2 * 1024 * 1024;

/// Minimum morsel size to amortize work-stealing overhead
pub(super) const MIN_MORSEL_SIZE: usize = 10_000;

/// Maximum morsel size to ensure enough morsels for load balancing
pub(super) const MAX_MORSEL_SIZE: usize = 100_000;

/// Default morsel size when no hints are available
pub(super) const DEFAULT_MORSEL_SIZE: usize = 50_000;

// Per-operation optimal morsel sizes based on benchmark data (TPC-H SF 0.1, 8 threads)
// See issue #4282 and docs/performance/MORSEL_SIZE_INVESTIGATION.md

/// Default filter morsel size - smaller for better cache locality
/// Benchmark: Q1 aggregation 46.7ms (1K) vs 50.3ms (50K) = ~7% improvement
pub(super) const DEFAULT_FILTER_SIZE: usize = 2048;

/// Default GROUP BY morsel size - smaller reduces hash table merge overhead
/// Benefits from same cache locality as filter operations
pub(super) const DEFAULT_GROUP_BY_SIZE: usize = 2048;

/// Default hash join build morsel size - medium size for hash table insertion
/// Benchmark: Q5 join 281ms (2K) vs 286ms (50K) = ~2% improvement
pub(super) const DEFAULT_JOIN_BUILD_SIZE: usize = 4096;

/// Default hash join probe morsel size - medium size for hash table lookups
pub(super) const DEFAULT_JOIN_PROBE_SIZE: usize = 4096;

/// Default sort morsel size - larger for efficient merge phases
/// Sort shows good parallel scaling, larger morsels reduce merge depth
pub(super) const DEFAULT_SORT_SIZE: usize = 8192;

/// Default scan morsel size - larger for sequential I/O efficiency
/// Scan is memory-bandwidth limited, larger morsels amortize overhead
pub(super) const DEFAULT_SCAN_SIZE: usize = 8192;

/// Default aggregate morsel size - smaller for cache locality
/// Similar characteristics to filter operations
pub(super) const DEFAULT_AGGREGATE_SIZE: usize = 2048;

impl MorselConfig {
    /// Create a new configuration with the given morsel size for all operations.
    ///
    /// This uses the same size for all operations, which is useful for testing
    /// or when you want uniform behavior. For production use, prefer `optimal()`
    /// which uses per-operation sizes based on benchmark data.
    pub fn new(morsel_size: usize) -> Self {
        Self {
            morsel_size,
            filter_size: morsel_size,
            group_by_size: morsel_size,
            join_build_size: morsel_size,
            join_probe_size: morsel_size,
            sort_size: morsel_size,
            scan_size: morsel_size,
            aggregate_size: morsel_size,
        }
    }

    /// Create a new configuration with per-operation optimal sizes.
    ///
    /// Uses different morsel sizes for each operation based on benchmark data:
    /// - Filter/Aggregate: 2048 (cache locality)
    /// - GROUP BY: 2048 (hash table merge efficiency)
    /// - Join: 4096 (hash table operations)
    /// - Sort: 8192 (merge phase efficiency)
    /// - Scan: 8192 (sequential I/O)
    pub fn with_per_operation_sizes() -> Self {
        Self {
            morsel_size: DEFAULT_MORSEL_SIZE,
            filter_size: DEFAULT_FILTER_SIZE,
            group_by_size: DEFAULT_GROUP_BY_SIZE,
            join_build_size: DEFAULT_JOIN_BUILD_SIZE,
            join_probe_size: DEFAULT_JOIN_PROBE_SIZE,
            sort_size: DEFAULT_SORT_SIZE,
            scan_size: DEFAULT_SCAN_SIZE,
            aggregate_size: DEFAULT_AGGREGATE_SIZE,
        }
    }

    /// Calculate optimal morsel size based on hardware characteristics.
    ///
    /// Uses per-operation morsel sizes based on benchmark data (see issue #4282).
    /// Supports `MORSEL_SIZE` environment variable override for all operations.
    ///
    /// When `MORSEL_SIZE` is set, that value is used for all operations (uniform mode).
    /// Otherwise, per-operation optimal sizes are used.
    pub fn optimal() -> Self {
        // Check for user override - if set, use uniform size for all operations
        if let Ok(size_str) = std::env::var("MORSEL_SIZE") {
            let morsel_size =
                size_str.parse::<usize>().unwrap_or(DEFAULT_MORSEL_SIZE).clamp(1000, 500_000);
            return Self::new(morsel_size);
        }

        // Use per-operation optimal sizes
        Self::with_per_operation_sizes()
    }

    /// Create an adaptive configuration based on estimated row width in bytes.
    ///
    /// Adjusts morsel size to maintain consistent L3 cache occupancy regardless
    /// of row width. Wide rows get smaller morsels, narrow rows get larger morsels.
    ///
    /// # Arguments
    ///
    /// * `avg_row_bytes` - Estimated average size of each row in bytes
    ///
    /// # Example
    ///
    /// ```
    /// use vibesql_executor::select::morsel::MorselConfig;
    ///
    /// // For wide rows (~500 bytes each), use smaller morsels
    /// let wide_config = MorselConfig::for_row_width(500);
    ///
    /// // For narrow rows (~20 bytes each), use larger morsels
    /// let narrow_config = MorselConfig::for_row_width(20);
    ///
    /// // Narrow rows get larger morsels than wide rows
    /// assert!(narrow_config.morsel_size > wide_config.morsel_size);
    /// ```
    pub fn for_row_width(avg_row_bytes: usize) -> Self {
        // Avoid division by zero, use minimum of 1 byte per row
        let row_bytes = avg_row_bytes.max(1);

        // Calculate morsel size to fit TARGET_CACHE_BYTES
        let morsel_size = (TARGET_CACHE_BYTES / row_bytes).clamp(MIN_MORSEL_SIZE, MAX_MORSEL_SIZE);

        if morsel_debug_enabled() {
            eprintln!(
                "[MORSEL] Adaptive sizing: {} bytes/row -> {} rows/morsel",
                row_bytes, morsel_size
            );
        }

        // Use uniform size for row-width-adaptive configs
        Self::new(morsel_size)
    }

    /// Create an adaptive configuration based on a schema (list of column types).
    ///
    /// Estimates row width from the schema and adjusts morsel size accordingly.
    /// This is the recommended method when schema information is available.
    ///
    /// # Arguments
    ///
    /// * `schema` - Slice of column data types in the row
    ///
    /// # Example
    ///
    /// ```
    /// use vibesql_executor::select::morsel::MorselConfig;
    /// use vibesql_types::DataType;
    ///
    /// let schema = [DataType::Integer, DataType::Varchar { max_length: Some(100) }, DataType::Date];
    /// let config = MorselConfig::for_schema(&schema);
    /// assert!(config.morsel_size > 0);
    /// ```
    pub fn for_schema(schema: &[DataType]) -> Self {
        if schema.is_empty() {
            return Self::optimal();
        }

        // Sum estimated sizes for all columns, plus Row struct overhead
        const ROW_OVERHEAD: usize = 24; // Vec header for values
        let row_bytes: usize =
            ROW_OVERHEAD + schema.iter().map(|dt| dt.estimated_size_bytes()).sum::<usize>();

        Self::for_row_width(row_bytes)
    }

    /// Create an adaptive configuration based on estimated filter selectivity.
    ///
    /// For filter operations with known selectivity, adjusts morsel size:
    /// - Low selectivity (few rows pass) -> larger morsels to reduce overhead
    /// - High selectivity (many rows pass) -> smaller morsels for better balancing
    ///
    /// # Arguments
    ///
    /// * `selectivity` - Fraction of rows expected to pass the filter (0.0 to 1.0)
    ///
    /// # Example
    ///
    /// ```
    /// use vibesql_executor::select::morsel::MorselConfig;
    ///
    /// // For a highly selective filter (1% pass rate), use larger morsels
    /// let selective = MorselConfig::for_selectivity(0.01);
    ///
    /// // For a low selectivity filter (90% pass rate), use smaller morsels
    /// let unselective = MorselConfig::for_selectivity(0.90);
    ///
    /// // More selective filters get larger morsels
    /// assert!(selective.morsel_size > unselective.morsel_size);
    /// ```
    pub fn for_selectivity(selectivity: f64) -> Self {
        // Clamp selectivity to valid range
        let sel = selectivity.clamp(0.001, 1.0);

        // For very low selectivity, increase morsel size to reduce overhead
        // The idea: if only 1% of rows pass, we need larger input morsels
        // to get meaningful output morsels
        let adjusted = if sel < 0.1 {
            // Scale inversely with selectivity, but cap at 2x default
            ((DEFAULT_MORSEL_SIZE as f64) / sel).min((MAX_MORSEL_SIZE * 2) as f64) as usize
        } else {
            DEFAULT_MORSEL_SIZE
        };

        let morsel_size = adjusted.clamp(MIN_MORSEL_SIZE, MAX_MORSEL_SIZE * 2);

        if morsel_debug_enabled() {
            eprintln!(
                "[MORSEL] Selectivity-based sizing: {:.1}% selectivity -> {} rows/morsel",
                sel * 100.0,
                morsel_size
            );
        }

        // Use uniform size for selectivity-adaptive configs
        Self::new(morsel_size)
    }

    /// Create an adaptive configuration combining row width and selectivity hints.
    ///
    /// This is the most accurate method when both schema and selectivity estimates
    /// are available (e.g., from query optimizer statistics).
    ///
    /// # Arguments
    ///
    /// * `schema` - Slice of column data types
    /// * `selectivity` - Optional filter selectivity (0.0 to 1.0)
    ///
    /// # Example
    ///
    /// ```
    /// use vibesql_executor::select::morsel::MorselConfig;
    /// use vibesql_types::DataType;
    ///
    /// let schema = [DataType::Integer, DataType::Bigint];
    /// let config = MorselConfig::adaptive(&schema, Some(0.05));
    /// assert!(config.morsel_size > 0);
    /// ```
    pub fn adaptive(schema: &[DataType], selectivity: Option<f64>) -> Self {
        // Start with schema-based sizing
        let base_config = Self::for_schema(schema);

        // Adjust for selectivity if provided
        match selectivity {
            Some(sel) if sel < 0.1 => {
                // For low selectivity, scale up from the schema-based size
                let adjusted = ((base_config.morsel_size as f64) / sel.clamp(0.001, 1.0))
                    .min((MAX_MORSEL_SIZE * 2) as f64) as usize;
                let morsel_size = adjusted.clamp(MIN_MORSEL_SIZE, MAX_MORSEL_SIZE * 2);

                if morsel_debug_enabled() {
                    eprintln!(
                        "[MORSEL] Adaptive sizing: schema={} bytes, selectivity={:.1}% -> {} rows/morsel",
                        schema.iter().map(|dt| dt.estimated_size_bytes()).sum::<usize>(),
                        sel * 100.0,
                        morsel_size
                    );
                }

                // Use uniform size for adaptive configs
                Self::new(morsel_size)
            }
            _ => base_config,
        }
    }
}

impl Default for MorselConfig {
    fn default() -> Self {
        Self::optimal()
    }
}

/// Global morsel configuration, initialized once on first access.
static GLOBAL_CONFIG: std::sync::OnceLock<MorselConfig> = std::sync::OnceLock::new();

/// Get the global morsel configuration.
pub fn global_config() -> &'static MorselConfig {
    GLOBAL_CONFIG.get_or_init(MorselConfig::optimal)
}
