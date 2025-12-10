//! Morsel-driven parallel execution with work-stealing
//!
//! This module implements the morsel-driven parallelism model from Leis et al. (SIGMOD 2014).
//! Instead of static partitioning (dividing rows into N equal chunks), morsels provide
//! dynamic load balancing through work-stealing, enabling near-linear scaling to 16+ cores.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────────┐
//! │  Traditional (Static)         vs    Morsel-Driven           │
//! ├─────────────────────────────────────────────────────────────┤
//! │  Divide into N equal parts    │    Morsel queue (~50K rows) │
//! │  at query start               │    Workers steal as needed  │
//! │  (fixed assignment)           │    (dynamic load balancing) │
//! └─────────────────────────────────────────────────────────────┘
//! ```
//!
//! # Benefits
//!
//! - **Load Balancing**: If one morsel has expensive rows (complex expressions, many joins),
//!   other workers can steal remaining morsels instead of sitting idle.
//! - **Cache Efficiency**: Morsel size is tuned to L3 cache for optimal memory bandwidth.
//! - **Scalability**: Near-linear scaling to 16+ cores (>85% efficiency).
//!
//! # Usage
//!
//! ```ignore
//! use crate::select::morsel::{morsel_parallel_filter, MorselConfig};
//!
//! let config = MorselConfig::default();
//! let results = morsel_parallel_filter(&rows, &config, |row| predicate(row));
//! ```
//!
//! # References
//!
//! - [Leis et al., SIGMOD 2014](https://dl.acm.org/doi/10.1145/2588555.2610507)

use crossbeam_deque::{Injector, Steal, Worker};
use std::cmp::Ordering as CmpOrdering;
use std::collections::BinaryHeap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use vibesql_storage::Row;
use vibesql_types::DataType;

/// Environment variable to enable morsel execution debug logging
const MORSEL_DEBUG_ENV: &str = "MORSEL_DEBUG";

/// Check if morsel debug logging is enabled
fn morsel_debug_enabled() -> bool {
    std::env::var(MORSEL_DEBUG_ENV).is_ok()
}

/// A unit of work containing a slice of rows to process.
///
/// Morsels are the fundamental unit of work distribution in morsel-driven execution.
/// Each morsel contains a contiguous slice of rows sized to fit in L3 cache.
#[derive(Debug, Clone)]
pub struct Morsel {
    /// Starting row index in source data
    start_idx: usize,
    /// Number of rows in this morsel
    row_count: usize,
}

impl Morsel {
    /// Create a new morsel with the given start index and row count
    pub fn new(start_idx: usize, row_count: usize) -> Self {
        Self { start_idx, row_count }
    }

    /// Get the starting index of this morsel in the source data
    #[inline]
    pub fn start_idx(&self) -> usize {
        self.start_idx
    }

    /// Get the number of rows in this morsel
    #[inline]
    pub fn row_count(&self) -> usize {
        self.row_count
    }

    /// Get the ending index (exclusive) of this morsel in the source data
    #[inline]
    pub fn end_idx(&self) -> usize {
        self.start_idx + self.row_count
    }

    /// Extract the rows for this morsel from the source data
    #[inline]
    pub fn rows<'a>(&self, source: &'a [Row]) -> &'a [Row] {
        &source[self.start_idx..self.end_idx()]
    }
}

/// Configuration for morsel-driven execution.
#[derive(Debug, Clone)]
pub struct MorselConfig {
    /// Morsel size (number of rows per morsel)
    pub morsel_size: usize,
}

/// Target cache size for morsel data (2MB = typical L3 cache slice)
const TARGET_CACHE_BYTES: usize = 2 * 1024 * 1024;

/// Minimum morsel size to amortize work-stealing overhead
const MIN_MORSEL_SIZE: usize = 10_000;

/// Maximum morsel size to ensure enough morsels for load balancing
const MAX_MORSEL_SIZE: usize = 100_000;

/// Default morsel size when no hints are available
const DEFAULT_MORSEL_SIZE: usize = 50_000;

impl MorselConfig {
    /// Create a new configuration with the given morsel size.
    pub fn new(morsel_size: usize) -> Self {
        Self { morsel_size }
    }

    /// Calculate optimal morsel size based on hardware characteristics.
    ///
    /// The goal is to make each morsel fit in L3 cache while being large enough
    /// to amortize work-stealing overhead.
    ///
    /// Heuristics:
    /// - Target: ~1MB of row data per morsel (fits in typical L3 cache slice)
    /// - Assume average row size of ~100 bytes (varies by query)
    /// - Minimum: 10,000 rows (amortize stealing overhead)
    /// - Maximum: 100,000 rows (ensure enough morsels for load balancing)
    pub fn optimal() -> Self {
        // Check for user override
        let morsel_size = if let Ok(size_str) = std::env::var("MORSEL_SIZE") {
            size_str.parse::<usize>().unwrap_or(DEFAULT_MORSEL_SIZE).max(1000).min(500_000)
        } else {
            // Default: 50,000 rows
            // This balances:
            // - Cache efficiency (50K rows * 100 bytes = 5MB, fits L3)
            // - Load balancing (enough morsels for 16+ cores)
            // - Stealing overhead (large enough to amortize)
            DEFAULT_MORSEL_SIZE
        };

        Self { morsel_size }
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
    /// ```ignore
    /// // For wide rows (~500 bytes each), use smaller morsels
    /// let config = MorselConfig::for_row_width(500);
    /// assert!(config.morsel_size < 50_000);
    ///
    /// // For narrow rows (~20 bytes each), use larger morsels
    /// let config = MorselConfig::for_row_width(20);
    /// assert!(config.morsel_size > 50_000);
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

        Self { morsel_size }
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
    /// ```ignore
    /// use vibesql_types::DataType;
    ///
    /// let schema = [
    ///     DataType::Integer,
    ///     DataType::Varchar { max_length: Some(100) },
    ///     DataType::Date,
    /// ];
    /// let config = MorselConfig::for_schema(&schema);
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
    /// ```ignore
    /// // For a highly selective filter (1% pass rate), use larger morsels
    /// let config = MorselConfig::for_selectivity(0.01);
    /// assert!(config.morsel_size > 50_000);
    ///
    /// // For a low selectivity filter (90% pass rate), use default sizing
    /// let config = MorselConfig::for_selectivity(0.90);
    /// assert!(config.morsel_size <= 50_000);
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

        Self { morsel_size }
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
    /// ```ignore
    /// use vibesql_types::DataType;
    ///
    /// let schema = [DataType::Integer, DataType::Bigint];
    /// let config = MorselConfig::adaptive(&schema, Some(0.05));
    /// ```
    pub fn adaptive(schema: &[DataType], selectivity: Option<f64>) -> Self {
        // Start with schema-based sizing
        let base_config = Self::for_schema(schema);

        // Adjust for selectivity if provided
        match selectivity {
            Some(sel) if sel < 0.1 => {
                // For low selectivity, scale up from the schema-based size
                let adjusted =
                    ((base_config.morsel_size as f64) / sel.clamp(0.001, 1.0)).min((MAX_MORSEL_SIZE * 2) as f64)
                        as usize;
                let morsel_size = adjusted.clamp(MIN_MORSEL_SIZE, MAX_MORSEL_SIZE * 2);

                if morsel_debug_enabled() {
                    eprintln!(
                        "[MORSEL] Adaptive sizing: schema={} bytes, selectivity={:.1}% -> {} rows/morsel",
                        schema.iter().map(|dt| dt.estimated_size_bytes()).sum::<usize>(),
                        sel * 100.0,
                        morsel_size
                    );
                }

                Self { morsel_size }
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

/// Create morsels from a row count.
fn create_morsels(total_rows: usize, morsel_size: usize) -> Vec<Morsel> {
    let mut morsels = Vec::with_capacity(total_rows.div_ceil(morsel_size));
    let mut start = 0;

    while start < total_rows {
        let count = (total_rows - start).min(morsel_size);
        morsels.push(Morsel::new(start, count));
        start += count;
    }

    morsels
}

/// Helper to steal a morsel from the injector queue
fn steal_morsel(injector: &Injector<Morsel>, worker: &Worker<Morsel>) -> Option<Morsel> {
    // Try local queue first
    worker.pop().or_else(|| {
        // Try to steal from global injector
        loop {
            match injector.steal() {
                Steal::Success(m) => return Some(m),
                Steal::Empty => return None,
                Steal::Retry => continue,
            }
        }
    })
}

/// Morsel-driven parallel filter with work-stealing.
///
/// Uses a global injector queue for work distribution. Workers steal morsels
/// from the queue and process them independently, providing dynamic load balancing.
///
/// # Arguments
///
/// - `rows`: Source data to filter
/// - `config`: Morsel configuration
/// - `predicate`: Predicate function to test each row
///
/// # Returns
///
/// Vector of rows that satisfy the predicate, in original order.
pub fn morsel_parallel_filter<F>(rows: &[Row], config: &MorselConfig, predicate: F) -> Vec<Row>
where
    F: Fn(&Row) -> bool + Sync + Send,
{
    if rows.is_empty() {
        return Vec::new();
    }

    // For small datasets, process directly (avoid morsel overhead)
    if rows.len() < config.morsel_size {
        return rows.iter().filter(|r| predicate(r)).cloned().collect();
    }

    // Create morsels
    let morsels = create_morsels(rows.len(), config.morsel_size);
    let morsel_count = morsels.len();

    if morsel_debug_enabled() {
        eprintln!(
            "[MORSEL] Filter: {} morsels for {} rows (size={})",
            morsel_count,
            rows.len(),
            config.morsel_size
        );
    }

    // Create global injector queue
    let injector: Injector<Morsel> = Injector::new();
    for morsel in morsels {
        injector.push(morsel);
    }

    // Results storage shared across threads
    let results: Arc<std::sync::Mutex<Vec<(usize, Vec<Row>)>>> =
        Arc::new(std::sync::Mutex::new(Vec::with_capacity(morsel_count)));
    let results_count = Arc::new(AtomicUsize::new(0));

    // Process morsels in parallel using rayon's thread pool
    rayon::scope(|s| {
        let num_threads = rayon::current_num_threads();

        for _ in 0..num_threads {
            let injector_ref = &injector;
            let predicate_ref = &predicate;
            let results_ref = results.clone();
            let results_count_ref = results_count.clone();

            s.spawn(move |_| {
                // Create thread-local worker queue
                let worker: Worker<Morsel> = Worker::new_fifo();

                while let Some(m) = steal_morsel(injector_ref, &worker) {
                    let start_idx = m.start_idx();
                    let morsel_rows = m.rows(rows);
                    let filtered: Vec<Row> =
                        morsel_rows.iter().filter(|r| predicate_ref(r)).cloned().collect();

                    if morsel_debug_enabled() {
                        eprintln!(
                            "[MORSEL] Thread processed morsel at {} ({} rows -> {} results)",
                            start_idx,
                            morsel_rows.len(),
                            filtered.len()
                        );
                    }

                    // Store result
                    results_ref.lock().unwrap().push((start_idx, filtered));
                    results_count_ref.fetch_add(1, Ordering::SeqCst);
                }
            });
        }
    });

    // Extract results after scope completes (all threads have finished)
    let mut sorted_results = Arc::try_unwrap(results)
        .expect("all threads should have completed")
        .into_inner()
        .expect("mutex not poisoned");

    // Sort by start index to maintain row order
    sorted_results.sort_by_key(|(start_idx, _)| *start_idx);

    // Flatten results
    let estimated_total: usize = sorted_results.iter().map(|(_, r)| r.len()).sum();
    let mut final_results = Vec::with_capacity(estimated_total);
    for (_, result) in sorted_results {
        final_results.extend(result);
    }

    if morsel_debug_enabled() {
        eprintln!(
            "[MORSEL] Filter complete: {} morsels, {} results",
            morsel_count,
            final_results.len()
        );
    }

    final_results
}

/// Morsel-driven parallel map with work-stealing.
///
/// Transforms each row using the provided function, with dynamic load balancing.
pub fn morsel_parallel_map<F>(rows: &[Row], config: &MorselConfig, transform: F) -> Vec<Row>
where
    F: Fn(&Row) -> Row + Sync + Send,
{
    if rows.is_empty() {
        return Vec::new();
    }

    // For small datasets, process directly
    if rows.len() < config.morsel_size {
        return rows.iter().map(&transform).collect();
    }

    // Create morsels
    let morsels = create_morsels(rows.len(), config.morsel_size);
    let morsel_count = morsels.len();

    // Create global injector queue
    let injector: Injector<Morsel> = Injector::new();
    for morsel in morsels {
        injector.push(morsel);
    }

    // Results storage shared across threads
    let results: Arc<std::sync::Mutex<Vec<(usize, Vec<Row>)>>> =
        Arc::new(std::sync::Mutex::new(Vec::with_capacity(morsel_count)));

    // Process morsels in parallel
    rayon::scope(|s| {
        let num_threads = rayon::current_num_threads();

        for _ in 0..num_threads {
            let injector_ref = &injector;
            let transform_ref = &transform;
            let results_ref = results.clone();

            s.spawn(move |_| {
                let worker: Worker<Morsel> = Worker::new_fifo();

                while let Some(m) = steal_morsel(injector_ref, &worker) {
                    let start_idx = m.start_idx();
                    let morsel_rows = m.rows(rows);
                    let transformed: Vec<Row> = morsel_rows.iter().map(transform_ref).collect();
                    results_ref.lock().unwrap().push((start_idx, transformed));
                }
            });
        }
    });

    // Extract results after scope completes
    let mut sorted_results = Arc::try_unwrap(results)
        .expect("all threads should have completed")
        .into_inner()
        .expect("mutex not poisoned");

    // Sort by start index and flatten
    sorted_results.sort_by_key(|(start_idx, _)| *start_idx);

    let total: usize = sorted_results.iter().map(|(_, r)| r.len()).sum();
    let mut final_results = Vec::with_capacity(total);
    for (_, result) in sorted_results {
        final_results.extend(result);
    }

    final_results
}

/// Morsel-driven parallel filter-map with work-stealing.
///
/// Combines filtering and transformation in a single pass.
pub fn morsel_parallel_filter_map<F>(
    rows: &[Row],
    config: &MorselConfig,
    filter_map: F,
) -> Vec<Row>
where
    F: Fn(&Row) -> Option<Row> + Sync + Send,
{
    if rows.is_empty() {
        return Vec::new();
    }

    // For small datasets, process directly
    if rows.len() < config.morsel_size {
        return rows.iter().filter_map(&filter_map).collect();
    }

    // Create morsels
    let morsels = create_morsels(rows.len(), config.morsel_size);
    let morsel_count = morsels.len();

    // Create global injector queue
    let injector: Injector<Morsel> = Injector::new();
    for morsel in morsels {
        injector.push(morsel);
    }

    // Results storage shared across threads
    let results: Arc<std::sync::Mutex<Vec<(usize, Vec<Row>)>>> =
        Arc::new(std::sync::Mutex::new(Vec::with_capacity(morsel_count)));

    // Process morsels in parallel
    rayon::scope(|s| {
        let num_threads = rayon::current_num_threads();

        for _ in 0..num_threads {
            let injector_ref = &injector;
            let filter_map_ref = &filter_map;
            let results_ref = results.clone();

            s.spawn(move |_| {
                let worker: Worker<Morsel> = Worker::new_fifo();

                while let Some(m) = steal_morsel(injector_ref, &worker) {
                    let start_idx = m.start_idx();
                    let morsel_rows = m.rows(rows);
                    let filtered: Vec<Row> =
                        morsel_rows.iter().filter_map(filter_map_ref).collect();
                    results_ref.lock().unwrap().push((start_idx, filtered));
                }
            });
        }
    });

    // Extract results after scope completes
    let mut sorted_results = Arc::try_unwrap(results)
        .expect("all threads should have completed")
        .into_inner()
        .expect("mutex not poisoned");

    // Sort by start index and flatten
    sorted_results.sort_by_key(|(start_idx, _)| *start_idx);

    let total: usize = sorted_results.iter().map(|(_, r)| r.len()).sum();
    let mut final_results = Vec::with_capacity(total);
    for (_, result) in sorted_results {
        final_results.extend(result);
    }

    final_results
}

/// Morsel-driven parallel reduce with work-stealing.
///
/// Processes rows in morsels and reduces results using the provided merge function.
/// Useful for aggregations like hash table building.
pub fn morsel_parallel_reduce<F, M, R>(
    rows: &[Row],
    config: &MorselConfig,
    operation: F,
    merge: M,
    initial: R,
) -> R
where
    F: Fn(&[Row]) -> R + Sync + Send,
    M: Fn(R, R) -> R + Sync + Send,
    R: Send,
{
    if rows.is_empty() {
        return initial;
    }

    // For small datasets, process directly
    if rows.len() < config.morsel_size {
        return operation(rows);
    }

    // Create morsels
    let morsels = create_morsels(rows.len(), config.morsel_size);

    // Create global injector queue
    let injector: Injector<Morsel> = Injector::new();
    for morsel in morsels {
        injector.push(morsel);
    }

    // Results storage shared across threads
    let results: Arc<std::sync::Mutex<Vec<R>>> =
        Arc::new(std::sync::Mutex::new(Vec::new()));

    // Process morsels in parallel
    rayon::scope(|s| {
        let num_threads = rayon::current_num_threads();

        for _ in 0..num_threads {
            let injector_ref = &injector;
            let operation_ref = &operation;
            let results_ref = results.clone();

            s.spawn(move |_| {
                let worker: Worker<Morsel> = Worker::new_fifo();

                while let Some(m) = steal_morsel(injector_ref, &worker) {
                    let morsel_rows = m.rows(rows);
                    let result = operation_ref(morsel_rows);
                    results_ref.lock().expect("mutex poisoned").push(result);
                }
            });
        }
    });

    // Extract results after scope completes
    let final_results = Arc::try_unwrap(results)
        .ok()
        .expect("all threads should have completed")
        .into_inner()
        .expect("mutex not poisoned");

    // Reduce all results
    final_results.into_iter().fold(initial, &merge)
}

/// Morsel-driven parallel GROUP BY with work-stealing.
///
/// Groups rows by key computed from the `key_fn`, using morsel-driven parallelism
/// with work-stealing for dynamic load balancing across threads.
///
/// # Arguments
///
/// - `rows`: Input rows to group
/// - `config`: Morsel configuration
/// - `key_fn`: Function to compute the group key from a row
/// - `merge`: Function to merge two hash maps of groups
///
/// # Returns
///
/// HashMap mapping group keys to vectors of rows in each group.
///
/// # Example
///
/// ```ignore
/// let groups = morsel_parallel_group(
///     &rows,
///     &config,
///     |row| vec![row.values[0].clone()], // Group by first column
///     |a, b| merge_hash_maps(a, b),
/// );
/// ```
pub fn morsel_parallel_group<K, M>(
    rows: &[Row],
    config: &MorselConfig,
    key_fn: K,
    merge: M,
) -> ahash::AHashMap<Vec<vibesql_types::SqlValue>, Vec<Row>>
where
    K: Fn(&Row) -> Vec<vibesql_types::SqlValue> + Sync + Send,
    M: Fn(
            ahash::AHashMap<Vec<vibesql_types::SqlValue>, Vec<Row>>,
            ahash::AHashMap<Vec<vibesql_types::SqlValue>, Vec<Row>>,
        ) -> ahash::AHashMap<Vec<vibesql_types::SqlValue>, Vec<Row>>
        + Sync
        + Send,
{
    use ahash::AHashMap;

    if rows.is_empty() {
        return AHashMap::new();
    }

    // For small datasets, process directly
    if rows.len() < config.morsel_size {
        let estimated_groups = (rows.len() / 10).max(16);
        let mut groups: AHashMap<Vec<vibesql_types::SqlValue>, Vec<Row>> =
            AHashMap::with_capacity(estimated_groups);

        for row in rows {
            let key = key_fn(row);
            groups.entry(key).or_default().push(row.clone());
        }
        return groups;
    }

    // Create morsels
    let morsels = create_morsels(rows.len(), config.morsel_size);
    let morsel_count = morsels.len();

    if morsel_debug_enabled() {
        eprintln!(
            "[MORSEL] Group: {} morsels for {} rows (size={})",
            morsel_count,
            rows.len(),
            config.morsel_size
        );
    }

    // Create global injector queue
    let injector: Injector<Morsel> = Injector::new();
    for morsel in morsels {
        injector.push(morsel);
    }

    // Results storage shared across threads
    let results: Arc<std::sync::Mutex<Vec<AHashMap<Vec<vibesql_types::SqlValue>, Vec<Row>>>>> =
        Arc::new(std::sync::Mutex::new(Vec::with_capacity(morsel_count)));

    // Process morsels in parallel
    rayon::scope(|s| {
        let num_threads = rayon::current_num_threads();

        for _ in 0..num_threads {
            let injector_ref = &injector;
            let key_fn_ref = &key_fn;
            let results_ref = results.clone();

            s.spawn(move |_| {
                let worker: Worker<Morsel> = Worker::new_fifo();
                let estimated_groups = (config.morsel_size / 10).max(16);
                let mut local_groups: AHashMap<Vec<vibesql_types::SqlValue>, Vec<Row>> =
                    AHashMap::with_capacity(estimated_groups);

                while let Some(m) = steal_morsel(injector_ref, &worker) {
                    let morsel_rows = m.rows(rows);

                    for row in morsel_rows {
                        let key = key_fn_ref(row);
                        local_groups.entry(key).or_default().push(row.clone());
                    }
                }

                if !local_groups.is_empty() {
                    results_ref.lock().unwrap().push(local_groups);
                }
            });
        }
    });

    // Extract results after scope completes
    let thread_results = Arc::try_unwrap(results)
        .ok()
        .expect("all threads should have completed")
        .into_inner()
        .expect("mutex not poisoned");

    if morsel_debug_enabled() {
        eprintln!(
            "[MORSEL] Group complete: {} morsels, {} thread results",
            morsel_count,
            thread_results.len()
        );
    }

    // Merge all thread-local maps
    thread_results
        .into_iter()
        .fold(AHashMap::new(), |acc, map| merge(acc, map))
}

/// Morsel-driven parallel sort with work-stealing.
///
/// Sorts rows using a two-phase approach:
/// 1. **Phase 1 (Parallel)**: Workers steal morsels and sort them locally
/// 2. **Phase 2 (Sequential)**: K-way merge of sorted morsels using a min-heap
///
/// This provides dynamic load balancing during the sort phase while maintaining
/// O(n log k) merge complexity where k = number of morsels.
///
/// # Arguments
///
/// - `rows`: Source data to sort
/// - `config`: Morsel configuration
/// - `compare`: Comparison function for ordering rows
///
/// # Returns
///
/// Vector of rows sorted according to the comparison function.
///
/// # Example
///
/// ```ignore
/// let sorted = morsel_parallel_sort(&rows, &config, |a, b| {
///     a.values[0].partial_cmp(&b.values[0]).unwrap_or(CmpOrdering::Equal)
/// });
/// ```
pub fn morsel_parallel_sort<F>(rows: &[Row], config: &MorselConfig, compare: F) -> Vec<Row>
where
    F: Fn(&Row, &Row) -> CmpOrdering + Sync + Send,
{
    if rows.is_empty() {
        return Vec::new();
    }

    // For small datasets, sort directly (avoid morsel overhead)
    if rows.len() < config.morsel_size {
        let mut result = rows.to_vec();
        result.sort_by(&compare);
        return result;
    }

    // Create morsels
    let morsels = create_morsels(rows.len(), config.morsel_size);
    let morsel_count = morsels.len();

    if morsel_debug_enabled() {
        eprintln!(
            "[MORSEL] Sort: {} morsels for {} rows (size={})",
            morsel_count,
            rows.len(),
            config.morsel_size
        );
    }

    // Create global injector queue
    let injector: Injector<Morsel> = Injector::new();
    for morsel in morsels {
        injector.push(morsel);
    }

    // Results storage: (morsel_index, sorted_rows)
    // We track the original morsel order for deterministic merging
    let results: Arc<std::sync::Mutex<Vec<(usize, Vec<Row>)>>> =
        Arc::new(std::sync::Mutex::new(Vec::with_capacity(morsel_count)));
    let morsel_index = Arc::new(AtomicUsize::new(0));

    // Phase 1: Parallel sort of each morsel
    rayon::scope(|s| {
        let num_threads = rayon::current_num_threads();

        for _ in 0..num_threads {
            let injector_ref = &injector;
            let compare_ref = &compare;
            let results_ref = results.clone();
            let morsel_index_ref = morsel_index.clone();

            s.spawn(move |_| {
                let worker: Worker<Morsel> = Worker::new_fifo();

                while let Some(m) = steal_morsel(injector_ref, &worker) {
                    // Get a unique index for this morsel (for merge ordering)
                    let idx = morsel_index_ref.fetch_add(1, Ordering::SeqCst);

                    // Clone and sort the morsel's rows
                    let mut sorted: Vec<Row> = m.rows(rows).to_vec();
                    sorted.sort_by(compare_ref);

                    if morsel_debug_enabled() {
                        eprintln!(
                            "[MORSEL] Thread sorted morsel {} ({} rows)",
                            idx,
                            sorted.len()
                        );
                    }

                    results_ref.lock().unwrap().push((idx, sorted));
                }
            });
        }
    });

    // Extract sorted morsels
    let mut sorted_morsels = Arc::try_unwrap(results)
        .expect("all threads should have completed")
        .into_inner()
        .expect("mutex not poisoned");

    // Sort by morsel index to ensure deterministic merge order
    sorted_morsels.sort_by_key(|(idx, _)| *idx);

    if morsel_debug_enabled() {
        eprintln!(
            "[MORSEL] Sort phase 1 complete: {} sorted morsels",
            sorted_morsels.len()
        );
    }

    // Phase 2: K-way merge using a min-heap
    // For single morsel, return directly
    if sorted_morsels.len() == 1 {
        return sorted_morsels.pop().unwrap().1;
    }

    // Convert to owned vectors for merging
    let sorted_chunks: Vec<Vec<Row>> = sorted_morsels.into_iter().map(|(_, rows)| rows).collect();

    // Use k-way merge with min-heap
    let result = kway_merge(sorted_chunks, &compare);

    if morsel_debug_enabled() {
        eprintln!(
            "[MORSEL] Sort complete: {} total rows",
            result.len()
        );
    }

    result
}

/// K-way merge of sorted vectors using a min-heap.
///
/// Merges k sorted vectors into a single sorted vector in O(n log k) time.
fn kway_merge<F>(sorted_chunks: Vec<Vec<Row>>, compare: &F) -> Vec<Row>
where
    F: Fn(&Row, &Row) -> CmpOrdering,
{
    if sorted_chunks.is_empty() {
        return Vec::new();
    }

    if sorted_chunks.len() == 1 {
        return sorted_chunks.into_iter().next().unwrap();
    }

    // Calculate total size for pre-allocation
    let total_size: usize = sorted_chunks.iter().map(|c| c.len()).sum();
    let mut result = Vec::with_capacity(total_size);

    // Create iterators for each chunk
    let mut iters: Vec<std::vec::IntoIter<Row>> =
        sorted_chunks.into_iter().map(|c| c.into_iter()).collect();

    // Initialize heap with first element from each non-empty chunk
    // We use a max-heap with reversed comparison to simulate min-heap
    let mut heap: BinaryHeap<MergeItem<F>> = BinaryHeap::new();

    for (chunk_idx, iter) in iters.iter_mut().enumerate() {
        if let Some(row) = iter.next() {
            heap.push(MergeItem {
                row,
                chunk_idx,
                compare,
            });
        }
    }

    // Merge by repeatedly taking the smallest element
    while let Some(MergeItem { row, chunk_idx, .. }) = heap.pop() {
        result.push(row);

        // Add next element from the same chunk
        if let Some(next_row) = iters[chunk_idx].next() {
            heap.push(MergeItem {
                row: next_row,
                chunk_idx,
                compare,
            });
        }
    }

    result
}

/// Helper struct for k-way merge heap.
///
/// Implements Ord to work with BinaryHeap (which is a max-heap by default).
/// We reverse the comparison to get min-heap behavior.
struct MergeItem<'a, F> {
    row: Row,
    chunk_idx: usize,
    compare: &'a F,
}

impl<'a, F> PartialEq for MergeItem<'a, F>
where
    F: Fn(&Row, &Row) -> CmpOrdering,
{
    fn eq(&self, other: &Self) -> bool {
        (self.compare)(&self.row, &other.row) == CmpOrdering::Equal
    }
}

impl<'a, F> Eq for MergeItem<'a, F> where F: Fn(&Row, &Row) -> CmpOrdering {}

impl<'a, F> PartialOrd for MergeItem<'a, F>
where
    F: Fn(&Row, &Row) -> CmpOrdering,
{
    fn partial_cmp(&self, other: &Self) -> Option<CmpOrdering> {
        Some(self.cmp(other))
    }
}

impl<'a, F> Ord for MergeItem<'a, F>
where
    F: Fn(&Row, &Row) -> CmpOrdering,
{
    fn cmp(&self, other: &Self) -> CmpOrdering {
        // Reverse comparison for min-heap behavior
        // BinaryHeap is a max-heap, so we flip the comparison
        (self.compare)(&other.row, &self.row)
    }
}

/// Convenience function: morsel sort using global config.
pub fn morsel_sort_by<F>(rows: &[Row], compare: F) -> Vec<Row>
where
    F: Fn(&Row, &Row) -> CmpOrdering + Sync + Send,
{
    morsel_parallel_sort(rows, global_config(), compare)
}

/// Convenience function: morsel filter using global config.
pub fn morsel_filter<F>(rows: &[Row], predicate: F) -> Vec<Row>
where
    F: Fn(&Row) -> bool + Sync + Send,
{
    morsel_parallel_filter(rows, global_config(), predicate)
}

/// Convenience function: morsel map using global config.
pub fn morsel_map<F>(rows: &[Row], transform: F) -> Vec<Row>
where
    F: Fn(&Row) -> Row + Sync + Send,
{
    morsel_parallel_map(rows, global_config(), transform)
}

/// Morsel-driven parallel probe for hash join with SqlValue keys.
///
/// Probes rows against a hash table using work-stealing for dynamic load balancing.
/// Returns index pairs (build_idx, probe_idx) for matched rows.
///
/// # Arguments
///
/// - `probe_rows`: Rows to probe against the hash table
/// - `probe_col_idx`: Column index to use as the probe key
/// - `hash_table`: Hash table mapping SqlValue keys to build row indices
/// - `config`: Morsel configuration
///
/// # Returns
///
/// Vector of (build_idx, probe_idx) pairs for matched rows.
pub fn morsel_parallel_probe_sqlvalue(
    probe_rows: &[Row],
    probe_col_idx: usize,
    hash_table: &ahash::AHashMap<vibesql_types::SqlValue, Vec<usize>>,
    config: &MorselConfig,
) -> Vec<(usize, usize)> {
    use vibesql_types::SqlValue;

    if probe_rows.is_empty() {
        return Vec::new();
    }

    // For small datasets, process directly
    if probe_rows.len() < config.morsel_size {
        let mut pairs = Vec::with_capacity(probe_rows.len());
        for (probe_idx, probe_row) in probe_rows.iter().enumerate() {
            let key = &probe_row.values[probe_col_idx];
            if *key == SqlValue::Null {
                continue;
            }
            if let Some(build_indices) = hash_table.get(key) {
                for &build_idx in build_indices {
                    pairs.push((build_idx, probe_idx));
                }
            }
        }
        return pairs;
    }

    // Create morsels
    let morsels = create_morsels(probe_rows.len(), config.morsel_size);
    let morsel_count = morsels.len();

    if morsel_debug_enabled() {
        eprintln!(
            "[MORSEL] Probe: {} morsels for {} rows (size={})",
            morsel_count,
            probe_rows.len(),
            config.morsel_size
        );
    }

    // Create global injector queue
    let injector: Injector<Morsel> = Injector::new();
    for morsel in morsels {
        injector.push(morsel);
    }

    // Results storage shared across threads
    let results: Arc<std::sync::Mutex<Vec<(usize, Vec<(usize, usize)>)>>> =
        Arc::new(std::sync::Mutex::new(Vec::with_capacity(morsel_count)));

    // Process morsels in parallel
    rayon::scope(|s| {
        let num_threads = rayon::current_num_threads();

        for _ in 0..num_threads {
            let injector_ref = &injector;
            let results_ref = results.clone();

            s.spawn(move |_| {
                let worker: Worker<Morsel> = Worker::new_fifo();
                let mut local_pairs = Vec::with_capacity(config.morsel_size);

                while let Some(m) = steal_morsel(injector_ref, &worker) {
                    let start_idx = m.start_idx();
                    let morsel_rows = m.rows(probe_rows);
                    local_pairs.clear();

                    for (local_idx, probe_row) in morsel_rows.iter().enumerate() {
                        let probe_idx = start_idx + local_idx;
                        let key = &probe_row.values[probe_col_idx];
                        if *key == SqlValue::Null {
                            continue;
                        }
                        if let Some(build_indices) = hash_table.get(key) {
                            for &build_idx in build_indices {
                                local_pairs.push((build_idx, probe_idx));
                            }
                        }
                    }

                    if !local_pairs.is_empty() {
                        results_ref
                            .lock()
                            .unwrap()
                            .push((start_idx, local_pairs.clone()));
                    }
                }
            });
        }
    });

    // Extract results after scope completes
    let mut sorted_results = Arc::try_unwrap(results)
        .expect("all threads should have completed")
        .into_inner()
        .expect("mutex not poisoned");

    // Sort by start index to maintain order, then flatten
    sorted_results.sort_by_key(|(start_idx, _)| *start_idx);

    let total: usize = sorted_results.iter().map(|(_, pairs)| pairs.len()).sum();
    let mut final_pairs = Vec::with_capacity(total);
    for (_, pairs) in sorted_results {
        final_pairs.extend(pairs);
    }

    if morsel_debug_enabled() {
        eprintln!(
            "[MORSEL] Probe complete: {} morsels, {} matches",
            morsel_count,
            final_pairs.len()
        );
    }

    final_pairs
}

#[cfg(test)]
mod tests {
    use super::*;
    use vibesql_types::SqlValue;

    fn create_test_rows(count: usize) -> Vec<Row> {
        (0..count)
            .map(|i| {
                Row::from_vec(vec![
                    SqlValue::Integer(i as i64),
                    SqlValue::Varchar(arcstr::ArcStr::from(format!("row{}", i))),
                ])
            })
            .collect()
    }

    #[test]
    fn test_morsel_creation() {
        let morsel = Morsel::new(100, 50);
        assert_eq!(morsel.start_idx(), 100);
        assert_eq!(morsel.row_count(), 50);
        assert_eq!(morsel.end_idx(), 150);
    }

    #[test]
    fn test_morsel_rows_extraction() {
        let rows = create_test_rows(100);
        let morsel = Morsel::new(10, 20);
        let extracted = morsel.rows(&rows);

        assert_eq!(extracted.len(), 20);
        assert!(matches!(extracted[0].values[0], SqlValue::Integer(10)));
        assert!(matches!(extracted[19].values[0], SqlValue::Integer(29)));
    }

    #[test]
    fn test_create_morsels() {
        let morsels = create_morsels(1000, 300);
        assert_eq!(morsels.len(), 4); // 300 + 300 + 300 + 100

        assert_eq!(morsels[0].start_idx(), 0);
        assert_eq!(morsels[0].row_count(), 300);
        assert_eq!(morsels[3].start_idx(), 900);
        assert_eq!(morsels[3].row_count(), 100);
    }

    #[test]
    fn test_morsel_filter_small_dataset() {
        let config = MorselConfig::new(100);
        let rows = create_test_rows(50); // Below morsel size

        let filtered = morsel_parallel_filter(&rows, &config, |row| {
            matches!(row.values[0], SqlValue::Integer(x) if x % 2 == 0)
        });

        assert_eq!(filtered.len(), 25); // 0, 2, 4, ..., 48
    }

    #[test]
    fn test_morsel_filter_large_dataset() {
        let config = MorselConfig::new(100);
        let rows = create_test_rows(1000); // Multiple morsels

        let filtered = morsel_parallel_filter(&rows, &config, |row| {
            matches!(row.values[0], SqlValue::Integer(x) if x % 2 == 0)
        });

        assert_eq!(filtered.len(), 500); // Even numbers

        // Verify order is preserved
        for (i, row) in filtered.iter().enumerate() {
            let expected = (i * 2) as i64;
            assert!(matches!(row.values[0], SqlValue::Integer(x) if x == expected));
        }
    }

    #[test]
    fn test_morsel_map() {
        let config = MorselConfig::new(100);
        let rows = create_test_rows(500);

        let transformed = morsel_parallel_map(&rows, &config, |row| {
            let mut new_row = row.clone();
            if let SqlValue::Integer(x) = row.values[0] {
                new_row.values[0] = SqlValue::Integer(x * 2);
            }
            new_row
        });

        assert_eq!(transformed.len(), 500);

        // Verify transformation and order
        for (i, row) in transformed.iter().enumerate() {
            let expected = (i * 2) as i64;
            assert!(matches!(row.values[0], SqlValue::Integer(x) if x == expected));
        }
    }

    #[test]
    fn test_morsel_reduce() {
        let config = MorselConfig::new(100);
        let rows = create_test_rows(500);

        // Sum all integer values
        let sum = morsel_parallel_reduce(
            &rows,
            &config,
            |morsel_rows| {
                morsel_rows
                    .iter()
                    .map(|r| {
                        if let SqlValue::Integer(x) = r.values[0] {
                            x
                        } else {
                            0
                        }
                    })
                    .sum::<i64>()
            },
            |a, b| a + b,
            0i64,
        );

        // Sum of 0..500 = 499 * 500 / 2 = 124750
        assert_eq!(sum, 124750);
    }

    #[test]
    fn test_morsel_filter_empty_input() {
        let config = MorselConfig::new(100);
        let rows: Vec<Row> = Vec::new();

        let filtered = morsel_parallel_filter(&rows, &config, |_| true);
        assert!(filtered.is_empty());
    }

    #[test]
    fn test_global_config() {
        let config = global_config();
        assert!(config.morsel_size >= 1000);
    }

    #[test]
    fn test_convenience_functions() {
        let rows = create_test_rows(100);

        let filtered = morsel_filter(&rows, |row| {
            matches!(row.values[0], SqlValue::Integer(x) if x < 10)
        });
        assert_eq!(filtered.len(), 10);

        let mapped = morsel_map(&rows, |row| row.clone());
        assert_eq!(mapped.len(), 100);
    }

    #[test]
    fn test_morsel_parallel_group() {
        use ahash::AHashMap;

        let config = MorselConfig::new(100);
        // Create rows with values 0..500, grouped by modulo 10
        let rows = create_test_rows(500);

        let groups = morsel_parallel_group(
            &rows,
            &config,
            |row| {
                // Group by value mod 10
                if let SqlValue::Integer(x) = row.values[0] {
                    vec![SqlValue::Integer(x % 10)]
                } else {
                    vec![SqlValue::Null]
                }
            },
            |a: AHashMap<Vec<SqlValue>, Vec<Row>>,
             b: AHashMap<Vec<SqlValue>, Vec<Row>>|
             -> AHashMap<Vec<SqlValue>, Vec<Row>> {
                let mut result = a;
                for (key, mut rows) in b {
                    result.entry(key).or_default().append(&mut rows);
                }
                result
            },
        );

        // Should have 10 groups (0..9)
        assert_eq!(groups.len(), 10);

        // Each group should have 50 rows
        for (_, group_rows) in groups.iter() {
            assert_eq!(group_rows.len(), 50);
        }
    }

    // ==================== Morsel Sort Tests ====================

    #[test]
    fn test_morsel_sort_empty_input() {
        let config = MorselConfig::new(100);
        let rows: Vec<Row> = Vec::new();

        let sorted = morsel_parallel_sort(&rows, &config, |a, b| {
            match (&a.values[0], &b.values[0]) {
                (SqlValue::Integer(x), SqlValue::Integer(y)) => x.cmp(y),
                _ => CmpOrdering::Equal,
            }
        });

        assert!(sorted.is_empty());
    }

    #[test]
    fn test_morsel_sort_small_dataset() {
        let config = MorselConfig::new(100);
        // Create rows in reverse order: 49, 48, ..., 1, 0
        let rows: Vec<Row> = (0..50)
            .rev()
            .map(|i| {
                Row::from_vec(vec![
                    SqlValue::Integer(i as i64),
                    SqlValue::Varchar(arcstr::ArcStr::from(format!("row{}", i))),
                ])
            })
            .collect();

        let sorted = morsel_parallel_sort(&rows, &config, |a, b| {
            match (&a.values[0], &b.values[0]) {
                (SqlValue::Integer(x), SqlValue::Integer(y)) => x.cmp(y),
                _ => CmpOrdering::Equal,
            }
        });

        // Verify sorted in ascending order
        assert_eq!(sorted.len(), 50);
        for (i, row) in sorted.iter().enumerate() {
            assert!(matches!(row.values[0], SqlValue::Integer(x) if x == i as i64));
        }
    }

    #[test]
    fn test_morsel_sort_large_dataset() {
        let config = MorselConfig::new(100); // Small morsel size to force multiple morsels
        // Create rows in reverse order: 999, 998, ..., 1, 0
        let rows: Vec<Row> = (0..1000)
            .rev()
            .map(|i| {
                Row::from_vec(vec![
                    SqlValue::Integer(i as i64),
                    SqlValue::Varchar(arcstr::ArcStr::from(format!("row{}", i))),
                ])
            })
            .collect();

        let sorted = morsel_parallel_sort(&rows, &config, |a, b| {
            match (&a.values[0], &b.values[0]) {
                (SqlValue::Integer(x), SqlValue::Integer(y)) => x.cmp(y),
                _ => CmpOrdering::Equal,
            }
        });

        // Verify sorted in ascending order
        assert_eq!(sorted.len(), 1000);
        for (i, row) in sorted.iter().enumerate() {
            assert!(matches!(row.values[0], SqlValue::Integer(x) if x == i as i64));
        }
    }

    #[test]
    fn test_morsel_sort_descending() {
        let config = MorselConfig::new(100);
        // Create rows in ascending order: 0, 1, 2, ..., 499
        let rows = create_test_rows(500);

        let sorted = morsel_parallel_sort(&rows, &config, |a, b| {
            // Descending order
            match (&a.values[0], &b.values[0]) {
                (SqlValue::Integer(x), SqlValue::Integer(y)) => y.cmp(x),
                _ => CmpOrdering::Equal,
            }
        });

        // Verify sorted in descending order
        assert_eq!(sorted.len(), 500);
        for (i, row) in sorted.iter().enumerate() {
            let expected = (499 - i) as i64;
            assert!(matches!(row.values[0], SqlValue::Integer(x) if x == expected));
        }
    }

    #[test]
    fn test_morsel_sort_with_nulls() {
        let config = MorselConfig::new(100);
        // Create rows with some NULLs interspersed
        let mut rows: Vec<Row> = Vec::new();
        for i in 0..200 {
            if i % 10 == 0 {
                rows.push(Row::from_vec(vec![SqlValue::Null]));
            } else {
                rows.push(Row::from_vec(vec![SqlValue::Integer(i as i64)]));
            }
        }

        // Sort with NULLs last
        let sorted = morsel_parallel_sort(&rows, &config, |a, b| {
            match (&a.values[0], &b.values[0]) {
                (SqlValue::Null, SqlValue::Null) => CmpOrdering::Equal,
                (SqlValue::Null, _) => CmpOrdering::Greater, // NULL sorts last
                (_, SqlValue::Null) => CmpOrdering::Less,
                (SqlValue::Integer(x), SqlValue::Integer(y)) => x.cmp(y),
                _ => CmpOrdering::Equal,
            }
        });

        // Verify: non-NULL values sorted first, then NULLs
        assert_eq!(sorted.len(), 200);

        // Count NULLs (should be 20: 0, 10, 20, ..., 190)
        let null_count = sorted.iter().filter(|r| r.values[0] == SqlValue::Null).count();
        assert_eq!(null_count, 20);

        // Verify NULLs are at the end
        for row in sorted.iter().skip(180) {
            assert_eq!(row.values[0], SqlValue::Null);
        }

        // Verify non-NULLs are sorted ascending before NULLs
        let mut last_val = -1i64;
        for row in sorted.iter().take(180) {
            if let SqlValue::Integer(x) = row.values[0] {
                assert!(x > last_val, "Values should be ascending: {} > {}", x, last_val);
                last_val = x;
            }
        }
    }

    #[test]
    fn test_morsel_sort_multi_key() {
        let config = MorselConfig::new(50);
        // Create rows with two columns: group (0-9) and value (0-99)
        // Multiple rows per group to test stable-like behavior
        let mut rows: Vec<Row> = Vec::new();
        for i in 0..100 {
            rows.push(Row::from_vec(vec![
                SqlValue::Integer((i % 10) as i64), // Group
                SqlValue::Integer(i as i64),        // Value
            ]));
        }

        // Sort by group ASC, then by value DESC within group
        let sorted = morsel_parallel_sort(&rows, &config, |a, b| {
            let group_a = match &a.values[0] { SqlValue::Integer(x) => *x, _ => 0 };
            let group_b = match &b.values[0] { SqlValue::Integer(x) => *x, _ => 0 };
            let val_a = match &a.values[1] { SqlValue::Integer(x) => *x, _ => 0 };
            let val_b = match &b.values[1] { SqlValue::Integer(x) => *x, _ => 0 };

            match group_a.cmp(&group_b) {
                CmpOrdering::Equal => val_b.cmp(&val_a), // DESC within group
                other => other,
            }
        });

        assert_eq!(sorted.len(), 100);

        // Verify: groups are in order 0-9, and within each group values are descending
        let mut current_group = 0i64;
        let mut last_val_in_group = i64::MAX;
        for row in sorted.iter() {
            let group = match &row.values[0] { SqlValue::Integer(x) => *x, _ => 0 };
            let val = match &row.values[1] { SqlValue::Integer(x) => *x, _ => 0 };

            if group != current_group {
                assert!(group > current_group, "Groups should be ascending");
                current_group = group;
                last_val_in_group = i64::MAX;
            }
            assert!(val < last_val_in_group, "Values within group should be descending");
            last_val_in_group = val;
        }
    }

    #[test]
    fn test_morsel_sort_by_convenience() {
        // Create rows in reverse order
        let rows: Vec<Row> = (0..100)
            .rev()
            .map(|i| Row::from_vec(vec![SqlValue::Integer(i as i64)]))
            .collect();

        let sorted = morsel_sort_by(&rows, |a, b| {
            match (&a.values[0], &b.values[0]) {
                (SqlValue::Integer(x), SqlValue::Integer(y)) => x.cmp(y),
                _ => CmpOrdering::Equal,
            }
        });

        assert_eq!(sorted.len(), 100);
        for (i, row) in sorted.iter().enumerate() {
            assert!(matches!(row.values[0], SqlValue::Integer(x) if x == i as i64));
        }
    }

    #[test]
    fn test_morsel_sort_single_morsel() {
        // Test with exactly one morsel worth of data
        let config = MorselConfig::new(100);
        let rows: Vec<Row> = (0..100)
            .rev()
            .map(|i| Row::from_vec(vec![SqlValue::Integer(i as i64)]))
            .collect();

        let sorted = morsel_parallel_sort(&rows, &config, |a, b| {
            match (&a.values[0], &b.values[0]) {
                (SqlValue::Integer(x), SqlValue::Integer(y)) => x.cmp(y),
                _ => CmpOrdering::Equal,
            }
        });

        assert_eq!(sorted.len(), 100);
        for (i, row) in sorted.iter().enumerate() {
            assert!(matches!(row.values[0], SqlValue::Integer(x) if x == i as i64));
        }
    }

    #[test]
    fn test_morsel_sort_all_equal() {
        let config = MorselConfig::new(50);
        // All rows have the same value
        let rows: Vec<Row> = (0..200)
            .map(|_| Row::from_vec(vec![SqlValue::Integer(42)]))
            .collect();

        let sorted = morsel_parallel_sort(&rows, &config, |a, b| {
            match (&a.values[0], &b.values[0]) {
                (SqlValue::Integer(x), SqlValue::Integer(y)) => x.cmp(y),
                _ => CmpOrdering::Equal,
            }
        });

        assert_eq!(sorted.len(), 200);
        for row in sorted.iter() {
            assert!(matches!(row.values[0], SqlValue::Integer(42)));
        }
    }

    // ============================================
    // Adaptive sizing tests
    // ============================================

    #[test]
    fn test_for_row_width_wide_rows() {
        // Wide rows (500 bytes) should use smaller morsels
        let config = MorselConfig::for_row_width(500);
        // 2MB / 500 bytes = 4096 rows, but clamped to MIN_MORSEL_SIZE (10,000)
        assert_eq!(config.morsel_size, MIN_MORSEL_SIZE);
    }

    #[test]
    fn test_for_row_width_narrow_rows() {
        // Narrow rows (20 bytes) should use larger morsels
        let config = MorselConfig::for_row_width(20);
        // 2MB / 20 bytes = 104,857 rows, but clamped to MAX_MORSEL_SIZE (100,000)
        assert_eq!(config.morsel_size, MAX_MORSEL_SIZE);
    }

    #[test]
    fn test_for_row_width_medium_rows() {
        // Medium rows (100 bytes) - typical case
        let config = MorselConfig::for_row_width(100);
        // 2MB / 100 bytes = 20,971 rows
        assert_eq!(config.morsel_size, 20_971);
    }

    #[test]
    fn test_for_row_width_zero_bytes() {
        // Zero bytes should be treated as 1 byte (avoid division by zero)
        let config = MorselConfig::for_row_width(0);
        // 2MB / 1 byte = way more than MAX, clamped to MAX_MORSEL_SIZE
        assert_eq!(config.morsel_size, MAX_MORSEL_SIZE);
    }

    #[test]
    fn test_for_schema_narrow() {
        // Schema with just integers - narrow rows
        let schema = [DataType::Integer, DataType::Integer];
        let config = MorselConfig::for_schema(&schema);
        // Row overhead (24) + 2 * (8 + 4) = 24 + 24 = 48 bytes
        // 2MB / 48 = ~43,690, within bounds
        assert!(config.morsel_size > 40_000 && config.morsel_size < 50_000);
    }

    #[test]
    fn test_for_schema_wide() {
        // Schema with varchars - wider rows
        let schema = [
            DataType::Integer,
            DataType::Varchar { max_length: Some(200) },
            DataType::Varchar { max_length: Some(200) },
        ];
        let config = MorselConfig::for_schema(&schema);
        // Row overhead (24) + (8+4) + 2*(8+16+200) = 24 + 12 + 448 = 484 bytes
        // Should result in smaller morsels due to wide rows
        assert!(config.morsel_size <= DEFAULT_MORSEL_SIZE);
    }

    #[test]
    fn test_for_schema_empty() {
        // Empty schema should use default
        let schema: [DataType; 0] = [];
        let config = MorselConfig::for_schema(&schema);
        assert_eq!(config.morsel_size, DEFAULT_MORSEL_SIZE);
    }

    #[test]
    fn test_for_selectivity_low() {
        // Low selectivity (1%) should use larger morsels
        let config = MorselConfig::for_selectivity(0.01);
        // 50,000 / 0.01 = 5,000,000, clamped to MAX_MORSEL_SIZE * 2 = 200,000
        assert_eq!(config.morsel_size, MAX_MORSEL_SIZE * 2);
    }

    #[test]
    fn test_for_selectivity_high() {
        // High selectivity (90%) should use default morsels
        let config = MorselConfig::for_selectivity(0.90);
        assert_eq!(config.morsel_size, DEFAULT_MORSEL_SIZE);
    }

    #[test]
    fn test_for_selectivity_medium() {
        // Medium-low selectivity (5%) should scale appropriately
        let config = MorselConfig::for_selectivity(0.05);
        // 50,000 / 0.05 = 1,000,000, clamped to MAX_MORSEL_SIZE * 2 = 200,000
        assert_eq!(config.morsel_size, MAX_MORSEL_SIZE * 2);
    }

    #[test]
    fn test_for_selectivity_boundary() {
        // At 10% boundary, should still use default
        let config = MorselConfig::for_selectivity(0.10);
        assert_eq!(config.morsel_size, DEFAULT_MORSEL_SIZE);

        // Just below 10% should scale up
        let config = MorselConfig::for_selectivity(0.09);
        assert!(config.morsel_size > DEFAULT_MORSEL_SIZE);
    }

    #[test]
    fn test_adaptive_schema_only() {
        // With schema but no selectivity
        let schema = [DataType::Integer, DataType::Bigint];
        let config = MorselConfig::adaptive(&schema, None);
        // Should be same as for_schema
        let expected = MorselConfig::for_schema(&schema);
        assert_eq!(config.morsel_size, expected.morsel_size);
    }

    #[test]
    fn test_adaptive_with_selectivity() {
        // With schema and low selectivity
        let schema = [DataType::Integer, DataType::Bigint];
        let config = MorselConfig::adaptive(&schema, Some(0.01));
        // Should be larger than schema-only due to low selectivity
        let schema_only = MorselConfig::for_schema(&schema);
        assert!(config.morsel_size > schema_only.morsel_size);
    }

    #[test]
    fn test_adaptive_high_selectivity() {
        // With schema and high selectivity - should be same as schema-only
        let schema = [DataType::Integer, DataType::Bigint];
        let config = MorselConfig::adaptive(&schema, Some(0.90));
        let schema_only = MorselConfig::for_schema(&schema);
        assert_eq!(config.morsel_size, schema_only.morsel_size);
    }

    #[test]
    fn test_data_type_size_estimates() {
        // Test a few key type size estimates
        assert_eq!(DataType::Integer.estimated_size_bytes(), 8 + 4); // enum + value
        assert_eq!(DataType::Bigint.estimated_size_bytes(), 8 + 8);
        assert_eq!(DataType::Boolean.estimated_size_bytes(), 8 + 1);

        // VARCHAR with max_length
        let varchar = DataType::Varchar { max_length: Some(100) };
        assert_eq!(varchar.estimated_size_bytes(), 8 + 16 + 100); // enum + arcstr + chars

        // Vector type
        let vector = DataType::Vector { dimensions: 128 };
        assert_eq!(vector.estimated_size_bytes(), 8 + 24 + 128 * 4); // enum + vec header + floats
    }
}
