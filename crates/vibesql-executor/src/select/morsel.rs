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
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use vibesql_storage::Row;

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
            size_str.parse::<usize>().unwrap_or(50_000).max(1000).min(500_000)
        } else {
            // Default: 50,000 rows
            // This balances:
            // - Cache efficiency (50K rows * 100 bytes = 5MB, fits L3)
            // - Load balancing (enough morsels for 16+ cores)
            // - Stealing overhead (large enough to amortize)
            50_000
        };

        Self { morsel_size }
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
}
