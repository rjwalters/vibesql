//! Parallel morsel operations with work-stealing.
//!
//! This module provides morsel-driven parallel implementations of common
//! operations: filter, map, filter_map, reduce, and group.

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc, Mutex,
};

use ahash::AHashMap;
use crossbeam_deque::{Injector, Worker};
use vibesql_storage::Row;
use vibesql_types::SqlValue;

use super::{
    config::{global_config, MorselConfig},
    create_morsels, morsel_debug_enabled, steal_morsel, Morsel, MorselResultsOrdered,
};

/// Thread-safe container for collecting grouped rows from parallel workers
type GroupedRowResults = Arc<Mutex<Vec<AHashMap<Vec<SqlValue>, Vec<Row>>>>>;

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

    // Use filter-specific morsel size
    let morsel_size = config.filter_size;

    // For small datasets, process directly (avoid morsel overhead)
    if rows.len() < morsel_size {
        return rows.iter().filter(|r| predicate(r)).cloned().collect();
    }

    // Create morsels
    let morsels = create_morsels(rows.len(), morsel_size);
    let morsel_count = morsels.len();

    if morsel_debug_enabled() {
        eprintln!(
            "[MORSEL] Filter: {} morsels for {} rows (size={})",
            morsel_count,
            rows.len(),
            morsel_size
        );
    }

    // Create global injector queue
    let injector: Injector<Morsel> = Injector::new();
    for morsel in morsels {
        injector.push(morsel);
    }

    // Results storage shared across threads
    let results: MorselResultsOrdered = Arc::new(Mutex::new(Vec::with_capacity(morsel_count)));
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

    // Use filter-size for map operations (similar cache locality characteristics)
    let morsel_size = config.filter_size;

    // For small datasets, process directly
    if rows.len() < morsel_size {
        return rows.iter().map(&transform).collect();
    }

    // Create morsels
    let morsels = create_morsels(rows.len(), morsel_size);
    let morsel_count = morsels.len();

    // Create global injector queue
    let injector: Injector<Morsel> = Injector::new();
    for morsel in morsels {
        injector.push(morsel);
    }

    // Results storage shared across threads
    let results: MorselResultsOrdered = Arc::new(Mutex::new(Vec::with_capacity(morsel_count)));

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
pub fn morsel_parallel_filter_map<F>(rows: &[Row], config: &MorselConfig, filter_map: F) -> Vec<Row>
where
    F: Fn(&Row) -> Option<Row> + Sync + Send,
{
    if rows.is_empty() {
        return Vec::new();
    }

    // Use filter-size for filter-map operations
    let morsel_size = config.filter_size;

    // For small datasets, process directly
    if rows.len() < morsel_size {
        return rows.iter().filter_map(&filter_map).collect();
    }

    // Create morsels
    let morsels = create_morsels(rows.len(), morsel_size);
    let morsel_count = morsels.len();

    // Create global injector queue
    let injector: Injector<Morsel> = Injector::new();
    for morsel in morsels {
        injector.push(morsel);
    }

    // Results storage shared across threads
    let results: MorselResultsOrdered = Arc::new(Mutex::new(Vec::with_capacity(morsel_count)));

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

    // Use aggregate-size for reduce operations
    let morsel_size = config.aggregate_size;

    // For small datasets, process directly
    if rows.len() < morsel_size {
        return operation(rows);
    }

    // Create morsels
    let morsels = create_morsels(rows.len(), morsel_size);

    // Create global injector queue
    let injector: Injector<Morsel> = Injector::new();
    for morsel in morsels {
        injector.push(morsel);
    }

    // Results storage shared across threads
    let results: Arc<std::sync::Mutex<Vec<R>>> = Arc::new(std::sync::Mutex::new(Vec::new()));

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
) -> AHashMap<Vec<SqlValue>, Vec<Row>>
where
    K: Fn(&Row) -> Vec<SqlValue> + Sync + Send,
    M: Fn(AHashMap<Vec<SqlValue>, Vec<Row>>, AHashMap<Vec<SqlValue>, Vec<Row>>)
            -> AHashMap<Vec<SqlValue>, Vec<Row>>
        + Sync
        + Send,
{
    if rows.is_empty() {
        return AHashMap::new();
    }

    // Use group_by-specific morsel size
    let morsel_size = config.group_by_size;

    // For small datasets, process directly
    if rows.len() < morsel_size {
        let estimated_groups = (rows.len() / 10).max(16);
        let mut groups: AHashMap<Vec<SqlValue>, Vec<Row>> =
            AHashMap::with_capacity(estimated_groups);

        for row in rows {
            let key = key_fn(row);
            groups.entry(key).or_default().push(row.clone());
        }
        return groups;
    }

    // Create morsels
    let morsels = create_morsels(rows.len(), morsel_size);
    let morsel_count = morsels.len();

    if morsel_debug_enabled() {
        eprintln!(
            "[MORSEL] Group: {} morsels for {} rows (size={})",
            morsel_count,
            rows.len(),
            morsel_size
        );
    }

    // Create global injector queue
    let injector: Injector<Morsel> = Injector::new();
    for morsel in morsels {
        injector.push(morsel);
    }

    // Results storage shared across threads
    let results: GroupedRowResults = Arc::new(Mutex::new(Vec::with_capacity(morsel_count)));

    // Process morsels in parallel
    rayon::scope(|s| {
        let num_threads = rayon::current_num_threads();

        for _ in 0..num_threads {
            let injector_ref = &injector;
            let key_fn_ref = &key_fn;
            let results_ref = results.clone();

            s.spawn(move |_| {
                let worker: Worker<Morsel> = Worker::new_fifo();
                let estimated_groups = (morsel_size / 10).max(16);
                let mut local_groups: AHashMap<Vec<SqlValue>, Vec<Row>> =
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
    thread_results.into_iter().fold(AHashMap::new(), merge)
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
