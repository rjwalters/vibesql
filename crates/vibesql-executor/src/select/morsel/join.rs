//! Morsel-driven parallel join operations.
//!
//! This module provides parallel hash join probe operations using morsel-driven
//! execution with work-stealing for dynamic load balancing.

use std::sync::{Arc, Mutex};

use ahash::AHashMap;
use crossbeam_deque::{Injector, Worker};
use vibesql_storage::Row;
use vibesql_types::SqlValue;

use super::{config::MorselConfig, create_morsels, morsel_debug_enabled, steal_morsel, Morsel};

/// Thread-safe container for collecting join results (row index pairs)
type JoinPairResults = Arc<Mutex<Vec<(usize, Vec<(usize, usize)>)>>>;

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
    hash_table: &AHashMap<SqlValue, Vec<usize>>,
    config: &MorselConfig,
) -> Vec<(usize, usize)> {
    if probe_rows.is_empty() {
        return Vec::new();
    }

    // Use join-probe-specific morsel size
    let morsel_size = config.join_probe_size;

    // For small datasets, process directly
    if probe_rows.len() < morsel_size {
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
    let morsels = create_morsels(probe_rows.len(), morsel_size);
    let morsel_count = morsels.len();

    if morsel_debug_enabled() {
        eprintln!(
            "[MORSEL] Probe: {} morsels for {} rows (size={})",
            morsel_count,
            probe_rows.len(),
            morsel_size
        );
    }

    // Create global injector queue
    let injector: Injector<Morsel> = Injector::new();
    for morsel in morsels {
        injector.push(morsel);
    }

    // Results storage shared across threads
    let results: JoinPairResults = Arc::new(Mutex::new(Vec::with_capacity(morsel_count)));

    // Process morsels in parallel
    rayon::scope(|s| {
        let num_threads = rayon::current_num_threads();

        for _ in 0..num_threads {
            let injector_ref = &injector;
            let results_ref = results.clone();

            s.spawn(move |_| {
                let worker: Worker<Morsel> = Worker::new_fifo();
                let mut local_pairs = Vec::with_capacity(morsel_size);

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
                        results_ref.lock().unwrap().push((start_idx, local_pairs.clone()));
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
