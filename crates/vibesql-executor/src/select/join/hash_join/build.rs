#[cfg(feature = "parallel")]
use std::sync::Arc;

use ahash::AHashMap;
#[cfg(feature = "parallel")]
use crossbeam_deque::{Injector, Steal, Worker};
#[cfg(feature = "parallel")]
use rayon::prelude::*;

#[cfg(feature = "parallel")]
use crate::select::morsel::{global_config, Morsel};
#[cfg(feature = "parallel")]
use crate::select::parallel::ParallelConfig;
use crate::{
    errors::ExecutorError,
    timeout::{TimeoutContext, CHECK_INTERVAL},
};

/// Environment variable to enable morsel build debug logging
#[cfg(feature = "parallel")]
const MORSEL_BUILD_DEBUG_ENV: &str = "MORSEL_BUILD_DEBUG";

/// Check if morsel build debug logging is enabled
#[cfg(feature = "parallel")]
fn morsel_build_debug_enabled() -> bool {
    std::env::var(MORSEL_BUILD_DEBUG_ENV).is_ok()
}

/// Create morsels from a row count (local helper for build phase)
#[cfg(feature = "parallel")]
fn create_build_morsels(total_rows: usize, morsel_size: usize) -> Vec<Morsel> {
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
#[cfg(feature = "parallel")]
fn steal_morsel(injector: &Injector<Morsel>, worker: &Worker<Morsel>) -> Option<Morsel> {
    worker.pop().or_else(|| loop {
        match injector.steal() {
            Steal::Success(m) => return Some(m),
            Steal::Empty => return None,
            Steal::Retry => continue,
        }
    })
}

/// Composite key for multi-column hash joins
///
/// This allows us to use multiple columns as the hash key, enabling
/// efficient hash joins for conditions like `a.x = b.x AND a.y = b.y`.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct CompositeKey(pub Vec<vibesql_types::SqlValue>);

impl CompositeKey {
    /// Create a composite key from a row using the specified column indices
    #[inline]
    pub fn from_row(row: &vibesql_storage::Row, col_indices: &[usize]) -> Self {
        let values: Vec<vibesql_types::SqlValue> =
            col_indices.iter().map(|&idx| row.values[idx].clone()).collect();
        CompositeKey(values)
    }

    /// Check if any value in the composite key is NULL
    #[inline]
    pub fn has_null(&self) -> bool {
        self.0.iter().any(|v| v == &vibesql_types::SqlValue::Null)
    }
}

/// Build hash table with composite (multi-column) key sequentially
///
/// Returns a map from composite key to row indices, enabling multi-column hash joins.
pub(crate) fn build_hash_table_composite_sequential(
    build_rows: &[vibesql_storage::Row],
    build_col_indices: &[usize],
) -> AHashMap<CompositeKey, Vec<usize>> {
    let mut hash_table: AHashMap<CompositeKey, Vec<usize>> =
        AHashMap::with_capacity(build_rows.len());
    for (idx, row) in build_rows.iter().enumerate() {
        let key = CompositeKey::from_row(row, build_col_indices);
        // Skip rows with any NULL key values - they never match in equi-joins
        if !key.has_null() {
            hash_table.entry(key).or_default().push(idx);
        }
    }
    hash_table
}

/// Build hash table with composite key in parallel using morsel-driven work-stealing
///
/// Uses the morsel-driven parallelism model for dynamic load balancing:
/// 1. Divide rows into morsels (cache-sized chunks)
/// 2. Workers steal morsels from a global queue
/// 3. Each worker builds a thread-local hash table
/// 4. Merge all partial tables at the end
///
/// This provides better load balancing than static `par_chunks()` when:
/// - Row sizes vary significantly
/// - Hash computation cost varies by data type
/// - Memory allocation patterns differ across partitions
#[cfg(feature = "parallel")]
#[allow(clippy::type_complexity)]
pub(crate) fn build_hash_table_composite_parallel(
    build_rows: &[vibesql_storage::Row],
    build_col_indices: &[usize],
) -> AHashMap<CompositeKey, Vec<usize>> {
    let parallel_config = ParallelConfig::global();
    let morsel_config = global_config();

    // Use sequential fallback for small inputs
    if !parallel_config.should_parallelize_join(build_rows.len()) {
        return build_hash_table_composite_sequential(build_rows, build_col_indices);
    }

    // Also fall back to sequential if below morsel threshold
    if build_rows.len() < morsel_config.morsel_size {
        return build_hash_table_composite_sequential(build_rows, build_col_indices);
    }

    // Create morsels
    let morsels = create_build_morsels(build_rows.len(), morsel_config.morsel_size);
    let morsel_count = morsels.len();

    if morsel_build_debug_enabled() {
        eprintln!(
            "[MORSEL_BUILD] Composite: {} morsels for {} rows (size={})",
            morsel_count,
            build_rows.len(),
            morsel_config.morsel_size
        );
    }

    // Create global injector queue
    let injector: Injector<Morsel> = Injector::new();
    for morsel in morsels {
        injector.push(morsel);
    }

    // Results storage: each thread produces a partial hash table
    let results: Arc<std::sync::Mutex<Vec<AHashMap<CompositeKey, Vec<usize>>>>> =
        Arc::new(std::sync::Mutex::new(Vec::with_capacity(morsel_count)));

    // Process morsels in parallel using rayon's thread pool
    rayon::scope(|s| {
        let num_threads = rayon::current_num_threads();

        for _ in 0..num_threads {
            let injector_ref = &injector;
            let results_ref = results.clone();

            s.spawn(move |_| {
                let worker: Worker<Morsel> = Worker::new_fifo();
                let mut local_table: AHashMap<CompositeKey, Vec<usize>> = AHashMap::new();

                while let Some(m) = steal_morsel(injector_ref, &worker) {
                    let base_idx = m.start_idx();
                    let morsel_rows = &build_rows[m.start_idx()..m.end_idx()];

                    for (i, row) in morsel_rows.iter().enumerate() {
                        let key = CompositeKey::from_row(row, build_col_indices);
                        if !key.has_null() {
                            // Use global index (base_idx + local index)
                            local_table.entry(key).or_default().push(base_idx + i);
                        }
                    }
                }

                if !local_table.is_empty() {
                    results_ref.lock().unwrap().push(local_table);
                }
            });
        }
    });

    // Extract results after scope completes
    let partial_tables = Arc::try_unwrap(results)
        .expect("all threads should have completed")
        .into_inner()
        .expect("mutex not poisoned");

    if morsel_build_debug_enabled() {
        eprintln!(
            "[MORSEL_BUILD] Composite complete: {} partial tables to merge",
            partial_tables.len()
        );
    }

    // Phase 2: Sequential merge of partial tables
    partial_tables.into_iter().fold(AHashMap::new(), |mut acc, partial| {
        for (key, mut indices) in partial {
            acc.entry(key).or_default().append(&mut indices);
        }
        acc
    })
}

#[cfg(not(feature = "parallel"))]
pub(crate) fn build_hash_table_composite_parallel(
    build_rows: &[vibesql_storage::Row],
    build_col_indices: &[usize],
) -> AHashMap<CompositeKey, Vec<usize>> {
    build_hash_table_composite_sequential(build_rows, build_col_indices)
}

/// Build hash table sequentially using indices (fallback for small inputs)
///
/// Returns a map from join key to row indices, avoiding storing row references
/// which enables deferred materialization.
#[allow(dead_code)]
pub(super) fn build_hash_table_sequential(
    build_rows: &[vibesql_storage::Row],
    build_col_idx: usize,
) -> AHashMap<vibesql_types::SqlValue, Vec<usize>> {
    let mut hash_table: AHashMap<vibesql_types::SqlValue, Vec<usize>> = AHashMap::new();
    for (idx, row) in build_rows.iter().enumerate() {
        let key = row.values[build_col_idx].clone();
        // Skip NULL values - they never match in equi-joins
        if key != vibesql_types::SqlValue::Null {
            hash_table.entry(key).or_default().push(idx);
        }
    }
    hash_table
}

/// Build hash table in parallel using morsel-driven work-stealing (single-column key)
///
/// Uses the morsel-driven parallelism model for dynamic load balancing:
/// 1. Divide rows into morsels (cache-sized chunks)
/// 2. Workers steal morsels from a global queue
/// 3. Each worker builds a thread-local hash table
/// 4. Merge all partial tables at the end
///
/// Performance: Near-linear scaling to 16+ cores with dynamic load balancing
/// Note: Falls back to sequential when parallel feature is disabled
#[allow(dead_code)]
#[allow(clippy::type_complexity)]
pub(crate) fn build_hash_table_parallel(
    build_rows: &[vibesql_storage::Row],
    build_col_idx: usize,
) -> AHashMap<vibesql_types::SqlValue, Vec<usize>> {
    #[cfg(feature = "parallel")]
    {
        let parallel_config = ParallelConfig::global();
        let morsel_config = global_config();

        // Use sequential fallback for small inputs
        if !parallel_config.should_parallelize_join(build_rows.len()) {
            return build_hash_table_sequential(build_rows, build_col_idx);
        }

        // Also fall back to sequential if below morsel threshold
        if build_rows.len() < morsel_config.morsel_size {
            return build_hash_table_sequential(build_rows, build_col_idx);
        }

        // Create morsels
        let morsels = create_build_morsels(build_rows.len(), morsel_config.morsel_size);
        let morsel_count = morsels.len();

        if morsel_build_debug_enabled() {
            eprintln!(
                "[MORSEL_BUILD] Single-key: {} morsels for {} rows (size={})",
                morsel_count,
                build_rows.len(),
                morsel_config.morsel_size
            );
        }

        // Create global injector queue
        let injector: Injector<Morsel> = Injector::new();
        for morsel in morsels {
            injector.push(morsel);
        }

        // Results storage: each thread produces a partial hash table
        let results: Arc<std::sync::Mutex<Vec<AHashMap<vibesql_types::SqlValue, Vec<usize>>>>> =
            Arc::new(std::sync::Mutex::new(Vec::with_capacity(morsel_count)));

        // Process morsels in parallel using rayon's thread pool
        rayon::scope(|s| {
            let num_threads = rayon::current_num_threads();

            for _ in 0..num_threads {
                let injector_ref = &injector;
                let results_ref = results.clone();

                s.spawn(move |_| {
                    let worker: Worker<Morsel> = Worker::new_fifo();
                    let mut local_table: AHashMap<vibesql_types::SqlValue, Vec<usize>> =
                        AHashMap::new();

                    while let Some(m) = steal_morsel(injector_ref, &worker) {
                        let base_idx = m.start_idx();
                        let morsel_rows = &build_rows[m.start_idx()..m.end_idx()];

                        for (i, row) in morsel_rows.iter().enumerate() {
                            let key = row.values[build_col_idx].clone();
                            if key != vibesql_types::SqlValue::Null {
                                // Use global index (base_idx + local index)
                                local_table.entry(key).or_default().push(base_idx + i);
                            }
                        }
                    }

                    if !local_table.is_empty() {
                        results_ref.lock().unwrap().push(local_table);
                    }
                });
            }
        });

        // Extract results after scope completes
        let partial_tables = Arc::try_unwrap(results)
            .expect("all threads should have completed")
            .into_inner()
            .expect("mutex not poisoned");

        if morsel_build_debug_enabled() {
            eprintln!(
                "[MORSEL_BUILD] Single-key complete: {} partial tables to merge",
                partial_tables.len()
            );
        }

        // Phase 2: Sequential merge of partial tables
        partial_tables.into_iter().fold(AHashMap::new(), |mut acc, partial| {
            for (key, mut indices) in partial {
                acc.entry(key).or_default().append(&mut indices);
            }
            acc
        })
    }

    #[cfg(not(feature = "parallel"))]
    {
        // Always use sequential build when parallel feature is disabled
        build_hash_table_sequential(build_rows, build_col_idx)
    }
}

// ============================================================================
// Existence Hash Table Builders (for semi-join and anti-join)
// ============================================================================
//
// These functions build hash tables that only track key existence (AHashMap<SqlValue, ()>)
// rather than storing row indices. This is more memory-efficient for semi-join and
// anti-join operations where we only need to know if a key exists, not which rows match.

/// Build existence hash table sequentially (stores only keys, not indices)
///
/// For semi-join and anti-join, we only need to know if a key exists, not track all
/// matching rows. This saves memory compared to inner join's Vec<usize> storage.
pub(crate) fn build_existence_hash_table_sequential(
    build_rows: &[vibesql_storage::Row],
    build_col_idx: usize,
    timeout_ctx: &TimeoutContext,
) -> Result<AHashMap<vibesql_types::SqlValue, ()>, ExecutorError> {
    let mut hash_table: AHashMap<vibesql_types::SqlValue, ()> = AHashMap::new();
    for (idx, row) in build_rows.iter().enumerate() {
        // Check timeout periodically during build phase
        if idx % CHECK_INTERVAL == 0 {
            timeout_ctx.check()?;
        }
        let key = row.values[build_col_idx].clone();
        // Skip NULL values - they never match in equi-joins
        if key != vibesql_types::SqlValue::Null {
            hash_table.insert(key, ());
        }
    }
    Ok(hash_table)
}

/// Build existence hash table in parallel (for semi-join/anti-join)
///
/// Algorithm (when parallel feature enabled):
/// 1. Divide build_rows into chunks (one per thread)
/// 2. Each thread builds a local hash table from its chunk (no synchronization)
/// 3. Merge partial hash tables sequentially (fast because we only store keys)
///
/// Performance: 3-6x speedup on large joins (50k+ rows) with 4+ cores
/// Note: Falls back to sequential when parallel feature is disabled
pub(crate) fn build_existence_hash_table_parallel(
    build_rows: &[vibesql_storage::Row],
    build_col_idx: usize,
    timeout_ctx: &TimeoutContext,
) -> Result<AHashMap<vibesql_types::SqlValue, ()>, ExecutorError> {
    #[cfg(feature = "parallel")]
    {
        let config = ParallelConfig::global();

        // Use sequential fallback for small inputs
        if !config.should_parallelize_join(build_rows.len()) {
            return build_existence_hash_table_sequential(build_rows, build_col_idx, timeout_ctx);
        }

        // Check timeout before parallel execution (can't check mid-parallel easily)
        timeout_ctx.check()?;

        // Phase 1: Parallel build of partial hash tables
        // Each thread processes a chunk and builds its own hash table
        let chunk_size = (build_rows.len() / config.num_threads).max(1000);
        let partial_tables: Vec<AHashMap<_, ()>> = build_rows
            .par_chunks(chunk_size)
            .map(|chunk| {
                let mut local_table: AHashMap<vibesql_types::SqlValue, ()> = AHashMap::new();
                for row in chunk.iter() {
                    let key = row.values[build_col_idx].clone();
                    if key != vibesql_types::SqlValue::Null {
                        local_table.insert(key, ());
                    }
                }
                local_table
            })
            .collect();

        // Check timeout after parallel build
        timeout_ctx.check()?;

        // Phase 2: Sequential merge of partial tables
        // This is fast because we only need to insert keys, not append vectors
        let result = partial_tables.into_iter().fold(AHashMap::new(), |mut acc, partial| {
            for (key, _) in partial {
                acc.insert(key, ());
            }
            acc
        });
        Ok(result)
    }

    #[cfg(not(feature = "parallel"))]
    {
        // Always use sequential build when parallel feature is disabled
        build_existence_hash_table_sequential(build_rows, build_col_idx, timeout_ctx)
    }
}
