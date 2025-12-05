use ahash::AHashMap;

#[cfg(feature = "parallel")]
use rayon::prelude::*;

#[cfg(feature = "parallel")]
use crate::select::parallel::ParallelConfig;

use crate::errors::ExecutorError;
use crate::timeout::{TimeoutContext, CHECK_INTERVAL};

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

/// Build hash table with composite key in parallel
///
/// For large tables, this builds partial hash tables in parallel and merges them.
#[cfg(feature = "parallel")]
pub(crate) fn build_hash_table_composite_parallel(
    build_rows: &[vibesql_storage::Row],
    build_col_indices: &[usize],
) -> AHashMap<CompositeKey, Vec<usize>> {
    let config = ParallelConfig::global();

    // Use sequential fallback for small inputs
    if !config.should_parallelize_join(build_rows.len()) {
        return build_hash_table_composite_sequential(build_rows, build_col_indices);
    }

    // Phase 1: Parallel build of partial hash tables with indices
    let chunk_size = (build_rows.len() / config.num_threads).max(1000);
    let partial_tables: Vec<(usize, AHashMap<CompositeKey, Vec<usize>>)> = build_rows
        .par_chunks(chunk_size)
        .enumerate()
        .map(|(chunk_idx, chunk)| {
            let base_idx = chunk_idx * chunk_size;
            let mut local_table: AHashMap<CompositeKey, Vec<usize>> = AHashMap::new();
            for (i, row) in chunk.iter().enumerate() {
                let key = CompositeKey::from_row(row, build_col_indices);
                if !key.has_null() {
                    local_table.entry(key).or_default().push(base_idx + i);
                }
            }
            (chunk_idx, local_table)
        })
        .collect();

    // Phase 2: Sequential merge of partial tables
    partial_tables.into_iter().fold(AHashMap::new(), |mut acc, (_chunk_idx, partial)| {
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

/// Build hash table in parallel using partitioned approach (index-based)
///
/// Algorithm (when parallel feature enabled):
/// 1. Divide build_rows into chunks (one per thread)
/// 2. Each thread builds a local hash table from its chunk (no synchronization)
/// 3. Merge partial hash tables sequentially (fast because only touching shared keys)
///
/// Performance: 3-6x speedup on large joins (50k+ rows) with 4+ cores
/// Note: Falls back to sequential when parallel feature is disabled
#[allow(dead_code)]
pub(crate) fn build_hash_table_parallel(
    build_rows: &[vibesql_storage::Row],
    build_col_idx: usize,
) -> AHashMap<vibesql_types::SqlValue, Vec<usize>> {
    #[cfg(feature = "parallel")]
    {
        let config = ParallelConfig::global();

        // Use sequential fallback for small inputs
        if !config.should_parallelize_join(build_rows.len()) {
            return build_hash_table_sequential(build_rows, build_col_idx);
        }

        // Phase 1: Parallel build of partial hash tables with indices
        // Each thread processes a chunk and builds its own hash table
        let chunk_size = (build_rows.len() / config.num_threads).max(1000);
        let partial_tables: Vec<(usize, AHashMap<_, _>)> = build_rows
            .par_chunks(chunk_size)
            .enumerate()
            .map(|(chunk_idx, chunk)| {
                let base_idx = chunk_idx * chunk_size;
                let mut local_table: AHashMap<vibesql_types::SqlValue, Vec<usize>> =
                    AHashMap::new();
                for (i, row) in chunk.iter().enumerate() {
                    let key = row.values[build_col_idx].clone();
                    if key != vibesql_types::SqlValue::Null {
                        local_table.entry(key).or_default().push(base_idx + i);
                    }
                }
                (chunk_idx, local_table)
            })
            .collect();

        // Phase 2: Sequential merge of partial tables
        // This is fast because we only touch keys that appear in multiple partitions
        partial_tables.into_iter().fold(AHashMap::new(), |mut acc, (_chunk_idx, partial)| {
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
