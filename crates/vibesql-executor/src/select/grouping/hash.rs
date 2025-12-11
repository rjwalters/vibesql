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
use crate::timeout::TimeoutContext;

/// Grouped rows: (group key values, rows in group)
pub type GroupedRows = Vec<(Vec<vibesql_types::SqlValue>, Vec<vibesql_storage::Row>)>;

/// Group rows by GROUP BY expressions
///
/// Optimized implementation using HashMap for O(1) group lookups instead of O(n) linear search.
/// This significantly improves performance for queries with many groups.
///
/// When the `parallel` feature is enabled and row count exceeds the threshold,
/// this function uses parallel grouping with thread-local evaluators (Issue #4132).
/// Timeout is checked every 1000 rows.
pub fn group_rows<'a>(
    rows: &[vibesql_storage::Row],
    group_by_exprs: &[vibesql_ast::Expression],
    evaluator: &crate::evaluator::CombinedExpressionEvaluator,
    executor: &crate::SelectExecutor<'a>,
) -> Result<GroupedRows, crate::errors::ExecutorError> {
    #[cfg(feature = "parallel")]
    {
        use crate::select::parallel::ParallelConfig;

        let config = ParallelConfig::global();

        // Use parallel grouping for large datasets
        if config.should_parallelize_aggregate(rows.len()) {
            // Get components needed for thread-local evaluators
            let components = evaluator.get_parallel_components();
            let timeout_ctx = TimeoutContext::from_executor(executor);

            return group_rows_parallel(rows, group_by_exprs, components, &timeout_ctx);
        }
    }

    // Sequential fallback
    group_rows_sequential(rows, group_by_exprs, evaluator, executor)
}

/// Sequential grouping implementation using index-based approach (Issue #4168)
///
/// Optimization: Instead of cloning rows during grouping, we store row indices
/// and materialize the rows at the end. This reduces memory pressure during
/// HashMap operations (smaller entries = faster rehashing) and improves cache
/// locality during the grouping phase.
///
/// Performance: For TPC-H Q18 subquery (60K rows, 15K groups), this reduces
/// the memory footprint during grouping from ~60K Row structs to ~60K usize.
fn group_rows_sequential<'a>(
    rows: &[vibesql_storage::Row],
    group_by_exprs: &[vibesql_ast::Expression],
    evaluator: &crate::evaluator::CombinedExpressionEvaluator,
    executor: &crate::SelectExecutor<'a>,
) -> Result<GroupedRows, crate::errors::ExecutorError> {
    // Use AHashMap for O(1) group lookups with faster hashing
    // Pre-allocate with reasonable capacity to reduce rehashing
    // Most GROUP BY queries have < 1000 groups; estimate 10% of rows as groups
    let estimated_groups = (rows.len() / 10).max(16);

    // Phase 1: Group by indices (no row cloning during grouping)
    // This uses much less memory per entry (usize vs Row struct)
    let mut groups_map: AHashMap<Vec<vibesql_types::SqlValue>, Vec<usize>> =
        AHashMap::with_capacity(estimated_groups);

    let mut rows_processed = 0;
    const CHECK_INTERVAL: usize = 1000;

    for (idx, row) in rows.iter().enumerate() {
        // Check timeout every 1000 rows
        rows_processed += 1;
        if rows_processed % CHECK_INTERVAL == 0 {
            executor.check_timeout()?;
        }

        // Clear CSE cache before evaluating each row to prevent column values
        // from being incorrectly cached across different rows
        evaluator.clear_cse_cache();

        // Evaluate GROUP BY expressions to get the group key
        let mut key = Vec::with_capacity(group_by_exprs.len());
        for expr in group_by_exprs {
            let value = evaluator.eval(expr, row)?;
            key.push(value);
        }

        // Store index instead of cloning the row
        groups_map.entry(key).or_default().push(idx);
    }

    // Phase 2: Materialize rows from indices
    // This clones each row exactly once at the end
    let group_count = groups_map.len();
    let mut result: GroupedRows = Vec::with_capacity(group_count);
    for (key, indices) in groups_map {
        let mut group_rows: Vec<vibesql_storage::Row> = Vec::with_capacity(indices.len());
        for idx in indices {
            group_rows.push(rows[idx].clone());
        }
        result.push((key, group_rows));
    }

    Ok(result)
}

/// Parallel grouping implementation using morsel-driven work-stealing (Issue #4161)
///
/// This function parallelizes GROUP BY using the morsel infrastructure for dynamic
/// load balancing. Instead of static `par_chunks()` partitioning, workers steal
/// morsels from a global queue, enabling better scaling on skewed distributions.
///
/// Architecture:
/// 1. **Morsels**: Split rows into fixed-size morsels (~50K rows each)
/// 2. **Work-Stealing**: Workers steal morsels from a global injector queue
/// 3. **Thread-Local Evaluators**: Each worker creates a fresh evaluator per thread
/// 4. **Reduce**: Thread-local group maps are merged into a global map
///
/// Thread safety is achieved by creating fresh evaluators per thread,
/// avoiding the `Send` constraint on `RefCell`/`Rc` in `CombinedExpressionEvaluator`.
///
/// Timeout is checked before and after parallel processing (Issue #4151).
#[cfg(feature = "parallel")]
#[allow(clippy::type_complexity)] // Complex type is inherent to parallel aggregation pattern
fn group_rows_parallel<'a>(
    rows: &[vibesql_storage::Row],
    group_by_exprs: &[vibesql_ast::Expression],
    components: crate::evaluator::parallel::ParallelComponents<'a>,
    timeout_ctx: &TimeoutContext,
) -> Result<GroupedRows, crate::errors::ExecutorError> {
    use crate::evaluator::CombinedExpressionEvaluator;

    let (schema, database, outer_row, outer_schema, window_mapping, cte_context, enable_cse) =
        components;

    // Check timeout before parallel execution
    timeout_ctx.check()?;

    let config = global_config();

    // For small datasets below morsel size, use simpler parallel approach
    if rows.len() < config.morsel_size {
        return group_rows_parallel_simple(rows, group_by_exprs, components, timeout_ctx);
    }

    // Create morsels for work distribution
    let morsels = create_morsels(rows.len(), config.morsel_size);
    let morsel_count = morsels.len();

    if morsel_debug_enabled() {
        eprintln!(
            "[MORSEL] GROUP BY: {} morsels for {} rows (size={})",
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

    // Results storage shared across threads - stores INDICES not rows (Issue #4168)
    // This reduces memory pressure and improves cache locality during grouping
    let results: Arc<
        std::sync::Mutex<
            Vec<
                Result<
                    AHashMap<Vec<vibesql_types::SqlValue>, Vec<usize>>,
                    crate::errors::ExecutorError,
                >,
            >,
        >,
    > = Arc::new(std::sync::Mutex::new(Vec::with_capacity(morsel_count)));

    // Process morsels in parallel using rayon's thread pool with work-stealing
    rayon::scope(|s| {
        let num_threads = rayon::current_num_threads();

        for _ in 0..num_threads {
            let injector_ref = &injector;
            let results_ref = results.clone();

            s.spawn(move |_| {
                // Create thread-local worker queue for work-stealing
                let worker: Worker<Morsel> = Worker::new_fifo();

                // Create thread-local evaluator (fresh instance, no Rc/RefCell sharing)
                // This is created once per thread, not per morsel, for efficiency
                let evaluator = CombinedExpressionEvaluator::from_parallel_components(
                    schema,
                    database,
                    outer_row,
                    outer_schema,
                    window_mapping,
                    cte_context,
                    enable_cse,
                );

                // Thread-local group map that accumulates across all morsels this thread processes
                // Stores INDICES instead of rows (Issue #4168)
                let estimated_groups = (config.morsel_size / 10).max(16);
                let mut local_groups: AHashMap<Vec<vibesql_types::SqlValue>, Vec<usize>> =
                    AHashMap::with_capacity(estimated_groups);

                // Steal and process morsels until queue is empty
                while let Some(m) = steal_morsel(injector_ref, &worker) {
                    let morsel_start = m.start_idx();
                    let morsel_rows = &rows[morsel_start..m.end_idx()];

                    // Process all rows in this morsel
                    let result: Result<(), crate::errors::ExecutorError> = (|| {
                        for (local_idx, row) in morsel_rows.iter().enumerate() {
                            // Clear CSE cache before evaluating each row
                            evaluator.clear_cse_cache();

                            // Evaluate GROUP BY expressions to get the group key
                            let mut key = Vec::with_capacity(group_by_exprs.len());
                            for expr in group_by_exprs {
                                let value = evaluator.eval(expr, row)?;
                                key.push(value);
                            }

                            // Store ABSOLUTE index instead of cloning the row
                            let absolute_idx = morsel_start + local_idx;
                            local_groups.entry(key).or_default().push(absolute_idx);
                        }
                        Ok(())
                    })();

                    // If we hit an error, store it and stop processing
                    if let Err(e) = result {
                        results_ref.lock().unwrap().push(Err(e));
                        return;
                    }
                }

                // Store final result for this thread
                if !local_groups.is_empty() {
                    results_ref.lock().unwrap().push(Ok(local_groups));
                }
            });
        }
    });

    // Check timeout after parallel phase
    timeout_ctx.check()?;

    // Extract results after scope completes
    let thread_results = Arc::try_unwrap(results)
        .expect("all threads should have completed")
        .into_inner()
        .expect("mutex not poisoned");

    if morsel_debug_enabled() {
        eprintln!(
            "[MORSEL] GROUP BY complete: {} morsels, {} thread results",
            morsel_count,
            thread_results.len()
        );
    }

    // Check for errors from any thread
    let mut validated_results: Vec<AHashMap<Vec<vibesql_types::SqlValue>, Vec<usize>>> =
        Vec::with_capacity(thread_results.len());
    for result in thread_results {
        validated_results.push(result?);
    }

    // Phase 2: Sequential reduce - merge thread-local index maps into global map
    let mut iter = validated_results.into_iter();
    let mut global_groups: AHashMap<Vec<vibesql_types::SqlValue>, Vec<usize>> =
        iter.next().unwrap_or_default();

    for local_groups in iter {
        for (key, mut local_indices) in local_groups {
            global_groups.entry(key).or_default().append(&mut local_indices);
        }
    }

    // Phase 3: Materialize rows from indices (Issue #4168)
    // This clones each row exactly once at the end
    let group_count = global_groups.len();
    let mut result: GroupedRows = Vec::with_capacity(group_count);
    for (key, indices) in global_groups {
        let mut group_rows: Vec<vibesql_storage::Row> = Vec::with_capacity(indices.len());
        for idx in indices {
            group_rows.push(rows[idx].clone());
        }
        result.push((key, group_rows));
    }

    Ok(result)
}

/// Simple parallel grouping for datasets smaller than morsel size.
///
/// Uses static `par_chunks()` partitioning since overhead of work-stealing
/// isn't justified for small datasets.
///
/// Updated to use index-based grouping (Issue #4168) for better memory efficiency.
#[cfg(feature = "parallel")]
#[allow(clippy::type_complexity)]
fn group_rows_parallel_simple<'a>(
    rows: &[vibesql_storage::Row],
    group_by_exprs: &[vibesql_ast::Expression],
    components: crate::evaluator::parallel::ParallelComponents<'a>,
    timeout_ctx: &TimeoutContext,
) -> Result<GroupedRows, crate::errors::ExecutorError> {
    use crate::evaluator::CombinedExpressionEvaluator;

    let (schema, database, outer_row, outer_schema, window_mapping, cte_context, enable_cse) =
        components;

    // Get number of threads for chunking
    let num_threads = rayon::current_num_threads();
    let chunk_size = rows.len().div_ceil(num_threads);

    // Phase 1: Parallel map - each thread groups its chunk with a thread-local evaluator
    // Returns (chunk_start_index, local_groups with local indices) for each chunk
    let thread_results: Vec<
        Result<
            (usize, AHashMap<Vec<vibesql_types::SqlValue>, Vec<usize>>),
            crate::errors::ExecutorError,
        >,
    > = rows
        .par_chunks(chunk_size.max(1))
        .enumerate()
        .map(|(chunk_idx, chunk)| {
            let chunk_start = chunk_idx * chunk_size;

            // Create thread-local evaluator (fresh instance, no Rc/RefCell sharing)
            let evaluator = CombinedExpressionEvaluator::from_parallel_components(
                schema,
                database,
                outer_row,
                outer_schema,
                window_mapping,
                cte_context,
                enable_cse,
            );

            // Thread-local group map - stores INDICES instead of rows (Issue #4168)
            let estimated_groups = (chunk.len() / 10).max(16);
            let mut local_groups: AHashMap<Vec<vibesql_types::SqlValue>, Vec<usize>> =
                AHashMap::with_capacity(estimated_groups);

            for (local_idx, row) in chunk.iter().enumerate() {
                // Clear CSE cache before evaluating each row
                evaluator.clear_cse_cache();

                // Evaluate GROUP BY expressions to get the group key
                let mut key = Vec::with_capacity(group_by_exprs.len());
                for expr in group_by_exprs {
                    let value = evaluator.eval(expr, row)?;
                    key.push(value);
                }

                // Store ABSOLUTE index instead of cloning the row
                let absolute_idx = chunk_start + local_idx;
                local_groups.entry(key).or_default().push(absolute_idx);
            }

            Ok((chunk_start, local_groups))
        })
        .collect();

    // Check timeout after parallel phase
    timeout_ctx.check()?;

    // Check for errors from any thread
    let mut validated_results: Vec<AHashMap<Vec<vibesql_types::SqlValue>, Vec<usize>>> =
        Vec::with_capacity(thread_results.len());
    for result in thread_results {
        let (_, local_groups) = result?;
        validated_results.push(local_groups);
    }

    // Phase 2: Sequential reduce - merge thread-local index maps into global map
    let mut iter = validated_results.into_iter();
    let mut global_groups: AHashMap<Vec<vibesql_types::SqlValue>, Vec<usize>> =
        iter.next().unwrap_or_default();

    for local_groups in iter {
        for (key, mut local_indices) in local_groups {
            global_groups.entry(key).or_default().append(&mut local_indices);
        }
    }

    // Phase 3: Materialize rows from indices (Issue #4168)
    // This clones each row exactly once at the end
    let group_count = global_groups.len();
    let mut result: GroupedRows = Vec::with_capacity(group_count);
    for (key, indices) in global_groups {
        let mut group_rows: Vec<vibesql_storage::Row> = Vec::with_capacity(indices.len());
        for idx in indices {
            group_rows.push(rows[idx].clone());
        }
        result.push((key, group_rows));
    }

    Ok(result)
}

/// Create morsels from a row count.
#[cfg(feature = "parallel")]
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
#[cfg(feature = "parallel")]
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

/// Check if morsel debug logging is enabled
#[cfg(feature = "parallel")]
fn morsel_debug_enabled() -> bool {
    std::env::var("MORSEL_DEBUG").is_ok()
}
