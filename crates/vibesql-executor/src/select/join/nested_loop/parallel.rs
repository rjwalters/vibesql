//! Parallel nested loop join implementation.
//!
//! This module provides morsel-driven parallel execution of nested loop joins
//! using work-stealing for efficient load balancing across threads.

#[cfg(feature = "parallel")]
use std::sync::{Arc, Mutex};

#[cfg(feature = "parallel")]
use crossbeam_deque::{Injector, Steal, Worker};

#[cfg(feature = "parallel")]
use super::super::combine_rows;
#[cfg(feature = "parallel")]
use super::condition::eval_join_condition_to_bool;
#[cfg(feature = "parallel")]
use crate::select::morsel::{Morsel, MorselConfig};
#[cfg(feature = "parallel")]
use crate::{
    errors::ExecutorError, evaluator::CombinedExpressionEvaluator, schema::CombinedSchema,
    timeout::TimeoutContext, timeout::CHECK_INTERVAL,
};

/// Environment variable to enable morsel execution debug logging for nested loop join
#[cfg(feature = "parallel")]
const NESTED_LOOP_DEBUG_ENV: &str = "NESTED_LOOP_DEBUG";

/// Check if nested loop debug logging is enabled
#[cfg(feature = "parallel")]
fn nested_loop_debug_enabled() -> bool {
    std::env::var(NESTED_LOOP_DEBUG_ENV).is_ok()
}

/// Create morsels from a row count (duplicated from morsel.rs for encapsulation)
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

/// Parallel nested loop join with morsel-driven parallelism.
///
/// Uses work-stealing to process morsels of the outer relation in parallel,
/// with each worker probing against the entire inner relation.
///
/// # Architecture
///
/// ```text
/// Outer Relation (morsels)
///     ├── Morsel 1 ──► Thread 1 ──► Probe Inner ──► Local Results
///     ├── Morsel 2 ──► Thread 2 ──► Probe Inner ──► Local Results
///     ├── Morsel 3 ──► Thread 3 ──► Probe Inner ──► Local Results
///     └── ...
///          └── Merge All Results (preserving order)
/// ```
///
/// # Performance
///
/// This provides 2-4x speedup for large nested loop joins where hash join
/// isn't applicable (complex predicates, non-equi joins).
#[cfg(feature = "parallel")]
#[allow(clippy::too_many_arguments)]
pub fn execute_nested_loop_parallel(
    outer_rows: &[vibesql_storage::Row],
    inner_rows: &[vibesql_storage::Row],
    condition: &Option<vibesql_ast::Expression>,
    schema: &CombinedSchema,
    database: &vibesql_storage::Database,
    timeout_ctx: &TimeoutContext,
) -> Result<Vec<vibesql_storage::Row>, ExecutorError> {
    let config = MorselConfig::optimal();
    let morsels = create_morsels(outer_rows.len(), config.morsel_size);
    let morsel_count = morsels.len();

    if nested_loop_debug_enabled() {
        eprintln!(
            "[NESTED_LOOP] Parallel join: {} outer rows, {} inner rows, {} morsels (size={})",
            outer_rows.len(),
            inner_rows.len(),
            morsel_count,
            config.morsel_size
        );
    }

    // Create global injector queue
    let injector: Injector<Morsel> = Injector::new();
    for morsel in morsels {
        injector.push(morsel);
    }

    // Thread-safe results storage: (morsel_start_idx, result_rows)
    // We track morsel order to maintain row ordering in final result
    type MorselResults = Arc<Mutex<Vec<(usize, Vec<vibesql_storage::Row>)>>>;
    let results: MorselResults = Arc::new(Mutex::new(Vec::with_capacity(morsel_count)));

    // Track any error that occurs during parallel execution
    let error: Arc<Mutex<Option<ExecutorError>>> = Arc::new(Mutex::new(None));

    // Process morsels in parallel using rayon's thread pool
    rayon::scope(|s| {
        let num_threads = rayon::current_num_threads();

        for _ in 0..num_threads {
            let injector_ref = &injector;
            let results_ref = results.clone();
            let error_ref = error.clone();

            s.spawn(move |_| {
                // Create thread-local worker queue
                let worker: Worker<Morsel> = Worker::new_fifo();

                // Create thread-local evaluator (CSE cache is per-thread)
                let evaluator = CombinedExpressionEvaluator::with_database(schema, database);

                while let Some(m) = steal_morsel(injector_ref, &worker) {
                    // Check if another thread hit an error
                    if error_ref.lock().unwrap().is_some() {
                        break;
                    }

                    let start_idx = m.start_idx();
                    let morsel_rows = m.rows(outer_rows);
                    let mut local_results = Vec::new();
                    let mut iterations = 0;

                    // Process this morsel against the entire inner relation
                    'outer: for outer_row in morsel_rows {
                        for inner_row in inner_rows {
                            // Check timeout periodically
                            iterations += 1;
                            if iterations % CHECK_INTERVAL == 0 {
                                if let Err(e) = timeout_ctx.check() {
                                    *error_ref.lock().unwrap() = Some(e);
                                    break 'outer;
                                }
                            }

                            // Combine rows using optimized helper (single allocation)
                            let combined_row = combine_rows(outer_row, inner_row);

                            // Evaluate join condition
                            let matches = match condition {
                                None => true, // No condition = CROSS JOIN behavior
                                Some(cond) => {
                                    // Clear CSE cache before evaluating join condition
                                    evaluator.clear_cse_cache();

                                    match evaluator.eval(cond, &combined_row) {
                                        Ok(value) => {
                                            match eval_join_condition_to_bool(value) {
                                                Ok(b) => b,
                                                Err(e) => {
                                                    *error_ref.lock().unwrap() = Some(e);
                                                    break 'outer;
                                                }
                                            }
                                        }
                                        Err(e) => {
                                            *error_ref.lock().unwrap() = Some(e);
                                            break 'outer;
                                        }
                                    }
                                }
                            };

                            if matches {
                                local_results.push(combined_row);
                            }
                        }
                    }

                    if nested_loop_debug_enabled() {
                        eprintln!(
                            "[NESTED_LOOP] Thread processed morsel at {} ({} outer rows -> {} results)",
                            start_idx,
                            morsel_rows.len(),
                            local_results.len()
                        );
                    }

                    // Store results with morsel index for ordering
                    results_ref.lock().unwrap().push((start_idx, local_results));
                }
            });
        }
    });

    // Check if any error occurred
    if let Some(e) = error.lock().unwrap().take() {
        return Err(e);
    }

    // Extract results after scope completes (all threads have finished)
    let mut sorted_results = Arc::try_unwrap(results)
        .expect("all threads should have completed")
        .into_inner()
        .expect("mutex not poisoned");

    // Sort by morsel start index to maintain row order
    sorted_results.sort_by_key(|(start_idx, _)| *start_idx);

    // Flatten results
    let total_rows: usize = sorted_results.iter().map(|(_, r)| r.len()).sum();
    let mut final_results = Vec::with_capacity(total_rows);
    for (_, result) in sorted_results {
        final_results.extend(result);
    }

    if nested_loop_debug_enabled() {
        eprintln!(
            "[NESTED_LOOP] Parallel join complete: {} morsels, {} result rows",
            morsel_count,
            final_results.len()
        );
    }

    Ok(final_results)
}
