use ahash::AHashMap;

#[cfg(feature = "parallel")]
use rayon::prelude::*;

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

            return group_rows_parallel(rows, group_by_exprs, components);
        }
    }

    // Sequential fallback
    group_rows_sequential(rows, group_by_exprs, evaluator, executor)
}

/// Sequential grouping implementation
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
    let mut groups_map: AHashMap<Vec<vibesql_types::SqlValue>, Vec<vibesql_storage::Row>> =
        AHashMap::with_capacity(estimated_groups);
    let mut rows_processed = 0;
    const CHECK_INTERVAL: usize = 1000;

    for row in rows {
        // Check timeout every 1000 rows
        rows_processed += 1;
        if rows_processed % CHECK_INTERVAL == 0 {
            executor.check_timeout()?;
        }

        // Clear CSE cache before evaluating each row to prevent column values
        // from being incorrectly cached across different rows
        evaluator.clear_cse_cache();

        // Evaluate GROUP BY expressions to get the group key
        let mut key = Vec::new();
        for expr in group_by_exprs {
            let value = evaluator.eval(expr, row)?;
            key.push(value);
        }

        // Insert or update group using HashMap (O(1) lookup)
        groups_map.entry(key).or_default().push(row.clone());
    }

    // Convert HashMap back to Vec for compatibility with existing code
    Ok(groups_map.into_iter().collect())
}

/// Parallel grouping implementation using thread-local evaluators (Issue #4132)
///
/// This function parallelizes GROUP BY by:
/// 1. **Partition**: Split rows into chunks for parallel processing
/// 2. **Map**: Each thread uses a fresh evaluator to compute group keys
/// 3. **Reduce**: Thread-local group maps are merged into a global map
///
/// Thread safety is achieved by creating fresh evaluators per thread,
/// avoiding the `Send` constraint on `RefCell`/`Rc` in `CombinedExpressionEvaluator`.
#[cfg(feature = "parallel")]
fn group_rows_parallel<'a>(
    rows: &[vibesql_storage::Row],
    group_by_exprs: &[vibesql_ast::Expression],
    components: crate::evaluator::parallel::ParallelComponents<'a>,
) -> Result<GroupedRows, crate::errors::ExecutorError> {
    use crate::evaluator::CombinedExpressionEvaluator;

    let (schema, database, outer_row, outer_schema, window_mapping, cte_context, enable_cse) =
        components;

    // Get number of threads for chunking
    let num_threads = rayon::current_num_threads();
    let chunk_size = (rows.len() + num_threads - 1) / num_threads;

    // Phase 1: Parallel map - each thread groups its chunk with a thread-local evaluator
    let thread_results: Vec<
        Result<AHashMap<Vec<vibesql_types::SqlValue>, Vec<vibesql_storage::Row>>, crate::errors::ExecutorError>,
    > = rows
        .par_chunks(chunk_size.max(1))
        .map(|chunk| {
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

            // Thread-local group map
            let estimated_groups = (chunk.len() / 10).max(16);
            let mut local_groups: AHashMap<Vec<vibesql_types::SqlValue>, Vec<vibesql_storage::Row>> =
                AHashMap::with_capacity(estimated_groups);

            for row in chunk {
                // Clear CSE cache before evaluating each row
                evaluator.clear_cse_cache();

                // Evaluate GROUP BY expressions to get the group key
                let mut key = Vec::with_capacity(group_by_exprs.len());
                for expr in group_by_exprs {
                    let value = evaluator.eval(expr, row)?;
                    key.push(value);
                }

                // Insert into thread-local map
                local_groups.entry(key).or_default().push(row.clone());
            }

            Ok(local_groups)
        })
        .collect();

    // Check for errors from any thread
    let mut validated_results: Vec<AHashMap<Vec<vibesql_types::SqlValue>, Vec<vibesql_storage::Row>>> =
        Vec::with_capacity(thread_results.len());
    for result in thread_results {
        validated_results.push(result?);
    }

    // Phase 2: Sequential reduce - merge thread-local maps into global map
    // Start with the largest map to minimize re-insertions
    let mut iter = validated_results.into_iter();
    let mut global_groups = iter.next().unwrap_or_default();

    for local_groups in iter {
        for (key, mut local_rows) in local_groups {
            global_groups.entry(key).or_default().append(&mut local_rows);
        }
    }

    // Convert HashMap back to Vec for compatibility with existing code
    Ok(global_groups.into_iter().collect())
}
