//! Parallel Sink/Combine/Finalize pattern for parallel aggregation
//!
//! This module implements DuckDB's Sink/Combine/Finalize pattern for parallel
//! aggregation (Issue #4132). The key insight is that we create thread-local
//! evaluators and accumulators, process chunks in parallel, then merge results.
//!
//! ## Design
//!
//! Each thread gets its own:
//! - `CombinedExpressionEvaluator` (fresh instance, no `Rc`/`RefCell` sharing)
//! - `HashMap<GroupKey, Vec<AggregateAccumulator>>` for thread-local groups
//!
//! After parallel processing, thread-local maps are merged using the
//! `AggregateAccumulator::combine()` method.
//!
//! ## Thread Safety
//!
//! This pattern avoids the `Send` constraint on `CombinedExpressionEvaluator` by:
//! 1. Creating fresh evaluators per thread (no shared `Rc`/`RefCell`)
//! 2. Only sharing read-only data (schema, database, expressions) across threads
//! 3. Merging results sequentially after parallel phase

#[cfg(feature = "parallel")]
use ahash::AHashMap;
#[cfg(feature = "parallel")]
use rayon::prelude::*;
#[cfg(feature = "parallel")]
use vibesql_storage::Row;
#[cfg(feature = "parallel")]
use vibesql_types::SqlValue;

#[cfg(feature = "parallel")]
use super::aggregates::AggregateAccumulator;
#[cfg(feature = "parallel")]
use crate::errors::ExecutorError;
#[cfg(feature = "parallel")]
use crate::evaluator::CombinedExpressionEvaluator;
#[cfg(feature = "parallel")]
use crate::schema::CombinedSchema;
#[cfg(feature = "parallel")]
use crate::timeout::TimeoutContext;

/// Thread-local state for parallel aggregation
///
/// Each thread maintains its own group map and evaluator to avoid
/// synchronization overhead during the sink phase.
#[cfg(feature = "parallel")]
#[allow(dead_code)] // WIP: Will be used when parallel aggregation is integrated
struct ThreadLocalAggregationState {
    /// Thread-local group accumulators: group_key -> accumulators
    groups: AHashMap<Vec<SqlValue>, Vec<AggregateAccumulator>>,
}

#[cfg(feature = "parallel")]
#[allow(dead_code)] // WIP: Will be used when parallel aggregation is integrated
impl ThreadLocalAggregationState {
    fn new() -> Self {
        Self { groups: AHashMap::new() }
    }
}

/// Parallel grouping and aggregation using Sink/Combine/Finalize pattern
///
/// This function parallelizes GROUP BY aggregation by:
/// 1. **Sink**: Each thread processes a chunk of rows with its own evaluator
/// 2. **Combine**: Thread-local group maps are merged using accumulator.combine()
/// 3. **Finalize**: Final aggregation results are returned
///
/// Timeout is checked before and after parallel processing (Issue #4151).
///
/// # Arguments
/// * `rows` - Input rows to group and aggregate
/// * `group_by_exprs` - GROUP BY expressions
/// * `aggregate_infos` - Aggregate function specifications
/// * `schema` - Combined schema for evaluation
/// * `database` - Database reference for subquery evaluation
///
/// # Returns
/// Vector of (group_key, finalized_values) pairs
#[cfg(feature = "parallel")]
#[allow(dead_code)] // WIP: Will be used when parallel aggregation is integrated
#[allow(clippy::type_complexity)] // Complex type is inherent to parallel aggregation pattern
pub fn parallel_group_aggregate<'a>(
    rows: &[Row],
    group_by_exprs: &[vibesql_ast::Expression],
    aggregate_infos: &[AggregateInfo],
    schema: &'a CombinedSchema,
    database: &'a vibesql_storage::Database,
) -> Result<Vec<(Vec<SqlValue>, Vec<SqlValue>)>, ExecutorError> {
    use crate::select::parallel::ParallelConfig;

    let config = ParallelConfig::global();

    // Check if we should use parallel execution
    if !config.should_parallelize_aggregate(rows.len()) {
        // Fall back to sequential for small datasets
        return sequential_group_aggregate(rows, group_by_exprs, aggregate_infos, schema, database);
    }

    // Use default timeout context (proper propagation from SelectExecutor is a future improvement)
    let timeout_ctx = TimeoutContext::new_default();

    // Check timeout before parallel execution (can't check mid-parallel easily)
    timeout_ctx.check()?;

    // Get number of threads for chunking
    let num_threads = rayon::current_num_threads();
    let chunk_size = rows.len().div_ceil(num_threads);

    // Phase 1: Sink - Process chunks in parallel with thread-local state
    let thread_local_results: Vec<
        Result<AHashMap<Vec<SqlValue>, Vec<AggregateAccumulator>>, ExecutorError>,
    > = rows
        .par_chunks(chunk_size.max(1))
        .map(|chunk| {
            // Create thread-local evaluator (fresh instance, no Rc sharing)
            let evaluator = CombinedExpressionEvaluator::with_database(schema, database);

            let mut state = ThreadLocalAggregationState::new();

            // Process each row in this chunk
            for row in chunk {
                // Clear CSE cache before evaluating each row
                evaluator.clear_cse_cache();

                // Evaluate GROUP BY expressions to get the group key
                let mut key = Vec::with_capacity(group_by_exprs.len());
                for expr in group_by_exprs {
                    let value = evaluator.eval(expr, row)?;
                    key.push(value);
                }

                // Get or create accumulators for this group
                let accumulators = state.groups.entry(key).or_insert_with(|| {
                    aggregate_infos
                        .iter()
                        .map(|info| AggregateAccumulator::new(&info.function_name, info.distinct))
                        .collect::<Result<Vec<_>, _>>()
                        .unwrap_or_default()
                });

                // Accumulate values for each aggregate
                for (i, info) in aggregate_infos.iter().enumerate() {
                    if i < accumulators.len() {
                        let value = evaluator.eval(&info.expr, row)?;
                        accumulators[i].accumulate(&value);
                    }
                }
            }

            Ok(state.groups)
        })
        .collect();

    // Check timeout after parallel phase
    timeout_ctx.check()?;

    // Check for errors from any thread
    let mut thread_results: Vec<AHashMap<Vec<SqlValue>, Vec<AggregateAccumulator>>> =
        Vec::with_capacity(thread_local_results.len());
    for result in thread_local_results {
        thread_results.push(result?);
    }

    // Phase 2: Combine - Merge thread-local maps
    let mut global_groups: AHashMap<Vec<SqlValue>, Vec<AggregateAccumulator>> = AHashMap::new();

    for local_groups in thread_results {
        for (key, local_accumulators) in local_groups {
            match global_groups.entry(key) {
                std::collections::hash_map::Entry::Occupied(mut entry) => {
                    // Merge with existing accumulators
                    let global_accumulators = entry.get_mut();
                    for (i, local_acc) in local_accumulators.into_iter().enumerate() {
                        if i < global_accumulators.len() {
                            global_accumulators[i].combine(local_acc)?;
                        }
                    }
                }
                std::collections::hash_map::Entry::Vacant(entry) => {
                    // First time seeing this group
                    entry.insert(local_accumulators);
                }
            }
        }
    }

    // Phase 3: Finalize - Extract final values
    let results: Vec<(Vec<SqlValue>, Vec<SqlValue>)> = global_groups
        .into_iter()
        .map(|(key, accumulators)| {
            let values: Vec<SqlValue> = accumulators.iter().map(|acc| acc.finalize()).collect();
            (key, values)
        })
        .collect();

    Ok(results)
}

/// Sequential fallback for small datasets
#[cfg(feature = "parallel")]
#[allow(dead_code)] // WIP: Will be used when parallel aggregation is integrated
#[allow(clippy::type_complexity)] // Complex type is inherent to parallel aggregation pattern
fn sequential_group_aggregate<'a>(
    rows: &[Row],
    group_by_exprs: &[vibesql_ast::Expression],
    aggregate_infos: &[AggregateInfo],
    schema: &'a CombinedSchema,
    database: &'a vibesql_storage::Database,
) -> Result<Vec<(Vec<SqlValue>, Vec<SqlValue>)>, ExecutorError> {
    let evaluator = CombinedExpressionEvaluator::with_database(schema, database);
    let mut groups: AHashMap<Vec<SqlValue>, Vec<AggregateAccumulator>> = AHashMap::new();

    for row in rows {
        evaluator.clear_cse_cache();

        // Evaluate GROUP BY expressions
        let mut key = Vec::with_capacity(group_by_exprs.len());
        for expr in group_by_exprs {
            let value = evaluator.eval(expr, row)?;
            key.push(value);
        }

        // Get or create accumulators
        let accumulators = groups.entry(key).or_insert_with(|| {
            aggregate_infos
                .iter()
                .map(|info| AggregateAccumulator::new(&info.function_name, info.distinct))
                .collect::<Result<Vec<_>, _>>()
                .unwrap_or_default()
        });

        // Accumulate values
        for (i, info) in aggregate_infos.iter().enumerate() {
            if i < accumulators.len() {
                let value = evaluator.eval(&info.expr, row)?;
                accumulators[i].accumulate(&value);
            }
        }
    }

    // Finalize
    let results: Vec<(Vec<SqlValue>, Vec<SqlValue>)> = groups
        .into_iter()
        .map(|(key, accumulators)| {
            let values: Vec<SqlValue> = accumulators.iter().map(|acc| acc.finalize()).collect();
            (key, values)
        })
        .collect();

    Ok(results)
}

/// Information about an aggregate function to compute
#[derive(Debug, Clone)]
#[allow(dead_code)] // WIP: Will be used when parallel aggregation is integrated
pub struct AggregateInfo {
    /// Function name (COUNT, SUM, AVG, MIN, MAX)
    pub function_name: String,
    /// Expression to aggregate
    pub expr: vibesql_ast::Expression,
    /// Whether DISTINCT modifier is applied
    pub distinct: bool,
}

#[allow(dead_code)] // WIP: Will be used when parallel aggregation is integrated
impl AggregateInfo {
    /// Create a new AggregateInfo
    pub fn new(function_name: String, expr: vibesql_ast::Expression, distinct: bool) -> Self {
        Self { function_name, expr, distinct }
    }
}

#[cfg(all(test, feature = "parallel"))]
mod tests {
    use super::*;

    #[test]
    fn test_aggregate_info_creation() {
        let expr = vibesql_ast::Expression::ColumnRef { schema: None, table: None, column: "col1".to_string() };
        let info = AggregateInfo::new("SUM".to_string(), expr, false);
        assert_eq!(info.function_name, "SUM");
        assert!(!info.distinct);
    }
}
