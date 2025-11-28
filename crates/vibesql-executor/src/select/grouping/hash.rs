use ahash::AHashMap;

/// Grouped rows: (group key values, rows in group)
pub type GroupedRows = Vec<(Vec<vibesql_types::SqlValue>, Vec<vibesql_storage::Row>)>;

// NOTE: Parallel aggregation functions commented out for future implementation
// The current evaluator architecture uses RefCell which is not Send, making it
// incompatible with rayon's parallel iteration. To enable parallel aggregation,
// we would need to:
// 1. Refactor evaluator to be thread-safe (use Arc<Mutex> or thread-local evaluators)
// 2. Or pre-evaluate all expressions sequentially, then parallelize only the accumulation
// 3. Or use a different approach like the hash join (avoid evaluator in parallel sections)
//
// For now, the combine() method infrastructure is in place and tested, ready for
// future parallelization when the evaluator architecture supports it.

// /// Information about an aggregate function to compute
// #[derive(Debug, Clone)]
// pub struct AggregateInfo {
//     pub function_name: String,
//     pub expr: vibesql_ast::Expression,
//     pub distinct: bool,
// }
//
// /// Grouped aggregates: (group key values, aggregate accumulators)
// pub type GroupedAggregates = Vec<(Vec<vibesql_types::SqlValue>, Vec<AggregateAccumulator>)>;

/// Group rows by GROUP BY expressions (original implementation, kept for compatibility)
///
/// Optimized implementation using HashMap for O(1) group lookups instead of O(n) linear search.
/// This significantly improves performance for queries with many groups.
/// Timeout is checked every 1000 rows.
pub fn group_rows<'a>(
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
