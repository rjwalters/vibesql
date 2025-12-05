//! WHERE clause filtering logic

mod pattern;
mod specialized;

use crate::{
    errors::ExecutorError,
    evaluator::{CombinedExpressionEvaluator, ExpressionEvaluator},
};

use pattern::PredicatePattern;
use specialized::create_evaluator;

#[cfg(feature = "parallel")]
use rayon::prelude::*;

#[cfg(feature = "parallel")]
use std::sync::Arc;

#[cfg(feature = "parallel")]
use super::parallel::ParallelConfig;

/// Fast truthy evaluation optimized for hot path (Combined evaluator version)
///
/// Inlined aggressively and optimized for the common case (Boolean values).
#[cfg(feature = "parallel")]
#[inline(always)]
fn is_truthy_combined(value: &vibesql_types::SqlValue) -> Result<bool, ExecutorError> {
    use vibesql_types::SqlValue;

    match value {
        // Fast path: Boolean values (most common case for WHERE predicates)
        SqlValue::Boolean(b) => Ok(*b),
        SqlValue::Null => Ok(false),

        // Integer types (SQLLogicTest compatibility)
        SqlValue::Integer(n) => Ok(*n != 0),
        SqlValue::Smallint(n) => Ok(*n != 0),
        SqlValue::Bigint(n) => Ok(*n != 0),

        // Float types
        SqlValue::Float(f) => Ok(*f != 0.0),
        SqlValue::Real(f) => Ok(*f != 0.0),
        SqlValue::Double(f) => Ok(*f != 0.0),

        // Error case (should be rare)
        other => Err(ExecutorError::InvalidWhereClause(format!(
            "WHERE clause must evaluate to boolean, got: {:?}",
            other
        ))),
    }
}

/// Fast truthy evaluation optimized for hot path (Basic evaluator version)
///
/// Inlined aggressively and optimized for the common case (Boolean values).
#[inline(always)]
fn is_truthy_basic(value: &vibesql_types::SqlValue) -> Result<bool, ExecutorError> {
    use vibesql_types::SqlValue;

    match value {
        // Fast path: Boolean values (most common case for WHERE predicates)
        SqlValue::Boolean(b) => Ok(*b),
        SqlValue::Null => Ok(false),

        // Integer types (SQLLogicTest compatibility)
        SqlValue::Integer(n) => Ok(*n != 0),
        SqlValue::Smallint(n) => Ok(*n != 0),
        SqlValue::Bigint(n) => Ok(*n != 0),

        // Float types
        SqlValue::Float(f) => Ok(*f != 0.0),
        SqlValue::Real(f) => Ok(*f != 0.0),
        SqlValue::Double(f) => Ok(*f != 0.0),

        // Error case (should be rare)
        other => Err(ExecutorError::InvalidWhereClause(format!(
            "WHERE must evaluate to boolean, got: {:?}",
            other
        ))),
    }
}

/// Apply WHERE clause filter to rows (Combined evaluator version)
///
/// Same as apply_where_filter but specifically for CombinedExpressionEvaluator.
/// Used in non-aggregation queries.
///
/// Accepts SelectExecutor for timeout enforcement. Timeout is checked every 1000 rows.
///
/// This version uses pattern recognition to detect common predicates and dispatch
/// to specialized fast-path evaluators that avoid SqlValue enum matching overhead.
pub(super) fn apply_where_filter_combined<'a>(
    rows: Vec<vibesql_storage::Row>,
    where_expr: Option<&vibesql_ast::Expression>,
    evaluator: &CombinedExpressionEvaluator,
    executor: &crate::SelectExecutor<'a>,
) -> Result<Vec<vibesql_storage::Row>, ExecutorError> {
    if where_expr.is_none() {
        // No WHERE clause, return all rows
        return Ok(rows);
    }

    let where_expr = where_expr.unwrap();

    // Try to detect predicate pattern and create specialized evaluator
    let pattern = PredicatePattern::from_where_clause(where_expr, evaluator.schema());
    let specialized_eval = create_evaluator(&pattern);

    // Use pooled buffer to reduce allocation overhead during filtering
    let mut filtered_rows = executor.query_buffer_pool().get_row_buffer(rows.len());
    let mut rows_processed = 0;
    const CHECK_INTERVAL: usize = 1000;

    // Dispatch to specialized fast path or general path
    if let Some(fast_evaluator) = specialized_eval {
        // Fast path: use specialized evaluator (no enum matching)
        for row in rows {
            // Check timeout every 1000 rows
            rows_processed += 1;
            if rows_processed % CHECK_INTERVAL == 0 {
                executor.check_timeout()?;
            }

            // Evaluate predicate using fast path
            if fast_evaluator.evaluate(&row)? {
                filtered_rows.push(row);
            }
        }
    } else {
        // General path: use full expression evaluator
        for row in rows {
            // Check timeout every 1000 rows
            rows_processed += 1;
            if rows_processed % CHECK_INTERVAL == 0 {
                executor.check_timeout()?;
            }

            // CSE cache is NOT cleared between rows because only deterministic expressions
            // (those without column references) are cached. Column values cannot be cached
            // since is_deterministic() returns false for expressions containing column refs.
            // This allows constant sub-expressions like (1 + 2) to be cached across all rows,
            // significantly improving performance for expression-heavy queries.

            let include_row = match evaluator.eval(where_expr, &row)? {
                vibesql_types::SqlValue::Boolean(true) => true,
                vibesql_types::SqlValue::Boolean(false) | vibesql_types::SqlValue::Null => false,
                // SQLLogicTest compatibility: treat integers as truthy/falsy (C-like behavior)
                vibesql_types::SqlValue::Integer(0) => false,
                vibesql_types::SqlValue::Integer(_) => true,
                vibesql_types::SqlValue::Smallint(0) => false,
                vibesql_types::SqlValue::Smallint(_) => true,
                vibesql_types::SqlValue::Bigint(0) => false,
                vibesql_types::SqlValue::Bigint(_) => true,
                vibesql_types::SqlValue::Float(0.0) => false,
                vibesql_types::SqlValue::Float(_) => true,
                vibesql_types::SqlValue::Real(0.0) => false,
                vibesql_types::SqlValue::Real(_) => true,
                vibesql_types::SqlValue::Double(0.0) => false,
                vibesql_types::SqlValue::Double(_) => true,
                other => {
                    return Err(ExecutorError::InvalidWhereClause(format!(
                        "WHERE clause must evaluate to boolean, got: {:?}",
                        other
                    )))
                }
            };

            if include_row {
                filtered_rows.push(row);
            }
        }

        // Clear CSE cache at end of query to prevent cross-query pollution
        // Cache can persist within a single query for performance, but must be
        // cleared between different SQL statements to avoid stale values
        evaluator.clear_cse_cache();
    }

    // Move data to final result and return pooled buffer
    // This allows buffer reuse while avoiding clone overhead
    let result = std::mem::take(&mut filtered_rows);
    executor.query_buffer_pool().return_row_buffer(filtered_rows);
    Ok(result)
}

/// Apply WHERE clause filter to rows (Basic evaluator version)
///
/// Same as apply_where_filter but specifically for ExpressionEvaluator.
/// Used in aggregation queries.
///
/// Accepts SelectExecutor for timeout enforcement. Timeout is checked every 1000 rows.
#[allow(dead_code)]
pub(super) fn apply_where_filter_basic<'a>(
    rows: Vec<vibesql_storage::Row>,
    where_expr: Option<&vibesql_ast::Expression>,
    evaluator: &ExpressionEvaluator,
    executor: &crate::SelectExecutor<'a>,
) -> Result<Vec<vibesql_storage::Row>, ExecutorError> {
    if where_expr.is_none() {
        // No WHERE clause, return all rows
        return Ok(rows);
    }

    let where_expr = where_expr.unwrap();
    // Use pooled buffer to reduce allocation overhead
    let mut filtered_rows = executor.query_buffer_pool().get_row_buffer(rows.len());
    let mut rows_processed = 0;
    const CHECK_INTERVAL: usize = 1000;

    // Consume input vector to avoid cloning rows
    for row in rows.into_iter() {
        // Check timeout every 1000 rows
        rows_processed += 1;
        if rows_processed % CHECK_INTERVAL == 0 {
            executor.check_timeout()?;
        }

        // CSE cache is NOT cleared between rows because only deterministic expressions
        // (those without column references) are cached. Column values cannot be cached
        // since is_deterministic() returns false for expressions containing column refs.
        // This allows constant sub-expressions like (1 + 2) to be cached across all rows,
        // significantly improving performance for expression-heavy queries.

        let value = evaluator.eval(where_expr, &row)?;
        let include_row = is_truthy_basic(&value)?;

        if include_row {
            filtered_rows.push(row); // Move row, no clone needed
        }
        // Row is dropped if filtered out
    }

    // Clear CSE cache at end of query to prevent cross-query pollution
    // Cache can persist within a single query for performance, but must be
    // cleared between different SQL statements to avoid stale values
    evaluator.clear_cse_cache();

    // Move data to final result and return pooled buffer
    // This allows buffer reuse while avoiding clone overhead
    let result = std::mem::take(&mut filtered_rows);
    executor.query_buffer_pool().return_row_buffer(filtered_rows);
    Ok(result)
}

/// Parallel version of apply_where_filter_combined
/// Uses rayon to evaluate WHERE predicates across multiple threads
#[cfg(feature = "parallel")]
#[allow(dead_code)]
pub(super) fn apply_where_filter_combined_parallel<'a>(
    rows: Vec<vibesql_storage::Row>,
    where_expr: Option<&vibesql_ast::Expression>,
    evaluator: &CombinedExpressionEvaluator,
    _executor: &crate::SelectExecutor<'a>,
) -> Result<Vec<vibesql_storage::Row>, ExecutorError> {
    if where_expr.is_none() {
        return Ok(rows);
    }

    // Check if we should parallelize based on hardware-aware heuristics
    let config = ParallelConfig::global();
    if !config.should_parallelize_scan(rows.len()) {
        return apply_where_filter_combined(rows, where_expr, evaluator, _executor);
    }

    let where_expr = where_expr.unwrap();

    // Clone the expression for thread-safe sharing
    let where_expr_arc = Arc::new(where_expr.clone());

    // Extract evaluator components before parallel execution (including CTE context)
    // Issue #3562: Now includes cte_context for IN subqueries referencing CTEs
    let (schema, database, outer_row, outer_schema, window_mapping, cte_context, enable_cse) =
        evaluator.get_parallel_components();

    // Use rayon's parallel iterator for filtering
    let result: Result<Vec<_>, ExecutorError> = rows
        .into_par_iter()
        .map(|row| {
            // Create a thread-local evaluator with independent caches
            let thread_evaluator = CombinedExpressionEvaluator::from_parallel_components(
                schema,
                database,
                outer_row,
                outer_schema,
                window_mapping,
                cte_context,
                enable_cse,
            );

            // Evaluate predicate for this row
            let value = thread_evaluator.eval(&where_expr_arc, &row)?;
            let include_row = is_truthy_combined(&value)?;

            if include_row {
                Ok(Some(row))
            } else {
                Ok(None)
            }
        })
        .collect();

    // Filter out None values and extract Ok rows
    result.map(|v| v.into_iter().flatten().collect())
}

/// Auto-selecting WHERE filter that uses hardware-aware heuristics
/// to choose between vectorized, parallel, or sequential execution.
///
/// The decision is based on:
/// - Row count (vectorized for medium datasets, parallel for large)
/// - Number of CPU cores available
/// - Cache optimization considerations
/// - User override via PARALLEL_THRESHOLD environment variable
///
/// Strategy:
/// - < 100 rows: Sequential (low overhead)
/// - 100-10000 rows: Vectorized (cache-friendly chunking)
/// - > 10000 rows: Parallel (multi-core utilization)
pub(crate) fn apply_where_filter_combined_auto<'a>(
    rows: Vec<vibesql_storage::Row>,
    where_expr: Option<&vibesql_ast::Expression>,
    evaluator: &CombinedExpressionEvaluator,
    executor: &crate::SelectExecutor<'a>,
) -> Result<Vec<vibesql_storage::Row>, ExecutorError> {
    if where_expr.is_none() {
        return Ok(rows);
    }

    // For very large datasets, use parallel execution
    #[cfg(feature = "parallel")]
    {
        let row_count = rows.len();
        let config = ParallelConfig::global();
        if config.should_parallelize_scan(row_count) {
            return apply_where_filter_combined_parallel(rows, where_expr, evaluator, executor);
        }
    }

    // For medium datasets, use vectorized (chunk-based) execution
    // This provides better cache locality than row-by-row without
    // the overhead of parallelization
    super::vectorized::apply_where_filter_vectorized(rows, where_expr, evaluator, executor)
}
