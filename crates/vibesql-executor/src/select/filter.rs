//! WHERE clause filtering logic

mod pattern;
mod specialized;

#[cfg(feature = "parallel")]
use std::sync::Arc;

use pattern::PredicatePattern;
use specialized::create_evaluator;

#[cfg(feature = "parallel")]
use super::parallel::ParallelConfig;
use crate::{
    errors::ExecutorError,
    evaluator::{CombinedExpressionEvaluator, ExpressionEvaluator},
};

/// Convert string to boolean using SQLite semantics
/// SQLite converts strings to numeric first, then to boolean:
/// - "1x" → 1.0 → true
/// - "0" → 0.0 → false
/// - "" → 0.0 → false
/// - "abc" → 0.0 → false (no leading digits)
#[inline(always)]
fn string_to_truthy(s: &str) -> bool {
    // SQLite behavior: parse leading numeric portion
    // Empty string is falsy
    if s.is_empty() {
        return false;
    }
    // Try to parse as f64, extracting leading numeric portion
    // "1x" → 1.0, "0.5abc" → 0.5, "abc" → 0.0
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return false;
    }
    // Find the longest numeric prefix
    let mut end = 0;
    let mut has_dot = false;
    let mut has_digit = false;
    let chars: Vec<char> = trimmed.chars().collect();

    // Handle optional leading sign
    if !chars.is_empty() && (chars[0] == '-' || chars[0] == '+') {
        end = 1;
    }

    while end < chars.len() {
        let c = chars[end];
        if c.is_ascii_digit() {
            has_digit = true;
            end += 1;
        } else if c == '.' && !has_dot {
            has_dot = true;
            end += 1;
        } else {
            break;
        }
    }

    if !has_digit {
        return false;
    }

    // Parse the numeric prefix
    let num_str: String = chars[..end].iter().collect();
    match num_str.parse::<f64>() {
        Ok(n) => n != 0.0,
        Err(_) => false,
    }
}

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
        SqlValue::Numeric(f) => Ok(*f != 0.0),

        // String types (SQLite coerces strings to numeric for boolean context)
        SqlValue::Varchar(s) | SqlValue::Character(s) => Ok(string_to_truthy(&s)),

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
        SqlValue::Numeric(f) => Ok(*f != 0.0),

        // String types (SQLite coerces strings to numeric for boolean context)
        SqlValue::Varchar(s) | SqlValue::Character(s) => Ok(string_to_truthy(&s)),

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
                vibesql_types::SqlValue::Numeric(n) if n == 0.0 => false,
                vibesql_types::SqlValue::Numeric(_) => true,
                // String types (SQLite coerces strings to numeric for boolean context)
                vibesql_types::SqlValue::Varchar(ref s)
                | vibesql_types::SqlValue::Character(ref s) => string_to_truthy(s),
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
/// Uses morsel-driven work-stealing for dynamic load balancing across threads.
///
/// Performance optimization: Attempts to compile predicates before parallel execution.
/// Compiled predicates avoid expression tree overhead and are thread-safe.
///
/// # Morsel-Driven Execution
///
/// Instead of static partitioning (dividing rows into N equal chunks), this uses
/// a morsel dispatcher with work-stealing, enabling near-linear scaling to 16+ cores.
/// Benefits:
/// - Dynamic load balancing when predicate evaluation costs vary
/// - Better cache efficiency with L3-cache-sized morsels (~50K rows)
/// - Near-linear scaling (>85% efficiency at 8+ cores)
#[cfg(feature = "parallel")]
#[allow(dead_code)]
pub(super) fn apply_where_filter_combined_parallel<'a>(
    rows: Vec<vibesql_storage::Row>,
    where_expr: Option<&vibesql_ast::Expression>,
    evaluator: &CombinedExpressionEvaluator,
    _executor: &crate::SelectExecutor<'a>,
) -> Result<Vec<vibesql_storage::Row>, ExecutorError> {
    use super::{
        morsel::{morsel_parallel_filter, MorselConfig},
        vectorized::compiled_predicate::{CompiledOrClause, CompiledWhereClause},
    };

    if where_expr.is_none() {
        return Ok(rows);
    }

    // Check if we should parallelize based on hardware-aware heuristics
    let config = ParallelConfig::global();
    if !config.should_parallelize_scan(rows.len()) {
        return apply_where_filter_combined(rows, where_expr, evaluator, _executor);
    }

    let where_expr = where_expr.unwrap();

    // Try to compile the predicate for fast parallel evaluation
    // Compiled predicates are thread-safe and avoid expression tree overhead
    let compiled_and = CompiledWhereClause::try_compile(where_expr, evaluator.schema());
    let compiled_or = if compiled_and.is_none() {
        CompiledOrClause::try_compile(where_expr, evaluator.schema())
    } else {
        None
    };

    // Debug: Log which path is being taken
    if std::env::var("OR_COMPILE_DEBUG").is_ok() {
        eprintln!(
            "[PARALLEL_COMPILE] AND compiled: {}, OR compiled: {}, using MORSEL execution",
            compiled_and.is_some(),
            compiled_or.is_some()
        );
    }

    // Use optimal morsel sizing for work-stealing efficiency
    // Adaptive sizing based on schema would require extracting DataTypes from CombinedSchema,
    // but the default 50K rows/morsel is well-tuned for typical workloads
    let morsel_config = MorselConfig::optimal();

    // Fast path 1: Use compiled AND predicates with morsel-driven work-stealing
    if let Some(compiled) = compiled_and {
        let compiled_arc = Arc::new(compiled);
        let filtered = morsel_parallel_filter(&rows, &morsel_config, |row| {
            // Compiled predicates are infallible for well-formed expressions
            compiled_arc.evaluate(row).unwrap_or(false)
        });
        return Ok(filtered);
    }

    // Fast path 2: Use compiled OR predicates with morsel-driven work-stealing
    if let Some(compiled) = compiled_or {
        let compiled_arc = Arc::new(compiled);
        let filtered = morsel_parallel_filter(&rows, &morsel_config, |row| {
            compiled_arc.evaluate(row).unwrap_or(false)
        });
        return Ok(filtered);
    }

    // Slow path: Fall back to expression tree evaluation with morsel-driven execution
    // Clone the expression for thread-safe sharing
    let where_expr_arc = Arc::new(where_expr.clone());

    // Extract evaluator components before parallel execution (including CTE context)
    // Issue #3562: Now includes cte_context for IN subqueries referencing CTEs
    let (schema, database, outer_row, outer_schema, window_mapping, cte_context, enable_cse) =
        evaluator.get_parallel_components();

    // Use morsel-driven parallel filter with work-stealing
    // Each worker creates a thread-local evaluator with independent caches
    let filtered = morsel_parallel_filter(&rows, &morsel_config, |row| {
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
        match thread_evaluator.eval(&where_expr_arc, row) {
            Ok(value) => is_truthy_combined(&value).unwrap_or(false),
            Err(_) => false, // Filter out rows that cause evaluation errors
        }
    });

    Ok(filtered)
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
    if std::env::var("OR_COMPILE_DEBUG").is_ok() {
        eprintln!(
            "[FILTER_AUTO_ENTRY] Called with {} rows, where_expr: {}",
            rows.len(),
            where_expr.is_some()
        );
    }
    if where_expr.is_none() {
        return Ok(rows);
    }

    // Issue #5809: evaluate uncorrelated scalar subqueries exactly once and
    // substitute their literal values before per-row filtering begins. The
    // parallel paths below build fresh thread-local evaluators (with empty
    // subquery caches) per row, so without this hoist an uncorrelated
    // subquery like `WHERE x = (SELECT MAX(x) FROM t)` re-executes its full
    // scan for every row — an O(n²) blowup.
    let hoisted = where_expr.and_then(|expr| evaluator.hoist_uncorrelated_scalar_subqueries(expr));
    let where_expr = match hoisted.as_ref() {
        Some(rewritten) => Some(rewritten),
        None => where_expr,
    };

    // For very large datasets, use parallel execution
    #[cfg(feature = "parallel")]
    {
        let row_count = rows.len();
        let config = ParallelConfig::global();
        if config.should_parallelize_scan(row_count) {
            if std::env::var("OR_COMPILE_DEBUG").is_ok() {
                eprintln!("[FILTER_AUTO] Using PARALLEL path for {} rows", row_count);
            }
            return apply_where_filter_combined_parallel(rows, where_expr, evaluator, executor);
        }
    }

    // For medium datasets, use vectorized (chunk-based) execution
    // This provides better cache locality than row-by-row without
    // the overhead of parallelization
    if std::env::var("OR_COMPILE_DEBUG").is_ok() {
        eprintln!("[FILTER_AUTO] Calling vectorized filter on {} rows", rows.len());
    }
    super::vectorized::apply_where_filter_vectorized(rows, where_expr, evaluator, executor)
}
