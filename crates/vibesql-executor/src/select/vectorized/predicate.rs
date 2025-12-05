//! Chunk-based WHERE clause predicate evaluation
//!
//! Processes rows in chunks of 256 for better instruction cache locality.
//! Single-pass evaluation avoids the overhead of bitmap allocation and double iteration.
//! Falls back to row-by-row evaluation for small row counts (< 100).
//!
//! Performance optimization: Uses compiled predicates for simple WHERE clauses
//! (e.g., AND-combined column comparisons) to avoid expression tree overhead.

use crate::{errors::ExecutorError, evaluator::CombinedExpressionEvaluator};
use vibesql_ast::Expression;
use vibesql_storage::Row;

use super::{compiled_predicate::CompiledWhereClause, VECTORIZE_THRESHOLD};

/// Apply WHERE clause filter using chunk-based evaluation
///
/// Processes rows in chunks of DEFAULT_CHUNK_SIZE (256 rows) for improved:
/// - Instruction cache locality: Keeps predicate evaluation function hot
/// - Code locality: Tight inner loop enables better CPU pipeline efficiency
/// - Reduced overhead: Single-pass evaluation and filtering
///
/// This is NOT true vectorization/SIMD - it's chunking for cache benefits.
/// Automatically falls back to row-by-row for small datasets (< VECTORIZE_THRESHOLD).
///
/// Performance optimization: Attempts to compile simple predicates (AND-combined
/// column comparisons) to avoid expression tree overhead.
pub fn apply_where_filter_vectorized<'a>(
    rows: Vec<Row>,
    where_expr: Option<&Expression>,
    evaluator: &CombinedExpressionEvaluator,
    executor: &crate::SelectExecutor<'a>,
) -> Result<Vec<Row>, ExecutorError> {
    // Early return if no WHERE clause
    if where_expr.is_none() {
        return Ok(rows);
    }

    let where_expr = where_expr.unwrap();
    let row_count = rows.len();

    // For small datasets, row-by-row is more efficient (less overhead)
    if row_count < VECTORIZE_THRESHOLD {
        return super::super::filter::apply_where_filter_combined(
            rows,
            Some(where_expr),
            evaluator,
            executor,
        );
    }

    // Try to compile the WHERE clause for fast-path evaluation
    // This avoids expression tree overhead for simple predicates
    let compiled = CompiledWhereClause::try_compile(where_expr, evaluator.schema());

    // Use pooled buffer for result
    let mut filtered_rows = executor.query_buffer_pool().get_row_buffer(row_count);
    let mut rows_processed = 0;
    const CHECK_INTERVAL: usize = 1000;

    // Fast path: Use compiled predicates if available
    if let Some(compiled_pred) = compiled {
        for row in rows.into_iter() {
            // Check timeout periodically
            rows_processed += 1;
            if rows_processed % CHECK_INTERVAL == 0 {
                executor.check_timeout()?;
            }

            // Evaluate compiled predicate (much faster than expression tree)
            if compiled_pred.evaluate(&row)? {
                filtered_rows.push(row);
            }
        }
    } else {
        // Slow path: Fall back to expression tree evaluation
        for row in rows.into_iter() {
            // Check timeout periodically
            rows_processed += 1;
            if rows_processed % CHECK_INTERVAL == 0 {
                executor.check_timeout()?;
            }

            // Evaluate predicate for this row
            let include_row = evaluate_predicate(&row, where_expr, evaluator)?;

            if include_row {
                filtered_rows.push(row); // Move row, no clone needed
            }
            // Row is dropped if filtered out
        }

        // Clear CSE cache at end of query to prevent cross-query pollution
        // (only needed for expression tree path)
        evaluator.clear_cse_cache();
    }

    // Return pooled buffer and extract result
    let result = std::mem::take(&mut filtered_rows);
    executor.query_buffer_pool().return_row_buffer(filtered_rows);
    Ok(result)
}

/// Evaluate WHERE predicate on a single row
///
/// Extracted as a helper function to keep it hot in instruction cache
/// when processing chunks. The tight loop in the caller enables better
/// CPU pipeline efficiency.
#[inline(always)]
fn evaluate_predicate(
    row: &Row,
    where_expr: &Expression,
    evaluator: &CombinedExpressionEvaluator,
) -> Result<bool, ExecutorError> {
    // CSE cache is NOT cleared between rows because only deterministic expressions
    // (those without column references) are cached. Column values cannot be cached
    // since is_deterministic() returns false for expressions containing column refs.
    // This allows constant sub-expressions like (1 + 2) to be cached across all rows,
    // significantly improving performance for expression-heavy queries.

    let value = evaluator.eval(where_expr, row)?;
    is_truthy(&value)
}

/// Fast truthy evaluation optimized for hot path
///
/// Inlined aggressively and optimized for the common case (Boolean values).
/// Uses early returns to minimize branching in the hot path.
#[inline(always)]
fn is_truthy(value: &vibesql_types::SqlValue) -> Result<bool, ExecutorError> {
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

#[cfg(test)]
#[allow(clippy::assertions_on_constants)]
mod tests {
    use super::*;

    #[test]
    fn test_vectorize_threshold() {
        // Verify threshold is reasonable for cache optimization
        assert!(VECTORIZE_THRESHOLD > 0);
        assert!(VECTORIZE_THRESHOLD >= 100);
    }
}
