//! Scalar subquery evaluation
//!
//! This module handles evaluation of scalar subqueries that must return
//! exactly one row and one column. Includes caching for both uncorrelated
//! and correlated subqueries.

use super::cache::{compute_correlated_cache_key, compute_subquery_hash};
use super::correlation::extract_correlation_values;
use super::super::super::core::CombinedExpressionEvaluator;
use crate::errors::ExecutorError;

impl CombinedExpressionEvaluator<'_> {
    /// Evaluate scalar subquery - must return exactly one row and one column
    ///
    /// **Optimization**: Caches scalar subquery results to avoid redundant execution:
    /// - **Uncorrelated subqueries**: Cached by subquery hash alone (issue #2451)
    /// - **Correlated subqueries**: Cached by (subquery hash, correlation values) (issue #2452)
    ///
    /// This is critical for performance when scalar subqueries appear in WHERE/HAVING
    /// clauses or are evaluated per-row (e.g., TPC-H Q2, Q11, Q17, Q20).
    ///
    /// **Cache key**:
    /// - Uncorrelated: Hash of subquery AST
    /// - Correlated: Hash of (subquery AST + correlation column values)
    ///
    /// **Cache scope**: Per-evaluator instance (lifetime tied to query execution)
    /// **Cache eviction**: LRU with configurable size (default: 5000 entries)
    ///
    /// **Examples**:
    ///
    /// TPC-H Q11 (uncorrelated):
    /// ```sql
    /// HAVING SUM(value) > (SELECT SUM(value) * 0.0001 FROM partsupp ...)
    /// ```
    /// Cached once, reused for all groups (10-100x speedup)
    ///
    /// TPC-H Q2 (correlated):
    /// ```sql
    /// WHERE ps_supplycost = (
    ///     SELECT MIN(ps_supplycost)
    ///     FROM partsupp
    ///     WHERE p_partkey = ps_partkey  -- Correlated on p_partkey
    /// )
    /// ```
    /// Cached per unique p_partkey value (avoids O(N²) execution)
    pub(in crate::evaluator::combined) fn eval_scalar_subquery(
        &self,
        subquery: &vibesql_ast::SelectStmt,
        row: &vibesql_storage::Row,
    ) -> Result<vibesql_types::SqlValue, ExecutorError> {
        // Check depth limit to prevent stack overflow
        if self.depth >= crate::limits::MAX_EXPRESSION_DEPTH {
            return Err(ExecutorError::ExpressionDepthExceeded {
                depth: self.depth,
                max_depth: crate::limits::MAX_EXPRESSION_DEPTH,
            });
        }

        let database = self.database.ok_or(ExecutorError::UnsupportedFeature(
            "Subquery execution requires database reference".to_string(),
        ))?;

        // Determine if subquery is correlated
        let is_uncorrelated = !crate::optimizer::subquery_rewrite::correlation::is_correlated(subquery);

        // Compute cache key (different strategies for correlated vs uncorrelated)
        let cache_key = if is_uncorrelated {
            // Uncorrelated: cache key is just the subquery hash
            compute_subquery_hash(subquery)
        } else if !self.schema.table_schemas.is_empty() {
            // Correlated: cache key includes correlation column values
            // Only attempt if we have an outer schema to reference
            if let Some(correlation_values) = extract_correlation_values(subquery, row, self.schema) {
                let subquery_hash = compute_subquery_hash(subquery);
                compute_correlated_cache_key(subquery_hash, &correlation_values)
            } else {
                // Failed to extract correlation values - skip caching
                // This can happen if correlation columns aren't in outer schema
                // Fall through to execute without caching
                let select_executor = if let Some(cte_ctx) = self.cte_context {
                    crate::select::SelectExecutor::new_with_outer_and_cte_and_depth(
                        database,
                        row,
                        self.schema,
                        cte_ctx,
                        self.depth,
                    )
                } else {
                    crate::select::SelectExecutor::new_with_outer_context_and_depth(
                        database,
                        row,
                        self.schema,
                        self.depth,
                    )
                };
                let rows = select_executor.execute(subquery)?;
                return crate::evaluator::subqueries_shared::eval_scalar_subquery_core(&rows, subquery.select_list.len());
            }
        } else {
            // Correlated but no outer schema - skip caching
            let select_executor = crate::select::SelectExecutor::new(database);
            let rows = select_executor.execute(subquery)?;
            return crate::evaluator::subqueries_shared::eval_scalar_subquery_core(&rows, subquery.select_list.len());
        };

        // Try cache lookup
        let cached_result = self.subquery_cache.borrow().peek(&cache_key).cloned();

        let rows = if let Some(cached_rows) = cached_result {
            // Cache hit - use cached result
            cached_rows
        } else {
            // Cache miss - execute subquery
            let select_executor = if is_uncorrelated {
                // Uncorrelated: execute without outer context
                if let Some(cte_ctx) = self.cte_context {
                    crate::select::SelectExecutor::new_with_cte_and_depth(
                        database,
                        cte_ctx,
                        self.depth,
                    )
                } else {
                    crate::select::SelectExecutor::new(database)
                }
            } else {
                // Correlated: execute with outer context
                if let Some(cte_ctx) = self.cte_context {
                    crate::select::SelectExecutor::new_with_outer_and_cte_and_depth(
                        database,
                        row,
                        self.schema,
                        cte_ctx,
                        self.depth,
                    )
                } else {
                    crate::select::SelectExecutor::new_with_outer_context_and_depth(
                        database,
                        row,
                        self.schema,
                        self.depth,
                    )
                }
            };

            let executed_rows = select_executor.execute(subquery)?;

            // Cache the result for future evaluations
            self.subquery_cache.borrow_mut().put(cache_key, executed_rows.clone());

            executed_rows
        };

        // Delegate to shared logic
        crate::evaluator::subqueries_shared::eval_scalar_subquery_core(&rows, subquery.select_list.len())
    }
}
