//! Scalar subquery evaluation
//!
//! This module handles evaluation of scalar subqueries that must return
//! exactly one row and one column. Includes caching for both uncorrelated
//! and correlated subqueries.
//!
//! ## Performance Optimization (Issue #4142)
//!
//! This module uses cached lookups for correlation analysis and subquery hashing:
//! - `is_correlated_cached()`: Caches correlation check result per subquery AST pointer
//! - `compute_subquery_hash_cached()`: Caches hash computation per subquery AST pointer
//!
//! These optimizations avoid O(n) AST traversal and Debug-format string allocation
//! for every row when evaluating correlated scalar subqueries.

use super::{
    super::super::core::CombinedExpressionEvaluator,
    correlation::extract_correlation_values,
    schema_utils::{build_merged_outer_row, build_merged_outer_schema},
};
use crate::{errors::ExecutorError, evaluator::caching::compute_correlated_cache_key};

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

        // Determine if subquery is correlated (cached by AST pointer - issue #4142)
        let is_uncorrelated = !self.is_correlated_cached(subquery);

        // Compute cache key (different strategies for correlated vs uncorrelated)
        let cache_key = if is_uncorrelated {
            // Uncorrelated: cache key is just the subquery hash (cached by AST pointer - issue
            // #4142)
            self.compute_subquery_hash_cached(subquery)
        } else if !self.schema.table_schemas.is_empty() {
            // Correlated: cache key includes correlation column values
            // Only attempt if we have an outer schema to reference
            if let Some(correlation_values) = extract_correlation_values(subquery, row, self.schema)
            {
                // Hash is cached by AST pointer (issue #4142)
                let subquery_hash = self.compute_subquery_hash_cached(subquery);
                compute_correlated_cache_key(subquery_hash, &correlation_values)
            } else {
                // Failed to extract correlation values - skip caching
                // This can happen if correlation columns aren't in outer schema
                // Fall through to execute without caching
                let merged_schema = build_merged_outer_schema(self.schema, self.outer_schema);
                let merged_row = build_merged_outer_row(row, self.outer_row);
                let select_executor = if let Some(cte_ctx) = self.cte_context {
                    crate::select::SelectExecutor::new_with_outer_and_cte_and_depth(
                        database,
                        &merged_row,
                        &merged_schema,
                        cte_ctx,
                        self.depth,
                    )
                } else {
                    crate::select::SelectExecutor::new_with_outer_context_and_depth(
                        database,
                        &merged_row,
                        &merged_schema,
                        self.depth,
                    )
                };
                let rows = select_executor.execute(subquery)?;
                return crate::evaluator::subqueries_shared::eval_scalar_subquery_core(&rows);
            }
        } else {
            // Correlated but no outer schema - skip caching
            // Still pass CTE context if available (fix for TPC-H Q15)
            let select_executor = if let Some(cte_ctx) = self.cte_context {
                crate::select::SelectExecutor::new_with_cte_and_depth(database, cte_ctx, self.depth)
            } else {
                crate::select::SelectExecutor::new(database)
            };
            let rows = select_executor.execute(subquery)?;
            return crate::evaluator::subqueries_shared::eval_scalar_subquery_core(&rows);
        };

        // Try cache lookup
        let cached_result = self.subquery_cache.borrow().peek(&cache_key).cloned();

        let rows = if let Some(cached_rows) = cached_result {
            // Cache hit - use cached result
            cached_rows
        } else {
            // Cache miss - execute subquery
            // Fix for select1-18.x: ALWAYS build merged schema if outer context exists,
            // even if is_correlated() returns false. The is_correlated() heuristic can
            // miss correlations with unqualified column names (e.g., column 'c' from outer
            // table t1 when subquery has no column named 'c'). By always passing outer
            // context, the column resolver can correctly find external references.
            // This matches the approach used in eval_exists().
            let has_outer_context =
                !self.schema.table_schemas.is_empty() || self.outer_schema.is_some();
            let merged_schema = if has_outer_context {
                Some(build_merged_outer_schema(self.schema, self.outer_schema))
            } else {
                None
            };

            let merged_row = if has_outer_context {
                Some(build_merged_outer_row(row, self.outer_row))
            } else {
                None
            };

            let select_executor =
                if let (Some(ref schema), Some(ref outer_row)) = (&merged_schema, &merged_row) {
                    // Execute with outer context for column resolution
                    if let Some(cte_ctx) = self.cte_context {
                        crate::select::SelectExecutor::new_with_outer_and_cte_and_depth(
                            database, outer_row, schema, cte_ctx, self.depth,
                        )
                    } else {
                        crate::select::SelectExecutor::new_with_outer_context_and_depth(
                            database, outer_row, schema, self.depth,
                        )
                    }
                } else if let Some(cte_ctx) = self.cte_context {
                    crate::select::SelectExecutor::new_with_cte_and_depth(
                        database, cte_ctx, self.depth,
                    )
                } else {
                    crate::select::SelectExecutor::new(database)
                };

            let executed_rows = select_executor.execute(subquery)?;

            // Cache the result for future evaluations
            self.subquery_cache.borrow_mut().put(cache_key, executed_rows.clone());

            executed_rows
        };

        // Delegate to shared logic
        crate::evaluator::subqueries_shared::eval_scalar_subquery_core(&rows)
    }
}
