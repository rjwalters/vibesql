//! Iterator-based execution strategy for simple queries
//!
//! This module implements lazy iteration to avoid materializing intermediate results.
//! The pipeline: scan → filter → skip → take → collect → project
//! WHERE filtering, OFFSET, and LIMIT are fully lazy, providing memory efficiency
//! and early termination.

use super::{builder::SelectExecutor, validation::validate_where_clause_subqueries};
use crate::{
    errors::ExecutorError,
    optimizer::optimize_where_clause,
    pipeline::ExecutionContext,
    select::{
        iterator::{FilterIterator, RowIterator, TableScanIterator},
        join::FromResult,
        projection::project_row_combined,
        projection_simd::try_batch_project_simd,
        window::{expression_has_window_function, has_window_functions},
    },
};

impl SelectExecutor<'_> {
    /// Determine if we can use iterator-based execution for this query
    ///
    /// Iterator execution is beneficial for queries that don't require full materialization.
    /// We must materialize for: ORDER BY, DISTINCT, and window functions.
    pub(in crate::select::executor) fn can_use_iterator_execution(
        stmt: &vibesql_ast::SelectStmt,
    ) -> bool {
        // Can't use iterators if we have ORDER BY (requires sorting all rows)
        if stmt.order_by.is_some() {
            return false;
        }

        // Can't use iterators if we have DISTINCT (requires deduplication of all rows)
        if stmt.distinct {
            return false;
        }

        // Can't use iterators if we have window functions (requires full window frames)
        if has_window_functions(&stmt.select_list) {
            return false;
        }

        // Can't use iterators if ORDER BY has window functions
        if let Some(order_by) = &stmt.order_by {
            if order_by.iter().any(|item| expression_has_window_function(&item.expr)) {
                return false;
            }
        }

        // All checks passed - we can use iterator execution!
        true
    }

    /// Execute SELECT using iterator-based execution (for simple queries)
    ///
    /// This method uses lazy iteration to avoid materializing intermediate results.
    /// The pipeline: scan → filter → skip → take → collect → project
    /// WHERE filtering, OFFSET, and LIMIT are fully lazy, providing memory efficiency
    /// and early termination. Projection happens after materialization due to its
    /// complexity (wildcard expansion, expression evaluation, etc.).
    pub(in crate::select::executor) fn execute_with_iterators(
        &self,
        stmt: &vibesql_ast::SelectStmt,
        from_result: FromResult,
        cte_results: &std::collections::HashMap<String, crate::select::CteResult>,
    ) -> Result<Vec<vibesql_storage::Row>, ExecutorError> {
        let schema = from_result.schema.clone();
        let sorted_by = from_result.sorted_by.clone();
        let rows = from_result.into_rows();

        // Create evaluator using consolidated ExecutionContext
        // Handles: outer context (subqueries), procedural context, CTE context
        let cte_ctx = if !cte_results.is_empty() { Some(cte_results) } else { self.cte_context };

        let mut ctx = ExecutionContext::new(&schema, self.database);
        if let (Some(outer_row), Some(outer_schema)) = (self.outer_row, self.outer_schema) {
            ctx = ctx.with_outer_context(outer_row, outer_schema);
        } else if let Some(proc_ctx) = self.procedural_context {
            ctx = ctx.with_procedural_context(proc_ctx);
        }
        if let Some(cte_ctx) = cte_ctx {
            ctx = ctx.with_cte_context(cte_ctx);
        }
        let evaluator = ctx.create_evaluator();

        // Validate WHERE clause subqueries upfront (before row iteration)
        // This ensures schema validation happens even for empty result sets
        // Issue #3562: Pass CTE context so CTEs can be resolved in IN subqueries
        if let Some(where_expr) = &stmt.where_clause {
            validate_where_clause_subqueries(where_expr, self.database, cte_ctx)?;
        }

        // Stage 1: Table scan
        let mut iterator: Box<dyn RowIterator> =
            Box::new(TableScanIterator::new(schema.clone(), rows));

        // Stage 2: WHERE filter (if present)
        if let Some(where_expr) = &stmt.where_clause {
            // Optimize WHERE clause
            let where_optimization = optimize_where_clause(Some(where_expr), &evaluator)?;

            match where_optimization {
                crate::optimizer::WhereOptimization::AlwaysFalse => {
                    // WHERE FALSE - return empty result immediately
                    return Ok(Vec::new());
                }
                crate::optimizer::WhereOptimization::AlwaysTrue => {
                    // WHERE TRUE - no filtering needed, keep current iterator
                }
                crate::optimizer::WhereOptimization::Optimized(expr) => {
                    // Apply optimized WHERE clause - use the evaluator that has outer context if present
                    iterator = Box::new(FilterIterator::new(
                        iterator,
                        expr,
                        evaluator.clone_for_new_expression(),
                    ));
                }
                crate::optimizer::WhereOptimization::Unchanged(Some(expr)) => {
                    // Apply original WHERE clause - use the evaluator that has outer context if present
                    iterator = Box::new(FilterIterator::new(
                        iterator,
                        expr.clone(),
                        evaluator.clone_for_new_expression(),
                    ));
                }
                crate::optimizer::WhereOptimization::Unchanged(None) => {
                    // No WHERE clause - keep current iterator
                }
            }
        }

        // Stage 3: OFFSET (skip rows lazily)
        let mut iterator: Box<dyn Iterator<Item = _>> = if let Some(offset) = stmt.offset {
            let offset_usize = offset.max(0);
            Box::new(iterator.skip(offset_usize))
        } else {
            iterator
        };

        // Stage 4: LIMIT (take only needed rows)
        if let Some(limit) = stmt.limit {
            iterator = Box::new(iterator.take(limit));
        }

        // Stage 5: Materialize filtered results
        // Use pooled buffer to reduce allocation overhead
        let mut filtered_rows = self.database.query_buffer_pool().get_row_buffer(128);
        for row_result in iterator {
            // Check timeout during iteration
            self.check_timeout()?;
            filtered_rows.push(row_result?);
        }

        // Stage 5.5: Apply implicit ordering for deterministic results
        // Queries without explicit ORDER BY get sorted by all columns in schema order
        // This ensures SQLLogicTest compatibility and deterministic behavior
        // Skip sorting if data is already sorted from index scan
        let needs_implicit_sort =
            stmt.order_by.is_none() && sorted_by.is_none() && !filtered_rows.is_empty();

        if needs_implicit_sort {
            use crate::select::grouping::compare_sql_values;

            #[cfg(feature = "parallel")]
            {
                use crate::select::parallel::ParallelConfig;
                use rayon::prelude::*;

                // Use parallel sorting for larger datasets
                let should_parallel =
                    ParallelConfig::global().should_parallelize_sort(filtered_rows.len());

                if should_parallel {
                    filtered_rows.par_sort_by(|row_a, row_b| {
                        // Compare column by column until we find a difference
                        for i in 0..row_a.values.len().min(row_b.values.len()) {
                            let cmp = compare_sql_values(&row_a.values[i], &row_b.values[i]);
                            if cmp != std::cmp::Ordering::Equal {
                                return cmp;
                            }
                        }
                        std::cmp::Ordering::Equal
                    });
                } else {
                    filtered_rows.sort_by(|row_a, row_b| {
                        // Compare column by column until we find a difference
                        for i in 0..row_a.values.len().min(row_b.values.len()) {
                            let cmp = compare_sql_values(&row_a.values[i], &row_b.values[i]);
                            if cmp != std::cmp::Ordering::Equal {
                                return cmp;
                            }
                        }
                        std::cmp::Ordering::Equal
                    });
                }
            }

            #[cfg(not(feature = "parallel"))]
            {
                filtered_rows.sort_by(|row_a, row_b| {
                    // Compare column by column until we find a difference
                    for i in 0..row_a.values.len().min(row_b.values.len()) {
                        let cmp = compare_sql_values(&row_a.values[i], &row_b.values[i]);
                        if cmp != std::cmp::Ordering::Equal {
                            return cmp;
                        }
                    }
                    std::cmp::Ordering::Equal
                });
            }
        }

        // Stage 6: Project columns (handles wildcards, expressions, etc.)
        // Try batch SIMD projection first for large datasets
        let final_rows = if let Some(projected) = try_batch_project_simd(
            &filtered_rows,
            &stmt.select_list,
            &evaluator,
            &schema,
            &None, // No window functions in iterator path
            self.database.query_buffer_pool(),
        )? {
            // SIMD batch projection succeeded
            projected
        } else {
            // Fall back to row-by-row projection
            // Use pooled buffer to reduce allocation overhead
            let mut rows = self.database.query_buffer_pool().get_row_buffer(filtered_rows.len());
            for row in &filtered_rows {
                // Clear CSE cache before projecting each row
                evaluator.clear_cse_cache();

                let projected_row = project_row_combined(
                    row,
                    &stmt.select_list,
                    &evaluator,
                    &schema,
                    &None, // No window functions in iterator path
                    self.database.query_buffer_pool(),
                )?;

                rows.push(projected_row);
            }
            rows
        };

        // Clear CSE cache at end of query to prevent cross-query pollution
        // Cache can persist within a single query for performance, but must be
        // cleared between different SQL statements to avoid stale values
        evaluator.clear_cse_cache();

        // Return intermediate buffer to pool, then return final result
        self.database.query_buffer_pool().return_row_buffer(filtered_rows);
        Ok(final_rows)
    }
}
