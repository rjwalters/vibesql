//! Materialized execution strategy for complex queries
//!
//! This module implements full materialization for queries that require:
//! - ORDER BY (sorting all rows)
//! - DISTINCT (deduplication)
//! - Window functions (window frame processing)
//!
//! The execution path includes optimizations like SIMD filtering and spatial indexes.

#![allow(clippy::ptr_arg)]

#[cfg(feature = "spatial")]
use super::super::index_optimization::try_spatial_index_optimization;
use super::{
    builder::SelectExecutor, simd::try_simd_filter,
    validation::validate_select_column_references_with_context,
};
use crate::{
    errors::ExecutorError,
    evaluator::CombinedExpressionEvaluator,
    optimizer::optimize_where_clause,
    pipeline::ExecutionContext,
    select::{
        filter::apply_where_filter_combined_auto,
        helpers::{apply_distinct, apply_limit_offset},
        join::FromResult,
        order::{apply_order_by, RowWithSortKeys},
        projection::{project_row_combined, SelectProjectionIterator},
        projection_simd::try_batch_project_simd,
        window::{
            collect_order_by_window_functions, evaluate_order_by_window_functions,
            evaluate_window_functions, expression_has_window_function, has_window_functions,
            WindowFunctionKey,
        },
        CteResult,
    },
};
use std::collections::HashMap;

impl SelectExecutor<'_> {
    /// Execute SELECT without aggregation
    ///
    /// This is the main entry point for non-aggregated queries. It routes to either:
    /// - Iterator-based execution (for simple queries)
    /// - Materialized execution (for complex queries requiring ORDER BY, DISTINCT, or windows)
    pub(in crate::select::executor) fn execute_without_aggregation(
        &self,
        stmt: &vibesql_ast::SelectStmt,
        from_result: FromResult,
        cte_results: &HashMap<String, CteResult>,
    ) -> Result<Vec<vibesql_storage::Row>, ExecutorError> {
        // Validate column references upfront, before row iteration
        // This ensures proper error messages even when the table is empty
        // Pass procedural context to allow procedure variables in WHERE clause
        // Pass outer_schema for correlated subqueries (#2694)
        validate_select_column_references_with_context(
            stmt,
            &from_result.schema,
            self.procedural_context,
            self.outer_schema,
        )?;

        // Phase D: Use iterator-based execution for simple queries
        // This provides memory efficiency and early termination for LIMIT queries
        if Self::can_use_iterator_execution(stmt) {
            return self.execute_with_iterators(stmt, from_result, cte_results);
        }

        // Fall back to materialized execution for complex queries
        // (ORDER BY, DISTINCT, window functions require full materialization)
        self.execute_materialized(stmt, from_result, cte_results)
    }

    /// Execute query with full materialization
    ///
    /// This method is used for complex queries that require:
    /// - ORDER BY: sorting all rows
    /// - DISTINCT: deduplicating all rows
    /// - Window functions: processing window frames
    fn execute_materialized(
        &self,
        stmt: &vibesql_ast::SelectStmt,
        from_result: FromResult,
        cte_results: &HashMap<String, CteResult>,
    ) -> Result<Vec<vibesql_storage::Row>, ExecutorError> {
        let schema = from_result.schema.clone();
        let sorted_by = from_result.sorted_by.clone();
        let where_filtered = from_result.where_filtered;
        let rows = from_result.into_rows();

        // Track memory used by FROM clause results (JOINs, table scans, etc.)
        let from_memory_bytes = std::mem::size_of::<vibesql_storage::Row>() * rows.len()
            + rows.iter().map(|r| std::mem::size_of_val(r.values.as_slice())).sum::<usize>();
        self.track_memory_allocation(from_memory_bytes)?;

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

        // Skip WHERE filtering if already applied during scan (e.g., by index scan)
        // This prevents redundant evaluation that can cause incorrect results
        //
        // NOTE: Index optimization has been moved to the scan level (execute_index_scan).
        // The FROM clause now handles all index-based optimizations (B-tree, spatial, IN clauses)
        // before returning rows, avoiding the row-index mismatch problem when predicate pushdown
        // is enabled. This fixes issues #1807, #1895, #1896, and #1902.
        //
        // Previously, we tried to apply index optimization here on `rows` from FROM, but:
        // 1. If predicate pushdown filtered rows, indices no longer match original table
        // 2. This caused incorrect results and crashes
        // 3. execute_index_scan() already handles this correctly at scan level
        //
        // Now we only do spatial optimization here as a special case, since it may not
        // always be pushed to scan level. All other index optimizations happen in FROM.
        let mut filtered_rows = if where_filtered {
            rows
        } else {
            self.apply_where_filter(stmt, rows, &schema, &evaluator)?
        };

        // Evaluate window functions if present
        let mut window_mapping =
            self.evaluate_select_windows(stmt, &mut filtered_rows, &evaluator)?;

        // Evaluate ORDER BY window functions
        if let Some(order_by) = &stmt.order_by {
            let has_order_by_windows =
                order_by.iter().any(|item| expression_has_window_function(&item.expr));

            if has_order_by_windows {
                let order_by_window_functions = collect_order_by_window_functions(order_by);
                if !order_by_window_functions.is_empty() {
                    let (rows_with_order_by_windows, order_by_mapping) =
                        evaluate_order_by_window_functions(
                            filtered_rows,
                            order_by_window_functions,
                            &evaluator,
                            window_mapping.as_ref(),
                        )?;
                    filtered_rows = rows_with_order_by_windows;

                    // Merge mappings
                    if let Some(ref mut existing_mapping) = window_mapping {
                        existing_mapping.extend(order_by_mapping);
                    } else {
                        window_mapping = Some(order_by_mapping);
                    }
                }
            }
        }

        // Convert to RowWithSortKeys format
        let mut result_rows: Vec<RowWithSortKeys> =
            filtered_rows.into_iter().map(|row| (row, None)).collect();

        // Apply sorting (explicit ORDER BY or implicit for determinism)
        self.apply_sorting(stmt, &mut result_rows, &sorted_by, &schema, &window_mapping, cte_ctx)?;

        // Apply projection strategy
        let final_rows =
            self.apply_projection(stmt, result_rows, &schema, &evaluator, &window_mapping)?;

        Ok(final_rows)
    }

    /// Apply WHERE filtering with optimizations
    fn apply_where_filter(
        &self,
        stmt: &vibesql_ast::SelectStmt,
        rows: Vec<vibesql_storage::Row>,
        schema: &crate::schema::CombinedSchema,
        evaluator: &CombinedExpressionEvaluator,
    ) -> Result<Vec<vibesql_storage::Row>, ExecutorError> {
        // Try spatial index optimization if feature is enabled
        #[cfg(feature = "spatial")]
        {
            if let Some(spatial_filtered) = try_spatial_index_optimization(
                self.database,
                stmt.where_clause.as_ref(),
                &rows,
                schema,
            )? {
                // Spatial index optimization (ST_Contains, ST_Intersects, etc.)
                return Ok(spatial_filtered);
            }
        }

        // Fall back to full WHERE clause evaluation
        let where_optimization = optimize_where_clause(stmt.where_clause.as_ref(), evaluator)?;

        match where_optimization {
            crate::optimizer::WhereOptimization::AlwaysTrue => {
                // WHERE TRUE - no filtering needed
                Ok(rows)
            }
            crate::optimizer::WhereOptimization::AlwaysFalse => {
                // WHERE FALSE - return empty result
                Ok(Vec::new())
            }
            crate::optimizer::WhereOptimization::Optimized(ref expr) => {
                // Try SIMD filtering first (for large datasets)
                let (filtered_rows, used_simd) = try_simd_filter(rows, expr, schema)?;
                if used_simd {
                    // SIMD path succeeded!
                    Ok(filtered_rows)
                } else {
                    // Fall back to row-based filtering (small dataset or unsupported predicate)
                    apply_where_filter_combined_auto(filtered_rows, Some(expr), evaluator, self)
                }
            }
            crate::optimizer::WhereOptimization::Unchanged(where_expr) => {
                // Try SIMD filtering first (for large datasets)
                if let Some(ref expr) = where_expr {
                    let (filtered_rows, used_simd) = try_simd_filter(rows, expr, schema)?;
                    if used_simd {
                        // SIMD path succeeded!
                        Ok(filtered_rows)
                    } else {
                        // Fall back to row-based filtering
                        apply_where_filter_combined_auto(
                            filtered_rows,
                            where_expr.as_ref(),
                            evaluator,
                            self,
                        )
                    }
                } else {
                    // No WHERE clause
                    Ok(rows)
                }
            }
        }
    }

    /// Evaluate window functions in SELECT list
    fn evaluate_select_windows(
        &self,
        stmt: &vibesql_ast::SelectStmt,
        filtered_rows: &mut Vec<vibesql_storage::Row>,
        evaluator: &CombinedExpressionEvaluator,
    ) -> Result<Option<HashMap<WindowFunctionKey, usize>>, ExecutorError> {
        let has_select_windows = has_window_functions(&stmt.select_list);

        if has_select_windows {
            let (rows_with_windows, mapping) = evaluate_window_functions(
                std::mem::take(filtered_rows),
                &stmt.select_list,
                evaluator,
            )?;
            *filtered_rows = rows_with_windows;
            Ok(Some(mapping))
        } else {
            Ok(None)
        }
    }

    /// Apply sorting (explicit ORDER BY or implicit for determinism)
    fn apply_sorting(
        &self,
        stmt: &vibesql_ast::SelectStmt,
        result_rows: &mut Vec<RowWithSortKeys>,
        sorted_by: &Option<Vec<(String, vibesql_ast::OrderDirection)>>,
        schema: &crate::schema::CombinedSchema,
        window_mapping: &Option<HashMap<WindowFunctionKey, usize>>,
        cte_ctx: Option<&HashMap<String, CteResult>>,
    ) -> Result<(), ExecutorError> {
        if let Some(order_by) = &stmt.order_by {
            // Check if results are already sorted by the index scan
            let already_sorted = self.check_if_already_sorted(sorted_by, order_by);

            if !already_sorted {
                // Create evaluator for ORDER BY using consolidated ExecutionContext
                // Priority: 1) window mapping 2) outer context 3) procedural context 4) database only
                let mut ctx = ExecutionContext::new(schema, self.database);
                if let Some(ref mapping) = window_mapping {
                    ctx = ctx.with_window_mapping(mapping);
                } else if let (Some(outer_row), Some(outer_schema)) =
                    (self.outer_row, self.outer_schema)
                {
                    ctx = ctx.with_outer_context(outer_row, outer_schema);
                } else if let Some(proc_ctx) = self.procedural_context {
                    ctx = ctx.with_procedural_context(proc_ctx);
                }
                if let Some(cte_ctx) = cte_ctx {
                    ctx = ctx.with_cte_context(cte_ctx);
                }
                let order_by_evaluator = ctx.create_evaluator();
                *result_rows = apply_order_by(
                    std::mem::take(result_rows),
                    order_by,
                    &order_by_evaluator,
                    &stmt.select_list,
                )?;
            }
        } else if sorted_by.is_none() && !result_rows.is_empty() {
            // No explicit ORDER BY - apply implicit ordering for deterministic results
            self.apply_implicit_sorting(result_rows);
        }

        Ok(())
    }

    /// Check if results are already sorted by index scan
    fn check_if_already_sorted(
        &self,
        sorted_by: &Option<Vec<(String, vibesql_ast::OrderDirection)>>,
        order_by: &[vibesql_ast::OrderByItem],
    ) -> bool {
        if let Some(ref sorted_cols) = sorted_by {
            // Results are pre-sorted if:
            // 1. ORDER BY has same number of columns as sorted_cols
            // 2. Each column and sort direction matches
            if sorted_cols.len() == order_by.len() {
                return sorted_cols.iter().zip(order_by.iter()).all(
                    |((col_name, sort_order), order_item)| {
                        // Check if column names match and sort direction matches
                        match &order_item.expr {
                            vibesql_ast::Expression::ColumnRef { table: None, column } => {
                                column == col_name && &order_item.direction == sort_order
                            }
                            _ => false,
                        }
                    },
                );
            }
        }
        false
    }

    /// Apply implicit sorting for deterministic results
    fn apply_implicit_sorting(&self, result_rows: &mut Vec<RowWithSortKeys>) {
        use crate::select::grouping::compare_sql_values;

        #[cfg(feature = "parallel")]
        {
            use crate::select::parallel::ParallelConfig;
            use rayon::prelude::*;

            // Use parallel sorting for larger datasets
            let should_parallel =
                ParallelConfig::global().should_parallelize_sort(result_rows.len());

            if should_parallel {
                result_rows.par_sort_by(|(row_a, _), (row_b, _)| {
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
                result_rows.sort_by(|(row_a, _), (row_b, _)| {
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
            result_rows.sort_by(|(row_a, _), (row_b, _)| {
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

    /// Apply projection strategy (eager or lazy based on DISTINCT/SET operations)
    fn apply_projection(
        &self,
        stmt: &vibesql_ast::SelectStmt,
        result_rows: Vec<RowWithSortKeys>,
        schema: &crate::schema::CombinedSchema,
        evaluator: &CombinedExpressionEvaluator,
        window_mapping: &Option<HashMap<WindowFunctionKey, usize>>,
    ) -> Result<Vec<vibesql_storage::Row>, ExecutorError> {
        // Choose projection strategy based on DISTINCT and set operations
        // - If DISTINCT is present, we must project all rows first, then deduplicate
        // - If no DISTINCT, we can apply LIMIT/OFFSET before projection for better performance
        if stmt.distinct || stmt.set_operation.is_some() {
            self.apply_eager_projection(stmt, result_rows, schema, evaluator, window_mapping)
        } else {
            self.apply_lazy_projection(stmt, result_rows, schema, evaluator, window_mapping)
        }
    }

    /// Eager projection: project all rows, then apply DISTINCT and/or LIMIT/OFFSET
    fn apply_eager_projection(
        &self,
        stmt: &vibesql_ast::SelectStmt,
        result_rows: Vec<RowWithSortKeys>,
        schema: &crate::schema::CombinedSchema,
        evaluator: &CombinedExpressionEvaluator,
        window_mapping: &Option<HashMap<WindowFunctionKey, usize>>,
    ) -> Result<Vec<vibesql_storage::Row>, ExecutorError> {
        // Extract rows from RowWithSortKeys (discard sort keys for projection)
        let rows: Vec<vibesql_storage::Row> = result_rows.into_iter().map(|(row, _)| row).collect();

        // Try batch SIMD projection first for large datasets
        let projected_rows = if let Some(projected) = try_batch_project_simd(
            &rows,
            &stmt.select_list,
            evaluator,
            schema,
            window_mapping,
            self.database.query_buffer_pool(),
        )? {
            // SIMD batch projection succeeded
            // Track memory for projected rows
            for projected_row in &projected {
                let row_memory = std::mem::size_of::<vibesql_storage::Row>()
                    + std::mem::size_of_val(projected_row.values.as_slice());
                self.track_memory_allocation(row_memory)?;
            }
            projected
        } else {
            // Fall back to row-by-row projection
            // Use pooled buffer to reduce allocation overhead
            let mut result = self.database.query_buffer_pool().get_row_buffer(rows.len());

            for row in rows {
                // Check timeout during projection
                self.check_timeout()?;

                // Clear CSE cache before projecting each row to prevent column values
                // from being incorrectly cached across different rows
                evaluator.clear_cse_cache();

                let projected_row = project_row_combined(
                    &row,
                    &stmt.select_list,
                    evaluator,
                    schema,
                    window_mapping,
                    self.database.query_buffer_pool(),
                )?;

                // Track memory for each projected row
                let row_memory = std::mem::size_of::<vibesql_storage::Row>()
                    + std::mem::size_of_val(projected_row.values.as_slice());
                self.track_memory_allocation(row_memory)?;

                result.push(projected_row);
            }
            result
        };

        // Apply DISTINCT if specified
        let projected_rows =
            if stmt.distinct { apply_distinct(projected_rows) } else { projected_rows };

        // Don't apply LIMIT/OFFSET if we have a set operation - it will be applied later
        if stmt.set_operation.is_some() {
            Ok(projected_rows)
        } else {
            Ok(apply_limit_offset(projected_rows, stmt.limit, stmt.offset))
        }
    }

    /// Lazy projection: apply LIMIT/OFFSET first, then project only needed rows
    fn apply_lazy_projection(
        &self,
        stmt: &vibesql_ast::SelectStmt,
        result_rows: Vec<RowWithSortKeys>,
        schema: &crate::schema::CombinedSchema,
        evaluator: &CombinedExpressionEvaluator,
        window_mapping: &Option<HashMap<WindowFunctionKey, usize>>,
    ) -> Result<Vec<vibesql_storage::Row>, ExecutorError> {
        // Extract rows from RowWithSortKeys (discard sort keys)
        let rows: Vec<vibesql_storage::Row> = result_rows.into_iter().map(|(row, _)| row).collect();

        // Apply LIMIT/OFFSET to reduce rows before projection
        let limited_rows = apply_limit_offset(rows, stmt.limit, stmt.offset);

        // Create iterator for lazy projection
        let projection_iter = SelectProjectionIterator::new(
            limited_rows.into_iter().map(Ok),
            stmt.select_list.clone(),
            evaluator.clone_for_new_expression(),
            schema.clone(),
            window_mapping.clone(),
            *self.database.query_buffer_pool(),
        );

        // Collect projected rows with memory tracking
        // Use pooled buffer to reduce allocation overhead
        let mut final_rows = self.database.query_buffer_pool().get_row_buffer(128);
        for projected_result in projection_iter {
            // Check timeout during projection
            self.check_timeout()?;

            let projected_row = projected_result?;

            // Track memory for each projected row
            let row_memory = std::mem::size_of::<vibesql_storage::Row>()
                + std::mem::size_of_val(projected_row.values.as_slice());
            self.track_memory_allocation(row_memory)?;

            final_rows.push(projected_row);
        }

        // Return buffer to pool and move data to result
        let result = std::mem::take(&mut final_rows);
        self.database.query_buffer_pool().return_row_buffer(final_rows);
        Ok(result)
    }
}
