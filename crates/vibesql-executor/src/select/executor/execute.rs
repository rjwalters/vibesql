//! Main execution methods for SelectExecutor
//!
//! This module implements the unified execution dispatcher that routes queries
//! to the appropriate execution pipeline based on the selected strategy.
//!
//! ## Execution Pipeline Architecture
//!
//! The dispatcher uses the `ExecutionPipeline` trait to provide a unified interface
//! for query execution across different strategies:
//!
//! - **NativeColumnar**: Zero-copy SIMD execution from columnar storage
//! - **StandardColumnar**: SIMD execution with row-to-batch conversion
//! - **RowOriented**: Traditional row-by-row execution
//! - **ExpressionOnly**: SELECT without FROM clause (special case)
//!
//! ```text
//! Strategy Selection → Create Pipeline → Execute Pipeline Stages → Results
//!                          ↓
//!          apply_filter → apply_projection → apply_aggregation → apply_limit_offset
//! ```

use std::collections::HashMap;

use super::builder::SelectExecutor;
use crate::{
    errors::ExecutorError,
    optimizer::adaptive::{choose_execution_strategy, ExecutionStrategy, StrategyContext},
    pipeline::{
        ColumnarPipeline, ExecutionContext, ExecutionPipeline, NativeColumnarPipeline,
        PipelineInput,
    },
    select::{
        cte::{execute_ctes, execute_ctes_with_memory_check, CteResult},
        helpers::{apply_limit_offset, estimate_result_size},
        join::FromResult,
        set_operations::apply_set_operation,
        SelectResult,
    },
};

impl SelectExecutor<'_> {
    /// Execute a SELECT statement
    pub fn execute(
        &self,
        stmt: &vibesql_ast::SelectStmt,
    ) -> Result<Vec<vibesql_storage::Row>, ExecutorError> {
        #[cfg(feature = "profile-q6")]
        let execute_start = std::time::Instant::now();

        // Reset arena for fresh query execution (only at top level)
        if self.subquery_depth == 0 {
            self.reset_arena();
        }

        // Check timeout before starting execution
        self.check_timeout()?;

        // Check subquery depth limit to prevent stack overflow
        if self.subquery_depth >= crate::limits::MAX_EXPRESSION_DEPTH {
            return Err(ExecutorError::ExpressionDepthExceeded {
                depth: self.subquery_depth,
                max_depth: crate::limits::MAX_EXPRESSION_DEPTH,
            });
        }

        // Fast path for simple point-lookup queries (TPC-C optimization)
        // This bypasses expensive optimizer passes for queries like:
        // SELECT col FROM table WHERE pk = value
        if self.subquery_depth == 0
            && self.outer_row.is_none()
            && self.cte_context.is_none()
            && super::fast_path::is_simple_point_query(stmt)
        {
            return self.execute_fast_path(stmt);
        }

        // Streaming aggregate fast path (#3815)
        // For queries like: SELECT SUM(k) FROM sbtest1 WHERE id BETWEEN ? AND ?
        // Accumulates aggregates inline during PK range scan without materializing rows
        if self.subquery_depth == 0
            && self.outer_row.is_none()
            && self.cte_context.is_none()
            && super::fast_path::is_streaming_aggregate_query(stmt)
        {
            if let Ok(result) = self.execute_streaming_aggregate(stmt) {
                return Ok(result);
            }
            // Fall through to standard path if streaming aggregate fails
        }

        #[cfg(feature = "profile-q6")]
        let _setup_time = execute_start.elapsed();

        // Apply subquery rewriting optimizations (Phase 2 of IN subquery optimization)
        // - Rewrites correlated IN → EXISTS with LIMIT 1 for early termination
        // - Adds DISTINCT to uncorrelated IN subqueries to reduce duplicate processing
        // This works in conjunction with Phase 1 (HashSet optimization, #2136)
        #[cfg(feature = "profile-q6")]
        let optimizer_start = std::time::Instant::now();

        let optimized_stmt = crate::optimizer::rewrite_subquery_optimizations(stmt);

        #[cfg(feature = "profile-q6")]
        let _optimizer_time = optimizer_start.elapsed();

        // Eliminate unused tables that create unnecessary cross joins (#3556)
        // Must run BEFORE semi-join transformation to avoid complex interactions
        // with derived tables from EXISTS/IN transformations
        let optimized_stmt = crate::optimizer::eliminate_unused_tables(&optimized_stmt);

        // Transform decorrelated IN/EXISTS subqueries to semi/anti-joins (#2424)
        // This enables hash-based join execution instead of row-by-row subquery evaluation
        // Converts WHERE clauses like "WHERE x IN (SELECT y FROM t)" to "SEMI JOIN t ON x = y"
        let optimized_stmt = crate::optimizer::transform_subqueries_to_joins(&optimized_stmt);

        // Execute CTEs if present and merge with outer query's CTE context
        let mut cte_results = if let Some(with_clause) = &optimized_stmt.with_clause {
            // This query has its own CTEs - execute them with memory tracking
            execute_ctes_with_memory_check(
                with_clause,
                |query, cte_ctx| self.execute_with_ctes(query, cte_ctx),
                |size| self.track_memory_allocation(size),
            )?
        } else {
            HashMap::new()
        };

        // If we have access to outer query's CTEs (for subqueries), merge them in
        // Local CTEs take precedence over outer CTEs if there are name conflicts
        if let Some(outer_cte_ctx) = self.cte_context {
            for (name, result) in outer_cte_ctx {
                cte_results.entry(name.clone()).or_insert_with(|| result.clone());
            }
        }

        #[cfg(feature = "profile-q6")]
        let _pre_execute_time = execute_start.elapsed();

        // Execute the main query with CTE context
        let result = self.execute_with_ctes(&optimized_stmt, &cte_results)?;

        #[cfg(feature = "profile-q6")]
        {
            let _total_execute = execute_start.elapsed();
        }

        Ok(result)
    }

    /// Execute a SELECT statement and return an iterator over results
    ///
    /// This enables early termination when the full result set is not needed,
    /// such as for IN subqueries where we stop after finding the first match.
    ///
    /// # Phase 1 Implementation (Early Termination for IN subqueries)
    ///
    /// Current implementation materializes results then returns an iterator.
    /// This still enables early termination in the consumer (e.g., eval_in_subquery)
    /// by stopping iteration when a match is found.
    ///
    /// Future optimization: Leverage the existing RowIterator infrastructure
    /// (crate::select::iterator) for truly lazy evaluation that stops execution
    /// early, not just iteration.
    pub fn execute_iter(
        &self,
        stmt: &vibesql_ast::SelectStmt,
    ) -> Result<impl Iterator<Item = vibesql_storage::Row>, ExecutorError> {
        // For Phase 1, materialize then return iterator
        // This still enables early termination in the consumer
        let rows = self.execute(stmt)?;
        Ok(rows.into_iter())
    }

    /// Execute a SELECT statement using the fast path directly
    ///
    /// This method is used by prepared statements with cached SimpleFastPath plans.
    /// It bypasses the `is_simple_point_query()` check because the eligibility was
    /// already determined at prepare time.
    ///
    /// # Performance
    ///
    /// For repeated execution of prepared statements, this saves the cost of
    /// re-checking fast path eligibility on every execution (~5-10µs per query).
    pub fn execute_fast_path_with_columns(
        &self,
        stmt: &vibesql_ast::SelectStmt,
    ) -> Result<SelectResult, ExecutorError> {
        // Reset arena for fresh query execution
        if self.subquery_depth == 0 {
            self.reset_arena();
        }

        // Check timeout before starting execution
        self.check_timeout()?;

        // Execute via fast path directly (skip is_simple_point_query check)
        let rows = self.execute_fast_path(stmt)?;

        // Derive column names from the SELECT list
        // For fast path queries, we don't have a FromResult, so pass None
        // The column derivation will use the SELECT list expressions directly
        let columns = self.derive_fast_path_column_names(stmt)?;

        Ok(SelectResult { columns, rows })
    }

    /// Derive column names for fast path execution
    ///
    /// For fast path queries, we derive column names directly from the SELECT list
    /// and table schema without going through the full FROM clause execution.
    ///
    /// # Performance Note (#3780)
    ///
    /// This method is called by `Session::execute_prepared()` to cache column names
    /// in `SimpleFastPathPlan`. After the first execution, cached column names are
    /// reused to avoid repeated table lookups and column name derivation.
    pub fn derive_fast_path_column_names(
        &self,
        stmt: &vibesql_ast::SelectStmt,
    ) -> Result<Vec<String>, ExecutorError> {
        use vibesql_ast::{FromClause, SelectItem};

        // Get table name and schema for column resolution
        let (table_name, table_alias) = match &stmt.from {
            Some(FromClause::Table { name, alias, .. }) => (name.as_str(), alias.as_deref()),
            _ => {
                return Err(ExecutorError::Other(
                    "Fast path requires simple table FROM clause".to_string(),
                ))
            }
        };

        let table = self.database.get_table(table_name).ok_or_else(|| {
            ExecutorError::TableNotFound(table_name.to_string())
        })?;

        let mut columns = Vec::with_capacity(stmt.select_list.len());

        for item in &stmt.select_list {
            match item {
                SelectItem::Wildcard { .. } => {
                    // Add all columns from the table
                    for col in &table.schema.columns {
                        columns.push(col.name.clone());
                    }
                }
                SelectItem::QualifiedWildcard { qualifier, .. } => {
                    // Check if qualifier matches table name or alias
                    let effective_name = table_alias.unwrap_or(table_name);
                    if qualifier.eq_ignore_ascii_case(effective_name)
                        || qualifier.eq_ignore_ascii_case(table_name)
                    {
                        for col in &table.schema.columns {
                            columns.push(col.name.clone());
                        }
                    }
                }
                SelectItem::Expression { expr, alias: col_alias } => {
                    // Use alias if provided, otherwise derive from expression
                    let col_name = if let Some(a) = col_alias {
                        a.clone()
                    } else {
                        self.derive_column_name_from_expr(expr)
                    };
                    columns.push(col_name);
                }
            }
        }

        Ok(columns)
    }

    /// Derive a column name from an expression
    fn derive_column_name_from_expr(&self, expr: &vibesql_ast::Expression) -> String {
        match expr {
            vibesql_ast::Expression::ColumnRef { column, .. } => column.clone(),
            vibesql_ast::Expression::Literal(val) => format!("{}", val),
            _ => "?column?".to_string(),
        }
    }

    /// Execute a SELECT statement and return both columns and rows
    pub fn execute_with_columns(
        &self,
        stmt: &vibesql_ast::SelectStmt,
    ) -> Result<SelectResult, ExecutorError> {
        // First, get the FROM result to access the schema
        let from_result = if let Some(from_clause) = &stmt.from {
            let mut cte_results = if let Some(with_clause) = &stmt.with_clause {
                execute_ctes(with_clause, |query, cte_ctx| self.execute_with_ctes(query, cte_ctx))?
            } else {
                HashMap::new()
            };
            // If we have access to outer query's CTEs (for subqueries/derived tables), merge them in
            // Local CTEs take precedence over outer CTEs if there are name conflicts
            // This is critical for queries like TPC-DS Q2 where CTEs are referenced from derived tables
            if let Some(outer_cte_ctx) = self.cte_context {
                for (name, result) in outer_cte_ctx {
                    cte_results.entry(name.clone()).or_insert_with(|| result.clone());
                }
            }
            // Pass WHERE, ORDER BY, and LIMIT for optimizations
            // This is critical for GROUP BY queries to avoid CROSS JOINs
            // LIMIT enables early termination when ORDER BY is satisfied by index (#3253)
            // Pass select_list for table elimination optimization (#3556)
            Some(self.execute_from_with_where(
                from_clause,
                &cte_results,
                stmt.where_clause.as_ref(),
                stmt.order_by.as_deref(),
                stmt.limit,
                Some(&stmt.select_list),
            )?)
        } else {
            None
        };

        // Derive column names from the SELECT list
        let columns = self.derive_column_names(&stmt.select_list, from_result.as_ref())?;

        // Execute the query to get rows
        let rows = self.execute(stmt)?;

        Ok(SelectResult { columns, rows })
    }

    /// Execute SELECT statement with CTE context
    ///
    /// Uses unified strategy selection to determine the optimal execution path:
    /// - NativeColumnar: Zero-copy SIMD execution from columnar storage
    /// - StandardColumnar: SIMD execution with row-to-batch conversion
    /// - RowOriented: Traditional row-by-row execution
    /// - ExpressionOnly: SELECT without FROM clause (special case)
    ///
    /// ## Pipeline-Based Execution (Phase 5)
    ///
    /// This method uses the `ExecutionPipeline` trait to provide a unified interface
    /// for query execution. Each strategy creates an appropriate pipeline that
    /// implements filter, projection, aggregation, and limit/offset operations.
    ///
    /// ```text
    /// Strategy Selection → Create Pipeline → Execute via Trait Methods
    ///                              ↓
    ///   NativeColumnar  → NativeColumnarPipeline::apply_*()
    ///   StandardColumnar → ColumnarPipeline::apply_*()
    ///   RowOriented     → RowOrientedPipeline::apply_*()
    ///   ExpressionOnly  → Special case (no table scan)
    /// ```
    pub(super) fn execute_with_ctes(
        &self,
        stmt: &vibesql_ast::SelectStmt,
        cte_results: &HashMap<String, CteResult>,
    ) -> Result<Vec<vibesql_storage::Row>, ExecutorError> {
        #[cfg(feature = "profile-q6")]
        let _execute_ctes_start = std::time::Instant::now();

        // Check if native columnar is enabled via feature flag or env var
        let native_columnar_enabled =
            cfg!(feature = "native-columnar") || std::env::var("VIBESQL_NATIVE_COLUMNAR").is_ok();

        // Use unified strategy selection for the execution path
        let strategy_ctx = StrategyContext::new(stmt, cte_results, native_columnar_enabled);
        let strategy = choose_execution_strategy(&strategy_ctx);

        log::debug!(
            "Execution strategy selected: {} (reason: {})",
            strategy.name(),
            strategy.score().reason
        );

        #[cfg(feature = "profile-q6")]
        eprintln!(
            "[PROFILE-Q6] Execution strategy: {} ({})",
            strategy.name(),
            strategy.score().reason
        );

        // Dispatch based on selected strategy using ExecutionPipeline trait
        // Pipeline execution returns Option<Vec<Row>> - None means fallback needed
        let mut results = match strategy {
            ExecutionStrategy::NativeColumnar { .. } => {
                // First try the optimized zero-copy native columnar path
                // This uses ColumnarBatch::from_storage_columnar() for zero-copy conversion
                // and executes filter+aggregate in a single pass without row materialization
                if let Some(result) = self.try_native_columnar_execution(stmt, cte_results)? {
                    #[cfg(feature = "profile-q6")]
                    eprintln!("[PROFILE-Q6] Native columnar: zero-copy path succeeded");
                    result
                } else {
                    // Fall back to pipeline-based execution if zero-copy path is not applicable
                    // (e.g., complex predicates, multiple tables, unsupported aggregates)
                    log::debug!("Native columnar: zero-copy path not applicable, trying pipeline");
                    match self.execute_via_pipeline(
                        stmt,
                        cte_results,
                        NativeColumnarPipeline::new,
                        "NativeColumnar",
                    )? {
                        Some(result) => result,
                        None => {
                            // Fall back to row-oriented if pipeline also fails
                            log::debug!("Native columnar runtime fallback to row-oriented");
                            #[cfg(feature = "profile-q6")]
                            eprintln!("[PROFILE-Q6] Native columnar fallback to row-oriented");
                            self.execute_row_oriented(stmt, cte_results)?
                        }
                    }
                }
            }

            ExecutionStrategy::StandardColumnar { .. } => {
                // StandardColumnar uses the pipeline-based execution path
                // Note: We don't use try_native_columnar_execution here because row tables
                // go through the pipeline which correctly handles all data types including dates.
                // The native columnar zero-copy path has known limitations with certain date comparisons.
                match self.execute_via_pipeline(
                    stmt,
                    cte_results,
                    ColumnarPipeline::new,
                    "StandardColumnar",
                )? {
                    Some(result) => result,
                    None => {
                        log::debug!("Standard columnar runtime fallback to row-oriented");
                        #[cfg(feature = "profile-q6")]
                        eprintln!("[PROFILE-Q6] Standard columnar fallback to row-oriented");
                        self.execute_row_oriented(stmt, cte_results)?
                    }
                }
            }

            ExecutionStrategy::RowOriented { .. } => {
                // Row-oriented uses the traditional path which has full feature support
                // The RowOrientedPipeline is used for simpler queries, but complex
                // queries (with JOINs, window functions, DISTINCT, etc.) need the
                // full execute_row_oriented implementation

                // Phase 4: Try columnar join execution for multi-table JOIN queries (#2943)
                // This provides 3-5x speedup for TPC-H Q3 style queries
                let has_joins = stmt
                    .from
                    .as_ref()
                    .is_some_and(|f| matches!(f, vibesql_ast::FromClause::Join { .. }));
                if has_joins {
                    if let Some(result) = self.try_columnar_join_execution(stmt, cte_results)? {
                        log::info!("Columnar join execution succeeded");
                        // Apply LIMIT/OFFSET to columnar join results (#3776)
                        // Skip if set_operation exists - it will be applied later
                        if stmt.set_operation.is_none() {
                            apply_limit_offset(result, stmt.limit, stmt.offset)
                        } else {
                            result
                        }
                    } else {
                        log::debug!(
                            "Columnar join execution not applicable, falling back to row-oriented"
                        );
                        self.execute_row_oriented(stmt, cte_results)?
                    }
                } else {
                    self.execute_row_oriented(stmt, cte_results)?
                }
            }

            ExecutionStrategy::ExpressionOnly { .. } => {
                // SELECT without FROM - special case that doesn't use pipelines
                // May still have aggregates (e.g., SELECT COUNT(*), SELECT MAX(1))
                // Note: Do NOT use early return here - we need to fall through to set operations handling
                self.execute_expression_only(stmt, cte_results)?
            }
        };

        // Handle set operations (UNION, INTERSECT, EXCEPT)
        // Process operations left-to-right to ensure correct associativity
        if let Some(set_op) = &stmt.set_operation {
            results = self.execute_set_operations(results, set_op, cte_results)?;

            // Apply LIMIT/OFFSET to the final result (after all set operations)
            // For queries WITHOUT set operations, LIMIT/OFFSET is already applied
            // in execute_without_aggregation() or execute_with_aggregation()
            results = apply_limit_offset(results, stmt.limit, stmt.offset);
        }

        Ok(results)
    }

    /// Execute SELECT without FROM clause (ExpressionOnly strategy)
    ///
    /// This is a special case that doesn't use the pipeline trait since there's
    /// no table scan involved. Handles both simple expressions and aggregates.
    fn execute_expression_only(
        &self,
        stmt: &vibesql_ast::SelectStmt,
        cte_results: &HashMap<String, CteResult>,
    ) -> Result<Vec<vibesql_storage::Row>, ExecutorError> {
        let has_aggregates = self.has_aggregates(&stmt.select_list) || stmt.having.is_some();

        if has_aggregates {
            // Aggregates without FROM need the aggregation path
            self.execute_with_aggregation(stmt, cte_results)
        } else {
            // Simple expression evaluation (e.g., SELECT 1 + 1)
            self.execute_select_without_from(stmt)
        }
    }

    /// Execute a query using the specified execution pipeline
    ///
    /// This method provides a unified interface for pipeline-based execution.
    /// It creates the pipeline, prepares input, and executes the pipeline stages.
    ///
    /// Returns `Ok(Some(results))` if the pipeline executed successfully,
    /// `Ok(None)` if the pipeline cannot handle the query (fallback needed),
    /// or `Err` if an error occurred.
    ///
    /// # Type Parameters
    ///
    /// * `P` - The pipeline type (must implement `ExecutionPipeline`)
    /// * `F` - Factory function to create the pipeline
    fn execute_via_pipeline<P, F>(
        &self,
        stmt: &vibesql_ast::SelectStmt,
        cte_results: &HashMap<String, CteResult>,
        create_pipeline: F,
        strategy_name: &str,
    ) -> Result<Option<Vec<vibesql_storage::Row>>, ExecutorError>
    where
        P: ExecutionPipeline,
        F: FnOnce() -> P,
    {
        #[cfg(feature = "profile-q6")]
        let start = std::time::Instant::now();

        // Check query complexity - pipelines don't support all features
        let has_aggregates = self.has_aggregates(&stmt.select_list) || stmt.having.is_some();
        let has_group_by = stmt.group_by.is_some();
        let has_joins =
            stmt.from.as_ref().is_some_and(|f| matches!(f, vibesql_ast::FromClause::Join { .. }));
        let has_order_by = stmt.order_by.is_some();
        let has_distinct = stmt.distinct;
        let has_set_ops = stmt.set_operation.is_some();
        let has_window_funcs = self.has_window_functions(&stmt.select_list);
        let has_distinct_aggregates = self.has_distinct_aggregates(&stmt.select_list);

        // Create the pipeline
        let pipeline = create_pipeline();

        // Check if the pipeline supports this query pattern
        if !pipeline.supports_query_pattern(has_aggregates, has_group_by, has_joins) {
            log::debug!(
                "{} pipeline doesn't support query pattern (agg={}, group_by={}, joins={})",
                strategy_name,
                has_aggregates,
                has_group_by,
                has_joins
            );
            return Ok(None);
        }

        // For complex queries (ORDER BY, DISTINCT, window functions, set ops, DISTINCT aggregates),
        // fall back to full execution paths which have complete support
        if has_order_by
            || has_distinct
            || has_window_funcs
            || has_set_ops
            || has_distinct_aggregates
        {
            log::debug!(
                "{} pipeline doesn't support complex features (order_by={}, distinct={}, window={}, set_ops={}, distinct_agg={})",
                strategy_name,
                has_order_by,
                has_distinct,
                has_window_funcs,
                has_set_ops,
                has_distinct_aggregates
            );
            return Ok(None);
        }

        // Must have a FROM clause for pipeline execution
        let from_clause = match &stmt.from {
            Some(from) => from,
            None => return Ok(None),
        };

        // Execute FROM clause to get input data
        // Note: WHERE, ORDER BY, and LIMIT are handled by the pipeline, not here
        // Note: Table elimination requires WHERE clause, so pass None for select_list too
        let from_result = self.execute_from_with_where(
            from_clause,
            cte_results,
            None, // Pipeline will apply WHERE filter
            None, // ORDER BY handled separately
            None, // LIMIT applied after pipeline
            None, // No table elimination when WHERE is deferred
        )?;

        // Build execution context
        let mut exec_ctx = ExecutionContext::new(&from_result.schema, self.database);
        // Add outer context for correlated subqueries (#2998)
        if let (Some(outer_row), Some(outer_schema)) = (self.outer_row, self.outer_schema) {
            exec_ctx = exec_ctx.with_outer_context(outer_row, outer_schema);
        }
        // Add CTE context if available
        if !cte_results.is_empty() {
            exec_ctx = exec_ctx.with_cte_context(cte_results);
        }

        // Validate column references BEFORE processing
        super::validation::validate_select_columns_with_context(
            &stmt.select_list,
            stmt.where_clause.as_ref(),
            &from_result.schema,
            self.procedural_context,
            self.outer_schema,
        )?;

        // Prepare input from FROM result
        let input = PipelineInput::from_rows_owned(from_result.data.into_rows());

        // Execute pipeline stages with fallback on error
        // If any pipeline stage fails with UnsupportedFeature, fall back to row-oriented

        // Stage 1: Filter (WHERE clause)
        let filtered = match pipeline.apply_filter(input, stmt.where_clause.as_ref(), &exec_ctx) {
            Ok(result) => result,
            Err(ExecutorError::UnsupportedFeature(_))
            | Err(ExecutorError::UnsupportedExpression(_)) => {
                log::debug!("{} pipeline filter failed, falling back", strategy_name);
                return Ok(None);
            }
            Err(e) => return Err(e),
        };

        // Stage 2: Projection or Aggregation
        let result = if has_aggregates || has_group_by {
            // Execute aggregation (includes projection)
            // Get GROUP BY expressions if present (as slice)
            let group_by_slice: Option<&[vibesql_ast::Expression]> =
                stmt.group_by.as_ref().and_then(|g| g.as_simple()).map(|v| v.as_slice());
            match pipeline.apply_aggregation(
                filtered.into_input(),
                &stmt.select_list,
                group_by_slice,
                stmt.having.as_ref(),
                &exec_ctx,
            ) {
                Ok(result) => result,
                Err(ExecutorError::UnsupportedFeature(_))
                | Err(ExecutorError::UnsupportedExpression(_)) => {
                    log::debug!("{} pipeline aggregation failed, falling back", strategy_name);
                    return Ok(None);
                }
                Err(e) => return Err(e),
            }
        } else {
            // Execute projection only
            match pipeline.apply_projection(filtered.into_input(), &stmt.select_list, &exec_ctx) {
                Ok(result) => result,
                Err(ExecutorError::UnsupportedFeature(_))
                | Err(ExecutorError::UnsupportedExpression(_)) => {
                    log::debug!("{} pipeline projection failed, falling back", strategy_name);
                    return Ok(None);
                }
                Err(e) => return Err(e),
            }
        };

        // Stage 3: Limit/Offset (convert usize to u64)
        let limit_u64 = stmt.limit.map(|l| l as u64);
        let offset_u64 = stmt.offset.map(|o| o as u64);
        let final_result = pipeline.apply_limit_offset(result, limit_u64, offset_u64)?;

        #[cfg(feature = "profile-q6")]
        {
            eprintln!("[PROFILE-Q6] ✓ {} pipeline execution: {:?}", strategy_name, start.elapsed());
        }

        log::debug!("✓ {} pipeline execution succeeded", strategy_name);
        Ok(Some(final_result))
    }

    /// Check if the select list contains window functions
    fn has_window_functions(&self, select_list: &[vibesql_ast::SelectItem]) -> bool {
        select_list.iter().any(|item| {
            if let vibesql_ast::SelectItem::Expression { expr, .. } = item {
                self.expr_has_window_function(expr)
            } else {
                false
            }
        })
    }

    /// Recursively check if an expression contains a window function
    #[allow(clippy::only_used_in_recursion)]
    fn expr_has_window_function(&self, expr: &vibesql_ast::Expression) -> bool {
        match expr {
            vibesql_ast::Expression::WindowFunction { .. } => true,
            vibesql_ast::Expression::BinaryOp { left, right, .. } => {
                self.expr_has_window_function(left) || self.expr_has_window_function(right)
            }
            vibesql_ast::Expression::UnaryOp { expr, .. } => self.expr_has_window_function(expr),
            vibesql_ast::Expression::Function { args, .. } => {
                args.iter().any(|arg| self.expr_has_window_function(arg))
            }
            vibesql_ast::Expression::Case { operand, when_clauses, else_result } => {
                operand.as_ref().is_some_and(|e| self.expr_has_window_function(e))
                    || when_clauses.iter().any(|case_when| {
                        case_when.conditions.iter().any(|c| self.expr_has_window_function(c))
                            || self.expr_has_window_function(&case_when.result)
                    })
                    || else_result.as_ref().is_some_and(|e| self.expr_has_window_function(e))
            }
            _ => false,
        }
    }

    /// Check if the select list contains any DISTINCT aggregates (e.g., COUNT(DISTINCT x))
    fn has_distinct_aggregates(&self, select_list: &[vibesql_ast::SelectItem]) -> bool {
        select_list.iter().any(|item| {
            if let vibesql_ast::SelectItem::Expression { expr, .. } = item {
                self.expr_has_distinct_aggregate(expr)
            } else {
                false
            }
        })
    }

    /// Recursively check if an expression contains a DISTINCT aggregate
    #[allow(clippy::only_used_in_recursion)]
    fn expr_has_distinct_aggregate(&self, expr: &vibesql_ast::Expression) -> bool {
        match expr {
            vibesql_ast::Expression::AggregateFunction { distinct, .. } => *distinct,
            vibesql_ast::Expression::BinaryOp { left, right, .. } => {
                self.expr_has_distinct_aggregate(left) || self.expr_has_distinct_aggregate(right)
            }
            vibesql_ast::Expression::UnaryOp { expr, .. } => self.expr_has_distinct_aggregate(expr),
            vibesql_ast::Expression::Function { args, .. } => {
                args.iter().any(|arg| self.expr_has_distinct_aggregate(arg))
            }
            vibesql_ast::Expression::Case { operand, when_clauses, else_result } => {
                operand.as_ref().is_some_and(|e| self.expr_has_distinct_aggregate(e))
                    || when_clauses.iter().any(|case_when| {
                        case_when.conditions.iter().any(|c| self.expr_has_distinct_aggregate(c))
                            || self.expr_has_distinct_aggregate(&case_when.result)
                    })
                    || else_result.as_ref().is_some_and(|e| self.expr_has_distinct_aggregate(e))
            }
            _ => false,
        }
    }

    /// Execute using traditional row-oriented path
    ///
    /// This is the fallback path when columnar execution is not available or not beneficial.
    fn execute_row_oriented(
        &self,
        stmt: &vibesql_ast::SelectStmt,
        cte_results: &HashMap<String, CteResult>,
    ) -> Result<Vec<vibesql_storage::Row>, ExecutorError> {
        let has_aggregates = self.has_aggregates(&stmt.select_list) || stmt.having.is_some();
        let has_group_by = stmt.group_by.is_some();

        if has_aggregates || has_group_by {
            self.execute_with_aggregation(stmt, cte_results)
        } else if let Some(from_clause) = &stmt.from {
            // Re-enabled predicate pushdown for all queries (issue #1902)
            //
            // Previously, predicate pushdown was selectively disabled for multi-column IN clauses
            // because index optimization happened in execute_without_aggregation() on row indices
            // from the FROM result. When predicate pushdown filtered rows early, the indices no
            // longer matched the original table, causing incorrect results.
            //
            // Now that all index optimization has been moved to the scan level (execute_index_scan),
            // it happens BEFORE predicate pushdown, avoiding the row-index mismatch problem.
            // This allows predicate pushdown to work correctly for all queries, improving performance.
            //
            // Fixes issues #1807, #1895, #1896, and #1902.

            // Pass WHERE, ORDER BY, and LIMIT to execute_from for optimization
            // LIMIT enables early termination when ORDER BY is satisfied by index (#3253)
            // Pass select_list for table elimination optimization (#3556)
            let from_result = self.execute_from_with_where(
                from_clause,
                cte_results,
                stmt.where_clause.as_ref(),
                stmt.order_by.as_deref(),
                stmt.limit,
                Some(&stmt.select_list),
            )?;

            // Validate column references BEFORE processing rows (issue #2654)
            // This ensures column errors are caught even when tables are empty
            // Pass procedural context to allow procedure variables in WHERE clause
            // Pass outer_schema for correlated subqueries (#2694)
            super::validation::validate_select_columns_with_context(
                &stmt.select_list,
                stmt.where_clause.as_ref(),
                &from_result.schema,
                self.procedural_context,
                self.outer_schema,
            )?;

            self.execute_without_aggregation(stmt, from_result, cte_results)
        } else {
            // SELECT without FROM - evaluate expressions as a single row
            self.execute_select_without_from(stmt)
        }
    }

    /// Execute a chain of set operations left-to-right
    ///
    /// SQL set operations are left-associative, so:
    /// A EXCEPT B EXCEPT C should evaluate as (A EXCEPT B) EXCEPT C
    ///
    /// The parser creates a right-recursive AST structure, but we need to execute left-to-right.
    fn execute_set_operations(
        &self,
        mut left_results: Vec<vibesql_storage::Row>,
        set_op: &vibesql_ast::SetOperation,
        cte_results: &HashMap<String, CteResult>,
    ) -> Result<Vec<vibesql_storage::Row>, ExecutorError> {
        // Execute the immediate right query WITHOUT its set operations
        // This prevents right-recursive evaluation
        let right_stmt = &set_op.right;
        let has_aggregates =
            self.has_aggregates(&right_stmt.select_list) || right_stmt.having.is_some();
        let has_group_by = right_stmt.group_by.is_some();

        let right_results = if has_aggregates || has_group_by {
            self.execute_with_aggregation(right_stmt, cte_results)?
        } else if let Some(from_clause) = &right_stmt.from {
            // Note: LIMIT is None for set operation sides - it's applied after the set operation
            // Pass select_list for table elimination optimization (#3556)
            let from_result = self.execute_from_with_where(
                from_clause,
                cte_results,
                right_stmt.where_clause.as_ref(),
                right_stmt.order_by.as_deref(),
                None,
                Some(&right_stmt.select_list),
            )?;
            self.execute_without_aggregation(right_stmt, from_result, cte_results)?
        } else {
            self.execute_select_without_from(right_stmt)?
        };

        // Track memory for right result before set operation
        let right_size = estimate_result_size(&right_results);
        self.track_memory_allocation(right_size)?;

        // Apply the current operation
        left_results = apply_set_operation(left_results, right_results, set_op)?;

        // Track memory for combined result after set operation
        let combined_size = estimate_result_size(&left_results);
        self.track_memory_allocation(combined_size)?;

        // If the right side has more set operations, continue processing them
        // This creates the left-to-right evaluation: ((A op B) op C) op D
        if let Some(next_set_op) = &right_stmt.set_operation {
            left_results = self.execute_set_operations(left_results, next_set_op, cte_results)?;
        }

        Ok(left_results)
    }

    /// Execute a FROM clause with WHERE, ORDER BY, and LIMIT for optimization
    ///
    /// The LIMIT parameter enables early termination optimization (#3253):
    /// - When ORDER BY is satisfied by an index and no post-filter is needed,
    ///   the index scan can stop after fetching LIMIT rows
    ///
    /// Note: Table elimination (#3556) is now handled at the optimizer level
    /// via crate::optimizer::eliminate_unused_tables(), which runs before
    /// semi-join transformation to avoid complex interactions.
    pub(super) fn execute_from_with_where(
        &self,
        from: &vibesql_ast::FromClause,
        cte_results: &HashMap<String, CteResult>,
        where_clause: Option<&vibesql_ast::Expression>,
        order_by: Option<&[vibesql_ast::OrderByItem]>,
        limit: Option<usize>,
        _select_list: Option<&[vibesql_ast::SelectItem]>, // No longer used - optimization moved to optimizer pass
    ) -> Result<FromResult, ExecutorError> {
        use crate::select::scan::execute_from_clause;

        let from_result = execute_from_clause(
            from,
            cte_results,
            self.database,
            where_clause,
            order_by,
            limit,
            self.outer_row,
            self.outer_schema,
            |query| {
                // For derived table subqueries, create a child executor with CTE context
                // This allows CTEs from the outer WITH clause to be referenced in subqueries
                // Critical for queries like TPC-DS Q2 where CTEs are used in FROM subqueries
                if !cte_results.is_empty() {
                    let child = SelectExecutor::new_with_cte_and_depth(
                        self.database,
                        cte_results,
                        self.subquery_depth,
                    );
                    child.execute_with_columns(query)
                } else {
                    self.execute_with_columns(query)
                }
            },
        )?;

        // NOTE: We DON'T merge outer schema with from_result.schema here because:
        // 1. from_result.rows only contain values from inner tables
        // 2. Outer columns are resolved via the evaluator's outer_row/outer_schema
        // 3. Merging would create schema/row mismatch (schema has outer cols, rows don't)

        Ok(from_result)
    }
}
