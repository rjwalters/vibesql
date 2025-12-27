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
        // Validate aggregate function argument counts FIRST (issue #4367)
        // This catches errors like max(), min(*), sum(*) before any execution
        // Must happen before any fast paths or strategy selection
        super::validation::validate_aggregate_arguments(&stmt.select_list)?;

        // Validate no nested aggregates (issue #4439)
        // This catches errors like SUM(MIN(x)) before any execution
        // Uses "misuse of aggregate function X()" format to match SQLite's resolve.c
        super::validation::validate_no_nested_aggregates(&stmt.select_list)?;

        // Validate join table limit (issue #4711)
        // SQLite enforces a limit of 64 tables in a single join
        // This catches queries like SELECT * FROM t, t, t, ... (65+ times)
        super::validation::validate_join_table_limit(stmt)?;

        // Validate that aggregates don't reference outer columns (issue #4730)
        // When a subquery contains an aggregate function whose arguments reference columns
        // from an outer query (not from the subquery's own tables), it's a misuse.
        // Example: SELECT max((SELECT count(x) FROM t35b)) FROM t35a
        // Here, count(x) references outer column x from t35a, which is invalid.
        if let Some(outer_schema) = &self.outer_schema {
            super::validation::validate_no_aggregate_with_outer_column(
                stmt,
                outer_schema,
                self.database,
            )?;
        }

        #[cfg(feature = "profile-q6")]
        let execute_start = std::time::Instant::now();

        // Reset arena and clear pointer-based caches for fresh query execution (only at top level)
        // The subquery hash cache uses pointer-based keys which become invalid when ASTs are
        // dropped and memory is reused, so we must clear it between queries.
        // Note: The IN subquery result cache and correlation cache use content hashes as keys,
        // not pointers, so they can safely persist across queries.
        if self.subquery_depth == 0 {
            self.reset_arena();
            crate::evaluator::caching::clear_subquery_hash_cache();
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

        let table = self
            .database
            .get_table(table_name)
            .ok_or_else(|| ExecutorError::TableNotFound(table_name.to_string()))?;

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
                SelectItem::Expression { expr, alias: col_alias, .. } => {
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
            vibesql_ast::Expression::ColumnRef(col_id) => col_id.column_canonical().to_string(),
            vibesql_ast::Expression::Literal(val) => format!("{}", val),
            _ => "?column?".to_string(),
        }
    }

    /// Execute a SELECT statement and return both columns and rows
    pub fn execute_with_columns(
        &self,
        stmt: &vibesql_ast::SelectStmt,
    ) -> Result<SelectResult, ExecutorError> {
        // Resolve SELECT aliases in WHERE clause BEFORE predicate pushdown (SQLite extension)
        // This allows queries like: SELECT f1-22 AS x FROM t1 WHERE x > 0
        // IMPORTANT: Use schema-aware resolution to avoid incorrectly substituting
        // table column names with aggregate aliases (SQLite behavior)
        // Example: SELECT COUNT(*) AS col1 FROM tab0 WHERE col1 > 0
        // Here 'col1' in WHERE refers to the TABLE COLUMN, not the COUNT(*) alias
        let resolved_where = stmt.where_clause.as_ref().map(|where_expr| {
            // Try to build early schema from FROM clause
            if let Some(from_clause) = &stmt.from {
                if let Some(early_schema) =
                    super::aggregation::build_early_schema(from_clause, self.database)
                {
                    // Use schema-aware resolution
                    return crate::select::order::resolve_where_aliases_with_schema(
                        where_expr,
                        &stmt.select_list,
                        &early_schema,
                    );
                }
            }
            // Fall back to non-schema-aware resolution for complex FROM clauses
            crate::select::order::resolve_where_aliases(where_expr, &stmt.select_list)
        });

        // First, get the FROM result to access the schema
        let from_result = if let Some(from_clause) = &stmt.from {
            let mut cte_results = if let Some(with_clause) = &stmt.with_clause {
                execute_ctes(with_clause, |query, cte_ctx| self.execute_with_ctes(query, cte_ctx))?
            } else {
                HashMap::new()
            };
            // If we have access to outer query's CTEs (for subqueries/derived tables), merge them
            // in Local CTEs take precedence over outer CTEs if there are name conflicts
            // This is critical for queries like TPC-DS Q2 where CTEs are referenced from derived
            // tables
            if let Some(outer_cte_ctx) = self.cte_context {
                for (name, result) in outer_cte_ctx {
                    cte_results.entry(name.clone()).or_insert_with(|| result.clone());
                }
            }
            // Pass WHERE, ORDER BY, and LIMIT for optimizations
            // This is critical for GROUP BY queries to avoid CROSS JOINs
            // LIMIT enables early termination when ORDER BY is satisfied by index (#3253)
            // Pass select_list for table elimination optimization (#3556)
            let limit_val = stmt
                .limit
                .as_ref()
                .map(|expr| self.eval_limit_offset_expr(expr, "LIMIT"))
                .transpose()?;
            Some(self.execute_from_with_where(
                from_clause,
                &cte_results,
                resolved_where.as_ref(),
                stmt.order_by.as_deref(),
                limit_val,
                Some(&stmt.select_list),
            )?)
        } else {
            None
        };

        // Derive column names from the SELECT list (with table prefix for display)
        // Issue #4696: For VALUES statements, select_list is empty - derive from VALUES rows
        let columns = if stmt.select_list.is_empty() {
            if let Some(values_rows) = &stmt.values {
                // Generate column names: column1, column2, etc.
                let num_cols = values_rows.first().map(|r| r.len()).unwrap_or(0);
                (1..=num_cols).map(|i| format!("column{}", i)).collect()
            } else {
                // Empty select_list and no VALUES - return empty columns
                Vec::new()
            }
        } else {
            self.derive_column_names(&stmt.select_list, from_result.as_ref())?
        };

        // Execute the query to get rows
        let rows = self.execute(stmt)?;

        Ok(SelectResult { columns, rows })
    }

    /// Execute SELECT statement and return results with simple column names
    ///
    /// This is similar to `execute_with_columns` but returns column names without
    /// table prefixes. This is used for internal purposes like view creation
    /// where the full table.column format would cause column lookup issues.
    pub fn execute_with_simple_columns(
        &self,
        stmt: &vibesql_ast::SelectStmt,
    ) -> Result<SelectResult, ExecutorError> {
        // Resolve SELECT aliases in WHERE clause BEFORE predicate pushdown (SQLite extension)
        // This allows queries like: SELECT f1-22 AS x FROM t1 WHERE x > 0
        // IMPORTANT: Use schema-aware resolution to avoid incorrectly substituting
        // table column names with aggregate aliases (SQLite behavior)
        let resolved_where = stmt.where_clause.as_ref().map(|where_expr| {
            // Try to build early schema from FROM clause
            if let Some(from_clause) = &stmt.from {
                if let Some(early_schema) =
                    super::aggregation::build_early_schema(from_clause, self.database)
                {
                    // Use schema-aware resolution
                    return crate::select::order::resolve_where_aliases_with_schema(
                        where_expr,
                        &stmt.select_list,
                        &early_schema,
                    );
                }
            }
            // Fall back to non-schema-aware resolution for complex FROM clauses
            crate::select::order::resolve_where_aliases(where_expr, &stmt.select_list)
        });

        // Execute the FROM clause to get combined schema
        let from_result = if let Some(from_clause) = &stmt.from {
            let mut cte_results = if let Some(with_clause) = &stmt.with_clause {
                execute_ctes(with_clause, |query, cte_ctx| self.execute_with_ctes(query, cte_ctx))?
            } else {
                HashMap::new()
            };
            // If we have access to outer query's CTEs (for subqueries/derived tables), merge them
            if let Some(outer_cte_ctx) = self.cte_context {
                for (name, result) in outer_cte_ctx {
                    cte_results.entry(name.clone()).or_insert_with(|| result.clone());
                }
            }
            let limit_val = stmt
                .limit
                .as_ref()
                .map(|expr| self.eval_limit_offset_expr(expr, "LIMIT"))
                .transpose()?;
            Some(self.execute_from_with_where(
                from_clause,
                &cte_results,
                resolved_where.as_ref(),
                stmt.order_by.as_deref(),
                limit_val,
                Some(&stmt.select_list),
            )?)
        } else {
            None
        };

        // Derive column names from the SELECT list (without table prefix)
        // Issue #4696: For VALUES statements, select_list is empty - derive from VALUES rows
        let columns = if stmt.select_list.is_empty() {
            if let Some(values_rows) = &stmt.values {
                // Generate column names: column1, column2, etc.
                let num_cols = values_rows.first().map(|r| r.len()).unwrap_or(0);
                (1..=num_cols).map(|i| format!("column{}", i)).collect()
            } else {
                // Empty select_list and no VALUES - return empty columns
                Vec::new()
            }
        } else {
            self.derive_simple_column_names(&stmt.select_list, from_result.as_ref())?
        };

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
        // Note: Aggregate argument validation is done in execute() at the entry point.
        // See issue #4367.

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
                // The native columnar zero-copy path has known limitations with certain date
                // comparisons.
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
                            let limit_val = stmt
                                .limit
                                .as_ref()
                                .map(|expr| self.eval_limit_offset_expr(expr, "LIMIT"))
                                .transpose()?;
                            let offset_val = stmt
                                .offset
                                .as_ref()
                                .map(|expr| self.eval_limit_offset_expr(expr, "OFFSET"))
                                .transpose()?;
                            apply_limit_offset(result, limit_val, offset_val)
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
                // Note: Do NOT use early return here - we need to fall through to set operations
                // handling
                self.execute_expression_only(stmt, cte_results)?
            }
        };

        // Handle set operations (UNION, INTERSECT, EXCEPT)
        // Process operations left-to-right to ensure correct associativity
        if let Some(set_op) = &stmt.set_operation {
            // Extract collations from the leftmost SELECT list for set operation comparisons
            let collations = Self::extract_collations_from_select_list(&stmt.select_list);
            // Issue #4602: Compute left column count from AST for schema-level validation
            // This is needed when the left result set is empty (table has no rows)
            let left_col_count = super::nonagg::compute_select_list_column_count(
                stmt,
                self.database,
                Some(cte_results),
            )
            .ok();
            results = self.execute_set_operations(
                results,
                set_op,
                cte_results,
                &collations,
                left_col_count,
            )?;

            // Apply ORDER BY after set operations (if specified)
            // The UNION's default sort is overridden by explicit ORDER BY
            if let Some(order_by) = &stmt.order_by {
                // Collect aliases from all UNION branches for ORDER BY resolution
                // Pass database to enable wildcard expansion using table schemas
                let all_aliases = collect_union_aliases(self.database, stmt);
                results = self.sort_set_operation_results(
                    results,
                    order_by,
                    &stmt.select_list,
                    &all_aliases,
                )?;
            }

            // Apply LIMIT/OFFSET to the final result (after all set operations and ORDER BY)
            // For queries WITHOUT set operations, LIMIT/OFFSET is already applied
            // in execute_without_aggregation() or execute_with_aggregation()
            let limit_val = stmt
                .limit
                .as_ref()
                .map(|expr| self.eval_limit_offset_expr(expr, "LIMIT"))
                .transpose()?;
            let offset_val = stmt
                .offset
                .as_ref()
                .map(|expr| self.eval_limit_offset_expr(expr, "OFFSET"))
                .transpose()?;
            results = apply_limit_offset(results, limit_val, offset_val);
        }

        Ok(results)
    }

    /// Execute SELECT without FROM clause (ExpressionOnly strategy)
    ///
    /// This is a special case that doesn't use the pipeline trait since there's
    /// no table scan involved. Handles both simple expressions, aggregates,
    /// and standalone VALUES statements.
    fn execute_expression_only(
        &self,
        stmt: &vibesql_ast::SelectStmt,
        cte_results: &HashMap<String, CteResult>,
    ) -> Result<Vec<vibesql_storage::Row>, ExecutorError> {
        // Handle standalone VALUES statements (Issue #4546)
        // VALUES(1,2), (3,4) returns rows directly without a SELECT list
        if let Some(values_rows) = &stmt.values {
            let from_result = crate::select::scan::values::execute_values(
                values_rows,
                "_values_",
                None,
                Some(self.database),
            )?;
            let mut results = from_result.into_rows();

            // Issue #4696: If there's a set_operation, don't apply ORDER BY or LIMIT/OFFSET here
            // Let the caller handle them after set operations are processed
            if stmt.set_operation.is_some() {
                return Ok(results);
            }

            // Apply ORDER BY if specified
            if let Some(order_by) = &stmt.order_by {
                // For VALUES, column names are column1, column2, etc.
                // We need to map ORDER BY expressions to column indices
                results = self.apply_values_order_by(results, order_by, values_rows)?;
            }

            // Apply LIMIT/OFFSET
            let limit_val = stmt
                .limit
                .as_ref()
                .map(|expr| self.eval_limit_offset_expr(expr, "LIMIT"))
                .transpose()?;
            let offset_val = stmt
                .offset
                .as_ref()
                .map(|expr| self.eval_limit_offset_expr(expr, "OFFSET"))
                .transpose()?;
            return Ok(apply_limit_offset(results, limit_val, offset_val));
        }

        let has_aggregates = self.has_aggregates(&stmt.select_list) || stmt.having.is_some();

        if has_aggregates {
            // Aggregates without FROM need the aggregation path
            self.execute_with_aggregation(stmt, cte_results)
        } else {
            // Simple expression evaluation (e.g., SELECT 1 + 1)
            self.execute_select_without_from(stmt)
        }
    }

    /// Apply ORDER BY to VALUES statement results
    fn apply_values_order_by(
        &self,
        mut rows: Vec<vibesql_storage::Row>,
        order_by: &[vibesql_ast::OrderByItem],
        values_rows: &[Vec<vibesql_ast::Expression>],
    ) -> Result<Vec<vibesql_storage::Row>, ExecutorError> {
        // For VALUES, we sort by column position (1-indexed)
        // or by column name (column1, column2, etc.)
        let num_cols = values_rows.first().map(|r| r.len()).unwrap_or(0);

        rows.sort_by(|a, b| {
            for item in order_by {
                // Get the column index from the ORDER BY expression
                let col_idx = match &item.expr {
                    vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(n)) => {
                        // 1-indexed column position
                        (*n as usize).saturating_sub(1)
                    }
                    vibesql_ast::Expression::ColumnRef(col_id) => {
                        // column1, column2, etc.
                        let col_name = col_id.column_canonical();
                        if let Some(stripped) = col_name.strip_prefix("column") {
                            stripped.parse::<usize>().unwrap_or(0).saturating_sub(1)
                        } else {
                            0
                        }
                    }
                    _ => 0, // Default to first column for complex expressions
                };

                if col_idx < num_cols {
                    let cmp = a.values[col_idx].partial_cmp(&b.values[col_idx]);
                    if let Some(ord) = cmp {
                        let ord = match item.direction {
                            vibesql_ast::OrderDirection::Asc => ord,
                            vibesql_ast::OrderDirection::Desc => ord.reverse(),
                        };
                        if ord != std::cmp::Ordering::Equal {
                            return ord;
                        }
                    }
                }
            }
            std::cmp::Ordering::Equal
        });

        Ok(rows)
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

        // Resolve SELECT aliases in WHERE clause (SQLite extension)
        // This allows queries like: SELECT f1-22 AS x FROM t1 WHERE x > 0
        // IMPORTANT: Use schema-aware resolution to avoid incorrectly substituting
        // table column names with aggregate aliases (issue #4XXX)
        // Example: SELECT COUNT(*) AS col1 FROM tab0 WHERE col1 > 0
        // Here 'col1' in WHERE refers to the TABLE COLUMN, not the COUNT(*) alias
        let resolved_where = stmt.where_clause.as_ref().map(|where_expr| {
            crate::select::order::resolve_where_aliases_with_schema(
                where_expr,
                &stmt.select_list,
                &from_result.schema,
            )
        });

        // Stage 1: Filter (WHERE clause)
        let filtered = match pipeline.apply_filter(input, resolved_where.as_ref(), &exec_ctx) {
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

        // Stage 3: Limit/Offset (evaluate expressions and convert to u64)
        let limit_usize = stmt
            .limit
            .as_ref()
            .map(|expr| self.eval_limit_offset_expr(expr, "LIMIT"))
            .transpose()?;
        let offset_usize = stmt
            .offset
            .as_ref()
            .map(|expr| self.eval_limit_offset_expr(expr, "OFFSET"))
            .transpose()?;
        let limit_u64 = limit_usize.map(|l| l as u64);
        let offset_u64 = offset_usize.map(|o| o as u64);
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
            // Now that all index optimization has been moved to the scan level
            // (execute_index_scan), it happens BEFORE predicate pushdown, avoiding the
            // row-index mismatch problem. This allows predicate pushdown to work
            // correctly for all queries, improving performance.
            //
            // Fixes issues #1807, #1895, #1896, and #1902.

            // Resolve SELECT aliases in WHERE clause BEFORE predicate pushdown (SQLite extension)
            // This allows queries like: SELECT f1-22 AS x FROM t1 WHERE x > 0
            // The alias 'x' is resolved to 'f1-22' so predicate pushdown can work correctly
            // IMPORTANT: Use schema-aware resolution to avoid incorrectly substituting
            // table column names with aggregate aliases (SQLite behavior)
            let resolved_where = stmt.where_clause.as_ref().map(|where_expr| {
                // Try to build early schema from FROM clause
                if let Some(early_schema) =
                    super::aggregation::build_early_schema(from_clause, self.database)
                {
                    // Use schema-aware resolution
                    return crate::select::order::resolve_where_aliases_with_schema(
                        where_expr,
                        &stmt.select_list,
                        &early_schema,
                    );
                }
                // Fall back to non-schema-aware resolution for complex FROM clauses
                crate::select::order::resolve_where_aliases(where_expr, &stmt.select_list)
            });

            // Pass WHERE, ORDER BY, and LIMIT to execute_from for optimization
            // LIMIT enables early termination when ORDER BY is satisfied by index (#3253)
            // Pass select_list for table elimination optimization (#3556)
            //
            // Don't pass ORDER BY if there's a set operation - it will be handled at the set operation level
            let order_by_hint =
                if stmt.set_operation.is_some() { None } else { stmt.order_by.as_deref() };
            let limit_val = stmt
                .limit
                .as_ref()
                .map(|expr| self.eval_limit_offset_expr(expr, "LIMIT"))
                .transpose()?;
            let from_result = self.execute_from_with_where(
                from_clause,
                cte_results,
                resolved_where.as_ref(),
                order_by_hint,
                limit_val,
                Some(&stmt.select_list),
            )?;

            // Validate column references BEFORE processing rows (issue #2654)
            // This ensures column errors are caught even when tables are empty
            // Pass procedural context to allow procedure variables in WHERE clause
            // Pass outer_schema for correlated subqueries (#2694)
            // Note: We validate with the resolved_where since that's what gets executed
            super::validation::validate_select_columns_with_context(
                &stmt.select_list,
                resolved_where.as_ref(),
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

    /// Extract collation information from a SELECT list.
    ///
    /// Returns a Vec of Option<String> where each element corresponds to a SELECT item.
    /// If the SELECT item has a COLLATE clause (e.g., `a COLLATE NOCASE`), the collation
    /// name is returned; otherwise None.
    fn extract_collations_from_select_list(
        select_list: &[vibesql_ast::SelectItem],
    ) -> Vec<Option<String>> {
        select_list
            .iter()
            .map(|item| {
                // Check if the item is an expression with a COLLATE clause
                if let vibesql_ast::SelectItem::Expression {
                    expr: vibesql_ast::Expression::Collate { collation, .. },
                    ..
                } = item
                {
                    return Some(collation.clone());
                }
                None
            })
            .collect()
    }

    /// Execute a chain of set operations left-to-right
    ///
    /// SQL set operations are left-associative, so:
    /// A EXCEPT B EXCEPT C should evaluate as (A EXCEPT B) EXCEPT C
    ///
    /// The parser creates a right-recursive AST structure, but we need to execute left-to-right.
    ///
    /// The `collations` parameter specifies the collation for each column from the leftmost
    /// SELECT statement in the set operation chain. This is used for collation-aware comparisons.
    ///
    /// The `expected_col_count` parameter is the column count from the leftmost SELECT,
    /// computed from AST for schema-level validation even when results are empty.
    fn execute_set_operations(
        &self,
        mut left_results: Vec<vibesql_storage::Row>,
        set_op: &vibesql_ast::SetOperation,
        cte_results: &HashMap<String, CteResult>,
        collations: &[Option<String>],
        expected_col_count: Option<usize>,
    ) -> Result<Vec<vibesql_storage::Row>, ExecutorError> {
        // Execute the immediate right query WITHOUT its set operations
        // This prevents right-recursive evaluation
        let right_stmt = &set_op.right;

        // Issue #4602: Validate column count at schema level BEFORE execution
        // This catches mismatches even when result sets are empty
        // Use expected_col_count (from AST), or fall back to actual results
        let left_col_count = expected_col_count.unwrap_or_else(|| {
            if !left_results.is_empty() {
                left_results[0].values.len()
            } else {
                0 // Can't determine column count
            }
        });
        if left_col_count > 0 {
            // Try to compute right side column count from AST
            // Skip validation if we can't compute it (e.g., complex subqueries)
            if let Ok(right_col_count) = super::nonagg::compute_select_list_column_count(
                right_stmt,
                self.database,
                Some(cte_results),
            ) {
                if right_col_count != left_col_count {
                    let operator = match (&set_op.op, set_op.all) {
                        (vibesql_ast::SetOperator::Union, true) => "UNION ALL",
                        (vibesql_ast::SetOperator::Union, false) => "UNION",
                        (vibesql_ast::SetOperator::Intersect, true) => "INTERSECT ALL",
                        (vibesql_ast::SetOperator::Intersect, false) => "INTERSECT",
                        (vibesql_ast::SetOperator::Except, true) => "EXCEPT ALL",
                        (vibesql_ast::SetOperator::Except, false) => "EXCEPT",
                    };
                    return Err(ExecutorError::SetOperationColumnMismatch {
                        operator: operator.to_string(),
                    });
                }
            }
        }

        let has_aggregates =
            self.has_aggregates(&right_stmt.select_list) || right_stmt.having.is_some();
        let has_group_by = right_stmt.group_by.is_some();

        // Resolve SELECT aliases in WHERE clause BEFORE predicate pushdown (SQLite extension)
        let resolved_where = right_stmt.where_clause.as_ref().map(|where_expr| {
            crate::select::order::resolve_where_aliases(where_expr, &right_stmt.select_list)
        });

        let right_results = if has_aggregates || has_group_by {
            self.execute_with_aggregation(right_stmt, cte_results)?
        } else if let Some(from_clause) = &right_stmt.from {
            // Note: LIMIT is None for set operation sides - it's applied after the set operation
            // Pass select_list for table elimination optimization (#3556)
            let from_result = self.execute_from_with_where(
                from_clause,
                cte_results,
                resolved_where.as_ref(),
                right_stmt.order_by.as_deref(),
                None,
                Some(&right_stmt.select_list),
            )?;
            self.execute_without_aggregation(right_stmt, from_result, cte_results)?
        } else if let Some(values_rows) = &right_stmt.values {
            // Handle standalone VALUES in set operation right side (Issue #4546)
            let from_result = crate::select::scan::values::execute_values(
                values_rows,
                "_values_",
                None,
                Some(self.database),
            )?;
            from_result.into_rows()
        } else {
            self.execute_select_without_from(right_stmt)?
        };

        // Track memory for right result before set operation
        let right_size = estimate_result_size(&right_results);
        self.track_memory_allocation(right_size)?;

        // Apply the current operation with collation-aware comparison
        left_results = apply_set_operation(left_results, right_results, set_op, collations)?;

        // Track memory for combined result after set operation
        let combined_size = estimate_result_size(&left_results);
        self.track_memory_allocation(combined_size)?;

        // If the right side has more set operations, continue processing them
        // This creates the left-to-right evaluation: ((A op B) op C) op D
        if let Some(next_set_op) = &right_stmt.set_operation {
            left_results = self.execute_set_operations(
                left_results,
                next_set_op,
                cte_results,
                collations,
                expected_col_count, // Pass through the expected column count
            )?;
        }

        Ok(left_results)
    }
}

/// Extract column names from a FROM clause for wildcard expansion
/// Returns column names from all tables in the FROM clause in order
fn extract_column_names_from_from(
    database: &vibesql_storage::Database,
    from_clause: Option<&vibesql_ast::FromClause>,
) -> Vec<String> {
    fn extract_from_clause(
        database: &vibesql_storage::Database,
        from: &vibesql_ast::FromClause,
        columns: &mut Vec<String>,
    ) {
        match from {
            vibesql_ast::FromClause::Table { name, .. } => {
                // Look up the table in the database and get its column names
                if let Some(table) = database.get_table(name) {
                    for col in &table.schema.columns {
                        columns.push(col.name.clone());
                    }
                }
            }
            vibesql_ast::FromClause::Join { left, right, .. } => {
                // For joins, collect columns from both sides
                extract_from_clause(database, left, columns);
                extract_from_clause(database, right, columns);
            }
            vibesql_ast::FromClause::Subquery { .. } => {
                // Subqueries are complex - we'd need to analyze the subquery
                // For now, leave as unknown
            }
            vibesql_ast::FromClause::Values { .. } => {
                // VALUES clause doesn't have named columns
            }
        }
    }

    let mut columns = Vec::new();
    if let Some(from) = from_clause {
        extract_from_clause(database, from, &mut columns);
    }
    columns
}

/// Collect aliases and column names from a SELECT's select_list
/// Expands wildcards using the database schema when possible
fn collect_select_aliases(
    database: &vibesql_storage::Database,
    stmt: &vibesql_ast::SelectStmt,
) -> Vec<Option<String>> {
    let mut aliases = Vec::new();

    // Pre-compute column names from FROM clause for wildcard expansion
    let from_columns = extract_column_names_from_from(database, stmt.from.as_ref());
    let mut from_col_idx = 0;

    for item in &stmt.select_list {
        match item {
            vibesql_ast::SelectItem::Expression { expr, alias, .. } => {
                // Use explicit alias if present, otherwise derive from expression
                let name = if let Some(a) = alias {
                    Some(a.clone())
                } else {
                    // Derive from expression - use column name for ColumnRef
                    match expr {
                        vibesql_ast::Expression::ColumnRef(col_id) => {
                            Some(col_id.column_canonical().to_string())
                        }
                        _ => None,
                    }
                };
                aliases.push(name);
            }
            vibesql_ast::SelectItem::Wildcard { .. } => {
                // Expand wildcard to actual column names from FROM clause
                if from_col_idx < from_columns.len() {
                    // Push all remaining columns from FROM clause
                    for col_name in &from_columns[from_col_idx..] {
                        aliases.push(Some(col_name.to_string()));
                    }
                    from_col_idx = from_columns.len();
                } else {
                    // Fallback: no FROM clause info available
                    aliases.push(None);
                }
            }
            vibesql_ast::SelectItem::QualifiedWildcard { qualifier, .. } => {
                // Expand qualified wildcard (e.g., t.*) to columns from that table
                if let Some(table) = database.get_table(qualifier) {
                    for col in &table.schema.columns {
                        aliases.push(Some(col.name.clone()));
                    }
                } else {
                    // Table not found - might be an alias, fallback to unknown
                    aliases.push(None);
                }
            }
        }
    }

    aliases
}

/// Collect aliases and column names from all SELECT statements in a UNION chain
/// Returns a vec where each element is a vec of aliases/column names for that SELECT's columns
///
/// For each SELECT item, this returns:
/// - The explicit alias if present (e.g., "x" in SELECT col AS x)
/// - The column name if it's a ColumnRef without an alias (e.g., "col" in SELECT col)
/// - For wildcards: The actual column names from the table schema
/// - None for complex expressions without aliases
fn collect_union_aliases(
    database: &vibesql_storage::Database,
    stmt: &vibesql_ast::SelectStmt,
) -> Vec<Vec<Option<String>>> {
    let mut all_aliases = Vec::new();

    // Collect from the main SELECT (with wildcard expansion)
    all_aliases.push(collect_select_aliases(database, stmt));

    // Recursively collect from set operations
    let mut current_set_op = stmt.set_operation.as_ref();
    while let Some(set_op) = current_set_op {
        all_aliases.push(collect_select_aliases(database, &set_op.right));
        current_set_op = set_op.right.set_operation.as_ref();
    }

    all_aliases
}

impl SelectExecutor<'_> {
    /// Resolve a column name to an index in set operation ORDER BY
    ///
    /// Tries to match by:
    /// 1. Numeric column reference (e.g., "1" -> column 0)
    /// 2. Alias name from first branch's column info
    /// 3. Alias name from any UNION branch (SQLite compatibility)
    fn resolve_order_by_column(
        column: &str,
        column_info: &[(String, Option<String>)],
        all_union_aliases: &[Vec<Option<String>>],
    ) -> Option<usize> {
        // Try to parse as numeric column reference first
        if let Ok(n) = column.parse::<usize>() {
            return Some(n.saturating_sub(1));
        }

        // Try to match by alias name first, then by original expression name
        // Use case-insensitive matching for SQLite compatibility
        let col_idx = column_info.iter().position(|(alias, orig)| {
            alias.eq_ignore_ascii_case(column)
                || orig.as_ref().is_some_and(|o| o.eq_ignore_ascii_case(column))
        });

        // If not found, check if any branch has this alias (SQLite compatibility)
        // SQLite allows ORDER BY to reference aliases from any UNION branch
        col_idx.or_else(|| {
            // Check all positions in all branches
            for branch_aliases in all_union_aliases {
                for (idx, alias_opt) in branch_aliases.iter().enumerate() {
                    if let Some(alias) = alias_opt {
                        if alias.eq_ignore_ascii_case(column) {
                            return Some(idx);
                        }
                    }
                }
            }
            None
        })
    }

    /// Sort set operation results by ORDER BY clause
    ///
    /// ORDER BY on a UNION/INTERSECT/EXCEPT can use:
    /// 1. Column positions (e.g., ORDER BY 1, 2 DESC)
    /// 2. Column names from the first SELECT (e.g., ORDER BY x)
    /// 3. Original column names from expressions (e.g., ORDER BY a when SELECT a AS x)
    ///
    /// The all_union_aliases parameter provides expanded column names (with wildcards resolved)
    /// from all UNION branches for name resolution.
    fn sort_set_operation_results(
        &self,
        mut rows: Vec<vibesql_storage::Row>,
        order_by: &[vibesql_ast::OrderByItem],
        select_list: &[vibesql_ast::SelectItem],
        all_union_aliases: &[Vec<Option<String>>], // Aliases from all UNION branches (wildcards expanded)
    ) -> Result<Vec<vibesql_storage::Row>, ExecutorError> {
        use crate::select::grouping::compare_sql_values_with_collation;
        use std::cmp::Ordering;

        // Build column name map from the first branch's aliases (with wildcards already expanded)
        // The all_union_aliases already contains expanded column names from collect_union_aliases
        let column_info: Vec<(String, Option<String>)> =
            if let Some(first_branch) = all_union_aliases.first() {
                first_branch
                    .iter()
                    .map(|name_opt| {
                        let name = name_opt.clone().unwrap_or_else(|| "?column?".to_string());
                        (name, None) // Original name is the same as alias for expanded wildcards
                    })
                    .collect()
            } else {
                Vec::new()
            };

        // Parse order_by items and resolve to column indices
        // (column_index, is_desc, nulls_first, collation)
        let mut sort_columns: Vec<(usize, bool, bool, Option<String>)> = Vec::new();
        for (term_index, item) in order_by.iter().enumerate() {
            // Extract column index and optional collation
            let (col_idx_opt, collation) = match &item.expr {
                // Numeric literal: ORDER BY 1
                vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(n)) => {
                    (Some((*n as usize).saturating_sub(1)), None) // 1-based to 0-based
                }
                // Column reference: ORDER BY x or ORDER BY a
                vibesql_ast::Expression::ColumnRef(col_id) => {
                    let column = col_id.column_canonical();
                    (Self::resolve_order_by_column(column, &column_info, all_union_aliases), None)
                }
                // COLLATE expression: ORDER BY x COLLATE nocase
                // Extract the expression and resolve it
                vibesql_ast::Expression::Collate { expr, collation } => {
                    let col_idx = match expr.as_ref() {
                        vibesql_ast::Expression::ColumnRef(col_id) => {
                            let column = col_id.column_canonical();
                            Self::resolve_order_by_column(column, &column_info, all_union_aliases)
                        }
                        vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(n)) => {
                            Some((*n as usize).saturating_sub(1))
                        }
                        // For complex expressions inside COLLATE, match against select_list
                        other_expr => select_list.iter().enumerate().find_map(|(idx, item)| {
                            if let vibesql_ast::SelectItem::Expression {
                                expr: select_expr, ..
                            } = item
                            {
                                if select_expr == other_expr {
                                    return Some(idx);
                                }
                            }
                            None
                        }),
                    };
                    (col_idx, Some(collation.clone()))
                }
                // Complex expressions (aggregates, arithmetic, etc.): match against first SELECT's select_list
                // SQLite allows ORDER BY to reference expressions from the first SELECT
                // e.g., SELECT count(*) FROM t UNION SELECT n FROM t2 ORDER BY count(*)
                other_expr => {
                    let col_idx = select_list.iter().enumerate().find_map(|(idx, item)| {
                        if let vibesql_ast::SelectItem::Expression { expr, .. } = item {
                            if expr == other_expr {
                                return Some(idx);
                            }
                        }
                        None
                    });
                    (col_idx, None)
                }
            };

            if let Some(col_idx) = col_idx_opt {
                // Validate column index is within bounds
                if col_idx >= column_info.len() {
                    return Err(ExecutorError::OrderByOutOfRange {
                        term_position: term_index + 1,
                        column_number: (col_idx + 1) as i64,
                        select_list_len: column_info.len(),
                    });
                }
                let is_desc = item.direction == vibesql_ast::OrderDirection::Desc;
                // Determine NULL ordering:
                // - If explicitly specified via NULLS FIRST/LAST, use that
                // - Default: SQLite treats NULL as the smallest value:
                //   - ASC order: NULL comes first (before all other values)
                //   - DESC order: NULL comes last (after all other values)
                let nulls_first = match item.nulls_order {
                    Some(vibesql_ast::NullsOrder::First) => true,
                    Some(vibesql_ast::NullsOrder::Last) => false,
                    None => !is_desc, // SQLite default: NULL is smallest
                };
                sort_columns.push((col_idx, is_desc, nulls_first, collation));
            } else {
                // ORDER BY term doesn't match any column in the result set
                return Err(ExecutorError::OrderByTermNotInResultSet {
                    term_position: term_index + 1,
                });
            }
        }

        if sort_columns.is_empty() {
            return Ok(rows);
        }

        rows.sort_by(|a, b| {
            for (col_idx, is_desc, nulls_first, collation) in &sort_columns {
                let val_a = a.values.get(*col_idx);
                let val_b = b.values.get(*col_idx);

                // Handle NULLs according to nulls_first setting
                let cmp = match (
                    val_a.map(|v| v.is_null()).unwrap_or(true),
                    val_b.map(|v| v.is_null()).unwrap_or(true),
                ) {
                    (true, true) => Ordering::Equal,
                    (true, false) => {
                        if *nulls_first {
                            return Ordering::Less; // NULL sorts before non-NULL
                        } else {
                            return Ordering::Greater; // NULL sorts after non-NULL
                        }
                    }
                    (false, true) => {
                        if *nulls_first {
                            return Ordering::Greater; // non-NULL sorts after NULL
                        } else {
                            return Ordering::Less; // non-NULL sorts before NULL
                        }
                    }
                    (false, false) => {
                        // Compare non-NULL values
                        match (val_a, val_b) {
                            (Some(a_val), Some(b_val)) => {
                                // Apply collation if specified
                                let cmp = compare_sql_values_with_collation(
                                    a_val,
                                    b_val,
                                    collation.as_deref(),
                                );
                                if *is_desc {
                                    cmp.reverse()
                                } else {
                                    cmp
                                }
                            }
                            _ => Ordering::Equal, // Shouldn't happen since we checked is_null above
                        }
                    }
                };

                if cmp != Ordering::Equal {
                    return cmp;
                }
            }
            Ordering::Equal
        });

        Ok(rows)
    }

    /// Execute a FROM clause with WHERE, ORDER BY, and LIMIT for optimization
    ///
    /// The LIMIT parameter enables early termination optimization (#3253):
    /// - When ORDER BY is satisfied by an index and no post-filter is needed, the index scan can
    ///   stop after fetching LIMIT rows
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
        _select_list: Option<&[vibesql_ast::SelectItem]>, /* No longer used - optimization moved
                                                           * to optimizer pass */
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
                // For derived table subqueries, create a child executor with:
                // 1. CTE context (allows CTEs from outer WITH clause to be referenced)
                // 2. Outer context (allows correlated columns from outer queries to be resolved)
                // Critical for:
                // - TPC-DS Q2: CTEs in FROM subqueries
                // - select1-18.x: Nested correlated subqueries referencing outer columns
                if !cte_results.is_empty() {
                    // FIX for select1-18.x: When both CTE and outer context exist,
                    // we need to use new_with_outer_and_cte_and_depth to pass both
                    if let (Some(outer_row), Some(outer_schema)) =
                        (self.outer_row, self.outer_schema)
                    {
                        let child = SelectExecutor::new_with_outer_and_cte_and_depth(
                            self.database,
                            outer_row,
                            outer_schema,
                            cte_results,
                            self.subquery_depth,
                        );
                        child.execute_with_columns(query)
                    } else {
                        let child = SelectExecutor::new_with_cte_and_depth(
                            self.database,
                            cte_results,
                            self.subquery_depth,
                        );
                        child.execute_with_columns(query)
                    }
                } else if let (Some(outer_row), Some(outer_schema)) =
                    (self.outer_row, self.outer_schema)
                {
                    // FIX for select1-18.x: Pass outer context for derived table subqueries
                    // This enables column resolution from outer scopes in deeply nested queries
                    let child = SelectExecutor::new_with_outer_context_and_depth(
                        self.database,
                        outer_row,
                        outer_schema,
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
