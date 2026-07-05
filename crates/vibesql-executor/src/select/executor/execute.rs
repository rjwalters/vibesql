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
        cte::{execute_ctes_for_stmt, execute_ctes_with_memory_check, CteResult},
        helpers::apply_limit_offset,
        join::FromResult,
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

        // Validate IN (SELECT ...) subquery column counts (issue #5191)
        // SQLite raises "sub-select returns N columns - expected 1" at
        // prepare time; the runtime check in the expression evaluator only
        // fires when a row is actually evaluated, so it misses the
        // empty-table case (window9.test 3.4).
        super::validation::validate_in_subquery_column_counts(stmt, self.database)?;

        // Validate SQLite index hints (issue #5235)
        // INDEXED BY must name an index that exists on that table; SQLite
        // reports "no such index: X" at prepare time. The hint is otherwise
        // ignored by the planner (PR #5234).
        super::validation::validate_index_hints(stmt, self.database)?;

        // Validate aggregates with outer column references in subqueries (issue #4853)
        // This catches the specific case where a scalar subquery appears as an argument
        // to an outer aggregate function, and the subquery's aggregate references an
        // outer column. Example: SELECT max((SELECT count(x) FROM t35b)) FROM t35a;
        // Note: Standalone correlated subqueries are valid and allowed, e.g.,
        // SELECT (SELECT count(x) FROM t35b) FROM t35a; is valid in SQLite.
        super::validation::validate_aggregate_subquery_outer_refs(stmt, self.database)?;

        // Validate bare scalar subqueries in the SELECT list / ORDER BY for
        // misuse of aggregate / window / row-value (#5069, refined #5104).
        //
        // A "bare" scalar subquery (no FROM) re-borrows the outer aggregation
        // context. SQLite has two distinct behaviors depending on where the
        // subquery appears:
        //
        // * SELECT-list (#5104): `SELECT (SELECT avg(a)) FROM t2` is allowed —
        //   the outer query implicitly collapses to a single-row aggregate. We
        //   pass `SubqueryContext::SelectList` so the validator skips the
        //   "misuse of aggregate" rejection in this position.
        // * WHERE / HAVING / ORDER BY (and arguments to outer functions): an
        //   outer-correlated aggregate inside a bare scalar subquery is still
        //   a misuse and must be rejected. We pass `WhereOrEqual` for ORDER BY
        //   to preserve SQLite's rejection there.
        //
        // Row-value misuse (`(SELECT (a, b))`) is detected in both contexts.
        for item in &stmt.select_list {
            if let vibesql_ast::SelectItem::Expression { expr, .. } = item {
                super::validation::validate_subquery_context_misuse(
                    expr,
                    super::validation::SubqueryContext::SelectList,
                )?;
            }
        }
        if let Some(order_by) = stmt.order_by.as_deref() {
            for item in order_by {
                super::validation::validate_subquery_context_misuse(
                    &item.expr,
                    super::validation::SubqueryContext::WhereOrEqual,
                )?;
            }
        }

        // Row-value misuse validation (SQLite: "row value misused" at prepare
        // time, even for empty tables). A row value is only legal in the
        // comparison / IS / BETWEEN / IN / simple-CASE positions; anywhere else
        // (bare in a SELECT list, compared against a scalar, ORDER BY /
        // GROUP BY expression) is an error.
        for item in &stmt.select_list {
            if let vibesql_ast::SelectItem::Expression { expr, .. } = item {
                super::validation::validate_row_value_usage(expr)?;
            }
        }
        if let Some(where_expr) = &stmt.where_clause {
            super::validation::validate_row_value_usage(where_expr)?;
        }
        if let Some(having_expr) = &stmt.having {
            super::validation::validate_row_value_usage(having_expr)?;
        }
        if let Some(order_by) = stmt.order_by.as_deref() {
            for item in order_by {
                super::validation::validate_row_value_usage(&item.expr)?;
            }
        }
        if let Some(group_by) = &stmt.group_by {
            if let Some(exprs) = group_by.as_simple() {
                for expr in exprs {
                    super::validation::validate_row_value_usage(expr)?;
                }
            }
        }

        // Validate GROUP BY clauses across this statement and all nested
        // subqueries for window-function misuse, including positional
        // (`GROUP BY 1`) and alias references that resolve to a window
        // function in the subquery's SELECT list (#5093, window1.test 47.2).
        // SQLite raises this at prepare time, so we must walk subqueries
        // here rather than relying on per-SELECT execution paths (the
        // outer WHERE may short-circuit before the inner SELECT executes).
        super::validation::validate_group_by_window_misuse(stmt)?;

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

        // Guard against exponentially self-referential view nests (SQLite ticket
        // [d58ccbb3f1b], exercised by view3.test / #5394). VibeSQL materializes
        // views lazily by executing their queries, so a doubling view nest
        // (v2=v1∪v1, v4=v2∪v2, …) expands exponentially and would hang. Match
        // SQLite by statically detecting >65535 references to any view before
        // execution. Only run once per top-level query.
        if self.subquery_depth == 0 {
            crate::select::view_reference_guard::check_view_reference_limit(stmt, self.database)?;
        }

        // Fast path for simple point-lookup queries (TPC-C optimization)
        // This bypasses expensive optimizer passes for queries like:
        // SELECT col FROM table WHERE pk = value
        // Skip fast path if reverse_unordered_selects is ON and no ORDER BY (needs reversal)
        let skip_fast_path_for_reversal =
            self.database.reverse_unordered_selects() && stmt.order_by.is_none();
        if self.subquery_depth == 0
            && self.outer_row.is_none()
            && self.cte_context.is_none()
            && !skip_fast_path_for_reversal
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
            && !skip_fast_path_for_reversal
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

        // Push WHERE conjuncts into window-function subqueries/views (#5292)
        // Predicates over a PARTITION BY prefix of every window in a derived
        // table or view are copied into the inner WHERE clause, filtering
        // whole partitions before the window functions run (and enabling
        // index scans inside the subquery). Mirrors SQLite's
        // pushDownWhereTerms() gate for window queries.
        //
        // CTE names already in scope (from enclosing queries) are threaded
        // in so the pass can decline view expansion for shadowed names: at
        // execution time CTEs take precedence over catalog views, and this
        // pass runs before CTE resolution. Empty set when there is no outer
        // CTE context (the common case; HashSet::new() does not allocate).
        let outer_cte_names: std::collections::HashSet<String> = self
            .cte_context
            .map(|ctx| ctx.keys().map(|name| name.to_ascii_lowercase()).collect())
            .unwrap_or_default();
        let optimized_stmt = crate::optimizer::push_where_into_window_subqueries(
            optimized_stmt,
            self.database,
            &outer_cte_names,
        );

        // Apply scalar subquery decorrelation (#4760)
        // Transforms correlated scalar subqueries with aggregates (e.g., AVG, SUM, MIN)
        // into CTE + JOIN patterns for O(n) instead of O(n²) execution.
        // Example: WHERE x > 1.2 * (SELECT AVG(y) FROM t WHERE t.c = outer.c)
        // Becomes: WITH _cte AS (SELECT c, AVG(y) FROM t GROUP BY c)
        //          ... JOIN _cte ON outer.c = _cte.c WHERE x > 1.2 * _cte._avg
        let optimized_stmt = crate::optimizer::apply_scalar_decorrelation(&optimized_stmt);

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
            // This query has its own CTEs - execute them with memory tracking.
            // The statement is passed as reachability root so unreferenced CTEs
            // are skipped (SQLite lazy expansion, issue #5838).
            execute_ctes_with_memory_check(
                with_clause,
                Some(&optimized_stmt),
                self.database,
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
        let mut result = self.execute_with_ctes(&optimized_stmt, &cte_results)?;

        #[cfg(feature = "profile-q6")]
        {
            let _total_execute = execute_start.elapsed();
        }

        // Apply PRAGMA reverse_unordered_selects if enabled
        // Only reverse if there's no ORDER BY clause in the original statement
        if stmt.order_by.is_none() && self.database.reverse_unordered_selects() {
            result.reverse();
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
            vibesql_ast::Expression::Wildcard => "*".to_string(),
            _ => "?column?".to_string(),
        }
    }

    /// Resolve the output column names of a SELECT statement WITHOUT returning rows.
    ///
    /// This is the read-only column-name resolver used by the PostgreSQL extended
    /// query protocol's `Describe` message (#5429), which must report the
    /// `RowDescription` before any `Execute`. It reuses the exact wildcard
    /// expansion and expression-naming logic as [`execute_with_columns`]
    /// (`derive_column_names`), so the names match the simple-query path
    /// label-for-label, including:
    /// - explicit columns and aliases (`SELECT a, b AS x` -> `[a, x]`)
    /// - `SELECT *` / `t.*` expanded against the catalog schema
    /// - join wildcards (`SELECT t1.*, t2.c` -> t1's columns then `c`)
    /// - derived names for expression columns
    ///
    /// Unlike `execute_with_columns`, this does NOT materialize result rows: it
    /// builds the FROM-clause schema (the same `FromResult` the executor uses)
    /// purely to expand wildcards, then derives the names.
    pub fn resolve_column_names(
        &self,
        stmt: &vibesql_ast::SelectStmt,
    ) -> Result<Vec<String>, ExecutorError> {
        // Guard exponentially self-referential view nests before touching the
        // FROM clause (which materializes views), mirroring execute_with_columns
        // (#5394, view3.test).
        crate::select::view_reference_guard::check_view_reference_limit(stmt, self.database)?;

        // Resolve SELECT aliases in WHERE clause for schema construction, matching
        // execute_with_columns so the FROM schema is built identically.
        let resolved_where = if stmt.where_clause.is_some()
            && crate::select::order::select_list_has_aliases(&stmt.select_list)
        {
            stmt.where_clause.as_ref().map(|where_expr| {
                if let Some(from_clause) = &stmt.from {
                    if let Some(early_schema) =
                        super::aggregation::build_early_schema(from_clause, self.database)
                    {
                        return crate::select::order::resolve_where_aliases_with_schema(
                            where_expr,
                            &stmt.select_list,
                            &early_schema,
                        );
                    }
                }
                crate::select::order::resolve_where_aliases(where_expr, &stmt.select_list)
            })
        } else {
            stmt.where_clause.clone()
        };

        // Build the FROM result to access the combined schema. This is required
        // to expand `*` / `table.*` against the real column layout (including
        // joins and NATURAL/USING deduplication). We do not return the rows.
        let from_result = if let Some(from_clause) = &stmt.from {
            let mut cte_results = if let Some(with_clause) = &stmt.with_clause {
                execute_ctes_for_stmt(with_clause, stmt, self.database, |query, cte_ctx| {
                    self.execute_with_ctes(query, cte_ctx)
                })?
            } else {
                HashMap::new()
            };
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

        // Derive column names exactly as execute_with_columns does.
        let columns = if stmt.select_list.is_empty() {
            if let Some(values_rows) = &stmt.values {
                let num_cols = values_rows.first().map(|r| r.len()).unwrap_or(0);
                (1..=num_cols).map(|i| format!("column{}", i)).collect()
            } else {
                Vec::new()
            }
        } else {
            self.derive_column_names(&stmt.select_list, from_result.as_ref())?
        };

        Ok(columns)
    }

    /// Execute a SELECT statement and return both columns and rows
    pub fn execute_with_columns(
        &self,
        stmt: &vibesql_ast::SelectStmt,
    ) -> Result<SelectResult, ExecutorError> {
        // Guard exponentially self-referential view nests before executing the
        // FROM clause (which materializes views). This entry point runs the FROM
        // *before* execute(), so the guard inside execute() would be too late
        // (#5394, view3.test).
        crate::select::view_reference_guard::check_view_reference_limit(stmt, self.database)?;

        // Resolve SELECT aliases in WHERE clause BEFORE predicate pushdown (SQLite extension)
        // This allows queries like: SELECT f1-22 AS x FROM t1 WHERE x > 0
        // IMPORTANT: Use schema-aware resolution to avoid incorrectly substituting
        // table column names with aggregate aliases (SQLite behavior)
        // Example: SELECT COUNT(*) AS col1 FROM tab0 WHERE col1 > 0
        // Here 'col1' in WHERE refers to the TABLE COLUMN, not the COUNT(*) alias
        // PERFORMANCE: Skip alias resolution if no aliases exist (common case in OLTP)
        let resolved_where = if stmt.where_clause.is_some()
            && crate::select::order::select_list_has_aliases(&stmt.select_list)
        {
            stmt.where_clause.as_ref().map(|where_expr| {
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
            })
        } else {
            stmt.where_clause.clone()
        };

        // First, get the FROM result to access the schema
        let from_result = if let Some(from_clause) = &stmt.from {
            let mut cte_results = if let Some(with_clause) = &stmt.with_clause {
                execute_ctes_for_stmt(with_clause, stmt, self.database, |query, cte_ctx| {
                    self.execute_with_ctes(query, cte_ctx)
                })?
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
        // Guard exponentially self-referential view nests before executing the
        // FROM clause (which materializes views). See execute_with_columns and
        // #5394 (view3.test).
        crate::select::view_reference_guard::check_view_reference_limit(stmt, self.database)?;

        // Resolve SELECT aliases in WHERE clause BEFORE predicate pushdown (SQLite extension)
        // This allows queries like: SELECT f1-22 AS x FROM t1 WHERE x > 0
        // IMPORTANT: Use schema-aware resolution to avoid incorrectly substituting
        // table column names with aggregate aliases (SQLite behavior)
        // PERFORMANCE: Skip alias resolution if no aliases exist (common case in OLTP)
        let resolved_where = if stmt.where_clause.is_some()
            && crate::select::order::select_list_has_aliases(&stmt.select_list)
        {
            stmt.where_clause.as_ref().map(|where_expr| {
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
            })
        } else {
            stmt.where_clause.clone()
        };

        // Execute the FROM clause to get combined schema
        let from_result = if let Some(from_clause) = &stmt.from {
            let mut cte_results = if let Some(with_clause) = &stmt.with_clause {
                execute_ctes_for_stmt(with_clause, stmt, self.database, |query, cte_ctx| {
                    self.execute_with_ctes(query, cte_ctx)
                })?
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
            // Use schema-aware lookup to get column collations from CREATE TABLE definitions
            let collations = Self::extract_collations_from_select_list_with_schema(
                &stmt.select_list,
                Some(self.database),
                stmt.from.as_ref(),
            );
            // Issue #4602: Compute left column count from AST for schema-level validation
            // This is needed when the left result set is empty (table has no rows)
            // Issue #4922: Must propagate errors (not use .ok()) to catch column count mismatches
            // in set operations like UNION/INTERSECT/EXCEPT
            let left_col_count = super::nonagg::compute_select_list_column_count(
                stmt,
                self.database,
                Some(cte_results),
            )?;
            let left_col_count = Some(left_col_count);
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
                let all_aliases = super::set_operations::collect_union_aliases(self.database, stmt);
                // Pass column collations from the first SELECT for collation-aware sorting
                results = self.sort_set_operation_results(
                    results,
                    order_by,
                    &stmt.select_list,
                    &all_aliases,
                    &collations,
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
            // Issue #5036: Check if any VALUES row contains window functions
            // If so, evaluate those rows via SELECT-without-FROM path which supports
            // window function evaluation, rather than the plain VALUES evaluator
            let has_windows = values_rows
                .iter()
                .any(|row| row.iter().any(crate::select::window::expression_has_window_function));

            let mut results = if has_windows {
                let mut all_rows = Vec::with_capacity(values_rows.len());
                for row_exprs in values_rows {
                    let row_has_window =
                        row_exprs.iter().any(crate::select::window::expression_has_window_function);
                    if row_has_window {
                        // Construct a SELECT <expr1>, <expr2>, ... statement
                        // and execute via the window-aware path
                        let select_list: Vec<vibesql_ast::SelectItem> = row_exprs
                            .iter()
                            .enumerate()
                            .map(|(i, expr)| vibesql_ast::SelectItem::Expression {
                                expr: expr.clone(),
                                alias: Some(format!("column{}", i + 1)),
                                source_text: None,
                            })
                            .collect();
                        let temp_stmt = vibesql_ast::SelectStmt {
                            with_clause: None,
                            distinct: false,
                            select_list,
                            into_table: None,
                            into_variables: None,
                            from: None,
                            where_clause: None,
                            group_by: None,
                            having: None,
                            window_definitions: None,
                            order_by: None,
                            limit: None,
                            offset: None,
                            set_operation: None,
                            values: None,
                        };
                        let row_results =
                            self.execute_select_without_from(&temp_stmt, cte_results)?;
                        all_rows.extend(row_results);
                    } else {
                        // No window functions - evaluate normally
                        let schema = crate::schema::CombinedSchema::empty();
                        let empty_row = vibesql_storage::Row::new(vec![]);
                        let mut evaluator =
                            crate::evaluator::CombinedExpressionEvaluator::with_database(
                                &schema,
                                self.database,
                            );
                        // Thread CTE context so subqueries in VALUES rows can
                        // reference names bound by an enclosing WITH (#5350)
                        if !cte_results.is_empty() {
                            evaluator = evaluator.with_cte_context(cte_results);
                        } else if let Some(cte_ctx) = self.cte_context {
                            evaluator = evaluator.with_cte_context(cte_ctx);
                        }
                        let mut values = Vec::with_capacity(row_exprs.len());
                        for expr in row_exprs {
                            let value = evaluator.eval(expr, &empty_row)?;
                            values.push(value);
                        }
                        all_rows.push(vibesql_storage::Row::new(values));
                    }
                }
                all_rows
            } else {
                // Thread CTE context so subqueries in VALUES rows can reference
                // names bound by an enclosing WITH clause (#5353); fall back to
                // the outer context when no local CTEs were materialized.
                let cte_ctx =
                    if !cte_results.is_empty() { Some(cte_results) } else { self.cte_context };
                let from_result = crate::select::scan::values::execute_values(
                    values_rows,
                    "_values_",
                    None,
                    Some(self.database),
                    cte_ctx,
                )?;
                from_result.into_rows()
            };

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
            self.execute_select_without_from(stmt, cte_results)
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

        // Issue #5104: implicit-outer-aggregate-collapse requires the
        // aggregation pipeline. The columnar pipelines do not support this
        // semantic, so fall back to the row-oriented path which routes
        // through `execute_with_aggregation`.
        if !has_aggregates
            && !has_group_by
            && self.select_list_has_outer_aggregate_collapse(&stmt.select_list)
        {
            log::debug!(
                "{} pipeline: implicit-outer-aggregate-collapse — falling back to row-oriented",
                strategy_name
            );
            return Ok(None);
        }

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
        } else if let Some(proc_ctx) = self.procedural_context {
            // Procedural variables in WHERE / SELECT (e.g. stored function bodies)
            exec_ctx = exec_ctx.with_procedural_context(proc_ctx);
        } else if let Some(trigger_ctx) = self.trigger_context {
            // Issue #5082: thread trigger context so OLD/NEW pseudo-vars resolve in
            // the synthetic SELECT used by UPDATE…FROM inside trigger bodies.
            exec_ctx = exec_ctx.with_trigger_context(trigger_ctx);
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
        // PERFORMANCE: Skip alias resolution if no aliases exist (common case in OLTP)
        let resolved_where = if crate::select::order::select_list_has_aliases(&stmt.select_list) {
            stmt.where_clause.as_ref().map(|where_expr| {
                crate::select::order::resolve_where_aliases_with_schema(
                    where_expr,
                    &stmt.select_list,
                    &from_result.schema,
                )
            })
        } else {
            stmt.where_clause.clone()
        };

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

        // Issue #5104: SQLite's implicit-outer-aggregate-collapse semantics.
        // When the SELECT list contains a bare scalar subquery whose body has
        // an aggregate referencing an outer column, the outer query collapses
        // into a single-row aggregate (with the inner aggregate computed over
        // all outer rows). Route through the aggregation pipeline so the
        // single-group grand-total path runs and `outer_rows` is wired through
        // to the inner subquery.
        let has_implicit_collapse = !has_aggregates
            && !has_group_by
            && self.select_list_has_outer_aggregate_collapse(&stmt.select_list);

        if has_aggregates || has_group_by || has_implicit_collapse {
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
            // PERFORMANCE: Skip alias resolution if no aliases exist (common case in OLTP)
            let resolved_where = if stmt.where_clause.is_some()
                && crate::select::order::select_list_has_aliases(&stmt.select_list)
            {
                stmt.where_clause.as_ref().map(|where_expr| {
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
                })
            } else {
                stmt.where_clause.clone()
            };

            // Pass WHERE, ORDER BY, and LIMIT to execute_from for optimization
            // LIMIT enables early termination when ORDER BY is satisfied by index (#3253)
            // Pass select_list for table elimination optimization (#3556)
            //
            // Don't pass ORDER BY if there's a set operation - it will be handled at the set
            // operation level
            let order_by_hint =
                if stmt.set_operation.is_some() { None } else { stmt.order_by.as_deref() };
            // Don't pass LIMIT hint for set operations - limit must be applied after combining
            // results This prevents early termination from incorrectly limiting the
            // left side of UNION queries
            let limit_val = if stmt.set_operation.is_some() {
                None
            } else {
                stmt.limit
                    .as_ref()
                    .map(|expr| self.eval_limit_offset_expr(expr, "LIMIT"))
                    .transpose()?
            };
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
            self.execute_select_without_from(stmt, cte_results)
        }
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
