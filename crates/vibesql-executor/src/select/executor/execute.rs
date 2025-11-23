//! Main execution methods for SelectExecutor

use std::collections::HashMap;

use super::builder::SelectExecutor;
use crate::{
    errors::ExecutorError,
    select::{
        cte::{execute_ctes, CteResult},
        helpers::apply_limit_offset,
        join::FromResult,
        set_operations::apply_set_operation,
        SelectResult,
    },
};

impl SelectExecutor<'_> {
    /// Execute a SELECT statement
    pub fn execute(&self, stmt: &vibesql_ast::SelectStmt) -> Result<Vec<vibesql_storage::Row>, ExecutorError> {
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

        #[cfg(feature = "profile-q6")]
        let setup_time = execute_start.elapsed();

        // Apply subquery rewriting optimizations (Phase 2 of IN subquery optimization)
        // - Rewrites correlated IN → EXISTS with LIMIT 1 for early termination
        // - Adds DISTINCT to uncorrelated IN subqueries to reduce duplicate processing
        // This works in conjunction with Phase 1 (HashSet optimization, #2136)
        #[cfg(feature = "profile-q6")]
        let optimizer_start = std::time::Instant::now();

        let optimized_stmt = crate::optimizer::rewrite_subquery_optimizations(stmt);

        #[cfg(feature = "profile-q6")]
        let optimizer_time = optimizer_start.elapsed();

        // Transform decorrelated IN/EXISTS subqueries to semi/anti-joins (#2424)
        // This enables hash-based join execution instead of row-by-row subquery evaluation
        // Converts WHERE clauses like "WHERE x IN (SELECT y FROM t)" to "SEMI JOIN t ON x = y"
        let optimized_stmt = crate::optimizer::transform_subqueries_to_joins(&optimized_stmt);

        // Execute CTEs if present and merge with outer query's CTE context
        let mut cte_results = if let Some(with_clause) = &optimized_stmt.with_clause {
            // This query has its own CTEs - execute them
            execute_ctes(with_clause, |query, cte_ctx| self.execute_with_ctes(query, cte_ctx))?
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
        let pre_execute_time = execute_start.elapsed();

        // Execute the main query with CTE context
        let result = self.execute_with_ctes(&optimized_stmt, &cte_results)?;

        #[cfg(feature = "profile-q6")]
        {
            let total_execute = execute_start.elapsed();
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

    /// Execute a SELECT statement and return both columns and rows
    pub fn execute_with_columns(
        &self,
        stmt: &vibesql_ast::SelectStmt,
    ) -> Result<SelectResult, ExecutorError> {
        // First, get the FROM result to access the schema
        let from_result = if let Some(from_clause) = &stmt.from {
            let cte_results = if let Some(with_clause) = &stmt.with_clause {
                execute_ctes(with_clause, |query, cte_ctx| self.execute_with_ctes(query, cte_ctx))?
            } else {
                HashMap::new()
            };
            // Pass WHERE and ORDER BY for join reordering optimization
            // This is critical for GROUP BY queries to avoid CROSS JOINs
            Some(self.execute_from_with_where(
                from_clause,
                &cte_results,
                stmt.where_clause.as_ref(),
                stmt.order_by.as_ref().map(|v| &**v),
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
    pub(super) fn execute_with_ctes(
        &self,
        stmt: &vibesql_ast::SelectStmt,
        cte_results: &HashMap<String, CteResult>,
    ) -> Result<Vec<vibesql_storage::Row>, ExecutorError> {
        #[cfg(feature = "profile-q6")]
        let execute_ctes_start = std::time::Instant::now();

        // Try columnar execution path FIRST for compatible queries
        // Phase 5: SIMD-accelerated columnar execution provides 6-10x speedup
        // This runs before monomorphic to allow columnar path to handle aggregate queries
        #[cfg(feature = "profile-q6")]
        let columnar_check_start = std::time::Instant::now();

        if let Some(result) = self.try_columnar_execution(stmt, cte_results)? {
            #[cfg(feature = "profile-q6")]
            {
                let total_execute_ctes = execute_ctes_start.elapsed();
                let columnar_check_time = columnar_check_start.elapsed();
            }
            return Ok(result);
        }

        // Try monomorphic execution path for known query patterns (TEMPORARILY DISABLED)
        // NOTE: Monomorphic execution currently has issues with complex aggregate expressions
        // For Phase 5, we're prioritizing columnar execution over monomorphic
        // TODO: Re-enable monomorphic execution after fixing complex aggregate handling
        let mono_result: Option<Vec<vibesql_storage::Row>> = None; // Disabled

        #[cfg(feature = "profile-q6")]
        let mono_check_start = std::time::Instant::now();

        if let Some(result) = mono_result {
            #[cfg(feature = "profile-q6")]
            {
                let total_execute_ctes = execute_ctes_start.elapsed();
                let mono_check_time = mono_check_start.elapsed();
            }
            return Ok(result);
        }

        // Execute the left-hand side query
        let has_aggregates = self.has_aggregates(&stmt.select_list) || stmt.having.is_some();
        let has_group_by = stmt.group_by.is_some();


        let mut results = if has_aggregates || has_group_by {
            self.execute_with_aggregation(stmt, cte_results)?
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

            // Pass WHERE and ORDER BY to execute_from for optimization
            let from_result =
                self.execute_from_with_where(from_clause, cte_results, stmt.where_clause.as_ref(), stmt.order_by.as_deref())?;
            self.execute_without_aggregation(stmt, from_result, cte_results)?
        } else {
            // SELECT without FROM - evaluate expressions as a single row
            self.execute_select_without_from(stmt)?
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
        let has_aggregates = self.has_aggregates(&right_stmt.select_list) || right_stmt.having.is_some();
        let has_group_by = right_stmt.group_by.is_some();

        let right_results = if has_aggregates || has_group_by {
            self.execute_with_aggregation(right_stmt, cte_results)?
        } else if let Some(from_clause) = &right_stmt.from {
            let from_result =
                self.execute_from_with_where(from_clause, cte_results, right_stmt.where_clause.as_ref(), right_stmt.order_by.as_deref())?;
            self.execute_without_aggregation(right_stmt, from_result, cte_results)?
        } else {
            self.execute_select_without_from(right_stmt)?
        };

        // Apply the current operation
        left_results = apply_set_operation(left_results, right_results, set_op)?;

        // If the right side has more set operations, continue processing them
        // This creates the left-to-right evaluation: ((A op B) op C) op D
        if let Some(next_set_op) = &right_stmt.set_operation {
            left_results = self.execute_set_operations(left_results, next_set_op, cte_results)?;
        }

        Ok(left_results)
    }

    /// Execute a FROM clause (table or join) and return combined schema and rows
    pub(super) fn execute_from(
        &self,
        from: &vibesql_ast::FromClause,
        cte_results: &HashMap<String, CteResult>,
    ) -> Result<FromResult, ExecutorError> {
        use crate::select::scan::execute_from_clause;
        execute_from_clause(from, cte_results, self.database, None, None, self.outer_row, self.outer_schema, |query| self.execute_with_columns(query))
    }

    /// Execute a FROM clause with WHERE and ORDER BY for optimization
    pub(super) fn execute_from_with_where(
        &self,
        from: &vibesql_ast::FromClause,
        cte_results: &HashMap<String, CteResult>,
        where_clause: Option<&vibesql_ast::Expression>,
        order_by: Option<&[vibesql_ast::OrderByItem]>,
    ) -> Result<FromResult, ExecutorError> {
        use crate::select::scan::execute_from_clause;
        let from_result = execute_from_clause(from, cte_results, self.database, where_clause, order_by, self.outer_row, self.outer_schema, |query| {
            self.execute_with_columns(query)
        })?;

        // NOTE: We DON'T merge outer schema with from_result.schema here because:
        // 1. from_result.rows only contain values from inner tables
        // 2. Outer columns are resolved via the evaluator's outer_row/outer_schema
        // 3. Merging would create schema/row mismatch (schema has outer cols, rows don't)

        Ok(from_result)
    }

    /// Try to execute using a monomorphic (type-specialized) plan
    ///
    /// Returns Some(rows) if a monomorphic plan was found and executed successfully.
    /// Returns None if no matching pattern was found (fall back to regular execution).
    ///
    /// Monomorphic plans eliminate SqlValue enum overhead by using type-specific
    /// accessors for ~2.4x performance improvement on known query patterns.
    fn try_monomorphic_execution(
        &self,
        stmt: &vibesql_ast::SelectStmt,
        cte_results: &HashMap<String, CteResult>,
    ) -> Result<Option<Vec<vibesql_storage::Row>>, ExecutorError> {
        use crate::select::monomorphic::try_create_monomorphic_plan;

        // Only try monomorphic path for queries without CTEs or set operations
        // Supports both single-table queries (Q6) and multi-table joins (Q3)
        if !cte_results.is_empty() || stmt.set_operation.is_some() {
            return Ok(None);
        }

        // Check if we have a FROM clause
        let from_clause = match &stmt.from {
            Some(from) => from,
            None => return Ok(None),
        };

        // Execute FROM clause (handles single tables, joins, etc.)
        // For Q6: returns raw lineitem rows
        // For Q3: returns joined rows (customer + orders + lineitem)
        // Pass WHERE clause for join reordering optimization (critical for Q3)
        #[cfg(feature = "profile-q6")]
        let load_start = std::time::Instant::now();

        let mut from_result = self.execute_from_with_where(
            from_clause,
            cte_results,
            stmt.where_clause.as_ref(),
            None, // ORDER BY applied after aggregation
        )?;

        #[cfg(feature = "profile-q6")]
        {
            let load_time = load_start.elapsed();
            eprintln!("[Q6 PROFILE] Load time: {:?}, rows: {}, per-row: {:?}",
                load_time, from_result.rows().len(), load_time / from_result.rows().len() as u32);
        }

        // Try to create a monomorphic plan using AST-based pattern matching
        let plan = match try_create_monomorphic_plan(stmt, &from_result.schema) {
            Some(p) => p,
            None => return Ok(None),
        };

        // Execute the monomorphic plan using streaming if data is still an iterator
        // This avoids materializing rows that will be filtered out, providing 2-3x speedup
        // for queries with selective filters (e.g., TPC-H Q6)
        let result_rows = match &mut from_result.data {
            crate::select::join::FromData::Iterator(iter) => {
                // Take ownership of the iterator and stream without materializing
                let owned_iter = std::mem::replace(
                    iter,
                    crate::select::from_iterator::FromIterator::from_vec(vec![]),
                );
                plan.execute_stream(Box::new(owned_iter))?
            }
            crate::select::join::FromData::Materialized(_rows) => {
                // Data is already materialized (e.g., from JOIN), use standard path
                plan.execute(from_result.rows())?
            }
        };

        Ok(Some(result_rows))
    }
}
