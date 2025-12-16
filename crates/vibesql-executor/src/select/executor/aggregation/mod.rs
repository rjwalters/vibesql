//! Aggregation execution methods for SelectExecutor

#[path = "detection.rs"]
mod detection;

mod evaluation;
mod window;

use std::collections::HashMap;

use super::builder::SelectExecutor;
use crate::{
    errors::ExecutorError,
    evaluator::compiled_pivot::PivotAggregateGroup,
    optimizer::{
        collect_columns_from_expr, collect_required_columns, compute_projection_indices,
        optimize_where_clause, project_rows, remap_schema,
    },
    pipeline::ExecutionContext,
    select::{
        cte::CteResult,
        filter::apply_where_filter_combined_auto,
        grouping::{
            expand_group_by_clause, get_base_expressions, group_rows,
            resolve_base_expressions_aliases, resolve_grouping_set_aliases,
            resolve_having_aliases, GroupingContext,
        },
        helpers::{apply_distinct, apply_limit_offset},
    },
};

impl SelectExecutor<'_> {
    /// Execute SELECT with aggregation/GROUP BY
    pub(in crate::select::executor) fn execute_with_aggregation(
        &self,
        stmt: &vibesql_ast::SelectStmt,
        cte_results: &HashMap<String, CteResult>,
    ) -> Result<Vec<vibesql_storage::Row>, ExecutorError> {
        // Note: Aggregate argument validation is done in execute_with_ctes() to catch
        // all execution paths. See issue #4367.

        // Fast path: Simple COUNT(*) without filtering
        // This optimization avoids materializing all rows when we just need the count
        if let Some(table_name) = self.is_simple_count_star(stmt) {
            // If table doesn't exist, fall through to normal path which will produce proper error
            if let Some(table) = self.database.get_table(&table_name) {
                let count = table.row_count();
                return Ok(vec![vibesql_storage::Row::new(vec![
                    vibesql_types::SqlValue::Integer(count as i64),
                ])]);
            }
        }

        // Execute FROM clause (handles JOINs, subqueries, CTEs)
        // Pass WHERE clause for predicate pushdown optimization
        // Note: ORDER BY and LIMIT are applied after aggregation, so we pass None here
        // Pass select_list for table elimination optimization (#3556)
        //
        // IMPORTANT: For aggregate queries with SELECT aliases that might shadow table columns,
        // we need to resolve aliases before passing the WHERE clause for predicate pushdown.
        // Otherwise, an alias like `COUNT(*) AS col1` would cause `col1` in WHERE to be
        // incorrectly interpreted as the aggregate alias instead of the table column.
        // See issue #4XXX for details.
        let from_result = match &stmt.from {
            Some(from_clause) => {
                // Pre-resolve SELECT aliases in WHERE clause before predicate pushdown
                // This uses a lightweight schema built from the FROM clause
                let resolved_where = stmt.where_clause.as_ref().map(|where_expr| {
                    // Build a minimal schema from the FROM clause to resolve aliases
                    // For simple table references, get the schema from the catalog
                    if let Some(schema) = build_early_schema(from_clause, self.database) {
                        crate::select::order::resolve_where_aliases_with_schema(
                            where_expr,
                            &stmt.select_list,
                            &schema,
                        )
                    } else {
                        // Fallback: use legacy resolution without schema
                        crate::select::order::resolve_where_aliases(where_expr, &stmt.select_list)
                    }
                });
                self.execute_from_with_where(
                    from_clause,
                    cte_results,
                    resolved_where.as_ref(),
                    None,
                    None,
                    Some(&stmt.select_list),
                )?
            }
            None => {
                // SELECT without FROM with aggregates - operate over ONE implicit row
                // SQL standard behavior: SELECT without FROM operates over single implicit row
                // - COUNT(*) returns 1 (counting one implicit row)
                // - COUNT(expr), SUM(expr), MAX/MIN/AVG(expr) evaluate expr on that one row
                use crate::{schema::CombinedSchema, select::join::FromResult};

                let empty_schema = vibesql_catalog::TableSchema::new("".to_string(), vec![]);
                let combined_schema = CombinedSchema::from_table("".to_string(), empty_schema);

                // One implicit row with no columns (SQL standard for SELECT without FROM)
                FromResult::from_rows(combined_schema, vec![vibesql_storage::Row::new(vec![])])
            }
        };

        // Validate column references BEFORE processing rows (issue #2654)
        // This ensures column errors are caught even when tables are empty
        // Only validate if we have a FROM clause (skip for SELECT without FROM)
        // Pass procedural context to allow procedure variables in WHERE clause
        // Pass outer_schema for correlated subqueries (#2694)
        if stmt.from.is_some() {
            crate::select::executor::validation::validate_select_columns_with_context(
                &stmt.select_list,
                stmt.where_clause.as_ref(),
                &from_result.schema,
                self.procedural_context,
                self.outer_schema,
            )?;
        }

        // Validate HAVING clause for misuse of aliased aggregates (#4432)
        // e.g., SELECT min(f1) AS m FROM t GROUP BY f1 HAVING max(m) < 10
        // The alias 'm' refers to an aggregate, and using it inside max() is an error.
        // Pass the schema so we can distinguish between actual columns and alias references.
        crate::select::executor::validation::validate_having_aliased_aggregates(
            stmt.having.as_ref(),
            &stmt.select_list,
            &from_result.schema,
        )?;

        // Extract schema for evaluator before moving from_result
        let original_schema = from_result.schema.clone();

        // Create evaluator using consolidated ExecutionContext
        // Handles: outer context (subqueries), procedural context, CTE context
        let cte_ctx = if !cte_results.is_empty() { Some(cte_results) } else { self.cte_context };

        // Apply WHERE clause filter first (using original schema for WHERE evaluation)
        let filtered_rows = {
            let mut ctx = ExecutionContext::new(&original_schema, self.database);
            if let (Some(outer_row), Some(outer_schema)) = (self.outer_row, self.outer_schema) {
                ctx = ctx.with_outer_context(outer_row, outer_schema);
            } else if let Some(proc_ctx) = self.procedural_context {
                ctx = ctx.with_procedural_context(proc_ctx);
            }
            if let Some(cte_ctx) = cte_ctx {
                ctx = ctx.with_cte_context(cte_ctx);
            }
            let evaluator = ctx.create_evaluator();

            // Resolve SELECT aliases in WHERE clause (SQLite extension)
            // This allows queries like: SELECT f1-22 AS x FROM t1 WHERE x > 0
            // NOTE: Table column names take precedence over aliases (SQLite behavior)
            let resolved_where = stmt.where_clause.as_ref().map(|where_expr| {
                crate::select::order::resolve_where_aliases_with_schema(where_expr, &stmt.select_list, &original_schema)
            });

            // Optimize WHERE clause with constant folding and dead code elimination
            let where_optimization = optimize_where_clause(resolved_where.as_ref(), &evaluator)?;

            // Apply WHERE clause to filter joined rows (optimized)
            match where_optimization {
                crate::optimizer::WhereOptimization::AlwaysTrue => {
                    // WHERE TRUE - no filtering needed
                    from_result.into_rows()
                }
                crate::optimizer::WhereOptimization::AlwaysFalse => {
                    // WHERE FALSE - return empty result
                    Vec::new()
                }
                crate::optimizer::WhereOptimization::Optimized(ref expr) => {
                    // Apply optimized WHERE clause (uses parallel if enabled)
                    apply_where_filter_combined_auto(
                        from_result.into_rows(),
                        Some(expr),
                        &evaluator,
                        self,
                    )?
                }
                crate::optimizer::WhereOptimization::Unchanged(where_expr) => {
                    // Apply original WHERE clause (uses parallel if enabled)
                    apply_where_filter_combined_auto(
                        from_result.into_rows(),
                        where_expr.as_ref(),
                        &evaluator,
                        self,
                    )?
                }
            }
        };

        // Extract aggregates from ORDER BY for pre-computation during GROUP BY
        // This allows ORDER BY expressions like "ORDER BY max(n)+0" to work
        // Note: This must be done before column pruning so we can include the columns
        // referenced by ORDER BY aggregates in the required columns.
        let order_by_aggregates = if let Some(order_by) = &stmt.order_by {
            crate::select::order::extract_order_by_aggregates(order_by)
        } else {
            Vec::new()
        };

        // Column pruning optimization (#4355, #4377)
        // After JOIN completes, project only the columns needed for aggregation.
        // This reduces memory and CPU overhead significantly for multi-way JOINs.
        // For example, Q7's 6-way JOIN produces 54 columns but only 14 are needed.
        let (filtered_rows, schema) = {
            // Collect required columns from SELECT, GROUP BY, HAVING, and ORDER BY aggregates
            let mut required_columns = collect_required_columns(
                &stmt.select_list,
                stmt.group_by.as_ref(),
                stmt.having.as_ref(),
            );

            // Also collect columns from ORDER BY aggregates
            for agg_expr in &order_by_aggregates {
                collect_columns_from_expr(agg_expr, &mut required_columns);
            }

            // Check if pruning would help (have required columns and would reduce width)
            if !required_columns.is_empty() {
                if let Some(projection_indices) =
                    compute_projection_indices(&required_columns, &original_schema)
                {
                    // Only apply pruning if we're removing at least some columns
                    if projection_indices.len() < original_schema.total_columns {
                        // Project rows to narrow format
                        let projected_rows = project_rows(filtered_rows, &projection_indices);

                        // Remap schema to match projected columns
                        let projected_schema = remap_schema(&original_schema, &projection_indices);

                        (projected_rows, projected_schema)
                    } else {
                        (filtered_rows, original_schema)
                    }
                } else {
                    (filtered_rows, original_schema)
                }
            } else {
                (filtered_rows, original_schema)
            }
        };

        // Create evaluator for aggregation using the (potentially pruned) schema
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

        // Expand wildcards in SELECT list to explicit column references
        // This allows SELECT * and SELECT table.* to work with GROUP BY/aggregates
        let expanded_select_list =
            self.expand_wildcards_for_aggregation(&stmt.select_list, &schema)?;

        // Detect and set up pivot aggregate optimization (#3136)
        // This detects patterns like: SUM(CASE WHEN col='A' THEN val END), SUM(CASE WHEN col='B'
        // THEN val END)... and batches them into a single pass over the data
        if let Some(pivot_group) = PivotAggregateGroup::try_detect(&expanded_select_list, &schema) {
            self.set_pivot_group(pivot_group);
        }

        // Process GROUP BY clause (handles ROLLUP, CUBE, GROUPING SETS)
        let mut result_rows = Vec::new();

        if let Some(group_by_clause) = &stmt.group_by {
            // Expand GROUP BY clause into list of grouping sets
            let grouping_sets = expand_group_by_clause(group_by_clause);
            let base_expressions = get_base_expressions(group_by_clause);

            // Resolve aliases in base expressions for GROUPING() function support
            let resolved_base_expressions =
                resolve_base_expressions_aliases(&base_expressions, &expanded_select_list)?;

            // For each grouping set, group rows and compute aggregates
            for original_set in grouping_sets {
                // Save original GROUP BY expressions for HAVING alias resolution
                // HAVING should reference GROUP BY columns, not SELECT aliases with same names
                let original_group_by_exprs = original_set.group_by_exprs.clone();

                // Resolve SELECT list aliases in GROUP BY expressions
                // This allows: SELECT n_name AS nation ... GROUP BY nation
                let resolved_set =
                    resolve_grouping_set_aliases(&original_set, &expanded_select_list)?;

                let grouping_context = GroupingContext {
                    base_expressions: resolved_base_expressions.clone(),
                    rolled_up: resolved_set.rolled_up.clone(),
                };

                // Group rows by this grouping set's expressions (now with aliases resolved)
                let groups = if resolved_set.group_by_exprs.is_empty() {
                    // Empty grouping set (grand total) - all rows in one group
                    vec![(Vec::new(), filtered_rows.clone())]
                } else {
                    group_rows(&filtered_rows, &resolved_set.group_by_exprs, &evaluator, self)?
                };

                // Process each group
                for (group_key, group_rows) in groups {
                    // Clear aggregate cache for new group
                    self.clear_aggregate_cache();

                    // Execute pivot aggregates in a single pass (if detected)
                    // This pre-populates the cache with all pivot aggregate results
                    if self.has_pivot_group() {
                        self.execute_pivot_aggregates(&group_rows)?;
                    }

                    // Clear CSE cache for new group to prevent cross-group contamination
                    evaluator.clear_cse_cache();

                    // Check timeout during aggregation
                    self.check_timeout()?;

                    // Compute aggregates for this group
                    let mut aggregate_results = Vec::new();
                    for item in &expanded_select_list {
                        match item {
                            vibesql_ast::SelectItem::Expression { expr, .. } => {
                                let value = self.evaluate_with_aggregates_and_grouping(
                                    expr,
                                    &group_rows,
                                    &group_key,
                                    &evaluator,
                                    &grouping_context,
                                )?;
                                aggregate_results.push(value);
                            }
                            vibesql_ast::SelectItem::Wildcard { .. }
                            | vibesql_ast::SelectItem::QualifiedWildcard { .. } => {
                                return Err(ExecutorError::UnsupportedFeature(
                                    "SELECT * and qualified wildcards not supported with aggregates"
                                        .to_string(),
                                ))
                            }
                        }
                    }

                    // Apply HAVING filter
                    let include_group = if let Some(having_expr) = &stmt.having {
                        // Resolve SELECT list aliases in HAVING (e.g., HAVING y >= 4 where y is count(*))
                        // Pass ORIGINAL GROUP BY expressions so aliases that shadow GROUP BY columns
                        // aren't resolved (HAVING should use GROUP BY columns, not SELECT aliases)
                        let resolved_having = resolve_having_aliases(
                            having_expr,
                            &expanded_select_list,
                            &original_group_by_exprs,
                        );
                        let having_result = self.evaluate_with_aggregates_and_grouping(
                            &resolved_having,
                            &group_rows,
                            &group_key,
                            &evaluator,
                            &grouping_context,
                        )?;
                        self.is_truthy(&having_result)?
                    } else {
                        true
                    };

                    if include_group {
                        // Include GROUP BY values after SELECT values for ORDER BY resolution
                        // ORDER BY can reference GROUP BY columns not in SELECT list
                        // For ROLLUP/CUBE/GROUPING SETS, we need to include ALL base expression
                        // values (with NULLs for rolled-up columns) to ensure consistent row width.
                        let mut row_values = aggregate_results;

                        // Build hidden columns for all base expressions
                        // group_key only contains values for non-rolled-up columns, so we need
                        // to reconstruct the full list using rolled_up flags
                        let mut key_idx = 0;
                        for is_rolled_up in &resolved_set.rolled_up {
                            if *is_rolled_up {
                                // Rolled up column - use NULL
                                row_values.push(vibesql_types::SqlValue::Null);
                            } else {
                                // Present column - use value from group_key
                                if key_idx < group_key.len() {
                                    row_values.push(group_key[key_idx].clone());
                                    key_idx += 1;
                                } else {
                                    // Safety fallback - shouldn't happen if logic is correct
                                    row_values.push(vibesql_types::SqlValue::Null);
                                }
                            }
                        }

                        // Compute ORDER BY aggregates and append them to row values
                        // This allows ORDER BY expressions like "ORDER BY max(n)+0"
                        for order_agg_expr in &order_by_aggregates {
                            let agg_value = self.evaluate_with_aggregates_and_grouping(
                                order_agg_expr,
                                &group_rows,
                                &group_key,
                                &evaluator,
                                &grouping_context,
                            )?;
                            row_values.push(agg_value);
                        }

                        let row = vibesql_storage::Row::new(row_values);

                        // Track memory for aggregation result row
                        let row_memory = std::mem::size_of::<vibesql_storage::Row>()
                            + std::mem::size_of_val(row.values.as_slice());
                        self.track_memory_allocation(row_memory)?;

                        result_rows.push(row);
                    }
                }
            }
        } else {
            // No GROUP BY - treat all rows as one group
            let groups = vec![(Vec::new(), filtered_rows)];
            let grouping_context = GroupingContext::default();

            for (group_key, group_rows) in groups {
                // Clear aggregate cache for new group
                self.clear_aggregate_cache();

                // Execute pivot aggregates in a single pass (if detected)
                // This pre-populates the cache with all pivot aggregate results
                if self.has_pivot_group() {
                    self.execute_pivot_aggregates(&group_rows)?;
                }

                // Clear CSE cache for new group to prevent cross-group contamination
                evaluator.clear_cse_cache();

                // Check timeout during aggregation
                self.check_timeout()?;

                // Compute aggregates for this group
                let mut aggregate_results = Vec::new();
                for item in &expanded_select_list {
                    match item {
                        vibesql_ast::SelectItem::Expression { expr, .. } => {
                            let value = self.evaluate_with_aggregates_and_grouping(
                                expr,
                                &group_rows,
                                &group_key,
                                &evaluator,
                                &grouping_context,
                            )?;
                            aggregate_results.push(value);
                        }
                        vibesql_ast::SelectItem::Wildcard { .. }
                        | vibesql_ast::SelectItem::QualifiedWildcard { .. } => {
                            return Err(ExecutorError::UnsupportedFeature(
                                "SELECT * and qualified wildcards not supported with aggregates"
                                    .to_string(),
                            ))
                        }
                    }
                }

                // Apply HAVING filter
                let include_group = if let Some(having_expr) = &stmt.having {
                    // Resolve SELECT list aliases in HAVING (e.g., HAVING y >= 4 where y is count(*))
                    // No GROUP BY, so no GROUP BY expressions to exclude from alias resolution
                    let resolved_having =
                        resolve_having_aliases(having_expr, &expanded_select_list, &[]);
                    let having_result = self.evaluate_with_aggregates_and_grouping(
                        &resolved_having,
                        &group_rows,
                        &group_key,
                        &evaluator,
                        &grouping_context,
                    )?;
                    self.is_truthy(&having_result)?
                } else {
                    true
                };

                if include_group {
                    let row = vibesql_storage::Row::new(aggregate_results);

                    // Track memory for aggregation result row
                    let row_memory = std::mem::size_of::<vibesql_storage::Row>()
                        + std::mem::size_of_val(row.values.as_slice());
                    self.track_memory_allocation(row_memory)?;

                    result_rows.push(row);
                }
            }
        }

        // Apply window functions that wrap aggregates (e.g., AVG(SUM(x)) OVER (...))
        // This must happen after GROUP BY but before ORDER BY
        let result_rows = if window::has_aggregate_window_functions(&expanded_select_list) {
            window::apply_window_functions_to_aggregates(
                result_rows,
                &expanded_select_list,
                self.database,
            )?
        } else {
            result_rows
        };

        // Get GROUP BY expressions for ORDER BY resolution
        let group_by_exprs: Vec<vibesql_ast::Expression> = if let Some(group_by) = &stmt.group_by {
            get_base_expressions(group_by)
        } else {
            Vec::new()
        };

        // Calculate count of hidden columns (GROUP BY + ORDER BY aggregates)
        let hidden_col_count = group_by_exprs.len() + order_by_aggregates.len();

        // Apply ORDER BY if present
        let result_rows = if let Some(order_by) = &stmt.order_by {
            self.apply_order_by_to_aggregates(
                result_rows,
                stmt,
                order_by,
                &expanded_select_list,
                &group_by_exprs,
                &order_by_aggregates,
            )?
        } else {
            result_rows
        };

        // Strip GROUP BY and ORDER BY aggregate values from result rows
        // (they were only needed for ORDER BY resolution)
        let result_rows: Vec<vibesql_storage::Row> = if hidden_col_count > 0 {
            let select_col_count = expanded_select_list.len();
            result_rows
                .into_iter()
                .map(|row| {
                    let values: Vec<_> = row.values.into_iter().take(select_col_count).collect();
                    vibesql_storage::Row::new(values)
                })
                .collect()
        } else {
            result_rows
        };

        // Apply DISTINCT if specified
        let result_rows = if stmt.distinct { apply_distinct(result_rows) } else { result_rows };

        // SQL Standard: Aggregates without GROUP BY must return exactly ONE row,
        // even if the input is empty. If we have no GROUP BY and result_rows is empty,
        // this is a bug - we should have created at least one group with empty rows.
        // Add a safety check here.
        // IMPORTANT: Don't apply this when HAVING is present - HAVING can legitimately
        // filter out the single group, resulting in 0 rows.
        let result_rows =
            if result_rows.is_empty() && stmt.group_by.is_none() && stmt.having.is_none() {
            // Recompute aggregates for empty input
            // This should not happen if the logic above is correct, but acts as a failsafe
            let grouping_context = GroupingContext::default();
            let mut aggregate_results = Vec::new();
            for item in &expanded_select_list {
                match item {
                    vibesql_ast::SelectItem::Expression { expr, .. } => {
                        // For aggregates on empty input: COUNT returns 0, others return NULL
                        let value = self.evaluate_with_aggregates_and_grouping(
                            expr,
                            &[], // Empty group_rows
                            &[], // Empty group_key
                            &evaluator,
                            &grouping_context,
                        )?;
                        aggregate_results.push(value);
                    }
                    _ => {
                        return Err(ExecutorError::UnsupportedFeature(
                            "Wildcards not supported in aggregates".to_string(),
                        ))
                    }
                }
            }
            vec![vibesql_storage::Row::new(aggregate_results)]
        } else {
            result_rows
        };

        // Don't apply LIMIT/OFFSET if we have a set operation - it will be applied later
        let final_result = if stmt.set_operation.is_some() {
            result_rows
        } else {
            let limit = crate::select::helpers::evaluate_limit(&stmt.limit, self.database)?;
            let offset = crate::select::helpers::evaluate_offset(&stmt.offset, self.database)?;
            apply_limit_offset(result_rows, limit, offset)
        };

        Ok(final_result)
    }

    /// Expand wildcards in SELECT list to explicit column references for aggregation
    ///
    /// This converts `SELECT *` and `SELECT table.*` into explicit column references
    /// so they can be processed in the aggregation path.
    fn expand_wildcards_for_aggregation(
        &self,
        select_list: &[vibesql_ast::SelectItem],
        schema: &crate::schema::CombinedSchema,
    ) -> Result<Vec<vibesql_ast::SelectItem>, ExecutorError> {
        let mut expanded = Vec::new();

        for item in select_list {
            match item {
                vibesql_ast::SelectItem::Wildcard { .. } => {
                    // Expand SELECT * to all columns from all tables in the schema
                    for (table_name, (_start_idx, table_schema)) in &schema.table_schemas {
                        for column in &table_schema.columns {
                            // Create a column reference expression for each column
                            let column_expr = if schema.table_schemas.len() > 1 {
                                // Multiple tables: qualify the column
                                vibesql_ast::Expression::ColumnRef(vibesql_ast::ColumnIdentifier::qualified(&table_name.to_string(), false, &column.name, false))
                            } else {
                                // Single table: no need to qualify
                                vibesql_ast::Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(&column.name, false))
                            };

                            expanded.push(vibesql_ast::SelectItem::Expression {
                                expr: column_expr,
                                alias: None, source_text: None });
                        }
                    }
                }
                vibesql_ast::SelectItem::QualifiedWildcard { qualifier, .. } => {
                    // Expand SELECT table.* to all columns from that specific table
                    // TableKey lookup is case-insensitive
                    let table_result = schema.get_table(qualifier).cloned();

                    if let Some((_start_idx, table_schema)) = table_result {
                        for column in &table_schema.columns {
                            let column_expr = vibesql_ast::Expression::ColumnRef(vibesql_ast::ColumnIdentifier::qualified(qualifier, false, &column.name, false));

                            expanded.push(vibesql_ast::SelectItem::Expression {
                                expr: column_expr,
                                alias: None, source_text: None });
                        }
                    } else {
                        return Err(ExecutorError::TableNotFound(format!(
                            "Table or alias '{}' not found for qualified wildcard",
                            qualifier
                        )));
                    }
                }
                vibesql_ast::SelectItem::Expression { .. } => {
                    // Regular expression - keep as is
                    expanded.push(item.clone());
                }
            }
        }

        Ok(expanded)
    }
}

/// Build an early schema from a FROM clause without fully executing it.
///
/// This is used to resolve SELECT aliases in WHERE clauses before predicate pushdown.
/// The schema only needs column names, not actual data.
///
/// Returns None for complex FROM clauses (subqueries, CTEs, etc.) where we can't
/// easily determine the schema without execution.
/// Build schema from FROM clause without executing it.
/// This enables schema-aware WHERE clause alias resolution before execution.
pub(crate) fn build_early_schema(
    from_clause: &vibesql_ast::FromClause,
    database: &vibesql_storage::Database,
) -> Option<crate::schema::CombinedSchema> {
    match from_clause {
        // Simple table reference - get schema from catalog
        vibesql_ast::FromClause::Table { name, alias, .. } => {
            let table_schema = database.catalog.get_table(name)?;
            let effective_name = alias.as_ref().unwrap_or(name).clone();
            Some(crate::schema::CombinedSchema::from_table(effective_name, table_schema.clone()))
        }

        // JOIN - recursively build schemas and combine
        vibesql_ast::FromClause::Join { left, right, .. } => {
            let left_schema = build_early_schema(left, database)?;
            let right_schema = build_early_schema(right, database)?;

            // Combine schemas
            let mut combined = left_schema;
            for (key, (_, schema)) in right_schema.table_schemas {
                combined.table_schemas.insert(
                    key,
                    (combined.total_columns, schema.clone()),
                );
                combined.total_columns += schema.columns.len();
            }
            Some(combined)
        }

        // Subqueries, VALUES, etc. - can't easily determine schema without execution
        vibesql_ast::FromClause::Subquery { .. } | vibesql_ast::FromClause::Values { .. } => None,
    }
}
