//! Aggregation execution methods for SelectExecutor

#[path = "detection.rs"]
mod detection;

mod evaluation;

use std::collections::HashMap;

use super::builder::SelectExecutor;
use crate::{
    errors::ExecutorError,
    evaluator::CombinedExpressionEvaluator,
    optimizer::optimize_where_clause,
    select::{
        cte::CteResult,
        filter::apply_where_filter_combined_auto,
        grouping::{expand_group_by_clause, get_base_expressions, group_rows, GroupingContext},
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
        // Fast path: Simple COUNT(*) without filtering
        // This optimization avoids materializing all rows when we just need the count
        if let Some(table_name) = self.is_simple_count_star(stmt) {
            // If table doesn't exist, fall through to normal path which will produce proper error
            if let Some(table) = self.database.get_table(&table_name) {
                let count = table.row_count();
                return Ok(vec![vibesql_storage::Row::new(vec![vibesql_types::SqlValue::Integer(count as i64)])]);
            }
        }

        // Execute FROM clause (handles JOINs, subqueries, CTEs)
        // Pass WHERE clause for predicate pushdown optimization
        // Note: ORDER BY is applied after aggregation, so we pass None here
        let from_result = match &stmt.from {
            Some(from_clause) => {
                self.execute_from_with_where(from_clause, cte_results, stmt.where_clause.as_ref(), None)?
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

        // Extract schema for evaluator before moving from_result
        let schema = from_result.schema.clone();

        // Create evaluator with procedural context and CTE context support
        // Priority: 1) outer context (for subqueries) 2) procedural context 3) just database
        // Also pass CTE context if available (from outer query or from current query's CTEs)
        let cte_ctx = if !cte_results.is_empty() {
            Some(cte_results)
        } else {
            self.cte_context
        };

        let evaluator =
            if let (Some(outer_row), Some(outer_schema)) = (self.outer_row, self.outer_schema) {
                if let Some(cte_ctx) = cte_ctx {
                    CombinedExpressionEvaluator::with_database_and_outer_context_and_cte(
                        &schema,
                        self.database,
                        outer_row,
                        outer_schema,
                        cte_ctx,
                    )
                } else {
                    CombinedExpressionEvaluator::with_database_and_outer_context(
                        &schema,
                        self.database,
                        outer_row,
                        outer_schema,
                    )
                }
            } else if let Some(proc_ctx) = self.procedural_context {
                if let Some(cte_ctx) = cte_ctx {
                    CombinedExpressionEvaluator::with_database_and_procedural_context_and_cte(
                        &schema,
                        self.database,
                        proc_ctx,
                        cte_ctx,
                    )
                } else {
                    CombinedExpressionEvaluator::with_database_and_procedural_context(
                        &schema,
                        self.database,
                        proc_ctx,
                    )
                }
            } else if let Some(cte_ctx) = cte_ctx {
                CombinedExpressionEvaluator::with_database_and_cte(&schema, self.database, cte_ctx)
            } else {
                CombinedExpressionEvaluator::with_database(&schema, self.database)
            };

        // Optimize WHERE clause with constant folding and dead code elimination
        let where_optimization = optimize_where_clause(stmt.where_clause.as_ref(), &evaluator)?;

        // Apply WHERE clause to filter joined rows (optimized)
        let filtered_rows = match where_optimization {
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
                apply_where_filter_combined_auto(from_result.into_rows(), Some(expr), &evaluator, self)?
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
        };

        // Expand wildcards in SELECT list to explicit column references
        // This allows SELECT * and SELECT table.* to work with GROUP BY/aggregates
        let expanded_select_list = self.expand_wildcards_for_aggregation(&stmt.select_list, &schema)?;

        // Process GROUP BY clause (handles ROLLUP, CUBE, GROUPING SETS)
        let mut result_rows = Vec::new();

        if let Some(group_by_clause) = &stmt.group_by {
            // Expand GROUP BY clause into list of grouping sets
            let grouping_sets = expand_group_by_clause(group_by_clause);
            let base_expressions = get_base_expressions(group_by_clause);

            // For each grouping set, group rows and compute aggregates
            for resolved_set in grouping_sets {
                let grouping_context = GroupingContext {
                    base_expressions: base_expressions.clone(),
                    rolled_up: resolved_set.rolled_up.clone(),
                };

                // Group rows by this grouping set's expressions
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
                        let having_result = self.evaluate_with_aggregates_and_grouping(
                            having_expr,
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
        } else {
            // No GROUP BY - treat all rows as one group
            let groups = vec![(Vec::new(), filtered_rows)];
            let grouping_context = GroupingContext::default();

            for (group_key, group_rows) in groups {
                // Clear aggregate cache for new group
                self.clear_aggregate_cache();

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
                    let having_result = self.evaluate_with_aggregates_and_grouping(
                        having_expr,
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

        // Apply ORDER BY if present
        let result_rows = if let Some(order_by) = &stmt.order_by {
            self.apply_order_by_to_aggregates(result_rows, stmt, order_by, &expanded_select_list)?
        } else {
            result_rows
        };

        // Apply DISTINCT if specified
        let result_rows = if stmt.distinct { apply_distinct(result_rows) } else { result_rows };

        // SQL Standard: Aggregates without GROUP BY must return exactly ONE row,
        // even if the input is empty. If we have no GROUP BY and result_rows is empty,
        // this is a bug - we should have created at least one group with empty rows.
        // Add a safety check here.
        let result_rows = if result_rows.is_empty() && stmt.group_by.is_none() {
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
                            &[],  // Empty group_rows
                            &[],  // Empty group_key
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
            apply_limit_offset(result_rows, stmt.limit, stmt.offset)
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
                            let column_expr = vibesql_ast::Expression::ColumnRef {
                                table: if schema.table_schemas.len() > 1 {
                                    // Multiple tables: qualify the column
                                    Some(table_name.clone())
                                } else {
                                    // Single table: no need to qualify
                                    None
                                },
                                column: column.name.clone(),
                            };

                            expanded.push(vibesql_ast::SelectItem::Expression {
                                expr: column_expr,
                                alias: None,
                            });
                        }
                    }
                }
                vibesql_ast::SelectItem::QualifiedWildcard { qualifier, .. } => {
                    // Expand SELECT table.* to all columns from that specific table
                    // Try exact match first
                    let table_result = schema.table_schemas.get(qualifier).cloned().or_else(|| {
                        // Fall back to case-insensitive lookup
                        let qualifier_lower = qualifier.to_lowercase();
                        schema
                            .table_schemas
                            .iter()
                            .find(|(key, _)| key.to_lowercase() == qualifier_lower)
                            .map(|(_key, value)| value.clone())
                    });

                    if let Some((_start_idx, table_schema)) = table_result {
                        for column in &table_schema.columns {
                            let column_expr = vibesql_ast::Expression::ColumnRef {
                                table: Some(qualifier.clone()),
                                column: column.name.clone(),
                            };

                            expanded.push(vibesql_ast::SelectItem::Expression {
                                expr: column_expr,
                                alias: None,
                            });
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
