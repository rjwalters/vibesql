//! Multi-table JOIN execution using columnar hash join (Phase 4)
//!
//! This module implements vectorized JOIN execution for multi-table queries,
//! providing 3-5x improvement for JOIN-heavy queries like TPC-H Q3, Q5, Q7-Q10, Q19.

use std::collections::HashMap;

use super::join_helpers::{
    build_combined_schema, extract_equijoin_conditions, extract_join_conditions,
    extract_non_join_predicates, flatten_join_tree_simple, has_cross_join_with_on_condition,
    is_column_in_table, is_column_in_tables, is_inner_or_cross_join_only,
    resolve_join_column_indices, EquiJoinCondition,
};
use crate::{
    errors::ExecutorError,
    evaluator::CombinedExpressionEvaluator,
    optimizer::optimize_expression,
    schema::CombinedSchema,
    select::{
        columnar, cte::CteResult, executor::builder::SelectExecutor, helpers::apply_distinct,
        join::hash_join::columnar as columnar_join, projection::project_row_combined,
    },
};
use vibesql_ast::{FromClause, SelectItem};

impl SelectExecutor<'_> {
    /// Try to execute a multi-table JOIN query using columnar hash join (Phase 4)
    ///
    /// This method attempts to execute queries with JOINs using the vectorized
    /// columnar hash join implementation for improved performance.
    ///
    /// Returns Some(rows) if columnar join execution succeeded.
    /// Returns None if the query should fall back to row-based execution.
    ///
    /// # Supported Query Patterns
    ///
    /// - Multi-table INNER JOINs (explicit `JOIN ... ON` syntax)
    /// - Implicit joins via comma-separated tables (`FROM a, b WHERE a.x = b.y`)
    /// - Equi-join conditions (col1 = col2) in ON clause or WHERE clause
    /// - Simple WHERE predicates
    /// - GROUP BY with aggregates (SUM, COUNT, AVG, MIN, MAX)
    ///
    /// # Implicit Join Syntax (Issue #3132)
    ///
    /// Comma-separated tables are parsed as CROSS JOINs. When combined with
    /// equijoin conditions in the WHERE clause, they are semantically equivalent
    /// to INNER JOINs. This enables columnar optimization for queries like:
    ///
    /// ```sql
    /// SELECT * FROM lineitem, part WHERE p_partkey = l_partkey
    /// ```
    ///
    /// # Performance
    ///
    /// - 3-5x improvement for JOIN-heavy queries
    /// - Targets TPC-H Q3, Q5, Q7-Q10, Q19 style queries
    pub(in crate::select::executor) fn try_columnar_join_execution(
        &self,
        stmt: &vibesql_ast::SelectStmt,
        cte_results: &HashMap<String, CteResult>,
    ) -> Result<Option<Vec<vibesql_storage::Row>>, ExecutorError> {
        // Disable via env var for debugging
        if std::env::var("VIBESQL_DISABLE_COLUMNAR_JOIN").is_ok() {
            log::debug!("Columnar join: disabled via VIBESQL_DISABLE_COLUMNAR_JOIN");
            return Ok(None);
        }

        // Only handle queries without CTEs, set operations, or DISTINCT
        // DISTINCT requires deduplication which the columnar path doesn't support yet
        if !cte_results.is_empty() || stmt.set_operation.is_some() {
            log::debug!("Columnar join: skipping - has CTEs or set operations");
            return Ok(None);
        }

        // DISTINCT queries need special handling - fall back to row-oriented
        if stmt.distinct {
            log::debug!("Columnar join: skipping - DISTINCT not supported");
            return Ok(None);
        }

        // LIMIT/OFFSET queries need special handling - fall back to row-oriented
        // TODO: Add LIMIT support to columnar join path
        if stmt.limit.is_some() || stmt.offset.is_some() {
            log::debug!("Columnar join: skipping - LIMIT/OFFSET not supported");
            return Ok(None);
        }

        // Must have a FROM clause with JOINs
        let from_clause = match &stmt.from {
            Some(from) => from,
            None => return Ok(None),
        };

        // Must be a JOIN clause - this includes:
        // - Explicit JOINs: FROM a JOIN b ON a.x = b.y
        // - Implicit joins (comma syntax): FROM a, b (parsed as CROSS JOIN)
        if !matches!(from_clause, FromClause::Join { .. }) {
            return Ok(None);
        }

        // Only handle INNER and CROSS joins - LEFT, RIGHT, FULL need special handling
        // CROSS joins are included because comma-separated tables (FROM a, b) parse as CROSS JOIN
        if !is_inner_or_cross_join_only(from_clause) {
            log::debug!("Columnar join: only INNER/CROSS joins are supported, falling back");
            return Ok(None);
        }

        // CROSS JOIN with ON condition is semantically invalid SQL
        // Fall back to regular execution to produce proper error message
        if has_cross_join_with_on_condition(from_clause) {
            log::debug!("Columnar join: CROSS JOIN with ON condition detected, falling back");
            return Ok(None);
        }

        // Flatten the join tree to get all tables
        let mut table_refs = Vec::new();
        flatten_join_tree_simple(from_clause, &mut table_refs);

        // We need at least 2 tables for a join
        if table_refs.len() < 2 {
            log::debug!("Columnar join: need at least 2 tables");
            return Ok(None);
        }

        // Don't handle subqueries in the columnar join path
        if table_refs.iter().any(|(_, _, is_subquery)| *is_subquery) {
            log::debug!("Columnar join: skipping - contains subqueries");
            return Ok(None);
        }

        log::info!(
            "Columnar join: attempting {} table join ({:?})",
            table_refs.len(),
            table_refs.iter().map(|(name, _, _)| name.as_str()).collect::<Vec<_>>()
        );

        // Load all tables as ColumnarBatch
        let mut batches: Vec<(
            String,
            Option<String>,
            columnar::ColumnarBatch,
            vibesql_catalog::TableSchema,
        )> = Vec::new();

        for (table_name, alias, _is_subquery) in &table_refs {
            let table = match self.database.get_table(table_name) {
                Some(t) => t,
                None => {
                    log::debug!("Columnar join: table '{}' not found", table_name);
                    return Ok(None);
                }
            };

            let columnar_arc = match self.database.get_columnar(table_name) {
                Ok(Some(ct)) => ct,
                Ok(None) | Err(_) => {
                    log::debug!("Columnar join: failed to get columnar data for '{}'", table_name);
                    return Ok(None);
                }
            };

            let batch = columnar::ColumnarBatch::from_storage_columnar(&columnar_arc)?;
            batches.push((table_name.clone(), alias.clone(), batch, table.schema.clone()));
        }

        // Extract equi-join conditions from WHERE clause and ON conditions
        let mut join_conditions = Vec::new();
        extract_join_conditions(from_clause, &mut join_conditions);

        if let Some(ref where_clause) = stmt.where_clause {
            extract_equijoin_conditions(where_clause, &mut join_conditions);
        }

        log::debug!(
            "Columnar join: found {} join conditions for {} tables",
            join_conditions.len(),
            table_refs.len()
        );

        // For N tables, we need exactly N-1 equijoin conditions for a simple connected join graph.
        // If there are more conditions, some are filters that need special handling.
        // Fall back to row-based execution in that case for correctness.
        let min_join_conditions = table_refs.len() - 1;
        if join_conditions.len() > min_join_conditions {
            log::debug!(
                "Columnar join: {} join conditions exceeds minimum {} for {} tables, falling back",
                join_conditions.len(),
                min_join_conditions,
                table_refs.len()
            );
            return Ok(None);
        }

        // Build combined schema for all tables
        let combined_schema = build_combined_schema(&batches);

        // Execute joins in sequence, building up the result batch
        let joined_batch =
            match self.execute_columnar_join_chain(&batches, &join_conditions, &combined_schema) {
                Ok(Some(batch)) => batch,
                Ok(None) => {
                    log::debug!("Columnar join: join chain execution returned None");
                    return Ok(None);
                }
                Err(e) => {
                    log::debug!("Columnar join: join chain execution failed: {:?}", e);
                    return Ok(None);
                }
            };

        // Apply remaining WHERE predicates (non-join conditions) using SIMD filtering
        // First, constant-fold the WHERE clause to handle expressions like `BETWEEN 1 AND 1+2`
        // which need to become `BETWEEN 1 AND 3` for the predicate extractor to recognize them
        let folded_where = if let Some(where_expr) = &stmt.where_clause {
            // Create evaluator for constant folding
            // SAFETY: combined_schema lives for the duration of this function call
            let schema_ref: &'static CombinedSchema =
                unsafe { std::mem::transmute(&combined_schema) };
            let evaluator = CombinedExpressionEvaluator::with_database(schema_ref, self.database);

            match optimize_expression(where_expr, &evaluator) {
                Ok(folded) => Some(folded),
                Err(_) => Some(where_expr.clone()), // Fall back to original if folding fails
            }
        } else {
            None
        };

        let predicates = folded_where
            .as_ref()
            .and_then(|where_expr| extract_non_join_predicates(where_expr, &combined_schema))
            .unwrap_or_default();

        let filtered_batch = if predicates.is_empty() {
            joined_batch
        } else {
            columnar::simd_filter_batch(&joined_batch, &predicates)?
        };

        let joined_row_count = filtered_batch.row_count();
        log::info!("Columnar join: {} rows after join and filter", joined_row_count);

        // Check for GROUP BY
        let has_group_by = stmt.group_by.is_some();

        if has_group_by {
            // Execute GROUP BY aggregation
            self.execute_columnar_join_group_by(stmt, &filtered_batch, &combined_schema)
        } else {
            // No GROUP BY - convert to rows and apply projection

            // Check if we need projection (i.e., not just SELECT *)
            let is_select_star = stmt.select_list.iter().all(|item| {
                matches!(item, SelectItem::Wildcard { .. } | SelectItem::QualifiedWildcard { .. })
            });

            if is_select_star {
                // SELECT * - apply columnar deduplication if DISTINCT, then convert to rows
                let final_batch = if stmt.distinct {
                    log::debug!(
                        "Columnar join: applying DISTINCT deduplication to {} rows",
                        filtered_batch.row_count()
                    );
                    filtered_batch.deduplicate()?
                } else {
                    filtered_batch
                };
                let rows = final_batch.to_rows()?;
                Ok(Some(rows))
            } else {
                // Check if SELECT list contains aggregate functions
                // Aggregates without GROUP BY need special handling (fall back to row-oriented)
                let has_aggregates =
                    crate::optimizer::aggregate_analysis::AggregateAnalysis::analyze(stmt)
                        .has_aggregates;
                if has_aggregates {
                    log::debug!(
                        "Columnar join: aggregates without GROUP BY not supported, falling back"
                    );
                    return Ok(None);
                }

                // Apply column projection to each row
                log::debug!(
                    "Columnar join: applying projection to {} rows",
                    filtered_batch.row_count()
                );

                // Create evaluator for projection
                // SAFETY: combined_schema lives for the duration of this function call
                let schema_ref: &'static CombinedSchema =
                    unsafe { std::mem::transmute(&combined_schema) };
                let evaluator =
                    CombinedExpressionEvaluator::with_database(schema_ref, self.database);
                let buffer_pool = self.database.query_buffer_pool();

                let rows = filtered_batch.to_rows()?;
                let mut projected_rows = Vec::with_capacity(rows.len());
                for row in &rows {
                    let projected = project_row_combined(
                        row,
                        &stmt.select_list,
                        &evaluator,
                        &combined_schema,
                        &None, // No window functions in columnar join path
                        buffer_pool,
                    )?;
                    projected_rows.push(projected);
                }

                // Apply DISTINCT after projection if requested
                let final_rows =
                    if stmt.distinct { apply_distinct(projected_rows) } else { projected_rows };

                Ok(Some(final_rows))
            }
        }
    }

    /// Execute a chain of hash joins on columnar batches
    pub(super) fn execute_columnar_join_chain(
        &self,
        batches: &[(
            String,
            Option<String>,
            columnar::ColumnarBatch,
            vibesql_catalog::TableSchema,
        )],
        join_conditions: &[EquiJoinCondition],
        combined_schema: &CombinedSchema,
    ) -> Result<Option<columnar::ColumnarBatch>, ExecutorError> {
        if batches.is_empty() {
            return Ok(None);
        }

        if batches.len() == 1 {
            return Ok(Some(batches[0].2.clone()));
        }

        // Start with the first table
        let mut current_batch = batches[0].2.clone();

        // Track which tables have been joined
        let mut joined_tables: Vec<&str> = vec![batches[0].1.as_deref().unwrap_or(&batches[0].0)];

        // Join subsequent tables
        for (table_name, alias, batch, schema) in batches.iter().skip(1) {
            let table_ref = alias.as_deref().unwrap_or(table_name.as_str());

            // Find a join condition that connects this table to already-joined tables
            let join_cond = join_conditions.iter().find(|cond| {
                let left_in_joined = joined_tables.iter().any(|t| {
                    cond.left_table.as_deref() == Some(*t)
                        || (cond.left_table.is_none()
                            && is_column_in_tables(
                                &cond.left_column,
                                &joined_tables,
                                combined_schema,
                            ))
                });
                let right_is_current = cond.right_table.as_deref() == Some(table_ref)
                    || (cond.right_table.is_none()
                        && is_column_in_table(&cond.right_column, table_ref, combined_schema));
                let right_in_joined = joined_tables.iter().any(|t| {
                    cond.right_table.as_deref() == Some(*t)
                        || (cond.right_table.is_none()
                            && is_column_in_tables(
                                &cond.right_column,
                                &joined_tables,
                                combined_schema,
                            ))
                });
                let left_is_current = cond.left_table.as_deref() == Some(table_ref)
                    || (cond.left_table.is_none()
                        && is_column_in_table(&cond.left_column, table_ref, combined_schema));

                (left_in_joined && right_is_current) || (right_in_joined && left_is_current)
            });

            let join_cond = match join_cond {
                Some(cond) => cond,
                None => {
                    log::debug!(
                        "Columnar join: no join condition found connecting '{}' to {:?}",
                        table_ref,
                        joined_tables
                    );
                    return Ok(None);
                }
            };

            // Determine which side of the condition refers to the current batch vs new table
            let (left_col_idx, right_col_idx) = resolve_join_column_indices(
                join_cond,
                &joined_tables,
                table_ref,
                schema,
                combined_schema,
            )?;

            log::debug!(
                "Columnar join: joining '{}' (col {}) with '{}' (col {})",
                joined_tables.join(", "),
                left_col_idx,
                table_ref,
                right_col_idx
            );

            // Execute the hash join
            current_batch = columnar_join::columnar_hash_join_inner(
                &current_batch,
                batch,
                left_col_idx,
                right_col_idx,
            )?;

            // Update tracking
            joined_tables.push(table_ref);
        }

        Ok(Some(current_batch))
    }
}
