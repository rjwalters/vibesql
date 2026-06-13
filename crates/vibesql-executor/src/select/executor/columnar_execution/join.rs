//! Multi-table JOIN execution using columnar hash join (Phase 4)
//!
//! This module implements vectorized JOIN execution for multi-table queries,
//! providing 3-5x improvement for JOIN-heavy queries like TPC-H Q3, Q5, Q7-Q10, Q19.

use std::collections::HashMap;
use std::sync::OnceLock;

use vibesql_ast::{FromClause, JoinType, SelectItem};

use super::join_helpers::{
    build_combined_schema, expression_has_is_null, extract_equijoin_conditions,
    extract_join_conditions, extract_non_join_predicates, flatten_join_tree_simple,
    flatten_join_tree_with_types, has_cross_join_with_on_condition, has_outer_join,
    is_columnar_supported_join, is_column_in_table, is_column_in_tables,
    resolve_join_column_indices, EquiJoinCondition,
};
use crate::{
    errors::ExecutorError,
    evaluator::CombinedExpressionEvaluator,
    optimizer::optimize_expression,
    schema::CombinedSchema,
    select::{
        columnar, cte::CteResult, executor::builder::SelectExecutor, helpers::apply_distinct,
        join::hash_join::columnar as columnar_join,
        order::{apply_order_by, RowWithSortKeys},
        projection::project_row_combined,
    },
};

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
        // Disable via env var for debugging (cached to avoid per-query global lock)
        if is_columnar_join_disabled() {
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

        // ROWID pseudo-column references require row-oriented execution (#4370)
        // Columnar batches don't track per-row ROWIDs, so we fall back when ROWID is referenced
        if select_list_has_rowid(&stmt.select_list) {
            log::debug!("Columnar join: skipping - ROWID pseudo-column not supported");
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

        // Only handle supported join types (INNER, CROSS, LEFT OUTER, RIGHT OUTER)
        // FULL OUTER, SEMI, and ANTI joins fall back to row-oriented execution
        // CROSS joins are included because comma-separated tables (FROM a, b) parse as CROSS JOIN
        if !is_columnar_supported_join(from_clause) {
            log::debug!("Columnar join: unsupported join type (FULL/SEMI/ANTI), falling back");
            return Ok(None);
        }

        // CROSS JOIN with ON condition is semantically invalid SQL
        // Fall back to regular execution to produce proper error message
        if has_cross_join_with_on_condition(from_clause) {
            log::debug!("Columnar join: CROSS JOIN with ON condition detected, falling back");
            return Ok(None);
        }

        // Outer joins with IS NULL / IS NOT NULL in WHERE clause need row-oriented execution.
        // The columnar SIMD filter does not support null-testing predicates (IS NULL / IS NOT NULL),
        // so they are silently dropped during predicate extraction. For outer joins this causes
        // incorrect results — e.g., anti-join pattern `LEFT JOIN ... WHERE t2.col IS NULL` would
        // return all rows instead of only unmatched rows.
        if has_outer_join(from_clause) {
            if let Some(ref where_clause) = stmt.where_clause {
                if expression_has_is_null(where_clause) {
                    log::debug!(
                        "Columnar join: outer join with IS NULL/IS NOT NULL in WHERE, falling back"
                    );
                    return Ok(None);
                }
            }
        }

        // Flatten the join tree to get all tables with their join types
        let mut table_refs_with_types = Vec::new();
        flatten_join_tree_with_types(from_clause, &mut table_refs_with_types);

        // Also flatten without types for backward compatibility with condition extraction
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

        // ── Early bail-out checks (#5047) ──────────────────────────────────
        // These checks detect queries the columnar join path cannot fully
        // handle BEFORE we do any expensive work (table loading, hash joins,
        // SIMD filtering). Without these, the columnar path would do all
        // that work and then discover it can't handle the GROUP BY / aggregate
        // / subquery predicate, return Ok(None), and the row-oriented path
        // would redo everything from scratch.

        // 1. WHERE clause subquery predicates (Q18: IN (SELECT ...))
        //    Must bail before join, not after.
        if let Some(ref where_clause) = stmt.where_clause {
            if contains_unsupported_predicates(where_clause) {
                log::debug!(
                    "Columnar join: WHERE contains unsupported predicates (subquery/EXISTS), skipping early"
                );
                return Ok(None);
            }
        }

        // 2. GROUP BY with non-column expressions (Q7: GROUP BY strftime(...))
        //    The columnar join GROUP BY path only supports simple ColumnRef keys.
        if let Some(ref group_by_clause) = stmt.group_by {
            match group_by_clause.as_simple() {
                Some(exprs) => {
                    if exprs.iter().any(|e| !matches!(e, vibesql_ast::Expression::ColumnRef(_))) {
                        log::debug!(
                            "Columnar join: GROUP BY has non-column expressions, skipping early"
                        );
                        return Ok(None);
                    }
                }
                None => {
                    // ROLLUP/CUBE/GROUPING SETS not supported
                    log::debug!(
                        "Columnar join: ROLLUP/CUBE/GROUPING SETS not supported, skipping early"
                    );
                    return Ok(None);
                }
            }
        }

        // 3. Aggregate function args the columnar join path can't handle
        //    (Q12: SUM(CASE WHEN ...), Q10: SUM(expr * expr) => Expression aggregate)
        //    The join GROUP BY path rejects AggregateSource::Expression entirely,
        //    so any aggregate arg that isn't a simple ColumnRef or Wildcard will
        //    cause a late bail-out. Check this up front.
        if stmt.group_by.is_some() {
            if has_unsupported_join_aggregates(&stmt.select_list) {
                log::debug!(
                    "Columnar join: SELECT has aggregate args unsupported in join path, skipping early"
                );
                return Ok(None);
            }
        }

        // ── End early bail-out checks ──────────────────────────────────────

        // Extract join types for the chain (first table has no join type)
        let join_types: Vec<Option<JoinType>> =
            table_refs_with_types.iter().map(|(_, jt)| jt.clone()).collect();

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
            match self.execute_columnar_join_chain(&batches, &join_conditions, &combined_schema, &join_types) {
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

        // Check if WHERE clause contains expressions that can't be handled by SIMD filtering
        // (e.g., scalar subqueries). If so, fall back to row-oriented execution (#4501).
        if let Some(ref where_expr) = folded_where {
            if contains_unsupported_predicates(where_expr) {
                log::debug!(
                    "Columnar join: WHERE clause contains unsupported predicates (e.g., scalar subquery), falling back"
                );
                return Ok(None);
            }
        }

        let predicates = folded_where
            .as_ref()
            .and_then(|where_expr| extract_non_join_predicates(where_expr, &combined_schema, self.database.case_sensitive_like()))
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
            let result = self.execute_columnar_join_group_by(stmt, &filtered_batch, &combined_schema);

            // Apply ORDER BY to GROUP BY results if needed (#5033)
            if let Ok(Some(rows)) = result {
                if let Some(order_by) = &stmt.order_by {
                    let sorted = self.apply_columnar_join_order_by(rows, order_by, &stmt.select_list, &combined_schema)?;
                    Ok(Some(sorted))
                } else {
                    Ok(Some(rows))
                }
            } else {
                result
            }
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

                // Apply ORDER BY if present (#5033)
                let final_rows = if let Some(order_by) = &stmt.order_by {
                    self.apply_columnar_join_order_by(rows, order_by, &stmt.select_list, &combined_schema)?
                } else {
                    rows
                };

                Ok(Some(final_rows))
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

                // Apply ORDER BY BEFORE projection so sort expressions can reference
                // columns from the full combined schema (e.g. `ORDER BY c` where `c`
                // is not in the SELECT list). The row-oriented path in
                // `nonagg/materialized.rs` follows the same sort-then-project order.
                // See issue #5189: projecting first produced "Column index out of
                // bounds" errors when ORDER BY referenced columns dropped by the
                // projection.
                let sorted_rows = if let Some(order_by) = &stmt.order_by {
                    self.apply_columnar_join_order_by(rows, order_by, &stmt.select_list, &combined_schema)?
                } else {
                    rows
                };

                let mut projected_rows = Vec::with_capacity(sorted_rows.len());
                for row in &sorted_rows {
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
    ///
    /// The `join_types` parameter contains the join type for each table in the chain.
    /// The first entry is always `None` (the leftmost table has no join type).
    /// Each subsequent entry specifies how that table joins to the already-joined tables.
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
        join_types: &[Option<JoinType>],
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
        for (i, (table_name, alias, batch, schema)) in batches.iter().enumerate().skip(1) {
            let table_ref = alias.as_deref().unwrap_or(table_name.as_str());

            // Get the join type for this table (default to Inner for backward compatibility)
            let jt = join_types
                .get(i)
                .and_then(|jt| jt.clone())
                .unwrap_or(JoinType::Inner);

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
                    // CROSS JOIN doesn't need a join condition
                    if matches!(jt, JoinType::Cross) {
                        log::debug!(
                            "Columnar join: CROSS JOIN '{}' with {:?} (no condition needed)",
                            table_ref,
                            joined_tables
                        );
                        // For cross join without condition, fall back to row-oriented
                        // since columnar cross join is not implemented
                        return Ok(None);
                    }
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
                "Columnar join: {:?} joining '{}' (col {}) with '{}' (col {})",
                jt,
                joined_tables.join(", "),
                left_col_idx,
                table_ref,
                right_col_idx
            );

            // Execute the hash join based on join type
            current_batch = match jt {
                JoinType::Inner | JoinType::Cross => {
                    columnar_join::columnar_hash_join_inner(
                        &current_batch,
                        batch,
                        left_col_idx,
                        right_col_idx,
                    )?
                }
                JoinType::LeftOuter => {
                    columnar_join::columnar_hash_join_left_outer(
                        &current_batch,
                        batch,
                        left_col_idx,
                        right_col_idx,
                    )?
                }
                JoinType::RightOuter => {
                    columnar_join::columnar_hash_join_right_outer(
                        &current_batch,
                        batch,
                        left_col_idx,
                        right_col_idx,
                    )?
                }
                _ => {
                    // FullOuter, Semi, Anti - not supported, fall back
                    log::debug!(
                        "Columnar join: unsupported join type {:?} for '{}', falling back",
                        jt,
                        table_ref
                    );
                    return Ok(None);
                }
            };

            // Update tracking
            joined_tables.push(table_ref);
        }

        Ok(Some(current_batch))
    }

    /// Apply ORDER BY sorting to rows produced by the columnar join path (#5033)
    ///
    /// This wraps rows as RowWithSortKeys, applies the ORDER BY logic from the
    /// order module, then unwraps back to plain rows.
    fn apply_columnar_join_order_by(
        &self,
        rows: Vec<vibesql_storage::Row>,
        order_by: &[vibesql_ast::OrderByItem],
        select_list: &[SelectItem],
        combined_schema: &CombinedSchema,
    ) -> Result<Vec<vibesql_storage::Row>, ExecutorError> {
        if rows.is_empty() {
            return Ok(rows);
        }

        log::debug!(
            "Columnar join: applying ORDER BY to {} rows",
            rows.len()
        );

        // Wrap rows as RowWithSortKeys (with None sort keys - apply_order_by will compute them)
        let rows_with_keys: Vec<RowWithSortKeys> =
            rows.into_iter().map(|r| (r, None)).collect();

        // Create evaluator for ORDER BY expression evaluation
        // SAFETY: combined_schema lives for the duration of this function call
        let schema_ref: &'static CombinedSchema =
            unsafe { std::mem::transmute(combined_schema) };
        let evaluator =
            CombinedExpressionEvaluator::with_database(schema_ref, self.database);

        // Apply ORDER BY sorting
        let sorted = apply_order_by(rows_with_keys, order_by, &evaluator, select_list)?;

        // Unwrap back to plain rows
        Ok(sorted.into_iter().map(|(row, _)| row).collect())
    }
}

/// Check if the SELECT list contains ROWID pseudo-column references
///
/// ROWID, _rowid_, and oid are SQLite-compatible pseudo-columns that require
/// row-oriented execution to track row IDs through JOINs. This function detects
/// when these are referenced so the columnar join path can fall back.
fn select_list_has_rowid(select_list: &[SelectItem]) -> bool {
    select_list.iter().any(|item| match item {
        SelectItem::Expression { expr, .. } => expression_has_rowid(expr),
        SelectItem::Wildcard { .. } | SelectItem::QualifiedWildcard { .. } => false,
    })
}

/// Check if an expression contains ROWID pseudo-column references
fn expression_has_rowid(expr: &vibesql_ast::Expression) -> bool {
    match expr {
        vibesql_ast::Expression::ColumnRef(col_id) => {
            let lower = col_id.column_canonical().to_lowercase();
            lower == "rowid" || lower == "_rowid_" || lower == "oid"
        }
        vibesql_ast::Expression::BinaryOp { left, right, .. } => {
            expression_has_rowid(left) || expression_has_rowid(right)
        }
        vibesql_ast::Expression::UnaryOp { expr, .. } => expression_has_rowid(expr),
        vibesql_ast::Expression::Function { args, .. } => args.iter().any(expression_has_rowid),
        vibesql_ast::Expression::Case { operand, when_clauses, else_result } => {
            operand.as_ref().is_some_and(|e| expression_has_rowid(e))
                || when_clauses.iter().any(|w| {
                    w.conditions.iter().any(expression_has_rowid) || expression_has_rowid(&w.result)
                })
                || else_result.as_ref().is_some_and(|e| expression_has_rowid(e))
        }
        vibesql_ast::Expression::InList { expr, values, .. } => {
            expression_has_rowid(expr) || values.iter().any(expression_has_rowid)
        }
        vibesql_ast::Expression::Between { expr, low, high, .. } => {
            expression_has_rowid(expr) || expression_has_rowid(low) || expression_has_rowid(high)
        }
        vibesql_ast::Expression::IsNull { expr, .. } => expression_has_rowid(expr),
        vibesql_ast::Expression::Cast { expr, .. } => expression_has_rowid(expr),
        vibesql_ast::Expression::RowValueConstructor(exprs) => {
            exprs.iter().any(expression_has_rowid)
        }
        vibesql_ast::Expression::Collate { expr, .. } => expression_has_rowid(expr),
        vibesql_ast::Expression::Raise { error_message, .. } => {
            error_message.as_ref().is_some_and(|msg| expression_has_rowid(msg))
        }
        // These variants don't contain column references that could be ROWID
        vibesql_ast::Expression::Literal(_)
        | vibesql_ast::Expression::Placeholder(_)
        | vibesql_ast::Expression::NumberedPlaceholder(_)
        | vibesql_ast::Expression::NamedPlaceholder(_)
        | vibesql_ast::Expression::Conjunction(_)
        | vibesql_ast::Expression::Disjunction(_)
        | vibesql_ast::Expression::AggregateFunction { .. }
        | vibesql_ast::Expression::IsDistinctFrom { .. }
        | vibesql_ast::Expression::IsTruthValue { .. }
        | vibesql_ast::Expression::Wildcard
        | vibesql_ast::Expression::ScalarSubquery(_)
        | vibesql_ast::Expression::In { .. }
        | vibesql_ast::Expression::Position { .. }
        | vibesql_ast::Expression::Trim { .. }
        | vibesql_ast::Expression::Extract { .. }
        | vibesql_ast::Expression::Like { .. }
        | vibesql_ast::Expression::Glob { .. }
        | vibesql_ast::Expression::Exists { .. }
        | vibesql_ast::Expression::QuantifiedComparison { .. }
        | vibesql_ast::Expression::CurrentDate
        | vibesql_ast::Expression::CurrentTime { .. }
        | vibesql_ast::Expression::CurrentTimestamp { .. }
        | vibesql_ast::Expression::Interval { .. }
        | vibesql_ast::Expression::Default
        | vibesql_ast::Expression::DuplicateKeyValue { .. }
        | vibesql_ast::Expression::WindowFunction { .. }
        | vibesql_ast::Expression::NextValue { .. }
        | vibesql_ast::Expression::MatchAgainst { .. }
        | vibesql_ast::Expression::PseudoVariable { .. }
        | vibesql_ast::Expression::SessionVariable { .. } => false,
    }
}

/// Check if a WHERE expression contains predicates that can't be handled by SIMD filtering
///
/// Returns true if the expression contains:
/// - Scalar subqueries (SELECT ... WHERE col = (SELECT ...))
/// - IN subqueries (SELECT ... WHERE col IN (SELECT ...))
/// - EXISTS predicates
/// - Quantified comparisons (ALL/ANY/SOME)
///
/// These require row-by-row evaluation and can't be optimized by columnar SIMD filtering.
/// When detected, the caller should fall back to row-oriented execution.
fn contains_unsupported_predicates(expr: &vibesql_ast::Expression) -> bool {
    match expr {
        // Subquery expressions can't be handled by SIMD filtering
        vibesql_ast::Expression::ScalarSubquery(_) => true,
        vibesql_ast::Expression::In { .. } => true, // IN with subquery
        vibesql_ast::Expression::Exists { .. } => true,
        vibesql_ast::Expression::QuantifiedComparison { .. } => true,

        // Binary operations need recursive checking
        vibesql_ast::Expression::BinaryOp { left, op, right } => {
            // For AND/OR, check both sides
            if matches!(op, vibesql_ast::BinaryOperator::And | vibesql_ast::BinaryOperator::Or) {
                return contains_unsupported_predicates(left)
                    || contains_unsupported_predicates(right);
            }

            // For equality comparisons, check if either side is a subquery
            // Skip join conditions (col1 = col2) as they're handled separately
            let is_join_condition = matches!(
                (left.as_ref(), right.as_ref()),
                (vibesql_ast::Expression::ColumnRef(_), vibesql_ast::Expression::ColumnRef(_))
            );

            if is_join_condition {
                return false;
            }

            // Check if either side contains unsupported predicates
            contains_unsupported_predicates(left) || contains_unsupported_predicates(right)
        }

        // Unary operations need recursive checking
        vibesql_ast::Expression::UnaryOp { expr, .. } => contains_unsupported_predicates(expr),

        // Other expressions don't contain subqueries
        _ => false,
    }
}

/// Cached check for VIBESQL_DISABLE_COLUMNAR_JOIN environment variable.
///
/// Uses `OnceLock` to read the env var once (avoiding the per-query global lock
/// that `std::env::var()` acquires).
fn is_columnar_join_disabled() -> bool {
    static DISABLED: OnceLock<bool> = OnceLock::new();
    *DISABLED.get_or_init(|| std::env::var("VIBESQL_DISABLE_COLUMNAR_JOIN").is_ok())
}

/// Check if the SELECT list contains aggregate functions with arguments that
/// the columnar JOIN GROUP BY path cannot handle.
///
/// The join GROUP BY path (`execute_columnar_join_group_by`) only supports:
/// - `AggregateSource::Column` (simple column ref like `SUM(col)`)
/// - `AggregateSource::CountStar` (COUNT(*))
///
/// It explicitly rejects `AggregateSource::Expression` (e.g., `SUM(a * b)`),
/// and `extract_aggregates` returns `None` for CASE, function calls, and other
/// complex argument types. This function detects those patterns early so we
/// can skip the expensive table loading and hash join work.
fn has_unsupported_join_aggregates(select_list: &[SelectItem]) -> bool {
    for item in select_list {
        let expr = match item {
            SelectItem::Expression { expr, .. } => expr,
            _ => continue,
        };
        if check_expr_for_unsupported_agg(expr) {
            return true;
        }
    }
    false
}

/// Recursively check an expression tree for aggregate functions with
/// unsupported argument types in the columnar join path.
fn check_expr_for_unsupported_agg(expr: &vibesql_ast::Expression) -> bool {
    match expr {
        vibesql_ast::Expression::AggregateFunction { args, distinct, .. } => {
            // DISTINCT aggregates not supported in columnar path
            if *distinct {
                return true;
            }
            // COUNT(*) / COUNT() -- always OK
            if args.is_empty() {
                return false;
            }
            if args.len() == 1 {
                match &args[0] {
                    vibesql_ast::Expression::Wildcard => false,        // COUNT(*)
                    vibesql_ast::Expression::ColumnRef(_) => false,    // SUM(col)
                    // Everything else (BinaryOp, CASE, Function, etc.) will be
                    // rejected by the join GROUP BY path, either as an
                    // unsupported arg type or as AggregateSource::Expression.
                    _ => true,
                }
            } else {
                // Multiple arguments not supported
                true
            }
        }
        // Recurse into expression wrappers that may contain aggregates
        vibesql_ast::Expression::BinaryOp { left, right, .. } => {
            check_expr_for_unsupported_agg(left) || check_expr_for_unsupported_agg(right)
        }
        vibesql_ast::Expression::UnaryOp { expr, .. } => check_expr_for_unsupported_agg(expr),
        vibesql_ast::Expression::Cast { expr, .. } => check_expr_for_unsupported_agg(expr),
        vibesql_ast::Expression::Case {
            operand,
            when_clauses,
            else_result,
        } => {
            if operand.as_ref().is_some_and(|e| check_expr_for_unsupported_agg(e)) {
                return true;
            }
            for w in when_clauses {
                if w.conditions.iter().any(check_expr_for_unsupported_agg)
                    || check_expr_for_unsupported_agg(&w.result)
                {
                    return true;
                }
            }
            if else_result.as_ref().is_some_and(|e| check_expr_for_unsupported_agg(e)) {
                return true;
            }
            false
        }
        vibesql_ast::Expression::Function { args, .. } => {
            args.iter().any(check_expr_for_unsupported_agg)
        }
        // Leaf expressions and other variants don't contain aggregates
        _ => false,
    }
}
