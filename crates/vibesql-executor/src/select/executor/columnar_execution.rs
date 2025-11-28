//! Columnar execution integration for SelectExecutor
//!
//! This module integrates the columnar execution engine with the query executor,
//! providing automatic detection and execution of queries that can benefit from
//! SIMD-accelerated columnar processing.
//!
//! Note: This module is experimental/research code. Some methods are not yet
//! integrated into the main execution path.

#![allow(clippy::ptr_arg)]
//!
//! ## Phase 2: Native Columnar Execution
//!
//! Phase 2 adds support for end-to-end columnar execution that operates on
//! `ColumnarBatch` throughout the pipeline, avoiding row materialization.
//!
//! ```text
//! Storage → ColumnarBatch → SIMD Filter → SIMD Aggregate → Vec<Row> (only at output)
//!          ↑ Zero-copy     ↑ 4-8x faster  ↑ 10x faster   ↑ Minimal materialization
//! ```
//!
//! ## Phase 3: GROUP BY Support
//!
//! Phase 3 extends columnar execution to support GROUP BY queries using hash-based
//! aggregation. This enables TPC-H Q1 style queries to use the columnar path.
//!
//! ```text
//! Storage → ColumnarBatch → SIMD Filter → Hash GROUP BY → Vec<Row>
//!          ↑ Zero-copy     ↑ 4-8x faster  ↑ Hash aggregation
//! ```
//!
//! ## Phase 4: Vectorized Hash Join (#2943)
//!
//! Phase 4 adds support for multi-table JOINs using columnar hash join.
//! This enables TPC-H Q3 style queries (3+ table joins with GROUP BY) to use
//! vectorized processing.
//!
//! ```text
//! Table1 → ColumnarBatch ─┬─→ Hash Join → Hash Join → SIMD Filter → GROUP BY → Vec<Row>
//! Table2 → ColumnarBatch ─┘        ↑
//! Table3 → ColumnarBatch ──────────┘
//! ```

use std::collections::HashMap;

use super::builder::SelectExecutor;
use crate::{
    errors::ExecutorError,
    optimizer::adaptive::{choose_execution_model, ExecutionModel},
    select::{columnar, cte::CteResult, join::hash_join::columnar as columnar_join},
    schema::CombinedSchema,
};
use vibesql_ast::{BinaryOperator, Expression, FromClause, JoinType};

impl SelectExecutor<'_> {
    /// Try to execute using columnar (auto-vectorized) execution
    ///
    /// Returns Some(rows) if the query is compatible with columnar execution.
    /// Returns None if the query should fall back to regular row-based execution.
    ///
    /// Columnar execution provides 6-10x speedup for queries with:
    /// - Simple predicates on numeric columns
    /// - Aggregations (SUM, AVG, MIN, MAX, COUNT)
    /// - Single table scans (no JOINs yet)
    ///
    /// # Phase 5 Implementation
    ///
    /// This initial implementation focuses on simple aggregate queries without GROUP BY.
    /// Future phases will add support for:
    /// - GROUP BY aggregations
    /// - JOIN operations
    /// - More complex predicates (OR logic, IN clauses)
    pub(in crate::select::executor) fn try_columnar_execution(
        &self,
        stmt: &vibesql_ast::SelectStmt,
        cte_results: &HashMap<String, CteResult>,
    ) -> Result<Option<Vec<vibesql_storage::Row>>, ExecutorError> {
        // GROUP BY queries are NOT supported in this columnar path
        // They should use try_native_columnar_execution or fall back to row-oriented
        // execute_columnar() computes overall aggregates, ignoring GROUP BY
        if stmt.group_by.is_some() {
            log::debug!("  Columnar execution: Not supported - has GROUP BY");
            return Ok(None);
        }

        // Check if this query is compatible with columnar execution
        // Use adaptive execution model selection for better query decisions
        match choose_execution_model(stmt) {
            ExecutionModel::RowOriented => {
                log::debug!("  Columnar execution: Query not compatible (adaptive execution selected row-oriented)");
                #[cfg(feature = "profile-q6")]
                eprintln!("[PROFILE-Q6]   Reason: Adaptive execution selected ROW-ORIENTED model");
                return Ok(None);
            }
            ExecutionModel::Columnar => {
                log::debug!("  Columnar execution: Query eligible (adaptive execution selected columnar)");
                #[cfg(feature = "profile-q6")]
                eprintln!("[PROFILE-Q6]   ✓ Adaptive execution selected COLUMNAR model");
                // Continue with columnar execution
            }
        }

        // Only handle queries without CTEs or set operations for now
        if !cte_results.is_empty() || stmt.set_operation.is_some() {
            log::debug!("  Columnar execution: Not supported - has CTEs or set operations");
            #[cfg(feature = "profile-q6")]
            eprintln!("[PROFILE-Q6]   Reason: Has CTEs or set operations (not supported yet)");
            return Ok(None);
        }

        // Must have a FROM clause
        let from_clause = match &stmt.from {
            Some(from) => from,
            None => {
                log::debug!("  Columnar execution: Not supported - no FROM clause");
                #[cfg(feature = "profile-q6")]
                eprintln!("[PROFILE-Q6]   Reason: No FROM clause");
                return Ok(None);
            }
        };

        // Execute FROM clause WITHOUT applying WHERE clause
        // The columnar module will apply the WHERE clause using SIMD-accelerated filtering
        let from_result = self.execute_from_with_where(
            from_clause,
            cte_results,
            None, // Don't filter here - columnar module will handle it with SIMD
            None, // ORDER BY applied after aggregation
        )?;

        // Extract schema before accessing rows (to avoid borrow checker issues)
        let schema = from_result.schema.clone();

        // Validate column references BEFORE processing (issue #2654)
        // This ensures column errors are caught even when tables are empty
        // Pass procedural context to allow procedure variables in WHERE clause
        // Pass outer_schema for correlated subqueries (#2694)
        super::validation::validate_select_columns_with_context(
            &stmt.select_list,
            stmt.where_clause.as_ref(),
            &schema,
            self.procedural_context,
            self.outer_schema,
        )?;

        // Extract expressions from SELECT list (only Expression items, skip wildcards)
        let select_exprs: Vec<_> = stmt
            .select_list
            .iter()
            .filter_map(|item| match item {
                vibesql_ast::SelectItem::Expression { expr, .. } => Some(expr.clone()),
                _ => None, // Skip wildcards
            })
            .collect();

        // Get a slice reference to rows WITHOUT triggering collect_vec() materialization
        // This is the critical optimization for #2521 - avoids the 137ms bottleneck
        let rows_slice = from_result.data.as_slice();

        // Try columnar execution with SIMD-accelerated filtering
        // If this returns None, the regular executor will handle the query with row-based execution
        #[cfg(feature = "profile-q6")]
        eprintln!("[PROFILE-Q6]   Attempting columnar execution on {} rows...", rows_slice.len());

        match columnar::execute_columnar(
            rows_slice,
            stmt.where_clause.as_ref(), // Let columnar module apply WHERE with SIMD
            &select_exprs,
            &schema,
        ) {
            Some(result) => {
                #[cfg(feature = "profile-q6")]
                eprintln!("[PROFILE-Q6]   ✓ Columnar execution succeeded");
                result.map(Some)
            },
            None => {
                #[cfg(feature = "profile-q6")]
                eprintln!("[PROFILE-Q6]   Reason: execute_columnar returned None (predicates or aggregates too complex)");
                Ok(None)
            }, // Fall back to regular execution
        }
    }

    /// Try to execute using native columnar batch execution (Phase 2)
    ///
    /// This method attempts to execute queries using the new end-to-end columnar
    /// pipeline that operates on ColumnarBatch throughout, avoiding row materialization.
    ///
    /// Returns Some(rows) if native columnar execution succeeded.
    /// Returns None if the query should fall back to row-based execution.
    ///
    /// # Phase 2 Benefits
    ///
    /// - **Zero row materialization**: Data stays in columnar format until final output
    /// - **SIMD filtering**: 4-8x faster filtering using vectorized instructions
    /// - **SIMD aggregation**: 10x faster aggregation for numeric columns
    /// - **Cache efficiency**: Columnar data access is cache-friendly
    pub(in crate::select::executor) fn try_native_columnar_execution(
        &self,
        stmt: &vibesql_ast::SelectStmt,
        cte_results: &HashMap<String, CteResult>,
    ) -> Result<Option<Vec<vibesql_storage::Row>>, ExecutorError> {
        // Native columnar execution is now enabled by default for eligible queries
        // Set VIBESQL_DISABLE_COLUMNAR=1 to opt-out and use row-oriented execution
        if std::env::var("VIBESQL_DISABLE_COLUMNAR").is_ok() {
            log::debug!("Native columnar: disabled via VIBESQL_DISABLE_COLUMNAR");
            return Ok(None);
        }

        // Only handle queries without CTEs or set operations
        if !cte_results.is_empty() || stmt.set_operation.is_some() {
            log::debug!("Native columnar: skipping - has CTEs or set operations");
            return Ok(None);
        }

        // Must have a FROM clause with a single table
        let from_clause = match &stmt.from {
            Some(from) => from,
            None => return Ok(None),
        };

        // Extract table name if this is a simple single-table scan
        let table_name = match extract_single_table_name(from_clause) {
            Some(name) => name,
            None => {
                log::debug!("Native columnar: skipping - not a simple single-table query");
                return Ok(None);
            }
        };

        // Check if adaptive execution model recommends columnar
        match choose_execution_model(stmt) {
            ExecutionModel::RowOriented => {
                log::debug!("Native columnar: skipping - adaptive model selected row-oriented");
                return Ok(None);
            }
            ExecutionModel::Columnar => {} // Continue
        }

        // Get the table and check it exists
        let table = match self.database.get_table(&table_name) {
            Some(t) => t,
            None => return Ok(None),
        };

        // Build schema for this table
        let schema = CombinedSchema::from_table(table_name.clone(), table.schema.clone());

        // Validate column references BEFORE processing (issue #2654)
        // This ensures column errors are caught even when tables are empty
        // Pass procedural context to allow procedure variables in WHERE clause
        // Pass outer_schema for correlated subqueries (#2694)
        super::validation::validate_select_columns_with_context(
            &stmt.select_list,
            stmt.where_clause.as_ref(),
            &schema,
            self.procedural_context,
            self.outer_schema,
        )?;

        // Get columnar representation from cache or convert from storage
        #[cfg(feature = "profile-q6")]
        let scan_start = std::time::Instant::now();

        // Use the database-level columnar cache for Arc-based sharing
        // This avoids the clone overhead (~14ms) on cache hits
        let columnar_arc = match self.database.get_columnar(&table_name) {
            Ok(Some(ct)) => ct,
            Ok(None) => {
                log::debug!("Native columnar: table not found in cache or storage");
                return Ok(None);
            }
            Err(e) => {
                log::debug!("Native columnar: get_columnar failed: {:?}", e);
                return Ok(None);
            }
        };

        #[cfg(feature = "profile-q6")]
        {
            let scan_time = scan_start.elapsed();
            let cache_stats = self.database.columnar_cache_stats();
            eprintln!(
                "[PROFILE-Q6] Native columnar scan: {:?} ({} rows, cache hits: {}, misses: {})",
                scan_time,
                columnar_arc.row_count(),
                cache_stats.hits,
                cache_stats.misses
            );
        }

        log::info!(
            "Native columnar execution: table={}, rows={}",
            table_name,
            columnar_arc.row_count()
        );

        // Skip empty tables - columnar provides no benefit and may have column lookup issues
        if columnar_arc.row_count() == 0 {
            log::debug!("Native columnar: skipping empty table");
            return Ok(None);
        }

        // Convert to ColumnarBatch (zero-copy when possible)
        // Use Arc deref to pass a reference to the cached ColumnarTable
        let batch = columnar::ColumnarBatch::from_storage_columnar(&columnar_arc)?;

        // Extract predicates from WHERE clause
        let predicates = stmt.where_clause.as_ref()
            .and_then(|where_expr| columnar::extract_column_predicates(where_expr, &schema))
            .unwrap_or_default();

        // Extract select expressions
        let select_exprs: Vec<_> = stmt.select_list.iter()
            .filter_map(|item| match item {
                vibesql_ast::SelectItem::Expression { expr, .. } => Some(expr.clone()),
                _ => None,
            })
            .collect();

        // Extract aggregates from select expressions
        let aggregates = match columnar::extract_aggregates(&select_exprs, &schema) {
            Some(aggs) => aggs,
            None => {
                log::debug!("Native columnar: skipping - no aggregates or unsupported expressions");
                return Ok(None);
            }
        };

        // Check if this query has GROUP BY
        let has_group_by = stmt.group_by.is_some();

        // Skip native columnar for complex GROUP BY (ROLLUP/CUBE/GROUPING SETS)
        // These require special handling that the columnar path doesn't support
        if let Some(ref group_by) = stmt.group_by {
            if group_by.as_simple().is_none() {
                log::debug!("Native columnar: skipping - ROLLUP/CUBE/GROUPING SETS not supported");
                return Ok(None);
            }
        }

        // Execute using native columnar pipeline
        #[cfg(feature = "profile-q6")]
        let exec_start = std::time::Instant::now();

        let result = if has_group_by {
            // GROUP BY path: Use hash-based grouping
            self.execute_columnar_group_by(stmt, &batch, &predicates, &aggregates, &schema)?
        } else {
            // Non-GROUP BY path: Simple aggregation
            columnar::execute_columnar_batch(&batch, &predicates, &aggregates, Some(&schema))?
        };

        #[cfg(feature = "profile-q6")]
        {
            let exec_time = exec_start.elapsed();
            eprintln!(
                "[PROFILE-Q6] Native columnar execution: {:?}",
                exec_time
            );
        }

        log::info!(
            "Native columnar execution completed: {} predicates, {} aggregates, group_by={}",
            predicates.len(),
            aggregates.len(),
            has_group_by
        );

        Ok(Some(result))
    }

    /// Execute a GROUP BY query using columnar hash aggregation
    ///
    /// This method implements the GROUP BY path for native columnar execution,
    /// using hash-based grouping with the existing `columnar_group_by` function.
    fn execute_columnar_group_by(
        &self,
        stmt: &vibesql_ast::SelectStmt,
        batch: &columnar::ColumnarBatch,
        predicates: &[columnar::ColumnPredicate],
        aggregates: &[columnar::AggregateSpec],
        schema: &CombinedSchema,
    ) -> Result<Vec<vibesql_storage::Row>, ExecutorError> {
        #[cfg(feature = "profile-q6")]
        let start = std::time::Instant::now();

        // Phase 1: Apply auto-vectorized filtering to get filtered batch
        let filtered_batch = if predicates.is_empty() {
            batch.clone()
        } else {
            columnar::simd_filter_batch(batch, predicates)?
        };

        #[cfg(feature = "profile-q6")]
        {
            let filter_time = start.elapsed();
            eprintln!(
                "[PROFILE-Q6]   GROUP BY Phase 1 - Filter: {:?} ({}/{} rows)",
                filter_time,
                filtered_batch.row_count(),
                batch.row_count()
            );
        }

        // Phase 2: Extract group column indices from GROUP BY clause
        let group_by_clause = stmt.group_by.as_ref().ok_or_else(|| {
            ExecutorError::Other("GROUP BY clause required for group_by execution".to_string())
        })?;

        // Only support simple GROUP BY in columnar path (not ROLLUP/CUBE/GROUPING SETS)
        let simple_exprs = group_by_clause.as_simple().ok_or_else(|| {
            ExecutorError::Other(
                "ROLLUP/CUBE/GROUPING SETS not supported in columnar execution path".to_string(),
            )
        })?;

        let group_cols: Vec<usize> = simple_exprs
            .iter()
            .filter_map(|expr| {
                match expr {
                    vibesql_ast::Expression::ColumnRef { table, column } => {
                        schema.get_column_index(table.as_deref(), column.as_str())
                    }
                    _ => None, // Only simple column references supported for now
                }
            })
            .collect();

        if group_cols.len() != simple_exprs.len() {
            log::debug!("GROUP BY contains non-column expressions, falling back to row-oriented");
            return Err(ExecutorError::Other(
                "GROUP BY with non-column expressions not supported in columnar path".to_string()
            ));
        }

        // Phase 3: Convert aggregates to (column_idx, op) format for columnar_group_by
        let agg_cols: Vec<(usize, columnar::AggregateOp)> = aggregates
            .iter()
            .filter_map(|spec| {
                match &spec.source {
                    columnar::AggregateSource::Column(idx) => Some((*idx, spec.op)),
                    columnar::AggregateSource::CountStar => {
                        // For COUNT(*), use column 0 with Count op
                        Some((0, columnar::AggregateOp::Count))
                    }
                    columnar::AggregateSource::Expression(_) => {
                        // Expression aggregates not yet supported in GROUP BY path
                        // TODO: Add support for expression aggregates in GROUP BY
                        log::debug!("Expression aggregate in GROUP BY not yet supported");
                        None
                    }
                }
            })
            .collect();

        if agg_cols.len() != aggregates.len() {
            log::debug!("Some aggregates not supported in columnar GROUP BY path");
            return Err(ExecutorError::Other(
                "Expression aggregates not supported in columnar GROUP BY path".to_string()
            ));
        }

        #[cfg(feature = "profile-q6")]
        let group_start = std::time::Instant::now();

        // Phase 4: Convert batch to rows for columnar_group_by
        // TODO: Implement native batch-based GROUP BY to avoid this conversion
        let rows = filtered_batch.to_rows()?;

        // Phase 5: Execute hash-based GROUP BY aggregation
        let result = columnar::columnar_group_by(&rows, &group_cols, &agg_cols, None)?;

        #[cfg(feature = "profile-q6")]
        {
            let group_time = group_start.elapsed();
            eprintln!(
                "[PROFILE-Q6]   GROUP BY Phase 2 - Hash aggregation: {:?} ({} groups)",
                group_time,
                result.len()
            );
        }

        Ok(result)
    }

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
    /// - Multi-table INNER JOINs (explicit JOIN or comma-separated tables)
    /// - Equi-join conditions (col1 = col2)
    /// - Simple WHERE predicates
    /// - GROUP BY with aggregates (SUM, COUNT, AVG, MIN, MAX)
    ///
    /// # Performance
    ///
    /// - 3-5x improvement for JOIN-heavy queries
    /// - Targets TPC-H Q3, Q5, Q7-Q10 style queries
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

        // Only handle queries without CTEs or set operations
        if !cte_results.is_empty() || stmt.set_operation.is_some() {
            log::debug!("Columnar join: skipping - has CTEs or set operations");
            return Ok(None);
        }

        // Must have a FROM clause with JOINs
        let from_clause = match &stmt.from {
            Some(from) => from,
            None => return Ok(None),
        };

        // Must be a JOIN clause (explicit or implicit via comma-separated tables)
        if !matches!(from_clause, FromClause::Join { .. }) {
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
        let mut batches: Vec<(String, Option<String>, columnar::ColumnarBatch, vibesql_catalog::TableSchema)> = Vec::new();

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

        log::debug!("Columnar join: found {} join conditions for {} tables", join_conditions.len(), table_refs.len());

        // For N tables, we need exactly N-1 equijoin conditions for a simple connected join graph.
        // If there are more conditions, some are filters that need special handling.
        // Fall back to row-based execution in that case for correctness.
        let min_join_conditions = table_refs.len() - 1;
        if join_conditions.len() > min_join_conditions {
            log::debug!(
                "Columnar join: {} join conditions exceeds minimum {} for {} tables, falling back",
                join_conditions.len(), min_join_conditions, table_refs.len()
            );
            return Ok(None);
        }

        // Build combined schema for all tables
        let combined_schema = build_combined_schema(&batches);

        // Execute joins in sequence, building up the result batch
        let joined_batch = match self.execute_columnar_join_chain(&batches, &join_conditions, &combined_schema) {
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
        let predicates = stmt.where_clause.as_ref()
            .and_then(|where_expr| extract_non_join_predicates(where_expr, &combined_schema))
            .unwrap_or_default();

        let filtered_batch = if predicates.is_empty() {
            joined_batch
        } else {
            columnar::simd_filter_batch(&joined_batch, &predicates)?
        };

        let joined_row_count = filtered_batch.row_count();
        log::info!(
            "Columnar join: {} rows after join and filter",
            joined_row_count
        );

        // Check for GROUP BY
        let has_group_by = stmt.group_by.is_some();

        if has_group_by {
            // Execute GROUP BY aggregation
            self.execute_columnar_join_group_by(stmt, &filtered_batch, &combined_schema)
        } else {
            // No GROUP BY - convert to rows
            let rows = filtered_batch.to_rows()?;
            Ok(Some(rows))
        }
    }

    /// Execute a chain of hash joins on columnar batches
    fn execute_columnar_join_chain(
        &self,
        batches: &[(String, Option<String>, columnar::ColumnarBatch, vibesql_catalog::TableSchema)],
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
        let mut current_col_offset = 0usize;

        // Track which tables have been joined
        let mut joined_tables: Vec<&str> = vec![
            batches[0].1.as_deref().unwrap_or(&batches[0].0)
        ];

        // Join subsequent tables
        for (table_name, alias, batch, schema) in batches.iter().skip(1) {
            let table_ref = alias.as_deref().unwrap_or(table_name.as_str());

            // Find a join condition that connects this table to already-joined tables
            let join_cond = join_conditions.iter().find(|cond| {
                let left_in_joined = joined_tables.iter().any(|t| {
                    cond.left_table.as_deref() == Some(*t) ||
                    (cond.left_table.is_none() && is_column_in_tables(&cond.left_column, &joined_tables, combined_schema))
                });
                let right_is_current = cond.right_table.as_deref() == Some(table_ref) ||
                    (cond.right_table.is_none() && is_column_in_table(&cond.right_column, table_ref, combined_schema));
                let right_in_joined = joined_tables.iter().any(|t| {
                    cond.right_table.as_deref() == Some(*t) ||
                    (cond.right_table.is_none() && is_column_in_tables(&cond.right_column, &joined_tables, combined_schema))
                });
                let left_is_current = cond.left_table.as_deref() == Some(table_ref) ||
                    (cond.left_table.is_none() && is_column_in_table(&cond.left_column, table_ref, combined_schema));

                (left_in_joined && right_is_current) || (right_in_joined && left_is_current)
            });

            let join_cond = match join_cond {
                Some(cond) => cond,
                None => {
                    log::debug!(
                        "Columnar join: no join condition found connecting '{}' to {:?}",
                        table_ref, joined_tables
                    );
                    return Ok(None);
                }
            };

            // Determine which side of the condition refers to the current batch vs new table
            let (left_col_idx, right_col_idx) = resolve_join_column_indices(
                join_cond,
                &joined_tables,
                table_ref,
                current_col_offset,
                schema,
                combined_schema,
            )?;

            log::debug!(
                "Columnar join: joining '{}' (col {}) with '{}' (col {})",
                joined_tables.join(", "), left_col_idx,
                table_ref, right_col_idx
            );

            // Execute the hash join
            current_batch = columnar_join::columnar_hash_join_inner(
                &current_batch,
                batch,
                left_col_idx,
                right_col_idx,
            )?;

            // Update tracking
            current_col_offset = current_batch.column_count() - batch.column_count();
            joined_tables.push(table_ref);
        }

        Ok(Some(current_batch))
    }

    /// Execute GROUP BY aggregation on a joined columnar batch
    fn execute_columnar_join_group_by(
        &self,
        stmt: &vibesql_ast::SelectStmt,
        batch: &columnar::ColumnarBatch,
        schema: &CombinedSchema,
    ) -> Result<Option<Vec<vibesql_storage::Row>>, ExecutorError> {
        let group_by_clause = match stmt.group_by.as_ref() {
            Some(g) => g,
            None => return Ok(Some(batch.to_rows()?)),
        };

        // Only support simple GROUP BY
        let simple_exprs = match group_by_clause.as_simple() {
            Some(exprs) => exprs,
            None => {
                log::debug!("Columnar join: ROLLUP/CUBE/GROUPING SETS not supported");
                return Ok(None);
            }
        };

        // Resolve group column indices
        let group_cols: Vec<usize> = simple_exprs
            .iter()
            .filter_map(|expr| {
                match expr {
                    Expression::ColumnRef { table, column } => {
                        schema.get_column_index(table.as_deref(), column.as_str())
                    }
                    _ => None,
                }
            })
            .collect();

        if group_cols.len() != simple_exprs.len() {
            log::debug!("Columnar join: some GROUP BY columns couldn't be resolved");
            return Ok(None);
        }

        // Extract select expressions
        let select_exprs: Vec<_> = stmt.select_list.iter()
            .filter_map(|item| match item {
                vibesql_ast::SelectItem::Expression { expr, .. } => Some(expr.clone()),
                _ => None,
            })
            .collect();

        // Extract aggregates
        let aggregates = match columnar::extract_aggregates(&select_exprs, schema) {
            Some(aggs) => aggs,
            None => {
                log::debug!("Columnar join: failed to extract aggregates");
                return Ok(None);
            }
        };

        // Convert aggregates to (column_idx, op) format
        let agg_cols: Vec<(usize, columnar::AggregateOp)> = aggregates
            .iter()
            .filter_map(|spec| {
                match &spec.source {
                    columnar::AggregateSource::Column(idx) => Some((*idx, spec.op)),
                    columnar::AggregateSource::CountStar => Some((0, columnar::AggregateOp::Count)),
                    columnar::AggregateSource::Expression(_) => {
                        // Expression aggregates require row-based evaluation
                        log::debug!("Columnar join: expression aggregate not supported");
                        None
                    }
                }
            })
            .collect();

        if agg_cols.len() != aggregates.len() {
            return Ok(None);
        }

        // Convert to rows and run GROUP BY
        let rows = batch.to_rows()?;
        let result = columnar::columnar_group_by(&rows, &group_cols, &agg_cols, None)?;

        log::info!("Columnar join: GROUP BY produced {} groups", result.len());
        Ok(Some(result))
    }
}

/// Simple table reference: (name, alias, is_subquery)
type SimpleTableRef = (String, Option<String>, bool);

/// Flatten a join tree into a list of simple table references
fn flatten_join_tree_simple(from: &FromClause, tables: &mut Vec<SimpleTableRef>) {
    match from {
        FromClause::Table { name, alias } => {
            tables.push((name.clone(), alias.clone(), false));
        }
        FromClause::Subquery { alias, .. } => {
            tables.push((alias.clone(), Some(alias.clone()), true));
        }
        FromClause::Join { left, right, .. } => {
            flatten_join_tree_simple(left, tables);
            flatten_join_tree_simple(right, tables);
        }
    }
}

/// Equi-join condition: left_table.left_column = right_table.right_column
#[derive(Debug, Clone)]
struct EquiJoinCondition {
    left_table: Option<String>,
    left_column: String,
    right_table: Option<String>,
    right_column: String,
}

/// Extract join conditions from a FROM clause (ON conditions)
fn extract_join_conditions(from: &FromClause, conditions: &mut Vec<EquiJoinCondition>) {
    match from {
        FromClause::Table { .. } | FromClause::Subquery { .. } => {}
        FromClause::Join { left, right, condition, join_type, .. } => {
            // Only handle INNER and CROSS joins in columnar path
            if !matches!(join_type, JoinType::Inner | JoinType::Cross) {
                return;
            }

            if let Some(cond) = condition {
                extract_equijoin_conditions(cond, conditions);
            }

            extract_join_conditions(left, conditions);
            extract_join_conditions(right, conditions);
        }
    }
}

/// Extract equi-join conditions from an expression
fn extract_equijoin_conditions(expr: &Expression, conditions: &mut Vec<EquiJoinCondition>) {
    match expr {
        Expression::BinaryOp { left, op: BinaryOperator::And, right } => {
            extract_equijoin_conditions(left, conditions);
            extract_equijoin_conditions(right, conditions);
        }
        Expression::BinaryOp { left, op: BinaryOperator::Equal, right } => {
            // Check if this is col1 = col2 (equi-join)
            if let (
                Expression::ColumnRef { table: lt, column: lc },
                Expression::ColumnRef { table: rt, column: rc }
            ) = (left.as_ref(), right.as_ref()) {
                conditions.push(EquiJoinCondition {
                    left_table: lt.clone(),
                    left_column: lc.clone(),
                    right_table: rt.clone(),
                    right_column: rc.clone(),
                });
            }
        }
        _ => {}
    }
}

/// Extract non-join predicates (conditions that aren't col1 = col2)
fn extract_non_join_predicates(
    expr: &Expression,
    schema: &CombinedSchema,
) -> Option<Vec<columnar::ColumnPredicate>> {
    let mut predicates = Vec::new();
    extract_non_join_predicates_recursive(expr, schema, &mut predicates);
    if predicates.is_empty() {
        None
    } else {
        Some(predicates)
    }
}

fn extract_non_join_predicates_recursive(
    expr: &Expression,
    schema: &CombinedSchema,
    predicates: &mut Vec<columnar::ColumnPredicate>,
) {
    match expr {
        Expression::BinaryOp { left, op: BinaryOperator::And, right } => {
            extract_non_join_predicates_recursive(left, schema, predicates);
            extract_non_join_predicates_recursive(right, schema, predicates);
        }
        Expression::BinaryOp { left, op: BinaryOperator::Equal, right } => {
            // Skip column = column (join conditions)
            if matches!(left.as_ref(), Expression::ColumnRef { .. })
                && matches!(right.as_ref(), Expression::ColumnRef { .. })
            {
                return;
            }
            // Try to extract as column predicate
            if let Some(pred) = columnar::extract_column_predicates(expr, schema) {
                predicates.extend(pred);
            }
        }
        _ => {
            // Try to extract other predicates
            if let Some(pred) = columnar::extract_column_predicates(expr, schema) {
                predicates.extend(pred);
            }
        }
    }
}

/// Build a combined schema from multiple table batches
fn build_combined_schema(
    batches: &[(String, Option<String>, columnar::ColumnarBatch, vibesql_catalog::TableSchema)],
) -> CombinedSchema {
    let mut combined = CombinedSchema {
        table_schemas: HashMap::new(),
        total_columns: 0,
    };

    for (table_name, alias, _batch, schema) in batches {
        let name = alias.as_ref().unwrap_or(table_name).clone();
        combined.table_schemas.insert(name, (combined.total_columns, schema.clone()));
        combined.total_columns += schema.columns.len();
    }

    combined
}

/// Check if a column exists in any of the given tables
fn is_column_in_tables(column: &str, tables: &[&str], schema: &CombinedSchema) -> bool {
    tables.iter().any(|t| is_column_in_table(column, t, schema))
}

/// Check if a column exists in a specific table
fn is_column_in_table(column: &str, table: &str, schema: &CombinedSchema) -> bool {
    if let Some((_, table_schema)) = schema.table_schemas.get(table) {
        table_schema.columns.iter().any(|c| c.name.eq_ignore_ascii_case(column))
    } else {
        false
    }
}

/// Resolve join column indices for the current join operation
fn resolve_join_column_indices(
    cond: &EquiJoinCondition,
    joined_tables: &[&str],
    new_table: &str,
    col_offset: usize,
    new_table_schema: &vibesql_catalog::TableSchema,
    combined_schema: &CombinedSchema,
) -> Result<(usize, usize), ExecutorError> {
    // Determine which side refers to joined tables vs new table
    let left_in_joined = cond.left_table.as_deref().map_or_else(
        || is_column_in_tables(&cond.left_column, joined_tables, combined_schema),
        |t| joined_tables.contains(&t)
    );

    let (left_col, right_col) = if left_in_joined {
        (&cond.left_column, &cond.right_column)
    } else {
        (&cond.right_column, &cond.left_column)
    };

    // Find left column index in the current (joined) batch
    let left_idx = combined_schema.get_column_index(None, left_col)
        .ok_or_else(|| ExecutorError::ColumnNotFound {
            column_name: left_col.clone(),
            table_name: String::new(),
            searched_tables: joined_tables.iter().map(|s| s.to_string()).collect(),
            available_columns: vec![],
        })?;

    // Find right column index in the new table
    let right_idx = new_table_schema.columns.iter()
        .position(|c| c.name.eq_ignore_ascii_case(right_col))
        .ok_or_else(|| ExecutorError::ColumnNotFound {
            column_name: right_col.clone(),
            table_name: new_table.to_string(),
            searched_tables: vec![new_table.to_string()],
            available_columns: new_table_schema.columns.iter().map(|c| c.name.clone()).collect(),
        })?;

    Ok((left_idx, right_idx))
}

/// Extract a single table name from a FROM clause if it's a simple table reference
///
/// Returns None if the FROM clause contains JOINs, subqueries, or other complex constructs.
fn extract_single_table_name(from_clause: &FromClause) -> Option<String> {
    match from_clause {
        FromClause::Table { name, .. } => Some(name.clone()),
        FromClause::Join { .. } => None, // JOINs not supported in native columnar path
        FromClause::Subquery { .. } => None, // Subqueries not supported
    }
}
