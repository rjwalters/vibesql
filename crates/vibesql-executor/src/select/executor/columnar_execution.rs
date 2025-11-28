//! Columnar execution integration for SelectExecutor
//!
//! This module integrates the columnar execution engine with the query executor,
//! providing automatic detection and execution of queries that can benefit from
//! SIMD-accelerated columnar processing.
//!
//! Note: This module is experimental/research code. Some methods are not yet
//! integrated into the main execution path.

#![allow(clippy::ptr_arg)]
#![allow(dead_code)]
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

use std::collections::HashMap;

use super::builder::SelectExecutor;
use crate::{
    errors::ExecutorError,
    optimizer::adaptive::{choose_execution_model, ExecutionModel},
    select::{columnar, cte::CteResult},
    schema::CombinedSchema,
};
use vibesql_ast::FromClause;

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

        // Phase 4: Execute SIMD-accelerated batch GROUP BY (no row conversion!)
        // This provides 3-5x speedup over the row-based path for TPC-H Q1
        #[cfg(feature = "simd")]
        let result = columnar::columnar_group_by_batch(&filtered_batch, &group_cols, &agg_cols)?;

        // Fallback to row-based GROUP BY when SIMD feature is disabled
        #[cfg(not(feature = "simd"))]
        let result = {
            let rows = filtered_batch.to_rows()?;
            columnar::columnar_group_by(&rows, &group_cols, &agg_cols, None)?
        };

        #[cfg(feature = "profile-q6")]
        {
            let group_time = group_start.elapsed();
            eprintln!(
                "[PROFILE-Q6]   GROUP BY Phase 2 - SIMD hash aggregation: {:?} ({} groups)",
                group_time,
                result.len()
            );
        }

        Ok(result)
    }
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
