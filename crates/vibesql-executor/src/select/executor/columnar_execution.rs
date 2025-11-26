//! Columnar execution integration for SelectExecutor
//!
//! This module integrates the columnar execution engine with the query executor,
//! providing automatic detection and execution of queries that can benefit from
//! SIMD-accelerated columnar processing.
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
    /// Try to execute using columnar (SIMD-accelerated) execution
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
        // Check if native columnar execution is enabled via feature flag
        if !cfg!(feature = "native-columnar") && std::env::var("VIBESQL_NATIVE_COLUMNAR").is_err() {
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

        // Execute using native columnar pipeline
        #[cfg(feature = "profile-q6")]
        let exec_start = std::time::Instant::now();

        let result = columnar::execute_columnar_batch(&batch, &predicates, &aggregates, Some(&schema))?;

        #[cfg(feature = "profile-q6")]
        {
            let exec_time = exec_start.elapsed();
            eprintln!(
                "[PROFILE-Q6] Native columnar execution: {:?}",
                exec_time
            );
        }

        log::info!(
            "Native columnar execution completed: {} predicates, {} aggregates",
            predicates.len(),
            aggregates.len()
        );

        Ok(Some(result))
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
