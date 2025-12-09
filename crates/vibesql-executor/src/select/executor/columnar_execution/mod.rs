//! Columnar execution integration for SelectExecutor
//!
//! This module integrates the columnar execution engine with the query executor,
//! providing automatic detection and execution of queries that can benefit from
//! SIMD-accelerated columnar processing.
//!
//! Note: This module is experimental/research code. Some methods are not yet
//! integrated into the main execution path.
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
//!
//! ## Module Organization
//!
//! - `mod.rs` - Main entry points (try_columnar_execution, try_native_columnar_execution)
//! - `group_by.rs` - GROUP BY execution with CSE optimization
//! - `join.rs` - Multi-table JOIN execution
//! - `join_helpers.rs` - Helper functions for JOIN handling
//! - `cse.rs` - Common Sub-Expression Elimination helpers

#![allow(clippy::ptr_arg)]
#![allow(dead_code)]

mod cse;
mod group_by;
mod join;
mod join_helpers;

use std::collections::HashMap;

use super::builder::SelectExecutor;
use crate::{
    errors::ExecutorError,
    optimizer::adaptive::{choose_execution_model, ExecutionModel},
    schema::CombinedSchema,
    select::{columnar, cte::CteResult},
};
use vibesql_ast::Expression;

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
                log::debug!(
                    "  Columnar execution: Query eligible (adaptive execution selected columnar)"
                );
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
        // Note: Table elimination requires WHERE clause, so pass None for select_list too
        let from_result = self.execute_from_with_where(
            from_clause,
            cte_results,
            None, // Don't filter here - columnar module will handle it with SIMD
            None, // ORDER BY applied after aggregation
            None, // LIMIT applied after aggregation
            None, // No table elimination when WHERE is deferred
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
            }
            None => {
                #[cfg(feature = "profile-q6")]
                eprintln!("[PROFILE-Q6]   Reason: execute_columnar returned None (predicates or aggregates too complex)");
                Ok(None)
            } // Fall back to regular execution
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

        // Skip native columnar for correlated subqueries (#4111)
        // Correlated subqueries have outer_schema set and may have WHERE clauses
        // that reference outer columns (e.g., `J.I_CATEGORY = I.I_CATEGORY`).
        // The columnar predicate extraction doesn't handle outer column references,
        // so these predicates are silently dropped, causing incorrect results.
        if self.outer_schema.is_some() {
            log::debug!("Native columnar: skipping - correlated subquery with outer schema");
            return Ok(None);
        }

        // Must have a FROM clause with a single table
        let from_clause = match &stmt.from {
            Some(from) => from,
            None => return Ok(None),
        };

        // Extract table name and alias if this is a simple single-table scan
        // Issue #4111: We need both the table name (for database lookup) and the alias
        // (for schema key, since queries reference columns using the alias)
        let (table_name, table_alias) = match join_helpers::extract_table_name_and_alias(from_clause) {
            Some((name, alias)) => (name, alias),
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
        // Issue #4111: Use the alias as the schema key if one is provided, otherwise use table name.
        // This ensures queries like `SELECT J.I_CURRENT_PRICE FROM item J` can resolve J.I_CURRENT_PRICE
        // against the schema key "J" (not "item").
        let schema_key = table_alias.unwrap_or_else(|| table_name.clone());
        let schema = CombinedSchema::from_table(schema_key, table.schema.clone());

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
        let predicates = stmt
            .where_clause
            .as_ref()
            .and_then(|where_expr| columnar::extract_column_predicates(where_expr, &schema))
            .unwrap_or_default();

        // Extract select expressions
        // For GROUP BY queries, filter to only aggregate functions (GROUP BY columns are handled separately)
        let has_group_by = stmt.group_by.is_some();
        let select_exprs: Vec<_> = stmt
            .select_list
            .iter()
            .filter_map(|item| match item {
                vibesql_ast::SelectItem::Expression { expr, .. } => {
                    // For GROUP BY queries, skip non-aggregate expressions (they're GROUP BY columns)
                    if has_group_by && !matches!(expr, Expression::AggregateFunction { .. }) {
                        None
                    } else {
                        Some(expr.clone())
                    }
                }
                _ => None,
            })
            .collect();

        // Extract aggregates from select expressions
        let aggregates = match columnar::extract_aggregates(&select_exprs, &schema) {
            Some(aggs) if !aggs.is_empty() => aggs,
            _ => {
                log::debug!("Native columnar: skipping - no aggregates or unsupported expressions");
                return Ok(None);
            }
        };

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
            eprintln!("[PROFILE-Q6] Native columnar execution: {:?}", exec_time);
        }

        log::info!(
            "Native columnar execution completed: {} predicates, {} aggregates, group_by={}",
            predicates.len(),
            aggregates.len(),
            has_group_by
        );

        Ok(Some(result))
    }
}
