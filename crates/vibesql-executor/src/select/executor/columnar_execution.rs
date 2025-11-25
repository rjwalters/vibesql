//! Columnar execution integration for SelectExecutor
//!
//! This module integrates the columnar execution engine with the query executor,
//! providing automatic detection and execution of queries that can benefit from
//! SIMD-accelerated columnar processing.

use std::collections::HashMap;

use super::builder::SelectExecutor;
use crate::{
    errors::ExecutorError,
    optimizer::adaptive::{choose_execution_model, ExecutionModel},
    select::{columnar, cte::CteResult},
};

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
}
