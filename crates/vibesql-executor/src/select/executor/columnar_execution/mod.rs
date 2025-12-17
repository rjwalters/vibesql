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
mod having;
mod join;
mod join_helpers;

use std::collections::HashMap;

use vibesql_ast::Expression;

use super::builder::SelectExecutor;
use crate::{
    errors::ExecutorError,
    optimizer::adaptive::{choose_execution_model, ExecutionModel},
    schema::CombinedSchema,
    select::{columnar, cte::CteResult},
};

/// Collect aggregate function expressions from an expression tree.
///
/// This is used to extract aggregates from HAVING clauses so they can be
/// included in columnar execution (Issue #4168).
fn collect_aggregates_from_expr(expr: &Expression) -> Vec<Expression> {
    let mut aggregates = vec![];
    collect_aggregates_recursive(expr, &mut aggregates);
    aggregates
}

fn collect_aggregates_recursive(expr: &Expression, aggregates: &mut Vec<Expression>) {
    match expr {
        Expression::AggregateFunction { .. } => {
            aggregates.push(expr.clone());
        }
        Expression::BinaryOp { left, right, .. } => {
            collect_aggregates_recursive(left, aggregates);
            collect_aggregates_recursive(right, aggregates);
        }
        Expression::UnaryOp { expr: inner, .. } => {
            collect_aggregates_recursive(inner, aggregates);
        }
        Expression::Case { operand, when_clauses, else_result } => {
            if let Some(op) = operand {
                collect_aggregates_recursive(op, aggregates);
            }
            for case_when in when_clauses {
                for cond in &case_when.conditions {
                    collect_aggregates_recursive(cond, aggregates);
                }
                collect_aggregates_recursive(&case_when.result, aggregates);
            }
            if let Some(else_expr) = else_result {
                collect_aggregates_recursive(else_expr, aggregates);
            }
        }
        Expression::Function { args, .. } => {
            for arg in args {
                collect_aggregates_recursive(arg, aggregates);
            }
        }
        Expression::InList { expr: inner, values, .. } => {
            collect_aggregates_recursive(inner, aggregates);
            for item in values {
                collect_aggregates_recursive(item, aggregates);
            }
        }
        Expression::Between { expr: inner, low, high, .. } => {
            collect_aggregates_recursive(inner, aggregates);
            collect_aggregates_recursive(low, aggregates);
            collect_aggregates_recursive(high, aggregates);
        }
        _ => {}
    }
}

/// Check if an expression contains aggregate functions (recursively)
///
/// This is used to detect expressions like `-AVG(col1)` which contain aggregates
/// but aren't direct `AggregateFunction` nodes.
fn contains_aggregate(expr: &Expression) -> bool {
    match expr {
        Expression::AggregateFunction { .. } => true,
        Expression::BinaryOp { left, right, .. } => {
            contains_aggregate(left) || contains_aggregate(right)
        }
        Expression::UnaryOp { expr: inner, .. } => contains_aggregate(inner),
        Expression::Function { args, .. } => args.iter().any(contains_aggregate),
        Expression::Case { operand, when_clauses, else_result, .. } => {
            operand.as_ref().is_some_and(|e| contains_aggregate(e))
                || when_clauses.iter().any(|clause| {
                    clause.conditions.iter().any(contains_aggregate)
                        || contains_aggregate(&clause.result)
                })
                || else_result.as_ref().is_some_and(|e| contains_aggregate(e))
        }
        Expression::InList { expr: inner, values, .. } => {
            contains_aggregate(inner) || values.iter().any(contains_aggregate)
        }
        Expression::Between { expr: inner, low, high, .. } => {
            contains_aggregate(inner) || contains_aggregate(low) || contains_aggregate(high)
        }
        Expression::Cast { expr: inner, .. } => contains_aggregate(inner),
        Expression::IsNull { expr: inner, .. } => contains_aggregate(inner),
        _ => false,
    }
}

/// Check if an expression contains any subqueries
fn contains_subquery(expr: &Expression) -> bool {
    match expr {
        // Subquery variants
        Expression::ScalarSubquery(_) => true,
        Expression::In { .. } => true,
        Expression::Exists { .. } => true,
        Expression::QuantifiedComparison { .. } => true,
        // Recursive checks
        Expression::BinaryOp { left, right, .. } => {
            contains_subquery(left) || contains_subquery(right)
        }
        Expression::UnaryOp { expr: inner, .. } => contains_subquery(inner),
        Expression::AggregateFunction { args, .. } => args.iter().any(contains_subquery),
        Expression::Function { args, .. } => args.iter().any(contains_subquery),
        Expression::Conjunction(exprs) | Expression::Disjunction(exprs) => {
            exprs.iter().any(contains_subquery)
        }
        Expression::Case { operand, when_clauses, else_result } => {
            operand.as_ref().is_some_and(|o| contains_subquery(o))
                || when_clauses.iter().any(|w| {
                    w.conditions.iter().any(contains_subquery) || contains_subquery(&w.result)
                })
                || else_result.as_ref().is_some_and(|e| contains_subquery(e))
        }
        Expression::InList { expr: inner, values, .. } => {
            contains_subquery(inner) || values.iter().any(contains_subquery)
        }
        Expression::Between { expr: inner, low, high, .. } => {
            contains_subquery(inner) || contains_subquery(low) || contains_subquery(high)
        }
        Expression::IsNull { expr: inner, .. } => contains_subquery(inner),
        Expression::Cast { expr: inner, .. } => contains_subquery(inner),
        _ => false,
    }
}

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
        let (table_name, table_alias) =
            match join_helpers::extract_table_name_and_alias(from_clause) {
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
        // Issue #4111: Use the alias as the schema key if one is provided, otherwise use table
        // name. This ensures queries like `SELECT J.I_CURRENT_PRICE FROM item J` can
        // resolve J.I_CURRENT_PRICE against the schema key "J" (not "item").
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
        // IMPORTANT: If we have a WHERE clause but can't extract predicates (e.g., IS NULL,
        // IS NOT NULL, or other unsupported expressions), we MUST fall back to row-based
        // execution. Using an empty predicate list would incorrectly skip filtering! (#4XXX)
        let predicates = match stmt.where_clause.as_ref() {
            Some(where_expr) => match columnar::extract_column_predicates(where_expr, &schema) {
                Some(preds) => preds,
                None => {
                    log::debug!(
                        "Native columnar: skipping - WHERE clause contains unsupported predicates"
                    );
                    return Ok(None);
                }
            },
            None => vec![],
        };

        // Extract select expressions
        // For GROUP BY queries, filter to only aggregate functions (GROUP BY columns are handled
        // separately)
        let has_group_by = stmt.group_by.is_some();
        let select_exprs: Vec<_> = stmt
            .select_list
            .iter()
            .filter_map(|item| match item {
                vibesql_ast::SelectItem::Expression { expr, .. } => {
                    // For GROUP BY queries, skip non-aggregate expressions (they're GROUP BY
                    // columns)
                    if has_group_by && !matches!(expr, Expression::AggregateFunction { .. }) {
                        None
                    } else {
                        Some(expr.clone())
                    }
                }
                _ => None,
            })
            .collect();

        // Also check HAVING clause for aggregates (Issue #4168)
        // Queries like "SELECT col FROM t GROUP BY col HAVING SUM(x) > N" have aggregates
        // only in HAVING, not in SELECT list. We need to include those for columnar execution.
        let having_aggregates = if let Some(having_expr) = &stmt.having {
            collect_aggregates_from_expr(having_expr)
        } else {
            vec![]
        };

        // Extract aggregates from select expressions
        // If extract_aggregates returns None, it means there are unsupported expressions
        // (like UnaryOp, Function, or DISTINCT) that the columnar path can't handle
        let select_aggregates = columnar::extract_aggregates(&select_exprs, &schema);
        let has_unsupported_select_aggregates = select_aggregates.is_none();
        let mut aggregates = select_aggregates.unwrap_or_default();

        // Add aggregates from HAVING if not already present
        let mut has_unsupported_having_aggregates = false;
        if !having_aggregates.is_empty() {
            let having_aggs = columnar::extract_aggregates(&having_aggregates, &schema);
            if having_aggs.is_none() {
                has_unsupported_having_aggregates = true;
            }
            if let Some(having_aggs) = having_aggs {
                for agg in having_aggs {
                    // Check if aggregate already exists (avoid duplicates)
                    if !aggregates.iter().any(|a| a.source == agg.source && a.op == agg.op) {
                        aggregates.push(agg);
                    }
                }
            }
        }

        // Fall back to row-oriented execution if:
        // 1. No aggregates and no GROUP BY (simple queries)
        // 2. Unsupported aggregate expressions in SELECT (UnaryOp, Function, DISTINCT, etc.)
        // 3. Unsupported aggregate expressions in HAVING
        if aggregates.is_empty() && !has_group_by {
            log::debug!("Native columnar: skipping - no aggregates");
            return Ok(None);
        }
        if has_unsupported_select_aggregates && has_group_by {
            log::debug!(
                "Native columnar: skipping - GROUP BY with unsupported aggregate expressions in SELECT"
            );
            return Ok(None);
        }
        if has_unsupported_having_aggregates {
            log::debug!(
                "Native columnar: skipping - unsupported aggregate expressions in HAVING"
            );
            return Ok(None);
        }

        // Check for subqueries in HAVING - not supported in columnar path
        if let Some(having_expr) = &stmt.having {
            if contains_subquery(having_expr) {
                log::debug!("Native columnar: skipping - HAVING contains subquery");
                return Ok(None);
            }
        }

        // Skip native columnar for complex GROUP BY (ROLLUP/CUBE/GROUPING SETS)
        // These require special handling that the columnar path doesn't support
        if let Some(ref group_by) = stmt.group_by {
            if group_by.as_simple().is_none() {
                log::debug!("Native columnar: skipping - ROLLUP/CUBE/GROUPING SETS not supported");
                return Ok(None);
            }
        }

        // Skip native columnar for GROUP BY with ORDER BY
        // The columnar GROUP BY path doesn't preserve ordering
        if has_group_by && stmt.order_by.is_some() {
            log::debug!("Native columnar: skipping - GROUP BY with ORDER BY not supported");
            return Ok(None);
        }

        // Skip native columnar for GROUP BY with complex SELECT expressions
        // The columnar GROUP BY path produces [group_key..., agg...] rows directly,
        // so it can't handle expressions that wrap aggregates (like -AVG(col1))
        // or SELECT lists that don't match the [group_keys, aggregates] format.
        // Issue #4XXX: Queries like "SELECT -AVG(col1), col1 FROM t GROUP BY col1"
        // need row-oriented execution to apply the final projection.
        if has_group_by {
            for item in &stmt.select_list {
                if let vibesql_ast::SelectItem::Expression { expr, .. } = item {
                    // If expression contains aggregate but isn't a direct AggregateFunction,
                    // we can't use columnar (e.g., -AVG(col), AVG(col) + 1)
                    if contains_aggregate(expr)
                        && !matches!(expr, Expression::AggregateFunction { .. })
                    {
                        log::debug!(
                            "Native columnar: skipping - SELECT contains expression wrapping aggregate"
                        );
                        return Ok(None);
                    }
                }
            }

            // Issue #4233, #4236: Validate SELECT list matches [group_keys..., aggregates...]
            // format columnar_group_by_batch returns rows with all group keys followed
            // by all aggregates. If the SELECT list doesn't have this exact structure
            // (e.g., only aggregates without group keys, or different column ordering),
            // we must fall back to row-oriented execution which applies proper
            // projection.
            //
            // IMPORTANT: Each non-aggregate SELECT expression must be IDENTICAL to a GROUP BY
            // expression. Expressions like `col1 * 11` where `col1` is the GROUP BY key are NOT
            // the same - they require post-projection computation that columnar doesn't support.
            let group_by_exprs_list = stmt
                .group_by
                .as_ref()
                .and_then(|g| g.as_simple())
                .map(|exprs| exprs.to_vec())
                .unwrap_or_default();

            // Collect non-aggregate SELECT expressions
            let mut select_non_agg_exprs: Vec<&Expression> = Vec::new();
            let mut select_agg_count = 0;
            for item in &stmt.select_list {
                if let vibesql_ast::SelectItem::Expression { expr, .. } = item {
                    if matches!(expr, Expression::AggregateFunction { .. }) {
                        select_agg_count += 1;
                    } else {
                        select_non_agg_exprs.push(expr);
                    }
                }
            }

            // SELECT list must have exactly: all group keys + all aggregates
            // If SELECT list has different number of non-aggregates than GROUP BY columns,
            // the projection won't match columnar_group_by_batch output format.
            if select_non_agg_exprs.len() != group_by_exprs_list.len() {
                log::debug!(
                    "Native columnar: skipping GROUP BY - SELECT list ({} non-aggs, {} aggs) doesn't match GROUP BY ({} keys)",
                    select_non_agg_exprs.len(),
                    select_agg_count,
                    group_by_exprs_list.len()
                );
                return Ok(None);
            }

            // Each non-aggregate SELECT expression must match a GROUP BY expression exactly
            // This catches cases like SELECT col1 * 11 FROM t GROUP BY col1 where col1 * 11 != col1
            use crate::select::grouping::expressions_equal;
            for select_expr in &select_non_agg_exprs {
                let matches_group_by = group_by_exprs_list
                    .iter()
                    .any(|group_expr| expressions_equal(select_expr, group_expr));
                if !matches_group_by {
                    log::debug!(
                        "Native columnar: skipping GROUP BY - SELECT expression {:?} is not a GROUP BY key",
                        select_expr
                    );
                    return Ok(None);
                }
            }

            // Issue #4510: columnar_group_by_batch returns [group_keys..., aggregates...] format.
            // If the SELECT list has aggregates before GROUP BY columns (e.g., SELECT count(*), col
            // FROM t GROUP BY col), the result order won't match. Fall back to row-oriented.
            // The SELECT list must have all non-aggregates first, then all aggregates.
            let mut first_agg_pos = None;
            let mut last_non_agg_pos = None;
            for (i, item) in stmt.select_list.iter().enumerate() {
                if let vibesql_ast::SelectItem::Expression { expr, .. } = item {
                    if matches!(expr, Expression::AggregateFunction { .. }) {
                        if first_agg_pos.is_none() {
                            first_agg_pos = Some(i);
                        }
                    } else {
                        last_non_agg_pos = Some(i);
                    }
                }
            }
            // If first aggregate comes before last non-aggregate, order doesn't match columnar
            if let (Some(first_agg), Some(last_non_agg)) = (first_agg_pos, last_non_agg_pos) {
                if first_agg < last_non_agg {
                    log::debug!(
                        "Native columnar: skipping GROUP BY - aggregate at position {} before non-aggregate at {}",
                        first_agg,
                        last_non_agg
                    );
                    return Ok(None);
                }
            }
        }

        // Execute using native columnar pipeline
        #[cfg(feature = "profile-q6")]
        let exec_start = std::time::Instant::now();

        let result = if has_group_by {
            // GROUP BY path: Use hash-based grouping
            // Catch unsupported feature errors and fall back to row-oriented execution
            let group_by_result = match self.execute_columnar_group_by(
                stmt,
                &batch,
                &predicates,
                &aggregates,
                &schema,
            ) {
                Ok(rows) => rows,
                Err(ExecutorError::Other(msg))
                    if msg.contains("not supported in columnar path") =>
                {
                    log::debug!("Native columnar: GROUP BY falling back to row-oriented: {}", msg);
                    return Ok(None);
                }
                Err(e) => return Err(e),
            };

            // Apply HAVING filter if present (Issue #4183)
            if let Some(having_expr) = &stmt.having {
                // Count GROUP BY columns to know where aggregates start in result rows
                let group_col_count = stmt
                    .group_by
                    .as_ref()
                    .and_then(|g| g.as_simple())
                    .map(|exprs| exprs.len())
                    .unwrap_or(0);

                log::debug!(
                    "Applying columnar HAVING filter: {} groups, {} group cols, {} aggregates",
                    group_by_result.len(),
                    group_col_count,
                    aggregates.len()
                );

                // Catch unsupported feature errors in HAVING and fall back
                match having::apply_having_filter(
                    group_by_result,
                    having_expr,
                    group_col_count,
                    &aggregates,
                    &schema,
                ) {
                    Ok(rows) => rows,
                    // having.rs throws UnsupportedFeature for DISTINCT aggregates
                    Err(ExecutorError::UnsupportedFeature(msg))
                        if msg.contains("not supported in columnar")
                            || msg.contains("not found in computed aggregates") =>
                    {
                        log::debug!(
                            "Native columnar: HAVING falling back to row-oriented: {}",
                            msg
                        );
                        return Ok(None);
                    }
                    // group_by.rs throws Other for unsupported features
                    Err(ExecutorError::Other(msg))
                        if msg.contains("not supported in columnar path")
                            || msg.contains("not found in computed aggregates") =>
                    {
                        log::debug!(
                            "Native columnar: HAVING falling back to row-oriented: {}",
                            msg
                        );
                        return Ok(None);
                    }
                    Err(e) => return Err(e),
                }
            } else {
                group_by_result
            }
        } else {
            // Non-GROUP BY path: Simple aggregation
            let agg_result =
                columnar::execute_columnar_batch(&batch, &predicates, &aggregates, Some(&schema))?;

            // Apply HAVING filter if present (same as GROUP BY path)
            // For non-GROUP BY, there are no group columns, so group_col_count = 0
            if let Some(having_expr) = &stmt.having {
                log::debug!(
                    "Applying columnar HAVING filter (no GROUP BY): {} rows, {} aggregates",
                    agg_result.len(),
                    aggregates.len()
                );

                match having::apply_having_filter(
                    agg_result,
                    having_expr,
                    0, // No GROUP BY columns
                    &aggregates,
                    &schema,
                ) {
                    Ok(rows) => rows,
                    Err(ExecutorError::UnsupportedFeature(msg))
                        if msg.contains("not supported in columnar")
                            || msg.contains("not found in computed aggregates") =>
                    {
                        log::debug!(
                            "Native columnar: HAVING (no GROUP BY) falling back to row-oriented: {}",
                            msg
                        );
                        return Ok(None);
                    }
                    Err(ExecutorError::Other(msg))
                        if msg.contains("not supported in columnar path")
                            || msg.contains("not found in computed aggregates") =>
                    {
                        log::debug!(
                            "Native columnar: HAVING (no GROUP BY) falling back to row-oriented: {}",
                            msg
                        );
                        return Ok(None);
                    }
                    Err(e) => return Err(e),
                }
            } else {
                agg_result
            }
        };

        #[cfg(feature = "profile-q6")]
        {
            let exec_time = exec_start.elapsed();
            eprintln!("[PROFILE-Q6] Native columnar execution: {:?}", exec_time);
        }

        log::info!(
            "Native columnar execution completed: {} predicates, {} aggregates, group_by={}, having={}",
            predicates.len(),
            aggregates.len(),
            has_group_by,
            stmt.having.is_some()
        );

        // Apply LIMIT/OFFSET to the result
        let limit = crate::select::helpers::evaluate_limit(&stmt.limit, self.database)?;
        let offset = crate::select::helpers::evaluate_offset(&stmt.offset, self.database)?;
        let final_result = crate::select::helpers::apply_limit_offset(result, limit, offset);

        Ok(Some(final_result))
    }
}
