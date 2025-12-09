//! Fast path execution for simple point-lookup queries
//!
//! This module provides an optimized execution path for simple OLTP queries that:
//! - Query a single table (no JOINs)
//! - Have no subqueries
//! - Have no aggregates, window functions, or GROUP BY
//! - Have simple column references in SELECT
//! - Have simple equality predicates in WHERE
//! - Have simple ORDER BY clauses (column references only, no expressions)
//!
//! These queries skip expensive optimizer passes and go directly to index scan,
//! providing 5-10x speedup for TPC-C style point lookups.
//!
//! # Streaming Aggregation (#3815)
//!
//! For queries like `SELECT SUM(k) FROM sbtest1 WHERE id BETWEEN ? AND ?`,
//! we provide an ultra-fast streaming aggregation path that:
//! - Accumulates aggregates inline during PK range scan
//! - Never materializes intermediate row objects
//! - Achieves SQLite-like performance (~4μs vs 30μs for standard path)
//!
//! # Lookup Strategies
//!
//! The fast path tries these lookup strategies in order:
//!
//! 1. **Primary Key Lookup** (`try_pk_lookup_fast`): Direct O(1) lookup when
//!    WHERE clause has equality predicates for all PK columns.
//!
//! 2. **Secondary Index Lookup** (`try_secondary_index_lookup_fast`): O(log n)
//!    lookup when WHERE clause has equality predicates for all columns of a
//!    secondary index. Handles queries like:
//!    `SELECT * FROM customer WHERE c_w_id = 1 AND c_d_id = 2 AND c_last = 'SMITH'`
//!
//! 3. **Standard Scan**: Falls back to execute_from_clause for other queries.
//!
//! # ORDER BY Support
//!
//! The fast path supports ORDER BY with simple column references:
//! - If an index exists that matches the ORDER BY column(s), results are
//!   returned pre-sorted from the index scan (zero-cost sorting)
//! - If no matching index exists, explicit sorting is applied after filtering
//!
//! # Performance Impact
//!
//! For a query like `SELECT w_tax FROM warehouse WHERE w_id = 1`:
//! - Standard path: ~1200us (optimizer passes, strategy selection, pipeline creation)
//! - Fast path: ~50-100us (direct index scan, minimal overhead)
//!
//! For secondary index lookups (TPC-C customer-by-last-name):
//! - Standard path: ~4000-5000us (full scan machinery)
//! - Fast path: ~100-200us (direct index lookup)
//!
//! # Example Queries
//!
//! ```sql
//! -- These queries use the fast path:
//! SELECT col FROM table WHERE pk = 1
//! SELECT col1, col2 FROM table WHERE pk1 = 1 AND pk2 = 2
//! SELECT * FROM table WHERE id = 123
//! SELECT no_o_id FROM new_order WHERE no_w_id = 1 ORDER BY no_o_id  -- with ORDER BY
//! SELECT * FROM customer WHERE c_w_id = 1 AND c_d_id = 2 AND c_last = 'SMITH'  -- secondary index
//!
//! -- These queries use the standard path:
//! SELECT COUNT(*) FROM table WHERE id = 1  -- aggregate
//! SELECT a FROM t1, t2 WHERE t1.id = t2.id  -- join
//! SELECT a FROM t WHERE id IN (SELECT id FROM t2)  -- subquery
//! SELECT a FROM t ORDER BY UPPER(a)  -- complex ORDER BY expression
//! ```
//!
//! # Module Structure
//!
//! - `analysis`: Query analysis functions to detect fast-path eligibility
//! - `helpers`: Shared utilities for projection, filtering, sorting
//! - `pk_lookup`: Primary key lookup strategies
//! - `index_lookup`: Secondary index lookup strategies
//! - `range_scan`: PK range scan with early projection
//! - `streaming_agg`: Streaming aggregation execution

use std::collections::HashMap;

use vibesql_ast::SelectStmt;
use vibesql_storage::Row;

use super::builder::SelectExecutor;
use crate::errors::ExecutorError;

// Submodules
pub(crate) mod analysis;
mod helpers;
mod index_lookup;
mod pk_lookup;
mod range_scan;
mod streaming_agg;

// Re-export public API
pub use analysis::{is_simple_point_query, is_streaming_aggregate_query};

impl SelectExecutor<'_> {
    /// Execute a query using the fast path
    ///
    /// This bypasses the optimizer infrastructure and goes directly to table scan
    /// with optional index optimization.
    ///
    /// # Performance Note (#3780)
    ///
    /// This method is called by `Session::execute_prepared()` for queries using
    /// `SimpleFastPath` cached plans. It executes the query and returns just the
    /// rows, leaving column name resolution to the cached plan.
    pub fn execute_fast_path(&self, stmt: &SelectStmt) -> Result<Vec<Row>, ExecutorError> {
        // Extract table name from FROM clause
        let (table_name, alias) = match &stmt.from {
            Some(vibesql_ast::FromClause::Table { name, alias, .. }) => {
                (name.as_str(), alias.as_ref())
            }
            _ => unreachable!("Fast path requires simple table FROM clause"),
        };

        // Try ultra-fast PK lookup path first
        if let Some(result) = self.try_pk_lookup_fast(table_name, alias, stmt)? {
            return Ok(result);
        }

        // Try PK prefix lookup with early LIMIT termination (TPC-C Delivery optimization)
        if let Some(result) = self.try_pk_prefix_with_limit_fast(table_name, alias, stmt)? {
            return Ok(result);
        }

        // Try secondary index prefix lookup with ORDER BY + LIMIT (TPC-C Order-Status optimization)
        if let Some(result) =
            self.try_secondary_index_prefix_with_limit_fast(table_name, alias, stmt)?
        {
            return Ok(result);
        }

        // Try secondary index lookup path next
        if let Some(result) = self.try_secondary_index_lookup_fast(table_name, alias, stmt)? {
            return Ok(result);
        }

        // Try covering index scan (index-only scan) for queries where all SELECT columns
        // are in the index key - eliminates table fetches entirely
        if let Some(result) = self.try_covering_index_scan_fast(table_name, alias, stmt)? {
            return Ok(result);
        }

        // Try PK range scan with early projection (#3799)
        // For simple range queries like `SELECT c FROM t WHERE pk BETWEEN ? AND ?`,
        // use streaming scan with early projection to avoid cloning unneeded columns.
        if let Some(result) = self.try_pk_range_scan_with_early_projection(table_name, stmt)? {
            return Ok(result);
        }

        // Fall back to standard fast path with execute_from_clause
        // Pass LIMIT for early termination optimization (#3253)
        let from_result = crate::select::scan::execute_from_clause(
            stmt.from.as_ref().unwrap(),
            &HashMap::new(), // No CTEs
            self.database,
            stmt.where_clause.as_ref(),
            stmt.order_by.as_deref(),
            stmt.limit, // LIMIT pushdown for ORDER BY optimization
            None,       // No outer row
            None,       // No outer schema
            |_| unreachable!("Fast path doesn't support subqueries"),
        )?;

        let schema = from_result.schema.clone();
        let where_filtered = from_result.where_filtered;
        let sorted_by = from_result.sorted_by.clone();
        let rows = from_result.into_rows();

        // Apply remaining WHERE clause if not already filtered
        let filtered_rows = if where_filtered || stmt.where_clause.is_none() {
            rows
        } else {
            self.apply_where_filter_fast(stmt.where_clause.as_ref().unwrap(), rows, &schema)?
        };

        // Apply ORDER BY sorting if needed (index didn't provide the order)
        let sorted_rows = if let Some(order_by) = &stmt.order_by {
            if analysis::needs_sorting(order_by, &sorted_by) {
                self.apply_order_by_fast(order_by, filtered_rows, &schema)?
            } else {
                filtered_rows
            }
        } else {
            filtered_rows
        };

        // Apply projection
        let projected_rows = self.apply_projection_fast(&stmt.select_list, sorted_rows, &schema)?;

        // Apply LIMIT/OFFSET
        let final_rows =
            crate::select::helpers::apply_limit_offset(projected_rows, stmt.limit, stmt.offset);

        Ok(final_rows)
    }
}
