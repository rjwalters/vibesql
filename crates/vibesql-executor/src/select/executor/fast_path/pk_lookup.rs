//! Primary key lookup strategies for fast path execution
//!
//! This module provides optimized execution paths for queries that can use
//! primary key lookups:
//! - Direct PK point lookup (single row)
//! - PK prefix lookup with ORDER BY + LIMIT

use vibesql_ast::{Expression, SelectItem, SelectStmt};
use vibesql_storage::Row;
use vibesql_types::SqlValue;

use super::helpers::EqualityResult;
use crate::errors::ExecutorError;
use crate::schema::CombinedSchema;
use crate::select::executor::builder::SelectExecutor;

impl SelectExecutor<'_> {
    /// Try ultra-fast primary key lookup path
    ///
    /// Returns Some(rows) if we can use direct PK lookup, None if we need standard path.
    /// This is the fastest path for simple queries like `SELECT * FROM t WHERE pk = 1`.
    pub(crate) fn try_pk_lookup_fast(
        &self,
        table_name: &str,
        alias: Option<&String>,
        stmt: &SelectStmt,
    ) -> Result<Option<Vec<Row>>, ExecutorError> {
        // Need a WHERE clause for PK lookup
        let where_clause = match &stmt.where_clause {
            Some(w) => w,
            None => return Ok(None),
        };

        // Get table to check PK columns
        // If it's not a table (e.g., it's a view), fall back to standard path
        let table = match self.database.get_table(table_name) {
            Some(t) => t,
            None => return Ok(None), // Not a table - could be a view, use standard path
        };

        // Get primary key column names from schema
        let pk_column_names = match &table.schema.primary_key {
            Some(cols) if !cols.is_empty() => cols,
            _ => return Ok(None), // No PK to use
        };

        let pk_columns: Vec<&str> = pk_column_names.iter().map(|s| s.as_str()).collect();

        // Try to extract equality predicates for PK columns from WHERE clause
        let pk_values = match self.extract_pk_values(where_clause, &pk_columns) {
            EqualityResult::Contradiction => {
                // Multiple equalities on same column with different values
                // This is always false, return empty result
                return Ok(Some(vec![]));
            }
            EqualityResult::Values(v) => v,
        };

        // Check if we have values for all PK columns
        if pk_values.len() != pk_columns.len() {
            return Ok(None); // Can't use PK lookup
        }

        // Build PK values in column order (use lowercase for lookup to match insert)
        let pk_key: Vec<SqlValue> = pk_columns
            .iter()
            .filter_map(|col| pk_values.get(&col.to_ascii_lowercase()).cloned())
            .collect();

        if pk_key.len() != pk_columns.len() {
            return Ok(None);
        }

        // Direct PK lookup - O(log n)
        let row = if pk_key.len() == 1 {
            self.database
                .get_row_by_pk(table_name, &pk_key[0])
                .map_err(|e| ExecutorError::StorageError(e.to_string()))?
        } else {
            self.database
                .get_row_by_composite_pk(table_name, &pk_key)
                .map_err(|e| ExecutorError::StorageError(e.to_string()))?
        };

        // If no row found, return empty result
        let row = match row {
            Some(r) => r,
            None => return Ok(Some(vec![])),
        };

        // Check if we need projection
        let is_select_star = stmt.select_list.len() == 1
            && matches!(&stmt.select_list[0], SelectItem::Wildcard { .. });

        if is_select_star {
            // No projection needed for SELECT * - clone the full row
            return Ok(Some(vec![row.clone()]));
        }

        // Try ultra-fast direct column projection (no full row clone)
        // Only clone the columns we actually need
        if let Some(col_indices) =
            self.try_extract_simple_column_indices(&stmt.select_list, &table.schema)
        {
            let projected_values: Vec<SqlValue> =
                col_indices.iter().map(|&idx| row.values[idx].clone()).collect();
            return Ok(Some(vec![Row::from_vec(projected_values)]));
        }

        // Fall back to full projection with evaluator for complex expressions
        let effective_name = alias.cloned().unwrap_or_else(|| table_name.to_string());
        let schema = CombinedSchema::from_table(effective_name, table.schema.clone());

        // Apply projection
        let projected =
            self.apply_projection_fast(&stmt.select_list, vec![row.clone()], &schema)?;
        Ok(Some(projected))
    }

    /// Try PK prefix lookup with early LIMIT termination
    ///
    /// This optimization handles queries like TPC-C Delivery:
    /// `SELECT no_o_id FROM new_order WHERE no_w_id = 1 AND no_d_id = 5 ORDER BY no_o_id LIMIT 1`
    /// `SELECT no_o_id FROM new_order WHERE no_w_id = 1 AND no_d_id = 5 ORDER BY no_o_id DESC LIMIT 1`
    ///
    /// For tables with composite PK (no_w_id, no_d_id, no_o_id), this query:
    /// 1. Filters by prefix of PK (no_w_id, no_d_id)
    /// 2. Orders by the remaining PK column (no_o_id) - ASC or DESC
    /// 3. Uses LIMIT 1
    ///
    /// The optimization uses prefix_scan_first (ASC) or prefix_scan_reverse_limit (DESC)
    /// to return just the first/last matching row, avoiding fetching all matching rows.
    ///
    /// # Performance
    /// - Before: O(log n + k) to fetch all k matching rows, then sort, then take 1
    /// - After: O(log n) to fetch just the first/last matching row (already sorted in index)
    pub(crate) fn try_pk_prefix_with_limit_fast(
        &self,
        table_name: &str,
        alias: Option<&String>,
        stmt: &SelectStmt,
    ) -> Result<Option<Vec<Row>>, ExecutorError> {
        // Only applies when LIMIT 1 is specified (most common case for this pattern)
        if stmt.limit != Some(1) {
            return Ok(None);
        }

        // Must have an ORDER BY clause
        let order_by = match &stmt.order_by {
            Some(ob) if !ob.is_empty() => ob,
            _ => return Ok(None),
        };

        // Must have a WHERE clause
        let where_clause = match &stmt.where_clause {
            Some(w) => w,
            None => return Ok(None),
        };

        // Get the table
        let table = match self.database.get_table(table_name) {
            Some(t) => t,
            None => return Ok(None),
        };

        // Get primary key columns
        let pk_column_names = match &table.schema.primary_key {
            Some(cols) if cols.len() >= 2 => cols, // Need at least 2 columns for prefix pattern
            _ => return Ok(None),
        };

        let pk_columns: Vec<&str> = pk_column_names.iter().map(|s| s.as_str()).collect();

        // Extract equality predicates from WHERE clause
        let equality_values = match self.extract_pk_values(where_clause, &pk_columns) {
            EqualityResult::Contradiction => {
                // Multiple equalities on same column with different values
                // This is always false, return empty result
                return Ok(Some(vec![]));
            }
            EqualityResult::Values(v) => v,
        };

        // Check if we have a prefix match (equality on first N-1 columns)
        // For a 3-column PK (a, b, c), we need equality on (a, b) and ORDER BY c
        let prefix_len = pk_columns.len() - 1;
        if equality_values.len() != prefix_len {
            return Ok(None);
        }

        // Verify we have equality values for the first N-1 columns (in order)
        // Use lowercase for lookup to match how extract_pk_values stores keys
        let mut prefix_key = Vec::with_capacity(prefix_len);
        for col in pk_columns.iter().take(prefix_len) {
            match equality_values.get(&col.to_ascii_lowercase()) {
                Some(val) => prefix_key.push(val.clone()),
                None => return Ok(None), // Missing a prefix column
            }
        }

        // Verify ORDER BY is on the last PK column
        let last_pk_col = pk_columns.last().unwrap();
        let order_col = match &order_by[0].expr {
            Expression::ColumnRef { column, .. } => column.as_str(),
            _ => return Ok(None),
        };

        if !order_col.eq_ignore_ascii_case(last_pk_col) {
            return Ok(None);
        }

        let is_desc = order_by[0].direction == vibesql_ast::OrderDirection::Desc;

        // Get PK index from database's index infrastructure (pk_{table_name})
        let pk_index_name = format!("pk_{}", table_name);
        let pk_index_data = match self.database.get_index_data(&pk_index_name) {
            Some(idx) => idx,
            None => return Ok(None),
        };

        // Use prefix_scan_first for ASC, prefix_scan_reverse_limit for DESC
        let row_idx = if is_desc {
            // For DESC order, get the last matching row using reverse scan
            let results = pk_index_data.prefix_scan_reverse_limit(&prefix_key, 1);
            match results.first() {
                Some(&idx) => idx,
                None => return Ok(Some(vec![])),
            }
        } else {
            // For ASC order, prefix_scan_first gives us the minimum
            match pk_index_data.prefix_scan_first(&prefix_key) {
                Some(idx) => idx,
                None => return Ok(Some(vec![])),
            }
        };

        // Fetch the single row
        // Issue #3790: Use get_row() which returns None for deleted rows
        let row = match table.get_row(row_idx) {
            Some(r) => r.clone(),
            None => return Ok(Some(vec![])), // Row deleted or invalid index
        };

        // Build schema for projection
        let effective_name = alias.cloned().unwrap_or_else(|| table_name.to_string());
        let schema = CombinedSchema::from_table(effective_name, table.schema.clone());

        // Apply projection
        let is_select_star = stmt.select_list.len() == 1
            && matches!(&stmt.select_list[0], SelectItem::Wildcard { .. });

        if is_select_star {
            return Ok(Some(vec![row]));
        }

        let projected = self.apply_projection_fast(&stmt.select_list, vec![row], &schema)?;
        Ok(Some(projected))
    }
}
