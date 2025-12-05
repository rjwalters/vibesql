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

use std::collections::HashMap;

use vibesql_ast::{Expression, SelectItem, SelectStmt};
use vibesql_storage::Row;
use vibesql_types::SqlValue;

use super::builder::SelectExecutor;
use crate::{errors::ExecutorError, schema::CombinedSchema};

/// Check if a query is a simple point-lookup that can use the fast path
///
/// Returns true for queries that:
/// 1. Query a single table (no joins, no subqueries in FROM)
/// 2. Have no WITH clause (CTEs)
/// 3. Have no aggregates or window functions
/// 4. Have no GROUP BY, HAVING, DISTINCT, or set operations
/// 5. Have no ORDER BY with complex expressions
/// 6. Have a simple WHERE clause (only AND-connected equality predicates)
pub fn is_simple_point_query(stmt: &SelectStmt) -> bool {
    // No CTEs
    if stmt.with_clause.is_some() {
        return false;
    }

    // No set operations (UNION, INTERSECT, EXCEPT)
    if stmt.set_operation.is_some() {
        return false;
    }

    // No SELECT INTO (DDL or procedural variable assignment)
    // These require special handling that the fast path doesn't support
    if stmt.into_table.is_some() || stmt.into_variables.is_some() {
        return false;
    }

    // No GROUP BY, HAVING, or DISTINCT
    if stmt.group_by.is_some() || stmt.having.is_some() || stmt.distinct {
        return false;
    }

    // Must have a FROM clause
    let Some(from) = &stmt.from else {
        return false;
    };

    // FROM must be a simple table (no joins, no subqueries)
    if !matches!(from, vibesql_ast::FromClause::Table { .. }) {
        return false;
    }

    // SELECT list must be simple columns or * (no aggregates, no subqueries)
    if !has_simple_select_list(&stmt.select_list) {
        return false;
    }

    // WHERE clause must be simple equality predicates (if present)
    if let Some(where_clause) = &stmt.where_clause {
        if !is_simple_where_clause(where_clause) {
            return false;
        }
    }

    // ORDER BY is allowed if it's simple (column references only, no complex expressions)
    // and doesn't use SELECT list aliases (which require post-projection sorting)
    // The index scan logic will automatically use index ordering when possible
    if let Some(order_by) = &stmt.order_by {
        if !is_simple_order_by(order_by) {
            return false;
        }
        // Check that ORDER BY doesn't use SELECT list aliases
        // Fast path sorts before projection, so aliases can't be resolved
        if uses_select_alias(order_by, &stmt.select_list) {
            return false;
        }
    }

    true
}

/// Check if an ORDER BY clause is simple enough for the fast path
///
/// Returns true if all ORDER BY items are simple column references.
/// Complex expressions (functions, arithmetic, subqueries) are not supported.
///
/// Examples:
/// - `ORDER BY col ASC` -> true
/// - `ORDER BY col1, col2 DESC` -> true
/// - `ORDER BY col LIMIT 1` -> true (LIMIT doesn't affect ORDER BY simplicity)
/// - `ORDER BY UPPER(col)` -> false (function call)
/// - `ORDER BY col + 1` -> false (arithmetic expression)
fn is_simple_order_by(order_by: &[vibesql_ast::OrderByItem]) -> bool {
    for item in order_by {
        // ORDER BY expression must be a simple column reference
        if !matches!(item.expr, Expression::ColumnRef { .. }) {
            return false;
        }
    }
    true
}

/// Check if ORDER BY uses any SELECT list aliases
///
/// Returns true if any ORDER BY column matches a SELECT list alias.
/// This is used to exclude such queries from the fast path, since
/// the fast path sorts before projection and can't resolve aliases.
fn uses_select_alias(order_by: &[vibesql_ast::OrderByItem], select_list: &[SelectItem]) -> bool {
    // Collect all aliases from the SELECT list
    let aliases: Vec<&str> = select_list
        .iter()
        .filter_map(|item| {
            if let SelectItem::Expression { alias: Some(alias), .. } = item {
                Some(alias.as_str())
            } else {
                None
            }
        })
        .collect();

    // If no aliases, no conflict possible
    if aliases.is_empty() {
        return false;
    }

    // Check if any ORDER BY column matches an alias
    for item in order_by {
        if let Expression::ColumnRef { table: None, column } = &item.expr {
            // Case-insensitive comparison for SQL identifiers
            if aliases.iter().any(|alias| alias.eq_ignore_ascii_case(column)) {
                return true;
            }
        }
    }

    false
}

/// Check if we need to apply explicit sorting (index didn't provide the order)
///
/// Returns true if ORDER BY columns don't match the sorted_by metadata from index scan.
fn needs_sorting(
    order_by: &[vibesql_ast::OrderByItem],
    sorted_by: &Option<Vec<(String, vibesql_ast::OrderDirection)>>,
) -> bool {
    let Some(sorted_cols) = sorted_by else {
        return true; // No sorting metadata, need to sort
    };

    // Check if ORDER BY is a prefix of sorted_by with matching directions
    if order_by.len() > sorted_cols.len() {
        return true; // ORDER BY has more columns than sorted
    }

    for (order_item, (col_name, col_dir)) in order_by.iter().zip(sorted_cols.iter()) {
        // Extract column name from ORDER BY expression
        let order_col = match &order_item.expr {
            Expression::ColumnRef { column, .. } => column,
            _ => return true, // Non-column expression, need to sort
        };

        // Check column name matches (case-insensitive)
        if !order_col.eq_ignore_ascii_case(col_name) {
            return true;
        }

        // Check direction matches
        if &order_item.direction != col_dir {
            return true;
        }
    }

    false // Sorting is already satisfied
}

/// Check if a SELECT list contains only simple columns or *
fn has_simple_select_list(select_list: &[SelectItem]) -> bool {
    for item in select_list {
        match item {
            SelectItem::Wildcard { .. } | SelectItem::QualifiedWildcard { .. } => continue,
            SelectItem::Expression { expr, .. } => {
                if !is_simple_expression(expr) {
                    return false;
                }
            }
        }
    }
    true
}

/// Check if an expression is simple (column ref, literal, or basic arithmetic)
fn is_simple_expression(expr: &Expression) -> bool {
    match expr {
        Expression::ColumnRef { .. } | Expression::Literal(_) => true,
        Expression::BinaryOp { left, right, op } => {
            // Allow simple arithmetic on columns/literals
            matches!(
                op,
                vibesql_ast::BinaryOperator::Plus
                    | vibesql_ast::BinaryOperator::Minus
                    | vibesql_ast::BinaryOperator::Multiply
                    | vibesql_ast::BinaryOperator::Divide
                    | vibesql_ast::BinaryOperator::Concat
            ) && is_simple_expression(left)
                && is_simple_expression(right)
        }
        Expression::UnaryOp { expr, .. } => is_simple_expression(expr),
        Expression::Cast { expr, .. } => is_simple_expression(expr),
        // Functions are not simple (could be aggregates or expensive)
        _ => false,
    }
}

/// Check if a WHERE clause is simple (only AND-connected equality/comparison predicates)
fn is_simple_where_clause(expr: &Expression) -> bool {
    match expr {
        // Simple comparison: col = val, col > val, etc.
        Expression::BinaryOp { left, op, right } => {
            match op {
                vibesql_ast::BinaryOperator::Equal
                | vibesql_ast::BinaryOperator::NotEqual
                | vibesql_ast::BinaryOperator::GreaterThan
                | vibesql_ast::BinaryOperator::GreaterThanOrEqual
                | vibesql_ast::BinaryOperator::LessThan
                | vibesql_ast::BinaryOperator::LessThanOrEqual => {
                    // Must be column vs literal (not column vs column for join conditions)
                    is_column_or_literal(left) && is_column_or_literal(right)
                }
                vibesql_ast::BinaryOperator::And => {
                    // AND is fine - recurse
                    is_simple_where_clause(left) && is_simple_where_clause(right)
                }
                // OR could be optimized but is more complex
                vibesql_ast::BinaryOperator::Or => false,
                _ => false,
            }
        }
        // BETWEEN is simple
        Expression::Between { expr, low, high, .. } => {
            is_column_or_literal(expr) && is_column_or_literal(low) && is_column_or_literal(high)
        }
        // IN list is simple (not IN subquery)
        Expression::InList { expr, values, .. } => {
            is_column_or_literal(expr) && values.iter().all(is_column_or_literal)
        }
        // IS NULL is simple
        Expression::IsNull { expr, .. } => is_column_or_literal(expr),
        // LIKE is simple
        Expression::Like { expr, pattern, .. } => {
            is_column_or_literal(expr) && is_column_or_literal(pattern)
        }
        _ => false,
    }
}

/// Check if an expression is a column reference or literal
fn is_column_or_literal(expr: &Expression) -> bool {
    matches!(expr, Expression::ColumnRef { .. } | Expression::Literal(_))
}

/// Result of extracting equality predicate values from WHERE clause
///
/// Distinguishes between:
/// - `Values(map)`: Successfully extracted equality values
/// - `Contradiction`: Multiple equality predicates on same column with different values
///   (e.g., col = 1 AND col = 2 is always false)
enum EqualityResult {
    Values(HashMap<String, vibesql_types::SqlValue>),
    Contradiction,
}

impl SelectExecutor<'_> {
    /// Execute a query using the fast path
    ///
    /// This bypasses the optimizer infrastructure and goes directly to table scan
    /// with optional index optimization.
    pub(super) fn execute_fast_path(&self, stmt: &SelectStmt) -> Result<Vec<Row>, ExecutorError> {
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
            if needs_sorting(order_by, &sorted_by) {
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

    /// Try ultra-fast primary key lookup path
    ///
    /// Returns Some(rows) if we can use direct PK lookup, None if we need standard path.
    /// This is the fastest path for simple queries like `SELECT * FROM t WHERE pk = 1`.
    fn try_pk_lookup_fast(
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
        let pk_key: Vec<vibesql_types::SqlValue> = pk_columns
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
            return Ok(Some(vec![Row { values: projected_values }]));
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
    fn try_pk_prefix_with_limit_fast(
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
        let all_rows = table.scan();
        let row = match all_rows.get(row_idx) {
            Some(r) => r.clone(),
            None => return Ok(Some(vec![])), // Invalid row index
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

    /// Try secondary index prefix lookup with ORDER BY and LIMIT optimization
    ///
    /// Returns Some(rows) if we can use the optimized path, None if we need standard path.
    /// This handles queries like:
    /// `SELECT o_id FROM orders WHERE o_w_id = 1 AND o_d_id = 2 AND o_c_id = 3 ORDER BY o_id DESC LIMIT 1`
    /// when there's a secondary index on (o_w_id, o_d_id, o_c_id, o_id).
    ///
    /// The optimization detects when:
    /// 1. WHERE has equality predicates for first N columns of an index
    /// 2. ORDER BY is on the (N+1)th column of the index
    /// 3. LIMIT is specified (optimized for LIMIT 1)
    ///
    /// # Performance
    /// For `ORDER BY col DESC LIMIT 1`, uses `prefix_scan_reverse_limit` which is O(log n)
    /// instead of O(log n + k) where k is matching rows.
    fn try_secondary_index_prefix_with_limit_fast(
        &self,
        table_name: &str,
        alias: Option<&String>,
        stmt: &SelectStmt,
    ) -> Result<Option<Vec<Row>>, ExecutorError> {
        // Only applies when LIMIT is specified
        let limit = match stmt.limit {
            Some(l) if l > 0 => l,
            _ => return Ok(None),
        };

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

        // Get all secondary indexes for this table
        let index_names = self.database.list_indexes_for_table(table_name);
        if index_names.is_empty() {
            return Ok(None);
        }

        // Get the ORDER BY column
        let order_col = match &order_by[0].expr {
            Expression::ColumnRef { column, .. } => column.as_str(),
            _ => return Ok(None),
        };
        let is_desc = order_by[0].direction == vibesql_ast::OrderDirection::Desc;

        // Try each index to find one that matches the pattern
        for index_name in &index_names {
            // Get index metadata
            let metadata = match self.database.get_index(index_name) {
                Some(m) => m,
                None => continue,
            };

            // Get index column names in order
            let index_columns: Vec<&str> =
                metadata.columns.iter().map(|c| c.column_name.as_str()).collect();

            // Need at least 2 columns for prefix + ORDER BY pattern
            if index_columns.len() < 2 {
                continue;
            }

            // Try to extract equality values from WHERE clause for index columns
            let index_values = match self.extract_pk_values(where_clause, &index_columns) {
                EqualityResult::Contradiction => {
                    // Multiple equalities on same column with different values
                    // This is always false, return empty result
                    return Ok(Some(vec![]));
                }
                EqualityResult::Values(v) => v,
            };

            // Build prefix key - equality predicates for first N columns
            let mut prefix_key: Vec<SqlValue> = Vec::new();
            for col in &index_columns {
                let col_lower = col.to_ascii_lowercase();
                if let Some(val) = index_values.get(&col_lower) {
                    prefix_key.push(val.clone());
                } else {
                    break; // Stop at first missing column
                }
            }

            // Need at least one prefix column
            if prefix_key.is_empty() {
                continue;
            }

            // Check if ORDER BY column is the next column after prefix
            let prefix_len = prefix_key.len();
            if prefix_len >= index_columns.len() {
                continue; // No room for ORDER BY column
            }

            let next_index_col = index_columns[prefix_len];
            if !next_index_col.eq_ignore_ascii_case(order_col) {
                continue; // ORDER BY column doesn't match next index column
            }

            // Get index data for prefix scan
            let index_data = match self.database.get_index_data(index_name) {
                Some(idx) => idx,
                None => continue,
            };

            // Perform the optimized prefix scan
            let row_indices = if is_desc {
                // Use reverse prefix scan for DESC ORDER BY
                index_data.prefix_scan_reverse_limit(&prefix_key, limit)
            } else {
                // Use forward prefix scan for ASC ORDER BY
                // For LIMIT 1, prefix_scan_first is more efficient
                if limit == 1 {
                    match index_data.prefix_scan_first(&prefix_key) {
                        Some(idx) => vec![idx],
                        None => vec![],
                    }
                } else {
                    // For larger limits, use prefix_scan with manual limit
                    let all_indices = index_data.prefix_scan(&prefix_key);
                    all_indices.into_iter().take(limit).collect()
                }
            };

            if row_indices.is_empty() {
                return Ok(Some(vec![]));
            }

            // Fetch the rows
            let all_rows = table.scan();
            let rows: Vec<Row> =
                row_indices.iter().filter_map(|&idx| all_rows.get(idx).cloned()).collect();

            // Build schema for projection and filtering
            let effective_name = alias.cloned().unwrap_or_else(|| table_name.to_string());
            let schema = CombinedSchema::from_table(effective_name, table.schema.clone());

            // Check if WHERE clause has predicates not covered by the index lookup.
            // The index lookup covers:
            // - Equality predicates on prefix columns (prefix_key)
            // - The ORDER BY column (next_index_col) is used for ordering, not filtering
            // Any other predicates need to be applied as a filter.
            let covered_columns: std::collections::HashSet<String> = index_columns
                .iter()
                .take(prefix_key.len())
                .map(|c| c.to_ascii_lowercase())
                .collect();

            // Check if WHERE clause is fully satisfied by the index lookup
            let needs_where_filter =
                !self.where_fully_satisfied_by_equality_columns(where_clause, &covered_columns);

            // Apply residual WHERE filter if needed
            let filtered_rows = if needs_where_filter && !rows.is_empty() {
                self.apply_where_filter_fast(where_clause, rows, &schema)?
            } else {
                rows
            };

            // Apply projection
            let is_select_star = stmt.select_list.len() == 1
                && matches!(&stmt.select_list[0], SelectItem::Wildcard { .. });

            if is_select_star {
                return Ok(Some(filtered_rows));
            }

            let projected =
                self.apply_projection_fast(&stmt.select_list, filtered_rows, &schema)?;
            return Ok(Some(projected));
        }

        // No suitable index found
        Ok(None)
    }

    /// Try secondary index lookup path for queries with composite key patterns
    ///
    /// Returns Some(rows) if we can use a secondary index lookup, None if we need standard path.
    /// This handles queries like `SELECT * FROM customer WHERE c_w_id = 1 AND c_d_id = 2 AND c_last = 'SMITH'`
    /// when there's a secondary index on (c_w_id, c_d_id, c_last).
    fn try_secondary_index_lookup_fast(
        &self,
        table_name: &str,
        alias: Option<&String>,
        stmt: &SelectStmt,
    ) -> Result<Option<Vec<Row>>, ExecutorError> {
        // Need a WHERE clause for index lookup
        let where_clause = match &stmt.where_clause {
            Some(w) => w,
            None => return Ok(None),
        };

        // Get the table
        let table = match self.database.get_table(table_name) {
            Some(t) => t,
            None => return Ok(None), // Not a table - could be a view
        };

        // Get all secondary indexes for this table
        let index_names = self.database.list_indexes_for_table(table_name);
        if index_names.is_empty() {
            return Ok(None);
        }

        // Try each index to see if we can use it
        for index_name in &index_names {
            // Get index metadata
            let metadata = match self.database.get_index(index_name) {
                Some(m) => m,
                None => continue,
            };

            // Get index column names in order
            let index_columns: Vec<&str> =
                metadata.columns.iter().map(|c| c.column_name.as_str()).collect();

            // Try to extract equality values from WHERE clause
            let index_values = match self.extract_pk_values(where_clause, &index_columns) {
                EqualityResult::Contradiction => {
                    // Multiple equalities on same column with different values
                    // This is always false, return empty result
                    return Ok(Some(vec![]));
                }
                EqualityResult::Values(v) => v,
            };

            // Need at least one column value to use the index
            if index_values.is_empty() {
                continue;
            }

            // Check for contradictions (e.g., col = 70 AND col IN (74, 69, 10))
            // If equality value is not in the IN list, return empty result immediately
            for (col_name, eq_value) in &index_values {
                if let Some(in_values) = Self::extract_in_values(where_clause, col_name) {
                    if !in_values.contains(eq_value) {
                        // Contradiction: equality value not in IN list - no rows can match
                        return Ok(Some(vec![]));
                    }
                }
            }

            // Build key values for the prefix of columns we have equality predicates for
            // This supports partial index usage (e.g., 3-column prefix of 4-column index)
            // Use case-insensitive lookup since schema may have different case than parser output
            let mut key_values: Vec<SqlValue> = Vec::new();
            for col in &index_columns {
                let col_lower = col.to_ascii_lowercase();
                if let Some(val) = index_values.get(&col_lower) {
                    key_values.push(val.clone());
                } else {
                    break; // Stop at first missing column (must be contiguous prefix)
                }
            }

            // Need at least one value to use the index
            if key_values.is_empty() {
                continue;
            }

            // Perform index lookup
            let rows = if key_values.len() == index_columns.len() {
                // Full key match - use exact lookup
                let rows_result = self
                    .database
                    .lookup_by_index(index_name, &key_values)
                    .map_err(|e| ExecutorError::StorageError(e.to_string()))?;
                match rows_result {
                    Some(refs) => refs.into_iter().cloned().collect::<Vec<_>>(),
                    None => vec![],
                }
            } else {
                // Prefix match - use prefix lookup
                self.database
                    .lookup_by_index_prefix(index_name, &key_values)
                    .map_err(|e| ExecutorError::StorageError(e.to_string()))?
                    .into_iter()
                    .cloned()
                    .collect::<Vec<_>>()
            };

            // Check if WHERE clause has predicates not covered by the index lookup.
            // If so, we need to apply the full WHERE clause as a filter.
            // Build set of columns covered by index lookup (those with equality predicates)
            let covered_columns: std::collections::HashSet<String> = index_columns
                .iter()
                .take(key_values.len())
                .map(|c| c.to_ascii_lowercase())
                .collect();

            // Check if WHERE clause is fully satisfied by the index lookup
            let needs_where_filter =
                !self.where_fully_satisfied_by_equality_columns(where_clause, &covered_columns);

            // Apply residual WHERE filter if needed
            let filtered_rows = if needs_where_filter && !rows.is_empty() {
                // Build schema for filtering
                let effective_name = alias.cloned().unwrap_or_else(|| table_name.to_string());
                let schema = CombinedSchema::from_table(effective_name, table.schema.clone());
                self.apply_where_filter_fast(where_clause, rows, &schema)?
            } else {
                rows
            };

            // Apply ORDER BY if needed (requires schema for column lookup)
            let sorted_rows = if let Some(order_by) = &stmt.order_by {
                // Only build schema if we need ORDER BY
                let effective_name = alias.cloned().unwrap_or_else(|| table_name.to_string());
                let schema = CombinedSchema::from_table(effective_name, table.schema.clone());
                // Index lookup doesn't guarantee order, so always sort
                self.apply_order_by_fast(order_by, filtered_rows, &schema)?
            } else {
                filtered_rows
            };

            // Apply LIMIT/OFFSET
            let limited_rows =
                crate::select::helpers::apply_limit_offset(sorted_rows, stmt.limit, stmt.offset);

            // Check if this is SELECT * - no projection needed
            let is_select_star = stmt.select_list.len() == 1
                && matches!(&stmt.select_list[0], SelectItem::Wildcard { .. });

            if is_select_star {
                return Ok(Some(limited_rows));
            }

            // Try ultra-fast direct column projection (no schema clone, no evaluator)
            if let Some(col_indices) =
                self.try_extract_simple_column_indices(&stmt.select_list, &table.schema)
            {
                let projected = self.project_by_indices_fast(limited_rows, &col_indices);
                return Ok(Some(projected));
            }

            // Fall back to full projection with evaluator for complex expressions
            let effective_name = alias.cloned().unwrap_or_else(|| table_name.to_string());
            let schema = CombinedSchema::from_table(effective_name, table.schema.clone());
            let projected = self.apply_projection_fast(&stmt.select_list, limited_rows, &schema)?;
            return Ok(Some(projected));
        }

        // No suitable index found
        Ok(None)
    }

    /// Extract equality predicate values for given columns from WHERE clause
    ///
    /// Returns `EqualityResult::Contradiction` if multiple equality predicates on the
    /// same column have different values (e.g., col = 1 AND col = 2), which means
    /// the WHERE clause is always false and no rows can match.
    fn extract_pk_values(&self, expr: &Expression, pk_columns: &[&str]) -> EqualityResult {
        let mut values = HashMap::new();
        if self.collect_pk_equality_values(expr, pk_columns, &mut values) {
            EqualityResult::Values(values)
        } else {
            EqualityResult::Contradiction
        }
    }

    /// Recursively collect equality values for PK columns
    ///
    /// Returns `false` if a contradiction is detected (multiple equalities on same
    /// column with different values), `true` otherwise.
    fn collect_pk_equality_values(
        &self,
        expr: &Expression,
        pk_columns: &[&str],
        values: &mut HashMap<String, vibesql_types::SqlValue>,
    ) -> bool {
        if let Expression::BinaryOp { left, op, right } = expr {
            match op {
                vibesql_ast::BinaryOperator::And => {
                    // Recurse into both sides of AND
                    // Short-circuit if contradiction found
                    if !self.collect_pk_equality_values(left, pk_columns, values) {
                        return false;
                    }
                    if !self.collect_pk_equality_values(right, pk_columns, values) {
                        return false;
                    }
                }
                vibesql_ast::BinaryOperator::Equal => {
                    // Check for column = literal pattern
                    if let Some((col_name, value)) = self.extract_column_literal_pair(left, right) {
                        // Case-insensitive comparison for SQL identifiers
                        // Parser uppercases identifiers but schema may have lowercase column names
                        if pk_columns.iter().any(|pk| pk.eq_ignore_ascii_case(&col_name)) {
                            let key = col_name.to_ascii_lowercase();
                            // Check for contradiction: multiple equalities with different values
                            if let Some(existing) = values.get(&key) {
                                if existing != &value {
                                    // Contradiction: col = X AND col = Y where X != Y
                                    return false;
                                }
                                // Same value, no need to insert again
                            } else {
                                values.insert(key, value);
                            }
                        }
                    }
                }
                _ => {}
            }
        }
        true
    }

    /// Extract column name and literal value from an equality expression
    fn extract_column_literal_pair(
        &self,
        left: &Expression,
        right: &Expression,
    ) -> Option<(String, vibesql_types::SqlValue)> {
        // Try left = column, right = literal
        if let Expression::ColumnRef { column, .. } = left {
            if let Some(value) = self.literal_to_value(right) {
                return Some((column.clone(), value));
            }
        }
        // Try left = literal, right = column
        if let Expression::ColumnRef { column, .. } = right {
            if let Some(value) = self.literal_to_value(left) {
                return Some((column.clone(), value));
            }
        }
        None
    }

    /// Extract SqlValue from a literal expression
    fn literal_to_value(&self, expr: &Expression) -> Option<SqlValue> {
        match expr {
            Expression::Literal(val) => Some(val.clone()),
            _ => None,
        }
    }

    /// Check if a WHERE clause is fully satisfied by equality predicates on the given columns.
    ///
    /// Returns true ONLY if the WHERE clause contains ONLY equality predicates
    /// on the specified columns (connected by AND). Any other predicates (non-equality
    /// comparisons, predicates on other columns, OR, etc.) will cause this to return false.
    ///
    /// This is used to determine if additional filtering is needed after an index lookup.
    fn where_fully_satisfied_by_equality_columns(
        &self,
        expr: &Expression,
        covered_columns: &std::collections::HashSet<String>,
    ) -> bool {
        match expr {
            // Equality predicate: col = literal
            Expression::BinaryOp { left, op: vibesql_ast::BinaryOperator::Equal, right } => {
                // Check if this is an equality on a covered column
                if let Some((col_name, _)) = self.extract_column_literal_pair(left, right) {
                    covered_columns.contains(&col_name.to_ascii_lowercase())
                } else {
                    false // Not a simple column = literal pattern
                }
            }
            // AND: both sides must be fully satisfied
            Expression::BinaryOp { left, op: vibesql_ast::BinaryOperator::And, right } => {
                self.where_fully_satisfied_by_equality_columns(left, covered_columns)
                    && self.where_fully_satisfied_by_equality_columns(right, covered_columns)
            }
            // Any other expression type is not satisfied by the index lookup
            _ => false,
        }
    }

    /// Extract IN list values for a column from WHERE clause
    /// Returns None if no IN predicate found for the column
    fn extract_in_values(expr: &Expression, column_name: &str) -> Option<Vec<SqlValue>> {
        match expr {
            Expression::InList { expr: col_expr, values, negated } => {
                if *negated {
                    return None; // NOT IN is not a contradiction detector
                }
                // Check if the IN expression is for our target column
                if let Expression::ColumnRef { column, .. } = col_expr.as_ref() {
                    if column.eq_ignore_ascii_case(column_name) {
                        // Extract all literal values from the IN list
                        let mut result = Vec::new();
                        for v in values {
                            if let Expression::Literal(val) = v {
                                result.push(val.clone());
                            }
                        }
                        if !result.is_empty() {
                            return Some(result);
                        }
                    }
                }
                None
            }
            Expression::BinaryOp { left, op: vibesql_ast::BinaryOperator::And, right } => {
                // Recursively search both sides of AND
                Self::extract_in_values(left, column_name)
                    .or_else(|| Self::extract_in_values(right, column_name))
            }
            _ => None,
        }
    }

    /// Apply WHERE filter in fast path (simplified, no CSE)
    fn apply_where_filter_fast(
        &self,
        where_clause: &Expression,
        rows: Vec<Row>,
        schema: &CombinedSchema,
    ) -> Result<Vec<Row>, ExecutorError> {
        use crate::evaluator::compiled::CompiledPredicate;

        // Try to use compiled predicate for fast evaluation
        let compiled = CompiledPredicate::compile(where_clause, schema);

        if compiled.is_fully_compiled() {
            // Fast path: use compiled predicate
            let filtered: Vec<Row> =
                rows.into_iter().filter(|row| compiled.evaluate(row).unwrap_or(false)).collect();
            Ok(filtered)
        } else {
            // Fall back to standard evaluator
            use crate::evaluator::CombinedExpressionEvaluator;
            let evaluator = CombinedExpressionEvaluator::with_database(schema, self.database);

            let mut filtered = Vec::new();
            for row in rows {
                let result = evaluator.eval(where_clause, &row)?;
                if matches!(result, vibesql_types::SqlValue::Boolean(true)) {
                    filtered.push(row);
                }
                evaluator.clear_cse_cache();
            }
            Ok(filtered)
        }
    }

    /// Apply projection in fast path
    fn apply_projection_fast(
        &self,
        select_list: &[SelectItem],
        rows: Vec<Row>,
        schema: &CombinedSchema,
    ) -> Result<Vec<Row>, ExecutorError> {
        use crate::evaluator::CombinedExpressionEvaluator;
        use crate::select::projection::project_row_combined;

        // Check if this is SELECT * - no projection needed
        if select_list.len() == 1 && matches!(&select_list[0], SelectItem::Wildcard { .. }) {
            return Ok(rows);
        }

        // Validate that all column references exist in the schema.
        // This is important even when there are no rows to return a proper error.
        self.validate_select_columns(select_list, schema)?;

        let evaluator = CombinedExpressionEvaluator::with_database(schema, self.database);
        let buffer_pool = self.query_buffer_pool();

        let mut projected = Vec::with_capacity(rows.len());
        for row in &rows {
            let projected_row =
                project_row_combined(row, select_list, &evaluator, schema, &None, buffer_pool)?;
            projected.push(projected_row);
        }

        Ok(projected)
    }

    /// Validate that all column references in the SELECT list exist in the schema
    fn validate_select_columns(
        &self,
        select_list: &[SelectItem],
        schema: &CombinedSchema,
    ) -> Result<(), ExecutorError> {
        for item in select_list {
            if let SelectItem::Expression { expr, .. } = item {
                Self::validate_expression_columns(expr, schema)?;
            }
        }
        Ok(())
    }

    /// Recursively validate column references in an expression
    fn validate_expression_columns(
        expr: &Expression,
        schema: &CombinedSchema,
    ) -> Result<(), ExecutorError> {
        match expr {
            Expression::ColumnRef { table, column } => {
                if schema.get_column_index(table.as_deref(), column).is_none() {
                    // Collect available column names for the error message
                    let available_columns: Vec<String> = schema
                        .table_schemas
                        .values()
                        .flat_map(|(_, s)| s.columns.iter().map(|c| c.name.clone()))
                        .collect();
                    return Err(ExecutorError::ColumnNotFound {
                        column_name: column.clone(),
                        table_name: table.clone().unwrap_or_else(|| "unknown".to_string()),
                        searched_tables: schema.table_schemas.keys().cloned().collect(),
                        available_columns,
                    });
                }
            }
            Expression::BinaryOp { left, right, .. } => {
                Self::validate_expression_columns(left, schema)?;
                Self::validate_expression_columns(right, schema)?;
            }
            Expression::UnaryOp { expr, .. } => {
                Self::validate_expression_columns(expr, schema)?;
            }
            Expression::Cast { expr, .. } => {
                Self::validate_expression_columns(expr, schema)?;
            }
            // Literals and other expressions don't need column validation
            _ => {}
        }
        Ok(())
    }

    /// Apply ORDER BY sorting in fast path
    ///
    /// Uses simple column-based sorting for the fast path.
    /// ORDER BY expressions must be simple column references (validated by is_simple_order_by).
    /// ORDER BY with aliases is excluded at detection time by uses_select_alias().
    fn apply_order_by_fast(
        &self,
        order_by: &[vibesql_ast::OrderByItem],
        mut rows: Vec<Row>,
        schema: &CombinedSchema,
    ) -> Result<Vec<Row>, ExecutorError> {
        use crate::select::grouping::compare_sql_values;
        use std::cmp::Ordering;

        // Pre-compute column indices for ORDER BY columns
        let mut sort_indices: Vec<(usize, vibesql_ast::OrderDirection)> =
            Vec::with_capacity(order_by.len());

        for item in order_by {
            let col_idx = match &item.expr {
                Expression::ColumnRef { table, column } => schema
                    .get_column_index(table.as_deref(), column)
                    .ok_or_else(|| ExecutorError::ColumnNotFound {
                        column_name: column.clone(),
                        table_name: table.clone().unwrap_or_default(),
                        searched_tables: schema.table_schemas.keys().cloned().collect(),
                        available_columns: vec![],
                    })?,
                _ => {
                    return Err(ExecutorError::Other(
                        "Fast path ORDER BY requires simple column references".to_string(),
                    ));
                }
            };
            sort_indices.push((col_idx, item.direction.clone()));
        }

        // Sort rows by the specified columns
        rows.sort_by(|a, b| {
            for (col_idx, dir) in &sort_indices {
                let val_a = &a.values[*col_idx];
                let val_b = &b.values[*col_idx];

                // Handle NULLs: always sort last regardless of ASC/DESC
                let cmp = match (val_a.is_null(), val_b.is_null()) {
                    (true, true) => Ordering::Equal,
                    (true, false) => return Ordering::Greater, // NULL always sorts last
                    (false, true) => return Ordering::Less,    // non-NULL always sorts first
                    (false, false) => {
                        // Compare non-NULL values, respecting direction
                        match dir {
                            vibesql_ast::OrderDirection::Asc => compare_sql_values(val_a, val_b),
                            vibesql_ast::OrderDirection::Desc => {
                                compare_sql_values(val_a, val_b).reverse()
                            }
                        }
                    }
                };

                if cmp != Ordering::Equal {
                    return cmp;
                }
            }
            Ordering::Equal
        });

        Ok(rows)
    }

    /// Try to extract simple column indices from a SELECT list
    ///
    /// Returns Some(indices) if all SELECT items are simple column references,
    /// None otherwise (indicating fallback to full evaluator path is needed).
    ///
    /// This is an optimization for TPC-C style queries where the SELECT list
    /// contains only column references like `SELECT c_id, c_first, c_middle ...`
    fn try_extract_simple_column_indices(
        &self,
        select_list: &[SelectItem],
        table_schema: &vibesql_catalog::TableSchema,
    ) -> Option<Vec<usize>> {
        let mut indices = Vec::with_capacity(select_list.len());

        for item in select_list {
            match item {
                SelectItem::Expression {
                    expr: Expression::ColumnRef { table: _, column }, ..
                } => {
                    // Find column index by name (case-insensitive)
                    let idx = table_schema
                        .columns
                        .iter()
                        .position(|c| c.name.eq_ignore_ascii_case(column))?;
                    indices.push(idx);
                }
                _ => return None, // Not a simple column reference
            }
        }

        Some(indices)
    }

    /// Project rows by direct column indices (ultra-fast path)
    ///
    /// This avoids:
    /// - Creating CombinedSchema (which clones TableSchema)
    /// - Creating CombinedExpressionEvaluator
    /// - Going through the full evaluator machinery
    ///
    /// For simple column projections, this is 10-100x faster than the full path.
    fn project_by_indices_fast(&self, rows: Vec<Row>, col_indices: &[usize]) -> Vec<Row> {
        rows.into_iter()
            .map(|row| {
                let projected_values: Vec<SqlValue> =
                    col_indices.iter().map(|&idx| row.values[idx].clone()).collect();
                Row { values: projected_values }
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vibesql_ast::Statement;
    use vibesql_parser::Parser;

    fn parse_select(sql: &str) -> SelectStmt {
        match Parser::parse_sql(sql).unwrap() {
            Statement::Select(stmt) => *stmt,
            _ => panic!("Expected SELECT statement"),
        }
    }

    #[test]
    fn test_simple_point_query_detection() {
        // Simple point queries should be detected
        assert!(is_simple_point_query(&parse_select("SELECT w_tax FROM warehouse WHERE w_id = 1")));
        assert!(is_simple_point_query(&parse_select("SELECT * FROM users WHERE id = 123")));
        assert!(is_simple_point_query(&parse_select("SELECT a, b FROM t WHERE x = 1 AND y = 2")));
        assert!(is_simple_point_query(&parse_select("SELECT a FROM t WHERE x > 10")));
        assert!(is_simple_point_query(&parse_select("SELECT a FROM t WHERE x BETWEEN 1 AND 10")));
        assert!(is_simple_point_query(&parse_select("SELECT a FROM t WHERE x IN (1, 2, 3)")));
        assert!(is_simple_point_query(&parse_select("SELECT a FROM t WHERE x IS NULL")));
    }

    #[test]
    fn test_non_simple_query_detection() {
        // Complex queries should not be detected as simple
        assert!(!is_simple_point_query(&parse_select("SELECT COUNT(*) FROM t WHERE id = 1")));
        assert!(!is_simple_point_query(&parse_select("SELECT a FROM t1, t2 WHERE t1.id = t2.id")));
        assert!(!is_simple_point_query(&parse_select(
            "SELECT a FROM t WHERE id IN (SELECT id FROM t2)"
        )));
        assert!(!is_simple_point_query(&parse_select("SELECT DISTINCT a FROM t")));
        assert!(!is_simple_point_query(&parse_select("SELECT a FROM t GROUP BY a")));
        assert!(!is_simple_point_query(&parse_select("WITH cte AS (SELECT 1) SELECT * FROM cte")));
        assert!(!is_simple_point_query(&parse_select("SELECT a FROM t UNION SELECT b FROM t2")));
    }

    #[test]
    fn test_or_not_simple() {
        // OR predicates are not simple (could be optimized later)
        assert!(!is_simple_point_query(&parse_select("SELECT a FROM t WHERE x = 1 OR y = 2")));
    }

    #[test]
    fn test_order_by_simple_queries() {
        // Simple ORDER BY with column references should be detected as simple
        assert!(is_simple_point_query(&parse_select(
            "SELECT no_o_id FROM new_order WHERE no_w_id = 1 ORDER BY no_o_id"
        )));
        assert!(is_simple_point_query(&parse_select(
            "SELECT * FROM t WHERE id = 1 ORDER BY col ASC"
        )));
        assert!(is_simple_point_query(&parse_select(
            "SELECT a, b FROM t WHERE x = 1 ORDER BY a DESC"
        )));
        assert!(is_simple_point_query(&parse_select("SELECT a FROM t WHERE x = 1 ORDER BY a, b")));
        assert!(is_simple_point_query(&parse_select(
            "SELECT a FROM t WHERE x = 1 ORDER BY a DESC, b ASC"
        )));
        // ORDER BY with LIMIT
        assert!(is_simple_point_query(&parse_select(
            "SELECT a FROM t WHERE x = 1 ORDER BY a LIMIT 1"
        )));
    }

    #[test]
    fn test_order_by_complex_not_simple() {
        // Complex ORDER BY expressions should not be detected as simple
        assert!(!is_simple_point_query(&parse_select(
            "SELECT a FROM t WHERE x = 1 ORDER BY UPPER(a)"
        )));
        assert!(!is_simple_point_query(&parse_select(
            "SELECT a FROM t WHERE x = 1 ORDER BY a + 1"
        )));
        assert!(!is_simple_point_query(&parse_select(
            "SELECT a FROM t WHERE x = 1 ORDER BY COALESCE(a, b)"
        )));
    }

    #[test]
    fn test_needs_sorting() {
        // No sorted_by means we need to sort
        assert!(needs_sorting(
            &[vibesql_ast::OrderByItem {
                expr: vibesql_ast::Expression::ColumnRef { table: None, column: "a".to_string() },
                direction: vibesql_ast::OrderDirection::Asc,
            }],
            &None
        ));

        // Matching sorted_by means no sorting needed
        assert!(!needs_sorting(
            &[vibesql_ast::OrderByItem {
                expr: vibesql_ast::Expression::ColumnRef { table: None, column: "a".to_string() },
                direction: vibesql_ast::OrderDirection::Asc,
            }],
            &Some(vec![("a".to_string(), vibesql_ast::OrderDirection::Asc)])
        ));

        // Different column means sorting needed
        assert!(needs_sorting(
            &[vibesql_ast::OrderByItem {
                expr: vibesql_ast::Expression::ColumnRef { table: None, column: "b".to_string() },
                direction: vibesql_ast::OrderDirection::Asc,
            }],
            &Some(vec![("a".to_string(), vibesql_ast::OrderDirection::Asc)])
        ));

        // Different direction means sorting needed
        assert!(needs_sorting(
            &[vibesql_ast::OrderByItem {
                expr: vibesql_ast::Expression::ColumnRef { table: None, column: "a".to_string() },
                direction: vibesql_ast::OrderDirection::Desc,
            }],
            &Some(vec![("a".to_string(), vibesql_ast::OrderDirection::Asc)])
        ));

        // ORDER BY prefix of sorted_by is OK
        assert!(!needs_sorting(
            &[vibesql_ast::OrderByItem {
                expr: vibesql_ast::Expression::ColumnRef { table: None, column: "a".to_string() },
                direction: vibesql_ast::OrderDirection::Asc,
            }],
            &Some(vec![
                ("a".to_string(), vibesql_ast::OrderDirection::Asc),
                ("b".to_string(), vibesql_ast::OrderDirection::Asc),
            ])
        ));

        // ORDER BY with more columns than sorted_by needs sorting
        assert!(needs_sorting(
            &[
                vibesql_ast::OrderByItem {
                    expr: vibesql_ast::Expression::ColumnRef {
                        table: None,
                        column: "a".to_string()
                    },
                    direction: vibesql_ast::OrderDirection::Asc,
                },
                vibesql_ast::OrderByItem {
                    expr: vibesql_ast::Expression::ColumnRef {
                        table: None,
                        column: "b".to_string()
                    },
                    direction: vibesql_ast::OrderDirection::Asc,
                },
            ],
            &Some(vec![("a".to_string(), vibesql_ast::OrderDirection::Asc)])
        ));
    }
}
