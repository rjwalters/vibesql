//! Secondary index lookup strategies for fast path execution
//!
//! This module provides optimized execution paths for queries that can use
//! secondary indexes:
//! - Secondary index point/prefix lookup
//! - Secondary index prefix with ORDER BY + LIMIT
//! - Covering index scans (index-only scans)

use std::collections::HashMap;
use std::collections::HashSet;

use vibesql_ast::{Expression, SelectItem, SelectStmt};
use vibesql_storage::Row;
use vibesql_types::SqlValue;

use super::helpers::EqualityResult;
use crate::errors::ExecutorError;
use crate::schema::CombinedSchema;
use crate::select::executor::builder::SelectExecutor;
use crate::select::scan::index_scan::covering::{check_covering_index, try_covering_index_scan};

impl SelectExecutor<'_> {
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
    pub(crate) fn try_secondary_index_prefix_with_limit_fast(
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
            // Issue #3790: Use get_row() which returns None for deleted rows
            let rows: Vec<Row> =
                row_indices.iter().filter_map(|&idx| table.get_row(idx).cloned()).collect();

            // Build schema for projection and filtering
            let effective_name = alias.cloned().unwrap_or_else(|| table_name.to_string());
            let schema = CombinedSchema::from_table(effective_name, table.schema.clone());

            // Check if WHERE clause has predicates not covered by the index lookup.
            // The index lookup covers:
            // - Equality predicates on prefix columns (prefix_key)
            // - The ORDER BY column (next_index_col) is used for ordering, not filtering
            // Any other predicates need to be applied as a filter.
            let covered_columns: HashSet<String> = index_columns
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
    pub(crate) fn try_secondary_index_lookup_fast(
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
            let covered_columns: HashSet<String> = index_columns
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

    /// Try covering index scan (index-only scan) for queries where all needed columns
    /// are in the index key.
    ///
    /// Returns Some(rows) if covering scan was successful, None if standard path needed.
    ///
    /// This optimization is critical for queries like TPC-C Stock-Level:
    /// `SELECT s_i_id FROM stock WHERE s_w_id = 1 AND s_quantity < 10`
    /// with index `idx_stock_quantity(s_w_id, s_quantity, s_i_id)`
    ///
    /// # Performance
    /// - Without covering scan: O(log n + k) index lookup + k table fetches
    /// - With covering scan: O(log n + k) index lookup only (no table access)
    ///
    /// For Stock-Level with ~300 matching rows, this eliminates 300 random table fetches.
    pub(crate) fn try_covering_index_scan_fast(
        &self,
        table_name: &str,
        alias: Option<&String>,
        stmt: &SelectStmt,
    ) -> Result<Option<Vec<Row>>, ExecutorError> {
        // Need a WHERE clause (covering scans use index predicates)
        let where_clause = match &stmt.where_clause {
            Some(w) => w,
            None => return Ok(None),
        };

        // Get table to verify it exists
        let table = match self.database.get_table(table_name) {
            Some(t) => t,
            None => return Ok(None),
        };

        // Extract needed columns from SELECT list
        let needed_columns = match self.extract_select_columns(&stmt.select_list, &table.schema) {
            Some(cols) => cols,
            None => return Ok(None), // Complex SELECT (*, expressions, etc.) - can't use covering
        };

        // Get all secondary indexes for this table
        let index_names = self.database.list_indexes_for_table(table_name);
        if index_names.is_empty() {
            return Ok(None);
        }

        // Try each index to find one that covers all needed columns
        for index_name in &index_names {
            // Get index metadata
            let metadata = match self.database.get_index(index_name) {
                Some(m) => m,
                None => continue,
            };

            // Get index column names
            let index_column_names: Vec<&str> =
                metadata.columns.iter().map(|c| c.column_name.as_str()).collect();

            // Check if this index covers all needed columns
            if check_covering_index(&index_column_names, &needed_columns).is_none() {
                continue; // Index doesn't cover all needed columns
            }

            // Try to use covering scan
            let from_result = try_covering_index_scan(
                table_name,
                index_name,
                alias,
                Some(where_clause),
                &needed_columns,
                self.database,
                &HashMap::new(), // No CTEs in fast path
            )?;

            if let Some(result) = from_result {
                // Covering scan succeeded - extract rows
                let rows = result.into_rows();

                // Apply LIMIT/OFFSET
                let limited_rows =
                    crate::select::helpers::apply_limit_offset(rows, stmt.limit, stmt.offset);

                return Ok(Some(limited_rows));
            }
        }

        // No suitable covering index found
        Ok(None)
    }
}
