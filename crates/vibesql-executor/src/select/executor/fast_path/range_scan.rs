//! PK range scan with early projection for fast path execution
//!
//! This module provides optimized execution for range queries with simple
//! column projections, avoiding full row materialization.

use vibesql_ast::{Expression, OrderDirection, SelectStmt};
use vibesql_storage::Row;
use vibesql_types::SqlValue;

use crate::errors::ExecutorError;
use crate::select::executor::builder::SelectExecutor;

impl SelectExecutor<'_> {
    /// Try PK range scan with early projection (issue #3799)
    ///
    /// For simple range queries like `SELECT c FROM t WHERE pk BETWEEN ? AND ?`,
    /// this uses streaming scan with early projection to avoid cloning unneeded columns.
    /// This is critical for sysbench range queries where full row clone was the bottleneck.
    ///
    /// Returns Some(rows) if we can use this optimization, None if fallback is needed.
    pub(crate) fn try_pk_range_scan_with_early_projection(
        &self,
        table_name: &str,
        stmt: &SelectStmt,
    ) -> Result<Option<Vec<Row>>, ExecutorError> {
        // Need a WHERE clause with BETWEEN
        let where_clause = match &stmt.where_clause {
            Some(w) => w,
            None => return Ok(None),
        };

        // No LIMIT (streaming doesn't help much with LIMIT)
        if stmt.limit.is_some() {
            return Ok(None);
        }

        // Get table
        let table = match self.database.get_table(table_name) {
            Some(t) => t,
            None => return Ok(None),
        };

        // Need single-column PK for streaming
        let pk_columns = match &table.schema.primary_key {
            Some(cols) if cols.len() == 1 => cols,
            _ => return Ok(None),
        };
        let pk_col = &pk_columns[0];

        // Extract BETWEEN bounds from WHERE clause
        let (low_value, high_value) = match self.extract_between_bounds(where_clause, pk_col) {
            Some(bounds) => bounds,
            None => return Ok(None),
        };

        // Need simple column references in SELECT (not SELECT *)
        let col_indices =
            match self.try_extract_simple_column_indices(&stmt.select_list, &table.schema) {
                Some(indices) => indices,
                None => return Ok(None),
            };

        // If ORDER BY is present, validate it's a simple column reference in the projected columns
        let order_by_info = if let Some(order_by) = &stmt.order_by {
            // Only support single-column ORDER BY
            if order_by.len() != 1 {
                return Ok(None);
            }
            let order_item = &order_by[0];
            // Must be a column reference
            let order_col = match &order_item.expr {
                Expression::ColumnRef { column, .. } => column,
                _ => return Ok(None),
            };
            // Find the order column in the projected columns
            // First, map projected indices to column names
            let projected_col_names: Vec<&str> = col_indices
                .iter()
                .map(|&idx| table.schema.columns[idx].name.as_str())
                .collect();
            // Find the position in projected columns
            let order_idx = match projected_col_names
                .iter()
                .position(|&name| name.eq_ignore_ascii_case(order_col))
            {
                Some(idx) => idx,
                None => return Ok(None), // ORDER BY column not in projected columns
            };
            Some((order_idx, order_item.direction.clone()))
        } else {
            None
        };

        // Find an index on the PK column (may not be named pk_{table})
        let index_names = self.database.list_indexes_for_table(table_name);
        let pk_index_data = index_names.iter().find_map(|idx_name| {
            let metadata = self.database.get_index(idx_name)?;
            if metadata.columns.len() == 1
                && metadata.columns[0].column_name.eq_ignore_ascii_case(pk_col)
            {
                self.database.get_index_data(idx_name)
            } else {
                None
            }
        });
        let pk_index_data = match pk_index_data {
            Some(idx) => idx,
            None => return Ok(None),
        };

        // Estimate result size for pre-allocation (#4059)
        // Use heuristic based on numeric range to avoid double-scanning the index
        let estimated_count = match (&low_value, &high_value) {
            (SqlValue::Integer(lo), SqlValue::Integer(hi)) => {
                // For integer PKs, estimate ~1 row per integer value
                (hi - lo + 1).max(0) as usize
            }
            (SqlValue::Bigint(lo), SqlValue::Bigint(hi)) => (hi - lo + 1).max(0) as usize,
            _ => 100, // Default estimate for non-integer types
        };

        // Try streaming range scan
        let streaming_iter = match pk_index_data.range_scan_streaming(
            Some(&low_value),
            Some(&high_value),
            true, // inclusive start (BETWEEN is inclusive)
            true, // inclusive end
        ) {
            Some(iter) => iter,
            None => return Ok(None),
        };

        // Stream with early projection - only clone needed columns
        // Pre-allocate Vec with estimated capacity to avoid reallocations (#4059)
        let mut rows: Vec<Row> = Vec::with_capacity(estimated_count);

        if col_indices.len() == 1 {
            // Single-column projection: use SmallVec directly to avoid Vec allocation
            let col_idx = col_indices[0];
            for idx in streaming_iter {
                if let Some(row) = table.get_row(idx) {
                    let mut values = vibesql_storage::RowValues::new();
                    values.push(row.values[col_idx].clone());
                    rows.push(Row::new(values));
                }
            }
        } else {
            // Multi-column projection: collect into SmallVec
            for idx in streaming_iter {
                if let Some(row) = table.get_row(idx) {
                    let projected_values: vibesql_storage::RowValues = col_indices
                        .iter()
                        .map(|&col_idx| row.values[col_idx].clone())
                        .collect();
                    rows.push(Row::new(projected_values));
                }
            }
        }

        // Apply DISTINCT if needed (deduplicate rows)
        // For DISTINCT + ORDER BY, sort first then deduplicate to preserve order
        if stmt.distinct {
            use crate::select::grouping::compare_sql_values;
            use std::cmp::Ordering;

            // Sort: use ORDER BY if specified, otherwise sort by all columns for dedup
            if let Some((order_idx, ref direction)) = order_by_info {
                rows.sort_by(|a, b| {
                    let cmp = compare_sql_values(&a.values[order_idx], &b.values[order_idx]);
                    match direction {
                        OrderDirection::Asc => cmp,
                        OrderDirection::Desc => cmp.reverse(),
                    }
                });
            } else {
                // Sort by all columns for deduplication (any consistent ordering works)
                rows.sort_by(|a, b| {
                    for (va, vb) in a.values.iter().zip(b.values.iter()) {
                        let cmp = compare_sql_values(va, vb);
                        if cmp != Ordering::Equal {
                            return cmp;
                        }
                    }
                    Ordering::Equal
                });
            }

            // Deduplicate by removing consecutive duplicates (stable dedup)
            // This works because we sorted first, so duplicates are adjacent
            rows.dedup_by(|a, b| {
                a.values
                    .iter()
                    .zip(b.values.iter())
                    .all(|(va, vb)| compare_sql_values(va, vb) == Ordering::Equal)
            });
        } else if let Some((order_idx, direction)) = order_by_info {
            // Apply ORDER BY without DISTINCT
            use crate::select::grouping::compare_sql_values;

            rows.sort_by(|a, b| {
                let cmp = compare_sql_values(&a.values[order_idx], &b.values[order_idx]);
                match direction {
                    OrderDirection::Asc => cmp,
                    OrderDirection::Desc => cmp.reverse(),
                }
            });
        }

        Ok(Some(rows))
    }
}
