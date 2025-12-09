//! Streaming aggregation for fast path execution
//!
//! This module provides ultra-fast execution for simple aggregate queries
//! that can accumulate results inline during a PK range scan.

use vibesql_ast::{SelectItem, SelectStmt};
use vibesql_storage::Row;
use vibesql_types::SqlValue;

use super::analysis::extract_simple_aggregate;
use crate::errors::ExecutorError;
use crate::select::executor::builder::SelectExecutor;
use crate::select::grouping::AggregateAccumulator;

impl SelectExecutor<'_> {
    /// Execute a streaming aggregate query (#3815)
    ///
    /// This provides ultra-fast execution for queries like:
    /// `SELECT SUM(k) FROM sbtest1 WHERE id BETWEEN ? AND ?`
    ///
    /// By accumulating aggregates inline during the PK range scan, we avoid:
    /// - Materializing intermediate Row objects
    /// - Going through the full pipeline infrastructure
    /// - Multiple allocations per row
    ///
    /// # Performance
    /// This achieves SQLite-like performance (~4μs) compared to ~30μs
    /// for the standard aggregation path.
    pub fn execute_streaming_aggregate(&self, stmt: &SelectStmt) -> Result<Vec<Row>, ExecutorError> {
        // Extract table name from FROM clause
        let table_name = match &stmt.from {
            Some(vibesql_ast::FromClause::Table { name, .. }) => name.as_str(),
            _ => {
                return Err(ExecutorError::Other(
                    "Streaming aggregate requires simple table FROM".to_string(),
                ))
            }
        };

        // Get table
        let table = match self.database.get_table(table_name) {
            Some(t) => t,
            None => {
                return Err(ExecutorError::TableNotFound(table_name.to_string()));
            }
        };

        // Need single-column PK for streaming
        let pk_columns = match &table.schema.primary_key {
            Some(cols) if cols.len() == 1 => cols,
            _ => {
                return Err(ExecutorError::Other(
                    "Streaming aggregate requires single-column PK".to_string(),
                ))
            }
        };
        let pk_col = &pk_columns[0];

        // Extract BETWEEN bounds from WHERE clause
        let where_clause = stmt.where_clause.as_ref().ok_or_else(|| {
            ExecutorError::Other("Streaming aggregate requires WHERE clause".to_string())
        })?;

        let (low_value, high_value) = match self.extract_between_bounds(where_clause, pk_col) {
            Some(bounds) => bounds,
            None => {
                return Err(ExecutorError::Other(
                    "Streaming aggregate requires BETWEEN predicate on PK".to_string(),
                ))
            }
        };

        // Find an index on the PK column
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
            None => {
                return Err(ExecutorError::Other(
                    "Streaming aggregate requires index on PK".to_string(),
                ))
            }
        };

        // Create accumulators for each aggregate in the SELECT list
        let mut accumulators: Vec<(AggregateAccumulator, usize)> =
            Vec::with_capacity(stmt.select_list.len());

        for item in &stmt.select_list {
            match item {
                SelectItem::Expression { expr, .. } => {
                    let (func_name, col_idx) =
                        extract_simple_aggregate(expr, &table.schema).ok_or_else(|| {
                            ExecutorError::Other(
                                "Streaming aggregate: invalid aggregate expression".to_string(),
                            )
                        })?;
                    let accumulator = AggregateAccumulator::new(&func_name, false)?;
                    accumulators.push((accumulator, col_idx));
                }
                _ => {
                    return Err(ExecutorError::Other(
                        "Streaming aggregate: SELECT must contain only aggregates".to_string(),
                    ))
                }
            }
        }

        // Use streaming range scan to accumulate inline
        if let Some(stream) = pk_index_data.range_scan_streaming(
            Some(&low_value),
            Some(&high_value),
            true, // inclusive start (BETWEEN is inclusive)
            true, // inclusive end
        ) {
            for row_idx in stream {
                if let Some(row) = table.get_row(row_idx) {
                    for (accumulator, col_idx) in &mut accumulators {
                        accumulator.accumulate(&row.values[*col_idx]);
                    }
                }
            }
        }

        // Finalize and return single row
        let result_values: Vec<SqlValue> =
            accumulators.iter().map(|(acc, _)| acc.finalize()).collect();

        Ok(vec![Row::from_vec(result_values)])
    }
}
