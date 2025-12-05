//! Common Table Expression (CTE) handling for SELECT queries

use std::collections::HashMap;
use std::sync::Arc;

use crate::errors::ExecutorError;

/// CTE result: (schema, shared rows)
///
/// Uses `Arc<Vec<Row>>` to enable O(1) cloning when CTEs are:
/// - Propagated from outer queries to subqueries
/// - Referenced multiple times without filtering
///
/// This avoids deep-cloning all rows on every CTE reference.
pub type CteResult = (vibesql_catalog::TableSchema, Arc<Vec<vibesql_storage::Row>>);

/// Execute all CTEs and return their results
///
/// CTEs are executed in order, allowing later CTEs to reference earlier ones.
pub(super) fn execute_ctes<F>(
    ctes: &[vibesql_ast::CommonTableExpr],
    executor: F,
) -> Result<HashMap<String, CteResult>, ExecutorError>
where
    F: Fn(
        &vibesql_ast::SelectStmt,
        &HashMap<String, CteResult>,
    ) -> Result<Vec<vibesql_storage::Row>, ExecutorError>,
{
    // Use the memory-tracking version with a no-op memory check
    execute_ctes_with_memory_check(ctes, executor, |_| Ok(()))
}

/// Execute all CTEs with memory tracking
///
/// CTEs are executed in order, allowing later CTEs to reference earlier ones.
/// After each CTE is materialized, the memory_check callback is called with
/// the estimated size of the CTE result to enforce memory limits.
pub(super) fn execute_ctes_with_memory_check<F, M>(
    ctes: &[vibesql_ast::CommonTableExpr],
    executor: F,
    memory_check: M,
) -> Result<HashMap<String, CteResult>, ExecutorError>
where
    F: Fn(
        &vibesql_ast::SelectStmt,
        &HashMap<String, CteResult>,
    ) -> Result<Vec<vibesql_storage::Row>, ExecutorError>,
    M: Fn(usize) -> Result<(), ExecutorError>,
{
    let mut cte_results = HashMap::new();

    // Execute each CTE in order
    // CTEs can reference previously defined CTEs
    for cte in ctes {
        // Execute the CTE query with accumulated CTE results so far
        // This allows later CTEs to reference earlier ones
        let rows = executor(&cte.query, &cte_results)?;

        // Track memory for this CTE result before storing
        let estimated_size = super::helpers::estimate_result_size(&rows);
        memory_check(estimated_size)?;

        //  Determine the schema for this CTE
        let schema = derive_cte_schema(cte, &rows)?;

        // Store the CTE result wrapped in Arc for efficient sharing
        cte_results.insert(cte.name.clone(), (schema, Arc::new(rows)));
    }

    Ok(cte_results)
}

/// Derive the schema for a CTE from its query and results
pub(super) fn derive_cte_schema(
    cte: &vibesql_ast::CommonTableExpr,
    rows: &[vibesql_storage::Row],
) -> Result<vibesql_catalog::TableSchema, ExecutorError> {
    // If column names are explicitly specified, use those
    if let Some(column_names) = &cte.columns {
        // Get data types from first row (if available)
        if let Some(first_row) = rows.first() {
            if first_row.values.len() != column_names.len() {
                return Err(ExecutorError::UnsupportedFeature(format!(
                    "CTE column count mismatch: specified {} columns but query returned {}",
                    column_names.len(),
                    first_row.values.len()
                )));
            }

            let columns = column_names
                .iter()
                .zip(&first_row.values)
                .map(|(name, value)| {
                    let data_type = infer_type_from_value(value);
                    vibesql_catalog::ColumnSchema::new(name.clone(), data_type, true)
                    // nullable for
                    // simplicity
                })
                .collect();

            Ok(vibesql_catalog::TableSchema::new(cte.name.clone(), columns))
        } else {
            // Empty result set - create schema with VARCHAR columns
            let columns = column_names
                .iter()
                .map(|name| {
                    vibesql_catalog::ColumnSchema::new(
                        name.clone(),
                        vibesql_types::DataType::Varchar { max_length: Some(255) },
                        true,
                    )
                })
                .collect();

            Ok(vibesql_catalog::TableSchema::new(cte.name.clone(), columns))
        }
    } else {
        // No explicit column names - infer from query SELECT list
        // Extract column names from SELECT items
        let columns = cte
            .query
            .select_list
            .iter()
            .enumerate()
            .map(|(i, item)| {
                // Infer data type from first row if available, otherwise use default
                let data_type = if let Some(first_row) = rows.first() {
                    infer_type_from_value(&first_row.values[i])
                } else {
                    // No rows - use default type (VARCHAR)
                    vibesql_types::DataType::Varchar { max_length: Some(255) }
                };

                // Extract column name from SELECT item
                let col_name = match item {
                    vibesql_ast::SelectItem::Wildcard { .. }
                    | vibesql_ast::SelectItem::QualifiedWildcard { .. } => format!("col{}", i),
                    vibesql_ast::SelectItem::Expression { expr, alias } => {
                        if let Some(a) = alias {
                            a.clone()
                        } else {
                            // Try to extract name from expression
                            match expr {
                                vibesql_ast::Expression::ColumnRef { table: _, column } => {
                                    column.clone()
                                }
                                _ => format!("col{}", i),
                            }
                        }
                    }
                };

                vibesql_catalog::ColumnSchema::new(col_name, data_type, true) // nullable
            })
            .collect();

        Ok(vibesql_catalog::TableSchema::new(cte.name.clone(), columns))
    }
}

/// Infer data type from a SQL value
pub(super) fn infer_type_from_value(value: &vibesql_types::SqlValue) -> vibesql_types::DataType {
    match value {
        vibesql_types::SqlValue::Null => vibesql_types::DataType::Varchar { max_length: Some(255) }, // default
        vibesql_types::SqlValue::Integer(_) => vibesql_types::DataType::Integer,
        vibesql_types::SqlValue::Varchar(_) => {
            vibesql_types::DataType::Varchar { max_length: Some(255) }
        }
        vibesql_types::SqlValue::Character(_) => vibesql_types::DataType::Character { length: 1 },
        vibesql_types::SqlValue::Boolean(_) => vibesql_types::DataType::Boolean,
        vibesql_types::SqlValue::Float(_) => vibesql_types::DataType::Float { precision: 53 },
        vibesql_types::SqlValue::Double(_) => vibesql_types::DataType::DoublePrecision,
        vibesql_types::SqlValue::Numeric(_) => {
            vibesql_types::DataType::Numeric { precision: 10, scale: 2 }
        }
        vibesql_types::SqlValue::Real(_) => vibesql_types::DataType::Real,
        vibesql_types::SqlValue::Smallint(_) => vibesql_types::DataType::Smallint,
        vibesql_types::SqlValue::Bigint(_) => vibesql_types::DataType::Bigint,
        vibesql_types::SqlValue::Unsigned(_) => vibesql_types::DataType::Unsigned,
        vibesql_types::SqlValue::Date(_) => vibesql_types::DataType::Date,
        vibesql_types::SqlValue::Time(_) => vibesql_types::DataType::Time { with_timezone: false },
        vibesql_types::SqlValue::Timestamp(_) => {
            vibesql_types::DataType::Timestamp { with_timezone: false }
        }
        vibesql_types::SqlValue::Interval(_) => {
            // For now, return a simple INTERVAL type (can be enhanced to detect field types)
            vibesql_types::DataType::Interval {
                start_field: vibesql_types::IntervalField::Day,
                end_field: None,
            }
        }
        vibesql_types::SqlValue::Vector(v) => {
            vibesql_types::DataType::Vector { dimensions: v.len() as u32 }
        }
    }
}
