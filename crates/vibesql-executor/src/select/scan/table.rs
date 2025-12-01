//! Table scanning logic
//!
//! Handles execution of simple table scans including:
//! - Regular database tables
//! - CTEs (Common Table Expressions)
//! - Views
//! - Predicate pushdown optimization
//! - SIMD-accelerated columnar filtering (#2972)

#![allow(clippy::too_many_arguments)]

use std::collections::HashMap;

use super::predicates::{apply_table_local_predicates, apply_table_local_predicates_ref};
use crate::{
    errors::ExecutorError, optimizer::PredicatePlan, privilege_checker::PrivilegeChecker,
    schema::CombinedSchema, select::cte::CteResult,
    select::columnar::{ColumnarBatch, ColumnPredicate, simd_filter_batch},
};

#[cfg(feature = "parallel")]
use crate::select::parallel::parallel_scan_materialize;

/// Minimum row count to benefit from SIMD columnar filtering
/// Below this threshold, row-by-row filtering is faster due to conversion overhead
const SIMD_COLUMNAR_THRESHOLD: usize = 500;

/// Execute a table scan (handles CTEs, views, and regular tables)
pub(crate) fn execute_table_scan(
    table_name: &str,
    alias: Option<&String>,
    cte_results: &HashMap<String, CteResult>,
    database: &vibesql_storage::Database,
    where_clause: Option<&vibesql_ast::Expression>,
    order_by: Option<&[vibesql_ast::OrderByItem]>,
    outer_row: Option<&vibesql_storage::Row>,
    outer_schema: Option<&CombinedSchema>,
) -> Result<super::FromResult, ExecutorError> {
    // Check if table is a CTE first (with case-insensitive lookup)
    let cte_result = cte_results.get(table_name).or_else(|| {
        // Fall back to case-insensitive lookup without allocation
        cte_results
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(table_name))
            .map(|(_, value)| value)
    });

    if let Some((cte_schema, cte_rows)) = cte_result {
        // Use CTE result
        let effective_name = alias.cloned().unwrap_or_else(|| table_name.to_string());
        let schema = CombinedSchema::from_table(effective_name, cte_schema.clone());

        // Apply table-local predicates from WHERE clause using pre-computed plan
        // Skip predicate pushdown for correlated subqueries (filtering happens later with full context)
        let is_correlated = outer_row.is_some() || outer_schema.is_some();
        if where_clause.is_some() && !is_correlated {
            // Build predicate plan once for this table
            let predicate_plan = PredicatePlan::from_where_clause(where_clause, &schema)
                .map_err(ExecutorError::InvalidWhereClause)?;

            // Use zero-copy filtering: only clone rows that pass the filter
            // This avoids O(n) deep cloning when most rows are filtered out
            let rows = apply_table_local_predicates_ref(
                cte_rows.as_ref(),
                schema.clone(),
                &predicate_plan,
                table_name,
                database,
                None,  // No outer context for non-correlated predicate pushdown
                None,
            )?;
            return Ok(super::FromResult::from_rows(schema, rows));
        }

        // No filtering needed - use zero-copy shared rows
        // This avoids O(n) cloning when CTE is referenced multiple times
        return Ok(super::FromResult::from_shared_rows(schema, cte_rows.clone()));
    }

    // Check if it's a view
    if let Some(view) = database.catalog.get_view(table_name) {
        // Check SELECT privilege on the view
        PrivilegeChecker::check_select(database, table_name)?;

        // Execute the view's query to get the result
        // We need to execute the entire SELECT statement, not just the FROM clause
        use crate::select::SelectExecutor;
        let executor = SelectExecutor::new(database);

        // Get both rows and column metadata
        let select_result = executor.execute_with_columns(&view.query)?;

        // Build a schema from the column names
        // Apply view's explicit column aliases if provided
        let column_names = if let Some(ref view_columns) = view.columns {
            // Use view's explicit column names
            view_columns.clone()
        } else {
            // Use column names from the SELECT statement
            select_result.columns.clone()
        };

        // Since views can have arbitrary SELECT expressions, we derive column types from the first row
        let columns = if !select_result.rows.is_empty() {
            let first_row = &select_result.rows[0];
            column_names
                .iter()
                .zip(&first_row.values)
                .map(|(name, value)| {
                    vibesql_catalog::ColumnSchema {
                        name: name.clone(),
                        data_type: value.get_type(),
                        nullable: true, // Views return nullable columns by default
                        default_value: None,
                    }
                })
                .collect()
        } else {
            // For empty views, create columns without specific types
            // This is a limitation but views with no rows are edge cases
            column_names
                .into_iter()
                .map(|name| vibesql_catalog::ColumnSchema {
                    name,
                    data_type: vibesql_types::DataType::Varchar { max_length: None },
                    nullable: true,
                    default_value: None,
                })
                .collect()
        };

        let view_schema = vibesql_catalog::TableSchema::new(table_name.to_string(), columns);
        let effective_name = alias.cloned().unwrap_or_else(|| table_name.to_string());
        let schema = CombinedSchema::from_table(effective_name, view_schema);
        let mut rows = select_result.rows;

        // Apply table-local predicates from WHERE clause using pre-computed plan
        // Skip predicate pushdown for correlated subqueries (filtering happens later with full context)
        let is_correlated = outer_row.is_some() || outer_schema.is_some();
        if where_clause.is_some() && !is_correlated {
            // Build predicate plan once for this table
            let predicate_plan = PredicatePlan::from_where_clause(where_clause, &schema)
                .map_err(ExecutorError::InvalidWhereClause)?;

            rows = apply_table_local_predicates(
                rows,
                schema.clone(),
                &predicate_plan,
                table_name,
                database,
                None,  // No outer context for non-correlated predicate pushdown
                None,
            )?;
        }

        return Ok(super::FromResult::from_rows(schema, rows));
    }

    // Check SELECT privilege on the table
    PrivilegeChecker::check_select(database, table_name)?;

    // Check if we should use an index scan (with cost-based selection)
    if let Some((index_name, sorted_columns)) = super::index_scan::cost_based_index_selection(table_name, where_clause, order_by, database) {
        // Use index scan for potentially better performance
        return super::index_scan::execute_index_scan(table_name, &index_name, alias, where_clause, sorted_columns, database);
    }

    // Use database table (fall back to table scan)
    let table = database
        .get_table(table_name)
        .ok_or_else(|| ExecutorError::TableNotFound(table_name.to_string()))?;

    let effective_name = alias.cloned().unwrap_or_else(|| table_name.to_string());
    let schema = CombinedSchema::from_table(effective_name, table.schema.clone());

    // Get row slice from table (zero-copy reference)
    let row_slice = table.scan();

    // Check if we need to apply table-local predicates (Phase 1 optimization)
    // NOTE: Skip predicate pushdown for correlated subqueries (when outer_row/outer_schema exist)
    // because the predicates may reference outer columns that aren't available during table scan.
    // For correlated subqueries, predicates are evaluated later with proper outer context.
    if let Some(where_expr) = where_clause {
        // Skip predicate pushdown if this is a correlated subquery
        let is_correlated = outer_row.is_some() || outer_schema.is_some();
        if is_correlated {
            // Return unfiltered rows for correlated subqueries
            // Filtering will happen later with full outer row context
            let rows = row_slice.to_vec();
            use crate::select::from_iterator::FromIterator;
            return Ok(super::FromResult::from_iterator(schema, FromIterator::from_table_scan(rows)));
        }

        // Build predicate plan once for this table
        let predicate_plan = PredicatePlan::from_where_clause(Some(where_expr), &schema)
            .map_err(ExecutorError::InvalidWhereClause)?;

        // Check if there are actually table-local predicates for this table
        // Note: has_table_filters does case-sensitive lookup - try lowercase first
        let has_filters = predicate_plan.has_table_filters(table_name)
            || predicate_plan.has_table_filters(&table_name.to_lowercase());

        if std::env::var("COLUMNAR_DEBUG").is_ok() {
            eprintln!("[COLUMNAR_DEBUG] {} table: has_table_filters={}, checking lowercase={}",
                table_name, predicate_plan.has_table_filters(table_name),
                predicate_plan.has_table_filters(&table_name.to_lowercase()));
        }

        if has_filters {
            // Try columnar filter optimization for simple predicates
            // Extract predicates once and choose the best execution path (#2972)
            if let Some(column_predicates) = crate::select::columnar::extract_column_predicates(where_expr, &schema) {
                if std::env::var("COLUMNAR_DEBUG").is_ok() {
                    eprintln!("[COLUMNAR_DEBUG] {} table: extracted {} predicates for {} rows",
                        table_name, column_predicates.len(), row_slice.len());
                }
                // For native columnar tables, use SIMD filtering on typed columns
                // This avoids SqlValue overhead by working directly on i64/f64/String arrays
                if table.is_native_columnar() && row_slice.len() >= SIMD_COLUMNAR_THRESHOLD {
                    if let Ok(filtered_rows) = filter_with_simd_columnar(table, &column_predicates) {
                        return Ok(super::FromResult::from_rows(schema, filtered_rows));
                    }
                    // Fall through to row-based path if SIMD fails
                }

                // For row-oriented tables, use bitmap-based filtering
                let indices = crate::select::columnar::apply_columnar_filter(row_slice, &column_predicates)?;
                let filtered_rows: Vec<_> = indices.into_iter().filter_map(|idx| row_slice.get(idx).cloned()).collect();
                return Ok(super::FromResult::from_rows(schema, filtered_rows));
            }

            // extract_column_predicates returned None - fall back
            if std::env::var("COLUMNAR_DEBUG").is_ok() {
                eprintln!("[COLUMNAR_DEBUG] {} table: extract_column_predicates returned None, using generic path",
                    table_name);
            }
            // Fall back to generic predicate evaluation for complex expressions
            let filtered_rows = apply_table_local_predicates_ref(
                row_slice,
                schema.clone(),
                &predicate_plan,
                table_name,
                database,
                None,  // No outer context for predicate pushdown
                None,
            )?;
            return Ok(super::FromResult::from_rows(schema, filtered_rows));
        }
    }

    // No table-local predicates or no WHERE clause: clone rows for iterator
    // TODO: Future optimization - use zero-copy iterator over row slice
    #[cfg(feature = "parallel")]
    let rows = parallel_scan_materialize(row_slice);

    #[cfg(not(feature = "parallel"))]
    let rows = row_slice.to_vec();

    use crate::select::from_iterator::FromIterator;
    Ok(super::FromResult::from_iterator(schema, FromIterator::from_table_scan(rows)))
}

/// Apply SIMD columnar filtering using native typed columns
///
/// This function implements the columnar predicate evaluation optimization from #2972:
/// 1. Get columnar data from table.scan_columnar() (native typed Vec<i64>, Vec<f64>, etc.)
/// 2. Convert to ColumnarBatch for SIMD operations
/// 3. Apply predicates using SIMD on native types (no SqlValue overhead)
/// 4. Convert only the filtered rows back to Row format
///
/// This avoids the overhead of SqlValue enum matching during predicate evaluation,
/// and only materializes rows that pass all filters.
///
/// # Performance
///
/// For Q3 with ~32K lineitem rows where ~3K pass filters:
/// - Old: Evaluate predicates on all 32K rows via SqlValue
/// - New: SIMD filter on native types, only materialize 3K passing rows
/// - Expected: ~10x reduction in predicate evaluation overhead
fn filter_with_simd_columnar(
    table: &vibesql_storage::Table,
    predicates: &[ColumnPredicate],
) -> Result<Vec<vibesql_storage::Row>, ExecutorError> {
    // Step 1: Get columnar data from table (uses cache if available)
    let columnar_table = table.scan_columnar()
        .map_err(|e| ExecutorError::StorageError(e.to_string()))?;

    // Step 2: Convert to ColumnarBatch for SIMD operations
    // This is zero-copy for Arc-wrapped data (just bumps reference count)
    let batch = ColumnarBatch::from_storage_columnar(&columnar_table)?;

    // Step 3: Apply SIMD-accelerated filtering on native types
    // This evaluates predicates directly on Vec<i64>, Vec<f64>, Vec<String>, etc.
    // without going through SqlValue enum matching
    let filtered_batch = simd_filter_batch(&batch, predicates)?;

    // Step 4: Convert filtered batch back to rows
    // Only the rows that passed all predicates are materialized
    let filtered_rows = filtered_batch.to_rows()?;

    Ok(filtered_rows)
}
