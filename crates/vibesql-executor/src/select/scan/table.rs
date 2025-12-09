//! Table scanning logic
//!
//! Handles execution of simple table scans including:
//! - Regular database tables
//! - CTEs (Common Table Expressions)
//! - Views
//! - Information schema virtual tables
//! - Predicate pushdown optimization
//! - SIMD-accelerated columnar filtering (#2972)

#![allow(clippy::too_many_arguments)]

use std::collections::HashMap;

use super::predicates::apply_table_local_predicates;
use crate::{
    errors::ExecutorError,
    evaluator::CombinedExpressionEvaluator,
    information_schema::{
        execute_information_schema_query, get_information_schema_table_schema, parse_qualified_name,
    },
    optimizer::PredicatePlan,
    privilege_checker::PrivilegeChecker,
    schema::CombinedSchema,
    select::columnar::{simd_filter_batch, simd_filter_to_indices, ColumnPredicate, ColumnarBatch},
    select::cte::CteResult,
};

#[cfg(feature = "parallel")]
use crate::select::parallel::parallel_scan_materialize;

/// Minimum row count to benefit from SIMD columnar filtering
/// Below this threshold, row-by-row filtering is faster due to conversion overhead
const SIMD_COLUMNAR_THRESHOLD: usize = 500;

/// Apply SQL:1999 E051-09 column aliases to a table schema
///
/// When `column_aliases` is Some, renames the columns in the schema to match
/// the provided aliases. Returns an error if the alias count doesn't match
/// the column count.
///
/// Example: `FROM t AS mytemp (x, y)` renames columns A, B to X, Y
fn apply_column_aliases(
    schema: vibesql_catalog::TableSchema,
    column_aliases: Option<&Vec<String>>,
) -> Result<vibesql_catalog::TableSchema, ExecutorError> {
    if let Some(aliases) = column_aliases {
        if aliases.len() != schema.columns.len() {
            return Err(ExecutorError::ColumnCountMismatch {
                expected: schema.columns.len(),
                provided: aliases.len(),
            });
        }
        // Create new columns with renamed names
        // We must create a new TableSchema to rebuild the column_index_cache
        let renamed_columns: Vec<vibesql_catalog::ColumnSchema> = schema
            .columns
            .iter()
            .zip(aliases.iter())
            .map(|(col, alias)| vibesql_catalog::ColumnSchema {
                name: alias.clone(),
                data_type: col.data_type.clone(),
                nullable: col.nullable,
                default_value: col.default_value.clone(),
            })
            .collect();
        // TableSchema::new() rebuilds the column_index_cache
        return Ok(vibesql_catalog::TableSchema::new(schema.name.clone(), renamed_columns));
    }
    Ok(schema)
}

/// Execute a table scan (handles CTEs, views, and regular tables)
///
/// # Arguments
/// * `table_name` - Name of the table to scan
/// * `alias` - Optional table alias
/// * `column_aliases` - SQL:1999 E051-09: Optional column renaming (e.g., `FROM t AS a (x, y)`)
/// * `cte_results` - CTE context for the query
/// * `database` - Database reference
/// * `where_clause` - Optional WHERE clause for filtering
/// * `order_by` - Optional ORDER BY clause for index selection
/// * `limit` - Optional LIMIT value for early termination optimization (#3253)
/// * `outer_row` - Outer row for correlated subqueries
/// * `outer_schema` - Outer schema for correlated subqueries
pub(crate) fn execute_table_scan(
    table_name: &str,
    alias: Option<&String>,
    column_aliases: Option<&Vec<String>>,
    cte_results: &HashMap<String, CteResult>,
    database: &vibesql_storage::Database,
    where_clause: Option<&vibesql_ast::Expression>,
    order_by: Option<&[vibesql_ast::OrderByItem]>,
    limit: Option<usize>,
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
        // SQL:1999 E051-09: Apply column aliases if provided
        let cte_table_schema = apply_column_aliases(cte_schema.clone(), column_aliases)?;
        let schema = CombinedSchema::from_table(effective_name.clone(), cte_table_schema);

        // Apply table-local predicates from WHERE clause using pre-computed plan
        // Skip predicate pushdown for correlated subqueries (filtering happens later with full context)
        let is_correlated = outer_row.is_some() || outer_schema.is_some();
        if where_clause.is_some() && !is_correlated {
            // Build predicate plan once for this table
            let predicate_plan = PredicatePlan::from_where_clause(where_clause, &schema)
                .map_err(ExecutorError::InvalidWhereClause)?;

            // Must clone rows for filtering (copy-on-write semantics)
            // Note: Use effective_name (alias) for filter lookup since PredicatePlan uses schema table names
            // Issue #3562: Pass CTE context so IN subqueries can reference other CTEs
            let rows = apply_table_local_predicates(
                cte_rows.as_ref().clone(),
                schema.clone(),
                &predicate_plan,
                &effective_name,
                database,
                None, // No outer context for non-correlated predicate pushdown
                None,
                Some(cte_results), // CTE context for IN subqueries referencing CTEs
            )?;
            return Ok(super::FromResult::from_rows(schema, rows));
        }

        // No filtering needed - use zero-copy shared rows
        // This avoids O(n) cloning when CTE is referenced multiple times
        return Ok(super::FromResult::from_shared_rows(schema, cte_rows.clone()));
    }

    // Check if it's an information_schema table (e.g., "information_schema.tables")
    let (schema_part, table_part) = parse_qualified_name(table_name);
    if schema_part.eq_ignore_ascii_case("information_schema") {
        // Execute information_schema query
        let result = execute_information_schema_query(table_part, &database.catalog)?;

        // Get the schema for this information_schema table
        let table_schema = get_information_schema_table_schema(table_part)
            .ok_or_else(|| ExecutorError::TableNotFound(table_name.to_string()))?;

        let effective_name = alias.cloned().unwrap_or_else(|| table_name.to_string());
        // SQL:1999 E051-09: Apply column aliases if provided
        let table_schema = apply_column_aliases(table_schema, column_aliases)?;
        let schema = CombinedSchema::from_table(effective_name, table_schema);

        return Ok(super::FromResult::from_rows(schema, result.rows));
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
        // SQL:1999 E051-09: Apply column aliases if provided
        let view_schema = apply_column_aliases(view_schema, column_aliases)?;
        let schema = CombinedSchema::from_table(effective_name.clone(), view_schema);
        let mut rows = select_result.rows;

        // Apply table-local predicates from WHERE clause using pre-computed plan
        // Skip predicate pushdown for correlated subqueries (filtering happens later with full context)
        let is_correlated = outer_row.is_some() || outer_schema.is_some();
        if where_clause.is_some() && !is_correlated {
            // Build predicate plan once for this table
            let predicate_plan = PredicatePlan::from_where_clause(where_clause, &schema)
                .map_err(ExecutorError::InvalidWhereClause)?;

            // Note: Use effective_name (alias) for filter lookup since PredicatePlan uses schema table names
            // Issue #3562: Pass CTE context so IN subqueries can reference CTEs
            rows = apply_table_local_predicates(
                rows,
                schema.clone(),
                &predicate_plan,
                &effective_name,
                database,
                None, // No outer context for non-correlated predicate pushdown
                None,
                Some(cte_results), // CTE context for IN subqueries referencing CTEs
            )?;
        }

        return Ok(super::FromResult::from_rows(schema, rows));
    }

    // Check SELECT privilege on the table
    PrivilegeChecker::check_select(database, table_name)?;

    // First, try primary key point lookup for O(1) access (TPC-C optimization #3221)
    // This handles queries like: SELECT ... FROM stock WHERE s_w_id = 1 AND s_i_id = 123
    // Issue #3562: Pass CTE context so IN subqueries can reference CTEs
    if let Some(result) =
        try_primary_key_lookup(table_name, alias, column_aliases, where_clause, database, cte_results)?
    {
        return Ok(result);
    }

    // Check if we should use an index scan (with cost-based selection)
    // This now includes skip-scan as a fallback option when regular index scan isn't available
    if let Some(scan_choice) =
        super::index_scan::select_index_scan_method(table_name, where_clause, order_by, database)
    {
        match scan_choice {
            super::index_scan::IndexScanChoice::Regular { index_name, sorted_columns } => {
                // Use regular index scan for potentially better performance
                if crate::profiling::is_scan_debug_enabled() {
                    eprintln!("[SCAN_PATH] Using index scan: table={}, index={}", table_name, index_name);
                }
                // Pass limit for LIMIT pushdown optimization when ORDER BY is satisfied by index (#3253)
                // Issue #3562: Pass CTE context so IN subqueries can reference CTEs
                return super::index_scan::execute_index_scan(
                    table_name,
                    &index_name,
                    alias,
                    where_clause,
                    sorted_columns,
                    limit,
                    database,
                    cte_results,
                );
            }
            super::index_scan::IndexScanChoice::SkipScan { index_name, skip_scan_info } => {
                // Use skip-scan for non-prefix column filtering
                if crate::profiling::is_scan_debug_enabled() {
                    eprintln!(
                        "[SCAN_PATH] Using skip-scan: table={}, index={}, filter_col={}",
                        table_name, index_name, skip_scan_info.filter_column
                    );
                }
                // Skip-scan requires a WHERE clause (guaranteed by selection)
                if let Some(where_expr) = where_clause {
                    return super::index_scan::execute_skip_scan(
                        table_name,
                        &index_name,
                        alias,
                        where_expr,
                        &skip_scan_info,
                        database,
                        cte_results,
                    );
                }
            }
        }
    }

    // Debug: Log when table scan is used instead of index
    if crate::profiling::is_scan_debug_enabled() && where_clause.is_some() {
        let indexes = database.list_indexes_for_table(table_name);
        eprintln!(
            "[SCAN_PATH] Falling back to table scan: table={}, available_indexes={:?}, where={:?}",
            table_name, indexes, where_clause
        );
    }

    // Use database table (fall back to table scan)
    let table = database
        .get_table(table_name)
        .ok_or_else(|| ExecutorError::TableNotFound(table_name.to_string()))?;

    let effective_name = alias.cloned().unwrap_or_else(|| table_name.to_string());
    // SQL:1999 E051-09: Apply column aliases if provided (e.g., FROM t AS a (x, y))
    let table_schema = apply_column_aliases(table.schema.clone(), column_aliases)?;
    let schema = CombinedSchema::from_table(effective_name.clone(), table_schema);

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
            // Issue #3790: Must use scan_live_vec() to filter deleted rows
            let live_rows = table.scan_live_vec();
            use crate::select::from_iterator::FromIterator;
            return Ok(super::FromResult::from_iterator(
                schema,
                FromIterator::from_table_scan(live_rows),
            ));
        }

        // Build predicate plan once for this table
        let predicate_plan = PredicatePlan::from_where_clause(Some(where_expr), &schema)
            .map_err(ExecutorError::InvalidWhereClause)?;

        // Check if there are actually table-local predicates for this table
        // Note: has_table_filters does case-sensitive lookup
        // Must check BOTH effective_name (alias) AND table_name because:
        // - IN predicates from OR expressions use the alias (e.g., "n1" from "nation n1")
        // - Regular predicates may use the actual table name
        let effective_name_lower = effective_name.to_lowercase();
        let has_filters = predicate_plan.has_table_filters(&effective_name)
            || predicate_plan.has_table_filters(&effective_name_lower)
            || predicate_plan.has_table_filters(table_name)
            || predicate_plan.has_table_filters(&table_name.to_lowercase());

        if crate::profiling::is_scan_debug_enabled() {
            eprintln!("[SCAN_PATH] {} (alias={}) table: has_filters={} (effective_name={}, table_name={})",
                table_name, effective_name, has_filters,
                predicate_plan.has_table_filters(&effective_name_lower),
                predicate_plan.has_table_filters(&table_name.to_lowercase()));
        }

        if has_filters {
            // Try columnar filter optimization for simple predicates
            // Extract predicates once and choose the best execution path (#2972)
            if let Some(column_predicates) =
                crate::select::columnar::extract_column_predicates(where_expr, &schema)
            {
                // OPTIMIZATION: Avoid double-cloning rows
                // Before: scan_live_vec() clones ALL rows, then filter clones passing rows again
                // After: scan() returns &[Row] references, filter returns indices,
                //        then we clone ONLY the rows that pass (and aren't deleted)
                //
                // For lineitem (60K rows, 20K pass filter):
                // - Before: 60K clones + 20K clones = 80K clones
                // - After: 20K clones only = 75% reduction in cloning
                let all_rows = table.scan();

                if crate::profiling::is_scan_debug_enabled() {
                    eprintln!(
                        "[SCAN_PATH] {} table: extracted {} columnar predicates for {} rows",
                        table_name,
                        column_predicates.len(),
                        all_rows.len()
                    );
                }

                // For native columnar tables, use SIMD filtering on typed columns
                // This avoids SqlValue overhead by working directly on i64/f64/String arrays
                if table.is_native_columnar() && all_rows.len() >= SIMD_COLUMNAR_THRESHOLD {
                    if let Ok(filtered_rows) = filter_with_simd_columnar(table, &column_predicates)
                    {
                        return Ok(super::FromResult::from_rows(schema, filtered_rows));
                    }
                    // Fall through to row-based path if SIMD fails
                }

                // For row-oriented tables, use cached columnar filter with late materialization
                // Issue #4136: Use database columnar cache for SIMD filtering, clone only passing rows
                if all_rows.len() >= SIMD_COLUMNAR_THRESHOLD {
                    if let Ok(filtered_rows) = filter_with_cached_columnar(
                        database,
                        table_name,
                        all_rows,
                        &column_predicates,
                    ) {
                        return Ok(super::FromResult::from_rows(schema, filtered_rows));
                    }
                    // Fall through to row-based path if cached columnar fails
                }

                // For smaller tables or if cached columnar fails, use direct row filtering
                let indices =
                    crate::select::columnar::apply_columnar_filter(all_rows, &column_predicates)?;

                // Clone only the rows that pass the filter AND aren't deleted
                // This is the key optimization: we skip cloning rows that don't pass
                let filtered_rows: Vec<_> = indices
                    .into_iter()
                    .filter(|&idx| !table.is_row_deleted(idx))
                    .filter_map(|idx| all_rows.get(idx).cloned())
                    .collect();
                return Ok(super::FromResult::from_rows(schema, filtered_rows));
            }

            // extract_column_predicates returned None - fall back
            if crate::profiling::is_scan_debug_enabled() {
                eprintln!("[SCAN_PATH] {} table: using generic predicate path (complex expression)",
                    table_name);
            }
            // Fall back to generic predicate evaluation for complex expressions
            // Must use scan_live_vec() here since apply_table_local_predicates expects owned rows
            // Note: Use effective_name (alias) for filter lookup since PredicatePlan uses schema table names
            // Issue #3562: Pass CTE context so IN subqueries can reference CTEs
            let live_rows = table.scan_live_vec();
            let filtered_rows = apply_table_local_predicates(
                live_rows,
                schema.clone(),
                &predicate_plan,
                &effective_name,
                database,
                None, // No outer context for predicate pushdown
                None,
                Some(cte_results), // CTE context for IN subqueries referencing CTEs
            )?;
            return Ok(super::FromResult::from_rows(schema, filtered_rows));
        }
    }

    // No table-local predicates or no WHERE clause: return live rows
    // Issue #3790: Must filter deleted rows via scan_live_vec()
    let live_rows = table.scan_live_vec();

    #[cfg(feature = "parallel")]
    let rows = parallel_scan_materialize(&live_rows);

    #[cfg(not(feature = "parallel"))]
    let rows = live_rows;

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
    let columnar_table =
        table.scan_columnar().map_err(|e| ExecutorError::StorageError(e.to_string()))?;

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

/// Filter row-oriented tables using database columnar cache with late materialization
///
/// This function implements the lazy columnar cache optimization from Issue #4136:
/// 1. Get cached columnar data from database.get_columnar() (LRU cache, Arc-shared)
/// 2. Convert to ColumnarBatch for SIMD operations (zero-copy via Arc)
/// 3. Filter using SIMD to get INDICES of passing rows (no row reconstruction)
/// 4. Clone only the rows that passed all predicates from live_rows
///
/// # Late Materialization Pattern
///
/// The key optimization is "late materialization" - we defer cloning row data
/// until after we know which rows pass all predicates. This avoids:
/// - Cloning all rows upfront (which we were doing via scan_live_vec())
/// - Reconstructing rows from columnar format after filtering
///
/// # Performance
///
/// For TPC-H Q10 with 600K lineitem rows where 150K pass:
/// - Old: Clone 600K rows, filter, keep 150K = 750K row operations
/// - New: Filter on columnar (cached), clone only 150K passing = 150K clones
/// - Expected: 5x reduction in memory allocation overhead
///
/// # Arguments
/// * `database` - Database containing the columnar cache
/// * `table_name` - Name of the table (for cache lookup)
/// * `live_rows` - Reference to live rows (already collected but not yet cloned into result)
/// * `predicates` - Column predicates for SIMD filtering
fn filter_with_cached_columnar(
    database: &vibesql_storage::Database,
    table_name: &str,
    live_rows: &[vibesql_storage::Row],
    predicates: &[ColumnPredicate],
) -> Result<Vec<vibesql_storage::Row>, ExecutorError> {
    // Step 1: Get cached columnar data from database
    // This uses the LRU columnar cache - if cached, this is O(1) Arc clone
    // If not cached, it converts and caches for future queries
    let columnar_table = database
        .get_columnar(table_name)
        .map_err(|e| ExecutorError::StorageError(e.to_string()))?
        .ok_or_else(|| ExecutorError::TableNotFound(table_name.to_string()))?;

    // Step 2: Convert to ColumnarBatch for SIMD operations
    // Zero-copy: just bumps Arc reference count
    let batch = ColumnarBatch::from_storage_columnar(&columnar_table)?;

    // Verify row count matches - columnar cache should be in sync with live_rows
    // (Cache invalidation on INSERT/UPDATE/DELETE ensures this)
    if batch.row_count() != live_rows.len() {
        // Cache is stale - invalidate and fall back to row-based filtering
        // This can happen in rare race conditions between cache population and mutations
        if crate::profiling::is_scan_debug_enabled() {
            eprintln!(
                "[SCAN_PATH] {} table: columnar cache stale (batch {} vs live {}), falling back",
                table_name,
                batch.row_count(),
                live_rows.len()
            );
        }
        return Err(ExecutorError::Other(format!(
            "Columnar cache stale for {} (expected {} rows, got {})",
            table_name,
            live_rows.len(),
            batch.row_count()
        )));
    }

    // Step 3: Apply SIMD-accelerated filtering to get INDICES only (not rows)
    // This is the key optimization - we don't reconstruct rows from columnar
    let passing_indices = simd_filter_to_indices(&batch, predicates)?;

    if crate::profiling::is_scan_debug_enabled() {
        eprintln!(
            "[SCAN_PATH] {} table: cached columnar filter {} -> {} rows ({}% selectivity)",
            table_name,
            live_rows.len(),
            passing_indices.len(),
            if live_rows.is_empty() { 0 } else { passing_indices.len() * 100 / live_rows.len() }
        );
    }

    // Step 4: Clone only the rows that passed all predicates (late materialization)
    // This is the payoff - we only clone passing_indices.len() rows instead of all rows
    let filtered_rows: Vec<vibesql_storage::Row> = passing_indices
        .into_iter()
        .filter_map(|idx| live_rows.get(idx).cloned())
        .collect();

    Ok(filtered_rows)
}

/// Try to use primary key index for O(1) point lookup
///
/// This optimization is critical for TPC-C workloads where most queries are point lookups
/// on primary key columns (e.g., `WHERE s_w_id = 1 AND s_i_id = 123`).
///
/// # Performance
/// For tables with 100K rows, this reduces lookup from O(n) table scan to O(1) hash lookup.
/// In TPC-C benchmarks, this can improve New-Order transaction from 800ms to <10ms.
///
/// # Returns
/// - `Ok(Some(result))` - Point lookup succeeded, result contains the matching row (or empty if no match)
/// - `Ok(None)` - Cannot use primary key lookup (fall back to other methods)
/// - `Err(...)` - An error occurred
///
/// # Arguments
/// * `column_aliases` - SQL:1999 E051-09: Optional column renaming (e.g., `FROM t AS a (x, y)`)
/// * `cte_results` - CTE context for IN subqueries that may reference CTEs (Issue #3562)
fn try_primary_key_lookup(
    table_name: &str,
    alias: Option<&String>,
    column_aliases: Option<&Vec<String>>,
    where_clause: Option<&vibesql_ast::Expression>,
    database: &vibesql_storage::Database,
    cte_results: &HashMap<String, CteResult>,
) -> Result<Option<super::FromResult>, ExecutorError> {
    // Need a WHERE clause to extract predicates
    let where_expr = match where_clause {
        Some(expr) => expr,
        None => return Ok(None),
    };

    // Get table and check if it has a primary key
    let table = match database.get_table(table_name) {
        Some(t) => t,
        None => return Ok(None),
    };

    // Get primary key column indices
    let pk_indices = match table.schema.get_primary_key_indices() {
        Some(indices) => indices,
        None => return Ok(None), // No primary key
    };

    // Get primary key column names
    let pk_column_names: Vec<&str> =
        pk_indices.iter().map(|&idx| table.schema.columns[idx].name.as_str()).collect();

    // Try to extract equality predicates for all primary key columns
    let pk_values = match extract_primary_key_values(where_expr, &pk_column_names) {
        Some(values) => values,
        None => return Ok(None), // Cannot extract all PK values
    };

    // Get primary key index
    let pk_index = match table.primary_key_index() {
        Some(idx) => idx,
        None => return Ok(None), // No PK index (shouldn't happen if pk_indices exists)
    };

    // Build schema for result
    let effective_name = alias.cloned().unwrap_or_else(|| table_name.to_string());
    // SQL:1999 E051-09: Apply column aliases if provided
    let table_schema = apply_column_aliases(table.schema.clone(), column_aliases)?;
    let schema = CombinedSchema::from_table(effective_name, table_schema);

    // Perform O(1) lookup in primary key index
    let rows = match pk_index.get(&pk_values) {
        Some(&row_idx) => {
            // Found the row via PK index - but we must still apply the FULL WHERE clause
            // in case there are additional predicates beyond the PK columns.
            // Example: SELECT * FROM stock WHERE s_w_id = 1 AND s_i_id = 123 AND s_quantity < 10
            // The PK lookup finds the row, but we must also check s_quantity < 10.
            // Issue #3790: Use get_row() which returns None for deleted rows
            if let Some(row) = table.get_row(row_idx) {
                // Evaluate the full WHERE clause on this row
                // Issue #3562: Pass CTE context so IN subqueries can reference CTEs
                let evaluator = if cte_results.is_empty() {
                    CombinedExpressionEvaluator::with_database(&schema, database)
                } else {
                    CombinedExpressionEvaluator::with_database_and_cte(
                        &schema,
                        database,
                        cte_results,
                    )
                };
                match evaluator.eval(where_expr, row) {
                    Ok(vibesql_types::SqlValue::Boolean(true)) => vec![row.clone()],
                    Ok(_) => vec![], // Row doesn't match full WHERE clause (false or NULL)
                    Err(_) => vec![], // Evaluation error - treat as no match
                }
            } else {
                vec![] // Row is deleted or index invalid
            }
        }
        None => vec![], // No matching row
    };

    Ok(Some(super::FromResult::from_rows(schema, rows)))
}

/// Extract primary key values from WHERE clause
///
/// Looks for equality predicates on all primary key columns and returns the values
/// in the order of the primary key columns.
///
/// # Example
/// For primary key (s_w_id, s_i_id) and WHERE clause `s_i_id = 123 AND s_w_id = 1`:
/// Returns Some([1, 123]) (values in PK column order, not WHERE clause order)
fn extract_primary_key_values(
    expr: &vibesql_ast::Expression,
    pk_column_names: &[&str],
) -> Option<Vec<vibesql_types::SqlValue>> {
    use std::collections::HashMap;

    // Collect all equality predicates: column_name -> value
    let mut predicates: HashMap<String, vibesql_types::SqlValue> = HashMap::new();
    collect_equality_predicates_recursive(expr, &mut predicates);

    // Check if we have predicates for all PK columns
    let mut values = Vec::with_capacity(pk_column_names.len());
    for &col_name in pk_column_names {
        // Case-insensitive lookup (SQL identifiers are normalized to uppercase)
        let col_upper = col_name.to_uppercase();
        match predicates.get(&col_upper) {
            Some(value) => values.push(value.clone()),
            None => return None, // Missing predicate for this PK column
        }
    }

    Some(values)
}

/// Recursively collect equality predicates from WHERE clause
fn collect_equality_predicates_recursive(
    expr: &vibesql_ast::Expression,
    predicates: &mut std::collections::HashMap<String, vibesql_types::SqlValue>,
) {
    use vibesql_ast::{BinaryOperator, Expression};

    match expr {
        // Handle equality: col = value or value = col
        Expression::BinaryOp { left, op: BinaryOperator::Equal, right } => {
            // Check col = literal
            if let Expression::ColumnRef { column, .. } = left.as_ref() {
                if let Expression::Literal(value) = right.as_ref() {
                    if !matches!(value, vibesql_types::SqlValue::Null) {
                        predicates.insert(column.to_uppercase(), value.clone());
                    }
                }
            }
            // Check literal = col (reversed)
            if let Expression::ColumnRef { column, .. } = right.as_ref() {
                if let Expression::Literal(value) = left.as_ref() {
                    if !matches!(value, vibesql_types::SqlValue::Null) {
                        predicates.insert(column.to_uppercase(), value.clone());
                    }
                }
            }
        }
        // Handle AND: recurse into both sides
        Expression::BinaryOp { left, op: BinaryOperator::And, right } => {
            collect_equality_predicates_recursive(left, predicates);
            collect_equality_predicates_recursive(right, predicates);
        }
        // Other expressions are not useful for PK lookup
        _ => {}
    }
}
