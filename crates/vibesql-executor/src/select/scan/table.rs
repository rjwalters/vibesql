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

use vibesql_catalog::TableIdentifier;

use super::predicates::{apply_table_local_predicates, filter_and_clone_rows};
#[cfg(feature = "parallel")]
use crate::select::parallel::parallel_scan_materialize;
use crate::{
    errors::ExecutorError,
    evaluator::{coercion::coerce_value_to_column_type, CombinedExpressionEvaluator},
    information_schema::{
        execute_information_schema_query, get_information_schema_table_schema, parse_qualified_name,
    },
    optimizer::PredicatePlan,
    pragma_compile_options::{
        execute_pragma_compile_options_query, get_pragma_compile_options_table_schema,
        is_pragma_compile_options_table,
    },
    privilege_checker::PrivilegeChecker,
    schema::CombinedSchema,
    select::{
        columnar::{simd_filter_batch, simd_filter_to_indices, ColumnPredicate, ColumnarBatch},
        cte::CteResult,
    },
    sqlite_schema::{
        execute_sqlite_schema_query, execute_sqlite_temp_schema_query,
        get_sqlite_schema_table_schema, is_sqlite_schema_table, is_sqlite_temp_schema_table,
    },
    sqlite_stat::{
        execute_sqlite_stat1_query, get_sqlite_stat1_table_schema, is_sqlite_stat_table,
    },
};

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
                generated_expr: col.generated_expr.clone(),
                collation: col.collation.clone(), // Preserve original collation
                is_exact_integer_type: col.is_exact_integer_type, // Preserve exact type flag
            })
            .collect();
        // TableSchema::new() rebuilds the column_index_cache
        return Ok(vibesql_catalog::TableSchema::new(schema.name.clone(), renamed_columns));
    }
    Ok(schema)
}

/// Resolve a bare column reference in a view body to its source column's
/// collation, walking the view's FROM clause (issue #5864).
///
/// Base-table sources contribute the column's declared collation; view sources
/// recurse into the inner view body so collation propagates through arbitrarily
/// nested views. Subqueries, table-valued functions, and other exotic sources
/// yield `None` (default BINARY), matching the safe-degradation contract of
/// `view_select_list_collations`.
fn view_body_column_collation(
    database: &vibesql_storage::Database,
    from: Option<&vibesql_ast::FromClause>,
    col_id: &vibesql_ast::ColumnIdentifier,
) -> Option<String> {
    let from = from?;
    let column_name = col_id.column_canonical();
    // A table-qualified reference resolves against that source directly.
    if let Some(table_name) = col_id.table_canonical() {
        return source_column_collation(database, table_name, column_name);
    }
    find_collation_in_from(database, from, column_name)
}

/// Search a FROM clause's table/join sources for a column's collation.
fn find_collation_in_from(
    database: &vibesql_storage::Database,
    from: &vibesql_ast::FromClause,
    column_name: &str,
) -> Option<String> {
    match from {
        vibesql_ast::FromClause::Table { name, .. } => {
            source_column_collation(database, name, column_name)
        }
        vibesql_ast::FromClause::Join { left, right, .. } => {
            find_collation_in_from(database, left, column_name)
                .or_else(|| find_collation_in_from(database, right, column_name))
        }
        _ => None,
    }
}

/// Resolve `column_name` in a FROM source named `source_name`. A base table
/// yields the column's declared collation; a view recurses into its defining
/// body so an explicit COLLATE (or a base column's collation) propagates
/// through nested views. Anything unresolved yields `None` (BINARY).
fn source_column_collation(
    database: &vibesql_storage::Database,
    source_name: &str,
    column_name: &str,
) -> Option<String> {
    if let Some(table) = database.get_table(source_name) {
        return table
            .schema
            .columns
            .iter()
            .find(|c| c.name.eq_ignore_ascii_case(column_name))
            .and_then(|c| c.collation.clone());
    }
    // View source: map the referenced column to its select-list position and
    // resolve that item's collation against the inner view's own FROM clause.
    let view = database.catalog.get_view(source_name)?;
    let inner_from = view.query.from.as_ref();
    if let Some(idx) = view_output_column_index(view, column_name) {
        let resolver = |c: &vibesql_ast::ColumnIdentifier| -> Option<String> {
            view_body_column_collation(database, inner_from, c)
        };
        let collations = crate::evaluator::collation::view_select_list_collations(
            &view.query.select_list,
            &resolver,
        );
        if let Some(result) = collations.get(idx).cloned().flatten() {
            return Some(result);
        }
    }
    // Wildcard fallback (issue #5925): `view_output_column_index` returns `None`
    // for a wildcard body (`SELECT * FROM ...`) because the output column's
    // select-list position is unknowable. It can also position the column via an
    // explicit view column list while the wildcard body still yields no derived
    // collation. In both cases resolve the column by name directly against the
    // inner view's FROM sources so collation propagates through nested wildcard
    // views.
    find_collation_in_from(database, inner_from?, column_name)
}

/// Map a view's exposed column name to its select-list index. Uses the view's
/// explicit column list when present, otherwise the select item's output name
/// (an alias, or a bare column reference's name). Returns `None` when the name
/// cannot be positioned (e.g. wildcard bodies) so collation degrades to BINARY.
fn view_output_column_index(
    view: &vibesql_catalog::ViewDefinition,
    column_name: &str,
) -> Option<usize> {
    if let Some(cols) = &view.columns {
        return cols.iter().position(|c| c.eq_ignore_ascii_case(column_name));
    }
    view.query.select_list.iter().position(|item| match item {
        vibesql_ast::SelectItem::Expression { expr, alias, .. } => {
            if let Some(a) = alias {
                a.eq_ignore_ascii_case(column_name)
            } else if let vibesql_ast::Expression::ColumnRef(c) = expr {
                c.column_canonical().eq_ignore_ascii_case(column_name)
            } else {
                false
            }
        }
        _ => false,
    })
}

/// Sort rows by INTEGER PRIMARY KEY column value (Issue #4926)
///
/// SQLite guarantees that tables with INTEGER PRIMARY KEY return rows in rowid order
/// when no ORDER BY is specified. This function sorts rows by the INTEGER PRIMARY KEY
/// column value to match SQLite's behavior.
///
/// # Arguments
/// * `rows` - Mutable vector of rows to sort in place
/// * `ipk_col_idx` - Column index of the INTEGER PRIMARY KEY column
fn sort_rows_by_integer_primary_key(rows: &mut Vec<vibesql_storage::Row>, ipk_col_idx: usize) {
    rows.sort_by(|a, b| {
        let a_val = a.get(ipk_col_idx);
        let b_val = b.get(ipk_col_idx);

        match (a_val, b_val) {
            (
                Some(vibesql_types::SqlValue::Integer(a)),
                Some(vibesql_types::SqlValue::Integer(b)),
            ) => a.cmp(b),
            (
                Some(vibesql_types::SqlValue::Bigint(a)),
                Some(vibesql_types::SqlValue::Bigint(b)),
            ) => a.cmp(b),
            (
                Some(vibesql_types::SqlValue::Unsigned(a)),
                Some(vibesql_types::SqlValue::Unsigned(b)),
            ) => a.cmp(b),
            // Cross-type comparisons (SQLite INTEGER can be any of these)
            (
                Some(vibesql_types::SqlValue::Integer(a)),
                Some(vibesql_types::SqlValue::Bigint(b)),
            ) => (*a).cmp(b),
            (
                Some(vibesql_types::SqlValue::Bigint(a)),
                Some(vibesql_types::SqlValue::Integer(b)),
            ) => a.cmp(&(*b)),
            // NULL handling: NULLs sort first (SQLite behavior)
            (None, _) | (Some(vibesql_types::SqlValue::Null), _) => std::cmp::Ordering::Less,
            (_, None) | (_, Some(vibesql_types::SqlValue::Null)) => std::cmp::Ordering::Greater,
            // Fallback for unexpected types
            _ => std::cmp::Ordering::Equal,
        }
    });
}

/// Execute a table scan with SQL:1999 identifier semantics
///
/// This is the new entry point that properly handles case-sensitivity based on
/// whether the identifier was quoted in the original SQL.
///
/// # Arguments
/// * `identifier` - TableIdentifier with proper case semantics
/// * `alias` - Optional table alias
/// * `column_aliases` - SQL:1999 E051-09: Optional column renaming (e.g., `FROM t AS a (x, y)`)
/// * `cte_results` - CTE context for the query
/// * `database` - Database reference
/// * `where_clause` - Optional WHERE clause for filtering
/// * `order_by` - Optional ORDER BY clause for index selection
/// * `limit` - Optional LIMIT value for early termination optimization (#3253)
/// * `outer_row` - Outer row for correlated subqueries
/// * `outer_schema` - Outer schema for correlated subqueries
pub(crate) fn execute_table_scan_with_identifier(
    identifier: &TableIdentifier,
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
    // Use the canonical form for table lookup (lowercase for unquoted, exact for quoted)
    // CTE lookup in execute_table_scan is already case-insensitive
    execute_table_scan(
        identifier.canonical(),
        alias,
        column_aliases,
        cte_results,
        database,
        where_clause,
        order_by,
        limit,
        outer_row,
        outer_schema,
    )
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
        // Note: Keep schema name as CTE name (not alias) for column name generation
        // SQLite returns original CTE names even when aliases are used
        let schema = CombinedSchema::from_table(effective_name.clone(), cte_table_schema);

        // Apply table-local predicates from WHERE clause using pre-computed plan
        // Skip predicate pushdown for correlated subqueries (filtering happens later with full
        // context)
        let is_correlated = outer_row.is_some() || outer_schema.is_some();
        if where_clause.is_some() && !is_correlated {
            // Build predicate plan once for this table
            let predicate_plan = PredicatePlan::from_where_clause(where_clause, &schema)
                .map_err(ExecutorError::InvalidWhereClause)?;

            // Issue #4199: Use filter-while-copy optimization for CTEs
            // This avoids cloning ALL rows before filtering - only clone rows that pass the filter.
            // Critical for queries like TPC-DS Q4 with 6-way self-join on CTEs.
            // Note: Use effective_name (alias) for filter lookup since PredicatePlan uses schema
            // table names
            let rows = filter_and_clone_rows(
                cte_rows.as_ref(),
                schema.clone(),
                &predicate_plan,
                &effective_name,
                database,
                Some(cte_results), // CTE context for IN subqueries referencing CTEs
            )?;
            return Ok(super::FromResult::from_rows(schema, rows));
        }

        // No filtering needed - use zero-copy shared rows
        // This avoids O(n) cloning when CTE is referenced multiple times
        return Ok(super::FromResult::from_shared_rows(schema, cte_rows.clone()));
    }

    // Check if it's sqlite_master or sqlite_schema (SQLite compatibility)
    if is_sqlite_schema_table(table_name) {
        // Execute sqlite_master query
        let result = execute_sqlite_schema_query(&database.catalog)?;

        // Get the schema for sqlite_master
        let table_schema = get_sqlite_schema_table_schema();

        let effective_name = alias.cloned().unwrap_or_else(|| table_name.to_string());
        // SQL:1999 E051-09: Apply column aliases if provided
        let table_schema = apply_column_aliases(table_schema, column_aliases)?;
        // Note: Keep schema name as original table name for column name generation
        let schema = CombinedSchema::from_table(effective_name, table_schema);

        return Ok(super::FromResult::from_rows(schema, result.rows));
    }

    // Check if it's sqlite_temp_master or sqlite_temp_schema (temp-schema
    // introspection — temp tables and indexes on temp tables). See #5513.
    if is_sqlite_temp_schema_table(table_name) {
        let result = execute_sqlite_temp_schema_query(&database.catalog)?;

        // sqlite_temp_master shares sqlite_master's column shape.
        let table_schema = get_sqlite_schema_table_schema();

        let effective_name = alias.cloned().unwrap_or_else(|| table_name.to_string());
        let table_schema = apply_column_aliases(table_schema, column_aliases)?;
        let schema = CombinedSchema::from_table(effective_name, table_schema);

        return Ok(super::FromResult::from_rows(schema, result.rows));
    }

    // Check if it's the pragma_compile_options eponymous system table (#6019).
    // Referenced with bare-identifier FROM syntax; VibeSQL advertises no
    // compile-time options, so this yields the correct single-column shape with
    // zero rows.
    //
    // A real user table of the same name takes precedence (#6030): SQLite's
    // eponymous virtual tables are shadowed by a same-named real table, and
    // `pragma_*` names are not reserved at CREATE TABLE time. Probe the catalog
    // first (case-insensitive, matching `is_pragma_compile_options_table`'s
    // folding) and only fall through to the synthetic zero-row table when no
    // real table exists.
    if is_pragma_compile_options_table(table_name) && database.get_table(table_name).is_none() {
        let result = execute_pragma_compile_options_query()?;
        let table_schema = get_pragma_compile_options_table_schema();

        let effective_name = alias.cloned().unwrap_or_else(|| table_name.to_string());
        let table_schema = apply_column_aliases(table_schema, column_aliases)?;
        let schema = CombinedSchema::from_table(effective_name, table_schema);

        return Ok(super::FromResult::from_rows(schema, result.rows));
    }

    // Check if it's sqlite_stat1/stat2/stat3/stat4 (SQLite compatibility)
    if is_sqlite_stat_table(table_name) {
        // Execute sqlite_stat1 query
        let result = execute_sqlite_stat1_query(&database.catalog, database)?;

        // Get the schema for sqlite_stat1
        let table_schema = get_sqlite_stat1_table_schema();

        let effective_name = alias.cloned().unwrap_or_else(|| table_name.to_string());
        // SQL:1999 E051-09: Apply column aliases if provided
        let table_schema = apply_column_aliases(table_schema, column_aliases)?;
        // Note: Keep schema name as original table name for column name generation
        let schema = CombinedSchema::from_table(effective_name, table_schema);

        return Ok(super::FromResult::from_rows(schema, result.rows));
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
        // Note: Keep schema name as original table name for column name generation
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

        // Get both rows and column metadata.
        //
        // A main-schema view body resolves its tables against the database
        // schema, so when one of those tables has since been dropped sqlite3
        // names it with the implicit `main.` prefix (`no such table:
        // main.test2`). Qualify the missing-table error here so the read path
        // matches sqlite3 3.51.0 and the INSTEAD OF view-DML paths fixed in
        // #5569. Two cases stay unqualified, also matching sqlite3: a bare
        // `SELECT * FROM missing_table` (not via a view) never reaches this
        // view-body resolution, and a temp view's body resolves against the
        // temp schema, which sqlite3 reports without a schema prefix. See #5570.
        let select_result = if view.is_temp() {
            executor.execute_with_columns(&view.query)?
        } else {
            executor
                .execute_with_columns(&view.query)
                .map_err(ExecutorError::with_main_schema_qualifier)?
        };

        // Build a schema from the column names
        // Apply view's explicit column aliases if provided
        let column_names = if let Some(ref view_columns) = view.columns {
            // Use view's explicit column names
            view_columns.clone()
        } else {
            // Use column names from the SELECT statement
            select_result.columns.clone()
        };

        // Propagate each view column's collation from the view body's select
        // list so that comparisons in the outer query use the right collating
        // sequence (ticket a7debbe0ad1, issue #5864). Without this, an outer
        // `SELECT B < a FROM v` where the view defines `B` as
        // `'B' COLLATE NOCASE` would compare BINARY instead of NOCASE.
        //
        // Only the (left) branch of the body's select list is inspected — for
        // a UNION body SQLite likewise takes the collation from the first
        // branch. The derived vector is applied only when it aligns 1:1 with
        // the result columns.
        let view_from = view.query.from.as_ref();
        let column_collation_fn = |col_id: &vibesql_ast::ColumnIdentifier| -> Option<String> {
            view_body_column_collation(database, view_from, col_id)
        };
        let derived_collations = crate::evaluator::collation::view_select_list_collations(
            &view.query.select_list,
            &column_collation_fn,
        );
        let use_derived_collations = derived_collations.len() == column_names.len();
        // Wildcard fallback (issue #5925): a `SELECT *` (or `SELECT t.*`) body
        // yields a select-item count that differs from the expanded output
        // column count, so `view_select_list_collations` cannot align 1:1.
        // Instead of degrading every column to BINARY, resolve each output
        // column by name against the view's FROM sources. `find_collation_in_from`
        // recurses through JOINs and delegates to base tables / nested views, so
        // multi-table wildcard bodies (`SELECT * FROM t1 JOIN t2 ...`) are
        // covered. A body with no FROM clause (e.g. `SELECT * FROM (VALUES ...)`)
        // resolves to `None` and degrades gracefully.
        let wildcard_collations: Vec<Option<String>> = if use_derived_collations {
            Vec::new()
        } else {
            column_names
                .iter()
                .map(|name| view_from.and_then(|from| find_collation_in_from(database, from, name)))
                .collect()
        };
        let collation_for = |idx: usize| -> Option<String> {
            if use_derived_collations {
                derived_collations.get(idx).cloned().flatten()
            } else {
                wildcard_collations.get(idx).cloned().flatten()
            }
        };

        // Since views can have arbitrary SELECT expressions, we derive column types from the first
        // row
        let columns = if !select_result.rows.is_empty() {
            let first_row = &select_result.rows[0];
            column_names
                .iter()
                .zip(&first_row.values)
                .enumerate()
                .map(|(idx, (name, value))| {
                    vibesql_catalog::ColumnSchema {
                        name: name.clone(),
                        data_type: value.get_type(),
                        nullable: true, // Views return nullable columns by default
                        default_value: None,
                        generated_expr: None, // Views don't have generated columns
                        collation: collation_for(idx), // Propagate view body collation (#5864)
                        is_exact_integer_type: false, // Views don't preserve exact type
                    }
                })
                .collect()
        } else {
            // For empty views, create columns without specific types
            // This is a limitation but views with no rows are edge cases
            column_names
                .iter()
                .enumerate()
                .map(|(idx, name)| vibesql_catalog::ColumnSchema {
                    name: name.clone(),
                    data_type: vibesql_types::DataType::Varchar { max_length: None },
                    nullable: true,
                    default_value: None,
                    generated_expr: None, // Views don't have generated columns
                    collation: collation_for(idx), // Propagate view body collation (#5864)
                    is_exact_integer_type: false, // Views don't preserve exact type
                })
                .collect()
        };

        let mut view_schema = vibesql_catalog::TableSchema::new(table_name.to_string(), columns);
        // Views have no implicit rowid: a `rowid`/`oid`/`_rowid_` reference
        // against a view in a SELECT must error (`no such column: rowid`),
        // matching sqlite3 with the default `allow_rowid_in_view` off (#5492).
        view_schema.set_is_view(true);
        let effective_name = alias.cloned().unwrap_or_else(|| table_name.to_string());
        // SQL:1999 E051-09: Apply column aliases if provided
        let view_schema = apply_column_aliases(view_schema, column_aliases)?;
        // Note: Keep schema name as view name (not alias) for column name generation
        let schema = CombinedSchema::from_table(effective_name.clone(), view_schema);
        let mut rows = select_result.rows;

        // Apply table-local predicates from WHERE clause using pre-computed plan
        // Skip predicate pushdown for correlated subqueries (filtering happens later with full
        // context)
        let is_correlated = outer_row.is_some() || outer_schema.is_some();
        if where_clause.is_some() && !is_correlated {
            // Build predicate plan once for this table
            let predicate_plan = PredicatePlan::from_where_clause(where_clause, &schema)
                .map_err(ExecutorError::InvalidWhereClause)?;

            // Note: Use effective_name (alias) for filter lookup since PredicatePlan uses schema
            // table names Issue #3562: Pass CTE context so IN subqueries can reference
            // CTEs
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
    if let Some(result) = try_primary_key_lookup(
        table_name,
        alias,
        column_aliases,
        where_clause,
        database,
        cte_results,
    )? {
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
                    eprintln!(
                        "[SCAN_PATH] Using index scan: table={}, index={}",
                        table_name, index_name
                    );
                }
                // Pass limit for LIMIT pushdown optimization when ORDER BY is satisfied by index
                // (#3253) Issue #3562: Pass CTE context so IN subqueries can
                // reference CTEs
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
            super::index_scan::IndexScanChoice::MultiIndexOr { branches, residual } => {
                // MULTI-INDEX OR (epic #5668, PR 2). Execute the union of
                // per-branch index lookups, deduplicating by rowid
                // (insertion-ordered), then apply the residual non-OR
                // AND-conjuncts as a post-filter.
                //
                // Selection only produces this variant for a genuine multi-index
                // union (>= 2 distinct indexes, no ORDER BY, rowid table) with
                // the feature flag on — see `try_multi_index_or`. The resulting
                // row set is identical to the prior single-index-scan + full-OR
                // residual path; only the access path differs.
                if crate::profiling::is_scan_debug_enabled() {
                    eprintln!(
                        "[SCAN_PATH] Using MULTI-INDEX OR: table={}, branches={}",
                        table_name,
                        branches.len()
                    );
                }
                return super::index_scan::execute_multi_index_or(
                    table_name,
                    alias,
                    &branches,
                    residual.as_ref(),
                    database,
                    cte_results,
                );
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
    // Note: Keep table_schema.name as the original table name (not the alias)
    // SQLite returns original table names in column names even when aliases are used
    // e.g., "SELECT a.f1 FROM test1 a" with full_column_names=ON returns "test1.f1"
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
            // Phase 1d of #5136: also apply MVCC visibility when feature is on.
            let snapshot = crate::mvcc::read_snapshot(database);
            let mut live_rows = table.scan_visible_vec(&snapshot);
            // sqlite_search_count: Track rows examined during table scan
            database.increment_search_count(live_rows.len() as u64);
            // Issue #4926: SQLite returns INTEGER PRIMARY KEY tables in rowid order
            if order_by.is_none() {
                if let Some(ipk_col_idx) = table.schema.rowid_alias_column {
                    sort_rows_by_integer_primary_key(&mut live_rows, ipk_col_idx);
                }
            }
            use crate::select::from_iterator::FromIterator;
            return Ok(super::FromResult::from_iterator(
                schema,
                FromIterator::from_table_scan(live_rows),
            ));
        }

        // Build predicate plan once for this table
        let predicate_plan = PredicatePlan::from_where_clause(Some(where_expr), &schema)
            .map_err(ExecutorError::InvalidWhereClause)?;

        // Check if WHERE clause contains a constant FALSE predicate (e.g., `1 = 2`)
        // If so, return empty result immediately - no need to scan the table
        if predicate_plan.is_always_false() {
            if crate::profiling::is_scan_debug_enabled() {
                eprintln!(
                    "[SCAN_PATH] {} table: WHERE clause is always false, returning empty",
                    table_name
                );
            }
            return Ok(super::FromResult::from_rows_where_filtered(schema, Vec::new(), None));
        }

        // Check if there are actually table-local predicates for this table
        // Note: has_table_filters does case-sensitive lookup
        // Must check BOTH effective_name (alias) AND table_name because:
        // - IN predicates from OR expressions use the alias (e.g., "n1" from "nation n1")
        // - Regular predicates may use the actual table name
        let effective_name_lower = effective_name.to_lowercase();
        let has_table_local = predicate_plan.has_table_filters(&effective_name)
            || predicate_plan.has_table_filters(&effective_name_lower)
            || predicate_plan.has_table_filters(table_name)
            || predicate_plan.has_table_filters(&table_name.to_lowercase());

        // Issue #5719: the optimized `has_filters` block below applies only the
        // table-local predicates and then marks the WHERE as fully consumed
        // (`from_rows_where_filtered(.., None)`). When the WHERE also carries a
        // "complex" predicate — e.g. a scalar-subquery equality
        // `run_id = (SELECT MAX(run_id) FROM t)`, which `decompose_where_clause`
        // routes to `complex_predicates` rather than `table_local_predicates` —
        // consuming the WHERE here would silently DROP that conjunct and
        // over-return rows. In that case fall through to the unfiltered path,
        // which preserves the WHERE (`where_filtered = false`) so the executor
        // re-evaluates the FULL WHERE clause (including the scalar subquery).
        let has_complex_predicates = !predicate_plan.get_complex_predicates().is_empty();
        let has_filters = has_table_local && !has_complex_predicates;

        if crate::profiling::is_scan_debug_enabled() {
            eprintln!("[SCAN_PATH] {} (alias={}) table: has_filters={} (effective_name={}, table_name={})",
                table_name, effective_name, has_filters,
                predicate_plan.has_table_filters(&effective_name_lower),
                predicate_plan.has_table_filters(&table_name.to_lowercase()));
        }

        if has_filters {
            // Try columnar filter optimization for simple predicates
            // Extract predicates once and choose the best execution path (#2972)
            //
            // Issue #5719: this single-table scan path consumes the ENTIRE WHERE
            // clause — on success it returns `from_rows_where_filtered(.., None)`,
            // marking the WHERE as fully applied. The lenient
            // `extract_column_predicates` silently skips conjuncts it cannot fold
            // (e.g. a scalar subquery `run_id = (SELECT MAX(run_id) FROM t)`),
            // which would then be dropped entirely, over-returning rows. Use the
            // strict full-coverage extractor: it returns `Some` only when every
            // conjunct is columnar, otherwise we fall through to the generic
            // predicate path below, which evaluates the full WHERE (including
            // scalar subqueries) correctly.
            if let Some(column_predicates) =
                crate::select::columnar::extract_full_coverage_predicates(
                    where_expr,
                    &schema,
                    database.case_sensitive_like(),
                )
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
                // sqlite_search_count: Track rows examined during table scan
                database.increment_search_count(all_rows.len() as u64);

                if crate::profiling::is_scan_debug_enabled() {
                    eprintln!(
                        "[SCAN_PATH] {} table: extracted {} columnar predicates for {} rows",
                        table_name,
                        column_predicates.len(),
                        all_rows.len()
                    );
                }

                // Phase 1d of #5136: SIMD/columnar fast paths originally
                // bypassed per-row visibility checks. PR #5209 gated them to
                // MVCC-OFF only as a conservative first step. Issue #5206
                // (this code) re-enables them under MVCC-ON by applying a
                // post-SIMD `is_row_visible` filter — Approach A from the
                // issue. A follow-up tracks Approach B (pre-computed
                // visibility bitmap ANDed into the SIMD predicate mask),
                // which removes the post-filter pass entirely.
                //
                // Snapshot is captured here so both fast paths share it; with
                // the `mvcc_enabled` feature OFF this reduces to the cheap
                // bitmap-only check inside `Table::is_row_visible` and the
                // snapshot is unused.
                let snapshot = crate::mvcc::read_snapshot(database);

                // For native columnar tables, use SIMD filtering on typed columns
                // This avoids SqlValue overhead by working directly on i64/f64/String arrays.
                //
                // MVCC safety (Issue #5206): the `native_columnar` mirror
                // holds exactly the bitmap-live rows — INSERT appends to it,
                // DELETE/UPDATE bitmap-deletes trigger a rebuild that excludes
                // tombstoned rows, and ROLLBACK restores the BEGIN-time table
                // clone (including the mirror). Under the current single-writer
                // transaction model every bitmap-live row is visible to the
                // active reader's snapshot: own-txn writes are visible (#5223),
                // and any other writer must have committed before this
                // reader's snapshot was captured. So this path needs no
                // per-row visibility post-filter to match the row-by-row
                // path's output. NOTE: if concurrent writers are ever
                // introduced, this argument breaks and the post-filter from
                // `filter_with_cached_columnar` must be applied here too
                // (the rows materialized by `ColumnarTable::to_rows` carry
                // the always-visible pre-MVCC sentinel, so they cannot be
                // re-checked after materialization).
                if table.is_native_columnar() && all_rows.len() >= SIMD_COLUMNAR_THRESHOLD {
                    if let Ok(mut filtered_rows) =
                        filter_with_simd_columnar(table, &column_predicates)
                    {
                        // Issue #4926: SQLite returns INTEGER PRIMARY KEY tables in rowid order
                        if order_by.is_none() {
                            if let Some(ipk_col_idx) = table.schema.rowid_alias_column {
                                sort_rows_by_integer_primary_key(&mut filtered_rows, ipk_col_idx);
                            }
                        }
                        // Mark WHERE as already filtered to avoid double-evaluation
                        return Ok(super::FromResult::from_rows_where_filtered(
                            schema,
                            filtered_rows,
                            None,
                        ));
                    }
                    // Fall through to row-based path if SIMD fails
                }

                // For row-oriented tables, use cached columnar filter with late materialization
                // Issue #4136: Use database columnar cache for SIMD filtering, clone only passing
                // rows
                //
                // Issue #5206: under MVCC-ON, `filter_with_cached_columnar`
                // applies `Table::is_row_visible(idx, &snapshot)` as a
                // post-SIMD filter so that rows tombstoned by concurrent
                // writers or written by uncommitted/future txns are not
                // surfaced through the columnar fast path.
                if all_rows.len() >= SIMD_COLUMNAR_THRESHOLD {
                    if let Ok(mut filtered_rows) = filter_with_cached_columnar(
                        database,
                        table,
                        table_name,
                        all_rows,
                        &column_predicates,
                        &snapshot,
                    ) {
                        // Issue #4926: SQLite returns INTEGER PRIMARY KEY tables in rowid order
                        if order_by.is_none() {
                            if let Some(ipk_col_idx) = table.schema.rowid_alias_column {
                                sort_rows_by_integer_primary_key(&mut filtered_rows, ipk_col_idx);
                            }
                        }
                        // Mark WHERE as already filtered to avoid double-evaluation
                        return Ok(super::FromResult::from_rows_where_filtered(
                            schema,
                            filtered_rows,
                            None,
                        ));
                    }
                    // Fall through to row-based path if cached columnar fails
                }

                // For smaller tables or if cached columnar fails, use direct row filtering
                let indices =
                    crate::select::columnar::apply_columnar_filter(all_rows, &column_predicates)?;

                // Clone only the rows that pass the filter AND aren't deleted AND
                // (under MVCC) are visible to the current snapshot.
                // Issue #4370: Preserve row_id for ROWID pseudo-column support
                // Issue #4536: Preserve explicit row_id from INSERT INTO t(rowid, ...) VALUES(...)
                // Phase 1d of #5136: also apply MVCC visibility when feature is on.
                // Issue #5206: snapshot is already captured above (shared with the
                // columnar fast paths) so we just reuse it here.
                let mut filtered_rows: Vec<_> = indices
                    .into_iter()
                    .filter(|&idx| table.is_row_visible(idx, &snapshot))
                    .filter_map(|idx| {
                        all_rows.get(idx).map(|row| {
                            let mut cloned = row.clone();
                            // Preserve explicit row_id if set, otherwise use 1-indexed position
                            if cloned.row_id.is_none() {
                                cloned.row_id = Some((idx + 1) as u64);
                            }
                            cloned
                        })
                    })
                    .collect();
                // Issue #4926: SQLite returns INTEGER PRIMARY KEY tables in rowid order
                if order_by.is_none() {
                    if let Some(ipk_col_idx) = table.schema.rowid_alias_column {
                        sort_rows_by_integer_primary_key(&mut filtered_rows, ipk_col_idx);
                    }
                }
                // Mark WHERE as already filtered to avoid double-evaluation
                return Ok(super::FromResult::from_rows_where_filtered(
                    schema,
                    filtered_rows,
                    None,
                ));
            }

            // extract_column_predicates returned None - fall back
            if crate::profiling::is_scan_debug_enabled() {
                eprintln!(
                    "[SCAN_PATH] {} table: using generic predicate path (complex expression)",
                    table_name
                );
            }
            // Fall back to generic predicate evaluation for complex expressions
            // Must use scan_live_vec() here since apply_table_local_predicates expects owned rows
            // Note: Use effective_name (alias) for filter lookup since PredicatePlan uses schema
            // table names Issue #3562: Pass CTE context so IN subqueries can reference
            // CTEs
            // Phase 1d of #5136: also apply MVCC visibility when feature is on.
            let snapshot = crate::mvcc::read_snapshot(database);
            let live_rows = table.scan_visible_vec(&snapshot);
            // sqlite_search_count: Track rows examined during table scan
            database.increment_search_count(live_rows.len() as u64);
            let mut filtered_rows = apply_table_local_predicates(
                live_rows,
                schema.clone(),
                &predicate_plan,
                &effective_name,
                database,
                None, // No outer context for predicate pushdown
                None,
                Some(cte_results), // CTE context for IN subqueries referencing CTEs
            )?;
            // Issue #4926: SQLite returns INTEGER PRIMARY KEY tables in rowid order
            if order_by.is_none() {
                if let Some(ipk_col_idx) = table.schema.rowid_alias_column {
                    sort_rows_by_integer_primary_key(&mut filtered_rows, ipk_col_idx);
                }
            }
            // Mark WHERE as already filtered to avoid double-evaluation
            return Ok(super::FromResult::from_rows_where_filtered(schema, filtered_rows, None));
        }
    }

    // No table-local predicates or no WHERE clause: return live rows
    // Issue #3790: Must filter deleted rows via scan_live_vec()
    // Phase 1d of #5136: also apply MVCC visibility when feature is on.
    let snapshot = crate::mvcc::read_snapshot(database);
    let mut live_rows = table.scan_visible_vec(&snapshot);
    // sqlite_search_count: Track rows examined during table scan
    database.increment_search_count(live_rows.len() as u64);

    // Issue #4926: SQLite returns INTEGER PRIMARY KEY tables in rowid order
    // when no explicit ORDER BY is specified. Apply implicit sorting here.
    if order_by.is_none() {
        if let Some(ipk_col_idx) = table.schema.rowid_alias_column {
            sort_rows_by_integer_primary_key(&mut live_rows, ipk_col_idx);
        }
    }

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
/// # MVCC visibility (Issue #5206)
///
/// When the `mvcc_enabled` feature is ON, SIMD/columnar filtering on its
/// own would surface rows that should be hidden from `snapshot` (rows
/// created by uncommitted/future txns, or tombstoned by a concurrent
/// writer). To stay correct, this function applies
/// [`Table::is_row_visible`](vibesql_storage::Table::is_row_visible) as a
/// post-SIMD filter on the indices that pass the predicate evaluation.
/// This is Approach A from issue #5206; a follow-up tracks the more
/// invasive Approach B (pre-computed visibility bitmap ANDed into the
/// SIMD predicate mask, removing the post-filter pass entirely).
///
/// With the feature OFF, `Table::is_row_visible` collapses to the
/// existing not-deletion-bitmap-tombstoned check, so behavior is
/// identical to pre-MVCC.
///
/// # Arguments
/// * `database` - Database containing the columnar cache
/// * `table` - Table reference for `is_row_visible` lookups
/// * `table_name` - Name of the table (for cache lookup)
/// * `live_rows` - Reference to live rows (already collected but not yet cloned into result)
/// * `predicates` - Column predicates for SIMD filtering
/// * `snapshot` - MVCC snapshot for visibility filtering (ignored under `mvcc_enabled = OFF`)
fn filter_with_cached_columnar(
    database: &vibesql_storage::Database,
    table: &vibesql_storage::Table,
    table_name: &str,
    live_rows: &[vibesql_storage::Row],
    predicates: &[ColumnPredicate],
    snapshot: &vibesql_storage::TxnSnapshot,
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
    // Issue #4370: Preserve row_id for ROWID pseudo-column support
    // Issue #4536: Preserve explicit row_id from INSERT INTO t(rowid, ...) VALUES(...)
    // Issue #5206: Apply MVCC visibility as a post-SIMD filter (Approach A).
    //
    // The Step-2 invariant (`batch.row_count() == live_rows.len()`) plus the
    // fact that `live_rows` is `table.scan()` in physical order means that
    // each index produced by SIMD is also a valid physical row index for
    // `table.is_row_visible`. With `mvcc_enabled` OFF, `is_row_visible`
    // reduces to the not-deletion-bitmap-tombstoned check (cheap).
    let filtered_rows: Vec<vibesql_storage::Row> = passing_indices
        .into_iter()
        .filter(|&idx| table.is_row_visible(idx, snapshot))
        .filter_map(|idx| {
            live_rows.get(idx).map(|row| {
                let mut cloned = row.clone();
                // Preserve explicit row_id if set, otherwise use 1-indexed position
                if cloned.row_id.is_none() {
                    cloned.row_id = Some((idx + 1) as u64);
                }
                cloned
            })
        })
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
/// - `Ok(Some(result))` - Point lookup succeeded, result contains the matching row (or empty if no
///   match)
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

    // Coerce PK values to match column data types (SQLite type affinity)
    // This handles cases like WHERE i='12' where i is INTEGER PRIMARY KEY
    // The literal '12' must be coerced to Integer(12) for the index lookup
    let pk_values: Vec<vibesql_types::SqlValue> = pk_values
        .into_iter()
        .zip(pk_indices.iter())
        .map(|(val, &idx)| {
            let col_type = &table.schema.columns[idx].data_type;
            coerce_value_to_column_type(val.clone(), col_type)
        })
        .collect();

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
    let lookup_result = pk_index.get(&pk_values);
    // Issue #5204: thread the MVCC snapshot through the PK fast path. Off-state
    // (`mvcc_enabled` OFF): `is_row_visible` reduces to the existing
    // not-bitmap-deleted check, so behavior is identical to today.
    let snapshot = crate::mvcc::read_snapshot(database);
    let rows = match lookup_result {
        Some(&row_idx) => {
            // Found the row via PK index - but we must still apply the FULL WHERE clause
            // in case there are additional predicates beyond the PK columns.
            // Example: SELECT * FROM stock WHERE s_w_id = 1 AND s_i_id = 123 AND s_quantity < 10
            // The PK lookup finds the row, but we must also check s_quantity < 10.
            // Issue #3790: Use get_row() which returns None for deleted rows
            // Issue #5204: also enforce MVCC visibility — a row that has been
            // tombstoned (xmax stamped) by a concurrent txn must not surface
            // through the PK fast path.
            if !table.is_row_visible(row_idx, &snapshot) {
                vec![]
            } else if let Some(row) = table.get_row(row_idx) {
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
                let eval_result = evaluator.eval(where_expr, row);
                match eval_result {
                    Ok(vibesql_types::SqlValue::Boolean(true)) => {
                        vec![row.clone()]
                    }
                    Ok(vibesql_types::SqlValue::Integer(i)) if i != 0 => {
                        // SQLite compatibility: treat non-zero integer as true
                        vec![row.clone()]
                    }
                    Ok(_) => {
                        vec![]
                    }
                    Err(_) => {
                        vec![]
                    }
                }
            } else {
                vec![] // Row is deleted or index invalid
            }
        }
        None => vec![], // No matching row
    };
    // Use from_rows_where_filtered because we already evaluated the WHERE clause
    // when filtering the PK-matched row. This prevents double-filtering in fast path.
    Ok(Some(super::FromResult::from_rows_where_filtered(schema, rows, None)))
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
            if let Expression::ColumnRef(col_id) = left.as_ref() {
                if let Expression::Literal(value) = right.as_ref() {
                    if !matches!(value, vibesql_types::SqlValue::Null) {
                        predicates.insert(col_id.column_canonical().to_uppercase(), value.clone());
                    }
                }
            }
            // Check literal = col (reversed)
            if let Expression::ColumnRef(col_id) = right.as_ref() {
                if let Expression::Literal(value) = left.as_ref() {
                    if !matches!(value, vibesql_types::SqlValue::Null) {
                        predicates.insert(col_id.column_canonical().to_uppercase(), value.clone());
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

/// Execute a table scan with Bloom filter pre-filtering for join optimization.
///
/// This function is used during join reordering to filter rows DURING scan based on
/// a Bloom filter built from the accumulated join result. This is critical for
/// multi-way join performance because it avoids scanning large tables (like lineitem)
/// when most rows won't match the join condition.
///
/// # Arguments
/// * `table_name` - Name of the table to scan
/// * `alias` - Optional table alias
/// * `cte_results` - CTE context for the query
/// * `database` - Database reference
/// * `where_clause` - Optional WHERE clause for filtering
/// * `bloom_context` - Optional Bloom filter context for join pre-filtering
///
/// # Performance
///
/// For TPC-H Q5 at scale factor 0.01:
/// - Without Bloom filtering: scans all 60K lineitem rows, then filters via hash join
/// - With Bloom filtering: checks Bloom filter during scan, only keeps matching rows
///
/// This can reduce scan time from ~21ms to ~5ms for lineitem alone.
#[allow(clippy::too_many_arguments)]
pub(crate) fn execute_table_scan_with_bloom(
    table_name: &str,
    alias: Option<&String>,
    cte_results: &HashMap<String, CteResult>,
    database: &vibesql_storage::Database,
    where_clause: Option<&vibesql_ast::Expression>,
    bloom_context: Option<&super::bloom_context::BloomFilterScanContext>,
) -> Result<super::FromResult, ExecutorError> {
    use super::bloom_context::hash_value;

    // Check if table is a CTE first (with case-insensitive lookup)
    // Issue #4338: CTEs must be checked before database tables to support
    // CTE-to-CTE references in join reordering when Bloom filter optimization is enabled
    let cte_result = cte_results.get(table_name).or_else(|| {
        cte_results
            .iter()
            .find(|(key, _)| key.eq_ignore_ascii_case(table_name))
            .map(|(_, value)| value)
    });

    if let Some((cte_schema, cte_rows)) = cte_result {
        // CTEs don't benefit from Bloom filtering during scan (already materialized)
        // but we still need to apply WHERE predicates if present
        let effective_name = alias.cloned().unwrap_or_else(|| table_name.to_string());
        let cte_table_schema = cte_schema.clone();
        let schema = CombinedSchema::from_table(effective_name.clone(), cte_table_schema);

        // Apply WHERE predicates if any
        if let Some(where_expr) = where_clause {
            let predicate_plan = PredicatePlan::from_where_clause(Some(where_expr), &schema)
                .map_err(ExecutorError::InvalidWhereClause)?;

            let rows = filter_and_clone_rows(
                cte_rows.as_ref(),
                schema.clone(),
                &predicate_plan,
                &effective_name,
                database,
                Some(cte_results),
            )?;
            return Ok(super::FromResult::from_rows(schema, rows));
        }

        // No filtering - use zero-copy shared rows
        return Ok(super::FromResult::from_shared_rows(schema, cte_rows.clone()));
    }

    // Check SELECT privilege on the table
    PrivilegeChecker::check_select(database, table_name)?;

    // Get table
    let table = database
        .get_table(table_name)
        .ok_or_else(|| ExecutorError::TableNotFound(table_name.to_string()))?;

    let effective_name = alias.cloned().unwrap_or_else(|| table_name.to_string());
    let schema = CombinedSchema::from_table(effective_name.clone(), table.schema.clone());

    // Find the Bloom filter column index if we have a Bloom context
    let bloom_col_index = bloom_context.and_then(|ctx| {
        // Look up the column in the table schema
        table.schema.columns.iter().position(|col| col.name.to_lowercase() == ctx.column_name)
    });

    // Build predicate plan for WHERE clause
    let predicate_plan = if let Some(where_expr) = where_clause {
        Some(
            PredicatePlan::from_where_clause(Some(where_expr), &schema)
                .map_err(ExecutorError::InvalidWhereClause)?,
        )
    } else {
        None
    };

    // Check if we have WHERE predicates for this table
    let has_where_predicates = predicate_plan.as_ref().is_some_and(|plan| {
        let effective_name_lower = effective_name.to_lowercase();
        plan.has_table_filters(&effective_name)
            || plan.has_table_filters(&effective_name_lower)
            || plan.has_table_filters(table_name)
            || plan.has_table_filters(&table_name.to_lowercase())
    });

    // Get live rows from table
    let all_rows = table.scan();
    // Phase 1d of #5136: thread the MVCC snapshot through the bloom-prefilter
    // path. With the feature OFF, `is_row_visible` reduces to the same
    // is-not-bitmap-deleted check this path already uses.
    let snapshot = crate::mvcc::read_snapshot(database);

    // If we have a Bloom filter, apply it during scan
    // This is the key optimization: filter rows BEFORE they enter memory
    if let (Some(ctx), Some(col_idx)) = (bloom_context, bloom_col_index) {
        let profile =
            std::env::var("JOIN_PROFILE").is_ok() || std::env::var("BLOOM_PREFILTER_DEBUG").is_ok();

        let original_count = all_rows.len();

        // Filter using both Bloom filter AND WHERE predicates in a single pass
        let filtered_rows: Vec<vibesql_storage::Row> = if has_where_predicates {
            // Apply both Bloom filter and WHERE predicates
            let predicate_plan = predicate_plan.as_ref().unwrap();
            let evaluator =
                CombinedExpressionEvaluator::with_database_and_cte(&schema, database, cte_results);

            // Get predicates and combine them
            let ordered_preds = predicate_plan.get_table_filters_ordered(&effective_name, None);
            let combined_where = super::predicates::combine_predicates_with_and(ordered_preds);

            all_rows
                .iter()
                .enumerate()
                .filter(|(idx, row)| {
                    // Skip deleted rows and (under MVCC) rows invisible to our snapshot.
                    if !table.is_row_visible(*idx, &snapshot) {
                        return false;
                    }

                    // Bloom filter check - quick rejection
                    if let Some(value) = row.values.get(col_idx) {
                        let hash = hash_value(value);
                        if !ctx.filter.might_contain_hash(hash) {
                            return false; // Definitely not a match
                        }
                    }

                    // WHERE predicate check
                    matches!(
                        evaluator.eval(&combined_where, row),
                        Ok(vibesql_types::SqlValue::Boolean(true))
                    )
                })
                .map(|(_, row)| row.clone())
                .collect()
        } else {
            // Apply only Bloom filter
            all_rows
                .iter()
                .enumerate()
                .filter(|(idx, row)| {
                    // Skip deleted rows and (under MVCC) rows invisible to our snapshot.
                    if !table.is_row_visible(*idx, &snapshot) {
                        return false;
                    }

                    // Bloom filter check
                    if let Some(value) = row.values.get(col_idx) {
                        let hash = hash_value(value);
                        ctx.filter.might_contain_hash(hash)
                    } else {
                        // NULL values won't match equijoin anyway
                        false
                    }
                })
                .map(|(_, row)| row.clone())
                .collect()
        };

        let filtered_count = filtered_rows.len();

        if profile {
            let reduction = if original_count > 0 {
                ((original_count - filtered_count) as f64 / original_count as f64) * 100.0
            } else {
                0.0
            };
            eprintln!(
                "[BLOOM_PREFILTER] {} ({} rows): Bloom filter on {} removed {} rows ({:.1}% reduction) -> {} rows",
                table_name,
                original_count,
                ctx.column_name,
                original_count - filtered_count,
                reduction,
                filtered_count
            );
        }

        return Ok(super::FromResult::from_rows(schema, filtered_rows));
    }

    // No Bloom filter - fall back to standard scan with WHERE predicates
    if has_where_predicates {
        let predicate_plan = predicate_plan.as_ref().unwrap();
        let evaluator =
            CombinedExpressionEvaluator::with_database_and_cte(&schema, database, cte_results);

        // Get predicates and combine them
        let ordered_preds = predicate_plan.get_table_filters_ordered(&effective_name, None);
        let combined_where = super::predicates::combine_predicates_with_and(ordered_preds);

        let filtered_rows: Vec<vibesql_storage::Row> = all_rows
            .iter()
            .enumerate()
            .filter(|(idx, row)| {
                // Phase 1d: combined deletion + MVCC visibility check.
                table.is_row_visible(*idx, &snapshot)
                    && matches!(
                        evaluator.eval(&combined_where, row),
                        Ok(vibesql_types::SqlValue::Boolean(true))
                    )
            })
            .map(|(_, row)| row.clone())
            .collect();

        return Ok(super::FromResult::from_rows(schema, filtered_rows));
    }

    // No filters - return all live rows
    // Phase 1d of #5136: also apply MVCC visibility when feature is on.
    let live_rows = table.scan_visible_vec(&snapshot);
    Ok(super::FromResult::from_rows(schema, live_rows))
}
