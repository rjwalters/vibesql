//! Common Table Expression (CTE) handling for SELECT queries

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

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
///
/// The `database` reference is used to statically expand wildcard SELECT items
/// (`SELECT * FROM t`) into the underlying table's column names when deriving
/// each CTE's schema (#5293).
pub fn execute_ctes<F>(
    ctes: &[vibesql_ast::CommonTableExpr],
    database: &vibesql_storage::Database,
    executor: F,
) -> Result<HashMap<String, CteResult>, ExecutorError>
where
    F: Fn(
        &vibesql_ast::SelectStmt,
        &HashMap<String, CteResult>,
    ) -> Result<Vec<vibesql_storage::Row>, ExecutorError>,
{
    // Use the memory-tracking version with a no-op memory check
    execute_ctes_with_memory_check(ctes, database, executor, |_| Ok(()))
}

/// Execute all CTEs with memory tracking
///
/// CTEs are executed in order, allowing later CTEs to reference earlier ones.
/// After each CTE is materialized, the memory_check callback is called with
/// the estimated size of the CTE result to enforce memory limits.
pub(super) fn execute_ctes_with_memory_check<F, M>(
    ctes: &[vibesql_ast::CommonTableExpr],
    database: &vibesql_storage::Database,
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
        // Check if this is a recursive CTE
        // SQLite compatibility: auto-detect recursive CTEs even without RECURSIVE keyword
        // A CTE is recursive if it references itself in a UNION/UNION ALL set operation
        let is_recursive = cte.recursive || is_cte_self_referential(cte);
        let rows = if is_recursive {
            // Recursive CTE: execute base term, then iteratively execute recursive term
            execute_recursive_cte(cte, &cte_results, database, &executor, &memory_check)?
        } else {
            // Non-recursive CTE: execute query directly
            executor(&cte.query, &cte_results)?
        };

        // Track memory for this CTE result before storing
        let estimated_size = super::helpers::estimate_result_size(&rows);
        memory_check(estimated_size)?;

        //  Determine the schema for this CTE
        let schema = derive_cte_schema(cte, &rows, database, &cte_results)?;

        // Store the CTE result wrapped in Arc for efficient sharing
        cte_results.insert(cte.name.clone(), (schema, Arc::new(rows)));
    }

    Ok(cte_results)
}

/// Derive the schema for a CTE from its query and results
///
/// `database` and `prior_ctes` are used to statically expand wildcard SELECT
/// items (`*` / `t.*`) into the column names of the underlying FROM sources
/// (#5293). Without expansion, `WITH cte AS (SELECT * FROM t)` would
/// materialize a single `col{i}` column, silently dropping columns.
pub(super) fn derive_cte_schema(
    cte: &vibesql_ast::CommonTableExpr,
    rows: &[vibesql_storage::Row],
    database: &vibesql_storage::Database,
    prior_ctes: &HashMap<String, CteResult>,
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

            Ok(cte_pseudo_schema(cte.name.clone(), columns))
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

            Ok(cte_pseudo_schema(cte.name.clone(), columns))
        }
    } else {
        // No explicit column names - infer from query SELECT list.
        // Wildcard items are statically expanded into the column names of the
        // underlying FROM sources (#5293). A running value offset tracks the
        // position of each output column in the materialized rows so that
        // type inference stays aligned after expansion (e.g. `SELECT *, expr`
        // from a 2-column table puts `expr` at value index 2, not 1).
        let mut columns: Vec<vibesql_catalog::ColumnSchema> = Vec::new();
        let mut value_idx = 0usize;

        for (i, item) in cte.query.select_list.iter().enumerate() {
            // Determine the output column name(s) for this SELECT item
            let names: Vec<String> = match item {
                vibesql_ast::SelectItem::Wildcard { .. }
                | vibesql_ast::SelectItem::QualifiedWildcard { .. } => {
                    expand_wildcard_names(item, &cte.query, database, prior_ctes)
                        // Unresolvable FROM source (e.g. a view): fall back to
                        // the legacy positional name for this item
                        .unwrap_or_else(|| vec![format!("col{}", i)])
                }
                vibesql_ast::SelectItem::Expression { expr, alias, .. } => {
                    vec![if let Some(a) = alias {
                        a.clone()
                    } else {
                        // Try to extract name from expression
                        match expr {
                            vibesql_ast::Expression::ColumnRef(col_id) => {
                                col_id.column_canonical().to_string()
                            }
                            _ => format!("col{}", i),
                        }
                    }]
                }
            };

            for name in names {
                // Infer data type from first row if available, otherwise use default
                let data_type = rows
                    .first()
                    .and_then(|first_row| first_row.values.get(value_idx))
                    .map(infer_type_from_value)
                    .unwrap_or(vibesql_types::DataType::Varchar { max_length: Some(255) });

                columns.push(vibesql_catalog::ColumnSchema::new(name, data_type, true)); // nullable
                value_idx += 1;
            }
        }

        // Sanity check: if static expansion disagrees with the actual row
        // width, the resolution was wrong (e.g. an exotic FROM source).
        // Fall back to the legacy one-column-per-item naming rather than
        // exposing a schema that misattributes columns.
        if let Some(first_row) = rows.first() {
            if columns.len() != first_row.values.len() {
                return Ok(legacy_cte_schema(cte, rows));
            }
        }

        Ok(cte_pseudo_schema(cte.name.clone(), columns))
    }
}

/// Build a CTE pseudo-schema.
///
/// CTEs, like views and derived tables, have no implicit `rowid`: SQLite errors
/// with `no such column: rowid` when `rowid`/`oid`/`_rowid_` is referenced
/// against a CTE that does not explicitly declare such a column. We mark the
/// schema with `is_view = true` so the shared rowid-resolution paths (added in
/// #5492) reject the pseudo-column. A CTE column genuinely named `rowid` still
/// resolves, because real columns take precedence over the pseudo-column in
/// those paths. See issue #5516.
fn cte_pseudo_schema(
    name: String,
    columns: Vec<vibesql_catalog::ColumnSchema>,
) -> vibesql_catalog::TableSchema {
    let mut schema = vibesql_catalog::TableSchema::new(name, columns);
    schema.set_is_view(true);
    schema
}

/// Legacy schema derivation: one column per SELECT item, wildcards named
/// `col{i}`. Used only as a fallback when static wildcard expansion cannot
/// resolve the FROM sources or disagrees with the materialized row width.
fn legacy_cte_schema(
    cte: &vibesql_ast::CommonTableExpr,
    rows: &[vibesql_storage::Row],
) -> vibesql_catalog::TableSchema {
    let columns = cte
        .query
        .select_list
        .iter()
        .enumerate()
        .map(|(i, item)| {
            let data_type = rows
                .first()
                .and_then(|first_row| first_row.values.get(i))
                .map(infer_type_from_value)
                .unwrap_or(vibesql_types::DataType::Varchar { max_length: Some(255) });

            let col_name = match item {
                vibesql_ast::SelectItem::Wildcard { .. }
                | vibesql_ast::SelectItem::QualifiedWildcard { .. } => format!("col{}", i),
                vibesql_ast::SelectItem::Expression { expr, alias, .. } => {
                    if let Some(a) = alias {
                        a.clone()
                    } else {
                        match expr {
                            vibesql_ast::Expression::ColumnRef(col_id) => {
                                col_id.column_canonical().to_string()
                            }
                            _ => format!("col{}", i),
                        }
                    }
                }
            };

            vibesql_catalog::ColumnSchema::new(col_name, data_type, true) // nullable
        })
        .collect();

    cte_pseudo_schema(cte.name.clone(), columns)
}

/// A column of a FROM-clause source resolved for wildcard expansion.
///
/// `hidden_for_star` marks right-side NATURAL/USING join columns that are
/// deduplicated out of plain-`*` expansion. Qualified wildcards (`t.*`) keep
/// ALL of the source's columns, including hidden ones, matching SQLite
/// (`SELECT b.* FROM a NATURAL JOIN b` returns the join column too).
struct WildcardColumn {
    name: String,
    hidden_for_star: bool,
}

/// A FROM-clause source resolved for wildcard expansion: the effective
/// qualifier (alias if present, else table name) and its column names.
struct WildcardSource {
    qualifier: String,
    columns: Vec<WildcardColumn>,
}

/// Wrap plain column names as star-visible wildcard columns.
fn visible_columns(names: Vec<String>) -> Vec<WildcardColumn> {
    names.into_iter().map(|name| WildcardColumn { name, hidden_for_star: false }).collect()
}

/// Resolve a catalog view's output column names *statically*, without executing
/// its body.
///
/// Prefers the view's stored column list (populated at CREATE VIEW time). When
/// that is absent, derives the names from the view's defining query's SELECT
/// list (recursively expanding wildcards over its own sources). Returns `None`
/// when `name` is not a view or its columns cannot be determined statically.
fn view_column_names(name: &str, database: &vibesql_storage::Database) -> Option<Vec<String>> {
    let view = database.catalog.get_view(name)?;
    if let Some(cols) = &view.columns {
        return Some(cols.clone());
    }
    // Fall back to deriving from the view's defining query. No prior CTEs are in
    // scope for a stored view definition.
    collect_select_list_columns(&view.query, database, &HashMap::new())
}

/// Expand a wildcard SELECT item (`*` or `qualifier.*`) into column names
/// using the statement's FROM clause.
///
/// Returns `None` if any FROM source cannot be resolved statically (e.g. a
/// view); callers fall back to legacy `col{i}` naming.
fn expand_wildcard_names(
    item: &vibesql_ast::SelectItem,
    stmt: &vibesql_ast::SelectStmt,
    database: &vibesql_storage::Database,
    prior_ctes: &HashMap<String, CteResult>,
) -> Option<Vec<String>> {
    match item {
        vibesql_ast::SelectItem::Wildcard { alias } => {
            // SQL:1999 E051-07 derived column list: SELECT * AS (a, b, ...)
            if let Some(alias_names) = alias {
                return Some(alias_names.clone());
            }
            let sources = collect_from_sources(stmt.from.as_ref()?, database, prior_ctes)?;
            Some(
                sources
                    .into_iter()
                    .flat_map(|s| s.columns)
                    .filter(|c| !c.hidden_for_star)
                    .map(|c| c.name)
                    .collect(),
            )
        }
        vibesql_ast::SelectItem::QualifiedWildcard { qualifier, alias } => {
            if let Some(alias_names) = alias {
                return Some(alias_names.clone());
            }
            let sources = collect_from_sources(stmt.from.as_ref()?, database, prior_ctes)?;
            sources
                .into_iter()
                .find(|s| s.qualifier.eq_ignore_ascii_case(qualifier))
                // Qualified wildcards keep ALL of the source's columns,
                // including NATURAL/USING join columns hidden from plain `*`
                .map(|s| s.columns.into_iter().map(|c| c.name).collect())
        }
        vibesql_ast::SelectItem::Expression { .. } => None,
    }
}

/// Resolve the sources of a FROM clause to their column names for wildcard
/// expansion. Mirrors the traversal in
/// `evaluator::combined::subqueries::schema_utils::count_columns_in_from_clause`
/// but collects names instead of counts.
///
/// Returns `None` when a source cannot be resolved statically; callers fall
/// back to legacy naming rather than erroring.
fn collect_from_sources(
    from: &vibesql_ast::FromClause,
    database: &vibesql_storage::Database,
    prior_ctes: &HashMap<String, CteResult>,
) -> Option<Vec<WildcardSource>> {
    match from {
        vibesql_ast::FromClause::Table { name, alias, column_aliases, .. } => {
            // Check prior CTEs first (case-insensitive, matching the
            // resolution convention used elsewhere), then database tables
            let base_columns: Vec<String> = if let Some((schema, _)) =
                prior_ctes.get(name).or_else(|| {
                    prior_ctes.iter().find(|(k, _)| k.eq_ignore_ascii_case(name)).map(|(_, v)| v)
                }) {
                schema.columns.iter().map(|c| c.name.clone()).collect()
            } else if let Some(table) = database.get_table(name) {
                table.schema.columns.iter().map(|c| c.name.clone()).collect()
            } else if let Some(cols) = view_column_names(name, database) {
                // A catalog view whose output columns are known statically.
                // Resolving views from their stored column list (rather than
                // executing their bodies) is essential to keep deeply nested
                // views cheap: it lets CREATE VIEW derive columns without the
                // exponential re-materialization of doubling view nests
                // (#5394, view3.test).
                cols
            } else {
                // Unknown source we cannot resolve statically.
                return None;
            };

            // SQL:1999 E051-09: FROM t AS a(x, y) renames the columns
            let columns = match column_aliases {
                Some(aliases) if aliases.len() == base_columns.len() => aliases.clone(),
                Some(_) => return None, // mismatched rename list - bail out
                None => base_columns,
            };

            let qualifier = alias.clone().unwrap_or_else(|| name.clone());
            Some(vec![WildcardSource { qualifier, columns: visible_columns(columns) }])
        }
        vibesql_ast::FromClause::Join { left, right, natural, using_columns, alias, .. } => {
            // Non-goal: aliased parenthesized NATURAL/USING joins
            // (`(a JOIN b USING(k)) AS j`) hoist the USING columns to the
            // front under SQLite semantics (#4916). Static expansion does not
            // model that reordering, so fall back to legacy naming.
            if alias.is_some() && (*natural || using_columns.is_some()) {
                return None;
            }

            let mut sources = collect_from_sources(left, database, prior_ctes)?;
            let mut right_sources = collect_from_sources(right, database, prior_ctes)?;

            // NATURAL/USING joins deduplicate the shared columns out of
            // plain-`*` expansion: ALL left columns stay in declaration order
            // (join columns are NOT hoisted to the front), then the right
            // columns minus the shared ones. This mirrors the runtime
            // expansion in `select/projection.rs` (issue #4916 ordering).
            if *natural || using_columns.is_some() {
                let shared: Vec<String> = if let Some(using) = using_columns {
                    using.clone()
                } else {
                    // NATURAL: case-insensitive intersection of the left
                    // operand's star-visible names with the right operand's
                    // star-visible names. Using star-visible (already
                    // deduplicated) names makes chained NATURAL joins compute
                    // each join's shared set against the accumulated output.
                    let left_visible: Vec<&str> = sources
                        .iter()
                        .flat_map(|s| s.columns.iter())
                        .filter(|c| !c.hidden_for_star)
                        .map(|c| c.name.as_str())
                        .collect();
                    right_sources
                        .iter()
                        .flat_map(|s| s.columns.iter())
                        .filter(|c| !c.hidden_for_star)
                        .filter(|c| left_visible.iter().any(|l| l.eq_ignore_ascii_case(&c.name)))
                        .map(|c| c.name.clone())
                        .collect()
                };

                // Hide the shared columns on the right side only; they remain
                // resolvable through qualified wildcards (`b.*`).
                for source in &mut right_sources {
                    for col in &mut source.columns {
                        if shared.iter().any(|s| s.eq_ignore_ascii_case(&col.name)) {
                            col.hidden_for_star = true;
                        }
                    }
                }
            }

            sources.extend(right_sources);
            Some(sources)
        }
        vibesql_ast::FromClause::Subquery { query, alias, column_aliases } => {
            let columns = if let Some(aliases) = column_aliases {
                aliases.clone()
            } else {
                collect_select_list_columns(query, database, prior_ctes)?
            };
            Some(vec![WildcardSource {
                qualifier: alias.clone(),
                columns: visible_columns(columns),
            }])
        }
        vibesql_ast::FromClause::Values { rows, alias, column_aliases } => {
            let columns = if let Some(aliases) = column_aliases {
                aliases.clone()
            } else {
                let first_row = rows.first()?;
                (0..first_row.len()).map(|i| format!("col{}", i)).collect()
            };
            Some(vec![WildcardSource {
                qualifier: alias.clone(),
                columns: visible_columns(columns),
            }])
        }
    }
}

/// Statically compute a SELECT statement's output column names, expanding
/// wildcards (resolving tables, CTEs, and views from catalog metadata) without
/// executing the query.
///
/// For compound queries (UNION/INTERSECT/EXCEPT) the column names come from the
/// leftmost SELECT, matching SQL/SQLite semantics.
///
/// Used by CREATE VIEW to derive a view's columns cheaply, avoiding the
/// exponential re-materialization of deeply nested views (#5394, view3.test).
/// Returns `None` when names cannot be determined statically; callers then fall
/// back to executing the body.
pub(crate) fn try_static_select_columns(
    stmt: &vibesql_ast::SelectStmt,
    database: &vibesql_storage::Database,
) -> Option<Vec<String>> {
    collect_select_list_columns(stmt, database, &HashMap::new())
}

/// Compute the output column names of a SELECT statement, expanding any
/// wildcard items. Used to resolve subqueries appearing in a FROM clause.
///
/// Returns `None` if names cannot be determined statically.
fn collect_select_list_columns(
    stmt: &vibesql_ast::SelectStmt,
    database: &vibesql_storage::Database,
    prior_ctes: &HashMap<String, CteResult>,
) -> Option<Vec<String>> {
    // VALUES statement: names come from the first row's width
    if let Some(values_rows) = &stmt.values {
        let first_row = values_rows.first()?;
        return Some((0..first_row.len()).map(|i| format!("col{}", i)).collect());
    }

    let mut names = Vec::new();
    for (i, item) in stmt.select_list.iter().enumerate() {
        match item {
            vibesql_ast::SelectItem::Wildcard { .. }
            | vibesql_ast::SelectItem::QualifiedWildcard { .. } => {
                names.extend(expand_wildcard_names(item, stmt, database, prior_ctes)?);
            }
            vibesql_ast::SelectItem::Expression { expr, alias, .. } => {
                names.push(if let Some(a) = alias {
                    a.clone()
                } else {
                    match expr {
                        vibesql_ast::Expression::ColumnRef(col_id) => {
                            col_id.column_canonical().to_string()
                        }
                        _ => format!("col{}", i),
                    }
                });
            }
        }
    }
    Some(names)
}

/// Execute a recursive CTE using iterative evaluation
///
/// Recursive CTEs in SQL:1999/SQLite are defined with UNION or UNION ALL:
/// ```sql
/// WITH RECURSIVE cte AS (
///   base_query          -- Executed once to get initial rows
///   UNION [ALL]
///   recursive_query     -- References 'cte', executed iteratively
/// )
/// ```
///
/// Algorithm:
/// 1. Split query into base and recursive terms (before/after UNION [ALL])
/// 2. Execute base term to get initial working table
/// 3. Repeat until no new rows or max depth reached:
///    - Make working table available as CTE
///    - Execute recursive term
///    - Add new rows to result (with deduplication for UNION)
///    - Update working table to new rows
fn execute_recursive_cte<F, M>(
    cte: &vibesql_ast::CommonTableExpr,
    cte_results: &HashMap<String, CteResult>,
    database: &vibesql_storage::Database,
    executor: &F,
    memory_check: &M,
) -> Result<Vec<vibesql_storage::Row>, ExecutorError>
where
    F: Fn(
        &vibesql_ast::SelectStmt,
        &HashMap<String, CteResult>,
    ) -> Result<Vec<vibesql_storage::Row>, ExecutorError>,
    M: Fn(usize) -> Result<(), ExecutorError>,
{
    use crate::limits::MAX_RECURSIVE_CTE_ITERATIONS;

    // Validate that recursive CTE uses UNION ALL
    let set_op = cte.query.set_operation.as_ref().ok_or_else(|| {
        ExecutorError::UnsupportedFeature(format!(
            "Recursive CTE '{}' must use UNION ALL",
            cte.name
        ))
    })?;

    if set_op.op != vibesql_ast::SetOperator::Union {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "Recursive CTE '{}' must use UNION or UNION ALL (not INTERSECT or EXCEPT)",
            cte.name
        )));
    }

    // Extract base and recursive terms
    // Base term: the main SELECT (before UNION [ALL])
    // Recursive term: the right side of UNION [ALL]

    // Create base-only query without the UNION ALL set operation
    // This prevents the base term from trying to reference the CTE before it exists
    let base_query = vibesql_ast::SelectStmt {
        with_clause: cte.query.with_clause.clone(),
        distinct: cte.query.distinct,
        select_list: cte.query.select_list.clone(),
        into_table: cte.query.into_table.clone(),
        into_variables: cte.query.into_variables.clone(),
        from: cte.query.from.clone(),
        where_clause: cte.query.where_clause.clone(),
        group_by: cte.query.group_by.clone(),
        having: cte.query.having.clone(),
        window_definitions: cte.query.window_definitions.clone(),
        order_by: cte.query.order_by.clone(),
        limit: cte.query.limit.clone(),
        offset: cte.query.offset.clone(),
        set_operation: None, // Remove UNION ALL for base term execution
        values: cte.query.values.clone(),
    };
    let recursive_query = &set_op.right;

    // SQLite compatibility: window functions are not allowed in the recursive
    // part of a recursive CTE (window1.test 15.0). SQLite reports the exact
    // error "cannot use window functions in recursive queries".
    if crate::select::window::has_window_functions(&recursive_query.select_list) {
        return Err(ExecutorError::SqliteCompatError(
            "cannot use window functions in recursive queries".to_string(),
        ));
    }

    // Try static validation first (works for explicit column lists and VALUES)
    // This provides better SQLite compatibility by catching errors at prepare time
    // rather than waiting until runtime
    // Note: For VALUES statements, column count comes from the VALUES rows, not select_list
    if let (Some(base_count), Some(recursive_count)) =
        (count_stmt_columns(&base_query), count_stmt_columns(recursive_query))
    {
        if base_count != recursive_count {
            return Err(ExecutorError::UnsupportedFeature(
                "SELECTs to the left and right of UNION ALL do not have the same number of result columns".to_string()
            ));
        }
    }
    // Fall back to runtime validation for wildcards (existing code below at line 279-289)

    // Step 1: Execute base term to get initial rows
    let mut all_rows = executor(&base_query, cte_results)?;
    let mut working_table = all_rows.clone();

    // Derive schema from base term result
    // Wildcards in the base term are expanded against database tables and
    // prior CTEs (#5293)
    let schema = derive_cte_schema(cte, &all_rows, database, cte_results)?;

    // Track seen rows for UNION (deduplication)
    // For UNION ALL, we skip tracking to preserve all rows
    let mut seen_rows: Option<HashSet<vibesql_storage::RowValues>> = if !set_op.all {
        let mut seen = HashSet::with_capacity(all_rows.len());
        for row in &all_rows {
            seen.insert(row.values.clone());
        }
        Some(seen)
    } else {
        None
    };

    // Step 2: Iterative evaluation
    let mut depth = 0;
    while !working_table.is_empty() && depth < MAX_RECURSIVE_CTE_ITERATIONS {
        depth += 1;

        // Make working table available as this CTE for recursive reference
        let mut recursive_cte_results = cte_results.clone();
        recursive_cte_results
            .insert(cte.name.clone(), (schema.clone(), Arc::new(working_table.clone())));

        // Execute recursive term with working table as CTE
        let new_rows = executor(recursive_query, &recursive_cte_results)?;

        // If no new rows, we're done
        if new_rows.is_empty() {
            break;
        }

        // Validate that recursive term returns same number of columns as base term
        // This check is done on first iteration to catch schema mismatches early
        if depth == 1 && !new_rows.is_empty() && !all_rows.is_empty() {
            let base_col_count = all_rows[0].values.len();
            let recursive_col_count = new_rows[0].values.len();
            if base_col_count != recursive_col_count {
                return Err(ExecutorError::UnsupportedFeature(
                    "SELECTs to the left and right of UNION ALL do not have the same number of result columns".to_string()
                ));
            }
        }

        // Check memory before adding new rows
        let estimated_size = super::helpers::estimate_result_size(&new_rows);
        memory_check(estimated_size)?;

        // Filter out duplicates for UNION (keep all for UNION ALL)
        let rows_to_add: Vec<vibesql_storage::Row> = if let Some(ref mut seen) = seen_rows {
            // UNION: only add rows we haven't seen before
            new_rows.into_iter().filter(|row| seen.insert(row.values.clone())).collect()
        } else {
            // UNION ALL: keep all rows
            new_rows
        };

        // If no new unique rows (for UNION), we're done
        if rows_to_add.is_empty() {
            break;
        }

        // Add new rows to result
        all_rows.extend(rows_to_add.clone());

        // Update working table to be the new rows for next iteration
        working_table = rows_to_add;
    }

    // Check if we hit max recursion depth
    if depth >= MAX_RECURSIVE_CTE_ITERATIONS {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "Recursive CTE '{}' exceeded maximum iteration limit of {}",
            cte.name, MAX_RECURSIVE_CTE_ITERATIONS
        )));
    }

    Ok(all_rows)
}

/// Count columns if select list has only explicit expressions (no wildcards)
///
/// Returns Some(count) if all select items are explicit expressions.
/// Returns None if any wildcards are present (requires schema info to count).
fn count_explicit_columns(select_list: &[vibesql_ast::SelectItem]) -> Option<usize> {
    let mut count = 0;
    for item in select_list {
        match item {
            vibesql_ast::SelectItem::Expression { .. } => count += 1,
            // Can't count wildcards statically - need schema info
            vibesql_ast::SelectItem::Wildcard { .. }
            | vibesql_ast::SelectItem::QualifiedWildcard { .. } => {
                return None;
            }
        }
    }
    Some(count)
}

/// Count columns in a SELECT statement, considering both select_list and VALUES.
/// For VALUES statements, the column count comes from the first row of values.
/// For SELECT statements, the column count comes from the select_list.
/// Returns None if any wildcards are present (requires schema info to count).
fn count_stmt_columns(stmt: &vibesql_ast::SelectStmt) -> Option<usize> {
    // If this is a VALUES statement, count columns from the first VALUES row
    if let Some(values_rows) = &stmt.values {
        return values_rows.first().map(|row| row.len());
    }

    // Otherwise, count columns from the select_list
    count_explicit_columns(&stmt.select_list)
}

/// Check if a CTE is self-referential (references itself in UNION/UNION ALL)
///
/// SQLite allows recursive CTEs without the RECURSIVE keyword if the CTE
/// references itself in a set operation. This function detects such cases
/// by checking if the right side of a UNION/UNION ALL references the CTE name.
fn is_cte_self_referential(cte: &vibesql_ast::CommonTableExpr) -> bool {
    // Check if the CTE has a UNION/UNION ALL set operation
    let set_op = match &cte.query.set_operation {
        Some(op) if op.op == vibesql_ast::SetOperator::Union => op,
        _ => return false,
    };

    // Check if the recursive term references this CTE
    stmt_references_table(&set_op.right, &cte.name)
}

/// Check if a SELECT statement references a table name
fn stmt_references_table(stmt: &vibesql_ast::SelectStmt, table_name: &str) -> bool {
    // Check FROM clause
    if let Some(from) = &stmt.from {
        if from_clause_references_table(from, table_name) {
            return true;
        }
    }

    // Check WHERE clause for subqueries
    if let Some(where_clause) = &stmt.where_clause {
        if expr_references_table(where_clause, table_name) {
            return true;
        }
    }

    // Check SELECT list for subqueries
    for item in &stmt.select_list {
        if let vibesql_ast::SelectItem::Expression { expr, .. } = item {
            if expr_references_table(expr, table_name) {
                return true;
            }
        }
    }

    false
}

/// Check if a FROM clause references a table name
fn from_clause_references_table(from: &vibesql_ast::FromClause, table_name: &str) -> bool {
    match from {
        vibesql_ast::FromClause::Table { name, .. } => name.eq_ignore_ascii_case(table_name),
        vibesql_ast::FromClause::Subquery { query, .. } => stmt_references_table(query, table_name),
        vibesql_ast::FromClause::Join { left, right, condition, .. } => {
            from_clause_references_table(left, table_name)
                || from_clause_references_table(right, table_name)
                || condition.as_ref().map_or(false, |c| expr_references_table(c, table_name))
        }
        vibesql_ast::FromClause::Values { .. } => false,
    }
}

/// Check if an expression references a table name (in subqueries)
fn expr_references_table(expr: &vibesql_ast::Expression, table_name: &str) -> bool {
    match expr {
        vibesql_ast::Expression::ScalarSubquery(subquery) => {
            stmt_references_table(subquery, table_name)
        }
        vibesql_ast::Expression::In { subquery, .. } => stmt_references_table(subquery, table_name),
        vibesql_ast::Expression::Exists { subquery, .. } => {
            stmt_references_table(subquery, table_name)
        }
        vibesql_ast::Expression::BinaryOp { left, right, .. } => {
            expr_references_table(left, table_name) || expr_references_table(right, table_name)
        }
        vibesql_ast::Expression::UnaryOp { expr, .. } => expr_references_table(expr, table_name),
        vibesql_ast::Expression::Function { args, .. } => {
            args.iter().any(|arg| expr_references_table(arg, table_name))
        }
        vibesql_ast::Expression::AggregateFunction { args, filter, .. } => {
            args.iter().any(|arg| expr_references_table(arg, table_name))
                || filter.as_ref().map_or(false, |f| expr_references_table(f, table_name))
        }
        vibesql_ast::Expression::Case { operand, when_clauses, else_result, .. } => {
            operand.as_ref().map_or(false, |o| expr_references_table(o, table_name))
                || when_clauses.iter().any(|when| {
                    when.conditions.iter().any(|c| expr_references_table(c, table_name))
                        || expr_references_table(&when.result, table_name)
                })
                || else_result.as_ref().map_or(false, |e| expr_references_table(e, table_name))
        }
        vibesql_ast::Expression::Between { expr, low, high, .. } => {
            expr_references_table(expr, table_name)
                || expr_references_table(low, table_name)
                || expr_references_table(high, table_name)
        }
        vibesql_ast::Expression::InList { expr, values, .. } => {
            expr_references_table(expr, table_name)
                || values.iter().any(|e| expr_references_table(e, table_name))
        }
        vibesql_ast::Expression::Cast { expr, .. }
        | vibesql_ast::Expression::Collate { expr, .. } => expr_references_table(expr, table_name),
        vibesql_ast::Expression::Conjunction(exprs)
        | vibesql_ast::Expression::Disjunction(exprs) => {
            exprs.iter().any(|e| expr_references_table(e, table_name))
        }
        vibesql_ast::Expression::QuantifiedComparison { expr, subquery, .. } => {
            expr_references_table(expr, table_name) || stmt_references_table(subquery, table_name)
        }
        _ => false,
    }
}

/// Infer data type from a SQL value
pub(super) fn infer_type_from_value(value: &vibesql_types::SqlValue) -> vibesql_types::DataType {
    match value {
        vibesql_types::SqlValue::Null => vibesql_types::DataType::Varchar { max_length: Some(255) }, /* default */
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
        vibesql_types::SqlValue::Blob(_) => vibesql_types::DataType::BinaryLargeObject,
    }
}
