//! Unused table elimination optimizer pass
//!
//! Detects and eliminates tables from FROM clauses that:
//! 1. Have no columns in the SELECT list
//! 2. Have no equijoin predicates with other tables
//! 3. Only have self-filters in WHERE
//!
//! These tables create expensive cross joins that multiply result rows
//! without providing useful data. They are converted to EXISTS checks.
//!
//! This pass runs BEFORE semi-join transformation to avoid complex
//! interactions with derived tables from EXISTS/IN transformations.

mod join_analysis;
mod predicate;
mod prefix;
mod select_analysis;
#[cfg(test)]
mod tests;
mod transform;
mod types;

use std::collections::HashSet;

use join_analysis::{extract_equijoin_tables, extract_equijoin_tables_from_joins};
use predicate::extract_local_predicates;
use prefix::build_column_prefix_map;
use select_analysis::{
    collect_unqualified_columns, extract_tables_from_select, has_global_aggregates,
};
use transform::{
    add_exists_to_where, build_exists_checks, flatten_from_clause, rebuild_from_clause,
    remove_eliminated_predicates,
};
use types::EliminatedTable;
use vibesql_ast::{Expression, SelectItem, SelectStmt};

/// Apply table elimination optimization to a SELECT statement
///
/// Returns a new statement with eliminable tables removed from FROM
/// and converted to EXISTS checks in WHERE
///
/// Can be disabled with TABLE_ELIM_DISABLED environment variable
pub fn eliminate_unused_tables(stmt: &SelectStmt) -> SelectStmt {
    // Check if optimization is disabled
    if std::env::var("TABLE_ELIM_DISABLED").is_ok() {
        return stmt.clone();
    }

    let verbose = std::env::var("TABLE_ELIM_VERBOSE").is_ok();

    // Must have FROM clause
    let from = match &stmt.from {
        Some(f) => f,
        None => return stmt.clone(),
    };

    // Extract tables from FROM clause
    let mut tables = Vec::new();
    flatten_from_clause(from, &mut tables);

    if verbose {
        eprintln!(
            "[TABLE_ELIM_OPT] Analyzing {} tables: {:?}",
            tables.len(),
            tables.iter().map(|t| &t.name).collect::<Vec<_>>()
        );
    }

    // Need at least 2 tables (keep at least 1, eliminate at least 1)
    if tables.len() < 2 {
        return stmt.clone();
    }

    // Don't apply to subqueries (SELECT 1 FROM ...) - these are intentional
    let is_select_literal = stmt.select_list.len() == 1
        && matches!(
            &stmt.select_list[0],
            SelectItem::Expression { expr: Expression::Literal(_), .. }
        );
    if is_select_literal {
        if verbose {
            eprintln!("[TABLE_ELIM_OPT] Skipping: SELECT literal subquery");
        }
        return stmt.clone();
    }

    // Build table name set
    let table_names: HashSet<String> =
        tables.iter().map(|t| t.alias.as_ref().unwrap_or(&t.name).to_lowercase()).collect();

    // Don't apply when query has global aggregates (like COUNT(*)) without GROUP BY.
    // Such aggregates operate over the entire Cartesian product, so eliminating tables
    // would change the result (e.g., COUNT(*) on cross join should count all product rows).
    if stmt.group_by.is_none() && has_global_aggregates(&stmt.select_list, &table_names) {
        if verbose {
            eprintln!("[TABLE_ELIM_OPT] Skipping: query has global aggregates without GROUP BY");
        }
        return stmt.clone();
    }

    // Collect unqualified column names from SELECT for later checking
    let unqualified_columns = collect_unqualified_columns(&stmt.select_list);

    if verbose && !unqualified_columns.is_empty() {
        eprintln!(
            "[TABLE_ELIM_OPT] Found {} unqualified columns in SELECT: {:?}",
            unqualified_columns.len(),
            unqualified_columns.iter().take(5).collect::<Vec<_>>()
        );
    }

    // Build column prefix mapping from qualified refs in the entire query
    // E.g., if we see `date_dim.d_year`, we know `date_dim` columns start with `d_`
    let table_column_prefixes = build_column_prefix_map(stmt, &table_names);

    if verbose && !table_column_prefixes.is_empty() {
        eprintln!("[TABLE_ELIM_OPT] Column prefixes: {:?}", table_column_prefixes);
    }

    // Find tables referenced in SELECT list
    let select_tables = extract_tables_from_select(&stmt.select_list, &table_names);

    if verbose {
        eprintln!("[TABLE_ELIM_OPT] Tables in SELECT: {:?}", select_tables);
    }

    // Safety check: If there are unqualified columns that don't match ANY known table prefix,
    // we can't reliably determine which table they belong to. Skip optimization to be safe.
    // This handles CTEs and other cases where derived prefixes don't match actual column names.
    if !unqualified_columns.is_empty() {
        let known_prefixes: Vec<_> = table_column_prefixes.values().collect();
        let has_unknown_columns = unqualified_columns.iter().any(|col| {
            let col_lower = col.to_lowercase();
            !known_prefixes.iter().any(|prefix| col_lower.starts_with(*prefix))
        });
        if has_unknown_columns {
            if verbose {
                eprintln!(
                    "[TABLE_ELIM_OPT] Skipping: unqualified columns don't match any known prefix"
                );
            }
            return stmt.clone();
        }
    }

    // Find tables in equijoins (WHERE clause predicates AND JOIN ON conditions)
    // Uses prefix matching to detect joins with unqualified column refs
    let mut equijoin_tables = if let Some(where_expr) = &stmt.where_clause {
        extract_equijoin_tables(where_expr, &table_names, &table_column_prefixes)
    } else {
        HashSet::new()
    };

    // Also extract tables from JOIN ON conditions in the FROM clause (#3572)
    // This ensures we don't eliminate tables that are part of explicit JOIN conditions
    if let Some(from_clause) = &stmt.from {
        let on_condition_tables =
            extract_equijoin_tables_from_joins(from_clause, &table_names, &table_column_prefixes);
        equijoin_tables.extend(on_condition_tables);
    }

    if verbose {
        eprintln!("[TABLE_ELIM_OPT] Tables in equijoins: {:?}", equijoin_tables);
    }

    // Find local predicates per table (using prefix matching for unqualified columns)
    let local_predicates = if let Some(where_expr) = &stmt.where_clause {
        extract_local_predicates(where_expr, &table_names, &table_column_prefixes)
    } else {
        std::collections::HashMap::new()
    };

    // Classify tables
    let mut eliminated = Vec::new();
    let mut kept_tables = Vec::new();

    for table in tables {
        let table_key = table.alias.as_ref().unwrap_or(&table.name).to_lowercase();

        // Check if table is referenced by qualified columns in SELECT
        let in_select_qualified = select_tables.contains(&table_key);

        // Check if table might be referenced by unqualified columns in SELECT
        // Using prefix matching: if table has known prefix (e.g., "d_" for date_dim)
        // check if any unqualified column matches
        let in_select_unqualified = if !unqualified_columns.is_empty() {
            if let Some(prefix) = table_column_prefixes.get(&table_key) {
                // We know this table's column prefix - check for matches
                let matches_prefix =
                    unqualified_columns.iter().any(|col| col.to_lowercase().starts_with(prefix));
                if verbose && matches_prefix {
                    eprintln!(
                        "[TABLE_ELIM_OPT] Table '{}' might be referenced by unqualified cols (prefix '{}')",
                        table_key, prefix
                    );
                }
                matches_prefix
            } else {
                // No prefix known for this table - conservatively assume it might be used
                // unless it's a common pattern (table has qualified refs but no matching
                // unqualified)
                if verbose {
                    eprintln!(
                        "[TABLE_ELIM_OPT] Table '{}' has no known prefix, conservatively keeping",
                        table_key
                    );
                }
                true
            }
        } else {
            false
        };

        let in_select = in_select_qualified || in_select_unqualified;
        let in_equijoin = equijoin_tables.contains(&table_key);

        if verbose {
            eprintln!(
                "[TABLE_ELIM_OPT] Table '{}': in_select={} (qualified={}, unqualified={}), in_equijoin={}",
                table_key, in_select, in_select_qualified, in_select_unqualified, in_equijoin
            );
        }

        // Table can be eliminated if:
        // 1. Not in SELECT list
        // 2. Not in any equijoin condition
        // 3. HAS a local predicate/filter (otherwise it's an intentional cross join that multiplies
        //    rows, and we must preserve that row count)
        let filter = local_predicates.get(&table_key).cloned();
        if !in_select && !in_equijoin && filter.is_some() {
            if verbose {
                eprintln!(
                    "[TABLE_ELIM_OPT] ✓ Eliminating table '{}' with filter: {:?}",
                    table_key, filter
                );
            }
            eliminated.push(EliminatedTable {
                name: table.name.clone(),
                alias: table.alias.clone(),
                filter,
            });
        } else {
            if verbose && !in_select && !in_equijoin && filter.is_none() {
                eprintln!(
                    "[TABLE_ELIM_OPT] ✗ Keeping table '{}': no filter (cross join multiplies rows)",
                    table_key
                );
            }
            kept_tables.push(table);
        }
    }

    // If no tables eliminated, return unchanged
    if eliminated.is_empty() {
        return stmt.clone();
    }

    // If ALL tables would be eliminated, return unchanged.
    // Eliminating all tables would leave no FROM clause, causing incorrect
    // semantics for WHERE clauses that evaluate to FALSE (like NULL IS NOT NULL).
    if kept_tables.is_empty() {
        if verbose {
            eprintln!(
                "[TABLE_ELIM_OPT] Skipping: would eliminate all tables, leaving no FROM clause"
            );
        }
        return stmt.clone();
    }

    // Build new FROM clause without eliminated tables
    let new_from = rebuild_from_clause(&kept_tables);

    // Build EXISTS checks for eliminated tables
    let exists_checks = build_exists_checks(&eliminated);

    // Build eliminated table names set
    let eliminated_names: HashSet<String> =
        eliminated.iter().map(|t| t.alias.as_ref().unwrap_or(&t.name).to_lowercase()).collect();

    // Build prefixes for eliminated tables
    let eliminated_prefixes: HashSet<String> =
        eliminated_names.iter().filter_map(|t| table_column_prefixes.get(t).cloned()).collect();

    // Remove eliminated table predicates from WHERE
    let filtered_where = if let Some(where_expr) = &stmt.where_clause {
        remove_eliminated_predicates(where_expr, &eliminated_names, &eliminated_prefixes)
    } else {
        None
    };

    // Add EXISTS checks to WHERE
    let new_where = add_exists_to_where(filtered_where.as_ref(), exists_checks);

    if verbose {
        eprintln!("[TABLE_ELIM_OPT] Applied elimination: {} tables removed", eliminated.len());
    }

    // Return modified statement
    SelectStmt {
        with_clause: stmt.with_clause.clone(),
        distinct: stmt.distinct,
        select_list: stmt.select_list.clone(),
        into_table: stmt.into_table.clone(),
        into_variables: stmt.into_variables.clone(),
        from: new_from,
        where_clause: new_where,
        group_by: stmt.group_by.clone(),
        having: stmt.having.clone(),
        window_definitions: stmt.window_definitions.clone(),
        order_by: stmt.order_by.clone(),
        limit: stmt.limit.clone(),
        offset: stmt.offset.clone(),
        set_operation: stmt.set_operation.clone(),
        values: stmt.values.clone(),
        hints: stmt.hints.clone(),
    }
}
