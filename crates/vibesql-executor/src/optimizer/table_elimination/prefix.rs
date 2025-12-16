//! Column prefix inference for table elimination
//!
//! Functions for building a map from table names to column prefixes based on
//! qualified column references. This allows us to attribute unqualified columns
//! to tables using naming conventions (e.g., TPC-DS style `d_year` for `date_dim`).

use std::collections::{HashMap, HashSet};

use vibesql_ast::{Expression, FromClause, SelectItem, SelectStmt};

/// Build a map from table name to column prefix based on qualified column references
///
/// For example, if we see `date_dim.d_year`, we extract prefix `d_` for table `date_dim`.
/// This allows us to determine if unqualified columns could belong to a table.
///
/// For tables with no qualified refs, we derive a potential prefix from the table name
/// (e.g., `date_dim` → `d_`, `customer` → `c_`) for TPC-DS style naming conventions.
pub(super) fn build_column_prefix_map(
    stmt: &SelectStmt,
    table_names: &HashSet<String>,
) -> HashMap<String, String> {
    let mut table_columns: HashMap<String, Vec<String>> = HashMap::new();

    // Collect qualified columns from entire statement
    collect_qualified_columns_from_select(&stmt.select_list, &mut table_columns);
    if let Some(from) = &stmt.from {
        collect_qualified_columns_from_from(from, &mut table_columns);
    }
    if let Some(where_expr) = &stmt.where_clause {
        collect_qualified_columns_from_expr(where_expr, &mut table_columns);
    }

    // Derive prefix for each table from its columns
    let mut prefixes = HashMap::new();
    for (table, columns) in &table_columns {
        let table_lower = table.to_lowercase();
        if !table_names.contains(&table_lower) {
            continue; // Skip tables not in FROM clause
        }
        if let Some(prefix) = find_common_prefix(columns) {
            prefixes.insert(table_lower, prefix);
        }
    }

    // For tables with no qualified refs, try to derive prefix from table name
    // This handles cases like `date_dim` with unqualified `d_year` filter
    for table_name in table_names {
        if !prefixes.contains_key(table_name) {
            if let Some(prefix) = derive_prefix_from_table_name(table_name) {
                prefixes.insert(table_name.clone(), prefix);
            }
        }
    }

    prefixes
}

/// Derive a column prefix from a table name using naming conventions
///
/// For TPC-DS style naming:
/// - Short names (2-3 chars, likely aliases): use as-is + underscore (ss → ss_, ws → ws_)
/// - Dimension tables (`_dim` suffix): first letter (date_dim → d_, time_dim → t_)
/// - Multi-word tables: acronym (customer_address → ca_, store_sales → ss_)
/// - Single word tables: first letter (customer → c_, item → i_)
pub(super) fn derive_prefix_from_table_name(table_name: &str) -> Option<String> {
    let name = table_name.to_lowercase();

    // Short names (2-3 chars) are likely aliases for tables
    // Use the full alias + underscore (ss → ss_, ws → ws_, cs → cs_)
    // This handles common TPC-DS alias patterns
    if name.len() <= 3 && !name.contains('_') {
        return Some(format!("{}_", name));
    }

    // Handle dimension tables: `*_dim` uses first letter only
    if name.ends_with("_dim") {
        let first_char = name.chars().next()?;
        return Some(format!("{}_", first_char));
    }

    // Handle multi-word table names (e.g., customer_address → ca_, store_sales → ss_)
    if let Some(underscore_pos) = name.find('_') {
        let first_word = &name[..underscore_pos];
        let rest = &name[underscore_pos + 1..];

        // Acronym style: first letter of each word
        if !rest.is_empty() {
            let mut prefix = String::new();
            prefix.push(first_word.chars().next()?);
            // Take first letter of second word
            if let Some(second_first) = rest.chars().next() {
                prefix.push(second_first);
            }
            prefix.push('_');
            return Some(prefix);
        }
    }

    // Single word: first letter + underscore (customer → c_, item → i_)
    let first_char = name.chars().next()?;
    Some(format!("{}_", first_char))
}

fn collect_qualified_columns_from_select(
    select_list: &[SelectItem],
    table_columns: &mut HashMap<String, Vec<String>>,
) {
    for item in select_list {
        if let SelectItem::Expression { expr, .. } = item {
            collect_qualified_columns_from_expr(expr, table_columns);
        }
    }
}

fn collect_qualified_columns_from_from(
    from: &FromClause,
    table_columns: &mut HashMap<String, Vec<String>>,
) {
    match from {
        FromClause::Table { .. } => {}
        FromClause::Subquery { .. } => {}
        FromClause::Values { .. } => {}
        FromClause::Join { left, right, condition, .. } => {
            collect_qualified_columns_from_from(left, table_columns);
            collect_qualified_columns_from_from(right, table_columns);
            if let Some(cond) = condition {
                collect_qualified_columns_from_expr(cond, table_columns);
            }
        }
    }
}

fn collect_qualified_columns_from_expr(
    expr: &Expression,
    table_columns: &mut HashMap<String, Vec<String>>,
) {
    match expr {
        Expression::ColumnRef { schema: None, table: Some(t), column, .. } => {
            table_columns.entry(t.to_lowercase()).or_default().push(column.to_lowercase());
        }
        Expression::BinaryOp { left, right, .. } => {
            collect_qualified_columns_from_expr(left, table_columns);
            collect_qualified_columns_from_expr(right, table_columns);
        }
        Expression::UnaryOp { expr, .. } => {
            collect_qualified_columns_from_expr(expr, table_columns);
        }
        Expression::Function { args, .. } | Expression::AggregateFunction { args, .. } => {
            for arg in args {
                collect_qualified_columns_from_expr(arg, table_columns);
            }
        }
        Expression::InList { expr, values, .. } => {
            collect_qualified_columns_from_expr(expr, table_columns);
            for v in values {
                collect_qualified_columns_from_expr(v, table_columns);
            }
        }
        Expression::Case { operand, when_clauses, else_result } => {
            if let Some(op) = operand {
                collect_qualified_columns_from_expr(op, table_columns);
            }
            for clause in when_clauses {
                for cond in &clause.conditions {
                    collect_qualified_columns_from_expr(cond, table_columns);
                }
                collect_qualified_columns_from_expr(&clause.result, table_columns);
            }
            if let Some(else_res) = else_result {
                collect_qualified_columns_from_expr(else_res, table_columns);
            }
        }
        Expression::IsNull { expr, .. } | Expression::Cast { expr, .. } => {
            collect_qualified_columns_from_expr(expr, table_columns);
        }
        _ => {}
    }
}

/// Find common prefix for a set of column names
///
/// For TPC-DS style naming (d_year, d_date_sk), finds the underscore-delimited prefix.
pub(super) fn find_common_prefix(columns: &[String]) -> Option<String> {
    if columns.is_empty() {
        return None;
    }

    // Try to find prefix ending with underscore
    let first = &columns[0];
    if let Some(underscore_pos) = first.find('_') {
        let prefix = &first[..=underscore_pos]; // Include the underscore

        // Check if all columns have this prefix
        if columns.iter().all(|c| c.starts_with(prefix)) {
            return Some(prefix.to_string());
        }
    }

    // Fallback: use first 2 characters if all columns share them
    if first.len() >= 2 {
        let prefix = &first[..2];
        if columns.iter().all(|c| c.starts_with(prefix)) {
            return Some(prefix.to_string());
        }
    }

    None
}
