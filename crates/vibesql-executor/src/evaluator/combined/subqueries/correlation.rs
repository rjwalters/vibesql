//! Correlation detection for subqueries
//!
//! This module provides utilities to detect and extract correlated column
//! references from subqueries. Correlated subqueries reference columns from
//! outer queries, which affects caching and execution strategies.

/// Extract correlation values from the current row for correlated subquery caching
///
/// For correlated subqueries, we need to identify which columns from the outer query
/// are referenced within the subquery, and extract their values from the current row.
/// These values, combined with the subquery hash, form a composite cache key.
///
/// # Implementation
///
/// This function collects all column references in the subquery that belong to the
/// outer schema (not the subquery's own FROM clause). The values of these columns
/// from the current row are extracted and returned in a deterministic order.
///
/// # Returns
///
/// A Vec of (column_name, value) pairs representing the correlation columns and their
/// values in the current row. Returns None if extraction fails (e.g., column not found).
///
/// # Example
///
/// For TPC-H Q2:
/// ```sql
/// SELECT ... WHERE ps_supplycost = (
///     SELECT MIN(ps_supplycost)
///     FROM partsupp, ...
///     WHERE p_partkey = ps_partkey  -- Correlation: references outer p_partkey
/// )
/// ```
/// Returns: vec![("p_partkey", <value_from_current_row>)]
pub(super) fn extract_correlation_values(
    subquery: &vibesql_ast::SelectStmt,
    row: &vibesql_storage::Row,
    outer_schema: &crate::schema::CombinedSchema,
) -> Option<Vec<(String, vibesql_types::SqlValue)>> {
    // Get all tables in the subquery's FROM clause
    let subquery_tables = extract_table_names_from_from_clause(subquery.from.as_ref());

    // Collect correlation column references
    let mut correlation_refs = std::collections::BTreeSet::new(); // Use BTreeSet for deterministic ordering
    collect_correlation_refs(subquery, outer_schema, &subquery_tables, &mut correlation_refs);

    // Extract values from the current row
    let mut correlation_values = Vec::new();
    for (table, column) in correlation_refs {
        match outer_schema.get_column_index(table.as_deref(), &column) {
            Some(idx) => {
                if let Some(value) = row.values.get(idx) {
                    let full_name = if let Some(t) = &table {
                        format!("{}.{}", t, column)
                    } else {
                        column.clone()
                    };
                    correlation_values.push((full_name, value.clone()));
                } else {
                    return None; // Column index out of bounds
                }
            }
            None => return None, // Column not found in outer schema
        }
    }

    Some(correlation_values)
}

/// Extract table names from a FROM clause (helper for correlation detection)
fn extract_table_names_from_from_clause(from: Option<&vibesql_ast::FromClause>) -> Vec<String> {
    let mut tables = Vec::new();
    if let Some(from_clause) = from {
        extract_table_names_recursive(from_clause, &mut tables);
    }
    tables
}

/// Recursively extract table names from a FROM clause
fn extract_table_names_recursive(from: &vibesql_ast::FromClause, tables: &mut Vec<String>) {
    match from {
        vibesql_ast::FromClause::Table { name, alias, .. } => {
            tables.push(alias.clone().unwrap_or_else(|| name.clone()));
        }
        vibesql_ast::FromClause::Join { left, right, .. } => {
            extract_table_names_recursive(left, tables);
            extract_table_names_recursive(right, tables);
        }
        vibesql_ast::FromClause::Subquery { alias, .. } => {
            tables.push(alias.clone());
        }
    }
}

/// Collect all correlation column references from a subquery
fn collect_correlation_refs(
    subquery: &vibesql_ast::SelectStmt,
    outer_schema: &crate::schema::CombinedSchema,
    subquery_tables: &[String],
    refs: &mut std::collections::BTreeSet<(Option<String>, String)>,
) {
    // Check WHERE clause
    if let Some(where_clause) = &subquery.where_clause {
        collect_correlation_refs_from_expr(where_clause, outer_schema, subquery_tables, refs);
    }

    // Check SELECT list
    for item in &subquery.select_list {
        if let vibesql_ast::SelectItem::Expression { expr, .. } = item {
            collect_correlation_refs_from_expr(expr, outer_schema, subquery_tables, refs);
        }
    }

    // Check HAVING clause
    if let Some(having) = &subquery.having {
        collect_correlation_refs_from_expr(having, outer_schema, subquery_tables, refs);
    }

    // Check GROUP BY
    if let Some(group_by) = &subquery.group_by {
        for expr in group_by.all_expressions() {
            collect_correlation_refs_from_expr(expr, outer_schema, subquery_tables, refs);
        }
    }
}

/// Collect correlation column references from an expression
fn collect_correlation_refs_from_expr(
    expr: &vibesql_ast::Expression,
    outer_schema: &crate::schema::CombinedSchema,
    subquery_tables: &[String],
    refs: &mut std::collections::BTreeSet<(Option<String>, String)>,
) {
    match expr {
        vibesql_ast::Expression::ColumnRef { table, column } => {
            // Check if this column reference belongs to the outer schema
            if let Some(table_name) = table {
                let table_lower = table_name.to_lowercase();
                if !subquery_tables.iter().any(|t| t.to_lowercase() == table_lower) {
                    // Not in subquery's tables, check if in outer schema
                    if outer_schema.get_column_index(Some(table_name), column).is_some() {
                        refs.insert((Some(table_name.clone()), column.clone()));
                    }
                }
            } else if !subquery_tables.is_empty() {
                // Unqualified reference - check if it's in outer schema but not in subquery tables
                // This is conservative: we only add it if we're sure it's external
                if outer_schema.get_column_index(None, column).is_some() {
                    refs.insert((None, column.clone()));
                }
            }
        }
        vibesql_ast::Expression::BinaryOp { left, right, .. } => {
            collect_correlation_refs_from_expr(left, outer_schema, subquery_tables, refs);
            collect_correlation_refs_from_expr(right, outer_schema, subquery_tables, refs);
        }
        vibesql_ast::Expression::UnaryOp { expr, .. } => {
            collect_correlation_refs_from_expr(expr, outer_schema, subquery_tables, refs);
        }
        vibesql_ast::Expression::Function { args, .. }
        | vibesql_ast::Expression::AggregateFunction { args, .. } => {
            for arg in args {
                collect_correlation_refs_from_expr(arg, outer_schema, subquery_tables, refs);
            }
        }
        vibesql_ast::Expression::IsNull { expr, .. } => {
            collect_correlation_refs_from_expr(expr, outer_schema, subquery_tables, refs);
        }
        vibesql_ast::Expression::Case { operand, when_clauses, else_result } => {
            if let Some(op) = operand {
                collect_correlation_refs_from_expr(op, outer_schema, subquery_tables, refs);
            }
            for when in when_clauses {
                for condition in &when.conditions {
                    collect_correlation_refs_from_expr(
                        condition,
                        outer_schema,
                        subquery_tables,
                        refs,
                    );
                }
                collect_correlation_refs_from_expr(
                    &when.result,
                    outer_schema,
                    subquery_tables,
                    refs,
                );
            }
            if let Some(else_expr) = else_result {
                collect_correlation_refs_from_expr(else_expr, outer_schema, subquery_tables, refs);
            }
        }
        vibesql_ast::Expression::InList { expr, values, .. } => {
            collect_correlation_refs_from_expr(expr, outer_schema, subquery_tables, refs);
            for val in values {
                collect_correlation_refs_from_expr(val, outer_schema, subquery_tables, refs);
            }
        }
        vibesql_ast::Expression::Between { expr, low, high, .. } => {
            collect_correlation_refs_from_expr(expr, outer_schema, subquery_tables, refs);
            collect_correlation_refs_from_expr(low, outer_schema, subquery_tables, refs);
            collect_correlation_refs_from_expr(high, outer_schema, subquery_tables, refs);
        }
        vibesql_ast::Expression::Like { expr, pattern, .. } => {
            collect_correlation_refs_from_expr(expr, outer_schema, subquery_tables, refs);
            collect_correlation_refs_from_expr(pattern, outer_schema, subquery_tables, refs);
        }
        vibesql_ast::Expression::Cast { expr, .. } => {
            collect_correlation_refs_from_expr(expr, outer_schema, subquery_tables, refs);
        }
        vibesql_ast::Expression::Position { substring, string, .. } => {
            collect_correlation_refs_from_expr(substring, outer_schema, subquery_tables, refs);
            collect_correlation_refs_from_expr(string, outer_schema, subquery_tables, refs);
        }
        vibesql_ast::Expression::Trim { removal_char, string, .. } => {
            if let Some(c) = removal_char {
                collect_correlation_refs_from_expr(c, outer_schema, subquery_tables, refs);
            }
            collect_correlation_refs_from_expr(string, outer_schema, subquery_tables, refs);
        }
        vibesql_ast::Expression::Interval { value, .. } => {
            collect_correlation_refs_from_expr(value, outer_schema, subquery_tables, refs);
        }
        // Literals, subqueries, and special expressions don't contribute to correlation
        _ => {}
    }
}
