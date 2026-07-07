//! Helper functions for subquery-to-join transformations
//!
//! This module provides utility functions for:
//! - Collecting and detecting table names (for self-join detection)
//! - Qualifying and rewriting column references

use vibesql_ast::{CommonTableExpr, Expression, FromClause, SelectItem, SelectStmt};
use vibesql_storage::Database;

/// Collect the `(effective_name, real_table_name)` pairs for every named table
/// directly reachable in a FROM clause, for outer-scope column resolution.
///
/// `effective_name` is the alias if present, otherwise the table name — it is the
/// qualifier a column reference would use. `real_table_name` is the catalog/CTE
/// name used to look the columns up (base table schema, view definition, or
/// enclosing CTE). Subquery/VALUES derived tables are skipped: their columns are
/// not resolvable against the catalog here, so an unqualified column that could
/// come from one of them is treated as unresolvable (we do not qualify it, and
/// the downstream ambiguity guard handles it).
fn collect_base_table_refs(from: &FromClause, out: &mut Vec<(String, String)>) {
    match from {
        FromClause::Table { name, alias, .. } => {
            let effective = alias.clone().unwrap_or_else(|| name.clone());
            out.push((effective, name.clone()));
        }
        FromClause::Join { left, right, .. } => {
            collect_base_table_refs(left, out);
            collect_base_table_refs(right, out);
        }
        // Derived tables (subquery/VALUES) have no catalog schema to consult here.
        FromClause::Subquery { .. } | FromClause::Values { .. } => {}
    }
}

/// Derive the output column names of a SELECT statement's projection.
///
/// Used to resolve which columns a view or CTE exposes to the outer scope when
/// no explicit column list was declared. Returns `None` if any output column
/// name cannot be determined (e.g. a `*` wildcard whose expansion is not known
/// here, or a computed expression without an alias or usable source text) — in
/// that case the caller must treat the source as unresolvable rather than risk a
/// false "column absent" verdict.
fn derived_output_columns(stmt: &SelectStmt) -> Option<Vec<String>> {
    // A VALUES-body view/CTE has no projection list; its column names are only
    // known from an explicit column list, handled by the callers.
    if stmt.select_list.is_empty() {
        return None;
    }
    let mut cols = Vec::with_capacity(stmt.select_list.len());
    for item in &stmt.select_list {
        match item {
            SelectItem::Expression { expr, alias, source_text } => {
                if let Some(a) = alias {
                    cols.push(a.clone());
                } else if let Expression::ColumnRef(col_id) = expr {
                    cols.push(col_id.column_canonical().to_string());
                } else if let Some(src) = source_text {
                    cols.push(src.clone());
                } else {
                    // Unnameable computed column — can't reason about this source.
                    return None;
                }
            }
            // Any wildcard means the output columns depend on the underlying
            // table(s), which we can't expand safely here.
            SelectItem::Wildcard { .. } | SelectItem::QualifiedWildcard { .. } => return None,
        }
    }
    Some(cols)
}

/// Find an enclosing CTE by name and return the column names it projects.
///
/// Prefers an explicit column list (`WITH cte(a, b) AS ...`), otherwise derives
/// the names from the CTE body's projection. Returns `None` when the name is not
/// a CTE in this WITH clause or its columns cannot be determined.
fn cte_output_columns(
    with_clause: Option<&[CommonTableExpr]>,
    name: &str,
) -> Option<Vec<String>> {
    let cte = with_clause?.iter().find(|c| c.name.eq_ignore_ascii_case(name))?;
    if let Some(cols) = &cte.columns {
        return Some(cols.clone());
    }
    derived_output_columns(&cte.query)
}

/// Return the output column names of a view, or `None` if the name is not a view
/// or its columns cannot be determined.
fn view_output_columns(database: &Database, name: &str) -> Option<Vec<String>> {
    let view = database.catalog.get_view(name)?;
    if let Some(cols) = &view.columns {
        return Some(cols.clone());
    }
    derived_output_columns(&view.query)
}

/// Whether the named outer-FROM source (base table, view, or enclosing CTE)
/// exposes `column`. Enclosing CTEs shadow catalog objects of the same name, and
/// views are consulted only when no base table matches — mirroring name
/// resolution precedence so the outer scope is measured exactly as SQLite sees it.
fn source_has_column(
    name: &str,
    column: &str,
    database: &Database,
    with_clause: Option<&[CommonTableExpr]>,
) -> bool {
    // CTEs take precedence: at transform time they are not in the catalog, but
    // they shadow any same-named catalog object in the query's scope.
    if let Some(cols) = cte_output_columns(with_clause, name) {
        return cols.iter().any(|c| c.eq_ignore_ascii_case(column));
    }
    if let Some(table) = database.get_table(name) {
        return table.schema.has_column(column);
    }
    if let Some(cols) = view_output_columns(database, name) {
        return cols.iter().any(|c| c.eq_ignore_ascii_case(column));
    }
    false
}

/// Resolve which outer-FROM source an unqualified column belongs to, matching
/// SQLite's scoping: ambiguity is measured ONLY against the outer FROM sources.
///
/// Outer sources are resolved through all three name-resolution kinds — base
/// tables (`database.get_table`), views (`catalog.get_view` /
/// `ViewDefinition.columns`), and enclosing CTEs (the query's `with_clause`) —
/// so a view or CTE in the outer FROM is treated exactly like a base table.
///
/// Returns:
/// - `Some(effective_name)` when exactly one outer source has the column — the
///   qualifier to attach so the reference is unambiguous.
/// - `None` when zero or two-or-more outer sources have the column, or when a
///   derived (subquery/VALUES) table is present in the outer FROM (unresolvable
///   here). In the two-or-more case the reference is genuinely ambiguous in the
///   outer scope and must stay unqualified so the downstream guard errors,
///   exactly as SQLite does.
pub(super) fn resolve_outer_column_qualifier(
    from: &FromClause,
    database: &Database,
    with_clause: Option<&[CommonTableExpr]>,
    column: &str,
) -> Option<String> {
    let mut refs = Vec::new();
    collect_base_table_refs(from, &mut refs);

    // If any derived table is in scope we cannot be sure the column does not also
    // live there, so we conservatively decline to qualify.
    let has_derived = from_has_derived_table(from);
    if has_derived {
        return None;
    }

    let mut matches = refs
        .iter()
        .filter(|(_, table_name)| source_has_column(table_name, column, database, with_clause));

    let first = matches.next()?;
    if matches.next().is_some() {
        // Two or more outer sources carry the column: genuinely ambiguous.
        None
    } else {
        Some(first.0.clone())
    }
}

/// Whether the FROM clause contains any subquery/VALUES derived table.
fn from_has_derived_table(from: &FromClause) -> bool {
    match from {
        FromClause::Table { .. } => false,
        FromClause::Join { left, right, .. } => {
            from_has_derived_table(left) || from_has_derived_table(right)
        }
        FromClause::Subquery { .. } | FromClause::Values { .. } => true,
    }
}

/// Extract all table names from a FROM clause (for self-join detection)
pub(super) fn collect_table_names(from: &FromClause, names: &mut Vec<String>) {
    match from {
        FromClause::Table { name, alias, .. } => {
            // Use alias if present, otherwise table name
            names.push(alias.clone().unwrap_or_else(|| name.clone()));
            names.push(name.clone()); // Also add original name for matching
        }
        FromClause::Join { left, right, .. } => {
            collect_table_names(left, names);
            collect_table_names(right, names);
        }
        FromClause::Subquery { alias, .. } => {
            names.push(alias.clone());
        }
        FromClause::Values { alias, .. } => {
            names.push(alias.clone());
        }
    }
}

/// Check if a table name conflicts with existing tables in the FROM clause
pub(super) fn is_self_join(
    from: &FromClause,
    table_name: &str,
    table_alias: &Option<String>,
) -> bool {
    let mut existing_names = Vec::new();
    collect_table_names(from, &mut existing_names);

    // The effective name is alias if present, otherwise table_name
    let effective_name = table_alias.as_deref().unwrap_or(table_name);

    // Check if this name conflicts with any existing table
    existing_names
        .iter()
        .any(|n| n.eq_ignore_ascii_case(effective_name) || n.eq_ignore_ascii_case(table_name))
}

/// Check if this is a simple self-join: exactly one table in outer query matching subquery's table
///
/// Returns true only when:
/// 1. The outer FROM clause has exactly ONE table (not a join, not multiple tables)
/// 2. That table's name matches the subquery's table name
///
/// This is used to safely determine if unqualified columns in a subquery can be optimized.
/// When the outer query has multiple tables, unqualified columns could reference any of them,
/// so we must skip optimization to avoid incorrectly rewriting correlated references.
pub(super) fn is_simple_single_table_self_join(
    from: &FromClause,
    table_name: &str,
    table_alias: &Option<String>,
) -> bool {
    // Only match simple single-table FROM clauses
    match from {
        FromClause::Table { name, alias, .. } => {
            // Check if this single table matches the subquery's table
            let outer_effective_name = alias.as_deref().unwrap_or(name);
            let subquery_effective_name = table_alias.as_deref().unwrap_or(table_name);

            // Match either by effective name or table name (case-insensitive)
            outer_effective_name.eq_ignore_ascii_case(subquery_effective_name)
                || name.eq_ignore_ascii_case(table_name)
        }
        // Joins, subqueries, or VALUES have multiple "tables" - not a simple self-join
        _ => false,
    }
}

/// Get the effective table name from a simple single-table FROM clause.
///
/// Returns the alias if present, otherwise the table name.
/// Returns None if the FROM clause is not a simple single table.
///
/// This is used to qualify outer expressions in self-joins to avoid ambiguity
/// when the same table appears on both sides of the join.
pub(super) fn get_outer_table_name(from: &FromClause) -> Option<String> {
    match from {
        FromClause::Table { name, alias, .. } => {
            // Return alias if present, otherwise table name
            Some(alias.clone().unwrap_or_else(|| name.clone()))
        }
        // For non-simple FROM clauses, we can't determine a single table name
        _ => None,
    }
}

/// Rewrite column references in an expression to use a new table qualifier
pub(super) fn rewrite_column_refs_with_alias(
    expr: &Expression,
    old_table: &str,
    new_alias: &str,
) -> Expression {
    match expr {
        Expression::ColumnRef(col_id) => {
            // Rewrite if:
            // 1. No table qualifier (unqualified column from the subquery table)
            // 2. Table qualifier matches the old table name
            let should_rewrite = match col_id.table_canonical() {
                None => true, // Unqualified columns from subquery should be rewritten
                Some(t) => t.eq_ignore_ascii_case(old_table),
            };

            if should_rewrite {
                Expression::ColumnRef(vibesql_ast::ColumnIdentifier::qualified(
                    new_alias,
                    false,
                    col_id.column_canonical(),
                    false,
                ))
            } else {
                expr.clone()
            }
        }
        Expression::BinaryOp { left, op, right } => Expression::BinaryOp {
            left: Box::new(rewrite_column_refs_with_alias(left, old_table, new_alias)),
            op: op.clone(),
            right: Box::new(rewrite_column_refs_with_alias(right, old_table, new_alias)),
        },
        Expression::UnaryOp { op, expr: inner } => Expression::UnaryOp {
            op: op.clone(),
            expr: Box::new(rewrite_column_refs_with_alias(inner, old_table, new_alias)),
        },
        Expression::IsNull { expr: inner, negated } => Expression::IsNull {
            expr: Box::new(rewrite_column_refs_with_alias(inner, old_table, new_alias)),
            negated: *negated,
        },
        Expression::Between { expr: inner, low, high, negated, symmetric } => Expression::Between {
            expr: Box::new(rewrite_column_refs_with_alias(inner, old_table, new_alias)),
            low: Box::new(rewrite_column_refs_with_alias(low, old_table, new_alias)),
            high: Box::new(rewrite_column_refs_with_alias(high, old_table, new_alias)),
            negated: *negated,
            symmetric: *symmetric,
        },
        Expression::InList { expr: inner, values, negated } => Expression::InList {
            expr: Box::new(rewrite_column_refs_with_alias(inner, old_table, new_alias)),
            values: values
                .iter()
                .map(|v| rewrite_column_refs_with_alias(v, old_table, new_alias))
                .collect(),
            negated: *negated,
        },
        Expression::Like { expr: inner, pattern, negated, escape } => Expression::Like {
            expr: Box::new(rewrite_column_refs_with_alias(inner, old_table, new_alias)),
            pattern: Box::new(rewrite_column_refs_with_alias(pattern, old_table, new_alias)),
            negated: *negated,
            escape: escape
                .as_ref()
                .map(|e| Box::new(rewrite_column_refs_with_alias(e, old_table, new_alias))),
        },
        Expression::Function { name, args, character_unit } => Expression::Function {
            name: name.clone(),
            args: args
                .iter()
                .map(|a| rewrite_column_refs_with_alias(a, old_table, new_alias))
                .collect(),
            character_unit: character_unit.clone(),
        },
        Expression::Case { operand, when_clauses, else_result } => Expression::Case {
            operand: operand
                .as_ref()
                .map(|o| Box::new(rewrite_column_refs_with_alias(o, old_table, new_alias))),
            when_clauses: when_clauses
                .iter()
                .map(|case_when| vibesql_ast::CaseWhen {
                    conditions: case_when
                        .conditions
                        .iter()
                        .map(|c| rewrite_column_refs_with_alias(c, old_table, new_alias))
                        .collect(),
                    result: rewrite_column_refs_with_alias(&case_when.result, old_table, new_alias),
                })
                .collect(),
            else_result: else_result
                .as_ref()
                .map(|e| Box::new(rewrite_column_refs_with_alias(e, old_table, new_alias))),
        },
        Expression::Cast { expr: inner, data_type } => Expression::Cast {
            expr: Box::new(rewrite_column_refs_with_alias(inner, old_table, new_alias)),
            data_type: data_type.clone(),
        },
        // Handle nested IN subqueries: rewrite the outer expression but NOT the subquery
        // (the subquery has its own scope and column references shouldn't be changed)
        Expression::In { expr: inner, subquery, negated } => Expression::In {
            expr: Box::new(rewrite_column_refs_with_alias(inner, old_table, new_alias)),
            subquery: subquery.clone(), // Don't rewrite inside subquery - different scope
            negated: *negated,
        },
        // Handle EXISTS subqueries similarly - don't rewrite inside the subquery
        Expression::Exists { subquery, negated } => Expression::Exists {
            subquery: subquery.clone(), // Don't rewrite inside subquery - different scope
            negated: *negated,
        },
        // Scalar subquery: same as EXISTS, separate scope
        Expression::ScalarSubquery(subquery) => Expression::ScalarSubquery(subquery.clone()),
        // For other expression types, just clone (they don't contain column refs that need
        // rewriting)
        _ => expr.clone(),
    }
}
