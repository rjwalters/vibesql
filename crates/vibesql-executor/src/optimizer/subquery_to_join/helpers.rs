//! Helper functions for subquery-to-join transformations
//!
//! This module provides utility functions for:
//! - Collecting and detecting table names (for self-join detection)
//! - Qualifying and rewriting column references

use vibesql_ast::{CommonTableExpr, Expression, FromClause, SelectItem, SelectStmt};
use vibesql_storage::Database;

/// One source directly reachable in the outer FROM clause, described in the terms
/// needed to decide whether it exposes a given unqualified column.
///
/// Every `FromClause` variant that can appear in the outer FROM maps to one of
/// these:
/// - `FromClause::Table` → `Named` (base table via `database.get_table`, or a
///   view via the catalog) unless it carries an explicit `column_aliases` list,
///   in which case its exposed names are exactly that list (`Columns`).
/// - `FromClause::Subquery` (derived table) → `Columns` from its explicit
///   `column_aliases`, else from the inner SELECT projection
///   (`derived_output_columns`); if neither can be determined, `Opaque`.
/// - `FromClause::Values` → `Columns` from its explicit `column_aliases`, else
///   the SQLite-style positional names `column1, column2, …`.
/// - `FromClause::Join` → recurses into both sides.
///
/// `effective` is always the qualifier a column reference would use (the alias
/// when present, otherwise the table name).
enum OuterSource {
    /// A catalog-resolvable source (base table, view, or enclosing CTE). The
    /// stored name is the real catalog/CTE name used to look columns up.
    Named { effective: String, real_name: String },
    /// A source whose exposed column names are known explicitly.
    Columns { effective: String, columns: Vec<String> },
    /// A derived (subquery/VALUES) source whose columns genuinely cannot be
    /// enumerated here (e.g. a `SELECT *` projection whose expansion is unknown).
    /// We can neither confirm nor deny the column lives here, so the transform
    /// must be skipped rather than risk a wrong qualification or a wrong error.
    /// Its effective name is irrelevant (we never qualify against it).
    Opaque,
}

/// The SQLite-style positional column names a bare VALUES table exposes when no
/// explicit column alias list is given: `column1`, `column2`, … one per column
/// in the first row.
fn values_positional_columns(rows: &[Vec<Expression>]) -> Option<Vec<String>> {
    let width = rows.first()?.len();
    if width == 0 {
        return None;
    }
    Some((1..=width).map(|i| format!("column{i}")).collect())
}

/// Collect every source directly reachable in the outer FROM clause, in the
/// terms needed for outer-scope column resolution. Covers all `FromClause`
/// variants (table/view, join, subquery/derived table, VALUES) uniformly.
fn collect_outer_sources(from: &FromClause, out: &mut Vec<OuterSource>) {
    match from {
        FromClause::Table { name, alias, column_aliases, .. } => {
            let effective = alias.clone().unwrap_or_else(|| name.clone());
            // An explicit `AS t(x, y)` column-alias list renames the exposed
            // columns; it is the authoritative output name set.
            if let Some(cols) = column_aliases {
                out.push(OuterSource::Columns { effective, columns: cols.clone() });
            } else {
                out.push(OuterSource::Named { effective, real_name: name.clone() });
            }
        }
        FromClause::Join { left, right, .. } => {
            collect_outer_sources(left, out);
            collect_outer_sources(right, out);
        }
        FromClause::Subquery { query, alias, column_aliases } => {
            let effective = alias.clone();
            if let Some(cols) = column_aliases {
                out.push(OuterSource::Columns { effective, columns: cols.clone() });
            } else if let Some(cols) = derived_output_columns(query) {
                out.push(OuterSource::Columns { effective, columns: cols });
            } else {
                out.push(OuterSource::Opaque);
            }
        }
        FromClause::Values { rows, alias, column_aliases } => {
            let effective = alias.clone();
            if let Some(cols) = column_aliases {
                out.push(OuterSource::Columns { effective, columns: cols.clone() });
            } else if let Some(cols) = values_positional_columns(rows) {
                out.push(OuterSource::Columns { effective, columns: cols });
            } else {
                out.push(OuterSource::Opaque);
            }
        }
        FromClause::TableFunction { alias, column_aliases, .. } => {
            // A table function's exposed columns are only statically known from
            // an explicit column-alias list; otherwise its output is opaque
            // here (the function is not yet executable).
            match (alias, column_aliases) {
                (Some(effective), Some(cols)) => out.push(OuterSource::Columns {
                    effective: effective.clone(),
                    columns: cols.clone(),
                }),
                _ => out.push(OuterSource::Opaque),
            }
        }
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
fn cte_output_columns(with_clause: Option<&[CommonTableExpr]>, name: &str) -> Option<Vec<String>> {
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

/// The outcome of resolving an unqualified column against the outer FROM scope.
///
/// SQLite scopes ambiguity to the outer FROM sources only (never the subquery's
/// own tables), so the whole point of this resolver is to reproduce that scope
/// exactly before the column is folded into the synthesized SEMI/ANTI-join
/// predicate — whose combined schema would otherwise trip the #5870 guard.
pub(super) enum OuterQualifier {
    /// Exactly one outer source exposes the column: qualify the reference with
    /// this effective name so it is unambiguous and bypasses the guard.
    Qualify(String),
    /// Leave the reference unqualified and let normal resolution / the ambiguity
    /// guard handle it. This covers two distinct enumerable cases that both match
    /// pre-existing behavior:
    /// - Two or more outer sources expose the column: genuinely ambiguous in the
    ///   outer scope, so the guard errors, matching SQLite.
    /// - No outer source exposes the column (and none were opaque): it is not an
    ///   outer-scope column at all (e.g. a correlated reference to a further-out
    ///   scope), so it stays unqualified for normal resolution.
    LeaveUnqualified,
    /// No enumerable outer source exposes the column and at least one source
    /// could not be enumerated (an opaque derived/VALUES table). We can neither
    /// qualify it (risking a wrong qualification) nor let the guard fire (risking
    /// a wrong error). The caller must skip the IN→join transform entirely and
    /// fall back to row-by-row `eval_in_subquery`, which never over-errors —
    /// preserving `main`'s over-accepting behavior instead of regressing to an
    /// error.
    Skip,
}

/// Resolve which outer-FROM source an unqualified column belongs to, matching
/// SQLite's scoping: ambiguity is measured ONLY against the outer FROM sources.
///
/// Every outer-FROM source kind is handled uniformly (see [`OuterSource`]):
/// - Base tables via `database.get_table`, views via `catalog.get_view`, and
///   enclosing CTEs via the query's `with_clause`.
/// - Derived (subquery) tables via their explicit `column_aliases` or their
///   inner SELECT projection.
/// - `VALUES` tables via their explicit `column_aliases` or the positional
///   `column1, column2, …` names.
/// - Table aliases with an explicit column list (`AS t(x, y)`).
///
/// The exactly-one → qualify / two-or-more → leave-for-guard rule applies to all
/// of them identically. When a derived/VALUES source is genuinely unenumerable
/// (e.g. `SELECT *`) and nothing else resolves the column, we return
/// [`OuterQualifier::Skip`] rather than let the guard fire — over-accepting
/// (matching `main`) is safe; over-erroring is the regression this PR exists to
/// prevent.
pub(super) fn resolve_outer_column_qualifier(
    from: &FromClause,
    database: &Database,
    with_clause: Option<&[CommonTableExpr]>,
    column: &str,
) -> OuterQualifier {
    let mut sources = Vec::new();
    collect_outer_sources(from, &mut sources);

    let mut matched_effective: Option<String> = None;
    let mut match_count = 0usize;
    let mut has_opaque = false;

    for source in &sources {
        match source {
            OuterSource::Named { effective, real_name } => {
                if source_has_column(real_name, column, database, with_clause) {
                    match_count += 1;
                    matched_effective.get_or_insert_with(|| effective.clone());
                }
            }
            OuterSource::Columns { effective, columns } => {
                if columns.iter().any(|c| c.eq_ignore_ascii_case(column)) {
                    match_count += 1;
                    matched_effective.get_or_insert_with(|| effective.clone());
                }
            }
            OuterSource::Opaque => {
                has_opaque = true;
            }
        }
    }

    match match_count {
        // Exactly one enumerable outer source carries the column.
        1 => OuterQualifier::Qualify(matched_effective.unwrap()),
        // Two or more carry it: genuinely ambiguous in the outer scope.
        n if n >= 2 => OuterQualifier::LeaveUnqualified,
        // Zero enumerable matches.
        _ => {
            if has_opaque {
                // The column might live in the source we couldn't enumerate.
                // Don't risk a wrong error — skip the transform.
                OuterQualifier::Skip
            } else {
                // Every source was enumerable and none carried the column. It is
                // therefore not an outer-scope column at all (e.g. a correlated
                // reference to a further-out scope, or resolvable only inside the
                // subquery). Leave it for normal resolution / the guard, exactly
                // as before.
                OuterQualifier::LeaveUnqualified
            }
        }
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
        FromClause::TableFunction { alias, .. } => {
            if let Some(a) = alias {
                names.push(a.clone());
            }
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
