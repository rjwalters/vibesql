//! RETURNING clause projection for DML statements (SQLite 3.35.0+)
//!
//! SQLite semantics: RETURNING yields one result row per affected row.
//! UPDATE evaluates against the NEW row values (after SET assignments);
//! DELETE evaluates against the OLD row values (before deletion). For
//! statements on views routed through INSTEAD OF triggers, the view row
//! is returned once per trigger fire, regardless of what the trigger body
//! actually does.
//!
//! The projection itself is statement-agnostic: callers pass the rows to
//! evaluate against (NEW rows for UPDATE, OLD rows for DELETE).

use vibesql_ast::{Expression, SelectItem};
use vibesql_catalog::TableSchema;
use vibesql_storage::{Database, Row};
use vibesql_types::SqlValue;

use crate::{errors::ExecutorError, evaluator::ExpressionEvaluator, select::SelectResult};

/// Project RETURNING select items against a set of rows.
///
/// `rows` must contain values in `schema` column order (one entry per
/// affected row / trigger fire). Returns a `SelectResult` whose columns are
/// derived from the RETURNING items (aliases win, then original source
/// text, then the expression's column name).
///
/// `table_alias` should be `None` for RETURNING projection: unlike the rest
/// of a DML statement (WHERE/SET), the RETURNING clause does NOT honor the
/// target table's alias. In SQLite, `UPDATE t1 AS a ... RETURNING a.b` raises
/// `no such column: a.b` while `RETURNING t1.b` succeeds — qualified
/// references resolve against the real table name, not the alias (see
/// returning1.test 7.7/7.8, issue #5840 item 6). The parameter is retained
/// for wildcard-qualifier validation flexibility but callers pass `None`.
///
/// `cte_results` carries the enclosing statement's WITH-clause CTEs (if any)
/// so subqueries in RETURNING expressions can reference CTE names, matching
/// SQLite (issue #5359).
pub(crate) fn project_returning(
    items: &[SelectItem],
    schema: &TableSchema,
    database: &Database,
    table_alias: Option<&str>,
    rows: &[&Row],
    cte_results: Option<&std::collections::HashMap<String, crate::select::cte::CteResult>>,
) -> Result<SelectResult, ExecutorError> {
    let mut evaluator = ExpressionEvaluator::with_database(schema, database);
    if let Some(ctes) = cte_results {
        evaluator = evaluator.with_cte_context(ctes);
    }
    if let Some(alias) = table_alias {
        evaluator.set_table_alias(alias.to_string());
    }

    let visible_columns = visible_columns(schema);
    let columns = derive_returning_columns(items, schema, table_alias, &visible_columns)?;

    // Evaluate each item against each row.
    let mut result_rows: Vec<Row> = Vec::with_capacity(rows.len());
    for row in rows {
        // Clear the CSE cache so expression results from the previous row
        // are not replayed for this one.
        evaluator.clear_cse_cache();
        let values = project_returning_row_with(
            &mut evaluator,
            items,
            &visible_columns,
            columns.len(),
            row,
        )?;
        result_rows.push(Row::new(values));
    }

    Ok(SelectResult { columns, rows: result_rows })
}

/// Indices of columns expanded by `*` (hidden `__hidden__*` view columns are
/// excluded, matching SQLite's hidden-column convention).
pub(crate) fn visible_columns(schema: &TableSchema) -> Vec<usize> {
    schema
        .columns
        .iter()
        .enumerate()
        .filter(|(_, c)| !c.name.starts_with("__hidden__"))
        .map(|(i, _)| i)
        .collect()
}

/// Derive RETURNING result column names (computable even with zero affected
/// rows: aliases win, then a bare column's canonical name, then the source
/// text, then a debug fallback).
pub(crate) fn derive_returning_columns(
    items: &[SelectItem],
    schema: &TableSchema,
    table_alias: Option<&str>,
    visible_columns: &[usize],
) -> Result<Vec<String>, ExecutorError> {
    let mut columns: Vec<String> = Vec::new();
    for item in items {
        match item {
            SelectItem::Wildcard { .. } => {
                columns.extend(visible_columns.iter().map(|&i| schema.columns[i].name.clone()));
            }
            SelectItem::QualifiedWildcard { qualifier, .. } => {
                validate_wildcard_qualifier(qualifier, schema, table_alias)?;
                columns.extend(visible_columns.iter().map(|&i| schema.columns[i].name.clone()));
            }
            SelectItem::Expression { expr, alias, source_text } => {
                let name = if let Some(alias) = alias {
                    alias.clone()
                } else if let Expression::ColumnRef(col_id) = expr {
                    col_id.column_canonical().to_string()
                } else if let Some(text) = source_text {
                    text.clone()
                } else {
                    format!("{:?}", expr)
                };
                columns.push(name);
            }
        }
    }
    Ok(columns)
}

/// Project a single RETURNING result row using an already-configured
/// evaluator. Shared by the batch path and the per-row (subquery) path.
fn project_returning_row_with(
    evaluator: &mut ExpressionEvaluator,
    items: &[SelectItem],
    visible_columns: &[usize],
    column_count: usize,
    row: &Row,
) -> Result<Vec<SqlValue>, ExecutorError> {
    let mut values = Vec::with_capacity(column_count);
    for item in items {
        match item {
            SelectItem::Wildcard { .. } | SelectItem::QualifiedWildcard { .. } => {
                for &i in visible_columns {
                    values.push(
                        row.values
                            .get(i)
                            .cloned()
                            .ok_or(ExecutorError::ColumnIndexOutOfBounds { index: i })?,
                    );
                }
            }
            SelectItem::Expression { expr, .. } => {
                values.push(evaluator.eval(expr, row)?);
            }
        }
    }
    Ok(values)
}

/// Project one RETURNING result row against the *current* database state.
///
/// Unlike [`project_returning`] (which evaluates every row against a single
/// database snapshot), this evaluates a single affected row against the
/// database as it stands right now — the caller invokes it once per row,
/// interleaved with the incremental mutation of each row, so subqueries in
/// RETURNING expressions observe the per-row post-mutation state. This matches
/// SQLite, where a subquery referencing the table being modified is treated as
/// correlated and recomputed after each step (returning1.test section 20).
///
/// A fresh evaluator is built per call so the subquery sees the live database;
/// this is only used on the subquery-bearing slow path, so the extra cost is
/// confined to that case.
pub(crate) fn project_returning_row(
    items: &[SelectItem],
    columns: &[String],
    schema: &TableSchema,
    database: &Database,
    row: &Row,
    visible_columns: &[usize],
    cte_results: Option<&std::collections::HashMap<String, crate::select::cte::CteResult>>,
) -> Result<Row, ExecutorError> {
    let mut evaluator = ExpressionEvaluator::with_database(schema, database);
    if let Some(ctes) = cte_results {
        evaluator = evaluator.with_cte_context(ctes);
    }
    let values =
        project_returning_row_with(&mut evaluator, items, visible_columns, columns.len(), row)?;
    Ok(Row::new(values))
}

/// True if any RETURNING expression contains a subquery. Subquery-bearing
/// RETURNING must be projected per row as each row is mutated (so the subquery
/// recomputes against the incremental table state); subquery-free RETURNING
/// keeps the cheaper statement-end batch path with zero behavior change.
pub(crate) fn returning_has_subquery(items: &[SelectItem]) -> bool {
    items.iter().any(|item| match item {
        SelectItem::Expression { expr, .. } => expression_has_subquery(expr),
        _ => false,
    })
}

/// Recursively test whether an expression contains any subquery form
/// (scalar subquery, IN (SELECT), EXISTS, or a quantified comparison).
fn expression_has_subquery(expr: &Expression) -> bool {
    use Expression::*;
    match expr {
        ScalarSubquery(_) | Exists { .. } => true,
        In { expr, .. } => {
            // `IN (SELECT ...)` always carries a subquery.
            let _ = expr;
            true
        }
        QuantifiedComparison { .. } => true,
        InList { expr, values, .. } => {
            expression_has_subquery(expr) || values.iter().any(expression_has_subquery)
        }
        BinaryOp { left, right, .. } | IsDistinctFrom { left, right, .. } => {
            expression_has_subquery(left) || expression_has_subquery(right)
        }
        Conjunction(exprs) | Disjunction(exprs) => exprs.iter().any(expression_has_subquery),
        UnaryOp { expr, .. } | IsNull { expr, .. } | IsTruthValue { expr, .. } => {
            expression_has_subquery(expr)
        }
        Cast { expr, .. } => expression_has_subquery(expr),
        Extract { expr, .. } => expression_has_subquery(expr),
        Between { expr, low, high, .. } => {
            expression_has_subquery(expr)
                || expression_has_subquery(low)
                || expression_has_subquery(high)
        }
        Like { expr, pattern, escape, .. } | Glob { expr, pattern, escape, .. } => {
            expression_has_subquery(expr)
                || expression_has_subquery(pattern)
                || escape.as_deref().is_some_and(expression_has_subquery)
        }
        Position { substring, string, .. } => {
            expression_has_subquery(substring) || expression_has_subquery(string)
        }
        Trim { removal_char, string, .. } => {
            removal_char.as_deref().is_some_and(expression_has_subquery)
                || expression_has_subquery(string)
        }
        Function { args, .. } => args.iter().any(expression_has_subquery),
        AggregateFunction { args, filter, .. } => {
            args.iter().any(expression_has_subquery)
                || filter.as_deref().is_some_and(expression_has_subquery)
        }
        Case { operand, when_clauses, else_result } => {
            operand.as_deref().is_some_and(expression_has_subquery)
                || when_clauses.iter().any(|w| {
                    w.conditions.iter().any(expression_has_subquery)
                        || expression_has_subquery(&w.result)
                })
                || else_result.as_deref().is_some_and(expression_has_subquery)
        }
        _ => false,
    }
}

/// Validate that a qualified wildcard (`t.*`) refers to the target table.
fn validate_wildcard_qualifier(
    qualifier: &str,
    schema: &TableSchema,
    table_alias: Option<&str>,
) -> Result<(), ExecutorError> {
    let matches_table = qualifier.eq_ignore_ascii_case(&schema.name)
        || table_alias.is_some_and(|a| qualifier.eq_ignore_ascii_case(a));
    if matches_table {
        Ok(())
    } else {
        Err(ExecutorError::TableNotFound(qualifier.to_string()))
    }
}
