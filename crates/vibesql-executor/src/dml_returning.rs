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

    // Indices of columns expanded by `*` (hidden `__hidden__*` view columns
    // are excluded, matching SQLite's hidden-column convention).
    let visible_columns: Vec<usize> = schema
        .columns
        .iter()
        .enumerate()
        .filter(|(_, c)| !c.name.starts_with("__hidden__"))
        .map(|(i, _)| i)
        .collect();

    // Derive result column names (computable even with zero updated rows).
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

    // Evaluate each item against each row.
    let mut result_rows: Vec<Row> = Vec::with_capacity(rows.len());
    for row in rows {
        // Clear the CSE cache so expression results from the previous row
        // are not replayed for this one.
        evaluator.clear_cse_cache();
        let mut values = Vec::with_capacity(columns.len());
        for item in items {
            match item {
                SelectItem::Wildcard { .. } | SelectItem::QualifiedWildcard { .. } => {
                    for &i in &visible_columns {
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
        result_rows.push(Row::new(values));
    }

    Ok(SelectResult { columns, rows: result_rows })
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
