//! RETURNING clause projection for UPDATE statements (SQLite 3.35.0+)
//!
//! SQLite semantics: RETURNING yields one result row per updated row,
//! evaluated against the NEW row values (after SET assignments). For
//! UPDATEs on views routed through INSTEAD OF triggers, the NEW view row
//! is returned once per trigger fire, regardless of what the trigger body
//! actually does.

use vibesql_ast::{Expression, SelectItem};
use vibesql_catalog::TableSchema;
use vibesql_storage::{Database, Row};

use crate::{errors::ExecutorError, evaluator::ExpressionEvaluator, select::SelectResult};

/// Project RETURNING select items against a set of NEW rows.
///
/// `rows` must contain values in `schema` column order (one entry per
/// updated row / trigger fire). Returns a `SelectResult` whose columns are
/// derived from the RETURNING items (aliases win, then original source
/// text, then the expression's column name).
pub(super) fn project_returning(
    items: &[SelectItem],
    schema: &TableSchema,
    database: &Database,
    table_alias: Option<&str>,
    rows: &[&Row],
) -> Result<SelectResult, ExecutorError> {
    let mut evaluator = ExpressionEvaluator::with_database(schema, database);
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

    // Evaluate each item against each NEW row.
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

/// Validate that a qualified wildcard (`t.*`) refers to the updated table.
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
