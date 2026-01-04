//! Trigger execution logic for UPDATE operations
//!
//! This module handles:
//! - Executing UPDATE statements on VIEWs via INSTEAD OF triggers
//! - Building pseudo-schemas for views
//! - Trigger context propagation

use vibesql_ast::{TriggerTiming, UpdateStmt, WhereClause};
use vibesql_catalog::{ColumnSchema, TableSchema, ViewDefinition};
use vibesql_storage::{Database, Row};
use vibesql_types::{DataType, SqlValue};

use crate::{errors::ExecutorError, evaluator::ExpressionEvaluator};

/// Execute an UPDATE statement with trigger context
/// This function is used when executing UPDATE statements within trigger bodies
/// to support OLD/NEW pseudo-variable references
pub fn execute_update_with_trigger_context(
    database: &mut Database,
    stmt: &UpdateStmt,
    trigger_context: &crate::trigger_execution::TriggerContext,
) -> Result<usize, ExecutorError> {
    super::UpdateExecutor::execute_with_trigger_context(stmt, database, trigger_context)
}

/// Execute UPDATE on a VIEW using INSTEAD OF triggers
///
/// When updating a view, we need to fire INSTEAD OF UPDATE triggers
/// instead of actually updating data. The triggers typically update
/// the underlying tables.
pub(super) fn execute_update_on_view(
    database: &mut Database,
    stmt: &UpdateStmt,
    view_def: &ViewDefinition,
    procedural_context: Option<&crate::procedural::ExecutionContext>,
    trigger_context: Option<&crate::trigger_execution::TriggerContext>,
) -> Result<usize, ExecutorError> {
    // Find INSTEAD OF UPDATE triggers for this view
    let triggers = crate::TriggerFirer::find_triggers(
        database,
        &view_def.name,
        TriggerTiming::InsteadOf,
        vibesql_ast::TriggerEvent::Update(None),
    );

    if triggers.is_empty() {
        return Err(ExecutorError::UnsupportedExpression(format!(
            "Cannot UPDATE view '{}' without INSTEAD OF trigger",
            view_def.name
        )));
    }

    // Build a pseudo-schema for the view
    let view_schema = build_view_schema(database, view_def)?;

    // Execute the view query to get the rows to potentially update
    let select_executor = crate::SelectExecutor::new(database);
    let all_rows = select_executor.execute_with_columns(&view_def.query)?;

    // Collect all (old_row, new_row) pairs first, before firing triggers
    // This avoids borrow conflicts with the evaluator
    let updates: Vec<(Row, Row)> = {
        // Create evaluator for WHERE clause (if any)
        let evaluator = if let Some(ctx) = trigger_context {
            ExpressionEvaluator::with_trigger_context(&view_schema, database, ctx)
        } else if let Some(ctx) = procedural_context {
            ExpressionEvaluator::with_procedural_context(&view_schema, database, ctx)
        } else {
            ExpressionEvaluator::with_database(&view_schema, database)
        };

        // Select rows matching WHERE clause and build updates
        let mut collected_updates = Vec::new();
        for row in &all_rows.rows {
            let matches = match &stmt.where_clause {
                Some(WhereClause::Condition(expr)) => match evaluator.eval(expr, row)? {
                    SqlValue::Boolean(b) => b,
                    SqlValue::Null => false,
                    _ => false,
                },
                None => true, // No WHERE clause - update all rows
                Some(WhereClause::CurrentOf(_)) => {
                    return Err(ExecutorError::UnsupportedExpression(
                        "CURRENT OF not supported for view updates".to_string(),
                    ));
                }
            };

            if matches {
                let old_row = row.clone();

                // Build NEW row by applying assignments
                let mut new_row_values = old_row.values.clone();

                for assignment in &stmt.assignments {
                    // Find column index in view
                    let col_idx = view_schema
                        .columns
                        .iter()
                        .position(|c| c.name.to_uppercase() == assignment.column.to_uppercase())
                        .ok_or_else(|| ExecutorError::ColumnNotFound {
                            column_name: assignment.column.clone(),
                            table_name: view_def.name.clone(),
                            searched_tables: vec![view_def.name.clone()],
                            available_columns: view_schema
                                .columns
                                .iter()
                                .map(|c| c.name.clone())
                                .collect(),
                        })?;

                    // Evaluate the new value
                    let new_value = evaluator.eval(&assignment.value, &old_row)?;
                    new_row_values[col_idx] = new_value;
                }

                let new_row = Row::new(new_row_values);
                collected_updates.push((old_row, new_row));
            }
        }
        collected_updates
    }; // evaluator dropped here

    // Now fire triggers (database can be mutably borrowed)
    let rows_processed = updates.len();
    for (old_row, new_row) in updates {
        for trigger in &triggers {
            crate::TriggerFirer::execute_trigger(
                database,
                trigger,
                Some(&old_row),
                Some(&new_row),
            )?;
        }
    }

    Ok(rows_processed)
}

/// Build a pseudo TableSchema from a view definition
pub(super) fn build_view_schema(
    database: &Database,
    view_def: &ViewDefinition,
) -> Result<TableSchema, ExecutorError> {
    // Execute the view's SELECT query to get column names
    let select_executor = crate::SelectExecutor::new(database);
    let result = select_executor.execute_with_columns(&view_def.query)?;

    // Use explicit column names if provided, otherwise derive from SELECT
    let column_names: Vec<String> =
        if let Some(ref cols) = view_def.columns { cols.clone() } else { result.columns.clone() };

    // Build columns with a generic data type (we just need names for trigger binding)
    let columns: Vec<ColumnSchema> = column_names
        .into_iter()
        .map(|name| ColumnSchema::new(name, DataType::Varchar { max_length: None }, true))
        .collect();

    Ok(TableSchema::new(view_def.name.clone(), columns))
}
