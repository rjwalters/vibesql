use crate::errors::ExecutorError;

/// Evaluate an INSERT expression to SqlValue
/// Supports literals, DEFAULT keyword, procedural variables, and trigger pseudo-variables (OLD/NEW)
#[allow(dead_code)]
pub fn evaluate_insert_expression(
    expr: &vibesql_ast::Expression,
    column: &vibesql_catalog::ColumnSchema,
    procedural_context: Option<&crate::procedural::ExecutionContext>,
) -> Result<vibesql_types::SqlValue, ExecutorError> {
    evaluate_insert_expression_with_trigger_context(expr, column, procedural_context, None, None)
}

/// Evaluate an INSERT expression with trigger context support
/// This is used when executing INSERT statements inside trigger bodies
pub fn evaluate_insert_expression_with_trigger_context(
    expr: &vibesql_ast::Expression,
    column: &vibesql_catalog::ColumnSchema,
    procedural_context: Option<&crate::procedural::ExecutionContext>,
    trigger_context: Option<&crate::trigger_execution::TriggerContext>,
    database: Option<&vibesql_storage::Database>,
) -> Result<vibesql_types::SqlValue, ExecutorError> {
    match expr {
        vibesql_ast::Expression::Literal(lit) => Ok(lit.clone()),
        vibesql_ast::Expression::Default => {
            // Use column's default value, or NULL if no default is defined
            if let Some(default_expr) = &column.default_value {
                // Evaluate the default expression
                evaluate_default_expression(default_expr)
            } else {
                // No default value defined, use NULL
                Ok(vibesql_types::SqlValue::Null)
            }
        }
        vibesql_ast::Expression::ColumnRef { table: None, column: col_name } => {
            // Check if this is a procedural variable reference
            if let Some(ctx) = procedural_context {
                // Try to resolve as procedural variable
                if let Some(value) = ctx.get_value(col_name) {
                    return Ok(value.clone());
                }
            }
            // Not a procedural variable, or no context provided
            Err(ExecutorError::UnsupportedExpression(format!(
                "Column reference '{}' not supported in INSERT VALUES. Did you mean to use a procedural variable?",
                col_name
            )))
        }
        vibesql_ast::Expression::PseudoVariable { .. } => {
            // Pseudo-variables (OLD.x, NEW.y) require full expression evaluation with trigger context
            if let (Some(ctx), Some(db)) = (trigger_context, database) {
                // Create a dummy row for evaluation (pseudo-variables don't depend on current row)
                let dummy_row = vibesql_storage::Row::new(vec![]);
                let evaluator =
                    crate::ExpressionEvaluator::with_trigger_context(ctx.table_schema, db, ctx);
                evaluator.eval(expr, &dummy_row)
            } else {
                Err(ExecutorError::UnsupportedExpression(
                    "Pseudo-variables (OLD/NEW) are only valid within trigger bodies".to_string(),
                ))
            }
        }
        _ => {
            // For any other expression type, use full expression evaluator if trigger context available
            if let (Some(ctx), Some(db)) = (trigger_context, database) {
                // Create a dummy row for evaluation
                let dummy_row = vibesql_storage::Row::new(vec![]);
                let evaluator =
                    crate::ExpressionEvaluator::with_trigger_context(ctx.table_schema, db, ctx);
                evaluator.eval(expr, &dummy_row)
            } else {
                Err(ExecutorError::UnsupportedExpression(
                    "Complex expressions in INSERT VALUES are only supported within trigger bodies"
                        .to_string(),
                ))
            }
        }
    }
}

/// Evaluate a DEFAULT expression to get its value
/// Supports literals, special functions (CURRENT_DATE, CURRENT_USER, etc.), and sequences
/// Note: For NextValue expressions, this function signature needs database access
/// This will require refactoring to pass db context
pub fn evaluate_default_expression(
    expr: &vibesql_ast::Expression,
) -> Result<vibesql_types::SqlValue, ExecutorError> {
    match expr {
        vibesql_ast::Expression::Literal(lit) => Ok(lit.clone()),
        vibesql_ast::Expression::NextValue { sequence_name } => {
            // NEXT VALUE FOR sequence - this should have been handled at a higher level
            // with access to the database. If we get here, it's an error.
            Err(ExecutorError::UnsupportedExpression(format!(
                "Sequence '{}' requires database context - this should have been handled earlier",
                sequence_name
            )))
        }
        vibesql_ast::Expression::Function { name, .. } => {
            // Evaluate special SQL functions that can be used in DEFAULT
            match name.to_uppercase().as_str() {
                "CURRENT_DATE" => {
                    use chrono::Datelike;
                    let now = chrono::Local::now();
                    let date =
                        vibesql_types::Date::new(now.year(), now.month() as u8, now.day() as u8)
                            .map_err(|e| {
                                ExecutorError::UnsupportedFeature(format!(
                                    "Failed to create date: {}",
                                    e
                                ))
                            })?;
                    Ok(vibesql_types::SqlValue::Date(date))
                }
                "CURRENT_TIME" => {
                    use chrono::Timelike;
                    let now = chrono::Local::now();
                    let time_naive = now.time();
                    let time = vibesql_types::Time::new(
                        time_naive.hour() as u8,
                        time_naive.minute() as u8,
                        time_naive.second() as u8,
                        time_naive.nanosecond(),
                    )
                    .map_err(|e| {
                        ExecutorError::UnsupportedFeature(format!("Failed to create time: {}", e))
                    })?;
                    Ok(vibesql_types::SqlValue::Time(time))
                }
                "CURRENT_TIMESTAMP" => {
                    use chrono::{Datelike, Timelike};
                    let now = chrono::Local::now();
                    let time_naive = now.time();
                    let date =
                        vibesql_types::Date::new(now.year(), now.month() as u8, now.day() as u8)
                            .map_err(|e| {
                                ExecutorError::UnsupportedFeature(format!(
                                    "Failed to create date: {}",
                                    e
                                ))
                            })?;
                    let time = vibesql_types::Time::new(
                        time_naive.hour() as u8,
                        time_naive.minute() as u8,
                        time_naive.second() as u8,
                        time_naive.nanosecond(),
                    )
                    .map_err(|e| {
                        ExecutorError::UnsupportedFeature(format!("Failed to create time: {}", e))
                    })?;
                    Ok(vibesql_types::SqlValue::Timestamp(vibesql_types::Timestamp::new(
                        date, time,
                    )))
                }
                "CURRENT_USER" | "USER" | "SESSION_USER" => {
                    // Return current user (placeholder - would come from session context)
                    Ok(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("public")))
                }
                "CURRENT_ROLE" => {
                    // Return current role (placeholder - would come from session context)
                    Ok(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from("public")))
                }
                _ => Err(ExecutorError::UnsupportedExpression(format!(
                    "Function '{}' not supported in DEFAULT expressions",
                    name
                ))),
            }
        }
        _ => Err(ExecutorError::UnsupportedExpression(
            "Only literals and functions are supported in DEFAULT expressions".to_string(),
        )),
    }
}

/// Apply DEFAULT values for unspecified columns
/// Now accepts database parameter to handle sequence NextValue expressions
///
/// Returns the first auto-generated sequence value (for LAST_INSERT_ROWID support),
/// or None if no sequence values were generated.
pub fn apply_default_values(
    schema: &vibesql_catalog::TableSchema,
    row_values: &mut [vibesql_types::SqlValue],
    database: &mut vibesql_storage::Database,
) -> Result<Option<i64>, ExecutorError> {
    let mut first_generated_id: Option<i64> = None;

    for (col_idx, col) in schema.columns.iter().enumerate() {
        // If column is NULL and has a default value, apply it
        if row_values[col_idx] == vibesql_types::SqlValue::Null {
            if let Some(default_expr) = &col.default_value {
                // Handle NextValue expressions specially
                let default_value = match default_expr {
                    vibesql_ast::Expression::NextValue { sequence_name } => {
                        // Get the next value from the sequence
                        let seq =
                            database.catalog.get_sequence_mut(sequence_name).map_err(|e| {
                                ExecutorError::UnsupportedExpression(format!(
                                    "Sequence error: {:?}",
                                    e
                                ))
                            })?;
                        let next_val = seq.next_value().map_err(|e| {
                            ExecutorError::ConstraintViolation(format!("Sequence error: {}", e))
                        })?;

                        // Track the first generated ID for LAST_INSERT_ROWID
                        if first_generated_id.is_none() {
                            first_generated_id = Some(next_val);
                        }

                        vibesql_types::SqlValue::Integer(next_val)
                    }
                    _ => evaluate_default_expression(default_expr)?,
                };
                let coerced_value = super::validation::coerce_value(default_value, &col.data_type)?;
                row_values[col_idx] = coerced_value;
            }
        }
    }
    Ok(first_generated_id)
}
