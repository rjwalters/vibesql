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
        vibesql_ast::Expression::ColumnRef(col_id)
            if col_id.schema_canonical().is_none() && col_id.table_canonical().is_none() =>
        {
            let col_name = col_id.column_canonical();
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
            // Pseudo-variables (OLD.x, NEW.y) require full expression evaluation with trigger
            // context
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
            // For any other expression type, use full expression evaluator
            if let (Some(ctx), Some(db)) = (trigger_context, database) {
                // Create a dummy row for evaluation (trigger context available)
                let dummy_row = vibesql_storage::Row::new(vec![]);
                let evaluator =
                    crate::ExpressionEvaluator::with_trigger_context(ctx.table_schema, db, ctx);
                evaluator.eval(expr, &dummy_row)
            } else if let Some(db) = database {
                // No trigger context, but we have database access - evaluate the expression
                // Create a minimal dummy schema for expression evaluation
                // INSERT value expressions don't reference columns from current row
                let dummy_schema =
                    vibesql_catalog::TableSchema::new("__insert_expr__".to_string(), vec![]);
                let dummy_row = vibesql_storage::Row::new(vec![]);
                let evaluator = crate::ExpressionEvaluator::with_database(&dummy_schema, db);
                evaluator.eval(expr, &dummy_row)
            } else {
                Err(ExecutorError::UnsupportedExpression(
                    "Complex expressions in INSERT VALUES require database context".to_string(),
                ))
            }
        }
    }
}

/// Evaluate a DEFAULT expression to get its value
/// Supports literals, special functions (CURRENT_DATE, CURRENT_USER, etc.), sequences,
/// and unary expressions (+val, -val) for numeric defaults
/// Note: For NextValue expressions, this function signature needs database access
/// This will require refactoring to pass db context
pub fn evaluate_default_expression(
    expr: &vibesql_ast::Expression,
) -> Result<vibesql_types::SqlValue, ExecutorError> {
    match expr {
        vibesql_ast::Expression::Literal(lit) => Ok(lit.clone()),
        // Support unary plus/minus for DEFAULT expressions like "default +4.32" or "default -5"
        vibesql_ast::Expression::UnaryOp { op, expr } => {
            let inner = evaluate_default_expression(expr)?;
            match op {
                vibesql_ast::UnaryOperator::Plus => Ok(inner), // +x = x
                vibesql_ast::UnaryOperator::Minus => {
                    // Negate the value
                    match inner {
                        vibesql_types::SqlValue::Integer(i) => {
                            Ok(vibesql_types::SqlValue::Integer(-i))
                        }
                        vibesql_types::SqlValue::Bigint(i) => {
                            Ok(vibesql_types::SqlValue::Bigint(-i))
                        }
                        vibesql_types::SqlValue::Smallint(i) => {
                            Ok(vibesql_types::SqlValue::Smallint(-i))
                        }
                        vibesql_types::SqlValue::Float(f) => {
                            Ok(vibesql_types::SqlValue::Float(-f))
                        }
                        vibesql_types::SqlValue::Real(f) => Ok(vibesql_types::SqlValue::Real(-f)),
                        vibesql_types::SqlValue::Double(f) => {
                            Ok(vibesql_types::SqlValue::Double(-f))
                        }
                        vibesql_types::SqlValue::Numeric(f) => {
                            Ok(vibesql_types::SqlValue::Numeric(-f))
                        }
                        _ => Err(ExecutorError::UnsupportedExpression(format!(
                            "Cannot apply unary minus to {:?}",
                            inner
                        ))),
                    }
                }
                _ => Err(ExecutorError::UnsupportedExpression(format!(
                    "Unary operator {:?} not supported in DEFAULT expressions",
                    op
                ))),
            }
        }
        vibesql_ast::Expression::NextValue { sequence_name } => {
            // NEXT VALUE FOR sequence - this should have been handled at a higher level
            // with access to the database. If we get here, it's an error.
            Err(ExecutorError::UnsupportedExpression(format!(
                "Sequence '{}' requires database context - this should have been handled earlier",
                sequence_name
            )))
        }
        // SQLite compatibility: unquoted identifiers in DEFAULT clauses are treated as string literals
        // e.g., CREATE TABLE t(x TEXT DEFAULT hello) treats 'hello' as the string "hello"
        vibesql_ast::Expression::ColumnRef(col_id)
            if col_id.schema_canonical().is_none() && col_id.table_canonical().is_none() =>
        {
            // Convert the column name to a string literal
            let col_name = col_id.column_canonical();
            Ok(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(
                col_name,
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
#[allow(dead_code)]
pub fn apply_default_values(
    schema: &vibesql_catalog::TableSchema,
    row_values: &mut [vibesql_types::SqlValue],
    database: &mut vibesql_storage::Database,
) -> Result<Option<i64>, ExecutorError> {
    apply_default_values_with_table_name(schema, row_values, database, &schema.name)
}

/// Apply DEFAULT values for unspecified columns, with explicit table name for storage lookup
///
/// This variant is used when the storage table name differs from the schema name
/// (e.g., for schema-qualified tables).
///
/// Returns the first auto-generated sequence value (for LAST_INSERT_ROWID support),
/// or None if no sequence values were generated.
pub fn apply_default_values_with_table_name(
    schema: &vibesql_catalog::TableSchema,
    row_values: &mut [vibesql_types::SqlValue],
    database: &mut vibesql_storage::Database,
    storage_table_name: &str,
) -> Result<Option<i64>, ExecutorError> {
    apply_default_values_with_batch_context(schema, row_values, database, storage_table_name, None)
}

/// Apply DEFAULT values for unspecified columns, with batch context for multi-row inserts
///
/// The `batch_max_ipk` parameter is used to track the maximum INTEGER PRIMARY KEY value
/// already assigned within the current batch of inserts. This prevents duplicate values
/// when multiple rows in the same INSERT have NULL for the INTEGER PRIMARY KEY column.
///
/// Returns the first auto-generated sequence value (for LAST_INSERT_ROWID support),
/// or None if no sequence values were generated.
pub fn apply_default_values_with_batch_context(
    schema: &vibesql_catalog::TableSchema,
    row_values: &mut [vibesql_types::SqlValue],
    database: &mut vibesql_storage::Database,
    storage_table_name: &str,
    batch_max_ipk: Option<i64>,
) -> Result<Option<i64>, ExecutorError> {
    let mut first_generated_id: Option<i64> = None;

    // Handle INTEGER PRIMARY KEY NULL auto-generation (SQLite semantics)
    // This must happen before regular default value processing
    if let Some(ipk_idx) = schema.get_integer_primary_key_index() {
        if row_values[ipk_idx] == vibesql_types::SqlValue::Null {
            // Auto-generate: max(existing_pk, batch_max) + 1, or 1 if table is empty
            let table_max = compute_next_integer_pk_value(database, storage_table_name, ipk_idx)?;
            // The next value should be max of (table_max, batch_max_ipk + 1)
            let next_val = match batch_max_ipk {
                Some(batch_max) => table_max.max(batch_max + 1),
                None => table_max,
            };
            row_values[ipk_idx] = vibesql_types::SqlValue::Integer(next_val);

            // Track for LAST_INSERT_ROWID
            if first_generated_id.is_none() {
                first_generated_id = Some(next_val);
            }
        }
    }

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

/// Compute the next INTEGER PRIMARY KEY value for auto-generation
///
/// Returns max(existing_pk_values) + 1, or 1 if the table is empty.
/// This implements SQLite's behavior where inserting NULL into an INTEGER PRIMARY KEY
/// column auto-generates the next available value.
fn compute_next_integer_pk_value(
    database: &vibesql_storage::Database,
    table_name: &str,
    pk_col_idx: usize,
) -> Result<i64, ExecutorError> {
    let table = match database.get_table(table_name) {
        Some(t) => t,
        None => {
            // Table doesn't exist in storage yet, start at 1
            return Ok(1);
        }
    };

    // Find the maximum value in the PRIMARY KEY column
    let mut max_val: i64 = 0;

    for row in table.scan() {
        if let Some(value) = row.get(pk_col_idx) {
            let int_val = match value {
                vibesql_types::SqlValue::Integer(i) => *i,
                vibesql_types::SqlValue::Bigint(i) => *i,
                vibesql_types::SqlValue::Null => continue, // Skip NULL values
                _ => continue, // Skip non-integer values (shouldn't happen)
            };
            if int_val > max_val {
                max_val = int_val;
            }
        }
    }

    // Return max + 1 (or 1 if table was empty, since max_val starts at 0)
    Ok(max_val + 1)
}

/// Apply generated/computed column values
/// Generated columns are defined with AS(expression) syntax and computed on INSERT/UPDATE
pub fn apply_generated_columns(
    schema: &vibesql_catalog::TableSchema,
    row_values: &mut [vibesql_types::SqlValue],
    _database: &vibesql_storage::Database,
) -> Result<(), ExecutorError> {
    // Create a temporary row to evaluate generated expressions
    let temp_row = vibesql_storage::Row::new(row_values.to_vec());
    let evaluator = crate::ExpressionEvaluator::new(schema);

    for (col_idx, col) in schema.columns.iter().enumerate() {
        // If column has a generated expression, compute and apply it
        if let Some(generated_expr) = &col.generated_expr {
            let generated_value = evaluator.eval(generated_expr, &temp_row)?;
            let coerced_value = super::validation::coerce_value(generated_value, &col.data_type)?;
            row_values[col_idx] = coerced_value;
        }
    }
    Ok(())
}
