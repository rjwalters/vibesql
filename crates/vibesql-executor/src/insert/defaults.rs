use crate::errors::ExecutorError;

/// Evaluate an INSERT expression to SqlValue
/// Supports literals, DEFAULT keyword, procedural variables, and trigger pseudo-variables (OLD/NEW)
#[allow(dead_code)]
pub fn evaluate_insert_expression(
    expr: &vibesql_ast::Expression,
    column: &vibesql_catalog::ColumnSchema,
    procedural_context: Option<&crate::procedural::ExecutionContext>,
) -> Result<vibesql_types::SqlValue, ExecutorError> {
    evaluate_insert_expression_with_trigger_context(
        expr,
        column,
        procedural_context,
        None,
        None,
        None,
    )
}

/// Evaluate an INSERT expression with trigger context support
/// This is used when executing INSERT statements inside trigger bodies
///
/// `cte_results` carries the enclosing INSERT statement's WITH-clause CTEs
/// (if any) so subqueries in VALUES rows can reference CTE names, matching
/// SQLite (issue #5359).
pub fn evaluate_insert_expression_with_trigger_context(
    expr: &vibesql_ast::Expression,
    column: &vibesql_catalog::ColumnSchema,
    procedural_context: Option<&crate::procedural::ExecutionContext>,
    trigger_context: Option<&crate::trigger_execution::TriggerContext>,
    database: Option<&vibesql_storage::Database>,
    cte_results: Option<&std::collections::HashMap<String, crate::select::cte::CteResult>>,
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
                let mut evaluator =
                    crate::ExpressionEvaluator::with_trigger_context(ctx.table_schema, db, ctx);
                if let Some(ctes) = cte_results {
                    evaluator = evaluator.with_cte_context(ctes);
                }
                evaluator.eval(expr, &dummy_row)
            } else if let Some(db) = database {
                // No trigger context, but we have database access - evaluate the expression
                // Create a minimal dummy schema for expression evaluation
                // INSERT value expressions don't reference columns from current row
                let dummy_schema =
                    vibesql_catalog::TableSchema::new("__insert_expr__".to_string(), vec![]);
                let dummy_row = vibesql_storage::Row::new(vec![]);
                let mut evaluator = crate::ExpressionEvaluator::with_database(&dummy_schema, db);
                if let Some(ctes) = cte_results {
                    evaluator = evaluator.with_cte_context(ctes);
                }
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
                        vibesql_types::SqlValue::Float(f) => Ok(vibesql_types::SqlValue::Float(-f)),
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
            Ok(vibesql_types::SqlValue::Varchar(arcstr::ArcStr::from(col_name)))
        }
        vibesql_ast::Expression::Function { name, args, .. } => {
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
                _ => {
                    // SQLite allows deterministic scalar functions in DEFAULT
                    // clauses (e.g. `DEFAULT(abs(1))`, table-16.3). The value is
                    // materialized at INSERT time, so the arguments must reduce
                    // to constants. Evaluate each argument as a DEFAULT
                    // sub-expression, then dispatch to the scalar function table.
                    let mut arg_values = Vec::with_capacity(args.len());
                    for arg in args {
                        arg_values.push(evaluate_default_expression(arg)?);
                    }
                    crate::evaluator::functions::eval_scalar_function_for_default(
                        name.display(),
                        &arg_values,
                    )
                    .map_err(|err| match err {
                        // A scalar function that does not exist surfaces as
                        // SQLite's "unknown function: name()" in this
                        // context rather than the generic "no such function".
                        ExecutorError::NoSuchFunction { function_name } => {
                            ExecutorError::UnknownFunction {
                                function_name: function_name.to_lowercase(),
                            }
                        }
                        other => other,
                    })
                }
            }
        }
        // Aggregate functions (COUNT, AVG, MIN, MAX, STRING_AGG, ...) are not
        // valid in DEFAULT expressions. SQLite reports these as unknown
        // functions when the default is materialized at INSERT time
        // (table-16.2 max, 16.4 avg, 16.5 count, 16.6/16.7 string_agg).
        vibesql_ast::Expression::AggregateFunction { name, .. } => {
            Err(ExecutorError::UnknownFunction { function_name: name.canonical().to_string() })
        }
        // The parser represents bare `DEFAULT CURRENT_DATE` /
        // `CURRENT_TIME` / `CURRENT_TIMESTAMP` as dedicated AST variants
        // (not `Function`), so evaluate them here with the same clock
        // semantics as the `Function`-named forms above. This is the
        // path the Raft freeze pass relies on to materialize a
        // deterministic value at propose time (#5381).
        vibesql_ast::Expression::CurrentDate => {
            use chrono::Datelike;
            let now = chrono::Local::now();
            let date = vibesql_types::Date::new(now.year(), now.month() as u8, now.day() as u8)
                .map_err(|e| {
                    ExecutorError::UnsupportedFeature(format!("Failed to create date: {}", e))
                })?;
            Ok(vibesql_types::SqlValue::Date(date))
        }
        vibesql_ast::Expression::CurrentTime { .. } => {
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
        vibesql_ast::Expression::CurrentTimestamp { .. } => {
            use chrono::{Datelike, Timelike};
            let now = chrono::Local::now();
            let time_naive = now.time();
            let date = vibesql_types::Date::new(now.year(), now.month() as u8, now.day() as u8)
                .map_err(|e| {
                    ExecutorError::UnsupportedFeature(format!("Failed to create date: {}", e))
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
            Ok(vibesql_types::SqlValue::Timestamp(vibesql_types::Timestamp::new(date, time)))
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
    // No column was explicitly assigned by the caller, so every NULL slot is an
    // omitted column that should receive its default.
    apply_default_values_with_batch_context(
        schema,
        row_values,
        database,
        storage_table_name,
        None,
        &std::collections::HashSet::new(),
    )
}

/// Apply DEFAULT values for unspecified columns, with batch context for multi-row inserts
///
/// The `batch_max_ipk` parameter is used to track the maximum INTEGER PRIMARY KEY value
/// already assigned within the current batch of inserts. This prevents duplicate values
/// when multiple rows in the same INSERT have NULL for the INTEGER PRIMARY KEY column.
///
/// `explicitly_assigned` holds the schema column indices that the caller supplied a
/// value for in this INSERT (including an explicit `NULL`). A default is NOT substituted
/// for those columns: an explicit `NULL` must survive so the NOT NULL constraint check
/// can fire, rather than being silently swapped for the column default (#5893). Columns
/// omitted from the INSERT are absent from this set and still receive their default.
///
/// Returns the first auto-generated sequence value (for LAST_INSERT_ROWID support),
/// or None if no sequence values were generated.
pub fn apply_default_values_with_batch_context(
    schema: &vibesql_catalog::TableSchema,
    row_values: &mut [vibesql_types::SqlValue],
    database: &mut vibesql_storage::Database,
    storage_table_name: &str,
    batch_max_ipk: Option<i64>,
    explicitly_assigned: &std::collections::HashSet<usize>,
) -> Result<Option<i64>, ExecutorError> {
    let mut first_generated_id: Option<i64> = None;

    // Handle INTEGER PRIMARY KEY NULL auto-generation (SQLite semantics)
    // This must happen before regular default value processing
    if let Some(ipk_idx) = schema.get_integer_primary_key_index() {
        if row_values[ipk_idx] == vibesql_types::SqlValue::Null {
            // Auto-generate: max(existing_pk, batch_max) + 1, or 1 if table is empty
            let table_max = compute_next_integer_pk_value(database, storage_table_name)?;
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
        // A column the caller explicitly assigned (even to NULL) keeps its
        // supplied value: an explicit NULL must reach the NOT NULL constraint
        // check rather than being silently replaced by the column default
        // (#5893). Only columns omitted from the INSERT get their default.
        if explicitly_assigned.contains(&col_idx) {
            continue;
        }
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

/// Compute the next INTEGER PRIMARY KEY value for auto-generation.
///
/// An `INTEGER PRIMARY KEY` column is an alias for the rowid, so this simply
/// delegates to the storage-layer rowid allocator ([`Table::allocate_rowid`]),
/// which is the single source of truth for both plain-rowid and IPK inserts
/// (issue #5894). That matches sqlite3 exactly and, unlike the old O(n) column
/// scan, correctly handles:
///
/// - negative existing IPKs (max `-5` allocates `-4`, not `1`), and
/// - saturation at `i64::MAX` (random unused rowid, then `SQLITE_FULL` — never
///   a debug panic or a release wrap to `i64::MIN`).
///
/// Returns `1` when the table does not yet exist in storage.
fn compute_next_integer_pk_value(
    database: &vibesql_storage::Database,
    table_name: &str,
) -> Result<i64, ExecutorError> {
    match database.get_table(table_name) {
        // Table doesn't exist in storage yet, start at 1.
        None => Ok(1),
        Some(table) => {
            table.allocate_rowid().map_err(|e| ExecutorError::StorageError(e.to_string()))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use vibesql_ast::{Expression, FunctionIdentifier};
    use vibesql_types::SqlValue;

    fn int_lit(v: i64) -> Expression {
        Expression::Literal(SqlValue::Integer(v))
    }

    #[test]
    fn deterministic_scalar_function_evaluates_in_default() {
        // table-16.3: DEFAULT(abs(1)) -> 1
        let expr = Expression::Function {
            name: FunctionIdentifier::new("abs"),
            args: vec![int_lit(-1)],
            character_unit: None,
        };
        let result = evaluate_default_expression(&expr).expect("abs(-1) should evaluate");
        assert_eq!(result, SqlValue::Integer(1));
    }

    #[test]
    fn aggregate_function_rejected_as_unknown_in_default() {
        // table-16.4 / 16.5 / 16.6: aggregates surface as "unknown function: name()".
        for name in ["avg", "count", "max", "string_agg"] {
            let expr = Expression::AggregateFunction {
                name: FunctionIdentifier::new(name),
                distinct: false,
                args: vec![int_lit(1)],
                order_by: None,
                filter: None,
            };
            let err =
                evaluate_default_expression(&expr).expect_err("aggregate in DEFAULT should error");
            assert_eq!(err.to_string(), format!("unknown function: {}()", name));
        }
    }

    #[test]
    fn unknown_scalar_function_reports_unknown_function() {
        let expr = Expression::Function {
            name: FunctionIdentifier::new("definitely_not_a_function"),
            args: vec![],
            character_unit: None,
        };
        let err = evaluate_default_expression(&expr).expect_err("unknown function should error");
        assert_eq!(err.to_string(), "unknown function: definitely_not_a_function()");
    }
}

/// Apply generated/computed column values
/// Generated columns are defined with AS(expression) syntax and computed on INSERT/UPDATE
pub fn apply_generated_columns(
    schema: &vibesql_catalog::TableSchema,
    row_values: &mut [vibesql_types::SqlValue],
    _database: &vibesql_storage::Database,
) -> Result<(), ExecutorError> {
    // Create a temporary row to evaluate generated expressions.
    // GeneratedColumn context: non-deterministic date/time uses (e.g.
    // `z AS (date())`) are rejected at evaluation time (SQLite, date2-140).
    let temp_row = vibesql_storage::Row::new(row_values.to_vec());
    let evaluator = crate::ExpressionEvaluator::new(schema)
        .with_schema_context(crate::evaluator::SchemaExprContext::GeneratedColumn);

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
