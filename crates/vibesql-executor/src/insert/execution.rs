use crate::{errors::ExecutorError, privilege_checker::PrivilegeChecker};

/// Execute an INSERT statement
/// Returns number of rows inserted
pub fn execute_insert(
    db: &mut vibesql_storage::Database,
    stmt: &vibesql_ast::InsertStmt,
) -> Result<usize, ExecutorError> {
    execute_insert_internal(db, stmt, None, None)
}

/// Execute an INSERT statement with procedural context
/// Returns number of rows inserted
pub fn execute_insert_with_procedural_context(
    db: &mut vibesql_storage::Database,
    stmt: &vibesql_ast::InsertStmt,
    procedural_context: &crate::procedural::ExecutionContext,
) -> Result<usize, ExecutorError> {
    execute_insert_internal(db, stmt, Some(procedural_context), None)
}

/// Execute an INSERT statement with trigger context
/// This allows INSERT statements within trigger bodies to reference OLD/NEW pseudo-variables
/// Returns number of rows inserted
pub fn execute_insert_with_trigger_context(
    db: &mut vibesql_storage::Database,
    stmt: &vibesql_ast::InsertStmt,
    trigger_context: &crate::trigger_execution::TriggerContext,
) -> Result<usize, ExecutorError> {
    execute_insert_internal(db, stmt, None, Some(trigger_context))
}

/// Internal implementation of INSERT execution
fn execute_insert_internal(
    db: &mut vibesql_storage::Database,
    stmt: &vibesql_ast::InsertStmt,
    procedural_context: Option<&crate::procedural::ExecutionContext>,
    trigger_context: Option<&crate::trigger_execution::TriggerContext>,
) -> Result<usize, ExecutorError> {
    // Check INSERT privilege on the table
    PrivilegeChecker::check_insert(db, &stmt.table_name)?;

    // Get table schema from catalog (clone to avoid borrow issues)
    let schema = db
        .catalog
        .get_table(&stmt.table_name)
        .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?
        .clone();

    // Determine target column indices and types
    let target_column_info =
        super::validation::resolve_target_columns(&schema, &stmt.table_name, &stmt.columns)?;

    // Get the rows to insert based on the source
    let rows_to_insert = match &stmt.source {
        vibesql_ast::InsertSource::Values(values) => {
            // For VALUES, we already have the rows as expressions
            values.clone()
        }
        vibesql_ast::InsertSource::Select(select_stmt) => {
            // Try bulk transfer optimization first (Phase 1-3)
            // This provides 10-50x performance improvement for compatible schemas
            if stmt.columns.is_empty() {
                // Only attempt bulk transfer for INSERT INTO table SELECT (no column list)
                if let Some(count) =
                    super::bulk_transfer::try_bulk_transfer(db, &stmt.table_name, select_stmt)?
                {
                    // Fast path succeeded, return early
                    return Ok(count);
                }
            }

            // Fall back to normal path: execute SELECT and convert to expressions
            let select_executor = crate::SelectExecutor::new(db);
            let select_result = select_executor.execute_with_columns(select_stmt)?;

            // Validate column count
            if select_result.columns.len() != target_column_info.len() {
                return Err(ExecutorError::UnsupportedExpression(format!(
                    "INSERT column count mismatch: expected {}, got {} from SELECT",
                    target_column_info.len(),
                    select_result.columns.len()
                )));
            }

            // Convert SelectResult to Vec<Vec<Expression>> format
            // Each row becomes a Vec<Expression> with literals
            select_result
                .rows
                .into_iter()
                .map(|row| row.values.into_iter().map(vibesql_ast::Expression::Literal).collect())
                .collect()
        }
    };

    // Validate each row has correct number of values
    super::validation::validate_row_column_counts(&rows_to_insert, target_column_info.len())?;

    // For multi-row INSERT, validate all rows first, then insert all
    // This ensures atomicity: all rows succeed or all fail
    let mut validated_rows = Vec::new();
    let mut primary_key_values: Vec<Vec<vibesql_types::SqlValue>> = Vec::new(); // Track PK values for duplicate checking within batch
    let mut unique_constraint_values = if schema.get_unique_constraint_indices().is_empty() {
        Vec::new()
    } else {
        vec![Vec::new(); schema.get_unique_constraint_indices().len()]
    }; // Track UNIQUE values for each constraint

    // Track the first auto-generated ID for LAST_INSERT_ROWID() support
    // Per MySQL semantics, for multi-row inserts, LAST_INSERT_ID() returns
    // the first auto-generated value, not the last
    let mut first_generated_id: Option<i64> = None;

    for value_exprs in &rows_to_insert {
        // Build a complete row with values for all columns
        // Start with NULL for all columns, then fill in provided values
        let mut full_row_values = vec![vibesql_types::SqlValue::Null; schema.columns.len()];

        for (expr, (col_idx, data_type)) in value_exprs.iter().zip(target_column_info.iter()) {
            // Evaluate expression (literals, DEFAULT, procedural variables, and trigger pseudo-variables)
            let value = super::defaults::evaluate_insert_expression_with_trigger_context(
                expr,
                &schema.columns[*col_idx],
                procedural_context,
                trigger_context,
                Some(db),
            )?;

            // Type check and coerce: ensure value matches column type
            let coerced_value = super::validation::coerce_value(value, data_type)?;

            full_row_values[*col_idx] = coerced_value;
        }

        // Apply DEFAULT values for unspecified columns
        // This returns the first generated sequence value (if any)
        let generated_id =
            super::defaults::apply_default_values(&schema, &mut full_row_values, db)?;

        // Track the first generated ID across all rows
        if first_generated_id.is_none() {
            first_generated_id = generated_id;
        }

        // Validate all constraints in a single pass and extract index keys
        // Skip PK/UNIQUE duplicate checks if using REPLACE conflict clause or ON DUPLICATE KEY UPDATE
        let skip_duplicate_checks =
            matches!(stmt.conflict_clause, Some(vibesql_ast::ConflictClause::Replace))
                || stmt.on_duplicate_key_update.is_some();
        let validator = super::row_validator::RowValidator::new(
            db,
            &schema,
            &stmt.table_name,
            &primary_key_values,
            &unique_constraint_values,
            skip_duplicate_checks,
        );
        let validation_result = validator.validate(&full_row_values)?;

        // Track PK values for batch duplicate checking (using pre-extracted keys)
        if let Some(pk_values) = validation_result.primary_key {
            primary_key_values.push(pk_values);
        }

        // Track UNIQUE values for batch duplicate checking (using pre-extracted keys)
        for (constraint_idx, unique_values) in validation_result.unique_keys.into_iter().enumerate()
        {
            if let Some(values) = unique_values {
                unique_constraint_values[constraint_idx].push(values);
            }
        }

        // Store validated row for insertion
        validated_rows.push(full_row_values);
    }

    // All rows validated successfully, now insert them

    // Fire BEFORE STATEMENT triggers (unless we're already inside a trigger context)
    if trigger_context.is_none() {
        crate::TriggerFirer::execute_before_statement_triggers(
            db,
            &stmt.table_name,
            vibesql_ast::TriggerEvent::Insert,
        )?;
    }

    let mut rows_inserted = 0;

    // Check if we can use batch insert optimization
    // Batch insert is faster but can only be used when:
    // 1. No ON DUPLICATE KEY UPDATE clause
    // 2. No REPLACE conflict clause
    // 3. No INSERT triggers on the table (BEFORE or AFTER)
    let has_insert_triggers = db
        .catalog
        .get_triggers_for_table(&stmt.table_name, Some(vibesql_ast::TriggerEvent::Insert))
        .next()
        .is_some();

    let use_batch_insert = stmt.on_duplicate_key_update.is_none()
        && !matches!(stmt.conflict_clause, Some(vibesql_ast::ConflictClause::Replace))
        && !has_insert_triggers;

    if use_batch_insert && validated_rows.len() > 1 {
        // Fast path: Use batch insert for multiple rows without triggers
        let rows: Vec<vibesql_storage::Row> =
            validated_rows.into_iter().map(vibesql_storage::Row::new).collect();

        rows_inserted = db
            .insert_rows_batch(&stmt.table_name, rows)
            .map_err(|e| ExecutorError::UnsupportedExpression(format!("Storage error: {}", e)))?;
    } else {
        // Slow path: Insert rows one by one (needed for triggers, special clauses)
        for full_row_values in validated_rows {
            // Check if ON DUPLICATE KEY UPDATE is specified
            if let Some(ref assignments) = stmt.on_duplicate_key_update {
                // Try to update an existing row if there's a conflict
                let update_result = super::duplicate_key_update::handle_duplicate_key_update(
                    db,
                    &stmt.table_name,
                    &schema,
                    &full_row_values,
                    assignments,
                )?;

                if update_result.is_some() {
                    // Row was updated, count it
                    rows_inserted += 1;
                    continue;
                }
                // No conflict, fall through to insert
            } else if matches!(stmt.conflict_clause, Some(vibesql_ast::ConflictClause::Replace)) {
                // If REPLACE conflict clause, delete conflicting rows first
                super::replace::handle_replace_conflicts(
                    db,
                    &stmt.table_name,
                    &schema,
                    &full_row_values,
                )?;
            }

            // Fire BEFORE INSERT triggers
            let row_to_insert = vibesql_storage::Row::new(full_row_values.clone());
            crate::TriggerFirer::execute_before_triggers(
                db,
                &stmt.table_name,
                vibesql_ast::TriggerEvent::Insert,
                None,
                Some(&row_to_insert),
            )?;

            // Get row count before insert to enable rollback
            let row_count_before = db
                .get_table(&stmt.table_name)
                .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?
                .row_count();

            // Insert the row
            let row = vibesql_storage::Row::new(full_row_values);
            db.insert_row(&stmt.table_name, row.clone()).map_err(|e| {
                ExecutorError::UnsupportedExpression(format!("Storage error: {}", e))
            })?;

            // Fire AFTER INSERT triggers
            // If AFTER triggers fail, we need to rollback the insert
            let trigger_result = crate::TriggerFirer::execute_after_triggers(
                db,
                &stmt.table_name,
                vibesql_ast::TriggerEvent::Insert,
                None,
                Some(&row),
            );

            if let Err(trigger_error) = trigger_result {
                // Rollback: Delete the row we just inserted
                // Note: This is a simple rollback mechanism for Phase 3
                // Full transaction support will come in a later phase
                let table = db
                    .get_table_mut(&stmt.table_name)
                    .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?;

                // Delete the last row (the one we just inserted)
                // Row was inserted at index row_count_before
                use std::cell::Cell;
                let current_index = Cell::new(0);
                let target_index = row_count_before;
                table.delete_where(|_row| {
                    let index = current_index.get();
                    current_index.set(index + 1);
                    index == target_index
                });

                // Rebuild indexes since we modified the table
                db.rebuild_indexes(&stmt.table_name);

                // Re-throw the trigger error
                return Err(trigger_error);
            }

            rows_inserted += 1;
        }
    }

    // Fire AFTER STATEMENT triggers (unless we're already inside a trigger context)
    if trigger_context.is_none() {
        crate::TriggerFirer::execute_after_statement_triggers(
            db,
            &stmt.table_name,
            vibesql_ast::TriggerEvent::Insert,
        )?;
    }

    // Update LAST_INSERT_ROWID if any auto-generated values were produced
    if let Some(id) = first_generated_id {
        db.set_last_insert_rowid(id);
    }

    Ok(rows_inserted)
}
