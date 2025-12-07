use crate::errors::ExecutorError;

/// Handle ON DUPLICATE KEY UPDATE logic: detect conflicts and update existing rows
/// Returns Ok(Some(row_id)) if an update occurred, Ok(None) if no conflict
pub fn handle_duplicate_key_update(
    db: &mut vibesql_storage::Database,
    table_name: &str,
    schema: &vibesql_catalog::TableSchema,
    row_values: &[vibesql_types::SqlValue],
    assignments: &[vibesql_ast::Assignment],
) -> Result<Option<usize>, ExecutorError> {
    // Find the conflicting row (if any)
    let conflicting_row_id = find_conflicting_row(db, table_name, schema, row_values)?;

    if let Some(row_id) = conflicting_row_id {
        // A conflict exists - update the row
        update_conflicting_row(db, table_name, schema, row_id, row_values, assignments)?;

        // Invalidate the database-level columnar cache since table data changed.
        // Note: The table-level cache is already invalidated by update_row().
        // Both invalidations are necessary because they manage separate caches:
        // - Table-level cache: used by Table::scan_columnar() for SIMD filtering
        // - Database-level cache: used by Database::get_columnar() for cached access
        db.invalidate_columnar_cache(table_name);

        Ok(Some(row_id))
    } else {
        // No conflict - caller should do a normal insert
        Ok(None)
    }
}

/// Find a row that conflicts with the given values based on PRIMARY KEY or UNIQUE constraints
/// Returns Some(row_id) if a conflict exists, None otherwise
fn find_conflicting_row(
    db: &vibesql_storage::Database,
    table_name: &str,
    schema: &vibesql_catalog::TableSchema,
    row_values: &[vibesql_types::SqlValue],
) -> Result<Option<usize>, ExecutorError> {
    let table = db
        .get_table(table_name)
        .ok_or_else(|| ExecutorError::TableNotFound(table_name.to_string()))?;

    let pk_indices = schema.get_primary_key_indices();
    let unique_constraint_indices = schema.get_unique_constraint_indices();

    // Check PRIMARY KEY conflict first
    if let Some(ref pk_idx) = pk_indices {
        let pk_values: Vec<vibesql_types::SqlValue> =
            pk_idx.iter().map(|&idx| row_values[idx].clone()).collect();

        // Search for a row with matching PRIMARY KEY
        // Use scan_live() to skip deleted rows and get correct physical indices
        for (row_id, row) in table.scan_live() {
            let row_pk_values: Vec<vibesql_types::SqlValue> =
                pk_idx.iter().map(|&idx| row.values[idx].clone()).collect();
            if row_pk_values == pk_values {
                return Ok(Some(row_id));
            }
        }
    }

    // Check UNIQUE constraints
    for unique_indices in unique_constraint_indices.iter() {
        let unique_values: Vec<vibesql_types::SqlValue> =
            unique_indices.iter().map(|&idx| row_values[idx].clone()).collect();

        // Skip if contains NULL (NULLs don't cause conflicts in UNIQUE constraints)
        if unique_values.contains(&vibesql_types::SqlValue::Null) {
            continue;
        }

        // Search for a row with matching UNIQUE constraint values
        // Use scan_live() to skip deleted rows and get correct physical indices
        for (row_id, row) in table.scan_live() {
            let row_unique_values: Vec<vibesql_types::SqlValue> =
                unique_indices.iter().map(|&idx| row.values[idx].clone()).collect();
            if row_unique_values == unique_values {
                return Ok(Some(row_id));
            }
        }
    }

    Ok(None)
}

/// Update a conflicting row with new values based on the assignments
/// The assignments can reference both existing column values and VALUES() for insert values
fn update_conflicting_row(
    db: &mut vibesql_storage::Database,
    table_name: &str,
    schema: &vibesql_catalog::TableSchema,
    row_id: usize,
    insert_values: &[vibesql_types::SqlValue],
    assignments: &[vibesql_ast::Assignment],
) -> Result<(), ExecutorError> {
    // Get the existing row
    let table = db
        .get_table(table_name)
        .ok_or_else(|| ExecutorError::TableNotFound(table_name.to_string()))?;

    let existing_row = table
        .scan()
        .get(row_id)
        .ok_or_else(|| ExecutorError::UnsupportedExpression("Row not found".to_string()))?;

    let mut new_row_values = existing_row.values.clone();

    // Apply each assignment
    for assignment in assignments {
        // Find the column index
        let column_idx =
            schema.columns.iter().position(|col| col.name == assignment.column).ok_or_else(
                || {
                    ExecutorError::UnsupportedExpression(format!(
                        "Column '{}' not found in table '{}'",
                        assignment.column, table_name
                    ))
                },
            )?;

        // Evaluate the expression in the context of the existing row and insert values
        let new_value = evaluate_duplicate_key_expression(
            &assignment.value,
            schema,
            &existing_row.values,
            insert_values,
        )?;

        new_row_values[column_idx] = new_value;
    }

    // Update the row in the table
    let table_mut = db
        .get_table_mut(table_name)
        .ok_or_else(|| ExecutorError::TableNotFound(table_name.to_string()))?;

    table_mut
        .update_row(row_id, vibesql_storage::Row::new(new_row_values))
        .map_err(|e| ExecutorError::UnsupportedExpression(format!("Storage error: {}", e)))?;

    Ok(())
}

/// Evaluate an expression in the context of ON DUPLICATE KEY UPDATE
/// - DuplicateKeyValue(column) returns the value from insert_values
/// - ColumnRef returns the value from existing_row_values
/// - Other expressions are evaluated normally
fn evaluate_duplicate_key_expression(
    expr: &vibesql_ast::Expression,
    schema: &vibesql_catalog::TableSchema,
    existing_row_values: &[vibesql_types::SqlValue],
    insert_values: &[vibesql_types::SqlValue],
) -> Result<vibesql_types::SqlValue, ExecutorError> {
    match expr {
        vibesql_ast::Expression::DuplicateKeyValue { column } => {
            // VALUES(column) - get the value from insert_values
            let column_idx =
                schema.columns.iter().position(|col| col.name == *column).ok_or_else(|| {
                    ExecutorError::UnsupportedExpression(format!(
                        "Column '{}' not found in VALUES() function",
                        column
                    ))
                })?;
            Ok(insert_values[column_idx].clone())
        }
        vibesql_ast::Expression::ColumnRef { table: _, column } => {
            // Column reference - get the value from existing row
            let column_idx =
                schema.columns.iter().position(|col| col.name == *column).ok_or_else(|| {
                    ExecutorError::UnsupportedExpression(format!("Column '{}' not found", column))
                })?;
            Ok(existing_row_values[column_idx].clone())
        }
        vibesql_ast::Expression::Literal(value) => Ok(value.clone()),
        vibesql_ast::Expression::BinaryOp { op, left, right } => {
            // Recursively evaluate left and right, then apply the operator
            let left_val = evaluate_duplicate_key_expression(
                left,
                schema,
                existing_row_values,
                insert_values,
            )?;
            let right_val = evaluate_duplicate_key_expression(
                right,
                schema,
                existing_row_values,
                insert_values,
            )?;

            // Simple binary operation evaluation for common cases
            match op {
                vibesql_ast::BinaryOperator::Plus => {
                    // Basic addition support
                    match (&left_val, &right_val) {
                        (vibesql_types::SqlValue::Integer(l), vibesql_types::SqlValue::Integer(r)) => {
                            Ok(vibesql_types::SqlValue::Integer(l + r))
                        }
                        _ => Err(ExecutorError::UnsupportedExpression(format!(
                            "Binary operation {:?} not yet fully supported in ON DUPLICATE KEY UPDATE for these types",
                            op
                        ))),
                    }
                }
                _ => Err(ExecutorError::UnsupportedExpression(format!(
                    "Binary operation {:?} not yet fully supported in ON DUPLICATE KEY UPDATE",
                    op
                ))),
            }
        }
        _ => {
            // For other expression types, we would need a full expression evaluator
            // For now, return an error for unsupported expressions
            Err(ExecutorError::UnsupportedExpression(format!(
                "Expression type not yet supported in ON DUPLICATE KEY UPDATE: {:?}",
                expr
            )))
        }
    }
}
