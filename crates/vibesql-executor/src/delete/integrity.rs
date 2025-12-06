//! Foreign key integrity checking and enforcement for DELETE operations

use crate::errors::ExecutorError;
use vibesql_catalog::ReferentialAction;
use vibesql_types::SqlValue;

/// Handle referential integrity for a row being deleted.
///
/// This function enforces referential integrity constraints by handling foreign key
/// actions (CASCADE, SET NULL, SET DEFAULT, NO ACTION) when a parent row is deleted.
///
/// # Arguments
///
/// * `db` - The database (mutable for CASCADE, SET NULL, SET DEFAULT)
/// * `parent_table_name` - Name of the table containing the row to delete
/// * `parent_row` - The row being deleted
///
/// # Returns
///
/// * `Ok(())` if integrity is maintained
/// * `Err(ExecutorError::ConstraintViolation)` if NO ACTION fails due to child references
pub fn check_no_child_references(
    db: &mut vibesql_storage::Database,
    parent_table_name: &str,
    parent_row: &vibesql_storage::Row,
) -> Result<(), ExecutorError> {
    let parent_schema = db
        .catalog
        .get_table(parent_table_name)
        .ok_or_else(|| ExecutorError::TableNotFound(parent_table_name.to_string()))?;

    // This check is only meaningful if the parent table has a primary key.
    let pk_indices = match parent_schema.get_primary_key_indices() {
        Some(indices) => indices,
        None => return Ok(()),
    };

    let parent_key_values: Vec<SqlValue> =
        pk_indices.iter().map(|&idx| parent_row.values[idx].clone()).collect();

    // Optimization: Check if any table in the database has foreign keys at all
    let has_any_fks = db.catalog.list_tables().iter().any(|table_name| {
        db.catalog
            .get_table(table_name)
            .map(|schema| !schema.foreign_keys.is_empty())
            .unwrap_or(false)
    });

    if !has_any_fks {
        return Ok(());
    }

    // Collect all child tables that reference this parent row
    // We need to collect first to avoid borrowing issues when we mutate
    let mut actions_to_perform: Vec<(
        String,
        vibesql_catalog::ForeignKeyConstraint,
        ReferentialAction,
    )> = Vec::new();

    for table_name in db.catalog.list_tables() {
        let child_schema = db.catalog.get_table(&table_name).unwrap();

        if child_schema.foreign_keys.is_empty() {
            continue;
        }

        for fk in &child_schema.foreign_keys {
            if fk.parent_table != parent_table_name {
                continue;
            }

            // Check if any row in the child table references the parent row
            // Use scan_live() to skip soft-deleted rows
            let child_table = db.get_table(&table_name).unwrap();
            let has_references = child_table.scan_live().any(|(_, child_row)| {
                let child_fk_values: Vec<SqlValue> =
                    fk.column_indices.iter().map(|&idx| child_row.values[idx].clone()).collect();

                // Skip NULL foreign keys (NULLs don't participate in FK constraints)
                if child_fk_values.iter().any(|v| matches!(v, SqlValue::Null)) {
                    return false;
                }

                child_fk_values == parent_key_values
            });

            if has_references {
                actions_to_perform.push((table_name.clone(), fk.clone(), fk.on_delete.clone()));
            }
        }
    }

    // Now perform the actions
    for (child_table_name, fk, action) in actions_to_perform {
        match action {
            ReferentialAction::NoAction | ReferentialAction::Restrict => {
                // Fail with constraint violation
                return Err(ExecutorError::ConstraintViolation(format!(
                    "FOREIGN KEY constraint violation: cannot delete or update a parent row when a foreign key constraint exists. The conflict occurred in table \'{}\', constraint \'{}\'.",
                    child_table_name,
                    fk.name.as_deref().unwrap_or(""),
                )));
            }
            ReferentialAction::Cascade => {
                // Delete child rows that reference the parent
                cascade_delete(db, &child_table_name, &fk, &parent_key_values)?;
            }
            ReferentialAction::SetNull => {
                // Set child FK columns to NULL
                set_null(db, &child_table_name, &fk, &parent_key_values)?;
            }
            ReferentialAction::SetDefault => {
                // Set child FK columns to their default values
                set_default(db, &child_table_name, &fk, &parent_key_values)?;
            }
        }
    }

    Ok(())
}

/// Delete child rows that reference a deleted parent row (CASCADE action)
fn cascade_delete(
    db: &mut vibesql_storage::Database,
    child_table_name: &str,
    fk: &vibesql_catalog::ForeignKeyConstraint,
    parent_key_values: &[SqlValue],
) -> Result<(), ExecutorError> {
    // Find rows to delete (use scan_live to skip soft-deleted rows)
    let child_table = db.get_table(child_table_name).unwrap();
    let mut rows_to_delete: Vec<vibesql_storage::Row> = Vec::new();

    for (_, child_row) in child_table.scan_live() {
        let child_fk_values: Vec<SqlValue> =
            fk.column_indices.iter().map(|&idx| child_row.values[idx].clone()).collect();

        // Skip NULL foreign keys
        if child_fk_values.iter().any(|v| matches!(v, SqlValue::Null)) {
            continue;
        }

        if child_fk_values == parent_key_values {
            rows_to_delete.push(child_row.clone());
        }
    }

    // Recursively check integrity for each child row before deleting
    // This handles multi-level CASCADE (grandchildren, etc.)
    for child_row in &rows_to_delete {
        check_no_child_references(db, child_table_name, child_row)?;
    }

    // Now delete the child rows
    let child_table_mut = db.get_table_mut(child_table_name).unwrap();
    for row_to_delete in &rows_to_delete {
        // Ignore delete_result since we unconditionally rebuild indexes after the loop
        let _ = child_table_mut.delete_where(|row| row == row_to_delete);
    }

    // Rebuild indexes after deletion (handles both compaction and non-compaction cases)
    db.rebuild_indexes(child_table_name);

    Ok(())
}

/// Set child FK columns to NULL (SET NULL action)
fn set_null(
    db: &mut vibesql_storage::Database,
    child_table_name: &str,
    fk: &vibesql_catalog::ForeignKeyConstraint,
    parent_key_values: &[SqlValue],
) -> Result<(), ExecutorError> {
    // Collect indices of rows to update
    let child_table = db.get_table(child_table_name).unwrap();
    let mut rows_to_update: Vec<(usize, vibesql_storage::Row)> = Vec::new();

    // Use scan_live() to skip deleted rows and get correct physical indices
    for (idx, child_row) in child_table.scan_live() {
        let child_fk_values: Vec<SqlValue> =
            fk.column_indices.iter().map(|&idx| child_row.values[idx].clone()).collect();

        // Skip NULL foreign keys
        if child_fk_values.iter().any(|v| matches!(v, SqlValue::Null)) {
            continue;
        }

        if child_fk_values == parent_key_values {
            // Create updated row with FK columns set to NULL
            let mut updated_row = child_row.clone();
            for &fk_col_idx in &fk.column_indices {
                updated_row.values[fk_col_idx] = SqlValue::Null;
            }
            rows_to_update.push((idx, updated_row));
        }
    }

    // Now update the rows
    let child_table_mut = db.get_table_mut(child_table_name).unwrap();
    for (idx, updated_row) in rows_to_update {
        child_table_mut
            .update_row(idx, updated_row)
            .map_err(|e| ExecutorError::StorageError(e.to_string()))?;
    }

    // Rebuild indexes after update
    db.rebuild_indexes(child_table_name);

    Ok(())
}

/// Set child FK columns to their default values (SET DEFAULT action)
fn set_default(
    db: &mut vibesql_storage::Database,
    child_table_name: &str,
    fk: &vibesql_catalog::ForeignKeyConstraint,
    parent_key_values: &[SqlValue],
) -> Result<(), ExecutorError> {
    // Get child schema to access column defaults
    let child_schema = db.catalog.get_table(child_table_name).unwrap().clone();

    // Get default values for FK columns by evaluating their default expressions
    let mut default_values: Vec<SqlValue> = Vec::new();
    for &fk_col_idx in &fk.column_indices {
        let column = &child_schema.columns[fk_col_idx];
        let default_value = if let Some(default_expr) = &column.default_value {
            // Evaluate the default expression
            match default_expr {
                vibesql_ast::Expression::NextValue { sequence_name } => {
                    // Get the next value from the sequence
                    let seq = db.catalog.get_sequence_mut(sequence_name).map_err(|e| {
                        ExecutorError::UnsupportedExpression(format!("Sequence error: {:?}", e))
                    })?;
                    let next_val = seq.next_value().map_err(|e| {
                        ExecutorError::ConstraintViolation(format!("Sequence error: {}", e))
                    })?;
                    SqlValue::Integer(next_val)
                }
                _ => crate::insert::defaults::evaluate_default_expression(default_expr)?,
            }
        } else {
            // No default value defined, use NULL
            SqlValue::Null
        };
        default_values.push(default_value);
    }

    // Collect indices of rows to update
    let child_table = db.get_table(child_table_name).unwrap();
    let mut rows_to_update: Vec<(usize, vibesql_storage::Row)> = Vec::new();

    // Use scan_live() to skip deleted rows and get correct physical indices
    for (idx, child_row) in child_table.scan_live() {
        let child_fk_values: Vec<SqlValue> =
            fk.column_indices.iter().map(|&idx| child_row.values[idx].clone()).collect();

        // Skip NULL foreign keys
        if child_fk_values.iter().any(|v| matches!(v, SqlValue::Null)) {
            continue;
        }

        if child_fk_values == parent_key_values {
            // Create updated row with FK columns set to default values
            let mut updated_row = child_row.clone();
            for (i, &fk_col_idx) in fk.column_indices.iter().enumerate() {
                updated_row.values[fk_col_idx] = default_values[i].clone();
            }
            rows_to_update.push((idx, updated_row));
        }
    }

    // Now update the rows
    let child_table_mut = db.get_table_mut(child_table_name).unwrap();
    for (idx, updated_row) in rows_to_update {
        child_table_mut
            .update_row(idx, updated_row)
            .map_err(|e| ExecutorError::StorageError(e.to_string()))?;
    }

    // Rebuild indexes after update
    db.rebuild_indexes(child_table_name);

    Ok(())
}
