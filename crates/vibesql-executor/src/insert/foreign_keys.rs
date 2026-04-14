use crate::errors::ExecutorError;

/// Validate FOREIGN KEY constraints for a new row
pub fn validate_foreign_key_constraints(
    db: &vibesql_storage::Database,
    table_name: &str,
    row_values: &[vibesql_types::SqlValue],
) -> Result<(), ExecutorError> {
    // Skip FK enforcement when PRAGMA foreign_keys is OFF (default)
    if !db.foreign_keys_enabled() {
        return Ok(());
    }

    let schema = db
        .catalog
        .get_table(table_name)
        .ok_or_else(|| ExecutorError::TableNotFound(table_name.to_string()))?;

    for fk in &schema.foreign_keys {
        // Extract FK values from the new row
        let fk_values: Vec<vibesql_types::SqlValue> =
            fk.column_indices.iter().map(|&idx| row_values[idx].clone()).collect();

        // If any part of the foreign key is NULL, the constraint is not violated.
        if fk_values.iter().any(|v| v.is_null()) {
            continue;
        }

        // Check if the referenced key exists in the parent table
        let parent_table = db
            .get_table(&fk.parent_table)
            .ok_or_else(|| ExecutorError::TableNotFound(fk.parent_table.clone()))?;

        let key_exists = parent_table.scan().iter().any(|parent_row| {
            fk.parent_column_indices
                .iter()
                .zip(&fk_values)
                .all(|(&parent_idx, fk_val)| parent_row.get(parent_idx) == Some(fk_val))
        });

        if !key_exists {
            return Err(ExecutorError::ConstraintViolation(format!(
                "FOREIGN KEY constraint \'{}\' violated: key ({}) not found in table \'{}\'",
                fk.name.as_deref().unwrap_or(""),
                fk.column_names.join(", "),
                fk.parent_table
            )));
        }
    }

    Ok(())
}
