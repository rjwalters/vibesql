use crate::errors::ExecutorError;

/// Enforce PRIMARY KEY constraint (uniqueness)
/// Returns Ok if constraint is satisfied
pub fn enforce_primary_key_constraint(
    db: &vibesql_storage::Database,
    schema: &vibesql_catalog::TableSchema,
    table_name: &str,
    row_values: &[vibesql_types::SqlValue],
    batch_pk_values: &[Vec<vibesql_types::SqlValue>],
) -> Result<(), ExecutorError> {
    if let Some(pk_indices) = schema.get_primary_key_indices() {
        // Extract primary key values from the new row
        let new_pk_values: Vec<vibesql_types::SqlValue> =
            pk_indices.iter().map(|&idx| row_values[idx].clone()).collect();

        // Check for duplicates within the batch of rows being inserted
        if batch_pk_values.contains(&new_pk_values) {
            let pk_col_names: Vec<String> = schema.primary_key.as_ref().unwrap().clone();
            return Err(ExecutorError::ConstraintViolation(format!(
                "PRIMARY KEY constraint violated: duplicate key value for ({})",
                pk_col_names.join(", ")
            )));
        }

        // Check if any existing row has the same primary key using the hash index
        let table = db
            .get_table(table_name)
            .ok_or_else(|| ExecutorError::TableNotFound(table_name.to_string()))?;

        // Skip existing row check if table is in append mode (sequential inserts)
        // Append mode guarantees no duplicates because PKs are strictly increasing
        if table.is_in_append_mode() {
            return Ok(());
        }

        // Use the primary key index for O(1) lookup instead of O(n) scan
        if let Some(pk_index) = table.primary_key_index() {
            if pk_index.contains_key(&new_pk_values) {
                let pk_col_names: Vec<String> = schema.primary_key.as_ref().unwrap().clone();
                return Err(ExecutorError::ConstraintViolation(format!(
                    "PRIMARY KEY constraint violated: duplicate key value for ({})",
                    pk_col_names.join(", ")
                )));
            }
        } else {
            // Fallback to table scan if index not available (should not happen in normal operation)
            for existing_row in table.scan() {
                let existing_pk_values: Vec<vibesql_types::SqlValue> =
                    pk_indices.iter().filter_map(|&idx| existing_row.get(idx).cloned()).collect();

                if new_pk_values == existing_pk_values {
                    let pk_col_names: Vec<String> = schema.primary_key.as_ref().unwrap().clone();
                    return Err(ExecutorError::ConstraintViolation(format!(
                        "PRIMARY KEY constraint violated: duplicate key value for ({})",
                        pk_col_names.join(", ")
                    )));
                }
            }
        }
    }

    Ok(())
}

/// Enforce UNIQUE constraints on a row
/// Returns Ok if all UNIQUE constraints are satisfied
pub fn enforce_unique_constraints(
    db: &vibesql_storage::Database,
    schema: &vibesql_catalog::TableSchema,
    table_name: &str,
    row_values: &[vibesql_types::SqlValue],
    batch_unique_values: &[Vec<Vec<vibesql_types::SqlValue>>],
) -> Result<(), ExecutorError> {
    let unique_constraint_indices = schema.get_unique_constraint_indices();

    for (constraint_idx, unique_indices) in unique_constraint_indices.iter().enumerate() {
        // Extract unique constraint values from the new row
        let new_unique_values: Vec<vibesql_types::SqlValue> =
            unique_indices.iter().map(|&idx| row_values[idx].clone()).collect();

        // Skip if any value in the unique constraint is NULL
        // (NULL != NULL in SQL, so multiple NULLs are allowed)
        if new_unique_values.contains(&vibesql_types::SqlValue::Null) {
            continue;
        }

        // Check for duplicates within the batch of rows being inserted
        if batch_unique_values[constraint_idx].contains(&new_unique_values) {
            let unique_col_names: Vec<String> = schema.unique_constraints[constraint_idx].clone();
            return Err(ExecutorError::ConstraintViolation(format!(
                "UNIQUE constraint violated: duplicate value for ({})",
                unique_col_names.join(", ")
            )));
        }

        // Check if any existing row has the same unique constraint values using hash index
        let table = db
            .get_table(table_name)
            .ok_or_else(|| ExecutorError::TableNotFound(table_name.to_string()))?;

        // Use the unique constraint index for O(1) lookup instead of O(n) scan
        if constraint_idx < table.unique_indexes().len() {
            let unique_index = &table.unique_indexes()[constraint_idx];
            if unique_index.contains_key(&new_unique_values) {
                let unique_col_names: Vec<String> =
                    schema.unique_constraints[constraint_idx].clone();
                return Err(ExecutorError::ConstraintViolation(format!(
                    "UNIQUE constraint violated: duplicate value for ({})",
                    unique_col_names.join(", ")
                )));
            }
        } else {
            // Fallback to table scan if index not available (should not happen in normal operation)
            for existing_row in table.scan() {
                let existing_unique_values: Vec<vibesql_types::SqlValue> = unique_indices
                    .iter()
                    .filter_map(|&idx| existing_row.get(idx).cloned())
                    .collect();

                // Skip if any existing value is NULL
                if existing_unique_values.contains(&vibesql_types::SqlValue::Null) {
                    continue;
                }

                if new_unique_values == existing_unique_values {
                    let unique_col_names: Vec<String> =
                        schema.unique_constraints[constraint_idx].clone();
                    return Err(ExecutorError::ConstraintViolation(format!(
                        "UNIQUE constraint violated: duplicate value for ({})",
                        unique_col_names.join(", ")
                    )));
                }
            }
        }
    }

    Ok(())
}

/// Enforce CHECK constraints on a row
/// Returns Ok if all CHECK constraints are satisfied
pub fn enforce_check_constraints(
    schema: &vibesql_catalog::TableSchema,
    row_values: &[vibesql_types::SqlValue],
) -> Result<(), ExecutorError> {
    if !schema.check_constraints.is_empty() {
        // Create a row from the values to evaluate the expression
        let row = vibesql_storage::Row::new(row_values.to_vec());
        let evaluator = crate::evaluator::ExpressionEvaluator::new(schema);

        for (constraint_name, check_expr) in &schema.check_constraints {
            // Evaluate the CHECK expression against the row
            let result = evaluator.eval(check_expr, &row)?;

            // CHECK constraint passes if result is TRUE or NULL (UNKNOWN)
            // CHECK constraint fails if result is FALSE
            if result == vibesql_types::SqlValue::Boolean(false) {
                return Err(ExecutorError::ConstraintViolation(format!(
                    "CHECK constraint '{}' violated",
                    constraint_name
                )));
            }
        }
    }

    Ok(())
}

/// Enforce UNIQUE constraint for user-defined indexes (CREATE UNIQUE INDEX)
/// Returns Ok if all unique index constraints are satisfied
pub fn enforce_unique_indexes(
    db: &vibesql_storage::Database,
    schema: &vibesql_catalog::TableSchema,
    table_name: &str,
    row_values: &[vibesql_types::SqlValue],
) -> Result<(), ExecutorError> {
    // Get all indexes for this table
    let indexes_for_table = db.list_indexes_for_table(table_name);

    for index_name in indexes_for_table {
        if let Some(index_metadata) = db.get_index(&index_name) {
            // Only check unique indexes
            if !index_metadata.unique {
                continue;
            }

            // Build the key values from the row for this index
            let mut key_values = Vec::new();
            for index_col in &index_metadata.columns {
                let col_idx = schema.get_column_index(&index_col.column_name).ok_or_else(|| {
                    ExecutorError::ColumnNotFound {
                        column_name: index_col.column_name.clone(),
                        table_name: table_name.to_string(),
                        searched_tables: vec![table_name.to_string()],
                        available_columns: schema.columns.iter().map(|c| c.name.clone()).collect(),
                    }
                })?;
                key_values.push(row_values[col_idx].clone());
            }

            // Skip if any value in the unique index is NULL
            // (NULL != NULL in SQL, so multiple NULLs are allowed)
            if key_values.contains(&vibesql_types::SqlValue::Null) {
                continue;
            }

            // Check if this key already exists in the index
            if let Some(index_data) = db.get_index_data(&index_name) {
                if index_data.contains_key(&key_values) {
                    // Format column names for error message
                    let column_names: Vec<String> =
                        index_metadata.columns.iter().map(|c| c.column_name.clone()).collect();

                    return Err(ExecutorError::ConstraintViolation(format!(
                        "UNIQUE constraint '{}' violated: duplicate key value for ({})",
                        index_metadata.index_name,
                        column_names.join(", ")
                    )));
                }
            }
        }
    }

    Ok(())
}
