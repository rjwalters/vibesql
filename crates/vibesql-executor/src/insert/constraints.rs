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

        // Skip uniqueness check if any primary key value is NULL
        // (NULL != NULL in SQL, so multiple NULLs are allowed in non-INTEGER PRIMARY KEY)
        if new_pk_values.contains(&vibesql_types::SqlValue::Null) {
            return Ok(());
        }

        // Check for duplicates within the batch of rows being inserted
        if batch_pk_values.contains(&new_pk_values) {
            let pk_col_names: Vec<String> = schema.primary_key.as_ref().unwrap().clone();
            // SQLite uses "UNIQUE constraint failed" for PRIMARY KEY violations
            let qualified_cols: Vec<String> =
                pk_col_names.iter().map(|col| format!("{}.{}", table_name, col)).collect();
            // SQLite-compatible: output the message as-is without prefix
            return Err(ExecutorError::SqliteCompatError(format!(
                "UNIQUE constraint failed: {}",
                qualified_cols.join(", ")
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

        // Issue #5204: Under MVCC we must treat tombstoned rows (xmax stamped
        // by a committed concurrent txn / visible-as-deleted to our snapshot)
        // as "not present" for uniqueness purposes — otherwise an UPDATE that
        // moves a row to a unique key currently held by a deleted row would
        // erroneously fail. Off-state (`mvcc_enabled` OFF): `is_row_visible`
        // reduces to the existing not-bitmap-deleted check.
        let snapshot = crate::mvcc::read_snapshot(db);

        // Use the primary key index for O(1) lookup instead of O(n) scan
        if let Some(pk_index) = table.primary_key_index() {
            if let Some(&row_idx) = pk_index.get(&new_pk_values) {
                if table.is_row_visible(row_idx, &snapshot) {
                    let pk_col_names: Vec<String> = schema.primary_key.as_ref().unwrap().clone();
                    // SQLite uses "UNIQUE constraint failed" for PRIMARY KEY violations
                    let qualified_cols: Vec<String> =
                        pk_col_names.iter().map(|col| format!("{}.{}", table_name, col)).collect();
                    // SQLite-compatible: output the message as-is without prefix
                    return Err(ExecutorError::SqliteCompatError(format!(
                        "UNIQUE constraint failed: {}",
                        qualified_cols.join(", ")
                    )));
                }
            }
        } else {
            // Fallback to table scan if index not available (should not happen in normal operation)
            // Issue #5204: respect MVCC visibility — iterate visible rows only.
            for (_idx, existing_row) in table.scan_visible(&snapshot) {
                let existing_pk_values: Vec<vibesql_types::SqlValue> =
                    pk_indices.iter().filter_map(|&idx| existing_row.get(idx).cloned()).collect();

                if new_pk_values == existing_pk_values {
                    let pk_col_names: Vec<String> = schema.primary_key.as_ref().unwrap().clone();
                    // SQLite uses "UNIQUE constraint failed" for PRIMARY KEY violations
                    let qualified_cols: Vec<String> =
                        pk_col_names.iter().map(|col| format!("{}.{}", table_name, col)).collect();
                    // SQLite-compatible: output the message as-is without prefix
                    return Err(ExecutorError::SqliteCompatError(format!(
                        "UNIQUE constraint failed: {}",
                        qualified_cols.join(", ")
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
        // (skip if batch_unique_values is empty or doesn't have this constraint)
        if constraint_idx < batch_unique_values.len()
            && batch_unique_values[constraint_idx].contains(&new_unique_values)
        {
            let unique_col_names: Vec<String> = schema.unique_constraints[constraint_idx].clone();
            // Format: "UNIQUE constraint failed: table.col1, table.col2" (SQLite-compatible)
            let qualified_cols: Vec<String> =
                unique_col_names.iter().map(|col| format!("{}.{}", table_name, col)).collect();
            // SQLite-compatible: output the message as-is without prefix
            return Err(ExecutorError::SqliteCompatError(format!(
                "UNIQUE constraint failed: {}",
                qualified_cols.join(", ")
            )));
        }

        // Check if any existing row has the same unique constraint values using hash index
        let table = db
            .get_table(table_name)
            .ok_or_else(|| ExecutorError::TableNotFound(table_name.to_string()))?;

        // Issue #5204: under MVCC a tombstoned existing row must not block a
        // new row with the same unique key. Off-state (`mvcc_enabled` OFF):
        // `is_row_visible` is the existing not-bitmap-deleted check.
        let snapshot = crate::mvcc::read_snapshot(db);

        // Use the unique constraint index for O(1) lookup instead of O(n) scan
        if constraint_idx < table.unique_indexes().len() {
            let unique_index = &table.unique_indexes()[constraint_idx];
            if let Some(&row_idx) = unique_index.get(&new_unique_values) {
                if table.is_row_visible(row_idx, &snapshot) {
                    let unique_col_names: Vec<String> =
                        schema.unique_constraints[constraint_idx].clone();
                    // Format: "UNIQUE constraint failed: table.col1, table.col2" (SQLite-compatible)
                    let qualified_cols: Vec<String> = unique_col_names
                        .iter()
                        .map(|col| format!("{}.{}", table_name, col))
                        .collect();
                    // SQLite-compatible: output the message as-is without prefix
                    return Err(ExecutorError::SqliteCompatError(format!(
                        "UNIQUE constraint failed: {}",
                        qualified_cols.join(", ")
                    )));
                }
            }
        } else {
            // Fallback to table scan if index not available (should not happen in normal operation)
            // Issue #5204: iterate only rows visible to our MVCC snapshot.
            for (_idx, existing_row) in table.scan_visible(&snapshot) {
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
                    // Format: "UNIQUE constraint failed: table.col1, table.col2"
                    // (SQLite-compatible)
                    let qualified_cols: Vec<String> = unique_col_names
                        .iter()
                        .map(|col| format!("{}.{}", table_name, col))
                        .collect();
                    // SQLite-compatible: output the message as-is without prefix
                    return Err(ExecutorError::SqliteCompatError(format!(
                        "UNIQUE constraint failed: {}",
                        qualified_cols.join(", ")
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
                // SQLite-compatible error format: "CHECK constraint failed: <name_or_expr>"
                return Err(ExecutorError::SqliteCompatError(format!(
                    "CHECK constraint failed: {}",
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

    // Issue #5204: under MVCC a unique index lookup may return row indices
    // that point to tombstoned (xmax-stamped) rows. Such rows must NOT block
    // a new row with the same unique key. Off-state (`mvcc_enabled` OFF):
    // `is_row_visible` is the existing not-bitmap-deleted check.
    let snapshot = crate::mvcc::read_snapshot(db);
    let table = db.get_table(table_name);

    for index_name in indexes_for_table {
        if let Some(index_metadata) = db.get_index(&index_name) {
            // Only check unique indexes
            if !index_metadata.unique {
                continue;
            }

            // Build the key values from the row for this index
            let mut key_values = Vec::new();
            for index_col in &index_metadata.columns {
                let col_idx =
                    schema.get_column_index(index_col.expect_column_name()).ok_or_else(|| {
                        ExecutorError::ColumnNotFound {
                            column_name: index_col.expect_column_name().to_string(),
                            table_name: table_name.to_string(),
                            searched_tables: vec![table_name.to_string()],
                            available_columns: schema
                                .columns
                                .iter()
                                .map(|c| c.name.clone())
                                .collect(),
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
                // Look up the actual row indices behind the key so we can
                // verify at least one of them is MVCC-visible. If every
                // matching row is tombstoned from our snapshot's perspective,
                // the key is effectively unused and we must not raise a
                // violation.
                if let Some(row_indices) = index_data.get(&key_values) {
                    let key_is_live = match table {
                        Some(t) => {
                            row_indices.iter().any(|&idx| t.is_row_visible(idx, &snapshot))
                        }
                        None => !row_indices.is_empty(),
                    };
                    if key_is_live {
                        // SQLite format: "UNIQUE constraint failed: table.col1, table.col2"
                        let columns_str = index_metadata
                            .columns
                            .iter()
                            .map(|col| format!("{}.{}", table_name, col.expect_column_name()))
                            .collect::<Vec<_>>()
                            .join(", ");
                        // SQLite-compatible: output the message as-is without prefix
                        return Err(ExecutorError::SqliteCompatError(format!(
                            "UNIQUE constraint failed: {}",
                            columns_str
                        )));
                    }
                }
            }
        }
    }

    Ok(())
}
