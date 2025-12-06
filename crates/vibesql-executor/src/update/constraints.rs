//! Constraint validation for UPDATE operations

use crate::{errors::ExecutorError, evaluator::ExpressionEvaluator};

/// Validator for table constraints
pub struct ConstraintValidator<'a> {
    schema: &'a vibesql_catalog::TableSchema,
}

impl<'a> ConstraintValidator<'a> {
    /// Create a new constraint validator
    pub fn new(schema: &'a vibesql_catalog::TableSchema) -> Self {
        Self { schema }
    }

    /// Validate all constraints for an updated row
    ///
    /// Checks NOT NULL, PRIMARY KEY, UNIQUE, and CHECK constraints.
    /// The original row is needed for PRIMARY KEY and UNIQUE checks to exclude self.
    pub fn validate_row(
        &self,
        table: &vibesql_storage::Table,
        table_name: &str,
        row_index: usize,
        new_row: &vibesql_storage::Row,
        original_row: &vibesql_storage::Row,
    ) -> Result<(), ExecutorError> {
        // Enforce NOT NULL constraints
        self.validate_not_null(table_name, new_row)?;

        // Enforce PRIMARY KEY constraints
        self.validate_primary_key(table, row_index, new_row, original_row)?;

        // Enforce UNIQUE constraints
        self.validate_unique_constraints(table, row_index, new_row, original_row)?;

        // Enforce CHECK constraints
        self.validate_check_constraints(new_row)?;

        Ok(())
    }

    /// Validate row against user-defined UNIQUE indexes
    /// This is separate from validate_row to allow access to db
    pub fn validate_unique_indexes(
        &self,
        db: &vibesql_storage::Database,
        table_name: &str,
        new_row: &vibesql_storage::Row,
        original_row: &vibesql_storage::Row,
    ) -> Result<(), ExecutorError> {
        // Get all indexes for this table
        let indexes_for_table = db.list_indexes_for_table(table_name);

        for index_name in indexes_for_table {
            if let Some(index_metadata) = db.get_index(&index_name) {
                // Only check unique indexes
                if !index_metadata.unique {
                    continue;
                }

                // Build the key values from the new row for this index
                let mut new_key_values = Vec::new();
                for index_col in &index_metadata.columns {
                    let col_idx =
                        self.schema.get_column_index(&index_col.column_name).ok_or_else(|| {
                            ExecutorError::ColumnNotFound {
                                column_name: index_col.column_name.clone(),
                                table_name: table_name.to_string(),
                                searched_tables: vec![table_name.to_string()],
                                available_columns: self
                                    .schema
                                    .columns
                                    .iter()
                                    .map(|c| c.name.clone())
                                    .collect(),
                            }
                        })?;
                    new_key_values.push(new_row.values[col_idx].clone());
                }

                // Skip if any value in the unique index is NULL
                // (NULL != NULL in SQL, so multiple NULLs are allowed)
                if new_key_values.contains(&vibesql_types::SqlValue::Null) {
                    continue;
                }

                // Build the original key values to check if they changed
                let mut original_key_values = Vec::new();
                for index_col in &index_metadata.columns {
                    let col_idx = self.schema.get_column_index(&index_col.column_name).unwrap();
                    original_key_values.push(original_row.values[col_idx].clone());
                }

                // If the key hasn't changed, skip the check (same row)
                if new_key_values == original_key_values {
                    continue;
                }

                // Check if this key already exists in the index
                if let Some(index_data) = db.get_index_data(&index_name) {
                    if index_data.contains_key(&new_key_values) {
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

    /// Validate NOT NULL constraints
    fn validate_not_null(
        &self,
        table_name: &str,
        new_row: &vibesql_storage::Row,
    ) -> Result<(), ExecutorError> {
        for (col_idx, col) in self.schema.columns.iter().enumerate() {
            let value = new_row
                .get(col_idx)
                .ok_or(ExecutorError::ColumnIndexOutOfBounds { index: col_idx })?;

            if !col.nullable && *value == vibesql_types::SqlValue::Null {
                return Err(ExecutorError::ConstraintViolation(format!(
                    "NOT NULL constraint violation: column '{}' in table '{}' cannot be NULL",
                    col.name, table_name
                )));
            }
        }

        Ok(())
    }

    /// Validate PRIMARY KEY constraints
    fn validate_primary_key(
        &self,
        table: &vibesql_storage::Table,
        row_index: usize,
        new_row: &vibesql_storage::Row,
        original_row: &vibesql_storage::Row,
    ) -> Result<(), ExecutorError> {
        if let Some(pk_indices) = self.schema.get_primary_key_indices() {
            // Extract primary key values from the updated row
            let new_pk_values: Vec<vibesql_types::SqlValue> =
                pk_indices.iter().map(|&idx| new_row.values[idx].clone()).collect();

            // Use hash index for O(1) lookup instead of O(n) scan
            if let Some(pk_index) = table.primary_key_index() {
                if pk_index.contains_key(&new_pk_values) {
                    // Check if it's not the same row (hash index doesn't store row index for easy
                    // exclusion) We need to verify it's actually a different
                    // row by checking if this is an update to the same PK
                    let original_pk_values: Vec<vibesql_types::SqlValue> =
                        pk_indices.iter().map(|&idx| original_row.values[idx].clone()).collect();

                    if new_pk_values != original_pk_values {
                        let pk_col_names: Vec<String> =
                            self.schema.primary_key.as_ref().unwrap().clone();
                        return Err(ExecutorError::ConstraintViolation(format!(
                            "PRIMARY KEY constraint violated: duplicate key value for ({})",
                            pk_col_names.join(", ")
                        )));
                    }
                }
            } else {
                // Fallback to table scan if index not available (should not happen in normal
                // operation). Use scan_live() to skip deleted rows.
                for (other_idx, other_row) in table.scan_live() {
                    // Skip the row being updated
                    if other_idx == row_index {
                        continue;
                    }

                    let other_pk_values: Vec<&vibesql_types::SqlValue> =
                        pk_indices.iter().filter_map(|&idx| other_row.get(idx)).collect();

                    if new_pk_values.iter().zip(other_pk_values).all(|(a, b)| *a == *b) {
                        let pk_col_names: Vec<String> =
                            self.schema.primary_key.as_ref().unwrap().clone();
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

    /// Validate UNIQUE constraints
    fn validate_unique_constraints(
        &self,
        table: &vibesql_storage::Table,
        row_index: usize,
        new_row: &vibesql_storage::Row,
        original_row: &vibesql_storage::Row,
    ) -> Result<(), ExecutorError> {
        let unique_constraint_indices = self.schema.get_unique_constraint_indices();
        for (constraint_idx, unique_indices) in unique_constraint_indices.iter().enumerate() {
            // Extract unique constraint values from the updated row
            let new_unique_values: Vec<vibesql_types::SqlValue> =
                unique_indices.iter().map(|&idx| new_row.values[idx].clone()).collect();

            // Skip if any value in the unique constraint is NULL
            // (NULL != NULL in SQL, so multiple NULLs are allowed)
            if new_unique_values.contains(&vibesql_types::SqlValue::Null) {
                continue;
            }

            // Use hash index for O(1) lookup instead of O(n) scan
            let unique_indexes = table.unique_indexes();
            if constraint_idx < unique_indexes.len() {
                let unique_index = &unique_indexes[constraint_idx];
                if unique_index.contains_key(&new_unique_values) {
                    // Check if it's not the same row by comparing original values
                    let original_unique_values: Vec<vibesql_types::SqlValue> = unique_indices
                        .iter()
                        .map(|&idx| original_row.values[idx].clone())
                        .collect();

                    if new_unique_values != original_unique_values {
                        let unique_col_names: Vec<String> =
                            self.schema.unique_constraints[constraint_idx].clone();
                        return Err(ExecutorError::ConstraintViolation(format!(
                            "UNIQUE constraint violated: duplicate value for ({})",
                            unique_col_names.join(", ")
                        )));
                    }
                }
            } else {
                // Fallback to table scan if index not available (should not happen in normal
                // operation). Use scan_live() to skip deleted rows.
                for (other_idx, other_row) in table.scan_live() {
                    // Skip the row being updated
                    if other_idx == row_index {
                        continue;
                    }

                    let other_unique_values: Vec<&vibesql_types::SqlValue> =
                        unique_indices.iter().filter_map(|&idx| other_row.get(idx)).collect();

                    // Skip if any existing value is NULL
                    if other_unique_values.iter().any(|v| **v == vibesql_types::SqlValue::Null) {
                        continue;
                    }

                    if new_unique_values.iter().zip(other_unique_values).all(|(a, b)| *a == *b) {
                        let unique_col_names: Vec<String> =
                            self.schema.unique_constraints[constraint_idx].clone();
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

    /// Validate CHECK constraints
    fn validate_check_constraints(
        &self,
        new_row: &vibesql_storage::Row,
    ) -> Result<(), ExecutorError> {
        if !self.schema.check_constraints.is_empty() {
            let evaluator = ExpressionEvaluator::new(self.schema);

            for (constraint_name, check_expr) in &self.schema.check_constraints {
                // Evaluate the CHECK expression against the updated row
                let result = evaluator.eval(check_expr, new_row)?;

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
}
