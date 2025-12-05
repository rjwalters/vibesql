use crate::errors::ExecutorError;

/// Pre-built index keys extracted during validation
#[derive(Debug, Clone)]
pub struct ValidationResult {
    /// Primary key values (if table has PK)
    pub primary_key: Option<Vec<vibesql_types::SqlValue>>,
    /// UNIQUE constraint values (one per constraint, empty if constraint has NULL)
    pub unique_keys: Vec<Option<Vec<vibesql_types::SqlValue>>>,
    /// Foreign key values (one per FK, empty if FK has NULL)
    pub foreign_keys: Vec<Option<Vec<vibesql_types::SqlValue>>>,
}

/// Single-pass row validator that checks all constraints and extracts index keys
pub struct RowValidator<'a> {
    db: &'a vibesql_storage::Database,
    schema: &'a vibesql_catalog::TableSchema,
    table_name: &'a str,
    /// Track PK values from batch for duplicate detection
    batch_pk_values: &'a [Vec<vibesql_types::SqlValue>],
    /// Track UNIQUE values from batch for duplicate detection
    batch_unique_values: &'a [Vec<Vec<vibesql_types::SqlValue>>],
    /// Skip PK/UNIQUE duplicate checks (for REPLACE conflict clause)
    skip_duplicate_checks: bool,
}

impl<'a> RowValidator<'a> {
    pub fn new(
        db: &'a vibesql_storage::Database,
        schema: &'a vibesql_catalog::TableSchema,
        table_name: &'a str,
        batch_pk_values: &'a [Vec<vibesql_types::SqlValue>],
        batch_unique_values: &'a [Vec<Vec<vibesql_types::SqlValue>>],
        skip_duplicate_checks: bool,
    ) -> Self {
        Self { db, schema, table_name, batch_pk_values, batch_unique_values, skip_duplicate_checks }
    }

    /// Validate all constraints in a single pass through the row
    /// Returns ValidationResult containing pre-built index keys
    pub fn validate(
        &self,
        row_values: &[vibesql_types::SqlValue],
    ) -> Result<ValidationResult, ExecutorError> {
        // Prepare result structure
        let mut result = ValidationResult {
            primary_key: None,
            unique_keys: vec![None; self.schema.unique_constraints.len()],
            foreign_keys: vec![None; self.schema.foreign_keys.len()],
        };

        // Phase 1: Single pass through columns for NOT NULL, PK, UNIQUE, FK extraction
        self.validate_column_constraints(row_values, &mut result)?;

        // Phase 2: Validate PK uniqueness (uses pre-extracted keys)
        // Skip if using REPLACE conflict clause
        if !self.skip_duplicate_checks {
            self.validate_primary_key_uniqueness(&result.primary_key)?;
        }

        // Phase 3: Validate UNIQUE constraint uniqueness (uses pre-extracted keys)
        // Skip if using REPLACE conflict clause
        if !self.skip_duplicate_checks {
            self.validate_unique_constraints(&result.unique_keys, row_values)?;
        }

        // Phase 4: Evaluate CHECK constraints (after column pass)
        self.validate_check_constraints(row_values)?;

        // Phase 5: Validate user-defined UNIQUE indexes (CREATE UNIQUE INDEX)
        // Skip if using REPLACE conflict clause
        if !self.skip_duplicate_checks {
            self.validate_unique_indexes(row_values)?;
        }

        // Phase 6: Validate FOREIGN KEY references (uses pre-extracted keys)
        self.validate_foreign_keys(&result.foreign_keys)?;

        Ok(result)
    }

    /// Phase 1: Single-pass column validation
    /// Performs NOT NULL checking and extracts PK/UNIQUE/FK keys
    fn validate_column_constraints(
        &self,
        row_values: &[vibesql_types::SqlValue],
        result: &mut ValidationResult,
    ) -> Result<(), ExecutorError> {
        // Get index information once
        let pk_indices = self.schema.get_primary_key_indices();
        let unique_constraint_indices = self.schema.get_unique_constraint_indices();

        // Prepare key buffers
        let mut pk_values = pk_indices.as_ref().map(|indices| Vec::with_capacity(indices.len()));
        let mut unique_values: Vec<Vec<vibesql_types::SqlValue>> = unique_constraint_indices
            .iter()
            .map(|indices| Vec::with_capacity(indices.len()))
            .collect();
        let mut fk_values: Vec<Vec<vibesql_types::SqlValue>> = self
            .schema
            .foreign_keys
            .iter()
            .map(|fk| Vec::with_capacity(fk.column_indices.len()))
            .collect();

        // Single pass through columns
        for (col_idx, col) in self.schema.columns.iter().enumerate() {
            let value = &row_values[col_idx];

            // 1. NOT NULL constraint check
            if !col.nullable && *value == vibesql_types::SqlValue::Null {
                return Err(ExecutorError::ConstraintViolation(format!(
                    "NOT NULL constraint violation: column '{}' in table '{}' cannot be NULL",
                    col.name, self.table_name
                )));
            }

            // 2. Extract PK values if this column is part of primary key
            if let Some(ref pk_idx) = pk_indices {
                if pk_idx.contains(&col_idx) {
                    if let Some(ref mut pk_buf) = pk_values {
                        pk_buf.push(value.clone());
                    }
                }
            }

            // 3. Extract UNIQUE constraint values if this column is part of any unique constraint
            for (constraint_idx, unique_indices) in unique_constraint_indices.iter().enumerate() {
                if unique_indices.contains(&col_idx) {
                    unique_values[constraint_idx].push(value.clone());
                }
            }

            // 4. Extract FK values if this column is part of any foreign key
            for (fk_idx, fk) in self.schema.foreign_keys.iter().enumerate() {
                if fk.column_indices.contains(&col_idx) {
                    fk_values[fk_idx].push(value.clone());
                }
            }
        }

        // Store extracted keys in result
        result.primary_key = pk_values;

        // Only store UNIQUE keys if no NULL values (multiple NULLs allowed)
        for (constraint_idx, values) in unique_values.into_iter().enumerate() {
            if !values.contains(&vibesql_types::SqlValue::Null) {
                result.unique_keys[constraint_idx] = Some(values);
            }
        }

        // Only store FK keys if no NULL values (NULL FK is allowed)
        for (fk_idx, values) in fk_values.into_iter().enumerate() {
            if !values.iter().any(|v| v.is_null()) {
                result.foreign_keys[fk_idx] = Some(values);
            }
        }

        Ok(())
    }

    /// Phase 2: Validate PK uniqueness using pre-extracted keys
    fn validate_primary_key_uniqueness(
        &self,
        pk_values: &Option<Vec<vibesql_types::SqlValue>>,
    ) -> Result<(), ExecutorError> {
        if let Some(ref new_pk_values) = pk_values {
            // Check for duplicates within the batch
            if self.batch_pk_values.contains(new_pk_values) {
                let pk_col_names: Vec<String> = self.schema.primary_key.as_ref().unwrap().clone();
                return Err(ExecutorError::ConstraintViolation(format!(
                    "PRIMARY KEY constraint violated: duplicate key value for ({})",
                    pk_col_names.join(", ")
                )));
            }

            // Check for duplicates in existing table data using index
            let table = self
                .db
                .get_table(self.table_name)
                .ok_or_else(|| ExecutorError::TableNotFound(self.table_name.to_string()))?;

            if let Some(pk_index) = table.primary_key_index() {
                if pk_index.contains_key(new_pk_values) {
                    let pk_col_names: Vec<String> =
                        self.schema.primary_key.as_ref().unwrap().clone();
                    return Err(ExecutorError::ConstraintViolation(format!(
                        "PRIMARY KEY constraint violated: duplicate key value for ({})",
                        pk_col_names.join(", ")
                    )));
                }
            }
        }

        Ok(())
    }

    /// Phase 3: Validate UNIQUE constraints using pre-extracted keys
    fn validate_unique_constraints(
        &self,
        unique_keys: &[Option<Vec<vibesql_types::SqlValue>>],
        _row_values: &[vibesql_types::SqlValue],
    ) -> Result<(), ExecutorError> {
        let unique_constraint_indices = self.schema.get_unique_constraint_indices();

        for (constraint_idx, unique_values) in unique_keys.iter().enumerate() {
            // Skip if any value is NULL (stored as None)
            let Some(ref new_unique_values) = unique_values else {
                continue;
            };

            // Check for duplicates within the batch
            if self.batch_unique_values[constraint_idx].contains(new_unique_values) {
                let unique_col_names: Vec<String> =
                    self.schema.unique_constraints[constraint_idx].clone();
                return Err(ExecutorError::ConstraintViolation(format!(
                    "UNIQUE constraint violated: duplicate value for ({})",
                    unique_col_names.join(", ")
                )));
            }

            // Check for duplicates in existing table data using index
            let table = self
                .db
                .get_table(self.table_name)
                .ok_or_else(|| ExecutorError::TableNotFound(self.table_name.to_string()))?;

            if constraint_idx < table.unique_indexes().len() {
                let unique_index = &table.unique_indexes()[constraint_idx];
                if unique_index.contains_key(new_unique_values) {
                    let unique_col_names: Vec<String> =
                        self.schema.unique_constraints[constraint_idx].clone();
                    return Err(ExecutorError::ConstraintViolation(format!(
                        "UNIQUE constraint violated: duplicate value for ({})",
                        unique_col_names.join(", ")
                    )));
                }
            } else {
                // Fallback to table scan if index not available
                let unique_indices = &unique_constraint_indices[constraint_idx];
                for existing_row in table.scan() {
                    let existing_unique_values: Vec<vibesql_types::SqlValue> = unique_indices
                        .iter()
                        .filter_map(|&idx| existing_row.get(idx).cloned())
                        .collect();

                    // Skip if any existing value is NULL
                    if existing_unique_values.contains(&vibesql_types::SqlValue::Null) {
                        continue;
                    }

                    if new_unique_values == &existing_unique_values {
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

    /// Phase 4: Evaluate CHECK constraints (after column pass)
    fn validate_check_constraints(
        &self,
        row_values: &[vibesql_types::SqlValue],
    ) -> Result<(), ExecutorError> {
        if !self.schema.check_constraints.is_empty() {
            let row = vibesql_storage::Row::new(row_values.to_vec());
            let evaluator = crate::evaluator::ExpressionEvaluator::new(self.schema);

            for (constraint_name, check_expr) in &self.schema.check_constraints {
                let result = evaluator.eval(check_expr, &row)?;

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

    /// Phase 5: Validate user-defined UNIQUE indexes
    fn validate_unique_indexes(
        &self,
        row_values: &[vibesql_types::SqlValue],
    ) -> Result<(), ExecutorError> {
        super::constraints::enforce_unique_indexes(
            self.db,
            self.schema,
            self.table_name,
            row_values,
        )
    }

    /// Phase 6: Validate FOREIGN KEY references using pre-extracted keys
    fn validate_foreign_keys(
        &self,
        fk_keys: &[Option<Vec<vibesql_types::SqlValue>>],
    ) -> Result<(), ExecutorError> {
        for (fk_idx, fk_values) in fk_keys.iter().enumerate() {
            // Skip if any FK value is NULL (stored as None)
            let Some(ref fk_values) = fk_values else {
                continue;
            };

            let fk = &self.schema.foreign_keys[fk_idx];

            // Check if the referenced key exists in the parent table
            let parent_table = self
                .db
                .get_table(&fk.parent_table)
                .ok_or_else(|| ExecutorError::TableNotFound(fk.parent_table.clone()))?;

            let key_exists = parent_table.scan().iter().any(|parent_row| {
                fk.parent_column_indices
                    .iter()
                    .zip(fk_values)
                    .all(|(&parent_idx, fk_val)| parent_row.get(parent_idx) == Some(fk_val))
            });

            if !key_exists {
                return Err(ExecutorError::ConstraintViolation(format!(
                    "FOREIGN KEY constraint '{}' violated: key ({}) not found in table '{}'",
                    fk.name.as_deref().unwrap_or(""),
                    fk.column_names.join(", "),
                    fk.parent_table
                )));
            }
        }

        Ok(())
    }
}
