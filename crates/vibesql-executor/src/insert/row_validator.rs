use vibesql_storage::DeferredFkViolation;

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
    /// FK violations that were deferred (because the constraint is
    /// `INITIALLY DEFERRED` or `PRAGMA defer_foreign_keys=ON`). The
    /// caller must push these onto the active transaction's queue
    /// before inserting the row. See Phase C2 of #5085.
    pub deferred_fk_violations: Vec<DeferredFkViolation>,
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
    /// Previously validated full rows from this multi-row INSERT batch.
    /// Used by self-referential FK validation so row N can find its parent
    /// among earlier rows in the same VALUES list (e.g. fkey1-5.1:
    /// `INSERT INTO t11 VALUES (1, NULL), (2, 1), (3, 2);` where t11.parent
    /// references t11.x — row (2, 1) must see the just-validated (1, NULL)).
    batch_full_rows: &'a [Vec<vibesql_types::SqlValue>],
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
        batch_full_rows: &'a [Vec<vibesql_types::SqlValue>],
        skip_duplicate_checks: bool,
    ) -> Self {
        Self {
            db,
            schema,
            table_name,
            batch_pk_values,
            batch_unique_values,
            batch_full_rows,
            skip_duplicate_checks,
        }
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
            deferred_fk_violations: Vec::new(),
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
        self.validate_foreign_keys(
            row_values,
            &result.foreign_keys,
            &mut result.deferred_fk_violations,
        )?;

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
        // Only store PRIMARY KEY if no NULL values (multiple NULLs allowed in non-INTEGER PRIMARY KEY)
        if let Some(pk_vals) = pk_values {
            if !pk_vals.contains(&vibesql_types::SqlValue::Null) {
                result.primary_key = Some(pk_vals);
            }
        }

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
                // SQLite uses "UNIQUE constraint failed" for PRIMARY KEY violations
                let qualified_cols: Vec<String> =
                    pk_col_names.iter().map(|col| format!("{}.{}", self.table_name, col)).collect();
                // SQLite-compatible: output the message as-is without prefix
                return Err(ExecutorError::SqliteCompatError(format!(
                    "UNIQUE constraint failed: {}",
                    qualified_cols.join(", ")
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
                    // SQLite uses "UNIQUE constraint failed" for PRIMARY KEY violations
                    let qualified_cols: Vec<String> = pk_col_names
                        .iter()
                        .map(|col| format!("{}.{}", self.table_name, col))
                        .collect();
                    // SQLite-compatible: output the message as-is without prefix
                    return Err(ExecutorError::SqliteCompatError(format!(
                        "UNIQUE constraint failed: {}",
                        qualified_cols.join(", ")
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
                // Format: "UNIQUE constraint failed: table.col1, table.col2" (SQLite-compatible)
                let qualified_cols: Vec<String> = unique_col_names
                    .iter()
                    .map(|col| format!("{}.{}", self.table_name, col))
                    .collect();
                // SQLite-compatible: output the message as-is without prefix
                return Err(ExecutorError::SqliteCompatError(format!(
                    "UNIQUE constraint failed: {}",
                    qualified_cols.join(", ")
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
                    // Format: "UNIQUE constraint failed: table.col1, table.col2" (SQLite-compatible)
                    let qualified_cols: Vec<String> = unique_col_names
                        .iter()
                        .map(|col| format!("{}.{}", self.table_name, col))
                        .collect();
                    // SQLite-compatible: output the message as-is without prefix
                    return Err(ExecutorError::SqliteCompatError(format!(
                        "UNIQUE constraint failed: {}",
                        qualified_cols.join(", ")
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
                        // Format: "UNIQUE constraint failed: table.col1, table.col2" (SQLite-compatible)
                        let qualified_cols: Vec<String> = unique_col_names
                            .iter()
                            .map(|col| format!("{}.{}", self.table_name, col))
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

    /// Phase 6: Validate FOREIGN KEY references using pre-extracted keys.
    ///
    /// Per Phase C2 of #5085, when the constraint is `INITIALLY DEFERRED`
    /// or the session has `PRAGMA defer_foreign_keys=ON`, a missing
    /// parent row is *not* an immediate error: instead a
    /// [`DeferredFkViolation`] is appended to `deferred_violations` and
    /// the caller pushes it onto the active transaction's queue. The
    /// queue is drained and re-checked at COMMIT.
    ///
    /// Steps 4-6 (parent-existence scan, self-FK row-self check,
    /// defer-or-error) are factored into
    /// [`crate::foreign_key_check::check_fk_row_existence`] and shared with
    /// the free-function [`super::foreign_keys::validate_foreign_key_constraints`].
    /// This wrapper handles the PRAGMA gate, schema-mismatch check, and the
    /// NULL-skip (via the `Option` representation of `fk_keys`); the caller
    /// drains `deferred_violations` after our immutable `&Database` borrow
    /// drops (preserves the pattern from #5125 / PR #5141).
    fn validate_foreign_keys(
        &self,
        full_row_values: &[vibesql_types::SqlValue],
        fk_keys: &[Option<Vec<vibesql_types::SqlValue>>],
        deferred_violations: &mut Vec<DeferredFkViolation>,
    ) -> Result<(), ExecutorError> {
        // Step 1: skip FK enforcement when PRAGMA foreign_keys is OFF (default).
        if !self.db.foreign_keys_enabled() {
            return Ok(());
        }

        for (fk_idx, fk_values) in fk_keys.iter().enumerate() {
            let fk = &self.schema.foreign_keys[fk_idx];

            // Step 2: mismatch check runs before any row-existence test so
            // that bad FK targets are reported even when the parent table is
            // empty (matches SQLite behaviour and fkey1-6.1 / fkey5-11.1).
            // Mismatch is a schema-level error and is *never* deferred:
            // SQLite reports it immediately even with INITIALLY DEFERRED.
            if let Some((child, parent)) =
                crate::foreign_key_check::detect_fk_mismatch(self.db, self.table_name, fk)
            {
                return Err(ExecutorError::ForeignKeyMismatch { child, parent });
            }

            // Step 3: skip row-existence check if any FK value is NULL
            // (stored as None by the column-pass phase).
            let Some(ref fk_values) = fk_values else {
                continue;
            };

            // Steps 4-6: shared per-FK row-existence + self-FK + defer decision.
            // `batch_full_rows` carries the previously-validated rows from the
            // same multi-row VALUES list so self-referential FKs can resolve
            // against siblings (fkey1-5.1: `INSERT INTO t11 VALUES
            // (1,NULL),(2,1),(3,2)` with t11.parent REFERENCES t11.x).
            match crate::foreign_key_check::check_fk_row_existence(
                self.db,
                self.table_name,
                fk,
                fk_idx,
                fk_values,
                full_row_values,
                self.batch_full_rows,
            )? {
                crate::foreign_key_check::FkRowCheck::Ok => continue,
                crate::foreign_key_check::FkRowCheck::Deferred(v) => {
                    deferred_violations.push(v);
                    continue;
                }
                crate::foreign_key_check::FkRowCheck::Violation => {
                    return Err(ExecutorError::ConstraintViolation(format!(
                        "FOREIGN KEY constraint '{}' violated: key ({}) not found in table '{}'",
                        fk.name.as_deref().unwrap_or(""),
                        fk.column_names.join(", "),
                        fk.parent_table
                    )));
                }
            }
        }

        Ok(())
    }
}
