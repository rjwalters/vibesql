//! Foreign key constraint validation for UPDATE operations

use vibesql_storage::Database;

use crate::errors::ExecutorError;

/// Validator for foreign key constraints
pub struct ForeignKeyValidator;

impl ForeignKeyValidator {
    /// Validate FOREIGN KEY constraints for a new row
    ///
    /// Checks that all foreign key values in the row reference existing parent rows.
    /// NULL values in foreign keys are allowed (not considered violations).
    pub fn validate_constraints(
        db: &Database,
        table_name: &str,
        row_values: &[vibesql_types::SqlValue],
    ) -> Result<(), ExecutorError> {
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

    /// Check that no child tables reference a row that is about to be deleted or updated.
    ///
    /// This is called before updating a primary key to ensure referential integrity.
    /// If foreign keys have ON UPDATE CASCADE, this function will propagate the update to child rows.
    pub fn check_no_child_references(
        db: &mut Database,
        parent_table_name: &str,
        parent_row: &vibesql_storage::Row,
        new_parent_row: &vibesql_storage::Row,
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

        let old_parent_key_values: Vec<vibesql_types::SqlValue> =
            pk_indices.iter().map(|&idx| parent_row.values[idx].clone()).collect();

        let new_parent_key_values: Vec<vibesql_types::SqlValue> =
            pk_indices.iter().map(|&idx| new_parent_row.values[idx].clone()).collect();

        // Optimization: Check if any table in the database has foreign keys at all
        // If not, skip the expensive scan of all tables
        let has_any_fks = db.catalog.list_tables().iter().any(|table_name| {
            db.catalog
                .get_table(table_name)
                .map(|schema| !schema.foreign_keys.is_empty())
                .unwrap_or(false)
        });

        if !has_any_fks {
            return Ok(());
        }

        // Collect cascade updates to apply after scanning (to avoid borrow checker issues)
        let mut cascade_updates: Vec<(String, Vec<(usize, vibesql_storage::Row)>)> = Vec::new();

        // Scan all tables in the database to find foreign keys that reference this table.
        for table_name in db.catalog.list_tables() {
            let child_schema = db.catalog.get_table(&table_name).unwrap().clone();

            // Skip tables without foreign keys (optimization)
            if child_schema.foreign_keys.is_empty() {
                continue;
            }

            for fk in &child_schema.foreign_keys {
                if fk.parent_table != parent_table_name {
                    continue;
                }

                // Get the child table and find matching rows
                let child_table = db.get_table(&table_name).unwrap();
                let matching_rows: Vec<(usize, vibesql_storage::Row)> = child_table
                    .scan()
                    .iter()
                    .enumerate()
                    .filter_map(|(idx, child_row)| {
                        let child_fk_values: Vec<vibesql_types::SqlValue> = fk
                            .column_indices
                            .iter()
                            .map(|&col_idx| child_row.values[col_idx].clone())
                            .collect();

                        if child_fk_values == old_parent_key_values {
                            Some((idx, child_row.clone()))
                        } else {
                            None
                        }
                    })
                    .collect();

                if !matching_rows.is_empty() {
                    // Check the referential action
                    match fk.on_update {
                        vibesql_catalog::ReferentialAction::Cascade => {
                            // Prepare cascade updates for this table
                            let updated_rows: Vec<(usize, vibesql_storage::Row)> = matching_rows
                                .into_iter()
                                .map(|(row_idx, mut child_row)| {
                                    // Update the FK columns to match the new parent key
                                    for (fk_col_idx, new_parent_val) in
                                        fk.column_indices.iter().zip(&new_parent_key_values)
                                    {
                                        child_row.values[*fk_col_idx] = new_parent_val.clone();
                                    }
                                    (row_idx, child_row)
                                })
                                .collect();

                            cascade_updates.push((table_name.clone(), updated_rows));
                        }
                        vibesql_catalog::ReferentialAction::SetNull => {
                            // Set child FK columns to NULL
                            let updated_rows: Vec<(usize, vibesql_storage::Row)> = matching_rows
                                .into_iter()
                                .map(|(row_idx, mut child_row)| {
                                    // Set FK columns to NULL
                                    for &fk_col_idx in &fk.column_indices {
                                        child_row.values[fk_col_idx] =
                                            vibesql_types::SqlValue::Null;
                                    }
                                    (row_idx, child_row)
                                })
                                .collect();

                            cascade_updates.push((table_name.clone(), updated_rows));
                        }
                        vibesql_catalog::ReferentialAction::SetDefault => {
                            // Set child FK columns to their default values by evaluating expressions
                            // First, collect default expressions (clone to avoid holding borrow)
                            let default_exprs: Vec<Option<vibesql_ast::Expression>> = fk
                                .column_indices
                                .iter()
                                .map(|&fk_col_idx| {
                                    child_schema.columns[fk_col_idx].default_value.clone()
                                })
                                .collect();

                            // Evaluate default values for each FK column
                            let mut default_values: Vec<vibesql_types::SqlValue> = Vec::new();
                            for default_expr_opt in default_exprs {
                                let default_value = if let Some(default_expr) = default_expr_opt {
                                    // Evaluate the default expression
                                    match default_expr {
                                        vibesql_ast::Expression::NextValue { sequence_name } => {
                                            // Get the next value from the sequence
                                            let seq =
                                                db.catalog
                                                    .get_sequence_mut(&sequence_name)
                                                    .map_err(|e| {
                                                        ExecutorError::UnsupportedExpression(
                                                            format!("Sequence error: {:?}", e),
                                                        )
                                                    })?;
                                            let next_val = seq.next_value().map_err(|e| {
                                                ExecutorError::ConstraintViolation(format!(
                                                    "Sequence error: {}",
                                                    e
                                                ))
                                            })?;
                                            vibesql_types::SqlValue::Integer(next_val)
                                        }
                                        _ => crate::insert::defaults::evaluate_default_expression(
                                            &default_expr,
                                        )?,
                                    }
                                } else {
                                    // No default value defined, use NULL
                                    vibesql_types::SqlValue::Null
                                };
                                default_values.push(default_value);
                            }

                            // Apply default values to matching rows
                            let updated_rows: Vec<(usize, vibesql_storage::Row)> = matching_rows
                                .into_iter()
                                .map(|(row_idx, mut child_row)| {
                                    // Set FK columns to their default values
                                    for (i, &fk_col_idx) in fk.column_indices.iter().enumerate() {
                                        child_row.values[fk_col_idx] = default_values[i].clone();
                                    }
                                    (row_idx, child_row)
                                })
                                .collect();

                            cascade_updates.push((table_name.clone(), updated_rows));
                        }
                        vibesql_catalog::ReferentialAction::NoAction
                        | vibesql_catalog::ReferentialAction::Restrict => {
                            // Block the update when child references exist
                            return Err(ExecutorError::ConstraintViolation(format!(
                                "FOREIGN KEY constraint violation: cannot update a parent row when a foreign key constraint exists. The conflict occurred in table \'{}\', constraint \'{}\'.",
                                table_name,
                                fk.name.as_deref().unwrap_or(""),
                            )));
                        }
                    }
                }
            }
        }

        // Apply cascade updates
        for (table_name, updates) in cascade_updates {
            let child_table = db.get_table_mut(&table_name).unwrap();
            for (row_idx, new_row) in updates {
                child_table
                    .update_row(row_idx, new_row)
                    .map_err(|e| ExecutorError::StorageError(e.to_string()))?;
            }
            // Rebuild indexes after updates (following the same pattern as DELETE operations)
            db.rebuild_indexes(&table_name);
        }

        Ok(())
    }
}
