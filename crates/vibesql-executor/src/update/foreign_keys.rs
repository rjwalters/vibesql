//! Foreign key constraint validation for UPDATE operations

use vibesql_storage::{Database, DeferredFkViolation, DeferredFkViolationKind};

use crate::errors::ExecutorError;

/// Validator for foreign key constraints
pub struct ForeignKeyValidator;

impl ForeignKeyValidator {
    /// Validate FOREIGN KEY constraints for a new row, returning any
    /// deferred violations rather than queueing them directly.
    ///
    /// Checks that all foreign key values in the row reference existing parent rows.
    /// NULL values in foreign keys are allowed (not considered violations).
    ///
    /// Phase C2 of #5085: when the constraint is `INITIALLY DEFERRED`
    /// or the session has `PRAGMA defer_foreign_keys=ON` (and a
    /// transaction is active), a missing parent row produces a
    /// `DeferredFkViolation` in the returned vector instead of an
    /// immediate `Err`. The caller must push the returned violations
    /// onto the transaction queue once any immutable borrow of
    /// `database` is released.
    pub fn collect_constraints(
        db: &Database,
        table_name: &str,
        row_values: &[vibesql_types::SqlValue],
    ) -> Result<Vec<DeferredFkViolation>, ExecutorError> {
        let mut deferred: Vec<DeferredFkViolation> = Vec::new();

        // Skip FK enforcement when PRAGMA foreign_keys is OFF (default)
        if !db.foreign_keys_enabled() {
            return Ok(deferred);
        }

        let session_defer = db.defer_foreign_keys();
        let in_txn = db.in_transaction();

        let schema = db
            .catalog
            .get_table(table_name)
            .ok_or_else(|| ExecutorError::TableNotFound(table_name.to_string()))?;

        for (fk_idx, fk) in schema.foreign_keys.iter().enumerate() {
            // Mismatch check runs before any row-existence test so that bad
            // FK targets are reported even when the parent table is empty.
            // Mismatch is never deferred (matches SQLite behaviour).
            if let Some((child, parent)) =
                crate::foreign_key_check::detect_fk_mismatch(db, table_name, fk)
            {
                return Err(ExecutorError::ForeignKeyMismatch { child, parent });
            }

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

            let parent_collations = crate::foreign_key_check::parent_collations_for_fk(db, fk);
            let parent_indices = crate::foreign_key_check::resolved_parent_indices_for_fk(db, fk);

            // Phase 1d follow-up (#5205): the parent-existence check on
            // the UPDATE path must honor MVCC visibility for the same
            // reason as INSERT — an uncommitted concurrent INSERT on the
            // parent must not satisfy this child's FK. Off-state
            // (`mvcc_enabled` OFF) preserves the previous `scan().iter()`
            // semantics (including bitmap-deleted physical rows).
            let snapshot = crate::mvcc::read_snapshot(db);
            let key_exists = {
                #[cfg(feature = "mvcc_enabled")]
                {
                    parent_table.scan_visible(&snapshot).any(|(_, parent_row)| {
                        parent_indices.iter().zip(&fk_values).enumerate().all(
                            |(i, (&parent_idx, fk_val))| match parent_row.get(parent_idx) {
                                Some(parent_val) => {
                                    crate::foreign_key_check::fk_values_equal(
                                        fk_val,
                                        parent_val,
                                        parent_collations.get(i).and_then(|c| c.as_deref()),
                                    )
                                }
                                None => false,
                            },
                        )
                    })
                }
                #[cfg(not(feature = "mvcc_enabled"))]
                {
                    let _ = &snapshot;
                    parent_table.scan().iter().any(|parent_row| {
                        parent_indices.iter().zip(&fk_values).enumerate().all(
                            |(i, (&parent_idx, fk_val))| match parent_row.get(parent_idx) {
                                Some(parent_val) => {
                                    crate::foreign_key_check::fk_values_equal(
                                        fk_val,
                                        parent_val,
                                        parent_collations.get(i).and_then(|c| c.as_deref()),
                                    )
                                }
                                None => false,
                            },
                        )
                    })
                }
            };

            if key_exists {
                continue;
            }

            let should_defer = in_txn && (fk.initially_deferred || session_defer);
            if should_defer {
                deferred.push(DeferredFkViolation {
                    child_table: table_name.to_string(),
                    fk_index: fk_idx,
                    child_row: row_values.to_vec(),
                    kind: DeferredFkViolationKind::ChildInsertOrUpdate,
                });
                continue;
            }

            return Err(ExecutorError::ConstraintViolation(format!(
                "FOREIGN KEY constraint \'{}\' violated: key ({}) not found in table \'{}\'",
                fk.name.as_deref().unwrap_or(""),
                fk.column_names.join(", "),
                fk.parent_table
            )));
        }

        Ok(deferred)
    }

    /// Check that no child tables reference a row that is about to be deleted or updated.
    ///
    /// This is called before updating a primary key to ensure referential integrity.
    /// If foreign keys have ON UPDATE CASCADE, this function will propagate the update to child
    /// rows.
    pub fn check_no_child_references(
        db: &mut Database,
        parent_table_name: &str,
        parent_row: &vibesql_storage::Row,
        new_parent_row: &vibesql_storage::Row,
    ) -> Result<(), ExecutorError> {
        // Skip FK enforcement when PRAGMA foreign_keys is OFF (default)
        if !db.foreign_keys_enabled() {
            return Ok(());
        }

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

        // Phase C2 of #5085: cache session defer + transaction state.
        let session_defer = db.defer_foreign_keys();
        let in_txn = db.in_transaction();

        // Collect cascade updates to apply after scanning (to avoid borrow checker issues)
        let mut cascade_updates: Vec<(String, Vec<(usize, vibesql_storage::Row)>)> = Vec::new();

        // Phase C2: collected NO ACTION orphans to queue after scanning
        // (queueing requires &mut Database, which conflicts with the
        // catalog/table borrows held inside the scan loop).
        let mut deferred_parent_orphans: Vec<(
            String,
            vibesql_catalog::ForeignKeyConstraint,
            usize,
            Vec<(usize, vibesql_storage::Row)>,
        )> = Vec::new();

        // Scan all tables in the database to find foreign keys that reference this table.
        for table_name in db.catalog.list_tables() {
            let child_schema = db.catalog.get_table(&table_name).unwrap().clone();

            // Skip tables without foreign keys (optimization)
            if child_schema.foreign_keys.is_empty() {
                continue;
            }

            for (fk_idx, fk) in child_schema.foreign_keys.iter().enumerate() {
                // Use case-insensitive comparison for SQL identifier matching
                if !fk.parent_table.eq_ignore_ascii_case(parent_table_name) {
                    continue;
                }

                // Resolve parent-side collations before borrowing the
                // child table so the FK comparison honors NOCASE/RTRIM
                // on the parent key (#5147).
                let parent_collations =
                    crate::foreign_key_check::parent_collations_for_fk(db, fk);

                // Get the child table and find matching rows.
                //
                // Phase 1d follow-up (#5205): the child-reference scan
                // must respect MVCC visibility. A child row that has been
                // deleted by a concurrent committed transaction must not
                // count as "referencing" the parent under our snapshot.
                // Equally, our own in-txn child writes participate via
                // the widened BEGIN-time snapshot (#5223). Off-state
                // (`mvcc_enabled` OFF) preserves the pre-MVCC behavior of
                // walking every physical row via `scan().iter()`.
                let snapshot = crate::mvcc::read_snapshot(db);
                let child_table = db.get_table(&table_name).unwrap();
                let matches_fk = |child_row: &vibesql_storage::Row| -> bool {
                    let child_fk_values: Vec<vibesql_types::SqlValue> = fk
                        .column_indices
                        .iter()
                        .map(|&col_idx| child_row.values[col_idx].clone())
                        .collect();
                    child_fk_values
                        .iter()
                        .zip(&old_parent_key_values)
                        .enumerate()
                        .all(|(i, (cv, pv))| {
                            crate::foreign_key_check::fk_values_equal(
                                cv,
                                pv,
                                parent_collations.get(i).and_then(|c| c.as_deref()),
                            )
                        })
                };
                let matching_rows: Vec<(usize, vibesql_storage::Row)> = {
                    #[cfg(feature = "mvcc_enabled")]
                    {
                        child_table
                            .scan_visible(&snapshot)
                            .filter_map(|(idx, child_row)| {
                                if matches_fk(child_row) {
                                    Some((idx, child_row.clone()))
                                } else {
                                    None
                                }
                            })
                            .collect()
                    }
                    #[cfg(not(feature = "mvcc_enabled"))]
                    {
                        let _ = &snapshot;
                        child_table
                            .scan()
                            .iter()
                            .enumerate()
                            .filter_map(|(idx, child_row)| {
                                if matches_fk(child_row) {
                                    Some((idx, child_row.clone()))
                                } else {
                                    None
                                }
                            })
                            .collect()
                    }
                };

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
                            // Set child FK columns to their default values by evaluating
                            // expressions First, collect default
                            // expressions (clone to avoid holding borrow)
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
                        vibesql_catalog::ReferentialAction::Restrict => {
                            // RESTRICT is immediate by default, but the
                            // session pragma `defer_foreign_keys=ON`
                            // delays it until COMMIT (EVIDENCE-OF
                            // R-18981-16292; see fkey6-3.2.3). Per-
                            // constraint INITIALLY DEFERRED does *not*
                            // defer RESTRICT — only the session pragma
                            // does. Mirrors the DELETE-side handling in
                            // delete/integrity.rs.
                            let should_defer = in_txn && session_defer;
                            if should_defer {
                                deferred_parent_orphans.push((
                                    table_name.clone(),
                                    fk.clone(),
                                    fk_idx,
                                    matching_rows.clone(),
                                ));
                            } else {
                                return Err(ExecutorError::ConstraintViolation(format!(
                                    "FOREIGN KEY constraint violation: cannot update a parent row when a foreign key constraint exists. The conflict occurred in table \'{}\', constraint \'{}\'.",
                                    table_name,
                                    fk.name.as_deref().unwrap_or(""),
                                )));
                            }
                        }
                        vibesql_catalog::ReferentialAction::NoAction => {
                            // NO ACTION can be deferred (Phase C2 of
                            // #5085). When deferred, queue every
                            // orphaned child row for COMMIT-time
                            // re-validation; otherwise fail immediately.
                            let should_defer = in_txn && (fk.initially_deferred || session_defer);
                            if should_defer {
                                deferred_parent_orphans.push((
                                    table_name.clone(),
                                    fk.clone(),
                                    fk_idx,
                                    matching_rows.clone(),
                                ));
                            } else {
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
        }

        // Apply cascade updates. Phase 1c (Issue #5150 / #5136): stamp
        // xmin on every cascade-update new row version. Off-state is a
        // no-op. We capture the txn id once outside the per-table loop
        // since the txn doesn't change within an UPDATE.
        let txn_id = db.transaction_id();
        for (table_name, mut updates) in cascade_updates {
            for (_row_idx, new_row) in updates.iter_mut() {
                vibesql_storage::stamp_xmin_for_write(new_row, txn_id);
                new_row.xmax = None;
            }
            let child_table = db.get_table_mut(&table_name).unwrap();
            for (row_idx, new_row) in updates {
                child_table
                    .update_row(row_idx, new_row)
                    .map_err(|e| ExecutorError::StorageError(e.to_string()))?;
            }
            // Rebuild indexes after updates (following the same pattern as DELETE operations)
            db.rebuild_indexes(&table_name);
        }

        // Phase C2 of #5085: queue NO ACTION orphans onto the deferred
        // FK queue for COMMIT-time re-validation.
        for (table_name, _fk, fk_idx, matching_rows) in deferred_parent_orphans {
            for (_row_idx, child_row) in matching_rows {
                db.queue_deferred_fk_violation(DeferredFkViolation {
                    child_table: table_name.clone(),
                    fk_index: fk_idx,
                    child_row: child_row.values.to_vec(),
                    kind: DeferredFkViolationKind::ChildInsertOrUpdate,
                });
            }
        }

        Ok(())
    }
}
