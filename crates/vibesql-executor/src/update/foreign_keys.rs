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
    ///
    /// The `old_row_values` parameter carries the pre-update (`OLD`) row so
    /// that self-referential foreign keys are checked against the row's
    /// *post-update* state.
    ///
    /// SQLite evaluates a self-referential FK against the table as it will
    /// look after the UPDATE is applied: the row being updated contributes
    /// its NEW parent-key (not its OLD one). Two corrections follow from
    /// this, both of which the plain (`old_row_values = None`) path gets
    /// wrong for a self-referential constraint:
    ///
    /// 1. **Self-rescue** — an UPDATE that sets a row's own parent-key equal
    ///    to its FK value (e.g. `UPDATE self SET a=14, b=14` on a
    ///    `b REFERENCES self(a)` table) satisfies the constraint via the
    ///    row itself. Without the OLD row we still scan for the new parent
    ///    key, which does not yet exist in the stored (pre-update) table,
    ///    and raise a spurious violation (fkey2-16.1.*.6 false positive).
    ///
    /// 2. **Old-row exclusion** — an UPDATE that moves a row's parent-key
    ///    away (e.g. `UPDATE self SET a=15` where the row was `(14, 14)`)
    ///    must *not* be rescued by its own stale OLD parent-key still sitting
    ///    in the table during the scan; that OLD row is about to be
    ///    overwritten. Excluding it exposes the real violation
    ///    (fkey2-16.1.*.4 false negative).
    ///
    /// Non-self-referential FKs are unaffected: the parent table is a
    /// different table, so neither the self-rescue nor the exclusion applies.
    pub fn collect_constraints_with_old(
        db: &Database,
        table_name: &str,
        row_values: &[vibesql_types::SqlValue],
        old_row_values: Option<&[vibesql_types::SqlValue]>,
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

            // Self-referential FKs are evaluated against the post-update row
            // (see the doc comment). Only relevant when the parent table is
            // this very table.
            let self_ref = fk.parent_table.eq_ignore_ascii_case(table_name);

            // Correction 1 (self-rescue): if the NEW row's own parent-key
            // equals its FK value, the row satisfies its own constraint.
            if self_ref
                && parent_indices.iter().zip(&fk_values).enumerate().all(
                    |(i, (&parent_idx, fk_val))| match row_values.get(parent_idx) {
                        Some(parent_val) => crate::foreign_key_check::fk_values_equal(
                            fk_val,
                            parent_val,
                            parent_collations.get(i).and_then(|c| c.as_deref()),
                        ),
                        None => false,
                    },
                )
            {
                continue;
            }

            // Correction 2 (old-row exclusion): for a self-referential FK,
            // skip the pre-update version of the row being changed — it is
            // about to be overwritten and must not satisfy the new FK value.
            let excluded_old: Option<&[vibesql_types::SqlValue]> =
                if self_ref { old_row_values } else { None };
            let is_excluded = |parent_row: &[vibesql_types::SqlValue]| -> bool {
                excluded_old.map(|old| parent_row == old).unwrap_or(false)
            };
            let row_satisfies = |parent_row: &vibesql_storage::Row| -> bool {
                if is_excluded(&parent_row.values) {
                    return false;
                }
                parent_indices.iter().zip(&fk_values).enumerate().all(
                    |(i, (&parent_idx, fk_val))| match parent_row.get(parent_idx) {
                        Some(parent_val) => crate::foreign_key_check::fk_values_equal(
                            fk_val,
                            parent_val,
                            parent_collations.get(i).and_then(|c| c.as_deref()),
                        ),
                        None => false,
                    },
                )
            };

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
                    parent_table
                        .scan_visible(&snapshot)
                        .any(|(_, parent_row)| row_satisfies(parent_row))
                }
                #[cfg(not(feature = "mvcc_enabled"))]
                {
                    let _ = &snapshot;
                    parent_table.scan().iter().any(row_satisfies)
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

        // SQLite fires no referential action and raises no violation when an
        // UPDATE does not actually change the parent key. Re-assigning a
        // parent-key column to the value it already holds (e.g.
        // `UPDATE t1 SET a = 1` when `a` is already 1, or `UPDATE t1 SET a = a`)
        // leaves every existing child reference valid, so there is nothing to
        // cascade, set-null/default, or restrict. Without this early-out the
        // NO ACTION / RESTRICT paths below see child rows still matching the
        // (unchanged) old key and raise a spurious "cannot update a parent
        // row" violation (fkey2-1.*.13: `UPDATE t1 SET a = 1` /
        // `UPDATE t7 SET b = 1` expect success). Plain equality is a
        // conservative test: when the stored representations differ (e.g.
        // 1 vs 1.0) we fall through to the full affinity-aware check below,
        // preserving prior behaviour.
        if old_parent_key_values == new_parent_key_values {
            return Ok(());
        }

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

        // Collect cascade updates to apply after scanning (to avoid borrow
        // checker issues). Each entry carries the row index, the OLD child
        // row, and the NEW child row so the cascade can fire the child
        // table's BEFORE/AFTER UPDATE triggers with both pseudo-rows (#5440).
        // Each entry also carries the FK constraint + its index so that a
        // cascade-fired BEFORE UPDATE trigger RAISE(IGNORE) (SkipRow) can run
        // the statement-end orphan FK check on the surviving row (#5465).
        #[allow(clippy::type_complexity)]
        let mut cascade_updates: Vec<(
            String,
            vibesql_catalog::ForeignKeyConstraint,
            usize,
            Vec<(usize, vibesql_storage::Row, vibesql_storage::Row)>,
        )> = Vec::new();

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
                let parent_collations = crate::foreign_key_check::parent_collations_for_fk(db, fk);

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

                // Self-referential exclusion: when the child table *is* the
                // parent table, the row being updated is its own child. That
                // row is updated in place by this same statement, and its
                // post-update FK value is validated separately by
                // `collect_constraints_with_old` (the child-side check). It
                // must therefore not count as an orphaned child of its own
                // parent-key change — otherwise `UPDATE self SET a=14, b=14`
                // (which moves both the parent key and the self-reference in
                // lock-step) would wrongly trip "cannot update a parent row"
                // (fkey2-16.1.*). Identify that row by full equality to the
                // OLD row still present in the pre-update scan.
                let self_ref_table = table_name.eq_ignore_ascii_case(parent_table_name);
                let excluded_self_row: &[vibesql_types::SqlValue] = &parent_row.values;
                let matches_fk = |child_row: &vibesql_storage::Row| -> bool {
                    if self_ref_table && child_row.values.as_slice() == excluded_self_row {
                        return false;
                    }
                    let child_fk_values: Vec<vibesql_types::SqlValue> = fk
                        .column_indices
                        .iter()
                        .map(|&col_idx| child_row.values[col_idx].clone())
                        .collect();
                    child_fk_values.iter().zip(&old_parent_key_values).enumerate().all(
                        |(i, (cv, pv))| {
                            crate::foreign_key_check::fk_values_equal(
                                cv,
                                pv,
                                parent_collations.get(i).and_then(|c| c.as_deref()),
                            )
                        },
                    )
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
                            // Prepare cascade updates for this table, keeping
                            // the OLD row so child UPDATE triggers see it.
                            let updated_rows: Vec<(
                                usize,
                                vibesql_storage::Row,
                                vibesql_storage::Row,
                            )> = matching_rows
                                .into_iter()
                                .map(|(row_idx, old_child_row)| {
                                    let mut child_row = old_child_row.clone();
                                    // Update the FK columns to match the new parent key
                                    for (fk_col_idx, new_parent_val) in
                                        fk.column_indices.iter().zip(&new_parent_key_values)
                                    {
                                        child_row.values[*fk_col_idx] = new_parent_val.clone();
                                    }
                                    (row_idx, old_child_row, child_row)
                                })
                                .collect();

                            cascade_updates.push((
                                table_name.clone(),
                                fk.clone(),
                                fk_idx,
                                updated_rows,
                            ));
                        }
                        vibesql_catalog::ReferentialAction::SetNull => {
                            // Set child FK columns to NULL
                            let updated_rows: Vec<(
                                usize,
                                vibesql_storage::Row,
                                vibesql_storage::Row,
                            )> = matching_rows
                                .into_iter()
                                .map(|(row_idx, old_child_row)| {
                                    let mut child_row = old_child_row.clone();
                                    // Set FK columns to NULL
                                    for &fk_col_idx in &fk.column_indices {
                                        child_row.values[fk_col_idx] =
                                            vibesql_types::SqlValue::Null;
                                    }
                                    (row_idx, old_child_row, child_row)
                                })
                                .collect();

                            cascade_updates.push((
                                table_name.clone(),
                                fk.clone(),
                                fk_idx,
                                updated_rows,
                            ));
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
                            let updated_rows: Vec<(
                                usize,
                                vibesql_storage::Row,
                                vibesql_storage::Row,
                            )> = matching_rows
                                .into_iter()
                                .map(|(row_idx, old_child_row)| {
                                    let mut child_row = old_child_row.clone();
                                    // Set FK columns to their default values
                                    for (i, &fk_col_idx) in fk.column_indices.iter().enumerate() {
                                        child_row.values[fk_col_idx] = default_values[i].clone();
                                    }
                                    (row_idx, old_child_row, child_row)
                                })
                                .collect();

                            cascade_updates.push((
                                table_name.clone(),
                                fk.clone(),
                                fk_idx,
                                updated_rows,
                            ));
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
        //
        // FK cascade fires the child table's BEFORE/AFTER UPDATE row
        // triggers for every cascaded row, matching sqlite3 3.51 (#5440).
        // sqlite3 fires these regardless of the `recursive_triggers`
        // pragma; the depth-16 RecursionGuard inside the firing helpers
        // bounds multi-level FK chains. Per row the order is
        //   BEFORE child trigger -> update -> AFTER child trigger
        // (verified against sqlite3 3.51.0). A RAISE(IGNORE) (SkipRow) in a
        // BEFORE trigger abandons that child row's cascade update; a
        // RAISE(ABORT|FAIL|ROLLBACK) propagates as Err and aborts the
        // whole statement.
        let txn_id = db.transaction_id();
        for (table_name, fk, fk_idx, mut updates) in cascade_updates {
            for (_row_idx, _old_row, new_row) in updates.iter_mut() {
                vibesql_storage::stamp_xmin_for_write(new_row, txn_id);
                new_row.xmax = None;
            }

            let has_child_update_triggers = db
                .catalog
                .get_triggers_for_table(&table_name, Some(vibesql_ast::TriggerEvent::Update(None)))
                .next()
                .is_some();

            for (row_idx, old_row, new_row) in updates {
                // BEFORE UPDATE row triggers on the child.
                if has_child_update_triggers {
                    let outcome = crate::TriggerFirer::execute_before_triggers(
                        db,
                        &table_name,
                        vibesql_ast::TriggerEvent::Update(None),
                        Some(&old_row),
                        Some(&new_row),
                    )?;
                    if outcome == crate::TriggerOutcome::SkipRow {
                        // RAISE(IGNORE) abandons this cascade update: the
                        // surviving child keeps its OLD FK value, which
                        // references the parent key that is about to change
                        // (the parent UPDATE is applied after this cascade
                        // returns). That is an orphaned FK reference and must
                        // trip the statement-end FK check, matching sqlite3
                        // 3.51 (#5465). Immediate FK -> raise now (statement
                        // savepoint rolls the statement back); deferred FK ->
                        // queue the surviving OLD row for the COMMIT re-check.
                        let should_defer = in_txn && (fk.initially_deferred || session_defer);
                        if should_defer {
                            db.queue_deferred_fk_violation(DeferredFkViolation {
                                child_table: table_name.clone(),
                                fk_index: fk_idx,
                                child_row: old_row.values.to_vec(),
                                kind: DeferredFkViolationKind::ChildInsertOrUpdate,
                            });
                        } else {
                            return Err(ExecutorError::ConstraintViolation(
                                "FOREIGN KEY constraint failed".to_string(),
                            ));
                        }
                        continue;
                    }
                }

                // A cascading UPDATE/SET NULL/SET DEFAULT is itself an UPDATE
                // on the child table and must satisfy the child's own NOT
                // NULL/CHECK constraints, exactly like a user-issued UPDATE
                // would. Without this, a CASCADE chain that lands on a value
                // forbidden by the child's CHECK constraint silently wrote the
                // row anyway instead of aborting the whole outer statement
                // (fkey2-3.1.3: `ab` -ON UPDATE CASCADE-> `cd` -ON UPDATE
                // CASCADE-> `ef` landing on `e=5` must trip `CHECK(e!=5)`).
                let child_schema_for_check = db.catalog.get_table(&table_name).unwrap().clone();
                crate::update::constraints::ConstraintValidator::new(&child_schema_for_check)
                    .validate_row_skip_uniqueness(&table_name, &new_row)?;

                // SET DEFAULT in particular can rewrite the FK column(s) to a
                // default value that is not itself a valid parent key (e.g.
                // the default references a parent row that no longer
                // exists) -- re-validate the rewritten row's own foreign keys
                // exactly as a user-issued UPDATE would (mirrors the
                // DELETE-side ON DELETE SET DEFAULT check, fkey2-9.1.5).
                //
                // This must NOT run for CASCADE: the cascaded value is the
                // parent's brand-new key, which this very function applies to
                // the parent's own row *after* this child-cascade loop
                // returns (Step 7 runs before the parent row is written) --
                // re-checking existence here would spuriously fail against
                // the not-yet-written parent key. SET NULL is unaffected
                // either way since NULL always short-circuits the FK check.
                if matches!(fk.on_update, vibesql_catalog::ReferentialAction::SetDefault)
                    && !child_schema_for_check.foreign_keys.is_empty()
                {
                    let deferred = Self::collect_constraints_with_old(
                        db,
                        &table_name,
                        &new_row.values,
                        Some(&old_row.values),
                    )?;
                    for violation in deferred {
                        db.queue_deferred_fk_violation(violation);
                    }
                }

                // Multi-level cascade: if this rewrite changed the child's own
                // primary key (e.g. the FK column doubles as the child's PK),
                // recursively propagate to grandchildren that reference *this*
                // table before the row is written, mirroring the DELETE-side
                // cascade's recursive `check_no_child_references` call for
                // multi-level chains (fkey2-3.1.*).
                Self::check_no_child_references(db, &table_name, &old_row, &new_row)?;

                let child_table = db.get_table_mut(&table_name).unwrap();
                child_table
                    .update_row(row_idx, new_row.clone())
                    .map_err(|e| ExecutorError::StorageError(e.to_string()))?;

                // AFTER UPDATE row triggers fire once the row is updated.
                if has_child_update_triggers {
                    let _after = crate::TriggerFirer::execute_after_triggers(
                        db,
                        &table_name,
                        vibesql_ast::TriggerEvent::Update(None),
                        Some(&old_row),
                        Some(&new_row),
                    )?;
                }
            }
            // Rebuild indexes after updates (following the same pattern as DELETE operations)
            db.rebuild_indexes(&table_name);
            // Cascade mutations bypass the Database-level DML API, so invalidate the
            // child table's columnar cache explicitly to avoid serving stale reads (#5876).
            db.invalidate_columnar_cache(&table_name);
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
