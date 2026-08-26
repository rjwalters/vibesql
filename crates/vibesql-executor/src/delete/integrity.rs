//! Foreign key integrity checking and enforcement for DELETE operations

use vibesql_catalog::ReferentialAction;
use vibesql_storage::{DeferredFkViolation, DeferredFkViolationKind};
use vibesql_types::SqlValue;

use crate::errors::ExecutorError;

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
    check_child_references_impl(db, parent_table_name, parent_row, false)
}

/// Emulate SQLite's implicit `DELETE FROM <table>` that a `DROP TABLE`
/// performs when foreign keys are enabled (`sqlite3FkDropTable`).
///
/// EVIDENCE-OF R-14208-23986 / R-11078-03945: the implicit DELETE removes
/// all rows and *may invoke foreign key actions or constraint violations*,
/// but does **not** fire SQL triggers on the dropped table. We therefore run
/// the child-reference enforcement (RESTRICT / NO ACTION raise, CASCADE /
/// SET NULL / SET DEFAULT actions) for every live parent row, but never fire
/// the parent's own DELETE triggers (this path bypasses the DELETE executor).
///
/// Foreign keys where the child table *is* the table being dropped are
/// skipped: a self-referential FK is always satisfied once the entire table
/// (all parent and child rows) disappears together, so it can never block the
/// drop (mirrors SQLite's statement-scoped counter reaching zero after the
/// whole-table delete). Only FKs from *other* tables can be violated.
///
/// EVIDENCE-OF R-57242-37005: any "foreign key mismatch" that would be
/// reported by an equivalent explicit DELETE is ignored here — an FK whose
/// parent-side key columns do not resolve to a valid parent key is skipped
/// rather than raised (handled by `check_child_references_impl` skipping
/// unresolved parent indices).
pub fn check_drop_table_references(
    db: &mut vibesql_storage::Database,
    table_name: &str,
) -> Result<(), ExecutorError> {
    // Skip FK enforcement when PRAGMA foreign_keys is OFF (default). The
    // special DROP-TABLE behavior only applies when foreign keys are enabled
    // (EVIDENCE-OF R-54142-41346).
    if !db.foreign_keys_enabled() {
        return Ok(());
    }

    // Fast exit: only tables *other* than the one being dropped can hold an FK
    // that this drop could violate. A self-referential FK never blocks a drop.
    let has_incoming_fks = db.catalog.list_tables().iter().any(|t| {
        !t.eq_ignore_ascii_case(table_name)
            && db
                .catalog
                .get_table(t)
                .map(|schema| {
                    schema
                        .foreign_keys
                        .iter()
                        .any(|fk| fk.parent_table.eq_ignore_ascii_case(table_name))
                })
                .unwrap_or(false)
    });
    if !has_incoming_fks {
        return Ok(());
    }

    // Snapshot every live parent row before mutating any child table, so the
    // set of rows the implicit DELETE removes is fixed up front (a CASCADE
    // into another table must not perturb this iteration).
    let snapshot = crate::mvcc::read_snapshot(db);
    let parent_rows: Vec<vibesql_storage::Row> = match db.get_table(table_name) {
        Some(t) => t.scan_visible(&snapshot).map(|(_, r)| r.clone()).collect(),
        None => return Ok(()),
    };

    for parent_row in &parent_rows {
        check_child_references_impl(db, table_name, parent_row, true)?;
    }

    Ok(())
}

/// Shared implementation behind [`check_no_child_references`] and
/// [`check_drop_table_references`]. When `exclude_self_table` is true, foreign
/// keys whose child table is `parent_table_name` are skipped (the DROP TABLE
/// self-reference case above); otherwise all child tables are considered.
fn check_child_references_impl(
    db: &mut vibesql_storage::Database,
    parent_table_name: &str,
    parent_row: &vibesql_storage::Row,
    exclude_self_table: bool,
) -> Result<(), ExecutorError> {
    // Skip FK enforcement when PRAGMA foreign_keys is OFF (default)
    if !db.foreign_keys_enabled() {
        return Ok(());
    }

    let _parent_schema = db
        .catalog
        .get_table(parent_table_name)
        .ok_or_else(|| ExecutorError::TableNotFound(parent_table_name.to_string()))?;

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

    // Phase C2: cache session defer + transaction state.
    //
    // RESTRICT is never deferred by `INITIALLY DEFERRED` on the
    // constraint (per SQLite docs), but the session pragma
    // `defer_foreign_keys=ON` *does* delay RESTRICT until COMMIT
    // (EVIDENCE-OF R-18981-16292; see fkey6-3.2.3).
    //
    // NO ACTION can be deferred by either INITIALLY DEFERRED or the
    // session pragma.
    let session_defer = db.defer_foreign_keys();
    let in_txn = db.in_transaction();

    // Phase 1d follow-up (#5205): capture the active snapshot once so
    // every child-reference scan in this DELETE path sees a consistent
    // MVCC view. With the `mvcc_enabled` feature OFF, `scan_visible`
    // reduces to `scan_live` (deleted-bitmap filter only) and the
    // snapshot is unused — behavior is bit-for-bit identical to the
    // previous `scan_live()` callers.
    let snapshot = crate::mvcc::read_snapshot(db);

    // Collect all child tables that reference this parent row
    // We need to collect first to avoid borrowing issues when we mutate.
    //
    // Phase C3 of #5085 / fkey8-3.1: each FK supplies its own
    // `parent_column_indices`, which may differ from the parent's PRIMARY
    // KEY (e.g. `FOREIGN KEY(b, c) REFERENCES self(d, e)` where d/e are
    // backed by a UNIQUE INDEX rather than the PK). The FK match must
    // therefore extract `parent_key_values` from the FK's *parent-side*
    // columns, not from the parent's PK.
    let mut actions_to_perform: Vec<(
        String,
        vibesql_catalog::ForeignKeyConstraint,
        usize,
        ReferentialAction,
        Vec<SqlValue>, // parent_key_values for THIS FK
    )> = Vec::new();

    for table_name in db.catalog.list_tables() {
        // DROP TABLE's implicit DELETE skips self-referential FKs: once the
        // whole table is removed both sides of the reference disappear, so it
        // can never be violated (see `check_drop_table_references`).
        if exclude_self_table && table_name.eq_ignore_ascii_case(parent_table_name) {
            continue;
        }

        let child_schema = db.catalog.get_table(&table_name).unwrap();

        if child_schema.foreign_keys.is_empty() {
            continue;
        }

        for (fk_idx, fk) in child_schema.foreign_keys.iter().enumerate() {
            // Use case-insensitive comparison for SQL identifier matching
            if !fk.parent_table.eq_ignore_ascii_case(parent_table_name) {
                continue;
            }

            // Resolve the FK's parent-side column indices and extract the
            // values from the parent row being deleted.
            let parent_indices = crate::foreign_key_check::resolved_parent_indices_for_fk(db, fk);
            if parent_indices.is_empty()
                || parent_indices.iter().any(|&idx| idx >= parent_row.values.len())
            {
                continue;
            }
            let fk_parent_key_values: Vec<SqlValue> =
                parent_indices.iter().map(|&idx| parent_row.values[idx].clone()).collect();

            // Skip if the parent-side key values are NULL — NULLs don't
            // participate in FK matching.
            if fk_parent_key_values.iter().any(|v| matches!(v, SqlValue::Null)) {
                continue;
            }

            let parent_collations = crate::foreign_key_check::parent_collations_for_fk(db, fk);
            let parent_affinities = crate::foreign_key_check::parent_affinities_for_fk(db, fk);
            // Check if any row in the child table references this parent
            // row's FK-target columns. Phase 1d follow-up (#5205): use
            // `scan_visible(&snapshot)` so the MVCC visibility filter
            // applies. With MVCC OFF, this reduces to `scan_live` and
            // behavior is unchanged.
            // Self-referential exclusion: when the child table *is* the
            // parent table, the row being deleted is its own child. Deleting
            // a row that references itself (e.g. `DELETE FROM self` where the
            // sole row is `(17, 17)` with `b REFERENCES self(a)`) is legal in
            // SQLite — the self-reference disappears together with the row, so
            // it must not count as a blocking child reference (fkey2-16.1.*).
            // Identify that row by full equality to the row being deleted.
            let self_ref_table = table_name.eq_ignore_ascii_case(parent_table_name);
            let child_table = db.get_table(&table_name).unwrap();
            let has_references = child_table.scan_visible(&snapshot).any(|(_, child_row)| {
                if self_ref_table && child_row.values.as_slice() == parent_row.values.as_slice() {
                    return false;
                }
                let child_fk_values: Vec<SqlValue> =
                    fk.column_indices.iter().map(|&idx| child_row.values[idx].clone()).collect();

                // Skip NULL foreign keys (NULLs don't participate in FK constraints)
                if child_fk_values.iter().any(|v| matches!(v, SqlValue::Null)) {
                    return false;
                }

                child_fk_values.iter().zip(&fk_parent_key_values).enumerate().all(
                    |(i, (cv, pv))| {
                        crate::foreign_key_check::fk_values_equal(
                            cv,
                            pv,
                            parent_collations.get(i).and_then(|c| c.as_deref()),
                            parent_affinities
                                .get(i)
                                .copied()
                                .unwrap_or(vibesql_types::TypeAffinity::None),
                        )
                    },
                )
            });

            if has_references {
                actions_to_perform.push((
                    table_name.clone(),
                    fk.clone(),
                    fk_idx,
                    fk.on_delete.clone(),
                    fk_parent_key_values,
                ));
            }
        }
    }

    // Now perform the actions
    for (child_table_name, fk, fk_idx, action, parent_key_values) in actions_to_perform {
        match action {
            ReferentialAction::Restrict => {
                // RESTRICT is immediate by default, but the session
                // pragma `defer_foreign_keys=ON` delays it until COMMIT
                // (EVIDENCE-OF R-18981-16292; see fkey6-3.2.3 and
                // fkey6-3.3.4). Per-constraint INITIALLY DEFERRED does
                // *not* defer RESTRICT — only the session pragma does.
                let should_defer = in_txn && session_defer;
                if should_defer {
                    queue_orphaned_children(db, &child_table_name, &fk, fk_idx, &parent_key_values);
                } else {
                    return Err(ExecutorError::ConstraintViolation(format!(
                        "FOREIGN KEY constraint violation: cannot delete or update a parent row when a foreign key constraint exists. The conflict occurred in table \'{}\', constraint \'{}\'.",
                        child_table_name,
                        fk.name.as_deref().unwrap_or(""),
                    )));
                }
            }
            ReferentialAction::NoAction => {
                // NO ACTION can be deferred (Phase C2 of #5085). Queue
                // every orphaned child row so that COMMIT-time re-check
                // verifies each one finds a parent. If not deferred,
                // fail immediately as before.
                // NOT DEFERRABLE constraints ignore any INITIALLY DEFERRED
                // clause (e_fkey-34.*: only a `DEFERRABLE INITIALLY
                // DEFERRED` constraint may be violated mid-transaction);
                // the session `defer_foreign_keys` override is not gated
                // on `is_deferrable` (EVIDENCE-OF R-18981-16292, fkey6-1.8).
                let should_defer =
                    in_txn && (session_defer || (fk.is_deferrable && fk.initially_deferred));
                if should_defer {
                    queue_orphaned_children(db, &child_table_name, &fk, fk_idx, &parent_key_values);
                } else {
                    return Err(ExecutorError::ConstraintViolation(format!(
                        "FOREIGN KEY constraint violation: cannot delete or update a parent row when a foreign key constraint exists. The conflict occurred in table \'{}\', constraint \'{}\'.",
                        child_table_name,
                        fk.name.as_deref().unwrap_or(""),
                    )));
                }
            }
            ReferentialAction::Cascade => {
                // Delete child rows that reference the parent. A child
                // BEFORE DELETE trigger RAISE(IGNORE) skips that row's
                // cascade, leaving it as an orphan that must still trip the
                // statement-end FK check (#5465).
                cascade_delete(
                    db,
                    &child_table_name,
                    &fk,
                    fk_idx,
                    &parent_key_values,
                    in_txn,
                    session_defer,
                )?;
            }
            ReferentialAction::SetNull => {
                // Set child FK columns to NULL. Fires the child's
                // BEFORE/AFTER UPDATE row triggers; a BEFORE UPDATE
                // RAISE(IGNORE) skips that child's rewrite, leaving an
                // orphan that the statement-end FK check must trip (#5465).
                set_null(
                    db,
                    &child_table_name,
                    &fk,
                    fk_idx,
                    &parent_key_values,
                    in_txn,
                    session_defer,
                )?;
            }
            ReferentialAction::SetDefault => {
                // Set child FK columns to their default values. Fires the
                // child's BEFORE/AFTER UPDATE row triggers; same
                // RAISE(IGNORE) -> orphan-check semantics as SET NULL.
                set_default(
                    db,
                    &child_table_name,
                    &fk,
                    fk_idx,
                    &parent_key_values,
                    in_txn,
                    session_defer,
                )?;
            }
        }
    }

    Ok(())
}

/// Queue every orphaned child row (referencing the deleted parent) onto
/// the transaction's deferred FK violation queue. Used by NO ACTION
/// when the constraint is deferred (Phase C2 of #5085).
fn queue_orphaned_children(
    db: &mut vibesql_storage::Database,
    child_table_name: &str,
    fk: &vibesql_catalog::ForeignKeyConstraint,
    fk_idx: usize,
    parent_key_values: &[SqlValue],
) {
    // Resolve parent-side collations up front so the immutable borrow
    // inside the scan loop only sees the child table (#5147).
    let parent_collations = crate::foreign_key_check::parent_collations_for_fk(db, fk);
    let parent_affinities = crate::foreign_key_check::parent_affinities_for_fk(db, fk);
    // Phase 1d follow-up (#5205): respect MVCC visibility when
    // identifying orphaned children. Off-state collapses to scan_live.
    let snapshot = crate::mvcc::read_snapshot(db);

    // Snapshot the orphaned child rows first to release the immutable
    // borrow before queueing (which mutates the transaction state).
    let orphaned: Vec<Vec<SqlValue>> = {
        let child_table = match db.get_table(child_table_name) {
            Some(t) => t,
            None => return,
        };
        child_table
            .scan_visible(&snapshot)
            .filter_map(|(_, child_row)| {
                let child_fk_values: Vec<SqlValue> =
                    fk.column_indices.iter().map(|&idx| child_row.values[idx].clone()).collect();
                if child_fk_values.iter().any(|v| matches!(v, SqlValue::Null)) {
                    return None;
                }
                let matches = child_fk_values.iter().zip(parent_key_values).enumerate().all(
                    |(i, (cv, pv))| {
                        crate::foreign_key_check::fk_values_equal(
                            cv,
                            pv,
                            parent_collations.get(i).and_then(|c| c.as_deref()),
                            parent_affinities
                                .get(i)
                                .copied()
                                .unwrap_or(vibesql_types::TypeAffinity::None),
                        )
                    },
                );
                if matches {
                    Some(child_row.values.to_vec())
                } else {
                    None
                }
            })
            .collect()
    };

    for row_values in orphaned {
        db.queue_deferred_fk_violation(DeferredFkViolation {
            child_table: child_table_name.to_string(),
            fk_index: fk_idx,
            child_row: row_values,
            kind: DeferredFkViolationKind::ChildInsertOrUpdate,
        });
    }
}

/// Delete child rows that reference a deleted parent row (CASCADE action)
#[allow(clippy::too_many_arguments)]
pub(crate) fn cascade_delete(
    db: &mut vibesql_storage::Database,
    child_table_name: &str,
    fk: &vibesql_catalog::ForeignKeyConstraint,
    fk_idx: usize,
    parent_key_values: &[SqlValue],
    in_txn: bool,
    session_defer: bool,
) -> Result<(), ExecutorError> {
    // Resolve parent-side collations before borrowing the child table so
    // FK comparisons honor NOCASE/RTRIM (#5147).
    let parent_collations = crate::foreign_key_check::parent_collations_for_fk(db, fk);
    let parent_affinities = crate::foreign_key_check::parent_affinities_for_fk(db, fk);
    // Phase 1d follow-up (#5205): respect MVCC visibility when picking
    // CASCADE-delete targets. Off-state collapses to scan_live.
    let snapshot = crate::mvcc::read_snapshot(db);

    // Find rows to delete (visibility-filtered).
    let child_table = db.get_table(child_table_name).unwrap();
    let mut rows_to_delete: Vec<vibesql_storage::Row> = Vec::new();

    for (_, child_row) in child_table.scan_visible(&snapshot) {
        let child_fk_values: Vec<SqlValue> =
            fk.column_indices.iter().map(|&idx| child_row.values[idx].clone()).collect();

        // Skip NULL foreign keys
        if child_fk_values.iter().any(|v| matches!(v, SqlValue::Null)) {
            continue;
        }

        let matches =
            child_fk_values.iter().zip(parent_key_values).enumerate().all(|(i, (cv, pv))| {
                crate::foreign_key_check::fk_values_equal(
                    cv,
                    pv,
                    parent_collations.get(i).and_then(|c| c.as_deref()),
                    parent_affinities.get(i).copied().unwrap_or(vibesql_types::TypeAffinity::None),
                )
            });
        if matches {
            rows_to_delete.push(child_row.clone());
        }
    }

    // FK cascade fires the child table's row triggers, matching sqlite3
    // 3.51 (#5440). sqlite3 fires child BEFORE/AFTER DELETE triggers for
    // every cascaded row regardless of the `recursive_triggers` pragma —
    // that pragma gates a trigger re-firing itself, not FK-cascade-fired
    // triggers. The depth-16 RecursionGuard inside the firing helpers
    // bounds multi-level FK chains.
    let has_child_delete_triggers = db
        .catalog
        .get_triggers_for_table(child_table_name, Some(vibesql_ast::TriggerEvent::Delete))
        .next()
        .is_some();

    // Phase 1c (Issue #5150 / #5136) note: FK cascade DELETE uses the
    // content-matching `delete_where` predicate (not index-based), so we
    // can't pre-stamp xmax on specific row indices the way the primary
    // DELETE path does. With `mvcc_enabled` ON, cascade-deleted rows
    // currently get a bitmap-delete but NO xmax stamp. Phase 1d will
    // either thread snapshot-aware deletion through here or refactor
    // cascade to use index-based deletion so the stamping helper applies.
    // This is a known minor gap; cascade DELETE in a transaction with
    // mvcc_enabled is uncommon and the off-state behavior is unchanged.

    // Process each cascaded child row in collection order so that trigger
    // firing interleaves with multi-level cascade exactly as sqlite3 does
    // (#5440): for a given child row the order is
    //   BEFORE child trigger -> grandchild cascade -> delete -> AFTER child trigger.
    // Verified against sqlite3 3.51.0.
    for child_row in &rows_to_delete {
        // BEFORE DELETE row triggers on the child. A RAISE(IGNORE)
        // (SkipRow) abandons this child row's cascade — it is not deleted.
        // The surviving child row still references the parent key that is
        // being deleted, so it becomes an orphan and must trip the
        // statement-end FK check, exactly as sqlite3 3.51 does (#5465).
        // A RAISE(ABORT|FAIL|ROLLBACK) propagates as Err and aborts the
        // whole statement.
        if has_child_delete_triggers {
            let outcome = crate::TriggerFirer::execute_before_triggers(
                db,
                child_table_name,
                vibesql_ast::TriggerEvent::Delete,
                Some(child_row),
                None,
            )?;
            if outcome == crate::TriggerOutcome::SkipRow {
                // The skipped child still carries the parent's key, which is
                // about to disappear (the parent DELETE is applied after this
                // cascade returns). That is an FK violation. Match sqlite3:
                //   - immediate FK -> raise now; the statement savepoint rolls the whole statement
                //     back.
                //   - deferred FK (INITIALLY DEFERRED or session defer_foreign_keys=ON inside a
                //     txn) -> queue the surviving row for the COMMIT-time re-check.
                // NOT DEFERRABLE constraints ignore any INITIALLY DEFERRED
                // clause (see foreign_key_check.rs / e_fkey-34.*); the
                // session override is not gated on `is_deferrable`.
                let should_defer =
                    in_txn && (session_defer || (fk.is_deferrable && fk.initially_deferred));
                if should_defer {
                    db.queue_deferred_fk_violation(DeferredFkViolation {
                        child_table: child_table_name.to_string(),
                        fk_index: fk_idx,
                        child_row: child_row.values.to_vec(),
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

        // Recursively check integrity for this child row before deleting.
        // This handles multi-level CASCADE (grandchildren, etc.) and runs
        // after the child's BEFORE trigger, matching sqlite3 ordering.
        check_no_child_references(db, child_table_name, child_row)?;

        // Delete this child row.
        let child_table_mut = db.get_table_mut(child_table_name).unwrap();
        // Ignore delete_result since we unconditionally rebuild indexes after the loop.
        let _ = child_table_mut.delete_where(|row| row == child_row);

        // AFTER DELETE row triggers fire once this child row is gone.
        if has_child_delete_triggers {
            let _after = crate::TriggerFirer::execute_after_triggers(
                db,
                child_table_name,
                vibesql_ast::TriggerEvent::Delete,
                Some(child_row),
                None,
            )?;
        }
    }

    // Rebuild indexes after deletion (handles both compaction and non-compaction cases)
    db.rebuild_indexes(child_table_name);
    // Cascade mutations bypass the Database-level DML API, so invalidate the
    // child table's columnar cache explicitly to avoid serving stale reads (#5876).
    db.invalidate_columnar_cache(child_table_name);

    Ok(())
}

/// Set child FK columns to NULL (SET NULL action)
#[allow(clippy::too_many_arguments)]
pub(crate) fn set_null(
    db: &mut vibesql_storage::Database,
    child_table_name: &str,
    fk: &vibesql_catalog::ForeignKeyConstraint,
    fk_idx: usize,
    parent_key_values: &[SqlValue],
    in_txn: bool,
    session_defer: bool,
) -> Result<(), ExecutorError> {
    // Resolve parent-side collations before borrowing the child table so
    // FK comparisons honor NOCASE/RTRIM (#5147).
    let parent_collations = crate::foreign_key_check::parent_collations_for_fk(db, fk);
    let parent_affinities = crate::foreign_key_check::parent_affinities_for_fk(db, fk);
    // Phase 1d follow-up (#5205): respect MVCC visibility when picking
    // SET NULL targets. Off-state collapses to scan_live.
    let snapshot = crate::mvcc::read_snapshot(db);

    // Collect (row index, OLD row, NEW row) triples to rewrite. The OLD
    // row is the current child row; the NEW row carries the FK columns set
    // to NULL. We keep OLD so the child's UPDATE triggers see the correct
    // OLD.* values, matching sqlite3 3.51 (the SET NULL action is an UPDATE
    // on the child).
    let child_table = db.get_table(child_table_name).unwrap();
    let mut rows_to_update: Vec<(usize, vibesql_storage::Row, vibesql_storage::Row)> = Vec::new();

    for (idx, child_row) in child_table.scan_visible(&snapshot) {
        let child_fk_values: Vec<SqlValue> =
            fk.column_indices.iter().map(|&idx| child_row.values[idx].clone()).collect();

        // Skip NULL foreign keys
        if child_fk_values.iter().any(|v| matches!(v, SqlValue::Null)) {
            continue;
        }

        let matches =
            child_fk_values.iter().zip(parent_key_values).enumerate().all(|(i, (cv, pv))| {
                crate::foreign_key_check::fk_values_equal(
                    cv,
                    pv,
                    parent_collations.get(i).and_then(|c| c.as_deref()),
                    parent_affinities.get(i).copied().unwrap_or(vibesql_types::TypeAffinity::None),
                )
            });
        if matches {
            let old_row = child_row.clone();
            // Create updated row with FK columns set to NULL
            let mut updated_row = old_row.clone();
            for &fk_col_idx in &fk.column_indices {
                updated_row.values[fk_col_idx] = SqlValue::Null;
            }
            rows_to_update.push((idx, old_row, updated_row));
        }
    }

    apply_cascade_child_updates(
        db,
        child_table_name,
        fk,
        fk_idx,
        rows_to_update,
        in_txn,
        session_defer,
    )
}

/// Set child FK columns to their default values (SET DEFAULT action)
#[allow(clippy::too_many_arguments)]
pub(crate) fn set_default(
    db: &mut vibesql_storage::Database,
    child_table_name: &str,
    fk: &vibesql_catalog::ForeignKeyConstraint,
    fk_idx: usize,
    parent_key_values: &[SqlValue],
    in_txn: bool,
    session_defer: bool,
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

    // Resolve parent-side collations before borrowing the child table so
    // FK comparisons honor NOCASE/RTRIM (#5147).
    let parent_collations = crate::foreign_key_check::parent_collations_for_fk(db, fk);
    let parent_affinities = crate::foreign_key_check::parent_affinities_for_fk(db, fk);
    // Phase 1d follow-up (#5205): respect MVCC visibility when picking
    // SET DEFAULT targets. Off-state collapses to scan_live.
    let snapshot = crate::mvcc::read_snapshot(db);

    // Collect (row index, OLD row, NEW row) triples to rewrite. The OLD
    // row is the current child row; the NEW row carries the FK columns set
    // to their defaults. We keep OLD so the child's UPDATE triggers see the
    // correct OLD.* values, matching sqlite3 3.51 (the SET DEFAULT action
    // is an UPDATE on the child).
    let child_table = db.get_table(child_table_name).unwrap();
    let mut rows_to_update: Vec<(usize, vibesql_storage::Row, vibesql_storage::Row)> = Vec::new();

    for (idx, child_row) in child_table.scan_visible(&snapshot) {
        let child_fk_values: Vec<SqlValue> =
            fk.column_indices.iter().map(|&idx| child_row.values[idx].clone()).collect();

        // Skip NULL foreign keys
        if child_fk_values.iter().any(|v| matches!(v, SqlValue::Null)) {
            continue;
        }

        let matches =
            child_fk_values.iter().zip(parent_key_values).enumerate().all(|(i, (cv, pv))| {
                crate::foreign_key_check::fk_values_equal(
                    cv,
                    pv,
                    parent_collations.get(i).and_then(|c| c.as_deref()),
                    parent_affinities.get(i).copied().unwrap_or(vibesql_types::TypeAffinity::None),
                )
            });
        if matches {
            let old_row = child_row.clone();
            // Create updated row with FK columns set to default values
            let mut updated_row = old_row.clone();
            for (i, &fk_col_idx) in fk.column_indices.iter().enumerate() {
                updated_row.values[fk_col_idx] = default_values[i].clone();
            }
            rows_to_update.push((idx, old_row, updated_row));
        }
    }

    apply_cascade_child_updates(
        db,
        child_table_name,
        fk,
        fk_idx,
        rows_to_update,
        in_txn,
        session_defer,
    )
}

/// Apply a set of cascade-driven child-row rewrites (SET NULL / SET
/// DEFAULT), firing the child table's BEFORE/AFTER UPDATE row triggers per
/// row exactly as sqlite3 3.51 does. This mirrors the UPDATE-side cascade
/// apply loop in `update/foreign_keys.rs` (#5440 / #5498) so the DELETE-side
/// SET NULL / SET DEFAULT actions get identical trigger semantics.
///
/// For each affected child row:
///   1. Fire BEFORE UPDATE triggers (OLD = current row, NEW = rewritten row).
///   2. On `RAISE(IGNORE)` (SkipRow) the child's rewrite is abandoned: the surviving child keeps
///      its OLD FK value, which references the parent key that is about to disappear (the parent
///      DELETE is applied after this cascade returns). That is an orphaned FK reference and must
///      trip the statement-end FK check (#5465) — immediate FK raises now; deferred FK queues the
///      surviving OLD row for the COMMIT re-check.
///   3. Otherwise apply the rewrite via `update_row`, then fire AFTER UPDATE triggers.
///
/// `RAISE(ABORT|FAIL|ROLLBACK)` propagates as `Err` and aborts the statement.
#[allow(clippy::too_many_arguments)]
fn apply_cascade_child_updates(
    db: &mut vibesql_storage::Database,
    child_table_name: &str,
    fk: &vibesql_catalog::ForeignKeyConstraint,
    fk_idx: usize,
    mut rows_to_update: Vec<(usize, vibesql_storage::Row, vibesql_storage::Row)>,
    in_txn: bool,
    session_defer: bool,
) -> Result<(), ExecutorError> {
    // Phase 1c (Issue #5150 / #5136): stamp xmin on every FK-cascaded new
    // row version. Off-state is a no-op.
    let txn_id = db.transaction_id();
    for (_, _old_row, new_row) in rows_to_update.iter_mut() {
        vibesql_storage::stamp_xmin_for_write(new_row, txn_id);
        new_row.xmax = None;
    }

    // FK cascade fires the child table's BEFORE/AFTER UPDATE row triggers
    // for every cascaded row, matching sqlite3 3.51 (#5440). sqlite3 fires
    // these regardless of the `recursive_triggers` pragma; the depth-16
    // RecursionGuard inside the firing helpers bounds multi-level FK chains.
    let has_child_update_triggers = db
        .catalog
        .get_triggers_for_table(child_table_name, Some(vibesql_ast::TriggerEvent::Update(None)))
        .next()
        .is_some();

    for (idx, old_row, new_row) in rows_to_update {
        // BEFORE UPDATE row triggers on the child (OLD = current row, NEW =
        // rewritten row).
        if has_child_update_triggers {
            let outcome = crate::TriggerFirer::execute_before_triggers(
                db,
                child_table_name,
                vibesql_ast::TriggerEvent::Update(None),
                Some(&old_row),
                Some(&new_row),
            )?;
            if outcome == crate::TriggerOutcome::SkipRow {
                // RAISE(IGNORE) abandons this cascade rewrite; the surviving
                // child still references the parent key being deleted, so it
                // becomes an orphan that must trip the statement-end FK
                // check, exactly as sqlite3 3.51 does (#5465).
                // NOT DEFERRABLE constraints ignore any INITIALLY DEFERRED
                // clause (see foreign_key_check.rs / e_fkey-34.*); the
                // session override is not gated on `is_deferrable`.
                let should_defer =
                    in_txn && (session_defer || (fk.is_deferrable && fk.initially_deferred));
                if should_defer {
                    db.queue_deferred_fk_violation(DeferredFkViolation {
                        child_table: child_table_name.to_string(),
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

        // A cascading SET NULL/SET DEFAULT rewrite is itself an UPDATE on the
        // child table and must satisfy the child's own NOT NULL/CHECK
        // constraints, exactly like a user-issued UPDATE would (mirrors the
        // equivalent check on the UPDATE-cascade path in
        // update/foreign_keys.rs, fkey2-3.1.*).
        let child_schema_for_check = db.catalog.get_table(child_table_name).unwrap().clone();
        crate::update::constraints::ConstraintValidator::new(&child_schema_for_check)
            .with_check_constraints_ignored(db.ignore_check_constraints())
            .validate_row_skip_uniqueness(child_table_name, &new_row)?;

        // SET DEFAULT in particular can rewrite the FK column(s) to a default
        // value that is not itself a valid parent key -- re-validate the
        // rewritten row's own foreign keys exactly as a user-issued UPDATE
        // would (mirrors the equivalent check on the UPDATE-cascade path in
        // update/foreign_keys.rs, fkey2-9.1.5).
        if !child_schema_for_check.foreign_keys.is_empty() {
            let deferred =
                crate::update::foreign_keys::ForeignKeyValidator::collect_constraints_with_old(
                    db,
                    child_table_name,
                    &new_row.values,
                    Some(&old_row.values),
                )?;
            for violation in deferred {
                db.queue_deferred_fk_violation(violation);
            }
        }

        let child_table_mut = db.get_table_mut(child_table_name).unwrap();
        child_table_mut
            .update_row(idx, new_row.clone())
            .map_err(|e| ExecutorError::StorageError(e.to_string()))?;

        // AFTER UPDATE row triggers fire once this child row is rewritten.
        if has_child_update_triggers {
            let _after = crate::TriggerFirer::execute_after_triggers(
                db,
                child_table_name,
                vibesql_ast::TriggerEvent::Update(None),
                Some(&old_row),
                Some(&new_row),
            )?;
        }
    }

    // Rebuild indexes after updates.
    db.rebuild_indexes(child_table_name);
    // Cascade mutations bypass the Database-level DML API, so invalidate the
    // child table's columnar cache explicitly to avoid serving stale reads (#5876).
    db.invalidate_columnar_cache(child_table_name);

    Ok(())
}
