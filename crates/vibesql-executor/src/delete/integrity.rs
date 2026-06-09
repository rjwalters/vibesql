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

            // Check if any row in the child table references this parent
            // row's FK-target columns. Use scan_live() to skip
            // soft-deleted rows.
            let child_table = db.get_table(&table_name).unwrap();
            let has_references = child_table.scan_live().any(|(_, child_row)| {
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
                let should_defer = in_txn && (fk.initially_deferred || session_defer);
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
                // Delete child rows that reference the parent
                cascade_delete(db, &child_table_name, &fk, &parent_key_values)?;
            }
            ReferentialAction::SetNull => {
                // Set child FK columns to NULL
                set_null(db, &child_table_name, &fk, &parent_key_values)?;
            }
            ReferentialAction::SetDefault => {
                // Set child FK columns to their default values
                set_default(db, &child_table_name, &fk, &parent_key_values)?;
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

    // Snapshot the orphaned child rows first to release the immutable
    // borrow before queueing (which mutates the transaction state).
    let orphaned: Vec<Vec<SqlValue>> = {
        let child_table = match db.get_table(child_table_name) {
            Some(t) => t,
            None => return,
        };
        child_table
            .scan_live()
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
fn cascade_delete(
    db: &mut vibesql_storage::Database,
    child_table_name: &str,
    fk: &vibesql_catalog::ForeignKeyConstraint,
    parent_key_values: &[SqlValue],
) -> Result<(), ExecutorError> {
    // Resolve parent-side collations before borrowing the child table so
    // FK comparisons honor NOCASE/RTRIM (#5147).
    let parent_collations = crate::foreign_key_check::parent_collations_for_fk(db, fk);

    // Find rows to delete (use scan_live to skip soft-deleted rows)
    let child_table = db.get_table(child_table_name).unwrap();
    let mut rows_to_delete: Vec<vibesql_storage::Row> = Vec::new();

    for (_, child_row) in child_table.scan_live() {
        let child_fk_values: Vec<SqlValue> =
            fk.column_indices.iter().map(|&idx| child_row.values[idx].clone()).collect();

        // Skip NULL foreign keys
        if child_fk_values.iter().any(|v| matches!(v, SqlValue::Null)) {
            continue;
        }

        let matches = child_fk_values.iter().zip(parent_key_values).enumerate().all(
            |(i, (cv, pv))| {
                crate::foreign_key_check::fk_values_equal(
                    cv,
                    pv,
                    parent_collations.get(i).and_then(|c| c.as_deref()),
                )
            },
        );
        if matches {
            rows_to_delete.push(child_row.clone());
        }
    }

    // Recursively check integrity for each child row before deleting
    // This handles multi-level CASCADE (grandchildren, etc.)
    for child_row in &rows_to_delete {
        check_no_child_references(db, child_table_name, child_row)?;
    }

    // Phase 1c (Issue #5150 / #5136) note: FK cascade DELETE uses the
    // content-matching `delete_where` predicate (not index-based), so we
    // can't pre-stamp xmax on specific row indices the way the primary
    // DELETE path does. With `mvcc_enabled` ON, cascade-deleted rows
    // currently get a bitmap-delete but NO xmax stamp. Phase 1d will
    // either thread snapshot-aware deletion through here or refactor
    // cascade to use index-based deletion so the stamping helper applies.
    // This is a known minor gap; cascade DELETE in a transaction with
    // mvcc_enabled is uncommon and the off-state behavior is unchanged.

    // Now delete the child rows
    let child_table_mut = db.get_table_mut(child_table_name).unwrap();
    for row_to_delete in &rows_to_delete {
        // Ignore delete_result since we unconditionally rebuild indexes after the loop
        let _ = child_table_mut.delete_where(|row| row == row_to_delete);
    }

    // Rebuild indexes after deletion (handles both compaction and non-compaction cases)
    db.rebuild_indexes(child_table_name);

    Ok(())
}

/// Set child FK columns to NULL (SET NULL action)
fn set_null(
    db: &mut vibesql_storage::Database,
    child_table_name: &str,
    fk: &vibesql_catalog::ForeignKeyConstraint,
    parent_key_values: &[SqlValue],
) -> Result<(), ExecutorError> {
    // Resolve parent-side collations before borrowing the child table so
    // FK comparisons honor NOCASE/RTRIM (#5147).
    let parent_collations = crate::foreign_key_check::parent_collations_for_fk(db, fk);

    // Collect indices of rows to update
    let child_table = db.get_table(child_table_name).unwrap();
    let mut rows_to_update: Vec<(usize, vibesql_storage::Row)> = Vec::new();

    // Use scan_live() to skip deleted rows and get correct physical indices
    for (idx, child_row) in child_table.scan_live() {
        let child_fk_values: Vec<SqlValue> =
            fk.column_indices.iter().map(|&idx| child_row.values[idx].clone()).collect();

        // Skip NULL foreign keys
        if child_fk_values.iter().any(|v| matches!(v, SqlValue::Null)) {
            continue;
        }

        let matches = child_fk_values.iter().zip(parent_key_values).enumerate().all(
            |(i, (cv, pv))| {
                crate::foreign_key_check::fk_values_equal(
                    cv,
                    pv,
                    parent_collations.get(i).and_then(|c| c.as_deref()),
                )
            },
        );
        if matches {
            // Create updated row with FK columns set to NULL
            let mut updated_row = child_row.clone();
            for &fk_col_idx in &fk.column_indices {
                updated_row.values[fk_col_idx] = SqlValue::Null;
            }
            rows_to_update.push((idx, updated_row));
        }
    }

    // Phase 1c (Issue #5150 / #5136): stamp xmin on every FK-cascaded
    // SET NULL new row version. Off-state is a no-op.
    let txn_id = db.transaction_id();
    for (_, updated_row) in rows_to_update.iter_mut() {
        vibesql_storage::stamp_xmin_for_write(updated_row, txn_id);
        updated_row.xmax = None;
    }

    // Now update the rows
    let child_table_mut = db.get_table_mut(child_table_name).unwrap();
    for (idx, updated_row) in rows_to_update {
        child_table_mut
            .update_row(idx, updated_row)
            .map_err(|e| ExecutorError::StorageError(e.to_string()))?;
    }

    // Rebuild indexes after update
    db.rebuild_indexes(child_table_name);

    Ok(())
}

/// Set child FK columns to their default values (SET DEFAULT action)
fn set_default(
    db: &mut vibesql_storage::Database,
    child_table_name: &str,
    fk: &vibesql_catalog::ForeignKeyConstraint,
    parent_key_values: &[SqlValue],
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

    // Collect indices of rows to update
    let child_table = db.get_table(child_table_name).unwrap();
    let mut rows_to_update: Vec<(usize, vibesql_storage::Row)> = Vec::new();

    // Use scan_live() to skip deleted rows and get correct physical indices
    for (idx, child_row) in child_table.scan_live() {
        let child_fk_values: Vec<SqlValue> =
            fk.column_indices.iter().map(|&idx| child_row.values[idx].clone()).collect();

        // Skip NULL foreign keys
        if child_fk_values.iter().any(|v| matches!(v, SqlValue::Null)) {
            continue;
        }

        let matches = child_fk_values.iter().zip(parent_key_values).enumerate().all(
            |(i, (cv, pv))| {
                crate::foreign_key_check::fk_values_equal(
                    cv,
                    pv,
                    parent_collations.get(i).and_then(|c| c.as_deref()),
                )
            },
        );
        if matches {
            // Create updated row with FK columns set to default values
            let mut updated_row = child_row.clone();
            for (i, &fk_col_idx) in fk.column_indices.iter().enumerate() {
                updated_row.values[fk_col_idx] = default_values[i].clone();
            }
            rows_to_update.push((idx, updated_row));
        }
    }

    // Phase 1c (Issue #5150 / #5136): stamp xmin on every FK-cascaded
    // SET DEFAULT new row version. Off-state is a no-op.
    let txn_id = db.transaction_id();
    for (_, updated_row) in rows_to_update.iter_mut() {
        vibesql_storage::stamp_xmin_for_write(updated_row, txn_id);
        updated_row.xmax = None;
    }

    // Now update the rows
    let child_table_mut = db.get_table_mut(child_table_name).unwrap();
    for (idx, updated_row) in rows_to_update {
        child_table_mut
            .update_row(idx, updated_row)
            .map_err(|e| ExecutorError::StorageError(e.to_string()))?;
    }

    // Rebuild indexes after update
    db.rebuild_indexes(child_table_name);

    Ok(())
}
