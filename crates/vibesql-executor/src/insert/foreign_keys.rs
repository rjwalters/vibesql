use vibesql_storage::{DeferredFkViolation, DeferredFkViolationKind};

use crate::errors::ExecutorError;

/// Validate FOREIGN KEY constraints for a new row.
///
/// Phase C2 of #5085: when the constraint is `INITIALLY DEFERRED` or
/// the session has `PRAGMA defer_foreign_keys=ON` (and a transaction is
/// active), a missing parent row is queued onto the transaction's
/// deferred-FK queue instead of returning an immediate error. The
/// queue is drained and re-checked at COMMIT.
pub fn validate_foreign_key_constraints(
    db: &mut vibesql_storage::Database,
    table_name: &str,
    row_values: &[vibesql_types::SqlValue],
) -> Result<(), ExecutorError> {
    // Skip FK enforcement when PRAGMA foreign_keys is OFF (default)
    if !db.foreign_keys_enabled() {
        return Ok(());
    }

    let session_defer = db.defer_foreign_keys();
    let in_txn = db.in_transaction();

    let schema = db
        .catalog
        .get_table(table_name)
        .ok_or_else(|| ExecutorError::TableNotFound(table_name.to_string()))?
        .clone();

    let mut deferred: Vec<DeferredFkViolation> = Vec::new();

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

        let key_exists = parent_table.scan().iter().any(|parent_row| {
            parent_indices.iter().zip(&fk_values).enumerate().all(|(i, (&parent_idx, fk_val))| {
                match parent_row.get(parent_idx) {
                    Some(parent_val) => crate::foreign_key_check::fk_values_equal(
                        fk_val,
                        parent_val,
                        parent_collations.get(i).and_then(|c| c.as_deref()),
                    ),
                    None => false,
                }
            })
        });

        if key_exists {
            continue;
        }

        // Phase C3 of #5085 / fkey8-3.0: self-referential FK. When the FK
        // points back at the table being inserted into, the row itself
        // can satisfy the constraint (e.g. INSERT ... VALUES (1, 'a',
        // 'a', 'a', 'a') with FK(b, c) REFERENCES self(d, e)). SQLite
        // checks the parent index *after* the row is inserted, so the
        // row participates in its own FK check. Mirror that here.
        if fk.parent_table.eq_ignore_ascii_case(table_name) {
            let row_satisfies_fk = parent_indices.iter().zip(&fk_values).enumerate().all(
                |(i, (&parent_idx, fk_val))| match row_values.get(parent_idx) {
                    Some(parent_val) => crate::foreign_key_check::fk_values_equal(
                        fk_val,
                        parent_val,
                        parent_collations.get(i).and_then(|c| c.as_deref()),
                    ),
                    None => false,
                },
            );
            if row_satisfies_fk {
                continue;
            }
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

    for v in deferred {
        db.queue_deferred_fk_violation(v);
    }

    Ok(())
}
