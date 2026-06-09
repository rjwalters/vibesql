use vibesql_storage::DeferredFkViolation;

use crate::errors::ExecutorError;
use crate::foreign_key_check::{check_fk_row_existence, FkRowCheck};

/// Validate FOREIGN KEY constraints for a new row.
///
/// Phase C2 of #5085: when the constraint is `INITIALLY DEFERRED` or
/// the session has `PRAGMA defer_foreign_keys=ON` (and a transaction is
/// active), a missing parent row is queued onto the transaction's
/// deferred-FK queue instead of returning an immediate error. The
/// queue is drained and re-checked at COMMIT.
///
/// Steps 4-6 (parent-existence scan, self-FK row-self check, defer-or-error)
/// are factored into [`check_fk_row_existence`] and shared with
/// [`super::row_validator::RowValidator::validate_foreign_keys`]. This wrapper
/// handles the PRAGMA gate, schema-mismatch check, NULL-skip, and the
/// post-loop queue push (deferred violations are accumulated locally and
/// pushed *after* the immutable `&Database` borrow drops, preserving the
/// pattern introduced in #5125 / PR #5141).
pub fn validate_foreign_key_constraints(
    db: &mut vibesql_storage::Database,
    table_name: &str,
    row_values: &[vibesql_types::SqlValue],
) -> Result<(), ExecutorError> {
    // Step 1: skip FK enforcement when PRAGMA foreign_keys is OFF (default).
    if !db.foreign_keys_enabled() {
        return Ok(());
    }

    let schema = db
        .catalog
        .get_table(table_name)
        .ok_or_else(|| ExecutorError::TableNotFound(table_name.to_string()))?
        .clone();

    let mut deferred: Vec<DeferredFkViolation> = Vec::new();

    for (fk_idx, fk) in schema.foreign_keys.iter().enumerate() {
        // Step 2: mismatch check runs before any row-existence test so that
        // bad FK targets are reported even when the parent table is empty.
        // Mismatch is never deferred (matches SQLite behaviour).
        if let Some((child, parent)) =
            crate::foreign_key_check::detect_fk_mismatch(db, table_name, fk)
        {
            return Err(ExecutorError::ForeignKeyMismatch { child, parent });
        }

        // Step 3: extract FK values from the new row and skip if any are NULL.
        let fk_values: Vec<vibesql_types::SqlValue> =
            fk.column_indices.iter().map(|&idx| row_values[idx].clone()).collect();
        if fk_values.iter().any(|v| v.is_null()) {
            continue;
        }

        // Steps 4-6: shared per-FK row-existence + self-FK + defer decision.
        match check_fk_row_existence(db, table_name, fk, fk_idx, &fk_values, row_values)? {
            FkRowCheck::Ok => continue,
            FkRowCheck::Deferred(v) => {
                deferred.push(v);
                continue;
            }
            FkRowCheck::Violation => {
                return Err(ExecutorError::ConstraintViolation(format!(
                    "FOREIGN KEY constraint '{}' violated: key ({}) not found in table '{}'",
                    fk.name.as_deref().unwrap_or(""),
                    fk.column_names.join(", "),
                    fk.parent_table
                )));
            }
        }
    }

    // Step 7: push deferred violations after the immutable borrow drops.
    for v in deferred {
        db.queue_deferred_fk_violation(v);
    }

    Ok(())
}
