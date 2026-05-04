//! Transaction control statement execution (BEGIN, COMMIT, ROLLBACK)

use vibesql_ast::{
    BeginStmt, CommitStmt, DurabilityHint, ReleaseSavepointStmt, RollbackStmt,
    RollbackToSavepointStmt, SavepointStmt,
};
use vibesql_storage::{Database, TransactionDurability};

use crate::errors::ExecutorError;

/// Convert AST DurabilityHint to storage TransactionDurability
fn convert_durability_hint(hint: &DurabilityHint) -> TransactionDurability {
    match hint {
        DurabilityHint::Default => TransactionDurability::Default,
        DurabilityHint::Durable => TransactionDurability::ForceDurable,
        DurabilityHint::Lazy => TransactionDurability::AllowLazy,
        DurabilityHint::Volatile => TransactionDurability::ForceVolatile,
    }
}

/// Executor for BEGIN TRANSACTION statements
pub struct BeginTransactionExecutor;

impl BeginTransactionExecutor {
    /// Execute a BEGIN TRANSACTION statement
    pub fn execute(stmt: &BeginStmt, db: &mut Database) -> Result<String, ExecutorError> {
        let durability = convert_durability_hint(&stmt.durability);
        db.begin_transaction_with_durability(durability).map_err(|e| {
            ExecutorError::StorageError(format!("Failed to begin transaction: {}", e))
        })?;

        let msg = match stmt.durability {
            DurabilityHint::Default => "Transaction started".to_string(),
            DurabilityHint::Durable => "Transaction started (durability: DURABLE)".to_string(),
            DurabilityHint::Lazy => "Transaction started (durability: LAZY)".to_string(),
            DurabilityHint::Volatile => "Transaction started (durability: VOLATILE)".to_string(),
        };
        Ok(msg)
    }
}

/// Executor for COMMIT statements
pub struct CommitExecutor;

impl CommitExecutor {
    /// Execute a COMMIT statement.
    ///
    /// Phase C2 of #5085: drains the transaction's deferred FK
    /// violation queue and re-checks each entry against the current
    /// parent state. If any deferred violation still holds, the COMMIT
    /// fails and the transaction is rolled back, matching SQLite
    /// semantics ("FOREIGN KEY constraint failed" raised at COMMIT).
    pub fn execute(_stmt: &CommitStmt, db: &mut Database) -> Result<String, ExecutorError> {
        // Drain and re-validate deferred FK violations before commit.
        let pending = db.take_deferred_fk_violations();
        if let Some(err_msg) = check_deferred_fk_violations(db, &pending) {
            // Deferred violation still holds at COMMIT — abort the
            // commit and roll back. SQLite raises "FOREIGN KEY
            // constraint failed" here; auto-rollback follows the same
            // convention as Bucket D (#5087, merged 17f23988).
            //
            // We ignore the rollback's own error: if rollback also
            // fails, the original FK error is the more useful one.
            let _ = db.rollback_transaction();
            return Err(ExecutorError::ConstraintViolation(err_msg));
        }

        db.commit_transaction().map_err(|e| {
            ExecutorError::StorageError(format!("Failed to commit transaction: {}", e))
        })?;

        Ok("Transaction committed".to_string())
    }
}

/// Re-validate every deferred FK violation against the current parent
/// state. Returns `Some(error_message)` for the first entry that still
/// fails; returns `None` when every entry has been satisfied (because
/// the parent row was inserted later, the child row was deleted, etc.).
fn check_deferred_fk_violations(
    db: &Database,
    pending: &[vibesql_storage::DeferredFkViolation],
) -> Option<String> {
    use crate::foreign_key_check::{
        fk_values_equal, parent_collations_for_fk, resolved_parent_indices_for_fk,
    };

    for violation in pending {
        let child_schema = match db.catalog.get_table(&violation.child_table) {
            Some(s) => s,
            None => {
                // Child table was dropped during the transaction —
                // there is no row left to violate the constraint.
                continue;
            }
        };
        let fk = match child_schema.foreign_keys.get(violation.fk_index) {
            Some(fk) => fk,
            None => continue, // FK index out of range — schema changed
        };

        // 1) Child-side check: does a row matching this snapshot still
        //    exist in the child table? If the user has since deleted or
        //    updated the offending child row, the violation is moot.
        let child_table = match db.get_table(&violation.child_table) {
            Some(t) => t,
            None => continue,
        };

        // Take FK column values from the snapshot of the child row.
        let snapshot_fk_values: Vec<vibesql_types::SqlValue> = fk
            .column_indices
            .iter()
            .map(|&idx| {
                violation.child_row.get(idx).cloned().unwrap_or(vibesql_types::SqlValue::Null)
            })
            .collect();

        // NULLs in the FK columns mean the constraint never applied.
        if snapshot_fk_values.iter().any(|v| v.is_null()) {
            continue;
        }

        // Does *any* live child row still carry these FK values? If
        // not, the conflict has been resolved (child deleted/updated).
        let child_still_present = child_table.scan_live().any(|(_, child_row)| {
            fk.column_indices.iter().enumerate().all(|(i, &col_idx)| {
                match child_row.values.get(col_idx) {
                    Some(v) => v == &snapshot_fk_values[i],
                    None => false,
                }
            })
        });
        if !child_still_present {
            continue;
        }

        // 2) Parent-side check: does a parent row now exist that
        //    matches the FK columns?
        let parent_table = match db.get_table(&fk.parent_table) {
            Some(t) => t,
            None => {
                // Parent table missing — definitely a violation.
                return Some(format!(
                    "FOREIGN KEY constraint failed: parent table '{}' missing at COMMIT",
                    fk.parent_table
                ));
            }
        };

        let parent_collations = parent_collations_for_fk(db, fk);
        let parent_indices = resolved_parent_indices_for_fk(db, fk);

        let key_exists = parent_table.scan().iter().any(|parent_row| {
            parent_indices.iter().zip(&snapshot_fk_values).enumerate().all(
                |(i, (&parent_idx, fk_val))| match parent_row.get(parent_idx) {
                    Some(parent_val) => fk_values_equal(
                        fk_val,
                        parent_val,
                        parent_collations.get(i).and_then(|c| c.as_deref()),
                    ),
                    None => false,
                },
            )
        });

        if !key_exists {
            // Match SQLite's wording for deferred FK failure at commit.
            return Some("FOREIGN KEY constraint failed".to_string());
        }
    }

    None
}

/// Executor for ROLLBACK statements
pub struct RollbackExecutor;

impl RollbackExecutor {
    /// Execute a ROLLBACK statement
    pub fn execute(_stmt: &RollbackStmt, db: &mut Database) -> Result<String, ExecutorError> {
        db.rollback_transaction().map_err(|e| {
            ExecutorError::StorageError(format!("Failed to rollback transaction: {}", e))
        })?;

        Ok("Transaction rolled back".to_string())
    }
}

/// Executor for SAVEPOINT statements
pub struct SavepointExecutor;

impl SavepointExecutor {
    /// Execute a SAVEPOINT statement
    pub fn execute(stmt: &SavepointStmt, db: &mut Database) -> Result<String, ExecutorError> {
        db.create_savepoint(stmt.name.clone()).map_err(|e| {
            ExecutorError::StorageError(format!("Failed to create savepoint: {}", e))
        })?;

        Ok(format!("Savepoint '{}' created", stmt.name))
    }
}

/// Executor for ROLLBACK TO SAVEPOINT statements
pub struct RollbackToSavepointExecutor;

impl RollbackToSavepointExecutor {
    /// Execute a ROLLBACK TO SAVEPOINT statement
    pub fn execute(
        stmt: &RollbackToSavepointStmt,
        db: &mut Database,
    ) -> Result<String, ExecutorError> {
        db.rollback_to_savepoint(stmt.name.clone()).map_err(|e| {
            ExecutorError::StorageError(format!("Failed to rollback to savepoint: {}", e))
        })?;

        Ok(format!("Rolled back to savepoint '{}'", stmt.name))
    }
}

/// Executor for RELEASE SAVEPOINT statements
pub struct ReleaseSavepointExecutor;

impl ReleaseSavepointExecutor {
    /// Execute a RELEASE SAVEPOINT statement
    pub fn execute(
        stmt: &ReleaseSavepointStmt,
        db: &mut Database,
    ) -> Result<String, ExecutorError> {
        db.release_savepoint(stmt.name.clone()).map_err(|e| {
            ExecutorError::StorageError(format!("Failed to release savepoint: {}", e))
        })?;

        Ok(format!("Savepoint '{}' released", stmt.name))
    }
}
