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
            // A BEGIN issued while a transaction is already open is a
            // user-level error in SQLite, not an internal storage fault.
            // SQLite raises SQLITE_ERROR with the message "cannot start a
            // transaction within a transaction" (see vdbe.c OP_AutoCommit).
            // Surface that wording verbatim instead of leaking the internal
            // "Failed to begin transaction: Transaction error: Transaction
            // already active" storage message (#5659).
            if matches!(
                e,
                vibesql_storage::StorageError::TransactionError(ref m)
                    if m == "Transaction already active"
            ) {
                ExecutorError::SqliteCompatError(
                    "cannot start a transaction within a transaction".to_string(),
                )
            } else {
                ExecutorError::StorageError(format!("Failed to begin transaction: {}", e))
            }
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
        //
        // Phase 1d of #5136 — FK deferred-replay coordination:
        // Capture a fresh **commit-time** snapshot for the re-validation
        // scan. The BEGIN-time snapshot (`db.current_snapshot()`) would
        // miss writes committed by other transactions between BEGIN and
        // COMMIT, which is exactly the wrong semantics for FK enforcement
        // (we want the latest visible parent state, not a stale one).
        //
        // Under the current single-writer model, the commit-time snapshot
        // treats every transaction id allocated so far as committed,
        // making the committing transaction's own writes (stamped with
        // `xmin = current_txn_id`) pass `visible_to`.
        let pending = db.take_deferred_fk_violations();
        let commit_snapshot = db.capture_commit_time_snapshot();
        if let Some(err_msg) = check_deferred_fk_violations(db, &pending, &commit_snapshot) {
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
///
/// # Phase 1d of #5136 — snapshot semantics
///
/// `snapshot` is the **commit-time** snapshot
/// ([`Database::capture_commit_time_snapshot`]), not the BEGIN-time
/// snapshot. Under MVCC, this ensures that:
///
/// - The child-side check sees the committing transaction's own
///   INSERT/UPDATE (the row that triggered the deferred violation in the
///   first place) — even though it was stamped with this txn's `xmin`,
///   the commit-time snapshot treats this txn id as committed.
/// - The parent-side check sees any parent rows committed by OTHER
///   transactions between BEGIN and COMMIT — which is exactly what we
///   want: deferred FK is checked against the latest visible state, not
///   a stale BEGIN-time view.
///
/// With the `mvcc_enabled` feature OFF (default), the storage-layer
/// `is_row_visible` reduces to a deletion-bitmap check and the snapshot
/// argument is ignored, preserving pre-MVCC semantics.
fn check_deferred_fk_violations(
    db: &Database,
    pending: &[vibesql_storage::DeferredFkViolation],
    snapshot: &vibesql_storage::TxnSnapshot,
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

        // Does *any* visible child row still carry these FK values? If
        // not, the conflict has been resolved (child deleted/updated).
        // Phase 1d: use `scan_visible` so the committing txn's own
        // child writes participate, and so a concurrent committed
        // delete is honored.
        let child_still_present = child_table.scan_visible(snapshot).any(|(_, child_row)| {
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

        // Phase 1d: scan visible parent rows (commit-time snapshot).
        // Under MVCC OFF this is equivalent to scanning all rows
        // (matches previous behavior of `parent_table.scan().iter()`,
        // which did not filter the deletion bitmap; we keep the
        // pre-existing wider semantics by also looking at non-bitmap-
        // deleted rows — the `scan_visible` iterator filters the
        // bitmap, but pre-Phase-1d the parent scan included deleted
        // rows too, so to preserve OFF-state semantics exactly we
        // continue to use `scan()` when MVCC is off).
        let key_exists = {
            #[cfg(feature = "mvcc_enabled")]
            {
                parent_table.scan_visible(snapshot).any(|(_, parent_row)| {
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
                })
            }
            #[cfg(not(feature = "mvcc_enabled"))]
            {
                let _ = snapshot;
                parent_table.scan().iter().any(|parent_row| {
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
                })
            }
        };

        if !key_exists {
            // Match SQLite's wording for deferred FK failure at commit.
            return Some("FOREIGN KEY constraint failed".to_string());
        }
    }

    None
}

/// Count deferred FK violations whose constraints would still fail
/// against the current visible state of the database.
///
/// This mirrors SQLite's `DBSTATUS_DEFERRED_FKS` semantics: entries
/// whose child row has since been deleted/updated, or whose missing
/// parent row has since been (re)inserted, are *not* counted because
/// they would no longer fail a COMMIT-time replay. Entries with NULL
/// FK columns are also skipped because the FK never applied to them.
///
/// Returns 0 when no transaction is active.
///
/// # Relationship to [`check_deferred_fk_violations`]
///
/// Both walk the same `deferred_fk_violations` queue and apply the
/// child-side / parent-side checks. The differences are:
///
/// 1. This function is purely read-only and never drains the queue.
/// 2. Both child- and parent-side scans use `scan_live()` (bitmap-
///    filtered) so that DELETEs performed earlier in the current
///    transaction are honored. The COMMIT path's parent scan
///    intentionally includes soft-deleted rows under the MVCC-OFF
///    feature flag for backward-compatibility — for status reporting
///    we want the SQLite-compatible "is there a live parent right
///    now?" answer instead.
///
/// Backs the `PRAGMA deferred_fk_count` bridge that the TCL shim
/// translates `sqlite3_db_status db DBSTATUS_DEFERRED_FKS` into
/// (issue #5187).
pub fn live_deferred_fk_violation_count(db: &Database) -> usize {
    use crate::foreign_key_check::{
        fk_values_equal, parent_collations_for_fk, resolved_parent_indices_for_fk,
    };

    let pending = db.deferred_fk_violations();
    if pending.is_empty() {
        return 0;
    }

    let mut live = 0usize;
    for violation in pending {
        let child_schema = match db.catalog.get_table(&violation.child_table) {
            Some(s) => s,
            None => continue, // child table dropped — violation no longer applies
        };
        let fk = match child_schema.foreign_keys.get(violation.fk_index) {
            Some(fk) => fk,
            None => continue, // schema changed — violation gone
        };

        let child_table = match db.get_table(&violation.child_table) {
            Some(t) => t,
            None => continue,
        };

        let snapshot_fk_values: Vec<vibesql_types::SqlValue> = fk
            .column_indices
            .iter()
            .map(|&idx| {
                violation.child_row.get(idx).cloned().unwrap_or(vibesql_types::SqlValue::Null)
            })
            .collect();

        // NULLs in FK columns mean the constraint never applied.
        if snapshot_fk_values.iter().any(|v| v.is_null()) {
            continue;
        }

        // Child still present? Use `scan_live` (bitmap-filtered) so that a
        // sibling DELETE in the same transaction is honored — that is the
        // resolution path exercised by fkey6-1.21.
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

        // Parent now exists?
        let parent_table = match db.get_table(&fk.parent_table) {
            Some(t) => t,
            None => {
                // Parent table missing — definite violation.
                live += 1;
                continue;
            }
        };

        let parent_collations = parent_collations_for_fk(db, fk);
        let parent_indices = resolved_parent_indices_for_fk(db, fk);

        // Parent-side check uses `scan_live` (bitmap-filtered) so that the
        // parent DELETE that originally triggered the deferred violation
        // is honored. Note this differs from the COMMIT-time replay path
        // (`check_deferred_fk_violations`), which deliberately includes
        // soft-deleted parent rows under the MVCC-OFF feature for
        // backward-compatibility reasons; for status reporting we want
        // SQLite-compatible "is there a live parent right now" semantics.
        let key_exists = parent_table.scan_live().any(|(_, parent_row)| {
            parent_indices.iter().zip(&snapshot_fk_values).enumerate().all(
                |(i, (&parent_idx, fk_val))| match parent_row.values.get(parent_idx) {
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
            live += 1;
        }
    }

    live
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
    /// Execute a SAVEPOINT statement.
    ///
    /// SQLite semantics: a `SAVEPOINT` issued outside an explicit
    /// `BEGIN`/`START TRANSACTION` implicitly opens a transaction (the
    /// connection leaves autocommit mode). That transaction is auto-committed
    /// when the matching outermost `RELEASE` empties the savepoint stack
    /// (see [`ReleaseSavepointExecutor`]). We flag such transactions so
    /// `RELEASE` knows to commit them, while an explicit `BEGIN; SAVEPOINT
    /// ...; RELEASE ...` keeps the transaction open until an explicit
    /// `COMMIT` (fkey2-2.38+, savepoint-driven deferred-FK matrix).
    pub fn execute(stmt: &SavepointStmt, db: &mut Database) -> Result<String, ExecutorError> {
        let auto_started = if db.in_transaction() {
            false
        } else {
            db.begin_transaction().map_err(|e| {
                ExecutorError::StorageError(format!("Failed to begin transaction: {}", e))
            })?;
            true
        };

        if let Err(e) = db.create_savepoint(stmt.name.clone()) {
            // Roll back the transaction we just auto-started so a failed
            // savepoint creation does not strand the connection mid-transaction.
            if auto_started {
                let _ = db.rollback_transaction();
            }
            return Err(ExecutorError::StorageError(format!(
                "Failed to create savepoint: {}",
                e
            )));
        }

        if auto_started {
            db.mark_implicit_savepoint_txn();
        }

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
    /// Execute a RELEASE SAVEPOINT statement.
    ///
    /// SQLite semantics: releasing the outermost savepoint of a transaction
    /// that was *implicitly* opened by a `SAVEPOINT` (i.e. no enclosing
    /// `BEGIN`) commits that transaction. The commit runs the deferred-FK
    /// re-check via [`CommitExecutor`], so a `RELEASE` that finalizes a
    /// transaction carrying an outstanding deferred violation fails with
    /// "FOREIGN KEY constraint failed" exactly as SQLite does (fkey2-2.40).
    /// For a transaction opened by an explicit `BEGIN`, `RELEASE` of the last
    /// savepoint leaves the transaction open until an explicit `COMMIT`.
    pub fn execute(
        stmt: &ReleaseSavepointStmt,
        db: &mut Database,
    ) -> Result<String, ExecutorError> {
        db.release_savepoint(stmt.name.clone()).map_err(|e| {
            ExecutorError::StorageError(format!("Failed to release savepoint: {}", e))
        })?;

        // If this RELEASE emptied the savepoint stack of an implicitly-opened
        // transaction, commit it (SQLite autocommit-on-outermost-release).
        if db.is_implicit_savepoint_txn() && db.savepoint_depth() == 0 {
            CommitExecutor::execute(&CommitStmt, db)?;
        }

        Ok(format!("Savepoint '{}' released", stmt.name))
    }
}
