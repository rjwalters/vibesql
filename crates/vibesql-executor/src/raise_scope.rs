//! Per-variant transaction-scope handling for the SQLite `RAISE()`
//! conflict-resolution actions (#5417).
//!
//! `RAISE(ABORT|FAIL|ROLLBACK, msg)` fired from a trigger reports `msg` (SQLite
//! error code 19) but the three variants differ in *how much* they undo when
//! they abort the firing statement inside an explicit multi-statement
//! transaction. Verified against sqlite3 3.51.x:
//!
//! - **`RAISE(ABORT, msg)`** — roll back the current **statement** (undo the
//!   partial changes it already applied), but keep the enclosing transaction
//!   open: statements that ran *before* it in the same transaction survive.
//!   This is also SQLite's default conflict behavior.
//! - **`RAISE(FAIL, msg)`** — stop the statement at the failing row but do
//!   **not** undo the changes it already applied to earlier rows; keep the
//!   transaction open.
//! - **`RAISE(ROLLBACK, msg)`** — roll back the **entire** enclosing
//!   transaction.
//! - `RAISE(IGNORE)` is handled earlier as a control-flow signal
//!   ([`ExecutorError::RaiseIgnore`]) and never reaches this module.
//!
//! `ABORT` vs `ROLLBACK` is only observable inside an explicit transaction
//! (whether earlier statements survive); in auto-commit both undo the whole
//! statement. `FAIL` differs from `ABORT` in *both* modes — it keeps the rows
//! the statement applied before the abort — so #5464 wires the auto-commit path
//! to commit-on-`FAIL` / rollback-otherwise, not to treat all three variants
//! identically. See [`apply_raise_scope`] and [`run_top_level_dml`].
//!
//! ## Statement atomicity (auto-commit, #5464)
//!
//! SQLite wraps every top-level statement in an implicit transaction. Inside an
//! explicit transaction VibeSQL uses an implicit *statement savepoint*; in
//! auto-commit it wraps the statement in an implicit *transaction* so a
//! mid-statement abort (RAISE(ABORT), FK cascade abort, constraint violation)
//! rolls back the statement's already-applied rows — matching sqlite3 3.51,
//! where e.g. a multi-row DML or cascade that aborts on a later row undoes the
//! earlier ones too. See [`run_top_level_dml`].
//!
//! ## Replication determinism
//!
//! The abort scope is a pure function of the `RaiseAction` carried in the
//! error and the storage transaction state — both of which are identical on
//! every replica that applies the same log entry — so the surviving rows are
//! deterministic across nodes. (The replicated apply path additionally rolls
//! back the *whole* buffered batch on any rejected statement, which is a
//! strictly coarser, equally-deterministic scope; see
//! `vibesql-consensus::state_machine`.)

use vibesql_ast::RaiseAction;
use vibesql_storage::Database;

use crate::errors::ExecutorError;

/// Cheap pre-check: does `table` carry any trigger that could fire a `RAISE`?
///
/// A `RAISE()` can only originate from a trigger body, so a table with no
/// triggers can never need a statement savepoint. This lets the common
/// (trigger-free) DML path skip the snapshot entirely.
///
/// FK cascade caveat (#5440): a DELETE/UPDATE on `table` can cascade into
/// child tables and fire *their* row triggers, which may `RAISE(ABORT)`. So
/// when foreign keys are enabled and any table in the database carries a
/// trigger, this statement could still fire a `RAISE()` even if `table`
/// itself has none. We arm the savepoint conservatively in that case so a
/// cascade-fired `RAISE(ABORT)` rolls back the whole statement (matching
/// sqlite3 3.51). The check is cheap: it only walks the catalog, and only
/// when FKs are on.
pub(crate) fn table_may_fire_trigger(db: &Database, table: &str) -> bool {
    if db.catalog.get_triggers_for_table(table, None).next().is_some() {
        return true;
    }

    // O(1) short-circuit for the dominant hot path: a database with zero
    // triggers can never fire a `RAISE()` (or any cascade-reached trigger),
    // regardless of FK state. Returning here avoids the allocating
    // `list_tables()` walk below on *every* trigger-free auto-commit write —
    // including the very common `PRAGMA foreign_keys=ON` + no-triggers case.
    if !db.catalog.has_any_triggers() {
        return false;
    }

    // Cascade can reach another table's trigger only when FK enforcement is
    // active. Bail out cheaply otherwise.
    if !db.foreign_keys_enabled() {
        return false;
    }

    // Conservative: any trigger anywhere in the DB could be reached by a
    // cascade chain rooted at `table`. We don't trace the full FK graph here
    // — arming an unused savepoint is harmless (it is released on success),
    // whereas missing one would silently skip statement rollback.
    db.catalog
        .list_tables()
        .iter()
        .any(|t| db.catalog.get_triggers_for_table(t, None).next().is_some())
}

/// Apply the transaction-scope rollback for a `RAISE(action, msg)` that
/// aborted a top-level statement, then return the (unchanged) error so the
/// caller can propagate it.
///
/// Assumes the executor armed an implicit statement savepoint via
/// [`Database::arm_statement_savepoint`] before running the statement (when a
/// transaction is active). The savepoint is consumed here regardless of which
/// scope is chosen.
///
/// - `Abort` → roll back to the statement savepoint (undo this statement only).
/// - `Fail` → keep the statement's partial changes; just release the savepoint.
/// - `Rollback` → roll back the whole transaction.
/// - `Ignore` → unreachable (handled as a control-flow signal upstream); kept
///   as a no-op for totality.
pub(crate) fn apply_raise_scope(
    db: &mut Database,
    action: RaiseAction,
    message: String,
) -> ExecutorError {
    match action {
        RaiseAction::Abort => {
            // Undo just this statement's changes; the transaction stays open
            // and earlier statements survive. No-op outside a transaction
            // (nothing was armed) — the statement's changes are dropped by the
            // caller's own per-statement rollback paths / auto-commit instead.
            db.rollback_statement_savepoint();
        }
        RaiseAction::Fail => {
            // Keep the partial changes the statement already applied; only
            // discard the now-unneeded snapshot.
            db.release_statement_savepoint();
        }
        RaiseAction::Rollback => {
            // Roll back the entire enclosing transaction. Best-effort: if no
            // transaction is active (RAISE(ROLLBACK) outside BEGIN), there is
            // nothing to roll back beyond the statement itself.
            db.release_statement_savepoint();
            if db.in_transaction() {
                let _ = db.rollback_transaction();
            }
        }
        RaiseAction::Ignore => {
            // Control-flow signal handled upstream; never reaches here.
            db.release_statement_savepoint();
        }
    }
    ExecutorError::Raise { action, message }
}

/// Run a top-level DML statement with SQLite per-variant `RAISE` scope
/// handling and statement-level atomicity.
///
/// SQLite wraps **every** top-level statement in an implicit transaction that
/// commits on success and rolls back atomically on any error — so a statement
/// that partially applies then aborts (a multi-row DML / FK cascade firing a
/// `RAISE(ABORT)`, an AFTER-trigger abort after earlier rows already landed,
/// or a constraint violation) leaves *no* partial changes. VibeSQL realizes
/// the two modes a statement can run in:
///
/// 1. **Inside an explicit transaction** (`db.in_transaction()`): arm an
///    implicit *statement savepoint* before `f` so a `RAISE(ABORT)` undoes just
///    this statement while earlier statements in the transaction survive, the
///    transaction staying open. This is the #5417/#5431/#5440 path, unchanged.
///
/// 2. **In auto-commit** (no explicit transaction, #5464): wrap the statement
///    in an *implicit transaction* (`begin_transaction`) so the same partial
///    changes can be rolled back. On success the implicit transaction commits;
///    on `RAISE(ABORT)`/`RAISE(ROLLBACK)` or any other error it rolls back the
///    whole statement; on `RAISE(FAIL)` it commits the rows applied so far
///    (matching sqlite3 3.51 FAIL semantics). The implicit transaction emits
///    the usual `TxnBegin`/`TxnCommit`/`TxnRollback` WAL markers, so crash
///    recovery buffers and applies-or-discards the statement atomically too.
///
/// In both modes the work only happens when `may_fire_trigger` is true (the
/// only way a `RAISE()` can fire, and the only way a single statement applies
/// rows incrementally across trigger programs). When no trigger can fire this
/// is a thin pass-through with zero transaction/snapshot cost — preserving the
/// hot path for the common trigger-free statement.
pub(crate) fn run_top_level_dml<T, F>(
    db: &mut Database,
    may_fire_trigger: bool,
    f: F,
) -> Result<T, ExecutorError>
where
    F: FnOnce(&mut Database) -> Result<T, ExecutorError>,
{
    // Fast path: nothing can RAISE and no trigger program can apply rows
    // incrementally — run directly with no transaction/snapshot overhead.
    if !may_fire_trigger {
        return f(db);
    }

    if db.in_transaction() {
        run_inside_transaction(db, f)
    } else {
        run_auto_commit(db, f)
    }
}

/// Statement-scope handling when an explicit transaction is already open: arm
/// an implicit statement savepoint so a `RAISE(ABORT)` undoes just this
/// statement, leaving earlier statements (and the transaction) intact.
fn run_inside_transaction<T, F>(db: &mut Database, f: F) -> Result<T, ExecutorError>
where
    F: FnOnce(&mut Database) -> Result<T, ExecutorError>,
{
    let armed = db.arm_statement_savepoint();

    match f(db) {
        Ok(v) => {
            if armed {
                db.release_statement_savepoint();
            }
            Ok(v)
        }
        Err(ExecutorError::Raise { action, message }) => {
            // Apply the per-variant scope. `apply_raise_scope` consumes the
            // savepoint (armed or not) and returns the error to propagate.
            Err(apply_raise_scope(db, action, message))
        }
        Err(other) => {
            // Non-RAISE errors keep their pre-existing behavior. The DML
            // executors already perform their own targeted rollback for these
            // (e.g. constraint failures), so we just drop the snapshot.
            if armed {
                db.release_statement_savepoint();
            }
            Err(other)
        }
    }
}

/// Statement-scope handling in auto-commit mode (#5464): wrap the statement in
/// an implicit transaction so its partial changes can be rolled back as a unit,
/// matching SQLite's implicit per-statement transaction.
///
/// If the implicit `begin_transaction` fails (it should not in auto-commit, but
/// be defensive) we fall back to running `f` directly — no worse than the
/// pre-#5464 behavior.
fn run_auto_commit<T, F>(db: &mut Database, f: F) -> Result<T, ExecutorError>
where
    F: FnOnce(&mut Database) -> Result<T, ExecutorError>,
{
    if db.begin_transaction().is_err() {
        return f(db);
    }

    match f(db) {
        Ok(v) => {
            // Statement succeeded: commit the implicit transaction so the
            // changes (and their WAL ops) become durable as one auto-commit
            // unit. A failed commit surfaces as the statement's error.
            commit_implicit(db)?;
            Ok(v)
        }
        Err(ExecutorError::Raise { action, message }) => {
            // Map the RAISE variant onto the implicit transaction, mirroring
            // the savepoint scopes (the implicit txn *is* the statement scope
            // here, so statement-rollback == transaction-rollback):
            //   ABORT / ROLLBACK -> undo the whole statement
            //   FAIL             -> keep the rows applied before the abort
            match action {
                RaiseAction::Fail => {
                    commit_implicit_best_effort(db);
                }
                _ => {
                    rollback_implicit_best_effort(db);
                }
            }
            Err(ExecutorError::Raise { action, message })
        }
        Err(other) => {
            // Any other error (constraint violation, FK failure, …) rolls back
            // the whole statement — SQLite's statement atomicity. This undoes
            // partial multi-row / cascade changes that the executor's own
            // targeted rollback may have left behind.
            rollback_implicit_best_effort(db);
            Err(other)
        }
    }
}

/// Commit the implicit auto-commit transaction, propagating a commit failure as
/// an executor error (so e.g. a deferred-FK COMMIT abort surfaces correctly).
fn commit_implicit(db: &mut Database) -> Result<(), ExecutorError> {
    db.commit_transaction().map_err(ExecutorError::from)
}

/// Commit the implicit transaction on a path that already carries the error to
/// return (RAISE(FAIL)); a commit failure here cannot change the reported
/// error, so it is best-effort.
fn commit_implicit_best_effort(db: &mut Database) {
    let _ = db.commit_transaction();
}

/// Roll back the implicit transaction, best-effort: the statement already has
/// an error to propagate, and rollback restores the pre-statement snapshot.
fn rollback_implicit_best_effort(db: &mut Database) {
    if db.in_transaction() {
        let _ = db.rollback_transaction();
    }
}
