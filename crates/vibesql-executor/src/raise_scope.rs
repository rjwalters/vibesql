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
//! The distinction is only observable inside an explicit transaction: outside
//! one, every statement is its own auto-commit unit, so all three variants
//! leave identical state (the failing statement's changes vanish either way).
//! See [`apply_raise_scope`] and [`run_top_level_dml`].
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
/// handling.
///
/// When a transaction is active **and** `may_fire_trigger` is true (the only
/// way a `RAISE()` can fire), this arms an implicit statement savepoint before
/// running `f`, so a `RAISE(ABORT)` can undo just this statement. On a
/// `RAISE(..)` error it applies the per-variant scope; on success or any other
/// error it releases the savepoint (leaving the changes in place — non-RAISE
/// errors keep their existing rollback behavior, unchanged by this wrapper).
///
/// When no transaction is active, or the table has no triggers, this is a
/// thin pass-through with zero snapshot cost — preserving the hot path.
pub(crate) fn run_top_level_dml<T, F>(
    db: &mut Database,
    may_fire_trigger: bool,
    f: F,
) -> Result<T, ExecutorError>
where
    F: FnOnce(&mut Database) -> Result<T, ExecutorError>,
{
    // Fast path: nothing can RAISE (no triggers) or there is no surrounding
    // transaction whose earlier statements must survive — run directly.
    let armed =
        if may_fire_trigger && db.in_transaction() { db.arm_statement_savepoint() } else { false };

    let result = f(db);

    match result {
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
