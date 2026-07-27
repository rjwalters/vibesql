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

use vibesql_ast::{ConflictClause, RaiseAction};
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
    run_top_level_dml_with_conflict_scope(db, may_fire_trigger, None, f)
}

/// Like [`run_top_level_dml`], but also applies the *statement's own*
/// `OR <action>` conflict-resolution clause (`ConflictClause`, e.g.
/// `INSERT/UPDATE/DELETE OR ROLLBACK|FAIL|ABORT`) to a non-`RAISE` error —
/// not just a `RAISE()` fired from a trigger body.
///
/// SQLite's `OR ROLLBACK`/`OR FAIL` conflict-resolution algorithms have the
/// exact same transaction-scope semantics as the identically-named
/// `RAISE(ROLLBACK|FAIL, …)` variants (R-59980-59522 for `ROLLBACK`): a bare
/// constraint violation (`ExecutorError::ConstraintViolation` /
/// `SqliteCompatError`, not `ExecutorError::Raise`) under `OR ROLLBACK` must
/// abort the whole enclosing transaction (autocommit re-enabled), and under
/// `OR FAIL` must keep the rows already applied before the offending row.
/// Passing `None` (via [`run_top_level_dml`]) preserves the prior behavior:
/// any non-`RAISE` error rolls back just the statement, matching the implicit
/// default (`ABORT`).
pub(crate) fn run_top_level_dml_with_conflict_scope<T, F>(
    db: &mut Database,
    may_fire_trigger: bool,
    conflict_clause: Option<ConflictClause>,
    f: F,
) -> Result<T, ExecutorError>
where
    F: FnOnce(&mut Database) -> Result<T, ExecutorError>,
{
    // A statement carrying `OR ROLLBACK`/`OR FAIL` needs the same
    // transaction/savepoint wrapping a trigger-fireable statement gets, even
    // when the table has no triggers at all — a bare constraint violation
    // must still get the ROLLBACK/FAIL scope rather than the trigger-free
    // fast path's "just propagate the error" behavior.
    let needs_scope_wrapper = may_fire_trigger
        || matches!(conflict_clause, Some(ConflictClause::Rollback) | Some(ConflictClause::Fail));

    // Fast path: nothing can RAISE, no trigger program can apply rows
    // incrementally, and the statement's own conflict clause needs no special
    // transaction scope — run directly with no transaction/snapshot overhead.
    if !needs_scope_wrapper {
        return f(db);
    }

    if db.in_transaction() {
        run_inside_transaction(db, conflict_clause, f)
    } else {
        run_auto_commit(db, conflict_clause, f)
    }
}

/// Statement-scope handling when an explicit transaction is already open: arm
/// an implicit statement savepoint so a `RAISE(ABORT)` undoes just this
/// statement, leaving earlier statements (and the transaction) intact.
fn run_inside_transaction<T, F>(
    db: &mut Database,
    conflict_clause: Option<ConflictClause>,
    f: F,
) -> Result<T, ExecutorError>
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
            // A FOREIGN KEY violation is exempt from the statement's own
            // conflict clause — SQLite always treats it as ABORT regardless of
            // `OR IGNORE`/`OR FAIL`/`OR ROLLBACK`/`OR REPLACE` on the statement
            // (fkey2-20.2.x/20.3.x). Route it through the default arm below
            // even when `conflict_clause` says otherwise.
            let scoped_clause =
                if other.is_foreign_key_violation() { None } else { conflict_clause };
            match scoped_clause {
                Some(ConflictClause::Rollback) => {
                    // `OR ROLLBACK`: abort the entire enclosing transaction,
                    // exactly like `RAISE(ROLLBACK)` (R-59980-59522).
                    db.release_statement_savepoint();
                    if db.in_transaction() {
                        let _ = db.rollback_transaction();
                    }
                }
                Some(ConflictClause::Fail) => {
                    // `OR FAIL`: keep the rows already applied before the
                    // offending row; only release the now-unneeded snapshot.
                    // (The insert/update executors already stop collecting at
                    // the offending row themselves — this only matters when
                    // `may_fire_trigger` armed the wrapper for an unrelated
                    // reason, e.g. a trigger-bearing table elsewhere in an FK
                    // cascade.)
                    db.release_statement_savepoint();
                }
                _ => {
                    // Non-RAISE errors (constraint violations — FK/UNIQUE/CHECK/NOT
                    // NULL, including the cascade-orphan immediate-FK check from #5465)
                    // get SQLite's statement-atomicity scope: roll the statement
                    // savepoint *back*, undoing every partial change this statement
                    // applied (e.g. an already-cascaded child DELETE), while leaving
                    // the enclosing transaction open for the application to COMMIT or
                    // ROLLBACK. This mirrors the auto-commit path (`run_auto_commit`),
                    // which rolls back its whole implicit transaction on any `Err`, and
                    // `RAISE(ABORT)`, which also rolls the statement back.
                    //
                    // Releasing instead of rolling back (the pre-#5502 behavior) relied
                    // on the DML executors performing their own targeted rollback, but
                    // for cascade/multi-row paths there is no such rollback — leaving
                    // inconsistent partial state (#5502). RAISE-variant scopes are
                    // unaffected: they are handled in the arm above.
                    if armed {
                        db.rollback_statement_savepoint();
                    }
                }
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
fn run_auto_commit<T, F>(
    db: &mut Database,
    conflict_clause: Option<ConflictClause>,
    f: F,
) -> Result<T, ExecutorError>
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
            // targeted rollback may have left behind. In auto-commit the
            // implicit transaction *is* the statement, so `OR ROLLBACK` needs
            // no different handling here than the default (both discard the
            // whole statement); `OR FAIL` commits the partial changes instead,
            // mirroring `RAISE(FAIL)` above — except a FOREIGN KEY violation,
            // which is exempt from the statement's own conflict clause and
            // always gets the default (rollback) scope (fkey2-20.2.x/20.3.x).
            let scoped_clause =
                if other.is_foreign_key_violation() { None } else { conflict_clause };
            match scoped_clause {
                Some(ConflictClause::Fail) => commit_implicit_best_effort(db),
                _ => rollback_implicit_best_effort(db),
            }
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
