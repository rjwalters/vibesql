//! Executor for TRIGGER objects (SQL:1999)

use vibesql_ast::*;
use vibesql_storage::Database;

use crate::errors::ExecutorError;

/// Execute CREATE TRIGGER statement.
///
/// Delegates to [`crate::TriggerExecutor::create_trigger`] so that every
/// CREATE TRIGGER entry point (CLI, consensus, WASM) shares one validation path
/// — IF NOT EXISTS handling, system-table rejection, and the
/// view/table/INSTEAD-OF target checks (with SQLite-exact error wording). The
/// success message is discarded here since this entry point returns `()`.
pub fn execute_create_trigger(
    stmt: &CreateTriggerStmt,
    db: &mut Database,
) -> Result<(), ExecutorError> {
    crate::TriggerExecutor::create_trigger(db, stmt).map(|_| ())
}

/// Execute ALTER TRIGGER statement
pub fn execute_alter_trigger(
    stmt: &AlterTriggerStmt,
    db: &mut Database,
) -> Result<(), ExecutorError> {
    use vibesql_ast::AlterTriggerAction;

    // Get the trigger (verify it exists)
    let mut trigger = db
        .catalog
        .get_trigger(&stmt.trigger_name)
        .ok_or_else(|| ExecutorError::TriggerNotFound(stmt.trigger_name.clone()))?
        .clone();

    // Apply the action
    match stmt.action {
        AlterTriggerAction::Enable => {
            trigger.enable();
        }
        AlterTriggerAction::Disable => {
            trigger.disable();
        }
    }

    // Update in catalog
    db.catalog.update_trigger(trigger)?;
    Ok(())
}

/// Execute DROP TRIGGER statement
pub fn execute_drop_trigger(
    stmt: &DropTriggerStmt,
    db: &mut Database,
) -> Result<(), ExecutorError> {
    // `DROP TRIGGER IF EXISTS` on a missing trigger is a no-op (SQLite /
    // SQL:2008); a bare DROP TRIGGER errors with TriggerNotFound.
    if stmt.if_exists && db.catalog.get_trigger(&stmt.trigger_name).is_none() {
        return Ok(());
    }
    db.catalog.drop_trigger(&stmt.trigger_name)?;
    Ok(())
}
