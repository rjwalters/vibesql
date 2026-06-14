//! Executor for TRIGGER objects (SQL:1999)

use vibesql_ast::*;
use vibesql_storage::Database;

use crate::errors::ExecutorError;

/// Execute CREATE TRIGGER statement
pub fn execute_create_trigger(
    stmt: &CreateTriggerStmt,
    db: &mut Database,
) -> Result<(), ExecutorError> {
    use vibesql_catalog::TriggerDefinition;

    // `CREATE TRIGGER IF NOT EXISTS <name>` is a no-op success when a trigger
    // with that name already exists (SQLite semantics, trigger1-1.2.0). Mirrors
    // the guard in `TriggerExecutor::create_trigger_with_sql` so every CREATE
    // TRIGGER path honors the clause.
    if stmt.if_not_exists && db.catalog.get_trigger(&stmt.trigger_name).is_some() {
        return Ok(());
    }

    let trigger = TriggerDefinition::new(
        stmt.trigger_name.clone(),
        stmt.timing.clone(),
        stmt.event.clone(),
        stmt.table_name.clone(),
        stmt.granularity.clone(),
        stmt.when_condition.clone(),
        stmt.triggered_action.clone(),
    );

    db.catalog.create_trigger(trigger)?;
    Ok(())
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
