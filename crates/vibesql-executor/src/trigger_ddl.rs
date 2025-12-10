//! Trigger DDL execution module
//!
//! Handles CREATE TRIGGER, ALTER TRIGGER, and DROP TRIGGER statements

use vibesql_ast::{
    AlterTriggerAction, AlterTriggerStmt, CreateTriggerStmt, DropTriggerStmt, TriggerTiming,
};
use vibesql_catalog::TriggerDefinition;
use vibesql_storage::Database;

use crate::errors::ExecutorError;

/// Executor for trigger DDL operations
pub struct TriggerExecutor;

impl TriggerExecutor {
    /// Execute a CREATE TRIGGER statement
    pub fn create_trigger(
        db: &mut Database,
        stmt: &CreateTriggerStmt,
    ) -> Result<String, ExecutorError> {
        // INSTEAD OF triggers can only be created on views
        // BEFORE and AFTER triggers can only be created on tables
        if stmt.timing == TriggerTiming::InsteadOf {
            // Verify the target view exists
            if db.catalog.get_view(&stmt.table_name).is_none() {
                return Err(ExecutorError::Other(format!(
                    "INSTEAD OF trigger requires a view, but '{}' is not a view",
                    stmt.table_name
                )));
            }
        } else {
            // Verify the target table exists
            if !db.catalog.table_exists(&stmt.table_name) {
                return Err(ExecutorError::TableNotFound(stmt.table_name.clone()));
            }
        }

        // Create trigger definition from statement
        let trigger = TriggerDefinition::new(
            stmt.trigger_name.clone(),
            stmt.timing.clone(),
            stmt.event.clone(),
            stmt.table_name.clone(),
            stmt.granularity.clone(),
            stmt.when_condition.clone(),
            stmt.triggered_action.clone(),
        );

        // Store in catalog
        db.catalog.create_trigger(trigger)?;

        Ok(format!("Trigger '{}' created successfully", stmt.trigger_name))
    }

    /// Execute an ALTER TRIGGER statement
    pub fn alter_trigger(
        db: &mut Database,
        stmt: &AlterTriggerStmt,
    ) -> Result<String, ExecutorError> {
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
                db.catalog.update_trigger(trigger)?;
                Ok(format!("Trigger '{}' enabled successfully", stmt.trigger_name))
            }
            AlterTriggerAction::Disable => {
                trigger.disable();
                db.catalog.update_trigger(trigger)?;
                Ok(format!("Trigger '{}' disabled successfully", stmt.trigger_name))
            }
        }
    }

    /// Execute a DROP TRIGGER statement
    pub fn drop_trigger(
        db: &mut Database,
        stmt: &DropTriggerStmt,
    ) -> Result<String, ExecutorError> {
        // Check if trigger exists
        if db.catalog.get_trigger(&stmt.trigger_name).is_none() {
            return Err(ExecutorError::TriggerNotFound(stmt.trigger_name.clone()));
        }

        // Remove from catalog
        db.catalog.drop_trigger(&stmt.trigger_name)?;

        // Note: CASCADE is not yet implemented
        // When CASCADE is implemented, we would need to also drop any
        // dependent objects (though triggers typically don't have dependents)

        Ok(format!("Trigger '{}' dropped successfully", stmt.trigger_name))
    }
}
