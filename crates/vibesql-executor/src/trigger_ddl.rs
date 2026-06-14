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
    /// Execute a CREATE TRIGGER statement.
    ///
    /// This is a convenience wrapper around [`TriggerExecutor::create_trigger_with_sql`]
    /// that does not preserve the original SQL text. Triggers created via this path
    /// will not survive SQL-dump persistence round-trips.
    pub fn create_trigger(
        db: &mut Database,
        stmt: &CreateTriggerStmt,
    ) -> Result<String, ExecutorError> {
        Self::create_trigger_with_sql(db, stmt, None)
    }

    /// Execute a CREATE TRIGGER statement, optionally preserving the original SQL text
    /// for SQL-dump persistence.
    ///
    /// When `original_sql` is `Some`, the SQL is stored on the catalog
    /// [`TriggerDefinition`] so it can be re-emitted verbatim by `save_sql_dump`.
    /// This is the path used by the CLI / Python / WASM frontends, which are the
    /// only callers that have access to the original textual SQL.
    pub fn create_trigger_with_sql(
        db: &mut Database,
        stmt: &CreateTriggerStmt,
        original_sql: Option<&str>,
    ) -> Result<String, ExecutorError> {
        // `CREATE TRIGGER IF NOT EXISTS <name>` is a no-op success when a
        // trigger with that name already exists (SQLite semantics, trigger1-1.2.0).
        // SQLite resolves the existing-trigger check before validating the target
        // object, so an already-present trigger short-circuits here.
        if stmt.if_not_exists && db.catalog.get_trigger(&stmt.trigger_name).is_some() {
            return Ok(format!("Trigger '{}' already exists", stmt.trigger_name));
        }

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

        // Create trigger definition from statement, preserving original SQL when available
        let trigger = match original_sql {
            Some(sql) => TriggerDefinition::new_with_sql(
                stmt.trigger_name.clone(),
                stmt.timing.clone(),
                stmt.event.clone(),
                stmt.table_name.clone(),
                stmt.granularity.clone(),
                stmt.when_condition.clone(),
                stmt.triggered_action.clone(),
                sql.to_string(),
            ),
            None => TriggerDefinition::new(
                stmt.trigger_name.clone(),
                stmt.timing.clone(),
                stmt.event.clone(),
                stmt.table_name.clone(),
                stmt.granularity.clone(),
                stmt.when_condition.clone(),
                stmt.triggered_action.clone(),
            ),
        };

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
            // `DROP TRIGGER IF EXISTS` on a missing trigger is a no-op
            // (SQLite / SQL:2008 semantics); a bare DROP TRIGGER still errors.
            if stmt.if_exists {
                return Ok(format!(
                    "Trigger '{}' does not exist, skipping",
                    stmt.trigger_name
                ));
            }
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
