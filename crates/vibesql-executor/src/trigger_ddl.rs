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

        // Reject triggers on SQLite system tables (any "sqlite_" prefixed name,
        // e.g. sqlite_master / sqlite_schema / sqlite_stat1). SQLite emits exactly
        // "cannot create trigger on system table" (sqlite3 3.51.0, trigger1-1.9).
        // These messages are produced verbatim via `Other` so the TCL conformance
        // shim passes them through unchanged.
        if is_system_table_name(&stmt.table_name) {
            return Err(ExecutorError::Other(
                "cannot create trigger on system table".to_string(),
            ));
        }

        // INSTEAD OF triggers can only be created on views;
        // BEFORE and AFTER triggers can only be created on (non-view) tables.
        let target_is_view = db.catalog.get_view(&stmt.table_name).is_some();
        if stmt.timing == TriggerTiming::InsteadOf {
            // INSTEAD OF on a real table is rejected with SQLite's exact wording
            // "cannot create INSTEAD OF trigger on table: <name>" (trigger1-1.12).
            if !target_is_view {
                if db.catalog.table_exists(&stmt.table_name) {
                    return Err(ExecutorError::Other(format!(
                        "cannot create INSTEAD OF trigger on table: {}",
                        stmt.table_name
                    )));
                }
                // Neither a view nor a table: report the missing target.
                return Err(ExecutorError::TableNotFound(stmt.table_name.clone()));
            }
        } else {
            // BEFORE / AFTER on a view is rejected with SQLite's exact wording
            // "cannot create BEFORE|AFTER trigger on view: <name>" (trigger1-1.13/1.14).
            if target_is_view {
                let timing_label = match stmt.timing {
                    TriggerTiming::Before => "BEFORE",
                    TriggerTiming::After => "AFTER",
                    // InsteadOf handled in the branch above.
                    TriggerTiming::InsteadOf => unreachable!(),
                };
                return Err(ExecutorError::Other(format!(
                    "cannot create {} trigger on view: {}",
                    timing_label, stmt.table_name
                )));
            }
            // Verify the target table exists.
            if !db.catalog.table_exists(&stmt.table_name) {
                return Err(ExecutorError::TableNotFound(stmt.table_name.clone()));
            }
        }

        // Create trigger definition from statement, preserving original SQL when available.
        // The trigger's schema (from `CREATE TEMP TRIGGER` or an explicit
        // `schema.` prefix) is threaded through so it binds to the correct
        // table when a temp table shadows a main table of the same name.
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
        }
        .with_schema(stmt.schema.clone());

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

/// Returns true if `name` refers to a SQLite system table. SQLite reserves any
/// object whose name begins with the case-insensitive prefix `sqlite_` for
/// internal use (sqlite_master / sqlite_schema / sqlite_stat* / sqlite_sequence,
/// etc.), and rejects `CREATE TRIGGER` on such tables.
fn is_system_table_name(name: &str) -> bool {
    let bytes = name.as_bytes();
    bytes.len() >= 7 && bytes[..7].eq_ignore_ascii_case(b"sqlite_")
}
