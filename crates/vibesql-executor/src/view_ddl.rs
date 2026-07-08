//! View DDL executor

use vibesql_ast::{CreateViewStmt, DropViewStmt};
use vibesql_catalog::{ViewDefinition, ViewDropBehavior};
use vibesql_storage::Database;

use crate::errors::ExecutorError;

/// Executor for view DDL statements
pub struct ViewExecutor;

impl ViewExecutor {
    /// Execute CREATE VIEW, CREATE OR REPLACE VIEW, or CREATE VIEW IF NOT EXISTS
    pub fn execute_create_view(
        stmt: &CreateViewStmt,
        database: &mut Database,
    ) -> Result<String, ExecutorError> {
        let view_exists = database.catalog.get_view(&stmt.view_name).is_some();

        // If IF NOT EXISTS and view already exists, just return success
        if stmt.if_not_exists && view_exists {
            return Ok(format!("View '{}' already exists (skipped)", stmt.view_name));
        }

        // If OR REPLACE, drop the view first if it exists
        if stmt.or_replace && view_exists {
            // Drop the existing view (no cascade needed for OR REPLACE)
            database.catalog.drop_view(&stmt.view_name, false).map_err(|e| {
                ExecutorError::StorageError(format!("Failed to drop existing view: {:?}", e))
            })?;
        }

        // Tag temp views with the `temp` schema so they surface via
        // sqlite_temp_master and are excluded from sqlite_master (#5541),
        // mirroring the temp-trigger (#5532) and temp-index (#5513) tags.
        let schema = if stmt.temporary { Some("temp".to_string()) } else { None };

        // Create the view definition
        let view_def = if let Some(ref sql) = stmt.sql_definition {
            ViewDefinition::new_with_sql(
                stmt.view_name.clone(),
                stmt.columns.clone(),
                *stmt.query.clone(),
                stmt.with_check_option,
                sql.clone(),
            )
        } else {
            ViewDefinition::new(
                stmt.view_name.clone(),
                stmt.columns.clone(),
                *stmt.query.clone(),
                stmt.with_check_option,
            )
        }
        .with_schema(schema);

        // Add to catalog
        database
            .catalog
            .create_view(view_def)
            .map_err(|e| ExecutorError::StorageError(format!("Failed to create view: {:?}", e)))?;

        if stmt.or_replace {
            Ok(format!("View '{}' created or replaced", stmt.view_name))
        } else {
            Ok(format!("View '{}' created", stmt.view_name))
        }
    }

    /// Execute DROP VIEW
    pub fn execute_drop_view(
        stmt: &DropViewStmt,
        database: &mut Database,
    ) -> Result<String, ExecutorError> {
        // Determine drop behavior:
        // - CASCADE: drop dependent views recursively
        // - RESTRICT (explicit): fail if dependents exist
        // - Neither: SQLite-compatible behavior (just drop, ignore dependents)
        let drop_behavior = if stmt.cascade {
            ViewDropBehavior::Cascade
        } else if stmt.restrict {
            ViewDropBehavior::Restrict
        } else {
            ViewDropBehavior::Silent // SQLite-compatible: allow dropping even with dependents
        };

        // Cascade-drop the INSTEAD OF triggers defined ON this view, matching
        // sqlite3 3.51.0: an INSTEAD OF trigger cannot outlive the view it is
        // attached to, so `DROP VIEW v` removes every trigger whose `ON v`
        // target is the dropped view. Resolve the view's temp/main schema tag
        // *before* the catalog removes it so the temp-shadows-main filter can be
        // applied (a main trigger on a main view must not be dropped when a
        // same-named temp view is dropped, and vice versa). A trigger on a
        // *different* view is left alone. See `Catalog::drop_view_triggers`
        // (view analogue of the table path in #5597).
        let view_is_temp = database.catalog.get_view(&stmt.view_name).is_some_and(|v| v.is_temp());

        // Drop the view
        let result = database.catalog.drop_view_with_behavior(&stmt.view_name, drop_behavior);

        match result {
            Ok(()) => {
                let dropped_triggers =
                    database.catalog.drop_view_triggers(&stmt.view_name, view_is_temp);
                if dropped_triggers.is_empty() {
                    Ok(format!("View '{}' dropped", stmt.view_name))
                } else {
                    Ok(format!(
                        "View '{}' and {} associated trigger(s) dropped",
                        stmt.view_name,
                        dropped_triggers.len()
                    ))
                }
            }
            Err(e) => {
                // If IF EXISTS and view doesn't exist, that's OK
                if stmt.if_exists
                    && matches!(e, vibesql_catalog::errors::CatalogError::ViewNotFound(_))
                {
                    Ok(format!("View '{}' does not exist (skipped)", stmt.view_name))
                } else {
                    Err(ExecutorError::StorageError(format!("Failed to drop view: {:?}", e)))
                }
            }
        }
    }
}
