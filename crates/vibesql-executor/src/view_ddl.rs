//! View DDL executor

use vibesql_ast::{CreateViewStmt, DropViewStmt};
use vibesql_catalog::{ViewDefinition, ViewDropBehavior};
use vibesql_storage::Database;

use crate::errors::ExecutorError;

/// Split a possibly schema-qualified `CREATE VIEW`/`DROP VIEW` name into its
/// `(schema, unqualified name)` parts, mirroring
/// `CreateTableExecutor::execute_impl`'s handling of `stmt.table_name`
/// (`create_table.rs`) so `CREATE VIEW <alias>.<name>` against an
/// attached-database alias creates a view named `<name>` inside `<alias>`
/// instead of literally storing `"<alias>.<name>"` in the default schema
/// (issue #6490).
///
/// - `CREATE TEMP VIEW <name>` (no dot) -> `(Some("temp"), name)`.
/// - `CREATE TEMP VIEW temp.<name>` -> `(Some("temp"), name)` (the redundant `temp.` qualifier is
///   allowed, matching `CREATE TEMP TABLE temp.t(...)`).
/// - `CREATE TEMP VIEW <schema>.<name>` for any other schema -> error "temporary view name must be
///   unqualified" (mirrors the CREATE TEMP TABLE handling for #6173/#6406).
/// - `CREATE VIEW <schema>.<name>` -> `(Some(schema), name)` verbatim; the schema may be `main`,
///   `temp`, or an attached alias.
/// - `CREATE VIEW <name>` (no dot, not temporary) -> `(None, name)`.
pub(crate) fn split_view_schema_qualifier(
    view_name: &str,
    temporary: bool,
) -> Result<(Option<String>, String), ExecutorError> {
    if temporary {
        if let Some((schema_part, name_part)) = view_name.split_once('.') {
            if !schema_part.eq_ignore_ascii_case(vibesql_catalog::TEMP_SCHEMA) {
                return Err(ExecutorError::SqliteCompatError(
                    "temporary view name must be unqualified".to_string(),
                ));
            }
            Ok((Some(vibesql_catalog::TEMP_SCHEMA.to_string()), name_part.to_string()))
        } else {
            Ok((Some(vibesql_catalog::TEMP_SCHEMA.to_string()), view_name.to_string()))
        }
    } else if let Some((schema_part, name_part)) = view_name.split_once('.') {
        Ok((Some(schema_part.to_string()), name_part.to_string()))
    } else {
        Ok((None, view_name.to_string()))
    }
}

/// Validate that an explicit schema qualifier names `main`, `temp`, or an
/// existing (e.g. ATTACHed, #6310) schema. Mirrors the identical guard in
/// `TriggerExecutor::create_trigger_with_sql` (`trigger_ddl.rs`) and
/// `CreateTableExecutor::execute_impl` (`create_table.rs`), echoing SQLite's
/// `unknown database <name>` wording for a qualifier that resolves to
/// nothing.
pub(crate) fn validate_view_schema_qualifier(
    schema: Option<&str>,
    database: &Database,
) -> Result<(), ExecutorError> {
    if let Some(schema) = schema {
        if !schema.eq_ignore_ascii_case(vibesql_catalog::DEFAULT_SCHEMA)
            && !schema.eq_ignore_ascii_case(vibesql_catalog::TEMP_SCHEMA)
            && !database.catalog.schema_exists(schema)
            && !database.catalog.schema_exists(&schema.to_ascii_lowercase())
        {
            return Err(ExecutorError::SqliteCompatError(format!("unknown database {}", schema)));
        }
    }
    Ok(())
}

/// Executor for view DDL statements
pub struct ViewExecutor;

impl ViewExecutor {
    /// Execute CREATE VIEW, CREATE OR REPLACE VIEW, or CREATE VIEW IF NOT EXISTS
    pub fn execute_create_view(
        stmt: &CreateViewStmt,
        database: &mut Database,
    ) -> Result<String, ExecutorError> {
        // Split off any schema qualifier (`<alias>.<name>`) so the view is
        // homed in the right schema instead of literally storing the dotted
        // name in the default schema (#6490).
        let (schema, unqualified_name) =
            split_view_schema_qualifier(&stmt.view_name, stmt.temporary)?;
        validate_view_schema_qualifier(schema.as_deref(), database)?;

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

        // Create the view definition
        let view_def = if let Some(ref sql) = stmt.sql_definition {
            ViewDefinition::new_with_sql(
                unqualified_name,
                stmt.columns.clone(),
                *stmt.query.clone(),
                stmt.with_check_option,
                sql.clone(),
            )
        } else {
            ViewDefinition::new(
                unqualified_name,
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
