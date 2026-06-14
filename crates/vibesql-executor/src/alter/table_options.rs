//! Table-level operation executors for ALTER TABLE

use vibesql_ast::{RenameTableStmt, TriggerAction};
use vibesql_storage::Database;

use crate::{errors::ExecutorError, trigger_rename::rewrite_table_refs_in_trigger_sql};

/// Execute RENAME TABLE
pub(super) fn execute_rename_table(
    stmt: &RenameTableStmt,
    database: &mut Database,
) -> Result<String, ExecutorError> {
    // Check if the new name already names a table or an index. SQLite shares a
    // single object namespace for tables and indexes, so a RENAME TO collision
    // against either reports the same SQLite-compatible message
    // (`there is already another table or index with this name: <name>`).
    if database.get_table(&stmt.new_table_name).is_some()
        || database.index_exists(&stmt.new_table_name)
    {
        return Err(ExecutorError::RenameTargetExists(stmt.new_table_name.clone()));
    }

    // Get the old table to ensure it exists
    let old_table = database
        .get_table(&stmt.table_name)
        .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?;

    // Clone the table and update its schema name
    let mut new_table = old_table.clone();
    new_table.schema_mut().name = stmt.new_table_name.clone();

    // Drop old table and create new one with the renamed schema
    // This handles indexes and spatial indexes via CASCADE
    database
        .drop_table(&stmt.table_name)
        .map_err(|e| ExecutorError::StorageError(e.to_string()))?;

    database
        .create_table(new_table.schema.clone())
        .map_err(|e| ExecutorError::StorageError(e.to_string()))?;

    // Restore the data by getting the new table and setting its rows
    let restored_table = database
        .get_table_mut(&stmt.new_table_name)
        .ok_or_else(|| ExecutorError::TableAlreadyExists(stmt.new_table_name.clone()))?;

    for row in new_table.scan() {
        restored_table
            .insert(row.clone())
            .map_err(|e| ExecutorError::StorageError(e.to_string()))?;
    }

    // Propagate the rename into any trigger definitions that reference the old
    // table name. SQLite (legacy_alter_table=OFF) rewrites table references
    // inside trigger bodies and the trigger's ON-target, and keeps the stored
    // `sqlite_master.sql` text consistent. See `crate::trigger_rename`.
    rewrite_triggers_for_rename(database, &stmt.table_name, &stmt.new_table_name);

    // Invalidate the database-level columnar cache for both old and new table names.
    // The old table name's cache is invalidated since the table no longer exists,
    // and the new table name's cache is invalidated to ensure fresh columnar data.
    database.invalidate_columnar_cache(&stmt.table_name);
    database.invalidate_columnar_cache(&stmt.new_table_name);

    Ok(format!("Table '{}' renamed to '{}'", stmt.table_name, stmt.new_table_name))
}

/// Rewrite all trigger definitions in the catalog that reference `old_name`,
/// replacing table references with `new_name`.
///
/// This keeps three pieces of trigger state consistent with SQLite's
/// `legacy_alter_table=OFF` behavior:
/// - `table_name`: the trigger's ON-target, if it was the renamed table;
/// - `triggered_action` (the raw body SQL used when the trigger fires);
/// - `sql_definition` (the verbatim `CREATE TRIGGER` text shown in
///   `sqlite_master`/`sqlite_schema`).
fn rewrite_triggers_for_rename(database: &mut Database, old_name: &str, new_name: &str) {
    let trigger_names = database.catalog.list_triggers();
    for name in trigger_names {
        let Some(existing) = database.catalog.get_trigger(&name) else {
            continue;
        };

        // Does this trigger reference the renamed table anywhere?
        let on_target_match = existing.table_name.eq_ignore_ascii_case(old_name);
        let body_text = match &existing.triggered_action {
            TriggerAction::RawSql(sql) => sql.clone(),
        };
        let new_body = rewrite_table_refs_in_trigger_sql(&body_text, old_name, new_name);
        let new_sql_definition = existing
            .sql_definition
            .as_ref()
            .map(|sql| rewrite_table_refs_in_trigger_sql(sql, old_name, new_name));

        let body_changed = new_body != body_text;
        let sql_def_changed = new_sql_definition.as_deref() != existing.sql_definition.as_deref();

        if !on_target_match && !body_changed && !sql_def_changed {
            continue;
        }

        let mut updated = existing.clone();
        if on_target_match {
            updated.table_name = new_name.to_string();
        }
        if body_changed {
            updated.triggered_action = TriggerAction::RawSql(new_body);
        }
        updated.sql_definition = new_sql_definition;

        // The trigger is known to exist (we just read it), so update_trigger
        // cannot fail; ignore the result defensively.
        let _ = database.catalog.update_trigger(updated);
    }
}
