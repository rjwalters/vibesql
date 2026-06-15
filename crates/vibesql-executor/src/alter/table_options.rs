//! Table-level operation executors for ALTER TABLE

use vibesql_ast::{RenameTableStmt, TriggerAction};
use vibesql_storage::Database;

use crate::{
    errors::ExecutorError,
    trigger_rename::{rewrite_column_refs_in_trigger_sql, rewrite_table_refs_in_trigger_sql},
};

/// Execute RENAME TABLE
pub(super) fn execute_rename_table(
    stmt: &RenameTableStmt,
    database: &mut Database,
) -> Result<String, ExecutorError> {
    // Reject renaming a table to a reserved `sqlite_`-prefixed name. SQLite
    // reserves that prefix for its own schema objects and errors
    // `object name reserved for internal use: <name>` (sqlite3 3.51.0,
    // alter-2.5), echoing the target name verbatim. Without this guard a user
    // could smuggle a `sqlite_`-prefixed *user table* into the catalog via
    // RENAME; that table then dumps as `CREATE TABLE sqlite_x (...)`, which the
    // load-path reserved-name guard would reject — bricking the database on
    // reload (issue #5614). Guarding RENAME closes that gap at the source.
    if crate::sqlite_schema::is_reserved_object_name(&stmt.new_table_name) {
        return Err(ExecutorError::SqliteCompatError(format!(
            "object name reserved for internal use: {}",
            stmt.new_table_name
        )));
    }

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

/// Rewrite trigger bodies that reference `<table>.<old_column>` after an
/// `ALTER TABLE <table> RENAME <old_column> TO <new_column>`.
///
/// Mirrors SQLite's `legacy_alter_table=OFF` behavior: references inside trigger
/// bodies that resolve to the renamed column are rewritten (unquoted), while the
/// rest of the `CREATE TRIGGER` text is preserved verbatim. The renamed table's
/// own `ON`-target name does not change. See `crate::trigger_rename`.
///
/// If an unqualified reference to the renamed column is *ambiguous* in a trigger
/// body (the renamed table and another in-scope table both own a column of that
/// name), SQLite aborts the whole ALTER and leaves the schema unchanged. This
/// returns `Err(ExecutorError::AmbiguousColumnName { .. })` (wrapped to mirror
/// SQLite's `error in trigger <name>: ambiguous column name: <col>` message) so
/// the caller can abort before committing the rename.
pub(super) fn rewrite_triggers_for_column_rename(
    database: &mut Database,
    table: &str,
    old_column: &str,
    new_column: &str,
) -> Result<(), ExecutorError> {
    // Snapshot table -> column-name set so the rewrite resolver can attribute
    // unqualified columns to their owning table without borrowing the database
    // while we mutate the catalog. The snapshot reflects the *post-rename* schema
    // (the column has already been renamed), so the renamed table's pre-rename
    // column name is handled explicitly in the closure below.
    let schema: std::collections::HashMap<String, Vec<String>> = database
        .list_tables()
        .into_iter()
        .filter_map(|name| {
            database.get_table(&name).map(|tbl| {
                (
                    name.to_ascii_lowercase(),
                    tbl.schema.columns.iter().map(|c| c.name.clone()).collect(),
                )
            })
        })
        .collect();

    let renamed_table_lc = table.to_ascii_lowercase();
    let old_column_owned = old_column.to_string();
    let table_has_column = move |t: &str, c: &str| -> bool {
        let t_lc = t.to_ascii_lowercase();
        // The renamed table still owns `old_column` for resolution purposes even
        // though the live schema now stores it under `new_column`.
        if t_lc == renamed_table_lc && c.eq_ignore_ascii_case(&old_column_owned) {
            return true;
        }
        schema.get(&t_lc).is_some_and(|cols| cols.iter().any(|col| col.eq_ignore_ascii_case(c)))
    };

    // Two-phase: compute every trigger update first so that an ambiguity error
    // in any trigger aborts the whole ALTER *before* any catalog mutation. This
    // mirrors SQLite, which leaves the schema entirely unchanged on a genuine
    // ambiguity rather than partially rewriting earlier triggers.
    let trigger_names = database.catalog.list_triggers();
    let mut pending_updates = Vec::new();
    for name in trigger_names {
        let Some(existing) = database.catalog.get_trigger(&name) else {
            continue;
        };

        let body_text = match &existing.triggered_action {
            TriggerAction::RawSql(sql) => sql.clone(),
        };
        // An ambiguous unqualified reference to the renamed column aborts the
        // ALTER (SQLite: "error in trigger <name>: ambiguous column name: <col>").
        // Map the resolver error to that message, using `Other` so the verbatim
        // SQLite wording is preserved.
        let ambiguity_error = |col: String| {
            ExecutorError::Other(format!(
                "error in trigger {}: ambiguous column name: {}",
                name, col
            ))
        };
        let new_body = rewrite_column_refs_in_trigger_sql(
            &body_text,
            table,
            old_column,
            new_column,
            &table_has_column,
        )
        .map_err(ambiguity_error)?;
        let new_sql_definition = existing
            .sql_definition
            .as_ref()
            .map(|sql| {
                rewrite_column_refs_in_trigger_sql(
                    sql,
                    table,
                    old_column,
                    new_column,
                    &table_has_column,
                )
            })
            .transpose()
            .map_err(ambiguity_error)?;

        let body_changed = new_body != body_text;
        let sql_def_changed = new_sql_definition.as_deref() != existing.sql_definition.as_deref();

        if !body_changed && !sql_def_changed {
            continue;
        }

        let mut updated = existing.clone();
        if body_changed {
            updated.triggered_action = TriggerAction::RawSql(new_body);
        }
        if sql_def_changed {
            updated.sql_definition = new_sql_definition;
        }
        pending_updates.push(updated);
    }

    for updated in pending_updates {
        let _ = database.catalog.update_trigger(updated);
    }
    Ok(())
}
