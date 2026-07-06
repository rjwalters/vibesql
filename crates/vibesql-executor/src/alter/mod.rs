//! ALTER TABLE executor

mod columns;
mod constraints;
mod drop_column_checks;
mod table_options;
mod validation;

use vibesql_ast::*;
use vibesql_storage::Database;

use crate::{errors::ExecutorError, privilege_checker::PrivilegeChecker};

/// Executor for ALTER TABLE statements
pub struct AlterTableExecutor;

impl AlterTableExecutor {
    /// Execute an ALTER TABLE statement.
    ///
    /// Convenience wrapper over [`execute_with_source`](Self::execute_with_source)
    /// for callers that do not have the original statement text (e.g. an AST
    /// built programmatically). Without the source text, ADD COLUMN falls back
    /// to reconstructing `sqlite_master.sql` from the schema rather than editing
    /// the verbatim text in place. See issue #5625.
    pub fn execute(
        stmt: &AlterTableStmt,
        database: &mut Database,
    ) -> Result<String, ExecutorError> {
        Self::execute_with_source(stmt, database, None)
    }

    /// Execute an ALTER TABLE statement, optionally given the verbatim original
    /// statement text (`alter_sql`).
    ///
    /// After a successful structural ALTER, two pieces of bookkeeping run
    /// (issue #5625):
    ///
    /// 1. **Catalog schema sync.** Column operations (ADD/DROP/RENAME/MODIFY)
    ///    only mutate the *storage* `Table` copy of the schema. The *catalog*
    ///    copy — read by `sqlite_master`, `PRAGMA table_info`, and DML column
    ///    resolution — is then re-synced from the storage copy so both stay
    ///    consistent. (RENAME TABLE syncs via drop+create and is exempt.)
    ///
    /// 2. **`sql_source` upkeep.** SQLite edits the verbatim `CREATE TABLE` text
    ///    in `sqlite_master.sql` in place on ALTER rather than reconstructing it.
    ///    For ADD COLUMN, RENAME COLUMN, DROP COLUMN, and RENAME TO the stored
    ///    verbatim text is edited to match SQLite (when `alter_sql` is available
    ///    / the structure permits; RENAME TO is handled in
    ///    `table_options::execute_rename_table`). For the remaining variants, and
    ///    whenever an in-place edit cannot apply cleanly, the stale text is
    ///    invalidated so it is reconstructed from the (now-synced) schema. Either
    ///    way the stored text remains re-parseable on reload (issue #5619).
    pub fn execute_with_source(
        stmt: &AlterTableStmt,
        database: &mut Database,
        alter_sql: Option<&str>,
    ) -> Result<String, ExecutorError> {
        // Get table name from the statement and check ALTER privilege
        let table_name = match stmt {
            AlterTableStmt::AddColumn(s) => &s.table_name,
            AlterTableStmt::DropColumn(s) => &s.table_name,
            AlterTableStmt::AlterColumn(s) => match s {
                AlterColumnStmt::SetDefault { table_name, .. } => table_name,
                AlterColumnStmt::DropDefault { table_name, .. } => table_name,
                AlterColumnStmt::SetNotNull { table_name, .. } => table_name,
                AlterColumnStmt::DropNotNull { table_name, .. } => table_name,
            },
            AlterTableStmt::AddConstraint(s) => &s.table_name,
            AlterTableStmt::DropConstraint(s) => &s.table_name,
            AlterTableStmt::RenameTable(s) => &s.table_name,
            AlterTableStmt::RenameColumn(s) => &s.table_name,
            AlterTableStmt::ModifyColumn(s) => &s.table_name,
            AlterTableStmt::ChangeColumn(s) => &s.table_name,
        };
        PrivilegeChecker::check_alter(database, table_name)?;

        let result = match stmt {
            AlterTableStmt::AddColumn(add_column) => {
                columns::execute_add_column(add_column, database)
            }
            AlterTableStmt::DropColumn(drop_column) => {
                columns::execute_drop_column(drop_column, database)
            }
            AlterTableStmt::AlterColumn(alter_column) => {
                columns::execute_alter_column(alter_column, database)
            }
            AlterTableStmt::AddConstraint(add_constraint) => {
                constraints::execute_add_constraint(add_constraint, database)
            }
            AlterTableStmt::DropConstraint(drop_constraint) => {
                constraints::execute_drop_constraint(drop_constraint, database)
            }
            AlterTableStmt::RenameTable(rename_table) => {
                // For RENAME TABLE, invalidate both old and new table names
                let old_name = &rename_table.table_name;
                let new_name = &rename_table.new_table_name;
                // `execute_rename_table` edits the verbatim CREATE TABLE text in
                // place (rewriting the table name to the double-quoted new name,
                // matching sqlite3) and keeps the catalog + storage copies in
                // sync via its drop+create, so no further bookkeeping is needed
                // here (issue #5634).
                let result = table_options::execute_rename_table(rename_table, database);
                if result.is_ok() {
                    // Invalidate the database-level columnar cache for both table names
                    // since the cache key is based on table name
                    database.invalidate_columnar_cache(old_name);
                    database.invalidate_columnar_cache(new_name);
                }
                return result;
            }
            AlterTableStmt::RenameColumn(rename_column) => {
                columns::execute_rename_column(rename_column, database)
            }
            AlterTableStmt::ModifyColumn(modify_column) => {
                columns::execute_modify_column(modify_column, database)
            }
            AlterTableStmt::ChangeColumn(change_column) => {
                columns::execute_change_column(change_column, database)
            }
        };

        // Invalidate the database-level columnar cache since table structure changed.
        // This ensures subsequent reads via `database.get_columnar()` return
        // fresh data with the updated schema rather than stale cached data.
        if result.is_ok() {
            database.invalidate_columnar_cache(table_name);

            // Update the stored verbatim CREATE TABLE text: edit it in place for
            // the variants SQLite edits in place (ADD COLUMN / RENAME COLUMN),
            // otherwise invalidate so it is reconstructed (issue #5625).
            update_sql_source_after_alter(database, table_name, stmt, alter_sql);

            // Re-sync the catalog schema copy from the (mutated) storage copy.
            // Column ALTERs only touched the storage `Table`; the catalog copy —
            // read by sqlite_master / PRAGMA table_info / DML resolution — would
            // otherwise stay stale (issue #5625).
            sync_catalog_schema_from_storage(database, table_name);
        }

        result
    }
}

/// Copy the (just-mutated) storage `Table` schema for `table_name` into the
/// catalog so both copies of the schema stay consistent after a column ALTER.
///
/// VibeSQL keeps a table's schema in two places: the storage `Table` (mutated
/// by the ALTER column executors) and the catalog `TableSchema` (read by
/// `sqlite_master`, `PRAGMA table_info`, and column resolution for DML). They
/// start identical at CREATE time; this restores that invariant after an ALTER.
/// No-op if the storage table is missing. See issue #5625.
fn sync_catalog_schema_from_storage(database: &mut Database, table_name: &str) {
    let schema = match database.get_table(table_name) {
        Some(table) => table.schema.clone(),
        None => return,
    };
    database.catalog.replace_table_schema(table_name, schema);
}

/// Edit (or invalidate) the verbatim `CREATE TABLE` text on the storage `Table`
/// schema for `table_name` after a successful column ALTER.
///
/// For ADD COLUMN, RENAME COLUMN, and DROP COLUMN — the cases SQLite edits in
/// place — the stored text is edited to match SQLite (when the verbatim
/// `alter_sql` is available / the structure permits). (RENAME TO is edited in
/// place separately, in `table_options::execute_rename_table`.) For every other
/// variant, and whenever the in-place edit cannot be applied cleanly, the stale
/// text is invalidated so `sqlite_master.sql` falls back to reconstruction from
/// the synced schema. The result is always re-parseable on reload (issue #5619).
fn update_sql_source_after_alter(
    database: &mut Database,
    table_name: &str,
    stmt: &AlterTableStmt,
    alter_sql: Option<&str>,
) {
    let Some(table) = database.get_table_mut(table_name) else {
        return;
    };
    let Some(current) = table.schema.sql_source.clone() else {
        // No verbatim text to edit; nothing to do (reconstruction is used).
        return;
    };

    let edited: Option<String> = match stmt {
        AlterTableStmt::AddColumn(_) => alter_sql
            .and_then(crate::alter_rewrite::extract_add_column_text)
            .and_then(|coldef| crate::alter_rewrite::append_column(&current, &coldef)),
        AlterTableStmt::RenameColumn(rename) => crate::alter_rewrite::rename_column(
            &current,
            table_name,
            &rename.old_column_name,
            &rename.new_column_name,
        ),
        AlterTableStmt::DropColumn(drop) => {
            crate::alter_rewrite::drop_column(&current, &drop.column_name)
        }
        // ALTER/MODIFY/CHANGE COLUMN, ADD/DROP CONSTRAINT: SQLite's in-place
        // editing for these is not yet matched; reconstruct instead.
        _ => None,
    };

    match edited {
        Some(text) => table.schema_mut().set_sql_source(text),
        None => table.schema_mut().invalidate_sql_source(),
    }
}
