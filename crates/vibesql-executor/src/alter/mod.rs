//! ALTER TABLE executor

mod columns;
mod constraints;
mod table_options;
mod validation;

use vibesql_ast::*;
use vibesql_storage::Database;

use crate::{errors::ExecutorError, privilege_checker::PrivilegeChecker};

/// Executor for ALTER TABLE statements
pub struct AlterTableExecutor;

impl AlterTableExecutor {
    /// Execute an ALTER TABLE statement
    pub fn execute(
        stmt: &AlterTableStmt,
        database: &mut Database,
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
                let result = table_options::execute_rename_table(rename_table, database);
                if result.is_ok() {
                    // Invalidate the database-level columnar cache for both table names
                    // since the cache key is based on table name
                    database.invalidate_columnar_cache(old_name);
                    database.invalidate_columnar_cache(new_name);
                    // Discard the verbatim CREATE TABLE text carried over from the
                    // pre-rename schema: it still names the old table, so emitting
                    // it in sqlite_master.sql / the SQL dump would be wrong and
                    // could break reload (issue #5619).
                    invalidate_sql_source(database, new_name);
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
            // Any structural ALTER (add/drop/rename column, change type, add/drop
            // constraint, ...) makes the captured verbatim CREATE TABLE text
            // stale. Discard it so sqlite_master.sql and the SQL dump fall back
            // to a reconstruction that matches the live schema (issue #5619).
            invalidate_sql_source(database, table_name);
        }

        result
    }
}

/// Discard any preserved verbatim CREATE TABLE source text for `table_name`
/// after a successful ALTER, so that sqlite_master.sql and SQL-dump persistence
/// reflect the mutated schema instead of the original text. See issue #5619.
///
/// The schema is stored twice — once in the storage `Table` (read by the
/// SQL-dump path) and once in the catalog (read by sqlite_master) — so both
/// copies must be cleared.
fn invalidate_sql_source(database: &mut Database, table_name: &str) {
    if let Some(table) = database.get_table_mut(table_name) {
        table.schema_mut().invalidate_sql_source();
    }
    database.catalog.invalidate_table_sql_source(table_name);
}
