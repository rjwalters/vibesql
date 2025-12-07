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
                }
                return result;
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
        }

        result
    }
}
