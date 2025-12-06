//! TRUNCATE TABLE statement execution
//!
//! This module coordinates truncate operations across multiple sub-modules:
//! - `core`: Core truncate logic and table clearing
//! - `triggers`: Trigger validation and coordination
//! - `constraints`: Constraint and foreign key validation

pub mod constraints;
pub mod core;
pub mod triggers;

use vibesql_ast::TruncateTableStmt;
use vibesql_storage::Database;

use crate::errors::ExecutorError;
use crate::privilege_checker::PrivilegeChecker;

use self::constraints::validate_truncate_allowed;
use self::core::{execute_truncate, execute_truncate_cascade};

/// Executor for TRUNCATE TABLE statements
pub struct TruncateTableExecutor;

impl TruncateTableExecutor {
    /// Execute a TRUNCATE TABLE statement
    ///
    /// # Arguments
    ///
    /// * `stmt` - The TRUNCATE TABLE statement AST node
    /// * `database` - The database to truncate the table(s) in
    ///
    /// # Returns
    ///
    /// Total number of rows deleted from all tables or error
    ///
    /// # Behavior
    ///
    /// Supports truncating multiple tables in a single statement with all-or-nothing semantics:
    /// - Validates all tables first (existence, privileges, constraints)
    /// - Only truncates if all validations pass
    /// - IF EXISTS: skips non-existent tables, continues with existing ones
    ///
    /// # Examples
    ///
    /// ```
    /// use vibesql_ast::{ColumnDef, CreateTableStmt, TruncateTableStmt};
    /// use vibesql_executor::{CreateTableExecutor, TruncateTableExecutor};
    /// use vibesql_storage::{Database, Row};
    /// use vibesql_types::{DataType, SqlValue};
    ///
    /// let mut db = Database::new();
    /// let create_stmt = CreateTableStmt {
    ///     if_not_exists: false,
    ///     table_name: "users".to_string(),
    ///     columns: vec![ColumnDef {
    ///         name: "id".to_string(),
    ///         data_type: DataType::Integer,
    ///         nullable: false,
    ///         constraints: vec![],
    ///         default_value: None,
    ///         comment: None,
    ///     }],
    ///     table_constraints: vec![],
    ///     table_options: vec![],
    /// };
    /// CreateTableExecutor::execute(&create_stmt, &mut db).unwrap();
    ///
    /// // Insert some rows
    /// db.insert_row("users", Row::new(vec![SqlValue::Integer(1)])).unwrap();
    /// db.insert_row("users", Row::new(vec![SqlValue::Integer(2)])).unwrap();
    ///
    /// let stmt = TruncateTableStmt { table_names: vec!["users".to_string()], if_exists: false, cascade: None };
    ///
    /// let result = TruncateTableExecutor::execute(&stmt, &mut db);
    /// assert_eq!(result.unwrap(), 2); // 2 rows deleted
    /// assert_eq!(db.get_table("users").unwrap().row_count(), 0);
    /// ```
    pub fn execute(
        stmt: &TruncateTableStmt,
        database: &mut Database,
    ) -> Result<usize, ExecutorError> {
        // Phase 1: Validation - Check all tables before truncating any
        // Collect tables that exist and need to be truncated
        let mut tables_to_truncate = Vec::new();

        for table_name in &stmt.table_names {
            // Check if table exists
            if !database.catalog.table_exists(table_name) {
                if stmt.if_exists {
                    // IF EXISTS specified and table doesn't exist - skip this table
                    continue;
                } else {
                    return Err(ExecutorError::TableNotFound(table_name.clone()));
                }
            }

            tables_to_truncate.push(table_name.as_str());
        }

        // If no tables to truncate (all were non-existent with IF EXISTS), return 0
        if tables_to_truncate.is_empty() {
            return Ok(0);
        }

        // Check DELETE privilege on all tables
        for table_name in &tables_to_truncate {
            PrivilegeChecker::check_delete(database, table_name)?;
        }

        // Determine CASCADE behavior - check explicit CASCADE, default to RESTRICT
        let cascade_mode = &stmt.cascade;

        match cascade_mode {
            Some(vibesql_ast::TruncateCascadeOption::Cascade) => {
                // CASCADE mode: recursively truncate dependent tables for each requested table
                let mut total_rows = 0;
                for table_name in &tables_to_truncate {
                    total_rows += execute_truncate_cascade(database, table_name)?;
                }
                Ok(total_rows)
            }
            _ => {
                // RESTRICT mode (default): fail if referenced by foreign keys
                // Check if TRUNCATE is allowed on all tables (no DELETE triggers, no FK references)
                for table_name in &tables_to_truncate {
                    validate_truncate_allowed(database, table_name)?;
                }

                // Phase 2: Execution - All validations passed, now truncate all tables
                let mut total_rows = 0;
                for table_name in &tables_to_truncate {
                    total_rows += execute_truncate(database, table_name)?;
                }

                Ok(total_rows)
            }
        }
    }
}
