//! UPDATE statement execution
//!
//! This module provides UPDATE statement execution with the following architecture:
//!
//! - `row_selector`: Handles WHERE clause evaluation and primary key index optimization
//! - `value_updater`: Applies assignment expressions to rows
//! - `constraints`: Validates NOT NULL, PRIMARY KEY, UNIQUE, and CHECK constraints
//! - `foreign_keys`: Validates foreign key constraints and child references
//! - `fast_path`: Fast path optimizations for single-row PK updates
//! - `triggers`: Trigger execution and view update handling
//! - `index_sync`: Index maintenance coordination for REPLACE operations
//! - `executor`: Core execution orchestration
//!
//! The main `UpdateExecutor` orchestrates these components to implement SQL's two-phase
//! update semantics: first collect all updates evaluating against original rows, then
//! apply all updates atomically.
//!
//! ## Performance Optimizations
//!
//! The executor includes a fast path for single-row primary key updates that:
//! - Skips trigger checks when no triggers exist for the table
//! - Avoids schema cloning
//! - Uses single-pass execution instead of two-phase
//! - Minimizes allocations

mod constraints;
mod executor;
mod fast_path;
mod foreign_keys;
mod from_clause;
mod index_sync;
mod row_selector;
mod triggers;
mod value_updater;

use vibesql_ast::UpdateStmt;
use vibesql_storage::Database;

use crate::errors::ExecutorError;

// Re-export for external use
pub use triggers::execute_update_with_trigger_context;

/// Executor for UPDATE statements
pub struct UpdateExecutor;

impl UpdateExecutor {
    /// Execute an UPDATE statement
    ///
    /// # Arguments
    ///
    /// * `stmt` - The UPDATE statement AST node
    /// * `database` - The database to update
    ///
    /// # Returns
    ///
    /// Number of rows updated or error
    ///
    /// # Examples
    ///
    /// ```
    /// use vibesql_ast::{Assignment, Expression, UpdateStmt};
    /// use vibesql_catalog::{ColumnSchema, TableSchema};
    /// use vibesql_executor::UpdateExecutor;
    /// use vibesql_storage::Database;
    /// use vibesql_types::{DataType, SqlValue};
    ///
    /// let mut db = Database::new();
    ///
    /// // Create table
    /// let schema = TableSchema::new(
    ///     "employees".to_string(),
    ///     vec![
    ///         ColumnSchema::new("id".to_string(), DataType::Integer, false),
    ///         ColumnSchema::new("salary".to_string(), DataType::Integer, false),
    ///     ],
    /// );
    /// db.create_table(schema).unwrap();
    ///
    /// // Insert a row
    /// db.insert_row(
    ///     "employees",
    ///     vibesql_storage::Row::new(vec![SqlValue::Integer(1), SqlValue::Integer(50000)]),
    /// )
    /// .unwrap();
    ///
    /// // Update salary
    /// let stmt = UpdateStmt {
    ///     with_clause: None,
    ///     table_name: "employees".to_string(),
    ///     quoted: false,
    ///     alias: None,
    ///     assignments: vec![Assignment {
    ///         column: "salary".to_string(),
    ///         value: Expression::Literal(SqlValue::Integer(60000)),
    ///     }],
    ///     from_clause: None,
    ///     where_clause: None,
    ///     conflict_clause: None,
    /// };
    ///
    /// let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
    /// assert_eq!(count, 1);
    /// ```
    pub fn execute(stmt: &UpdateStmt, database: &mut Database) -> Result<usize, ExecutorError> {
        executor::execute_internal(stmt, database, None, None, None)
    }

    /// Execute an UPDATE statement with procedural context
    /// Supports procedural variables in SET and WHERE clauses
    pub fn execute_with_procedural_context(
        stmt: &UpdateStmt,
        database: &mut Database,
        procedural_context: &crate::procedural::ExecutionContext,
    ) -> Result<usize, ExecutorError> {
        executor::execute_internal(stmt, database, None, Some(procedural_context), None)
    }

    /// Execute an UPDATE statement with trigger context
    /// This allows UPDATE statements within trigger bodies to reference OLD/NEW pseudo-variables
    pub fn execute_with_trigger_context(
        stmt: &UpdateStmt,
        database: &mut Database,
        trigger_context: &crate::trigger_execution::TriggerContext,
    ) -> Result<usize, ExecutorError> {
        executor::execute_internal(stmt, database, None, None, Some(trigger_context))
    }

    /// Execute an UPDATE statement with optional pre-fetched schema
    ///
    /// This method allows cursor-level schema caching to reduce redundant catalog lookups.
    /// If schema is provided, skips the catalog lookup step.
    ///
    /// # Arguments
    ///
    /// * `stmt` - The UPDATE statement AST node
    /// * `database` - The database to update
    /// * `schema` - Optional pre-fetched schema (from cursor cache)
    ///
    /// # Returns
    ///
    /// Number of rows updated or error
    pub fn execute_with_schema(
        stmt: &UpdateStmt,
        database: &mut Database,
        schema: Option<&vibesql_catalog::TableSchema>,
    ) -> Result<usize, ExecutorError> {
        executor::execute_internal(stmt, database, schema, None, None)
    }
}
