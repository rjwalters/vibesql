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

use std::collections::HashSet;

use vibesql_ast::UpdateStmt;
use vibesql_storage::{Database, Row};

use crate::errors::ExecutorError;

/// One pending UPDATE row, collected in the first phase of two-phase UPDATE
/// execution and applied in the second phase.
///
/// Both the default UPDATE path (`executor::execute_internal`) and the
/// `UPDATE ... FROM ...` path (`executor::execute_update_from`) collect a
/// `Vec<PendingUpdate>` before validating constraints and writing rows back.
///
/// This was previously an anonymous 5-tuple
/// `(usize, Row, Row, HashSet<usize>, bool)`. The judge on PR #5138 flagged
/// the tuple as load-bearing once a second consumer existed (UPDATE FROM, in
/// PR for issue #5140). Naming the fields makes call sites self-documenting
/// and prevents accidental positional confusion when the shape grows.
#[derive(Debug, Clone)]
pub(super) struct PendingUpdate {
    /// Index of the row in the target table's storage (the rowid in storage
    /// order, used by `update_row_selective` and index maintenance).
    pub row_index: usize,
    /// Original row state — needed for PK/UNIQUE self-check and trigger OLD.
    pub old_row: Row,
    /// New row state after assignments (and generated-column recomputation).
    pub new_row: Row,
    /// Column indices whose value changed; passed to `update_row_selective`
    /// and index maintenance so unchanged columns are skipped.
    pub changed_columns: HashSet<usize>,
    /// True if any assignment touches a PRIMARY KEY column (or the rowid
    /// alias for INTEGER PRIMARY KEY tables). Used to gate child-reference
    /// FK checks.
    pub updates_pk: bool,
}

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
    ///     returning: None,
    /// };
    ///
    /// let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
    /// assert_eq!(count, 1);
    /// ```
    pub fn execute(stmt: &UpdateStmt, database: &mut Database) -> Result<usize, ExecutorError> {
        // SQLite per-variant RAISE scope handling for the top-level statement
        // (#5417): see [`crate::raise_scope::run_top_level_dml`].
        let may_fire = crate::raise_scope::table_may_fire_trigger(database, &stmt.table_name);
        crate::raise_scope::run_top_level_dml(database, may_fire, |database| {
            executor::execute_internal(stmt, database, None, None, None).map(|(count, _)| count)
        })
    }

    /// Execute an UPDATE statement, capturing RETURNING rows (SQLite 3.35.0+)
    ///
    /// Returns the number of updated rows plus, when the statement carries a
    /// RETURNING clause, the projected NEW rows (after SET assignments) — one
    /// per updated row, or one per INSTEAD OF trigger fire for views.
    ///
    /// When the statement has no RETURNING clause the second element is `None`.
    pub fn execute_returning(
        stmt: &UpdateStmt,
        database: &mut Database,
    ) -> Result<(usize, Option<crate::select::SelectResult>), ExecutorError> {
        // Per-variant RAISE scope for RETURNING DML (#5432, follow-on to #5417):
        // a RAISE fired from a trigger during a RETURNING UPDATE gets the same
        // statement-savepoint scope as the bare `execute` path; when it aborts
        // the statement the error propagates (no rows returned).
        let may_fire = crate::raise_scope::table_may_fire_trigger(database, &stmt.table_name);
        crate::raise_scope::run_top_level_dml(database, may_fire, |database| {
            executor::execute_internal(stmt, database, None, None, None)
        })
    }

    /// Execute an UPDATE statement with procedural context
    /// Supports procedural variables in SET and WHERE clauses
    pub fn execute_with_procedural_context(
        stmt: &UpdateStmt,
        database: &mut Database,
        procedural_context: &crate::procedural::ExecutionContext,
    ) -> Result<usize, ExecutorError> {
        // Per-variant RAISE scope for procedural-context DML (#5432): a RAISE
        // fired from a trigger inside a procedure/script gets the same
        // statement-savepoint scope as the bare `execute` path.
        let may_fire = crate::raise_scope::table_may_fire_trigger(database, &stmt.table_name);
        crate::raise_scope::run_top_level_dml(database, may_fire, |database| {
            executor::execute_internal(stmt, database, None, Some(procedural_context), None)
                .map(|(count, _)| count)
        })
    }

    /// Execute an UPDATE statement with trigger context
    /// This allows UPDATE statements within trigger bodies to reference OLD/NEW pseudo-variables
    pub fn execute_with_trigger_context(
        stmt: &UpdateStmt,
        database: &mut Database,
        trigger_context: &crate::trigger_execution::TriggerContext,
    ) -> Result<usize, ExecutorError> {
        executor::execute_internal(stmt, database, None, None, Some(trigger_context))
            .map(|(count, _)| count)
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
        executor::execute_internal(stmt, database, schema, None, None).map(|(count, _)| count)
    }
}
