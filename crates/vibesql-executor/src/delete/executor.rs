//! DELETE statement execution

use vibesql_ast::DeleteStmt;
use vibesql_storage::Database;

use super::integrity::check_no_child_references;
use crate::{
    errors::ExecutorError, evaluator::ExpressionEvaluator, privilege_checker::PrivilegeChecker,
    truncate_validation::can_use_truncate,
};

/// Executor for DELETE statements
pub struct DeleteExecutor;

impl DeleteExecutor {
    /// Execute a DELETE statement
    ///
    /// # Arguments
    ///
    /// * `stmt` - The DELETE statement AST node
    /// * `database` - The database to delete from
    ///
    /// # Returns
    ///
    /// Number of rows deleted or error
    ///
    /// # Examples
    ///
    /// ```
    /// use vibesql_ast::{BinaryOperator, DeleteStmt, Expression, WhereClause};
    /// use vibesql_catalog::{ColumnSchema, TableSchema};
    /// use vibesql_executor::DeleteExecutor;
    /// use vibesql_storage::Database;
    /// use vibesql_types::{DataType, SqlValue};
    ///
    /// let mut db = Database::new();
    ///
    /// // Create table
    /// let schema = TableSchema::new(
    ///     "users".to_string(),
    ///     vec![
    ///         ColumnSchema::new("id".to_string(), DataType::Integer, false),
    ///         ColumnSchema::new(
    ///             "name".to_string(),
    ///             DataType::Varchar { max_length: Some(50) },
    ///             false,
    ///         ),
    ///     ],
    /// );
    /// db.create_table(schema).unwrap();
    ///
    /// // Insert rows
    /// db.insert_row(
    ///     "users",
    ///     vibesql_storage::Row::new(vec![SqlValue::Integer(1), SqlValue::Varchar("Alice".to_string())]),
    /// )
    /// .unwrap();
    /// db.insert_row(
    ///     "users",
    ///     vibesql_storage::Row::new(vec![SqlValue::Integer(2), SqlValue::Varchar("Bob".to_string())]),
    /// )
    /// .unwrap();
    ///
    /// // Delete specific row
    /// let stmt = DeleteStmt {
    ///     only: false,
    ///     table_name: "users".to_string(),
    ///     where_clause: Some(WhereClause::Condition(Expression::BinaryOp {
    ///         left: Box::new(Expression::ColumnRef { table: None, column: "id".to_string() }),
    ///         op: BinaryOperator::Equal,
    ///         right: Box::new(Expression::Literal(SqlValue::Integer(1))),
    ///     })),
    /// };
    ///
    /// let count = DeleteExecutor::execute(&stmt, &mut db).unwrap();
    /// assert_eq!(count, 1);
    /// ```
    pub fn execute(stmt: &DeleteStmt, database: &mut Database) -> Result<usize, ExecutorError> {
        Self::execute_internal(stmt, database, None, None)
    }

    /// Execute a DELETE statement with procedural context
    /// Supports procedural variables in WHERE clause
    pub fn execute_with_procedural_context(
        stmt: &DeleteStmt,
        database: &mut Database,
        procedural_context: &crate::procedural::ExecutionContext,
    ) -> Result<usize, ExecutorError> {
        Self::execute_internal(stmt, database, Some(procedural_context), None)
    }

    /// Execute a DELETE statement with trigger context
    /// This allows DELETE statements within trigger bodies to reference OLD/NEW pseudo-variables
    pub fn execute_with_trigger_context(
        stmt: &DeleteStmt,
        database: &mut Database,
        trigger_context: &crate::trigger_execution::TriggerContext,
    ) -> Result<usize, ExecutorError> {
        Self::execute_internal(stmt, database, None, Some(trigger_context))
    }

    /// Internal implementation supporting procedural context and trigger context
    fn execute_internal(
        stmt: &DeleteStmt,
        database: &mut Database,
        procedural_context: Option<&crate::procedural::ExecutionContext>,
        trigger_context: Option<&crate::trigger_execution::TriggerContext>,
    ) -> Result<usize, ExecutorError> {
        // Note: stmt.only is currently ignored (treated as false)
        // ONLY keyword is used in table inheritance to exclude derived tables.
        // Since table inheritance is not yet implemented, we treat all deletes the same.

        // Check DELETE privilege on the table
        PrivilegeChecker::check_delete(database, &stmt.table_name)?;

        // Check table exists
        if !database.catalog.table_exists(&stmt.table_name) {
            return Err(ExecutorError::TableNotFound(stmt.table_name.clone()));
        }

        // Fast path: DELETE FROM table (no WHERE clause)
        // Use TRUNCATE-style optimization for 100-1000x performance improvement
        if stmt.where_clause.is_none() && can_use_truncate(database, &stmt.table_name)? {
            return execute_truncate(database, &stmt.table_name);
        }

        // Step 1: Get schema (clone to avoid borrow issues)
        let schema = database
            .catalog
            .get_table(&stmt.table_name)
            .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?
            .clone();

        // Step 2: Evaluate WHERE clause and collect rows to delete (two-phase execution)
        // Get table for scanning
        let table = database
            .get_table(&stmt.table_name)
            .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?;

        // Create evaluator with database reference for subquery support (EXISTS, NOT EXISTS, IN
        // with subquery, etc.) and optional procedural/trigger context for variable resolution
        let evaluator = if let Some(ctx) = trigger_context {
            // Trigger context takes precedence (trigger statements can't have procedural context)
            ExpressionEvaluator::with_trigger_context(&schema, database, ctx)
        } else if let Some(ctx) = procedural_context {
            ExpressionEvaluator::with_procedural_context(&schema, database, ctx)
        } else {
            ExpressionEvaluator::with_database(&schema, database)
        };

        // Find rows to delete and their indices
        // Try to use primary key index for fast lookup
        let mut rows_and_indices_to_delete: Vec<(usize, vibesql_storage::Row)> = Vec::new();

        if let Some(vibesql_ast::WhereClause::Condition(where_expr)) = &stmt.where_clause {
            // Try primary key optimization
            if let Some(pk_values) = Self::extract_primary_key_lookup(where_expr, &schema) {
                if let Some(pk_index) = table.primary_key_index() {
                    if let Some(&row_index) = pk_index.get(&pk_values) {
                        // Found the row via index - single row to delete
                        rows_and_indices_to_delete
                            .push((row_index, table.scan()[row_index].clone()));
                    }
                    // If not found, rows_and_indices_to_delete stays empty (no rows to delete)
                } else {
                    // No PK index, fall through to table scan below
                    Self::collect_rows_with_scan(
                        table,
                        &stmt.where_clause,
                        &evaluator,
                        &mut rows_and_indices_to_delete,
                    )?;
                }
            } else {
                // Can't extract PK lookup, fall through to table scan
                Self::collect_rows_with_scan(
                    table,
                    &stmt.where_clause,
                    &evaluator,
                    &mut rows_and_indices_to_delete,
                )?;
            }
        } else {
            // No WHERE clause - collect all rows
            Self::collect_rows_with_scan(
                table,
                &stmt.where_clause,
                &evaluator,
                &mut rows_and_indices_to_delete,
            )?;
        }

        // Fire BEFORE STATEMENT triggers (unless we're already inside a trigger context)
        if trigger_context.is_none() {
            crate::TriggerFirer::execute_before_statement_triggers(
                database,
                &stmt.table_name,
                vibesql_ast::TriggerEvent::Delete,
            )?;
        }

        // Step 3: Fire BEFORE DELETE ROW triggers
        for (_, row) in &rows_and_indices_to_delete {
            crate::TriggerFirer::execute_before_triggers(
                database,
                &stmt.table_name,
                vibesql_ast::TriggerEvent::Delete,
                Some(row),
                None,
            )?;
        }

        // Step 4: Handle referential integrity for each row to be deleted
        // This may CASCADE deletes, SET NULL, or SET DEFAULT in child tables
        for (_, row) in &rows_and_indices_to_delete {
            check_no_child_references(database, &stmt.table_name, row)?;
        }

        // Extract indices for deletion
        let mut deleted_indices: Vec<usize> =
            rows_and_indices_to_delete.iter().map(|(idx, _)| *idx).collect();
        deleted_indices.sort_unstable();

        // Step 5a: Remove entries from user-defined indexes BEFORE deleting rows
        // (while row indices are still valid)
        for (idx, row) in &rows_and_indices_to_delete {
            database.update_indexes_for_delete(&stmt.table_name, row, *idx);
        }

        // Step 5b: Actually delete the rows using fast path (no table scan needed)
        let table_mut = database
            .get_table_mut(&stmt.table_name)
            .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?;

        // Use delete_by_indices for O(d * log n) instead of O(n) where d = deletes
        let deleted_count = table_mut.delete_by_indices(&deleted_indices);

        // Step 5c: Adjust remaining user-defined index entries
        // (entries pointing to indices > deleted need to be decremented)
        database.adjust_indexes_after_delete(&stmt.table_name, &deleted_indices);

        // Invalidate columnar cache since table data has changed
        if deleted_count > 0 {
            database.invalidate_columnar_cache(&stmt.table_name);
        }

        // Step 6: Fire AFTER DELETE ROW triggers for each deleted row
        for (_, row) in &rows_and_indices_to_delete {
            crate::TriggerFirer::execute_after_triggers(
                database,
                &stmt.table_name,
                vibesql_ast::TriggerEvent::Delete,
                Some(row),
                None,
            )?;
        }

        // Fire AFTER STATEMENT triggers (unless we're already inside a trigger context)
        if trigger_context.is_none() {
            crate::TriggerFirer::execute_after_statement_triggers(
                database,
                &stmt.table_name,
                vibesql_ast::TriggerEvent::Delete,
            )?;
        }

        Ok(deleted_count)
    }

    /// Extract primary key value from WHERE expression if it's a simple equality
    fn extract_primary_key_lookup(
        where_expr: &vibesql_ast::Expression,
        schema: &vibesql_catalog::TableSchema,
    ) -> Option<Vec<vibesql_types::SqlValue>> {
        use vibesql_ast::{BinaryOperator, Expression};

        // Only handle simple binary equality operations
        if let Expression::BinaryOp { left, op: BinaryOperator::Equal, right } = where_expr {
            // Check if left side is a column reference and right side is a literal
            if let (Expression::ColumnRef { column, .. }, Expression::Literal(value)) =
                (left.as_ref(), right.as_ref())
            {
                // Check if this column is the primary key
                if let Some(pk_indices) = schema.get_primary_key_indices() {
                    if let Some(col_index) = schema.get_column_index(column) {
                        // Only handle single-column primary keys for now
                        if pk_indices.len() == 1 && pk_indices[0] == col_index {
                            return Some(vec![value.clone()]);
                        }
                    }
                }
            }

            // Also check the reverse: literal = column
            if let (Expression::Literal(value), Expression::ColumnRef { column, .. }) =
                (left.as_ref(), right.as_ref())
            {
                if let Some(pk_indices) = schema.get_primary_key_indices() {
                    if let Some(col_index) = schema.get_column_index(column) {
                        if pk_indices.len() == 1 && pk_indices[0] == col_index {
                            return Some(vec![value.clone()]);
                        }
                    }
                }
            }
        }

        None
    }

    /// Collect rows using table scan (fallback when PK optimization can't be used)
    fn collect_rows_with_scan(
        table: &vibesql_storage::Table,
        where_clause: &Option<vibesql_ast::WhereClause>,
        evaluator: &ExpressionEvaluator,
        rows_and_indices: &mut Vec<(usize, vibesql_storage::Row)>,
    ) -> Result<(), ExecutorError> {
        for (index, row) in table.scan().iter().enumerate() {
            // Clear CSE cache before evaluating each row to prevent column values
            // from being incorrectly cached across different rows
            evaluator.clear_cse_cache();

            let should_delete = if let Some(ref where_clause) = where_clause {
                match where_clause {
                    vibesql_ast::WhereClause::Condition(where_expr) => {
                        matches!(
                            evaluator.eval(where_expr, row),
                            Ok(vibesql_types::SqlValue::Boolean(true))
                        )
                    }
                    vibesql_ast::WhereClause::CurrentOf(_cursor_name) => {
                        return Err(ExecutorError::UnsupportedFeature(
                            "WHERE CURRENT OF cursor is not yet implemented".to_string(),
                        ));
                    }
                }
            } else {
                true
            };

            if should_delete {
                rows_and_indices.push((index, row.clone()));
            }
        }

        Ok(())
    }
}

/// Execute TRUNCATE-style fast path for DELETE FROM table (no WHERE)
///
/// Clears all rows and indexes in a single operation instead of row-by-row deletion.
/// Provides 100-1000x performance improvement for full table deletes.
///
/// # Safety
/// Only call this after `can_use_truncate` returns true.
fn execute_truncate(database: &mut Database, table_name: &str) -> Result<usize, ExecutorError> {
    let table = database
        .get_table_mut(table_name)
        .ok_or_else(|| ExecutorError::TableNotFound(table_name.to_string()))?;

    let row_count = table.row_count();

    // Clear all data at once (O(1) operation)
    table.clear();

    // Invalidate columnar cache since table data has changed
    if row_count > 0 {
        database.invalidate_columnar_cache(table_name);
    }

    Ok(row_count)
}

/// Execute a DELETE statement with trigger context
/// This function is used when executing DELETE statements within trigger bodies
/// to support OLD/NEW pseudo-variable references
pub fn execute_delete_with_trigger_context(
    database: &mut Database,
    stmt: &DeleteStmt,
    trigger_context: &crate::trigger_execution::TriggerContext,
) -> Result<usize, ExecutorError> {
    DeleteExecutor::execute_with_trigger_context(stmt, database, trigger_context)
}
