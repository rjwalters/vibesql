//! UPDATE statement execution
//!
//! This module provides UPDATE statement execution with the following architecture:
//!
//! - `row_selector`: Handles WHERE clause evaluation and primary key index optimization
//! - `value_updater`: Applies assignment expressions to rows
//! - `constraints`: Validates NOT NULL, PRIMARY KEY, UNIQUE, and CHECK constraints
//! - `foreign_keys`: Validates foreign key constraints and child references
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
mod foreign_keys;
mod row_selector;
mod value_updater;

use constraints::ConstraintValidator;
use foreign_keys::ForeignKeyValidator;
use row_selector::RowSelector;
use value_updater::ValueUpdater;
use vibesql_ast::{BinaryOperator, Expression, UpdateStmt};
use vibesql_storage::Database;

use crate::{
    errors::ExecutorError, evaluator::ExpressionEvaluator, privilege_checker::PrivilegeChecker,
};

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
    ///     table_name: "employees".to_string(),
    ///     assignments: vec![Assignment {
    ///         column: "salary".to_string(),
    ///         value: Expression::Literal(SqlValue::Integer(60000)),
    ///     }],
    ///     where_clause: None,
    /// };
    ///
    /// let count = UpdateExecutor::execute(&stmt, &mut db).unwrap();
    /// assert_eq!(count, 1);
    /// ```
    pub fn execute(stmt: &UpdateStmt, database: &mut Database) -> Result<usize, ExecutorError> {
        Self::execute_internal(stmt, database, None, None, None)
    }

    /// Execute an UPDATE statement with procedural context
    /// Supports procedural variables in SET and WHERE clauses
    pub fn execute_with_procedural_context(
        stmt: &UpdateStmt,
        database: &mut Database,
        procedural_context: &crate::procedural::ExecutionContext,
    ) -> Result<usize, ExecutorError> {
        Self::execute_internal(stmt, database, None, Some(procedural_context), None)
    }

    /// Execute an UPDATE statement with trigger context
    /// This allows UPDATE statements within trigger bodies to reference OLD/NEW pseudo-variables
    pub fn execute_with_trigger_context(
        stmt: &UpdateStmt,
        database: &mut Database,
        trigger_context: &crate::trigger_execution::TriggerContext,
    ) -> Result<usize, ExecutorError> {
        Self::execute_internal(stmt, database, None, None, Some(trigger_context))
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
        Self::execute_internal(stmt, database, schema, None, None)
    }

    /// Internal implementation supporting both schema caching, procedural context, and trigger context
    fn execute_internal(
        stmt: &UpdateStmt,
        database: &mut Database,
        schema: Option<&vibesql_catalog::TableSchema>,
        procedural_context: Option<&crate::procedural::ExecutionContext>,
        trigger_context: Option<&crate::trigger_execution::TriggerContext>,
    ) -> Result<usize, ExecutorError> {
        // Check UPDATE privilege on the table
        PrivilegeChecker::check_update(database, &stmt.table_name)?;

        // Step 1: Get table schema - clone it to avoid borrow issues
        // We need owned schema because we take mutable references to database later
        let schema_owned: vibesql_catalog::TableSchema = if let Some(s) = schema {
            s.clone()
        } else {
            database
                .catalog
                .get_table(&stmt.table_name)
                .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?
                .clone()
        };
        let schema = &schema_owned;

        // Check if table has UPDATE triggers (check once, use multiple times)
        let has_triggers = trigger_context.is_none()
            && database
                .catalog
                .get_triggers_for_table(
                    &stmt.table_name,
                    Some(vibesql_ast::TriggerEvent::Update(None)),
                )
                .next()
                .is_some();

        // Try fast path for simple single-row PK updates without triggers
        // Conditions: no triggers, no procedural context, simple WHERE pk = value
        if !has_triggers && procedural_context.is_none() && trigger_context.is_none() {
            if let Some(result) = Self::try_fast_path_update(stmt, database, schema)? {
                return Ok(result);
            }
        }

        // Fire BEFORE STATEMENT triggers only if triggers exist
        if has_triggers {
            crate::TriggerFirer::execute_before_statement_triggers(
                database,
                &stmt.table_name,
                vibesql_ast::TriggerEvent::Update(None),
            )?;
        }

        // Get PK indices without cloning entire schema
        let pk_indices = schema.get_primary_key_indices();

        // Step 2: Get table from storage (for reading rows)
        let table = database
            .get_table(&stmt.table_name)
            .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?;

        // Step 3: Create expression evaluator with database reference for subquery support
        //         and optional procedural/trigger context for variable resolution
        let evaluator = if let Some(ctx) = trigger_context {
            // Trigger context takes precedence (trigger statements can't have procedural context)
            ExpressionEvaluator::with_trigger_context(schema, database, ctx)
        } else if let Some(ctx) = procedural_context {
            ExpressionEvaluator::with_procedural_context(schema, database, ctx)
        } else {
            ExpressionEvaluator::with_database(schema, database)
        };

        // Step 4: Select rows to update using RowSelector
        let row_selector = RowSelector::new(schema, &evaluator);
        let candidate_rows = row_selector.select_rows(table, &stmt.where_clause)?;

        // Step 5: Create value updater
        let value_updater = ValueUpdater::new(schema, &evaluator, &stmt.table_name);

        // Step 6: Build list of updates (two-phase execution for SQL semantics)
        // Each update consists of: (row_index, old_row, new_row, changed_columns, updates_pk)
        let mut updates: Vec<(
            usize,
            vibesql_storage::Row,
            vibesql_storage::Row,
            std::collections::HashSet<usize>,
            bool, // whether PK is being updated
        )> = Vec::new();

        for (row_index, row) in candidate_rows {
            // Clear CSE cache before evaluating assignment expressions for this row
            // to prevent cached column values from previous rows
            evaluator.clear_cse_cache();

            // Apply assignments to build updated row
            let (new_row, changed_columns) =
                value_updater.apply_assignments(&row, &stmt.assignments)?;

            // Check if primary key is being updated
            let updates_pk = if let Some(ref pk_idx) = pk_indices {
                stmt.assignments.iter().any(|a| {
                    let col_index = schema.get_column_index(&a.column).unwrap();
                    pk_idx.contains(&col_index)
                })
            } else {
                false
            };

            // Validate all constraints (NOT NULL, PRIMARY KEY, UNIQUE, CHECK)
            let constraint_validator = ConstraintValidator::new(schema);
            constraint_validator.validate_row(
                table,
                &stmt.table_name,
                row_index,
                &new_row,
                &row,
            )?;

            // Validate user-defined UNIQUE indexes (CREATE UNIQUE INDEX)
            constraint_validator.validate_unique_indexes(
                database,
                &stmt.table_name,
                &new_row,
                &row,
            )?;

            // Enforce FOREIGN KEY constraints (child table)
            if !schema.foreign_keys.is_empty() {
                ForeignKeyValidator::validate_constraints(
                    database,
                    &stmt.table_name,
                    &new_row.values,
                )?;
            }

            updates.push((row_index, row.clone(), new_row, changed_columns, updates_pk));
        }

        // Step 7: Handle CASCADE updates for primary key changes (before triggers)
        // This must happen after validation but before applying parent updates
        for (_row_index, old_row, new_row, _changed_columns, updates_pk) in &updates {
            if *updates_pk {
                ForeignKeyValidator::check_no_child_references(
                    database,
                    &stmt.table_name,
                    old_row,
                    new_row,
                )?;
            }
        }

        // Fire BEFORE UPDATE triggers for all rows (before database mutation)
        if has_triggers {
            for (_row_index, old_row, new_row, _changed_columns, _updates_pk) in &updates {
                crate::TriggerFirer::execute_before_triggers(
                    database,
                    &stmt.table_name,
                    vibesql_ast::TriggerEvent::Update(None),
                    Some(old_row),
                    Some(new_row),
                )?;
            }
        }

        // Step 8: Apply all updates (after evaluation phase completes)
        let update_count = updates.len();

        // Get mutable table reference
        let table_mut = database
            .get_table_mut(&stmt.table_name)
            .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?;

        // Collect the updates first
        let mut index_updates = Vec::new();
        for (index, old_row, new_row, changed_columns, _updates_pk) in &updates {
            table_mut
                .update_row_selective(*index, new_row.clone(), changed_columns)
                .map_err(|e| ExecutorError::StorageError(e.to_string()))?;

            index_updates.push((*index, old_row.clone(), new_row.clone()));
        }

        // Fire AFTER UPDATE triggers for all updated rows
        if has_triggers {
            for (_index, old_row, new_row) in &index_updates {
                crate::TriggerFirer::execute_after_triggers(
                    database,
                    &stmt.table_name,
                    vibesql_ast::TriggerEvent::Update(None),
                    Some(old_row),
                    Some(new_row),
                )?;
            }
        }

        // Now update user-defined indexes after releasing table borrow
        for (index, old_row, new_row) in index_updates {
            database.update_indexes_for_update(&stmt.table_name, &old_row, &new_row, index);
        }

        // Invalidate columnar cache since table data has changed
        if update_count > 0 {
            database.invalidate_columnar_cache(&stmt.table_name);
        }

        // Fire AFTER STATEMENT triggers only if triggers exist
        if has_triggers {
            crate::TriggerFirer::execute_after_statement_triggers(
                database,
                &stmt.table_name,
                vibesql_ast::TriggerEvent::Update(None),
            )?;
        }

        Ok(update_count)
    }

    /// Try to execute UPDATE via fast path for simple single-row PK updates.
    /// Returns Some(count) if fast path succeeded, None if we should use normal path.
    ///
    /// Fast path conditions:
    /// - WHERE clause is simple equality on single-column primary key
    /// - No foreign keys to validate
    /// - Table has a primary key index
    fn try_fast_path_update(
        stmt: &UpdateStmt,
        database: &mut Database,
        schema: &vibesql_catalog::TableSchema,
    ) -> Result<Option<usize>, ExecutorError> {
        // Check if we have a simple PK lookup in WHERE clause
        let where_clause = match &stmt.where_clause {
            Some(vibesql_ast::WhereClause::Condition(expr)) => expr,
            _ => return Ok(None), // No WHERE or CURRENT OF - use normal path
        };

        // Extract PK value from WHERE clause
        let pk_value = match Self::extract_pk_equality(where_clause, schema) {
            Some(val) => val,
            None => return Ok(None), // Not a simple PK equality
        };

        // Get table and check for PK index
        let table = database
            .get_table(&stmt.table_name)
            .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?;

        let pk_index = match table.primary_key_index() {
            Some(idx) => idx,
            None => return Ok(None), // No PK index
        };

        // Look up row by PK
        let row_index = match pk_index.get(&pk_value) {
            Some(&idx) => idx,
            None => return Ok(Some(0)), // Row not found - 0 rows updated
        };

        // Skip fast path if table has foreign keys (need validation)
        if !schema.foreign_keys.is_empty() {
            return Ok(None);
        }

        // Skip fast path if table has unique constraints (need validation)
        if !schema.unique_constraints.is_empty() {
            return Ok(None);
        }

        // Check if we're updating PK columns - if so, check for CASCADE requirements
        if let Some(ref pk_idx) = schema.get_primary_key_indices() {
            let updates_pk = stmt.assignments.iter().any(|a| {
                schema.get_column_index(&a.column).map(|idx| pk_idx.contains(&idx)).unwrap_or(false)
            });
            if updates_pk {
                // Check if ANY table in database has foreign keys (might need CASCADE)
                let has_any_fks = database.catalog.list_tables().iter().any(|table_name| {
                    database
                        .catalog
                        .get_table(table_name)
                        .map(|s| !s.foreign_keys.is_empty())
                        .unwrap_or(false)
                });
                if has_any_fks {
                    return Ok(None); // Use normal path for CASCADE handling
                }
            }
        }

        // Get the old row
        let old_row = table.scan()[row_index].clone();

        // Create evaluator for expression evaluation
        let evaluator = ExpressionEvaluator::with_database(schema, database);

        // Apply assignments
        let mut new_row = old_row.clone();
        let mut changed_columns = std::collections::HashSet::new();

        for assignment in &stmt.assignments {
            let col_index = schema.get_column_index(&assignment.column).ok_or_else(|| {
                ExecutorError::ColumnNotFound {
                    column_name: assignment.column.clone(),
                    table_name: stmt.table_name.clone(),
                    searched_tables: vec![stmt.table_name.clone()],
                    available_columns: schema.columns.iter().map(|c| c.name.clone()).collect(),
                }
            })?;

            let new_value = match &assignment.value {
                vibesql_ast::Expression::Default => {
                    let column = &schema.columns[col_index];
                    if let Some(default_expr) = &column.default_value {
                        match default_expr {
                            vibesql_ast::Expression::Literal(lit) => lit.clone(),
                            _ => return Ok(None), // Complex default - use normal path
                        }
                    } else {
                        vibesql_types::SqlValue::Null
                    }
                }
                _ => evaluator.eval(&assignment.value, &old_row)?,
            };

            new_row
                .set(col_index, new_value)
                .map_err(|e| ExecutorError::StorageError(e.to_string()))?;
            changed_columns.insert(col_index);
        }

        // Quick constraint validation (NOT NULL only for changed columns)
        for &col_idx in &changed_columns {
            let column = &schema.columns[col_idx];
            if !column.nullable && new_row.values[col_idx] == vibesql_types::SqlValue::Null {
                return Err(ExecutorError::ConstraintViolation(format!(
                    "NOT NULL constraint violation: column '{}' cannot be NULL",
                    column.name
                )));
            }
        }

        // Check PK uniqueness if updating PK columns
        let pk_indices = schema.get_primary_key_indices();
        if let Some(ref pk_idx) = pk_indices {
            let updates_pk = changed_columns.iter().any(|c| pk_idx.contains(c));
            if updates_pk {
                // PK is being updated - need to check uniqueness
                let new_pk: Vec<_> = pk_idx.iter().map(|&i| new_row.values[i].clone()).collect();
                if let Some(pk_index) = table.primary_key_index() {
                    if let Some(&existing_idx) = pk_index.get(&new_pk) {
                        if existing_idx != row_index {
                            return Err(ExecutorError::ConstraintViolation(format!(
                                "PRIMARY KEY constraint violation: duplicate key {:?} on {}",
                                new_pk, stmt.table_name
                            )));
                        }
                    }
                }
            }
        }

        // Apply the update directly
        let table_mut = database
            .get_table_mut(&stmt.table_name)
            .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?;

        table_mut
            .update_row_selective(row_index, new_row.clone(), &changed_columns)
            .map_err(|e| ExecutorError::StorageError(e.to_string()))?;

        // Update user-defined indexes
        database.update_indexes_for_update(&stmt.table_name, &old_row, &new_row, row_index);

        // Invalidate columnar cache
        database.invalidate_columnar_cache(&stmt.table_name);

        Ok(Some(1))
    }

    /// Extract primary key value from a simple equality expression.
    /// Returns Some(pk_values) if expression is `pk_column = literal` or `literal = pk_column`.
    fn extract_pk_equality(
        expr: &Expression,
        schema: &vibesql_catalog::TableSchema,
    ) -> Option<Vec<vibesql_types::SqlValue>> {
        if let Expression::BinaryOp { left, op: BinaryOperator::Equal, right } = expr {
            // Check: column = literal
            if let (Expression::ColumnRef { column, .. }, Expression::Literal(value)) =
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

            // Check: literal = column
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
}

/// Execute an UPDATE statement with trigger context
/// This function is used when executing UPDATE statements within trigger bodies
/// to support OLD/NEW pseudo-variable references
pub fn execute_update_with_trigger_context(
    database: &mut Database,
    stmt: &UpdateStmt,
    trigger_context: &crate::trigger_execution::TriggerContext,
) -> Result<usize, ExecutorError> {
    UpdateExecutor::execute_with_trigger_context(stmt, database, trigger_context)
}
