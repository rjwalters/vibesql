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
use vibesql_storage::statistics::CostEstimator;
use vibesql_storage::Database;

use crate::{
    dml_cost::DmlOptimizer, errors::ExecutorError, evaluator::ExpressionEvaluator,
    privilege_checker::PrivilegeChecker,
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

        // Estimate DML cost for query analysis and optimization decisions
        if std::env::var("DML_COST_DEBUG").is_ok() && !candidate_rows.is_empty() {
            if let Some(index_info) = database.get_table_index_info(&stmt.table_name) {
                // Get table statistics for cost estimation (use cached if available, or fallback to estimate)
                let table_stats = table
                    .get_statistics()
                    .cloned()
                    .unwrap_or_else(|| vibesql_storage::TableStatistics::estimate_from_row_count(table.row_count()));

                // Estimate the ratio of indexes affected based on columns being updated
                // This is a heuristic: assume columns are distributed evenly across indexes
                let total_columns = schema.columns.len();
                let changed_columns = stmt.assignments.len();
                let indexes_affected_ratio = if total_columns > 0 {
                    (changed_columns as f64 / total_columns as f64).min(1.0)
                } else {
                    1.0 // Conservative estimate if no columns
                };

                let cost_estimator = CostEstimator::default();
                let estimated_cost = cost_estimator.estimate_update(
                    candidate_rows.len(),
                    &table_stats,
                    &index_info,
                    indexes_affected_ratio,
                );
                eprintln!(
                    "DML_COST_DEBUG: UPDATE {} rows in {} - estimated_cost: {:.2} (hash_indexes: {}, btree_indexes: {}, columnar: {}, affected_ratio: {:.2})",
                    candidate_rows.len(),
                    stmt.table_name,
                    estimated_cost,
                    index_info.hash_index_count,
                    index_info.btree_index_count,
                    index_info.is_native_columnar,
                    indexes_affected_ratio
                );
            }
        }

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

        // Cost-based optimization: Log update cost with indexes_affected_ratio
        if !updates.is_empty() {
            // Compute aggregate changed columns across all updates
            let mut all_changed_columns = std::collections::HashSet::new();
            for (_, _, _, changed_cols, _) in &updates {
                all_changed_columns.extend(changed_cols.iter().copied());
            }

            let optimizer = DmlOptimizer::new(database, &stmt.table_name);
            let indexes_affected_ratio =
                optimizer.compute_indexes_affected_ratio(&all_changed_columns, schema);
            let _update_cost = optimizer.estimate_update_cost(updates.len(), indexes_affected_ratio);

            // Log optimization insight: selective updates (low affected ratio) are much cheaper
            if std::env::var("DML_COST_DEBUG").is_ok() && indexes_affected_ratio < 1.0 {
                eprintln!(
                    "DML_COST_DEBUG: UPDATE on {} - {} rows, {:.0}% indexes affected (selective update optimization)",
                    stmt.table_name,
                    updates.len(),
                    indexes_affected_ratio * 100.0
                );
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

            index_updates.push((*index, old_row.clone(), new_row.clone(), changed_columns.clone()));
        }

        // Fire AFTER UPDATE triggers for all updated rows
        if has_triggers {
            for (_index, old_row, new_row, _changed_columns) in &index_updates {
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
        // Pass changed_columns to skip indexes that don't involve any modified columns
        for (index, old_row, new_row, changed_columns) in index_updates {
            database.update_indexes_for_update(&stmt.table_name, &old_row, &new_row, index, Some(&changed_columns));
        }

        // Invalidate the database-level columnar cache since table data changed.
        // Note: Table-level cache is invalidated by update_row_fast()/update_row_selective().
        // Both invalidations are necessary because they manage separate caches:
        // - Table-level cache: used by Table::scan_columnar() for SIMD filtering
        // - Database-level cache: used by Database::get_columnar() for cached access
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

        // Get table and check for PK index, look up row index
        let row_index = {
            let table = database
                .get_table(&stmt.table_name)
                .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?;

            let pk_index = match table.primary_key_index() {
                Some(idx) => idx,
                None => return Ok(None), // No PK index
            };

            // Look up row by PK
            match pk_index.get(&pk_value) {
                Some(&idx) => idx,
                None => return Ok(Some(0)), // Row not found - 0 rows updated
            }
        }; // table borrow ends here

        // SUPER-FAST PATH: All literal assignments to non-indexed, non-PK, non-unique columns
        // This path avoids ALL row cloning by updating columns in-place
        // Extended from single-assignment to support multiple assignments (ONEPASS optimization)
        if let Some(result) = Self::try_super_fast_path(stmt, database, schema, row_index)? {
            return Ok(Some(result));
        }

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

        // Re-borrow table to get the old row
        let table = database
            .get_table(&stmt.table_name)
            .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?;
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

        // Update user-defined indexes FIRST (while we still have both row references)
        // Pass changed_columns to skip indexes that don't involve any modified columns
        database.update_indexes_for_update(&stmt.table_name, &old_row, &new_row, row_index, Some(&changed_columns));

        // Apply the update directly (transfers ownership of new_row, no clone needed)
        let table_mut = database
            .get_table_mut(&stmt.table_name)
            .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?;

        // Use unchecked variant - row is already validated above
        table_mut.update_row_unchecked(row_index, new_row, &old_row, &changed_columns);

        Ok(Some(1))
    }

    /// Try SUPER-FAST path: direct in-place column updates for literal assignments
    /// to non-indexed, non-PK, non-unique columns.
    ///
    /// This is the ONEPASS optimization for single-row updates:
    /// - Supports multiple assignments (not just single)
    /// - Validates all columns can be updated in-place
    /// - No row cloning required
    ///
    /// Returns Some(1) if all updates were applied in-place, None if should use normal path.
    fn try_super_fast_path(
        stmt: &UpdateStmt,
        database: &mut Database,
        schema: &vibesql_catalog::TableSchema,
        row_index: usize,
    ) -> Result<Option<usize>, ExecutorError> {
        // Collect all literal updates that can be done in-place
        let mut inplace_updates: Vec<(usize, vibesql_types::SqlValue)> = Vec::new();

        let pk_indices = schema.get_primary_key_indices();

        for assignment in &stmt.assignments {
            // Check if value is a literal (no expression evaluation needed)
            let new_value = match &assignment.value {
                vibesql_ast::Expression::Literal(val) => val.clone(),
                _ => return Ok(None), // Non-literal expression - use normal path
            };

            let col_index = match schema.get_column_index(&assignment.column) {
                Some(idx) => idx,
                None => return Ok(None), // Column not found - let normal path handle error
            };

            // Check column is not in PK
            let is_pk_col = pk_indices
                .as_ref()
                .map(|pk| pk.contains(&col_index))
                .unwrap_or(false);
            if is_pk_col {
                return Ok(None); // PK update needs full validation
            }

            // Check column is not in any unique constraint
            let col_name_upper = assignment.column.to_uppercase();
            let is_unique_col = schema
                .unique_constraints
                .iter()
                .any(|uc| uc.iter().any(|name| name.to_uppercase() == col_name_upper));
            if is_unique_col {
                return Ok(None); // Unique constraint needs validation
            }

            // Check NOT NULL constraint
            let column = &schema.columns[col_index];
            if !column.nullable && new_value == vibesql_types::SqlValue::Null {
                return Err(ExecutorError::ConstraintViolation(format!(
                    "NOT NULL constraint violation: column '{}' cannot be NULL",
                    column.name
                )));
            }

            // Check no user-defined indexes on this column
            if database.has_index_on_column(&stmt.table_name, &assignment.column) {
                return Ok(None); // Index update needs normal path
            }

            inplace_updates.push((col_index, new_value));
        }

        // All checks passed - apply updates in-place
        if inplace_updates.is_empty() {
            return Ok(None); // No updates to apply
        }

        let table_mut = database
            .get_table_mut(&stmt.table_name)
            .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?;

        // Apply all column updates in-place (no row cloning!)
        for (col_index, new_value) in inplace_updates {
            table_mut.update_column_inplace(row_index, col_index, new_value);
        }

        Ok(Some(1))
    }

    /// Extract primary key values from WHERE expression.
    ///
    /// Supports:
    /// - Single-column PK: `pk = value` or `value = pk`
    /// - Composite PK: `pk1 = val1 AND pk2 = val2` (any order)
    ///
    /// Returns Some(pk_values) in PK column order if all PK columns are matched,
    /// None otherwise.
    fn extract_pk_equality(
        expr: &Expression,
        schema: &vibesql_catalog::TableSchema,
    ) -> Option<Vec<vibesql_types::SqlValue>> {
        let pk_indices = schema.get_primary_key_indices()?;
        if pk_indices.is_empty() {
            return None;
        }

        // Collect all column = literal equalities from the expression
        let mut equalities: std::collections::HashMap<usize, vibesql_types::SqlValue> =
            std::collections::HashMap::new();
        Self::collect_pk_equalities(expr, schema, &mut equalities);

        // Check if we have all PK columns and build result in PK order
        let mut pk_values = Vec::with_capacity(pk_indices.len());
        for &pk_col in &pk_indices {
            match equalities.get(&pk_col) {
                Some(value) => pk_values.push(value.clone()),
                None => return None, // Missing PK column
            }
        }

        Some(pk_values)
    }

    /// Recursively collect column = literal equalities from WHERE expression
    fn collect_pk_equalities(
        expr: &Expression,
        schema: &vibesql_catalog::TableSchema,
        equalities: &mut std::collections::HashMap<usize, vibesql_types::SqlValue>,
    ) {
        match expr {
            Expression::BinaryOp { left, op: BinaryOperator::And, right } => {
                // Recurse into AND branches
                Self::collect_pk_equalities(left, schema, equalities);
                Self::collect_pk_equalities(right, schema, equalities);
            }
            Expression::Conjunction(exprs) => {
                // Handle flattened AND chains
                for e in exprs {
                    Self::collect_pk_equalities(e, schema, equalities);
                }
            }
            Expression::BinaryOp { left, op: BinaryOperator::Equal, right } => {
                // Check: column = literal
                if let (Expression::ColumnRef { column, .. }, Expression::Literal(value)) =
                    (left.as_ref(), right.as_ref())
                {
                    if let Some(col_index) = schema.get_column_index(column) {
                        equalities.insert(col_index, value.clone());
                    }
                }
                // Check: literal = column
                else if let (Expression::Literal(value), Expression::ColumnRef { column, .. }) =
                    (left.as_ref(), right.as_ref())
                {
                    if let Some(col_index) = schema.get_column_index(column) {
                        equalities.insert(col_index, value.clone());
                    }
                }
            }
            _ => {} // Ignore other expressions
        }
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
