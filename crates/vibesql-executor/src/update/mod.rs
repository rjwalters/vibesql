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
use vibesql_catalog::TableIdentifier;
use vibesql_storage::{statistics::CostEstimator, Database};

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
    ///     quoted: false,
    ///     assignments: vec![Assignment {
    ///         column: "salary".to_string(),
    ///         value: Expression::Literal(SqlValue::Integer(60000)),
    ///     }],
    ///     where_clause: None,
    ///     conflict_clause: None,
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

    /// Internal implementation supporting both schema caching, procedural context, and trigger
    /// context
    fn execute_internal(
        stmt: &UpdateStmt,
        database: &mut Database,
        schema: Option<&vibesql_catalog::TableSchema>,
        procedural_context: Option<&crate::procedural::ExecutionContext>,
        trigger_context: Option<&crate::trigger_execution::TriggerContext>,
    ) -> Result<usize, ExecutorError> {
        // Check UPDATE privilege on the table
        PrivilegeChecker::check_update(database, &stmt.table_name)?;

        // Check if target is a VIEW with INSTEAD OF triggers
        if let Some(view_def) = database.catalog.get_view(&stmt.table_name).cloned() {
            return execute_update_on_view(database, stmt, &view_def, procedural_context, trigger_context);
        }

        // Step 1: Get table schema - clone it to avoid borrow issues
        // We need owned schema because we take mutable references to database later
        // Use TableIdentifier for SQL:1999 case-sensitive lookups when quoted
        let table_id = TableIdentifier::new(&stmt.table_name, stmt.quoted);
        let schema_owned: vibesql_catalog::TableSchema = if let Some(s) = schema {
            s.clone()
        } else {
            database
                .catalog
                .get_table_by_identifier(&table_id)
                .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?
                .clone()
        };
        let schema = &schema_owned;

        // Use canonical table name from schema for all storage operations
        // This ensures case-sensitive tables (quoted identifiers) are accessed correctly
        let table_name = &schema_owned.name;

        // Check if table has UPDATE triggers (check once, use multiple times)
        let has_triggers = trigger_context.is_none()
            && database
                .catalog
                .get_triggers_for_table(
                    table_name,
                    Some(vibesql_ast::TriggerEvent::Update(None)),
                )
                .next()
                .is_some();

        // Try fast path for simple single-row PK updates without triggers
        // Conditions: no triggers, no procedural context, simple WHERE pk = value, no assertions
        // Skip fast path if assertions exist because we need rollback capability on violation
        let has_assertions = database.catalog.get_all_assertions().next().is_some();
        if !has_triggers
            && procedural_context.is_none()
            && trigger_context.is_none()
            && !has_assertions
        {
            if let Some(result) = Self::try_fast_path_update(stmt, database, schema)? {
                // Invalidate columnar cache after fast path update
                if result > 0 {
                    database.invalidate_columnar_cache(table_name);
                }
                return Ok(result);
            }
        }

        // Fire BEFORE STATEMENT triggers only if triggers exist
        if has_triggers {
            crate::TriggerFirer::execute_before_statement_triggers(
                database,
                table_name,
                vibesql_ast::TriggerEvent::Update(None),
            )?;
        }

        // Get PK indices without cloning entire schema
        let pk_indices = schema.get_primary_key_indices();

        // Step 2: Get table from storage (for reading rows)
        let table = database
            .get_table(table_name)
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
            if let Some(index_info) = database.get_table_index_info(table_name) {
                // Get table statistics for cost estimation (use cached if available, or fallback to
                // estimate)
                let table_stats = table.get_statistics().cloned().unwrap_or_else(|| {
                    vibesql_storage::TableStatistics::estimate_from_row_count(table.row_count())
                });

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
        let value_updater = ValueUpdater::new(schema, &evaluator, table_name);

        // Check conflict resolution clause
        let use_ignore =
            matches!(stmt.conflict_clause, Some(vibesql_ast::ConflictClause::Ignore));
        let use_replace =
            matches!(stmt.conflict_clause, Some(vibesql_ast::ConflictClause::Replace));

        // Step 6: Build list of updates (two-phase execution for SQL semantics)
        // Each update consists of: (row_index, old_row, new_row, changed_columns, updates_pk)
        let mut updates: Vec<(
            usize,
            vibesql_storage::Row,
            vibesql_storage::Row,
            std::collections::HashSet<usize>,
            bool, // whether PK is being updated
        )> = Vec::new();

        // Track rows to delete for REPLACE conflict resolution (before applying updates)
        let mut rows_to_delete_for_replace: Vec<usize> = Vec::new();

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

            // For REPLACE: find and mark conflicting rows for deletion before validation
            if use_replace {
                let conflicting_indices = find_conflicting_rows_for_update(
                    table,
                    schema,
                    database,
                    table_name,
                    &new_row,
                    row_index,
                );
                rows_to_delete_for_replace.extend(conflicting_indices);
            }

            // Validate all constraints (NOT NULL, PRIMARY KEY, UNIQUE, CHECK)
            // For IGNORE: catch constraint violations and skip the row
            // For REPLACE: we've already marked conflicts for deletion, so skip PK/UNIQUE validation
            let constraint_validator = ConstraintValidator::new(schema);

            if use_ignore {
                // For IGNORE: try validation and skip row on any constraint violation
                let validation_result = constraint_validator.validate_row(
                    table,
                    table_name,
                    row_index,
                    &new_row,
                    &row,
                );
                if validation_result.is_err() {
                    continue; // Skip this row
                }

                // Validate user-defined UNIQUE indexes
                let unique_index_result = constraint_validator.validate_unique_indexes(
                    database,
                    table_name,
                    &new_row,
                    &row,
                );
                if unique_index_result.is_err() {
                    continue; // Skip this row
                }

                // Validate foreign key constraints
                if !schema.foreign_keys.is_empty() {
                    let fk_result = ForeignKeyValidator::validate_constraints(
                        database,
                        table_name,
                        &new_row.values,
                    );
                    if fk_result.is_err() {
                        continue; // Skip this row
                    }
                }
            } else if use_replace {
                // For REPLACE: validate NOT NULL and CHECK constraints, but skip PK/UNIQUE
                // since conflicting rows will be deleted
                validate_non_uniqueness_constraints(schema, table_name, &new_row)?;

                // Validate foreign key constraints
                if !schema.foreign_keys.is_empty() {
                    ForeignKeyValidator::validate_constraints(
                        database,
                        table_name,
                        &new_row.values,
                    )?;
                }
            } else {
                // Default: validate all constraints
                constraint_validator.validate_row(
                    table,
                    table_name,
                    row_index,
                    &new_row,
                    &row,
                )?;

                // Validate user-defined UNIQUE indexes (CREATE UNIQUE INDEX)
                constraint_validator.validate_unique_indexes(
                    database,
                    table_name,
                    &new_row,
                    &row,
                )?;

                // Enforce FOREIGN KEY constraints (child table)
                if !schema.foreign_keys.is_empty() {
                    ForeignKeyValidator::validate_constraints(
                        database,
                        table_name,
                        &new_row.values,
                    )?;
                }
            }

            updates.push((row_index, row.clone(), new_row, changed_columns, updates_pk));
        }

        // Cross-update uniqueness validation: check if multiple updates would produce
        // the same PK or UNIQUE constraint values. This must be done after collecting
        // all updates but before applying them to ensure SQL's two-phase semantics.
        // Skip for REPLACE mode since conflicts will be resolved by deletion.
        if !use_replace && !use_ignore && updates.len() > 1 {
            validate_cross_update_uniqueness(&updates, schema)?;
        }

        // For REPLACE: handle cross-update conflicts by keeping only the last update
        // for each PK/UNIQUE value. Earlier updates with conflicting values are removed
        // from updates and their rows are deleted instead.
        if use_replace && updates.len() > 1 {
            let removed_indices =
                resolve_cross_update_conflicts_for_replace(&mut updates, schema);
            rows_to_delete_for_replace.extend(removed_indices);
        }

        // For REPLACE: delete conflicting rows before applying updates
        if use_replace && !rows_to_delete_for_replace.is_empty() {
            // De-duplicate and sort
            rows_to_delete_for_replace.sort_unstable();
            rows_to_delete_for_replace.dedup();

            // Filter out any rows that we're going to update (shouldn't delete our own rows)
            let update_indices: std::collections::HashSet<usize> =
                updates.iter().map(|(idx, _, _, _, _)| *idx).collect();
            rows_to_delete_for_replace.retain(|idx| !update_indices.contains(idx));

            if !rows_to_delete_for_replace.is_empty() {
                // Get rows for index cleanup
                let rows_for_index: Vec<(usize, vibesql_storage::Row)> = rows_to_delete_for_replace
                    .iter()
                    .filter_map(|&idx| table.scan().get(idx).map(|r| (idx, r.clone())))
                    .collect();

                // Update indexes before deletion
                let rows_refs: Vec<(usize, &vibesql_storage::Row)> =
                    rows_for_index.iter().map(|(idx, row)| (*idx, row)).collect();
                database.batch_update_indexes_for_delete(table_name, &rows_refs);

                // Delete conflicting rows
                let table_mut = database
                    .get_table_mut(table_name)
                    .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?;

                let delete_result = table_mut.delete_by_indices_batch(&rows_to_delete_for_replace);

                // Handle index maintenance based on compaction
                if delete_result.compacted {
                    database.rebuild_indexes(table_name);
                    // KNOWN LIMITATION: After compaction, row indices in the `updates` vector
                    // may be stale since compaction can shift row positions. This is safe in
                    // practice because:
                    // 1. Compaction only occurs when deletion count exceeds a high threshold
                    //    (typically when > 50% of rows are deleted)
                    // 2. UPDATE OR REPLACE typically deletes a small number of conflicting rows
                    // 3. The likelihood of triggering compaction during UPDATE OR REPLACE is low
                    // For correctness in edge cases, a future improvement could re-scan the
                    // table to recalculate update indices based on row content matching.
                } else {
                    database.adjust_indexes_after_delete(table_name, &rows_to_delete_for_replace);
                }

                // Re-fetch table for the remaining operations
                let _table = database
                    .get_table(table_name)
                    .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?;
            }
        }

        // Step 7: Handle CASCADE updates for primary key changes (before triggers)
        // This must happen after validation but before applying parent updates
        for (_row_index, old_row, new_row, _changed_columns, updates_pk) in &updates {
            if *updates_pk {
                ForeignKeyValidator::check_no_child_references(
                    database,
                    table_name,
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

            let optimizer = DmlOptimizer::new(database, table_name);
            let indexes_affected_ratio =
                optimizer.compute_indexes_affected_ratio(&all_changed_columns, schema);
            let _update_cost =
                optimizer.estimate_update_cost(updates.len(), indexes_affected_ratio);

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
                    table_name,
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
            .get_table_mut(table_name)
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
                    table_name,
                    vibesql_ast::TriggerEvent::Update(None),
                    Some(old_row),
                    Some(new_row),
                )?;
            }
        }

        // Now update user-defined indexes after releasing table borrow
        // Pass changed_columns to skip indexes that don't involve any modified columns
        // Clone for rollback support if assertions exist
        let index_updates_for_rollback: Vec<_> = index_updates
            .iter()
            .map(|(idx, old, _new, changed)| (*idx, old.clone(), changed.clone()))
            .collect();
        for (index, old_row, new_row, changed_columns) in index_updates {
            database.update_indexes_for_update(
                table_name,
                &old_row,
                &new_row,
                index,
                Some(&changed_columns),
            );
        }

        // Invalidate the database-level columnar cache since table data changed.
        // Note: Table-level cache is invalidated by update_row_fast()/update_row_selective().
        // Both invalidations are necessary because they manage separate caches:
        // - Table-level cache: used by Table::scan_columnar() for SIMD filtering
        // - Database-level cache: used by Database::get_columnar() for cached access
        if update_count > 0 {
            database.invalidate_columnar_cache(table_name);
        }

        // Fire AFTER STATEMENT triggers only if triggers exist
        if has_triggers {
            crate::TriggerFirer::execute_after_statement_triggers(
                database,
                table_name,
                vibesql_ast::TriggerEvent::Update(None),
            )?;
        }

        // Check all assertions after UPDATE completes (SQL:1999 Feature F671/F672)
        // This ensures database-wide integrity constraints are maintained
        if let Err(assertion_error) =
            crate::advanced_objects::AssertionChecker::check_all_assertions(database)
        {
            // Assertion violated - rollback the update by restoring old values
            if let Some(table_mut) = database.get_table_mut(table_name) {
                for (index, old_row, changed_columns) in &index_updates_for_rollback {
                    // Restore the old row values for changed columns
                    let _ =
                        table_mut.update_row_selective(*index, old_row.clone(), changed_columns);
                }
            }
            // Also invalidate cache after rollback
            database.invalidate_columnar_cache(table_name);
            return Err(assertion_error);
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
        // Use canonical table name from schema for all storage operations
        let table_name = &schema.name;

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
                .get_table(table_name)
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
            .get_table(table_name)
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
        database.update_indexes_for_update(
            table_name,
            &old_row,
            &new_row,
            row_index,
            Some(&changed_columns),
        );

        // Apply the update directly (transfers ownership of new_row, no clone needed)
        let table_mut = database
            .get_table_mut(table_name)
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
        // Use canonical table name from schema for all storage operations
        let table_name = &schema.name;

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
            let is_pk_col = pk_indices.as_ref().map(|pk| pk.contains(&col_index)).unwrap_or(false);
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
            if database.has_index_on_column(table_name, &assignment.column) {
                return Ok(None); // Index update needs normal path
            }

            inplace_updates.push((col_index, new_value));
        }

        // All checks passed - apply updates in-place
        if inplace_updates.is_empty() {
            return Ok(None); // No updates to apply
        }

        let table_mut = database
            .get_table_mut(table_name)
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
                if let (Expression::ColumnRef(col_id), Expression::Literal(value)) =
                    (left.as_ref(), right.as_ref())
                {
                    if let Some(col_index) = schema.get_column_index(col_id.column_canonical()) {
                        equalities.insert(col_index, value.clone());
                    }
                }
                // Check: literal = column
                else if let (Expression::Literal(value), Expression::ColumnRef(col_id)) =
                    (left.as_ref(), right.as_ref())
                {
                    if let Some(col_index) = schema.get_column_index(col_id.column_canonical()) {
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

/// Execute UPDATE on a VIEW using INSTEAD OF triggers
///
/// When updating a view, we need to fire INSTEAD OF UPDATE triggers
/// instead of actually updating data. The triggers typically update
/// the underlying tables.
fn execute_update_on_view(
    database: &mut Database,
    stmt: &UpdateStmt,
    view_def: &vibesql_catalog::ViewDefinition,
    procedural_context: Option<&crate::procedural::ExecutionContext>,
    trigger_context: Option<&crate::trigger_execution::TriggerContext>,
) -> Result<usize, ExecutorError> {
    use vibesql_ast::TriggerTiming;

    // Find INSTEAD OF UPDATE triggers for this view
    let triggers = crate::TriggerFirer::find_triggers(
        database,
        &view_def.name,
        TriggerTiming::InsteadOf,
        vibesql_ast::TriggerEvent::Update(None),
    );

    if triggers.is_empty() {
        return Err(ExecutorError::UnsupportedExpression(format!(
            "Cannot UPDATE view '{}' without INSTEAD OF trigger",
            view_def.name
        )));
    }

    // Build a pseudo-schema for the view
    let view_schema = build_view_schema(database, view_def)?;

    // Execute the view query to get the rows to potentially update
    let select_executor = crate::SelectExecutor::new(database);
    let all_rows = select_executor.execute_with_columns(&view_def.query)?;

    // Collect all (old_row, new_row) pairs first, before firing triggers
    // This avoids borrow conflicts with the evaluator
    let updates: Vec<(vibesql_storage::Row, vibesql_storage::Row)> = {
        // Create evaluator for WHERE clause (if any)
        let evaluator = if let Some(ctx) = trigger_context {
            ExpressionEvaluator::with_trigger_context(&view_schema, database, ctx)
        } else if let Some(ctx) = procedural_context {
            ExpressionEvaluator::with_procedural_context(&view_schema, database, ctx)
        } else {
            ExpressionEvaluator::with_database(&view_schema, database)
        };

        // Select rows matching WHERE clause and build updates
        let mut collected_updates = Vec::new();
        for row in &all_rows.rows {
            let matches = match &stmt.where_clause {
                Some(vibesql_ast::WhereClause::Condition(expr)) => {
                    match evaluator.eval(expr, row)? {
                        vibesql_types::SqlValue::Boolean(b) => b,
                        vibesql_types::SqlValue::Null => false,
                        _ => false,
                    }
                }
                None => true, // No WHERE clause - update all rows
                Some(vibesql_ast::WhereClause::CurrentOf(_)) => {
                    return Err(ExecutorError::UnsupportedExpression(
                        "CURRENT OF not supported for view updates".to_string(),
                    ));
                }
            };

            if matches {
                let old_row = row.clone();

                // Build NEW row by applying assignments
                let mut new_row_values = old_row.values.clone();

                for assignment in &stmt.assignments {
                    // Find column index in view
                    let col_idx = view_schema
                        .columns
                        .iter()
                        .position(|c| c.name.to_uppercase() == assignment.column.to_uppercase())
                        .ok_or_else(|| ExecutorError::ColumnNotFound {
                            column_name: assignment.column.clone(),
                            table_name: view_def.name.clone(),
                            searched_tables: vec![view_def.name.clone()],
                            available_columns: view_schema.columns.iter().map(|c| c.name.clone()).collect(),
                        })?;

                    // Evaluate the new value
                    let new_value = evaluator.eval(&assignment.value, &old_row)?;
                    new_row_values[col_idx] = new_value;
                }

                let new_row = vibesql_storage::Row::new(new_row_values);
                collected_updates.push((old_row, new_row));
            }
        }
        collected_updates
    }; // evaluator dropped here

    // Now fire triggers (database can be mutably borrowed)
    let rows_processed = updates.len();
    for (old_row, new_row) in updates {
        for trigger in &triggers {
            crate::TriggerFirer::execute_trigger(database, trigger, Some(&old_row), Some(&new_row))?;
        }
    }

    Ok(rows_processed)
}

/// Build a pseudo TableSchema from a view definition
fn build_view_schema(
    database: &Database,
    view_def: &vibesql_catalog::ViewDefinition,
) -> Result<vibesql_catalog::TableSchema, ExecutorError> {
    // Execute the view's SELECT query to get column names
    let select_executor = crate::SelectExecutor::new(database);
    let result = select_executor.execute_with_columns(&view_def.query)?;

    // Use explicit column names if provided, otherwise derive from SELECT
    let column_names: Vec<String> = if let Some(ref cols) = view_def.columns {
        cols.clone()
    } else {
        result.columns.clone()
    };

    // Build columns with a generic data type (we just need names for trigger binding)
    let columns: Vec<vibesql_catalog::ColumnSchema> = column_names
        .into_iter()
        .map(|name| {
            vibesql_catalog::ColumnSchema::new(name, vibesql_types::DataType::Varchar { max_length: None }, true)
        })
        .collect();

    Ok(vibesql_catalog::TableSchema::new(view_def.name.clone(), columns))
}

/// Find row indices that would conflict with an updated row (for REPLACE conflict resolution)
/// Returns a list of row indices that have conflicting PK or UNIQUE constraint values
fn find_conflicting_rows_for_update(
    table: &vibesql_storage::Table,
    schema: &vibesql_catalog::TableSchema,
    database: &vibesql_storage::Database,
    table_name: &str,
    new_row: &vibesql_storage::Row,
    current_row_index: usize,
) -> Vec<usize> {
    let mut conflicting_indices = Vec::new();

    // Check PRIMARY KEY conflicts
    if let Some(pk_indices) = schema.get_primary_key_indices() {
        let new_pk_values: Vec<vibesql_types::SqlValue> =
            pk_indices.iter().map(|&idx| new_row.values[idx].clone()).collect();

        // Skip if any PK value is NULL
        if !new_pk_values.contains(&vibesql_types::SqlValue::Null) {
            if let Some(pk_index) = table.primary_key_index() {
                if let Some(&existing_idx) = pk_index.get(&new_pk_values) {
                    // Don't consider the current row as a conflict
                    if existing_idx != current_row_index {
                        conflicting_indices.push(existing_idx);
                    }
                }
            }
        }
    }

    // Check UNIQUE constraint conflicts
    let unique_constraint_indices = schema.get_unique_constraint_indices();
    for (constraint_idx, unique_indices) in unique_constraint_indices.iter().enumerate() {
        let new_unique_values: Vec<vibesql_types::SqlValue> =
            unique_indices.iter().map(|&idx| new_row.values[idx].clone()).collect();

        // Skip if any value is NULL
        if new_unique_values.contains(&vibesql_types::SqlValue::Null) {
            continue;
        }

        let unique_indexes = table.unique_indexes();
        if constraint_idx < unique_indexes.len() {
            if let Some(&existing_idx) = unique_indexes[constraint_idx].get(&new_unique_values) {
                if existing_idx != current_row_index {
                    conflicting_indices.push(existing_idx);
                }
            }
        }
    }

    // Check user-defined UNIQUE indexes
    for index_name in database.list_indexes_for_table(table_name) {
        if let Some(index_metadata) = database.get_index(&index_name) {
            if !index_metadata.unique {
                continue;
            }

            // Build key values for this index
            let mut key_values = Vec::new();
            for index_col in &index_metadata.columns {
                if let Some(col_idx) = schema.get_column_index(&index_col.expect_column_name()) {
                    key_values.push(new_row.values[col_idx].clone());
                }
            }

            // Skip if any value is NULL
            if key_values.contains(&vibesql_types::SqlValue::Null) {
                continue;
            }

            // Check if key exists in index
            if let Some(index_data) = database.get_index_data(&index_name) {
                if let Some(existing_indices) = index_data.get(&key_values) {
                    // Index data returns a Vec of row indices for this key
                    for existing_idx in existing_indices {
                        if existing_idx != current_row_index {
                            conflicting_indices.push(existing_idx);
                        }
                    }
                }
            }
        }
    }

    conflicting_indices
}

/// Validate only NOT NULL and CHECK constraints (for REPLACE conflict resolution)
/// This skips PK and UNIQUE validation since conflicting rows will be deleted
fn validate_non_uniqueness_constraints(
    schema: &vibesql_catalog::TableSchema,
    table_name: &str,
    new_row: &vibesql_storage::Row,
) -> Result<(), ExecutorError> {
    // Check NOT NULL constraints
    for (col_idx, col) in schema.columns.iter().enumerate() {
        let value = new_row
            .get(col_idx)
            .ok_or(ExecutorError::ColumnIndexOutOfBounds { index: col_idx })?;

        if !col.nullable && *value == vibesql_types::SqlValue::Null {
            return Err(ExecutorError::ConstraintViolation(format!(
                "NOT NULL constraint violation: column '{}' in table '{}' cannot be NULL",
                col.name, table_name
            )));
        }
    }

    // Check CHECK constraints
    if !schema.check_constraints.is_empty() {
        let evaluator = crate::evaluator::ExpressionEvaluator::new(schema);

        for (constraint_name, check_expr) in &schema.check_constraints {
            let result = evaluator.eval(check_expr, new_row)?;

            // CHECK constraint passes if result is TRUE or NULL (UNKNOWN)
            // CHECK constraint fails if result is FALSE
            if result == vibesql_types::SqlValue::Boolean(false) {
                return Err(ExecutorError::ConstraintViolation(format!(
                    "CHECK constraint '{}' violated",
                    constraint_name
                )));
            }
        }
    }

    Ok(())
}

/// Validate that multiple updates in the same batch don't produce conflicting
/// PK or UNIQUE constraint values. This ensures SQL's deferred constraint semantics
/// where all rows must satisfy constraints after the entire UPDATE completes.
///
/// This catches cases like `UPDATE t SET pk = 1` when multiple rows are being updated -
/// all rows would end up with the same PK value, violating the UNIQUE constraint.
fn validate_cross_update_uniqueness(
    updates: &[(
        usize,
        vibesql_storage::Row,
        vibesql_storage::Row,
        std::collections::HashSet<usize>,
        bool,
    )],
    schema: &vibesql_catalog::TableSchema,
) -> Result<(), ExecutorError> {
    // Check PRIMARY KEY uniqueness across updates
    if let Some(pk_indices) = schema.get_primary_key_indices() {
        let mut seen_pks: std::collections::HashSet<Vec<vibesql_types::SqlValue>> =
            std::collections::HashSet::new();

        for (_row_index, _old_row, new_row, _changed_columns, _updates_pk) in updates {
            let pk_values: Vec<vibesql_types::SqlValue> =
                pk_indices.iter().map(|&idx| new_row.values[idx].clone()).collect();

            // Skip NULL PKs (they're allowed to have duplicates in the update set
            // since NULL != NULL)
            if pk_values.contains(&vibesql_types::SqlValue::Null) {
                continue;
            }

            if !seen_pks.insert(pk_values.clone()) {
                let pk_col_names: Vec<String> = schema.primary_key.as_ref().unwrap().clone();
                return Err(ExecutorError::ConstraintViolation(format!(
                    "UNIQUE constraint failed: {} (multiple rows would have same key)",
                    pk_col_names.join(", ")
                )));
            }
        }
    }

    // Check UNIQUE constraint uniqueness across updates
    let unique_constraint_indices = schema.get_unique_constraint_indices();
    for (constraint_idx, unique_indices) in unique_constraint_indices.iter().enumerate() {
        let mut seen_values: std::collections::HashSet<Vec<vibesql_types::SqlValue>> =
            std::collections::HashSet::new();

        for (_row_index, _old_row, new_row, _changed_columns, _updates_pk) in updates {
            let unique_values: Vec<vibesql_types::SqlValue> =
                unique_indices.iter().map(|&idx| new_row.values[idx].clone()).collect();

            // Skip if any value is NULL
            if unique_values.contains(&vibesql_types::SqlValue::Null) {
                continue;
            }

            if !seen_values.insert(unique_values.clone()) {
                let unique_col_names: Vec<String> = schema.unique_constraints[constraint_idx].clone();
                return Err(ExecutorError::ConstraintViolation(format!(
                    "UNIQUE constraint failed: {} (multiple rows would have same key)",
                    unique_col_names.join(", ")
                )));
            }
        }
    }

    Ok(())
}

/// For UPDATE OR REPLACE: resolve cross-update conflicts by keeping only the last
/// update for each PK/UNIQUE value. Earlier updates are removed from the updates list
/// and their row indices are returned for deletion.
///
/// This ensures that when multiple rows are updated to the same PK/UNIQUE value,
/// only the last one (in order of processing) survives - matching SQLite's behavior.
fn resolve_cross_update_conflicts_for_replace(
    updates: &mut Vec<(
        usize,
        vibesql_storage::Row,
        vibesql_storage::Row,
        std::collections::HashSet<usize>,
        bool,
    )>,
    schema: &vibesql_catalog::TableSchema,
) -> Vec<usize> {
    let mut indices_to_delete = Vec::new();
    let mut indices_to_remove = std::collections::HashSet::new();

    // Check PRIMARY KEY conflicts
    if let Some(pk_indices) = schema.get_primary_key_indices() {
        // Map: PK values -> (position in updates list, row_index)
        let mut pk_map: std::collections::HashMap<Vec<vibesql_types::SqlValue>, (usize, usize)> =
            std::collections::HashMap::new();

        for (pos, (row_index, _old_row, new_row, _changed_columns, _updates_pk)) in
            updates.iter().enumerate()
        {
            // Skip if already marked for removal
            if indices_to_remove.contains(&pos) {
                continue;
            }

            let pk_values: Vec<vibesql_types::SqlValue> =
                pk_indices.iter().map(|&idx| new_row.values[idx].clone()).collect();

            // Skip NULL PKs
            if pk_values.contains(&vibesql_types::SqlValue::Null) {
                continue;
            }

            if let Some((prev_pos, prev_row_index)) = pk_map.get(&pk_values) {
                // Conflict found - earlier update should be deleted
                indices_to_remove.insert(*prev_pos);
                indices_to_delete.push(*prev_row_index);
            }
            pk_map.insert(pk_values, (pos, *row_index));
        }
    }

    // Check UNIQUE constraint conflicts
    let unique_constraint_indices = schema.get_unique_constraint_indices();
    for unique_indices in unique_constraint_indices.iter() {
        let mut unique_map: std::collections::HashMap<Vec<vibesql_types::SqlValue>, (usize, usize)> =
            std::collections::HashMap::new();

        for (pos, (row_index, _old_row, new_row, _changed_columns, _updates_pk)) in
            updates.iter().enumerate()
        {
            // Skip if already marked for removal
            if indices_to_remove.contains(&pos) {
                continue;
            }

            let unique_values: Vec<vibesql_types::SqlValue> =
                unique_indices.iter().map(|&idx| new_row.values[idx].clone()).collect();

            // Skip if any value is NULL
            if unique_values.contains(&vibesql_types::SqlValue::Null) {
                continue;
            }

            if let Some((prev_pos, prev_row_index)) = unique_map.get(&unique_values) {
                // Conflict found - earlier update should be deleted
                if !indices_to_remove.contains(prev_pos) {
                    indices_to_remove.insert(*prev_pos);
                    indices_to_delete.push(*prev_row_index);
                }
            }
            unique_map.insert(unique_values, (pos, *row_index));
        }
    }

    // Remove conflicting updates from the list (in reverse order to maintain indices)
    let mut remove_positions: Vec<usize> = indices_to_remove.into_iter().collect();
    remove_positions.sort_unstable();
    remove_positions.reverse();
    for pos in remove_positions {
        updates.remove(pos);
    }

    indices_to_delete
}
