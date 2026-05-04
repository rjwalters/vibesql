//! Core UPDATE statement execution logic
//!
//! This module contains the main execution orchestration for UPDATE statements,
//! implementing SQL's two-phase update semantics: first collect all updates
//! evaluating against original rows, then apply all updates atomically.

use std::collections::HashSet;

use vibesql_ast::{Expression, UpdateStmt};
use vibesql_catalog::TableIdentifier;
use vibesql_storage::{statistics::CostEstimator, Database, Row};
use vibesql_types::SqlValue;

use crate::{
    dml_cost::DmlOptimizer, errors::ExecutorError, evaluator::ExpressionEvaluator,
    expression_index_maintenance, privilege_checker::PrivilegeChecker,
    sqlite_schema::is_sqlite_schema_table,
};

use super::{
    constraints::ConstraintValidator,
    fast_path,
    foreign_keys::ForeignKeyValidator,
    from_clause::{apply_update_from_matches, execute_update_from_join},
    index_sync::{
        find_conflicting_rows_for_update, resolve_cross_update_conflicts_for_replace,
        validate_cross_update_uniqueness, validate_post_statement_uniqueness,
    },
    row_selector::RowSelector,
    triggers,
    value_updater::ValueUpdater,
};

/// Internal implementation supporting both schema caching, procedural context, and trigger
/// context
pub(super) fn execute_internal(
    stmt: &UpdateStmt,
    database: &mut Database,
    schema: Option<&vibesql_catalog::TableSchema>,
    procedural_context: Option<&crate::procedural::ExecutionContext>,
    trigger_context: Option<&crate::trigger_execution::TriggerContext>,
) -> Result<usize, ExecutorError> {
    // Check if target is sqlite_master/sqlite_schema (read-only system table)
    if is_sqlite_schema_table(&stmt.table_name) {
        return Err(ExecutorError::SqliteSystemTableReadOnly {
            table_name: stmt.table_name.clone(),
            operation: "modified".to_string(),
        });
    }

    // Check UPDATE privilege on the table
    PrivilegeChecker::check_update(database, &stmt.table_name)?;

    // Check if target is a VIEW with INSTEAD OF triggers
    if let Some(view_def) = database.catalog.get_view(&stmt.table_name).cloned() {
        return triggers::execute_update_on_view(
            database,
            stmt,
            &view_def,
            procedural_context,
            trigger_context,
        );
    }

    // Step 1: Get table schema - clone it to avoid borrow issues
    // We need owned schema because we take mutable references to database later
    // Use TableIdentifier for SQL:1999 case-sensitive lookups when quoted
    // Handle schema-qualified table names (e.g., "temp.t1")
    let table_id = if let Some((schema_part, table_part)) = stmt.table_name.split_once('.') {
        // Schema-qualified name: schema.table
        TableIdentifier::qualified(schema_part, false, table_part, stmt.quoted)
    } else {
        TableIdentifier::new(&stmt.table_name, stmt.quoted)
    };
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
            .get_triggers_for_table(table_name, Some(vibesql_ast::TriggerEvent::Update(None)))
            .next()
            .is_some();

    // Try fast path for simple single-row PK updates without triggers
    // Conditions: no triggers, no procedural context, simple WHERE pk = value, no assertions
    // Skip fast path if assertions exist because we need rollback capability on violation
    // Skip fast path if expression indexes exist because they need maintenance
    let has_assertions = database.catalog.get_all_assertions().next().is_some();
    let has_expression_indexes = database.has_expression_indexes(table_name);
    if !has_triggers
        && procedural_context.is_none()
        && trigger_context.is_none()
        && !has_assertions
        && !has_expression_indexes
    {
        if let Some(result) = fast_path::try_fast_path_update(stmt, database, schema)? {
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

    // Execute CTEs if present (WITH clause support)
    let cte_results = if let Some(ref cte_list) = stmt.with_clause {
        Some(crate::select::cte::execute_ctes(cte_list, |cte_query, prior_ctes| {
            let cte_executor = crate::SelectExecutor::new_with_cte(database, prior_ctes);
            cte_executor.execute(cte_query)
        })?)
    } else {
        None
    };

    // Step 3: Create expression evaluator with database reference for subquery support
    //         and optional procedural/trigger context for variable resolution
    let mut evaluator = if let Some(ctx) = trigger_context {
        // Trigger context takes precedence (trigger statements can't have procedural context)
        ExpressionEvaluator::with_trigger_context(schema, database, ctx)
    } else if let Some(ctx) = procedural_context {
        ExpressionEvaluator::with_procedural_context(schema, database, ctx)
    } else if let Some(ref cte_ctx) = cte_results {
        // Use CTE context for WITH clause support
        ExpressionEvaluator::with_database_and_cte(schema, database, cte_ctx)
    } else {
        ExpressionEvaluator::with_database(schema, database)
    };

    // Set table alias if present (SQLite extension: UPDATE t1 AS xyz SET ...)
    if let Some(ref alias) = stmt.alias {
        evaluator.set_table_alias(alias.clone());
    }

    // Step 3.5: Validate SET expressions BEFORE row selection
    // SQLite validates expressions at preparation time, not execution time.
    // This ensures errors like "no such column: x" are raised even when
    // no rows match the WHERE clause.
    // Note: Skip validation for UPDATE FROM since the SET expressions can reference
    // columns from the FROM tables, which will be validated during synthetic SELECT.
    if stmt.from_clause.is_none() {
        validate_set_expressions(schema, &stmt.assignments, database)?;
    }

    // Step 3.6: Handle UPDATE FROM (multi-table UPDATE) if FROM clause is present
    // This uses a completely different code path that builds a synthetic SELECT
    // to join tables and compute SET values in the joined context.
    if let Some(ref from_clauses) = stmt.from_clause {
        return execute_update_from(
            stmt,
            from_clauses,
            database,
            schema,
            table_name,
            has_triggers,
            &pk_indices,
            trigger_context,
        );
    }

    // Step 4: Select rows to update using RowSelector
    let row_selector = RowSelector::new(schema);
    let candidate_rows = row_selector.select_rows(table, &stmt.where_clause, &mut evaluator)?;

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
    let use_ignore = matches!(stmt.conflict_clause, Some(vibesql_ast::ConflictClause::Ignore));
    let use_replace = matches!(stmt.conflict_clause, Some(vibesql_ast::ConflictClause::Replace));

    // Step 6: Build list of updates (two-phase execution for SQL semantics)
    // Each update consists of: (row_index, old_row, new_row, changed_columns, updates_pk)
    let mut updates: Vec<(usize, Row, Row, HashSet<usize>, bool)> = Vec::new();

    // Track rows to delete for REPLACE conflict resolution (before applying updates)
    let mut rows_to_delete_for_replace: Vec<usize> = Vec::new();

    // Phase C2 of #5085: collect deferred FK violations during the loop
    // and queue them after the loop ends, since the loop body holds an
    // immutable borrow of `database` (via `table`) and queueing requires
    // `&mut database`.
    let mut pending_deferred_violations: Vec<vibesql_storage::DeferredFkViolation> = Vec::new();

    for (row_index, row) in candidate_rows {
        // Clear CSE cache before evaluating assignment expressions for this row
        // to prevent cached column values from previous rows
        evaluator.clear_cse_cache();

        // Apply assignments to build updated row
        let (mut new_row, mut changed_columns) =
            value_updater.apply_assignments(&row, &stmt.assignments)?;

        // Recompute generated columns if any source columns changed
        // Generated columns are defined with AS(expression) syntax and must be updated
        // whenever their dependent columns are modified
        apply_generated_columns_for_update(schema, &mut new_row, &mut changed_columns)?;

        // Check if primary key is being updated
        let updates_pk = if let Some(ref pk_idx) = pk_indices {
            stmt.assignments.iter().any(|a| {
                // Check if this is a rowid assignment
                let col_name_lower = a.column.to_lowercase();
                let is_rowid = col_name_lower == "rowid"
                    || col_name_lower == "_rowid_"
                    || col_name_lower == "oid";

                if is_rowid {
                    // For INTEGER PRIMARY KEY tables, rowid IS the PK
                    // For other tables, rowid is virtual and not in PK
                    schema.rowid_alias_column.is_some()
                } else if let Some(col_index) = schema.get_column_index(&a.column) {
                    pk_idx.contains(&col_index)
                } else {
                    false
                }
            })
        } else {
            false
        };

        // For REPLACE: find and mark conflicting rows for deletion before validation
        if use_replace {
            let conflicting_indices = find_conflicting_rows_for_update(
                table, schema, database, table_name, &new_row, row_index,
            );
            rows_to_delete_for_replace.extend(conflicting_indices);
        }

        // Validate all constraints (NOT NULL, PRIMARY KEY, UNIQUE, CHECK)
        // For IGNORE: catch constraint violations and skip the row
        // For REPLACE: we've already marked conflicts for deletion, so skip PK/UNIQUE validation
        let constraint_validator = ConstraintValidator::new(schema);

        if use_ignore {
            // For IGNORE: try validation and skip row on any constraint violation
            let validation_result =
                constraint_validator.validate_row(table, table_name, row_index, &new_row, &row);
            if validation_result.is_err() {
                continue; // Skip this row
            }

            // Validate user-defined UNIQUE indexes
            let unique_index_result =
                constraint_validator.validate_unique_indexes(database, table_name, &new_row, &row);
            if unique_index_result.is_err() {
                continue; // Skip this row
            }

            // Validate foreign key constraints
            if !schema.foreign_keys.is_empty() {
                match ForeignKeyValidator::collect_constraints(
                    database,
                    table_name,
                    &new_row.values,
                ) {
                    Ok(deferred) => pending_deferred_violations.extend(deferred),
                    Err(_) => continue, // Skip this row
                }
            }
        } else if use_replace {
            // For REPLACE: validate NOT NULL and CHECK constraints, but skip PK/UNIQUE
            // since conflicting rows will be deleted
            validate_non_uniqueness_constraints(schema, table_name, &new_row)?;

            // Validate foreign key constraints
            if !schema.foreign_keys.is_empty() {
                let deferred = ForeignKeyValidator::collect_constraints(
                    database,
                    table_name,
                    &new_row.values,
                )?;
                pending_deferred_violations.extend(deferred);
            }
        } else {
            // Default: validate NOT NULL and CHECK per-row.
            // PRIMARY KEY / UNIQUE checks are deferred to a post-statement pass
            // (see validate_post_statement_uniqueness below). This matches SQLite's
            // deferred UNIQUE semantics — e.g. `UPDATE p SET a = a - 1` must succeed
            // even when intermediate states transiently duplicate keys (issue #5137).
            constraint_validator.validate_row_skip_uniqueness(table_name, &new_row)?;

            // Enforce FOREIGN KEY constraints (child table)
            if !schema.foreign_keys.is_empty() {
                let deferred = ForeignKeyValidator::collect_constraints(
                    database,
                    table_name,
                    &new_row.values,
                )?;
                pending_deferred_violations.extend(deferred);
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

    // Deferred uniqueness check (issue #5137): validate PK / UNIQUE / user-defined unique
    // indexes against the post-statement table state. Rows that are themselves being
    // updated to a different key are excluded from "existing" entries, allowing
    // statements like `UPDATE p SET a = a - 1` to succeed even when intermediate
    // states transiently duplicate keys.
    //
    // Skipped for IGNORE/REPLACE since those modes use per-row validation/resolution.
    if !use_replace && !use_ignore && !updates.is_empty() {
        // Re-borrow the table — `database` may have been mutated above for REPLACE,
        // and we need an immutable read of the current PK/UNIQUE indexes.
        let table_for_check = database
            .get_table(table_name)
            .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?;
        validate_post_statement_uniqueness(
            &updates,
            schema,
            table_for_check,
            database,
            table_name,
        )?;
    }

    // For REPLACE: handle cross-update conflicts by keeping only the last update
    // for each PK/UNIQUE value. Earlier updates with conflicting values are removed
    // from updates and their rows are deleted instead.
    if use_replace && updates.len() > 1 {
        let removed_indices = resolve_cross_update_conflicts_for_replace(&mut updates, schema);
        rows_to_delete_for_replace.extend(removed_indices);
    }

    // For REPLACE: delete conflicting rows before applying updates
    if use_replace && !rows_to_delete_for_replace.is_empty() {
        // De-duplicate and sort
        rows_to_delete_for_replace.sort_unstable();
        rows_to_delete_for_replace.dedup();

        // Filter out any rows that we're going to update (shouldn't delete our own rows)
        let update_indices: HashSet<usize> = updates.iter().map(|(idx, _, _, _, _)| *idx).collect();
        rows_to_delete_for_replace.retain(|idx| !update_indices.contains(idx));

        if !rows_to_delete_for_replace.is_empty() {
            // Get rows for index cleanup
            let rows_for_index: Vec<(usize, Row)> = rows_to_delete_for_replace
                .iter()
                .filter_map(|&idx| table.scan().get(idx).map(|r| (idx, r.clone())))
                .collect();

            // Update indexes before deletion
            let rows_refs: Vec<(usize, &Row)> =
                rows_for_index.iter().map(|(idx, row)| (*idx, row)).collect();
            database.batch_update_indexes_for_delete(table_name, &rows_refs);

            // Maintain expression indexes for each deleted row
            for (row_index, row) in &rows_for_index {
                expression_index_maintenance::maintain_expression_indexes_for_delete(
                    database, table_name, row, *row_index,
                );
            }

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

    // Phase C2 of #5085: push deferred FK violations collected during
    // per-row validation onto the active transaction's queue. The
    // immutable borrows of `database` (via `table`) used during
    // validation have been released by this point.
    for v in pending_deferred_violations {
        database.queue_deferred_fk_violation(v);
    }

    // Step 7: Handle CASCADE updates for primary key changes (before triggers)
    // This must happen after validation but before applying parent updates
    for (_row_index, old_row, new_row, _changed_columns, updates_pk) in &updates {
        if *updates_pk {
            ForeignKeyValidator::check_no_child_references(database, table_name, old_row, new_row)?;
        }
    }

    // Cost-based optimization: Log update cost with indexes_affected_ratio
    if !updates.is_empty() {
        // Compute aggregate changed columns across all updates
        let mut all_changed_columns = HashSet::new();
        for (_, _, _, changed_cols, _) in &updates {
            all_changed_columns.extend(changed_cols.iter().copied());
        }

        let optimizer = DmlOptimizer::new(database, table_name);
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

        // Maintain expression indexes for this update
        expression_index_maintenance::maintain_expression_indexes_for_update(
            database, table_name, &old_row, &new_row, index,
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
                let _ = table_mut.update_row_selective(*index, old_row.clone(), changed_columns);
            }
        }
        // Also invalidate cache after rollback
        database.invalidate_columnar_cache(table_name);
        return Err(assertion_error);
    }

    Ok(update_count)
}

/// Validate only NOT NULL and CHECK constraints (for REPLACE conflict resolution)
/// This skips PK and UNIQUE validation since conflicting rows will be deleted
fn validate_non_uniqueness_constraints(
    schema: &vibesql_catalog::TableSchema,
    table_name: &str,
    new_row: &Row,
) -> Result<(), ExecutorError> {
    // Check NOT NULL constraints
    for (col_idx, col) in schema.columns.iter().enumerate() {
        let value =
            new_row.get(col_idx).ok_or(ExecutorError::ColumnIndexOutOfBounds { index: col_idx })?;

        if !col.nullable && *value == SqlValue::Null {
            return Err(ExecutorError::ConstraintViolation(format!(
                "NOT NULL constraint violation: column '{}' in table '{}' cannot be NULL",
                col.name, table_name
            )));
        }
    }

    // Check CHECK constraints
    if !schema.check_constraints.is_empty() {
        let evaluator = ExpressionEvaluator::new(schema);

        for (constraint_name, check_expr) in &schema.check_constraints {
            let result = evaluator.eval(check_expr, new_row)?;

            // CHECK constraint passes if result is TRUE or NULL (UNKNOWN)
            // CHECK constraint fails if result is FALSE
            if result == SqlValue::Boolean(false) {
                // SQLite-compatible error format: "CHECK constraint failed: <name_or_expr>"
                return Err(ExecutorError::SqliteCompatError(format!(
                    "CHECK constraint failed: {}",
                    constraint_name
                )));
            }
        }
    }

    Ok(())
}

/// Validate SET expressions at preparation time (before row selection).
///
/// SQLite validates expressions when preparing statements, not during execution.
/// This ensures errors like "no such column: x" or "no such function: y" are
/// raised even when no rows would match the WHERE clause.
///
/// This validation walks each assignment's value expression and checks that:
/// - All column references exist in the table schema
/// - All function calls refer to valid functions
fn validate_set_expressions(
    schema: &vibesql_catalog::TableSchema,
    assignments: &[vibesql_ast::Assignment],
    database: &Database,
) -> Result<(), ExecutorError> {
    for assignment in assignments {
        // Validate target column exists (LHS of assignment)
        // Special case: rowid is a virtual column that always exists (SQLite compatibility)
        let col_name_lower = assignment.column.to_lowercase();
        let is_rowid =
            col_name_lower == "rowid" || col_name_lower == "_rowid_" || col_name_lower == "oid";
        if !is_rowid && schema.get_column_index(&assignment.column).is_none() {
            return Err(ExecutorError::NoSuchColumn { column_ref: assignment.column.clone() });
        }

        // Skip DEFAULT keyword - it's always valid in SET context
        if matches!(assignment.value, Expression::Default) {
            continue;
        }

        // Recursively validate the expression (RHS of assignment)
        validate_expression(&assignment.value, schema, database)?;
    }
    Ok(())
}

/// Recursively validate an expression, checking column and function references.
fn validate_expression(
    expr: &Expression,
    schema: &vibesql_catalog::TableSchema,
    database: &Database,
) -> Result<(), ExecutorError> {
    match expr {
        // Column reference - verify it exists in schema
        Expression::ColumnRef(col_id) => {
            let column_name = col_id.column_canonical();

            // Check for ROWID pseudo-columns (always valid)
            let column_lower = column_name.to_lowercase();
            if column_lower == "rowid" || column_lower == "_rowid_" || column_lower == "oid" {
                // ROWID is always valid if there's no real column with that name
                if schema.get_column_index(column_name).is_none() {
                    return Ok(());
                }
            }

            // Check if column exists in schema
            if schema.get_column_index(column_name).is_none() {
                return Err(ExecutorError::NoSuchColumn { column_ref: column_name.to_string() });
            }
            Ok(())
        }

        // Function call - verify the function exists
        Expression::Function { name, args, character_unit } => {
            // Validate the function name
            let func_name = name.display();
            validate_function_exists(&func_name, database)?;

            // Recursively validate all arguments
            for arg in args {
                validate_expression(arg, schema, database)?;
            }

            // character_unit doesn't need validation
            let _ = character_unit;
            Ok(())
        }

        // Binary operations - validate both sides
        Expression::BinaryOp { left, right, .. } => {
            validate_expression(left, schema, database)?;
            validate_expression(right, schema, database)?;
            Ok(())
        }

        // Unary operations - validate the operand
        Expression::UnaryOp { expr, .. } => validate_expression(expr, schema, database),

        // CASE expression - validate all parts
        Expression::Case { operand, when_clauses, else_result } => {
            if let Some(op) = operand {
                validate_expression(op, schema, database)?;
            }
            for case_when in when_clauses {
                for cond in &case_when.conditions {
                    validate_expression(cond, schema, database)?;
                }
                validate_expression(&case_when.result, schema, database)?;
            }
            if let Some(else_expr) = else_result {
                validate_expression(else_expr, schema, database)?;
            }
            Ok(())
        }

        // CAST expression
        Expression::Cast { expr, .. } => validate_expression(expr, schema, database),

        // BETWEEN expression
        Expression::Between { expr, low, high, .. } => {
            validate_expression(expr, schema, database)?;
            validate_expression(low, schema, database)?;
            validate_expression(high, schema, database)?;
            Ok(())
        }

        // IN list
        Expression::InList { expr, values, .. } => {
            validate_expression(expr, schema, database)?;
            for val in values {
                validate_expression(val, schema, database)?;
            }
            Ok(())
        }

        // LIKE/GLOB
        Expression::Like { expr, pattern, .. } | Expression::Glob { expr, pattern, .. } => {
            validate_expression(expr, schema, database)?;
            validate_expression(pattern, schema, database)?;
            Ok(())
        }

        // IS NULL
        Expression::IsNull { expr, .. } => validate_expression(expr, schema, database),

        // Collate
        Expression::Collate { expr, .. } => validate_expression(expr, schema, database),

        // POSITION
        Expression::Position { substring, string, .. } => {
            validate_expression(substring, schema, database)?;
            validate_expression(string, schema, database)?;
            Ok(())
        }

        // TRIM
        Expression::Trim { removal_char, string, .. } => {
            if let Some(rc) = removal_char {
                validate_expression(rc, schema, database)?;
            }
            validate_expression(string, schema, database)?;
            Ok(())
        }

        // EXTRACT
        Expression::Extract { expr, .. } => validate_expression(expr, schema, database),

        // INTERVAL
        Expression::Interval { value, .. } => validate_expression(value, schema, database),

        // Conjunction/Disjunction
        Expression::Conjunction(exprs) | Expression::Disjunction(exprs) => {
            for e in exprs {
                validate_expression(e, schema, database)?;
            }
            Ok(())
        }

        // Expressions that don't need validation (literals, special keywords, etc.)
        Expression::Literal(_)
        | Expression::Default
        | Expression::CurrentDate
        | Expression::CurrentTime { .. }
        | Expression::CurrentTimestamp { .. }
        | Expression::Wildcard => Ok(()),

        // Subqueries - these have their own validation during execution
        Expression::In { .. }
        | Expression::ScalarSubquery(_)
        | Expression::Exists { .. }
        | Expression::QuantifiedComparison { .. } => Ok(()),

        // Other expressions that we don't deeply validate at this stage
        Expression::DuplicateKeyValue { .. }
        | Expression::IsTruthValue { .. }
        | Expression::IsDistinctFrom { .. }
        | Expression::WindowFunction { .. }
        | Expression::AggregateFunction { .. }
        | Expression::NextValue { .. }
        | Expression::MatchAgainst { .. }
        | Expression::PseudoVariable { .. }
        | Expression::SessionVariable { .. }
        | Expression::Placeholder(_)
        | Expression::NumberedPlaceholder(_)
        | Expression::NamedPlaceholder(_)
        | Expression::RowValueConstructor(_) => Ok(()),
    }
}

/// Check if a function name refers to a valid function.
fn validate_function_exists(name: &str, database: &Database) -> Result<(), ExecutorError> {
    let name_upper = name.to_uppercase();

    // Check built-in scalar functions
    static BUILTIN_FUNCTIONS: &[&str] = &[
        // Aggregate functions
        "COUNT",
        "SUM",
        "AVG",
        "MIN",
        "MAX",
        "TOTAL",
        "GROUP_CONCAT",
        // String functions
        "LENGTH",
        "SUBSTR",
        "SUBSTRING",
        "UPPER",
        "LOWER",
        "TRIM",
        "LTRIM",
        "RTRIM",
        "REPLACE",
        "INSTR",
        "PRINTF",
        "QUOTE",
        "HEX",
        "UNHEX",
        "ZEROBLOB",
        "CHAR",
        "UNICODE",
        "GLOB",
        "LIKE",
        "CONCAT",
        "CONCAT_WS",
        "REVERSE",
        "LEFT",
        "RIGHT",
        "LPAD",
        "RPAD",
        "REPEAT",
        "SPACE",
        "SOUNDEX",
        // Math functions
        "ABS",
        "ROUND",
        "RANDOM",
        "SIGN",
        "CEIL",
        "CEILING",
        "FLOOR",
        "TRUNC",
        "MOD",
        "POWER",
        "SQRT",
        "LOG",
        "LOG10",
        "LOG2",
        "LN",
        "EXP",
        "SIN",
        "COS",
        "TAN",
        "ASIN",
        "ACOS",
        "ATAN",
        "ATAN2",
        "DEGREES",
        "RADIANS",
        "PI",
        // Type functions
        "TYPEOF",
        "CAST",
        "COALESCE",
        "IFNULL",
        "NULLIF",
        "IIF",
        "CASE",
        // Date/time functions
        "DATE",
        "TIME",
        "DATETIME",
        "JULIANDAY",
        "STRFTIME",
        "NOW",
        "CURRENT_DATE",
        "CURRENT_TIME",
        "CURRENT_TIMESTAMP",
        "UNIXEPOCH",
        "TIMEDIFF",
        "YEAR",
        "MONTH",
        "DAY",
        "HOUR",
        "MINUTE",
        "SECOND",
        "DAYOFWEEK",
        "DAYOFYEAR",
        "WEEKDAY",
        "WEEK",
        "QUARTER",
        "EXTRACT",
        "DATE_ADD",
        "DATE_SUB",
        "DATEDIFF",
        "TIMESTAMPDIFF",
        "ADDDATE",
        "SUBDATE",
        "MAKEDATE",
        "MAKETIME",
        "STR_TO_DATE",
        "DATE_FORMAT",
        "TIME_FORMAT",
        "LAST_DAY",
        "MONTHNAME",
        "DAYNAME",
        // SQLite-specific
        "LAST_INSERT_ROWID",
        "CHANGES",
        "TOTAL_CHANGES",
        "SQLITE_VERSION",
        "SQLITE_SEARCH_COUNT",
        "SQLITE_SEARCH_COUNT_RESET",
        "INSTR",
        "LIKELY",
        "UNLIKELY",
        "LOAD_EXTENSION",
        "MAX",
        "MIN", // Can be used as scalar functions too
        // Encryption (if enabled)
        "MD5",
        "SHA1",
        "SHA256",
        "SHA384",
        "SHA512",
        // JSON functions
        "JSON",
        "JSON_ARRAY",
        "JSON_OBJECT",
        "JSON_EXTRACT",
        "JSON_QUOTE",
        "JSON_TYPE",
        "JSON_VALID",
        "JSON_ARRAY_LENGTH",
        "JSON_PATCH",
        "JSON_REMOVE",
        "JSON_REPLACE",
        "JSON_SET",
        "JSON_INSERT",
        "JSON_GROUP_ARRAY",
        "JSON_GROUP_OBJECT",
        "JSON_EACH",
        "JSON_TREE",
        // Window functions (also valid as regular aggregates in some contexts)
        "ROW_NUMBER",
        "RANK",
        "DENSE_RANK",
        "NTILE",
        "LAG",
        "LEAD",
        "FIRST_VALUE",
        "LAST_VALUE",
        "NTH_VALUE",
        "CUME_DIST",
        "PERCENT_RANK",
        // Misc
        "ZEROBLOB",
        "RANDOMBLOB",
        "BLOB",
        "OCTET_LENGTH",
        "BIT_LENGTH",
        "POSITION",
        "OVERLAY",
        "SIMILAR",
        "ISNULL",
        "NVL",
        "DECODE",
        "IF",
        "FORMAT",
        "VERSION",
    ];

    if BUILTIN_FUNCTIONS.contains(&name_upper.as_str()) {
        return Ok(());
    }

    // Check user-defined functions in the database
    if database.catalog.get_function(&name_upper).is_some() {
        return Ok(());
    }

    // Function not found
    Err(ExecutorError::NoSuchFunction { function_name: name.to_string() })
}

/// Recompute generated columns after UPDATE assignments are applied.
///
/// Generated columns (defined with AS(expression) syntax) must be recomputed whenever
/// their dependent columns are modified. This function:
/// 1. Evaluates each generated column's expression against the updated row
/// 2. Updates the row with the new computed values
/// 3. Tracks generated columns in changed_columns for index maintenance
fn apply_generated_columns_for_update(
    schema: &vibesql_catalog::TableSchema,
    row: &mut Row,
    changed_columns: &mut HashSet<usize>,
) -> Result<(), ExecutorError> {
    // Check if there are any generated columns
    let has_generated = schema.columns.iter().any(|col| col.generated_expr.is_some());
    if !has_generated {
        return Ok(());
    }

    // Create evaluator with the current row values
    let evaluator = ExpressionEvaluator::new(schema);

    for (col_idx, col) in schema.columns.iter().enumerate() {
        if let Some(generated_expr) = &col.generated_expr {
            // Evaluate the generated expression against the current row
            let generated_value = evaluator.eval(generated_expr, row)?;
            let coerced_value =
                crate::insert::validation::coerce_value(generated_value, &col.data_type)?;

            // Check if the value actually changed
            let old_value = row.get(col_idx);
            let value_changed = old_value != Some(&coerced_value);

            // Update the row with the new generated value
            row.set(col_idx, coerced_value)
                .map_err(|e| ExecutorError::StorageError(e.to_string()))?;

            // Track the generated column as changed for index maintenance
            if value_changed {
                changed_columns.insert(col_idx);
            }
        }
    }

    Ok(())
}

/// Execute UPDATE FROM (multi-table UPDATE) - SQLite 3.33.0+ syntax
///
/// This handles UPDATE statements with FROM clause that join other tables:
/// ```sql
/// UPDATE t1 SET col = t2.val FROM t2 WHERE t1.id = t2.id;
/// ```
///
/// The implementation:
/// 1. Builds a synthetic SELECT joining target table with FROM tables
/// 2. Computes SET expression values in the joined context
/// 3. Applies the pre-computed values to target rows
fn execute_update_from(
    stmt: &UpdateStmt,
    from_clauses: &[vibesql_ast::FromClause],
    database: &mut Database,
    schema: &vibesql_catalog::TableSchema,
    table_name: &str,
    has_triggers: bool,
    pk_indices: &Option<Vec<usize>>,
    trigger_context: Option<&crate::trigger_execution::TriggerContext<'_>>,
) -> Result<usize, ExecutorError> {
    // Execute the join and get matched rows with computed SET values
    // Issue #5082: pass trigger_context so the synthetic SELECT can resolve
    // OLD/NEW pseudo-variables when this UPDATE runs inside a trigger body.
    let join_result =
        execute_update_from_join(stmt, from_clauses, database, schema, trigger_context)?;

    // Fire BEFORE STATEMENT triggers if needed
    if has_triggers {
        crate::TriggerFirer::execute_before_statement_triggers(
            database,
            table_name,
            vibesql_ast::TriggerEvent::Update(None),
        )?;
    }

    // Convert join results to update operations
    let updates = apply_update_from_matches(&join_result.matched_rows, &stmt.assignments, schema)?;

    if updates.is_empty() {
        // Fire AFTER STATEMENT triggers even when no rows matched
        if has_triggers {
            crate::TriggerFirer::execute_after_statement_triggers(
                database,
                table_name,
                vibesql_ast::TriggerEvent::Update(None),
            )?;
        }
        return Ok(0);
    }

    // Validate constraints for each update
    let table = database
        .get_table(table_name)
        .ok_or_else(|| ExecutorError::TableNotFound(table_name.to_string()))?;
    let constraint_validator = ConstraintValidator::new(schema);

    // Phase C2 of #5085: collect deferred FK violations during the loop
    // and queue them after the loop ends, since the loop body holds an
    // immutable borrow of `database` via `table`.
    let mut pending_deferred_violations: Vec<vibesql_storage::DeferredFkViolation> = Vec::new();

    for (row_index, old_row, new_row, _changed_columns, _updates_pk) in &updates {
        // Validate all constraints
        constraint_validator.validate_row(table, table_name, *row_index, new_row, old_row)?;
        constraint_validator.validate_unique_indexes(database, table_name, new_row, old_row)?;

        // Validate foreign key constraints
        if !schema.foreign_keys.is_empty() {
            let deferred =
                ForeignKeyValidator::collect_constraints(database, table_name, &new_row.values)?;
            pending_deferred_violations.extend(deferred);
        }
    }

    // Push deferred FK violations onto the queue now that `table` has
    // been released.
    for v in pending_deferred_violations {
        database.queue_deferred_fk_violation(v);
    }

    // Cross-update uniqueness validation
    if updates.len() > 1 {
        validate_cross_update_uniqueness(&updates, schema)?;
    }

    // Handle CASCADE updates for primary key changes
    for (_row_index, old_row, new_row, _changed_columns, updates_pk) in &updates {
        if *updates_pk {
            ForeignKeyValidator::check_no_child_references(database, table_name, old_row, new_row)?;
        }
    }

    // Fire BEFORE UPDATE triggers
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

    // Apply all updates
    let update_count = updates.len();
    let table_mut = database
        .get_table_mut(table_name)
        .ok_or_else(|| ExecutorError::TableNotFound(table_name.to_string()))?;

    let mut index_updates = Vec::new();
    for (index, old_row, new_row, changed_columns, _updates_pk) in &updates {
        table_mut
            .update_row_selective(*index, new_row.clone(), changed_columns)
            .map_err(|e| ExecutorError::StorageError(e.to_string()))?;

        index_updates.push((*index, old_row.clone(), new_row.clone(), changed_columns.clone()));
    }

    // Fire AFTER UPDATE triggers
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

    // Update indexes
    for (index, old_row, new_row, changed_columns) in index_updates {
        database.update_indexes_for_update(
            table_name,
            &old_row,
            &new_row,
            index,
            Some(&changed_columns),
        );

        expression_index_maintenance::maintain_expression_indexes_for_update(
            database, table_name, &old_row, &new_row, index,
        );
    }

    // Invalidate columnar cache
    if update_count > 0 {
        database.invalidate_columnar_cache(table_name);
    }

    // Fire AFTER STATEMENT triggers
    if has_triggers {
        crate::TriggerFirer::execute_after_statement_triggers(
            database,
            table_name,
            vibesql_ast::TriggerEvent::Update(None),
        )?;
    }

    // Check assertions
    if let Err(assertion_error) =
        crate::advanced_objects::AssertionChecker::check_all_assertions(database)
    {
        return Err(assertion_error);
    }

    // Mark pk_indices as used (it's available for future enhancements)
    let _ = pk_indices;

    Ok(update_count)
}
