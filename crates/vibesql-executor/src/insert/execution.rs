use vibesql_catalog::TableIdentifier;
use vibesql_storage::statistics::CostEstimator;

use crate::{dml_cost::DmlOptimizer, errors::ExecutorError, privilege_checker::PrivilegeChecker};

/// Execute an INSERT statement
/// Returns number of rows inserted
pub fn execute_insert(
    db: &mut vibesql_storage::Database,
    stmt: &vibesql_ast::InsertStmt,
) -> Result<usize, ExecutorError> {
    execute_insert_internal(db, stmt, None, None)
}

/// Execute an INSERT statement with procedural context
/// Returns number of rows inserted
pub fn execute_insert_with_procedural_context(
    db: &mut vibesql_storage::Database,
    stmt: &vibesql_ast::InsertStmt,
    procedural_context: &crate::procedural::ExecutionContext,
) -> Result<usize, ExecutorError> {
    execute_insert_internal(db, stmt, Some(procedural_context), None)
}

/// Execute an INSERT statement with trigger context
/// This allows INSERT statements within trigger bodies to reference OLD/NEW pseudo-variables
/// Returns number of rows inserted
pub fn execute_insert_with_trigger_context(
    db: &mut vibesql_storage::Database,
    stmt: &vibesql_ast::InsertStmt,
    trigger_context: &crate::trigger_execution::TriggerContext,
) -> Result<usize, ExecutorError> {
    execute_insert_internal(db, stmt, None, Some(trigger_context))
}

/// Internal implementation of INSERT execution
fn execute_insert_internal(
    db: &mut vibesql_storage::Database,
    stmt: &vibesql_ast::InsertStmt,
    procedural_context: Option<&crate::procedural::ExecutionContext>,
    trigger_context: Option<&crate::trigger_execution::TriggerContext>,
) -> Result<usize, ExecutorError> {
    // Build full table name for error messages and privilege checks
    let full_table_name = match &stmt.schema_name {
        Some(schema) => format!("{}.{}", schema, stmt.table_name),
        None => stmt.table_name.clone(),
    };

    // Check INSERT privilege on the table
    PrivilegeChecker::check_insert(db, &full_table_name)?;

    // Check if target is a VIEW with INSTEAD OF triggers
    if let Some(view_def) = db.catalog.get_view(&stmt.table_name).cloned() {
        return execute_insert_on_view(db, stmt, &view_def, procedural_context, trigger_context);
    }

    // Get table schema from catalog (clone to avoid borrow issues)
    // Use TableIdentifier for SQL:1999 case-sensitive lookups when quoted
    // For schema-qualified names, use TableIdentifier::qualified to preserve
    // the individual quoted status of schema and table parts
    let table_id = match &stmt.schema_name {
        Some(schema_name) => TableIdentifier::qualified(
            schema_name,
            stmt.schema_quoted,
            &stmt.table_name,
            stmt.table_quoted,
        ),
        None => TableIdentifier::new(&stmt.table_name, stmt.table_quoted),
    };
    let schema = db
        .catalog
        .get_table_by_identifier(&table_id)
        .ok_or_else(|| ExecutorError::TableNotFound(full_table_name.clone()))?
        .clone();

    // Use canonical table name from identifier for storage layer operations
    // For schema-qualified inserts (e.g., INSERT INTO "mySchema"."users"), this produces
    // the full qualified name so storage looks up in the correct schema.
    // For unqualified inserts (e.g., INSERT INTO tab1), this produces just the table name -
    // the storage layer's fallback logic will find it and this matches how indexes
    // are registered (with unqualified table names).
    let storage_table_name = table_id.canonical().to_string();

    // Use the schema's table name for catalog operations (matches how table was created)
    let table_name = &schema.name;

    // Determine target column indices and types, including rowid pseudo-column support
    let resolved_columns =
        super::validation::resolve_target_columns_with_rowid(&schema, table_name, &stmt.columns)?;
    let target_column_info = &resolved_columns.columns;
    let rowid_position = resolved_columns.rowid_position;

    // Get the rows to insert based on the source
    let rows_to_insert = match &stmt.source {
        vibesql_ast::InsertSource::Values(values) => {
            // For VALUES, we already have the rows as expressions
            values.clone()
        }
        vibesql_ast::InsertSource::Select(select_stmt) => {
            // Try bulk transfer optimization first (Phase 1-3)
            // This provides 10-50x performance improvement for compatible schemas
            // Note: bulk transfer doesn't support CTEs, so skip if with_clause is present
            if stmt.columns.is_empty() && stmt.with_clause.is_none() {
                // Only attempt bulk transfer for INSERT INTO table SELECT (no column list)
                if let Some(count) =
                    super::bulk_transfer::try_bulk_transfer(db, table_name, select_stmt)?
                {
                    // Fast path succeeded, return early
                    return Ok(count);
                }
            }

            // Fall back to normal path: execute SELECT and convert to expressions
            // If we have a with_clause (CTEs), execute them first and pass to SelectExecutor
            let select_result = if let Some(ref cte_list) = stmt.with_clause {
                // Execute CTEs first
                let cte_results = crate::select::cte::execute_ctes(cte_list, |cte_query, prior_ctes| {
                    let cte_executor = crate::SelectExecutor::new_with_cte(db, prior_ctes);
                    cte_executor
                        .execute_with_columns(cte_query)
                        .map(|result| result.rows.into_iter().collect())
                })?;

                // Create executor with CTE results
                let select_executor = crate::SelectExecutor::new_with_cte(db, &cte_results);
                select_executor.execute_with_columns(select_stmt)?
            } else {
                let select_executor = crate::SelectExecutor::new(db);
                select_executor.execute_with_columns(select_stmt)?
            };

            // Validate column count
            if select_result.columns.len() != target_column_info.len() {
                // Match SQLite's error message format exactly
                return Err(ExecutorError::InsertColumnCountMismatch {
                    expected: target_column_info.len(),
                    provided: select_result.columns.len(),
                });
            }

            // Convert SelectResult to Vec<Vec<Expression>> format
            // Each row becomes a Vec<Expression> with literals
            select_result
                .rows
                .into_iter()
                .map(|row| row.values.into_iter().map(vibesql_ast::Expression::Literal).collect())
                .collect()
        }
    };

    // Validate each row has correct number of values
    // If rowid is specified, the expected count includes the rowid column
    let expected_value_count = if rowid_position.is_some() {
        target_column_info.len() + 1
    } else {
        target_column_info.len()
    };
    super::validation::validate_row_column_counts(
        &rows_to_insert,
        expected_value_count,
        table_name,
    )?;

    // Estimate DML cost for query analysis and optimization decisions
    // This helps with profiling and can inform future batch size decisions
    if std::env::var("DML_COST_DEBUG").is_ok() {
        if let Some(index_info) = db.get_table_index_info(&storage_table_name) {
            // Get table statistics for cost estimation (use cached if available, or fallback to
            // estimate)
            if let Some(table) = db.get_table(&storage_table_name) {
                let table_stats = table.get_statistics().cloned().unwrap_or_else(|| {
                    vibesql_storage::TableStatistics::estimate_from_row_count(table.row_count())
                });
                let cost_estimator = CostEstimator::default();
                let estimated_cost =
                    cost_estimator.estimate_insert(rows_to_insert.len(), &table_stats, &index_info);
                eprintln!(
                    "DML_COST_DEBUG: INSERT {} rows into {} - estimated_cost: {:.2} (hash_indexes: {}, btree_indexes: {}, columnar: {})",
                    rows_to_insert.len(),
                    stmt.table_name,
                    estimated_cost,
                    index_info.hash_index_count,
                    index_info.btree_index_count,
                    index_info.is_native_columnar
                );
            }
        }
    }

    // For multi-row INSERT, validate all rows first, then insert all
    // This ensures atomicity: all rows succeed or all fail (unless IGNORE is used)
    let mut validated_rows: Vec<(Vec<vibesql_types::SqlValue>, Option<u64>)> = Vec::new();
    let mut primary_key_values: Vec<Vec<vibesql_types::SqlValue>> = Vec::new(); // Track PK values for duplicate checking within batch
    let mut unique_constraint_values = if schema.get_unique_constraint_indices().is_empty() {
        Vec::new()
    } else {
        vec![Vec::new(); schema.get_unique_constraint_indices().len()]
    }; // Track UNIQUE values for each constraint

    // Check if IGNORE conflict clause is set - if so, skip rows with constraint violations
    let use_ignore = matches!(stmt.conflict_clause, Some(vibesql_ast::ConflictClause::Ignore));

    // Track the first auto-generated ID for LAST_INSERT_ROWID() support
    // Per MySQL semantics, for multi-row inserts, LAST_INSERT_ID() returns
    // the first auto-generated value, not the last
    let mut first_generated_id: Option<i64> = None;

    // Track the maximum INTEGER PRIMARY KEY value assigned within this batch
    // to handle multi-row INSERTs with NULL values correctly (SQLite semantics)
    let mut batch_max_ipk: Option<i64> = None;

    // Get INTEGER PRIMARY KEY column index if present
    let ipk_col_idx = schema.get_integer_primary_key_index();

    for value_exprs in &rows_to_insert {
        // Build a complete row with values for all columns
        // Start with NULL for all columns, then fill in provided values
        let mut full_row_values = vec![vibesql_types::SqlValue::Null; schema.columns.len()];

        // Extract rowid value if present (SQLite compatibility)
        let explicit_rowid = if let Some(rowid_pos) = rowid_position {
            // Get the rowid expression
            let rowid_expr = &value_exprs[rowid_pos];

            // Extract literal value from expression
            match rowid_expr {
                vibesql_ast::Expression::Literal(val) => {
                    // Convert to u64 for row_id
                    match val {
                        vibesql_types::SqlValue::Integer(i) if *i > 0 => Some(*i as u64),
                        vibesql_types::SqlValue::Bigint(i) if *i > 0 => Some(*i as u64),
                        vibesql_types::SqlValue::Null => None, // NULL rowid means auto-assign
                        _ => {
                            return Err(ExecutorError::UnsupportedExpression(
                                "ROWID must be a positive integer".to_string(),
                            ));
                        }
                    }
                }
                _ => {
                    return Err(ExecutorError::UnsupportedExpression(
                        "ROWID value must be a literal integer".to_string(),
                    ));
                }
            }
        } else {
            None
        };

        // Filter out the rowid value when iterating over column values
        let column_values: Vec<_> = if let Some(rowid_pos) = rowid_position {
            value_exprs
                .iter()
                .enumerate()
                .filter(|(idx, _)| *idx != rowid_pos)
                .map(|(_, expr)| expr)
                .collect()
        } else {
            value_exprs.iter().collect()
        };

        for (expr, (col_idx, data_type)) in column_values.iter().zip(target_column_info.iter()) {
            // Evaluate expression (literals, DEFAULT, procedural variables, and trigger
            // pseudo-variables)
            let value = super::defaults::evaluate_insert_expression_with_trigger_context(
                expr,
                &schema.columns[*col_idx],
                procedural_context,
                trigger_context,
                Some(db),
            )?;

            // Type check and coerce: ensure value matches column type
            let coerced_value = super::validation::coerce_value(value, data_type)?;

            full_row_values[*col_idx] = coerced_value;
        }

        // Apply DEFAULT values for unspecified columns
        // This returns the first generated sequence value (if any)
        // Use storage_table_name for correct table lookup (handles schema-qualified tables)
        // Pass batch_max_ipk to handle multi-row INSERTs with NULL INTEGER PRIMARY KEY
        let generated_id = super::defaults::apply_default_values_with_batch_context(
            &schema,
            &mut full_row_values,
            db,
            &storage_table_name,
            batch_max_ipk,
        )?;

        // Apply generated/computed column values (AS(expression) syntax)
        super::defaults::apply_generated_columns(&schema, &mut full_row_values, db)?;

        // Track the first generated ID across all rows
        if first_generated_id.is_none() {
            first_generated_id = generated_id;
        }

        // Update batch_max_ipk if this row has an INTEGER PRIMARY KEY value
        if let Some(idx) = ipk_col_idx {
            if let Some(vibesql_types::SqlValue::Integer(val)) = full_row_values.get(idx) {
                batch_max_ipk = Some(batch_max_ipk.map_or(*val, |prev| prev.max(*val)));
            }
        }

        // Validate all constraints in a single pass and extract index keys
        // Skip PK/UNIQUE duplicate checks if using REPLACE conflict clause or ON DUPLICATE KEY
        // UPDATE. Also skip for IGNORE since we'll handle violations by skipping the row.
        let skip_duplicate_checks =
            matches!(stmt.conflict_clause, Some(vibesql_ast::ConflictClause::Replace))
                || matches!(stmt.conflict_clause, Some(vibesql_ast::ConflictClause::Ignore))
                || stmt.on_duplicate_key_update.is_some();
        let validator = super::row_validator::RowValidator::new(
            db,
            &schema,
            &storage_table_name,
            &primary_key_values,
            &unique_constraint_values,
            skip_duplicate_checks,
        );

        // For IGNORE, we need to check for constraint violations before adding to validated_rows
        // If there's a violation, skip this row instead of returning an error
        if use_ignore {
            // Check if this row would violate any constraints
            let would_violate = check_would_violate_constraints(
                db,
                &schema,
                &storage_table_name,
                &full_row_values,
                &primary_key_values,
                &unique_constraint_values,
            );
            if would_violate {
                // Skip this row - don't add to validated_rows
                continue;
            }
        }

        let validation_result = validator.validate(&full_row_values)?;

        // Track PK values for batch duplicate checking (using pre-extracted keys)
        if let Some(pk_values) = validation_result.primary_key {
            primary_key_values.push(pk_values);
        }

        // Track UNIQUE values for batch duplicate checking (using pre-extracted keys)
        for (constraint_idx, unique_values) in validation_result.unique_keys.into_iter().enumerate()
        {
            if let Some(values) = unique_values {
                unique_constraint_values[constraint_idx].push(values);
            }
        }

        // Store validated row for insertion (with optional explicit rowid)
        validated_rows.push((full_row_values, explicit_rowid));
    }

    // All rows validated successfully, now insert them

    // Check once if any INSERT triggers exist for this table (used for batch optimization)
    let has_insert_triggers = db
        .catalog
        .get_triggers_for_table(table_name, Some(vibesql_ast::TriggerEvent::Insert))
        .next()
        .is_some();

    // Fire BEFORE STATEMENT triggers only if triggers exist AND we're not inside a trigger context
    // (Statement-level triggers don't fire for inserts within trigger bodies)
    if has_insert_triggers && trigger_context.is_none() {
        crate::TriggerFirer::execute_before_statement_triggers(
            db,
            table_name,
            vibesql_ast::TriggerEvent::Insert,
        )?;
    }

    let mut rows_inserted = 0;

    // Check if any assertions exist - needed for rollback support
    let has_assertions = db.catalog.get_all_assertions().next().is_some();

    // Track row count before inserts for assertion rollback
    let row_count_before_all = if has_assertions {
        Some(
            db.get_table(&storage_table_name)
                .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?
                .row_count(),
        )
    } else {
        None
    };

    let use_batch_insert = stmt.on_duplicate_key_update.is_none()
        && !matches!(stmt.conflict_clause, Some(vibesql_ast::ConflictClause::Replace))
        && !has_insert_triggers;

    // Helper to create a Row with optional explicit rowid
    let make_row = |(values, rowid): (Vec<vibesql_types::SqlValue>, Option<u64>)| match rowid {
        Some(id) => vibesql_storage::Row::with_row_id(values, id),
        None => vibesql_storage::Row::new(values),
    };

    if use_batch_insert && validated_rows.len() > 1 {
        // Fast path: Use batch insert for multiple rows without triggers
        // Use cost-based batch sizing to optimize for tables with many indexes
        let optimizer = DmlOptimizer::new(db, table_name);
        let optimal_batch_size = optimizer.optimal_insert_batch_size(validated_rows.len());

        // If optimal batch size is smaller than total rows, insert in batches
        if optimal_batch_size < validated_rows.len() {
            // Chunked batch insert for high-cost tables
            for chunk in validated_rows.chunks(optimal_batch_size) {
                let rows: Vec<vibesql_storage::Row> =
                    chunk.iter().map(|(v, rowid)| make_row((v.clone(), *rowid))).collect();

                rows_inserted += db.insert_rows_batch(&storage_table_name, rows).map_err(|e| {
                    ExecutorError::UnsupportedExpression(format!("Storage error: {}", e))
                })?;
            }
        } else {
            // Single batch insert for low-cost tables
            let rows: Vec<vibesql_storage::Row> =
                validated_rows.into_iter().map(make_row).collect();

            rows_inserted = db.insert_rows_batch(&storage_table_name, rows).map_err(|e| {
                ExecutorError::UnsupportedExpression(format!("Storage error: {}", e))
            })?;
        }
    } else {
        // Slow path: Insert rows one by one (needed for triggers, special clauses)
        for (full_row_values, explicit_rowid) in validated_rows {
            // Check if ON DUPLICATE KEY UPDATE is specified
            if let Some(ref assignments) = stmt.on_duplicate_key_update {
                // Try to update an existing row if there's a conflict
                let update_result = super::duplicate_key_update::handle_duplicate_key_update(
                    db,
                    table_name,
                    &schema,
                    &full_row_values,
                    assignments,
                )?;

                if update_result.is_some() {
                    // Row was updated, count it
                    rows_inserted += 1;
                    continue;
                }
                // No conflict, fall through to insert
            } else if matches!(stmt.conflict_clause, Some(vibesql_ast::ConflictClause::Replace)) {
                // If REPLACE conflict clause, delete conflicting rows first
                super::replace::handle_replace_conflicts(
                    db,
                    table_name,
                    &schema,
                    &full_row_values,
                )?;
            }

            // Fire BEFORE INSERT triggers only if triggers exist
            let row_to_insert = make_row((full_row_values.clone(), explicit_rowid));
            if has_insert_triggers {
                crate::TriggerFirer::execute_before_triggers(
                    db,
                    table_name,
                    vibesql_ast::TriggerEvent::Insert,
                    None,
                    Some(&row_to_insert),
                )?;
            }

            // Get row count before insert to enable rollback
            let row_count_before = db
                .get_table(&storage_table_name)
                .ok_or_else(|| ExecutorError::TableNotFound(full_table_name.clone()))?
                .row_count();

            // Insert the row
            let row = make_row((full_row_values, explicit_rowid));
            db.insert_row(&storage_table_name, row.clone()).map_err(|e| {
                ExecutorError::UnsupportedExpression(format!("Storage error: {}", e))
            })?;

            // Fire AFTER INSERT triggers only if triggers exist
            // If AFTER triggers fail, we need to rollback the insert
            if has_insert_triggers {
                let trigger_result = crate::TriggerFirer::execute_after_triggers(
                    db,
                    table_name,
                    vibesql_ast::TriggerEvent::Insert,
                    None,
                    Some(&row),
                );

                if let Err(trigger_error) = trigger_result {
                    // Rollback: Delete the row we just inserted
                    // Note: This is a simple rollback mechanism for Phase 3
                    // Full transaction support will come in a later phase
                    let table = db
                        .get_table_mut(&storage_table_name)
                        .ok_or_else(|| ExecutorError::TableNotFound(full_table_name.clone()))?;

                    // Delete the last row (the one we just inserted)
                    // Row was inserted at index row_count_before
                    use std::cell::Cell;
                    let current_index = Cell::new(0);
                    let target_index = row_count_before;
                    // Ignore delete_result since we unconditionally rebuild indexes below
                    let _ = table.delete_where(|_row| {
                        let index = current_index.get();
                        current_index.set(index + 1);
                        index == target_index
                    });

                    // Rebuild indexes since we modified the table (handles compaction)
                    db.rebuild_indexes(&storage_table_name);

                    // Re-throw the trigger error
                    return Err(trigger_error);
                }
            }

            rows_inserted += 1;
        }
    }

    // Fire AFTER STATEMENT triggers only if triggers exist AND we're not inside a trigger context
    // (Statement-level triggers don't fire for inserts within trigger bodies)
    if has_insert_triggers && trigger_context.is_none() {
        crate::TriggerFirer::execute_after_statement_triggers(
            db,
            table_name,
            vibesql_ast::TriggerEvent::Insert,
        )?;
    }

    // Update LAST_INSERT_ROWID if any auto-generated values were produced
    if let Some(id) = first_generated_id {
        db.set_last_insert_rowid(id);
    }

    // Invalidate the database-level columnar cache since table data changed.
    // Note: The table-level cache is already invalidated by insert_row()/insert_rows_batch().
    // Both invalidations are necessary because they manage separate caches:
    // - Table-level cache: used by Table::scan_columnar() for SIMD filtering
    // - Database-level cache: used by Database::get_columnar() for cached access
    if rows_inserted > 0 {
        db.invalidate_columnar_cache(&storage_table_name);
    }

    // Check all assertions after INSERT completes (SQL:1999 Feature F671/F672)
    // This ensures database-wide integrity constraints are maintained
    if let Err(assertion_error) =
        crate::advanced_objects::AssertionChecker::check_all_assertions(db)
    {
        // Rollback: Delete the rows we just inserted
        if let Some(start_index) = row_count_before_all {
            if rows_inserted > 0 {
                // Delete rows starting from start_index (the rows we inserted)
                if let Some(table_mut) = db.get_table_mut(&storage_table_name) {
                    use std::cell::Cell;
                    let current_index = Cell::new(0);
                    // Delete all rows from start_index onwards (the newly inserted rows)
                    let _ = table_mut.delete_where(|_row| {
                        let index = current_index.get();
                        current_index.set(index + 1);
                        index >= start_index
                    });
                }

                // Rebuild indexes since we modified the table (handles compaction)
                db.rebuild_indexes(&storage_table_name);
                db.invalidate_columnar_cache(&storage_table_name);
            }
        }
        return Err(assertion_error);
    }

    Ok(rows_inserted)
}

/// Check if inserting a row would violate any constraints (for IGNORE conflict resolution)
/// Returns true if any constraint would be violated
fn check_would_violate_constraints(
    db: &vibesql_storage::Database,
    schema: &vibesql_catalog::TableSchema,
    table_name: &str,
    row_values: &[vibesql_types::SqlValue],
    batch_pk_values: &[Vec<vibesql_types::SqlValue>],
    batch_unique_values: &[Vec<Vec<vibesql_types::SqlValue>>],
) -> bool {
    // Check NOT NULL constraints
    for (col_idx, col) in schema.columns.iter().enumerate() {
        if !col.nullable && row_values[col_idx] == vibesql_types::SqlValue::Null {
            return true;
        }
    }

    // Check PRIMARY KEY uniqueness
    if let Some(pk_indices) = schema.get_primary_key_indices() {
        let new_pk_values: Vec<vibesql_types::SqlValue> =
            pk_indices.iter().map(|&idx| row_values[idx].clone()).collect();

        // Skip if any PK value is NULL (multiple NULLs are allowed for non-INTEGER PRIMARY KEY)
        if !new_pk_values.contains(&vibesql_types::SqlValue::Null) {
            // Check against batch
            if batch_pk_values.contains(&new_pk_values) {
                return true;
            }

            // Check against existing table data
            if let Some(table) = db.get_table(table_name) {
                if let Some(pk_index) = table.primary_key_index() {
                    if pk_index.contains_key(&new_pk_values) {
                        return true;
                    }
                }
            }
        }
    }

    // Check UNIQUE constraints
    let unique_constraint_indices = schema.get_unique_constraint_indices();
    for (constraint_idx, unique_indices) in unique_constraint_indices.iter().enumerate() {
        let new_unique_values: Vec<vibesql_types::SqlValue> =
            unique_indices.iter().map(|&idx| row_values[idx].clone()).collect();

        // Skip if any value is NULL
        if new_unique_values.contains(&vibesql_types::SqlValue::Null) {
            continue;
        }

        // Check against batch
        if constraint_idx < batch_unique_values.len()
            && batch_unique_values[constraint_idx].contains(&new_unique_values)
        {
            return true;
        }

        // Check against existing table data
        if let Some(table) = db.get_table(table_name) {
            let unique_indexes = table.unique_indexes();
            if constraint_idx < unique_indexes.len()
                && unique_indexes[constraint_idx].contains_key(&new_unique_values)
            {
                return true;
            }
        }
    }

    // Check user-defined UNIQUE indexes
    if let Some(table) = db.get_table(table_name) {
        for index_name in db.list_indexes_for_table(table_name) {
            if let Some(index_metadata) = db.get_index(&index_name) {
                if !index_metadata.unique {
                    continue;
                }

                // Build key values for this index
                let mut key_values = Vec::new();
                for index_col in &index_metadata.columns {
                    if let Some(col_idx) = schema.get_column_index(index_col.expect_column_name()) {
                        key_values.push(row_values[col_idx].clone());
                    }
                }

                // Skip if any value is NULL
                if key_values.contains(&vibesql_types::SqlValue::Null) {
                    continue;
                }

                // Check if key exists in index
                if let Some(index_data) = db.get_index_data(&index_name) {
                    if index_data.contains_key(&key_values) {
                        return true;
                    }
                }
            }
        }
        // Use `table` to suppress the unused variable warning in the `let Some(table)` pattern.
        // This is a read-only check, so we just need to ensure the table exists.
        let _ = table.row_count();
    }

    // Check CHECK constraints
    if !schema.check_constraints.is_empty() {
        let row = vibesql_storage::Row::new(row_values.to_vec());
        let evaluator = crate::evaluator::ExpressionEvaluator::new(schema);

        for (_constraint_name, check_expr) in &schema.check_constraints {
            if let Ok(result) = evaluator.eval(check_expr, &row) {
                if result == vibesql_types::SqlValue::Boolean(false) {
                    return true;
                }
            }
        }
    }

    // Check FOREIGN KEY constraints
    for fk in &schema.foreign_keys {
        let fk_values: Vec<vibesql_types::SqlValue> =
            fk.column_indices.iter().map(|&idx| row_values[idx].clone()).collect();

        // Skip if any FK value is NULL
        if fk_values.iter().any(|v| v.is_null()) {
            continue;
        }

        // Check if referenced key exists in parent table
        if let Some(parent_table) = db.get_table(&fk.parent_table) {
            let key_exists = parent_table.scan().iter().any(|parent_row| {
                fk.parent_column_indices
                    .iter()
                    .zip(&fk_values)
                    .all(|(&parent_idx, fk_val)| parent_row.get(parent_idx) == Some(fk_val))
            });

            if !key_exists {
                return true;
            }
        }
    }

    false
}

/// Execute INSERT on a VIEW using INSTEAD OF triggers
///
/// When inserting into a view, we need to fire INSTEAD OF INSERT triggers
/// instead of actually inserting data. The triggers typically insert into
/// the underlying tables.
fn execute_insert_on_view(
    db: &mut vibesql_storage::Database,
    stmt: &vibesql_ast::InsertStmt,
    view_def: &vibesql_catalog::ViewDefinition,
    procedural_context: Option<&crate::procedural::ExecutionContext>,
    trigger_context: Option<&crate::trigger_execution::TriggerContext>,
) -> Result<usize, ExecutorError> {
    use vibesql_ast::TriggerTiming;

    // Find INSTEAD OF INSERT triggers for this view
    let triggers = crate::TriggerFirer::find_triggers(
        db,
        &view_def.name,
        TriggerTiming::InsteadOf,
        vibesql_ast::TriggerEvent::Insert,
    );

    if triggers.is_empty() {
        return Err(ExecutorError::UnsupportedExpression(format!(
            "Cannot INSERT into view '{}' without INSTEAD OF trigger",
            view_def.name
        )));
    }

    // Build a pseudo-schema for the view to evaluate values and resolve column names
    // We derive column info from the view's SELECT query
    let view_schema = build_view_schema(db, view_def)?;

    // Get the rows to insert based on the source
    let rows_to_insert = match &stmt.source {
        vibesql_ast::InsertSource::Values(values) => values.clone(),
        vibesql_ast::InsertSource::Select(select_stmt) => {
            // Execute SELECT and convert to expressions
            let select_executor = crate::SelectExecutor::new(db);
            let select_result = select_executor.execute_with_columns(select_stmt)?;
            select_result
                .rows
                .into_iter()
                .map(|row| row.values.into_iter().map(vibesql_ast::Expression::Literal).collect())
                .collect()
        }
    };

    // Determine target column indices from the statement's column list
    // If no columns specified, use all view columns in order
    let target_columns: Vec<(usize, &vibesql_catalog::ColumnSchema)> = if stmt.columns.is_empty() {
        view_schema.columns.iter().enumerate().collect()
    } else {
        stmt.columns
            .iter()
            .map(|col_name| {
                view_schema
                    .columns
                    .iter()
                    .enumerate()
                    .find(|(_, c)| c.name.to_uppercase() == col_name.to_uppercase())
                    .ok_or_else(|| ExecutorError::ColumnNotFound {
                        column_name: col_name.clone(),
                        table_name: view_def.name.clone(),
                        searched_tables: vec![view_def.name.clone()],
                        available_columns: view_schema
                            .columns
                            .iter()
                            .map(|c| c.name.clone())
                            .collect(),
                    })
            })
            .collect::<Result<Vec<_>, _>>()?
    };

    // Collect all new rows first, before firing triggers
    // This avoids borrow conflicts with the evaluator
    let new_rows: Vec<vibesql_storage::Row> = {
        let dummy_row = vibesql_storage::Row::new(vec![]);
        let evaluator = if let Some(ctx) = trigger_context {
            crate::evaluator::ExpressionEvaluator::with_trigger_context(&view_schema, db, ctx)
        } else if let Some(ctx) = procedural_context {
            crate::evaluator::ExpressionEvaluator::with_procedural_context(&view_schema, db, ctx)
        } else {
            crate::evaluator::ExpressionEvaluator::with_database(&view_schema, db)
        };

        let mut collected_rows = Vec::new();
        for value_exprs in &rows_to_insert {
            // Validate column count
            if value_exprs.len() != target_columns.len() {
                return Err(ExecutorError::InsertColumnCountMismatch {
                    expected: target_columns.len(),
                    provided: value_exprs.len(),
                });
            }

            // Build a row with values for all view columns
            let mut row_values = vec![vibesql_types::SqlValue::Null; view_schema.columns.len()];

            for (expr, (col_idx, _col)) in value_exprs.iter().zip(target_columns.iter()) {
                // Evaluate expression - for INSERT, these are typically literals
                let value = evaluator.eval(expr, &dummy_row)?;
                row_values[*col_idx] = value;
            }

            collected_rows.push(vibesql_storage::Row::new(row_values));
        }
        collected_rows
    }; // evaluator dropped here

    // Now fire triggers (database can be mutably borrowed)
    let rows_processed = new_rows.len();
    for row in new_rows {
        for trigger in &triggers {
            crate::TriggerFirer::execute_trigger(db, trigger, None, Some(&row))?;
        }
    }

    Ok(rows_processed)
}

/// Build a pseudo TableSchema from a view definition
fn build_view_schema(
    db: &vibesql_storage::Database,
    view_def: &vibesql_catalog::ViewDefinition,
) -> Result<vibesql_catalog::TableSchema, ExecutorError> {
    // Execute the view's SELECT query to get column names
    let select_executor = crate::SelectExecutor::new(db);
    let result = select_executor.execute_with_columns(&view_def.query)?;

    // Use explicit column names if provided, otherwise derive from SELECT
    let column_names: Vec<String> =
        if let Some(ref cols) = view_def.columns { cols.clone() } else { result.columns.clone() };

    // Build columns with a generic data type (we just need names for trigger binding)
    let columns: Vec<vibesql_catalog::ColumnSchema> = column_names
        .into_iter()
        .map(|name| {
            vibesql_catalog::ColumnSchema::new(
                name,
                vibesql_types::DataType::Varchar { max_length: None },
                true,
            )
        })
        .collect();

    Ok(vibesql_catalog::TableSchema::new(view_def.name.clone(), columns))
}
