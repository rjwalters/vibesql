//! DELETE statement execution

use vibesql_ast::DeleteStmt;
use vibesql_catalog::TableIdentifier;
use vibesql_storage::Database;
use vibesql_types::SqlValue;

use super::integrity::check_no_child_references;
use crate::{
    dml_cost::DmlOptimizer,
    errors::ExecutorError,
    evaluator::{coercion::coerce_value_to_column_type, ExpressionEvaluator},
    expression_index_maintenance, partial_index_maintenance,
    privilege_checker::PrivilegeChecker,
    sqlite_schema::is_sqlite_schema_table,
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
    ///     vibesql_storage::Row::new(vec![
    ///         SqlValue::Integer(1),
    ///         SqlValue::Varchar(arcstr::ArcStr::from("Alice")),
    ///     ]),
    /// )
    /// .unwrap();
    /// db.insert_row(
    ///     "users",
    ///     vibesql_storage::Row::new(vec![
    ///         SqlValue::Integer(2),
    ///         SqlValue::Varchar(arcstr::ArcStr::from("Bob")),
    ///     ]),
    /// )
    /// .unwrap();
    ///
    /// // Delete specific row
    /// let stmt = DeleteStmt {
    ///     with_clause: None,
    ///     only: false,
    ///     table_name: "users".to_string(),
    ///     quoted: false,
    ///     where_clause: Some(WhereClause::Condition(Expression::BinaryOp {
    ///         left: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple("id", false))),
    ///         op: BinaryOperator::Equal,
    ///         right: Box::new(Expression::Literal(SqlValue::Integer(1))),
    ///     })),
    ///     order_by: None,
    ///     limit: None,
    ///     offset: None,
    ///     returning: None,
    /// };
    ///
    /// let count = DeleteExecutor::execute(&stmt, &mut db).unwrap();
    /// assert_eq!(count, 1);
    /// ```
    pub fn execute(stmt: &DeleteStmt, database: &mut Database) -> Result<usize, ExecutorError> {
        // SQLite per-variant RAISE scope handling for the top-level statement
        // (#5417): see [`crate::raise_scope::run_top_level_dml`].
        let may_fire = crate::raise_scope::table_may_fire_trigger(database, &stmt.table_name);
        crate::raise_scope::run_top_level_dml(database, may_fire, |database| {
            Self::execute_internal(stmt, database, None, None).map(|(count, _)| count)
        })
    }

    /// Execute a DELETE statement, capturing RETURNING rows (SQLite 3.35.0+)
    ///
    /// Returns the number of deleted rows plus, when the statement carries a
    /// RETURNING clause, the projected OLD rows (values before deletion) —
    /// one per deleted row, or one per INSTEAD OF trigger fire for views.
    ///
    /// When the statement has no RETURNING clause the second element is `None`.
    pub fn execute_returning(
        stmt: &DeleteStmt,
        database: &mut Database,
    ) -> Result<(usize, Option<crate::select::SelectResult>), ExecutorError> {
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
            .map(|(count, _)| count)
    }

    /// Execute a DELETE statement with trigger context
    /// This allows DELETE statements within trigger bodies to reference OLD/NEW pseudo-variables
    pub fn execute_with_trigger_context(
        stmt: &DeleteStmt,
        database: &mut Database,
        trigger_context: &crate::trigger_execution::TriggerContext,
    ) -> Result<usize, ExecutorError> {
        Self::execute_internal(stmt, database, None, Some(trigger_context)).map(|(count, _)| count)
    }

    /// Internal implementation supporting procedural context and trigger context
    fn execute_internal(
        stmt: &DeleteStmt,
        database: &mut Database,
        procedural_context: Option<&crate::procedural::ExecutionContext>,
        trigger_context: Option<&crate::trigger_execution::TriggerContext>,
    ) -> Result<(usize, Option<crate::select::SelectResult>), ExecutorError> {
        // Note: stmt.only is currently ignored (treated as false)
        // ONLY keyword is used in table inheritance to exclude derived tables.
        // Since table inheritance is not yet implemented, we treat all deletes the same.

        // Check if target is sqlite_master/sqlite_schema (read-only system table)
        if is_sqlite_schema_table(&stmt.table_name) {
            return Err(ExecutorError::SqliteSystemTableReadOnly {
                table_name: stmt.table_name.clone(),
                operation: "modified".to_string(),
            });
        }

        // Check DELETE privilege on the table
        PrivilegeChecker::check_delete(database, &stmt.table_name)?;

        // Check if target is a VIEW with INSTEAD OF triggers
        if let Some(view_def) = database.catalog.get_view(&stmt.table_name).cloned() {
            return execute_delete_on_view(
                database,
                stmt,
                &view_def,
                procedural_context,
                trigger_context,
            );
        }

        // Use TableIdentifier for SQL:1999 case-sensitive lookups when quoted
        // Handle schema-qualified table names (e.g., "temp.t1")
        let table_id = if let Some((schema_part, table_part)) = stmt.table_name.split_once('.') {
            // Schema-qualified name: schema.table
            TableIdentifier::qualified(schema_part, false, table_part, stmt.quoted)
        } else {
            TableIdentifier::new(&stmt.table_name, stmt.quoted)
        };

        // Check table exists
        if !database.catalog.table_exists_by_identifier(&table_id) {
            return Err(ExecutorError::TableNotFound(stmt.table_name.clone()));
        }

        // Fast path: DELETE FROM table (no WHERE clause)
        // Use TRUNCATE-style optimization for 100-1000x performance improvement
        // Only use truncate if there's no ORDER BY or LIMIT (which would restrict which rows to
        // delete). RETURNING needs the standard scan path so the OLD rows can be captured.
        if stmt.where_clause.is_none()
            && stmt.order_by.is_none()
            && stmt.limit.is_none()
            && stmt.returning.is_none()
            && can_use_truncate(database, &stmt.table_name)?
        {
            return execute_truncate(database, &stmt.table_name).map(|count| (count, None));
        }

        // Step 1: Get schema (clone to avoid borrow issues)
        let schema = database
            .catalog
            .get_table_by_identifier(&table_id)
            .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?
            .clone();

        // Use canonical table name from schema for all storage operations
        // This ensures case-sensitive tables (quoted identifiers) are accessed correctly
        let table_name = &schema.name;

        // Fast path: Single-row PK delete without triggers/FKs
        // This avoids ExpressionEvaluator creation and row cloning
        // RETURNING needs the standard path so the OLD row can be captured
        if procedural_context.is_none() && trigger_context.is_none() && stmt.returning.is_none() {
            if let Some(vibesql_ast::WhereClause::Condition(where_expr)) = &stmt.where_clause {
                if let Some(pk_values) = Self::extract_primary_key_lookup(where_expr, &schema) {
                    // Coerce extracted WHERE-clause literals to match the PK
                    // column affinities. The PK index HashMap is keyed on
                    // stored (already-coerced) values, so a raw literal can
                    // silently miss when types differ — e.g. `WHERE p=1200`
                    // on a TEXT PRIMARY KEY storing "1200". See issue #5145.
                    let pk_values: Vec<SqlValue> =
                        if let Some(pk_indices) = schema.get_primary_key_indices() {
                            pk_values
                                .into_iter()
                                .zip(pk_indices.iter())
                                .map(|(val, &idx)| {
                                    coerce_value_to_column_type(val, &schema.columns[idx].data_type)
                                })
                                .collect()
                        } else {
                            pk_values
                        };

                    // Check if we can use the super-fast path (no triggers, no FKs)
                    let has_triggers = database
                        .catalog
                        .get_triggers_for_table(table_name, Some(vibesql_ast::TriggerEvent::Delete))
                        .next()
                        .is_some();

                    // Fast check: if this table has no PK, FKs can't reference it
                    let has_pk = schema.get_primary_key_indices().is_some();
                    let has_referencing_fks = has_pk
                        && database.catalog.list_tables().iter().any(|t| {
                            database
                                .catalog
                                .get_table(t)
                                .map(|s| {
                                    s.foreign_keys
                                        .iter()
                                        .any(|fk| fk.parent_table.eq_ignore_ascii_case(table_name))
                                })
                                .unwrap_or(false)
                        });

                    // Also skip fast path if there are expression indexes that need maintenance
                    let has_expression_indexes = database.has_expression_indexes(table_name);
                    // Skip fast path if there are partial indexes — they need
                    // the executor to evaluate the WHERE predicate against
                    // the row being deleted before removing the entry.
                    let has_partial_indexes = database.has_partial_indexes(table_name);
                    if !has_triggers
                        && !has_referencing_fks
                        && !has_expression_indexes
                        && !has_partial_indexes
                    {
                        // Use the fast path - no triggers, no FKs, no expression indexes, single row PK delete
                        match database.delete_by_pk_fast(table_name, &pk_values) {
                            Ok(deleted) => {
                                let count = if deleted { 1 } else { 0 };
                                // Check all assertions after DELETE completes (SQL:1999 Feature
                                // F671/F672) This ensures
                                // database-wide integrity constraints are maintained
                                crate::advanced_objects::AssertionChecker::check_all_assertions(
                                    database,
                                )?;
                                return Ok((count, None));
                            }
                            Err(_) => {
                                // Fall through to standard path on error
                            }
                        }
                    }
                }
            }
        }

        // Step 2: Evaluate WHERE clause and collect rows to delete (two-phase execution)
        // Get table for scanning
        let table = database
            .get_table(table_name)
            .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?;

        // Phase 1d follow-up (#5205): capture the active MVCC snapshot
        // once so both the PK fast path and the WHERE-clause scan agree
        // on visibility. Off-state collapses to the pre-MVCC live-row
        // filter; the snapshot argument is ignored by the storage layer.
        let snapshot = crate::mvcc::read_snapshot(database);

        // Execute CTEs if present (WITH clause support)
        let cte_results = if let Some(ref cte_list) = stmt.with_clause {
            Some(crate::select::cte::execute_ctes(cte_list, database, |cte_query, prior_ctes| {
                let cte_executor = crate::SelectExecutor::new_with_cte(database, prior_ctes);
                cte_executor.execute(cte_query)
            })?)
        } else {
            None
        };

        // Create evaluator with database reference for subquery support (EXISTS, NOT EXISTS, IN
        // with subquery, etc.) and optional procedural/trigger context for variable resolution
        let mut evaluator = if let Some(ctx) = trigger_context {
            // Trigger context takes precedence (trigger statements can't have procedural context)
            ExpressionEvaluator::with_trigger_context(&schema, database, ctx)
        } else if let Some(ctx) = procedural_context {
            ExpressionEvaluator::with_procedural_context(&schema, database, ctx)
        } else if let Some(ref cte_ctx) = cte_results {
            // Use CTE context for WITH clause support
            ExpressionEvaluator::with_database_and_cte(&schema, database, cte_ctx)
        } else {
            ExpressionEvaluator::with_database(&schema, database)
        };

        // Check once if any DELETE triggers exist for this table (used for fast-path checks)
        let has_delete_triggers = database
            .catalog
            .get_triggers_for_table(table_name, Some(vibesql_ast::TriggerEvent::Delete))
            .next()
            .is_some();

        // Validate WHERE clause columns upfront (catches errors even on empty tables)
        if let Some(vibesql_ast::WhereClause::Condition(where_expr)) = &stmt.where_clause {
            Self::validate_where_columns(where_expr, &schema, table_name)?;
        }

        // Find rows to delete and their indices
        // Try to use primary key index for fast lookup
        let mut rows_and_indices_to_delete: Vec<(usize, vibesql_storage::Row)> = Vec::new();

        // For ORDER BY in DELETE, we need to skip PK optimization and do a full scan
        // because we need all matching rows to sort them properly
        let has_order_by = stmt.order_by.is_some();

        if !has_order_by {
            if let Some(vibesql_ast::WhereClause::Condition(where_expr)) = &stmt.where_clause {
                // Try primary key optimization
                if let Some(pk_values) = Self::extract_primary_key_lookup(where_expr, &schema) {
                    // Coerce extracted WHERE-clause literals to match the PK
                    // column affinities. The PK index HashMap is keyed on
                    // stored (already-coerced) values, so a raw literal can
                    // silently miss when types differ — e.g. `WHERE p=1200` on
                    // a TEXT PRIMARY KEY storing "1200". See issue #5145.
                    let pk_values: Vec<SqlValue> =
                        if let Some(pk_indices) = schema.get_primary_key_indices() {
                            pk_values
                                .into_iter()
                                .zip(pk_indices.iter())
                                .map(|(val, &idx)| {
                                    coerce_value_to_column_type(val, &schema.columns[idx].data_type)
                                })
                                .collect()
                        } else {
                            pk_values
                        };

                    if let Some(pk_index) = table.primary_key_index() {
                        if let Some(&row_index) = pk_index.get(&pk_values) {
                            // Phase 1d follow-up (#5205): the PK fast
                            // path must honor MVCC visibility — a row
                            // whose xmax is committed under our snapshot
                            // must not be picked for DELETE. Off-state
                            // collapses to a deletion-bitmap check.
                            if table.is_row_visible(row_index, &snapshot) {
                                rows_and_indices_to_delete
                                    .push((row_index, table.scan()[row_index].clone()));
                            }
                        }
                        // If not found, rows_and_indices_to_delete stays empty (no rows to delete)
                    } else {
                        // No PK index, fall through to table scan below
                        Self::collect_rows_with_scan(
                            table,
                            &stmt.where_clause,
                            &mut evaluator,
                            &mut rows_and_indices_to_delete,
                            &snapshot,
                        )?;
                    }
                } else {
                    // Can't extract PK lookup, fall through to table scan
                    Self::collect_rows_with_scan(
                        table,
                        &stmt.where_clause,
                        &mut evaluator,
                        &mut rows_and_indices_to_delete,
                        &snapshot,
                    )?;
                }
            } else {
                // No WHERE clause - collect all rows
                Self::collect_rows_with_scan(
                    table,
                    &stmt.where_clause,
                    &mut evaluator,
                    &mut rows_and_indices_to_delete,
                    &snapshot,
                )?;
            }
        } else {
            // ORDER BY present - must do full scan to get all rows for sorting
            Self::collect_rows_with_scan(
                table,
                &stmt.where_clause,
                &mut evaluator,
                &mut rows_and_indices_to_delete,
                &snapshot,
            )?;
        }

        // Apply ORDER BY sorting and LIMIT/OFFSET (SQLite extension)
        if let Some(ref order_by) = stmt.order_by {
            Self::apply_order_by_and_limit(
                &mut rows_and_indices_to_delete,
                order_by,
                &stmt.limit,
                &stmt.offset,
                &schema,
                &evaluator,
            )?;
        }

        // Cost-based optimization: Log delete cost and check for early compaction recommendation
        let optimizer = DmlOptimizer::new(database, table_name);
        if optimizer.should_chunk_delete(rows_and_indices_to_delete.len()) {
            // Log recommendation for potential chunked delete (informational only)
            // Actual chunked delete would require transaction support to be safe
            if std::env::var("DML_COST_DEBUG").is_ok() {
                eprintln!(
                    "DML_COST_DEBUG: DELETE on {} - {} rows qualifies for chunked delete",
                    stmt.table_name,
                    rows_and_indices_to_delete.len()
                );
            }
        }
        if optimizer.should_trigger_early_compaction() {
            // Log early compaction recommendation (informational only)
            // Table compaction is triggered automatically after >50% deleted rows
            if std::env::var("DML_COST_DEBUG").is_ok() {
                eprintln!(
                    "DML_COST_DEBUG: DELETE on {} - early compaction recommended due to high deleted ratio",
                    stmt.table_name
                );
            }
        }

        // Fire BEFORE STATEMENT triggers only if triggers exist AND we're not inside a trigger
        // context (Statement-level triggers don't fire for deletes within trigger bodies)
        if has_delete_triggers && trigger_context.is_none() {
            crate::TriggerFirer::execute_before_statement_triggers(
                database,
                table_name,
                vibesql_ast::TriggerEvent::Delete,
            )?;
        }

        // Step 3: Fire BEFORE DELETE ROW triggers only if triggers exist.
        // A RAISE(IGNORE) in a BEFORE DELETE trigger abandons that row: drop it
        // from the batch so it is neither deleted nor counted (SQLite).
        if has_delete_triggers {
            let mut keep = Vec::with_capacity(rows_and_indices_to_delete.len());
            for (idx, row) in rows_and_indices_to_delete {
                let outcome = crate::TriggerFirer::execute_before_triggers(
                    database,
                    table_name,
                    vibesql_ast::TriggerEvent::Delete,
                    Some(&row),
                    None,
                )?;
                if outcome != crate::TriggerOutcome::SkipRow {
                    keep.push((idx, row));
                }
            }
            rows_and_indices_to_delete = keep;
        }

        // Step 4: Handle referential integrity for each row to be deleted
        // This may CASCADE deletes, SET NULL, or SET DEFAULT in child tables
        for (_, row) in &rows_and_indices_to_delete {
            check_no_child_references(database, table_name, row)?;
        }

        // Extract indices for deletion
        let mut deleted_indices: Vec<usize> =
            rows_and_indices_to_delete.iter().map(|(idx, _)| *idx).collect();
        deleted_indices.sort_unstable();

        // Step 5a: Emit WAL entries and remove entries from user-defined indexes
        // BEFORE deleting rows (while row indices are still valid and we have old values)
        // First emit WAL entries for each row (needed for recovery replay)
        for (idx, row) in &rows_and_indices_to_delete {
            database.emit_wal_delete(table_name, *idx as u64, row.values.to_vec());
        }

        // Then use batch method for index updates: O(d + m*log n) vs O(d*m*log n)
        // where d=deletes, m=indexes
        let rows_refs: Vec<(usize, &vibesql_storage::Row)> =
            rows_and_indices_to_delete.iter().map(|(idx, row)| (*idx, row)).collect();
        database.batch_update_indexes_for_delete(table_name, &rows_refs);

        // Maintain expression indexes for each deleted row
        for (row_index, row) in &rows_and_indices_to_delete {
            expression_index_maintenance::maintain_expression_indexes_for_delete(
                database, table_name, row, *row_index,
            );
            partial_index_maintenance::maintain_partial_indexes_for_delete(
                database, table_name, row, *row_index,
            );
        }

        // Phase 1c (Issue #5150 / #5136): stamp xmax on every row about
        // to be bitmap-deleted with the active txn id when the
        // `mvcc_enabled` feature is on. Fetched BEFORE the mutable borrow
        // of `table_mut`. We keep the physical bitmap delete in place;
        // Phase 1d's visibility filter will eventually treat the xmax
        // stamp as the canonical deletion record. Off-state: no stamp,
        // bit-for-bit pre-MVCC behavior.
        #[cfg(feature = "mvcc_enabled")]
        let mvcc_delete_txn_id = database.transaction_id();

        // Step 5b: Actually delete the rows using fast path (no table scan needed)
        let table_mut = database
            .get_table_mut(table_name)
            .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?;

        // Stamp xmax in-place on each row BEFORE bitmap-deleting. The
        // bitmap delete leaves the row in `self.rows`, so the xmax field
        // is observable via `Table::scan()` (which returns the raw rows
        // slice including bitmap-deleted entries).
        #[cfg(feature = "mvcc_enabled")]
        if let Some(id) = mvcc_delete_txn_id {
            for &idx in &deleted_indices {
                table_mut.stamp_row_xmax_inplace(idx, id);
            }
        }

        // Use delete_by_indices_batch for O(d) instead of O(n) where d = deletes
        // The batch version pre-computes schema lookups for internal hash indexes,
        // reducing overhead by ~30-40% for multi-row deletes.
        // User-defined index entries have already been removed by batch_update_indexes_for_delete
        // above. Note: If >50% of rows are deleted, compaction triggers and row indices
        // change. When compaction occurs, we must rebuild user-defined indexes.
        let delete_result = table_mut.delete_by_indices_batch(&deleted_indices);

        // If compaction occurred, rebuild user-defined indexes since all row indices changed
        if delete_result.compacted {
            database.rebuild_indexes(table_name);
            // Expression indexes need special handling (expression evaluation)
            expression_index_maintenance::rebuild_expression_indexes_after_compaction(
                database, table_name,
            );
            // Partial indexes need WHERE-predicate evaluation per row
            partial_index_maintenance::rebuild_partial_indexes_after_compaction(
                database, table_name,
            );
        }

        // Invalidate the database-level columnar cache since table data changed.
        // Note: The table-level cache is already invalidated by delete_by_indices().
        // Both invalidations are necessary because they manage separate caches:
        // - Table-level cache: used by Table::scan_columnar() for SIMD filtering
        // - Database-level cache: used by Database::get_columnar() for cached access
        if delete_result.deleted_count > 0 {
            database.invalidate_columnar_cache(table_name);
        }

        // Step 6: Fire AFTER DELETE ROW triggers only if triggers exist
        if has_delete_triggers {
            for (_, row) in &rows_and_indices_to_delete {
                crate::TriggerFirer::execute_after_triggers(
                    database,
                    table_name,
                    vibesql_ast::TriggerEvent::Delete,
                    Some(row),
                    None,
                )?;
            }
        }

        // Fire AFTER STATEMENT triggers only if triggers exist AND we're not inside a trigger
        // context (Statement-level triggers don't fire for deletes within trigger bodies)
        if has_delete_triggers && trigger_context.is_none() {
            crate::TriggerFirer::execute_after_statement_triggers(
                database,
                table_name,
                vibesql_ast::TriggerEvent::Delete,
            )?;
        }

        // Check all assertions after DELETE completes (SQL:1999 Feature F671/F672)
        // This ensures database-wide integrity constraints are maintained
        crate::advanced_objects::AssertionChecker::check_all_assertions(database)?;

        // Project RETURNING items against the OLD rows (SQLite 3.35.0+).
        // Rows are projected in collection order (ORDER BY/LIMIT already
        // applied); zero deleted rows yields an empty result whose column
        // names are still derived from the RETURNING items.
        let returning = if let Some(items) = &stmt.returning {
            let old_rows: Vec<&vibesql_storage::Row> =
                rows_and_indices_to_delete.iter().map(|(_, row)| row).collect();
            Some(crate::dml_returning::project_returning(
                items,
                &schema,
                database,
                None,
                &old_rows,
                cte_results.as_ref(),
            )?)
        } else {
            None
        };

        Ok((delete_result.deleted_count, returning))
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
            if let (Expression::ColumnRef(col_id), Expression::Literal(value)) =
                (left.as_ref(), right.as_ref())
            {
                // Check if this column is the primary key
                let column = col_id.column_canonical();
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
            if let (Expression::Literal(value), Expression::ColumnRef(col_id)) =
                (left.as_ref(), right.as_ref())
            {
                let column = col_id.column_canonical();
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

    /// Validate that all column references in the WHERE clause exist in the schema
    /// This catches errors like "DELETE FROM t WHERE nonexistent_col = 5" even on empty tables
    #[allow(clippy::only_used_in_recursion)] // table_name preserved for future error messages
    fn validate_where_columns(
        expr: &vibesql_ast::Expression,
        schema: &vibesql_catalog::TableSchema,
        table_name: &str,
    ) -> Result<(), ExecutorError> {
        use vibesql_ast::Expression;

        match expr {
            Expression::ColumnRef(col_id) => {
                let col_name = col_id.column_canonical();
                // Check if column exists in schema (case-insensitive)
                // Also allow ROWID pseudo-column aliases
                let is_rowid = col_name.eq_ignore_ascii_case("rowid")
                    || col_name.eq_ignore_ascii_case("_rowid_")
                    || col_name.eq_ignore_ascii_case("oid");
                if !is_rowid
                    && !schema.columns.iter().any(|c| c.name.eq_ignore_ascii_case(col_name))
                {
                    return Err(ExecutorError::NoSuchColumn { column_ref: col_name.to_string() });
                }
                Ok(())
            }
            Expression::BinaryOp { left, right, .. } => {
                Self::validate_where_columns(left, schema, table_name)?;
                Self::validate_where_columns(right, schema, table_name)
            }
            Expression::UnaryOp { expr, .. } => {
                Self::validate_where_columns(expr, schema, table_name)
            }
            Expression::IsNull { expr, .. } => {
                Self::validate_where_columns(expr, schema, table_name)
            }
            Expression::Between { expr, low, high, .. } => {
                Self::validate_where_columns(expr, schema, table_name)?;
                Self::validate_where_columns(low, schema, table_name)?;
                Self::validate_where_columns(high, schema, table_name)
            }
            Expression::InList { expr, values, .. } => {
                Self::validate_where_columns(expr, schema, table_name)?;
                for item in values {
                    Self::validate_where_columns(item, schema, table_name)?;
                }
                Ok(())
            }
            Expression::Function { args, .. } => {
                for arg in args {
                    Self::validate_where_columns(arg, schema, table_name)?;
                }
                Ok(())
            }
            Expression::AggregateFunction { args, .. } => {
                for arg in args {
                    Self::validate_where_columns(arg, schema, table_name)?;
                }
                Ok(())
            }
            Expression::Case { operand, when_clauses, else_result } => {
                if let Some(op) = operand {
                    Self::validate_where_columns(op, schema, table_name)?;
                }
                for case_when in when_clauses {
                    for cond in &case_when.conditions {
                        Self::validate_where_columns(cond, schema, table_name)?;
                    }
                    Self::validate_where_columns(&case_when.result, schema, table_name)?;
                }
                if let Some(else_expr) = else_result {
                    Self::validate_where_columns(else_expr, schema, table_name)?;
                }
                Ok(())
            }
            // Literals and other expressions that don't reference columns
            Expression::Literal(_)
            | Expression::Wildcard
            | Expression::Placeholder(_)
            | Expression::NumberedPlaceholder(_)
            | Expression::NamedPlaceholder(_)
            | Expression::CurrentDate
            | Expression::CurrentTime { .. }
            | Expression::CurrentTimestamp { .. } => Ok(()),
            // Subqueries have their own scope - don't validate against parent table
            Expression::ScalarSubquery(_)
            | Expression::In { .. }
            | Expression::Exists { .. }
            | Expression::QuantifiedComparison { .. } => Ok(()),
            // Other expressions - recurse into children if any
            _ => Ok(()),
        }
    }

    /// Collect rows using table scan (fallback when PK optimization can't be used).
    ///
    /// Phase 1d follow-up (#5205): uses `scan_visible(snapshot)` so the
    /// WHERE-clause scan honors MVCC visibility. Off-state reduces to
    /// `scan_live` (deletion-bitmap filter), preserving pre-MVCC semantics.
    fn collect_rows_with_scan(
        table: &vibesql_storage::Table,
        where_clause: &Option<vibesql_ast::WhereClause>,
        evaluator: &mut ExpressionEvaluator,
        rows_and_indices: &mut Vec<(usize, vibesql_storage::Row)>,
        snapshot: &vibesql_storage::TxnSnapshot,
    ) -> Result<(), ExecutorError> {
        for (index, row) in table.scan_visible(snapshot) {
            // Clear CSE cache before evaluating each row to prevent column values
            // from being incorrectly cached across different rows
            evaluator.clear_cse_cache();

            // Set row_id for ROWID pseudo-column support (SQLite compatibility)
            // SQLite uses 1-indexed rowids, so add 1 to the physical index
            // This allows WHERE rowid = N to work correctly
            // Use the row's explicit row_id if set, otherwise compute from physical index
            let row_id = row.row_id.unwrap_or((index + 1) as u64);
            evaluator.set_row_index(row_id);

            let should_delete = if let Some(ref where_clause) = where_clause {
                match where_clause {
                    vibesql_ast::WhereClause::Condition(where_expr) => {
                        // Propagate errors from eval() - don't silently swallow them
                        let result = evaluator.eval(where_expr, row)?;
                        // SQLite treats non-zero numeric values as TRUE
                        is_truthy(&result)
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

    /// Apply ORDER BY sorting and LIMIT/OFFSET to the collected rows
    /// This implements the SQLite extension for DELETE with ORDER BY LIMIT
    fn apply_order_by_and_limit(
        rows_and_indices: &mut Vec<(usize, vibesql_storage::Row)>,
        order_by: &[vibesql_ast::OrderByItem],
        limit: &Option<vibesql_ast::Expression>,
        offset: &Option<vibesql_ast::Expression>,
        _schema: &vibesql_catalog::TableSchema,
        evaluator: &ExpressionEvaluator,
    ) -> Result<(), ExecutorError> {
        use vibesql_ast::OrderDirection;
        use vibesql_types::SqlValue;

        // Sort rows by ORDER BY columns
        rows_and_indices.sort_by(|a, b| {
            for item in order_by {
                // Evaluate the ORDER BY expression for both rows
                let val_a = evaluator.eval(&item.expr, &a.1).unwrap_or(SqlValue::Null);
                let val_b = evaluator.eval(&item.expr, &b.1).unwrap_or(SqlValue::Null);

                // Compare values with proper NULL handling
                // NULLS FIRST: nulls come first (default for DESC)
                // NULLS LAST: nulls come last (default for ASC)
                let nulls_first = match item.nulls_order {
                    Some(vibesql_ast::NullsOrder::First) => true,
                    Some(vibesql_ast::NullsOrder::Last) => false,
                    None => matches!(item.direction, OrderDirection::Desc),
                };

                let cmp = match (&val_a, &val_b) {
                    (SqlValue::Null, SqlValue::Null) => std::cmp::Ordering::Equal,
                    (SqlValue::Null, _) => {
                        if nulls_first {
                            std::cmp::Ordering::Less
                        } else {
                            std::cmp::Ordering::Greater
                        }
                    }
                    (_, SqlValue::Null) => {
                        if nulls_first {
                            std::cmp::Ordering::Greater
                        } else {
                            std::cmp::Ordering::Less
                        }
                    }
                    _ => val_a.partial_cmp(&val_b).unwrap_or(std::cmp::Ordering::Equal),
                };

                // Apply direction
                let cmp = match item.direction {
                    OrderDirection::Desc => cmp.reverse(),
                    OrderDirection::Asc => cmp,
                };

                if cmp != std::cmp::Ordering::Equal {
                    return cmp;
                }
            }
            std::cmp::Ordering::Equal
        });

        // Evaluate OFFSET expression if present
        let offset_val = if let Some(ref offset_expr) = offset {
            // Evaluate the offset expression without a row context
            // (it should be a constant or simple expression)
            let empty_row = vibesql_storage::Row::new(vec![]);
            match evaluator.eval(offset_expr, &empty_row)? {
                SqlValue::Integer(n) if n >= 0 => n as usize,
                SqlValue::Bigint(n) if n >= 0 => n as usize,
                SqlValue::Null => 0, // NULL offset treated as 0
                _ => {
                    return Err(ExecutorError::TypeError(
                        "OFFSET value must be a non-negative integer".to_string(),
                    ))
                }
            }
        } else {
            0
        };

        // Evaluate LIMIT expression if present
        let limit_val = if let Some(ref limit_expr) = limit {
            let empty_row = vibesql_storage::Row::new(vec![]);
            match evaluator.eval(limit_expr, &empty_row)? {
                SqlValue::Integer(n) if n >= 0 => Some(n as usize),
                SqlValue::Bigint(n) if n >= 0 => Some(n as usize),
                SqlValue::Integer(-1) | SqlValue::Bigint(-1) => None, // -1 means no limit (SQLite extension)
                SqlValue::Null => None, // NULL limit treated as no limit
                _ => {
                    return Err(ExecutorError::TypeError(
                        "LIMIT value must be a non-negative integer".to_string(),
                    ))
                }
            }
        } else {
            None
        };

        // Apply OFFSET: skip first N rows
        if offset_val > 0 {
            if offset_val >= rows_and_indices.len() {
                rows_and_indices.clear();
            } else {
                rows_and_indices.drain(..offset_val);
            }
        }

        // Apply LIMIT: keep only first N rows
        if let Some(limit) = limit_val {
            rows_and_indices.truncate(limit);
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
    // Note: table.clear() invalidates the table-level columnar cache internally
    table.clear();

    // Rebuild user-defined indexes (clears them, since the table is now empty).
    // Without this, database-level index data retains the deleted rows' keys
    // and subsequent INSERTs fail with spurious UNIQUE constraint errors
    // (upsert1-710/740/770). The TRUNCATE TABLE executor does the same
    // (see truncate/core.rs::execute_truncate).
    database.rebuild_indexes(table_name);

    // Invalidate the database-level columnar cache since table data changed.
    // Both the table-level (via clear()) and database-level invalidations are
    // necessary because they manage separate caches at different levels.
    if row_count > 0 {
        database.invalidate_columnar_cache(table_name);
    }

    // Check all assertions after DELETE completes (SQL:1999 Feature F671/F672)
    // This ensures database-wide integrity constraints are maintained
    crate::advanced_objects::AssertionChecker::check_all_assertions(database)?;

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

/// Execute DELETE on a VIEW using INSTEAD OF triggers
///
/// When deleting from a view, we need to fire INSTEAD OF DELETE triggers
/// instead of actually deleting data. The triggers typically delete from
/// the underlying tables.
fn execute_delete_on_view(
    database: &mut Database,
    stmt: &DeleteStmt,
    view_def: &vibesql_catalog::ViewDefinition,
    procedural_context: Option<&crate::procedural::ExecutionContext>,
    trigger_context: Option<&crate::trigger_execution::TriggerContext>,
) -> Result<(usize, Option<crate::select::SelectResult>), ExecutorError> {
    use vibesql_ast::TriggerTiming;

    // Find INSTEAD OF DELETE triggers for this view
    let triggers = crate::TriggerFirer::find_triggers(
        database,
        &view_def.name,
        TriggerTiming::InsteadOf,
        vibesql_ast::TriggerEvent::Delete,
    );

    if triggers.is_empty() {
        return Err(ExecutorError::UnsupportedExpression(format!(
            "Cannot DELETE from view '{}' without INSTEAD OF trigger",
            view_def.name
        )));
    }

    // Build a pseudo-schema for the view
    let view_schema = build_view_schema(database, view_def)?;

    // Execute the view query to get the rows to potentially delete
    let select_executor = crate::SelectExecutor::new(database);
    let all_rows = select_executor.execute_with_columns(&view_def.query)?;

    // Collect rows to delete first, before firing triggers
    // This avoids borrow conflicts with the evaluator
    let rows_to_delete: Vec<vibesql_storage::Row> = {
        // Create evaluator for WHERE clause (if any)
        let evaluator = if let Some(ctx) = trigger_context {
            ExpressionEvaluator::with_trigger_context(&view_schema, database, ctx)
        } else if let Some(ctx) = procedural_context {
            ExpressionEvaluator::with_procedural_context(&view_schema, database, ctx)
        } else {
            ExpressionEvaluator::with_database(&view_schema, database)
        };

        // Select rows matching WHERE clause
        let mut collected_rows = Vec::new();
        for row in &all_rows.rows {
            let matches = match &stmt.where_clause {
                Some(vibesql_ast::WhereClause::Condition(expr)) => {
                    let result = evaluator.eval(expr, row)?;
                    // SQLite treats non-zero numeric values as TRUE
                    is_truthy(&result)
                }
                None => true, // No WHERE clause - delete all rows
                Some(vibesql_ast::WhereClause::CurrentOf(_)) => {
                    return Err(ExecutorError::UnsupportedExpression(
                        "CURRENT OF not supported for view deletes".to_string(),
                    ));
                }
            };

            if matches {
                collected_rows.push(row.clone());
            }
        }
        collected_rows
    }; // evaluator dropped here

    // Now fire triggers (database can be mutably borrowed)
    let rows_processed = rows_to_delete.len();
    for old_row in &rows_to_delete {
        for trigger in &triggers {
            crate::TriggerFirer::execute_trigger(database, trigger, Some(old_row), None)?;
        }
    }

    // Project RETURNING items against the OLD view rows (SQLite 3.35.0+):
    // the OLD view row is returned once per trigger fire, regardless of
    // what the trigger body actually does.
    let returning = if let Some(items) = &stmt.returning {
        let old_rows: Vec<&vibesql_storage::Row> = rows_to_delete.iter().collect();
        Some(crate::dml_returning::project_returning(
            items,
            &view_schema,
            database,
            None,
            &old_rows,
            None,
        )?)
    } else {
        None
    };

    Ok((rows_processed, returning))
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
    let column_names: Vec<String> =
        if let Some(ref cols) = view_def.columns { cols.clone() } else { result.columns.clone() };

    // Build columns with NONE affinity (DataType::Null), mirroring the
    // UPDATE-on-view pseudo-schema (#5233/#5260). The pseudo-schema mainly
    // provides column names for trigger binding, but its data types feed
    // SQLite affinity rules during WHERE evaluation: declaring the columns
    // as Varchar gave them TEXT affinity, which converted numeric literals
    // to text and made comparisons like `WHERE b=4` never match. NONE
    // affinity compares values by their actual types, matching bare
    // (undeclared) columns in SQLite.
    let columns: Vec<vibesql_catalog::ColumnSchema> = column_names
        .into_iter()
        .map(|name| vibesql_catalog::ColumnSchema::new(name, vibesql_types::DataType::Null, true))
        .collect();

    Ok(vibesql_catalog::TableSchema::new(view_def.name.clone(), columns))
}

/// Check if a SqlValue is truthy using SQLite truthiness rules.
///
/// Delegates to the shared helper in `crate::evaluator::operators` so DELETE
/// and UPDATE-on-view row selection use identical truthiness semantics.
fn is_truthy(value: &SqlValue) -> bool {
    crate::evaluator::operators::is_truthy(value)
}
