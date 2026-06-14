use vibesql_catalog::TableIdentifier;
use vibesql_storage::statistics::CostEstimator;

use crate::{
    dml_cost::DmlOptimizer, errors::ExecutorError, expression_index_maintenance,
    partial_index_maintenance, privilege_checker::PrivilegeChecker,
    sqlite_schema::is_sqlite_schema_table, sqlite_stat::is_sqlite_stat1_table,
};

/// Outcome of an INSERT statement execution.
#[derive(Debug)]
pub struct InsertOutcome {
    /// Total affected rows: direct inserts plus rows taken through the
    /// `ON CONFLICT DO UPDATE` arm. Matches SQLite's `changes()`.
    pub affected_rows: usize,
    /// Rows handled via the `ON CONFLICT DO UPDATE` arm (subset of
    /// `affected_rows`). SQLite's `PRAGMA count_changes` reports
    /// `affected_rows - upsert_updated_rows` for INSERT (direct inserts
    /// only), while `changes()` includes the update-arm rows.
    pub upsert_updated_rows: usize,
    /// Projected RETURNING rows when the statement carries a RETURNING
    /// clause; `None` otherwise.
    pub returning: Option<crate::select::SelectResult>,
}

/// Execute an INSERT statement
/// Returns number of rows inserted
pub fn execute_insert(
    db: &mut vibesql_storage::Database,
    stmt: &vibesql_ast::InsertStmt,
) -> Result<usize, ExecutorError> {
    execute_insert_internal(db, stmt, None, None).map(|outcome| outcome.affected_rows)
}

/// Execute an INSERT statement, capturing RETURNING rows (SQLite 3.35.0+)
///
/// Returns an [`InsertOutcome`] carrying the number of affected rows, the
/// number of rows handled via the upsert `DO UPDATE` arm, and, when the
/// statement carries a RETURNING clause, the projected NEW rows (values as
/// actually inserted, including defaults, generated columns, and auto
/// INTEGER PRIMARY KEY) — one per affected row, in insertion order. Rows
/// skipped by `OR IGNORE` / `ON CONFLICT DO NOTHING` do not appear. For
/// `ON DUPLICATE KEY UPDATE`, the post-UPDATE row is returned.
pub fn execute_insert_returning(
    db: &mut vibesql_storage::Database,
    stmt: &vibesql_ast::InsertStmt,
) -> Result<InsertOutcome, ExecutorError> {
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
        .map(|outcome| outcome.affected_rows)
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
        .map(|outcome| outcome.affected_rows)
}

/// Internal implementation of INSERT execution
fn execute_insert_internal(
    db: &mut vibesql_storage::Database,
    stmt: &vibesql_ast::InsertStmt,
    procedural_context: Option<&crate::procedural::ExecutionContext>,
    trigger_context: Option<&crate::trigger_execution::TriggerContext>,
) -> Result<InsertOutcome, ExecutorError> {
    // Build full table name for error messages and privilege checks
    let full_table_name = match &stmt.schema_name {
        Some(schema) => format!("{}.{}", schema, stmt.table_name),
        None => stmt.table_name.clone(),
    };

    // Check if target is sqlite_master/sqlite_schema (read-only system table)
    if is_sqlite_schema_table(&stmt.table_name) {
        return Err(ExecutorError::SqliteSystemTableReadOnly {
            table_name: stmt.table_name.clone(),
            operation: "modified".to_string(),
        });
    }

    // Check if target is sqlite_stat1 (special writable statistics table)
    // RETURNING is not supported on this virtual statistics table.
    if is_sqlite_stat1_table(&stmt.table_name) {
        return execute_insert_sqlite_stat1(db, stmt).map(|count| InsertOutcome {
            affected_rows: count,
            upsert_updated_rows: 0,
            returning: None,
        });
    }

    // Check INSERT privilege on the table
    PrivilegeChecker::check_insert(db, &full_table_name)?;

    // Materialize statement-level WITH-clause CTEs once, up front, so they
    // are visible to subqueries in VALUES rows, the INSERT ... SELECT
    // source, the upsert DO UPDATE arm, and RETURNING expressions —
    // matching SQLite semantics (issue #5359). CTE names shadow same-named
    // catalog tables/views and resolve ASCII case-insensitively (#5350).
    let cte_results: Option<std::collections::HashMap<String, crate::select::cte::CteResult>> =
        if let Some(ref cte_list) = stmt.with_clause {
            Some(crate::select::cte::execute_ctes(cte_list, db, |cte_query, prior_ctes| {
                let cte_executor = crate::SelectExecutor::new_with_cte(db, prior_ctes);
                cte_executor
                    .execute_with_columns(cte_query)
                    .map(|result| result.rows.into_iter().collect())
            })?)
        } else {
            None
        };

    // Check if target is a VIEW with INSTEAD OF triggers
    if let Some(view_def) = db.catalog.get_view(&stmt.table_name).cloned() {
        // SQLite: the upsert syntax cannot target a view, even when INSTEAD
        // OF INSERT triggers exist (upsert1-910).
        if stmt.on_conflict.is_some() {
            return Err(ExecutorError::SqliteCompatError("cannot UPSERT a view".to_string()));
        }
        return execute_insert_on_view(
            db,
            stmt,
            &view_def,
            procedural_context,
            trigger_context,
            cte_results.as_ref(),
        )
        .map(|(count, returning)| InsertOutcome {
            affected_rows: count,
            upsert_updated_rows: 0,
            returning,
        });
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

    // Validate an explicit ON CONFLICT (cols) target up front. SQLite does
    // this at prepare time, even when no row actually conflicts: unknown
    // columns raise "no such column" and known columns without a matching
    // PRIMARY KEY / UNIQUE constraint / unique index raise the canonical
    // "ON CONFLICT clause does not match..." error (upsert1-110/120/300).
    if let Some(ref on_conflict) = stmt.on_conflict {
        // Targets the AST cannot represent exactly (currently non-BINARY
        // COLLATE) never match (upsert1-130; see issue #5269).
        if on_conflict.target_inexact {
            return Err(ExecutorError::SqliteCompatError(
                "ON CONFLICT clause does not match any PRIMARY KEY or UNIQUE constraint"
                    .to_string(),
            ));
        }
        if let Some(ref target) = on_conflict.conflict_target {
            super::on_conflict_update::validate_conflict_target(
                db,
                table_name,
                &schema,
                target,
                on_conflict.target_where.as_ref(),
            )?;
        }
    }

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
        vibesql_ast::InsertSource::DefaultValues => {
            // For DEFAULT VALUES, insert a single row with DEFAULT for all columns
            // The number of expressions must match target_column_info.len()
            // Each Expression::Default will be evaluated to the column's default value
            let default_row = vec![vibesql_ast::Expression::Default; target_column_info.len()];
            vec![default_row]
        }
        vibesql_ast::InsertSource::Select(select_stmt) => {
            // Try bulk transfer optimization first (Phase 1-3)
            // This provides 10-50x performance improvement for compatible schemas
            // Note: bulk transfer doesn't support CTEs, so skip if with_clause is present.
            // RETURNING also gates this fast path: it returns early with only a
            // count, but RETURNING must project the actually-inserted NEW rows.
            if stmt.columns.is_empty() && stmt.with_clause.is_none() && stmt.returning.is_none() {
                // Only attempt bulk transfer for INSERT INTO table SELECT (no column list)
                if let Some(count) =
                    super::bulk_transfer::try_bulk_transfer(db, table_name, select_stmt)?
                {
                    // Fast path succeeded, return early
                    return Ok(InsertOutcome {
                        affected_rows: count,
                        upsert_updated_rows: 0,
                        returning: None,
                    });
                }
            }

            // Fall back to normal path: execute SELECT and convert to expressions
            // If we have a with_clause (CTEs), reuse the results materialized above.
            //
            // When this INSERT runs inside a trigger body and the source is a
            // from-less SELECT that references OLD/NEW (e.g.
            // `INSERT INTO log SELECT OLD.a || ',' || OLD.b;`, trigger5-1.1),
            // the SELECT needs the firing row's pseudo-variable context — the
            // same context the WHEN clause and body DML statements already get.
            // Without it the evaluator rejects the OLD/NEW column references
            // with "Column reference requires FROM clause" (#5470).
            let select_result = if let Some(ref ctes) = cte_results {
                // Create executor with CTE results
                let select_executor = crate::SelectExecutor::new_with_cte(db, ctes);
                select_executor.execute_with_columns(select_stmt)?
            } else if let Some(ctx) = trigger_context {
                let select_executor = crate::SelectExecutor::new_with_trigger_context(db, ctx);
                select_executor.execute_with_columns(select_stmt)?
            } else {
                let select_executor = crate::SelectExecutor::new(db);
                select_executor.execute_with_columns(select_stmt)?
            };

            // Validate column count - if rowid is a pseudo-column (not INTEGER PRIMARY KEY),
            // SELECT should return one extra column for the rowid value.
            // For INTEGER PRIMARY KEY columns, the rowid value is already part of target_column_info.
            let expected_select_columns = if resolved_columns.rowid_is_pseudo_column {
                target_column_info.len() + 1
            } else {
                target_column_info.len()
            };
            if select_result.columns.len() != expected_select_columns {
                return Err(ExecutorError::InsertColumnCountMismatch {
                    table_name: table_name.to_string(),
                    expected: expected_select_columns,
                    provided: select_result.columns.len(),
                    has_explicit_columns: !stmt.columns.is_empty(),
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
    // If a pseudo-column rowid is specified (rowid, _rowid_, oid), the expected count
    // includes the rowid column. If rowid comes from an INTEGER PRIMARY KEY column,
    // it's already in target_column_info.
    let expected_value_count = if resolved_columns.rowid_is_pseudo_column {
        target_column_info.len() + 1
    } else {
        target_column_info.len()
    };
    super::validation::validate_row_column_counts(
        &rows_to_insert,
        expected_value_count,
        table_name,
        !stmt.columns.is_empty(),
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
    // Track previously-validated full rows for self-referential FK lookups
    // (fkey1-5.1: `INSERT INTO t VALUES (1,NULL),(2,1),(3,2)` where t.parent
    // references t.x — row N must find its parent among rows 0..N-1).
    let mut batch_full_rows: Vec<Vec<vibesql_types::SqlValue>> = Vec::new();

    // Check if IGNORE conflict clause is set - if so, skip rows with constraint violations
    // Also treat ON CONFLICT ... DO NOTHING as equivalent to IGNORE
    let or_ignore = matches!(stmt.conflict_clause, Some(vibesql_ast::ConflictClause::Ignore));
    let use_ignore = or_ignore
        || matches!(
            &stmt.on_conflict,
            Some(vibesql_ast::OnConflictClause {
                action: vibesql_ast::OnConflictAction::DoNothing,
                ..
            })
        );

    // `ON CONFLICT (cols) [WHERE ...] DO NOTHING` with an explicit target
    // (and without INSERT OR IGNORE) only suppresses conflicts on the
    // *targeted* constraint; conflicts on other constraints surface as
    // normal UNIQUE errors (upsert1-201). Untargeted DO NOTHING and
    // INSERT OR IGNORE keep the suppress-everything behavior.
    let do_nothing_target: Option<(
        &[vibesql_ast::ConflictTargetItem],
        Option<&vibesql_ast::Expression>,
    )> = match &stmt.on_conflict {
        Some(vibesql_ast::OnConflictClause {
            conflict_target: Some(items),
            target_where,
            action: vibesql_ast::OnConflictAction::DoNothing,
            ..
        }) if !or_ignore => Some((items.as_slice(), target_where.as_ref())),
        _ => None,
    };

    // Track the first auto-generated ID for LAST_INSERT_ROWID() support
    // Per MySQL semantics, for multi-row inserts, LAST_INSERT_ID() returns
    // the first auto-generated value, not the last
    let mut first_generated_id: Option<i64> = None;

    // Track the maximum INTEGER PRIMARY KEY value assigned within this batch
    // to handle multi-row INSERTs with NULL values correctly (SQLite semantics)
    let mut batch_max_ipk: Option<i64> = None;

    // Get INTEGER PRIMARY KEY column index if present
    let ipk_col_idx = schema.get_integer_primary_key_index();

    // Get table's current max rowid for auto-assignment (SQLite semantics)
    // When inserting with explicit rowid column, NULL rowids should be assigned values
    // greater than both:
    // 1. The table's current max rowid
    // 2. Explicit rowids processed so far in the batch (NOT future rows)
    let table_max_rowid =
        db.get_table(&storage_table_name).map(|t| t.row_count() as u64).unwrap_or(0);

    // Track maximum rowid seen so far (updated as we process each row)
    let mut batch_max_rowid = table_max_rowid;

    for value_exprs in &rows_to_insert {
        // Build a complete row with values for all columns
        // Start with NULL for all columns, then fill in provided values
        let mut full_row_values = vec![vibesql_types::SqlValue::Null; schema.columns.len()];

        // Extract rowid value if present (SQLite compatibility)
        // For NULL rowids, auto-assign using batch_max_rowid + 1
        let explicit_rowid = if let Some(rowid_pos) = rowid_position {
            // Get the rowid expression
            let rowid_expr = &value_exprs[rowid_pos];

            // Extract literal value from expression
            // For INTEGER PRIMARY KEY columns (rowid_is_pseudo_column=false), allow any integer
            // For pseudo-columns (rowid, _rowid_, oid), only allow positive integers
            match rowid_expr {
                vibesql_ast::Expression::Literal(val) => {
                    match val {
                        vibesql_types::SqlValue::Integer(i) => {
                            // For pseudo-columns, require positive; for IPK columns, allow any value
                            if resolved_columns.rowid_is_pseudo_column && *i <= 0 {
                                return Err(ExecutorError::UnsupportedExpression(
                                    "datatype mismatch".to_string(),
                                ));
                            }
                            let rowid = *i as u64;
                            // Update batch_max_rowid for subsequent NULL auto-assignments
                            if *i > 0 {
                                batch_max_rowid = batch_max_rowid.max(rowid);
                            }
                            Some(rowid)
                        }
                        vibesql_types::SqlValue::Bigint(i) => {
                            if resolved_columns.rowid_is_pseudo_column && *i <= 0 {
                                return Err(ExecutorError::UnsupportedExpression(
                                    "datatype mismatch".to_string(),
                                ));
                            }
                            let rowid = *i as u64;
                            if *i > 0 {
                                batch_max_rowid = batch_max_rowid.max(rowid);
                            }
                            Some(rowid)
                        }
                        vibesql_types::SqlValue::Null => {
                            // NULL rowid means auto-assign: max_seen + 1
                            batch_max_rowid += 1;
                            Some(batch_max_rowid)
                        }
                        // SQLite type affinity: try to convert numeric strings to integers
                        vibesql_types::SqlValue::Varchar(s)
                        | vibesql_types::SqlValue::Character(s) => {
                            let trimmed = s.trim();
                            if let Ok(i) = trimmed.parse::<i64>() {
                                if resolved_columns.rowid_is_pseudo_column && i <= 0 {
                                    return Err(ExecutorError::UnsupportedExpression(
                                        "datatype mismatch".to_string(),
                                    ));
                                }
                                let rowid = i as u64;
                                if i > 0 {
                                    batch_max_rowid = batch_max_rowid.max(rowid);
                                }
                                Some(rowid)
                            } else {
                                // Non-numeric string cannot be used as rowid
                                return Err(ExecutorError::UnsupportedExpression(
                                    "datatype mismatch".to_string(),
                                ));
                            }
                        }
                        // SQLite type affinity: convert float literals to integers if whole numbers
                        vibesql_types::SqlValue::Float(f) => {
                            let f64_val = *f as f64;
                            if f64_val.fract() == 0.0
                                && f64_val >= i64::MIN as f64
                                && f64_val <= i64::MAX as f64
                            {
                                let i = f64_val as i64;
                                if resolved_columns.rowid_is_pseudo_column && i <= 0 {
                                    return Err(ExecutorError::UnsupportedExpression(
                                        "datatype mismatch".to_string(),
                                    ));
                                }
                                let rowid = i as u64;
                                if i > 0 {
                                    batch_max_rowid = batch_max_rowid.max(rowid);
                                }
                                Some(rowid)
                            } else {
                                return Err(ExecutorError::UnsupportedExpression(
                                    "datatype mismatch".to_string(),
                                ));
                            }
                        }
                        vibesql_types::SqlValue::Real(f)
                        | vibesql_types::SqlValue::Double(f)
                        | vibesql_types::SqlValue::Numeric(f) => {
                            if f.fract() == 0.0 && *f >= i64::MIN as f64 && *f <= i64::MAX as f64 {
                                let i = *f as i64;
                                if resolved_columns.rowid_is_pseudo_column && i <= 0 {
                                    return Err(ExecutorError::UnsupportedExpression(
                                        "datatype mismatch".to_string(),
                                    ));
                                }
                                let rowid = i as u64;
                                if i > 0 {
                                    batch_max_rowid = batch_max_rowid.max(rowid);
                                }
                                Some(rowid)
                            } else {
                                return Err(ExecutorError::UnsupportedExpression(
                                    "datatype mismatch".to_string(),
                                ));
                            }
                        }
                        _ => {
                            return Err(ExecutorError::UnsupportedExpression(
                                "datatype mismatch".to_string(),
                            ));
                        }
                    }
                }
                vibesql_ast::Expression::Default => {
                    // DEFAULT means auto-assign, like NULL
                    batch_max_rowid += 1;
                    Some(batch_max_rowid)
                }
                vibesql_ast::Expression::UnaryOp {
                    op: vibesql_ast::UnaryOperator::Minus,
                    expr,
                } => {
                    // Handle negative integers (parsed as unary minus)
                    match expr.as_ref() {
                        vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Integer(i)) => {
                            let neg_val = -(*i);
                            if resolved_columns.rowid_is_pseudo_column && neg_val <= 0 {
                                return Err(ExecutorError::UnsupportedExpression(
                                    "datatype mismatch".to_string(),
                                ));
                            }
                            // Note: negative rowid as u64 wraps, but SQLite allows this for IPK
                            Some(neg_val as u64)
                        }
                        vibesql_ast::Expression::Literal(vibesql_types::SqlValue::Bigint(i)) => {
                            let neg_val = -(*i);
                            if resolved_columns.rowid_is_pseudo_column && neg_val <= 0 {
                                return Err(ExecutorError::UnsupportedExpression(
                                    "datatype mismatch".to_string(),
                                ));
                            }
                            Some(neg_val as u64)
                        }
                        _ => {
                            return Err(ExecutorError::UnsupportedExpression(
                                "datatype mismatch".to_string(),
                            ));
                        }
                    }
                }
                _ => {
                    // For complex expressions (CASE, functions, subqueries, trigger
                    // pseudo-variables, procedural variables, etc.), evaluate them to get
                    // the resulting value.
                    //
                    // When inserting into an INTEGER PRIMARY KEY column, the column's value
                    // is treated as the rowid and routed through this expression-evaluation
                    // path. If the INSERT runs inside a trigger body, the expression may
                    // reference NEW.x / OLD.x pseudo-variables (e.g.
                    // `INSERT INTO audit(id, ...) VALUES (NEW.id, ...)` where audit.id is an
                    // INTEGER PRIMARY KEY). We must thread the trigger / procedural context
                    // into the evaluator so those references resolve, matching the non-IPK
                    // column path in `evaluate_insert_expression_with_trigger_context`
                    // (issue #5397).
                    //
                    // rowid expressions don't reference columns from a current row, so a
                    // dummy row is sufficient.
                    let dummy_schema =
                        vibesql_catalog::TableSchema::new("__rowid_expr__".to_string(), vec![]);
                    let dummy_row = vibesql_storage::Row::new(vec![]);
                    let mut evaluator = if let Some(ctx) = trigger_context {
                        crate::ExpressionEvaluator::with_trigger_context(
                            ctx.table_schema,
                            db,
                            ctx,
                        )
                    } else if let Some(ctx) = procedural_context {
                        crate::ExpressionEvaluator::with_procedural_context(&dummy_schema, db, ctx)
                    } else {
                        crate::ExpressionEvaluator::with_database(&dummy_schema, db)
                    };
                    if let Some(ref ctes) = cte_results {
                        evaluator = evaluator.with_cte_context(ctes);
                    }
                    let val = evaluator.eval(rowid_expr, &dummy_row)?;

                    match val {
                        vibesql_types::SqlValue::Integer(i) => {
                            if resolved_columns.rowid_is_pseudo_column && i <= 0 {
                                return Err(ExecutorError::UnsupportedExpression(
                                    "datatype mismatch".to_string(),
                                ));
                            }
                            let rowid = i as u64;
                            if i > 0 {
                                batch_max_rowid = batch_max_rowid.max(rowid);
                            }
                            Some(rowid)
                        }
                        vibesql_types::SqlValue::Bigint(i) => {
                            if resolved_columns.rowid_is_pseudo_column && i <= 0 {
                                return Err(ExecutorError::UnsupportedExpression(
                                    "datatype mismatch".to_string(),
                                ));
                            }
                            let rowid = i as u64;
                            if i > 0 {
                                batch_max_rowid = batch_max_rowid.max(rowid);
                            }
                            Some(rowid)
                        }
                        vibesql_types::SqlValue::Null => {
                            // NULL rowid means auto-assign: max_seen + 1
                            batch_max_rowid += 1;
                            Some(batch_max_rowid)
                        }
                        _ => {
                            return Err(ExecutorError::UnsupportedExpression(
                                "datatype mismatch".to_string(),
                            ));
                        }
                    }
                }
            }
        } else {
            None
        };

        // Filter out the rowid value when iterating over column values
        // Only filter if it's a pseudo-column (rowid, _rowid_, oid) that's not in the schema
        // For INTEGER PRIMARY KEY columns, the value IS part of the columns list
        let column_values: Vec<_> = if let Some(rowid_pos) =
            rowid_position.filter(|_| resolved_columns.rowid_is_pseudo_column)
        {
            value_exprs
                .iter()
                .enumerate()
                .filter(|(idx, _)| *idx != rowid_pos)
                .map(|(_, expr)| expr)
                .collect()
        } else {
            value_exprs.iter().collect()
        };

        // Track which columns have been assigned (SQLite uses first occurrence for duplicates)
        let mut assigned_columns = std::collections::HashSet::new();

        for (expr, (col_idx, data_type)) in column_values.iter().zip(target_column_info.iter()) {
            // SQLite behavior: if a column is specified multiple times, use the first value
            // Skip duplicate column assignments
            if assigned_columns.contains(col_idx) {
                continue;
            }
            assigned_columns.insert(*col_idx);

            // Evaluate expression (literals, DEFAULT, procedural variables, and trigger
            // pseudo-variables)
            let value = super::defaults::evaluate_insert_expression_with_trigger_context(
                expr,
                &schema.columns[*col_idx],
                procedural_context,
                trigger_context,
                Some(db),
                cte_results.as_ref(),
            )?;

            // Type check and coerce: ensure value matches column type
            let coerced_value = super::validation::coerce_value(value, data_type)?;

            // INTEGER PRIMARY KEY validation: only accept Integer or Null (for auto-generation)
            // SQLite rejects non-integer values with "datatype mismatch"
            if ipk_col_idx == Some(*col_idx) {
                match &coerced_value {
                    vibesql_types::SqlValue::Integer(_) | vibesql_types::SqlValue::Null => {
                        // Valid for INTEGER PRIMARY KEY
                    }
                    _ => {
                        return Err(crate::errors::ExecutorError::SqliteCompatError(
                            "datatype mismatch".to_string(),
                        ));
                    }
                }
            }

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
        // Also track explicit INTEGER PRIMARY KEY values for last_insert_rowid()
        // SQLite semantics: last_insert_rowid() returns the rowid of the most recently
        // inserted row, whether auto-generated or explicitly provided
        if let Some(idx) = ipk_col_idx {
            if let Some(vibesql_types::SqlValue::Integer(val)) = full_row_values.get(idx) {
                batch_max_ipk = Some(batch_max_ipk.map_or(*val, |prev| prev.max(*val)));
                // Track first explicit INTEGER PRIMARY KEY value for last_insert_rowid()
                if first_generated_id.is_none() {
                    first_generated_id = Some(*val);
                }
            }
        }

        // Validate all constraints in a single pass and extract index keys
        // Skip PK/UNIQUE duplicate checks if using REPLACE conflict clause, ON DUPLICATE KEY
        // UPDATE, or ON CONFLICT clause. Also skip for IGNORE since we'll handle violations
        // by skipping the row.
        //
        // Exception: a *targeted* DO NOTHING (without OR IGNORE) must NOT
        // skip duplicate checks — rows that conflict on the targeted
        // constraint are skipped before validation (below), so any remaining
        // duplicate is on a non-targeted constraint and must raise a normal
        // UNIQUE error (SQLite semantics, upsert1-201).
        let skip_duplicate_checks =
            matches!(stmt.conflict_clause, Some(vibesql_ast::ConflictClause::Replace))
                || matches!(stmt.conflict_clause, Some(vibesql_ast::ConflictClause::Ignore))
                || stmt.on_duplicate_key_update.is_some()
                || (stmt.on_conflict.is_some() && do_nothing_target.is_none());
        let validator = super::row_validator::RowValidator::new(
            db,
            &schema,
            &storage_table_name,
            &primary_key_values,
            &unique_constraint_values,
            &batch_full_rows,
            skip_duplicate_checks,
        );

        // For IGNORE, we need to check for constraint violations before adding to validated_rows
        // If there's a violation, skip this row instead of returning an error
        if use_ignore {
            let would_violate = if let Some((target, target_where)) = do_nothing_target {
                // Targeted DO NOTHING: only conflicts on the targeted
                // constraint are suppressed (expression- and partial-index
                // aware; checks earlier batch rows too — upsert1-320).
                super::on_conflict_update::row_conflicts_on_target(
                    db,
                    table_name,
                    &schema,
                    &full_row_values,
                    target,
                    target_where,
                    &batch_full_rows,
                )?
            } else {
                // OR IGNORE / untargeted DO NOTHING: skip the row on any
                // constraint violation.
                check_would_violate_constraints(
                    db,
                    &schema,
                    &storage_table_name,
                    &full_row_values,
                    &primary_key_values,
                    &unique_constraint_values,
                )
            };
            if would_violate {
                // Skip this row - don't add to validated_rows
                continue;
            }
        }

        let validation_result = validator.validate(&full_row_values)?;
        drop(validator); // release the immutable &Database borrow before the queue push below

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

        // Phase C2 of #5085: queue any FK violations that were deferred
        // (INITIALLY DEFERRED constraint or PRAGMA defer_foreign_keys=ON)
        // onto the active transaction's deferred-FK queue.
        for v in validation_result.deferred_fk_violations {
            db.queue_deferred_fk_violation(v);
        }

        // Track row for self-referential FK lookups by later rows in the
        // same batch (see fkey1-5.1 note above).
        batch_full_rows.push(full_row_values.clone());

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
        // Statement-level RAISE(IGNORE) has no sqlite3 analog (SQLite triggers
        // are always FOR EACH ROW); drop the must-use outcome and keep the
        // pre-#5418 proceed behavior (#5418).
        let _stmt_outcome = crate::TriggerFirer::execute_before_statement_triggers(
            db,
            table_name,
            vibesql_ast::TriggerEvent::Insert,
        )?;
    }

    let mut rows_inserted = 0;

    // Rows taken through the upsert ON CONFLICT DO UPDATE arm. These count
    // toward changes() (rows_inserted) but are excluded from the direct-insert
    // count that PRAGMA count_changes reports for INSERT (issue #5283).
    let mut upsert_updated_rows = 0;

    // RETURNING (SQLite 3.35.0+): collect the rows as ACTUALLY inserted (or
    // updated by ON DUPLICATE KEY UPDATE). The slow path can rewrite the
    // IPK/rowid after validation (REPLACE reserved-rowid interplay), so rows
    // are captured at the insert_row call sites, not from validated_rows.
    let mut returned_rows: Vec<vibesql_storage::Row> = Vec::new();
    let capture_returning = stmt.returning.is_some();

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
        && !matches!(
            &stmt.on_conflict,
            Some(vibesql_ast::OnConflictClause {
                action: vibesql_ast::OnConflictAction::DoUpdate { .. },
                ..
            })
        )
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

        // Track initial row count for expression index maintenance
        let initial_row_count =
            db.get_table(&storage_table_name).map(|t| t.row_count()).unwrap_or(0);

        // If optimal batch size is smaller than total rows, insert in batches
        if optimal_batch_size < validated_rows.len() {
            // Chunked batch insert for high-cost tables
            let mut row_offset = initial_row_count;
            for chunk in validated_rows.chunks(optimal_batch_size) {
                let rows: Vec<vibesql_storage::Row> =
                    chunk.iter().map(|(v, rowid)| make_row((v.clone(), *rowid))).collect();

                // Partial-aware unique-constraint check (storage skips
                // partial UNIQUE indexes; the executor must do this itself).
                for row in &rows {
                    partial_index_maintenance::check_partial_unique_for_insert(
                        db,
                        &storage_table_name,
                        row,
                    )?;
                }

                let chunk_inserted =
                    db.insert_rows_batch(&storage_table_name, rows.clone()).map_err(|e| {
                        ExecutorError::UnsupportedExpression(format!("Storage error: {}", e))
                    })?;

                if capture_returning {
                    returned_rows.extend(rows.iter().cloned());
                }

                // Maintain expression indexes for each inserted row
                for (i, row) in rows.iter().enumerate() {
                    expression_index_maintenance::maintain_expression_indexes_for_insert(
                        db,
                        &storage_table_name,
                        row,
                        row_offset + i,
                    );
                    partial_index_maintenance::maintain_partial_indexes_for_insert(
                        db,
                        &storage_table_name,
                        row,
                        row_offset + i,
                    );
                }
                row_offset += chunk_inserted;
                rows_inserted += chunk_inserted;
            }
        } else {
            // Single batch insert for low-cost tables
            let rows: Vec<vibesql_storage::Row> =
                validated_rows.into_iter().map(make_row).collect();

            // Partial-aware unique-constraint check (storage skips partial
            // UNIQUE indexes; the executor must do this itself).
            for row in &rows {
                partial_index_maintenance::check_partial_unique_for_insert(
                    db,
                    &storage_table_name,
                    row,
                )?;
            }

            rows_inserted =
                db.insert_rows_batch(&storage_table_name, rows.clone()).map_err(|e| {
                    ExecutorError::UnsupportedExpression(format!("Storage error: {}", e))
                })?;

            if capture_returning {
                returned_rows.extend(rows.iter().cloned());
            }

            // Maintain expression indexes for each inserted row
            for (i, row) in rows.iter().enumerate() {
                expression_index_maintenance::maintain_expression_indexes_for_insert(
                    db,
                    &storage_table_name,
                    row,
                    initial_row_count + i,
                );
                partial_index_maintenance::maintain_partial_indexes_for_insert(
                    db,
                    &storage_table_name,
                    row,
                    initial_row_count + i,
                );
            }
        }
    } else {
        // Slow path: Insert rows one by one (needed for triggers, special clauses)
        for (mut full_row_values, mut explicit_rowid) in validated_rows {
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

                if let Some(updated_row_id) = update_result {
                    // Row was updated, count it
                    rows_inserted += 1;

                    // RETURNING: SQLite/MySQL return the post-UPDATE row for
                    // upserts that take the update arm.
                    if capture_returning {
                        if let Some(updated_row) = db
                            .get_table(table_name)
                            .and_then(|table| table.scan().get(updated_row_id))
                        {
                            returned_rows.push(updated_row.clone());
                        }
                    }
                    continue;
                }
                // No conflict, fall through to insert
            } else if let Some(vibesql_ast::OnConflictClause {
                ref conflict_target,
                ref target_where,
                action:
                    vibesql_ast::OnConflictAction::DoUpdate { ref assignments, ref where_clause },
                ..
            }) = stmt.on_conflict
            {
                // SQLite upsert: ON CONFLICT [(cols)] DO UPDATE SET ... [WHERE ...]
                match super::on_conflict_update::handle_on_conflict_update(
                    db,
                    table_name,
                    &schema,
                    &full_row_values,
                    conflict_target.as_deref(),
                    target_where.as_ref(),
                    assignments,
                    where_clause.as_ref(),
                    cte_results.as_ref(),
                )? {
                    super::on_conflict_update::UpsertAction::Updated(updated_row_id) => {
                        // Row was updated, count it toward affected rows
                        rows_inserted += 1;
                        upsert_updated_rows += 1;

                        // RETURNING: SQLite returns the post-UPDATE row for
                        // upserts that take the update arm.
                        if capture_returning {
                            if let Some(updated_row) = db
                                .get_table(table_name)
                                .and_then(|table| table.scan().get(updated_row_id))
                            {
                                returned_rows.push(updated_row.clone());
                            }
                        }
                        continue;
                    }
                    super::on_conflict_update::UpsertAction::Skipped => {
                        // DO UPDATE ... WHERE was false/NULL: the row is
                        // neither inserted nor updated (SQLite semantics).
                        continue;
                    }
                    super::on_conflict_update::UpsertAction::NoConflict => {
                        // No conflict on the targeted constraint; fall through
                        // to a normal insert. Conflicts on OTHER constraints
                        // still surface as UNIQUE errors (upsert1-201).
                    }
                }
            } else if matches!(stmt.conflict_clause, Some(vibesql_ast::ConflictClause::Replace)) {
                // SQLite REPLACE semantics: allocate the rowid for the new row BEFORE firing
                // BEFORE DELETE triggers. This ensures that any INSERT within those triggers
                // that tries to allocate the same rowid will fail with a UNIQUE constraint
                // violation on rowid.
                //
                // Compute the rowid for the new row and reserve it.
                // Track whether the rowid is explicitly specified (INTEGER PRIMARY KEY)
                // or auto-allocated, as this affects how conflicts are handled.
                let is_explicit = explicit_rowid.is_some();
                let reserved_rowid = if let Some(rowid) = explicit_rowid {
                    rowid
                } else {
                    // Auto-allocate: next available rowid is physical row count + 1
                    // (physical includes deleted rows since rowid is based on physical index)
                    let current_physical = db
                        .get_table(&storage_table_name)
                        .map(|t| t.physical_row_count() as u64)
                        .unwrap_or(0);
                    current_physical + 1
                };

                // Reserve the rowid before firing triggers
                db.reserve_rowid(&storage_table_name, reserved_rowid, is_explicit);

                // If REPLACE conflict clause, delete conflicting rows first
                // This also fires DELETE triggers which may create new constraints violations.
                // handle_replace_conflicts() releases the reserved rowid after BEFORE DELETE
                // triggers but before AFTER DELETE triggers.
                let replace_result = super::replace::handle_replace_conflicts(
                    db,
                    table_name,
                    &storage_table_name,
                    &schema,
                    &full_row_values,
                );

                // On error, ensure the reserved rowid is released and propagate the error.
                // Note: The reserved rowid may already be released if the error occurred
                // after BEFORE DELETE triggers, but release_reserved_rowid is idempotent.
                if let Err(e) = replace_result {
                    db.release_reserved_rowid(&storage_table_name);
                    return Err(e);
                }

                // After REPLACE conflict handling (which fires triggers), re-validate constraints.
                // Triggers may have inserted new rows that conflict with the row we want to insert.
                // Pass empty batch values since we're only checking against existing table data.
                // Note: The reserved rowid has already been released by handle_replace_conflicts()
                // after BEFORE DELETE triggers, so no need to release on error here.
                super::constraints::enforce_primary_key_constraint(
                    db,
                    &schema,
                    table_name,
                    &full_row_values,
                    &[], // No batch values to check against
                )?;

                super::constraints::enforce_unique_constraints(
                    db,
                    &schema,
                    table_name,
                    &full_row_values,
                    &[], // No batch values to check against
                )?;

                super::constraints::enforce_unique_indexes(
                    db,
                    &schema,
                    table_name,
                    &full_row_values,
                )?;

                // Use the reserved rowid for the REPLACE INSERT to match SQLite semantics.
                // This ensures the new row gets the rowid that was reserved before triggers fired.
                explicit_rowid = Some(reserved_rowid);

                // Release the reservation now that all delete triggers have fired.
                // The REPLACE INSERT will use explicit_rowid which already has the reserved value.
                // This prevents the REPLACE INSERT from failing on its own reservation.
                db.release_reserved_rowid(&storage_table_name);
            }

            // Fire BEFORE INSERT triggers only if triggers exist
            let row_to_insert = make_row((full_row_values.clone(), explicit_rowid));
            if has_insert_triggers {
                // RAISE(IGNORE) in a BEFORE INSERT trigger abandons this row:
                // skip the insert and continue with the next row (SQLite).
                if crate::TriggerFirer::execute_before_triggers(
                    db,
                    table_name,
                    vibesql_ast::TriggerEvent::Insert,
                    None,
                    Some(&row_to_insert),
                )? == crate::trigger_execution::TriggerOutcome::SkipRow
                {
                    continue;
                }
            }

            // Get physical row count before insert to enable rollback and rowid calculation
            let table_ref = db
                .get_table(&storage_table_name)
                .ok_or_else(|| ExecutorError::TableNotFound(full_table_name.clone()))?;
            let row_count_before = table_ref.row_count();
            let physical_row_count_before = table_ref.physical_row_count();

            // SQLite REPLACE semantics: Check if the rowid we're about to use is reserved.
            // During REPLACE, the rowid for the new row is allocated BEFORE firing triggers.
            // The behavior depends on the type of reservation:
            // - Auto-allocated reservation: BEFORE DELETE trigger INSERTs fail on rowid conflict
            // - Explicit reservation: AFTER DELETE trigger auto-INSERTs skip to next rowid
            //   (SQLite reuses freed rowids, but VibeSQL doesn't, so we skip instead)
            let mut final_explicit_rowid = explicit_rowid;
            let rowid_to_use = explicit_rowid.unwrap_or((physical_row_count_before + 1) as u64);
            if let Some((reserved_rowid, is_explicit_reservation)) =
                db.get_reserved_rowid_info(&storage_table_name)
            {
                if rowid_to_use == reserved_rowid {
                    if !is_explicit_reservation {
                        // Auto-allocated REPLACE rowid: trigger INSERT must fail
                        // Use actual column name for INTEGER PRIMARY KEY columns
                        let col_name = ipk_col_idx
                            .and_then(|idx| schema.columns.get(idx))
                            .map(|col| col.name.as_str())
                            .unwrap_or("rowid");
                        return Err(ExecutorError::SqliteCompatError(format!(
                            "UNIQUE constraint failed: {}.{}",
                            table_name, col_name
                        )));
                    } else if explicit_rowid.is_none() {
                        // Explicit REPLACE rowid: auto-allocated trigger INSERT skips to next rowid
                        // This approximates SQLite's rowid reuse behavior
                        let new_rowid = reserved_rowid + 1;
                        final_explicit_rowid = Some(new_rowid);

                        // Also update the INTEGER PRIMARY KEY column value to match the new rowid
                        // The column value was auto-generated earlier but now we're using a different rowid
                        if let Some(ipk_idx) = ipk_col_idx {
                            full_row_values[ipk_idx] =
                                vibesql_types::SqlValue::Integer(new_rowid as i64);
                        }
                    } else {
                        // Explicit REPLACE rowid and explicit trigger INSERT with same rowid: fail
                        // Use actual column name for INTEGER PRIMARY KEY columns
                        let col_name = ipk_col_idx
                            .and_then(|idx| schema.columns.get(idx))
                            .map(|col| col.name.as_str())
                            .unwrap_or("rowid");
                        return Err(ExecutorError::SqliteCompatError(format!(
                            "UNIQUE constraint failed: {}.{}",
                            table_name, col_name
                        )));
                    }
                }
            }

            // Insert the row
            let row = make_row((full_row_values, final_explicit_rowid));
            // Partial-aware unique-constraint check (storage skips partial
            // UNIQUE indexes; the executor must do that check itself).
            partial_index_maintenance::check_partial_unique_for_insert(
                db,
                &storage_table_name,
                &row,
            )?;
            db.insert_row(&storage_table_name, row.clone()).map_err(|e| {
                ExecutorError::UnsupportedExpression(format!("Storage error: {}", e))
            })?;

            // RETURNING: capture the row exactly as inserted (after any
            // REPLACE reserved-rowid / IPK rewrites above).
            if capture_returning {
                returned_rows.push(row.clone());
            }

            // Maintain expression indexes for this insert
            expression_index_maintenance::maintain_expression_indexes_for_insert(
                db,
                &storage_table_name,
                &row,
                row_count_before,
            );
            partial_index_maintenance::maintain_partial_indexes_for_insert(
                db,
                &storage_table_name,
                &row,
                row_count_before,
            );

            // Fire AFTER INSERT triggers only if triggers exist.
            //
            // On a trigger error we simply propagate it: the statement-scope
            // machinery in `raise_scope::run_top_level_dml` (which wraps every
            // top-level INSERT) handles undoing the right amount of work, exactly
            // matching sqlite3 3.51 (#5464, #5474):
            //   - RAISE(ABORT)/RAISE(ROLLBACK)/any non-RAISE trigger error → the
            //     statement savepoint (in an explicit txn) or implicit
            //     transaction (auto-commit) rolls back the *whole* statement,
            //     removing this offending row AND every earlier row.
            //   - RAISE(FAIL) → the partial changes the statement already applied
            //     are KEPT, including this offending row (which was inserted
            //     before its AFTER trigger fired). SQLite keeps it; so do we.
            //
            // Previously this site unconditionally tombstoned the just-inserted
            // offending row on any trigger error. That was redundant for
            // ABORT/non-RAISE (rollback already removes it) and *wrong* for FAIL
            // (it hid a row SQLite keeps — visible only as a bitmap tombstone in
            // raw `Table::scan()`, missing from any live SELECT). See #5474.
            if has_insert_triggers {
                // A `TriggerOutcome::SkipRow` (RAISE(IGNORE) in an AFTER trigger)
                // has no SQLite-observable effect here: the row is already
                // inserted and RAISE(IGNORE) only abandons the rest of the
                // trigger program, leaving the row in place. So we only act on
                // the error (RAISE / non-RAISE) path; drop the Ok outcome.
                let _after_outcome = crate::TriggerFirer::execute_after_triggers(
                    db,
                    table_name,
                    vibesql_ast::TriggerEvent::Insert,
                    None,
                    Some(&row),
                )?;
            }

            rows_inserted += 1;
        }
    }

    // Fire AFTER STATEMENT triggers only if triggers exist AND we're not inside a trigger context
    // (Statement-level triggers don't fire for inserts within trigger bodies)
    if has_insert_triggers && trigger_context.is_none() {
        // Statement-level RAISE(IGNORE) has no sqlite3 analog; drop the
        // must-use outcome (#5418).
        let _stmt_outcome = crate::TriggerFirer::execute_after_statement_triggers(
            db,
            table_name,
            vibesql_ast::TriggerEvent::Insert,
        )?;
    }

    // Update LAST_INSERT_ROWID if any auto-generated values were produced
    // SQLite: last_insert_rowid() is NOT updated for WITHOUT ROWID tables
    // (see SQLite documentation R-47220-63683)
    if let Some(id) = first_generated_id {
        if !schema.without_rowid {
            db.set_last_insert_rowid(id);
        }
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

    // Project the RETURNING clause against the rows actually inserted/updated
    // (one result row per affected row, in insertion order). Zero affected
    // rows still yield an empty result with the derived column headers.
    let returning_result = if let Some(items) = &stmt.returning {
        let row_refs: Vec<&vibesql_storage::Row> = returned_rows.iter().collect();
        Some(crate::dml_returning::project_returning(
            items,
            &schema,
            db,
            None,
            &row_refs,
            cte_results.as_ref(),
        )?)
    } else {
        None
    };

    Ok(InsertOutcome {
        affected_rows: rows_inserted,
        upsert_updated_rows,
        returning: returning_result,
    })
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
        // Lazily-built evaluator context for expression-index components and
        // partial-index predicates (issue #5278: expect_column_name() on an
        // expression component used to panic here).
        let candidate_row = vibesql_storage::Row::new(row_values.to_vec());
        let evaluator = crate::evaluator::ExpressionEvaluator::new(schema);

        for index_name in db.list_indexes_for_table(table_name) {
            if let Some(index_metadata) = db.get_index(&index_name) {
                if !index_metadata.unique {
                    continue;
                }

                // Partial index: a row that doesn't satisfy the predicate is
                // never added to the index, so it cannot conflict through it.
                if let Some(predicate) = index_metadata.where_clause.as_deref() {
                    let satisfied = evaluator
                        .eval(predicate, &candidate_row)
                        .map(|v| crate::partial_index_maintenance::is_predicate_truthy(&v))
                        .unwrap_or(false);
                    if !satisfied {
                        continue;
                    }
                }

                // Build key values for this index (evaluating expression
                // components against the candidate row; evaluation failures
                // become NULL, matching expression-index maintenance).
                let mut key_values = Vec::new();
                for index_col in &index_metadata.columns {
                    if let Some(name) = index_col.column_name() {
                        if let Some(col_idx) = schema.get_column_index(name) {
                            key_values.push(row_values[col_idx].clone());
                        }
                    } else if let Some(expr) = index_col.get_expression() {
                        key_values.push(
                            evaluator
                                .eval(expr, &candidate_row)
                                .unwrap_or(vibesql_types::SqlValue::Null),
                        );
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
    cte_results: Option<&std::collections::HashMap<String, crate::select::cte::CteResult>>,
) -> Result<(usize, Option<crate::select::SelectResult>), ExecutorError> {
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
        vibesql_ast::InsertSource::DefaultValues => {
            // For DEFAULT VALUES on a view, create a single row with DEFAULT for all target columns
            let target_col_count = if stmt.columns.is_empty() {
                view_schema.columns.len()
            } else {
                stmt.columns.len()
            };
            let default_row = vec![vibesql_ast::Expression::Default; target_col_count];
            vec![default_row]
        }
        vibesql_ast::InsertSource::Select(select_stmt) => {
            // Execute SELECT and convert to expressions, with the enclosing
            // statement's CTEs (if any) visible to the source query.
            let select_result = if let Some(ctes) = cte_results {
                crate::SelectExecutor::new_with_cte(db, ctes).execute_with_columns(select_stmt)?
            } else {
                crate::SelectExecutor::new(db).execute_with_columns(select_stmt)?
            };
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
        let mut evaluator = if let Some(ctx) = trigger_context {
            crate::evaluator::ExpressionEvaluator::with_trigger_context(&view_schema, db, ctx)
        } else if let Some(ctx) = procedural_context {
            crate::evaluator::ExpressionEvaluator::with_procedural_context(&view_schema, db, ctx)
        } else {
            crate::evaluator::ExpressionEvaluator::with_database(&view_schema, db)
        };
        if let Some(ctes) = cte_results {
            evaluator = evaluator.with_cte_context(ctes);
        }

        let mut collected_rows = Vec::new();
        for value_exprs in &rows_to_insert {
            // Validate column count
            if value_exprs.len() != target_columns.len() {
                return Err(ExecutorError::InsertColumnCountMismatch {
                    table_name: view_def.name.clone(),
                    expected: target_columns.len(),
                    provided: value_exprs.len(),
                    has_explicit_columns: !stmt.columns.is_empty(),
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

    // RETURNING: project the NEW view rows (one per INSTEAD OF trigger fire),
    // mirroring the UPDATE/DELETE view semantics. Projected before the
    // triggers run since the database must be borrowed mutably below.
    let returning_result = if let Some(items) = &stmt.returning {
        let row_refs: Vec<&vibesql_storage::Row> = new_rows.iter().collect();
        Some(crate::dml_returning::project_returning(
            items,
            &view_schema,
            db,
            None,
            &row_refs,
            cte_results,
        )?)
    } else {
        None
    };

    // Now fire triggers (database can be mutably borrowed).
    //
    // RAISE(IGNORE) inside an INSTEAD OF INSERT trigger abandons the view
    // operation for that row (#5418): the trigger body stops at the RAISE
    // (`execute_trigger` returns SkipRow) and we move on to the next row. On
    // SkipRow we `break` out of this row's remaining INSTEAD OF triggers,
    // following the first-SkipRow-wins convention of the primary DML loops
    // (#5415). Verified against sqlite3 3.51: a RAISE(IGNORE) before the body's
    // `INSERT INTO base` skips that base insert while later rows proceed.
    let rows_processed = new_rows.len();
    for row in new_rows {
        for trigger in &triggers {
            if crate::TriggerFirer::execute_trigger(db, trigger, None, Some(&row))?
                == crate::trigger_execution::TriggerOutcome::SkipRow
            {
                break;
            }
        }
    }

    Ok((rows_processed, returning_result))
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

/// Execute INSERT into sqlite_stat1 virtual table
///
/// This special handler allows users to manually insert statistics for query optimizer tuning,
/// matching SQLite's behavior where sqlite_stat1 is writable.
///
/// sqlite_stat1 schema: (tbl TEXT, idx TEXT, stat TEXT)
fn execute_insert_sqlite_stat1(
    db: &mut vibesql_storage::Database,
    stmt: &vibesql_ast::InsertStmt,
) -> Result<usize, ExecutorError> {
    use vibesql_ast::InsertSource;

    // Get column indices for tbl, idx, stat
    // Expected columns: tbl (required), idx (nullable), stat (required)
    let columns = &stmt.columns;
    let (tbl_idx, idx_idx, stat_idx) = if columns.is_empty() {
        // Default order: tbl, idx, stat
        (0usize, Some(1usize), 2usize)
    } else {
        // Find column positions
        let tbl_idx = columns.iter().position(|c| c.eq_ignore_ascii_case("tbl"));
        let idx_idx = columns.iter().position(|c| c.eq_ignore_ascii_case("idx"));
        let stat_idx = columns.iter().position(|c| c.eq_ignore_ascii_case("stat"));

        let tbl_idx = tbl_idx.ok_or_else(|| {
            ExecutorError::Other("sqlite_stat1 INSERT requires 'tbl' column".to_string())
        })?;
        let stat_idx = stat_idx.ok_or_else(|| {
            ExecutorError::Other("sqlite_stat1 INSERT requires 'stat' column".to_string())
        })?;

        (tbl_idx, idx_idx, stat_idx)
    };

    // Process VALUES clause
    let values = match &stmt.source {
        InsertSource::Values(v) => v,
        InsertSource::Select(_) => {
            return Err(ExecutorError::Other(
                "INSERT INTO sqlite_stat1 ... SELECT is not supported".to_string(),
            ));
        }
        InsertSource::DefaultValues => {
            return Err(ExecutorError::Other(
                "INSERT INTO sqlite_stat1 DEFAULT VALUES is not supported".to_string(),
            ));
        }
    };

    let mut rows_inserted = 0;

    for row in values {
        // Extract tbl value
        let tbl = extract_string_value(&row[tbl_idx]).ok_or_else(|| {
            ExecutorError::Other(format!("sqlite_stat1.tbl must be TEXT, got {:?}", row[tbl_idx]))
        })?;

        // Extract idx value (nullable)
        let idx = if let Some(idx_pos) = idx_idx {
            if idx_pos < row.len() {
                extract_string_value(&row[idx_pos])
            } else {
                None
            }
        } else {
            None
        };

        // Extract stat value
        let stat = extract_string_value(&row[stat_idx]).ok_or_else(|| {
            ExecutorError::Other(format!("sqlite_stat1.stat must be TEXT, got {:?}", row[stat_idx]))
        })?;

        // Insert into database's sqlite_stat1 storage
        db.insert_sqlite_stat1(tbl, idx, stat);
        rows_inserted += 1;
    }

    Ok(rows_inserted)
}

/// Extract a string value from an expression (for sqlite_stat1 INSERT)
fn extract_string_value(expr: &vibesql_ast::Expression) -> Option<String> {
    use vibesql_ast::Expression;
    use vibesql_types::SqlValue;

    match expr {
        Expression::Literal(SqlValue::Varchar(s)) => Some(s.to_string()),
        Expression::Literal(SqlValue::Null) => None,
        // Handle string literals that may not be wrapped in SqlValue
        _ => None,
    }
}
