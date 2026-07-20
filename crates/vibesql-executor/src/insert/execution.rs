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

/// Result of applying SQLite rowid (INTEGER) affinity to a value supplied for
/// the rowid / INTEGER PRIMARY KEY position on INSERT.
enum RowidAffinity {
    /// The value coerced to a concrete integer rowid.
    Value(i64),
    /// The value was NULL (or DEFAULT) — auto-assign the next rowid.
    Auto,
}

/// Apply SQLite's rowid (INTEGER) affinity to an already-evaluated value.
///
/// SQLite coerces the value supplied for a rowid / INTEGER PRIMARY KEY into an
/// integer before storing it (and before triggers observe `NEW.rowid`):
/// - integers pass through;
/// - a TEXT value that is losslessly an integer (e.g. `'45'`) is coerced;
/// - a REAL value that is losslessly an integer (e.g. `45.0`, `-42.0`) is coerced; a value with a
///   fractional part (e.g. `42.4`) cannot be a rowid and raises `datatype mismatch`;
/// - NULL means auto-assign;
/// - anything else (non-numeric TEXT, BLOB, fractional REAL) raises `datatype mismatch`, matching
///   sqlite3 3.51.
///
/// sqlite3 3.51.0 accepts any integer rowid, including zero and negatives
/// (`INSERT INTO t(rowid) VALUES(-42.0)` stores rowid -42; triggerC-4.1.4), so
/// no positivity restriction is applied — only the affinity coercion and the
/// lossless-integer check.
fn coerce_rowid_affinity(value: &vibesql_types::SqlValue) -> Result<RowidAffinity, ExecutorError> {
    use vibesql_types::SqlValue;

    let datatype_mismatch =
        || ExecutorError::UnsupportedExpression("datatype mismatch".to_string());

    // Coerce a lossless-integer f64 to i64, else datatype mismatch.
    let from_float = |f: f64| -> Result<i64, ExecutorError> {
        if f.fract() == 0.0 && f >= i64::MIN as f64 && f <= i64::MAX as f64 {
            Ok(f as i64)
        } else {
            Err(datatype_mismatch())
        }
    };

    let i = match value {
        SqlValue::Null => return Ok(RowidAffinity::Auto),
        SqlValue::Integer(i) => *i,
        SqlValue::Bigint(i) => *i,
        SqlValue::Smallint(i) => *i as i64,
        SqlValue::Varchar(s) | SqlValue::Character(s) => {
            let trimmed = s.trim();
            if let Ok(i) = trimmed.parse::<i64>() {
                i
            } else if let Ok(f) = trimmed.parse::<f64>() {
                from_float(f)?
            } else {
                return Err(datatype_mismatch());
            }
        }
        SqlValue::Float(f) => from_float(*f as f64)?,
        SqlValue::Real(f) | SqlValue::Double(f) | SqlValue::Numeric(f) => from_float(*f)?,
        _ => return Err(datatype_mismatch()),
    };

    Ok(RowidAffinity::Value(i))
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
        if !stmt.on_conflict.is_empty() {
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

    // Schema-aware trigger firing (triggerD-3.1/3.2): resolve which schema this
    // INSERT's target table actually lives in (temp shadows main for unqualified
    // names; an explicit `main.`/`temp.` qualifier pins the schema). Only triggers
    // bound to this schema will fire, so a `main` trigger does not fire on a
    // same-named `temp` table and vice versa. `None` => could not resolve, in
    // which case firing falls back to the legacy schema-unaware match.
    let dml_schema: Option<String> = db.catalog.resolve_table_schema_name(&full_table_name);

    // Validate INSERT trigger body statements at prepare time (mirrors the
    // DELETE path). SQLite resolves a trigger's body when the firing INSERT is
    // prepared, so a body with an unresolvable column reference (triggerB-2.1's
    // `SELECT wen.x`) or a mis-arity scalar subquery (#6046) errors *before* the
    // INSERT's own constraint checks run. Only at the top level (not inside
    // another trigger's body), matching SQLite's prepare of the outermost
    // statement.
    if trigger_context.is_none() {
        crate::TriggerFirer::validate_trigger_bodies_for_event(
            db,
            table_name,
            vibesql_ast::TriggerEvent::Insert,
            dml_schema.as_deref(),
        )?;
    }

    // Validate every explicit ON CONFLICT (cols) target up front. SQLite
    // does this at prepare time, even when no row actually conflicts:
    // unknown columns raise "no such column" and known columns without a
    // matching PRIMARY KEY / UNIQUE constraint / unique index raise the
    // canonical "ON CONFLICT clause does not match..." error
    // (upsert1-110/120/300). With multiple clauses (generalized UPSERT,
    // upsert5), each clause's target is validated in order.
    for on_conflict in &stmt.on_conflict {
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
            // For INTEGER PRIMARY KEY columns, the rowid value is already part of
            // target_column_info.
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
    // Each entry is (row values, explicit rowid, ipk_auto_assigned,
    // candidate_last_id). The `ipk_auto_assigned` flag records whether the row's
    // INTEGER PRIMARY KEY (rowid alias) value was auto-allocated rather than
    // supplied by the INSERT; it is used to present `NEW.<ipk>` as the
    // unwritten-rowid sentinel (-1) to BEFORE INSERT triggers.
    //
    // `candidate_last_id` is the value this row would contribute to
    // last_insert_rowid() *if it is actually inserted* — the row's INTEGER
    // PRIMARY KEY (rowid alias) value, or the first auto-generated sequence
    // value for non-IPK tables. It is committed to `last_generated_id` only at
    // the real insert call sites (below), so a row that takes the ON CONFLICT
    // DO UPDATE arm — or is skipped by OR IGNORE / DO NOTHING — does not update
    // last_insert_rowid() (SQLite excludes pure updates and skips; issue #5886).
    let mut validated_rows: Vec<(Vec<vibesql_types::SqlValue>, Option<u64>, bool, Option<i64>)> =
        Vec::new();
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
    // Also treat a single-clause ON CONFLICT ... DO NOTHING as equivalent to
    // IGNORE. Multi-clause statements (generalized UPSERT, upsert5) resolve
    // conflicts per row in the slow path instead, because clause selection
    // depends on WHICH constraint each row violates.
    let or_ignore = matches!(stmt.conflict_clause, Some(vibesql_ast::ConflictClause::Ignore));
    let use_ignore = or_ignore
        || matches!(
            stmt.on_conflict.as_slice(),
            [vibesql_ast::OnConflictClause {
                action: vibesql_ast::OnConflictAction::DoNothing,
                ..
            }]
        );

    // `ON CONFLICT (cols) [WHERE ...] DO NOTHING` with an explicit target
    // (and without INSERT OR IGNORE) only suppresses conflicts on the
    // *targeted* constraint; conflicts on other constraints surface as
    // normal UNIQUE errors (upsert1-201). Untargeted DO NOTHING and
    // INSERT OR IGNORE keep the suppress-everything behavior.
    let do_nothing_target: Option<(
        &[vibesql_ast::ConflictTargetItem],
        Option<&vibesql_ast::Expression>,
    )> = match stmt.on_conflict.as_slice() {
        [vibesql_ast::OnConflictClause {
            conflict_target: Some(items),
            target_where,
            action: vibesql_ast::OnConflictAction::DoNothing,
            ..
        }] if !or_ignore => Some((items.as_slice(), target_where.as_ref())),
        _ => None,
    };

    // Statements whose ON CONFLICT clauses cannot be fully resolved in the
    // validation loop below need per-row clause dispatch in the slow path:
    // any DO UPDATE clause, and any multi-clause statement (leftmost-match
    // clause selection against the violated constraint — upsert5). A single
    // DO NOTHING clause is fully handled by use_ignore / do_nothing_target
    // above and keeps the batch-insert fast path.
    let on_conflict_needs_row_dispatch = !stmt.on_conflict.is_empty()
        && !matches!(
            stmt.on_conflict.as_slice(),
            [vibesql_ast::OnConflictClause {
                action: vibesql_ast::OnConflictAction::DoNothing,
                ..
            }]
        );

    // Track the most recently generated ID for last_insert_rowid() support.
    // SQLite semantics: last_insert_rowid() returns the rowid of the most
    // recently inserted row, so for multi-row INSERT ... VALUES / INSERT ... SELECT
    // the LAST row in the batch wins (last writer wins).
    let mut last_generated_id: Option<i64> = None;

    // Track the maximum INTEGER PRIMARY KEY value assigned within this batch
    // to handle multi-row INSERTs with NULL values correctly (SQLite semantics)
    let mut batch_max_ipk: Option<i64> = None;

    // Get INTEGER PRIMARY KEY column index if present
    let ipk_col_idx = schema.get_integer_primary_key_index();

    // Get table's current max rowid for auto-assignment (SQLite semantics)
    // When inserting with explicit rowid column, NULL rowids should be assigned values
    // greater than both:
    // 1. The table's current max *signed* rowid (`max_rowid_signed()`: covers
    //    every effective rowid ever assigned — implicit positions AND explicit
    //    rowids, including rowids reloaded from disk, which can exceed the
    //    compacted physical count; issue #5835). SQLite rowids are signed:
    //    a table whose only rowid is -1 auto-assigns 0 next (verified against
    //    sqlite3), and `None` (no rows ever) auto-assigns 1.
    // 2. Explicit rowids processed so far in the batch (NOT future rows)
    let table_max_rowid: Option<i64> =
        db.get_table(&storage_table_name).and_then(|t| t.max_rowid_signed());

    // Track maximum signed rowid seen so far (updated as we process each row)
    let mut batch_max_rowid: Option<i64> = table_max_rowid;

    for value_exprs in &rows_to_insert {
        // Build a complete row with values for all columns
        // Start with NULL for all columns, then fill in provided values
        let mut full_row_values = vec![vibesql_types::SqlValue::Null; schema.columns.len()];

        // Extract rowid value if present (SQLite compatibility)
        // For NULL rowids, auto-assign using batch_max_rowid + 1
        let explicit_rowid = if let Some(rowid_pos) = rowid_position {
            // Get the rowid expression
            let rowid_expr = &value_exprs[rowid_pos];

            // Apply a coerced rowid affinity result, updating batch_max_rowid
            // for subsequent NULL/DEFAULT auto-assignments. SQLite's INTEGER
            // affinity is applied to the rowid value before storage (and before
            // triggers observe NEW.rowid): a TEXT/REAL value that is losslessly
            // an integer is coerced (e.g. '45' -> 45, -42.0 -> -42), a value
            // with a fractional part (e.g. 42.4) or a non-numeric TEXT/BLOB
            // raises "datatype mismatch" (triggerC-4.1.x).
            // Rowids are signed (issue #5835): explicit values are tracked
            // under signed comparison (a negative rowid raises the max only
            // if the max is even more negative, matching sqlite3's
            // `max(existing) + 1` allocation), and NULL/DEFAULT auto-assigns
            // signed-max + 1 (or 1 for a table that never held a row).
            // The returned u64 is the two's-complement bit pattern stored in
            // `Row::row_id`.
            let mut apply_affinity = |affinity: RowidAffinity| -> Result<u64, ExecutorError> {
                match affinity {
                    RowidAffinity::Value(i) => {
                        batch_max_rowid = Some(batch_max_rowid.map_or(i, |m| m.max(i)));
                        Ok(i as u64)
                    }
                    RowidAffinity::Auto => {
                        // sqlite3 parity (issue #5894): max rowid + 1, but when the
                        // running max is already i64::MAX, delegate to the storage
                        // allocator's random-probe / SQLITE_FULL fallback instead of
                        // saturating (which would silently reuse i64::MAX — a
                        // duplicate rowid). The table is only consulted in that
                        // saturated corner, so the common path stays allocation-free.
                        let next = match batch_max_rowid {
                            None => 1,
                            Some(m) if m < i64::MAX => m + 1,
                            Some(_) => db
                                .get_table(&storage_table_name)
                                .map(|t| t.allocate_rowid())
                                .unwrap_or(Ok(1))
                                .map_err(|e| ExecutorError::StorageError(e.to_string()))?,
                        };
                        batch_max_rowid = Some(next);
                        Ok(next as u64)
                    }
                }
            };

            match rowid_expr {
                // Literal value: apply rowid (INTEGER) affinity directly.
                vibesql_ast::Expression::Literal(val) => {
                    Some(apply_affinity(coerce_rowid_affinity(val)?)?)
                }
                // DEFAULT means auto-assign, like NULL.
                vibesql_ast::Expression::Default => Some(apply_affinity(RowidAffinity::Auto)?),
                // Negative numeric literal (parsed as unary minus over a literal):
                // negate, then apply rowid affinity. This handles -42 (integer)
                // AND -42.0 (real) uniformly (triggerC-4.1.4).
                vibesql_ast::Expression::UnaryOp {
                    op: vibesql_ast::UnaryOperator::Minus,
                    expr,
                } if matches!(expr.as_ref(), vibesql_ast::Expression::Literal(_)) => {
                    let vibesql_ast::Expression::Literal(inner) = expr.as_ref() else {
                        unreachable!("guarded by matches! above")
                    };
                    let negated = match inner {
                        vibesql_types::SqlValue::Integer(i) => {
                            vibesql_types::SqlValue::Integer(-*i)
                        }
                        vibesql_types::SqlValue::Bigint(i) => vibesql_types::SqlValue::Bigint(-*i),
                        vibesql_types::SqlValue::Smallint(i) => {
                            vibesql_types::SqlValue::Smallint(-*i)
                        }
                        vibesql_types::SqlValue::Float(f) => vibesql_types::SqlValue::Float(-*f),
                        vibesql_types::SqlValue::Real(f) => vibesql_types::SqlValue::Real(-*f),
                        vibesql_types::SqlValue::Double(f) => vibesql_types::SqlValue::Double(-*f),
                        vibesql_types::SqlValue::Numeric(f) => {
                            vibesql_types::SqlValue::Numeric(-*f)
                        }
                        _ => {
                            return Err(ExecutorError::UnsupportedExpression(
                                "datatype mismatch".to_string(),
                            ));
                        }
                    };
                    Some(apply_affinity(coerce_rowid_affinity(&negated)?)?)
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
                        crate::ExpressionEvaluator::with_trigger_context(ctx.table_schema, db, ctx)
                    } else if let Some(ctx) = procedural_context {
                        crate::ExpressionEvaluator::with_procedural_context(&dummy_schema, db, ctx)
                    } else {
                        crate::ExpressionEvaluator::with_database(&dummy_schema, db)
                    };
                    if let Some(ref ctes) = cte_results {
                        evaluator = evaluator.with_cte_context(ctes);
                    }
                    let val = evaluator.eval(rowid_expr, &dummy_row)?;

                    // Apply rowid (INTEGER) affinity to the evaluated value,
                    // matching the literal path (triggerC-4.1.x).
                    Some(apply_affinity(coerce_rowid_affinity(&val)?)?)
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

            // Type check and coerce: ensure value matches column type.
            // STRICT tables (issue #5837) apply SQLite's rigid strict-datatype
            // rules (with lossless coercions) in place of the default affinity
            // coercion, erroring with `cannot store <T> value in <T> column ...`.
            let coerced_value = if let Some(st) = schema.strict_type_of(*col_idx) {
                crate::strict::enforce_strict_type(
                    value,
                    st,
                    &schema.name,
                    &schema.columns[*col_idx].name,
                )?
            } else {
                super::validation::coerce_value(value, data_type)?
            };

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

        // Determine whether the INTEGER PRIMARY KEY (rowid alias) value will be
        // auto-allocated by the default-value pass below, rather than being
        // supplied by this INSERT. SQLite does not allocate the real rowid until
        // the row is written (AFTER the BEFORE INSERT trigger fires), so inside a
        // BEFORE INSERT trigger `NEW.<ipk>` / `NEW.rowid` must read as the
        // unwritten-rowid sentinel (-1) when auto-allocated, but as the supplied
        // value when explicitly provided (insert3-2.1/2.2, triggerC-4.1.2).
        //
        // The IPK is auto-allocated when neither an explicit rowid pseudo-column
        // value (`explicit_rowid`) nor a non-NULL named IPK column value was
        // provided. We check the IPK slot here, *before* the default pass fills
        // it in.
        let ipk_auto_assigned = ipk_col_idx.is_some_and(|idx| {
            explicit_rowid.is_none()
                && matches!(full_row_values.get(idx), Some(vibesql_types::SqlValue::Null))
        });

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
            &assigned_columns,
        )?;

        // Apply generated/computed column values (AS(expression) syntax)
        super::defaults::apply_generated_columns(&schema, &mut full_row_values, db)?;

        // Update batch_max_ipk if this row has an INTEGER PRIMARY KEY value.
        // (last_insert_rowid() tracking happens later, only for rows that are
        // actually inserted — a row skipped by OR IGNORE must not update it.)
        if let Some(idx) = ipk_col_idx {
            if let Some(vibesql_types::SqlValue::Integer(val)) = full_row_values.get(idx) {
                batch_max_ipk = Some(batch_max_ipk.map_or(*val, |prev| prev.max(*val)));
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
                || (!stmt.on_conflict.is_empty() && do_nothing_target.is_none());
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

        // Compute the value this row would contribute to last_insert_rowid()
        // *if it is actually inserted*. We commit it to `last_generated_id` only
        // at the real insert call sites below (never in this validation loop),
        // so a row that later takes the ON CONFLICT DO UPDATE arm, is skipped by
        // a DO NOTHING clause, or is dropped by OR IGNORE does NOT update
        // last_insert_rowid() — SQLite excludes pure updates and skipped rows
        // (issue #5886). For rowid tables the value is the INTEGER PRIMARY KEY
        // (whether explicit or auto-assigned); otherwise fall back to any
        // auto-generated sequence value.
        let candidate_last_id = if let Some(idx) = ipk_col_idx {
            match full_row_values.get(idx) {
                Some(vibesql_types::SqlValue::Integer(val)) => Some(*val),
                _ => None,
            }
        } else {
            generated_id
        };

        // Store validated row for insertion (with optional explicit rowid)
        validated_rows.push((
            full_row_values,
            explicit_rowid,
            ipk_auto_assigned,
            candidate_last_id,
        ));
    }

    // All rows validated successfully, now insert them

    // Check once if any INSERT triggers exist for this table (used for batch optimization)
    let has_insert_triggers = db
        .catalog
        .get_triggers_for_table_in_schema(
            table_name,
            Some(vibesql_ast::TriggerEvent::Insert),
            dml_schema.as_deref(),
        )
        .next()
        .is_some();

    // Fire BEFORE STATEMENT triggers only if triggers exist AND we're not inside a trigger context
    // (Statement-level triggers don't fire for inserts within trigger bodies)
    if has_insert_triggers && trigger_context.is_none() {
        // Statement-level RAISE(IGNORE) has no sqlite3 analog (SQLite triggers
        // are always FOR EACH ROW); drop the must-use outcome and keep the
        // pre-#5418 proceed behavior (#5418).
        let _stmt_outcome = crate::TriggerFirer::execute_before_statement_triggers_in_schema(
            db,
            table_name,
            vibesql_ast::TriggerEvent::Insert,
            dml_schema.as_deref(),
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

    // Per-row RETURNING subquery path (parity with DELETE/UPDATE, #5972/#5976).
    // A subquery in a RETURNING expression is treated as correlated with the
    // table being modified: it must recompute against the incremental table
    // state after each row is inserted, so every returned row observes the
    // count/sum/etc. as of its own insertion (returning1.test section 20).
    // Subquery-free RETURNING keeps the cheaper statement-end batch projection
    // with zero behavior change.
    let returning_needs_per_row =
        stmt.returning.as_deref().is_some_and(crate::dml_returning::returning_has_subquery);
    // Columns and the visible-column set are derivable once (before the loop):
    // they do not depend on per-row table state.
    let (per_row_returning_columns, per_row_visible_columns) = if returning_needs_per_row {
        let items = stmt.returning.as_deref().expect("returning present");
        let visible_columns = crate::dml_returning::visible_columns(&schema);
        let columns =
            crate::dml_returning::derive_returning_columns(items, &schema, None, &visible_columns)?;
        (columns, visible_columns)
    } else {
        (Vec::new(), Vec::new())
    };
    // Collected per-row projections (one per inserted row, in insertion order).
    let mut per_row_returning_rows: Vec<vibesql_storage::Row> = Vec::new();

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
        && !on_conflict_needs_row_dispatch
        && !has_insert_triggers
        // A RETURNING subquery must recompute per row against the incremental
        // table state, which the batch path (all rows inserted at once) cannot
        // provide. Force the row-by-row slow path so each row's RETURNING is
        // projected immediately after that row is inserted.
        && !returning_needs_per_row;

    // Helper to create a Row with optional explicit rowid
    let make_row = |(values, rowid): (Vec<vibesql_types::SqlValue>, Option<u64>)| match rowid {
        Some(id) => vibesql_storage::Row::with_row_id(values, id),
        None => vibesql_storage::Row::new(values),
    };

    if use_batch_insert && validated_rows.len() > 1 {
        // Fast path: Use batch insert for multiple rows without triggers.
        // Every validated row is inserted (the batch path never runs for the ON
        // CONFLICT DO UPDATE / DO NOTHING dispatch or REPLACE), so the last row
        // carrying a candidate id wins last_insert_rowid() (last inserted row).
        if let Some(id) = validated_rows.iter().rev().find_map(|t| t.3) {
            last_generated_id = Some(id);
        }

        // For a non-IPK rowid table whose column list names the rowid
        // pseudo-column, every validated row carries its exact per-row rowid in
        // tuple field `.1` (`explicit_rowid`) — the supplied value, or the
        // value auto-assigned for a NULL/DEFAULT rowid. Capture the LAST row's
        // rowid now (before the batch insert consumes `validated_rows`) so the
        // readback below can report SQLite's last-inserted-row semantics even
        // when the batch supplies explicit, out-of-order rowids
        // (e.g. VALUES(10,'a'),(5,'b') → 5, not the batch max 10; issue #5955).
        // The stored u64 is the two's-complement bit pattern of a signed rowid.
        let last_row_explicit_rowid: Option<i64> =
            validated_rows.last().and_then(|t| t.1).map(|r| r as i64);

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
                    chunk.iter().map(|(v, rowid, _, _)| make_row((v.clone(), *rowid))).collect();

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
                validated_rows.into_iter().map(|(v, rowid, _, _)| make_row((v, rowid))).collect();

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

        // For a rowid table WITHOUT an INTEGER PRIMARY KEY the batch path
        // allocates implicit rowids inside the storage layer, so no candidate id
        // surfaced above (last_generated_id is still None). Recover the last
        // inserted row's rowid so last_insert_rowid() tracks it.
        //
        // Prefer the last validated row's exact per-row rowid (`explicit_rowid`,
        // captured above) when the column list named the rowid pseudo-column:
        // that yields SQLite's last-inserted-row semantics regardless of value
        // ordering, so a batch supplying explicit out-of-order rowids reports
        // the final row's value (VALUES(10,'a'),(5,'b') → 5), not the batch max
        // (issue #5955). Only when no per-row rowid is available (the storage
        // layer auto-allocated ascending implicit rowids) fall back to the max
        // rowid readback — for that pure auto-allocated case the max equals the
        // final row's rowid (#5944). VibeSQL is single-threaded within a
        // session, so nothing can insert between the batch and this readback.
        // The WITHOUT ROWID guard at the `set_last_insert_rowid()` call below
        // still excludes WITHOUT ROWID tables.
        if ipk_col_idx.is_none() && last_generated_id.is_none() && !schema.without_rowid {
            if let Some(rowid) = last_row_explicit_rowid {
                last_generated_id = Some(rowid);
            } else if let Some(max_rowid) =
                db.get_table(&storage_table_name).and_then(|t| t.max_rowid_signed())
            {
                last_generated_id = Some(max_rowid);
            }
        }
    } else {
        // Slow path: Insert rows one by one (needed for triggers, special clauses)
        for (mut full_row_values, mut explicit_rowid, ipk_auto_assigned, candidate_last_id) in
            validated_rows
        {
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
            } else if on_conflict_needs_row_dispatch {
                // SQLite generalized upsert (upsert5): select the leftmost
                // ON CONFLICT clause whose target matches a constraint this
                // row actually violates (a target-less clause matches any),
                // then apply that clause's DO NOTHING / DO UPDATE action.
                match super::on_conflict_update::dispatch_on_conflict_clauses(
                    db,
                    table_name,
                    &schema,
                    &full_row_values,
                    &stmt.on_conflict,
                    cte_results.as_ref(),
                )? {
                    super::on_conflict_update::ClauseDispatch::Updated(updated_row_id) => {
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
                    super::on_conflict_update::ClauseDispatch::SkipRow => {
                        // A DO NOTHING clause fired, or DO UPDATE ... WHERE
                        // was false/NULL: the row is neither inserted nor
                        // updated (SQLite semantics).
                        continue;
                    }
                    super::on_conflict_update::ClauseDispatch::NoConflict => {
                        // No unique constraint violated; fall through to a
                        // normal insert.
                    }
                    super::on_conflict_update::ClauseDispatch::UnmatchedConflict(message) => {
                        // The row conflicts, but no clause's target matches
                        // the violated constraint: the statement's conflict
                        // resolution algorithm still applies (upsert5
                        // section 3 — REPLACE must delete the conflicting
                        // rows rather than insert a duplicate key).
                        match stmt.conflict_clause {
                            Some(vibesql_ast::ConflictClause::Replace) => {
                                explicit_rowid = Some(resolve_replace_conflicts_for_row(
                                    db,
                                    table_name,
                                    &storage_table_name,
                                    &schema,
                                    &full_row_values,
                                    explicit_rowid,
                                )?);
                            }
                            Some(vibesql_ast::ConflictClause::Ignore) => continue,
                            _ => return Err(ExecutorError::ConstraintViolation(message)),
                        }
                    }
                }
            } else if matches!(stmt.conflict_clause, Some(vibesql_ast::ConflictClause::Replace)) {
                explicit_rowid = Some(resolve_replace_conflicts_for_row(
                    db,
                    table_name,
                    &storage_table_name,
                    &schema,
                    &full_row_values,
                    explicit_rowid,
                )?);
            }

            // Fire BEFORE INSERT triggers only if triggers exist.
            //
            // SQLite does not allocate the real rowid until the row is written,
            // which happens AFTER the BEFORE INSERT trigger fires. So inside a
            // BEFORE INSERT trigger an auto-allocated rowid is reported as the
            // unwritten-rowid sentinel (-1), both via `NEW.rowid` and via
            // `NEW.<ipk>` for an INTEGER PRIMARY KEY (the rowid alias). We have
            // already computed the real rowid and persist it to the actual row,
            // but the trigger must observe the sentinel — except when the rowid
            // was supplied explicitly (then the supplied value is visible as-is),
            // or when a REPLACE reservation has fixed the rowid before triggers
            // (`explicit_rowid` is then `Some`). insert3-2.1/2.2, triggerC-4.1.2.
            let row_to_insert = if ipk_auto_assigned && explicit_rowid.is_none() {
                let mut trigger_values = full_row_values.clone();
                if let Some(idx) = ipk_col_idx {
                    trigger_values[idx] = vibesql_types::SqlValue::Integer(-1);
                }
                // Pass no row_id so `NEW.rowid` also resolves to the -1 sentinel
                // (the rowid-alias path in trigger_execution.rs reads the IPK
                // column above; the non-alias path reads row_id).
                vibesql_storage::Row::new(trigger_values)
            } else {
                make_row((full_row_values.clone(), explicit_rowid))
            };
            if has_insert_triggers {
                // RAISE(IGNORE) in a BEFORE INSERT trigger abandons this row:
                // skip the insert and continue with the next row (SQLite).
                if crate::TriggerFirer::execute_before_triggers_in_schema(
                    db,
                    table_name,
                    vibesql_ast::TriggerEvent::Insert,
                    None,
                    Some(&row_to_insert),
                    dml_schema.as_deref(),
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

            // SQLite REPLACE semantics: Check if the rowid we're about to use is reserved.
            // During REPLACE, the rowid for the new row is allocated BEFORE firing triggers.
            // The behavior depends on the type of reservation:
            // - Auto-allocated reservation: BEFORE DELETE trigger INSERTs fail on rowid conflict
            // - Explicit reservation: AFTER DELETE trigger auto-INSERTs skip to next rowid (SQLite
            //   reuses freed rowids, but VibeSQL doesn't, so we skip instead)
            let mut final_explicit_rowid = explicit_rowid;
            // Collision-safe auto-rowid (issue #5835, #5894): only allocated when
            // the row has no explicit rowid. `allocate_rowid_u64()` matches sqlite3
            // — the max rowid + 1 (identical to the old `physical_row_count + 1`
            // for purely implicit tables, but also exceeding every explicit rowid
            // ever assigned, required after a reload), a random *unused* rowid when
            // the max is already `i64::MAX`, or `SQLITE_FULL` if the rowid space is
            // exhausted — never a silent reuse of `i64::MAX` (a duplicate rowid).
            let rowid_to_use = match explicit_rowid {
                Some(r) => r,
                None => table_ref
                    .allocate_rowid_u64()
                    .map_err(|e| ExecutorError::StorageError(e.to_string()))?,
            };
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
                        // This approximates SQLite's rowid reuse behavior.
                        // Rowids are signed bit patterns (issue #5835): step
                        // in signed space so a reserved rowid of -1
                        // (u64::MAX) steps to 0, not past u64 range.
                        let new_rowid = (reserved_rowid as i64).saturating_add(1) as u64;
                        final_explicit_rowid = Some(new_rowid);

                        // Also update the INTEGER PRIMARY KEY column value to match the new rowid
                        // The column value was auto-generated earlier but now we're using a
                        // different rowid
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

            // Materialize the auto-allocated rowid on the stored row (issue
            // #5835) so it persists across saves/reloads and can never be
            // silently renumbered. Skip INTEGER PRIMARY KEY (rowid alias)
            // tables: there the IPK column value IS the rowid and every
            // rowid read resolves through the alias column, so stamping a
            // physical-position-derived row_id could disagree with it.
            if final_explicit_rowid.is_none() && ipk_col_idx.is_none() {
                final_explicit_rowid = Some(rowid_to_use);
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

            // Per-row RETURNING subquery projection: drop the stale row-oriented
            // columnar snapshot so a RETURNING subquery sees this row's insertion,
            // then project this row's RETURNING against the now-updated table
            // state. Each returned row thus observes the incremental count/sum/etc.
            // as of its own insertion, matching sqlite3 (returning1.test §20).
            if returning_needs_per_row {
                db.invalidate_columnar_cache(&storage_table_name);
                let items = stmt.returning.as_deref().expect("returning present");
                let projected = crate::dml_returning::project_returning_row(
                    items,
                    &per_row_returning_columns,
                    &schema,
                    db,
                    &row,
                    &per_row_visible_columns,
                    cte_results.as_ref(),
                )?;
                per_row_returning_rows.push(projected);
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
            //   - RAISE(ABORT)/RAISE(ROLLBACK)/any non-RAISE trigger error → the statement
            //     savepoint (in an explicit txn) or implicit transaction (auto-commit) rolls back
            //     the *whole* statement, removing this offending row AND every earlier row.
            //   - RAISE(FAIL) → the partial changes the statement already applied are KEPT,
            //     including this offending row (which was inserted before its AFTER trigger fired).
            //     SQLite keeps it; so do we.
            //
            // Previously this site unconditionally tombstoned the just-inserted
            // offending row on any trigger error. That was redundant for
            // ABORT/non-RAISE (rollback already removes it) and *wrong* for FAIL
            // (it hid a row SQLite keeps — visible only as a bitmap tombstone in
            // raw `Table::scan()`, missing from any live SELECT). See #5474.
            if has_insert_triggers {
                // Stamp the NEW row with its allocated rowid so `NEW.rowid` (and
                // the `oid` / `_rowid_` aliases) resolve in the AFTER INSERT
                // trigger body (#5485). For an explicit `INSERT INTO t(rowid,...)`
                // the row already carries `row_id`; for an auto-allocated rowid
                // the row was created with `row_id == None`, and the actual rowid
                // is `rowid_to_use` (the table's next auto-allocated rowid).
                let after_row = {
                    let mut r = row.clone();
                    if r.row_id.is_none() {
                        r.row_id = Some(rowid_to_use);
                    }
                    r
                };
                // A `TriggerOutcome::SkipRow` (RAISE(IGNORE) in an AFTER trigger)
                // has no SQLite-observable effect here: the row is already
                // inserted and RAISE(IGNORE) only abandons the rest of the
                // trigger program, leaving the row in place. So we only act on
                // the error (RAISE / non-RAISE) path; drop the Ok outcome.
                let _after_outcome = crate::TriggerFirer::execute_after_triggers_in_schema(
                    db,
                    table_name,
                    vibesql_ast::TriggerEvent::Insert,
                    None,
                    Some(&after_row),
                    dml_schema.as_deref(),
                )?;
            }

            // For a rowid table WITHOUT an INTEGER PRIMARY KEY, the implicit
            // rowid is allocated above (materialized into `final_explicit_rowid`
            // from `allocate_rowid_u64()`) and never flows through
            // `candidate_last_id`, which only tracks explicit/auto IPK and
            // sequence values. Feed the allocated rowid back so
            // last_insert_rowid() matches sqlite3 for plain tables (#5944). The
            // WITHOUT ROWID guard at the `set_last_insert_rowid()` call below
            // still excludes WITHOUT ROWID tables.
            let candidate_last_id = match candidate_last_id {
                Some(id) => Some(id),
                None if ipk_col_idx.is_none() => final_explicit_rowid.map(|r| r as i64),
                None => None,
            };

            // The row was genuinely inserted (not an ON CONFLICT DO UPDATE
            // update, a DO NOTHING skip, or a RAISE(IGNORE) trigger skip — those
            // paths `continue` above without reaching here), so it updates
            // last_insert_rowid(). Last inserted row wins (issue #5886).
            if let Some(id) = candidate_last_id {
                last_generated_id = Some(id);
            }

            rows_inserted += 1;
        }
    }

    // Fire AFTER STATEMENT triggers only if triggers exist AND we're not inside a trigger context
    // (Statement-level triggers don't fire for inserts within trigger bodies)
    if has_insert_triggers && trigger_context.is_none() {
        // Statement-level RAISE(IGNORE) has no sqlite3 analog; drop the
        // must-use outcome (#5418).
        let _stmt_outcome = crate::TriggerFirer::execute_after_statement_triggers_in_schema(
            db,
            table_name,
            vibesql_ast::TriggerEvent::Insert,
            dml_schema.as_deref(),
        )?;
    }

    // Update LAST_INSERT_ROWID if any auto-generated values were produced
    // SQLite: last_insert_rowid() is NOT updated for WITHOUT ROWID tables
    // (see SQLite documentation R-47220-63683)
    if let Some(id) = last_generated_id {
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
    let returning_result = if returning_needs_per_row {
        // Slow path: each row was already projected against its own incremental
        // table state inside the insert loop. Assemble the collected rows with
        // the columns derived up front (so zero inserted rows still yields the
        // correct empty header set).
        Some(crate::select::SelectResult {
            columns: per_row_returning_columns,
            rows: per_row_returning_rows,
        })
    } else if let Some(items) = &stmt.returning {
        // Fast path (subquery-free RETURNING): batch-project once against the
        // final post-INSERT state — every row sees the same snapshot, which is
        // correct when no subquery observes the table.
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

/// SQLite REPLACE conflict resolution for one candidate row.
///
/// Allocates and reserves the new row's rowid BEFORE firing BEFORE DELETE
/// triggers (so a trigger INSERT that grabs the same rowid fails with a
/// UNIQUE error), deletes the conflicting rows (firing DELETE triggers),
/// re-validates constraints against post-trigger table state, and returns
/// the reserved rowid the REPLACE insert must use as its explicit rowid.
///
/// Called from the plain `INSERT OR REPLACE` / `REPLACE INTO` path, and from
/// the generalized-upsert fallback when a `REPLACE INTO ... ON CONFLICT ...`
/// statement hits a conflict that no ON CONFLICT clause matches (upsert5
/// section 3): REPLACE resolution still applies to unmatched conflicts.
fn resolve_replace_conflicts_for_row(
    db: &mut vibesql_storage::Database,
    table_name: &str,
    storage_table_name: &str,
    schema: &vibesql_catalog::TableSchema,
    full_row_values: &[vibesql_types::SqlValue],
    explicit_rowid: Option<u64>,
) -> Result<u64, ExecutorError> {
    // Compute the rowid for the new row and reserve it.
    // Track whether the rowid is explicitly specified (INTEGER PRIMARY KEY)
    // or auto-allocated, as this affects how conflicts are handled.
    let is_explicit = explicit_rowid.is_some();
    let reserved_rowid = if let Some(rowid) = explicit_rowid {
        rowid
    } else {
        // Auto-allocate the next available rowid. `allocate_rowid_u64()` covers
        // both the physical row count (implicit rowids, including deleted rows)
        // and every explicit rowid ever assigned — required after a reload,
        // where persisted rowids can exceed the compacted physical count
        // (issue #5835) — and matches sqlite3 at `i64::MAX` (random unused rowid,
        // then `SQLITE_FULL`) instead of silently reusing it (issue #5894).
        match db.get_table(storage_table_name) {
            None => 1,
            Some(t) => {
                t.allocate_rowid_u64().map_err(|e| ExecutorError::StorageError(e.to_string()))?
            }
        }
    };

    // Reserve the rowid before firing triggers
    db.reserve_rowid(storage_table_name, reserved_rowid, is_explicit);

    // Delete conflicting rows first. This also fires DELETE triggers which
    // may create new constraint violations. handle_replace_conflicts()
    // releases the reserved rowid after BEFORE DELETE triggers but before
    // AFTER DELETE triggers.
    let replace_result = super::replace::handle_replace_conflicts(
        db,
        table_name,
        storage_table_name,
        schema,
        full_row_values,
    );

    // On error, ensure the reserved rowid is released and propagate the error.
    // Note: The reserved rowid may already be released if the error occurred
    // after BEFORE DELETE triggers, but release_reserved_rowid is idempotent.
    if let Err(e) = replace_result {
        db.release_reserved_rowid(storage_table_name);
        return Err(e);
    }

    // After REPLACE conflict handling (which fires triggers), re-validate constraints.
    // Triggers may have inserted new rows that conflict with the row we want to insert.
    // Pass empty batch values since we're only checking against existing table data.
    // Note: The reserved rowid has already been released by handle_replace_conflicts()
    // after BEFORE DELETE triggers, so no need to release on error here.
    super::constraints::enforce_primary_key_constraint(
        db,
        schema,
        table_name,
        full_row_values,
        &[], // No batch values to check against
    )?;

    super::constraints::enforce_unique_constraints(
        db,
        schema,
        table_name,
        full_row_values,
        &[], // No batch values to check against
    )?;

    super::constraints::enforce_unique_indexes(db, schema, table_name, full_row_values)?;

    // Release the reservation now that all delete triggers have fired.
    // The REPLACE INSERT uses the returned rowid as its explicit rowid, so it
    // will not fail on its own reservation.
    db.release_reserved_rowid(storage_table_name);

    Ok(reserved_rowid)
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

    // NOTE: FOREIGN KEY constraints are intentionally NOT checked here.
    //
    // This helper decides whether an OR IGNORE / untargeted DO NOTHING row
    // should be silently skipped. Per SQLite, the ON CONFLICT algorithm applies
    // only to NOT NULL, UNIQUE, PRIMARY KEY, and CHECK constraints — it does
    // *not* apply to FOREIGN KEY constraints. An immediate FK violation must
    // always raise "FOREIGN KEY constraint failed" (never be swallowed by
    // OR IGNORE), and a deferred FK violation must be queued for COMMIT-time
    // re-validation rather than dropping the row. Both are handled downstream
    // by `RowValidator::validate_foreign_keys` (which also honors
    // `PRAGMA foreign_keys` gating), so treating a missing parent key as an
    // "ignorable conflict" here would both suppress real errors and misbehave
    // when foreign keys are disabled (fkey2-20.2/20.3).

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
    for row in new_rows {
        for trigger in &triggers {
            if crate::TriggerFirer::execute_trigger(db, trigger, None, Some(&row))?
                == crate::trigger_execution::TriggerOutcome::SkipRow
            {
                break;
            }
        }
    }

    // changes() after an INSERT/UPDATE/DELETE on a view is always zero: no
    // physical table rows were modified by the statement itself (only the
    // INSTEAD OF trigger body touches real tables, and those changes are
    // saved/restored around the trigger program). SQLite evidence
    // R-09813-48563; e_changes-4.2.2 (#5840).
    Ok((0, returning_result))
}

/// Build a pseudo TableSchema from a view definition
fn build_view_schema(
    db: &vibesql_storage::Database,
    view_def: &vibesql_catalog::ViewDefinition,
) -> Result<vibesql_catalog::TableSchema, ExecutorError> {
    // Execute the view's SELECT query to get column names. A table referenced by
    // the view body that has since been dropped surfaces here; for a main-schema
    // view sqlite3 reports it schema-qualified (`no such table: main.test2`,
    // trigger4-3.1/3.2) because the view body resolves against the database
    // schema, so qualify the bare name. A temp view's body resolves against the
    // temp schema, which sqlite3 reports unqualified, so skip the qualifier
    // there. This mirrors the read-path guard in select/scan/table.rs (#5584)
    // for read/write parity.
    let select_executor = crate::SelectExecutor::new(db);
    let select_result = select_executor.execute_with_columns(&view_def.query);
    let result = if view_def.is_temp() {
        select_result?
    } else {
        select_result.map_err(ExecutorError::with_main_schema_qualifier)?
    };

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
