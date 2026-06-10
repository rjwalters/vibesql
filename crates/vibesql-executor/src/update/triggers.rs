//! Trigger execution logic for UPDATE operations
//!
//! This module handles:
//! - Executing UPDATE statements on VIEWs via INSTEAD OF triggers
//! - Building pseudo-schemas for views
//! - Trigger context propagation

use vibesql_ast::{
    ColumnIdentifier, Expression, FromClause, JoinType, SelectItem, SelectStmt, TriggerTiming,
    UpdateStmt, WhereClause,
};
use vibesql_catalog::{ColumnSchema, TableSchema, ViewDefinition};
use vibesql_storage::{Database, Row};
use vibesql_types::{DataType, SqlValue};

use crate::{errors::ExecutorError, evaluator::ExpressionEvaluator};

/// Execute an UPDATE statement with trigger context
/// This function is used when executing UPDATE statements within trigger bodies
/// to support OLD/NEW pseudo-variable references
pub fn execute_update_with_trigger_context(
    database: &mut Database,
    stmt: &UpdateStmt,
    trigger_context: &crate::trigger_execution::TriggerContext,
) -> Result<usize, ExecutorError> {
    super::UpdateExecutor::execute_with_trigger_context(stmt, database, trigger_context)
}

/// Execute UPDATE on a VIEW using INSTEAD OF triggers
///
/// When updating a view, we need to fire INSTEAD OF UPDATE triggers
/// instead of actually updating data. The triggers typically update
/// the underlying tables.
///
/// When the statement has a RETURNING clause, the projected NEW view rows
/// (old row with SET assignments applied) are returned — one per trigger
/// fire, regardless of what the trigger body does (SQLite semantics).
pub(super) fn execute_update_on_view(
    database: &mut Database,
    stmt: &UpdateStmt,
    view_def: &ViewDefinition,
    procedural_context: Option<&crate::procedural::ExecutionContext>,
    trigger_context: Option<&crate::trigger_execution::TriggerContext>,
) -> Result<(usize, Option<crate::select::SelectResult>), ExecutorError> {
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

    // Issue #5192 (4.3): UPDATE ... FROM ... on a view must resolve column refs
    // from the FROM tables. The default per-row evaluator only sees the view's
    // own schema, so qualifiers like `map.k` would otherwise fail with
    // "no such column: map.k".
    //
    // When `stmt.from_clause` is set, build a synthetic SELECT that joins the
    // view (referenced by name; SelectExecutor expands views in FROM) with the
    // FROM tables. The SELECT projects the view's columns (for OLD row
    // reconstruction) plus each SET expression evaluated in the joined
    // context (for NEW row construction). For each matched row we then fire
    // the INSTEAD OF UPDATE trigger with the (old_row, new_row) pair.
    let updates: Vec<(Row, Row)> = if let Some(ref from_clauses) = stmt.from_clause {
        collect_view_updates_with_from(
            database,
            stmt,
            view_def,
            &view_schema,
            from_clauses,
            trigger_context,
        )?
    } else {
        collect_view_updates_no_from(
            database,
            stmt,
            view_def,
            &view_schema,
            procedural_context,
            trigger_context,
        )?
    };

    // Now fire triggers (database can be mutably borrowed)
    let rows_processed = updates.len();
    for (old_row, new_row) in &updates {
        for trigger in &triggers {
            crate::TriggerFirer::execute_trigger(database, trigger, Some(old_row), Some(new_row))?;
        }
    }

    // Project RETURNING items against the NEW view rows (SQLite returns the
    // updated view row per trigger fire, not whatever the trigger body did).
    let returning = if let Some(items) = &stmt.returning {
        let new_rows: Vec<&Row> = updates.iter().map(|(_, new_row)| new_row).collect();
        Some(super::returning::project_returning(
            items,
            &view_schema,
            database,
            stmt.alias.as_deref(),
            &new_rows,
        )?)
    } else {
        None
    };

    Ok((rows_processed, returning))
}

/// Build (old_row, new_row) update pairs for an UPDATE on a view WITHOUT a FROM
/// clause. Iterates the view's materialized rows, applies WHERE filter, and
/// computes assignments using a single-table evaluator over the view schema.
fn collect_view_updates_no_from(
    database: &Database,
    stmt: &UpdateStmt,
    view_def: &ViewDefinition,
    view_schema: &TableSchema,
    procedural_context: Option<&crate::procedural::ExecutionContext>,
    trigger_context: Option<&crate::trigger_execution::TriggerContext>,
) -> Result<Vec<(Row, Row)>, ExecutorError> {
    // Execute the view query to get the rows to potentially update
    let select_executor = crate::SelectExecutor::new(database);
    let all_rows = select_executor.execute_with_columns(&view_def.query)?;

    // Create evaluator for WHERE clause and assignments
    let evaluator = if let Some(ctx) = trigger_context {
        ExpressionEvaluator::with_trigger_context(view_schema, database, ctx)
    } else if let Some(ctx) = procedural_context {
        ExpressionEvaluator::with_procedural_context(view_schema, database, ctx)
    } else {
        ExpressionEvaluator::with_database(view_schema, database)
    };

    // Select rows matching WHERE clause and build updates
    let mut collected_updates = Vec::new();
    for row in &all_rows.rows {
        // Clear the CSE cache before evaluating this row: cached expression
        // results (e.g. the WHERE comparison itself) from a previous row
        // would otherwise be replayed for every subsequent row, making the
        // first row's WHERE verdict apply to the whole view (issue #5233 —
        // `UPDATE v SET ... WHERE b=4` fired the trigger 0 times).
        evaluator.clear_cse_cache();

        let matches = match &stmt.where_clause {
            // Use SQLite truthiness rather than a Boolean-only match so
            // SQLite-style Integer(1)/Integer(0) comparison results count
            // as matches too (issue #5233).
            Some(WhereClause::Condition(expr)) => {
                crate::evaluator::operators::is_truthy(&evaluator.eval(expr, row)?)
            }
            None => true, // No WHERE clause - update all rows
            Some(WhereClause::CurrentOf(_)) => {
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
                        available_columns: view_schema
                            .columns
                            .iter()
                            .map(|c| c.name.clone())
                            .collect(),
                    })?;

                // Evaluate the new value
                let new_value = evaluator.eval(&assignment.value, &old_row)?;
                new_row_values[col_idx] = new_value;
            }

            let new_row = Row::new(new_row_values);
            collected_updates.push((old_row, new_row));
        }
    }
    Ok(collected_updates)
}

/// Build (old_row, new_row) update pairs for an UPDATE ... FROM ... on a view.
///
/// This builds a synthetic SELECT analogous to
/// `from_clause::execute_update_from_join` but with the view as the "target":
///
/// 1. SELECT list = view's columns (for OLD row reconstruction)
///    + each SET expression value (for NEW row construction).
/// 2. FROM list   = view, FROM clauses (combined as cross-joins, mirroring
///    `from_clause::combine_with_from_clause`).
/// 3. WHERE       = `stmt.where_clause` (evaluated in joined context).
///
/// Each result row yields (old_row, new_row) — the old row taken from the
/// first N projected columns (where N = view column count), and the new row
/// built by copying old_row and overwriting the columns named in
/// `stmt.assignments` with the corresponding `__set_<i>__` projected value.
///
/// The view's `__hidden__*` columns (SQLite ENABLE_HIDDEN_COLUMNS suffix
/// convention used by triggerupfrom-4.3) are projected unchanged so the
/// INSTEAD OF trigger sees the full OLD/NEW pseudo-row.
fn collect_view_updates_with_from(
    database: &Database,
    stmt: &UpdateStmt,
    view_def: &ViewDefinition,
    view_schema: &TableSchema,
    from_clauses: &[FromClause],
    trigger_context: Option<&crate::trigger_execution::TriggerContext>,
) -> Result<Vec<(Row, Row)>, ExecutorError> {
    let view_name = &view_def.name;
    let view_prefix = stmt.alias.clone().unwrap_or_else(|| view_name.clone());

    // Project each of the view's columns (qualified by the view's prefix) so
    // we can recover the old row from the join result.
    let view_col_count = view_schema.columns.len();
    let mut select_list = Vec::with_capacity(view_col_count + stmt.assignments.len());
    for col in &view_schema.columns {
        select_list.push(SelectItem::Expression {
            expr: Expression::ColumnRef(ColumnIdentifier::qualified(
                &view_prefix,
                false,
                &col.name,
                false,
            )),
            alias: Some(format!("__old_{}__", col.name)),
            source_text: None,
        });
    }

    // Project each SET expression so we get computed NEW values in the
    // joined context. These appear after the view's columns in the result.
    for (i, assignment) in stmt.assignments.iter().enumerate() {
        select_list.push(SelectItem::Expression {
            expr: assignment.value.clone(),
            alias: Some(format!("__set_{}__", i)),
            source_text: None,
        });
    }

    // Build FROM clause: view [AS alias], from_clause1, from_clause2, ...
    let view_from = FromClause::Table {
        index_hint: None,
        name: view_name.clone(),
        alias: stmt.alias.clone(),
        column_aliases: None,
        quoted: stmt.quoted,
    };

    // Combine the view with FROM clauses as cross-joins. This mirrors
    // `from_clause::combine_with_from_clause` so JOIN-typed FROM clauses
    // reattach their structure correctly (`view, t1 LEFT JOIN t2` parses as
    // `(view CROSS JOIN t1) LEFT JOIN t2`).
    let mut combined_from = view_from;
    for from_clause in from_clauses {
        combined_from = combine_with_from_clause(combined_from, from_clause.clone());
    }

    // WHERE clause is forwarded as-is; column refs to either the view or
    // any FROM table now resolve through the standard join column resolver.
    let where_clause = match stmt.where_clause.as_ref() {
        Some(WhereClause::Condition(expr)) => Some(expr.clone()),
        Some(WhereClause::CurrentOf(_)) => {
            return Err(ExecutorError::UnsupportedExpression(
                "CURRENT OF not supported for view updates".to_string(),
            ));
        }
        None => None,
    };

    let select_stmt = SelectStmt {
        with_clause: stmt.with_clause.clone(),
        select_list,
        distinct: false,
        into_table: None,
        into_variables: None,
        from: Some(combined_from),
        where_clause,
        group_by: None,
        having: None,
        window_definitions: None,
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None,
        values: None,
    };

    // Execute the synthetic SELECT. Forward trigger_context when present so
    // OLD/NEW references in SET / WHERE (i.e., when the UPDATE is nested
    // inside another trigger body) resolve against the firing row.
    let rows = match trigger_context {
        Some(ctx) => {
            let executor = crate::SelectExecutor::new_with_trigger_context(database, ctx);
            executor.execute(&select_stmt)?
        }
        None => {
            let executor = crate::SelectExecutor::new(database);
            executor.execute(&select_stmt)?
        }
    };

    // Reconstruct (old_row, new_row) pairs from the joined output.
    //
    // The synthetic SELECT's WHERE has already filtered the rows, so every
    // result row corresponds to one join match. SQLite fires the INSTEAD OF
    // trigger once per join match — if a single view row matches multiple
    // FROM-side rows, the trigger fires once per match (verified against
    // sqlite3 3.51.0 for window1.test 73.4, which expects 9 fires for
    // 3 view rows x 3 subquery rows). No deduplication is performed.
    let mut collected_updates: Vec<(Row, Row)> = Vec::new();

    for row in rows {
        if row.values.len() < view_col_count + stmt.assignments.len() {
            return Err(ExecutorError::UnsupportedExpression(format!(
                "Internal error: synthetic SELECT for UPDATE on view '{}' produced row with {} \
                 columns, expected at least {}",
                view_name,
                row.values.len(),
                view_col_count + stmt.assignments.len()
            )));
        }

        // First view_col_count values are the old row's column values.
        let old_values: Vec<SqlValue> = row.values[..view_col_count].to_vec();
        let old_row = Row::new(old_values);

        // Build NEW row: copy old, then overwrite assigned columns with
        // the corresponding __set_<i>__ values.
        let mut new_row_values = old_row.values.clone();
        for (i, assignment) in stmt.assignments.iter().enumerate() {
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
            new_row_values[col_idx] = row.values[view_col_count + i].clone();
        }
        let new_row = Row::new(new_row_values);

        collected_updates.push((old_row, new_row));
    }

    Ok(collected_updates)
}

/// Combine the accumulated FROM clause with a new FROM clause as a CROSS JOIN.
///
/// Mirrors `from_clause::combine_with_from_clause` so JOIN-typed FROM clauses
/// reattach their structure correctly (`view, t1 LEFT JOIN t2` parses as
/// `(view CROSS JOIN t1) LEFT JOIN t2`).
fn combine_with_from_clause(accumulated: FromClause, from_clause: FromClause) -> FromClause {
    match from_clause {
        FromClause::Table { .. } | FromClause::Subquery { .. } | FromClause::Values { .. } => {
            FromClause::Join {
                left: Box::new(accumulated),
                right: Box::new(from_clause),
                join_type: JoinType::Cross,
                condition: None,
                using_columns: None,
                natural: false,
                alias: None,
            }
        }
        FromClause::Join { left, right, join_type, condition, using_columns, natural, alias } => {
            let new_left = combine_with_from_clause(accumulated, *left);
            FromClause::Join {
                left: Box::new(new_left),
                right,
                join_type,
                condition,
                using_columns,
                natural,
                alias,
            }
        }
    }
}

/// Build a pseudo TableSchema from a view definition
pub(super) fn build_view_schema(
    database: &Database,
    view_def: &ViewDefinition,
) -> Result<TableSchema, ExecutorError> {
    // Execute the view's SELECT query to get column names
    let select_executor = crate::SelectExecutor::new(database);
    let result = select_executor.execute_with_columns(&view_def.query)?;

    // Use explicit column names if provided, otherwise derive from SELECT
    let column_names: Vec<String> =
        if let Some(ref cols) = view_def.columns { cols.clone() } else { result.columns.clone() };

    // Build columns with NONE affinity (DataType::Null). The pseudo-schema
    // mainly provides column names for trigger binding, but its data types
    // feed SQLite affinity rules during WHERE evaluation: declaring the
    // columns as Varchar gave them TEXT affinity, which converted numeric
    // literals to text and made comparisons like `WHERE b=4` never match
    // (issue #5233). NONE affinity compares values by their actual types,
    // matching bare (undeclared) columns in SQLite.
    let columns: Vec<ColumnSchema> = column_names
        .into_iter()
        .map(|name| ColumnSchema::new(name, DataType::Null, true))
        .collect();

    Ok(TableSchema::new(view_def.name.clone(), columns))
}
