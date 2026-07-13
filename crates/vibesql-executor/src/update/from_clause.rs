//! UPDATE FROM clause handling
//!
//! This module implements SQLite's UPDATE FROM syntax (added in SQLite 3.33.0),
//! which allows multi-table UPDATE statements:
//!
//! ```sql
//! UPDATE t1 SET col = t2.val FROM t2 WHERE t1.id = t2.id;
//! ```
//!
//! The FROM clause specifies additional tables to join with the target table.
//! Values from these tables can be used in both SET expressions and WHERE clause.

use std::collections::{HashMap, HashSet};

use vibesql_ast::{
    Assignment, ColumnIdentifier, Expression, FromClause, JoinType, SelectItem, SelectStmt,
    UpdateStmt, WhereClause,
};
use vibesql_catalog::TableSchema;
use vibesql_storage::{Database, Row};
use vibesql_types::SqlValue;

use super::PendingUpdate;
use crate::errors::ExecutorError;

/// Result of executing the join between target table and FROM tables
pub struct UpdateFromJoinResult {
    /// Target table row index and the computed SET values for each matched row
    pub matched_rows: Vec<UpdateFromMatch>,
}

/// A single matched row from the UPDATE FROM join
pub struct UpdateFromMatch {
    /// Physical row index in the target table
    pub row_index: usize,
    /// Original row from target table
    pub target_row: Row,
    /// Computed values for each SET assignment (in order)
    pub set_values: Vec<SqlValue>,
}

/// Execute the join for UPDATE FROM and return matched rows with computed SET values
///
/// This builds and executes a synthetic SELECT that:
/// 1. Joins the target table with all FROM tables
/// 2. Computes SET expression values as part of the SELECT
/// 3. Returns the target row index and computed values for each match
///
/// `trigger_context` (issue #5082, Bucket B of #5073): when the enclosing UPDATE
/// is running inside a trigger body, the trigger context is threaded into the
/// synthetic SELECT so `OLD.col` / `NEW.col` references in SET / WHERE clauses
/// can be resolved against the firing row. `None` for top-level UPDATE…FROM.
pub fn execute_update_from_join(
    stmt: &UpdateStmt,
    from_clauses: &[FromClause],
    database: &Database,
    target_schema: &TableSchema,
    trigger_context: Option<&crate::trigger_execution::TriggerContext<'_>>,
) -> Result<UpdateFromJoinResult, ExecutorError> {
    let target_table_name = &target_schema.name;
    let target_alias = stmt.alias.clone();
    let target_prefix = target_alias.clone().unwrap_or_else(|| target_table_name.clone());

    // Determine how to identify target rows:
    // - For WITHOUT ROWID tables: use PRIMARY KEY columns
    // - For regular tables: use rowid
    let pk_columns = get_pk_column_names(target_schema);
    let use_rowid = !target_schema.without_rowid && pk_columns.is_empty();

    // Build SELECT list:
    // 1. Target table identifier columns (rowid or PK columns)
    // 2. Each SET expression value (computed using joined context)
    let mut select_list = Vec::new();
    let num_id_columns: usize;

    if use_rowid {
        // Use rowid for regular tables
        select_list.push(SelectItem::Expression {
            expr: Expression::ColumnRef(ColumnIdentifier::qualified(
                &target_prefix,
                false,
                "rowid",
                false,
            )),
            alias: Some("__target_rowid__".to_string()),
            source_text: None,
        });
        num_id_columns = 1;
    } else {
        // Use PRIMARY KEY columns for WITHOUT ROWID tables or tables with explicit PK
        for (i, pk_col) in pk_columns.iter().enumerate() {
            select_list.push(SelectItem::Expression {
                expr: Expression::ColumnRef(ColumnIdentifier::qualified(
                    &target_prefix,
                    false,
                    pk_col,
                    false,
                )),
                alias: Some(format!("__target_pk_{}__", i)),
                source_text: None,
            });
        }
        num_id_columns = pk_columns.len();
    }

    // Add each SET expression to be computed in the join context
    //
    // Issue #5082: when running inside a trigger body, pre-resolve any OLD/NEW
    // pseudo-variable references in SET expressions to literals using the
    // firing row. This avoids needing to thread trigger context through the
    // entire scan/join expression-evaluation stack — the synthetic SELECT
    // sees only literal values plus column refs from the joined tables.
    //
    // Issue #6047: a *tuple* assignment `SET (a, b, …) = (row-value | subquery)`
    // is a single AST `Assignment` whose `value` is a row-valued expression
    // (a `RowValueConstructor` or a multi-column `ScalarSubquery`). Projecting
    // it as one `__set_i__` scalar item would route the row value through the
    // ordinary scalar evaluator, which correctly rejects a >1-column result
    // ("sub-select returns N columns - expected 1").
    //
    // Direct row-value RHS (`RowValueConstructor`, or a non-row misuse) is
    // expanded into one select item per target column: each output column is an
    // ordinary scalar computed once in the join context, so `(t1.z, t1.x)` and
    // `(7, 8)` are correct and single-pass.
    //
    // Issue #6086: a *subquery* RHS `(SELECT e0, e1, …)` must be evaluated
    // ONCE per matched row (SQLite single-pass semantics). Splitting it into one
    // narrowed scalar subquery per column re-executes the subquery per column;
    // worse, two textually-identical narrowed subqueries (e.g. both `random()`)
    // collapse through the subquery-result cache and yield equal values, so
    // `SET (a, b) = (SELECT random(), random())` produced `a = b` instead of
    // independent values. Instead we project the subquery's *outer* column
    // references (its correlation inputs) into the synthetic SELECT, then in the
    // result loop substitute those join-row values as literals and evaluate the
    // now-uncorrelated multi-column subquery a single time via
    // `ExpressionEvaluator::eval_row_value`, distributing its row tuple
    // positionally to the target columns.
    //
    // `assignment_plans` records, per assignment, how its projected synthetic
    // columns map to final SET values so the result loop can rebuild the
    // positionally-flattened `set_values` (single-column assignments contribute
    // one value; tuple assignments contribute one value per target column).
    let outer_tables =
        collect_outer_table_schemas(target_schema, &target_prefix, from_clauses, database);
    let mut assignment_plans: Vec<AssignmentProjPlan> = Vec::with_capacity(stmt.assignments.len());
    for (i, assignment) in stmt.assignments.iter().enumerate() {
        if assignment.is_tuple() {
            if let Expression::ScalarSubquery(sub) = &assignment.value {
                // Single-evaluation subquery path (#6086). Collect the outer
                // column references the subquery correlates on and project them
                // as `__corr_i_k__` inputs; the subquery is evaluated once per
                // matched row in the result loop.
                let corr_refs = collect_outer_column_refs(sub, &outer_tables);
                for (k, corr) in corr_refs.iter().enumerate() {
                    let expr = match trigger_context {
                        Some(ctx) => substitute_pseudo_vars(&corr.expr, ctx)?,
                        None => corr.expr.clone(),
                    };
                    select_list.push(SelectItem::Expression {
                        expr,
                        alias: Some(format!("__corr_{}_{}__", i, k)),
                        source_text: None,
                    });
                }
                // The subquery may itself still reference OLD/NEW pseudo-vars
                // (trigger body). Pre-resolve those to literals now; the
                // remaining outer refs are substituted per-row below.
                let subquery = match trigger_context {
                    Some(ctx) => match substitute_pseudo_vars(&assignment.value, ctx)? {
                        Expression::ScalarSubquery(s) => s,
                        _ => sub.clone(),
                    },
                    None => sub.clone(),
                };
                assignment_plans.push(AssignmentProjPlan::SubqueryOnce {
                    output_arity: assignment.columns.len(),
                    corr_refs,
                    subquery,
                });
                continue;
            }
            // Direct row-value RHS: one select item per target column.
            let col_exprs = tuple_assignment_column_exprs(assignment)?;
            for (j, expr) in col_exprs.into_iter().enumerate() {
                let expr = match trigger_context {
                    Some(ctx) => substitute_pseudo_vars(&expr, ctx)?,
                    None => expr,
                };
                select_list.push(SelectItem::Expression {
                    expr,
                    alias: Some(format!("__set_{}_{}__", i, j)),
                    source_text: None,
                });
            }
            assignment_plans.push(AssignmentProjPlan::DirectColumns(assignment.columns.len()));
        } else {
            let expr = match trigger_context {
                Some(ctx) => substitute_pseudo_vars(&assignment.value, ctx)?,
                None => assignment.value.clone(),
            };
            select_list.push(SelectItem::Expression {
                expr,
                alias: Some(format!("__set_{}__", i)),
                source_text: None,
            });
            assignment_plans.push(AssignmentProjPlan::DirectColumns(1));
        }
    }
    // Number of final SET values each assignment contributes (positional flatten).
    let total_set_columns: usize = assignment_plans.iter().map(|p| p.output_columns()).sum();

    // Build FROM clause: target_table [alias], from_clause1, from_clause2, ...
    let target_from = FromClause::Table {
        index_hint: None,
        name: target_table_name.clone(),
        alias: target_alias,
        column_aliases: None,
        quoted: stmt.quoted,
    };

    // Combine the target table with FROM clauses
    // For JOIN-type FROM clauses, we need to extract the leftmost table and
    // cross-join with target, then reattach the rest of the join structure.
    // This ensures `t5, m1 LEFT JOIN m2` behaves as `(t5 CROSS JOIN m1) LEFT JOIN m2`
    let mut combined_from = target_from;
    for from_clause in from_clauses {
        combined_from = combine_with_from_clause(combined_from, from_clause.clone());
    }

    // Build WHERE clause from UPDATE's WHERE clause
    // Issue #5082: pre-resolve OLD/NEW pseudo-variables to literals when running
    // inside a trigger body (see comment on SET expressions above).
    let where_clause = match stmt.where_clause.as_ref() {
        Some(WhereClause::Condition(expr)) => match trigger_context {
            Some(ctx) => Some(substitute_pseudo_vars(expr, ctx)?),
            None => Some(expr.clone()),
        },
        _ => None,
    };

    // Build the synthetic SELECT statement
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

    // Execute the SELECT
    // Issue #5082: when running inside a trigger body, thread the trigger context
    // into the synthetic SELECT so OLD.col / NEW.col references in SET / WHERE
    // resolve against the firing row.
    let executor = match trigger_context {
        Some(ctx) => crate::SelectExecutor::new_with_trigger_context(database, ctx),
        None => crate::SelectExecutor::new(database),
    };
    let rows = executor.execute(&select_stmt)?;

    // Build a map from identifier values to SET values
    // Key: either rowid (as single-element vec) or PK column values.
    // `set_values` is a positional flattening of every SET column: single-column
    // assignments contribute one value, tuple assignments contribute one value
    // per target column (issue #6047).
    //
    // The synthetic row layout is `[id columns][per-assignment projected
    // columns]`. `DirectColumns` assignments project their final values
    // directly; a `SubqueryOnce` assignment projects only its correlation
    // inputs, so its final values are computed here by substituting those inputs
    // into the subquery and evaluating it exactly once (issue #6086).
    let mut id_to_set_values: HashMap<Vec<SqlValue>, Vec<SqlValue>> = HashMap::new();
    // Dummy schema/row for evaluating the (post-substitution) uncorrelated
    // subquery: after correlation inputs are replaced with literals the subquery
    // resolves purely against its own scope plus the database, so the outer
    // schema/row are unused. `eval_row_value` needs a database-backed evaluator.
    let eval_schema = target_schema;
    let empty_row = Row::new(vec![]);

    for row in rows {
        // Extract identifier values (first num_id_columns values)
        // Normalize integer types to ensure consistent comparison
        let id_values: Vec<SqlValue> =
            row.values[..num_id_columns].iter().map(normalize_integer_type).collect();

        // Skip NULL identifiers
        if id_values.iter().any(|v| matches!(v, SqlValue::Null)) {
            continue;
        }

        // Only keep first match per identifier (SQLite semantics)
        if id_to_set_values.contains_key(&id_values) {
            continue;
        }

        // Walk each assignment's projected synthetic columns and produce the
        // final positionally-flattened SET values.
        let mut set_values: Vec<SqlValue> = Vec::with_capacity(total_set_columns);
        let mut projected_offset = num_id_columns;
        for plan in &assignment_plans {
            match plan {
                AssignmentProjPlan::DirectColumns(n) => {
                    for k in 0..*n {
                        set_values.push(
                            row.values.get(projected_offset + k).cloned().unwrap_or(SqlValue::Null),
                        );
                    }
                    projected_offset += n;
                }
                AssignmentProjPlan::SubqueryOnce { output_arity, corr_refs, subquery } => {
                    // Substitute each projected correlation input as a literal,
                    // carrying the outer column's declared collation (issue #6105)
                    // so collation-sensitive comparisons inside the now-uncorrelated
                    // subquery still use that collation rather than reverting to
                    // BINARY.
                    let mut literal_map: HashMap<CorrKey, (SqlValue, Option<String>)> =
                        HashMap::with_capacity(corr_refs.len());
                    for (k, corr) in corr_refs.iter().enumerate() {
                        let value =
                            row.values.get(projected_offset + k).cloned().unwrap_or(SqlValue::Null);
                        literal_map.insert(corr.key.clone(), (value, corr.collation.clone()));
                    }
                    projected_offset += corr_refs.len();

                    let substituted = substitute_column_literals(
                        &Expression::ScalarSubquery(subquery.clone()),
                        &literal_map,
                    );

                    // Evaluate the now-uncorrelated multi-column subquery exactly
                    // once, distributing its row tuple positionally (issue #6086).
                    let evaluator =
                        crate::evaluator::ExpressionEvaluator::with_database(eval_schema, database);
                    let values =
                        evaluator.eval_row_value(&substituted, &empty_row, *output_arity)?;
                    set_values.extend(values);
                }
            }
        }

        id_to_set_values.insert(id_values, set_values);
    }

    // Now scan the target table to find matching rows
    let target_table = database
        .get_table(target_table_name)
        .ok_or_else(|| ExecutorError::TableNotFound(target_table_name.clone()))?;

    let mut matched_rows = Vec::new();

    // Get PK column indices for lookup
    let pk_indices: Vec<usize> = if use_rowid {
        vec![] // Not used when using rowid
    } else {
        pk_columns.iter().filter_map(|name| target_schema.get_column_index(name)).collect()
    };

    for (row_index, target_row) in target_table.scan().iter().enumerate() {
        // Build identifier for this row
        // Normalize integer types to match the keys in id_to_set_values
        let id_values: Vec<SqlValue> = if use_rowid {
            // Use the row's rowid
            let rowid = target_row
                .row_id
                .map(|id| SqlValue::Integer(id as i64))
                .unwrap_or_else(|| SqlValue::Integer((row_index + 1) as i64));
            vec![normalize_integer_type(&rowid)]
        } else {
            // Use PK column values
            pk_indices
                .iter()
                .map(|&idx| normalize_integer_type(target_row.get(idx).unwrap_or(&SqlValue::Null)))
                .collect()
        };

        // Check if this row matches any from the join result
        if let Some(set_values) = id_to_set_values.remove(&id_values) {
            matched_rows.push(UpdateFromMatch {
                row_index,
                target_row: target_row.clone(),
                set_values,
            });
        }
    }

    Ok(UpdateFromJoinResult { matched_rows })
}

/// Expand a tuple assignment's row-valued RHS into one scalar expression per
/// target column, for projection into the synthetic UPDATE…FROM SELECT
/// (issue #6047).
///
/// A tuple assignment `SET (a, b, …) = value` has a single row-valued `value`
/// whose elements map positionally onto `assignment.columns`. To evaluate each
/// target column as an ordinary scalar in the join context we need a per-column
/// expression:
///
/// - `RowValueConstructor([e0, e1, …])` → the elements directly (`e0`, `e1`, …).
///   Arity is validated against the column list here so a mismatch reports the
///   SQLite "N columns assigned M values" error rather than a downstream shape
///   error.
/// - Any other non-subquery RHS (e.g. `SET (a, b) = 1`) is projected once per
///   column and the scalar evaluator surfaces the SQLite error.
///
/// A `ScalarSubquery` RHS is NOT handled here: it is intercepted earlier and
/// evaluated once per matched row (issue #6086) so its columns keep SQLite's
/// single-pass semantics. The `ScalarSubquery` arm below is retained only as a
/// defensive fallback and is unreachable from the current caller.
fn tuple_assignment_column_exprs(
    assignment: &Assignment,
) -> Result<Vec<Expression>, ExecutorError> {
    let expected = assignment.columns.len();
    match &assignment.value {
        Expression::RowValueConstructor(elems) => {
            if elems.len() != expected {
                return Err(ExecutorError::ColumnsAssignedValues {
                    columns: expected,
                    values: elems.len(),
                });
            }
            Ok(elems.clone())
        }
        Expression::ScalarSubquery(sub) => {
            // Only split when the select-list is a plain list of expressions
            // matching the target arity (no wildcards). Otherwise leave the
            // subquery whole in every slot and let the scalar evaluator report
            // the arity mismatch (matching the non-split fallback below).
            let can_split = sub.select_list.len() == expected
                && sub.select_list.iter().all(|item| matches!(item, SelectItem::Expression { .. }));
            if can_split {
                Ok((0..expected)
                    .map(|j| {
                        let mut narrowed = (**sub).clone();
                        narrowed.select_list = vec![sub.select_list[j].clone()];
                        Expression::ScalarSubquery(Box::new(narrowed))
                    })
                    .collect())
            } else {
                // Fall back to projecting the whole subquery for each column;
                // the scalar evaluator will surface the correct arity error.
                Ok(vec![assignment.value.clone(); expected])
            }
        }
        // Any other RHS for a multi-column target is a misuse per SQLite
        // (e.g. `SET (a, b) = 1`). Project it once per column and let the
        // scalar evaluator handle it consistently with the non-FROM path.
        _ => Ok(vec![assignment.value.clone(); expected]),
    }
}

/// Canonical key identifying an outer column reference the subquery correlates
/// on: `(optional-table, column)` (both lowercased). Distinct qualified/
/// unqualified spellings of the same column collapse to distinct keys so
/// substitution replaces exactly the spelling that appeared.
#[derive(Clone, PartialEq, Eq, Hash)]
struct CorrKey {
    table: Option<String>,
    column: String,
}

/// A single outer column reference of a tuple subquery, with the join-context
/// expression to project (`__corr_i_k__`) and the key used to substitute it back
/// as a literal at evaluation time.
struct CorrRef {
    key: CorrKey,
    expr: Expression,
    /// The outer column's *declared* collating sequence, if any (issue #6105).
    /// When the substituted value replaces this ref as a literal, this collation
    /// is carried on the resulting node so collation-sensitive comparisons inside
    /// the (now uncorrelated) subquery still use the column's collation rather
    /// than silently reverting to BINARY. `None` (and an explicit `"BINARY"`)
    /// means default BINARY — a plain literal is emitted.
    collation: Option<String>,
}

/// How an UPDATE…FROM assignment maps its projected synthetic columns to the
/// final positionally-flattened SET values.
enum AssignmentProjPlan {
    /// The assignment projects `n` scalar columns directly usable as SET values
    /// (single-column assignments contribute 1; direct row-value tuples
    /// contribute one per target column).
    DirectColumns(usize),
    /// A tuple subquery assignment (issue #6086): it projects only its `corr_refs`
    /// correlation inputs. The final `output_arity` SET values are produced by
    /// substituting those inputs as literals into `subquery` and evaluating it
    /// exactly once per matched row.
    SubqueryOnce { output_arity: usize, corr_refs: Vec<CorrRef>, subquery: Box<SelectStmt> },
}

impl AssignmentProjPlan {
    /// Number of final SET values this assignment contributes.
    fn output_columns(&self) -> usize {
        match self {
            AssignmentProjPlan::DirectColumns(n) => *n,
            AssignmentProjPlan::SubqueryOnce { output_arity, .. } => *output_arity,
        }
    }
}

/// Resolve the *declared* collating sequence of an outer column reference
/// (issue #6105) so it can be carried on the substituted literal.
///
/// - Qualified (`t.c`): looks the column up in table `t`'s schema.
/// - Unqualified (`c`): finds the first outer table declaring a column `c`
///   (mirroring the resolution used to classify the ref as outer above).
///
/// Returns the column's declared collation (`None` = default BINARY). A collation
/// that names BINARY is treated as BINARY by the caller (a plain literal).
fn outer_column_collation(
    table: &Option<String>,
    column: &str,
    outer_tables: &HashMap<String, TableSchema>,
) -> Option<String> {
    match table {
        Some(t) => outer_tables
            .get(&t.to_lowercase())
            .and_then(|schema| schema.get_column(column))
            .and_then(|col| col.collation.clone()),
        None => outer_tables
            .values()
            .find_map(|schema| schema.get_column(column))
            .and_then(|col| col.collation.clone()),
    }
}

/// Collect the lowercased names/aliases of the tables the UPDATE…FROM join binds
/// in the *outer* scope: the target table (and its alias) plus every table named
/// in the FROM clauses. A tuple subquery's column reference is treated as an
/// outer (correlation) reference when it resolves to one of these tables and not
/// to the subquery's own local scope.
fn collect_outer_table_schemas(
    target_schema: &TableSchema,
    target_prefix: &str,
    from_clauses: &[FromClause],
    database: &Database,
) -> HashMap<String, TableSchema> {
    let mut out: HashMap<String, TableSchema> = HashMap::new();
    out.insert(target_prefix.to_lowercase(), target_schema.clone());
    out.insert(target_schema.name.to_lowercase(), target_schema.clone());
    for fc in from_clauses {
        collect_from_clause_tables(fc, database, &mut out);
    }
    out
}

/// Recursively register the base tables (with any alias) referenced by a FROM
/// clause. Only plain `Table` sources contribute a resolvable schema; derived
/// tables / VALUES / table-functions are skipped (their columns are handled by
/// the synthetic SELECT's own correlation resolution when projected directly, and
/// tuple subqueries correlating on them are uncommon).
fn collect_from_clause_tables(
    fc: &FromClause,
    database: &Database,
    out: &mut HashMap<String, TableSchema>,
) {
    match fc {
        FromClause::Table { name, alias, .. } => {
            if let Some(table) = database.get_table(name) {
                let schema = table.schema.clone();
                out.insert(name.to_lowercase(), schema.clone());
                if let Some(a) = alias {
                    out.insert(a.to_lowercase(), schema);
                }
            }
        }
        FromClause::Join { left, right, .. } => {
            collect_from_clause_tables(left, database, out);
            collect_from_clause_tables(right, database, out);
        }
        FromClause::Subquery { .. }
        | FromClause::Values { .. }
        | FromClause::TableFunction { .. } => {}
    }
}

/// Collect the tuple subquery's *outer* column references — those correlating
/// against the UPDATE…FROM join row rather than the subquery's own scope.
///
/// A reference is treated as outer when:
/// - it is qualified (`t.c`) and `t` is one of the outer (target/FROM) tables and
///   NOT a table bound locally inside the subquery, or
/// - it is unqualified and matches a column of some outer table while not being a
///   column of any table the subquery binds locally.
///
/// References that resolve to the subquery's own scope are left untouched so the
/// subquery evaluator resolves them normally. Only the *top-level* subquery scope
/// is analysed for local bindings; nested subqueries within it correlate through
/// the same literal-substitution because their outer refs are a subset of these.
fn collect_outer_column_refs(
    subquery: &SelectStmt,
    outer_tables: &HashMap<String, TableSchema>,
) -> Vec<CorrRef> {
    // Tables bound locally by this subquery's own FROM clause.
    let mut local_tables: HashSet<String> = HashSet::new();
    let mut local_columns: HashSet<String> = HashSet::new();
    if let Some(from) = &subquery.from {
        collect_local_bindings(from, &mut local_tables, &mut local_columns);
    }

    let mut seen: HashSet<CorrKey> = HashSet::new();
    let mut refs: Vec<CorrRef> = Vec::new();
    for item in &subquery.select_list {
        if let SelectItem::Expression { expr, .. } = item {
            collect_outer_refs_in_expr(
                expr,
                outer_tables,
                &local_tables,
                &local_columns,
                &mut seen,
                &mut refs,
            );
        }
    }
    // Also consider the subquery's WHERE clause and other clauses that may carry
    // outer correlation (e.g. `(SELECT c FROM s WHERE s.k = t1.k LIMIT 1, ...)`).
    if let Some(cond) = &subquery.where_clause {
        collect_outer_refs_in_expr(
            cond,
            outer_tables,
            &local_tables,
            &local_columns,
            &mut seen,
            &mut refs,
        );
    }
    refs
}

/// Register the table names/aliases and column names a subquery FROM clause binds
/// locally. Used to distinguish inner references from outer correlation refs.
fn collect_local_bindings(
    from: &FromClause,
    tables: &mut HashSet<String>,
    columns: &mut HashSet<String>,
) {
    match from {
        FromClause::Table { name, alias, column_aliases, .. } => {
            tables.insert(name.to_lowercase());
            if let Some(a) = alias {
                tables.insert(a.to_lowercase());
            }
            if let Some(cols) = column_aliases {
                for c in cols {
                    columns.insert(c.to_lowercase());
                }
            }
        }
        FromClause::Join { left, right, .. } => {
            collect_local_bindings(left, tables, columns);
            collect_local_bindings(right, tables, columns);
        }
        FromClause::Subquery { alias, column_aliases, .. } => {
            tables.insert(alias.to_lowercase());
            if let Some(cols) = column_aliases {
                for c in cols {
                    columns.insert(c.to_lowercase());
                }
            }
        }
        FromClause::Values { alias, column_aliases, .. } => {
            tables.insert(alias.to_lowercase());
            if let Some(cols) = column_aliases {
                for c in cols {
                    columns.insert(c.to_lowercase());
                }
            }
        }
        FromClause::TableFunction { name, alias, column_aliases, .. } => {
            tables.insert(name.to_lowercase());
            if let Some(a) = alias {
                tables.insert(a.to_lowercase());
            }
            if let Some(cols) = column_aliases {
                for c in cols {
                    columns.insert(c.to_lowercase());
                }
            }
        }
    }
}

/// Walk an expression, appending each distinct *outer* column reference to `refs`.
///
/// Nested subqueries ARE descended into, extending the local-binding scope with
/// the nested subquery's own FROM tables/columns, so a correlation ref buried in
/// a nested subquery (e.g. `sum((SELECT t1.y))`) is still collected while a name
/// bound by the nested subquery's own FROM is not misclassified as outer. This
/// matches SQLite's scoping: a ref is outer iff no enclosing scope up to (and
/// including) the tuple subquery binds it.
#[allow(clippy::only_used_in_recursion)]
fn collect_outer_refs_in_expr(
    expr: &Expression,
    outer_tables: &HashMap<String, TableSchema>,
    local_tables: &HashSet<String>,
    local_columns: &HashSet<String>,
    seen: &mut HashSet<CorrKey>,
    refs: &mut Vec<CorrRef>,
) {
    match expr {
        Expression::ColumnRef(id) => {
            let column = id.column_canonical().to_string();
            let table = id.table_canonical().map(|t| t.to_string());
            let is_outer = match &table {
                Some(t) => outer_tables.contains_key(t) && !local_tables.contains(t),
                None => {
                    // Unqualified: outer only if it names a column of some outer
                    // table and is not locally bound.
                    !local_columns.contains(&column)
                        && outer_tables.values().any(|s| s.get_column_index(&column).is_some())
                }
            };
            if is_outer {
                let key = CorrKey { table: table.clone(), column: column.clone() };
                if seen.insert(key.clone()) {
                    let collation = outer_column_collation(&table, &column, outer_tables);
                    refs.push(CorrRef { key, expr: expr.clone(), collation });
                }
            }
        }
        Expression::BinaryOp { left, right, .. } => {
            collect_outer_refs_in_expr(left, outer_tables, local_tables, local_columns, seen, refs);
            collect_outer_refs_in_expr(
                right,
                outer_tables,
                local_tables,
                local_columns,
                seen,
                refs,
            );
        }
        Expression::UnaryOp { expr, .. }
        | Expression::Cast { expr, .. }
        | Expression::Collate { expr, .. }
        | Expression::IsNull { expr, .. } => {
            collect_outer_refs_in_expr(expr, outer_tables, local_tables, local_columns, seen, refs);
        }
        Expression::Function { args, .. } | Expression::AggregateFunction { args, .. } => {
            for a in args {
                collect_outer_refs_in_expr(
                    a,
                    outer_tables,
                    local_tables,
                    local_columns,
                    seen,
                    refs,
                );
            }
        }
        Expression::Conjunction(parts) | Expression::Disjunction(parts) => {
            for p in parts {
                collect_outer_refs_in_expr(
                    p,
                    outer_tables,
                    local_tables,
                    local_columns,
                    seen,
                    refs,
                );
            }
        }
        Expression::Between { expr, low, high, .. } => {
            collect_outer_refs_in_expr(expr, outer_tables, local_tables, local_columns, seen, refs);
            collect_outer_refs_in_expr(low, outer_tables, local_tables, local_columns, seen, refs);
            collect_outer_refs_in_expr(high, outer_tables, local_tables, local_columns, seen, refs);
        }
        Expression::InList { expr, values, .. } => {
            collect_outer_refs_in_expr(expr, outer_tables, local_tables, local_columns, seen, refs);
            for v in values {
                collect_outer_refs_in_expr(
                    v,
                    outer_tables,
                    local_tables,
                    local_columns,
                    seen,
                    refs,
                );
            }
        }
        Expression::Like { expr, pattern, .. } | Expression::Glob { expr, pattern, .. } => {
            collect_outer_refs_in_expr(expr, outer_tables, local_tables, local_columns, seen, refs);
            collect_outer_refs_in_expr(
                pattern,
                outer_tables,
                local_tables,
                local_columns,
                seen,
                refs,
            );
        }
        Expression::Case { operand, when_clauses, else_result } => {
            if let Some(op) = operand {
                collect_outer_refs_in_expr(
                    op,
                    outer_tables,
                    local_tables,
                    local_columns,
                    seen,
                    refs,
                );
            }
            for clause in when_clauses {
                for cond in &clause.conditions {
                    collect_outer_refs_in_expr(
                        cond,
                        outer_tables,
                        local_tables,
                        local_columns,
                        seen,
                        refs,
                    );
                }
                collect_outer_refs_in_expr(
                    &clause.result,
                    outer_tables,
                    local_tables,
                    local_columns,
                    seen,
                    refs,
                );
            }
            if let Some(e) = else_result {
                collect_outer_refs_in_expr(
                    e,
                    outer_tables,
                    local_tables,
                    local_columns,
                    seen,
                    refs,
                );
            }
        }
        Expression::RowValueConstructor(elems) => {
            for e in elems {
                collect_outer_refs_in_expr(
                    e,
                    outer_tables,
                    local_tables,
                    local_columns,
                    seen,
                    refs,
                );
            }
        }
        Expression::ScalarSubquery(sub) => {
            collect_outer_refs_in_select(
                sub,
                outer_tables,
                local_tables,
                local_columns,
                seen,
                refs,
            );
        }
        Expression::Exists { subquery, .. } => {
            collect_outer_refs_in_select(
                subquery,
                outer_tables,
                local_tables,
                local_columns,
                seen,
                refs,
            );
        }
        Expression::In { expr: inner, subquery, .. } => {
            collect_outer_refs_in_expr(
                inner,
                outer_tables,
                local_tables,
                local_columns,
                seen,
                refs,
            );
            collect_outer_refs_in_select(
                subquery,
                outer_tables,
                local_tables,
                local_columns,
                seen,
                refs,
            );
        }
        Expression::WindowFunction { function, over } => {
            let (args, filter) = window_function_parts(function);
            for a in args {
                collect_outer_refs_in_expr(
                    a,
                    outer_tables,
                    local_tables,
                    local_columns,
                    seen,
                    refs,
                );
            }
            if let Some(f) = filter {
                collect_outer_refs_in_expr(
                    f,
                    outer_tables,
                    local_tables,
                    local_columns,
                    seen,
                    refs,
                );
            }
            if let Some(pb) = &over.partition_by {
                for e in pb {
                    collect_outer_refs_in_expr(
                        e,
                        outer_tables,
                        local_tables,
                        local_columns,
                        seen,
                        refs,
                    );
                }
            }
            if let Some(ob) = &over.order_by {
                for item in ob {
                    collect_outer_refs_in_expr(
                        &item.expr,
                        outer_tables,
                        local_tables,
                        local_columns,
                        seen,
                        refs,
                    );
                }
            }
        }
        // Do not descend into nested subqueries (see doc comment). Literals,
        // placeholders, and other leaf/opaque forms carry no outer ref.
        _ => {}
    }
}

/// Descend into a nested SELECT, collecting its outer column references relative
/// to the tuple subquery's join scope. The nested subquery's own FROM bindings
/// are unioned into the local scope so its inner refs are not misclassified as
/// outer correlation.
fn collect_outer_refs_in_select(
    select: &SelectStmt,
    outer_tables: &HashMap<String, TableSchema>,
    local_tables: &HashSet<String>,
    local_columns: &HashSet<String>,
    seen: &mut HashSet<CorrKey>,
    refs: &mut Vec<CorrRef>,
) {
    let mut nested_tables = local_tables.clone();
    let mut nested_columns = local_columns.clone();
    if let Some(from) = &select.from {
        collect_local_bindings(from, &mut nested_tables, &mut nested_columns);
    }
    for item in &select.select_list {
        if let SelectItem::Expression { expr, .. } = item {
            collect_outer_refs_in_expr(
                expr,
                outer_tables,
                &nested_tables,
                &nested_columns,
                seen,
                refs,
            );
        }
    }
    if let Some(cond) = &select.where_clause {
        collect_outer_refs_in_expr(cond, outer_tables, &nested_tables, &nested_columns, seen, refs);
    }
    if let Some(having) = &select.having {
        collect_outer_refs_in_expr(
            having,
            outer_tables,
            &nested_tables,
            &nested_columns,
            seen,
            refs,
        );
    }
    if let Some(vibesql_ast::GroupByClause::Simple(exprs)) = &select.group_by {
        for e in exprs {
            collect_outer_refs_in_expr(
                e,
                outer_tables,
                &nested_tables,
                &nested_columns,
                seen,
                refs,
            );
        }
    }
}

/// Borrow a window function's argument expressions and optional FILTER for
/// outer-reference collection.
fn window_function_parts(
    spec: &vibesql_ast::WindowFunctionSpec,
) -> (&[Expression], Option<&Expression>) {
    match spec {
        vibesql_ast::WindowFunctionSpec::Aggregate { args, filter, .. } => {
            (args.as_slice(), filter.as_deref())
        }
        vibesql_ast::WindowFunctionSpec::Ranking { args, .. }
        | vibesql_ast::WindowFunctionSpec::Value { args, .. } => (args.as_slice(), None),
    }
}

/// Replace every `ColumnRef` matching a key in `literals` with the corresponding
/// literal value. Nested subqueries are rewritten too, so a correlation ref used
/// inside an inner subquery of the tuple RHS is also substituted.
fn substitute_column_literals(
    expr: &Expression,
    literals: &HashMap<CorrKey, (SqlValue, Option<String>)>,
) -> Expression {
    use vibesql_ast::CaseWhen;
    match expr {
        Expression::ColumnRef(id) => {
            let key = CorrKey {
                table: id.table_canonical().map(|t| t.to_string()),
                column: id.column_canonical().to_string(),
            };
            match literals.get(&key) {
                // Carry the outer column's collation onto the substituted value
                // (issue #6105). The result is a `CollatedLiteral` — an
                // *implicit* collating operand behaving exactly like the original
                // column reference under datatype3 §7.1 rule 2 (left-operand
                // precedence), NOT an explicit `COLLATE` (rule 1).
                //
                // This holds even for a BINARY column (no declared collation):
                // a `CollatedLiteral{collation:"BINARY"}` is still a *collating
                // operand*, so when it is the left operand its default BINARY
                // blocks the right operand's implicit collation — exactly as a
                // real column would. Emitting a bare `Expression::Literal` here
                // (which is NOT a collating operand) would wrongly let the right
                // operand's collation win, e.g. `outer_binary_col = inner_nocase`
                // would compare NOCASE instead of SQLite's BINARY.
                Some((value, declared)) => Expression::CollatedLiteral {
                    value: value.clone(),
                    collation: declared.clone().unwrap_or_else(|| "BINARY".to_string()),
                },
                None => expr.clone(),
            }
        }
        Expression::BinaryOp { op, left, right } => Expression::BinaryOp {
            op: *op,
            left: Box::new(substitute_column_literals(left, literals)),
            right: Box::new(substitute_column_literals(right, literals)),
        },
        Expression::UnaryOp { op, expr: inner } => Expression::UnaryOp {
            op: *op,
            expr: Box::new(substitute_column_literals(inner, literals)),
        },
        Expression::Conjunction(parts) => Expression::Conjunction(
            parts.iter().map(|p| substitute_column_literals(p, literals)).collect(),
        ),
        Expression::Disjunction(parts) => Expression::Disjunction(
            parts.iter().map(|p| substitute_column_literals(p, literals)).collect(),
        ),
        Expression::Function { name, args, character_unit } => Expression::Function {
            name: name.clone(),
            args: args.iter().map(|a| substitute_column_literals(a, literals)).collect(),
            character_unit: character_unit.clone(),
        },
        Expression::AggregateFunction { name, distinct, args, order_by, filter } => {
            Expression::AggregateFunction {
                name: name.clone(),
                distinct: *distinct,
                args: args.iter().map(|a| substitute_column_literals(a, literals)).collect(),
                order_by: order_by.clone(),
                filter: filter.as_ref().map(|f| Box::new(substitute_column_literals(f, literals))),
            }
        }
        Expression::Cast { expr: inner, data_type } => Expression::Cast {
            expr: Box::new(substitute_column_literals(inner, literals)),
            data_type: data_type.clone(),
        },
        Expression::Collate { expr: inner, collation } => Expression::Collate {
            expr: Box::new(substitute_column_literals(inner, literals)),
            collation: collation.clone(),
        },
        Expression::IsNull { expr: inner, negated } => Expression::IsNull {
            expr: Box::new(substitute_column_literals(inner, literals)),
            negated: *negated,
        },
        Expression::Between { expr: inner, low, high, negated, symmetric } => Expression::Between {
            expr: Box::new(substitute_column_literals(inner, literals)),
            low: Box::new(substitute_column_literals(low, literals)),
            high: Box::new(substitute_column_literals(high, literals)),
            negated: *negated,
            symmetric: *symmetric,
        },
        Expression::InList { expr: inner, values, negated } => Expression::InList {
            expr: Box::new(substitute_column_literals(inner, literals)),
            values: values.iter().map(|v| substitute_column_literals(v, literals)).collect(),
            negated: *negated,
        },
        Expression::Like { expr: inner, pattern, negated, escape } => Expression::Like {
            expr: Box::new(substitute_column_literals(inner, literals)),
            pattern: Box::new(substitute_column_literals(pattern, literals)),
            negated: *negated,
            escape: escape.as_ref().map(|e| Box::new(substitute_column_literals(e, literals))),
        },
        Expression::Glob { expr: inner, pattern, negated, escape } => Expression::Glob {
            expr: Box::new(substitute_column_literals(inner, literals)),
            pattern: Box::new(substitute_column_literals(pattern, literals)),
            negated: *negated,
            escape: escape.as_ref().map(|e| Box::new(substitute_column_literals(e, literals))),
        },
        Expression::Case { operand, when_clauses, else_result } => Expression::Case {
            operand: operand.as_ref().map(|o| Box::new(substitute_column_literals(o, literals))),
            when_clauses: when_clauses
                .iter()
                .map(|w| CaseWhen {
                    conditions: w
                        .conditions
                        .iter()
                        .map(|c| substitute_column_literals(c, literals))
                        .collect(),
                    result: substitute_column_literals(&w.result, literals),
                })
                .collect(),
            else_result: else_result
                .as_ref()
                .map(|e| Box::new(substitute_column_literals(e, literals))),
        },
        Expression::RowValueConstructor(elems) => Expression::RowValueConstructor(
            elems.iter().map(|e| substitute_column_literals(e, literals)).collect(),
        ),
        Expression::ScalarSubquery(sub) => Expression::ScalarSubquery(Box::new(
            substitute_column_literals_in_select(sub, literals),
        )),
        Expression::Exists { subquery, negated } => Expression::Exists {
            subquery: Box::new(substitute_column_literals_in_select(subquery, literals)),
            negated: *negated,
        },
        Expression::In { expr: inner, subquery, negated } => Expression::In {
            expr: Box::new(substitute_column_literals(inner, literals)),
            subquery: Box::new(substitute_column_literals_in_select(subquery, literals)),
            negated: *negated,
        },
        Expression::WindowFunction { function, over } => Expression::WindowFunction {
            function: substitute_column_literals_in_window_spec(function, literals),
            over: substitute_column_literals_in_over(over, literals),
        },
        // Leaf / unsupported-here forms are returned unchanged.
        _ => expr.clone(),
    }
}

/// Substitute correlation literals inside a window function's args / FILTER.
fn substitute_column_literals_in_window_spec(
    spec: &vibesql_ast::WindowFunctionSpec,
    literals: &HashMap<CorrKey, (SqlValue, Option<String>)>,
) -> vibesql_ast::WindowFunctionSpec {
    use vibesql_ast::WindowFunctionSpec;
    match spec {
        WindowFunctionSpec::Aggregate { name, args, filter } => WindowFunctionSpec::Aggregate {
            name: name.clone(),
            args: args.iter().map(|a| substitute_column_literals(a, literals)).collect(),
            filter: filter.as_ref().map(|f| Box::new(substitute_column_literals(f, literals))),
        },
        WindowFunctionSpec::Ranking { name, args } => WindowFunctionSpec::Ranking {
            name: name.clone(),
            args: args.iter().map(|a| substitute_column_literals(a, literals)).collect(),
        },
        WindowFunctionSpec::Value { name, args } => WindowFunctionSpec::Value {
            name: name.clone(),
            args: args.iter().map(|a| substitute_column_literals(a, literals)).collect(),
        },
    }
}

/// Substitute correlation literals inside a window OVER clause's PARTITION BY /
/// ORDER BY expressions. FRAME bounds and named-window inheritance are left
/// as-is (they do not carry correlation refs for the shapes this path handles).
fn substitute_column_literals_in_over(
    over: &vibesql_ast::WindowSpec,
    literals: &HashMap<CorrKey, (SqlValue, Option<String>)>,
) -> vibesql_ast::WindowSpec {
    let mut cloned = over.clone();
    cloned.partition_by = over
        .partition_by
        .as_ref()
        .map(|pb| pb.iter().map(|e| substitute_column_literals(e, literals)).collect());
    cloned.order_by = over.order_by.as_ref().map(|ob| {
        ob.iter()
            .map(|item| vibesql_ast::OrderByItem {
                expr: substitute_column_literals(&item.expr, literals),
                direction: item.direction.clone(),
                nulls_order: item.nulls_order,
            })
            .collect()
    });
    cloned
}

/// Apply `substitute_column_literals` across the correlation-bearing parts of a
/// SELECT (select-list expressions, WHERE, HAVING). FROM/GROUP/ORDER are left
/// intact — correlation into those is not expected for the tuple RHS shapes this
/// path handles.
fn substitute_column_literals_in_select(
    select: &SelectStmt,
    literals: &HashMap<CorrKey, (SqlValue, Option<String>)>,
) -> SelectStmt {
    let mut cloned = select.clone();
    cloned.select_list = select
        .select_list
        .iter()
        .map(|item| match item {
            SelectItem::Expression { expr, alias, source_text } => SelectItem::Expression {
                expr: substitute_column_literals(expr, literals),
                alias: alias.clone(),
                source_text: source_text.clone(),
            },
            other => other.clone(),
        })
        .collect();
    if let Some(cond) = &select.where_clause {
        cloned.where_clause = Some(substitute_column_literals(cond, literals));
    }
    if let Some(having) = &select.having {
        cloned.having = Some(substitute_column_literals(having, literals));
    }
    if let Some(vibesql_ast::GroupByClause::Simple(exprs)) = &select.group_by {
        cloned.group_by = Some(vibesql_ast::GroupByClause::Simple(
            exprs.iter().map(|e| substitute_column_literals(e, literals)).collect(),
        ));
    }
    cloned
}

/// Get the PRIMARY KEY column names for a table
fn get_pk_column_names(schema: &TableSchema) -> Vec<String> {
    // Primary key is stored as Option<Vec<String>> in TableSchema
    schema.primary_key.clone().unwrap_or_default()
}

/// Combine the accumulated FROM clause with a new FROM clause
///
/// For simple tables, this creates a CROSS JOIN.
/// For JOIN-type FROM clauses, this extracts the leftmost table, cross-joins with it,
/// then reattaches the rest of the join structure. This matches SQLite's parsing
/// of comma-separated FROM items: `t1, t2 LEFT JOIN t3` = `(t1 CROSS JOIN t2) LEFT JOIN t3`
fn combine_with_from_clause(accumulated: FromClause, from_clause: FromClause) -> FromClause {
    match from_clause {
        // For a simple table, just cross join
        FromClause::Table { .. }
        | FromClause::Subquery { .. }
        | FromClause::Values { .. }
        | FromClause::TableFunction { .. } => FromClause::Join {
            left: Box::new(accumulated),
            right: Box::new(from_clause),
            join_type: JoinType::Cross,
            condition: None,
            using_columns: None,
            natural: false,
            alias: None,
        },
        // For a JOIN, we need to inject the accumulated clause on the left side
        FromClause::Join { left, right, join_type, condition, using_columns, natural, alias } => {
            // Recursively combine with the left side of the join
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

/// Apply UPDATE FROM matches to build update operations
///
/// Takes the matched rows with pre-computed SET values and produces a
/// `Vec<PendingUpdate>` for the executor's two-phase apply step.
pub(super) fn apply_update_from_matches(
    matches: &[UpdateFromMatch],
    assignments: &[Assignment],
    target_schema: &TableSchema,
) -> Result<Vec<PendingUpdate>, ExecutorError> {
    let pk_indices = target_schema.get_primary_key_indices();
    let mut updates = Vec::with_capacity(matches.len());

    for m in matches {
        let mut new_row = m.target_row.clone();
        let mut changed_columns = HashSet::new();
        let mut updates_pk = false;

        // `m.set_values` is a positional flattening of every SET column: a
        // single-column assignment consumes one value, a tuple assignment
        // consumes one value per target column (issue #6047). Track the running
        // offset into `set_values` as we walk assignments.
        let mut value_offset = 0usize;

        for assignment in assignments {
            // Tuple assignment `(a, b, …) = (row-value | subquery)`: consume one
            // computed value per target column, in order.
            if assignment.is_tuple() {
                for col_name in &assignment.columns {
                    let value = m.set_values.get(value_offset).cloned().unwrap_or(SqlValue::Null);
                    value_offset += 1;

                    let col_index = target_schema.get_column_index(col_name).ok_or_else(|| {
                        ExecutorError::NoSuchColumn { column_ref: col_name.clone() }
                    })?;
                    let coerced_value = crate::insert::validation::coerce_value(
                        value,
                        &target_schema.columns[col_index].data_type,
                    )?;
                    new_row
                        .set(col_index, coerced_value)
                        .map_err(|e| ExecutorError::StorageError(e.to_string()))?;
                    changed_columns.insert(col_index);
                    if let Some(ref pk) = pk_indices {
                        if pk.contains(&col_index) {
                            updates_pk = true;
                        }
                    }
                }
                continue;
            }

            let value = m.set_values.get(value_offset).cloned().unwrap_or(SqlValue::Null);
            value_offset += 1;

            // Handle rowid assignment specially
            let col_name_lower = assignment.column.to_lowercase();
            let is_rowid =
                col_name_lower == "rowid" || col_name_lower == "_rowid_" || col_name_lower == "oid";

            if is_rowid {
                // Handle rowid update
                if let Some(ipk_col_idx) = target_schema.rowid_alias_column {
                    new_row
                        .set(ipk_col_idx, value)
                        .map_err(|e| ExecutorError::StorageError(e.to_string()))?;
                    changed_columns.insert(ipk_col_idx);
                    if pk_indices.as_ref().is_some_and(|pk| pk.contains(&ipk_col_idx)) {
                        updates_pk = true;
                    }
                } else {
                    // Update virtual rowid
                    let new_rowid = match &value {
                        SqlValue::Integer(id) => *id as u64,
                        SqlValue::Bigint(id) => *id as u64,
                        other => {
                            return Err(ExecutorError::UnsupportedExpression(format!(
                                "ROWID must be an integer, got {:?}",
                                other
                            )));
                        }
                    };
                    new_row.row_id = Some(new_rowid);
                }
                continue;
            }

            // Find column index
            let col_index =
                target_schema.get_column_index(&assignment.column).ok_or_else(|| {
                    ExecutorError::NoSuchColumn { column_ref: assignment.column.clone() }
                })?;

            // Coerce value to column type
            let coerced_value = crate::insert::validation::coerce_value(
                value,
                &target_schema.columns[col_index].data_type,
            )?;

            new_row
                .set(col_index, coerced_value)
                .map_err(|e| ExecutorError::StorageError(e.to_string()))?;
            changed_columns.insert(col_index);

            // Check if this column is part of primary key
            if let Some(ref pk) = pk_indices {
                if pk.contains(&col_index) {
                    updates_pk = true;
                }
            }
        }

        updates.push(PendingUpdate {
            row_index: m.row_index,
            old_row: m.target_row.clone(),
            new_row,
            changed_columns,
            updates_pk,
        });
    }

    Ok(updates)
}

/// Recursively substitute `OLD.col` / `NEW.col` pseudo-variable references in
/// `expr` with the corresponding literal values from `trigger_context`.
///
/// This is the substitution pass used by `UPDATE … FROM …` inside trigger
/// bodies (issue #5082, Bucket B of #5073). Pre-resolving the pseudo-variables
/// to literals lets the synthetic SELECT execute through the normal scan/join
/// pipeline without needing trigger context to be threaded through every
/// scan-level evaluator constructor.
///
/// Subqueries inside the expression are walked recursively but the SelectStmt
/// itself is left structurally intact — only `Expression::PseudoVariable` nodes
/// are rewritten to `Expression::Literal`.
fn substitute_pseudo_vars(
    expr: &Expression,
    trigger_context: &crate::trigger_execution::TriggerContext<'_>,
) -> Result<Expression, ExecutorError> {
    use vibesql_ast::CaseWhen;
    Ok(match expr {
        Expression::PseudoVariable { pseudo_table, column } => {
            let value = trigger_context.resolve_pseudo_var(*pseudo_table, column)?;
            Expression::Literal(value)
        }
        Expression::Literal(_)
        | Expression::CollatedLiteral { .. }
        | Expression::Placeholder(_)
        | Expression::NumberedPlaceholder(_)
        | Expression::NamedPlaceholder(_)
        | Expression::ColumnRef(_)
        | Expression::Wildcard
        | Expression::CurrentDate
        | Expression::CurrentTime { .. }
        | Expression::CurrentTimestamp { .. }
        | Expression::Default
        | Expression::DuplicateKeyValue { .. }
        | Expression::NextValue { .. }
        | Expression::SessionVariable { .. } => expr.clone(),

        Expression::BinaryOp { op, left, right } => Expression::BinaryOp {
            op: op.clone(),
            left: Box::new(substitute_pseudo_vars(left, trigger_context)?),
            right: Box::new(substitute_pseudo_vars(right, trigger_context)?),
        },
        Expression::Conjunction(children) => Expression::Conjunction(
            children
                .iter()
                .map(|c| substitute_pseudo_vars(c, trigger_context))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        Expression::Disjunction(children) => Expression::Disjunction(
            children
                .iter()
                .map(|c| substitute_pseudo_vars(c, trigger_context))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        Expression::UnaryOp { op, expr: inner } => Expression::UnaryOp {
            op: op.clone(),
            expr: Box::new(substitute_pseudo_vars(inner, trigger_context)?),
        },
        Expression::Function { name, args, character_unit } => Expression::Function {
            name: name.clone(),
            args: args
                .iter()
                .map(|a| substitute_pseudo_vars(a, trigger_context))
                .collect::<Result<Vec<_>, _>>()?,
            character_unit: character_unit.clone(),
        },
        Expression::AggregateFunction { name, distinct, args, order_by, filter } => {
            Expression::AggregateFunction {
                name: name.clone(),
                distinct: *distinct,
                args: args
                    .iter()
                    .map(|a| substitute_pseudo_vars(a, trigger_context))
                    .collect::<Result<Vec<_>, _>>()?,
                order_by: order_by.clone(),
                filter: match filter {
                    Some(f) => Some(Box::new(substitute_pseudo_vars(f, trigger_context)?)),
                    None => None,
                },
            }
        }
        Expression::IsNull { expr: inner, negated } => Expression::IsNull {
            expr: Box::new(substitute_pseudo_vars(inner, trigger_context)?),
            negated: *negated,
        },
        Expression::IsDistinctFrom { left, right, negated } => Expression::IsDistinctFrom {
            left: Box::new(substitute_pseudo_vars(left, trigger_context)?),
            right: Box::new(substitute_pseudo_vars(right, trigger_context)?),
            negated: *negated,
        },
        Expression::IsTruthValue { expr: inner, truth_value, negated } => {
            Expression::IsTruthValue {
                expr: Box::new(substitute_pseudo_vars(inner, trigger_context)?),
                truth_value: *truth_value,
                negated: *negated,
            }
        }
        Expression::Case { operand, when_clauses, else_result } => Expression::Case {
            operand: match operand {
                Some(o) => Some(Box::new(substitute_pseudo_vars(o, trigger_context)?)),
                None => None,
            },
            when_clauses: when_clauses
                .iter()
                .map(|w| {
                    Ok::<CaseWhen, ExecutorError>(CaseWhen {
                        conditions: w
                            .conditions
                            .iter()
                            .map(|c| substitute_pseudo_vars(c, trigger_context))
                            .collect::<Result<Vec<_>, _>>()?,
                        result: substitute_pseudo_vars(&w.result, trigger_context)?,
                    })
                })
                .collect::<Result<Vec<_>, _>>()?,
            else_result: match else_result {
                Some(e) => Some(Box::new(substitute_pseudo_vars(e, trigger_context)?)),
                None => None,
            },
        },
        // Subqueries: leave SelectStmt intact (it can reference OLD/NEW too,
        // but resolving those would require walking the entire SELECT tree;
        // SQLite's behavior for OLD/NEW inside subqueries within UPDATE…FROM
        // matches our existing single-table evaluator behavior, which is OK
        // because correlated subqueries inside a trigger body still go
        // through the per-row evaluator that has trigger context).
        Expression::ScalarSubquery(_) | Expression::Exists { .. } => expr.clone(),
        Expression::In { expr: inner, subquery, negated } => Expression::In {
            expr: Box::new(substitute_pseudo_vars(inner, trigger_context)?),
            subquery: subquery.clone(),
            negated: *negated,
        },
        Expression::InList { expr: inner, values, negated } => Expression::InList {
            expr: Box::new(substitute_pseudo_vars(inner, trigger_context)?),
            values: values
                .iter()
                .map(|v| substitute_pseudo_vars(v, trigger_context))
                .collect::<Result<Vec<_>, _>>()?,
            negated: *negated,
        },
        Expression::Between { expr: inner, low, high, negated, symmetric } => Expression::Between {
            expr: Box::new(substitute_pseudo_vars(inner, trigger_context)?),
            low: Box::new(substitute_pseudo_vars(low, trigger_context)?),
            high: Box::new(substitute_pseudo_vars(high, trigger_context)?),
            negated: *negated,
            symmetric: *symmetric,
        },
        Expression::Cast { expr: inner, data_type } => Expression::Cast {
            expr: Box::new(substitute_pseudo_vars(inner, trigger_context)?),
            data_type: data_type.clone(),
        },
        Expression::Position { substring, string, character_unit } => Expression::Position {
            substring: Box::new(substitute_pseudo_vars(substring, trigger_context)?),
            string: Box::new(substitute_pseudo_vars(string, trigger_context)?),
            character_unit: character_unit.clone(),
        },
        Expression::Trim { position, removal_char, string } => Expression::Trim {
            position: position.clone(),
            removal_char: match removal_char {
                Some(r) => Some(Box::new(substitute_pseudo_vars(r, trigger_context)?)),
                None => None,
            },
            string: Box::new(substitute_pseudo_vars(string, trigger_context)?),
        },
        Expression::Extract { field, expr: inner } => Expression::Extract {
            field: field.clone(),
            expr: Box::new(substitute_pseudo_vars(inner, trigger_context)?),
        },
        Expression::Like { expr: inner, pattern, negated, escape } => Expression::Like {
            expr: Box::new(substitute_pseudo_vars(inner, trigger_context)?),
            pattern: Box::new(substitute_pseudo_vars(pattern, trigger_context)?),
            negated: *negated,
            escape: match escape {
                Some(e) => Some(Box::new(substitute_pseudo_vars(e, trigger_context)?)),
                None => None,
            },
        },
        Expression::Glob { expr: inner, pattern, negated, escape } => Expression::Glob {
            expr: Box::new(substitute_pseudo_vars(inner, trigger_context)?),
            pattern: Box::new(substitute_pseudo_vars(pattern, trigger_context)?),
            negated: *negated,
            escape: match escape {
                Some(e) => Some(Box::new(substitute_pseudo_vars(e, trigger_context)?)),
                None => None,
            },
        },
        Expression::QuantifiedComparison { expr: inner, op, quantifier, subquery } => {
            Expression::QuantifiedComparison {
                expr: Box::new(substitute_pseudo_vars(inner, trigger_context)?),
                op: op.clone(),
                quantifier: quantifier.clone(),
                subquery: subquery.clone(),
            }
        }
        Expression::Interval { value, unit, leading_precision, fractional_precision } => {
            Expression::Interval {
                value: Box::new(substitute_pseudo_vars(value, trigger_context)?),
                unit: unit.clone(),
                leading_precision: *leading_precision,
                fractional_precision: *fractional_precision,
            }
        }
        Expression::WindowFunction { .. } => expr.clone(),
        Expression::MatchAgainst { columns, search_modifier, mode } => Expression::MatchAgainst {
            columns: columns.clone(),
            search_modifier: Box::new(substitute_pseudo_vars(search_modifier, trigger_context)?),
            mode: mode.clone(),
        },
        Expression::RowValueConstructor(values) => Expression::RowValueConstructor(
            values
                .iter()
                .map(|v| substitute_pseudo_vars(v, trigger_context))
                .collect::<Result<Vec<_>, _>>()?,
        ),
        Expression::Collate { expr: inner, collation } => Expression::Collate {
            expr: Box::new(substitute_pseudo_vars(inner, trigger_context)?),
            collation: collation.clone(),
        },
        Expression::Raise { action, error_message } => Expression::Raise {
            action: *action,
            error_message: match error_message {
                Some(msg) => Some(Box::new(substitute_pseudo_vars(msg, trigger_context)?)),
                None => None,
            },
        },
    })
}

/// Normalize integer types to ensure consistent comparison in HashMaps
///
/// SQLite's rowid can be returned as different integer types (Integer, Bigint)
/// depending on how it's accessed. This function normalizes all integer types
/// to a consistent representation (Bigint) for reliable HashMap lookups.
fn normalize_integer_type(value: &SqlValue) -> SqlValue {
    match value {
        SqlValue::Integer(i) => SqlValue::Bigint(*i),
        SqlValue::Bigint(i) => SqlValue::Bigint(*i),
        SqlValue::Smallint(i) => SqlValue::Bigint(*i as i64),
        SqlValue::Unsigned(i) => SqlValue::Bigint(*i as i64),
        other => other.clone(),
    }
}
