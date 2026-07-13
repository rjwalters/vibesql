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
    // ("sub-select returns N columns - expected 1"). Instead, expand a tuple
    // assignment into one select item per target column so each output column
    // is an ordinary scalar computed in the join context (correlation-safe:
    // the subquery variant keeps its FROM/WHERE, only its select-list is
    // narrowed to the single projected column). `set_values_per_assignment`
    // records how many output columns each assignment contributes so the
    // unpacking below (and `apply_update_from_matches`) can flatten positionally.
    let mut set_values_per_assignment: Vec<usize> = Vec::with_capacity(stmt.assignments.len());
    for (i, assignment) in stmt.assignments.iter().enumerate() {
        if assignment.is_tuple() {
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
            set_values_per_assignment.push(assignment.columns.len());
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
            set_values_per_assignment.push(1);
        }
    }
    let total_set_columns: usize = set_values_per_assignment.iter().sum();

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
    let mut id_to_set_values: HashMap<Vec<SqlValue>, Vec<SqlValue>> = HashMap::new();

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

        // Extract SET values (flattened across all assignments; see above)
        let set_values: Vec<SqlValue> = (0..total_set_columns)
            .map(|i| row.values.get(num_id_columns + i).cloned().unwrap_or(SqlValue::Null))
            .collect();

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
/// - `ScalarSubquery(sub)` → one cloned subquery per column, each with its
///   select-list narrowed to the single projected item. This preserves
///   correlation (FROM/WHERE and the column expression are retained) and
///   first-row semantics while yielding a single scalar column per output. A
///   subquery whose select-list can't be split by simple index (e.g. it
///   contains a `*` wildcard, or its arity can't be matched to the column
///   count) is left intact so the ordinary evaluator raises the correct error.
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
