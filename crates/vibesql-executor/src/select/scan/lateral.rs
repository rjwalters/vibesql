//! LATERAL / dependent-join execution for table-valued-function siblings.
//!
//! Implements the narrow dependent-join capability that makes the ubiquitous
//! JSON idiom
//!
//! ```sql
//! SELECT t.id, je.value FROM t, json_each(t.j) AS je
//! ```
//!
//! work. Here the argument to the table function (`t.j`) references a column of
//! a *preceding* FROM sibling (`t`). VibeSQL's comma-joined siblings are
//! normally cross-joined *independently* — the right sibling cannot see the left
//! sibling's columns (see ADR-0005 §1d/§2d). That is exactly LATERAL semantics.
//!
//! ## What this handles (and only this)
//!
//! A [`FromClause::Join`] whose **right** side is a
//! [`FromClause::TableFunction`] whose argument expressions reference at least
//! one column (i.e. a correlated / lateral argument). The dependent join:
//!
//! 1. Executes the **left** side once (materialized).
//! 2. For each left row, re-evaluates the table function with that row supplied as the
//!    outer-correlation context (the executor already threads `outer_row`/`outer_schema` into
//!    [`super::table_function::execute_table_function`]).
//! 3. Cross-products the left row with the rows the table function produced for it, concatenating
//!    columns left-then-right and merging the two schemas with [`CombinedSchema::merge`] — the same
//!    column layout a plain cross join yields.
//!
//! A NULL / absent JSON value for a given left row yields zero table-function
//! rows for that row (matching sqlite3: `json_each(NULL)` → 0 rows), so that
//! left row simply contributes nothing to the output — an inner-join-style
//! dependent join, which is what SQLite's comma-join does.
//!
//! ## What this deliberately does NOT handle
//!
//! - General LATERAL subqueries (`FROM t, LATERAL (SELECT ...)`).
//! - A table-function that is the *left* side of a join, or nested deeper than the immediate right
//!   child.
//! - Reordering a lateral TVF ahead of the sibling it depends on (join reordering is suppressed
//!   when a lateral TVF is present; see [`from_contains_lateral_tvf`]).
//!
//! Reference: ADR-0005 step 4; <https://www.sqlite.org/json1.html#jeach>.

use std::collections::HashMap;

use vibesql_ast::{BinaryOperator, Expression, FromClause};

use super::FromResult;
use crate::{
    errors::ExecutorError,
    evaluator::{operators::is_truthy, CombinedExpressionEvaluator},
    schema::CombinedSchema,
    select::cte::CteResult,
};

/// Does this expression reference at least one column? A table-function
/// argument that references any column is a *lateral* argument: the table
/// function has no columns of its own to reference, so any [`Expression::ColumnRef`]
/// necessarily points at a preceding FROM sibling (or an outer query).
fn expr_references_a_column(expr: &Expression) -> bool {
    let mut found = false;
    walk_column_refs(expr, &mut found);
    found
}

/// Recursively scan an expression for any [`Expression::ColumnRef`].
fn walk_column_refs(expr: &Expression, found: &mut bool) {
    if *found {
        return;
    }
    match expr {
        Expression::ColumnRef(_) => *found = true,
        Expression::BinaryOp { left, right, .. } => {
            walk_column_refs(left, found);
            walk_column_refs(right, found);
        }
        Expression::UnaryOp { expr, .. } => walk_column_refs(expr, found),
        Expression::Function { args, .. } | Expression::AggregateFunction { args, .. } => {
            for a in args {
                walk_column_refs(a, found);
            }
        }
        Expression::Cast { expr, .. } => walk_column_refs(expr, found),
        Expression::Collate { expr, .. } => walk_column_refs(expr, found),
        Expression::IsNull { expr, .. } => walk_column_refs(expr, found),
        Expression::Between { expr, low, high, .. } => {
            walk_column_refs(expr, found);
            walk_column_refs(low, found);
            walk_column_refs(high, found);
        }
        Expression::InList { expr, values, .. } => {
            walk_column_refs(expr, found);
            for v in values {
                walk_column_refs(v, found);
            }
        }
        Expression::Like { expr, pattern, .. } | Expression::Glob { expr, pattern, .. } => {
            walk_column_refs(expr, found);
            walk_column_refs(pattern, found);
        }
        Expression::Case { operand, when_clauses, else_result } => {
            if let Some(op) = operand {
                walk_column_refs(op, found);
            }
            for clause in when_clauses {
                for cond in &clause.conditions {
                    walk_column_refs(cond, found);
                }
                walk_column_refs(&clause.result, found);
            }
            if let Some(e) = else_result {
                walk_column_refs(e, found);
            }
        }
        // Literals, parameters, subqueries, wildcards, etc. carry no *local*
        // column reference relevant to lateral detection.
        _ => {}
    }
}

/// Split a WHERE expression into its top-level AND conjuncts, flattening nested
/// `AND` (both the `BinaryOp { And }` and `Conjunction` spellings the parser can
/// produce). Non-AND expressions yield a single conjunct. This mirrors the
/// top-level-AND walk used elsewhere for predicate pushdown.
fn split_conjuncts<'a>(expr: &'a Expression, out: &mut Vec<&'a Expression>) {
    match expr {
        Expression::BinaryOp { left, op: BinaryOperator::And, right } => {
            split_conjuncts(left, out);
            split_conjuncts(right, out);
        }
        Expression::Conjunction(parts) => {
            for p in parts {
                split_conjuncts(p, out);
            }
        }
        other => out.push(other),
    }
}

/// Collect every column reference in `expr` as `(table, column)` canonical-name
/// pairs. Used to decide whether a conjunct references only left-side columns.
fn collect_column_refs<'a>(expr: &'a Expression, out: &mut Vec<(Option<&'a str>, &'a str)>) {
    match expr {
        Expression::ColumnRef(id) => out.push((id.table_canonical(), id.column_canonical())),
        Expression::BinaryOp { left, right, .. } => {
            collect_column_refs(left, out);
            collect_column_refs(right, out);
        }
        Expression::UnaryOp { expr, .. } => collect_column_refs(expr, out),
        Expression::Function { args, .. } | Expression::AggregateFunction { args, .. } => {
            for a in args {
                collect_column_refs(a, out);
            }
        }
        Expression::Cast { expr, .. } => collect_column_refs(expr, out),
        Expression::Collate { expr, .. } => collect_column_refs(expr, out),
        Expression::IsNull { expr, .. } => collect_column_refs(expr, out),
        Expression::Between { expr, low, high, .. } => {
            collect_column_refs(expr, out);
            collect_column_refs(low, out);
            collect_column_refs(high, out);
        }
        Expression::InList { expr, values, .. } => {
            collect_column_refs(expr, out);
            for v in values {
                collect_column_refs(v, out);
            }
        }
        Expression::Like { expr, pattern, .. } | Expression::Glob { expr, pattern, .. } => {
            collect_column_refs(expr, out);
            collect_column_refs(pattern, out);
        }
        Expression::Conjunction(parts) | Expression::Disjunction(parts) => {
            for p in parts {
                collect_column_refs(p, out);
            }
        }
        Expression::Case { operand, when_clauses, else_result } => {
            if let Some(op) = operand {
                collect_column_refs(op, out);
            }
            for clause in when_clauses {
                for cond in &clause.conditions {
                    collect_column_refs(cond, out);
                }
                collect_column_refs(&clause.result, out);
            }
            if let Some(e) = else_result {
                collect_column_refs(e, out);
            }
        }
        // Literals, parameters, and (importantly) subqueries carry no *local*
        // column reference we can safely resolve against the left schema. A
        // conjunct containing a subquery is conservatively treated as
        // non-pushable by `conjunct_is_left_only` (it returns a sentinel that
        // fails resolution), so it stays in the post-join filter.
        _ => {}
    }
}

/// Is `expr` a conjunct that references **only** columns of the left side of the
/// dependent join (and no table-function columns)? Such a conjunct is safe to
/// evaluate per-left-row *before* invoking the table function — standard
/// predicate pushdown into the dependent join.
///
/// The test is purely name-based against `left_schema`: every column reference
/// must resolve there. A reference to a table-function column (e.g.
/// `json_each.value`, or the alias's `value`) does not resolve in `left_schema`,
/// so any conjunct touching the right side is left for the post-join filter.
///
/// A conjunct with **no** column references at all (a constant like `1=1`) is
/// not pushed here — it is uniform across all left rows and the post-join filter
/// handles it identically, so there is nothing to gain and we avoid changing
/// its evaluation context.
///
/// Conservative by construction: any expression form we do not descend into
/// (subqueries, etc.) contributes no collected refs; combined with the
/// "must have at least one ref, all resolvable" rule, an unrecognized construct
/// that hides a right-side reference cannot be misclassified as left-only,
/// because such conjuncts also reference the TVF column explicitly and that
/// reference fails resolution. Subquery-only conjuncts (no left ref) simply
/// aren't pushed.
fn conjunct_is_left_only(expr: &Expression, left_schema: &CombinedSchema) -> bool {
    let mut refs: Vec<(Option<&str>, &str)> = Vec::new();
    collect_column_refs(expr, &mut refs);
    if refs.is_empty() {
        return false;
    }
    refs.iter().all(|(table, column)| left_schema.get_column_index(*table, column).is_some())
}

/// Is `from` a table function with at least one lateral (column-referencing)
/// argument?
pub(crate) fn is_lateral_table_function(from: &FromClause) -> bool {
    match from {
        FromClause::TableFunction { args, .. } => args.iter().any(expr_references_a_column),
        _ => false,
    }
}

/// Does the FROM tree contain any lateral table-function dependent join?
///
/// Used to suppress join reordering (which would be free to move the lateral
/// TVF ahead of the sibling it depends on, breaking correlation) and to route
/// execution through [`execute_lateral_tvf_join`].
pub(crate) fn from_contains_lateral_tvf(from: &FromClause) -> bool {
    match from {
        FromClause::Join { left, right, .. } => {
            is_lateral_table_function(right)
                || from_contains_lateral_tvf(left)
                || from_contains_lateral_tvf(right)
        }
        _ => false,
    }
}

/// Execute a `Join` whose right side is a lateral table function.
///
/// The `join_type` is only meaningfully a comma / CROSS / INNER join here — a
/// bare `FROM t, json_each(t.j)` parses as a CROSS join. We treat the dependent
/// join as inner-style: left rows that yield no table-function rows drop out
/// (matching sqlite3's comma-join over `json_each`).
#[allow(clippy::too_many_arguments)]
pub(crate) fn execute_lateral_tvf_join<F>(
    left: &FromClause,
    right: &FromClause,
    cte_results: &HashMap<String, CteResult>,
    database: &vibesql_storage::Database,
    where_clause: Option<&vibesql_ast::Expression>,
    outer_row: Option<&vibesql_storage::Row>,
    outer_schema: Option<&CombinedSchema>,
    execute_subquery: F,
) -> Result<FromResult, ExecutorError>
where
    F: Fn(&vibesql_ast::SelectStmt) -> Result<crate::select::SelectResult, ExecutorError> + Copy,
{
    let (name, args, alias, column_aliases) = match right {
        FromClause::TableFunction { name, args, alias, column_aliases } => {
            (name, args, alias, column_aliases)
        }
        // Caller guarantees the right side is a lateral table function.
        _ => {
            return Err(ExecutorError::UnsupportedFeature(
                "lateral join right side is not a table function".to_string(),
            ));
        }
    };

    // 1. Execute the left side once. The outer WHERE clause is NOT pushed into the left scan here:
    //    predicates that reference the table-function columns must be evaluated post-join, and
    //    predicates local to the left table are still applied by the outer WHERE pass after this
    //    join. We do thread the enclosing outer_row/outer_schema so a lateral TVF nested inside a
    //    correlated subquery still resolves its outer references.
    let left_result = super::execute_from_clause(
        left,
        cte_results,
        database,
        None,
        None,
        None,
        outer_row,
        outer_schema,
        execute_subquery,
    )?;

    let left_schema = left_result.schema.clone();
    // Table names for ROWID tracking (issue #4370): a bare table scan carries a
    // per-row scalar `row_id`; `combine_for_join` maps it to the left table's
    // name so `t.rowid` resolves after the dependent join.
    let left_table_names = left_schema.table_names();
    let left_rows = left_result.into_rows();

    // Predicate pushdown into the dependent join (issue #5989, json102-1011).
    //
    // SQLite applies the WHERE clause to the comma-join's cross product but
    // short-circuits so that a WHERE conjunct which excludes a left row is
    // applied *before* the table function is evaluated for that row. This
    // matters for the malformed-JSON guard idiom
    //
    //   SELECT ... FROM t, json_each(t.j) WHERE json_valid(t.j) AND ...
    //
    // where `t.j` may be non-JSON for some rows: the `json_valid(t.j)` guard
    // must exclude those rows so `json_each` never runs on them and errors.
    //
    // We collect the WHERE conjuncts that reference ONLY left-side columns and
    // evaluate them per left row before invoking the table function; a left row
    // failing any such conjunct is skipped (contributes no output). Conjuncts
    // that reference table-function columns (e.g. `json_each.value LIKE ...`)
    // are NOT pushed — they still resolve only after the join and are handled by
    // the caller's post-join filter pass. Without a left-only guard, malformed
    // JSON still reaches the TVF and errors, matching sqlite3's behavior for
    // `FROM t, json_each(t.j)` with no `json_valid` guard.
    let pushed_conjuncts: Vec<&Expression> = match where_clause {
        Some(w) => {
            let mut all = Vec::new();
            split_conjuncts(w, &mut all);
            all.into_iter().filter(|c| conjunct_is_left_only(c, &left_schema)).collect()
        }
        None => Vec::new(),
    };
    let left_only_evaluator = if pushed_conjuncts.is_empty() {
        None
    } else {
        Some(match (outer_row, outer_schema) {
            (Some(orow), Some(oschema)) => {
                CombinedExpressionEvaluator::with_database_and_outer_context(
                    &left_schema,
                    database,
                    orow,
                    oschema,
                )
            }
            _ => CombinedExpressionEvaluator::with_database(&left_schema, database),
        })
    };

    // 2/3. For each left row, evaluate the table function against that row as
    //      the outer-correlation context and cross-product.
    let mut combined_rows: Vec<vibesql_storage::Row> = Vec::new();
    // The table-function output schema is identical for every left row (a fixed
    // 8-column contract), so capture it once for the merged result schema.
    let mut tvf_schema: Option<CombinedSchema> = None;

    for left_row in &left_rows {
        // Apply pushed-down left-only WHERE conjuncts before evaluating the TVF.
        // A row failing any such conjunct (false or NULL) is excluded here, so
        // the table function never runs on it — matching SQLite's short-circuit
        // of the WHERE guard ahead of the correlated table function.
        if let Some(evaluator) = &left_only_evaluator {
            let mut excluded = false;
            for conjunct in &pushed_conjuncts {
                let value = evaluator.eval(conjunct, left_row).map_err(|e| {
                    ExecutorError::TypeError(format!("Error evaluating lateral WHERE guard: {}", e))
                })?;
                if !is_truthy(&value) {
                    excluded = true;
                    break;
                }
            }
            if excluded {
                continue;
            }
        }

        // Build the effective outer context for this left row. When we are
        // already nested inside an outer correlation, the immediate left row is
        // the closest scope, so it takes precedence for resolving the TVF
        // argument (e.g. `json_each(t.j)` resolves `t.j` from the left row).
        let tvf_result = super::table_function::execute_table_function(
            name,
            args,
            alias.as_ref(),
            column_aliases.as_ref(),
            database,
            cte_results,
            Some(left_row),
            Some(&left_schema),
        )?;

        let tvf_table_names = tvf_result.schema.table_names();
        if tvf_schema.is_none() {
            tvf_schema = Some(tvf_result.schema.clone());
        }

        for tvf_row in tvf_result.into_rows() {
            combined_rows.push(vibesql_storage::Row::combine_for_join(
                left_row,
                &tvf_row,
                &left_table_names,
                &tvf_table_names,
            ));
        }
    }

    // Merge the left schema with the table-function schema so downstream
    // column resolution (WHERE over both sides, projection) sees both. When the
    // left side produced no rows we never evaluated the TVF; build the fixed
    // output schema directly so the result still has the correct shape.
    let tvf_schema = match tvf_schema {
        Some(s) => s,
        None => {
            // No left rows: the result is empty, so no argument evaluation is
            // needed (or possible — a correlated argument like `t.j` has no left
            // row to resolve against). Build the fixed 8-column schema directly
            // rather than calling execute_table_function, which would evaluate
            // the argument against a NULL context and error on the correlated
            // column reference (issue #5989 empty-left defect). A bad
            // column-alias count is still surfaced as an error here.
            super::table_function::build_schema(name, alias.as_ref(), column_aliases.as_ref())?
        }
    };

    let merged_schema = CombinedSchema::merge(left_schema, tvf_schema);

    // The outer WHERE clause is applied by the caller's post-join filter pass
    // (it references both sides), so we return the raw cross product here.
    let _ = where_clause;

    Ok(FromResult::from_rows(merged_schema, combined_rows))
}

#[cfg(test)]
mod tests {
    use vibesql_ast::{ColumnIdentifier, Expression};
    use vibesql_types::SqlValue;

    use super::*;

    fn col(table: &str, name: &str) -> Expression {
        Expression::ColumnRef(ColumnIdentifier::qualified(table, false, name, false))
    }

    fn lit_str(s: &str) -> Expression {
        Expression::Literal(SqlValue::Varchar(s.into()))
    }

    fn tvf(name: &str, args: Vec<Expression>) -> FromClause {
        FromClause::TableFunction {
            name: name.to_string(),
            args,
            alias: None,
            column_aliases: None,
        }
    }

    fn table(name: &str) -> FromClause {
        FromClause::Table {
            name: name.to_string(),
            alias: None,
            column_aliases: None,
            quoted: false,
            index_hint: None,
        }
    }

    fn cross(left: FromClause, right: FromClause) -> FromClause {
        FromClause::Join {
            left: Box::new(left),
            right: Box::new(right),
            join_type: vibesql_ast::JoinType::Cross,
            condition: None,
            using_columns: None,
            natural: false,
            alias: None,
        }
    }

    #[test]
    fn column_arg_is_lateral() {
        // json_each(t.j) references a column -> lateral.
        assert!(is_lateral_table_function(&tvf("json_each", vec![col("t", "j")])));
    }

    #[test]
    fn literal_arg_is_not_lateral() {
        // json_each('[1,2,3]') references no column -> non-correlated.
        assert!(!is_lateral_table_function(&tvf("json_each", vec![lit_str("[1,2,3]")])));
    }

    #[test]
    fn column_in_path_arg_is_lateral() {
        // json_tree(lit, t.path) -> the second (path) arg is a column ref.
        assert!(is_lateral_table_function(&tvf(
            "json_tree",
            vec![lit_str("{}"), col("t", "path")]
        )));
    }

    #[test]
    fn nested_function_arg_column_is_lateral() {
        // json_each(json_extract(t.j, '$.x')) -> column nested inside a function.
        let inner = Expression::Function {
            name: "json_extract".to_string().into(),
            args: vec![col("t", "j"), lit_str("$.x")],
            character_unit: None,
        };
        assert!(is_lateral_table_function(&tvf("json_each", vec![inner])));
    }

    #[test]
    fn from_contains_lateral_detects_right_tvf() {
        // FROM t, json_each(t.j)
        let from = cross(table("t"), tvf("json_each", vec![col("t", "j")]));
        assert!(from_contains_lateral_tvf(&from));
    }

    #[test]
    fn from_contains_lateral_ignores_noncorrelated_tvf() {
        // FROM t, json_each('[1,2]')  -> not lateral (literal arg).
        let from = cross(table("t"), tvf("json_each", vec![lit_str("[1,2]")]));
        assert!(!from_contains_lateral_tvf(&from));
    }

    #[test]
    fn from_contains_lateral_detects_second_of_two_preceding_tables() {
        // FROM a, b, json_each(b.j)  parses left-deep as
        // Join{ Join{a,b}, json_each(b.j) } -> the outer join's right child is a
        // lateral TVF referencing b (the SECOND preceding table).
        let inner = cross(table("a"), table("b"));
        let from = cross(inner, tvf("json_each", vec![col("b", "j")]));
        assert!(from_contains_lateral_tvf(&from));
    }

    #[test]
    fn from_contains_lateral_detects_nested_left_lateral() {
        // FROM (t, json_each(t.j)), other  -> the lateral TVF is nested in the
        // left subtree; still detected.
        let inner = cross(table("t"), tvf("json_each", vec![col("t", "j")]));
        let from = cross(inner, table("other"));
        assert!(from_contains_lateral_tvf(&from));
    }

    #[test]
    fn plain_table_from_is_not_lateral() {
        assert!(!from_contains_lateral_tvf(&table("t")));
        assert!(!from_contains_lateral_tvf(&cross(table("a"), table("b"))));
    }

    // --- Predicate-pushdown helpers (issue #5989, json102-1011) ---

    use vibesql_types::DataType;

    /// Build a left schema for table `user(name, phone)` to mirror json102-1011.
    fn user_left_schema() -> CombinedSchema {
        CombinedSchema::from_derived_table(
            "user".to_string(),
            vec!["name".to_string(), "phone".to_string()],
            vec![DataType::Varchar { max_length: None }, DataType::Varchar { max_length: None }],
        )
    }

    fn func(name: &str, args: Vec<Expression>) -> Expression {
        Expression::Function { name: name.to_string().into(), args, character_unit: None }
    }

    fn and(left: Expression, right: Expression) -> Expression {
        Expression::BinaryOp {
            left: Box::new(left),
            op: BinaryOperator::And,
            right: Box::new(right),
        }
    }

    fn like(expr: Expression, pattern: Expression) -> Expression {
        Expression::Like {
            expr: Box::new(expr),
            pattern: Box::new(pattern),
            negated: false,
            escape: None,
        }
    }

    fn col_unqualified(name: &str) -> Expression {
        Expression::ColumnRef(ColumnIdentifier::simple(name, false))
    }

    #[test]
    fn split_conjuncts_flattens_nested_and() {
        // a AND b AND c  (left-deep BinaryOp AND) -> three conjuncts.
        let e = and(and(col("t", "a"), col("t", "b")), col("t", "c"));
        let mut out = Vec::new();
        split_conjuncts(&e, &mut out);
        assert_eq!(out.len(), 3);
    }

    #[test]
    fn split_conjuncts_flattens_conjunction_variant() {
        let e = Expression::Conjunction(vec![col("t", "a"), col("t", "b"), col("t", "c")]);
        let mut out = Vec::new();
        split_conjuncts(&e, &mut out);
        assert_eq!(out.len(), 3);
    }

    #[test]
    fn split_conjuncts_non_and_is_single() {
        let e = like(col("t", "phone"), lit_str("704-%"));
        let mut out = Vec::new();
        split_conjuncts(&e, &mut out);
        assert_eq!(out.len(), 1);
    }

    #[test]
    fn json_valid_guard_is_left_only_pushable() {
        // json_valid(user.phone) references only a left column -> pushable.
        let schema = user_left_schema();
        let guard = func("json_valid", vec![col("user", "phone")]);
        assert!(conjunct_is_left_only(&guard, &schema));
    }

    #[test]
    fn tvf_column_predicate_is_not_pushable() {
        // json_each.value LIKE '704-%' references a table-function column that
        // does NOT resolve in the left schema -> NOT pushable (post-join only).
        let schema = user_left_schema();
        let pred = like(col("json_each", "value"), lit_str("704-%"));
        assert!(!conjunct_is_left_only(&pred, &schema));
    }

    #[test]
    fn unqualified_tvf_column_is_not_pushable() {
        // A bare `value` (the TVF's column, unqualified) is not in the left
        // schema, so it is not pushable.
        let schema = user_left_schema();
        let pred = like(col_unqualified("value"), lit_str("704-%"));
        assert!(!conjunct_is_left_only(&pred, &schema));
    }

    #[test]
    fn mixed_conjunct_referencing_tvf_is_not_pushable() {
        // json_valid(user.phone) AND json_each.value LIKE ... : as a single
        // conjunct (not split) it references a TVF column, so it is not
        // pushable. (Splitting happens before this check in the real path.)
        let schema = user_left_schema();
        let mixed = and(
            func("json_valid", vec![col("user", "phone")]),
            like(col("json_each", "value"), lit_str("704-%")),
        );
        assert!(!conjunct_is_left_only(&mixed, &schema));
    }

    #[test]
    fn constant_conjunct_is_not_pushed() {
        // A conjunct with no column references (e.g. a bare literal) is uniform
        // across left rows; we do not push it (the post-join filter handles it).
        let schema = user_left_schema();
        let konst = Expression::Literal(SqlValue::Boolean(true));
        assert!(!conjunct_is_left_only(&konst, &schema));
    }

    #[test]
    fn json102_1011_shape_splits_into_pushable_and_postjoin() {
        // Full json102-1011 WHERE: json_valid(user.phone) AND json_each.value LIKE '704-%'
        // The guard conjunct pushes; the TVF-column conjunct does not.
        let schema = user_left_schema();
        let where_clause = and(
            func("json_valid", vec![col("user", "phone")]),
            like(col("json_each", "value"), lit_str("704-%")),
        );
        let mut conjuncts = Vec::new();
        split_conjuncts(&where_clause, &mut conjuncts);
        assert_eq!(conjuncts.len(), 2);
        let pushable: Vec<_> =
            conjuncts.iter().filter(|c| conjunct_is_left_only(c, &schema)).collect();
        assert_eq!(pushable.len(), 1, "only the json_valid guard is pushed");
    }
}
