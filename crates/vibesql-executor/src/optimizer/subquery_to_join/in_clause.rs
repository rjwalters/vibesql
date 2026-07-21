//! IN/NOT IN subquery to SEMI/ANTI join transformations
//!
//! This module handles converting IN and NOT IN subqueries into
//! SEMI and ANTI joins respectively.
//!
//! ## Examples
//!
//! ### IN → SEMI JOIN
//! ```sql
//! -- Before:
//! SELECT * FROM orders WHERE o_orderkey IN (SELECT l_orderkey FROM lineitem)
//!
//! -- After:
//! SELECT orders.* FROM orders SEMI JOIN lineitem ON o_orderkey = l_orderkey
//! ```
//!
//! ### NOT IN → ANTI JOIN
//! ```sql
//! -- Before:
//! SELECT * FROM orders WHERE o_orderkey NOT IN (SELECT l_orderkey FROM lineitem)
//!
//! -- After:
//! SELECT orders.* FROM orders ANTI JOIN lineitem ON o_orderkey = l_orderkey
//! ```
//!
//! ### Aggregate IN → SEMI JOIN with Derived Table (TPC-H Q18)
//! ```sql
//! -- Before:
//! SELECT * FROM orders WHERE o_orderkey IN (
//!     SELECT l_orderkey FROM lineitem GROUP BY l_orderkey HAVING SUM(l_quantity) > 300
//! )
//!
//! -- After:
//! SELECT orders.* FROM orders SEMI JOIN (
//!     SELECT l_orderkey FROM lineitem GROUP BY l_orderkey HAVING SUM(l_quantity) > 300
//! ) AS __in_agg ON o_orderkey = __in_agg.l_orderkey
//! ```

use vibesql_ast::{
    BinaryOperator, CommonTableExpr, Expression, FromClause, JoinType, SelectItem, SelectStmt,
};
use vibesql_storage::Database;

use super::helpers::{
    get_outer_table_name, is_self_join, is_simple_single_table_self_join,
    outer_column_is_provably_non_null, resolve_outer_column_qualifier,
    rewrite_column_refs_with_alias, OuterQualifier,
};

/// Result of converting an IN subquery to a join
/// Contains the new FROM clause
pub(super) struct InToJoinResult {
    pub from: FromClause,
}

/// Qualify a bare outer column reference against the outer FROM scope (issues
/// #5926 / #5870), covering every outer-FROM source kind uniformly (base table,
/// view, CTE, derived subquery table, and `VALUES` table).
///
/// Returns:
/// - `Some(expr')` — the expression to fold into the join predicate. When `expr`
///   is an unqualified `ColumnRef` that resolves to exactly one outer source,
///   `expr'` is that reference qualified with the source's effective name; in
///   every other resolvable case (already qualified, not a column reference, or
///   genuinely ambiguous / a correlated reference) `expr'` is `expr` unchanged so
///   downstream resolution — including the ambiguity guard — behaves exactly as
///   SQLite requires.
/// - `None` — the outer column can only be resolved against a derived/VALUES
///   source whose columns cannot be enumerated here. The caller must abort the
///   IN→join transform and fall back to row-by-row evaluation, which never
///   over-errors (preserving `main`'s behavior instead of regressing to an
///   ambiguity error).
fn qualify_outer_expr_in_scope(
    expr: &Expression,
    from: &FromClause,
    database: &Database,
    with_clause: Option<&[CommonTableExpr]>,
) -> Option<Expression> {
    if let Expression::ColumnRef(col_id) = expr {
        if col_id.schema_canonical().is_none() && col_id.table_canonical().is_none() {
            // The bare `rowid`/`_rowid_`/`oid` pseudo-column is not a real column
            // on any outer source, so `resolve_outer_column_qualifier` finds zero
            // matches and returns `LeaveUnqualified`. Folding a bare `rowid` into
            // the synthesized SEMI/ANTI-join ON condition then breaks: in the
            // join's combined schema (outer table + subquery table) an unqualified
            // `rowid` no longer resolves to the outer table's rowid and evaluates
            // to NULL, so the predicate never matches and the whole IN filter
            // drops every row (issue #6088; rowvalue9 5.2/5.3).
            //
            // Qualify it against the single outer table that actually owns a
            // rowid. If the outer scope is anything other than exactly one
            // rowid-bearing base table (multiple tables, a WITHOUT ROWID table, a
            // view, or a derived/VALUES source), abort the transform and fall back
            // to row-by-row `eval_in_subquery`, which resolves the bare rowid
            // against the scanned table correctly.
            if is_rowid_pseudo_column(col_id.column_canonical()) {
                return match single_outer_rowid_table(from, database) {
                    Some(table) => {
                        Some(rewrite_column_refs_with_alias(expr, &table, &table))
                    }
                    None => None,
                };
            }

            match resolve_outer_column_qualifier(
                from,
                database,
                with_clause,
                col_id.column_canonical(),
            ) {
                OuterQualifier::Qualify(qualifier) => {
                    return Some(rewrite_column_refs_with_alias(expr, &qualifier, &qualifier));
                }
                OuterQualifier::Skip => return None,
                OuterQualifier::LeaveUnqualified => {}
            }
        }
    }
    Some(expr.clone())
}

/// Whether `column` is one of SQLite's rowid pseudo-column spellings.
fn is_rowid_pseudo_column(column: &str) -> bool {
    column.eq_ignore_ascii_case("rowid")
        || column.eq_ignore_ascii_case("_rowid_")
        || column.eq_ignore_ascii_case("oid")
}

/// Resolve the effective name of the single outer-FROM base table that owns a
/// rowid, or `None` if the outer scope is not exactly one rowid-bearing base
/// table.
///
/// Used to qualify a bare `rowid` LHS before it becomes the left side of a
/// synthesized SEMI/ANTI-join predicate (issue #6088). Returns `None` — signaling
/// the caller to abort the IN→join transform and fall back to row-by-row
/// evaluation — when the outer FROM is a join/multiple tables (a bare rowid would
/// be ambiguous), a WITHOUT ROWID table or a view (no rowid pseudo-column), or a
/// derived/VALUES source (no rowid, and its effective name is not a base table).
fn single_outer_rowid_table(from: &FromClause, database: &Database) -> Option<String> {
    match from {
        FromClause::Table { name, alias, .. } => {
            let table = database.get_table(name)?;
            // WITHOUT ROWID tables and views have no rowid pseudo-column.
            if table.schema.without_rowid || table.schema.is_view {
                return None;
            }
            Some(alias.clone().unwrap_or_else(|| name.clone()))
        }
        // Joins/multiple tables → a bare rowid is ambiguous; derived/VALUES
        // sources have no rowid. In all these cases fall back to row-by-row.
        _ => None,
    }
}

/// Whether the subquery's single projection column can be proven to never yield
/// a NULL value, given the subquery body.
///
/// This gates the `NOT IN` → ANTI-join rewrite (issue #6109). Per SQL three-valued
/// logic, `x NOT IN (S)` is UNKNOWN (never TRUE) whenever `S` contains a NULL and
/// `x` does not equal any non-NULL member, so a `NOT IN` whose subquery result can
/// contain NULL must return **no rows** for that `x`. A plain ANTI join instead
/// emits the complementary rows — it is only equivalent to `NOT IN` when the right
/// side is provably free of NULLs. When we cannot prove the projection is
/// non-NULL, the caller aborts the transform (returns `None`) and falls back to
/// row-by-row `eval_in_subquery`, which already implements the three-valued
/// semantics correctly.
///
/// A projection is provably non-NULL when it is:
/// - a non-NULL literal; or
/// - a bare column reference to a base table column that is either declared
///   `NOT NULL` or is the `INTEGER PRIMARY KEY` rowid alias (rowids are never
///   NULL). Note that a *composite* or non-INTEGER `PRIMARY KEY` column is **not**
///   sufficient: SQLite (and VibeSQL) permit NULLs in such columns, so we must not
///   treat them as non-NULL here.
///
/// Everything else (arbitrary expressions, function calls, nullable columns,
/// columns from a derived/joined/VALUES source, the bare `rowid` pseudo-column,
/// etc.) is treated conservatively as possibly-NULL. The bare rowid case is
/// intentionally excluded because determining the owning table here would
/// duplicate the outer-qualification logic; falling back to row-by-row is always
/// correct.
fn subquery_projection_is_provably_non_null(
    projection: &Expression,
    subquery: &SelectStmt,
    database: &Database,
) -> bool {
    // Only reason about a subquery whose FROM is exactly one base table.
    let table_name = match &subquery.from {
        Some(FromClause::Table { name, .. }) => name.as_str(),
        _ => return false,
    };
    expression_is_provably_non_null(projection, table_name, database)
}

/// Whether the outer `NOT IN` left-hand-side expression can be proven to never be
/// NULL, given the outer FROM clause.
///
/// This is the *second half* of the NULL-safety gate for the `NOT IN` → ANTI-join
/// rewrite (issue #6109). A plain ANTI join keeps a row of the left input whenever
/// that row has no join partner on the right. But `NULL NOT IN (S)` is never TRUE
/// for a non-empty `S` (it is UNKNOWN) and is TRUE only for an empty `S` — neither
/// of which an equality-based ANTI join models: `NULL = v` is never TRUE, so the
/// ANTI join *keeps* every NULL-LHS row unconditionally. The ANTI join therefore
/// matches `NOT IN` only when the left side is also provably free of NULLs.
///
/// We prove non-NULL only for the simple, common case where the outer FROM is a
/// single base table and the LHS is a non-NULL literal or a `NOT NULL` /
/// `INTEGER PRIMARY KEY` column of that table. Anything else (multi-table FROM,
/// joins, derived/VALUES sources, nullable columns, arbitrary expressions) is
/// treated conservatively as possibly-NULL, aborting the rewrite in favor of the
/// NULL-correct row-by-row path.
fn outer_expr_is_provably_non_null(expr: &Expression, from: &FromClause, database: &Database) -> bool {
    match expr {
        Expression::Literal(value) => !matches!(value, vibesql_types::SqlValue::Null),
        // Resolve the column against the whole outer FROM scope (which may already
        // be a join from an earlier IN→SEMI/ANTI rewrite) and check its declared
        // nullability. The bare `rowid`/`_rowid_`/`oid` pseudo-column is not a real
        // catalog column, so it will not resolve and is conservatively treated as
        // possibly-NULL — falling back to the row-by-row path, which is correct.
        Expression::ColumnRef(col_id) => {
            outer_column_is_provably_non_null(from, database, col_id.column_canonical())
        }
        _ => false,
    }
}

/// Shared core: whether `expr` is provably non-NULL when evaluated against the
/// single base table `table_name`.
///
/// A value is provably non-NULL when it is a non-NULL literal, or a bare/qualified
/// column reference to a column of `table_name` that is either the
/// `INTEGER PRIMARY KEY` rowid alias (rowids are never NULL) or declared
/// `NOT NULL`. A *composite* or non-INTEGER `PRIMARY KEY` column is deliberately
/// NOT treated as non-NULL: SQLite (and VibeSQL) permit NULLs there. The bare
/// `rowid` pseudo-column and all other expression shapes are treated
/// conservatively as possibly-NULL.
fn expression_is_provably_non_null(expr: &Expression, table_name: &str, database: &Database) -> bool {
    match expr {
        Expression::Literal(value) => !matches!(value, vibesql_types::SqlValue::Null),
        Expression::ColumnRef(col_id) => {
            let table = match database.get_table(table_name) {
                Some(t) => t,
                None => return false,
            };
            // Views and derived shapes are out of scope for this proof.
            if table.schema.is_view {
                return false;
            }
            let column = col_id.column_canonical();
            // INTEGER PRIMARY KEY (rowid alias) columns are never NULL.
            if let Some(pk_idx) = table.schema.get_integer_primary_key_index() {
                if let Some(pk_col) = table.schema.columns.get(pk_idx) {
                    if pk_col.name.eq_ignore_ascii_case(column) {
                        return true;
                    }
                }
            }
            // Otherwise require an explicit NOT NULL declaration.
            match table.schema.columns.iter().find(|c| c.name.eq_ignore_ascii_case(column)) {
                Some(c) => !c.nullable,
                None => false,
            }
        }
        _ => false,
    }
}

/// Try to convert an IN subquery to a SEMI or ANTI join
pub(super) fn try_convert_in_to_join(
    from: &FromClause,
    expr: &Expression,
    subquery: &SelectStmt,
    negated: bool,
    database: &Database,
    with_clause: Option<&[CommonTableExpr]>,
) -> Option<InToJoinResult> {
    // A subquery that declares its own CTEs — `x IN (WITH c AS (...) SELECT ... FROM c)`
    // — resolves the names in its FROM clause against those CTEs, not against catalog
    // base tables. The IN→SEMI/ANTI-join rewrite below reads `subquery.from` as if it
    // named a real table and synthesizes a join against it, producing a bogus
    // "no such table" for the CTE name (with2.test 7.5:
    // `... WHERE y IN (WITH ss(x) AS (VALUES(7) UNION ALL SELECT x+7 FROM ss ...) SELECT x FROM ss)`).
    // The subquery's WITH clause is also silently dropped by the rewrite. Bail so the
    // caller falls back to row-by-row `eval_in_subquery`, whose `.execute()` path
    // materializes the subquery's own WITH clause correctly.
    if subquery.with_clause.is_some() {
        return None;
    }

    // Must have exactly one column in SELECT list
    if subquery.select_list.len() != 1 {
        return None;
    }

    let subquery_column = match &subquery.select_list[0] {
        SelectItem::Expression { expr, .. } => expr.clone(),
        _ => return None,
    };

    // NULL-safety gate for NOT IN (issue #6109).
    //
    // `x NOT IN (S)` cannot become a plain ANTI join unless BOTH sides are provably
    // free of NULLs, because an equality-based ANTI join diverges from three-valued
    // `NOT IN` at each NULL:
    //   - NULL in the subquery `S`: when `S` yields a NULL and `x` matches no
    //     non-NULL member, `x NOT IN (S)` is UNKNOWN → the row is dropped; but an
    //     ANTI join *keeps* it (no equal partner on the right). So the subquery
    //     projection must be provably non-NULL.
    //   - NULL on the left (`x`): `NULL NOT IN (S)` is UNKNOWN for non-empty `S`
    //     (row dropped) and TRUE only for empty `S`; but `NULL = v` is never TRUE,
    //     so an ANTI join *always* keeps a NULL-LHS row. So the outer LHS must be
    //     provably non-NULL too.
    // If we cannot prove both, abort the transform so the caller falls back to
    // row-by-row `eval_in_subquery`, which implements the correct three-valued
    // semantics. (Plain `IN` → SEMI join is unaffected: for IN, a NULL on either
    // side simply never matches, which the SEMI join already models correctly.)
    if negated
        && (!subquery_projection_is_provably_non_null(&subquery_column, subquery, database)
            || !outer_expr_is_provably_non_null(expr, from, database))
    {
        return None;
    }

    // Skip if the subquery's SELECT expression contains a window function (issue #5231).
    // Window functions are computed over the subquery's entire result set, so hoisting
    // them into a per-row join ON condition is semantically invalid:
    //   x IN (SELECT row_number() OVER (ORDER BY ...) FROM t)
    // cannot become `... SEMI JOIN t ON x = row_number() OVER (...)`.
    // Falling back to row-by-row IN evaluation (eval_in_subquery) handles both
    // correlated and uncorrelated forms correctly. This guard also covers the
    // NOT IN → ANTI join path and the aggregate (GROUP BY/HAVING) path below.
    if crate::select::window::expression_has_window_function(&subquery_column) {
        return None;
    }

    // Skip if subquery has LIMIT, OFFSET, or set operations (can't safely convert)
    if subquery.limit.is_some() || subquery.offset.is_some() || subquery.set_operation.is_some() {
        return None;
    }

    // Check if this is an aggregate subquery (GROUP BY or HAVING)
    // These need to be wrapped in a derived table for the semi-join
    let is_aggregate_subquery = subquery.group_by.is_some() || subquery.having.is_some();

    if is_aggregate_subquery {
        return try_convert_aggregate_in_to_join(
            from,
            expr,
            subquery,
            &subquery_column,
            negated,
            database,
            with_clause,
        );
    }

    // Simple subquery path: requires single table in FROM clause
    let (table_name, table_alias) = match &subquery.from {
        Some(FromClause::Table { name, alias, .. }) => (name.clone(), alias.clone()),
        _ => return None, // Complex FROM clause for non-aggregate, skip
    };

    // Detect self-join: check if subquery table name conflicts with outer query tables
    let needs_alias = is_self_join(from, &table_name, &table_alias);

    // FIX for issue #4493: Skip transformation if SELECT list contains unqualified columns
    // that might be correlated references IN SELF-JOINS.
    //
    // This check is ONLY needed for self-joins because:
    // - For non-self-joins (orders vs lineitem): unqualified columns in the subquery
    //   can ONLY refer to the subquery's table. No ambiguity possible.
    // - For self-joins (tab0 vs tab0): unqualified columns could refer to either
    //   the outer tab0 or inner tab0, creating ambiguity.
    //
    // Example that should NOT be transformed (self-join with multiple outer tables):
    //   SELECT x FROM t2, t1 WHERE x IN (SELECT x FROM t1 WHERE ...)
    //   If the inner `SELECT x` has `x` as a correlated reference (from t2, not t1),
    //   rewriting it to `__subquery_t1.x` will fail because that column doesn't exist in t1.
    //
    // Safe to optimize (simple self-join):
    //   SELECT col0 FROM tab0 WHERE col0 IN (SELECT col3 FROM tab0 WHERE ...)
    //   Single table 'tab0' in outer query, unqualified 'col3' must be from tab0.
    //
    // Safe to optimize (non-self-join):
    //   SELECT o_orderkey FROM orders WHERE o_orderkey IN (SELECT l_orderkey FROM lineitem)
    //   Different tables, no ambiguity - l_orderkey can only be from lineitem.
    if needs_alias {
        if let Expression::ColumnRef(col_id) = &subquery_column {
            if col_id.schema_canonical().is_none() && col_id.table_canonical().is_none() {
                // Check if outer query has exactly one table and it matches the subquery's table
                let is_simple_self_join =
                    is_simple_single_table_self_join(from, &table_name, &table_alias);

                if !is_simple_self_join {
                    // Self-join with multiple outer tables - unqualified column might be correlated.
                    // Skip optimization to be safe.
                    return None;
                }
                // else: Simple self-join with single table - safe to optimize
            }
        }
    }

    // Generate a unique alias for self-joins to avoid schema conflicts
    let (
        effective_alias,
        outer_expr_qualified,
        subquery_column_rewritten,
        subquery_where_rewritten,
    ) = if needs_alias {
        // Create a unique alias for the right side of the self-join
        let new_alias = format!("__subquery_{}", table_name);

        if std::env::var("SUBQUERY_TRANSFORM_VERBOSE").is_ok() {
            eprintln!(
                "[SUBQUERY_TRANSFORM] Self-join detected: table={}, new_alias={}",
                table_name, new_alias
            );
        }

        // FIX for issue #4709: For simple self-joins, qualify the outer expression
        // with the outer table name to avoid ambiguity.
        //
        // When we have:
        //   SELECT * FROM t1 WHERE w IN (SELECT rowid FROM t1)
        //
        // After transformation:
        //   SELECT * FROM t1 SEMI JOIN t1 AS __subquery_t1 ON w = __subquery_t1.rowid
        //
        // The unqualified 'w' is ambiguous because both t1 and __subquery_t1 have column 'w'.
        // We need to qualify it as 't1.w'.
        //
        // However, for multi-table outer queries (FIX for issue #4493), we still leave
        // the expression unqualified:
        //   SELECT x FROM t2, t1 WHERE x IN (SELECT c FROM t1 WHERE ...)
        //
        // Here 'x' might be from t2, and qualifying as 't1.x' would be wrong.
        // The is_simple_single_table_self_join check above already filters out these cases.
        let qualified_expr = if let Some(outer_table) = get_outer_table_name(from) {
            // Simple self-join: qualify outer expression with outer table name
            rewrite_column_refs_with_alias(expr, &outer_table, &outer_table)
        } else {
            // Complex FROM clause: let SQL resolution handle it
            expr.clone()
        };

        // Use the table alias (if present) for matching column references, not just the table name
        // This is critical for Q21 where the subquery uses an alias like "l2" or "l3"
        // Column references like "l2.l_orderkey" need to match against "l2", not "LINEITEM"
        let old_table_ref = table_alias.as_ref().unwrap_or(&table_name);

        // Rewrite column references in the subquery column to use the new alias
        let rewritten_col =
            rewrite_column_refs_with_alias(&subquery_column, old_table_ref, &new_alias);

        // Rewrite column references in the WHERE clause to use the new alias.
        // We need to qualify columns that reference the subquery's own table to avoid
        // ambiguity with the outer query's table in self-joins.
        //
        // Example: SELECT * FROM tab0 WHERE col0 IN (SELECT col3 FROM tab0 WHERE col3 > 259)
        //   - Subquery WHERE: col3 > 259
        //   - After rewrite: __subquery_tab0.col3 > 259
        //   - This prevents col3 from incorrectly resolving to the outer tab0.col3
        //
        // Note: This is safe because:
        // 1. For self-joins with a single outer table, unqualified columns in the subquery
        //    must belong to the subquery's table (verified by is_simple_single_table_self_join check above)
        // 2. For deeply nested subqueries with correlation, the earlier check at line 109-121
        //    skips this optimization path entirely, so we won't break correlation
        let rewritten_where = subquery
            .where_clause
            .as_ref()
            .map(|w| rewrite_column_refs_with_alias(w, old_table_ref, &new_alias));

        if std::env::var("SUBQUERY_TRANSFORM_VERBOSE").is_ok() {
            eprintln!("[SUBQUERY_TRANSFORM] qualified_expr={:?}", qualified_expr);
            eprintln!("[SUBQUERY_TRANSFORM] rewritten_col={:?}", rewritten_col);
            eprintln!("[SUBQUERY_TRANSFORM] rewritten_where={:?}", rewritten_where);
        }

        (Some(new_alias), qualified_expr, rewritten_col, rewritten_where)
    } else {
        // Even when not a self-join, we need to qualify the subquery column
        // to avoid ambiguity when both sides have the same column name.
        // Use the effective table name (alias if present, otherwise table name).
        let effective_table = table_alias.as_deref().unwrap_or(&table_name);
        let qualified_subquery_column =
            rewrite_column_refs_with_alias(&subquery_column, effective_table, effective_table);
        let qualified_subquery_where = subquery
            .where_clause
            .as_ref()
            .map(|w| rewrite_column_refs_with_alias(w, effective_table, effective_table));

        // FIX for issues #5926 / #5870: qualify the outer expression against its
        // ORIGINAL lexical scope (the outer FROM) before it becomes the left side of
        // the synthesized SEMI/ANTI-join predicate.
        //
        // Without this, an unqualified outer column such as `id` in
        //   SELECT id FROM customers WHERE id IN (SELECT customer_id FROM orders)
        // is folded verbatim into the join ON condition, whose combined schema now
        // contains BOTH customers and orders. The #5870 ambiguity guard then sees
        // `id` present in two tables and wrongly raises "ambiguous column name: id"
        // — even though `id` was unambiguous in the outer scope where it was written
        // (only customers was visible there; orders lives inside the subquery).
        //
        // SQLite scopes ambiguity to the OUTER FROM only, never the subquery tables.
        // So we resolve which outer table the column belongs to using the catalog and
        // qualify against exactly that table, regardless of how many tables are in the
        // outer FROM:
        //   - column present in exactly ONE outer table  → qualify to it (unambiguous,
        //     even if the subquery table shares the name). Qualified refs bypass the
        //     guard, restoring SQLite-matching results on both the row and columnar
        //     join paths.
        //   - column present in TWO+ outer tables         → genuinely ambiguous in the
        //     outer scope; leave unqualified so the guard errors, matching SQLite.
        //   - column resolvable only against a derived/VALUES table whose columns
        //     can't be enumerated here → abort the transform (return None) and
        //     fall back to row-by-row IN evaluation, which never over-errors.
        //   - anything else (non-column expr, correlated reference, unresolved) →
        //     leave unchanged.
        let outer_expr_qualified = qualify_outer_expr_in_scope(expr, from, database, with_clause)?;

        (
            table_alias.clone(),
            outer_expr_qualified,
            qualified_subquery_column,
            qualified_subquery_where,
        )
    };

    // Create the join condition: expr = subquery_column
    let join_condition = Expression::BinaryOp {
        op: BinaryOperator::Equal,
        left: Box::new(outer_expr_qualified),
        right: Box::new(subquery_column_rewritten),
    };

    // Combine join condition with subquery's WHERE clause if it exists
    let final_condition = if let Some(subquery_where) = subquery_where_rewritten {
        Some(Expression::BinaryOp {
            op: BinaryOperator::And,
            left: Box::new(join_condition),
            right: Box::new(subquery_where),
        })
    } else {
        Some(join_condition)
    };

    // Create the right side of the join
    let right_from = FromClause::Table {
        index_hint: None,
        name: table_name,
        alias: effective_alias,
        column_aliases: None,
        quoted: false, // Synthesized from subquery, treat as unquoted
    };

    // Create SEMI or ANTI join based on negation
    let join_type = if negated { JoinType::Anti } else { JoinType::Semi };

    // Create the join
    let new_from = FromClause::Join {
        left: Box::new(from.clone()),
        right: Box::new(right_from),
        join_type,
        condition: final_condition.clone(),
        using_columns: None,
        natural: false,
        alias: None,
    };

    if std::env::var("SUBQUERY_TRANSFORM_VERBOSE").is_ok() {
        eprintln!("[SUBQUERY_TRANSFORM] Final condition: {:?}", final_condition);
        eprintln!("[SUBQUERY_TRANSFORM] New FROM: {:?}", new_from);
    }

    Some(InToJoinResult { from: new_from })
}

/// Convert an IN subquery with GROUP BY/HAVING to a SEMI/ANTI join with derived table
///
/// For aggregate subqueries like:
/// ```sql
/// WHERE o_orderkey IN (
///     SELECT l_orderkey FROM lineitem GROUP BY l_orderkey HAVING SUM(l_quantity) > 300
/// )
/// ```
///
/// We convert to:
/// ```sql
/// SEMI JOIN (
///     SELECT l_orderkey FROM lineitem GROUP BY l_orderkey HAVING SUM(l_quantity) > 300
/// ) AS __in_agg ON o_orderkey = __in_agg.l_orderkey
/// ```
///
/// This is more efficient than row-by-row IN evaluation because:
/// 1. The subquery is executed once (not per row)
/// 2. The join uses hash-based semi-join (O(1) probe per row)
fn try_convert_aggregate_in_to_join(
    from: &FromClause,
    outer_expr: &Expression,
    subquery: &SelectStmt,
    subquery_column: &Expression,
    negated: bool,
    database: &Database,
    with_clause: Option<&[CommonTableExpr]>,
) -> Option<InToJoinResult> {
    // Extract the column name from the subquery's select list for the join condition
    // The column must be a simple column reference for us to build the join condition
    let column_name = match subquery_column {
        Expression::ColumnRef(col_id) => col_id.column_canonical().to_string(),
        // For aggregate subqueries, we could also handle expressions by giving them an alias,
        // but for now we only support simple column references
        _ => return None,
    };

    // Use a counter to generate unique aliases for nested cases
    // Thread-local counter ensures uniqueness within a query optimization pass
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let alias = format!("__in_agg_{}", COUNTER.fetch_add(1, Ordering::Relaxed));

    if std::env::var("SUBQUERY_TRANSFORM_VERBOSE").is_ok() {
        eprintln!(
            "[SUBQUERY_TRANSFORM] Converting aggregate IN subquery to derived table semi-join"
        );
        eprintln!("[SUBQUERY_TRANSFORM] Derived table alias: {}", alias);
        eprintln!("[SUBQUERY_TRANSFORM] Column for join: {}", column_name);
    }

    // Create the derived table from the subquery
    let right_from = FromClause::Subquery {
        query: Box::new(subquery.clone()),
        alias: alias.clone(),
        column_aliases: None,
    };

    // Qualify the outer expression against its outer-FROM scope (issues #5926 /
    // #5870), same as the simple-subquery path above, so a bare outer column that
    // is unambiguous in the outer scope is not tripped by the ambiguity guard after
    // being folded into the synthesized predicate. If it can only be resolved
    // against an unenumerable derived/VALUES source, abort the transform (return
    // None) and fall back to row-by-row IN evaluation, which never over-errors.
    let outer_expr_qualified =
        qualify_outer_expr_in_scope(outer_expr, from, database, with_clause)?;

    // Create the join condition: outer_expr = __in_agg.column_name
    let join_condition = Expression::BinaryOp {
        op: BinaryOperator::Equal,
        left: Box::new(outer_expr_qualified),
        right: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::qualified(
            &alias,
            false,
            &column_name,
            false,
        ))),
    };

    // Create SEMI or ANTI join based on negation
    let join_type = if negated { JoinType::Anti } else { JoinType::Semi };

    // Create the join
    let new_from = FromClause::Join {
        left: Box::new(from.clone()),
        right: Box::new(right_from),
        join_type,
        condition: Some(join_condition.clone()),
        using_columns: None,
        natural: false,
        alias: None,
    };

    if std::env::var("SUBQUERY_TRANSFORM_VERBOSE").is_ok() {
        eprintln!("[SUBQUERY_TRANSFORM] Final condition: {:?}", join_condition);
        eprintln!("[SUBQUERY_TRANSFORM] New FROM: {:?}", new_from);
    }

    Some(InToJoinResult { from: new_from })
}
