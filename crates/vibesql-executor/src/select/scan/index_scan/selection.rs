//! Index selection logic
//!
//! Determines when and which index to use for query optimization.
//! Supports both rule-based (simple) and cost-based (statistics-aware) selection.
//! Also includes skip-scan optimization for queries filtering on non-prefix columns.
//! Supports expression indexes (functional indexes) like CREATE INDEX idx ON t(lower(name)).

use vibesql_ast::{pretty_print::ToSql, Expression, IndexColumn};
use vibesql_catalog::TableSchema;
use vibesql_storage::{
    statistics::{AccessMethod, CostEstimator},
    Database,
};

use crate::evaluator::expression_hash::ExpressionHasher;

use crate::optimizer::index_planner::{IndexPlanner, SkipScanInfo};

/// Conservative selectivity assumed for an **expression-index** predicate when no
/// column histogram is available. Matches the flat 0.33 fallback `estimate_selectivity`
/// returns for predicates it cannot model from statistics, so an expression index
/// (e.g. `CREATE INDEX t1a1 ON t1(substr(a,1,12))`) is costed the same whether the
/// query routes through the column path or the expression path.
const EXPRESSION_INDEX_SELECTIVITY: f64 = 0.33;

/// Classification of an OR branch's index lookup shape.
///
/// SQLite routes `IS NULL` branches to a *different* index lookup than equality
/// (`=`) branches: `d IS NULL` seeks the NULL key, while `d = ?` seeks a concrete
/// value. Preserving this distinction in the plan representation lets later PRs
/// dispatch each branch to the correct lookup. For PR 1 it is dead-but-tested
/// metadata; the `branch_predicate` carries the original expression regardless.
#[allow(dead_code)] // PR 1: plan representation only; consumed in PR 2+.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum OrBranchKind {
    /// `col IS NULL` — seeks the NULL key (distinct from `=`).
    IsNull,
    /// Any other indexable predicate (`=`, range, `IN`, `BETWEEN`, ...).
    Lookup,
}

/// One indexable branch of a top-level OR, in MULTI-INDEX OR plan form.
///
/// `ordinal` is the branch's **1-based term position in the original OR
/// expression**, NOT a renumbering over the chosen branches. SQLite renders OR
/// branches as `INDEX <ordinal>` keyed by this original position (e.g. where9-3.1
/// expects `INDEX 1` / `INDEX 3` for a three-term OR whose middle term is folded
/// away), so the analyzer must preserve original ordinals.
#[allow(dead_code)] // PR 1: plan representation only; consumed in PR 2+.
#[derive(Debug, Clone)]
pub(crate) struct OrBranch {
    /// 1-based term position in the original OR expression.
    pub ordinal: usize,
    /// Name of the index this branch resolves to.
    pub index_name: String,
    /// The original predicate for this branch (e.g. `c = 31031`, `d IS NULL`).
    pub branch_predicate: Expression,
    /// Whether this branch is an `IS NULL` seek or an ordinary lookup.
    pub kind: OrBranchKind,
}

/// Result of index selection, distinguishing between regular and skip-scan
#[derive(Debug, Clone)]
pub(crate) enum IndexScanChoice {
    /// Regular index scan using prefix columns
    Regular {
        index_name: String,
        sorted_columns: Option<Vec<(String, vibesql_ast::OrderDirection)>>,
    },
    /// Skip-scan using non-prefix column filter
    SkipScan { index_name: String, skip_scan_info: SkipScanInfo },
    /// MULTI-INDEX OR: a union of per-branch index lookups.
    ///
    /// Selected only when *every* top-level OR branch is independently
    /// indexable. Each branch lookup yields a set of rowids; the union
    /// (deduplicated by rowid) is the result, with `residual` applied as a
    /// post-filter for the non-OR AND-conjuncts.
    ///
    /// NOTE (PR 1): this variant is plan representation only. Selection and
    /// execution intentionally never produce or consume it yet — it is
    /// dead-but-tested code. Behavior is byte-identical to before.
    #[allow(dead_code)]
    MultiIndexOr {
        /// One entry per indexable OR branch, in original-branch ordinal order.
        branches: Vec<OrBranch>,
        /// Non-OR AND-conjuncts applied around the union (e.g. `b > 1000`).
        residual: Option<Expression>,
    },
}

/// Check if any ORDER BY column is nullable in a way that prevents using the index
/// to satisfy the ORDER BY.
///
/// BTreeMap orders NULLs first (NULL < everything), but SQL default is:
/// - NULLS LAST for ASC
/// - NULLS FIRST for DESC
///
/// When a nullable column is used for ORDER BY without explicit NULLS FIRST/LAST,
/// the BTreeMap-iteration order would not match the SQL default for non-NULL
/// columns. To avoid producing rows in the wrong order, we normally reject the
/// index as a "pre-sort" — runtime path: `sorted_columns = None` → post-scan
/// `apply_order_by` runs.
///
/// **Pinned-column exception (per ORDER BY item)**: If an ORDER BY item is itself
/// one of the pinned leading index columns (e.g., `ORDER BY a, b` with
/// `WHERE a IN (1,2,3)` and pinned `a`), that ORDER BY item is constant within
/// each scan group and its NULL ordering is irrelevant — IN-lists/equality with
/// literal NULL never match NULL rows. The nullable check is skipped for such
/// items.
///
/// `index_columns` is the indexed-column list and `pinned_columns` is the count
/// of leading consecutive columns pinned by equality/IN predicates. Pass an
/// empty slice and `0` to apply the unconditional nullable rejection (callers
/// without index context should use this form).
fn any_order_by_column_nullable(
    order_items: &[vibesql_ast::OrderByItem],
    table_schema: &TableSchema,
    index_columns: &[IndexColumn],
    pinned_columns: usize,
) -> bool {
    let pinned_index_slice = &index_columns[..pinned_columns.min(index_columns.len())];
    for item in order_items {
        // Skip the per-item nullable check when this ORDER BY item refers to
        // one of the pinned leading index columns (constant within scan group).
        if !pinned_index_slice.is_empty()
            && order_item_matches_pinned_column(item, pinned_index_slice)
        {
            continue;
        }

        if let Expression::ColumnRef(col_id) = &item.expr {
            let column = col_id.column_canonical();
            // Look up column in schema (case-insensitive)
            if let Some(idx) = table_schema.get_column_index(column) {
                if table_schema.columns[idx].nullable {
                    return true;
                }
            }
        }
    }
    false
}

/// Determines if an index scan is beneficial for the given query
///
/// Returns Some((index_name, sorted_columns)) if an index should be used, None otherwise.
/// The sorted_columns vector indicates which columns are pre-sorted by the index scan.
#[allow(clippy::type_complexity)]
pub(crate) fn should_use_index_scan(
    table_name: &str,
    where_clause: Option<&Expression>,
    order_by: Option<&[vibesql_ast::OrderByItem]>,
    database: &Database,
) -> Option<(String, Option<Vec<(String, vibesql_ast::OrderDirection)>>)> {
    // We use indexes in three scenarios:
    // 1. WHERE clause references an indexed column (with or without ORDER BY)
    // 2. ORDER BY references an indexed column (even without WHERE)
    // 3. Both WHERE and ORDER BY use the same index
    //
    // Note: Index scans can provide partial optimization even for complex
    // predicates (including OR expressions). The full WHERE clause is always
    // applied as a post-filter in execute_index_scan() to ensure correctness.

    // Get all indexes for this table
    let table = database.get_table(table_name)?;
    let indexes = database.list_indexes_for_table(table_name);

    if indexes.is_empty() {
        return None;
    }

    // Find the best index (most pinned columns = better filtering)
    // We evaluate all applicable indexes and pick the one that covers the most WHERE columns
    let mut best_index: Option<(
        String,
        usize,
        bool,
        bool,
        Option<Vec<(String, vibesql_ast::OrderDirection)>>,
    )> = None;
    // (index_name, pinned_count, top_level_seekable, can_use_for_order, sorted_columns)

    for index_name in &indexes {
        if let Some(index_metadata) = database.get_index(index_name) {
            // Expression indexes reloaded from a snapshot with an empty,
            // not-yet-rebuilt body must not be consulted for reads (they would
            // silently return zero rows). Decline them so we fall back to a
            // full-table scan until the executor rebuilds the body. See #5784.
            if database.is_index_pending_rebuild(index_name) {
                continue;
            }

            // Partial indexes (CREATE INDEX ... WHERE expr) only cover the
            // subset of rows for which the predicate evaluates to TRUE. They
            // are usable only when the query's WHERE clause structurally
            // implies the index predicate (every index-predicate conjunct
            // appears among the query's top-level AND conjuncts). The full
            // WHERE clause is re-applied as a post-filter in
            // execute_index_scan(), so an over-inclusive index body cannot
            // produce wrong rows. The partial predicate lives on the
            // catalog-side `IndexMetadata`.
            if !crate::optimizer::predicate_implication::partial_index_usable(
                database,
                index_name,
                where_clause,
            ) {
                continue;
            }

            let first_indexed_column = index_metadata.columns.first()?;

            // Check if this index can be used for WHERE clause
            // Supports both column indexes and expression indexes
            let can_use_for_where = where_clause
                .map(|expr| index_column_can_filter(expr, first_indexed_column))
                .unwrap_or(false);

            // Count how many leading index columns are pinned by equality predicates
            // Note: For expression indexes, we count expression matches as pinned.
            // This (IN-inclusive) count drives index *seeking* and cost comparison.
            let pinned_columns = count_pinned_index_columns(where_clause, &index_metadata.columns);

            // For ORDER BY satisfaction we must use the stricter single-value pin
            // count: a column constrained by a multi-valued IN-list is NOT
            // constant within the scan output, so it cannot be skipped when
            // matching the ORDER BY against trailing index columns. Treating an
            // IN-pinned column as constant produced wrong row order for
            // `x IN (..) ORDER BY x DESC, y` (where-5.102 / where-5.103).
            let order_pinned_columns =
                count_single_value_pinned_index_columns(where_clause, &index_metadata.columns);

            // Check if this index can be used for ORDER BY clause
            let can_use_for_order = if let Some(order_items) = order_by {
                // Check if ORDER BY columns match the index columns (after skipping pinned columns)
                let columns_match = can_use_index_for_order_by_with_pinned(
                    order_items,
                    &index_metadata.columns,
                    order_pinned_columns,
                );

                // Don't use index for ORDER BY if any non-pinned column is nullable.
                // BTreeMap orders NULLs first, but SQL default is NULLS LAST for ASC,
                // which would produce incorrect results for nullable columns. When
                // at least one leading index column is pinned by equality/IN, the
                // pinned-prefix exception in `any_order_by_column_nullable` allows
                // the index to be used regardless of trailing nullability.
                if columns_match
                    && any_order_by_column_nullable(
                        order_items,
                        &table.schema,
                        &index_metadata.columns,
                        order_pinned_columns,
                    )
                {
                    false
                } else {
                    columns_match
                }
            } else {
                false
            };

            // Skip if this index can't help with WHERE or ORDER BY
            if !can_use_for_where && !can_use_for_order {
                continue;
            }

            // Build sorted_columns metadata if ORDER BY can be satisfied
            let sorted_columns = if can_use_for_order {
                let order_items = order_by.unwrap();
                Some(
                    order_items
                        .iter()
                        .map(|item| {
                            // For expression indexes, ORDER BY may use expressions (e.g., length(a))
                            // We use to_sql() to convert the expression to a string representation
                            // The actual string is only used for metadata; the important part
                            // is the direction for determining scan order
                            let col_name = match &item.expr {
                                Expression::ColumnRef(col_id) => {
                                    col_id.column_canonical().to_string()
                                }
                                expr => expr.to_sql(),
                            };
                            (col_name, item.direction.clone())
                        })
                        .collect(),
                )
            } else {
                None
            };

            // Whether this index's leading column yields a real index *seek*
            // from a top-level AND conjunct (renders `SEARCH ... (col op ?)`)
            // rather than degrading to a bare `SCAN` because its only predicate
            // is buried inside an OR branch. This is the principled
            // SEARCH-over-SCAN tie-break that fixes where9-5.3 (`b>1000 AND
            // (c>=31031 OR d IS NULL)` must pick `t1b`/SEARCH over `t1c`/SCAN).
            let top_level_seekable =
                index_leading_column_seekable_at_top_level(where_clause, &index_metadata.columns);

            // Compare with best index so far.
            // Tie-break order (all deterministic — no HashMap-iteration
            // dependence, which was the source of the where9-5.3
            // non-determinism, #5660):
            //   1. leading column seekable at top level (SEARCH beats SCAN)
            //   2. more pinned columns (narrows more rows)
            //   3. can satisfy ORDER BY
            //   4. lexicographically smaller index name (stable final tie-break)
            //
            // Seekability is the PRIMARY signal because a pin that does not
            // produce a real seek is illusory: an `IN (SELECT ...)` subquery on
            // the leading column counts as a "pinned" column for cost purposes,
            // yet the seek extractor produces NO seek for it, so EQP renders that
            // index as a bare `SCAN`. Ranking such a (pinned-but-SCAN) index
            // above a genuinely-seekable (`SEARCH`) competitor is exactly the
            // SEARCH-over-SCAN inversion this PR fixes (e.g. `x IN (SELECT ...)
            // AND y>?` must pick `SEARCH ty (y>?)`, not `SCAN tx`). Any index
            // with a genuine equality/`IN`-list pin on its leading column is
            // itself top-level seekable, so this reordering only demotes the
            // illusory-pin (IN-subquery / negated-predicate) case.
            let is_better = match &best_index {
                None => true,
                Some((best_name, best_pinned, best_seekable, best_can_order, _)) => {
                    if top_level_seekable != *best_seekable {
                        top_level_seekable && !*best_seekable
                    } else if pinned_columns != *best_pinned {
                        pinned_columns > *best_pinned
                    } else if can_use_for_order != *best_can_order {
                        can_use_for_order && !*best_can_order
                    } else {
                        index_name.as_str() < best_name.as_str()
                    }
                }
            };

            if is_better {
                best_index = Some((
                    index_name.clone(),
                    pinned_columns,
                    top_level_seekable,
                    can_use_for_order,
                    sorted_columns,
                ));
            }
        }
    }

    // Return the best index if we found one
    if let Some((index_name, _, _, _, sorted_columns)) = best_index {
        return Some((index_name, sorted_columns));
    }

    None
}

/// Check if an expression filters a specific column
///
/// Returns true if the expression contains a predicate on the given column
/// For example: "WHERE age = 25" filters column "age"
pub(crate) fn expression_filters_column(expr: &Expression, column_name: &str) -> bool {
    match expr {
        Expression::BinaryOp { left, op, right } => {
            // Check for comparison operators
            match op {
                vibesql_ast::BinaryOperator::Equal
                | vibesql_ast::BinaryOperator::GreaterThan
                | vibesql_ast::BinaryOperator::GreaterThanOrEqual
                | vibesql_ast::BinaryOperator::LessThan
                | vibesql_ast::BinaryOperator::LessThanOrEqual => {
                    // Index can only filter when comparing column to a LITERAL value
                    // NOT when comparing column to another column (equijoin conditions)
                    // e.g., `l_shipdate > '1995-03-15'` CAN use index
                    // e.g., `l_orderkey = o_orderkey` CANNOT use index
                    let left_is_col = is_column_reference(left, column_name);
                    let right_is_col = is_column_reference(right, column_name);
                    let left_is_literal = is_literal(left);
                    let right_is_literal = is_literal(right);

                    // column op literal OR literal op column
                    if (left_is_col && right_is_literal) || (left_is_literal && right_is_col) {
                        return true;
                    }
                }
                vibesql_ast::BinaryOperator::And | vibesql_ast::BinaryOperator::Or => {
                    // Recursively check sub-expressions for AND/OR
                    return expression_filters_column(left, column_name)
                        || expression_filters_column(right, column_name);
                }
                _ => {}
            }
            false
        }
        // IS / IS NOT (NULL-safe comparison)
        // negated=true means "IS NOT DISTINCT FROM" which is equivalent to "IS" (NULL-safe equals)
        // negated=false means "IS DISTINCT FROM" which is equivalent to "IS NOT" (NULL-safe not-equals)
        // We only use the index for IS (negated=true) as it's equivalent to =
        Expression::IsDistinctFrom { left, right, negated: true } => {
            let left_is_col = is_column_reference(left, column_name);
            let right_is_col = is_column_reference(right, column_name);
            let left_is_literal = is_literal(left);
            let right_is_literal = is_literal(right);
            // column IS literal OR literal IS column
            (left_is_col && right_is_literal) || (left_is_literal && right_is_col)
        }
        // IN with value list: col IN (1, 2, 3)
        Expression::InList { expr, .. } => is_column_reference(expr, column_name),
        // IN with subquery: col IN (SELECT ...)
        Expression::In { expr, .. } => is_column_reference(expr, column_name),
        // BETWEEN: col BETWEEN low AND high
        Expression::Between { expr, .. } => is_column_reference(expr, column_name),
        // Conjunction: AND
        Expression::Conjunction(exprs) => {
            exprs.iter().any(|e| expression_filters_column(e, column_name))
        }
        // Disjunction: OR
        Expression::Disjunction(exprs) => {
            exprs.iter().any(|e| expression_filters_column(e, column_name))
        }
        _ => false,
    }
}

/// Check if an expression matches an expression index
///
/// For expression indexes like `CREATE INDEX idx ON t(lower(name))`, this function
/// checks if the WHERE clause contains a predicate on the indexed expression.
/// For example: `WHERE lower(name) = 'john'` matches the index.
///
/// Uses structural hashing to compare expressions - two expressions are considered
/// equivalent if they have the same structure (same operations on same columns).
pub(crate) fn expression_filters_index_expression(
    where_expr: &Expression,
    index_expr: &Expression,
) -> bool {
    let index_hash = ExpressionHasher::hash(index_expr);
    expression_contains_matching_predicate(where_expr, index_hash)
}

/// Check if an expression contains a predicate on a specific expression (by hash)
///
/// Recursively searches through AND/OR combinations to find predicates.
fn expression_contains_matching_predicate(expr: &Expression, target_hash: u64) -> bool {
    match expr {
        Expression::BinaryOp { left, op, right } => {
            match op {
                vibesql_ast::BinaryOperator::Equal
                | vibesql_ast::BinaryOperator::GreaterThan
                | vibesql_ast::BinaryOperator::GreaterThanOrEqual
                | vibesql_ast::BinaryOperator::LessThan
                | vibesql_ast::BinaryOperator::LessThanOrEqual => {
                    // Check if left or right side matches the indexed expression
                    let left_hash = ExpressionHasher::hash(left);
                    let right_hash = ExpressionHasher::hash(right);
                    let left_is_literal = is_literal(left);
                    let right_is_literal = is_literal(right);

                    // expr op literal OR literal op expr
                    if (left_hash == target_hash && right_is_literal)
                        || (right_hash == target_hash && left_is_literal)
                    {
                        return true;
                    }
                }
                vibesql_ast::BinaryOperator::And | vibesql_ast::BinaryOperator::Or => {
                    return expression_contains_matching_predicate(left, target_hash)
                        || expression_contains_matching_predicate(right, target_hash);
                }
                _ => {}
            }
            false
        }
        // IS / IS NOT (NULL-safe comparison)
        Expression::IsDistinctFrom { left, right, negated: true } => {
            let left_hash = ExpressionHasher::hash(left);
            let right_hash = ExpressionHasher::hash(right);
            let left_is_literal = is_literal(left);
            let right_is_literal = is_literal(right);
            (left_hash == target_hash && right_is_literal)
                || (right_hash == target_hash && left_is_literal)
        }
        // IN with value list: expr IN (1, 2, 3)
        Expression::InList { expr, .. } => ExpressionHasher::hash(expr) == target_hash,
        // IN with subquery: expr IN (SELECT ...)
        Expression::In { expr, .. } => ExpressionHasher::hash(expr) == target_hash,
        // BETWEEN: expr BETWEEN low AND high
        Expression::Between { expr, .. } => ExpressionHasher::hash(expr) == target_hash,
        // Conjunction: AND
        Expression::Conjunction(exprs) => {
            exprs.iter().any(|e| expression_contains_matching_predicate(e, target_hash))
        }
        // Disjunction: OR
        Expression::Disjunction(exprs) => {
            exprs.iter().any(|e| expression_contains_matching_predicate(e, target_hash))
        }
        _ => false,
    }
}

/// Check if an IndexColumn can be used for the given WHERE clause
///
/// This function handles both column indexes and expression indexes:
/// - For column indexes: delegates to `expression_filters_column`
/// - For expression indexes: uses `expression_filters_index_expression`
pub(crate) fn index_column_can_filter(where_expr: &Expression, index_col: &IndexColumn) -> bool {
    match index_col {
        IndexColumn::Column { column_name, .. } => {
            expression_filters_column(where_expr, column_name)
        }
        IndexColumn::Expression { expr, .. } => {
            expression_filters_index_expression(where_expr, expr)
        }
    }
}

/// Whether the leading column of `index_columns` is constrained by a predicate
/// that lives in a **top-level conjunct** of `where_clause` — i.e. a predicate
/// that survives as an index *seek/range bound* (`SEARCH ... (col op ?)`) rather
/// than degrading to a full `SCAN`.
///
/// ## Why this is the SEARCH-vs-SCAN distinguisher (where9-5.3)
///
/// `extract_index_predicates` (the EQP / runtime seek extractor) descends into
/// `AND`/`Conjunction`/`BETWEEN` but **not** into `OR`/`Disjunction`: only a
/// predicate reachable through top-level AND structure becomes an index seek.
/// A leading column referenced *only* inside an OR branch (e.g. `c>=31031` in
/// `b>1000 AND (c>=31031 OR d IS NULL)`) yields **no** extractable predicate, so
/// that index renders as a bare `SCAN` — strictly worse than a `SEARCH` range
/// seek on the AND-clause column `b>1000`.
///
/// `index_column_can_filter` cannot make this distinction: it descends into OR
/// (so it reports `true` for both `t1b` and `t1c`), which is exactly why the
/// pre-fix selector could pick `t1c`/SCAN. This predicate mirrors the seek
/// extractor's descent rule so the selector prefers the index that actually
/// produces a seek. It is the general, query-shape-driven signal — not a
/// where9-5.3 special case.
pub(crate) fn index_leading_column_seekable_at_top_level(
    where_clause: Option<&Expression>,
    index_columns: &[IndexColumn],
) -> bool {
    let where_clause = match where_clause {
        Some(expr) => expr,
        None => return false,
    };
    let leading = match index_columns.first() {
        Some(col) => col,
        None => return false,
    };
    leading_column_seekable_top_level(where_clause, leading)
}

/// Recursive helper for [`index_leading_column_seekable_at_top_level`]. Descends
/// through top-level AND structure only (`Conjunction` / `BinaryOp { And }`),
/// matching the descent rule of the seek extractor; an `OR`/`Disjunction` node
/// short-circuits to `false` because nothing inside it is a top-level seek.
fn leading_column_seekable_top_level(expr: &Expression, leading: &IndexColumn) -> bool {
    match expr {
        // AND structure: a top-level seek may live in any conjunct.
        Expression::Conjunction(exprs) => {
            exprs.iter().any(|e| leading_column_seekable_top_level(e, leading))
        }
        Expression::BinaryOp { left, op: vibesql_ast::BinaryOperator::And, right } => {
            leading_column_seekable_top_level(left, leading)
                || leading_column_seekable_top_level(right, leading)
        }
        // OR / Disjunction: nothing here is a top-level seek — stop descending.
        Expression::Disjunction(_)
        | Expression::BinaryOp { op: vibesql_ast::BinaryOperator::Or, .. } => false,
        // A leaf predicate: does it directly produce an index *seek* on the
        // leading column? We must mirror the seek extractor's leaf set EXACTLY
        // (`extract_predicates_recursive` in explain.rs) so the two functions
        // genuinely agree — otherwise the selector could deterministically pick
        // a SCAN-rendering index over an available SEARCH-rendering one.
        other => leaf_predicate_produces_seek(other, leading),
    }
}

/// Whether a single leaf predicate produces an index *seek* on `leading`.
///
/// This mirrors the accepted leaf set of the seek extractor
/// (`extract_predicates_recursive` in explain.rs) EXACTLY. It is deliberately
/// NARROWER than [`index_column_can_filter`] / [`expression_filters_column`],
/// which accept a wider predicate set (negated `BETWEEN`/`IN-list`, and
/// `IN (SELECT ...)` subqueries) that the extractor produces NO seek for.
/// Accepting those wider shapes here would make the selector report an index as
/// "seekable at top level" when EQP actually renders it as a bare `SCAN`.
///
/// Accepted (matching the extractor):
/// - comparison ops `= < <= > >=` (column compared to a literal/parameter),
/// - `IsDistinctFrom { negated: true }` (NULL-safe `IS`, rendered as `=`),
/// - `Between { negated: false }`,
/// - `InList { negated: false }`.
///
/// Explicitly EXCLUDED (the extractor produces no seek for these):
/// - `Expression::In` (IN-subquery),
/// - negated `Between` / `InList`,
/// - `!=` / `NotEqual`.
fn leaf_predicate_produces_seek(expr: &Expression, leading: &IndexColumn) -> bool {
    // For expression indexes, compare by structural hash against the indexed
    // expression; for column indexes, compare against the column name.
    let leaf_matches_index = |target: &Expression| -> bool {
        match leading {
            IndexColumn::Column { column_name, .. } => is_column_reference(target, column_name),
            IndexColumn::Expression { expr: index_expr, .. } => {
                ExpressionHasher::hash(target) == ExpressionHasher::hash(index_expr)
            }
        }
    };

    match expr {
        Expression::BinaryOp { left, op, right } => {
            match op {
                vibesql_ast::BinaryOperator::Equal
                | vibesql_ast::BinaryOperator::GreaterThan
                | vibesql_ast::BinaryOperator::GreaterThanOrEqual
                | vibesql_ast::BinaryOperator::LessThan
                | vibesql_ast::BinaryOperator::LessThanOrEqual => {
                    // index_expr op literal OR literal op index_expr.
                    // Comparing the indexed column/expr to another column is an
                    // equijoin condition, not a seek bound, so require a literal
                    // on the opposite side.
                    (leaf_matches_index(left) && is_literal(right))
                        || (is_literal(left) && leaf_matches_index(right))
                }
                // NotEqual and all other ops: no seek.
                _ => false,
            }
        }
        // IS (NULL-safe equals): negated=true is "IS NOT DISTINCT FROM" == "IS",
        // rendered as `=`. negated=false (IS DISTINCT FROM) produces no seek.
        Expression::IsDistinctFrom { left, right, negated: true } => {
            (leaf_matches_index(left) && is_literal(right))
                || (is_literal(left) && leaf_matches_index(right))
        }
        // BETWEEN expands to `>= AND <=` — a seek — but ONLY when not negated.
        Expression::Between { expr, negated: false, .. } => leaf_matches_index(expr),
        // IN-list is treated as equality — a seek — but ONLY when not negated.
        Expression::InList { expr, negated: false, .. } => leaf_matches_index(expr),
        // Everything else (IN-subquery, negated BETWEEN/InList, ...) is NOT a
        // top-level seek; the extractor produces nothing for these.
        _ => false,
    }
}

/// Check if an expression is a reference to a specific column.
///
/// Uses case-insensitive comparison because:
/// - ColumnIdentifier.column_canonical() returns lowercase for unquoted identifiers
/// - Schema metadata (column_name) may not be consistently lowercased
/// - For SQL:1999 compliance with quoted identifiers, both sides should use canonical forms
///
/// TODO: Once schema metadata consistently uses canonical forms, change to direct equality
pub(super) fn is_column_reference(expr: &Expression, column_name: &str) -> bool {
    match expr {
        Expression::ColumnRef(col_id) => {
            col_id.column_canonical().eq_ignore_ascii_case(column_name)
        }
        _ => false,
    }
}

/// Check if an expression is a non-NULL literal value or parameter placeholder
///
/// Index scans can filter on columns compared to literal values or bound parameters,
/// not columns compared to other columns (which are equijoin conditions).
/// Parameter placeholders (?, $1, :name, etc.) are treated as literals for index selection
/// because they are resolved to concrete values at execution time.
///
/// **Important**: NULL literals are excluded because `col = NULL` can never return true
/// (per SQL three-valued logic), so using an index for this predicate is incorrect.
fn is_literal(expr: &Expression) -> bool {
    match expr {
        // Exclude NULL literals - col = NULL can never match rows
        Expression::Literal(vibesql_types::SqlValue::Null) => false,
        Expression::Literal(_) => true,
        Expression::Placeholder(_)
        | Expression::NumberedPlaceholder(_)
        | Expression::NamedPlaceholder(_) => true,
        _ => false,
    }
}

/// Check if two expressions are structurally equivalent for index matching purposes.
///
/// This performs a normalized comparison of expressions, suitable for matching
/// WHERE clause expressions against expression index definitions.
///
/// Normalization includes:
/// - Case-insensitive comparison of column names and function names
/// - Order-independent comparison for commutative operators where applicable
///
/// # Example
/// ```text
/// Index: CREATE INDEX idx ON t(LOWER(email))
/// Query: SELECT * FROM t WHERE LOWER(email) = 'x'
///
/// expressions_match(LOWER(email), LOWER(email)) -> true
/// ```
pub(crate) fn expressions_match(expr1: &Expression, expr2: &Expression) -> bool {
    match (expr1, expr2) {
        // Column references: case-insensitive comparison
        (Expression::ColumnRef(col1), Expression::ColumnRef(col2)) => {
            col1.column_canonical().eq_ignore_ascii_case(&col2.column_canonical())
        }

        // Function calls: compare function name and arguments
        (
            Expression::Function { name: name1, args: args1, .. },
            Expression::Function { name: name2, args: args2, .. },
        ) => {
            // Function names are case-insensitive in SQL
            name1.canonical().eq_ignore_ascii_case(name2.canonical())
                && args1.len() == args2.len()
                && args1.iter().zip(args2.iter()).all(|(a1, a2)| expressions_match(a1, a2))
        }

        // Binary operations: compare operator and operands
        (
            Expression::BinaryOp { left: l1, op: op1, right: r1 },
            Expression::BinaryOp { left: l2, op: op2, right: r2 },
        ) => op1 == op2 && expressions_match(l1, l2) && expressions_match(r1, r2),

        // Unary operations
        (Expression::UnaryOp { op: op1, expr: e1 }, Expression::UnaryOp { op: op2, expr: e2 }) => {
            op1 == op2 && expressions_match(e1, e2)
        }

        // Cast expressions
        (
            Expression::Cast { expr: e1, data_type: t1 },
            Expression::Cast { expr: e2, data_type: t2 },
        ) => t1 == t2 && expressions_match(e1, e2),

        // Literals: exact match
        (Expression::Literal(v1), Expression::Literal(v2)) => v1 == v2,

        // CASE expressions
        (
            Expression::Case { operand: op1, when_clauses: wc1, else_result: er1 },
            Expression::Case { operand: op2, when_clauses: wc2, else_result: er2 },
        ) => {
            // Compare operand
            let op_match = match (op1, op2) {
                (Some(o1), Some(o2)) => expressions_match(o1, o2),
                (None, None) => true,
                _ => false,
            };
            // Compare else
            let else_match = match (er1, er2) {
                (Some(e1), Some(e2)) => expressions_match(e1, e2),
                (None, None) => true,
                _ => false,
            };
            // Compare when clauses (conditions is a Vec, result is Expression)
            op_match
                && else_match
                && wc1.len() == wc2.len()
                && wc1.iter().zip(wc2.iter()).all(|(w1, w2)| {
                    w1.conditions.len() == w2.conditions.len()
                        && w1
                            .conditions
                            .iter()
                            .zip(w2.conditions.iter())
                            .all(|(c1, c2)| expressions_match(c1, c2))
                        && expressions_match(&w1.result, &w2.result)
                })
        }

        // NULL handling
        (
            Expression::IsNull { expr: e1, negated: n1 },
            Expression::IsNull { expr: e2, negated: n2 },
        ) => n1 == n2 && expressions_match(e1, e2),

        // Conjunction (AND chain)
        (Expression::Conjunction(exprs1), Expression::Conjunction(exprs2)) => {
            exprs1.len() == exprs2.len()
                && exprs1.iter().zip(exprs2.iter()).all(|(e1, e2)| expressions_match(e1, e2))
        }

        // Disjunction (OR chain)
        (Expression::Disjunction(exprs1), Expression::Disjunction(exprs2)) => {
            exprs1.len() == exprs2.len()
                && exprs1.iter().zip(exprs2.iter()).all(|(e1, e2)| expressions_match(e1, e2))
        }

        // For all other expression types, they must be exactly equal
        // This is conservative but safe
        _ => expr1 == expr2,
    }
}

/// Case-insensitive lookup of column statistics
///
/// Returns the ColumnStatistics for a column name, ignoring case differences.
/// This is necessary because schema column names may use different casing than
/// index column names or query column references.
fn get_column_stats_ignore_case<'a>(
    columns: &'a std::collections::HashMap<String, vibesql_storage::statistics::ColumnStatistics>,
    column_name: &str,
) -> Option<&'a vibesql_storage::statistics::ColumnStatistics> {
    // First try exact match for efficiency
    if let Some(stats) = columns.get(column_name) {
        return Some(stats);
    }
    // Fall back to case-insensitive search
    columns.iter().find(|(key, _)| key.eq_ignore_ascii_case(column_name)).map(|(_, stats)| stats)
}

/// Check if an index can be used to satisfy an ORDER BY clause
///
/// Returns true if the ORDER BY columns match the index columns (after skipping
/// any prefix columns pinned by equality predicates) and the sort directions
/// are compatible (either all matching or all reversed).
///
/// Examples:
/// - ORDER BY col0 ASC can use index (col0 ASC)
/// - ORDER BY col0 DESC can use index (col0 DESC) via reversal
/// - ORDER BY col0, col1 can use index (col0, col1)
/// - WHERE col0 = 1 ORDER BY col1 can use index (col0, col1) - col0 is pinned
pub(crate) fn can_use_index_for_order_by(
    order_items: &[vibesql_ast::OrderByItem],
    index_columns: &[vibesql_ast::IndexColumn],
) -> bool {
    can_use_index_for_order_by_with_pinned(order_items, index_columns, 0)
}

/// Returns true if `item` is a simple column-reference ORDER BY whose column name
/// matches one of the pinned leading index columns. Pinned columns are constants
/// within an index scan group (because they were filtered to a single value or
/// IN-list), so a leading ORDER BY reference to a pinned column is trivially
/// satisfied by the index regardless of nullability or direction.
fn order_item_matches_pinned_column(
    item: &vibesql_ast::OrderByItem,
    pinned_index_columns: &[vibesql_ast::IndexColumn],
) -> bool {
    let order_col_name = match &item.expr {
        Expression::ColumnRef(col_id)
            if col_id.schema_canonical().is_none() && col_id.table_canonical().is_none() =>
        {
            col_id.column_canonical()
        }
        _ => return false,
    };
    pinned_index_columns.iter().any(|ic| match ic {
        vibesql_ast::IndexColumn::Column { column_name, .. } => {
            order_col_name.eq_ignore_ascii_case(column_name)
        }
        vibesql_ast::IndexColumn::Expression { .. } => false,
    })
}

/// EQP-only: Determine if the ORDER BY can be served by an existing index in a
/// way that suppresses the `USE TEMP B-TREE FOR ORDER BY` step in EXPLAIN QUERY
/// PLAN output, even when a runtime post-sort would still be needed.
///
/// SQLite's EQP suppresses `USE TEMP B-TREE FOR ORDER BY` whenever an index's
/// natural traversal yields rows in (or close to) the requested order — even if
/// a final stabilization pass is required for NULL placement on nullable
/// trailing columns. We mirror that behavior: when an index has its leading
/// columns pinned by equality/IN predicates AND the ORDER BY structurally
/// aligns with the pinned-prefix-then-trailing-index layout, the EQP omits the
/// temp B-tree step. The runtime path still emits a post-scan sort when the
/// `sorted_columns` returned by `cost_based_index_selection` is `None` (which
/// happens when trailing nullable columns appear in the ORDER BY).
///
/// Returns true if a temp B-tree should be shown in EXPLAIN, false otherwise.
pub(crate) fn needs_temp_btree_for_order_by_eqp(
    table_name: &str,
    where_clause: Option<&Expression>,
    order_by: &[vibesql_ast::OrderByItem],
    database: &Database,
) -> bool {
    // No table → constant expression, no sort needed.
    let Some(table) = database.get_table(table_name) else {
        return true;
    };

    // #5375: ORDER BY led by the table's INTEGER PRIMARY KEY (the rowid
    // alias) over a plain sequential scan. The executor guarantees that a
    // sequential scan of a rowid-alias table delivers rows in rowid order
    // (`sort_rows_by_integer_primary_key`, #4926), so the requested order —
    // or its exact reverse, for DESC — IS the scan's natural output order
    // and the runtime ORDER BY sort is order-equivalent to the natural
    // traversal. sqlite3 3.51.0 shows a bare `SCAN t` with no temp line here
    // (its table B-tree is keyed by rowid); suppressing the line falls under
    // the same permissive EQP convention as the index stabilization-sort
    // suppression below. Trailing ORDER BY terms are ignored: the rowid is
    // unique, so they can never affect the order (verified live: sqlite3
    // suppresses for `ORDER BY id, y` and `ORDER BY id ASC, y DESC`).
    //
    // This only applies when the planner picks NO index for the scan: if the
    // WHERE clause rides an index (SEARCH/skip-scan), rows arrive in index
    // order, not rowid order, and the sort is real. It also does NOT apply
    // to the bare `rowid` pseudo-column on tables WITHOUT an INTEGER PRIMARY
    // KEY: their sequential scan yields physical insertion order, which
    // explicit-rowid INSERTs and `UPDATE ... SET rowid = ...` can decouple
    // from rowid order, so that temp line stays truthful (documented
    // divergence from sqlite3).
    if order_by_leads_with_rowid_alias(&table.schema, table_name, order_by)
        && cost_based_index_selection(table_name, where_clause, Some(order_by), database).is_none()
        && !has_skip_scan_plan(table_name, where_clause, database)
    {
        return false;
    }

    eqp_ordering_index(table_name, where_clause, order_by, database, false).is_none()
}

/// True when the planner would choose a skip-scan for this table + WHERE
/// (mirrors the skip-scan fallback in `ExplainExecutor::explain_table_scan`).
fn has_skip_scan_plan(
    table_name: &str,
    where_clause: Option<&Expression>,
    database: &Database,
) -> bool {
    let Some(where_expr) = where_clause else {
        return false;
    };
    IndexPlanner::new(database).plan_skip_scan(table_name, where_expr).is_some()
}

/// EQP-only (#5375): true when the FIRST ORDER BY term is a column reference
/// to the table's INTEGER PRIMARY KEY rowid alias — either by the declared
/// column name, or via the `rowid`/`_rowid_`/`oid` pseudo-column keywords
/// (which resolve to the alias column; real columns shadow the keywords, and
/// WITHOUT ROWID tables have no rowid pseudo-column, #4953).
///
/// Direction is ignored: ASC is the scan's natural rowid order and DESC its
/// exact reverse (the same reverse-traversal convention as index DESC
/// suppression; sqlite3 shows a bare `SCAN t` for both). Trailing terms are
/// ignored because the rowid is unique.
fn order_by_leads_with_rowid_alias(
    schema: &TableSchema,
    table_name: &str,
    order_by: &[vibesql_ast::OrderByItem],
) -> bool {
    let Some(ipk_idx) = schema.rowid_alias_column else {
        return false;
    };
    let Some(ipk_col) = schema.columns.get(ipk_idx) else {
        return false;
    };
    let Some(first) = order_by.first() else {
        return false;
    };
    let Expression::ColumnRef(col_id) = &first.expr else {
        return false;
    };
    // Schema-qualified references are out of scope; a table qualifier must
    // match the scanned table.
    if col_id.schema_canonical().is_some() {
        return false;
    }
    if let Some(qualifier) = col_id.table_canonical() {
        if !qualifier.eq_ignore_ascii_case(table_name) {
            return false;
        }
    }

    let col_name = col_id.column_canonical();
    if col_name.eq_ignore_ascii_case(&ipk_col.name) {
        return true;
    }

    // rowid/_rowid_/oid keywords: only when no real column shadows the
    // keyword (real columns take precedence, matching the evaluator) and the
    // table is not WITHOUT ROWID (no rowid pseudo-column there).
    let is_rowid_keyword = col_name.eq_ignore_ascii_case("rowid")
        || col_name.eq_ignore_ascii_case("_rowid_")
        || col_name.eq_ignore_ascii_case("oid");
    is_rowid_keyword && !schema.without_rowid && schema.get_column_index(col_name).is_none()
}

/// EQP-only: the index whose natural traversal delivers `order_by` for a
/// scan of `table_name`, if any.
///
/// Checks, in order:
/// 1. The planner's chosen index (`cost_based_index_selection`): when it
///    returns full `sorted_columns` the order is satisfied outright; even
///    when the runtime nullable-column guard withholds `sorted_columns`, a
///    structural match means the scan still delivers index order (e.g.
///    windowpushd.test 2.1.3.4: `SEARCH t1 USING INDEX i2 (b>?)` feeds
///    PARTITION BY b — SQLite shows no temp B-tree).
/// 2. Any other index with its leading columns pinned by equality/IN whose
///    remaining columns structurally align with the ORDER BY.
///
/// With a WHERE clause, no leading pinned column, and a competing access
/// path chosen by the planner, an unpinned structural match does NOT count
/// (the planner scans a different index, so ordering is not delivered).
/// `prefer_ordering_scan` relaxes that gate when the planner chose NO index:
/// the scan is then free to traverse the ordering index — SQLite picks the
/// index that delivers a window's PARTITION BY/ORDER BY order even without
/// any usable predicate (windowpushd.test 2.1.1.5, 2.1.3.6).
pub(crate) fn eqp_ordering_index(
    table_name: &str,
    where_clause: Option<&Expression>,
    order_by: &[vibesql_ast::OrderByItem],
    database: &Database,
    prefer_ordering_scan: bool,
) -> Option<String> {
    if order_by.is_empty() {
        return None;
    }

    let chosen = cost_based_index_selection(table_name, where_clause, Some(order_by), database);

    // The planner's chosen index delivers index order when the ORDER BY
    // structurally fits (pinned-prefix + trailing-index suffix), regardless
    // of the runtime nullable-column guard.
    if let Some((chosen_name, _)) = &chosen {
        if let Some(index_metadata) = database.get_index(chosen_name) {
            let pinned_columns = count_pinned_index_columns(where_clause, &index_metadata.columns);
            if can_use_index_for_order_by_with_pinned(
                order_by,
                &index_metadata.columns,
                pinned_columns,
            ) {
                return Some(chosen_name.clone());
            }
        }
    }

    // Fallback: check whether ANY index has its leading columns pinned by
    // equality/IN AND the ORDER BY structurally aligns with the index. If so,
    // SQLite's EQP would omit `USE TEMP B-TREE FOR ORDER BY`. The runtime
    // correctness of NULL placement is handled by the post-scan sort.
    let indexes = database.list_indexes_for_table(table_name);
    for index_name in &indexes {
        let Some(index_metadata) = database.get_index(index_name) else { continue };
        // An expression index reloaded with an empty, not-yet-rebuilt body
        // cannot satisfy ORDER BY either — skip it until it is rebuilt (#5784).
        if database.is_index_pending_rebuild(index_name) {
            continue;
        }
        // Partial indexes participate in the EQP exemption only when the
        // query's WHERE clause structurally implies the index predicate —
        // otherwise the index cannot be relied on to satisfy ORDER BY.
        if !crate::optimizer::predicate_implication::partial_index_usable(
            database,
            index_name,
            where_clause,
        ) {
            continue;
        }
        if index_metadata.columns.is_empty() {
            continue;
        }
        let pinned_columns = count_pinned_index_columns(where_clause, &index_metadata.columns);
        // With a WHERE clause and no leading pinned column, the planner may
        // choose a different (filtering) index, so the EQP exception only
        // applies when leading columns are pinned. Without a WHERE clause
        // there is no competing access path: a structural prefix match alone
        // means the index's natural traversal yields the requested order
        // (e.g. window1.test 23.1: index t5ab(a, b) serves key (a, b) with
        // no predicate), so SQLite's EQP shows no temp B-tree even when the
        // columns are nullable and a runtime stabilization sort still runs.
        // `prefer_ordering_scan` extends the no-competing-path exception to
        // WHERE clauses the planner could not use any index for.
        if pinned_columns == 0
            && where_clause.is_some()
            && !(prefer_ordering_scan && chosen.is_none())
        {
            continue; // No leading pin → no EQP exception applies.
        }

        // ORDER BY must structurally fit (pinned-prefix + trailing-index suffix).
        if can_use_index_for_order_by_with_pinned(order_by, &index_metadata.columns, pinned_columns)
        {
            return Some(index_name.clone());
        }
    }

    None
}

/// Check if an index can be used for ORDER BY, accounting for pinned prefix columns
///
/// When a query has equality predicates on leading index columns (e.g., WHERE a = 1 AND b = 2),
/// those columns are "pinned" and the index is effectively sorted by the remaining columns.
/// This function skips over the pinned columns and checks if the ORDER BY matches.
///
/// **Pinned ORDER BY items**: ORDER BY can also include the pinned columns themselves —
/// e.g., `WHERE a IN (1,2,3) ORDER BY a, b`. Within each value of the IN-list, the
/// remaining index columns are sorted; the pinned ORDER BY items are constants within
/// the scan group, so they sort trivially. We skip leading ORDER BY items whose column
/// matches one of the pinned leading index columns and only require the remaining
/// ORDER BY items to align with the post-pinned index suffix.
///
/// For example, with index (a, b, c):
/// - WHERE a = 1 ORDER BY b, c → can use index (skip 1 pinned column)
/// - WHERE a = 1 AND b = 2 ORDER BY c → can use index (skip 2 pinned columns)
/// - WHERE a = 1 ORDER BY a, b, c → can use index (a is pinned, skip from ORDER BY)
/// - WHERE a IN (1,2,3) ORDER BY a, b → can use index (a is pinned)
/// - WHERE a = 1 ORDER BY c → cannot use index (b must come before c)
pub(crate) fn can_use_index_for_order_by_with_pinned(
    order_items: &[vibesql_ast::OrderByItem],
    index_columns: &[vibesql_ast::IndexColumn],
    pinned_columns: usize,
) -> bool {
    // Skip pinned columns at the head of the index — they're constants within the scan.
    let remaining_index_columns = &index_columns[pinned_columns..];

    // Skip leading ORDER BY items that refer to one of the pinned index columns.
    // These are constants within each scan group, so the index trivially satisfies
    // their ordering. We accept any prefix of pinned-column references in any order
    // (since each is a single value within the group, their relative order is moot).
    let pinned_index_slice = &index_columns[..pinned_columns];
    let pinned_skip = order_items
        .iter()
        .take_while(|item| order_item_matches_pinned_column(item, pinned_index_slice))
        .count();
    let remaining_order_items = &order_items[pinned_skip..];

    // If every ORDER BY item was a pinned column reference, the index trivially
    // satisfies the ORDER BY (each scan group has one effective value per pinned col).
    if remaining_order_items.is_empty() {
        return !order_items.is_empty();
    }

    // ORDER BY must not have more columns than remaining index columns
    if remaining_order_items.len() > remaining_index_columns.len() {
        return false;
    }

    // Check if ORDER BY matches index direction or is completely reversed
    // (allowing reverse scan to satisfy DESC ordering)
    let mut all_match = true;
    let mut all_reversed = true;

    // Check each ORDER BY column against corresponding index column
    for (order_item, index_col) in remaining_order_items.iter().zip(remaining_index_columns.iter())
    {
        // For expression indexes, we need to check if ORDER BY uses the same expression
        // For column indexes, ORDER BY must be a simple column reference
        match index_col {
            vibesql_ast::IndexColumn::Column { column_name, .. } => {
                // ORDER BY expression must be a simple column reference
                let order_col_name = match &order_item.expr {
                    Expression::ColumnRef(col_id)
                        if col_id.schema_canonical().is_none()
                            && col_id.table_canonical().is_none() =>
                    {
                        col_id.column_canonical()
                    }
                    _ => return false, // Complex expressions not supported for column indexes
                };

                // Column names must match (case-insensitive due to SQL identifier normalization)
                if !order_col_name.eq_ignore_ascii_case(column_name) {
                    return false;
                }
            }
            vibesql_ast::IndexColumn::Expression { expr: index_expr, .. } => {
                // For expression indexes, ORDER BY must use the exact same expression
                // e.g., ORDER BY LOWER(name) can use index on LOWER(name)
                if !expressions_match(&order_item.expr, index_expr) {
                    return false;
                }
            }
        }

        // Check sort directions
        let directions_match = order_item.direction == index_col.direction();
        let directions_opposite = matches!(
            (&order_item.direction, &index_col.direction()),
            (vibesql_ast::OrderDirection::Asc, vibesql_ast::OrderDirection::Desc)
                | (vibesql_ast::OrderDirection::Desc, vibesql_ast::OrderDirection::Asc)
        );

        if !directions_match {
            all_match = false;
        }
        if !directions_opposite {
            all_reversed = false;
        }
    }

    // Accept if all directions match OR all are reversed (reverse scan)
    all_match || all_reversed
}

/// Count how many leading index columns are pinned by equality predicates
///
/// A column is "pinned" if there's an equality predicate (col = value) in the WHERE clause.
/// For expression indexes, an expression is "pinned" if there's an equality predicate
/// (expr = value) where expr matches the indexed expression.
/// Returns the number of consecutive leading index columns that are pinned.
pub(crate) fn count_pinned_index_columns(
    where_clause: Option<&Expression>,
    index_columns: &[vibesql_ast::IndexColumn],
) -> usize {
    let where_clause = match where_clause {
        Some(expr) => expr,
        None => return 0,
    };

    // Collect all columns that have equality predicates
    let mut pinned_columns = std::collections::HashSet::new();
    collect_equality_columns(where_clause, &mut pinned_columns);

    // Collect all expressions that have equality predicates (for expression indexes)
    let mut pinned_expressions: Vec<&Expression> = Vec::new();
    collect_equality_expressions(where_clause, &mut pinned_expressions);

    // Count consecutive leading index columns that are pinned
    let mut count = 0;
    for index_col in index_columns {
        let is_pinned = match index_col {
            vibesql_ast::IndexColumn::Column { column_name, .. } => {
                // Check if this column is pinned (case-insensitive match)
                pinned_columns.iter().any(|c| c.eq_ignore_ascii_case(column_name))
            }
            vibesql_ast::IndexColumn::Expression { expr: index_expr, .. } => {
                // Check if any pinned expression matches the indexed expression
                pinned_expressions.iter().any(|e| expressions_match(e, index_expr))
            }
        };

        if is_pinned {
            count += 1;
        } else {
            break; // Stop at first non-pinned column
        }
    }
    count
}

/// Count how many leading index columns are pinned to a **single** value by a
/// true equality predicate (`col = literal`), ignoring multi-valued IN pins.
///
/// This is the pin count that is meaningful for ORDER BY satisfaction: only a
/// column constrained to exactly one value is constant within the scan output
/// and may therefore be skipped when matching the ORDER BY against trailing
/// index columns. See [`collect_single_value_equality_columns`].
pub(crate) fn count_single_value_pinned_index_columns(
    where_clause: Option<&Expression>,
    index_columns: &[vibesql_ast::IndexColumn],
) -> usize {
    let where_clause = match where_clause {
        Some(expr) => expr,
        None => return 0,
    };

    let mut pinned_columns = std::collections::HashSet::new();
    collect_single_value_equality_columns(where_clause, &mut pinned_columns);

    let mut pinned_expressions: Vec<&Expression> = Vec::new();
    collect_single_value_equality_expressions(where_clause, &mut pinned_expressions);

    let mut count = 0;
    for index_col in index_columns {
        let is_pinned = match index_col {
            vibesql_ast::IndexColumn::Column { column_name, .. } => {
                pinned_columns.iter().any(|c| c.eq_ignore_ascii_case(column_name))
            }
            vibesql_ast::IndexColumn::Expression { expr: index_expr, .. } => {
                pinned_expressions.iter().any(|e| expressions_match(e, index_expr))
            }
        };

        if is_pinned {
            count += 1;
        } else {
            break;
        }
    }
    count
}

/// A column constrained to a single value by a WHERE equality, together with the
/// collation that governed the comparison. Used by the DISTINCT EQP suppression
/// (orderby5): a `SELECT DISTINCT` key column may be dropped from the distinctness
/// key when WHERE constrains it to one value — but only when the collation of the
/// WHERE comparison matches the collation under which the DISTINCT considers it,
/// since `a = 'xyz' COLLATE nocase` does not make BINARY-collated `a` constant
/// (orderby5 1.2.2 / 1.2.3).
///
/// `collation` is the lowercased collation name, or `None` for the default
/// (BINARY) collation when no explicit `COLLATE` appears on the predicate.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EqualityPinnedColumn {
    /// Uppercased column name.
    pub column: String,
    /// Lowercased explicit collation, or `None` for default (BINARY).
    pub collation: Option<String>,
}

/// Collect the columns constrained to a single value by top-level `col = literal`
/// equalities in the WHERE clause, recording the collation that governed each
/// comparison.
///
/// Only bare `col = literal` / `literal = col` (and the AND/Conjunction nesting of
/// such) are recognized. A `COLLATE` wrapper on the column side is captured as the
/// pin's collation; a `COLLATE` on the literal side is recorded as the comparison
/// collation as well (SQLite applies the explicit collation of either operand to
/// the comparison). Anything else — IN-lists, `+a=0`, function calls — does not
/// produce a single-value, collation-known pin and is skipped, which keeps the
/// `+a=0` (orderby5 1.7) and IN cases conservative.
pub(crate) fn collect_equality_pinned_columns_with_collation(
    where_clause: Option<&Expression>,
) -> Vec<EqualityPinnedColumn> {
    let mut out = Vec::new();
    if let Some(expr) = where_clause {
        collect_equality_pinned_with_collation_inner(expr, &mut out);
    }
    out
}

/// If `expr` is a bare column reference, optionally wrapped in a single `COLLATE`,
/// return `(uppercased column, explicit collation lowercased or None)`.
fn column_with_collation(expr: &Expression) -> Option<(String, Option<String>)> {
    match expr {
        Expression::ColumnRef(col_id) => Some((col_id.column_canonical().to_uppercase(), None)),
        Expression::Collate { expr: inner, collation } => {
            if let Expression::ColumnRef(col_id) = &**inner {
                Some((col_id.column_canonical().to_uppercase(), Some(collation.to_lowercase())))
            } else {
                None
            }
        }
        _ => None,
    }
}

/// If `expr` is a literal, optionally wrapped in a single `COLLATE`, return the
/// explicit collation (lowercased) the comparison would carry from that operand,
/// or `Some(None)` when it is a bare literal. Returns `None` when `expr` is not a
/// (possibly collated) literal.
fn literal_collation(expr: &Expression) -> Option<Option<String>> {
    match expr {
        _ if is_literal(expr) => Some(None),
        Expression::Collate { expr: inner, collation } if is_literal(inner) => {
            Some(Some(collation.to_lowercase()))
        }
        _ => None,
    }
}

fn collect_equality_pinned_with_collation_inner(
    expr: &Expression,
    out: &mut Vec<EqualityPinnedColumn>,
) {
    match expr {
        Expression::BinaryOp { left, op: vibesql_ast::BinaryOperator::Equal, right } => {
            // Determine the (column, column-side collation) and the literal-side
            // collation, in whichever order the operands appear.
            let pair = column_with_collation(left)
                .zip(literal_collation(right))
                .or_else(|| column_with_collation(right).zip(literal_collation(left)));
            if let Some(((column, col_coll), lit_coll)) = pair {
                // SQLite resolves the comparison collation from an explicit
                // COLLATE on either operand (column side takes precedence). The
                // pin only makes the column constant under that collation.
                let collation = col_coll.or(lit_coll);
                out.push(EqualityPinnedColumn { column, collation });
            }
        }
        Expression::BinaryOp { left, op: vibesql_ast::BinaryOperator::And, right } => {
            collect_equality_pinned_with_collation_inner(left, out);
            collect_equality_pinned_with_collation_inner(right, out);
        }
        Expression::Conjunction(exprs) => {
            for e in exprs {
                collect_equality_pinned_with_collation_inner(e, out);
            }
        }
        _ => {}
    }
}

/// Collect all column names that have equality predicates (col = literal or col IS literal)
///
/// Also recognizes positive IN-list (`col IN (literal-list)`) and IN-subquery
/// (`col IN (SELECT ...)`) as pinning the column. SQLite treats both as equality
/// predicates for index-selection purposes — they restrict the column to a finite
/// set of values, which is sufficient for the planner to skip the column when
/// matching ORDER BY against trailing index columns.
///
/// Negated IN (`NOT IN`) is NOT pinning. For an IN-list, all values must be
/// literals (or parameter placeholders) — a column reference inside the list
/// would make the IN equivalent to a join condition, not an equality.
fn collect_equality_columns(expr: &Expression, columns: &mut std::collections::HashSet<String>) {
    collect_pinned_columns(expr, columns, true);
}

/// Like `collect_equality_columns`, but only collects columns pinned to a
/// **single** value by a true equality (`col = literal` / `col IS literal`).
///
/// IN-lists and IN-subqueries pin a column to a *finite set* of values, which
/// is enough to drive an index seek but does NOT make the column constant
/// within the scan output. For ORDER BY satisfaction a multi-valued pin cannot
/// be treated as a constant: scanning `(x, y)` for `x IN (1, 5)` yields the
/// y-runs for x=1 and x=5 concatenated, which is not globally ordered by y, and
/// (for `ORDER BY x DESC, y`) cannot be produced by any single forward/reverse
/// index traversal. Excluding IN-pins here forces a post-scan sort in those
/// cases (where-5.102 / where-5.103).
fn collect_single_value_equality_columns(
    expr: &Expression,
    columns: &mut std::collections::HashSet<String>,
) {
    collect_pinned_columns(expr, columns, false);
}

/// Shared traversal for [`collect_equality_columns`] and
/// [`collect_single_value_equality_columns`].
///
/// When `include_in_lists` is true, positive `col IN (...)` predicates pin the
/// column (finite-set equality, used for index *seeking*). When false, only
/// single-value equalities pin the column (used for ORDER BY satisfaction).
fn collect_pinned_columns(
    expr: &Expression,
    columns: &mut std::collections::HashSet<String>,
    include_in_lists: bool,
) {
    match expr {
        Expression::BinaryOp { left, op, right } => {
            match op {
                vibesql_ast::BinaryOperator::Equal => {
                    // Check for column = literal pattern
                    if let Expression::ColumnRef(col_id) = &**left {
                        if is_literal(right) {
                            columns.insert(col_id.column_canonical().to_uppercase());
                        }
                    }
                    if let Expression::ColumnRef(col_id) = &**right {
                        if is_literal(left) {
                            columns.insert(col_id.column_canonical().to_uppercase());
                        }
                    }
                }
                vibesql_ast::BinaryOperator::And => {
                    // Recurse into both sides of AND
                    collect_pinned_columns(left, columns, include_in_lists);
                    collect_pinned_columns(right, columns, include_in_lists);
                }
                _ => {}
            }
        }
        // Handle IS (NULL-safe equals): negated=true means "IS NOT DISTINCT FROM" = "IS"
        Expression::IsDistinctFrom { left, right, negated: true } => {
            // Check for column IS literal pattern
            if let Expression::ColumnRef(col_id) = &**left {
                if is_literal(right) {
                    columns.insert(col_id.column_canonical().to_uppercase());
                }
            }
            if let Expression::ColumnRef(col_id) = &**right {
                if is_literal(left) {
                    columns.insert(col_id.column_canonical().to_uppercase());
                }
            }
        }
        // IN-list with literal values: `col IN (1, 2, 3)` pins the column.
        // Empty lists, NOT IN, and lists containing non-literal expressions are
        // excluded since they don't behave as a finite-set equality.
        Expression::InList { expr: target, values, negated: false } if include_in_lists => {
            if let Expression::ColumnRef(col_id) = &**target {
                if !values.is_empty() && values.iter().all(is_literal) {
                    columns.insert(col_id.column_canonical().to_uppercase());
                }
            }
        }
        // IN-subquery: `col IN (SELECT ...)` pins the column. The subquery is
        // resolved at execution time to a finite set of values, equivalent for
        // index-selection purposes to an IN-list.
        Expression::In { expr: target, negated: false, .. } if include_in_lists => {
            if let Expression::ColumnRef(col_id) = &**target {
                columns.insert(col_id.column_canonical().to_uppercase());
            }
        }
        // Recurse into Conjunction (AND)
        Expression::Conjunction(exprs) => {
            for e in exprs {
                collect_pinned_columns(e, columns, include_in_lists);
            }
        }
        _ => {}
    }
}

/// Collect all expressions that have equality predicates (expr = literal)
///
/// This is used for expression index support. It collects non-column expressions
/// that are compared to literals in equality predicates.
///
/// Like `collect_equality_columns`, this also treats positive `expr IN (literal-list)`
/// and `expr IN (SELECT ...)` as pinning the expression for matching against
/// expression-index columns.
fn collect_equality_expressions<'a>(expr: &'a Expression, expressions: &mut Vec<&'a Expression>) {
    collect_pinned_expressions(expr, expressions, true);
}

/// Like `collect_equality_expressions`, but only collects expressions pinned to
/// a single value by a true equality. See
/// [`collect_single_value_equality_columns`] for why IN-pins are excluded when
/// reasoning about ORDER BY satisfaction.
fn collect_single_value_equality_expressions<'a>(
    expr: &'a Expression,
    expressions: &mut Vec<&'a Expression>,
) {
    collect_pinned_expressions(expr, expressions, false);
}

/// Shared traversal for [`collect_equality_expressions`] and
/// [`collect_single_value_equality_expressions`].
fn collect_pinned_expressions<'a>(
    expr: &'a Expression,
    expressions: &mut Vec<&'a Expression>,
    include_in_lists: bool,
) {
    match expr {
        Expression::BinaryOp { left, op, right } => {
            match op {
                vibesql_ast::BinaryOperator::Equal => {
                    // Check for expr = literal pattern (where expr is not a simple column)
                    if is_literal(right) && !matches!(&**left, Expression::ColumnRef(_)) {
                        expressions.push(left);
                    }
                    if is_literal(left) && !matches!(&**right, Expression::ColumnRef(_)) {
                        expressions.push(right);
                    }
                }
                vibesql_ast::BinaryOperator::And => {
                    // Recurse into both sides of AND
                    collect_pinned_expressions(left, expressions, include_in_lists);
                    collect_pinned_expressions(right, expressions, include_in_lists);
                }
                _ => {}
            }
        }
        // Handle IS (NULL-safe equals): negated=true means "IS NOT DISTINCT FROM" = "IS"
        Expression::IsDistinctFrom { left, right, negated: true } => {
            // Check for expr IS literal pattern
            if is_literal(right) && !matches!(&**left, Expression::ColumnRef(_)) {
                expressions.push(left);
            }
            if is_literal(left) && !matches!(&**right, Expression::ColumnRef(_)) {
                expressions.push(right);
            }
        }
        // IN-list with literal values: `expr IN (1, 2, 3)` pins the expression.
        // Restricted to non-column expressions (column equality is collected via
        // `collect_equality_columns`) and lists where every element is a literal.
        Expression::InList { expr: target, values, negated: false } if include_in_lists => {
            if !matches!(&**target, Expression::ColumnRef(_))
                && !values.is_empty()
                && values.iter().all(is_literal)
            {
                expressions.push(target);
            }
        }
        // IN-subquery: `expr IN (SELECT ...)` pins the expression for non-column targets.
        Expression::In { expr: target, negated: false, .. } if include_in_lists => {
            if !matches!(&**target, Expression::ColumnRef(_)) {
                expressions.push(target);
            }
        }
        // Recurse into Conjunction (AND)
        Expression::Conjunction(exprs) => {
            for e in exprs {
                collect_pinned_expressions(e, expressions, include_in_lists);
            }
        }
        _ => {}
    }
}

/// Cost-based index selection using statistics
///
/// This function uses table and column statistics to make intelligent decisions
/// about whether to use an index scan or a table scan. It estimates the cost of
/// both access methods and chooses the cheaper one.
///
/// # Arguments
/// * `table_name` - Name of the table being queried
/// * `where_clause` - Optional WHERE clause predicate
/// * `order_by` - Optional ORDER BY clause
/// * `database` - Database reference for accessing statistics and indexes
///
/// # Returns
/// - `Some((index_name, sorted_columns))` if cost-based analysis suggests using an index
/// - `None` if table scan is cheaper or no statistics are available
///
/// # Fallback Behavior
/// If statistics are not available or stale, falls back to rule-based selection
/// using `should_use_index_scan()`.
#[allow(clippy::type_complexity)]
pub(crate) fn cost_based_index_selection(
    table_name: &str,
    where_clause: Option<&Expression>,
    order_by: Option<&[vibesql_ast::OrderByItem]>,
    database: &Database,
) -> Option<(String, Option<Vec<(String, vibesql_ast::OrderDirection)>>)> {
    // Get table statistics
    let table = database.get_table(table_name)?;
    let table_stats = table.get_statistics();

    // If no statistics or stale, fall back to rule-based selection
    if table_stats.is_none() || table_stats.as_ref().map(|s| s.needs_refresh()).unwrap_or(false) {
        return should_use_index_scan(table_name, where_clause, order_by, database);
    }

    let table_stats = table_stats.unwrap();
    let cost_estimator = CostEstimator::default();

    // Get all indexes for this table
    let indexes = database.list_indexes_for_table(table_name);
    if indexes.is_empty() {
        return None; // No indexes available
    }

    // Try each index and find the one with best score (pinned columns + cost)
    #[allow(clippy::type_complexity)]
    let mut best_index: Option<(
        String,
        AccessMethod,
        usize,
        bool,
        Option<Vec<(String, vibesql_ast::OrderDirection)>>,
    )> = None;
    // (index_name, access_method, pinned_count, top_level_seekable, sorted_columns)
    let mut has_applicable_index_without_stats = false;

    for index_name in &indexes {
        if let Some(index_metadata) = database.get_index(index_name) {
            // Expression indexes reloaded from a snapshot with an empty,
            // not-yet-rebuilt body must not be consulted for reads — their body
            // would silently return zero rows. Decline them here so selection
            // falls back to a full-table scan (correct results). The executor's
            // `rebuild_pending_expression_indexes` repopulates the body on load,
            // after which this guard no longer trips. See issue #5784.
            if database.is_index_pending_rebuild(index_name) {
                if std::env::var("INDEX_SELECT_DEBUG").is_ok() {
                    eprintln!(
                        "[INDEX_SELECT] skipping {} - expression index pending rebuild after reload",
                        index_name
                    );
                }
                continue;
            }

            // Partial indexes are usable only when the query WHERE clause
            // structurally implies the index predicate — see
            // `should_use_index_scan` for the full rationale.
            if !crate::optimizer::predicate_implication::partial_index_usable(
                database,
                index_name,
                where_clause,
            ) {
                if std::env::var("INDEX_SELECT_DEBUG").is_ok() {
                    eprintln!(
                        "[INDEX_SELECT] skipping {} - partial index predicate not implied by query WHERE",
                        index_name
                    );
                }
                continue;
            }

            let first_indexed_column = index_metadata.columns.first()?;

            // Check if this index can be used for WHERE or ORDER BY
            // Supports both column indexes and expression indexes
            let can_use_for_where = where_clause
                .map(|expr| index_column_can_filter(expr, first_indexed_column))
                .unwrap_or(false);

            // Count how many leading index columns are pinned by equality predicates
            let pinned_columns = count_pinned_index_columns(where_clause, &index_metadata.columns);

            // Get column name for stats lookup (only for column indexes)
            let column_name = first_indexed_column.column_name();

            // Debug: trace index selection
            if std::env::var("INDEX_SELECT_DEBUG").is_ok() {
                let col_debug = column_name.unwrap_or("<expression>");
                eprintln!(
                    "[INDEX_SELECT] table={}, index={}, first_col={}, can_use_for_where={}",
                    table_name, index_name, col_debug, can_use_for_where
                );
            }

            let can_use_for_order = if let Some(order_items) = order_by {
                // Check if ORDER BY columns match the index columns (after skipping pinned columns)
                let columns_match = can_use_index_for_order_by_with_pinned(
                    order_items,
                    &index_metadata.columns,
                    pinned_columns,
                );

                // Don't use index for ORDER BY if any non-pinned column is nullable.
                // See `any_order_by_column_nullable` for the pinned-column exception.
                if columns_match
                    && any_order_by_column_nullable(
                        order_items,
                        &table.schema,
                        &index_metadata.columns,
                        pinned_columns,
                    )
                {
                    false
                } else {
                    columns_match
                }
            } else {
                false
            };

            // Skip this index if it can't help with WHERE or ORDER BY
            if !can_use_for_where && !can_use_for_order {
                if std::env::var("INDEX_SELECT_DEBUG").is_ok() {
                    eprintln!(
                        "[INDEX_SELECT] skipping {} - can't use for where or order",
                        index_name
                    );
                }
                continue;
            }

            // Debug: continue trace
            if std::env::var("INDEX_SELECT_DEBUG").is_ok() {
                eprintln!(
                    "[INDEX_SELECT] {} passed where/order check, checking stats...",
                    index_name
                );
            }

            // Compute the per-index (selectivity, access_method).
            //
            // Expression indexes (`CREATE INDEX t1a1 ON t1(substr(a,1,12))`) carry
            // no per-column statistics, so they cannot be costed via the usual
            // column-histogram path. Rather than bail out of the cost comparison
            // entirely (which let a less-selective *column* index on the same
            // table win, and broke the `indexexpr1`/`indexexpr2` EQP parity
            // tests), cost them conservatively: assume the flat 0.33 selectivity
            // VibeSQL uses for predicates with no histogram and let the
            // expression index compete in the `best_index` comparison below. This
            // mirrors `estimate_selectivity`'s expression-index default.
            let (selectivity, access_method) = if first_indexed_column.is_expression() {
                let selectivity =
                    if where_clause.is_some() { EXPRESSION_INDEX_SELECTIVITY } else { 1.0 };
                let access_method =
                    cost_estimator.choose_access_method_no_col_stats(table_stats, selectivity);
                if std::env::var("INDEX_SELECT_DEBUG").is_ok() {
                    eprintln!(
                        "[INDEX_SELECT] {} is expression index, costed conservatively (selectivity={:.4})",
                        index_name, selectivity
                    );
                }
                // Still record that a stats-free applicable index exists so the
                // post-loop rule-based fallback fires if no index ends up chosen
                // (e.g. the conservative cost lost to a table scan).
                has_applicable_index_without_stats = true;
                (selectivity, access_method)
            } else {
                // Get column statistics for the indexed column (case-insensitive lookup)
                let col_stats = column_name
                    .and_then(|cn| get_column_stats_ignore_case(&table_stats.columns, cn));
                if col_stats.is_none() {
                    // Track that we found an applicable index without column stats
                    // We'll fall back to rule-based selection if cost-based fails
                    if std::env::var("INDEX_SELECT_DEBUG").is_ok() {
                        let col_debug = column_name.unwrap_or("<expression>");
                        eprintln!(
                            "[INDEX_SELECT] {} no column stats for {}, will fallback",
                            index_name, col_debug
                        );
                    }
                    has_applicable_index_without_stats = true;
                    continue; // No stats for this column, try next index
                }
                let col_stats = col_stats.unwrap();
                // At this point, column_name must be Some since we got col_stats
                let column_name = column_name.unwrap();

                if std::env::var("INDEX_SELECT_DEBUG").is_ok() {
                    eprintln!("[INDEX_SELECT] {} has column stats for {}", index_name, column_name);
                }

                // Estimate selectivity based on WHERE clause
                let selectivity = if let Some(where_expr) = where_clause {
                    estimate_selectivity(where_expr, column_name, col_stats)
                } else {
                    1.0 // No WHERE clause means all rows
                };

                // Use cost estimator to decide
                let access_method =
                    cost_estimator.choose_access_method(table_stats, Some(col_stats), selectivity);
                (selectivity, access_method)
            };

            if std::env::var("INDEX_SELECT_DEBUG").is_ok() {
                eprintln!(
                    "[INDEX_SELECT] {} selectivity={:.4}, access_method={:?}, is_index_scan={}",
                    index_name,
                    selectivity,
                    access_method,
                    access_method.is_index_scan()
                );
            }

            // Build sorted_columns metadata if ORDER BY can be satisfied
            let sorted_columns = if can_use_for_order {
                let order_items = order_by.unwrap();
                Some(
                    order_items
                        .iter()
                        .map(|item| {
                            // For expression indexes, ORDER BY may use expressions (e.g., length(a))
                            // We use to_sql() to convert the expression to a string representation
                            // The actual string is only used for metadata; the important part
                            // is the direction for determining scan order
                            let col_name = match &item.expr {
                                Expression::ColumnRef(col_id) => {
                                    col_id.column_canonical().to_string()
                                }
                                expr => expr.to_sql(),
                            };
                            (col_name, item.direction.clone())
                        })
                        .collect(),
                )
            } else {
                None
            };

            // Whether this index's leading column yields a real index *seek*
            // from a top-level AND conjunct (`SEARCH ... (col op ?)`) rather than
            // a bare `SCAN` (only OR-nested predicates). See
            // `index_leading_column_seekable_at_top_level`.
            let top_level_seekable =
                index_leading_column_seekable_at_top_level(where_clause, &index_metadata.columns);

            // Track the best index.
            // Tie-break order (deterministic — eliminates the HashMap-iteration
            // dependence behind the where9-5.3 non-determinism, #5660):
            //   1. leading column seekable at top level (SEARCH beats SCAN)
            //   2. more pinned columns (better filtering)
            //   3. lower estimated cost
            //   4. lexicographically smaller index name (stable final tie-break)
            //
            // Seekability leads because a pin that yields no real seek (an
            // `IN (SELECT ...)` subquery on the leading column counts as pinned
            // for cost yet the extractor produces no seek → EQP renders `SCAN`)
            // must not outrank a genuinely-seekable `SEARCH` competitor. Any
            // genuine equality/`IN`-list pin is itself top-level seekable, so
            // this only demotes the illusory-pin case. Mirrors the ordering in
            // `should_use_index_scan`.
            if access_method.is_index_scan() {
                let is_better = match &best_index {
                    None => true,
                    Some((best_name, best_method, best_pinned, best_seekable, _)) => {
                        if top_level_seekable != *best_seekable {
                            top_level_seekable && !*best_seekable
                        } else if pinned_columns != *best_pinned {
                            pinned_columns > *best_pinned
                        } else if access_method.cost() != best_method.cost() {
                            access_method.cost() < best_method.cost()
                        } else {
                            index_name.as_str() < best_name.as_str()
                        }
                    }
                };

                if is_better {
                    best_index = Some((
                        index_name.clone(),
                        access_method,
                        pinned_columns,
                        top_level_seekable,
                        sorted_columns,
                    ));
                }
            } else if selectivity < 0.40 && can_use_for_where {
                // Cost-based chose table scan, but selectivity is good enough for index
                // The cost model may be too conservative for in-memory/prefix scans
                // Common fallback values: 0.33 (single predicate), 0.1089 (two predicates)
                // Fall back to rule-based selection for selective queries
                if std::env::var("INDEX_SELECT_DEBUG").is_ok() {
                    eprintln!(
                        "[INDEX_SELECT] {} selectivity={:.4} good, falling back to rule-based",
                        index_name, selectivity
                    );
                }
                return should_use_index_scan(table_name, where_clause, order_by, database);
            }
        }
    }

    // Return the best index if we found one
    if let Some((index_name, _, _, _, sorted_columns)) = best_index {
        if std::env::var("INDEX_SELECT_DEBUG").is_ok() {
            eprintln!("[INDEX_SELECT] selected best_index={} for table={}", index_name, table_name);
        }
        return Some((index_name, sorted_columns));
    }

    // If we have applicable indexes but no column stats, fall back to rule-based selection
    // This ensures we use indexes even when statistics are incomplete
    if has_applicable_index_without_stats {
        if std::env::var("INDEX_SELECT_DEBUG").is_ok() {
            eprintln!("[INDEX_SELECT] falling back to rule-based for table={}", table_name);
        }
        return should_use_index_scan(table_name, where_clause, order_by, database);
    }

    // No applicable indexes found
    if std::env::var("INDEX_SELECT_DEBUG").is_ok() {
        eprintln!("[INDEX_SELECT] no index selected for table={}", table_name);
    }
    None
}

/// Estimate the cost of a single MULTI-INDEX OR *branch* as an index seek/scan.
///
/// This is the heart of the OR-aware cost model (epic #5668, PR 3). It costs one
/// OR branch (e.g. `c = 31031`, `d IS NULL`, `c >= 31031`) as the per-branch index
/// scan VibeSQL would run for it, using **real column statistics**. The returned
/// cost feeds [`multi_index_or_cost`], whose sum-over-branches decides whether the
/// union beats a single index or a full scan.
///
/// ## Why this distinguishes where9-5.1 from where9-5.3 honestly
///
/// The branch cost is `CostEstimator::estimate_index_scan(table_stats, col_stats,
/// selectivity)`, and `selectivity` is derived from the branch predicate:
/// - An **equality** branch (`c = 31031`) gets `estimate_eq_selectivity` ≈
///   `1/n_distinct` — a point seek matching very few rows → **cheap**.
/// - An **IS NULL** branch (`d IS NULL`) gets `null_count / row_count` — typically
///   a handful of rows → **cheap**.
/// - An **inequality/range** branch (`c >= 31031`) gets
///   `estimate_range_selectivity` — a range scan matching many rows → **expensive**.
///
/// So the equality-seek-vs-range-scan distinction the conformance gate demands
/// falls directly out of the selectivity, not out of any per-query special case.
///
/// Returns `None` if the branch is not costable with statistics (no index, no
/// column stats, expression index) — the caller then declines the MULTI-INDEX OR
/// plan and falls back to single-index / full-scan selection.
fn estimate_branch_index_cost(
    branch: &OrBranch,
    table_stats: &vibesql_storage::statistics::TableStatistics,
    cost_estimator: &CostEstimator,
    database: &Database,
) -> Option<f64> {
    let index_metadata = database.get_index(&branch.index_name)?;
    let first_indexed_column = index_metadata.columns.first()?;

    // Expression indexes have no column statistics — not costable here.
    if first_indexed_column.is_expression() {
        return None;
    }
    let column_name = first_indexed_column.column_name()?;
    let col_stats = get_column_stats_ignore_case(&table_stats.columns, column_name)?;

    // Selectivity is what makes equality seeks cheap and range scans expensive.
    // IS NULL branches are routed to the NULL-key seek and costed from the
    // measured null fraction; `estimate_selectivity` does not model IS NULL
    // (it falls through to a flat 0.33), which would wrongly make a NULL seek
    // look like a mid-range scan, so handle it explicitly here.
    let selectivity = match branch.kind {
        OrBranchKind::IsNull => {
            if table_stats.row_count > 0 {
                (col_stats.null_count as f64 / table_stats.row_count as f64).clamp(0.0, 1.0)
            } else {
                0.0
            }
        }
        OrBranchKind::Lookup => {
            estimate_selectivity(&branch.branch_predicate, column_name, col_stats)
        }
    };

    Some(cost_estimator.estimate_index_scan(table_stats, col_stats, selectivity))
}

/// Whether an OR branch is a cheap **point seek** — an equality (`col = ?`) or an
/// `IS NULL` seek — as opposed to a range scan (`>`, `>=`, `<`, `<=`, `BETWEEN`)
/// that reads many rows.
///
/// Used by the no-statistics rule-based MULTI-INDEX OR heuristic in
/// [`select_or_aware_index_method`]: SQLite's default optimizer (no ANALYZE)
/// chooses a MULTI-INDEX OR union only when every branch is a point seek, because
/// a union of cheap seeks beats a single scan, whereas a range OR-branch is
/// cheaper to handle via a single index on the AND-clause. This is the structural
/// form of the equality-seek-vs-range-scan cost signal that, with statistics,
/// [`multi_index_or_cost`] computes numerically.
fn branch_is_point_seek(branch: &OrBranch) -> bool {
    match branch.kind {
        // `col IS NULL` seeks a single NULL key — a point seek.
        OrBranchKind::IsNull => true,
        OrBranchKind::Lookup => matches!(
            &branch.branch_predicate,
            Expression::BinaryOp { op: vibesql_ast::BinaryOperator::Equal, .. }
        ),
    }
}

/// Whether the residual AND-clause contains a top-level **equality** predicate
/// (`col = <literal>`) on a column that is the leading column of some index on
/// `table_name`.
///
/// Used by the no-statistics MULTI-INDEX OR heuristic: when the AND-clause around
/// an OR has an indexable equality (e.g. where9-5.2 `b=1000`), SQLite prefers that
/// single equality seek over the OR-union, so the union is declined.
fn residual_has_equality_index(
    table_name: &str,
    residual: &Expression,
    database: &Database,
) -> bool {
    // Flatten top-level AND-conjuncts (the residual may be a single predicate or
    // a conjunction of several).
    let conjuncts: Vec<&Expression> = match residual {
        Expression::Conjunction(exprs) => exprs.iter().collect(),
        Expression::BinaryOp { op: vibesql_ast::BinaryOperator::And, left, right } => {
            vec![left.as_ref(), right.as_ref()]
        }
        other => vec![other],
    };

    for conjunct in conjuncts {
        if let Expression::BinaryOp { op: vibesql_ast::BinaryOperator::Equal, left, right } =
            conjunct
        {
            // Accept `col = literal` or `literal = col`.
            let col = match (left.as_ref(), right.as_ref()) {
                (Expression::ColumnRef(c), _) => Some(c.column_canonical()),
                (_, Expression::ColumnRef(c)) => Some(c.column_canonical()),
                _ => None,
            };
            if let Some(col) = col {
                for index_name in database.list_indexes_for_table(table_name) {
                    if let Some(meta) = database.get_index(&index_name) {
                        if let Some(first) = meta.columns.first() {
                            if let Some(name) = first.column_name() {
                                if name.eq_ignore_ascii_case(col) {
                                    return true;
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    false
}

/// Total estimated cost of executing a MULTI-INDEX OR plan.
///
/// Cost = (sum over branches of per-branch index-seek/scan cost) + a dedup/fetch
/// overhead proportional to the total rows the branches produce. The dedup term
/// charges the union for materializing and de-duplicating each branch's rowids
/// before fetching — without it the model would treat a union of N branches as
/// free coordination, biasing toward MULTI-INDEX OR. The overhead is deliberately
/// modest (CPU-tuple cost per produced rowid) so it never dominates the seek
/// costs; its role is to break ties against an equivalently-selective single
/// index, matching SQLite's mild preference for a single B-tree search.
///
/// Returns `None` if **any** branch is not costable with statistics — a partial
/// cost would be meaningless, so the whole MULTI-INDEX OR alternative is declined.
fn multi_index_or_cost(
    branches: &[OrBranch],
    table_stats: &vibesql_storage::statistics::TableStatistics,
    cost_estimator: &CostEstimator,
    database: &Database,
) -> Option<f64> {
    let mut total = 0.0;
    let mut estimated_rows = 0.0;
    for branch in branches {
        let branch_cost =
            estimate_branch_index_cost(branch, table_stats, cost_estimator, database)?;
        total += branch_cost;
        // Recover the branch's row estimate to charge dedup/fetch overhead. The
        // index-scan cost already includes a fetch term; this adds the union's
        // bookkeeping (collect + dedup rowids) on top.
        if let Some(index_metadata) = database.get_index(&branch.index_name) {
            if let Some(col) = index_metadata.columns.first() {
                if let Some(col_name) = col.column_name() {
                    if let Some(col_stats) =
                        get_column_stats_ignore_case(&table_stats.columns, col_name)
                    {
                        let selectivity = match branch.kind {
                            OrBranchKind::IsNull => {
                                if table_stats.row_count > 0 {
                                    col_stats.null_count as f64 / table_stats.row_count as f64
                                } else {
                                    0.0
                                }
                            }
                            OrBranchKind::Lookup => {
                                estimate_selectivity(&branch.branch_predicate, col_name, col_stats)
                            }
                        };
                        estimated_rows += table_stats.row_count as f64 * selectivity;
                    }
                }
            }
        }
    }

    // Dedup/fetch overhead: one CPU-tuple cost per rowid produced across branches.
    total += estimated_rows * cost_estimator.cpu_tuple_cost;
    Some(total)
}

/// Estimate the cost of the best *single index* over the full WHERE clause, with
/// the OR (if any) applied as a residual filter. This is alternative (2) of the
/// OR-aware cost model — the competing plan that wins where9-5.3 (a single range
/// search on the AND-clause `b > 1000` via index `t1b`).
///
/// Returns `(index_name, sorted_columns, cost)` for the cheapest applicable index,
/// or `None` when no index applies or statistics are unavailable. The selection
/// mirrors [`cost_based_index_selection`]'s scoring (pinned columns then cost) but
/// also surfaces the winning index's estimated cost so it can be compared against
/// the MULTI-INDEX OR and full-scan alternatives.
#[allow(clippy::type_complexity)]
fn best_single_index_cost(
    table_name: &str,
    where_clause: Option<&Expression>,
    order_by: Option<&[vibesql_ast::OrderByItem]>,
    table_stats: &vibesql_storage::statistics::TableStatistics,
    cost_estimator: &CostEstimator,
    database: &Database,
) -> Option<(String, Option<Vec<(String, vibesql_ast::OrderDirection)>>, f64)> {
    let indexes = database.list_indexes_for_table(table_name);
    if indexes.is_empty() {
        return None;
    }

    let mut best: Option<(String, Option<Vec<(String, vibesql_ast::OrderDirection)>>, usize, f64)> =
        None;

    for index_name in &indexes {
        let Some(index_metadata) = database.get_index(index_name) else { continue };

        // Skip expression indexes reloaded with an empty, not-yet-rebuilt body
        // (they would silently return zero rows). See issue #5784.
        if database.is_index_pending_rebuild(index_name) {
            continue;
        }

        if !crate::optimizer::predicate_implication::partial_index_usable(
            database,
            index_name,
            where_clause,
        ) {
            continue;
        }

        let Some(first_indexed_column) = index_metadata.columns.first() else { continue };

        let can_use_for_where = where_clause
            .map(|expr| index_column_can_filter(expr, first_indexed_column))
            .unwrap_or(false);
        let pinned_columns = count_pinned_index_columns(where_clause, &index_metadata.columns);

        let can_use_for_order = if let Some(order_items) = order_by {
            let columns_match = can_use_index_for_order_by_with_pinned(
                order_items,
                &index_metadata.columns,
                pinned_columns,
            );
            if columns_match {
                let table = database.get_table(table_name)?;
                !any_order_by_column_nullable(
                    order_items,
                    &table.schema,
                    &index_metadata.columns,
                    pinned_columns,
                )
            } else {
                false
            }
        } else {
            false
        };

        if !can_use_for_where && !can_use_for_order {
            continue;
        }

        // Expression indexes / indexes without column stats are not costable here.
        if first_indexed_column.is_expression() {
            continue;
        }
        let Some(column_name) = first_indexed_column.column_name() else { continue };
        let Some(col_stats) = get_column_stats_ignore_case(&table_stats.columns, column_name)
        else {
            continue;
        };

        let selectivity = if let Some(where_expr) = where_clause {
            estimate_selectivity(where_expr, column_name, col_stats)
        } else {
            1.0
        };

        let cost = cost_estimator.estimate_index_scan(table_stats, col_stats, selectivity);

        let sorted_columns = if can_use_for_order {
            let order_items = order_by.unwrap();
            Some(
                order_items
                    .iter()
                    .map(|item| {
                        let col_name = match &item.expr {
                            Expression::ColumnRef(col_id) => col_id.column_canonical().to_string(),
                            expr => expr.to_sql(),
                        };
                        (col_name, item.direction.clone())
                    })
                    .collect(),
            )
        } else {
            None
        };

        let is_better = match &best {
            None => true,
            Some((_, _, best_pinned, best_cost)) => {
                if pinned_columns > *best_pinned {
                    true
                } else if pinned_columns == *best_pinned {
                    cost < *best_cost
                } else {
                    false
                }
            }
        };
        if is_better {
            best = Some((index_name.clone(), sorted_columns, pinned_columns, cost));
        }
    }

    best.map(|(name, sorted, _pinned, cost)| (name, sorted, cost))
}

/// Estimate selectivity of a predicate on a specific column
///
/// Uses column statistics to estimate what fraction of rows will match the predicate.
/// Returns a value between 0.0 (no rows) and 1.0 (all rows).
pub(crate) fn estimate_selectivity(
    expr: &Expression,
    column_name: &str,
    col_stats: &vibesql_storage::statistics::ColumnStatistics,
) -> f64 {
    match expr {
        Expression::BinaryOp { left, op, right } => {
            match op {
                vibesql_ast::BinaryOperator::Equal => {
                    // Check if this is a predicate on our column (case-insensitive)
                    // For literal values, use actual statistics
                    if let (Expression::ColumnRef(col_id), Expression::Literal(value)) =
                        (&**left, &**right)
                    {
                        if col_id.column_canonical().eq_ignore_ascii_case(column_name) {
                            return col_stats.estimate_eq_selectivity(value);
                        }
                    }
                    if let (Expression::Literal(value), Expression::ColumnRef(col_id)) =
                        (&**left, &**right)
                    {
                        if col_id.column_canonical().eq_ignore_ascii_case(column_name) {
                            return col_stats.estimate_eq_selectivity(value);
                        }
                    }
                    // For placeholder parameters, estimate using 1/n_distinct
                    // This is more accurate than the generic 0.33 fallback for equality predicates
                    let left_is_col = is_column_reference(left, column_name);
                    let right_is_col = is_column_reference(right, column_name);
                    let left_is_lit = is_literal(left);
                    let right_is_lit = is_literal(right);

                    if (left_is_col && right_is_lit) || (left_is_lit && right_is_col) {
                        // Use 1/n_distinct as selectivity estimate for equality with parameter
                        if col_stats.n_distinct > 0 {
                            return 1.0 / col_stats.n_distinct as f64;
                        }
                    }
                    0.33 // Default fallback
                }
                vibesql_ast::BinaryOperator::GreaterThan
                | vibesql_ast::BinaryOperator::GreaterThanOrEqual
                | vibesql_ast::BinaryOperator::LessThan
                | vibesql_ast::BinaryOperator::LessThanOrEqual => {
                    // Range predicates (case-insensitive column comparison)
                    if let (Expression::ColumnRef(col_id), Expression::Literal(value)) =
                        (&**left, &**right)
                    {
                        if col_id.column_canonical().eq_ignore_ascii_case(column_name) {
                            let op_str = match op {
                                vibesql_ast::BinaryOperator::GreaterThan => ">",
                                vibesql_ast::BinaryOperator::GreaterThanOrEqual => ">=",
                                vibesql_ast::BinaryOperator::LessThan => "<",
                                vibesql_ast::BinaryOperator::LessThanOrEqual => "<=",
                                _ => unreachable!(),
                            };
                            return col_stats.estimate_range_selectivity(value, op_str);
                        }
                    }
                    // For placeholder parameters, use a conservative 0.25 (assume filtering ~75% of
                    // rows)
                    if (is_column_reference(left, column_name) && is_literal(right))
                        || (is_literal(left) && is_column_reference(right, column_name))
                    {
                        return 0.25;
                    }
                    0.33 // Default fallback
                }
                vibesql_ast::BinaryOperator::And => {
                    // For AND, multiply selectivities (assuming independence)
                    let left_sel = estimate_selectivity(left, column_name, col_stats);
                    let right_sel = estimate_selectivity(right, column_name, col_stats);
                    left_sel * right_sel
                }
                vibesql_ast::BinaryOperator::Or => {
                    // For OR, use formula: P(A OR B) = P(A) + P(B) - P(A AND B)
                    // Assuming independence: P(A OR B) = P(A) + P(B) - P(A)*P(B)
                    let left_sel = estimate_selectivity(left, column_name, col_stats);
                    let right_sel = estimate_selectivity(right, column_name, col_stats);
                    left_sel + right_sel - (left_sel * right_sel)
                }
                _ => 0.33, // Default fallback for other operators
            }
        }
        Expression::Between { expr, low, high, negated: _, symmetric: _ } => {
            if let Expression::ColumnRef(col_id) = &**expr {
                if col_id.column_canonical().eq_ignore_ascii_case(column_name) {
                    // Estimate BETWEEN as: P(col >= low AND col <= high)
                    if let (Expression::Literal(low_val), Expression::Literal(high_val)) =
                        (&**low, &**high)
                    {
                        let low_sel = col_stats.estimate_range_selectivity(low_val, ">=");
                        let high_sel = col_stats.estimate_range_selectivity(high_val, "<=");
                        return low_sel * high_sel; // Assuming independence
                    }
                }
            }
            0.33 // Default fallback
        }
        // IS (NULL-safe equals): negated=true means "IS NOT DISTINCT FROM" = "IS"
        // Treat IS like = for selectivity estimation
        Expression::IsDistinctFrom { left, right, negated: true } => {
            // Check if this is a predicate on our column (case-insensitive)
            // For literal values, use actual statistics
            if let (Expression::ColumnRef(col_id), Expression::Literal(value)) = (&**left, &**right)
            {
                if col_id.column_canonical().eq_ignore_ascii_case(column_name) {
                    return col_stats.estimate_eq_selectivity(value);
                }
            }
            if let (Expression::Literal(value), Expression::ColumnRef(col_id)) = (&**left, &**right)
            {
                if col_id.column_canonical().eq_ignore_ascii_case(column_name) {
                    return col_stats.estimate_eq_selectivity(value);
                }
            }
            // For placeholder parameters, estimate using 1/n_distinct
            let left_is_col = is_column_reference(left, column_name);
            let right_is_col = is_column_reference(right, column_name);
            let left_is_lit = is_literal(left);
            let right_is_lit = is_literal(right);

            if (left_is_col && right_is_lit) || (left_is_lit && right_is_col) {
                if col_stats.n_distinct > 0 {
                    return 1.0 / col_stats.n_distinct as f64;
                }
            }
            0.33 // Default fallback
        }
        // Handle Conjunction (AND) similarly to BinaryOp::And
        Expression::Conjunction(exprs) => {
            let mut selectivity = 1.0;
            for e in exprs {
                selectivity *= estimate_selectivity(e, column_name, col_stats);
            }
            selectivity
        }
        // Handle Disjunction (OR) similarly to BinaryOp::Or
        Expression::Disjunction(exprs) => {
            let mut selectivity = 0.0;
            let mut product = 1.0;
            for e in exprs {
                let sel = estimate_selectivity(e, column_name, col_stats);
                selectivity += sel;
                product *= sel;
            }
            // P(A OR B OR C) ≈ P(A) + P(B) + P(C) - P(A)*P(B)*P(C) (simplified)
            (selectivity - product).min(1.0)
        }
        _ => 0.33, // Default fallback for unsupported expressions
    }
}

/// Unified index selection that returns IndexScanChoice
///
/// This function first tries regular index selection, and if no suitable index is found,
/// it attempts skip-scan optimization as a fallback. Skip-scan enables using composite
/// indexes when the WHERE clause filters on non-prefix columns.
///
/// # Arguments
/// * `table_name` - Name of the table being queried
/// * `where_clause` - Optional WHERE clause predicate
/// * `order_by` - Optional ORDER BY clause
/// * `database` - Database reference for accessing statistics and indexes
///
/// # Returns
/// - `Some(IndexScanChoice::Regular {...})` if a regular index scan should be used
/// - `Some(IndexScanChoice::SkipScan {...})` if skip-scan is beneficial
/// - `None` if table scan is more appropriate
///
/// # Example
/// ```text
/// // Query: SELECT * FROM sales WHERE date = '2024-01-01'
/// // Index: (region, date) - a composite index
/// //
/// // Regular index selection fails (no filter on 'region' prefix column)
/// // Skip-scan is considered: iterate through distinct 'region' values,
/// // for each region, seek to entries with date = '2024-01-01'
/// //
/// // If skip-scan cost < table scan cost, returns IndexScanChoice::SkipScan
/// ```
pub(crate) fn select_index_scan_method(
    table_name: &str,
    where_clause: Option<&Expression>,
    order_by: Option<&[vibesql_ast::OrderByItem]>,
    database: &Database,
) -> Option<IndexScanChoice> {
    // OR-AWARE COST MODEL (epic #5668, PR 3). When the WHERE clause contains a
    // genuine multi-index OR, cost the MULTI-INDEX OR union against the best
    // single index and the full table scan, and take the cheapest — replacing
    // PR 2's conservative "only when no single index applies" trigger. This is
    // where where9-5.1 (cheap equality/IS-NULL seeks → union wins) and where9-5.3
    // (range OR-branches lose to a single range search on the AND-clause `b>?`)
    // diverge honestly. `select_or_aware_index_method` returns the union only
    // when it is actually the cheapest plan; otherwise it declines and selection
    // continues to single-index / skip-scan below.
    if let Some(choice) = select_or_aware_index_method(table_name, where_clause, order_by, database)
    {
        if crate::profiling::is_scan_debug_enabled() {
            if let IndexScanChoice::MultiIndexOr { ref branches, .. } = choice {
                eprintln!(
                    "[SCAN_PATH] Selected MULTI-INDEX OR for table={}, branches={}",
                    table_name,
                    branches.len()
                );
            }
        }
        return Some(choice);
    }

    // First, try regular cost-based index selection
    if let Some((index_name, sorted_columns)) =
        cost_based_index_selection(table_name, where_clause, order_by, database)
    {
        return Some(IndexScanChoice::Regular { index_name, sorted_columns });
    }

    // If regular index selection failed and we have a WHERE clause,
    // try skip-scan optimization
    if let Some(where_expr) = where_clause {
        let planner = IndexPlanner::new(database);
        if let Some(plan) = planner.plan_skip_scan(table_name, where_expr) {
            if plan.is_skip_scan {
                if let Some(skip_info) = plan.skip_scan_info {
                    if std::env::var("SKIP_SCAN_DEBUG").is_ok() {
                        eprintln!(
                            "[SKIP_SCAN] Selected skip-scan for table={}, index={}, filter_col={}, prefix_cardinality={}",
                            table_name,
                            plan.index_name,
                            skip_info.filter_column,
                            skip_info.prefix_cardinality
                        );
                    }
                    return Some(IndexScanChoice::SkipScan {
                        index_name: plan.index_name,
                        skip_scan_info: skip_info,
                    });
                }
            }
        }
    }

    // No index or skip-scan option found
    None
}

/// Whether the MULTI-INDEX OR optimization (epic #5668) is enabled.
///
/// The feature is **ON by default** and disabled only when the
/// `MULTI_INDEX_OR_DISABLED` environment variable is set, mirroring the
/// `JOIN_REORDER_DISABLED` opt-out (see `reorder::utils::should_apply_join_reordering`).
/// When disabled, [`select_index_scan_method`] never produces an
/// [`IndexScanChoice::MultiIndexOr`], so behavior is byte-identical to before
/// the feature landed.
pub(crate) fn multi_index_or_enabled() -> bool {
    std::env::var("MULTI_INDEX_OR_DISABLED").is_err()
}

/// Database-backed [`BranchIndexResolver`](super::or_analysis::BranchIndexResolver).
///
/// Resolves a single OR-branch predicate to the index VibeSQL would actually use
/// for it, by running the branch predicate through [`cost_based_index_selection`]
/// (the same selection used for the whole WHERE clause). A branch is considered
/// independently indexable iff that selection yields a `Regular` index. This
/// reuses the existing, well-tested selection logic so a resolved branch is
/// guaranteed executable via [`execute_index_scan`](super::execution).
struct DatabaseBranchResolver<'a> {
    table_name: &'a str,
    database: &'a Database,
}

impl super::or_analysis::BranchIndexResolver for DatabaseBranchResolver<'_> {
    fn resolve(&self, branch: &Expression) -> Option<String> {
        // `col IS NULL` is a NULL-key index seek (see execution.rs), but it is not
        // a `=`/range predicate, so `cost_based_index_selection` (which routes
        // through `index_column_can_filter`) does not recognize it. Resolve such a
        // branch directly to an index whose **leading** column is the IS-NULL
        // column — exactly the index `execute_index_scan` will use for the NULL
        // seek. This is what makes where9-5.1 (`c=31031 OR d IS NULL`) resolve all
        // branches and become eligible for the MULTI-INDEX OR cost comparison.
        if let Expression::IsNull { expr, negated: false } = branch {
            if let Expression::ColumnRef(col_id) = expr.as_ref() {
                return self.leading_column_index(col_id.column_canonical());
            }
            return None;
        }

        // No ORDER BY here: a branch is indexable purely as a WHERE lookup.
        cost_based_index_selection(self.table_name, Some(branch), None, self.database)
            .map(|(index_name, _sorted_columns)| index_name)
    }
}

impl DatabaseBranchResolver<'_> {
    /// Find an index on `table_name` whose **leading** column is `column_name`
    /// (case-insensitive). Returns the index name, or `None` if no such index
    /// exists. Used to resolve `IS NULL` branches to their NULL-seek index.
    fn leading_column_index(&self, column_name: &str) -> Option<String> {
        for index_name in self.database.list_indexes_for_table(self.table_name) {
            if let Some(index_metadata) = self.database.get_index(&index_name) {
                if let Some(first) = index_metadata.columns.first() {
                    if let Some(col) = first.column_name() {
                        if col.eq_ignore_ascii_case(column_name) {
                            return Some(index_name);
                        }
                    }
                }
            }
        }
        None
    }
}

/// OR-aware index selection (epic #5668, PR 3).
///
/// Returns `Some(IndexScanChoice::MultiIndexOr { .. })` only when a genuine
/// multi-index OR union is **the cheapest** of three costed alternatives:
///
/// 1. **MULTI-INDEX OR** — sum of per-branch index-seek/scan costs plus a
///    dedup/fetch overhead ([`multi_index_or_cost`]).
/// 2. **Best single index** over the full WHERE, with the OR applied as a
///    residual filter ([`best_single_index_cost`]).
/// 3. **Full table scan** ([`CostEstimator::estimate_table_scan`]).
///
/// This replaces PR 2's conservative "only when no single index applies"
/// structural trigger with an honest cost comparison. The union is returned iff
/// its estimated cost is strictly cheaper than both the best single index and the
/// full scan — so an OR of cheap equality/IS-NULL seeks (where9-5.1) wins, while
/// an OR of range scans that loses to a single range search on an AND-clause
/// (where9-5.3) does not.
///
/// Returns `None` (leaving the caller's single-index / skip-scan / full-scan path
/// unchanged) when:
/// - the feature is disabled via `MULTI_INDEX_OR_DISABLED`,
/// - there is an ORDER BY (a single index may satisfy the ordering; the union has
///   no inherent sort order — deferred per #5668),
/// - the table is WITHOUT ROWID (no rowid to dedup on, #5668 §2b),
/// - the analyzer finds no fully-indexable top-level OR,
/// - all branches resolve to the **same** index (a single-index scan covers it
///   identically and more cheaply),
/// - statistics are missing/stale (cannot cost honestly — defer to the existing
///   rule-based / single-index path rather than guess), or
/// - the union is not the cheapest alternative.
fn select_or_aware_index_method(
    table_name: &str,
    where_clause: Option<&Expression>,
    order_by: Option<&[vibesql_ast::OrderByItem]>,
    database: &Database,
) -> Option<IndexScanChoice> {
    if !multi_index_or_enabled() {
        return None;
    }

    // ORDER BY out of scope: a single index may satisfy the ordering, and the
    // union has no inherent sort order. Defer to the existing path.
    if order_by.is_some() {
        return None;
    }

    let where_expr = where_clause?;

    // WITHOUT ROWID tables have no rowid to dedup on — fall back.
    let table = database.get_table(table_name)?;
    if table.schema.without_rowid {
        return None;
    }

    let resolver = DatabaseBranchResolver { table_name, database };
    let plan = super::or_analysis::analyze_multi_index_or(where_expr, &resolver)?;

    // Genuine multi-index union only: require at least two DISTINCT indexes.
    // If every branch resolves to the same index, the existing single-index
    // scan + residual already covers it identically and more cheaply.
    let distinct_indexes: std::collections::HashSet<&str> =
        plan.branches.iter().map(|b| b.index_name.as_str()).collect();
    if distinct_indexes.len() < 2 {
        return None;
    }

    // Cost the three alternatives. We prefer real statistics to make an honest
    // cost decision; without them (no ANALYZE — the common case in the SQLite
    // conformance harness, which strips ANALYZE), fall back to a rule-based
    // structural heuristic that mirrors SQLite's default optimizer. SQLite's
    // preference order (verified against sqlite3 3.51.0 on the where9 fixture):
    //
    //   1. A single index on an AND-clause **equality** (`b=?`) beats everything
    //      — it is the most selective point seek (where9-5.2: `b=1000 AND (...)`
    //      → `SEARCH t1 USING INDEX t1b (b=?)`).
    //   2. Otherwise, a MULTI-INDEX OR union wins iff *every* OR branch is an
    //      equality / IS-NULL point seek (where9-5.1: `b>1000 AND (c=? OR d IS
    //      NULL)` → the union of two cheap seeks beats the `b>?` range search).
    //   3. A range OR-branch (`c>=?`) scans many rows, so the union loses to the
    //      single AND-clause index (where9-5.3: `b>1000 AND (c>=? OR d IS NULL)`
    //      → `SEARCH t1 USING INDEX t1b (b>?)`).
    //
    // This is the structural form of the equality-seek-vs-range-scan cost signal
    // the architect identified; with statistics, the numeric cost comparison
    // below computes the same decision.
    let table_stats = match table.get_statistics() {
        Some(stats) if !stats.needs_refresh() => stats,
        _ => {
            // (1) An AND-clause equality on an indexed column wins outright.
            let residual_equality_index = plan
                .residual
                .as_ref()
                .map(|r| residual_has_equality_index(table_name, r, database))
                .unwrap_or(false);
            // (2) Otherwise the union wins iff every branch is a point seek.
            if !residual_equality_index && plan.branches.iter().all(branch_is_point_seek) {
                if std::env::var("INDEX_SELECT_DEBUG").is_ok() {
                    eprintln!(
                        "[INDEX_SELECT] MULTI-INDEX OR (no-stats rule) table={}: all branches are point seeks, no AND-clause equality index -> OR wins",
                        table_name
                    );
                }
                return Some(IndexScanChoice::MultiIndexOr {
                    branches: plan.branches,
                    residual: plan.residual,
                });
            }
            return None;
        }
    };
    let cost_estimator = CostEstimator::default();

    let or_cost = multi_index_or_cost(&plan.branches, table_stats, &cost_estimator, database)?;

    let single_index_cost = best_single_index_cost(
        table_name,
        where_clause,
        order_by,
        table_stats,
        &cost_estimator,
        database,
    )
    .map(|(_, _, cost)| cost);

    // What the non-OR path would actually choose: a single index (if one is
    // applicable via the normal selectivity-aware selection) or a full table
    // scan. We must compare the OR-union against THAT decision, not against the
    // raw table-scan cost — because the normal selection (`cost_based_index_selection`)
    // deliberately prefers a selective index over a literally-cheaper table scan
    // (see the `selectivity < 0.40` rule there). If we vetoed the union with the
    // raw scan cost, we would reject a union that beats the very index the engine
    // is about to use, which is incoherent. So:
    //   - If a single index is applicable, the union competes head-to-head with
    //     it (the scan is not a separate veto — the engine already preferred the
    //     index over the scan for this selectivity).
    //   - If NO single index is applicable, the non-OR fallback is a full scan,
    //     so the union must beat the table scan to be worthwhile.
    let single_index_applicable =
        cost_based_index_selection(table_name, where_clause, order_by, database).is_some();
    let table_scan_cost = cost_estimator.estimate_table_scan(table_stats);

    let or_wins = match single_index_cost {
        Some(c) if single_index_applicable => or_cost < c,
        // No applicable single index — compete against the full table scan.
        _ => or_cost < table_scan_cost,
    };

    if std::env::var("INDEX_SELECT_DEBUG").is_ok() {
        eprintln!(
            "[INDEX_SELECT] MULTI-INDEX OR cost-compare table={}: or={:.3} single={:?} (applicable={}) scan={:.3} -> {}",
            table_name,
            or_cost,
            single_index_cost,
            single_index_applicable,
            table_scan_cost,
            if or_wins { "OR wins" } else { "OR loses" }
        );
    }

    if or_wins {
        Some(IndexScanChoice::MultiIndexOr { branches: plan.branches, residual: plan.residual })
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use vibesql_ast::BinaryOperator;
    use vibesql_types::SqlValue;

    use super::*;

    #[test]
    fn test_expression_filters_column_simple() {
        let expr = Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                "age", false,
            ))),
            right: Box::new(Expression::Literal(SqlValue::Integer(25))),
        };

        assert!(expression_filters_column(&expr, "age"));
        assert!(!expression_filters_column(&expr, "name"));
    }

    #[test]
    fn test_expression_filters_column_and() {
        let expr = Expression::BinaryOp {
            op: BinaryOperator::And,
            left: Box::new(Expression::BinaryOp {
                op: BinaryOperator::GreaterThan,
                left: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                    "age", false,
                ))),
                right: Box::new(Expression::Literal(SqlValue::Integer(18))),
            }),
            right: Box::new(Expression::BinaryOp {
                op: BinaryOperator::Equal,
                left: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                    "city", false,
                ))),
                right: Box::new(Expression::Literal(SqlValue::Varchar(arcstr::ArcStr::from(
                    "Boston",
                )))),
            }),
        };

        assert!(expression_filters_column(&expr, "age"));
        assert!(expression_filters_column(&expr, "city"));
        assert!(!expression_filters_column(&expr, "name"));
    }

    #[test]
    fn test_is_column_reference() {
        let expr = Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple("age", false));

        assert!(is_column_reference(&expr, "age"));
        assert!(!is_column_reference(&expr, "name"));
    }

    #[test]
    fn test_is_column_reference_case_insensitive() {
        // SQL parser normalizes unquoted identifiers to uppercase
        // but index columns might be lowercase
        let expr = Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple("I_ID", false)); // Uppercase from parser

        // Should match regardless of case
        assert!(is_column_reference(&expr, "i_id")); // lowercase
        assert!(is_column_reference(&expr, "I_ID")); // exact match
        assert!(is_column_reference(&expr, "I_id")); // mixed case
        assert!(!is_column_reference(&expr, "other_column"));
    }

    #[test]
    fn test_expression_filters_column_case_insensitive() {
        // WHERE I_ID = 42 (uppercase from parser)
        let expr = Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                "I_ID", false,
            ))),
            right: Box::new(Expression::Literal(SqlValue::Integer(42))),
        };

        // Should match lowercase index column name
        assert!(expression_filters_column(&expr, "i_id"));
        assert!(expression_filters_column(&expr, "I_ID"));
        assert!(!expression_filters_column(&expr, "other"));
    }

    /// Helper for tests: build a column-only IndexColumn list from names.
    fn idx_cols(names: &[&str]) -> Vec<vibesql_ast::IndexColumn> {
        names
            .iter()
            .map(|n| vibesql_ast::IndexColumn::Column {
                column_name: n.to_string(),
                direction: vibesql_ast::OrderDirection::Asc,
                prefix_length: None,
            })
            .collect()
    }

    /// Helper: build `WHERE col IN (literal-list)` expression.
    fn in_list_expr(col: &str, values: Vec<SqlValue>) -> Expression {
        Expression::InList {
            expr: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                col, false,
            ))),
            values: values.into_iter().map(Expression::Literal).collect(),
            negated: false,
        }
    }

    #[test]
    fn test_count_pinned_columns_in_list() {
        // WHERE a IN (1, 2, 3) — column `a` should be pinned for index (a, b).
        let where_expr = in_list_expr(
            "a",
            vec![SqlValue::Integer(1), SqlValue::Integer(2), SqlValue::Integer(3)],
        );
        let index = idx_cols(&["a", "b"]);
        assert_eq!(count_pinned_index_columns(Some(&where_expr), &index), 1);
    }

    #[test]
    fn test_count_pinned_columns_not_in_list_excluded() {
        // WHERE a NOT IN (1, 2) — column `a` should NOT be pinned.
        let where_expr = Expression::InList {
            expr: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                "a", false,
            ))),
            values: vec![
                Expression::Literal(SqlValue::Integer(1)),
                Expression::Literal(SqlValue::Integer(2)),
            ],
            negated: true,
        };
        let index = idx_cols(&["a", "b"]);
        assert_eq!(count_pinned_index_columns(Some(&where_expr), &index), 0);
    }

    #[test]
    fn test_count_pinned_columns_empty_in_list_excluded() {
        // WHERE a IN () — no values means no effective pinning.
        let where_expr = Expression::InList {
            expr: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                "a", false,
            ))),
            values: vec![],
            negated: false,
        };
        let index = idx_cols(&["a", "b"]);
        assert_eq!(count_pinned_index_columns(Some(&where_expr), &index), 0);
    }

    #[test]
    fn test_count_pinned_columns_in_list_with_non_literal_excluded() {
        // WHERE a IN (1, b) — non-literal in list disqualifies pinning.
        let where_expr = Expression::InList {
            expr: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                "a", false,
            ))),
            values: vec![
                Expression::Literal(SqlValue::Integer(1)),
                Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple("b", false)),
            ],
            negated: false,
        };
        let index = idx_cols(&["a", "b"]);
        assert_eq!(count_pinned_index_columns(Some(&where_expr), &index), 0);
    }

    #[test]
    fn test_count_pinned_columns_in_list_with_null_still_pins() {
        // WHERE a IN (1, NULL, 3) — list contains NULL but the IN-list still pins.
        // (NULL never compares true, so rows with a IS NULL never match — equivalent
        // to filtering them out, which preserves the pinning semantic.)
        let where_expr = Expression::InList {
            expr: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                "a", false,
            ))),
            values: vec![
                Expression::Literal(SqlValue::Integer(1)),
                Expression::Literal(SqlValue::Null),
                Expression::Literal(SqlValue::Integer(3)),
            ],
            negated: false,
        };
        let index = idx_cols(&["a", "b"]);
        // Note: `is_literal` returns false for NULL, so the IN-list is rejected as
        // pinning. This is conservative but correct — current behavior.
        assert_eq!(count_pinned_index_columns(Some(&where_expr), &index), 0);
    }

    #[test]
    fn test_count_pinned_columns_in_list_combined_with_equality() {
        // WHERE a IN (1, 2) AND b = 5 — both `a` and `b` should be pinned.
        let in_a = in_list_expr("a", vec![SqlValue::Integer(1), SqlValue::Integer(2)]);
        let eq_b = Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                "b", false,
            ))),
            right: Box::new(Expression::Literal(SqlValue::Integer(5))),
        };
        let where_expr = Expression::BinaryOp {
            op: BinaryOperator::And,
            left: Box::new(in_a),
            right: Box::new(eq_b),
        };
        let index = idx_cols(&["a", "b", "c"]);
        assert_eq!(count_pinned_index_columns(Some(&where_expr), &index), 2);
    }

    #[test]
    fn test_can_use_index_for_order_by_with_pinned_in_list_leading() {
        // Index (a, b), WHERE a IN (1,2,3), ORDER BY a, b — should be usable.
        // The leading ORDER BY item `a` is pinned; the trailing `b` aligns with
        // the remaining index column.
        let order_by = vec![
            vibesql_ast::OrderByItem {
                expr: Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple("a", false)),
                direction: vibesql_ast::OrderDirection::Asc,
                nulls_order: None,
            },
            vibesql_ast::OrderByItem {
                expr: Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple("b", false)),
                direction: vibesql_ast::OrderDirection::Asc,
                nulls_order: None,
            },
        ];
        let index = idx_cols(&["a", "b"]);
        // pinned_columns = 1 (a is pinned); ORDER BY [a, b] should still be matched.
        assert!(can_use_index_for_order_by_with_pinned(&order_by, &index, 1));
    }

    #[test]
    fn test_can_use_index_for_order_by_all_pinned() {
        // Index (a, b), WHERE a = 1 AND b = 2, ORDER BY a, b — every ORDER BY item
        // is pinned, so the index trivially satisfies the order.
        let order_by = vec![
            vibesql_ast::OrderByItem {
                expr: Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple("a", false)),
                direction: vibesql_ast::OrderDirection::Asc,
                nulls_order: None,
            },
            vibesql_ast::OrderByItem {
                expr: Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple("b", false)),
                direction: vibesql_ast::OrderDirection::Asc,
                nulls_order: None,
            },
        ];
        let index = idx_cols(&["a", "b"]);
        assert!(can_use_index_for_order_by_with_pinned(&order_by, &index, 2));
    }

    #[test]
    fn test_single_value_pin_excludes_in_list() {
        // WHERE a IN (1, 2, 3): IN-inclusive count pins `a` (for seeking), but the
        // single-value count does NOT (a is not constant within the scan output),
        // so ORDER BY satisfaction must not skip `a`.
        let where_expr = in_list_expr("a", vec![SqlValue::Integer(1), SqlValue::Integer(2)]);
        let index = idx_cols(&["a", "b"]);
        assert_eq!(count_pinned_index_columns(Some(&where_expr), &index), 1);
        assert_eq!(count_single_value_pinned_index_columns(Some(&where_expr), &index), 0);
    }

    #[test]
    fn test_single_value_pin_counts_equality() {
        // WHERE a = 1: a true single-value equality pins `a` under both counts.
        let where_expr = Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                "a", false,
            ))),
            right: Box::new(Expression::Literal(SqlValue::Integer(1))),
        };
        let index = idx_cols(&["a", "b"]);
        assert_eq!(count_pinned_index_columns(Some(&where_expr), &index), 1);
        assert_eq!(count_single_value_pinned_index_columns(Some(&where_expr), &index), 1);
    }

    #[test]
    fn test_single_value_pin_mixed_equality_and_in_list() {
        // WHERE a = 1 AND b IN (2, 3): equality pins `a` for ordering; the IN on
        // `b` does not. So the single-value prefix stops at `a` (count 1), while
        // the IN-inclusive count covers both (count 2).
        let eq_a = Expression::BinaryOp {
            op: BinaryOperator::Equal,
            left: Box::new(Expression::ColumnRef(vibesql_ast::ColumnIdentifier::simple(
                "a", false,
            ))),
            right: Box::new(Expression::Literal(SqlValue::Integer(1))),
        };
        let in_b = in_list_expr("b", vec![SqlValue::Integer(2), SqlValue::Integer(3)]);
        let where_expr = Expression::BinaryOp {
            op: BinaryOperator::And,
            left: Box::new(eq_a),
            right: Box::new(in_b),
        };
        let index = idx_cols(&["a", "b", "c"]);
        assert_eq!(count_pinned_index_columns(Some(&where_expr), &index), 2);
        assert_eq!(count_single_value_pinned_index_columns(Some(&where_expr), &index), 1);
    }

    // ---- OR-aware cost model (epic #5668, PR 3) -----------------------------
    //
    // These tests exercise `select_index_scan_method` against a real `Database`
    // with computed statistics (via `ANALYZE`) and assert the **chosen access
    // path** programmatically (the returned `IndexScanChoice` variant), per
    // #5668 §2c / PR 3 acceptance. EQP text rendering + where9 un-skip is PR 4.
    mod or_aware_cost {
        use std::sync::Mutex;

        use super::super::IndexScanChoice;

        // `MULTI_INDEX_OR_DISABLED` is set via process-global `std::env::set_var`
        // in the kill-switch test. Serialize ALL tests in this module so the flag
        // toggle cannot race with the default-ON cost-decision tests.
        static ENV_LOCK: Mutex<()> = Mutex::new(());

        /// Build a fresh `t1` mirroring the where9 fixture (`a INTEGER PRIMARY
        /// KEY,b,c,d,e,f,g`) with the 99 canonical rows and single-column indexes
        /// on b, c, d, then run `ANALYZE` so cost-based selection engages with
        /// **real** column statistics (null counts, n_distinct, min/max).
        fn where9_t1() -> vibesql_storage::Database {
            let mut db = vibesql_storage::Database::new();
            // The 7-column fixture rows from docs/reference/sqlite/test/where9.test.
            // We only need columns a,b,c,d to be faithful for selection on b/c/d;
            // e,f,g are filled with simple placeholders.
            let inserts: &[(i64, Option<i64>, Option<i64>, Option<f64>)] = &WHERE9_ROWS;
            run(&mut db, "CREATE TABLE t1(a INTEGER PRIMARY KEY, b, c, d, e, f, g)");
            for &(a, b, c, d) in inserts {
                let b_s = b.map(|v| v.to_string()).unwrap_or_else(|| "NULL".into());
                let c_s = c.map(|v| v.to_string()).unwrap_or_else(|| "NULL".into());
                let d_s = d.map(|v| v.to_string()).unwrap_or_else(|| "NULL".into());
                run(
                    &mut db,
                    &format!("INSERT INTO t1 VALUES ({a}, {b_s}, {c_s}, {d_s}, 0, 'x', 'y')"),
                );
            }
            run(&mut db, "CREATE INDEX t1b ON t1(b)");
            run(&mut db, "CREATE INDEX t1c ON t1(c)");
            run(&mut db, "CREATE INDEX t1d ON t1(d)");
            run(&mut db, "ANALYZE");
            db
        }

        fn run(db: &mut vibesql_storage::Database, sql: &str) {
            let stmt = vibesql_parser::Parser::parse_sql(sql)
                .unwrap_or_else(|e| panic!("parse {sql}: {e:?}"));
            match stmt {
                vibesql_ast::Statement::CreateTable(c) => {
                    crate::CreateTableExecutor::execute(&c, db).unwrap();
                }
                vibesql_ast::Statement::CreateIndex(c) => {
                    crate::CreateIndexExecutor::execute(&c, db).unwrap();
                }
                vibesql_ast::Statement::Insert(i) => {
                    crate::InsertExecutor::execute(db, &i).unwrap();
                }
                vibesql_ast::Statement::Analyze(a) => {
                    crate::AnalyzeExecutor::execute(&a, db).unwrap();
                }
                other => panic!("unsupported setup statement: {other:?}"),
            }
        }

        /// Parse a bare WHERE expression by parsing a SELECT and extracting it.
        fn where_of(sql: &str) -> vibesql_ast::Expression {
            let stmt = vibesql_parser::Parser::parse_sql(sql).unwrap();
            let vibesql_ast::Statement::Select(select) = stmt else { panic!("not select") };
            select.where_clause.expect("query has a WHERE clause")
        }

        fn choose(db: &vibesql_storage::Database, sql: &str) -> Option<IndexScanChoice> {
            let where_expr = where_of(sql);
            super::super::select_index_scan_method("t1", Some(&where_expr), None, db)
        }

        #[test]
        fn where9_5_1_chooses_multi_index_or() {
            let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
            // c=31031 OR d IS NULL — two cheap equality/IS-NULL point seeks. The
            // OR-union beats the single range search on the AND-clause b>1000, so
            // MULTI-INDEX OR wins.
            let db = where9_t1();
            let choice = choose(&db, "SELECT a FROM t1 WHERE b>1000 AND (c=31031 OR d IS NULL)");
            match choice {
                Some(IndexScanChoice::MultiIndexOr { branches, .. }) => {
                    assert_eq!(branches.len(), 2, "two OR branches");
                }
                other => panic!("expected MULTI-INDEX OR for where9-5.1, got {other:?}"),
            }
        }

        #[test]
        fn where9_5_3_does_not_choose_multi_index_or() {
            let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
            // c>=31031 OR d IS NULL — branch 1 is a RANGE scan (many rows). The
            // OR-union of a range scan + IS-NULL seek is MORE expensive than a
            // single index on the AND-clause `b>1000`, so the OR-aware cost model
            // declines MULTI-INDEX OR and a single Regular index is chosen instead.
            //
            // This is the PR-3 conformance gate's contrast to where9-5.1: the
            // equality-seek-vs-range-scan signal flips the decision purely on
            // cost (or≈57 vs single≈29; for 5.1 it was or≈12 vs single≈19).
            //
            // The *specific* single index here (SQLite renders `t1b (b>?)`) is
            // decided by the single-index selector's ranking of t1b vs t1c for an
            // `AND`+`OR` clause. With the #5692 fix this is deterministic: t1b is
            // chosen because its leading column has a top-level seekable predicate
            // (`b>1000` → SEARCH), whereas t1c's only predicate is OR-nested
            // (`c>=31031` → would degrade to SCAN). See
            // `index_leading_column_seekable_at_top_level`.
            let db = where9_t1();
            let choice = choose(&db, "SELECT a FROM t1 WHERE b>1000 AND (c>=31031 OR d IS NULL)");
            match &choice {
                Some(IndexScanChoice::Regular { index_name, .. }) => {
                    assert_eq!(
                        index_name, "t1b",
                        "where9-5.3 must pick t1b (top-level seekable b>?), not t1c/SCAN; got {index_name}"
                    );
                }
                other => {
                    panic!("where9-5.3 must choose a single Regular index, got {other:?}")
                }
            }
        }

        #[test]
        fn where9_5_3_single_index_choice_is_deterministic() {
            let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
            // Regression guard for #5692 (non-determinism flagged in #5660):
            // the single-index selector must pick t1b on EVERY evaluation, not
            // sometimes t1c, regardless of HashMap index-iteration order. Repeat
            // the selection many times and require a stable answer. Pre-fix this
            // flipped between t1b/SEARCH and t1c/SCAN across runs.
            let db = where9_t1();
            for i in 0..64 {
                let choice =
                    choose(&db, "SELECT a FROM t1 WHERE b>1000 AND (c>=31031 OR d IS NULL)");
                match &choice {
                    Some(IndexScanChoice::Regular { index_name, .. }) => {
                        assert_eq!(
                            index_name, "t1b",
                            "iteration {i}: expected t1b, got {index_name}"
                        );
                    }
                    other => panic!("iteration {i}: expected Regular(t1b), got {other:?}"),
                }
            }
        }

        #[test]
        fn top_level_seekable_distinguishes_and_clause_from_or_branch() {
            // Unit-level check of the principle: a leading column constrained by a
            // top-level AND conjunct is seekable; one referenced only inside an OR
            // branch is not.
            let make_col = |name: &str| {
                vec![vibesql_ast::IndexColumn::Column {
                    column_name: name.to_string(),
                    direction: vibesql_ast::OrderDirection::Asc,
                    prefix_length: None,
                }]
            };
            let where_expr = where_of("SELECT a FROM t1 WHERE b>1000 AND (c>=31031 OR d IS NULL)");
            // b is in the top-level AND conjunct → seekable.
            assert!(
                super::super::index_leading_column_seekable_at_top_level(
                    Some(&where_expr),
                    &make_col("b")
                ),
                "b>1000 is a top-level conjunct → seekable"
            );
            // c is only inside the OR branch → NOT a top-level seek (would SCAN).
            assert!(
                !super::super::index_leading_column_seekable_at_top_level(
                    Some(&where_expr),
                    &make_col("c")
                ),
                "c>=31031 is OR-nested → not a top-level seek"
            );
            // d is only inside the OR branch (IS NULL) → NOT a top-level seek.
            assert!(
                !super::super::index_leading_column_seekable_at_top_level(
                    Some(&where_expr),
                    &make_col("d")
                ),
                "d IS NULL is OR-nested → not a top-level seek"
            );
        }

        #[test]
        fn top_level_seekable_leaf_set_matches_seek_extractor() {
            // Regression guard for #5694 (Judge feedback): the helper's leaf
            // check must mirror the seek extractor's accepted set EXACTLY. The
            // extractor produces NO seek for IN-subqueries or negated
            // BETWEEN/IN-list, so the helper must report those as NOT seekable —
            // otherwise the selector deterministically picks a SCAN-rendering
            // index over an available SEARCH-rendering one.
            let make_col = |name: &str| {
                vec![vibesql_ast::IndexColumn::Column {
                    column_name: name.to_string(),
                    direction: vibesql_ast::OrderDirection::Asc,
                    prefix_length: None,
                }]
            };
            let seekable = |sql: &str, col: &str| {
                super::super::index_leading_column_seekable_at_top_level(
                    Some(&where_of(sql)),
                    &make_col(col),
                )
            };

            // Shape 1: `x NOT BETWEEN 10 AND 20 AND y>100`.
            // x's only predicate is a NEGATED BETWEEN → extractor yields no seek
            // → x must NOT be top-level seekable (else we'd pick SCAN on x's
            // index over SEARCH on y's). y>100 IS a top-level seek.
            let sql1 = "SELECT a FROM t1 WHERE x NOT BETWEEN 10 AND 20 AND y>100";
            assert!(
                !seekable(sql1, "x"),
                "negated BETWEEN on leading column must NOT be top-level seekable"
            );
            assert!(seekable(sql1, "y"), "y>100 is a top-level seek");

            // Shape 2: `x IN (SELECT v FROM s) AND y>100`.
            // x's only predicate is an IN-subquery (Expression::In) → extractor
            // yields no seek → x must NOT be top-level seekable. y>100 IS a seek.
            let sql2 = "SELECT a FROM t1 WHERE x IN (SELECT v FROM s) AND y>100";
            assert!(
                !seekable(sql2, "x"),
                "IN (SELECT ...) on leading column must NOT be top-level seekable"
            );
            assert!(seekable(sql2, "y"), "y>100 is a top-level seek");

            // Negated IN-list is likewise not a seek.
            assert!(
                !seekable("SELECT a FROM t1 WHERE x NOT IN (1, 2, 3) AND y>100", "x"),
                "negated IN-list on leading column must NOT be top-level seekable"
            );
        }

        #[test]
        fn top_level_seekable_accepts_valid_between_and_inlist() {
            // Guard against over-restriction: a NON-negated BETWEEN or IN-list on
            // the leading column DOES produce a seek (the extractor expands
            // BETWEEN to `>= AND <=` and treats IN-list as `=`), so the helper
            // must still report these as top-level seekable.
            let make_col = |name: &str| {
                vec![vibesql_ast::IndexColumn::Column {
                    column_name: name.to_string(),
                    direction: vibesql_ast::OrderDirection::Asc,
                    prefix_length: None,
                }]
            };
            let seekable = |sql: &str, col: &str| {
                super::super::index_leading_column_seekable_at_top_level(
                    Some(&where_of(sql)),
                    &make_col(col),
                )
            };

            assert!(
                seekable("SELECT a FROM t1 WHERE x BETWEEN 10 AND 20", "x"),
                "non-negated BETWEEN on leading column IS a top-level seek"
            );
            assert!(
                seekable("SELECT a FROM t1 WHERE x IN (1, 2, 3)", "x"),
                "non-negated IN-list on leading column IS a top-level seek"
            );
            // And `=` plus IS (NULL-safe equals) remain seeks.
            assert!(
                seekable("SELECT a FROM t1 WHERE x = 5", "x"),
                "equality on leading column IS a top-level seek"
            );
        }

        #[test]
        fn pure_equality_or_over_two_indexes_chooses_multi_index_or() {
            let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
            // No AND-clause competitor: a pure equality OR over two distinct
            // single-column indexes is the textbook MULTI-INDEX OR win.
            let db = where9_t1();
            let choice = choose(&db, "SELECT a FROM t1 WHERE c=31031 OR d IS NULL");
            assert!(
                matches!(choice, Some(IndexScanChoice::MultiIndexOr { .. })),
                "pure equality/IS-NULL OR over two indexes should choose MULTI-INDEX OR, got {choice:?}"
            );
        }

        #[test]
        fn wide_range_or_does_not_choose_multi_index_or() {
            let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
            // c>=1001 matches essentially every non-NULL row (c's minimum is
            // 1001), so the OR-union is at least a near-full scan + a seek — never
            // cheaper than the plain alternatives. The honest cost says: do NOT
            // use MULTI-INDEX OR. (It picks a single index or a full scan; either
            // is acceptable — the point is the union must lose.)
            let db = where9_t1();
            let choice = choose(&db, "SELECT a FROM t1 WHERE c>=1001 OR d IS NULL");
            assert!(
                !matches!(choice, Some(IndexScanChoice::MultiIndexOr { .. })),
                "a wide-range OR branch must not trigger MULTI-INDEX OR, got {choice:?}"
            );
        }

        #[test]
        fn kill_switch_disables_multi_index_or() {
            let _g = ENV_LOCK.lock().unwrap_or_else(|e| e.into_inner());
            // The MULTI_INDEX_OR_DISABLED kill switch must still suppress the union
            // even for a query the cost model would otherwise pick it for.
            // SAFETY: serialized within this single-threaded test; restored before
            // returning.
            let db = where9_t1();
            unsafe {
                std::env::set_var("MULTI_INDEX_OR_DISABLED", "1");
            }
            let choice = choose(&db, "SELECT a FROM t1 WHERE c=31031 OR d IS NULL");
            unsafe {
                std::env::remove_var("MULTI_INDEX_OR_DISABLED");
            }
            assert!(
                !matches!(choice, Some(IndexScanChoice::MultiIndexOr { .. })),
                "kill switch must disable MULTI-INDEX OR, got {choice:?}"
            );
        }

        // The 99 canonical where9 rows, projected to (a, b, c, d). NULLs preserved
        // from the fixture (rows 90,91,92,96,97,99 carry NULLs in b/c/d).
        const WHERE9_ROWS: [(i64, Option<i64>, Option<i64>, Option<f64>); 99] = [
            (1, Some(11), Some(1001), Some(1.001)),
            (2, Some(22), Some(1001), Some(2.002)),
            (3, Some(33), Some(1001), Some(3.003)),
            (4, Some(44), Some(2002), Some(4.004)),
            (5, Some(55), Some(2002), Some(5.005)),
            (6, Some(66), Some(2002), Some(6.006)),
            (7, Some(77), Some(3003), Some(7.007)),
            (8, Some(88), Some(3003), Some(8.008)),
            (9, Some(99), Some(3003), Some(9.009)),
            (10, Some(110), Some(4004), Some(10.01)),
            (11, Some(121), Some(4004), Some(11.011)),
            (12, Some(132), Some(4004), Some(12.012)),
            (13, Some(143), Some(5005), Some(13.013)),
            (14, Some(154), Some(5005), Some(14.014)),
            (15, Some(165), Some(5005), Some(15.015)),
            (16, Some(176), Some(6006), Some(16.016)),
            (17, Some(187), Some(6006), Some(17.017)),
            (18, Some(198), Some(6006), Some(18.018)),
            (19, Some(209), Some(7007), Some(19.019)),
            (20, Some(220), Some(7007), Some(20.02)),
            (21, Some(231), Some(7007), Some(21.021)),
            (22, Some(242), Some(8008), Some(22.022)),
            (23, Some(253), Some(8008), Some(23.023)),
            (24, Some(264), Some(8008), Some(24.024)),
            (25, Some(275), Some(9009), Some(25.025)),
            (26, Some(286), Some(9009), Some(26.026)),
            (27, Some(297), Some(9009), Some(27.027)),
            (28, Some(308), Some(10010), Some(28.028)),
            (29, Some(319), Some(10010), Some(29.029)),
            (30, Some(330), Some(10010), Some(30.03)),
            (31, Some(341), Some(11011), Some(31.031)),
            (32, Some(352), Some(11011), Some(32.032)),
            (33, Some(363), Some(11011), Some(33.033)),
            (34, Some(374), Some(12012), Some(34.034)),
            (35, Some(385), Some(12012), Some(35.035)),
            (36, Some(396), Some(12012), Some(36.036)),
            (37, Some(407), Some(13013), Some(37.037)),
            (38, Some(418), Some(13013), Some(38.038)),
            (39, Some(429), Some(13013), Some(39.039)),
            (40, Some(440), Some(14014), Some(40.04)),
            (41, Some(451), Some(14014), Some(41.041)),
            (42, Some(462), Some(14014), Some(42.042)),
            (43, Some(473), Some(15015), Some(43.043)),
            (44, Some(484), Some(15015), Some(44.044)),
            (45, Some(495), Some(15015), Some(45.045)),
            (46, Some(506), Some(16016), Some(46.046)),
            (47, Some(517), Some(16016), Some(47.047)),
            (48, Some(528), Some(16016), Some(48.048)),
            (49, Some(539), Some(17017), Some(49.049)),
            (50, Some(550), Some(17017), Some(50.05)),
            (51, Some(561), Some(17017), Some(51.051)),
            (52, Some(572), Some(18018), Some(52.052)),
            (53, Some(583), Some(18018), Some(53.053)),
            (54, Some(594), Some(18018), Some(54.054)),
            (55, Some(605), Some(19019), Some(55.055)),
            (56, Some(616), Some(19019), Some(56.056)),
            (57, Some(627), Some(19019), Some(57.057)),
            (58, Some(638), Some(20020), Some(58.058)),
            (59, Some(649), Some(20020), Some(59.059)),
            (60, Some(660), Some(20020), Some(60.06)),
            (61, Some(671), Some(21021), Some(61.061)),
            (62, Some(682), Some(21021), Some(62.062)),
            (63, Some(693), Some(21021), Some(63.063)),
            (64, Some(704), Some(22022), Some(64.064)),
            (65, Some(715), Some(22022), Some(65.065)),
            (66, Some(726), Some(22022), Some(66.066)),
            (67, Some(737), Some(23023), Some(67.067)),
            (68, Some(748), Some(23023), Some(68.068)),
            (69, Some(759), Some(23023), Some(69.069)),
            (70, Some(770), Some(24024), Some(70.07)),
            (71, Some(781), Some(24024), Some(71.071)),
            (72, Some(792), Some(24024), Some(72.072)),
            (73, Some(803), Some(25025), Some(73.073)),
            (74, Some(814), Some(25025), Some(74.074)),
            (75, Some(825), Some(25025), Some(75.075)),
            (76, Some(836), Some(26026), Some(76.076)),
            (77, Some(847), Some(26026), Some(77.077)),
            (78, Some(858), Some(26026), Some(78.078)),
            (79, Some(869), Some(27027), Some(79.079)),
            (80, Some(880), Some(27027), Some(80.08)),
            (81, Some(891), Some(27027), Some(81.081)),
            (82, Some(902), Some(28028), Some(82.082)),
            (83, Some(913), Some(28028), Some(83.083)),
            (84, Some(924), Some(28028), Some(84.084)),
            (85, Some(935), Some(29029), Some(85.085)),
            (86, Some(946), Some(29029), Some(86.086)),
            (87, Some(957), Some(29029), Some(87.087)),
            (88, Some(968), Some(30030), Some(88.088)),
            (89, Some(979), Some(30030), Some(89.089)),
            (90, None, Some(30030), Some(90.09)),
            (91, Some(1001), None, Some(91.091)),
            (92, Some(1012), Some(31031), None),
            (93, Some(1023), Some(31031), Some(93.093)),
            (94, Some(1034), Some(32032), Some(94.094)),
            (95, Some(1045), Some(32032), Some(95.095)),
            (96, None, None, Some(96.096)),
            (97, Some(1067), Some(33033), None),
            (98, Some(1078), Some(33033), Some(98.098)),
            (99, None, None, None),
        ];
    }
}
