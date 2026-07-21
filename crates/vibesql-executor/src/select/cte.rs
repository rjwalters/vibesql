//! Common Table Expression (CTE) handling for SELECT queries

use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};

use crate::errors::ExecutorError;

/// CTE result: (schema, shared rows)
///
/// Uses `Arc<Vec<Row>>` to enable O(1) cloning when CTEs are:
/// - Propagated from outer queries to subqueries
/// - Referenced multiple times without filtering
///
/// This avoids deep-cloning all rows on every CTE reference.
pub type CteResult = (vibesql_catalog::TableSchema, Arc<Vec<vibesql_storage::Row>>);

/// Execute all CTEs and return their results
///
/// CTEs are executed in order, allowing later CTEs to reference earlier ones.
///
/// The `database` reference is used to statically expand wildcard SELECT items
/// (`SELECT * FROM t`) into the underlying table's column names when deriving
/// each CTE's schema (#5293).
pub fn execute_ctes<F>(
    ctes: &[vibesql_ast::CommonTableExpr],
    database: &vibesql_storage::Database,
    executor: F,
) -> Result<HashMap<String, CteResult>, ExecutorError>
where
    F: Fn(
        &vibesql_ast::SelectStmt,
        &HashMap<String, CteResult>,
    ) -> Result<Vec<vibesql_storage::Row>, ExecutorError>,
{
    // Use the memory-tracking version with a no-op memory check.
    // No root statement is provided, so every CTE in the list is executed
    // eagerly (used by DML executors, where laziness is not required).
    execute_ctes_with_memory_check(ctes, None, database, executor, |_| Ok(()))
}

/// Execute the CTEs of a SELECT statement, skipping CTEs that the statement
/// never references (directly or transitively through other referenced CTEs).
///
/// SQLite expands CTEs lazily: a `WITH` entry that is never referenced by the
/// main statement is never evaluated, so errors inside its body are never
/// reported (with2.test 11.x/12.1). This entry point mirrors that behavior for
/// paths that execute a statement's WITH clause (issue #5838).
///
/// `outer_ctes` seeds the enclosing CTE scope so a local CTE body may reference
/// a CTE from an outer query — needed when this statement is a subquery of an
/// outer query that also has CTEs (with3.test 2.1). Pass an empty map for a
/// top-level statement. Local names shadow outer names.
pub fn execute_ctes_for_stmt<F>(
    ctes: &[vibesql_ast::CommonTableExpr],
    root: &vibesql_ast::SelectStmt,
    outer_ctes: &HashMap<String, CteResult>,
    database: &vibesql_storage::Database,
    executor: F,
) -> Result<HashMap<String, CteResult>, ExecutorError>
where
    F: Fn(
        &vibesql_ast::SelectStmt,
        &HashMap<String, CteResult>,
    ) -> Result<Vec<vibesql_storage::Row>, ExecutorError>,
{
    execute_ctes_with_outer(ctes, Some(root), outer_ctes, database, executor, |_| Ok(()))
}

/// SQLite evaluates a recursive CTE lazily against the queue that feeds the
/// outer query. When the outer statement simply reads a single recursive CTE
/// with a `LIMIT`, materialization stops once enough rows have been produced —
/// so an otherwise infinite/circular CTE terminates (with1.test 5.1, 5.4).
///
/// We approximate that laziness with a *row hint*: the number of rows the outer
/// query can possibly consume from a directly-referenced recursive CTE. The
/// hint is only derived when the outer statement reads the CTE without any
/// clause that could drop or reorder rows (no WHERE/JOIN/GROUP BY/HAVING/
/// DISTINCT/ORDER BY/set-operation), so stopping early can never change the
/// outer result. When the shape is anything more complex, the hint is `None`
/// and the CTE materializes fully (up to the recursion cap).
///
/// The hint is `outer LIMIT + outer OFFSET`: OFFSET rows are consumed and
/// discarded before LIMIT rows are returned, so both count toward the number of
/// CTE rows the outer query needs.
fn outer_row_hint_for_stmt(
    root: &vibesql_ast::SelectStmt,
    ctes: &[vibesql_ast::CommonTableExpr],
    database: &vibesql_storage::Database,
) -> Option<usize> {
    // Only a bare `SELECT ... FROM <name> LIMIT n [OFFSET m]` qualifies: any
    // filtering/reordering/aggregation could make an early stop drop rows the
    // outer query would have kept.
    if root.limit.is_none()
        || root.where_clause.is_some()
        || root.group_by.is_some()
        || root.having.is_some()
        || root.distinct
        || root.order_by.is_some()
        || root.set_operation.is_some()
        || root.values.is_some()
    {
        return None;
    }

    // FROM must be a single table reference naming one of this statement's CTEs.
    let name = match root.from.as_ref()? {
        vibesql_ast::FromClause::Table { name, .. } => name,
        _ => return None,
    };
    if !ctes.iter().any(|c| c.name.eq_ignore_ascii_case(name)) {
        return None;
    }

    let limit = eval_const_count(root.limit.as_ref()?, database)?;
    let offset = match &root.offset {
        Some(expr) => eval_const_count(expr, database)?,
        None => 0,
    };
    Some(limit.saturating_add(offset))
}

/// Evaluate a LIMIT/OFFSET expression against an empty row/schema, returning the
/// non-negative row count. Returns `None` for negative ("unlimited") or
/// unresolvable expressions, which disable the early-stop optimization.
fn eval_const_count(
    expr: &vibesql_ast::Expression,
    database: &vibesql_storage::Database,
) -> Option<usize> {
    use crate::evaluator::ExpressionEvaluator;

    let empty_schema = vibesql_catalog::TableSchema::new(String::new(), vec![]);
    let evaluator = ExpressionEvaluator::with_database(&empty_schema, database);
    let empty_row = vibesql_storage::Row::new(vec![]);
    let value = evaluator.eval(expr, &empty_row).ok()?;
    let n = crate::select::helpers::coerce_limit_offset_to_i64(value).ok()?;
    if n < 0 {
        None
    } else {
        Some(n as usize)
    }
}

/// Execute all CTEs with memory tracking
///
/// CTEs are executed in order, allowing later CTEs to reference earlier ones.
/// After each CTE is materialized, the memory_check callback is called with
/// the estimated size of the CTE result to enforce memory limits.
///
/// When `root` is provided, only CTEs that are (transitively) referenced by the
/// root statement are executed, matching SQLite's lazy CTE expansion.
pub(super) fn execute_ctes_with_memory_check<F, M>(
    ctes: &[vibesql_ast::CommonTableExpr],
    root: Option<&vibesql_ast::SelectStmt>,
    database: &vibesql_storage::Database,
    executor: F,
    memory_check: M,
) -> Result<HashMap<String, CteResult>, ExecutorError>
where
    F: Fn(
        &vibesql_ast::SelectStmt,
        &HashMap<String, CteResult>,
    ) -> Result<Vec<vibesql_storage::Row>, ExecutorError>,
    M: Fn(usize) -> Result<(), ExecutorError>,
{
    execute_ctes_with_outer(ctes, root, &HashMap::new(), database, executor, memory_check)
}

/// Like [`execute_ctes_with_memory_check`], but with an explicit `outer_ctes`
/// scope seeded from an enclosing query.
///
/// When a statement that carries its own `WITH` clause is itself a subquery of
/// an outer query that also has CTEs (`WITH x AS (...) SELECT ... FROM (WITH y
/// AS (SELECT * FROM x) SELECT ...)`), the inner CTE bodies must be able to see
/// the outer CTEs (with3.test 2.1). The top-level entry previously seeded the
/// outer scope with an empty map, so a local CTE body referencing an outer CTE
/// failed with a spurious "no such table". Threading `outer_ctes` through fixes
/// that; local names still shadow outer names.
pub(super) fn execute_ctes_with_outer<F, M>(
    ctes: &[vibesql_ast::CommonTableExpr],
    root: Option<&vibesql_ast::SelectStmt>,
    outer_ctes: &HashMap<String, CteResult>,
    database: &vibesql_storage::Database,
    executor: F,
    memory_check: M,
) -> Result<HashMap<String, CteResult>, ExecutorError>
where
    F: Fn(
        &vibesql_ast::SelectStmt,
        &HashMap<String, CteResult>,
    ) -> Result<Vec<vibesql_storage::Row>, ExecutorError>,
    M: Fn(usize) -> Result<(), ExecutorError>,
{
    // If the root statement reads a directly-referenced recursive CTE under a
    // LIMIT, thread that limit inward so lazy recursion can terminate early
    // (with1.test 5.1, 5.4). Only top-level CTEs of the root statement are
    // eligible for this hint.
    let outer_row_hint = root.and_then(|stmt| outer_row_hint_for_stmt(stmt, ctes, database));

    let mut in_progress = Vec::new();
    execute_cte_list(
        ctes,
        root,
        outer_ctes,
        &mut in_progress,
        false,
        outer_row_hint,
        database,
        &executor,
        &memory_check,
    )
}

/// Per-CTE execution state used for sibling dependency resolution and
/// within-list cycle detection.
#[derive(Clone, Copy, PartialEq)]
enum CteState {
    Pending,
    InFlight,
    Done,
}

/// Execute a (possibly nested) WITH-clause CTE list.
///
/// - `root`: when provided, CTEs not (transitively) referenced by this statement are skipped
///   entirely (SQLite lazy expansion).
/// - `outer_ctes`: fully-materialized CTEs from enclosing scopes. Local names shadow outer names.
/// - `in_progress`: names of enclosing CTE definitions currently being executed. A body reference
///   that can only resolve to one of these is a circular reference (with2.test 3.5).
/// - `nested`: true when this list is the WITH clause of a CTE body (as opposed to a statement's
///   top-level WITH). Both kinds resolve sibling references regardless of declaration order
///   (with1.test 2.5, with2.test 1.11); the flag is retained to distinguish the two scopes for
///   future use.
#[allow(clippy::too_many_arguments)]
fn execute_cte_list<F, M>(
    ctes: &[vibesql_ast::CommonTableExpr],
    root: Option<&vibesql_ast::SelectStmt>,
    outer_ctes: &HashMap<String, CteResult>,
    in_progress: &mut Vec<String>,
    nested: bool,
    outer_row_hint: Option<usize>,
    database: &vibesql_storage::Database,
    executor: &F,
    memory_check: &M,
) -> Result<HashMap<String, CteResult>, ExecutorError>
where
    F: Fn(
        &vibesql_ast::SelectStmt,
        &HashMap<String, CteResult>,
    ) -> Result<Vec<vibesql_storage::Row>, ExecutorError>,
    M: Fn(usize) -> Result<(), ExecutorError>,
{
    // Two CTEs sharing a name within the same WITH clause are rejected by SQLite
    // at prepare time, before any lazy pruning or body evaluation
    // (with1.test 3.2: `WITH tmp(a) AS (...), tmp(a) AS (...)`). The check is
    // scoped to this single list — a name may be redefined by a *nested* WITH in
    // a CTE body (with1.test 3.4), which arrives here as a separate list.
    {
        let mut seen: HashSet<String> = HashSet::with_capacity(ctes.len());
        for cte in ctes {
            if !seen.insert(cte.name.to_ascii_lowercase()) {
                return Err(ExecutorError::SqliteCompatError(format!(
                    "duplicate WITH table name: {}",
                    cte.name
                )));
            }
        }
    }

    let needed = match root {
        Some(stmt) => compute_needed_ctes(ctes, stmt),
        None => vec![true; ctes.len()],
    };

    // The outer LIMIT hint only applies to the specific CTE the outer statement
    // reads directly; identify it so the hint is not misapplied to a sibling.
    let hinted_cte: Option<String> = outer_row_hint.and(root).and_then(|stmt| match &stmt.from {
        Some(vibesql_ast::FromClause::Table { name, .. }) => Some(name.to_ascii_lowercase()),
        _ => None,
    });

    let mut states = vec![CteState::Pending; ctes.len()];
    let mut local = HashMap::new();

    // Process order: CTEs the root statement references directly come first
    // (in declaration order among them), then the rest. SQLite resolves CTEs
    // lazily from the query that reads them, so a circular reference is named
    // by the member closest to that query — starting resolution from the
    // root's direct references makes the reported name match (with1.test 3.1,
    // with2.test 3.4). When no root is provided (DML paths), this reduces to
    // plain declaration order.
    let process_order: Vec<usize> = {
        let mut root_refs = HashSet::new();
        if let Some(stmt) = root {
            collect_stmt_table_refs(stmt, &HashSet::new(), true, false, &mut root_refs);
        }
        let (mut first, mut rest): (Vec<usize>, Vec<usize>) = (Vec::new(), Vec::new());
        for (idx, cte) in ctes.iter().enumerate() {
            if root_refs.contains(&cte.name.to_ascii_lowercase()) {
                first.push(idx);
            } else {
                rest.push(idx);
            }
        }
        first.into_iter().chain(rest).collect()
    };

    // When a root statement is provided, `needed[]` has pruned this list to only
    // the CTEs the statement (transitively) references, so any CTE that reaches
    // execute_cte_at is genuinely referenced. DML paths pass no root and execute
    // every CTE eagerly, where an unreferenced CTE must NOT be validated
    // (SQLite never evaluates it — with1.test 1.2/1.4).
    let stmt_scoped = root.is_some();

    for idx in process_order {
        if needed[idx] && states[idx] == CteState::Pending {
            let hint =
                if hinted_cte.as_deref().is_some_and(|h| ctes[idx].name.eq_ignore_ascii_case(h)) {
                    outer_row_hint
                } else {
                    None
                };
            execute_cte_at(
                idx,
                ctes,
                &mut states,
                &mut local,
                outer_ctes,
                in_progress,
                nested,
                stmt_scoped,
                hint,
                database,
                executor,
                memory_check,
            )?;
        }
    }

    Ok(local)
}

/// Execute a single CTE from a list, resolving nested WITH clauses, sibling
/// dependencies (nested lists only), and circular references.
#[allow(clippy::too_many_arguments)]
fn execute_cte_at<F, M>(
    idx: usize,
    ctes: &[vibesql_ast::CommonTableExpr],
    states: &mut [CteState],
    local: &mut HashMap<String, CteResult>,
    outer_ctes: &HashMap<String, CteResult>,
    in_progress: &mut Vec<String>,
    nested: bool,
    stmt_scoped: bool,
    outer_row_hint: Option<usize>,
    database: &vibesql_storage::Database,
    executor: &F,
    memory_check: &M,
) -> Result<(), ExecutorError>
where
    F: Fn(
        &vibesql_ast::SelectStmt,
        &HashMap<String, CteResult>,
    ) -> Result<Vec<vibesql_storage::Row>, ExecutorError>,
    M: Fn(usize) -> Result<(), ExecutorError>,
{
    match states[idx] {
        CteState::Done => return Ok(()),
        CteState::InFlight => {
            // A sibling dependency chain looped back to a CTE we are already
            // executing: mutual recursion is not supported.
            return Err(ExecutorError::SqliteCompatError(format!(
                "circular reference: {}",
                ctes[idx].name
            )));
        }
        CteState::Pending => {}
    }
    states[idx] = CteState::InFlight;

    let cte = &ctes[idx];
    let name_lower = cte.name.to_ascii_lowercase();

    // Table names the body references, with names defined by the body's own
    // nested WITH clause (and deeper nesting) already shadowed out.
    let mut body_refs = HashSet::new();
    collect_stmt_table_refs(&cte.query, &HashSet::new(), false, false, &mut body_refs);

    // A WITH clause brings all its member names into scope at once (SQLite), so
    // a body may reference a sibling declared later in the list — at the top
    // level (with1.test 2.5) as well as in nested lists (with2.test 1.11).
    // Materialize referenced siblings first; a chain that loops back to a
    // sibling still InFlight is a mutual circular reference (with2.test
    // 3.2/3.3/3.4). SQLite names such a cycle by the referenced InFlight member,
    // which — because the list is processed starting from the CTEs the root
    // query reads directly (see execute_cte_list) — is the entry CTE closest to
    // the triggering query (with1.test 3.1, with2.test 3.4).
    for sib_idx in 0..ctes.len() {
        if sib_idx == idx || !body_refs.contains(&ctes[sib_idx].name.to_ascii_lowercase()) {
            continue;
        }
        match states[sib_idx] {
            CteState::Pending => {
                execute_cte_at(
                    sib_idx,
                    ctes,
                    states,
                    local,
                    outer_ctes,
                    in_progress,
                    nested,
                    stmt_scoped,
                    // A sibling materialized on demand is not the CTE the outer
                    // statement reads directly, so the outer LIMIT hint never
                    // applies to it.
                    None,
                    database,
                    executor,
                    memory_check,
                )?;
            }
            CteState::InFlight => {
                return Err(ExecutorError::SqliteCompatError(format!(
                    "circular reference: {}",
                    ctes[sib_idx].name
                )));
            }
            CteState::Done => {}
        }
    }

    // Circular reference detection through enclosing definitions: a body
    // reference resolves to sibling names and completed outer CTEs first. If it
    // can only resolve to an enclosing CTE whose definition is still being
    // executed, the reference is circular (with2.test 3.5).
    for enclosing in in_progress.iter() {
        let enclosing_lower = enclosing.to_ascii_lowercase();
        if enclosing_lower == name_lower || !body_refs.contains(&enclosing_lower) {
            continue;
        }
        let shadowed_by_sibling = ctes.iter().any(|c| c.name.eq_ignore_ascii_case(enclosing));
        let shadowed_by_outer = outer_ctes.keys().any(|k| k.eq_ignore_ascii_case(enclosing));
        if !shadowed_by_sibling && !shadowed_by_outer {
            return Err(ExecutorError::SqliteCompatError(format!(
                "circular reference: {}",
                enclosing
            )));
        }
    }

    // Names defined by the body's own nested WITH clause shadow the CTE's own
    // name: `WITH RECURSIVE t(a,b) AS (WITH t(x) AS (...) SELECT ... FROM t)`
    // is NOT recursive - the inner t wins (with1.test 21.1).
    let inner_shadow: HashSet<String> = cte
        .query
        .with_clause
        .as_ref()
        .map(|list| list.iter().map(|c| c.name.to_ascii_lowercase()).collect())
        .unwrap_or_default();
    let shadowed_by_inner = inner_shadow.contains(&name_lower);

    // Check if this is a recursive CTE.
    //
    // SQLite treats the RECURSIVE keyword as advisory, not mandatory: a CTE in
    // a `WITH RECURSIVE` list that does not actually reference itself is run as
    // an ordinary CTE (issue #5838, item 3). Classifying by self-reference
    // alone lets non-self-referential members of a RECURSIVE list — e.g. the
    // mandelbrot/sudoku showcase queries where only one CTE recurses — execute
    // instead of hard-erroring "must use UNION ALL". `is_cte_self_referential`
    // inspects the whole recursive term (including its subqueries), so genuine
    // recursion is still detected without the parser's per-CTE `recursive` flag.
    let is_recursive = !shadowed_by_inner && is_cte_self_referential(cte);

    // A recursive CTE may only reference itself in the recursive term; a
    // self-reference in the base term is circular (with1.test 17.3).
    if is_recursive {
        let mut base_refs = HashSet::new();
        collect_stmt_table_refs(&cte.query, &inner_shadow, true, true, &mut base_refs);
        if base_refs.contains(&name_lower) {
            return Err(ExecutorError::SqliteCompatError(format!(
                "circular reference: {}",
                cte.name
            )));
        }
    } else if !shadowed_by_inner && body_refs.contains(&name_lower) {
        // A non-recursive CTE whose body references its own name is circular:
        // the name is in scope within the body (shadowing any real table), but a
        // non-UNION self-reference cannot be materialized. SQLite reports
        // "circular reference: <name>" (with2.test 3.1, e.g.
        // `WITH i(x,y) AS (VALUES(1,(SELECT x FROM i)))`). Names redefined by
        // the body's own nested WITH (shadowed_by_inner) are excluded — those
        // resolve to the inner definition, not a self-reference.
        return Err(ExecutorError::SqliteCompatError(format!("circular reference: {}", cte.name)));
    }

    // Build the visible CTE context for this body: enclosing scopes first,
    // locally materialized siblings shadow them.
    let mut visible = outer_ctes.clone();
    for (name, result) in local.iter() {
        visible.insert(name.clone(), result.clone());
    }

    // When a CTE declares an explicit column list, SQLite validates its arity
    // against the CTE body's (leftmost/base term) output columns up front —
    // before evaluating the body or checking UNION-term consistency. A mismatch
    // is reported as `table <name> has <n> values for <m> columns`
    // (with1.test 5.6.1-5.6.4/5.6.7, with3.test 6.0/6.1). This static check
    // fires even when the body would produce no rows (with1.test 5.6.3) and
    // takes precedence over the "same number of result columns" UNION check
    // (with1.test 5.6.4/5.6.7). Wildcards are resolved against the visible CTE
    // context and catalog; if the base arity cannot be determined statically we
    // defer to the row-based check in derive_cte_schema.
    //
    // Only stmt-scoped lists (where `needed[]` proved this CTE is referenced)
    // are validated: SQLite never evaluates an unreferenced CTE, so a
    // `WITH x(a) AS (SELECT * FROM two_col_table) INSERT ...` where `x` is
    // unused must not raise an arity error (with1.test 1.2/1.4).
    if stmt_scoped {
        if let Some(declared) = &cte.columns {
            if let Some(base_cols) = collect_select_list_columns(&cte.query, database, &visible) {
                if base_cols.len() != declared.len() {
                    return Err(ExecutorError::SqliteCompatError(format!(
                        "table {} has {} values for {} columns",
                        cte.name,
                        base_cols.len(),
                        declared.len()
                    )));
                }
            }
        }
    }

    // Execute the body with this CTE's name marked in-progress so nested WITH
    // lists can detect circular references back to it.
    in_progress.push(cte.name.clone());
    let rows_result = execute_cte_body(
        cte,
        is_recursive,
        &mut visible,
        in_progress,
        outer_row_hint,
        database,
        executor,
        memory_check,
    );
    in_progress.pop();
    let rows = rows_result?;

    // Track memory for this CTE result before storing
    let estimated_size = super::helpers::estimate_result_size(&rows);
    memory_check(estimated_size)?;

    // Determine the schema for this CTE. Wildcards are expanded against the
    // full visible context, including nested-WITH CTEs (with1.test 17.2).
    let schema = derive_cte_schema(cte, &rows, database, &visible)?;

    // Store the CTE result wrapped in Arc for efficient sharing
    local.insert(cte.name.clone(), (schema, Arc::new(rows)));
    states[idx] = CteState::Done;
    Ok(())
}

/// Execute a CTE body: first materialize its own nested WITH clause (local
/// names shadow outer names), then run the body itself.
///
/// This is the core fix for issue #5838 (PR A): previously a WITH clause
/// nested inside a CTE body was silently ignored, so the body's references
/// resolved to outer CTEs or real tables instead of the nested CTEs.
#[allow(clippy::too_many_arguments)]
fn execute_cte_body<F, M>(
    cte: &vibesql_ast::CommonTableExpr,
    is_recursive: bool,
    visible: &mut HashMap<String, CteResult>,
    in_progress: &mut Vec<String>,
    outer_row_hint: Option<usize>,
    database: &vibesql_storage::Database,
    executor: &F,
    memory_check: &M,
) -> Result<Vec<vibesql_storage::Row>, ExecutorError>
where
    F: Fn(
        &vibesql_ast::SelectStmt,
        &HashMap<String, CteResult>,
    ) -> Result<Vec<vibesql_storage::Row>, ExecutorError>,
    M: Fn(usize) -> Result<(), ExecutorError>,
{
    if let Some(inner_list) = &cte.query.with_clause {
        // The body is the reachability root for its own WITH list: nested CTEs
        // the body never references are skipped (SQLite lazy expansion). The
        // outer LIMIT hint belongs to the enclosing statement's scope, not this
        // nested list, so it is not propagated inward.
        let snapshot = visible.clone();
        let inner_results = execute_cte_list(
            inner_list,
            Some(&cte.query),
            &snapshot,
            in_progress,
            true,
            None,
            database,
            executor,
            memory_check,
        )?;
        // Nested CTEs shadow outer CTEs and previously executed siblings.
        for (name, result) in inner_results {
            visible.insert(name, result);
        }
    }

    if is_recursive {
        // Recursive CTE: execute base term, then iteratively execute recursive term
        execute_recursive_cte(cte, visible, outer_row_hint, database, executor, memory_check)
    } else {
        // Non-recursive CTE: execute query directly
        executor(&cte.query, visible)
    }
}

/// Determine which CTEs in a list are (transitively) referenced by the root
/// statement.
///
/// SQLite expands CTEs lazily, so a `WITH` entry the statement never uses is
/// never evaluated (and errors inside it are never reported). The computation
/// starts from the names the root statement references (ignoring the WITH
/// clause itself) and follows references from the bodies of needed CTEs to a
/// fixpoint. Shadowing by deeper WITH clauses is respected by the collector,
/// so a reference to a name redefined in a nested scope does not mark the
/// outer CTE as needed.
fn compute_needed_ctes(
    ctes: &[vibesql_ast::CommonTableExpr],
    root: &vibesql_ast::SelectStmt,
) -> Vec<bool> {
    let names: Vec<String> = ctes.iter().map(|c| c.name.to_ascii_lowercase()).collect();

    // Seed: names referenced by the root statement itself. `skip_with` is set
    // because the root's WITH clause is the very list being filtered.
    let mut refs = HashSet::new();
    collect_stmt_table_refs(root, &HashSet::new(), true, false, &mut refs);

    let mut needed = vec![false; ctes.len()];
    loop {
        let mut changed = false;
        for (i, cte) in ctes.iter().enumerate() {
            if !needed[i] && refs.contains(&names[i]) {
                needed[i] = true;
                changed = true;
                // A needed CTE's body references contribute to reachability.
                collect_stmt_table_refs(&cte.query, &HashSet::new(), false, false, &mut refs);
            }
        }
        if !changed {
            break;
        }
    }
    needed
}

/// Collect (lowercased) table names referenced anywhere in a SELECT statement,
/// respecting WITH-clause shadowing: a name defined by a (nested) WITH clause
/// is not reported as a reference by the scopes it covers.
///
/// - `skip_with`: ignore the statement's own WITH clause entirely (used when the caller is
///   processing that clause itself).
/// - `skip_set_op`: ignore the statement's set operation (used to inspect only the base term of a
///   recursive CTE).
fn collect_stmt_table_refs(
    stmt: &vibesql_ast::SelectStmt,
    shadowed: &HashSet<String>,
    skip_with: bool,
    skip_set_op: bool,
    out: &mut HashSet<String>,
) {
    let owned_shadow;
    let shadow: &HashSet<String> = match (&stmt.with_clause, skip_with) {
        (Some(ctes), false) => {
            let mut extended = shadowed.clone();
            for cte in ctes {
                extended.insert(cte.name.to_ascii_lowercase());
            }
            // CTE bodies see all sibling names as shadowed (references between
            // siblings resolve within the list, not to enclosing scopes).
            for cte in ctes {
                collect_stmt_table_refs(&cte.query, &extended, false, false, out);
            }
            owned_shadow = extended;
            &owned_shadow
        }
        _ => shadowed,
    };

    if let Some(from) = &stmt.from {
        collect_from_table_refs(from, shadow, out);
    }
    for item in &stmt.select_list {
        if let vibesql_ast::SelectItem::Expression { expr, .. } = item {
            collect_expr_table_refs(expr, shadow, out);
        }
    }
    if let Some(where_clause) = &stmt.where_clause {
        collect_expr_table_refs(where_clause, shadow, out);
    }
    if let Some(group_by) = &stmt.group_by {
        for expr in group_by.all_expressions() {
            collect_expr_table_refs(expr, shadow, out);
        }
    }
    if let Some(having) = &stmt.having {
        collect_expr_table_refs(having, shadow, out);
    }
    if let Some(windows) = &stmt.window_definitions {
        for window in windows {
            if let Some(partition_by) = &window.spec.partition_by {
                for expr in partition_by {
                    collect_expr_table_refs(expr, shadow, out);
                }
            }
            if let Some(order_by) = &window.spec.order_by {
                for item in order_by {
                    collect_expr_table_refs(&item.expr, shadow, out);
                }
            }
        }
    }
    if let Some(order_by) = &stmt.order_by {
        for item in order_by {
            collect_expr_table_refs(&item.expr, shadow, out);
        }
    }
    if let Some(limit) = &stmt.limit {
        collect_expr_table_refs(limit, shadow, out);
    }
    if let Some(offset) = &stmt.offset {
        collect_expr_table_refs(offset, shadow, out);
    }
    if let Some(values_rows) = &stmt.values {
        for row in values_rows {
            for expr in row {
                collect_expr_table_refs(expr, shadow, out);
            }
        }
    }
    if !skip_set_op {
        if let Some(set_op) = &stmt.set_operation {
            collect_stmt_table_refs(&set_op.right, shadow, false, false, out);
        }
    }
}

/// Collect table names referenced by a FROM clause (shadow-aware).
fn collect_from_table_refs(
    from: &vibesql_ast::FromClause,
    shadowed: &HashSet<String>,
    out: &mut HashSet<String>,
) {
    match from {
        vibesql_ast::FromClause::Table { name, .. } => {
            let lower = name.to_ascii_lowercase();
            if !shadowed.contains(&lower) {
                out.insert(lower);
            }
        }
        vibesql_ast::FromClause::Subquery { query, .. } => {
            collect_stmt_table_refs(query, shadowed, false, false, out);
        }
        vibesql_ast::FromClause::Join { left, right, condition, .. } => {
            collect_from_table_refs(left, shadowed, out);
            collect_from_table_refs(right, shadowed, out);
            if let Some(cond) = condition {
                collect_expr_table_refs(cond, shadowed, out);
            }
        }
        vibesql_ast::FromClause::Values { rows, .. } => {
            for row in rows {
                for expr in row {
                    collect_expr_table_refs(expr, shadowed, out);
                }
            }
        }
        vibesql_ast::FromClause::TableFunction { args, .. } => {
            for expr in args {
                collect_expr_table_refs(expr, shadowed, out);
            }
        }
    }
}

/// Collect table names referenced inside an expression's subqueries
/// (shadow-aware). Subquery-bearing nodes are handled manually so their
/// statements are traversed with the correct shadow set; the generic walker
/// handles all other expression shapes.
fn collect_expr_table_refs(
    expr: &vibesql_ast::Expression,
    shadowed: &HashSet<String>,
    out: &mut HashSet<String>,
) {
    use vibesql_ast::visitor::{walk_expression, ExpressionVisitor, VisitResult};

    struct Collector<'a> {
        shadowed: &'a HashSet<String>,
        out: &'a mut HashSet<String>,
    }

    impl ExpressionVisitor for Collector<'_> {
        fn pre_visit_expression(&mut self, expr: &vibesql_ast::Expression) -> VisitResult {
            match expr {
                vibesql_ast::Expression::ScalarSubquery(query)
                | vibesql_ast::Expression::Exists { subquery: query, .. } => {
                    collect_stmt_table_refs(query, self.shadowed, false, false, self.out);
                    // Skip so the generic walker does not descend into the
                    // subquery with a stale shadow set.
                    VisitResult::Skip
                }
                vibesql_ast::Expression::In { expr: operand, subquery, .. }
                | vibesql_ast::Expression::QuantifiedComparison {
                    expr: operand, subquery, ..
                } => {
                    walk_expression(self, operand);
                    collect_stmt_table_refs(subquery, self.shadowed, false, false, self.out);
                    VisitResult::Skip
                }
                _ => VisitResult::Continue,
            }
        }
    }

    let mut collector = Collector { shadowed, out };
    walk_expression(&mut collector, expr);
}

/// Derive the schema for a CTE from its query and results
///
/// `database` and `prior_ctes` are used to statically expand wildcard SELECT
/// items (`*` / `t.*`) into the column names of the underlying FROM sources
/// (#5293). Without expansion, `WITH cte AS (SELECT * FROM t)` would
/// materialize a single `col{i}` column, silently dropping columns.
pub(super) fn derive_cte_schema(
    cte: &vibesql_ast::CommonTableExpr,
    rows: &[vibesql_storage::Row],
    database: &vibesql_storage::Database,
    prior_ctes: &HashMap<String, CteResult>,
) -> Result<vibesql_catalog::TableSchema, ExecutorError> {
    // If column names are explicitly specified, use those
    if let Some(column_names) = &cte.columns {
        // Get data types from first row (if available)
        if let Some(first_row) = rows.first() {
            if first_row.values.len() != column_names.len() {
                // SQLite's wording: `table <name> has <values> values for
                // <columns> columns` (with1.test 5.6.x, with3.test 6.0/6.1).
                // Reached only when the static pre-check in execute_cte_at could
                // not resolve the base arity (e.g. an exotic FROM source).
                return Err(ExecutorError::SqliteCompatError(format!(
                    "table {} has {} values for {} columns",
                    cte.name,
                    first_row.values.len(),
                    column_names.len()
                )));
            }

            let columns = column_names
                .iter()
                .zip(&first_row.values)
                .map(|(name, value)| {
                    let data_type = infer_type_from_value(value);
                    vibesql_catalog::ColumnSchema::new(name.clone(), data_type, true)
                    // nullable for
                    // simplicity
                })
                .collect();

            Ok(cte_pseudo_schema(cte.name.clone(), columns))
        } else {
            // Empty result set - create schema with VARCHAR columns
            let columns = column_names
                .iter()
                .map(|name| {
                    vibesql_catalog::ColumnSchema::new(
                        name.clone(),
                        vibesql_types::DataType::Varchar { max_length: Some(255) },
                        true,
                    )
                })
                .collect();

            Ok(cte_pseudo_schema(cte.name.clone(), columns))
        }
    } else if cte.query.values.is_some() {
        // A CTE whose body is a bare `VALUES (...)` clause has no select_list to
        // infer names from. SQLite auto-names such columns `column1`, `column2`,
        // ... so `WITH v AS (VALUES('a','b')) SELECT column1 FROM v` and
        // `SELECT * FROM v` resolve (values.test 8.1.*). Derive the width from
        // the materialized rows (falling back to the VALUES AST row width when
        // the result set is empty).
        let width = rows
            .first()
            .map(|r| r.values.len())
            .or_else(|| cte.query.values.as_ref().and_then(|vr| vr.first()).map(|r| r.len()))
            .unwrap_or(0);
        let columns = (0..width)
            .map(|i| {
                let data_type = rows
                    .first()
                    .and_then(|first_row| first_row.values.get(i))
                    .map(infer_type_from_value)
                    .unwrap_or(vibesql_types::DataType::Varchar { max_length: Some(255) });
                vibesql_catalog::ColumnSchema::new(format!("column{}", i + 1), data_type, true)
            })
            .collect();
        Ok(cte_pseudo_schema(cte.name.clone(), columns))
    } else {
        // No explicit column names - infer from query SELECT list.
        // Wildcard items are statically expanded into the column names of the
        // underlying FROM sources (#5293). A running value offset tracks the
        // position of each output column in the materialized rows so that
        // type inference stays aligned after expansion (e.g. `SELECT *, expr`
        // from a 2-column table puts `expr` at value index 2, not 1).
        let mut columns: Vec<vibesql_catalog::ColumnSchema> = Vec::new();
        let mut value_idx = 0usize;

        for (i, item) in cte.query.select_list.iter().enumerate() {
            // Determine the output column name(s) for this SELECT item
            let names: Vec<String> = match item {
                vibesql_ast::SelectItem::Wildcard { .. }
                | vibesql_ast::SelectItem::QualifiedWildcard { .. } => {
                    expand_wildcard_names(item, &cte.query, database, prior_ctes)
                        // Unresolvable FROM source (e.g. a view): fall back to
                        // the legacy positional name for this item
                        .unwrap_or_else(|| vec![format!("col{}", i)])
                }
                vibesql_ast::SelectItem::Expression { expr, alias, .. } => {
                    vec![if let Some(a) = alias {
                        a.clone()
                    } else {
                        // Try to extract name from expression
                        match expr {
                            vibesql_ast::Expression::ColumnRef(col_id) => {
                                col_id.column_canonical().to_string()
                            }
                            _ => format!("col{}", i),
                        }
                    }]
                }
            };

            for name in names {
                // Infer data type from first row if available, otherwise use default
                let data_type = rows
                    .first()
                    .and_then(|first_row| first_row.values.get(value_idx))
                    .map(infer_type_from_value)
                    .unwrap_or(vibesql_types::DataType::Varchar { max_length: Some(255) });

                columns.push(vibesql_catalog::ColumnSchema::new(name, data_type, true)); // nullable
                value_idx += 1;
            }
        }

        // Sanity check: if static expansion disagrees with the actual row
        // width, the resolution was wrong (e.g. an exotic FROM source).
        // Fall back to the legacy one-column-per-item naming rather than
        // exposing a schema that misattributes columns.
        if let Some(first_row) = rows.first() {
            if columns.len() != first_row.values.len() {
                return Ok(legacy_cte_schema(cte, rows));
            }
        }

        Ok(cte_pseudo_schema(cte.name.clone(), columns))
    }
}

/// Build a CTE pseudo-schema.
///
/// CTEs, like views and derived tables, have no implicit `rowid`: SQLite errors
/// with `no such column: rowid` when `rowid`/`oid`/`_rowid_` is referenced
/// against a CTE that does not explicitly declare such a column. We mark the
/// schema with `is_view = true` so the shared rowid-resolution paths (added in
/// #5492) reject the pseudo-column. A CTE column genuinely named `rowid` still
/// resolves, because real columns take precedence over the pseudo-column in
/// those paths. See issue #5516.
fn cte_pseudo_schema(
    name: String,
    columns: Vec<vibesql_catalog::ColumnSchema>,
) -> vibesql_catalog::TableSchema {
    let mut schema = vibesql_catalog::TableSchema::new(name, columns);
    schema.set_is_view(true);
    schema
}

/// Legacy schema derivation: one column per SELECT item, wildcards named
/// `col{i}`. Used only as a fallback when static wildcard expansion cannot
/// resolve the FROM sources or disagrees with the materialized row width.
fn legacy_cte_schema(
    cte: &vibesql_ast::CommonTableExpr,
    rows: &[vibesql_storage::Row],
) -> vibesql_catalog::TableSchema {
    let columns = cte
        .query
        .select_list
        .iter()
        .enumerate()
        .map(|(i, item)| {
            let data_type = rows
                .first()
                .and_then(|first_row| first_row.values.get(i))
                .map(infer_type_from_value)
                .unwrap_or(vibesql_types::DataType::Varchar { max_length: Some(255) });

            let col_name = match item {
                vibesql_ast::SelectItem::Wildcard { .. }
                | vibesql_ast::SelectItem::QualifiedWildcard { .. } => format!("col{}", i),
                vibesql_ast::SelectItem::Expression { expr, alias, .. } => {
                    if let Some(a) = alias {
                        a.clone()
                    } else {
                        match expr {
                            vibesql_ast::Expression::ColumnRef(col_id) => {
                                col_id.column_canonical().to_string()
                            }
                            _ => format!("col{}", i),
                        }
                    }
                }
            };

            vibesql_catalog::ColumnSchema::new(col_name, data_type, true) // nullable
        })
        .collect();

    cte_pseudo_schema(cte.name.clone(), columns)
}

/// A column of a FROM-clause source resolved for wildcard expansion.
///
/// `hidden_for_star` marks right-side NATURAL/USING join columns that are
/// deduplicated out of plain-`*` expansion. Qualified wildcards (`t.*`) keep
/// ALL of the source's columns, including hidden ones, matching SQLite
/// (`SELECT b.* FROM a NATURAL JOIN b` returns the join column too).
struct WildcardColumn {
    name: String,
    hidden_for_star: bool,
}

/// A FROM-clause source resolved for wildcard expansion: the effective
/// qualifier (alias if present, else table name) and its column names.
struct WildcardSource {
    qualifier: String,
    columns: Vec<WildcardColumn>,
}

/// Wrap plain column names as star-visible wildcard columns.
fn visible_columns(names: Vec<String>) -> Vec<WildcardColumn> {
    names.into_iter().map(|name| WildcardColumn { name, hidden_for_star: false }).collect()
}

/// Resolve a catalog view's output column names *statically*, without executing
/// its body.
///
/// Prefers the view's stored column list (populated at CREATE VIEW time). When
/// that is absent, derives the names from the view's defining query's SELECT
/// list (recursively expanding wildcards over its own sources). Returns `None`
/// when `name` is not a view or its columns cannot be determined statically.
fn view_column_names(name: &str, database: &vibesql_storage::Database) -> Option<Vec<String>> {
    let view = database.catalog.get_view(name)?;
    if let Some(cols) = &view.columns {
        return Some(cols.clone());
    }
    // Fall back to deriving from the view's defining query. No prior CTEs are in
    // scope for a stored view definition.
    collect_select_list_columns(&view.query, database, &HashMap::new())
}

/// Expand a wildcard SELECT item (`*` or `qualifier.*`) into column names
/// using the statement's FROM clause.
///
/// Returns `None` if any FROM source cannot be resolved statically (e.g. a
/// view); callers fall back to legacy `col{i}` naming.
fn expand_wildcard_names(
    item: &vibesql_ast::SelectItem,
    stmt: &vibesql_ast::SelectStmt,
    database: &vibesql_storage::Database,
    prior_ctes: &HashMap<String, CteResult>,
) -> Option<Vec<String>> {
    match item {
        vibesql_ast::SelectItem::Wildcard { alias } => {
            // SQL:1999 E051-07 derived column list: SELECT * AS (a, b, ...)
            if let Some(alias_names) = alias {
                return Some(alias_names.clone());
            }
            let sources = collect_from_sources(stmt.from.as_ref()?, database, prior_ctes)?;
            Some(
                sources
                    .into_iter()
                    .flat_map(|s| s.columns)
                    .filter(|c| !c.hidden_for_star)
                    .map(|c| c.name)
                    .collect(),
            )
        }
        vibesql_ast::SelectItem::QualifiedWildcard { qualifier, alias } => {
            if let Some(alias_names) = alias {
                return Some(alias_names.clone());
            }
            let sources = collect_from_sources(stmt.from.as_ref()?, database, prior_ctes)?;
            sources
                .into_iter()
                .find(|s| s.qualifier.eq_ignore_ascii_case(qualifier))
                // Qualified wildcards keep ALL of the source's columns,
                // including NATURAL/USING join columns hidden from plain `*`
                .map(|s| s.columns.into_iter().map(|c| c.name).collect())
        }
        vibesql_ast::SelectItem::Expression { .. } => None,
    }
}

/// Resolve the sources of a FROM clause to their column names for wildcard
/// expansion. Mirrors the traversal in
/// `evaluator::combined::subqueries::schema_utils::count_columns_in_from_clause`
/// but collects names instead of counts.
///
/// Returns `None` when a source cannot be resolved statically; callers fall
/// back to legacy naming rather than erroring.
fn collect_from_sources(
    from: &vibesql_ast::FromClause,
    database: &vibesql_storage::Database,
    prior_ctes: &HashMap<String, CteResult>,
) -> Option<Vec<WildcardSource>> {
    match from {
        vibesql_ast::FromClause::Table { name, alias, column_aliases, .. } => {
            // Check prior CTEs first (case-insensitive, matching the
            // resolution convention used elsewhere), then database tables
            let base_columns: Vec<String> = if let Some((schema, _)) =
                prior_ctes.get(name).or_else(|| {
                    prior_ctes.iter().find(|(k, _)| k.eq_ignore_ascii_case(name)).map(|(_, v)| v)
                }) {
                schema.columns.iter().map(|c| c.name.clone()).collect()
            } else if let Some(table) = database.get_table(name) {
                table.schema.columns.iter().map(|c| c.name.clone()).collect()
            } else if let Some(cols) = view_column_names(name, database) {
                // A catalog view whose output columns are known statically.
                // Resolving views from their stored column list (rather than
                // executing their bodies) is essential to keep deeply nested
                // views cheap: it lets CREATE VIEW derive columns without the
                // exponential re-materialization of doubling view nests
                // (#5394, view3.test).
                cols
            } else {
                // Unknown source we cannot resolve statically.
                return None;
            };

            // SQL:1999 E051-09: FROM t AS a(x, y) renames the columns
            let columns = match column_aliases {
                Some(aliases) if aliases.len() == base_columns.len() => aliases.clone(),
                Some(_) => return None, // mismatched rename list - bail out
                None => base_columns,
            };

            let qualifier = alias.clone().unwrap_or_else(|| name.clone());
            Some(vec![WildcardSource { qualifier, columns: visible_columns(columns) }])
        }
        vibesql_ast::FromClause::Join { left, right, natural, using_columns, alias, .. } => {
            // Non-goal: aliased parenthesized NATURAL/USING joins
            // (`(a JOIN b USING(k)) AS j`) hoist the USING columns to the
            // front under SQLite semantics (#4916). Static expansion does not
            // model that reordering, so fall back to legacy naming.
            if alias.is_some() && (*natural || using_columns.is_some()) {
                return None;
            }

            let mut sources = collect_from_sources(left, database, prior_ctes)?;
            let mut right_sources = collect_from_sources(right, database, prior_ctes)?;

            // NATURAL/USING joins deduplicate the shared columns out of
            // plain-`*` expansion: ALL left columns stay in declaration order
            // (join columns are NOT hoisted to the front), then the right
            // columns minus the shared ones. This mirrors the runtime
            // expansion in `select/projection.rs` (issue #4916 ordering).
            if *natural || using_columns.is_some() {
                let shared: Vec<String> = if let Some(using) = using_columns {
                    using.clone()
                } else {
                    // NATURAL: case-insensitive intersection of the left
                    // operand's star-visible names with the right operand's
                    // star-visible names. Using star-visible (already
                    // deduplicated) names makes chained NATURAL joins compute
                    // each join's shared set against the accumulated output.
                    let left_visible: Vec<&str> = sources
                        .iter()
                        .flat_map(|s| s.columns.iter())
                        .filter(|c| !c.hidden_for_star)
                        .map(|c| c.name.as_str())
                        .collect();
                    right_sources
                        .iter()
                        .flat_map(|s| s.columns.iter())
                        .filter(|c| !c.hidden_for_star)
                        .filter(|c| left_visible.iter().any(|l| l.eq_ignore_ascii_case(&c.name)))
                        .map(|c| c.name.clone())
                        .collect()
                };

                // Hide the shared columns on the right side only; they remain
                // resolvable through qualified wildcards (`b.*`).
                for source in &mut right_sources {
                    for col in &mut source.columns {
                        if shared.iter().any(|s| s.eq_ignore_ascii_case(&col.name)) {
                            col.hidden_for_star = true;
                        }
                    }
                }
            }

            sources.extend(right_sources);
            Some(sources)
        }
        vibesql_ast::FromClause::Subquery { query, alias, column_aliases } => {
            let columns = if let Some(aliases) = column_aliases {
                aliases.clone()
            } else {
                collect_select_list_columns(query, database, prior_ctes)?
            };
            Some(vec![WildcardSource {
                qualifier: alias.clone(),
                columns: visible_columns(columns),
            }])
        }
        vibesql_ast::FromClause::Values { rows, alias, column_aliases } => {
            let columns = if let Some(aliases) = column_aliases {
                aliases.clone()
            } else {
                let first_row = rows.first()?;
                (0..first_row.len()).map(|i| format!("col{}", i)).collect()
            };
            Some(vec![WildcardSource {
                qualifier: alias.clone(),
                columns: visible_columns(columns),
            }])
        }
        vibesql_ast::FromClause::TableFunction { alias, column_aliases, .. } => {
            // Only statically resolvable when both an alias and an explicit
            // column-alias list are present; otherwise fall back to legacy
            // naming (the function is not yet executable to derive columns).
            match (alias, column_aliases) {
                (Some(qualifier), Some(aliases)) => Some(vec![WildcardSource {
                    qualifier: qualifier.clone(),
                    columns: visible_columns(aliases.clone()),
                }]),
                _ => None,
            }
        }
    }
}

/// Statically compute a SELECT statement's output column names, expanding
/// wildcards (resolving tables, CTEs, and views from catalog metadata) without
/// executing the query.
///
/// For compound queries (UNION/INTERSECT/EXCEPT) the column names come from the
/// leftmost SELECT, matching SQL/SQLite semantics.
///
/// Used by CREATE VIEW to derive a view's columns cheaply, avoiding the
/// exponential re-materialization of deeply nested views (#5394, view3.test).
/// Returns `None` when names cannot be determined statically; callers then fall
/// back to executing the body.
pub(crate) fn try_static_select_columns(
    stmt: &vibesql_ast::SelectStmt,
    database: &vibesql_storage::Database,
) -> Option<Vec<String>> {
    collect_select_list_columns(stmt, database, &HashMap::new())
}

/// Compute the output column names of a SELECT statement, expanding any
/// wildcard items. Used to resolve subqueries appearing in a FROM clause.
///
/// Returns `None` if names cannot be determined statically.
fn collect_select_list_columns(
    stmt: &vibesql_ast::SelectStmt,
    database: &vibesql_storage::Database,
    prior_ctes: &HashMap<String, CteResult>,
) -> Option<Vec<String>> {
    // VALUES statement: names come from the first row's width
    if let Some(values_rows) = &stmt.values {
        let first_row = values_rows.first()?;
        return Some((0..first_row.len()).map(|i| format!("col{}", i)).collect());
    }

    let mut names = Vec::new();
    for (i, item) in stmt.select_list.iter().enumerate() {
        match item {
            vibesql_ast::SelectItem::Wildcard { .. }
            | vibesql_ast::SelectItem::QualifiedWildcard { .. } => {
                names.extend(expand_wildcard_names(item, stmt, database, prior_ctes)?);
            }
            vibesql_ast::SelectItem::Expression { expr, alias, .. } => {
                names.push(if let Some(a) = alias {
                    a.clone()
                } else {
                    match expr {
                        vibesql_ast::Expression::ColumnRef(col_id) => {
                            col_id.column_canonical().to_string()
                        }
                        _ => format!("col{}", i),
                    }
                });
            }
        }
    }
    Some(names)
}

/// Execute a recursive CTE using iterative evaluation.
///
/// Recursive CTEs in SQL:1999/SQLite are defined with UNION or UNION ALL:
/// ```sql
/// WITH RECURSIVE cte AS (
///   base_query          -- Executed once to get initial rows
///   UNION [ALL]
///   recursive_query     -- References 'cte', executed iteratively
/// )
/// ```
///
/// SQLite evaluates this with a **queue** (see <https://sqlite.org/lang_with.html>):
/// base rows seed the queue, then rows are pulled one at a time, the recursive
/// term is run against the pulled row, and the produced rows are appended to the
/// queue and (for UNION) deduplicated. Two ordering disciplines matter:
///
/// - **No `ORDER BY` on the recursive term** — the queue is FIFO, giving a breadth-first traversal.
///   This is the common case and is handled by an efficient *frontier* loop that expands a whole
///   level per iteration.
/// - **`ORDER BY` on the recursive term** — the queue becomes a priority queue: the row that sorts
///   first is pulled next, so the result is emitted in a global sorted order across all levels.
///   `ORDER BY … DESC` on a depth column yields depth-first search, ascending yields breadth-first
///   (with1.test 10.3–10.6, 11.1–11.3). This path pulls one row at a time.
///
/// A `LIMIT`/`OFFSET` written after the `UNION ALL` caps and windows the *total*
/// CTE result (with1.test 5.3, 5.2.3), and an `outer_row_hint` (an outer `LIMIT`
/// over a directly-referenced CTE) lets an otherwise infinite/circular CTE
/// terminate early (with1.test 5.1, 5.4).
fn execute_recursive_cte<F, M>(
    cte: &vibesql_ast::CommonTableExpr,
    cte_results: &HashMap<String, CteResult>,
    outer_row_hint: Option<usize>,
    database: &vibesql_storage::Database,
    executor: &F,
    memory_check: &M,
) -> Result<Vec<vibesql_storage::Row>, ExecutorError>
where
    F: Fn(
        &vibesql_ast::SelectStmt,
        &HashMap<String, CteResult>,
    ) -> Result<Vec<vibesql_storage::Row>, ExecutorError>,
    M: Fn(usize) -> Result<(), ExecutorError>,
{
    use crate::limits::MAX_RECURSIVE_CTE_ITERATIONS;

    // Validate that recursive CTE uses UNION ALL
    let set_op = cte.query.set_operation.as_ref().ok_or_else(|| {
        ExecutorError::UnsupportedFeature(format!(
            "Recursive CTE '{}' must use UNION ALL",
            cte.name
        ))
    })?;

    if set_op.op != vibesql_ast::SetOperator::Union {
        return Err(ExecutorError::UnsupportedFeature(format!(
            "Recursive CTE '{}' must use UNION or UNION ALL (not INTERSECT or EXCEPT)",
            cte.name
        )));
    }

    // Extract base and recursive terms
    // Base term: the main SELECT (before UNION [ALL])
    // Recursive term: the right side of UNION [ALL]

    // ORDER BY / LIMIT / OFFSET written after the `UNION ALL` bind to the
    // *compound* query (the parser stores them on `cte.query`, not on the base
    // or recursive terms). For a recursive CTE these are queue directives — the
    // ORDER BY controls the priority-queue traversal, and LIMIT/OFFSET cap and
    // window the total result — so they must NOT be applied to the base term or
    // re-applied on each recursive iteration. We interpret them here and strip
    // them from the base term.
    let recursive_order_by = cte.query.order_by.clone();

    // Create base-only query without the UNION ALL set operation
    // This prevents the base term from trying to reference the CTE before it exists
    let base_query = vibesql_ast::SelectStmt {
        with_clause: cte.query.with_clause.clone(),
        distinct: cte.query.distinct,
        select_list: cte.query.select_list.clone(),
        into_table: cte.query.into_table.clone(),
        into_variables: cte.query.into_variables.clone(),
        from: cte.query.from.clone(),
        where_clause: cte.query.where_clause.clone(),
        group_by: cte.query.group_by.clone(),
        having: cte.query.having.clone(),
        window_definitions: cte.query.window_definitions.clone(),
        // Compound-level ORDER BY/LIMIT/OFFSET do not apply to the base term.
        order_by: None,
        limit: None,
        offset: None,
        set_operation: None, // Remove UNION ALL for base term execution
        values: cte.query.values.clone(),
    };

    // The recursive term drives per-iteration expansion; execute it as written
    // (it already carries no compound-level ORDER BY/LIMIT/OFFSET).
    let recursive_query = &set_op.right;

    // SQLite compatibility: window functions are not allowed in the recursive
    // part of a recursive CTE (window1.test 15.0). SQLite reports the exact
    // error "cannot use window functions in recursive queries".
    if crate::select::window::has_window_functions(&recursive_query.select_list) {
        return Err(ExecutorError::SqliteCompatError(
            "cannot use window functions in recursive queries".to_string(),
        ));
    }

    // SQLite requires a recursive term to reference the CTE exactly once, and
    // only as a base table in its FROM clause. Two shapes are rejected before
    // any rows are produced (with1.test 7.4/7.5):
    //   - the sole reference is buried in a subquery (no direct FROM ref)
    //     -> "circular reference: <name>" (7.4:
    //     `... FROM tree WHERE p IN (SELECT id FROM t)`);
    //   - more than one reference — a FROM ref plus a subquery ref, or the
    //     name twice in FROM -> "multiple recursive references: <name>" (7.5:
    //     `... FROM tree, t WHERE p=id AND p IN (SELECT id FROM t)`).
    // Only this leading recursive term is inspected; a compound recursive term
    // (`... UNION R1 UNION R2`, with5.test 110-112) keeps its sibling terms in
    // its own set-operation chain, which the counters deliberately do not walk,
    // so each legitimately references the CTE once.
    {
        let self_name = &cte.name;
        let from_refs = recursive_query
            .from
            .as_ref()
            .map_or(0, |from| count_from_table_occurrences(from, self_name));
        let indirect = recursive_term_has_indirect_ref(recursive_query, self_name);
        if from_refs > 1 || (from_refs >= 1 && indirect) {
            return Err(ExecutorError::SqliteCompatError(format!(
                "multiple recursive references: {}",
                self_name
            )));
        }
        if from_refs == 0 && indirect {
            return Err(ExecutorError::SqliteCompatError(format!(
                "circular reference: {}",
                self_name
            )));
        }
    }

    // Try static validation first (works for explicit column lists and VALUES)
    // This provides better SQLite compatibility by catching errors at prepare time
    // rather than waiting until runtime
    // Note: For VALUES statements, column count comes from the VALUES rows, not select_list
    if let (Some(base_count), Some(recursive_count)) =
        (count_stmt_columns(&base_query), count_stmt_columns(recursive_query))
    {
        if base_count != recursive_count {
            // SQLite reports this verbatim (no "Unsupported feature:" prefix)
            // for a recursive CTE whose terms disagree in arity
            // (with1.test 5.6.6).
            return Err(ExecutorError::SqliteCompatError(
                "SELECTs to the left and right of UNION ALL do not have the same number of result columns".to_string()
            ));
        }
    }
    // Fall back to runtime validation for wildcards (existing code below)

    // A LIMIT/OFFSET after the UNION ALL caps and windows the TOTAL CTE result —
    // base rows included (with1.test 5.3: `... LIMIT 5` yields exactly 5 rows
    // counting the base row; `LIMIT 0` yields none, not even the base row).
    // Resolve them (and the outer LIMIT hint) to a single "produce at most
    // `total_cap` rows, then discard the first `output_offset`" pair.
    let term_limit = cte.query.limit.as_ref().and_then(|e| eval_const_count(e, database));
    let term_offset =
        cte.query.offset.as_ref().and_then(|e| eval_const_count(e, database)).unwrap_or(0);
    // Rows produced (before OFFSET is applied) that we must generate.
    let mut total_cap = term_limit.map(|l| l.saturating_add(term_offset));
    if let Some(hint) = outer_row_hint {
        total_cap =
            Some(total_cap.map_or(hint, |c: usize| c.min(hint.saturating_add(term_offset))));
    }
    // Rows to drop from the front of the emitted result (recursive-term OFFSET).
    let output_offset = term_offset;

    // Step 1: Execute base term to get initial rows
    let mut base_rows = executor(&base_query, cte_results)?;

    // Derive schema from base term result
    // Wildcards in the base term are expanded against database tables and
    // prior CTEs (#5293)
    let schema = derive_cte_schema(cte, &base_rows, database, cte_results)?;

    // Track seen rows for UNION (deduplication)
    // For UNION ALL, we skip tracking to preserve all rows
    let mut seen_rows: Option<HashSet<vibesql_storage::RowValues>> = if !set_op.all {
        let mut seen = HashSet::with_capacity(base_rows.len());
        // For plain UNION, SQLite also deduplicates the base term itself, not
        // just recursive-term rows (issue #5838, item 7; with1.test 26.2).
        // Drop duplicate seed rows so the result and the working table start
        // deduplicated.
        base_rows.retain(|row| seen.insert(row.values.clone()));
        Some(seen)
    } else {
        None
    };

    // Resolve each ORDER BY term to an output-column index (positional or by
    // alias/column name). `None` means "no priority ordering" (FIFO). An
    // unresolvable term is a name-resolution error (with1.test 10.7.1).
    let order_indices = match &recursive_order_by {
        Some(items) => Some(resolve_recursive_order_indices(items, &set_op.right, &base_query)?),
        None => None,
    };

    // Helper to run one expansion of the recursive term against a working set.
    let expand =
        |working: &[vibesql_storage::Row]| -> Result<Vec<vibesql_storage::Row>, ExecutorError> {
            let mut recursive_cte_results = cte_results.clone();
            recursive_cte_results
                .insert(cte.name.clone(), (schema.clone(), Arc::new(working.to_vec())));
            executor(recursive_query, &recursive_cte_results)
        };

    let all_rows = if let (Some(order_by), Some(indices)) =
        (recursive_order_by.as_ref(), order_indices.as_ref())
    {
        // Priority-queue traversal: pull one row at a time in sorted order.
        execute_recursive_queue(
            base_rows,
            &expand,
            order_by,
            indices,
            &mut seen_rows,
            total_cap,
            memory_check,
        )?
    } else {
        // FIFO frontier traversal (breadth-first): expand a whole level at once.
        execute_recursive_frontier(
            base_rows,
            &expand,
            &mut seen_rows,
            total_cap,
            memory_check,
            MAX_RECURSIVE_CTE_ITERATIONS,
            &cte.name,
        )?
    };

    // Apply the recursive-term OFFSET as a window over the emitted result.
    let all_rows = if output_offset > 0 {
        all_rows.into_iter().skip(output_offset).collect()
    } else {
        all_rows
    };

    Ok(all_rows)
}

/// FIFO (breadth-first) recursive-CTE traversal.
///
/// Expands an entire frontier per iteration, which is equivalent to a FIFO queue
/// but far cheaper than pulling rows one at a time. Terminates when the frontier
/// is empty, when `total_cap` rows have been produced, or (as a safety net for
/// genuinely unbounded CTEs with no cap) when the iteration limit is reached.
#[allow(clippy::too_many_arguments)]
fn execute_recursive_frontier<E, M>(
    base_rows: Vec<vibesql_storage::Row>,
    expand: &E,
    seen_rows: &mut Option<HashSet<vibesql_storage::RowValues>>,
    total_cap: Option<usize>,
    memory_check: &M,
    max_iterations: usize,
    cte_name: &str,
) -> Result<Vec<vibesql_storage::Row>, ExecutorError>
where
    E: Fn(&[vibesql_storage::Row]) -> Result<Vec<vibesql_storage::Row>, ExecutorError>,
    M: Fn(usize) -> Result<(), ExecutorError>,
{
    let mut all_rows = base_rows.clone();
    let mut working_table = base_rows;

    // Honor a cap on the base (seed) rows too: `LIMIT n` counts them.
    if let Some(cap) = total_cap {
        if all_rows.len() >= cap {
            all_rows.truncate(cap);
            return Ok(all_rows);
        }
    }

    let mut depth = 0;
    while !working_table.is_empty() {
        // With no cap, an unbounded CTE would loop forever; bound it.
        if total_cap.is_none() && depth >= max_iterations {
            return Err(ExecutorError::UnsupportedFeature(format!(
                "Recursive CTE '{}' exceeded maximum iteration limit of {}",
                cte_name, max_iterations
            )));
        }
        depth += 1;

        let new_rows = expand(&working_table)?;
        if new_rows.is_empty() {
            break;
        }

        let estimated_size = super::helpers::estimate_result_size(&new_rows);
        memory_check(estimated_size)?;

        // Filter out duplicates for UNION (keep all for UNION ALL)
        let rows_to_add: Vec<vibesql_storage::Row> = if let Some(seen) = seen_rows.as_mut() {
            new_rows.into_iter().filter(|row| seen.insert(row.values.clone())).collect()
        } else {
            new_rows
        };

        if rows_to_add.is_empty() {
            break;
        }

        all_rows.extend(rows_to_add.clone());
        working_table = rows_to_add;

        if let Some(cap) = total_cap {
            if all_rows.len() >= cap {
                all_rows.truncate(cap);
                break;
            }
        }
    }

    Ok(all_rows)
}

/// Priority-queue recursive-CTE traversal (ORDER BY on the recursive term).
///
/// Implements SQLite's queue model exactly: the smallest-sorting queued row is
/// pulled, emitted to the result, and expanded; produced rows are appended to
/// the queue. The emission order is therefore a global sorted order across all
/// recursion levels. `ORDER BY … DESC` yields depth-first, ASC yields
/// breadth-first (with1.test 10.3–10.6, 11.1–11.3, 5.2.x).
fn execute_recursive_queue<E, M>(
    base_rows: Vec<vibesql_storage::Row>,
    expand: &E,
    order_by: &[vibesql_ast::OrderByItem],
    order_indices: &[usize],
    seen_rows: &mut Option<HashSet<vibesql_storage::RowValues>>,
    total_cap: Option<usize>,
    memory_check: &M,
) -> Result<Vec<vibesql_storage::Row>, ExecutorError>
where
    E: Fn(&[vibesql_storage::Row]) -> Result<Vec<vibesql_storage::Row>, ExecutorError>,
    M: Fn(usize) -> Result<(), ExecutorError>,
{
    // The queue holds rows not yet expanded, kept sorted so the front is the
    // next row to pull. Base rows seed the queue in base order.
    let mut queue: Vec<vibesql_storage::Row> = base_rows;
    let mut result: Vec<vibesql_storage::Row> = Vec::new();

    // Sort the queue by the ORDER BY terms, reading values at the resolved
    // output-column positions. A stable sort preserves insertion order among
    // equal keys, matching SQLite's FIFO tie-break.
    let sort_queue = |queue: &mut Vec<vibesql_storage::Row>| {
        queue.sort_by(|a, b| {
            for (term, &idx) in order_by.iter().zip(order_indices.iter()) {
                let va = a.values.get(idx).unwrap_or(&vibesql_types::SqlValue::Null);
                let vb = b.values.get(idx).unwrap_or(&vibesql_types::SqlValue::Null);
                let ord = super::grouping::compare_sql_values(va, vb);
                let ord = match term.direction {
                    vibesql_ast::OrderDirection::Asc => ord,
                    vibesql_ast::OrderDirection::Desc => ord.reverse(),
                };
                if ord != std::cmp::Ordering::Equal {
                    return ord;
                }
            }
            std::cmp::Ordering::Equal
        });
    };

    sort_queue(&mut queue);

    while !queue.is_empty() {
        if let Some(cap) = total_cap {
            if result.len() >= cap {
                break;
            }
        }

        // Pull the smallest-sorting row (front of the sorted queue).
        let row = queue.remove(0);
        result.push(row.clone());

        // Expand it, dedup for UNION, then merge into the queue and re-sort so
        // the next pull respects the global order.
        let new_rows = expand(std::slice::from_ref(&row))?;
        if new_rows.is_empty() {
            continue;
        }
        let estimated_size = super::helpers::estimate_result_size(&new_rows);
        memory_check(estimated_size)?;

        let to_queue: Vec<vibesql_storage::Row> = if let Some(seen) = seen_rows.as_mut() {
            new_rows.into_iter().filter(|r| seen.insert(r.values.clone())).collect()
        } else {
            new_rows
        };
        if !to_queue.is_empty() {
            queue.extend(to_queue);
            sort_queue(&mut queue);
        }
    }

    if let Some(cap) = total_cap {
        result.truncate(cap);
    }
    Ok(result)
}

/// Resolve each ORDER BY term of a recursive CTE's recursive term to a 0-based
/// output-column index.
///
/// SQLite resolves the ORDER BY against the compound query's result columns:
/// - A positive integer literal `N` selects output column `N` (1-based).
/// - A bare name matches a SELECT-list alias/column of the recursive term or the base term
///   (with1.test 10.7.2 uses the base-term alias `b`, 10.7.3 the recursive-term alias `c`). A name
///   that matches neither is an error (with1.test 10.7.1: `ORDER BY a` where the output is aliased
///   `b`/`c`).
fn resolve_recursive_order_indices(
    order_by: &[vibesql_ast::OrderByItem],
    recursive_term: &vibesql_ast::SelectStmt,
    base_query: &vibesql_ast::SelectStmt,
) -> Result<Vec<usize>, ExecutorError> {
    let col_count = recursive_term.select_list.len();
    let mut indices = Vec::with_capacity(order_by.len());
    for (term_index, item) in order_by.iter().enumerate() {
        // A COLLATE wrapper attaches a collation to the sort key but does not
        // change which column the term selects: `ORDER BY 3 COLLATE nocase`
        // still orders by output column 3 (with1.test 10.8.1). Unwrap it before
        // position/name resolution. (The collation itself is not yet applied in
        // the priority-queue comparator.)
        let expr = match &item.expr {
            vibesql_ast::Expression::Collate { expr, .. } => expr.as_ref(),
            other => other,
        };
        match super::order::extract_column_position(expr) {
            super::order::ColumnPositionResult::Position(pos) => {
                indices.push(super::order::validate_column_position(pos, col_count, term_index)?);
            }
            super::order::ColumnPositionResult::Negative(pos) => {
                return Err(ExecutorError::OrderByOutOfRange {
                    term_position: term_index + 1,
                    column_number: pos,
                    select_list_len: col_count,
                });
            }
            super::order::ColumnPositionResult::NotAPosition => {
                // Resolve a bare column name against the recursive and base
                // select-list output aliases.
                let name = match expr {
                    vibesql_ast::Expression::ColumnRef(col) => {
                        Some(col.column_canonical().to_string())
                    }
                    _ => None,
                };
                let idx = name.as_deref().and_then(|n| {
                    select_output_index(&recursive_term.select_list, n)
                        .or_else(|| select_output_index(&base_query.select_list, n))
                });
                match idx {
                    Some(i) => indices.push(i),
                    None => {
                        return Err(ExecutorError::SqliteCompatError(format!(
                            "{}{} ORDER BY term does not match any column in the result set",
                            term_index + 1,
                            ordinal_suffix(term_index + 1),
                        )));
                    }
                }
            }
        }
    }
    Ok(indices)
}

/// Find the 0-based output index of a SELECT-list item whose alias or bare
/// column name matches `name` (case-insensitive).
fn select_output_index(select_list: &[vibesql_ast::SelectItem], name: &str) -> Option<usize> {
    for (i, item) in select_list.iter().enumerate() {
        if let vibesql_ast::SelectItem::Expression { expr, alias, .. } = item {
            if let Some(a) = alias {
                if a.eq_ignore_ascii_case(name) {
                    return Some(i);
                }
            }
            if let vibesql_ast::Expression::ColumnRef(col) = expr {
                if col.column_canonical().eq_ignore_ascii_case(name) {
                    return Some(i);
                }
            }
        }
    }
    None
}

/// English ordinal suffix for the SQLite ORDER BY error message ("1st", "2nd").
fn ordinal_suffix(n: usize) -> &'static str {
    match (n % 10, n % 100) {
        (1, 11) | (2, 12) | (3, 13) => "th",
        (1, _) => "st",
        (2, _) => "nd",
        (3, _) => "rd",
        _ => "th",
    }
}

/// Count columns if select list has only explicit expressions (no wildcards)
///
/// Returns Some(count) if all select items are explicit expressions.
/// Returns None if any wildcards are present (requires schema info to count).
fn count_explicit_columns(select_list: &[vibesql_ast::SelectItem]) -> Option<usize> {
    let mut count = 0;
    for item in select_list {
        match item {
            vibesql_ast::SelectItem::Expression { .. } => count += 1,
            // Can't count wildcards statically - need schema info
            vibesql_ast::SelectItem::Wildcard { .. }
            | vibesql_ast::SelectItem::QualifiedWildcard { .. } => {
                return None;
            }
        }
    }
    Some(count)
}

/// Count columns in a SELECT statement, considering both select_list and VALUES.
/// For VALUES statements, the column count comes from the first row of values.
/// For SELECT statements, the column count comes from the select_list.
/// Returns None if any wildcards are present (requires schema info to count).
fn count_stmt_columns(stmt: &vibesql_ast::SelectStmt) -> Option<usize> {
    // If this is a VALUES statement, count columns from the first VALUES row
    if let Some(values_rows) = &stmt.values {
        return values_rows.first().map(|row| row.len());
    }

    // Otherwise, count columns from the select_list
    count_explicit_columns(&stmt.select_list)
}

/// Check if a CTE is self-referential (references itself in UNION/UNION ALL)
///
/// SQLite allows recursive CTEs without the RECURSIVE keyword if the CTE
/// references itself in a set operation. This function detects such cases
/// by checking if the right side of a UNION/UNION ALL references the CTE name.
fn is_cte_self_referential(cte: &vibesql_ast::CommonTableExpr) -> bool {
    // A CTE is recursive only if it has a compound set operation whose right
    // (recursive) term references the CTE itself. The specific operator is not
    // checked here: a self-referential INTERSECT/EXCEPT is still classified
    // recursive so `execute_recursive_cte` can report the precise "must use
    // UNION or UNION ALL" error rather than a generic "table not found". A
    // `WITH RECURSIVE` CTE with no set operation (issue #5838, item 3) is not
    // self-referential and runs as an ordinary CTE.
    let set_op = match &cte.query.set_operation {
        Some(op) => op,
        None => return false,
    };

    // Check if the recursive term references this CTE
    stmt_references_table(&set_op.right, &cte.name)
}

/// Count how many times `name` appears as a direct base table in a FROM clause,
/// descending through joins but NOT into FROM-clause subqueries. Used to tell a
/// well-formed single FROM-clause recursive self-reference apart from references
/// buried elsewhere (with1.test 7.4/7.5).
fn count_from_table_occurrences(from: &vibesql_ast::FromClause, name: &str) -> usize {
    match from {
        vibesql_ast::FromClause::Table { name: t, .. } => usize::from(t.eq_ignore_ascii_case(name)),
        vibesql_ast::FromClause::Join { left, right, .. } => {
            count_from_table_occurrences(left, name) + count_from_table_occurrences(right, name)
        }
        _ => 0,
    }
}

/// True when `name` is referenced anywhere in a recursive term OTHER than a
/// direct FROM-clause base table: a FROM-clause subquery, a JOIN-condition
/// subquery, or a subquery in WHERE / the SELECT list / HAVING. Does not walk
/// the statement's own set-operation chain, so sibling terms of a compound
/// recursive term are not counted here.
fn recursive_term_has_indirect_ref(stmt: &vibesql_ast::SelectStmt, name: &str) -> bool {
    if let Some(from) = &stmt.from {
        if from_clause_has_indirect_ref(from, name) {
            return true;
        }
    }
    if let Some(where_clause) = &stmt.where_clause {
        if expr_references_table(where_clause, name) {
            return true;
        }
    }
    for item in &stmt.select_list {
        if let vibesql_ast::SelectItem::Expression { expr, .. } = item {
            if expr_references_table(expr, name) {
                return true;
            }
        }
    }
    if let Some(having) = &stmt.having {
        if expr_references_table(having, name) {
            return true;
        }
    }
    false
}

/// True when `name` is referenced through a FROM-clause subquery or a
/// JOIN-condition subquery (a direct base-table match is NOT an indirect ref).
fn from_clause_has_indirect_ref(from: &vibesql_ast::FromClause, name: &str) -> bool {
    match from {
        vibesql_ast::FromClause::Table { .. } => false,
        vibesql_ast::FromClause::Subquery { query, .. } => stmt_references_table(query, name),
        vibesql_ast::FromClause::Join { left, right, condition, .. } => {
            from_clause_has_indirect_ref(left, name)
                || from_clause_has_indirect_ref(right, name)
                || condition.as_ref().is_some_and(|c| expr_references_table(c, name))
        }
        vibesql_ast::FromClause::Values { .. } => false,
        vibesql_ast::FromClause::TableFunction { args, .. } => {
            args.iter().any(|expr| expr_references_table(expr, name))
        }
    }
}

/// Check if a SELECT statement references a table name
fn stmt_references_table(stmt: &vibesql_ast::SelectStmt, table_name: &str) -> bool {
    // Check FROM clause
    if let Some(from) = &stmt.from {
        if from_clause_references_table(from, table_name) {
            return true;
        }
    }

    // Check WHERE clause for subqueries
    if let Some(where_clause) = &stmt.where_clause {
        if expr_references_table(where_clause, table_name) {
            return true;
        }
    }

    // Check SELECT list for subqueries
    for item in &stmt.select_list {
        if let vibesql_ast::SelectItem::Expression { expr, .. } = item {
            if expr_references_table(expr, table_name) {
                return true;
            }
        }
    }

    false
}

/// Check if a FROM clause references a table name
fn from_clause_references_table(from: &vibesql_ast::FromClause, table_name: &str) -> bool {
    match from {
        vibesql_ast::FromClause::Table { name, .. } => name.eq_ignore_ascii_case(table_name),
        vibesql_ast::FromClause::Subquery { query, .. } => stmt_references_table(query, table_name),
        vibesql_ast::FromClause::Join { left, right, condition, .. } => {
            from_clause_references_table(left, table_name)
                || from_clause_references_table(right, table_name)
                || condition.as_ref().map_or(false, |c| expr_references_table(c, table_name))
        }
        vibesql_ast::FromClause::Values { .. } => false,
        vibesql_ast::FromClause::TableFunction { args, .. } => {
            args.iter().any(|expr| expr_references_table(expr, table_name))
        }
    }
}

/// Check if an expression references a table name (in subqueries)
fn expr_references_table(expr: &vibesql_ast::Expression, table_name: &str) -> bool {
    match expr {
        vibesql_ast::Expression::ScalarSubquery(subquery) => {
            stmt_references_table(subquery, table_name)
        }
        vibesql_ast::Expression::In { subquery, .. } => stmt_references_table(subquery, table_name),
        vibesql_ast::Expression::Exists { subquery, .. } => {
            stmt_references_table(subquery, table_name)
        }
        vibesql_ast::Expression::BinaryOp { left, right, .. } => {
            expr_references_table(left, table_name) || expr_references_table(right, table_name)
        }
        vibesql_ast::Expression::UnaryOp { expr, .. } => expr_references_table(expr, table_name),
        vibesql_ast::Expression::Function { args, .. } => {
            args.iter().any(|arg| expr_references_table(arg, table_name))
        }
        vibesql_ast::Expression::AggregateFunction { args, filter, .. } => {
            args.iter().any(|arg| expr_references_table(arg, table_name))
                || filter.as_ref().map_or(false, |f| expr_references_table(f, table_name))
        }
        vibesql_ast::Expression::Case { operand, when_clauses, else_result, .. } => {
            operand.as_ref().map_or(false, |o| expr_references_table(o, table_name))
                || when_clauses.iter().any(|when| {
                    when.conditions.iter().any(|c| expr_references_table(c, table_name))
                        || expr_references_table(&when.result, table_name)
                })
                || else_result.as_ref().map_or(false, |e| expr_references_table(e, table_name))
        }
        vibesql_ast::Expression::Between { expr, low, high, .. } => {
            expr_references_table(expr, table_name)
                || expr_references_table(low, table_name)
                || expr_references_table(high, table_name)
        }
        vibesql_ast::Expression::InList { expr, values, .. } => {
            expr_references_table(expr, table_name)
                || values.iter().any(|e| expr_references_table(e, table_name))
        }
        vibesql_ast::Expression::Cast { expr, .. }
        | vibesql_ast::Expression::Collate { expr, .. } => expr_references_table(expr, table_name),
        vibesql_ast::Expression::Conjunction(exprs)
        | vibesql_ast::Expression::Disjunction(exprs) => {
            exprs.iter().any(|e| expr_references_table(e, table_name))
        }
        vibesql_ast::Expression::QuantifiedComparison { expr, subquery, .. } => {
            expr_references_table(expr, table_name) || stmt_references_table(subquery, table_name)
        }
        _ => false,
    }
}

/// Infer data type from a SQL value
pub(super) fn infer_type_from_value(value: &vibesql_types::SqlValue) -> vibesql_types::DataType {
    match value {
        vibesql_types::SqlValue::Null => vibesql_types::DataType::Varchar { max_length: Some(255) }, /* default */
        vibesql_types::SqlValue::Integer(_) => vibesql_types::DataType::Integer,
        vibesql_types::SqlValue::Varchar(_) => {
            vibesql_types::DataType::Varchar { max_length: Some(255) }
        }
        vibesql_types::SqlValue::Character(_) => vibesql_types::DataType::Character { length: 1 },
        vibesql_types::SqlValue::Boolean(_) => vibesql_types::DataType::Boolean,
        vibesql_types::SqlValue::Float(_) => vibesql_types::DataType::Float { precision: 53 },
        vibesql_types::SqlValue::Double(_) => vibesql_types::DataType::DoublePrecision,
        vibesql_types::SqlValue::Numeric(_) => {
            vibesql_types::DataType::Numeric { precision: 10, scale: 2 }
        }
        vibesql_types::SqlValue::Real(_) => vibesql_types::DataType::Real,
        vibesql_types::SqlValue::Smallint(_) => vibesql_types::DataType::Smallint,
        vibesql_types::SqlValue::Bigint(_) => vibesql_types::DataType::Bigint,
        vibesql_types::SqlValue::Unsigned(_) => vibesql_types::DataType::Unsigned,
        vibesql_types::SqlValue::Date(_) => vibesql_types::DataType::Date,
        vibesql_types::SqlValue::Time(_) => vibesql_types::DataType::Time { with_timezone: false },
        vibesql_types::SqlValue::Timestamp(_) => {
            vibesql_types::DataType::Timestamp { with_timezone: false }
        }
        vibesql_types::SqlValue::Interval(_) => {
            // For now, return a simple INTERVAL type (can be enhanced to detect field types)
            vibesql_types::DataType::Interval {
                start_field: vibesql_types::IntervalField::Day,
                end_field: None,
            }
        }
        vibesql_types::SqlValue::Vector(v) => {
            vibesql_types::DataType::Vector { dimensions: v.len() as u32 }
        }
        vibesql_types::SqlValue::Blob(_) => vibesql_types::DataType::BinaryLargeObject,
    }
}
