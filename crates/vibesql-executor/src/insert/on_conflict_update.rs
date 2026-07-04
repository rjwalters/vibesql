//! ON CONFLICT ... DO UPDATE execution (SQLite upsert)
//!
//! Implements the update arm of SQLite's upsert syntax:
//!
//! ```sql
//! INSERT INTO t VALUES (...) ON CONFLICT [(cols)] DO UPDATE SET ... [WHERE ...];
//! ```
//!
//! Semantics (see <https://www.sqlite.org/lang_upsert.html>):
//! - When a conflict target `(cols)` is given, only conflicts on that exact
//!   PRIMARY KEY / UNIQUE constraint / unique index take the update arm.
//!   Conflicts on other constraints surface as normal UNIQUE errors via the
//!   regular insert path.
//! - `excluded.col` in SET/WHERE expressions refers to the row that would
//!   have been inserted; unqualified or target-table-qualified references
//!   resolve to the existing (conflicting) row.
//! - When the `DO UPDATE ... WHERE` predicate is false or NULL the candidate
//!   row is silently dropped (neither inserted nor updated).
//!
//! Conflict targets match the PRIMARY KEY, table-level UNIQUE constraints,
//! and unique indexes — including expression indexes (`ON CONFLICT(a+b)`
//! matches `CREATE UNIQUE INDEX ... ON t(a+b)`, upsert1-200) and partial
//! indexes (`ON CONFLICT(b) WHERE b>10` matches
//! `CREATE UNIQUE INDEX ... ON t(b) WHERE b>10`, upsert1-320). Expression
//! components are compared structurally (`a+(+b)` does NOT match `a+b`,
//! upsert1-210) and a target WHERE must structurally equal the index
//! predicate (upsert1-300/310).
//!
//! Known limitations (issue #5269):
//! - Non-BINARY `COLLATE` in the target is never matched (upsert1-130).
//! - UPDATE triggers do not fire on the upsert update arm (parity with the
//!   MySQL-style `ON DUPLICATE KEY UPDATE` path).
//!
//! Subqueries in SET/WHERE execute with full scope resolution (issue #5279):
//! names bound by a subquery's own FROM clause win over the upsert scope, and
//! a FROM item named/aliased `excluded` shadows the pseudo-table. See
//! `UpsertColumnSubstituter` for one documented edge involving `IN`.

use crate::errors::ExecutorError;
use vibesql_ast::{
    visitor::{transform_expression, ExpressionMutVisitor, VisitResult},
    Assignment, ConflictTargetItem, Expression, FromClause, SelectStmt,
};
use vibesql_types::SqlValue;

use crate::partial_index_maintenance::is_predicate_truthy;
use crate::select::grouping::expressions_equal;

/// Outcome of attempting the DO UPDATE arm for one candidate row.
pub enum UpsertAction {
    /// A conflicting row was found and updated (physical row id).
    Updated(usize),
    /// A conflicting row was found but the DO UPDATE WHERE clause was not
    /// satisfied: the row is neither inserted nor updated (SQLite semantics).
    Skipped,
    /// No conflict on the targeted constraint; caller should insert normally.
    NoConflict,
}

/// One key component of a unique constraint/index candidate.
enum KeyPart {
    /// Plain column, resolved to a schema column index.
    Column(usize),
    /// Expression component of an expression index.
    Expr(Expression),
}

/// A unique constraint or index that can act as an upsert conflict target:
/// the PRIMARY KEY, a table-level UNIQUE constraint, or a unique index
/// created via `CREATE UNIQUE INDEX` (including expression and partial
/// indexes).
struct UniqueCandidate {
    /// Key components (column references or index expressions).
    parts: Vec<KeyPart>,
    /// Partial-index WHERE predicate; None for full indexes/constraints.
    predicate: Option<Expression>,
    /// Name of the unique index this candidate came from; None for the
    /// PRIMARY KEY and table-level UNIQUE constraints. Used for SQLite's
    /// "UNIQUE constraint failed: index 'name'" message on expression
    /// indexes.
    index_name: Option<String>,
}

/// Collect every unique constraint/index that can act as an upsert conflict
/// target.
fn collect_unique_candidates(
    db: &vibesql_storage::Database,
    table_name: &str,
    schema: &vibesql_catalog::TableSchema,
) -> Vec<UniqueCandidate> {
    let mut candidates: Vec<UniqueCandidate> = Vec::new();

    if let Some(pk) = schema.get_primary_key_indices() {
        if !pk.is_empty() {
            candidates.push(UniqueCandidate {
                parts: pk.into_iter().map(KeyPart::Column).collect(),
                predicate: None,
                index_name: None,
            });
        }
    }

    for unique in schema.get_unique_constraint_indices() {
        if !unique.is_empty() {
            candidates.push(UniqueCandidate {
                parts: unique.into_iter().map(KeyPart::Column).collect(),
                predicate: None,
                index_name: None,
            });
        }
    }

    for index_name in db.list_indexes_for_table(table_name) {
        let Some(meta) = db.get_index(&index_name) else { continue };
        if !meta.unique {
            continue;
        }
        let mut parts = Vec::with_capacity(meta.columns.len());
        let mut representable = true;
        for index_col in &meta.columns {
            if let Some(name) = index_col.column_name() {
                match schema.get_column_index(name) {
                    Some(idx) => parts.push(KeyPart::Column(idx)),
                    None => {
                        // Unknown column: the index cannot be matched.
                        representable = false;
                        break;
                    }
                }
            } else if let Some(expr) = index_col.get_expression() {
                // Normalize bare column-ref expressions to plain columns so
                // a column-name target can match them.
                match bare_column_index(schema, expr) {
                    Some(idx) => parts.push(KeyPart::Column(idx)),
                    None => parts.push(KeyPart::Expr(expr.clone())),
                }
            } else {
                representable = false;
                break;
            }
        }
        if representable && !parts.is_empty() {
            candidates.push(UniqueCandidate {
                parts,
                predicate: meta.where_clause.as_deref().cloned(),
                index_name: Some(index_name.clone()),
            });
        }
    }

    candidates
}

/// If `expr` is a bare (unqualified) column reference naming a schema
/// column, return that column's index.
fn bare_column_index(schema: &vibesql_catalog::TableSchema, expr: &Expression) -> Option<usize> {
    match expr {
        Expression::ColumnRef(id) if id.table_canonical().is_none() => {
            schema.get_column_index(id.column_canonical())
        }
        _ => None,
    }
}

/// A conflict-target item resolved against the table schema.
enum ResolvedTargetItem<'a> {
    Column(usize),
    Expr(&'a Expression),
}

/// Resolve conflict-target items against the schema. Plain column names that
/// don't exist raise SQLite's "no such column" error (upsert1-110).
fn resolve_target_items<'a>(
    schema: &vibesql_catalog::TableSchema,
    target: &'a [ConflictTargetItem],
) -> Result<Vec<ResolvedTargetItem<'a>>, ExecutorError> {
    target
        .iter()
        .map(|item| match item {
            ConflictTargetItem::Column(name) => {
                schema.get_column_index(name).map(ResolvedTargetItem::Column).ok_or_else(|| {
                    ExecutorError::SqliteCompatError(format!("no such column: {}", name))
                })
            }
            ConflictTargetItem::Expression(expr) => match bare_column_index(schema, expr) {
                Some(idx) => Ok(ResolvedTargetItem::Column(idx)),
                None => Ok(ResolvedTargetItem::Expr(expr)),
            },
        })
        .collect()
}

/// Does this candidate match the resolved conflict target?
///
/// The target must cover the candidate's key components exactly
/// (order-insensitive). Column items match resolved column indices;
/// expression items match expression components structurally
/// (`expressions_equal`, so `a+(+b)` does NOT match `a+b` — upsert1-210).
/// The target-level WHERE must structurally equal the index predicate:
/// a bare target never matches a partial index (upsert1-300) and a
/// mismatched predicate never matches (upsert1-310).
fn candidate_matches_target(
    candidate: &UniqueCandidate,
    target: &[ResolvedTargetItem<'_>],
    target_where: Option<&Expression>,
) -> bool {
    // Partial-index predicate must match structurally.
    match (target_where, candidate.predicate.as_ref()) {
        (None, None) => {}
        (Some(tw), Some(pred)) => {
            if !expressions_equal(tw, pred) {
                return false;
            }
        }
        _ => return false,
    }

    if candidate.parts.len() != target.len() {
        return false;
    }

    // Order-insensitive multiset match between target items and key parts.
    let mut used = vec![false; candidate.parts.len()];
    for item in target {
        let mut matched = false;
        for (i, part) in candidate.parts.iter().enumerate() {
            if used[i] {
                continue;
            }
            let matches = match (item, part) {
                (ResolvedTargetItem::Column(t), KeyPart::Column(c)) => t == c,
                (ResolvedTargetItem::Expr(t), KeyPart::Expr(c)) => expressions_equal(t, c),
                _ => false,
            };
            if matches {
                used[i] = true;
                matched = true;
                break;
            }
        }
        if !matched {
            return false;
        }
    }
    true
}

/// Validate an explicit `ON CONFLICT (cols) [WHERE ...]` target against the
/// table's PRIMARY KEY, UNIQUE constraints, and unique indexes.
///
/// SQLite validates the conflict target at prepare time, even when no row
/// actually conflicts (upsert1-110/120). Unknown columns raise
/// "no such column"; known columns without a matching unique constraint
/// raise the canonical "does not match" error.
pub fn validate_conflict_target(
    db: &vibesql_storage::Database,
    table_name: &str,
    schema: &vibesql_catalog::TableSchema,
    target: &[ConflictTargetItem],
    target_where: Option<&Expression>,
) -> Result<(), ExecutorError> {
    let resolved = resolve_target_items(schema, target)?;
    let matched = collect_unique_candidates(db, table_name, schema)
        .iter()
        .any(|candidate| candidate_matches_target(candidate, &resolved, target_where));

    if matched {
        Ok(())
    } else {
        Err(ExecutorError::SqliteCompatError(
            "ON CONFLICT clause does not match any PRIMARY KEY or UNIQUE constraint".to_string(),
        ))
    }
}

/// Evaluate a candidate's key for a row. Returns `None` when any component
/// is NULL (NULL keys never conflict under UNIQUE semantics) or when an
/// expression component fails to evaluate (treated as NULL, matching
/// expression-index maintenance).
fn eval_candidate_key(
    evaluator: &crate::evaluator::ExpressionEvaluator,
    candidate: &UniqueCandidate,
    row: &vibesql_storage::Row,
) -> Option<Vec<SqlValue>> {
    let mut key = Vec::with_capacity(candidate.parts.len());
    for part in &candidate.parts {
        let value = match part {
            KeyPart::Column(idx) => row.values.get(*idx)?.clone(),
            KeyPart::Expr(expr) => evaluator.eval(expr, row).unwrap_or(SqlValue::Null),
        };
        if matches!(value, SqlValue::Null) {
            return None;
        }
        key.push(value);
    }
    Some(key)
}

/// Does the row satisfy the candidate's partial-index predicate (or is the
/// candidate a full index/constraint)? Rows outside a partial index can
/// never conflict through it.
fn row_in_candidate(
    evaluator: &crate::evaluator::ExpressionEvaluator,
    candidate: &UniqueCandidate,
    row: &vibesql_storage::Row,
) -> bool {
    match &candidate.predicate {
        None => true,
        Some(pred) => evaluator.eval(pred, row).map(|v| is_predicate_truthy(&v)).unwrap_or(false),
    }
}

/// Find a live row that conflicts with `row_values` on one of the candidate
/// constraints. Candidates are tested in order (SQLite tests the targeted
/// constraint first — upsert1-700 series).
fn find_conflicting_live_row(
    db: &vibesql_storage::Database,
    table_name: &str,
    schema: &vibesql_catalog::TableSchema,
    candidates: &[UniqueCandidate],
    row_values: &[SqlValue],
) -> Result<Option<usize>, ExecutorError> {
    let table = db
        .get_table(table_name)
        .ok_or_else(|| ExecutorError::TableNotFound(table_name.to_string()))?;
    let evaluator = crate::evaluator::ExpressionEvaluator::new(schema);
    let candidate_row = vibesql_storage::Row::new(row_values.to_vec());

    for candidate in candidates {
        let Some(new_key) = eval_candidate_key(&evaluator, candidate, &candidate_row) else {
            continue;
        };
        if !row_in_candidate(&evaluator, candidate, &candidate_row) {
            continue;
        }
        for (row_id, row) in table.scan_live() {
            if !row_in_candidate(&evaluator, candidate, row) {
                continue;
            }
            if eval_candidate_key(&evaluator, candidate, row).as_deref() == Some(&new_key[..]) {
                return Ok(Some(row_id));
            }
        }
    }
    Ok(None)
}

/// SQLite-format violation message for a unique candidate: qualified column
/// names for the PRIMARY KEY, table-level UNIQUE constraints, and plain
/// column indexes (`UNIQUE constraint failed: t.x, t.y`); the index name for
/// expression indexes (`UNIQUE constraint failed: index 'name'`).
fn unique_violation_message(
    schema: &vibesql_catalog::TableSchema,
    table_name: &str,
    candidate: &UniqueCandidate,
) -> String {
    let mut cols = Vec::with_capacity(candidate.parts.len());
    for part in &candidate.parts {
        match part {
            KeyPart::Column(idx) => {
                let name = schema.columns.get(*idx).map(|c| c.name.as_str()).unwrap_or("?");
                cols.push(format!("{}.{}", table_name, name));
            }
            KeyPart::Expr(_) => {
                // Expression components only occur for unique indexes.
                return format!(
                    "UNIQUE constraint failed: index '{}'",
                    candidate.index_name.as_deref().unwrap_or("?")
                );
            }
        }
    }
    format!("UNIQUE constraint failed: {}", cols.join(", "))
}

/// Enforce constraints on the row produced by the DO UPDATE arm — the same
/// checks a normal UPDATE runs (issue #5836, upsert4 1.x.5):
///
/// - NOT NULL and CHECK constraints (per-row, via the shared UPDATE
///   validator);
/// - PRIMARY KEY / UNIQUE constraints / unique indexes, by scanning live
///   rows directly. The scan excludes the row being updated (`row_id`) so a
///   SET that leaves a key unchanged never conflicts with itself. Live-row
///   scanning matches this module's conflict detection and does not depend
///   on database-level index data, which the upsert arm does not maintain
///   (issue #5269).
///
/// Errors use SQLite's exact wording, verified against sqlite3:
/// `UNIQUE constraint failed: t.c` / `NOT NULL constraint failed: t.b` /
/// `UNIQUE constraint failed: index 'name'` (expression indexes).
fn validate_do_update_row(
    db: &vibesql_storage::Database,
    table_name: &str,
    schema: &vibesql_catalog::TableSchema,
    row_id: usize,
    new_row: &vibesql_storage::Row,
) -> Result<(), ExecutorError> {
    // NOT NULL and CHECK — the per-row half of normal UPDATE validation.
    crate::update::constraints::ConstraintValidator::new(schema)
        .validate_row_skip_uniqueness(table_name, new_row)?;

    // PRIMARY KEY, UNIQUE constraints, and unique indexes (including
    // expression and partial indexes) — all candidates, not just the
    // conflict target: SQLite aborts the statement when the update arm
    // would violate ANY uniqueness constraint (upsert4 1.x.5).
    let table = db
        .get_table(table_name)
        .ok_or_else(|| ExecutorError::TableNotFound(table_name.to_string()))?;
    let evaluator = crate::evaluator::ExpressionEvaluator::new(schema);
    for candidate in collect_unique_candidates(db, table_name, schema) {
        let Some(new_key) = eval_candidate_key(&evaluator, &candidate, new_row) else {
            continue;
        };
        if !row_in_candidate(&evaluator, &candidate, new_row) {
            continue;
        }
        for (other_id, other_row) in table.scan_live() {
            if other_id == row_id {
                continue;
            }
            if !row_in_candidate(&evaluator, &candidate, other_row) {
                continue;
            }
            if eval_candidate_key(&evaluator, &candidate, other_row).as_deref()
                == Some(&new_key[..])
            {
                return Err(ExecutorError::ConstraintViolation(unique_violation_message(
                    schema, table_name, &candidate,
                )));
            }
        }
    }
    Ok(())
}

/// Would inserting `row_values` conflict on the explicit DO NOTHING conflict
/// target? Used by the targeted `ON CONFLICT (cols) [WHERE ...] DO NOTHING`
/// path: only conflicts on the *targeted* constraint are suppressed —
/// conflicts on other constraints surface as normal UNIQUE errors
/// (upsert1-201).
///
/// `batch_rows` carries earlier rows from the same multi-row INSERT that
/// have been validated but not yet inserted, so later rows in a VALUES list
/// can conflict with earlier ones (upsert1-320).
pub fn row_conflicts_on_target(
    db: &vibesql_storage::Database,
    table_name: &str,
    schema: &vibesql_catalog::TableSchema,
    row_values: &[SqlValue],
    target: &[ConflictTargetItem],
    target_where: Option<&Expression>,
    batch_rows: &[Vec<SqlValue>],
) -> Result<bool, ExecutorError> {
    let resolved = resolve_target_items(schema, target)?;
    let candidates: Vec<UniqueCandidate> = collect_unique_candidates(db, table_name, schema)
        .into_iter()
        .filter(|candidate| candidate_matches_target(candidate, &resolved, target_where))
        .collect();

    if find_conflicting_live_row(db, table_name, schema, &candidates, row_values)?.is_some() {
        return Ok(true);
    }

    // Conflicts with earlier (not-yet-inserted) rows from the same batch.
    let evaluator = crate::evaluator::ExpressionEvaluator::new(schema);
    let candidate_row = vibesql_storage::Row::new(row_values.to_vec());
    for candidate in &candidates {
        let Some(new_key) = eval_candidate_key(&evaluator, candidate, &candidate_row) else {
            continue;
        };
        if !row_in_candidate(&evaluator, candidate, &candidate_row) {
            continue;
        }
        for batch_row in batch_rows {
            let row = vibesql_storage::Row::new(batch_row.clone());
            if !row_in_candidate(&evaluator, candidate, &row) {
                continue;
            }
            if eval_candidate_key(&evaluator, candidate, &row).as_deref() == Some(&new_key[..]) {
                return Ok(true);
            }
        }
    }
    Ok(false)
}

/// Handle the `ON CONFLICT [(cols)] DO UPDATE SET ... [WHERE ...]` arm for a
/// single candidate row.
///
/// Returns:
/// - `Updated(row_id)` when a conflicting row was found and updated,
/// - `Skipped` when a conflicting row was found but the WHERE predicate was
///   not satisfied (the candidate row is dropped silently),
/// - `NoConflict` when no row conflicts on the targeted constraint(s); the
///   caller should proceed with a normal insert (which may still raise
///   UNIQUE errors for constraints other than the named target — SQLite
///   semantics, upsert1-201).
#[allow(clippy::too_many_arguments)]
pub fn handle_on_conflict_update(
    db: &mut vibesql_storage::Database,
    table_name: &str,
    schema: &vibesql_catalog::TableSchema,
    row_values: &[SqlValue],
    conflict_target: Option<&[ConflictTargetItem]>,
    target_where: Option<&Expression>,
    assignments: &[Assignment],
    where_clause: Option<&Expression>,
    cte_results: Option<&std::collections::HashMap<String, crate::select::cte::CteResult>>,
) -> Result<UpsertAction, ExecutorError> {
    // Determine which unique constraints/indexes the update arm applies to.
    // SQLite tests the targeted constraint first (upsert1-700 series).
    let all_candidates = collect_unique_candidates(db, table_name, schema);
    let candidates: Vec<UniqueCandidate> = match conflict_target {
        Some(target) => {
            let resolved = resolve_target_items(schema, target)?;
            all_candidates
                .into_iter()
                .filter(|candidate| candidate_matches_target(candidate, &resolved, target_where))
                .collect()
        }
        None => all_candidates,
    };

    // Find a live row that conflicts on one of the candidate constraints.
    let conflicting_row_id =
        find_conflicting_live_row(db, table_name, schema, &candidates, row_values)?;

    let Some(row_id) = conflicting_row_id else {
        return Ok(UpsertAction::NoConflict);
    };

    // Snapshot the existing (conflicting) row before mutation. All SET
    // expressions and the WHERE predicate see the pre-update values.
    let existing_row = {
        let table = db
            .get_table(table_name)
            .ok_or_else(|| ExecutorError::TableNotFound(table_name.to_string()))?;
        table
            .scan()
            .get(row_id)
            .cloned()
            .ok_or_else(|| ExecutorError::UnsupportedExpression("Row not found".to_string()))?
    };

    // Evaluate the WHERE predicate and SET assignments in a scope that only
    // borrows `db` immutably: the evaluator needs a database reference so
    // subqueries in the update arm can execute (issue #5279), and the borrow
    // must end before `get_table_mut` below.
    //
    // Scope resolution mirrors the regular UPDATE path (update/executor.rs):
    // the existing (conflicting) row is the evaluation row, so unqualified
    // and target-table-qualified references resolve to the existing row at
    // the top level and correlate into subqueries, while names bound by a
    // subquery's own FROM clause win inside that subquery (innermost scope
    // first — SQLite semantics). Only `excluded.` references need rewriting,
    // since `excluded` is not a real table.
    let new_values = {
        let mut evaluator = crate::evaluator::ExpressionEvaluator::with_database(schema, db);
        // Make the enclosing INSERT statement's WITH-clause CTEs visible to
        // subqueries in the DO UPDATE SET/WHERE expressions (issue #5359).
        if let Some(ctes) = cte_results {
            evaluator = evaluator.with_cte_context(ctes);
        }

        // DO UPDATE ... WHERE: when false or NULL, drop the row silently.
        if let Some(where_expr) = where_clause {
            let substituted = substitute_excluded_refs(where_expr.clone(), schema, row_values)?;
            let value = evaluator.eval(&substituted, &existing_row)?;
            if !crate::evaluator::operators::is_truthy(&value) {
                return Ok(UpsertAction::Skipped);
            }
        }

        // Apply SET assignments against a copy of the existing row.
        let mut new_values = existing_row.values.clone();
        for assignment in assignments {
            let col_idx = schema.get_column_index(&assignment.column).ok_or_else(|| {
                ExecutorError::SqliteCompatError(format!("no such column: {}", assignment.column))
            })?;
            let substituted =
                substitute_excluded_refs(assignment.value.clone(), schema, row_values)?;
            new_values[col_idx] = evaluator.eval(&substituted, &existing_row)?;
        }
        new_values
    };

    // Enforce constraints on the updated row BEFORE writing it back: the DO
    // UPDATE arm runs the same checks as a normal UPDATE, so a SET that
    // would duplicate a UNIQUE/PK key or null a NOT NULL column aborts the
    // statement and leaves the table untouched (issue #5836, upsert4 1.x.5-6).
    let new_row = vibesql_storage::Row::new(new_values);
    validate_do_update_row(db, table_name, schema, row_id, &new_row)?;

    let table_mut = db
        .get_table_mut(table_name)
        .ok_or_else(|| ExecutorError::TableNotFound(table_name.to_string()))?;
    table_mut
        .update_row(row_id, new_row)
        .map_err(|e| ExecutorError::UnsupportedExpression(format!("Storage error: {}", e)))?;

    // NOTE: database-level index data is not rebuilt here, matching the
    // MySQL-style ON DUPLICATE KEY UPDATE path. Updating an indexed column
    // through the upsert arm can leave index data stale — a known v1
    // limitation (issue #5269). Conflict detection above scans live rows
    // directly, so upsert correctness does not depend on index data.

    // Invalidate the database-level columnar cache since table data changed.
    // The table-level cache is already invalidated by update_row(); both are
    // needed because they manage separate caches (see duplicate_key_update).
    db.invalidate_columnar_cache(table_name);

    Ok(UpsertAction::Updated(row_id))
}

/// Rewrites `excluded.col` references in upsert SET/WHERE expressions to
/// literal values from the would-be-inserted row.
///
/// `excluded` is the only name in the update arm that does not correspond to
/// a real table, so it is the only one substituted here. Unqualified and
/// target-table-qualified references are left for the standard evaluator,
/// which resolves them against the existing (conflicting) row at the top
/// level and applies innermost-scope-first resolution plus outer-row
/// correlation inside subqueries — matching the regular UPDATE path.
///
/// Shadowing: a subquery whose FROM clause binds the name `excluded` (a table
/// named `excluded` or any item aliased `excluded`) shadows the upsert
/// pseudo-table in SQLite. Such subqueries are skipped entirely — every
/// `excluded.` reference inside them belongs to the FROM binding, so nothing
/// in that subtree needs substitution.
///
/// Known limitation: for `IN (SELECT ...)` and quantified comparisons whose
/// subquery shadows `excluded`, the left-hand expression (which is in the
/// outer scope) is skipped along with the subquery, so an `excluded.` ref
/// there surfaces as an unresolvable-column error instead of being
/// substituted. SQLite resolves it to the pseudo-table; the failure mode here
/// is an error, never wrong data.
struct UpsertColumnSubstituter<'a> {
    schema: &'a vibesql_catalog::TableSchema,
    inserted: &'a [SqlValue],
    error: Option<ExecutorError>,
}

/// Does this FROM clause bind the name `excluded` (case-insensitive)?
///
/// A base table named `excluded` (without an alias overriding it), or any
/// table / join / derived-table / VALUES item aliased `excluded`, shadows the
/// upsert pseudo-table for the subquery's scope. Only the immediate FROM
/// items are inspected: bindings inside a derived table's own query belong to
/// a deeper scope and do not shadow names at this level (and the derived
/// table cannot see the pseudo-table anyway).
fn from_binds_excluded(from: &FromClause) -> bool {
    match from {
        FromClause::Table { name, alias, .. } => match alias {
            Some(a) => a.eq_ignore_ascii_case("excluded"),
            None => name.eq_ignore_ascii_case("excluded"),
        },
        FromClause::Join { left, right, alias, .. } => {
            alias.as_ref().is_some_and(|a| a.eq_ignore_ascii_case("excluded"))
                || from_binds_excluded(left)
                || from_binds_excluded(right)
        }
        FromClause::Subquery { alias, .. } | FromClause::Values { alias, .. } => {
            alias.eq_ignore_ascii_case("excluded")
        }
    }
}

/// Does this subquery's immediate FROM clause shadow the `excluded`
/// pseudo-table?
fn select_binds_excluded(select: &SelectStmt) -> bool {
    select.from.as_ref().is_some_and(from_binds_excluded)
}

impl ExpressionMutVisitor for UpsertColumnSubstituter<'_> {
    fn pre_visit_expression(&mut self, expr: &Expression) -> VisitResult {
        if self.error.is_some() {
            return VisitResult::Skip;
        }
        // Skip subqueries that rebind `excluded` in their FROM clause: every
        // `excluded.` reference inside belongs to that binding (SQLite scope
        // shadowing), so no substitution applies in the subtree.
        let subquery = match expr {
            Expression::ScalarSubquery(select) => Some(select.as_ref()),
            Expression::Exists { subquery, .. } => Some(subquery.as_ref()),
            Expression::In { subquery, .. } => Some(subquery.as_ref()),
            Expression::QuantifiedComparison { subquery, .. } => Some(subquery.as_ref()),
            _ => None,
        };
        match subquery {
            Some(select) if select_binds_excluded(select) => VisitResult::Skip,
            _ => VisitResult::Continue,
        }
    }

    fn post_visit_expression(&mut self, expr: Expression) -> Expression {
        if self.error.is_some() {
            return expr;
        }
        if let Expression::ColumnRef(ref id) = expr {
            let is_excluded = id
                .table_canonical()
                .map(|q| q.eq_ignore_ascii_case("excluded"))
                .unwrap_or(false);
            if !is_excluded {
                // Real-table or unqualified ref: the evaluator resolves it
                // (existing row at top level, subquery scoping inside).
                return expr;
            }
            match self.schema.get_column_index(id.column_canonical()) {
                Some(idx) => return Expression::Literal(self.inserted[idx].clone()),
                None => {
                    self.error = Some(ExecutorError::SqliteCompatError(format!(
                        "no such column: excluded.{}",
                        id.column_canonical()
                    )));
                    return expr;
                }
            }
        }
        expr
    }
}

/// Substitute `excluded.` column references with literal values from the
/// would-be-inserted row so the expression can be evaluated by the standard
/// evaluator against the existing (conflicting) row.
fn substitute_excluded_refs(
    expr: Expression,
    schema: &vibesql_catalog::TableSchema,
    inserted: &[SqlValue],
) -> Result<Expression, ExecutorError> {
    let mut substituter = UpsertColumnSubstituter { schema, inserted, error: None };
    let result = transform_expression(&mut substituter, expr);
    if let Some(err) = substituter.error {
        return Err(err);
    }
    Ok(result)
}
