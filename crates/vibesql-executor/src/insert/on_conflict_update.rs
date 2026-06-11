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
//! Known limitations (v1, issue #5269):
//! - Expression-index conflict targets (`ON CONFLICT(a+b)`) and partial-index
//!   targets (`ON CONFLICT(b) WHERE ...`) are not matched; they raise the
//!   canonical "does not match" error like a non-unique target.
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
    Assignment, Expression, FromClause, SelectStmt,
};
use vibesql_types::SqlValue;

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

/// Collect every unique column set that can act as an upsert conflict target:
/// the PRIMARY KEY, table-level UNIQUE constraints, and unique non-partial,
/// simple-column indexes created via `CREATE UNIQUE INDEX`.
///
/// Partial indexes (`CREATE UNIQUE INDEX ... WHERE ...`) and expression
/// indexes are intentionally excluded: a plain column-list conflict target
/// cannot match them in SQLite either (upsert1-300).
fn collect_unique_column_sets(
    db: &vibesql_storage::Database,
    table_name: &str,
    schema: &vibesql_catalog::TableSchema,
) -> Vec<Vec<usize>> {
    let mut sets: Vec<Vec<usize>> = Vec::new();

    if let Some(pk) = schema.get_primary_key_indices() {
        if !pk.is_empty() {
            sets.push(pk);
        }
    }

    for unique in schema.get_unique_constraint_indices() {
        if !unique.is_empty() {
            sets.push(unique);
        }
    }

    for index_name in db.list_indexes_for_table(table_name) {
        let Some(meta) = db.get_index(&index_name) else { continue };
        if !meta.unique || meta.is_partial() {
            continue;
        }
        let mut cols = Vec::with_capacity(meta.columns.len());
        let mut simple = true;
        for index_col in &meta.columns {
            match index_col.column_name().and_then(|name| schema.get_column_index(name)) {
                Some(idx) => cols.push(idx),
                None => {
                    // Expression index component (or unknown column): the
                    // whole index cannot be matched by a column-list target.
                    simple = false;
                    break;
                }
            }
        }
        if simple && !cols.is_empty() {
            sets.push(cols);
        }
    }

    sets
}

/// Resolve conflict-target column names to schema column indices.
/// Errors with SQLite's "no such column" message for unknown names.
fn resolve_target_indices(
    schema: &vibesql_catalog::TableSchema,
    target: &[String],
) -> Result<Vec<usize>, ExecutorError> {
    target
        .iter()
        .map(|col| {
            schema.get_column_index(col).ok_or_else(|| {
                ExecutorError::SqliteCompatError(format!("no such column: {}", col))
            })
        })
        .collect()
}

/// Normalize a column index set for order-insensitive comparison.
fn normalized(mut indices: Vec<usize>) -> Vec<usize> {
    indices.sort_unstable();
    indices.dedup();
    indices
}

/// Validate an explicit `ON CONFLICT (cols)` target against the table's
/// PRIMARY KEY, UNIQUE constraints, and unique indexes.
///
/// SQLite validates the conflict target at prepare time, even when no row
/// actually conflicts (upsert1-110/120). Unknown columns raise
/// "no such column"; known columns without a matching unique constraint
/// raise the canonical "does not match" error.
pub fn validate_conflict_target(
    db: &vibesql_storage::Database,
    table_name: &str,
    schema: &vibesql_catalog::TableSchema,
    target: &[String],
) -> Result<(), ExecutorError> {
    let target_set = normalized(resolve_target_indices(schema, target)?);
    let matched = collect_unique_column_sets(db, table_name, schema)
        .into_iter()
        .any(|set| normalized(set) == target_set);

    if matched {
        Ok(())
    } else {
        Err(ExecutorError::SqliteCompatError(
            "ON CONFLICT clause does not match any PRIMARY KEY or UNIQUE constraint".to_string(),
        ))
    }
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
pub fn handle_on_conflict_update(
    db: &mut vibesql_storage::Database,
    table_name: &str,
    schema: &vibesql_catalog::TableSchema,
    row_values: &[SqlValue],
    conflict_target: Option<&Vec<String>>,
    assignments: &[Assignment],
    where_clause: Option<&Expression>,
) -> Result<UpsertAction, ExecutorError> {
    // Determine which unique column sets the update arm applies to.
    // SQLite tests the targeted constraint first (upsert1-700 series).
    let all_sets = collect_unique_column_sets(db, table_name, schema);
    let candidate_sets: Vec<Vec<usize>> = match conflict_target {
        Some(target) => {
            let target_set = normalized(resolve_target_indices(schema, target)?);
            all_sets.into_iter().filter(|set| normalized(set.clone()) == target_set).collect()
        }
        None => all_sets,
    };

    // Find a live row that conflicts on one of the candidate constraints.
    let conflicting_row_id = {
        let table = db
            .get_table(table_name)
            .ok_or_else(|| ExecutorError::TableNotFound(table_name.to_string()))?;
        let mut found = None;
        'outer: for set in &candidate_sets {
            // NULLs never conflict under UNIQUE semantics.
            if set.iter().any(|&i| matches!(row_values[i], SqlValue::Null)) {
                continue;
            }
            for (row_id, row) in table.scan_live() {
                if set.iter().all(|&i| row.values[i] == row_values[i]) {
                    found = Some(row_id);
                    break 'outer;
                }
            }
        }
        found
    };

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
        let evaluator = crate::evaluator::ExpressionEvaluator::with_database(schema, db);

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

    let table_mut = db
        .get_table_mut(table_name)
        .ok_or_else(|| ExecutorError::TableNotFound(table_name.to_string()))?;
    table_mut
        .update_row(row_id, vibesql_storage::Row::new(new_values))
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
