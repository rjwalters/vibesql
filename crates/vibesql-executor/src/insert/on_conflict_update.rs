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

use crate::errors::ExecutorError;
use vibesql_ast::{
    visitor::{transform_expression, ExpressionMutVisitor},
    Assignment, Expression,
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

    let evaluator = crate::evaluator::ExpressionEvaluator::new(schema);

    // DO UPDATE ... WHERE: when false or NULL, drop the row silently.
    if let Some(where_expr) = where_clause {
        let substituted = substitute_upsert_columns(
            where_expr.clone(),
            schema,
            table_name,
            &existing_row.values,
            row_values,
        )?;
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
        let substituted = substitute_upsert_columns(
            assignment.value.clone(),
            schema,
            table_name,
            &existing_row.values,
            row_values,
        )?;
        new_values[col_idx] = evaluator.eval(&substituted, &existing_row)?;
    }

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

/// Rewrites column references in upsert SET/WHERE expressions to literals:
/// - `excluded.col` resolves to the would-be-inserted row,
/// - unqualified or target-table-qualified refs resolve to the existing row.
///
/// Other qualifiers and unresolvable unqualified names are left untouched for
/// the standard evaluator to handle (e.g. ROWID pseudo-columns).
struct UpsertColumnSubstituter<'a> {
    schema: &'a vibesql_catalog::TableSchema,
    table_name: &'a str,
    existing: &'a [SqlValue],
    inserted: &'a [SqlValue],
    error: Option<ExecutorError>,
}

impl ExpressionMutVisitor for UpsertColumnSubstituter<'_> {
    fn post_visit_expression(&mut self, expr: Expression) -> Expression {
        if self.error.is_some() {
            return expr;
        }
        if let Expression::ColumnRef(ref id) = expr {
            let is_excluded = id
                .table_canonical()
                .map(|q| q.eq_ignore_ascii_case("excluded"))
                .unwrap_or(false);
            let source = match id.table_canonical() {
                Some(_) if is_excluded => self.inserted,
                Some(q) if q.eq_ignore_ascii_case(self.table_name) => self.existing,
                // Unknown qualifier: leave for the evaluator to report.
                Some(_) => return expr,
                None => self.existing,
            };
            match self.schema.get_column_index(id.column_canonical()) {
                Some(idx) => return Expression::Literal(source[idx].clone()),
                None => {
                    if is_excluded {
                        self.error = Some(ExecutorError::SqliteCompatError(format!(
                            "no such column: excluded.{}",
                            id.column_canonical()
                        )));
                    }
                    // Unqualified non-column (e.g. rowid): defer to evaluator.
                    return expr;
                }
            }
        }
        expr
    }
}

/// Substitute `excluded.` / existing-row column references with literal
/// values so the expression can be evaluated by the standard evaluator.
fn substitute_upsert_columns(
    expr: Expression,
    schema: &vibesql_catalog::TableSchema,
    table_name: &str,
    existing: &[SqlValue],
    inserted: &[SqlValue],
) -> Result<Expression, ExecutorError> {
    let mut substituter = UpsertColumnSubstituter {
        schema,
        table_name,
        existing,
        inserted,
        error: None,
    };
    let result = transform_expression(&mut substituter, expr);
    if let Some(err) = substituter.error {
        return Err(err);
    }
    Ok(result)
}
