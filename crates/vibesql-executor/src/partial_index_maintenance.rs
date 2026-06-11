//! Partial-index maintenance for DML operations.
//!
//! Partial indexes (`CREATE INDEX ... WHERE predicate`) require the WHERE
//! predicate to be evaluated per row to decide whether the row belongs in
//! the index. The storage layer cannot evaluate AST expressions, so these
//! helpers evaluate the predicate against the affected row and call the
//! storage's `*_partial_indexes_*` entry points with a pre-computed
//! inclusion set.
//!
//! The companion module `expression_index_maintenance` follows the same
//! pattern for expression-based indexes.
//!
//! NOTE on non-deterministic date/time rejection (issue #5313): these
//! maintenance paths run AFTER the row mutation has been applied, so they
//! intentionally stay lenient (predicate evaluation errors mean
//! not-in-index) and do NOT evaluate with `SchemaExprContext::Index`. The
//! SQLite-compatible "non-deterministic use of <fn>() in an index" rejection
//! is enforced PRE-mutation by
//! `insert::constraints::enforce_index_expression_determinism` (called from
//! the INSERT row validator and the UPDATE executors), which guarantees rows
//! reaching this module evaluate deterministically.

use std::collections::HashSet;

use vibesql_storage::Database;
use vibesql_types::SqlValue;

use crate::evaluator::ExpressionEvaluator;

/// Evaluate a SQL value as a boolean predicate in the SQLite sense.
///
/// SQLite treats NULL as "not true" (rows are excluded from partial indexes
/// when the predicate is NULL), and any non-zero, non-NULL value as true.
/// String values are interpreted as numeric where possible — matching the
/// behaviour of other predicate-evaluation paths in the executor.
#[inline]
pub fn is_predicate_truthy(value: &SqlValue) -> bool {
    match value {
        SqlValue::Boolean(b) => *b,
        SqlValue::Null => false,
        SqlValue::Integer(n) => *n != 0,
        SqlValue::Smallint(n) => *n != 0,
        SqlValue::Bigint(n) => *n != 0,
        SqlValue::Unsigned(n) => *n != 0,
        SqlValue::Float(f) => *f != 0.0,
        SqlValue::Real(f) => *f != 0.0,
        SqlValue::Double(f) => *f != 0.0,
        SqlValue::Numeric(f) => *f != 0.0,
        SqlValue::Varchar(s) | SqlValue::Character(s) => {
            s.parse::<f64>().map(|n| n != 0.0).unwrap_or(false)
        }
        _ => false,
    }
}

/// Evaluate every partial index's WHERE predicate against `row` and return
/// the set of normalized index names whose predicate evaluated to truthy.
///
/// Returns an empty set when the table has no partial indexes, allowing the
/// caller to skip subsequent storage calls cheaply.
pub fn evaluate_partial_index_predicates(
    db: &Database,
    table_name: &str,
    row: &vibesql_storage::Row,
) -> HashSet<String> {
    if !db.has_partial_indexes(table_name) {
        return HashSet::new();
    }

    let table_schema = match db.catalog.get_table(table_name) {
        Some(schema) => schema.clone(),
        None => return HashSet::new(),
    };

    let evaluator = ExpressionEvaluator::new(&table_schema);

    let mut included: HashSet<String> = HashSet::new();
    for (index_name, metadata) in db.get_partial_indexes_for_table(table_name) {
        let Some(predicate) = metadata.where_clause.as_deref() else { continue };
        match evaluator.eval(predicate, row) {
            Ok(v) if is_predicate_truthy(&v) => {
                included.insert(index_name);
            }
            Ok(_) => {}
            Err(e) => {
                log::warn!(
                    "Failed to evaluate WHERE predicate for partial index '{}': {:?}; \
                     treating row as not-in-index",
                    index_name,
                    e
                );
            }
        }
    }
    included
}

/// Maintain partial indexes after inserting a row.
pub fn maintain_partial_indexes_for_insert(
    db: &mut Database,
    table_name: &str,
    row: &vibesql_storage::Row,
    row_index: usize,
) {
    if !db.has_partial_indexes(table_name) {
        return;
    }
    let included = evaluate_partial_index_predicates(db, table_name, row);
    if included.is_empty() {
        return;
    }
    db.add_to_partial_indexes_for_insert(table_name, row, row_index, &included);
}

/// Maintain partial indexes after updating a row.
pub fn maintain_partial_indexes_for_update(
    db: &mut Database,
    table_name: &str,
    old_row: &vibesql_storage::Row,
    new_row: &vibesql_storage::Row,
    row_index: usize,
) {
    if !db.has_partial_indexes(table_name) {
        return;
    }
    let old_included = evaluate_partial_index_predicates(db, table_name, old_row);
    let new_included = evaluate_partial_index_predicates(db, table_name, new_row);
    if old_included.is_empty() && new_included.is_empty() {
        return;
    }
    db.update_partial_indexes_for_update(
        table_name,
        old_row,
        new_row,
        row_index,
        &old_included,
        &new_included,
    );
}

/// Maintain partial indexes after deleting a row.
pub fn maintain_partial_indexes_for_delete(
    db: &mut Database,
    table_name: &str,
    row: &vibesql_storage::Row,
    row_index: usize,
) {
    if !db.has_partial_indexes(table_name) {
        return;
    }
    let included = evaluate_partial_index_predicates(db, table_name, row);
    if included.is_empty() {
        return;
    }
    db.update_partial_indexes_for_delete_with_values(table_name, &row.values, row_index, &included);
}

/// Rebuild partial-index bodies after a table compaction shifted row indices.
///
/// `delete_by_indices_batch` may compact the table (when >50% of rows are
/// being removed) which renumbers the surviving rows. The storage layer's
/// `rebuild_indexes` repairs non-partial indexes but explicitly skips partial
/// indexes — it cannot evaluate the WHERE predicate. This helper:
///
///  1. clears every partial-index body for the table,
///  2. iterates the post-compaction rows,
///  3. evaluates each partial index's WHERE predicate against each row, and
///  4. re-inserts entries via `add_to_partial_indexes_for_insert`.
///
/// Must be invoked at every compaction site (`delete_by_indices_batch`
/// returning `compacted=true`) directly after `rebuild_indexes` — otherwise
/// partial-index row indices point at the *wrong* table rows (silent
/// corruption).
pub fn rebuild_partial_indexes_after_compaction(db: &mut Database, table_name: &str) {
    if !db.has_partial_indexes(table_name) {
        return;
    }

    // Snapshot rows so we can iterate without holding a borrow on the
    // database while we call mutating index APIs.
    let rows: Vec<vibesql_storage::Row> = match db.get_table(table_name) {
        Some(table) => table.scan().to_vec(),
        None => return,
    };

    // Clear the existing body — every row_index in there points at a
    // now-incorrect table row after compaction.
    db.clear_partial_index_data(table_name);

    for (row_index, row) in rows.iter().enumerate() {
        let included = evaluate_partial_index_predicates(db, table_name, row);
        if included.is_empty() {
            continue;
        }
        db.add_to_partial_indexes_for_insert(table_name, row, row_index, &included);
    }
}

/// Check partial UNIQUE-index conflicts for a candidate insert.
///
/// For every partial UNIQUE index on `table_name`, evaluates the WHERE
/// predicate against `row`. When truthy, looks up the candidate key in the
/// index body and returns an error if the key already exists.
///
/// This mirrors `IndexManager::check_unique_constraints_for_insert` but with
/// partial-aware semantics: a partial UNIQUE index only enforces uniqueness
/// over rows that satisfy the predicate, so two rows whose predicate is
/// false for either of them never conflict.
pub fn check_partial_unique_for_insert(
    db: &Database,
    table_name: &str,
    row: &vibesql_storage::Row,
) -> Result<(), crate::errors::ExecutorError> {
    if !db.has_partial_indexes(table_name) {
        return Ok(());
    }
    let table_schema = match db.catalog.get_table(table_name) {
        Some(schema) => schema.clone(),
        None => return Ok(()),
    };
    let evaluator = ExpressionEvaluator::new(&table_schema);

    for (index_name, metadata) in db.get_partial_indexes_for_table(table_name) {
        if !metadata.unique {
            continue;
        }
        // Expression-based partial UNIQUE indexes are not supported yet.
        if metadata.columns.iter().any(|col| col.is_expression()) {
            continue;
        }
        let Some(predicate) = metadata.where_clause.as_deref() else { continue };

        // If the candidate row's predicate is falsy, this index does not
        // enforce uniqueness over it. Skip.
        match evaluator.eval(predicate, row) {
            Ok(v) if is_predicate_truthy(&v) => {}
            _ => continue,
        }

        // Build the candidate key.
        let key_values: Vec<SqlValue> = metadata
            .columns
            .iter()
            .map(|col| {
                let col_name =
                    col.column_name().expect("Partial-index column should have a column name");
                let col_idx =
                    table_schema.get_column_index(col_name).expect("Index column should exist");
                row.values[col_idx].clone()
            })
            .collect();

        // Match SQLite: skip uniqueness check when any key component is NULL.
        if key_values.iter().any(|v| matches!(v, SqlValue::Null)) {
            continue;
        }

        match db.check_partial_unique_conflict(&index_name, &key_values) {
            Ok(true) => {
                let columns_str = metadata
                    .columns
                    .iter()
                    .map(|col| {
                        format!("{}.{}", metadata.table_name, col.column_name().unwrap_or(""))
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                return Err(crate::errors::ExecutorError::UnsupportedExpression(format!(
                    "UNIQUE constraint failed: {}",
                    columns_str
                )));
            }
            Ok(false) => {}
            Err(e) => {
                log::warn!(
                    "Failed to check partial-unique conflict for index '{}': {:?}",
                    index_name,
                    e
                );
            }
        }
    }
    Ok(())
}
