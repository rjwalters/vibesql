//! Index maintenance coordination for UPDATE operations
//!
//! This module handles:
//! - Detecting conflicting rows for UPDATE OR REPLACE operations
//! - Cross-update uniqueness validation (preventing multiple rows from getting same PK)
//! - Post-statement uniqueness validation (deferred PK/UNIQUE checks against final state)
//! - Resolving cross-update conflicts for REPLACE mode

use std::collections::{HashMap, HashSet};

use vibesql_catalog::TableSchema;
use vibesql_storage::{Database, Row, Table};
use vibesql_types::SqlValue;

use crate::errors::ExecutorError;

/// Find row indices that would conflict with an updated row (for REPLACE conflict resolution)
/// Returns a list of row indices that have conflicting PK or UNIQUE constraint values
pub(super) fn find_conflicting_rows_for_update(
    table: &Table,
    schema: &TableSchema,
    database: &Database,
    table_name: &str,
    new_row: &Row,
    current_row_index: usize,
) -> Vec<usize> {
    let mut conflicting_indices = Vec::new();

    // Check PRIMARY KEY conflicts
    if let Some(pk_indices) = schema.get_primary_key_indices() {
        let new_pk_values: Vec<SqlValue> =
            pk_indices.iter().map(|&idx| new_row.values[idx].clone()).collect();

        // Skip if any PK value is NULL
        if !new_pk_values.contains(&SqlValue::Null) {
            if let Some(pk_index) = table.primary_key_index() {
                if let Some(&existing_idx) = pk_index.get(&new_pk_values) {
                    // Don't consider the current row as a conflict
                    if existing_idx != current_row_index {
                        conflicting_indices.push(existing_idx);
                    }
                }
            }
        }
    }

    // Check UNIQUE constraint conflicts
    let unique_constraint_indices = schema.get_unique_constraint_indices();
    for (constraint_idx, unique_indices) in unique_constraint_indices.iter().enumerate() {
        let new_unique_values: Vec<SqlValue> =
            unique_indices.iter().map(|&idx| new_row.values[idx].clone()).collect();

        // Skip if any value is NULL
        if new_unique_values.contains(&SqlValue::Null) {
            continue;
        }

        let unique_indexes = table.unique_indexes();
        if constraint_idx < unique_indexes.len() {
            if let Some(&existing_idx) = unique_indexes[constraint_idx].get(&new_unique_values) {
                if existing_idx != current_row_index {
                    conflicting_indices.push(existing_idx);
                }
            }
        }
    }

    // Check user-defined UNIQUE indexes
    for index_name in database.list_indexes_for_table(table_name) {
        if let Some(index_metadata) = database.get_index(&index_name) {
            if !index_metadata.unique {
                continue;
            }

            // Build key values for this index
            // Skip expression indexes - they are handled separately
            let mut key_values = Vec::new();
            let mut is_expression_index = false;
            for index_col in &index_metadata.columns {
                if index_col.get_expression().is_some() {
                    is_expression_index = true;
                    break;
                }
                if let Some(col_name) = index_col.column_name() {
                    if let Some(col_idx) = schema.get_column_index(col_name) {
                        key_values.push(new_row.values[col_idx].clone());
                    }
                } else {
                    is_expression_index = true;
                    break;
                }
            }
            if is_expression_index {
                continue;
            }

            // Skip if any value is NULL
            if key_values.contains(&SqlValue::Null) {
                continue;
            }

            // Check if key exists in index
            if let Some(index_data) = database.get_index_data(&index_name) {
                if let Some(existing_indices) = index_data.get(&key_values) {
                    // Index data returns a Vec of row indices for this key
                    for existing_idx in existing_indices {
                        if existing_idx != current_row_index {
                            conflicting_indices.push(existing_idx);
                        }
                    }
                }
            }
        }
    }

    conflicting_indices
}

/// Validate that multiple updates in the same batch don't produce conflicting
/// PK or UNIQUE constraint values. This ensures SQL's deferred constraint semantics
/// where all rows must satisfy constraints after the entire UPDATE completes.
///
/// This catches cases like `UPDATE t SET pk = 1` when multiple rows are being updated -
/// all rows would end up with the same PK value, violating the UNIQUE constraint.
pub(super) fn validate_cross_update_uniqueness(
    updates: &[(usize, Row, Row, HashSet<usize>, bool)],
    schema: &TableSchema,
) -> Result<(), ExecutorError> {
    // Check PRIMARY KEY uniqueness across updates
    if let Some(pk_indices) = schema.get_primary_key_indices() {
        let mut seen_pks: HashSet<Vec<SqlValue>> = HashSet::new();

        for (_row_index, _old_row, new_row, _changed_columns, _updates_pk) in updates {
            let pk_values: Vec<SqlValue> =
                pk_indices.iter().map(|&idx| new_row.values[idx].clone()).collect();

            // Skip NULL PKs (they're allowed to have duplicates in the update set
            // since NULL != NULL)
            if pk_values.contains(&SqlValue::Null) {
                continue;
            }

            if !seen_pks.insert(pk_values.clone()) {
                let pk_col_names: Vec<String> = schema.primary_key.as_ref().unwrap().clone();
                return Err(ExecutorError::ConstraintViolation(format!(
                    "UNIQUE constraint failed: {} (multiple rows would have same key)",
                    pk_col_names.join(", ")
                )));
            }
        }
    }

    // Check UNIQUE constraint uniqueness across updates
    let unique_constraint_indices = schema.get_unique_constraint_indices();
    for (constraint_idx, unique_indices) in unique_constraint_indices.iter().enumerate() {
        let mut seen_values: HashSet<Vec<SqlValue>> = HashSet::new();

        for (_row_index, _old_row, new_row, _changed_columns, _updates_pk) in updates {
            let unique_values: Vec<SqlValue> =
                unique_indices.iter().map(|&idx| new_row.values[idx].clone()).collect();

            // Skip if any value is NULL
            if unique_values.contains(&SqlValue::Null) {
                continue;
            }

            if !seen_values.insert(unique_values.clone()) {
                let unique_col_names: Vec<String> =
                    schema.unique_constraints[constraint_idx].clone();
                return Err(ExecutorError::ConstraintViolation(format!(
                    "UNIQUE constraint failed: {} (multiple rows would have same key)",
                    unique_col_names.join(", ")
                )));
            }
        }
    }

    Ok(())
}

/// For UPDATE OR REPLACE: resolve cross-update conflicts by keeping only the last
/// update for each PK/UNIQUE value. Earlier updates are removed from the updates list
/// and their row indices are returned for deletion.
///
/// This ensures that when multiple rows are updated to the same PK/UNIQUE value,
/// only the last one (in order of processing) survives - matching SQLite's behavior.
pub(super) fn resolve_cross_update_conflicts_for_replace(
    updates: &mut Vec<(usize, Row, Row, HashSet<usize>, bool)>,
    schema: &TableSchema,
) -> Vec<usize> {
    let mut indices_to_delete = Vec::new();
    let mut indices_to_remove = HashSet::new();

    // Check PRIMARY KEY conflicts
    if let Some(pk_indices) = schema.get_primary_key_indices() {
        // Map: PK values -> (position in updates list, row_index)
        let mut pk_map: HashMap<Vec<SqlValue>, (usize, usize)> = HashMap::new();

        for (pos, (row_index, _old_row, new_row, _changed_columns, _updates_pk)) in
            updates.iter().enumerate()
        {
            // Skip if already marked for removal
            if indices_to_remove.contains(&pos) {
                continue;
            }

            let pk_values: Vec<SqlValue> =
                pk_indices.iter().map(|&idx| new_row.values[idx].clone()).collect();

            // Skip NULL PKs
            if pk_values.contains(&SqlValue::Null) {
                continue;
            }

            if let Some((prev_pos, prev_row_index)) = pk_map.get(&pk_values) {
                // Conflict found - earlier update should be deleted
                indices_to_remove.insert(*prev_pos);
                indices_to_delete.push(*prev_row_index);
            }
            pk_map.insert(pk_values, (pos, *row_index));
        }
    }

    // Check UNIQUE constraint conflicts
    let unique_constraint_indices = schema.get_unique_constraint_indices();
    for unique_indices in unique_constraint_indices.iter() {
        let mut unique_map: HashMap<Vec<SqlValue>, (usize, usize)> = HashMap::new();

        for (pos, (row_index, _old_row, new_row, _changed_columns, _updates_pk)) in
            updates.iter().enumerate()
        {
            // Skip if already marked for removal
            if indices_to_remove.contains(&pos) {
                continue;
            }

            let unique_values: Vec<SqlValue> =
                unique_indices.iter().map(|&idx| new_row.values[idx].clone()).collect();

            // Skip if any value is NULL
            if unique_values.contains(&SqlValue::Null) {
                continue;
            }

            if let Some((prev_pos, prev_row_index)) = unique_map.get(&unique_values) {
                // Conflict found - earlier update should be deleted
                if !indices_to_remove.contains(prev_pos) {
                    indices_to_remove.insert(*prev_pos);
                    indices_to_delete.push(*prev_row_index);
                }
            }
            unique_map.insert(unique_values, (pos, *row_index));
        }
    }

    // Remove conflicting updates from the list (in reverse order to maintain indices)
    let mut remove_positions: Vec<usize> = indices_to_remove.into_iter().collect();
    remove_positions.sort_unstable();
    remove_positions.reverse();
    for pos in remove_positions {
        updates.remove(pos);
    }

    indices_to_delete
}

/// Validate uniqueness against the post-statement table state (deferred PK/UNIQUE check).
///
/// SQLite defers UNIQUE constraint checks until the end of the statement. This means a
/// statement like `UPDATE p SET a = a - 1` succeeds even when intermediate states transiently
/// duplicate keys, as long as the final state has no duplicates.
///
/// This function implements that semantic. It checks each pending update's new PK / UNIQUE
/// values against the table's current index state, but excludes any "conflict" with a row
/// that is itself being updated to a different key in this same statement (because that row
/// is being moved away from the conflicting key).
///
/// Cross-update conflicts (multiple updates landing on the same key) are caught separately
/// by [`validate_cross_update_uniqueness`], which must be called before this function.
///
/// # Arguments
/// * `updates` - All pending updates: (row_index, old_row, new_row, changed_columns, updates_pk)
/// * `schema` - Table schema (for PK/UNIQUE constraint metadata)
/// * `table` - Storage table reference (for accessing PK/UNIQUE hash indexes)
/// * `database` - Database reference (for accessing user-defined UNIQUE indexes)
/// * `table_name` - Canonical table name
pub(super) fn validate_post_statement_uniqueness(
    updates: &[(usize, Row, Row, HashSet<usize>, bool)],
    schema: &TableSchema,
    table: &Table,
    database: &Database,
    table_name: &str,
) -> Result<(), ExecutorError> {
    // Build a map of (updated_row_index -> new PK values) so we can identify rows that
    // are being moved away from their original key.
    let pk_indices_opt = schema.get_primary_key_indices();
    let mut updated_new_pk: HashMap<usize, Vec<SqlValue>> = HashMap::new();
    if let Some(ref pk_indices) = pk_indices_opt {
        for (row_index, _old_row, new_row, _changed, _upd_pk) in updates {
            let new_pk: Vec<SqlValue> =
                pk_indices.iter().map(|&i| new_row.values[i].clone()).collect();
            updated_new_pk.insert(*row_index, new_pk);
        }
    }

    // PRIMARY KEY: for each pending update, check the new PK against the table's PK index.
    // If a conflicting row exists, only error if that row is NOT being updated to a different key
    // (because rows in the update set will be moved away; only their FINAL state matters).
    if let Some(ref pk_indices) = pk_indices_opt {
        if let Some(pk_index) = table.primary_key_index() {
            for (row_index, old_row, new_row, _changed, _upd_pk) in updates {
                let new_pk: Vec<SqlValue> =
                    pk_indices.iter().map(|&i| new_row.values[i].clone()).collect();
                let old_pk: Vec<SqlValue> =
                    pk_indices.iter().map(|&i| old_row.values[i].clone()).collect();

                // No-op update for PK: skip
                if new_pk == old_pk {
                    continue;
                }

                if let Some(&existing_idx) = pk_index.get(&new_pk) {
                    // Self: this row already had this PK
                    if existing_idx == *row_index {
                        continue;
                    }

                    // If the conflicting row is itself being updated to a different PK,
                    // it's being moved away — not a real conflict.
                    if let Some(other_new_pk) = updated_new_pk.get(&existing_idx) {
                        if other_new_pk != &new_pk {
                            continue;
                        }
                        // Otherwise the other row's new PK equals our new PK — that's a
                        // cross-update collision, which validate_cross_update_uniqueness
                        // should have already caught. Fall through to error for safety.
                    }

                    let pk_col_names: Vec<String> =
                        schema.primary_key.as_ref().unwrap().clone();
                    let qualified_cols: Vec<String> = pk_col_names
                        .iter()
                        .map(|col| format!("{}.{}", schema.name, col))
                        .collect();
                    return Err(ExecutorError::ConstraintViolation(format!(
                        "UNIQUE constraint failed: {}",
                        qualified_cols.join(", ")
                    )));
                }
            }
        }
    }

    // UNIQUE constraints (table-level): same logic as PK.
    let unique_constraint_indices = schema.get_unique_constraint_indices();
    let unique_indexes = table.unique_indexes();
    for (constraint_idx, unique_indices) in unique_constraint_indices.iter().enumerate() {
        if constraint_idx >= unique_indexes.len() {
            continue; // No backing hash index — fallback path is not deferred-aware
        }
        let unique_index = &unique_indexes[constraint_idx];

        // Build map of (updated_row_index -> new unique values) for this constraint
        let mut updated_new_unique: HashMap<usize, Vec<SqlValue>> = HashMap::new();
        for (row_index, _old_row, new_row, _changed, _upd_pk) in updates {
            let new_uv: Vec<SqlValue> =
                unique_indices.iter().map(|&i| new_row.values[i].clone()).collect();
            updated_new_unique.insert(*row_index, new_uv);
        }

        for (row_index, old_row, new_row, _changed, _upd_pk) in updates {
            let new_uv: Vec<SqlValue> =
                unique_indices.iter().map(|&i| new_row.values[i].clone()).collect();
            let old_uv: Vec<SqlValue> =
                unique_indices.iter().map(|&i| old_row.values[i].clone()).collect();

            // NULL values are exempt (NULL != NULL in SQL)
            if new_uv.contains(&SqlValue::Null) {
                continue;
            }

            // No-op for this constraint
            if new_uv == old_uv {
                continue;
            }

            if let Some(&existing_idx) = unique_index.get(&new_uv) {
                if existing_idx == *row_index {
                    continue;
                }
                if let Some(other_new_uv) = updated_new_unique.get(&existing_idx) {
                    if other_new_uv != &new_uv {
                        continue;
                    }
                }
                let unique_col_names: Vec<String> =
                    schema.unique_constraints[constraint_idx].clone();
                let qualified_cols: Vec<String> = unique_col_names
                    .iter()
                    .map(|col| format!("{}.{}", schema.name, col))
                    .collect();
                return Err(ExecutorError::ConstraintViolation(format!(
                    "UNIQUE constraint failed: {}",
                    qualified_cols.join(", ")
                )));
            }
        }
    }

    // User-defined UNIQUE indexes (CREATE UNIQUE INDEX).
    for index_name in database.list_indexes_for_table(table_name) {
        let index_metadata = match database.get_index(&index_name) {
            Some(m) => m,
            None => continue,
        };
        if !index_metadata.unique {
            continue;
        }

        // Resolve column indices for this index. Skip expression indexes (handled separately).
        let mut col_idxs: Vec<usize> = Vec::with_capacity(index_metadata.columns.len());
        let mut is_expression_index = false;
        for ic in &index_metadata.columns {
            if ic.get_expression().is_some() {
                is_expression_index = true;
                break;
            }
            let cn = match ic.column_name() {
                Some(n) => n,
                None => {
                    is_expression_index = true;
                    break;
                }
            };
            match schema.get_column_index(cn) {
                Some(ci) => col_idxs.push(ci),
                None => {
                    is_expression_index = true;
                    break;
                }
            }
        }
        if is_expression_index {
            continue;
        }

        // Build map of (updated_row_index -> new index key values) for this index
        let mut updated_new_key: HashMap<usize, Vec<SqlValue>> = HashMap::new();
        for (row_index, _old_row, new_row, _changed, _upd_pk) in updates {
            let nk: Vec<SqlValue> =
                col_idxs.iter().map(|&i| new_row.values[i].clone()).collect();
            updated_new_key.insert(*row_index, nk);
        }

        let index_data = match database.get_index_data(&index_name) {
            Some(d) => d,
            None => continue,
        };

        for (row_index, old_row, new_row, _changed, _upd_pk) in updates {
            let new_key: Vec<SqlValue> =
                col_idxs.iter().map(|&i| new_row.values[i].clone()).collect();
            let old_key: Vec<SqlValue> =
                col_idxs.iter().map(|&i| old_row.values[i].clone()).collect();

            // NULL values are exempt
            if new_key.contains(&SqlValue::Null) {
                continue;
            }

            // No-op
            if new_key == old_key {
                continue;
            }

            // Get all rows that currently hold this key
            let conflicting_rows = match index_data.get(&new_key) {
                Some(v) => v,
                None => continue,
            };

            // Check each conflicting row index: skip self and rows being moved off this key
            let mut real_conflict = false;
            for existing_idx in &conflicting_rows {
                if *existing_idx == *row_index {
                    continue;
                }
                if let Some(other_new_key) = updated_new_key.get(existing_idx) {
                    if other_new_key != &new_key {
                        // This row is being moved away — not a conflict
                        continue;
                    }
                }
                real_conflict = true;
                break;
            }

            if real_conflict {
                let columns_str = index_metadata
                    .columns
                    .iter()
                    .map(|col| {
                        format!("{}.{}", table_name, col.column_name().unwrap_or("?"))
                    })
                    .collect::<Vec<_>>()
                    .join(", ");
                return Err(ExecutorError::ConstraintViolation(format!(
                    "UNIQUE constraint failed: {}",
                    columns_str
                )));
            }
        }
    }

    Ok(())
}
