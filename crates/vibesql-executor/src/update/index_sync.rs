//! Index maintenance coordination for UPDATE operations
//!
//! This module handles:
//! - Detecting conflicting rows for UPDATE OR REPLACE operations
//! - Cross-update uniqueness validation (preventing multiple rows from getting same PK)
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
