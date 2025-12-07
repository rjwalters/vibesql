use crate::errors::ExecutorError;

/// Handle REPLACE logic: detect conflicts and delete conflicting rows
/// Returns Ok(()) if no conflict or conflict was resolved
pub fn handle_replace_conflicts(
    db: &mut vibesql_storage::Database,
    table_name: &str,
    schema: &vibesql_catalog::TableSchema,
    row_values: &[vibesql_types::SqlValue],
) -> Result<(), ExecutorError> {
    // Build list of row values to match for deletion
    let mut pk_match: Option<Vec<vibesql_types::SqlValue>> = None;
    let mut unique_matches: Vec<Option<Vec<vibesql_types::SqlValue>>> = Vec::new();

    // Get indices once before the closure (performance optimization)
    let pk_indices = schema.get_primary_key_indices();
    let unique_constraint_indices = schema.get_unique_constraint_indices();

    // Check PRIMARY KEY conflict
    if let Some(ref pk_idx) = pk_indices {
        pk_match = Some(pk_idx.iter().map(|&idx| row_values[idx].clone()).collect());
    }

    // Check UNIQUE constraints conflicts
    for unique_indices in unique_constraint_indices.iter() {
        let unique_values: Vec<vibesql_types::SqlValue> =
            unique_indices.iter().map(|&idx| row_values[idx].clone()).collect();

        // Skip if contains NULL (NULLs don't cause conflicts in UNIQUE constraints)
        if unique_values.contains(&vibesql_types::SqlValue::Null) {
            unique_matches.push(None);
        } else {
            unique_matches.push(Some(unique_values));
        }
    }

    // First, find conflicting rows and their indices (read-only pass)
    let table = db
        .get_table(table_name)
        .ok_or_else(|| ExecutorError::TableNotFound(table_name.to_string()))?;

    let mut rows_to_delete: Vec<(usize, vibesql_storage::Row)> = Vec::new();

    // Use scan_live() to skip deleted rows and get correct physical indices
    for (row_index, row) in table.scan_live() {
        let mut should_delete = false;

        // Check if this row matches the PRIMARY KEY
        if let Some(ref pk_values) = pk_match {
            if let Some(ref pk_idx) = pk_indices {
                let row_pk_values: Vec<vibesql_types::SqlValue> =
                    pk_idx.iter().map(|&idx| row.values[idx].clone()).collect();
                if &row_pk_values == pk_values {
                    should_delete = true;
                }
            }
        }

        // Check if this row matches any UNIQUE constraint
        if !should_delete {
            for (constraint_idx, unique_indices) in unique_constraint_indices.iter().enumerate() {
                if let Some(unique_values) =
                    unique_matches.get(constraint_idx).and_then(|v| v.as_ref())
                {
                    let row_unique_values: Vec<vibesql_types::SqlValue> =
                        unique_indices.iter().map(|&idx| row.values[idx].clone()).collect();
                    if row_unique_values == *unique_values {
                        should_delete = true;
                        break;
                    }
                }
            }
        }

        if should_delete {
            rows_to_delete.push((row_index, row.clone()));
        }
    }

    // If no conflicts, nothing to delete
    if rows_to_delete.is_empty() {
        return Ok(());
    }

    // Remove entries from user-defined indexes BEFORE deleting rows
    // (while row indices are still valid)
    // Use batch method for better performance (pre-computes column indices once)
    let rows_refs: Vec<(usize, &vibesql_storage::Row)> =
        rows_to_delete.iter().map(|(idx, row)| (*idx, row)).collect();
    db.batch_update_indexes_for_delete(table_name, &rows_refs);

    // Collect indices for deletion
    let mut deleted_indices: Vec<usize> = rows_to_delete.iter().map(|(idx, _)| *idx).collect();
    deleted_indices.sort_unstable();

    // Delete the conflicting rows using the fast path
    let table_mut = db
        .get_table_mut(table_name)
        .ok_or_else(|| ExecutorError::TableNotFound(table_name.to_string()))?;

    let delete_result = table_mut.delete_by_indices_batch(&deleted_indices);

    // Handle index maintenance based on whether compaction occurred
    if delete_result.compacted {
        // Compaction changed all row indices - rebuild indexes from scratch
        db.rebuild_indexes(table_name);
    } else {
        // No compaction - just adjust remaining user-defined index entries
        // (entries pointing to indices > deleted need to be decremented)
        db.adjust_indexes_after_delete(table_name, &deleted_indices);
    }

    // Invalidate the database-level columnar cache since table data changed.
    // Note: The table-level cache is already invalidated by delete_by_indices().
    // Both invalidations are necessary because they manage separate caches:
    // - Table-level cache: used by Table::scan_columnar() for SIMD filtering
    // - Database-level cache: used by Database::get_columnar() for cached access
    if delete_result.deleted_count > 0 {
        db.invalidate_columnar_cache(table_name);
    }

    Ok(())
}
