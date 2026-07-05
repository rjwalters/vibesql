use crate::{
    errors::ExecutorError, expression_index_maintenance, partial_index_maintenance, TriggerFirer,
};

/// Handle REPLACE logic: detect conflicts and delete conflicting rows
/// Returns Ok(()) if no conflict or conflict was resolved
///
/// This function implements SQLite REPLACE semantics:
/// 1. Find rows that conflict with the new row (by PK or UNIQUE constraints)
/// 2. Fire BEFORE DELETE triggers for each conflicting row (rowid is reserved)
/// 3. Release the reserved rowid (AFTER DELETE triggers can allocate new rowids)
/// 4. Delete the conflicting rows
/// 5. Fire AFTER DELETE triggers for each conflicting row
/// 6. The caller then inserts the new row (must re-reserve or use the rowid)
///
/// The `storage_table_name` is used to release the reserved rowid after BEFORE DELETE
/// triggers complete. This matches SQLite semantics where BEFORE DELETE trigger INSERTs
/// fail on rowid conflict, but AFTER DELETE trigger INSERTs can succeed (and may fail
/// on other constraints).
///
/// Deletion is a bitmap tombstone, not a physical removal: the conflicting old
/// version stays in `Table`'s backing storage (and is visible to the raw
/// `Table::scan()`) until compaction. Live reads (`scan_live()`, MVCC-aware
/// scans, and every `SELECT`) skip tombstones, so callers and clients see
/// exactly one row per key. (#5437 mistook a raw-`scan()` read of a tombstone
/// for a duplicate row; the conflict-delete itself is correct.)
pub fn handle_replace_conflicts(
    db: &mut vibesql_storage::Database,
    table_name: &str,
    _storage_table_name: &str,
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
    // (Re-bound below after BEFORE DELETE triggers run; rows whose delete a
    // RAISE(IGNORE) abandons are dropped from this vector — #5418.)

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

        // Check if this row matches any user-defined UNIQUE index
        // (This handles CREATE UNIQUE INDEX statements, including partial indexes)
        if !should_delete {
            for index_name in db.list_indexes_for_table(table_name) {
                if let Some(index_metadata) = db.get_index(&index_name) {
                    if !index_metadata.unique {
                        continue;
                    }

                    // Build key values for the new row
                    let mut new_key_values = Vec::new();
                    let mut existing_key_values = Vec::new();
                    let mut valid_index = true;

                    for index_col in &index_metadata.columns {
                        let col_name = index_col.expect_column_name();
                        if let Some(col_idx) = schema.get_column_index(col_name) {
                            new_key_values.push(row_values[col_idx].clone());
                            existing_key_values.push(row.values[col_idx].clone());
                        } else {
                            valid_index = false;
                            break;
                        }
                    }

                    if !valid_index {
                        continue;
                    }

                    // Skip if new row has NULL in key columns
                    if new_key_values.contains(&vibesql_types::SqlValue::Null) {
                        continue;
                    }

                    // Skip if existing row has NULL in key columns
                    if existing_key_values.contains(&vibesql_types::SqlValue::Null) {
                        continue;
                    }

                    // Note: Partial index WHERE clause is not yet supported in VibeSQL.
                    // For now, all unique indexes are treated as full indexes.

                    // Check if key values match
                    if new_key_values == existing_key_values {
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

    // Check if any DELETE triggers exist for this table.
    //
    // SQLite rule (lang_conflict.html): "When the REPLACE conflict resolution
    // strategy deletes rows in order to satisfy a constraint, delete triggers
    // fire if and only if recursive triggers are enabled." A plain `DELETE`
    // statement always fires its triggers, but the *implicit* deletes REPLACE
    // performs to clear a PK/UNIQUE conflict are gated on `recursive_triggers`.
    // With the default (OFF), these conflict-deletes are silent (triggerC-5.3,
    // #5840).
    let fire_delete_triggers = db
        .catalog
        .get_triggers_for_table(table_name, Some(vibesql_ast::TriggerEvent::Delete))
        .next()
        .is_some()
        && db.recursive_triggers();

    // Fire BEFORE DELETE triggers for each conflicting row
    // This must happen BEFORE actual deletion (SQLite semantics)
    // The reserved rowid is active during this phase, so trigger INSERTs that try
    // to allocate the same rowid will fail.
    //
    // RAISE(IGNORE) in a BEFORE DELETE trigger abandons the delete of that
    // conflicting row (#5418). We drop the row from `rows_to_delete` so it is
    // NOT deleted; the conflicting row then survives. Because the REPLACE
    // caller re-validates PK/UNIQUE constraints after this function returns,
    // a surviving conflict surfaces as a UNIQUE/PK error — matching sqlite3
    // 3.51 with `recursive_triggers=ON`, where a BEFORE DELETE RAISE(IGNORE)
    // during REPLACE leaves the row in place and the subsequent insert fails
    // with "UNIQUE constraint failed".
    if fire_delete_triggers {
        let mut kept = Vec::with_capacity(rows_to_delete.len());
        for (idx, row) in rows_to_delete {
            let outcome = TriggerFirer::execute_before_triggers(
                db,
                table_name,
                vibesql_ast::TriggerEvent::Delete,
                Some(&row),
                None,
            )?;
            if outcome != crate::trigger_execution::TriggerOutcome::SkipRow {
                kept.push((idx, row));
            }
        }
        rows_to_delete = kept;

        // Every conflicting row's delete was skipped by RAISE(IGNORE); there is
        // nothing to delete. Return early so the caller's constraint
        // re-validation runs against the still-conflicting table state.
        if rows_to_delete.is_empty() {
            return Ok(());
        }
    }

    // NOTE: The reserved rowid remains active during AFTER DELETE triggers.
    // For auto-allocated reservations: AFTER DELETE trigger INSERTs will fail on rowid conflict.
    // For explicit reservations: AFTER DELETE trigger INSERTs will skip to next rowid.
    // The caller is responsible for releasing the reservation after the REPLACE INSERT.

    // WAL-log the conflict deletes BEFORE applying them (issues #5835 /
    // #5871), mirroring the DELETE executor. Without these ops the REPLACE's
    // delete of the conflicting row is invisible to crash recovery: WAL
    // replay re-inserts the new row next to the undeleted old one, yielding
    // duplicate PRIMARY KEY rows after a restart (check-13.x, upsert5-3.x).
    for (row_index, row) in &rows_to_delete {
        db.emit_wal_delete(table_name, *row_index as u64, row.values.to_vec());
    }

    // Remove entries from user-defined indexes BEFORE deleting rows
    // (while row indices are still valid)
    // Use batch method for better performance (pre-computes column indices once)
    let rows_refs: Vec<(usize, &vibesql_storage::Row)> =
        rows_to_delete.iter().map(|(idx, row)| (*idx, row)).collect();
    db.batch_update_indexes_for_delete(table_name, &rows_refs);

    // Maintain expression indexes for each deleted row
    for (row_index, row) in &rows_to_delete {
        expression_index_maintenance::maintain_expression_indexes_for_delete(
            db, table_name, row, *row_index,
        );
        partial_index_maintenance::maintain_partial_indexes_for_delete(
            db, table_name, row, *row_index,
        );
    }

    // Collect indices for deletion
    let mut deleted_indices: Vec<usize> = rows_to_delete.iter().map(|(idx, _)| *idx).collect();
    deleted_indices.sort_unstable();

    // Phase 1c (Issue #5150 / #5136): capture the active txn id before
    // taking the mutable borrow on `table_mut`. The REPLACE-conflict path
    // bitmap-deletes the conflicting old version; we stamp xmax on the
    // tombstone when MVCC is on so visibility-aware reads (Phase 1d) can
    // tell who superseded the row.
    #[cfg(feature = "mvcc_enabled")]
    let mvcc_delete_txn_id = db.transaction_id();

    // Delete the conflicting rows using the fast path
    let table_mut = db
        .get_table_mut(table_name)
        .ok_or_else(|| ExecutorError::TableNotFound(table_name.to_string()))?;

    #[cfg(feature = "mvcc_enabled")]
    if let Some(id) = mvcc_delete_txn_id {
        for &idx in &deleted_indices {
            table_mut.stamp_row_xmax_inplace(idx, id);
        }
    }

    let delete_result = table_mut.delete_by_indices_batch(&deleted_indices);

    // Handle index maintenance based on whether compaction occurred
    if delete_result.compacted {
        // Compaction changed all row indices - rebuild indexes from scratch
        db.rebuild_indexes(table_name);
        // Partial indexes need WHERE-predicate evaluation per row; the
        // storage layer's `rebuild_indexes` skips them. Without this call,
        // partial-index row indices would point at the wrong table rows
        // after compaction (silent corruption).
        partial_index_maintenance::rebuild_partial_indexes_after_compaction(db, table_name);
        // NOTE: Expression-index compaction rebuild (`rebuild_expression_indexes_after_compaction`)
        // is intentionally not invoked here because the existing code path
        // never did so; that is a separate pre-existing gap tracked outside
        // this PR.
    }
    // No compaction: the deleted keys' index entries were already removed by
    // `batch_update_indexes_for_delete`, and the bitmap-delete model keeps every
    // surviving row's physical position stable, so no row-id renumbering is
    // needed (issue #5524 / #5537).

    // Invalidate the database-level columnar cache since table data changed.
    // Note: The table-level cache is already invalidated by delete_by_indices().
    // Both invalidations are necessary because they manage separate caches:
    // - Table-level cache: used by Table::scan_columnar() for SIMD filtering
    // - Database-level cache: used by Database::get_columnar() for cached access
    if delete_result.deleted_count > 0 {
        db.invalidate_columnar_cache(table_name);
    }

    // Fire AFTER DELETE triggers for each deleted row
    // This must happen AFTER actual deletion (SQLite semantics)
    if fire_delete_triggers {
        for (_, row) in &rows_to_delete {
            // AFTER DELETE fires once the row is already gone. A RAISE(IGNORE)
            // here cannot un-delete it (sqlite3 3.51 leaves the row deleted),
            // so SkipRow is a no-op — explicitly drop the must-use outcome.
            let _after_outcome = TriggerFirer::execute_after_triggers(
                db,
                table_name,
                vibesql_ast::TriggerEvent::Delete,
                Some(row),
                None,
            )?;
        }
    }

    Ok(())
}
