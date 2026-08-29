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
    explicit_rowid: Option<u64>,
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

        // Check if this row's own internal rowid collides with the new row's
        // explicitly-supplied rowid (`REPLACE INTO t(rowid, ...) VALUES(id,
        // ...)`, fkey2-13.1.2.*). This is independent of any declared
        // PRIMARY KEY / UNIQUE constraint: a table whose PK is a composite
        // key (or has none at all) still has a hidden internal rowid, and
        // SQLite's REPLACE conflict resolution treats a rowid collision the
        // same as any other UNIQUE-style conflict — the existing row must be
        // deleted (and the FK/trigger machinery below run against it) rather
        // than silently leaving two physical rows with the "same" rowid.
        // When the table's rowid IS aliased by an INTEGER PRIMARY KEY column,
        // this is redundant with the PK check above (harmless to re-check).
        if !should_delete {
            if let Some(target_rowid) = explicit_rowid {
                let this_row_id = row.row_id.unwrap_or((row_index + 1) as u64);
                if this_row_id == target_rowid {
                    should_delete = true;
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

    // SQLite performs foreign-key processing when a row is REPLACEd: the
    // implicit DELETE of each conflicting row must not silently orphan the
    // child rows that referenced it (fkey2-13.*, fkey1-5.2/5.4,
    // without_rowid3). Previously the conflict-clearing delete removed the
    // parent row without any FK processing at all.
    //
    // NO ACTION: SQLite performs this check at statement end — AFTER the
    // REPLACE re-inserts its new row. Because a foreign key's parent columns
    // are always a key (PRIMARY KEY / UNIQUE), at most one parent row can
    // carry a given key value, so a child orphaned by the conflict-delete is
    // repaired precisely when the re-inserted row restores that same
    // parent-key value (fkey2-13.1.3/4). We evaluate that repair here using
    // `row_values` (the row about to be inserted) rather than erroring
    // eagerly on the delete.
    //
    // RESTRICT: enforced *immediately* at delete time (before the new row is
    // re-inserted), so it does NOT get NO ACTION's statement-end repair — a
    // RESTRICT-constrained child still referencing the deleted key trips the
    // violation even when the re-inserted row would restore that exact key
    // (e_fkey-42.7/42.8).
    //
    // CASCADE / SET NULL / SET DEFAULT: the conflict-delete must run the
    // same referential action a plain DELETE of the conflicting row would
    // (fkey1-5.2/5.4: `INSERT OR REPLACE` deleting a self-referential
    // `ON DELETE CASCADE` parent must cascade to its children — and if that
    // cascade orphans the row the REPLACE is about to re-insert, the whole
    // statement fails with "FOREIGN KEY constraint failed"). Reuses the same
    // action handlers the DELETE executor uses (`delete::integrity`).
    if db.foreign_keys_enabled() {
        enforce_replace_fk_actions(db, table_name, &rows_to_delete, row_values)?;
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

    // When DELETE triggers fire, SQLite processes the conflicting rows one at a
    // time and INTERLEAVED: for each conflict row R it runs BEFORE DELETE on R,
    // physically removes R, then runs AFTER DELETE on R, *before* moving to the
    // next conflict row. A trigger body that reads the table mid-statement
    // (e.g. `SELECT count(*) FROM t`) therefore sees the table shrink between
    // conflict deletions — item 2b (#5840). The previous implementation fired
    // ALL BEFORE triggers, then batch-deleted every row, then fired all AFTERs,
    // which made every trigger body observe the same stale (pre-deletion) table
    // state. Route the trigger case through the interleaved path below; the
    // no-trigger case keeps the faster batch path that follows.
    //
    // The reserved rowid is active during this phase, so trigger INSERTs that
    // try to allocate the same rowid will fail.
    //
    // RAISE(IGNORE) in a BEFORE DELETE trigger abandons the delete of that
    // conflicting row (#5418): the row is not deleted and survives. Because the
    // REPLACE caller re-validates PK/UNIQUE constraints after this function
    // returns, a surviving conflict surfaces as a UNIQUE/PK error — matching
    // sqlite3 3.51 with `recursive_triggers=ON`.
    if fire_delete_triggers {
        // If an ancestor statement is already iterating this table (an outer
        // interleaved DELETE/UPDATE/REPLACE loop), defer our final compaction to
        // it so we never shift the ancestor's not-yet-processed row indices.
        // Captured BEFORE we register our own guard below.
        let defer_compaction = crate::compaction_guard::is_iterating(table_name);

        // Defer compaction across the whole loop so each row's physical index
        // stays valid until every conflict row is processed (mirrors the DELETE
        // executor's interleaved path, #5486). A nested DELETE fired by a
        // trigger body on this same table also defers compaction while the
        // guard is live.
        let _iter_guard = crate::compaction_guard::IterationGuard::new(table_name);

        #[cfg(feature = "mvcc_enabled")]
        let mvcc_delete_txn_id = db.transaction_id();

        let mut any_deleted = false;
        for (idx, row) in &rows_to_delete {
            // BEFORE(R): a RAISE(IGNORE) abandons this row's delete.
            let outcome = TriggerFirer::execute_before_triggers(
                db,
                table_name,
                vibesql_ast::TriggerEvent::Delete,
                Some(row),
                None,
            )?;
            if outcome == crate::trigger_execution::TriggerOutcome::SkipRow {
                continue;
            }

            // A trigger body may have already deleted R (e.g. via a nested
            // DELETE on this table). Skip R rather than double-deleting.
            if db.get_table(table_name).map(|t| t.is_row_deleted(*idx)).unwrap_or(true) {
                continue;
            }

            // WAL + index maintenance for this single row (indices are still
            // valid: compaction is deferred to the end of the loop).
            db.emit_wal_delete(table_name, *idx as u64, row.values.to_vec());
            let rows_refs: Vec<(usize, &vibesql_storage::Row)> = vec![(*idx, row)];
            db.batch_update_indexes_for_delete(table_name, &rows_refs);
            expression_index_maintenance::maintain_expression_indexes_for_delete(
                db, table_name, row, *idx,
            );
            partial_index_maintenance::maintain_partial_indexes_for_delete(
                db, table_name, row, *idx,
            );

            // delete(R): flip the deletion bitmap for just this row so the AFTER
            // trigger (and the next conflict row's BEFORE trigger) observes R as
            // gone. Compaction is deferred to the end of the loop.
            {
                let table_mut = db
                    .get_table_mut(table_name)
                    .ok_or_else(|| ExecutorError::TableNotFound(table_name.to_string()))?;

                #[cfg(feature = "mvcc_enabled")]
                if let Some(id) = mvcc_delete_txn_id {
                    table_mut.stamp_row_xmax_inplace(*idx, id);
                }

                if table_mut.mark_deleted_inplace(*idx) {
                    any_deleted = true;
                }
            }

            // Invalidate the database-level columnar cache NOW, between the
            // bitmap delete and the AFTER trigger (#5523), so a trigger body's
            // `SELECT count(*) FROM t` observes the live, decremented count
            // rather than a stale pre-delete snapshot.
            db.invalidate_columnar_cache(table_name);

            // AFTER(R): the row is already deleted; a RAISE(IGNORE) cannot
            // un-delete it, so SkipRow is a no-op.
            let _after_outcome = TriggerFirer::execute_after_triggers(
                db,
                table_name,
                vibesql_ast::TriggerEvent::Delete,
                Some(row),
                None,
            )?;
        }

        // Compact once, after all conflict rows are processed (unless deferred
        // to an ancestor interleaved loop). If it compacts, every row index
        // changed and the indexes must be rebuilt.
        if any_deleted && !defer_compaction {
            if let Some(table_mut) = db.get_table_mut(table_name) {
                if table_mut.compact_if_needed() {
                    db.rebuild_indexes(table_name);
                    expression_index_maintenance::rebuild_expression_indexes_after_compaction(
                        db, table_name,
                    );
                    partial_index_maintenance::rebuild_partial_indexes_after_compaction(
                        db, table_name,
                    );
                }
            }
        }

        return Ok(());
    }

    // ---- No DELETE triggers fire: fast batch-delete path ----

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

    // NOTE: No AFTER DELETE triggers fire on this batch path — it is only
    // reached when `fire_delete_triggers` is false (no DELETE triggers, or
    // `recursive_triggers` is OFF). The trigger-firing case is fully handled by
    // the interleaved per-row path above, which returns before reaching here.

    Ok(())
}

/// Enforce foreign keys against a REPLACE's conflict-clearing delete.
///
/// When a REPLACE deletes a conflicting parent row, any child row that
/// referenced that parent's key is affected exactly as it would be by a
/// plain `DELETE` of that row:
///
/// - **NO ACTION**: SQLite checks this at *statement end*, after the REPLACE re-inserts its new
///   row: the orphan is repaired if the new row restores the same parent-key value
///   (fkey2-13.1.3/4). Because a foreign key's parent columns are always a key (PRIMARY KEY /
///   UNIQUE), at most one parent row can carry a given key value, so "some surviving parent still
///   provides the key" reduces to "the re-inserted row provides the key".
/// - **RESTRICT**: enforced *immediately* at delete time (EVIDENCE-OF: R-37997-42187), before the
///   new row is (re-)inserted, so it never benefits from NO ACTION's statement-end repair — a
///   RESTRICT-constrained child still referencing the about-to-be-deleted key trips the violation
///   even when the re-inserted row would have restored that exact key (e_fkey-42.7/42.8).
/// - **CASCADE / SET NULL / SET DEFAULT**: the action actually runs against the child rows (reusing
///   `delete::integrity`'s DELETE-side handlers), exactly as a plain `DELETE FROM <parent_table>
///   WHERE ...` would (fkey1-5.2/5.4). If that action itself orphans the row the REPLACE is about
///   to re-insert (e.g. a self-referential CASCADE that removes the very parent key the new row
///   needs), the statement still fails.
///
/// Returns `Err(ConstraintViolation)` on any of the above.
fn enforce_replace_fk_actions(
    db: &mut vibesql_storage::Database,
    parent_table: &str,
    rows_to_delete: &[(usize, vibesql_storage::Row)],
    new_row_values: &[vibesql_types::SqlValue],
) -> Result<(), ExecutorError> {
    use vibesql_catalog::ReferentialAction;
    use vibesql_types::SqlValue;

    // Collect (child_table, fk, fk_idx, action) pairs up front so the
    // action-performing loop below does not hold a borrow of `db.catalog`
    // while mutating tables.
    let mut matching_fks: Vec<(String, vibesql_catalog::ForeignKeyConstraint, usize)> = Vec::new();
    for child_table_name in db.catalog.list_tables() {
        let child_schema = match db.catalog.get_table(&child_table_name) {
            Some(s) => s,
            None => continue,
        };
        if child_schema.foreign_keys.is_empty() {
            continue;
        }
        for (fk_idx, fk) in child_schema.foreign_keys.iter().enumerate() {
            if !fk.parent_table.eq_ignore_ascii_case(parent_table) {
                continue;
            }
            matching_fks.push((child_table_name.clone(), fk.clone(), fk_idx));
        }
    }

    let in_txn = db.in_transaction();
    let session_defer = db.defer_foreign_keys();

    // Built once for this REPLACE conflict-resolution pass and threaded
    // through every `cascade_delete` call below so a multi-level cascade
    // reuses it instead of rescanning the whole catalog per level (#6170
    // perf; see `delete::integrity::ChildFkIndex`).
    let child_index = crate::foreign_key_check::build_child_fk_index(db);

    for (child_table_name, fk, fk_idx) in &matching_fks {
        let parent_indices = crate::foreign_key_check::resolved_parent_indices_for_fk(db, fk);
        if parent_indices.is_empty()
            || parent_indices.iter().any(|&idx| idx >= new_row_values.len())
        {
            continue;
        }
        let parent_collations = crate::foreign_key_check::parent_collations_for_fk(db, fk);
        let parent_affinities = crate::foreign_key_check::parent_affinities_for_fk(db, fk);
        // Parent-key values the re-inserted row will provide (only relevant
        // to the NO ACTION / RESTRICT repair check below).
        let new_key: Vec<SqlValue> =
            parent_indices.iter().map(|&idx| new_row_values[idx].clone()).collect();

        for (_, del_row) in rows_to_delete {
            if parent_indices.iter().any(|&idx| idx >= del_row.values.len()) {
                continue;
            }
            let deleted_key: Vec<SqlValue> =
                parent_indices.iter().map(|&idx| del_row.values[idx].clone()).collect();

            // NULL parent-key values never match a child reference.
            if deleted_key.iter().any(|v| matches!(v, SqlValue::Null)) {
                continue;
            }

            match fk.on_delete {
                ReferentialAction::Cascade => {
                    crate::delete::integrity::cascade_delete(
                        db,
                        child_table_name,
                        fk,
                        *fk_idx,
                        &deleted_key,
                        in_txn,
                        session_defer,
                        &child_index,
                    )?;
                }
                ReferentialAction::SetNull => {
                    crate::delete::integrity::set_null(
                        db,
                        child_table_name,
                        fk,
                        *fk_idx,
                        &deleted_key,
                        in_txn,
                        session_defer,
                    )?;
                }
                ReferentialAction::SetDefault => {
                    crate::delete::integrity::set_default(
                        db,
                        child_table_name,
                        fk,
                        *fk_idx,
                        &deleted_key,
                        in_txn,
                        session_defer,
                    )?;
                }
                ReferentialAction::NoAction | ReferentialAction::Restrict => {
                    // RESTRICT is enforced *immediately* at delete time, not
                    // deferred to statement end like NO ACTION (EVIDENCE-OF:
                    // R-37997-42187 — "the RESTRICT action processing
                    // happens as soon as the field is updated - not at the
                    // end of the current statement"). Because REPLACE's
                    // conflict-delete happens strictly before the new row is
                    // (re-)inserted, a RESTRICT-constrained child that still
                    // references the about-to-be-deleted key must trip the
                    // violation immediately — it cannot be forgiven by a
                    // same-statement re-insert that would restore the key,
                    // unlike NO ACTION's statement-end repair check below
                    // (e_fkey-42.7/42.8: `REPLACE INTO parent VALUES('key1')`
                    // errors even though the re-inserted row restores 'key1'
                    // exactly, because child1's FK is ON DELETE RESTRICT).
                    //
                    // NO ACTION only: if the re-inserted row restores this
                    // exact key, no child that referenced the deleted row
                    // can be orphaned by the time the statement-end check
                    // runs.
                    if matches!(fk.on_delete, ReferentialAction::NoAction) {
                        let restored =
                            deleted_key.iter().zip(&new_key).enumerate().all(|(i, (dk, nk))| {
                                crate::foreign_key_check::fk_values_equal(
                                    dk,
                                    nk,
                                    parent_collations.get(i).and_then(|c| c.as_deref()),
                                    parent_affinities
                                        .get(i)
                                        .copied()
                                        .unwrap_or(vibesql_types::TypeAffinity::None),
                                )
                            });
                        if restored {
                            continue;
                        }
                    }

                    // The key is being dropped: any child row that
                    // references it is now orphaned. Scan the child table
                    // (visibility-aware) for such a reference.
                    let snapshot = crate::mvcc::read_snapshot(db);
                    let child_table = match db.get_table(child_table_name) {
                        Some(t) => t,
                        None => continue,
                    };
                    let orphaned = child_table.scan_visible(&snapshot).any(|(_, child_row)| {
                        let child_fk_values: Vec<SqlValue> = fk
                            .column_indices
                            .iter()
                            .map(|&idx| child_row.values[idx].clone())
                            .collect();
                        if child_fk_values.iter().any(|v| matches!(v, SqlValue::Null)) {
                            return false;
                        }
                        child_fk_values.iter().zip(&deleted_key).enumerate().all(|(i, (cv, pv))| {
                            crate::foreign_key_check::fk_values_equal(
                                cv,
                                pv,
                                parent_collations.get(i).and_then(|c| c.as_deref()),
                                parent_affinities
                                    .get(i)
                                    .copied()
                                    .unwrap_or(vibesql_types::TypeAffinity::None),
                            )
                        })
                    });

                    if orphaned {
                        return Err(ExecutorError::ConstraintViolation(format!(
                            "FOREIGN KEY constraint violation: cannot delete or update a parent row when a foreign key constraint exists. The conflict occurred in table '{}', constraint '{}'.",
                            child_table_name,
                            fk.name.as_deref().unwrap_or(""),
                        )));
                    }
                }
            }
        }
    }

    Ok(())
}
