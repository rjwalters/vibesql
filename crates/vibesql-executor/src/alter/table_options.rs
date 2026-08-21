//! Table-level operation executors for ALTER TABLE

use vibesql_ast::{RenameTableStmt, TriggerAction, TriggerEvent};
use vibesql_catalog::TriggerDefinition;
use vibesql_storage::Database;

use crate::{
    errors::ExecutorError,
    trigger_rename::{
        rewrite_column_refs_in_trigger_sql, rewrite_table_refs_in_trigger_sql,
        rewrite_table_refs_in_view_sql,
    },
};

/// Execute RENAME TABLE
pub(super) fn execute_rename_table(
    stmt: &RenameTableStmt,
    database: &mut Database,
) -> Result<String, ExecutorError> {
    // A view is not a table: SQLite rejects `ALTER TABLE <view> RENAME TO ...`
    // with a view-specific message (`view <name> may not be altered`), before
    // any table-name resolution — otherwise it falls through to the generic
    // (and wrong) `no such table` (alter-12.2). Uses the view's stored name for
    // case-preserving output, matching how RENAME COLUMN's equivalent check
    // (below in `columns::execute_rename_column`) reports it.
    if let Some(view) = database.catalog.get_view(&stmt.table_name) {
        return Err(ExecutorError::Other(format!("view {} may not be altered", view.name)));
    }

    // Reject renaming a table to a reserved `sqlite_`-prefixed name. SQLite
    // reserves that prefix for its own schema objects and errors
    // `object name reserved for internal use: <name>` (sqlite3 3.51.0,
    // alter-2.5), echoing the target name verbatim. Without this guard a user
    // could smuggle a `sqlite_`-prefixed *user table* into the catalog via
    // RENAME; that table then dumps as `CREATE TABLE sqlite_x (...)`, which the
    // load-path reserved-name guard would reject — bricking the database on
    // reload (issue #5614). Guarding RENAME closes that gap at the source.
    if crate::sqlite_schema::is_reserved_object_name(&stmt.new_table_name) {
        return Err(ExecutorError::SqliteCompatError(format!(
            "object name reserved for internal use: {}",
            stmt.new_table_name
        )));
    }

    // Check if the new name already names a table or an index. SQLite shares a
    // single object namespace for tables and indexes, so a RENAME TO collision
    // against either reports the same SQLite-compatible message
    // (`there is already another table or index with this name: <name>`).
    if database.get_table(&stmt.new_table_name).is_some()
        || database.index_exists(&stmt.new_table_name)
    {
        return Err(ExecutorError::RenameTargetExists(stmt.new_table_name.clone()));
    }

    // Get the old table to ensure it exists
    let old_table = database
        .get_table(&stmt.table_name)
        .ok_or_else(|| ExecutorError::TableNotFound(stmt.table_name.clone()))?;

    // Clone the table and update its schema name
    let mut new_table = old_table.clone();
    new_table.schema_mut().name = stmt.new_table_name.clone();
    // The cloned schema still carries the verbatim CREATE TABLE text captured
    // for the *old* table name (issue #5619). SQLite edits this text in place on
    // RENAME TO — rewriting the table name to the double-quoted new name and
    // preserving all other formatting — rather than reconstructing it
    // (issue #5634). Apply the same in-place edit here. The dump statement
    // splitter is now quote-aware (a `'` inside the emitted `"new_name"`
    // identifier no longer desyncs reload — see
    // `vibesql_storage::persistence::load::parse_sql_statements`), so the
    // preserved verbatim text round-trips through both the `.sql` and `.vbsql`
    // formats. If the in-place edit cannot be applied cleanly, fall back to the
    // proven-safe invalidate-and-reconstruct path (which emits a clean,
    // plainly-quoted name) — preserving the re-parseable-on-reload invariant
    // (issue #5619).
    let renamed_sql = new_table
        .schema
        .sql_source
        .as_deref()
        .and_then(|sql| crate::alter_rewrite::rename_table(sql, &stmt.new_table_name));
    match renamed_sql {
        Some(text) => new_table.schema_mut().set_sql_source(text),
        None => new_table.schema_mut().invalidate_sql_source(),
    }

    // `database.drop_table` below cascade-drops every trigger whose `ON <table>`
    // target is the table being dropped (the correct behavior for a genuine
    // `DROP TABLE`, per SQL standard R-37808-62273). RENAME TABLE, however, is
    // implemented here as drop-old + create-new, and SQLite's RENAME TABLE does
    // *not* drop such triggers — it rewrites their `ON`-target (and any body
    // references) to the new name and keeps them (issue #6174, alter-21.2/21.4).
    // Snapshot those triggers before the drop so they can be restored into the
    // catalog afterward, before `rewrite_triggers_for_rename` runs (which
    // performs the actual on-target/body rewrite over whatever triggers are in
    // the catalog at that point).
    let triggers_on_renamed_table: Vec<TriggerDefinition> = database
        .catalog
        .iter_triggers()
        .filter(|t| t.table_name.eq_ignore_ascii_case(&stmt.table_name))
        .cloned()
        .collect();

    // Drop old table and create new one with the renamed schema
    // This handles indexes and spatial indexes via CASCADE
    database
        .drop_table(&stmt.table_name)
        .map_err(|e| ExecutorError::StorageError(e.to_string()))?;

    database
        .create_table(new_table.schema.clone())
        .map_err(|e| ExecutorError::StorageError(e.to_string()))?;

    // Restore the triggers cascade-dropped above so `rewrite_triggers_for_rename`
    // (below) can rewrite their `ON`-target/body to the new table name instead of
    // losing them permanently.
    for trigger in triggers_on_renamed_table {
        let _ = database.catalog.create_trigger(trigger);
    }

    // Restore the data by getting the new table and setting its rows
    let restored_table = database
        .get_table_mut(&stmt.new_table_name)
        .ok_or_else(|| ExecutorError::TableAlreadyExists(stmt.new_table_name.clone()))?;

    for row in new_table.scan() {
        restored_table
            .insert(row.clone())
            .map_err(|e| ExecutorError::StorageError(e.to_string()))?;
    }

    // Propagate the rename into any trigger definitions that reference the old
    // table name. SQLite (legacy_alter_table=OFF) rewrites table references
    // inside trigger bodies and the trigger's ON-target, and keeps the stored
    // `sqlite_master.sql` text consistent. See `crate::trigger_rename`.
    rewrite_triggers_for_rename(database, &stmt.table_name, &stmt.new_table_name);

    // Re-bind every child table's foreign key that referenced the old parent
    // name, and rewrite its verbatim REFERENCES text. SQLite
    // (legacy_alter_table=OFF) rewrites both the child's in-memory FK target and
    // the stored `sqlite_master.sql` REFERENCES clause so cascade enforcement
    // survives the rename. Without this, `fk.parent_table` keeps pointing at the
    // now-nonexistent old name (silently severing enforcement) and a reload would
    // resurrect the stale binding from the un-rewritten sql_source. Runs after
    // the drop+create above, so the renamed table already exists under its new
    // name — self-referential FKs on it are picked up by the same loop.
    rebind_child_foreign_keys(database, &stmt.table_name, &stmt.new_table_name);

    // Propagate the rename into any dependent VIEW definitions that reference the
    // old table name. SQLite (legacy_alter_table=OFF) rewrites view bodies the
    // same way it rewrites trigger bodies and FK REFERENCES clauses on
    // `ALTER TABLE ... RENAME TO`; without this, a view resolving the old name
    // fails with a stale "table not found" lookup on the next read (issue
    // #6303). Mirrors `rewrite_views_for_column_rename`'s two-phase compute/
    // commit shape, but table-reference rewriting is purely lexical (see
    // `rewrite_table_refs_in_view_sql`) so there is no ambiguity to abort on.
    rewrite_views_for_table_rename(database, &stmt.table_name, &stmt.new_table_name);

    // Invalidate the database-level columnar cache for both old and new table names.
    // The old table name's cache is invalidated since the table no longer exists,
    // and the new table name's cache is invalidated to ensure fresh columnar data.
    database.invalidate_columnar_cache(&stmt.table_name);
    database.invalidate_columnar_cache(&stmt.new_table_name);

    Ok(format!("Table '{}' renamed to '{}'", stmt.table_name, stmt.new_table_name))
}

/// Re-bind foreign keys that referenced a just-renamed parent table.
///
/// For every table in the database, any `ForeignKeyConstraint` whose
/// `parent_table` matches `old_name` (case-insensitively) is repointed to
/// `new_name`, and the table's verbatim `sql_source` `REFERENCES <old_name>`
/// text is rewritten to `REFERENCES "<new_name>"` so `sqlite_master.sql` and any
/// future reload (which rehydrates constraints from `sql_source`) stay
/// consistent. Mirrors SQLite's `sqlite_rename_parent` under
/// `legacy_alter_table=OFF`.
///
/// The storage `Table` schema is the mutation target; the catalog copy is then
/// re-synced from it (matching the `sync_catalog_schema_from_storage` pattern in
/// `alter/mod.rs`). If the `REFERENCES` text cannot be rewritten cleanly, the
/// stale `sql_source` is invalidated so it is reconstructed from the (already
/// re-bound) schema on next serialization — never left pointing at the old name.
fn rebind_child_foreign_keys(database: &mut Database, old_name: &str, new_name: &str) {
    for tbl in database.list_tables() {
        // Mutate the storage copy in place, returning the updated schema to sync
        // into the catalog. The `continue` short-circuits tables with no matching
        // FK so untouched tables are neither cloned nor re-inserted.
        let updated_schema = {
            let Some(table) = database.get_table_mut(&tbl) else {
                continue;
            };
            let mut changed = false;
            for fk in table.schema_mut().foreign_keys.iter_mut() {
                if fk.parent_table.eq_ignore_ascii_case(old_name) {
                    fk.parent_table = new_name.to_string();
                    changed = true;
                }
            }
            if !changed {
                continue;
            }
            let rewritten = table.schema.sql_source.as_deref().and_then(|sql| {
                crate::alter_rewrite::rename_references_parent(sql, old_name, new_name)
            });
            match rewritten {
                Some(text) => table.schema_mut().set_sql_source(text),
                None => table.schema_mut().invalidate_sql_source(),
            }
            table.schema.clone()
        };
        database.catalog.replace_table_schema(&tbl, updated_schema);
    }
}

/// Rewrite all trigger definitions in the catalog that reference `old_name`,
/// replacing table references with `new_name`.
///
/// This keeps three pieces of trigger state consistent with SQLite's
/// `legacy_alter_table=OFF` behavior:
/// - `table_name`: the trigger's ON-target, if it was the renamed table;
/// - `triggered_action` (the raw body SQL used when the trigger fires);
/// - `sql_definition` (the verbatim `CREATE TRIGGER` text shown in
///   `sqlite_master`/`sqlite_schema`).
fn rewrite_triggers_for_rename(database: &mut Database, old_name: &str, new_name: &str) {
    // Snapshot trigger definitions up front. Triggers are keyed per schema, so a
    // name-only `get_trigger` resolves temp-first and would rewrite a `temp`
    // trigger twice while leaving its `main` namesake untouched (issue #6296).
    // Cloning also frees the immutable catalog borrow so `update_trigger` can
    // mutate below.
    let existing_triggers: Vec<_> = database.catalog.iter_triggers().cloned().collect();
    for existing in existing_triggers {
        // Does this trigger reference the renamed table anywhere?
        let on_target_match = existing.table_name.eq_ignore_ascii_case(old_name);
        let body_text = match &existing.triggered_action {
            TriggerAction::RawSql(sql) => sql.clone(),
        };
        let new_body = rewrite_table_refs_in_trigger_sql(&body_text, old_name, new_name);
        let new_sql_definition = existing
            .sql_definition
            .as_ref()
            .map(|sql| rewrite_table_refs_in_trigger_sql(sql, old_name, new_name));

        let body_changed = new_body != body_text;
        let sql_def_changed = new_sql_definition.as_deref() != existing.sql_definition.as_deref();

        if !on_target_match && !body_changed && !sql_def_changed {
            continue;
        }

        let mut updated = existing.clone();
        if on_target_match {
            updated.table_name = new_name.to_string();
        }
        if body_changed {
            updated.triggered_action = TriggerAction::RawSql(new_body);
        }
        updated.sql_definition = new_sql_definition;

        // The trigger is known to exist (we just read it), so update_trigger
        // cannot fail; ignore the result defensively.
        let _ = database.catalog.update_trigger(updated);
    }
}

/// Rewrite all VIEW definitions in the catalog that reference `old_name` as a
/// table, replacing table references with `new_name`, when
/// `ALTER TABLE <old_name> RENAME TO <new_name>` runs.
///
/// SQLite (`legacy_alter_table=OFF`) rewrites references to a renamed table
/// inside dependent view bodies and updates the stored `sqlite_master.sql`
/// text, mirroring what it does for trigger bodies
/// ([`rewrite_triggers_for_rename`]) and child FK `REFERENCES` clauses
/// ([`rebind_child_foreign_keys`]). VibeSQL materializes views by executing
/// their stored `query` AST, so both the verbatim `sql_definition` and the
/// parsed `query` are rewritten here (the latter by re-parsing the rewritten
/// text) so the view keeps working after the rename — matching the shape used
/// by `rewrite_views_for_column_rename`.
///
/// Unlike the column-rename cascade, a table-name rewrite is purely lexical
/// (no schema-aware resolution of unqualified columns is involved), so there
/// is no ambiguity case to abort on. A rewritten view body should always
/// re-parse (only an identifier changed), but if it somehow doesn't, only the
/// verbatim `sql_definition` is updated; the in-memory `query` AST is left
/// untouched (still naming the old table) rather than risk leaving the view in
/// a half-updated state. `sqlite_master.sql` reflects the rename either way, so
/// a fresh load re-resolves the view correctly even in that unexpected case.
fn rewrite_views_for_table_rename(database: &mut Database, old_name: &str, new_name: &str) {
    let view_names = database.catalog.list_views();
    let mut pending: Vec<(String, String)> = Vec::new();
    for name in view_names {
        let Some(view) = database.catalog.get_view(&name) else {
            continue;
        };
        // Prefer the verbatim `CREATE VIEW` text; fall back to reconstructing it
        // from the parsed query when a view was stored without captured source,
        // so a view still tracks the rename either way.
        let old_text = view.sql_definition.clone().unwrap_or_else(|| {
            use vibesql_ast::pretty_print::ToSql;
            let cols =
                view.columns.as_ref().map(|c| format!("({})", c.join(", "))).unwrap_or_default();
            format!("CREATE VIEW {}{} AS {}", view.name, cols, view.query.to_sql())
        });
        let new_text = rewrite_table_refs_in_view_sql(&old_text, old_name, new_name);
        if new_text != old_text {
            pending.push((name, new_text));
        }
    }

    for (name, new_text) in pending {
        match vibesql_parser::parse_with_arena_fallback(&new_text) {
            Ok(vibesql_ast::Statement::CreateView(cv)) => {
                if let Some(view) = database.catalog.get_view_mut(&name) {
                    view.query = *cv.query;
                    view.sql_definition = Some(new_text);
                }
            }
            _ => {
                // The rewritten text failed to (re)parse cleanly — extremely
                // unlikely for a purely lexical identifier swap, but fall back
                // to updating only the verbatim text so `sqlite_master` still
                // reflects the rename; the in-memory `query` AST (which never
                // referenced the table by name at this layer) keeps working.
                if let Some(view) = database.catalog.get_view_mut(&name) {
                    view.sql_definition = Some(new_text);
                }
            }
        }
    }
}

/// Rewrite trigger bodies that reference `<table>.<old_column>` after an
/// `ALTER TABLE <table> RENAME <old_column> TO <new_column>`.
///
/// Mirrors SQLite's `legacy_alter_table=OFF` behavior: references inside trigger
/// bodies that resolve to the renamed column are rewritten (unquoted), while the
/// rest of the `CREATE TRIGGER` text is preserved verbatim. The renamed table's
/// own `ON`-target name does not change. See `crate::trigger_rename`.
///
/// If an unqualified reference to the renamed column is *ambiguous* in a trigger
/// body (the renamed table and another in-scope table both own a column of that
/// name), SQLite aborts the whole ALTER and leaves the schema unchanged. This
/// returns `Err(ExecutorError::AmbiguousColumnName { .. })` (wrapped to mirror
/// SQLite's `error in trigger <name>: ambiguous column name: <col>` message) so
/// the caller can abort before committing the rename.
pub(super) fn rewrite_triggers_for_column_rename(
    database: &mut Database,
    table: &str,
    old_column: &str,
    new_column: &str,
) -> Result<(), ExecutorError> {
    // Snapshot table -> column-name set so the rewrite resolver can attribute
    // unqualified columns to their owning table without borrowing the database
    // while we mutate the catalog. The snapshot reflects the *post-rename* schema
    // (the column has already been renamed), so the renamed table's pre-rename
    // column name is handled explicitly in the closure below.
    let schema: std::collections::HashMap<String, Vec<String>> = database
        .list_tables()
        .into_iter()
        .filter_map(|name| {
            database.get_table(&name).map(|tbl| {
                (
                    name.to_ascii_lowercase(),
                    tbl.schema.columns.iter().map(|c| c.name.clone()).collect(),
                )
            })
        })
        .collect();

    let renamed_table_lc = table.to_ascii_lowercase();
    let old_column_owned = old_column.to_string();
    let table_has_column = move |t: &str, c: &str| -> bool {
        let t_lc = t.to_ascii_lowercase();
        // The renamed table still owns `old_column` for resolution purposes even
        // though the live schema now stores it under `new_column`.
        if t_lc == renamed_table_lc && c.eq_ignore_ascii_case(&old_column_owned) {
            return true;
        }
        schema.get(&t_lc).is_some_and(|cols| cols.iter().any(|col| col.eq_ignore_ascii_case(c)))
    };

    // Two-phase: compute every trigger update first so that an ambiguity error
    // in any trigger aborts the whole ALTER *before* any catalog mutation. This
    // mirrors SQLite, which leaves the schema entirely unchanged on a genuine
    // ambiguity rather than partially rewriting earlier triggers.
    // Snapshot trigger definitions up front. Triggers are keyed per schema, so a
    // name-only `get_trigger` resolves temp-first and would rewrite a `temp`
    // trigger twice while leaving its `main` namesake untouched (issue #6296).
    let existing_triggers: Vec<_> = database.catalog.iter_triggers().cloned().collect();
    let mut pending_updates = Vec::new();
    for existing in existing_triggers {
        let name = existing.name.clone();

        let body_text = match &existing.triggered_action {
            TriggerAction::RawSql(sql) => sql.clone(),
        };
        // The `NEW`/`OLD` pseudo-tables always alias the trigger's own subject
        // table, so `new.<col>` / `old.<col>` references resolve to the renamed
        // table only when this trigger fires on the renamed table. Gate the
        // new/old rewrite on that (SQLite rewrites the WHEN clause and body
        // `new.old_col` references for such triggers, e.g. altercol-3.x).
        let subject_is_renamed = existing.table_name.eq_ignore_ascii_case(table);
        // An ambiguous unqualified reference to the renamed column aborts the
        // ALTER (SQLite: "error in trigger <name>: ambiguous column name: <col>").
        // Map the resolver error to that message, using `Other` so the verbatim
        // SQLite wording is preserved.
        let ambiguity_error = |col: String| {
            ExecutorError::Other(format!(
                "error in trigger {}: ambiguous column name: {}",
                name, col
            ))
        };
        let new_body = rewrite_column_refs_in_trigger_sql(
            &body_text,
            table,
            old_column,
            new_column,
            &table_has_column,
            subject_is_renamed,
        )
        .map_err(ambiguity_error)?;
        let new_sql_definition = existing
            .sql_definition
            .as_ref()
            .map(|sql| {
                rewrite_column_refs_in_trigger_sql(
                    sql,
                    table,
                    old_column,
                    new_column,
                    &table_has_column,
                    subject_is_renamed,
                )
            })
            .transpose()
            .map_err(ambiguity_error)?;

        // The runtime WHEN condition is stored as a separate AST (evaluated per
        // row, not re-parsed from the trigger text), so it must be rewritten in
        // its own right. A WHEN clause can only reference the subject table's
        // columns via `NEW`/`OLD`, so rewriting is unambiguous when the trigger
        // fires on the renamed table (altercol-3.3).
        let new_when_condition = if subject_is_renamed {
            existing.when_condition.as_ref().map(|expr| {
                let mut rewritten = expr.clone();
                let changed = vibesql_ast::rename::rename_column_in_expression(
                    &mut rewritten,
                    old_column,
                    new_column,
                );
                (rewritten, changed)
            })
        } else {
            None
        };

        // An `UPDATE OF <col-list>` trigger stores its watched columns
        // separately from the body (`TriggerEvent::Update(Some(cols))`), and
        // `should_fire_update_of` (trigger_execution.rs) looks each one up by
        // exact name against the *current* table schema. Left un-rewritten,
        // a renamed watched column silently stops matching (the lookup
        // returns `None`, so it is never counted as "changed"), and the
        // trigger stops firing on updates to its own renamed column —
        // matching SQLite's `sqlite_rename_column` propagation into the
        // trigger's event mask (altercol-7.1.3). Only applies when the
        // trigger fires on the renamed table (the column list always names
        // that table's columns).
        let new_event = if subject_is_renamed {
            if let TriggerEvent::Update(Some(cols)) = &existing.event {
                if cols.iter().any(|c| c.eq_ignore_ascii_case(old_column)) {
                    let rewritten: Vec<String> = cols
                        .iter()
                        .map(|c| {
                            if c.eq_ignore_ascii_case(old_column) {
                                new_column.to_string()
                            } else {
                                c.clone()
                            }
                        })
                        .collect();
                    Some(TriggerEvent::Update(Some(rewritten)))
                } else {
                    None
                }
            } else {
                None
            }
        } else {
            None
        };

        let body_changed = new_body != body_text;
        let sql_def_changed = new_sql_definition.as_deref() != existing.sql_definition.as_deref();
        let when_changed =
            new_when_condition.as_ref().map(|(_, changed)| *changed).unwrap_or(false);
        let event_changed = new_event.is_some();

        if !body_changed && !sql_def_changed && !when_changed && !event_changed {
            continue;
        }

        let mut updated = existing.clone();
        if body_changed {
            updated.triggered_action = TriggerAction::RawSql(new_body);
        }
        if sql_def_changed {
            updated.sql_definition = new_sql_definition;
        }
        if when_changed {
            if let Some((rewritten, _)) = new_when_condition {
                updated.when_condition = Some(rewritten);
            }
        }
        if let Some(event) = new_event {
            updated.event = event;
        }
        pending_updates.push(updated);
    }

    for updated in pending_updates {
        let _ = database.catalog.update_trigger(updated);
    }
    Ok(())
}

/// Propagate a *parent* table's column rename into every *child* table's foreign
/// key that references it, when `ALTER TABLE <parent> RENAME <old_col> TO
/// <new_col>` runs.
///
/// SQLite (`legacy_alter_table=OFF`) rewrites the referenced column name in each
/// child's `REFERENCES <parent>(<col_list>)` clause — both the stored
/// `sqlite_master.sql` text and the in-memory FK metadata — so FK enforcement and
/// reload stay consistent (verified against sqlite3 3.51.0, altercol.test
/// 4.1/4.4). Without the in-memory `parent_column_names` update, FK checks would
/// keep matching on the old parent column; without the `sql_source` rewrite, a
/// reload would rehydrate the stale reference (and the fail-closed open policy
/// would reject the child table's checkpoint).
///
/// The renamed parent's *own* table (its PK/UNIQUE/FK-local column references) is
/// handled separately by `update_sql_source_after_alter` +
/// `alter_rewrite::rename_column`; this loop only touches *other* tables' FK
/// references to it (a self-referential child FK is covered because the parent is
/// just another table in the same loop).
pub(super) fn rewrite_child_foreign_keys_for_column_rename(
    database: &mut Database,
    parent_table: &str,
    old_column: &str,
    new_column: &str,
) {
    for tbl in database.list_tables() {
        let updated_schema = {
            let Some(table) = database.get_table_mut(&tbl) else {
                continue;
            };
            let mut changed = false;
            for fk in table.schema_mut().foreign_keys.iter_mut() {
                if !fk.parent_table.eq_ignore_ascii_case(parent_table) {
                    continue;
                }
                for pcol in fk.parent_column_names.iter_mut() {
                    if pcol.eq_ignore_ascii_case(old_column) {
                        *pcol = new_column.to_string();
                        changed = true;
                    }
                }
            }
            if !changed {
                continue;
            }
            // Rewrite the verbatim `REFERENCES <parent>(<col_list>)` text. If the
            // in-place edit cannot apply cleanly, invalidate so `sql_source` is
            // reconstructed from the (already-updated) FK metadata — never left
            // naming the old column.
            let rewritten = table.schema.sql_source.as_deref().and_then(|sql| {
                crate::alter_rewrite::rename_references_column(
                    sql,
                    parent_table,
                    old_column,
                    new_column,
                )
            });
            match rewritten {
                Some(text) => table.schema_mut().set_sql_source(text),
                None => table.schema_mut().invalidate_sql_source(),
            }
            table.schema.clone()
        };
        database.catalog.replace_table_schema(&tbl, updated_schema);
    }
}

/// Rewrite dependent VIEW definitions that reference a renamed column, when
/// `ALTER TABLE <table> RENAME <old_column> TO <new_column>` runs.
///
/// SQLite (`legacy_alter_table=OFF`) rewrites references inside view bodies that
/// resolve to `<table>.<old_column>` — both qualified (`t.col`) and unqualified —
/// updating the `sqlite_master.sql` text and re-resolving the view (verified
/// against sqlite3 3.51.0, altercol.test group 8). VibeSQL materializes views by
/// executing their stored `query` AST, so both the verbatim `sql_definition` and
/// the parsed `query` are rewritten here (the latter by re-parsing the rewritten
/// text) so the view keeps working after the rename.
///
/// Reuses the trigger-body column resolver
/// ([`rewrite_column_refs_in_trigger_sql`]), which is table-name-aware and
/// aborts on a genuinely ambiguous unqualified reference during the rewrite
/// itself. That check alone is not sufficient: SQLite re-resolves the *entire*
/// rewritten view against the (already-renamed) schema, and a reference that
/// was perfectly unambiguous before the rename can become ambiguous once the
/// renamed column duplicates a name already in scope elsewhere in the view's
/// FROM clause (verified against sqlite3 3.51.0, altercol.test 16.1.1). Each
/// rewritten view is therefore re-validated via
/// [`drop_column_checks::find_ambiguous_column_in_query`] before it is
/// committed. Two-phase (compute + validate all rewrites, then commit) so an
/// ambiguity in any view aborts the whole ALTER before any view is mutated,
/// matching SQLite's atomic behavior.
pub(super) fn rewrite_views_for_column_rename(
    database: &mut Database,
    table: &str,
    old_column: &str,
    new_column: &str,
) -> Result<(), ExecutorError> {
    // Snapshot table -> column-name set for the resolver (same pattern as the
    // trigger rewrite). Reflects the post-rename schema, so the renamed table's
    // pre-rename column is handled explicitly in the closure.
    let schema: std::collections::HashMap<String, Vec<String>> = database
        .list_tables()
        .into_iter()
        .filter_map(|name| {
            database.get_table(&name).map(|tbl| {
                (
                    name.to_ascii_lowercase(),
                    tbl.schema.columns.iter().map(|c| c.name.clone()).collect(),
                )
            })
        })
        .collect();

    let renamed_table_lc = table.to_ascii_lowercase();
    let old_column_owned = old_column.to_string();
    let table_has_column = move |t: &str, c: &str| -> bool {
        let t_lc = t.to_ascii_lowercase();
        if t_lc == renamed_table_lc && c.eq_ignore_ascii_case(&old_column_owned) {
            return true;
        }
        schema.get(&t_lc).is_some_and(|cols| cols.iter().any(|col| col.eq_ignore_ascii_case(c)))
    };

    // Phase 1: compute the rewritten verbatim text for each affected view, then
    // re-parse and re-validate it before anything is committed. A genuine
    // ambiguity — either at rewrite time (SQLite:
    // "error in view <name>: ambiguous column name: <col>") or in the
    // post-rename re-resolution of the whole query (SQLite:
    // "error in view <name> after rename: ambiguous column name: <col>") —
    // aborts the whole ALTER before any view is mutated.
    let view_names = database.catalog.list_views();
    let mut pending: Vec<(String, String, vibesql_ast::SelectStmt)> = Vec::new();
    for name in view_names {
        let Some(view) = database.catalog.get_view(&name) else {
            continue;
        };
        // Prefer the verbatim `CREATE VIEW` text; fall back to reconstructing it
        // from the parsed query when a view was stored without captured source,
        // so a view still tracks the rename either way.
        let old_text = view.sql_definition.clone().unwrap_or_else(|| {
            use vibesql_ast::pretty_print::ToSql;
            let cols =
                view.columns.as_ref().map(|c| format!("({})", c.join(", "))).unwrap_or_default();
            format!("CREATE VIEW {}{} AS {}", view.name, cols, view.query.to_sql())
        });
        let new_text = rewrite_column_refs_in_trigger_sql(
            &old_text,
            table,
            old_column,
            new_column,
            &table_has_column,
            // A view has no NEW/OLD pseudo-tables.
            false,
        )
        .map_err(|col| {
            ExecutorError::Other(format!("error in view {}: ambiguous column name: {}", name, col))
        })?;
        if new_text == old_text {
            continue;
        }
        let stmt = vibesql_parser::parse_with_arena_fallback(&new_text)
            .map_err(|e| ExecutorError::Other(format!("error in view {}: {}", name, e)))?;
        let vibesql_ast::Statement::CreateView(cv) = stmt else {
            continue;
        };
        if let Some(col) = super::drop_column_checks::find_ambiguous_column_in_query(
            &cv.query, database, table, old_column, new_column,
        ) {
            return Err(ExecutorError::Other(format!(
                "error in view {} after rename: ambiguous column name: {}",
                name, col
            )));
        }
        pending.push((name, new_text, *cv.query));
    }

    // Phase 2: commit each update — store both the freshly re-parsed `query`
    // AST (so view execution resolves the new column) and the verbatim
    // `sql_definition`.
    for (name, new_text, query) in pending {
        if let Some(view) = database.catalog.get_view_mut(&name) {
            view.query = query;
            view.sql_definition = Some(new_text);
        }
    }
    Ok(())
}
