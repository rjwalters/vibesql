// ============================================================================
// Catalog Serialization
// ============================================================================
//
// Handles serialization of schemas, tables, indexes, and roles.

use std::io::{Read, Write};

use super::io::*;
use crate::{persistence::save, Database, StorageError};

pub fn write_catalog<W: Write>(writer: &mut W, db: &Database) -> Result<(), StorageError> {
    // Write schemas
    // Skip built-in schemas (main and all temp schemas) - they are recreated on load
    let schemas: Vec<String> = db
        .catalog
        .list_schemas()
        .into_iter()
        .filter(|s| {
            s != vibesql_catalog::DEFAULT_SCHEMA
                && !vibesql_catalog::Catalog::is_temp_schema(s)
                // Attached schemas are session-scoped (#6310) and never persisted.
                && !db.catalog.is_attached_schema(s)
        })
        .collect();

    write_u32(writer, schemas.len() as u32)?;
    for schema_name in schemas {
        write_string(writer, &schema_name)?;
    }

    // Write roles
    let roles = db.catalog.list_roles();
    write_u32(writer, roles.len() as u32)?;
    for role_name in roles {
        write_string(writer, &role_name)?;
    }

    // Write sequences (for AUTO_INCREMENT support)
    let sequence_names = db.catalog.list_sequences();
    write_u32(writer, sequence_names.len() as u32)?;
    for name in sequence_names {
        let seq = db.catalog.get_sequence(name).map_err(|e| {
            StorageError::NotImplemented(format!("Failed to get sequence {}: {}", name, e))
        })?;
        write_string(writer, name)?;
        write_i64(writer, seq.start_with)?;
        write_i64(writer, seq.increment_by)?;
        // Write min_value (optional)
        write_bool(writer, seq.min_value.is_some())?;
        if let Some(min) = seq.min_value {
            write_i64(writer, min)?;
        }
        // Write max_value (optional)
        write_bool(writer, seq.max_value.is_some())?;
        if let Some(max) = seq.max_value {
            write_i64(writer, max)?;
        }
        write_bool(writer, seq.cycle)?;
        write_i64(writer, seq.current_value)?;
        write_bool(writer, seq.exhausted)?;
    }

    // Write table schemas
    let table_names = db.catalog.list_tables();
    write_u32(writer, table_names.len() as u32)?;

    // `list_tables()` returns only the current (main) schema's tables, but the
    // bare-name `db.get_table` applies SQLite temp-shadowing — a same-named TEMP
    // table wins. Persisting through the bare lookup therefore serialized an
    // ephemeral TEMP table's schema under the main table's name, clobbering the
    // real main-schema table in the checkpoint (pragma-6.6.4; a TEMP table must
    // never be persisted). Qualify the lookup with the current schema so the
    // main-schema table is always the one written.
    let current_schema = db.catalog.get_current_schema().to_string();
    for table_name in &table_names {
        let qualified_name = format!("{}.{}", current_schema, table_name);
        if let Some(table) = db.get_table(&qualified_name) {
            // Persist the table's exact-case display name (`table.schema.name`),
            // NOT `table_name` — the latter is `Catalog::list_tables()`'s
            // *canonical* lookup key, which is lowercased for an unquoted
            // (case-insensitive) identifier. Writing the canonical key here
            // silently lowercased any mixed/upper-case unquoted table name on
            // reload (issue #6599) — most visibly after `ALTER TABLE ...
            // RENAME TO Foo`, whose new name is set directly on
            // `table.schema.name` and displays correctly in the *same*
            // session (`sqlite_master` reads `table.name`, not the catalog
            // key — see `sqlite_schema.rs`), but was lost the moment the
            // catalog got serialized to a snapshot/checkpoint.
            write_string(writer, &table.schema.name)?;

            // Write column count
            write_u32(writer, table.schema.columns.len() as u32)?;

            // Write each column definition
            for col in &table.schema.columns {
                write_string(writer, &col.name)?;
                write_string(writer, &save::format_data_type(&col.data_type))?;
                write_bool(writer, col.nullable)?;
                // Write default_value expression (for AUTO_INCREMENT support)
                write_bool(writer, col.default_value.is_some())?;
                if let Some(default_expr) = &col.default_value {
                    super::expression::write_expression(writer, default_expr)?;
                }
                // Write collation (v5+)
                write_bool(writer, col.collation.is_some())?;
                if let Some(coll) = &col.collation {
                    write_string(writer, coll)?;
                }
                // Write generated-column expression (v11+, issue #5794).
                // Mirrors the default_value encoding: present-flag bool +
                // expression. Without this, a reloaded schema forgets the
                // `c AS (a+b)` expression and post-reload INSERTs store NULL
                // for the generated column.
                write_bool(writer, col.generated_expr.is_some())?;
                if let Some(gen_expr) = &col.generated_expr {
                    super::expression::write_expression(writer, gen_expr)?;
                }
            }

            // Write primary key columns (v3+)
            write_bool(writer, table.schema.primary_key.is_some())?;
            if let Some(pk_cols) = &table.schema.primary_key {
                write_u32(writer, pk_cols.len() as u32)?;
                for col in pk_cols {
                    write_string(writer, col)?;
                }
            }

            // Write quoted flag for TableIdentifier (v4+)
            // This enables SQL:1999 case-sensitivity to be preserved across save/load
            let quoted = db
                .catalog
                .get_table_identifier(table_name)
                .map(|id| id.is_quoted())
                .unwrap_or(false);
            write_bool(writer, quoted)?;

            // Write the verbatim original CREATE TABLE text (v9+, issue #5619).
            // SQLite stores the byte-for-byte source in sqlite_master.sql; the
            // binary catalog must persist it so a cross-process reload (e.g. the
            // TCL shim's per-batch CLI processes against a shared .vbsql file)
            // returns the user's exact formatting rather than a reconstruction.
            // Written last in the per-table record so v8-and-earlier readers,
            // which stop after `quoted`, are unaffected.
            match &table.schema.sql_source {
                Some(src) => {
                    write_bool(writer, true)?;
                    write_string(writer, src)?;
                }
                None => {
                    write_bool(writer, false)?;
                }
            }

            // Write the WITHOUT ROWID flag (v12+, issue #5796). Reloaded
            // schemas need it so sqlite_master keeps hiding the implicit
            // PRIMARY KEY autoindex of a WITHOUT ROWID table across processes.
            write_bool(writer, table.schema.without_rowid)?;
        }
    }

    // Write indexes
    //
    // Indexes on tables in ATTACHed database schemas are session-scoped
    // (#6310) and are filtered out. Filter on `metadata.schema` — the owning
    // schema resolved at CREATE INDEX time — NOT on a qualifier embedded in
    // `metadata.table_name`: an unqualified `CREATE INDEX i1 ON t(z)` that
    // resolves to an attached table stores the bare `"t"` as table_name, so a
    // name-prefix check would leak the index into the checkpoint and brick the
    // main database on reload (the index's table doesn't exist there). The
    // count and the write loop iterate the same filtered list so they stay in
    // lockstep.
    let index_names: Vec<String> = db
        .list_indexes()
        .into_iter()
        .filter(|name| {
            db.get_index(name)
                .is_none_or(|metadata| !db.catalog.is_attached_schema(&metadata.schema))
        })
        .collect();
    write_u32(writer, index_names.len() as u32)?;

    for index_name in index_names {
        if let Some(metadata) = db.get_index(&index_name) {
            // Emit the index name with its original case, mirroring the
            // SQL-dump persistence path's `quote_identifier(&metadata.index_name)`
            // (issue #5579). `index_name` here is `IndexManager`'s *storage
            // key*, which `make_index_key` normalizes to lowercase for a
            // main-schema index (issue #5540) — writing it directly silently
            // lowercased every index name (including a table's implicit
            // `sqlite_autoindex_*` names retargeted by `ALTER TABLE ... RENAME
            // TO ...`, issue #6607) across a binary save/reload cycle.
            // `metadata.index_name` is the exact-case name `create_index`
            // stored in the VALUE (not the key), so it survives the round trip.
            write_string(writer, &metadata.index_name)?;
            write_string(writer, &metadata.table_name)?;
            write_bool(writer, metadata.unique)?;

            // Write indexed columns
            // Format (v6+): type_byte, content_string, direction_byte
            // type_byte: 0 = column reference, 1 = expression
            write_u32(writer, metadata.columns.len() as u32)?;
            for col in &metadata.columns {
                use vibesql_ast::IndexColumn;
                match col {
                    IndexColumn::Column { column_name, .. } => {
                        // Type 0 = column reference
                        writer.write_all(&[0u8]).map_err(|e| {
                            StorageError::NotImplemented(format!("Write error: {}", e))
                        })?;
                        write_string(writer, column_name)?;
                    }
                    IndexColumn::Expression { expr, .. } => {
                        // Type 1 = expression (stored as SQL text)
                        writer.write_all(&[1u8]).map_err(|e| {
                            StorageError::NotImplemented(format!("Write error: {}", e))
                        })?;
                        use vibesql_ast::pretty_print::ToSql;
                        write_string(writer, &expr.to_sql())?;
                    }
                }
                // Write direction as u8 (0 = Asc, 1 = Desc)
                let direction = match col.direction() {
                    vibesql_ast::OrderDirection::Asc => 0u8,
                    vibesql_ast::OrderDirection::Desc => 1u8,
                };
                writer
                    .write_all(&[direction])
                    .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;

                // v15+: per-key-part explicit collation, column-type parts only
                // (issue #5921). Expression parts encode any collation inside
                // their SQL text, so nothing extra is written for them; the read
                // path mirrors this (only reads for type byte 0).
                if let IndexColumn::Column { collation, .. } = col {
                    match collation {
                        Some(name) => {
                            write_bool(writer, true)?;
                            write_string(writer, name)?;
                        }
                        None => write_bool(writer, false)?,
                    }
                }

                // v18+: per-key-part quoting bit, column-type parts only
                // (issue #6560). Whether `column_name` was written as a
                // delimited identifier in the original CREATE INDEX text —
                // drives the "should this be a string literal in
                // single-quotes?" hint on a later DROP COLUMN dependent-index
                // error. Expression parts have no separate quoting bit; any
                // quoting inside them round-trips through their SQL text.
                if let IndexColumn::Column { is_quoted, .. } = col {
                    write_bool(writer, *is_quoted)?;
                }
            }

            // v8+: persist partial-index WHERE clause (if any). Catalog-side
            // metadata carries the predicate; the storage-side struct does
            // not, so look up by index name. Serialised as SQL text and
            // re-parsed on load.
            use vibesql_ast::pretty_print::ToSql;
            let where_sql = db
                .catalog
                .find_index_by_name(&index_name)
                .and_then(|m| m.where_clause.as_deref())
                .map(|expr| expr.to_sql());
            match where_sql {
                Some(sql) => {
                    write_bool(writer, true)?;
                    write_string(writer, &sql)?;
                }
                None => {
                    write_bool(writer, false)?;
                }
            }
        }
    }

    // Write views (v10+, issue #5771).
    //
    // Written BEFORE triggers so that, on load, a view-dependent INSTEAD OF
    // trigger resolves against an already-present view during recovery. The
    // defining SELECT is serialized as SQL text (via `ToSql`) and re-parsed on
    // load, mirroring how expression indexes store/re-parse their SQL and how
    // triggers store `RawSql`. The verbatim `sql_definition` (the original
    // `CREATE VIEW` text) is persisted too so `sqlite_master.sql` renders the
    // user's exact formatting; when absent, load falls back to the
    // `ToSql`-reconstructed SELECT.
    //
    // Temp views (`view.is_temp()`, e.g. `CREATE TEMP VIEW`) are session-scoped
    // and MUST NOT survive a checkpoint. They are filtered out before the count
    // is written so a session-local temp view never reappears in the next
    // session's catalog (issue #5940, Cluster A). The count and the write loop
    // iterate the same filtered list so they stay in lockstep.
    // Iterate view definitions directly rather than via `list_views()` +
    // `get_view()`: views are keyed per schema (#6490), so a name-only
    // `get_view` always resolves to the same (temp-then-main-then-attached
    // priority) entry — iterating by name would silently write the *same*
    // main-schema view twice whenever an attached schema holds a same-named
    // view, corrupting the snapshot with a duplicate `CREATE VIEW` that fails
    // to reload.
    let views_to_persist: Vec<&vibesql_catalog::ViewDefinition> = db
        .catalog
        .iter_views()
        .filter(|v| {
            // Views in ATTACHed database schemas are session-scoped (#6310),
            // like temp views. The schema may be carried as a tag or (for a
            // legacy pre-#6490 snapshot) embedded in the stored name.
            let name_in_attached_schema = v
                .name
                .split_once('.')
                .is_some_and(|(schema, _)| db.catalog.is_attached_schema(schema));
            !name_in_attached_schema
                && !v.is_temp()
                && !v.schema.as_deref().is_some_and(|s| db.catalog.is_attached_schema(s))
        })
        .collect();
    write_u32(writer, views_to_persist.len() as u32)?;

    for view in views_to_persist {
        // 1. name
        write_string(writer, &view.name)?;

        // 2. schema (present-flag + string) — preserves temp tagging
        match &view.schema {
            Some(schema) => {
                write_bool(writer, true)?;
                write_string(writer, schema)?;
            }
            None => {
                write_bool(writer, false)?;
            }
        }

        // 3. columns (present-flag + count + strings)
        match &view.columns {
            Some(cols) => {
                write_bool(writer, true)?;
                write_u32(writer, cols.len() as u32)?;
                for col in cols {
                    write_string(writer, col)?;
                }
            }
            None => {
                write_bool(writer, false)?;
            }
        }

        // 4. with_check_option
        write_bool(writer, view.with_check_option)?;

        // 5. defining SELECT as SQL text
        use vibesql_ast::pretty_print::ToSql;
        write_string(writer, &view.query.to_sql())?;

        // 6. sql_definition (present-flag + string)
        match &view.sql_definition {
            Some(def) => {
                write_bool(writer, true)?;
                write_string(writer, def)?;
            }
            None => {
                write_bool(writer, false)?;
            }
        }
    }

    // Write triggers
    //
    // Temp triggers (`trigger.is_temp()`, e.g. `CREATE TEMP TRIGGER`) are
    // session-scoped and MUST NOT survive a checkpoint. They are filtered out
    // before the count is written so a session-local temp trigger never
    // reappears in the next session's catalog (issue #5940, Cluster A). The
    // count and the write loop iterate the same filtered list so they stay in
    // lockstep.
    // Iterate trigger definitions directly. Triggers are keyed per schema, so a
    // name-only `get_trigger` could return the temp namesake of a main trigger;
    // collecting definitions keeps every non-temp trigger and keeps the count in
    // lockstep with the write loop below.
    // Triggers in ATTACHed database schemas are session-scoped (#6310), like
    // temp triggers.
    let triggers: Vec<&vibesql_catalog::TriggerDefinition> = db
        .catalog
        .iter_triggers()
        .filter(|t| {
            !t.is_temp() && !t.schema.as_deref().is_some_and(|s| db.catalog.is_attached_schema(s))
        })
        .collect();
    write_u32(writer, triggers.len() as u32)?;

    for trigger in triggers {
        {
            write_string(writer, &trigger.name)?;
            write_string(writer, &trigger.table_name)?;

            // Write timing as u8 (0 = Before, 1 = After, 2 = InsteadOf)
            let timing = match trigger.timing {
                vibesql_ast::TriggerTiming::Before => 0u8,
                vibesql_ast::TriggerTiming::After => 1u8,
                vibesql_ast::TriggerTiming::InsteadOf => 2u8,
            };
            writer
                .write_all(&[timing])
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;

            // Write event as u8 (0 = Insert, 1 = Update, 2 = Delete)
            // For Update with columns, write 3 followed by column list
            match &trigger.event {
                vibesql_ast::TriggerEvent::Insert => {
                    writer
                        .write_all(&[0u8])
                        .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
                }
                vibesql_ast::TriggerEvent::Update(None) => {
                    writer
                        .write_all(&[1u8])
                        .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
                }
                vibesql_ast::TriggerEvent::Update(Some(cols)) => {
                    writer
                        .write_all(&[3u8])
                        .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
                    write_u32(writer, cols.len() as u32)?;
                    for col in cols {
                        write_string(writer, col)?;
                    }
                }
                vibesql_ast::TriggerEvent::Delete => {
                    writer
                        .write_all(&[2u8])
                        .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
                }
            }

            // Write granularity as u8 (0 = Row, 1 = Statement)
            let granularity = match trigger.granularity {
                vibesql_ast::TriggerGranularity::Row => 0u8,
                vibesql_ast::TriggerGranularity::Statement => 1u8,
            };
            writer
                .write_all(&[granularity])
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;

            // Write when_condition (optional)
            match &trigger.when_condition {
                Some(expr) => {
                    write_bool(writer, true)?;
                    super::expression::write_expression(writer, expr)?;
                }
                None => {
                    write_bool(writer, false)?;
                }
            }

            // Write triggered_action
            match &trigger.triggered_action {
                vibesql_ast::TriggerAction::RawSql(sql) => {
                    writer
                        .write_all(&[0u8])
                        .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
                    write_string(writer, sql)?;
                }
            }

            // Write the trigger's schema (v14+, issue #5940). `TriggerDefinition`
            // carries a `schema` field (`None` == main; `Some("temp")` == temp)
            // that SQLite name resolution depends on — a trigger on a temp-table
            // namesake must bind to the temp table. Earlier binary versions never
            // wrote it, so every reloaded trigger silently became a main-schema
            // trigger. Temp triggers are already filtered out above, so this in
            // practice persists an explicit non-temp schema (e.g. `Some("main")`);
            // it is encoded as a present-flag bool + string. Written last in the
            // per-trigger record so v13-and-earlier readers, which stop after the
            // triggered_action, are unaffected; the read path gates on
            // `version >= 14`.
            match &trigger.schema {
                Some(schema) => {
                    write_bool(writer, true)?;
                    write_string(writer, schema)?;
                }
                None => {
                    write_bool(writer, false)?;
                }
            }

            // Write the trigger's verbatim `CREATE TRIGGER` text (v16+, issue
            // #6174). Written last in the per-trigger record so v15-and-earlier
            // readers, which stop after the schema field, are unaffected; the
            // read path gates on `version >= 16`. Encoded as a present-flag bool
            // + optional string, mirroring the view `sql_definition` field.
            // Without it, a reloaded trigger loses its verbatim text and
            // `sqlite_master.sql` renders an AST-reconstructed form.
            match &trigger.sql_definition {
                Some(def) => {
                    write_bool(writer, true)?;
                    write_string(writer, def)?;
                }
                None => {
                    write_bool(writer, false)?;
                }
            }
        }
    }

    // Write the schema-object creation-order section (v17+, issue #6175).
    // `sqlite_master` must list objects in creation order, but the reader
    // re-registers tables and indexes in separate passes, so without this the
    // ordering degrades to "tables first, then indexes" after every reload.
    // Persist each recorded ordinal as an opaque `(key, seq)` pair. Written last
    // so v16-and-earlier readers, which stop after the triggers section, are
    // unaffected; the read path gates on `version >= 17`.
    let creation_seq_entries: Vec<(&str, u64)> = db.catalog.creation_seq_entries().collect();
    write_u32(writer, creation_seq_entries.len() as u32)?;
    for (key, seq) in creation_seq_entries {
        write_string(writer, key)?;
        write_u64(writer, seq)?;
    }

    Ok(())
}

/// Read catalog from binary format with version awareness
/// Use version 0 to auto-detect (for backward compatibility with existing callers)
pub fn read_catalog_v<R: Read>(reader: &mut R, version: u8) -> Result<Database, StorageError> {
    let mut db = Database::new();

    // Read schemas
    let schema_count = read_u32(reader)?;
    for _ in 0..schema_count {
        let schema_name = read_string(reader)?;
        // Skip built-in schemas (main and all temp schemas) - they are already created by
        // Database::new() This handles backward compatibility with databases saved before
        // the write-side fix
        if schema_name == vibesql_catalog::DEFAULT_SCHEMA
            || vibesql_catalog::Catalog::is_temp_schema(&schema_name)
        {
            continue;
        }
        // Create schema directly on catalog
        db.catalog
            .create_schema(schema_name)
            .map_err(|e| StorageError::NotImplemented(format!("Failed to create schema: {}", e)))?;
    }

    // Read roles
    let role_count = read_u32(reader)?;
    for _ in 0..role_count {
        let role_name = read_string(reader)?;
        db.catalog
            .create_role(role_name)
            .map_err(|e| StorageError::NotImplemented(format!("Failed to create role: {}", e)))?;
    }

    // Read sequences (for AUTO_INCREMENT support) - v2+
    if version >= 2 {
        let sequence_count = read_u32(reader)?;
        for _ in 0..sequence_count {
            let name = read_string(reader)?;
            let start_with = read_i64(reader)?;
            let increment_by = read_i64(reader)?;
            // Read min_value (optional)
            let has_min = read_bool(reader)?;
            let min_value = if has_min { Some(read_i64(reader)?) } else { None };
            // Read max_value (optional)
            let has_max = read_bool(reader)?;
            let max_value = if has_max { Some(read_i64(reader)?) } else { None };
            let cycle = read_bool(reader)?;
            let current_value = read_i64(reader)?;
            let exhausted = read_bool(reader)?;

            // Create the sequence with all its state
            let mut seq = vibesql_catalog::Sequence::new(
                name.clone(),
                Some(start_with),
                increment_by,
                min_value,
                max_value,
                cycle,
            );
            // Restore the current_value and exhausted state
            seq.current_value = current_value;
            seq.exhausted = exhausted;

            // Insert sequence using public API
            db.catalog.insert_sequence(name, seq);
        }
    }

    // Read table schemas (will create tables later when we read data)
    let table_count = read_u32(reader)?;
    let mut table_schemas = Vec::new();

    for _ in 0..table_count {
        let table_name = read_string(reader)?;
        let column_count = read_u32(reader)?;

        let mut columns = Vec::new();
        for _ in 0..column_count {
            let col_name = read_string(reader)?;
            let col_type_str = read_string(reader)?;
            let nullable = read_bool(reader)?;

            // Parse data type from string (reuse existing logic)
            let data_type = parse_data_type(&col_type_str)?;

            // Read default_value expression (for AUTO_INCREMENT support) - v2+
            let default_value = if version >= 2 {
                let has_default = read_bool(reader)?;
                if has_default {
                    Some(super::expression::read_expression(reader)?)
                } else {
                    None
                }
            } else {
                None
            };

            // Read collation (v5+)
            let collation = if version >= 5 {
                let has_collation = read_bool(reader)?;
                if has_collation {
                    Some(read_string(reader)?)
                } else {
                    None
                }
            } else {
                None
            };

            // Read generated-column expression (v11+, issue #5794).
            // v10-and-earlier files do not include this field; absence means
            // "not a generated column" (None), which matches prior behavior.
            let generated_expr = if version >= 11 {
                let has_generated = read_bool(reader)?;
                if has_generated {
                    Some(super::expression::read_expression(reader)?)
                } else {
                    None
                }
            } else {
                None
            };

            columns.push(vibesql_catalog::ColumnSchema {
                name: col_name,
                data_type,
                nullable,
                default_value,
                generated_expr,
                collation,
                // Default to false for backward compatibility with existing databases
                // New tables will have this set correctly at creation time
                is_exact_integer_type: false,
            });
        }

        // Read primary key columns (v3+)
        let primary_key = if version >= 3 {
            let has_pk = read_bool(reader)?;
            if has_pk {
                let pk_count = read_u32(reader)?;
                let mut pk_cols = Vec::new();
                for _ in 0..pk_count {
                    pk_cols.push(read_string(reader)?);
                }
                Some(pk_cols)
            } else {
                None
            }
        } else {
            None
        };

        // Read quoted flag for TableIdentifier (v4+)
        // For older versions, default to unquoted (case-insensitive)
        let quoted = if version >= 4 { read_bool(reader)? } else { false };

        // Read the verbatim original CREATE TABLE text (v9+, issue #5619).
        // v8-and-earlier files do not include this field, so absence means
        // "no captured source" (None) and the schema falls back to a
        // reconstructed CREATE TABLE for sqlite_master.sql.
        let sql_source = if version >= 9 {
            let has_source = read_bool(reader)?;
            if has_source {
                Some(read_string(reader)?)
            } else {
                None
            }
        } else {
            None
        };

        // Read the WITHOUT ROWID flag (v12+, issue #5796). v11-and-earlier
        // files do not include it; default to false (prior behavior).
        let without_rowid = if version >= 12 { read_bool(reader)? } else { false };

        table_schemas.push((table_name, columns, primary_key, quoted, sql_source, without_rowid));
    }

    // Build a column-name lookup covering EVERY table in the file, so FK
    // parent column indices can be resolved during rehydration regardless of
    // the order tables appear in (a child may be stored before its parent).
    let parent_lookup: super::constraints::ParentColumnLookup = table_schemas
        .iter()
        .map(|(name, columns, ..)| {
            (name.to_lowercase(), columns.iter().map(|c| c.name.clone()).collect())
        })
        .collect();

    // Create tables
    for (table_name, columns, primary_key, quoted, sql_source, without_rowid) in table_schemas {
        let mut schema = if let Some(pk_cols) = primary_key {
            vibesql_catalog::TableSchema::with_primary_key(table_name.clone(), columns, pk_cols)
        } else {
            vibesql_catalog::TableSchema::new(table_name.clone(), columns)
        };

        // Restore the verbatim CREATE TABLE source dropped by the TableSchema
        // constructors above (issue #5619). set_sql_source strips the trailing
        // semicolon to match SQLite.
        if let Some(src) = sql_source {
            schema.set_sql_source(src);
        }

        // Restore the WITHOUT ROWID flag (v12+, issue #5796).
        schema.without_rowid = without_rowid;

        // Rebuild CHECK and FOREIGN KEY constraint state from the persisted
        // CREATE TABLE source (issue #5834). The binary format has no
        // dedicated fields for these; without this re-parse every reloaded
        // schema silently stopped enforcing them. Must run BEFORE
        // create_table_with_identifier so both schema copies (catalog +
        // storage Table) carry the constraints, matching CREATE TABLE.
        super::constraints::rehydrate_constraints_from_sql_source(&mut schema, &parent_lookup)?;

        // Use TableIdentifier to preserve case-sensitivity semantics
        let identifier = vibesql_catalog::TableIdentifier::from_canonical(table_name, quoted);
        db.create_table_with_identifier(schema, identifier)
            .map_err(|e| StorageError::NotImplemented(format!("Failed to create table: {}", e)))?;
    }

    // Read indexes
    let index_count = read_u32(reader)?;
    let mut index_specs = Vec::new();

    for _ in 0..index_count {
        let index_name = read_string(reader)?;
        let table_name = read_string(reader)?;
        let unique = read_bool(reader)?;

        let column_count = read_u32(reader)?;
        let mut columns = Vec::new();

        for _ in 0..column_count {
            // Version 6+ stores index column type (0 = column, 1 = expression)
            // Version 1-5 stored only column names (no type byte)
            let index_column = if version >= 6 {
                let type_byte = read_u8(reader)?;
                let content = read_string(reader)?;
                let direction_byte = read_u8(reader)?;
                let direction = match direction_byte {
                    0 => vibesql_ast::OrderDirection::Asc,
                    1 => vibesql_ast::OrderDirection::Desc,
                    _ => {
                        return Err(StorageError::NotImplemented(format!(
                            "Invalid sort direction: {}",
                            direction_byte
                        )))
                    }
                };

                match type_byte {
                    0 => {
                        // Column reference. v15+ appends a present-flag bool +
                        // optional collation name after the direction byte
                        // (issue #5921); v14 and earlier omit it entirely.
                        let collation = if version >= 15 {
                            if read_bool(reader)? {
                                Some(read_string(reader)?)
                            } else {
                                None
                            }
                        } else {
                            None
                        };
                        // v18+ appends a quoting bit after the collation field
                        // (issue #6560); v17 and earlier omit it, so absence
                        // defaults to "not quoted" (prior behavior).
                        let is_quoted = if version >= 18 { read_bool(reader)? } else { false };
                        vibesql_ast::IndexColumn::Column {
                            column_name: content,
                            direction,
                            prefix_length: None,
                            collation,
                            is_quoted,
                        }
                    }
                    1 => {
                        // Expression index - parse the SQL expression. Use the
                        // full main-parser grammar: the arena parser rejects
                        // forms the main parser accepted at CREATE INDEX time
                        // (e.g. COLLATE), which would drop the index on reload
                        // (issue #5833).
                        let expr = vibesql_parser::Parser::parse_expression_sql(&content).map_err(
                            |e| {
                                StorageError::NotImplemented(format!(
                                    "Failed to parse expression index '{}': {}",
                                    content, e
                                ))
                            },
                        )?;
                        vibesql_ast::IndexColumn::Expression { expr: Box::new(expr), direction }
                    }
                    _ => {
                        return Err(StorageError::NotImplemented(format!(
                            "Invalid index column type: {}",
                            type_byte
                        )))
                    }
                }
            } else {
                // Legacy format (v1-5): just column name + direction
                let column_name = read_string(reader)?;
                let direction_byte = read_u8(reader)?;
                let direction = match direction_byte {
                    0 => vibesql_ast::OrderDirection::Asc,
                    1 => vibesql_ast::OrderDirection::Desc,
                    _ => {
                        return Err(StorageError::NotImplemented(format!(
                            "Invalid sort direction: {}",
                            direction_byte
                        )))
                    }
                };
                vibesql_ast::IndexColumn::Column {
                    column_name,
                    direction,
                    prefix_length: None,
                    collation: None,
                    is_quoted: false,
                }
            };

            columns.push(index_column);
        }

        // v8+: read optional partial-index WHERE clause. v1-v7 files do not
        // include this field, so we treat the absence as "full index".
        let where_clause: Option<vibesql_ast::Expression> = if version >= 8 {
            let has_where = read_bool(reader)?;
            if has_where {
                let sql = read_string(reader)?;
                // Full main-parser grammar for the same reason as expression
                // indexes above (issue #5833).
                let parsed = vibesql_parser::Parser::parse_expression_sql(&sql).map_err(|e| {
                    StorageError::NotImplemented(format!(
                        "Failed to parse partial-index WHERE expression '{}': {}",
                        sql, e
                    ))
                })?;
                Some(parsed)
            } else {
                None
            }
        } else {
            None
        };

        index_specs.push((index_name, table_name, unique, columns, where_clause));
    }

    // Create indexes
    for (index_name, table_name, unique, columns, where_clause) in index_specs {
        // Create the storage-side index first (it manages the index body but
        // does not touch the catalog at all).
        db.create_index(index_name.clone(), table_name.clone(), unique, columns.clone())
            .map_err(|e| StorageError::NotImplemented(format!("Failed to create index: {}", e)))?;

        // Now populate the catalog-side `IndexMetadata` for this index. The
        // executor's `CreateIndexExecutor::execute` normally does this at
        // CREATE INDEX time, but the binary-load path bypasses the executor,
        // so we mirror that work here. Without this, `Catalog::find_index_by_name`
        // returns `None` for every persisted index after a cold load — which
        // also silently swallows partial-index WHERE clauses, expression-index
        // metadata, and breaks any planner/FK check that consults the catalog.
        //
        // For partial indexes we additionally patch the storage-side
        // `where_clause` so subsequent insert/update/delete maintenance
        // routes through the partial-index code paths.
        //
        // SAFETY / KNOWN LIMITATION: the index body created above includes
        // every table row (since binary persistence dumped the unfiltered
        // index body). Query correctness today is preserved because the
        // planner's `is_partial()` skip prevents partial indexes from being
        // consulted for reads. A subsequent REINDEX through the executor
        // (which can evaluate the WHERE predicate) is needed to rebuild the
        // body with only matching rows. For the SQL-dump load path, the
        // dump replays the CREATE INDEX statement through the executor which
        // already applies the predicate at build time.
        let catalog_columns = convert_ast_columns_to_catalog(&columns);
        let catalog_meta = vibesql_catalog::IndexMetadata::new(
            index_name.clone(),
            table_name.clone(),
            vibesql_catalog::IndexType::BTree,
            catalog_columns,
            unique,
        )
        .with_where_clause(where_clause.clone());
        db.catalog.add_index(catalog_meta).map_err(|e| {
            StorageError::NotImplemented(format!(
                "Failed to add catalog index metadata for '{}': {}",
                index_name, e
            ))
        })?;
        if let Some(expr) = where_clause {
            db.set_index_where_clause(&index_name, Some(Box::new(expr)));
        }
    }

    // Read views (v10+, issue #5771). v9-and-earlier files do not include a
    // views section, so the read is gated on `version >= 10`; absence is
    // treated as "zero views." Views are read BEFORE triggers so that a
    // view-dependent INSTEAD OF trigger resolves against an already-present
    // view during recovery.
    if version >= 10 {
        let view_count = read_u32(reader)?;
        for _ in 0..view_count {
            // 1. name
            let name = read_string(reader)?;

            // 2. schema (present-flag + string)
            let has_schema = read_bool(reader)?;
            let schema = if has_schema { Some(read_string(reader)?) } else { None };

            // 3. columns (present-flag + count + strings)
            let has_columns = read_bool(reader)?;
            let columns = if has_columns {
                let col_count = read_u32(reader)?;
                let mut cols = Vec::with_capacity(col_count as usize);
                for _ in 0..col_count {
                    cols.push(read_string(reader)?);
                }
                Some(cols)
            } else {
                None
            };

            // 4. with_check_option
            let with_check_option = read_bool(reader)?;

            // 5. defining SELECT as SQL text — re-parse into a SelectStmt.
            //
            // Must use the full main-parser grammar (`Parser::parse_sql`), not
            // the arena parser's `parse_select_to_owned`: the arena parser
            // covers a smaller grammar and rejects forms the main parser
            // accepted at CREATE VIEW time — bare `VALUES(...)` bodies and
            // `COLLATE` in the select list among them. Under fail-closed
            // recovery a single such view made every subsequent open of the
            // checkpoint fail (issue #5833; join7/join9, tkt-a7debbe0).
            let query_sql = read_string(reader)?;
            let query = match vibesql_parser::Parser::parse_sql(&query_sql) {
                Ok(vibesql_ast::Statement::Select(stmt)) => *stmt,
                Ok(_) => {
                    return Err(StorageError::NotImplemented(format!(
                        "Persisted defining query for view '{}' is not a SELECT: '{}'",
                        name, query_sql
                    )))
                }
                Err(e) => {
                    return Err(StorageError::NotImplemented(format!(
                        "Failed to parse view '{}' SELECT '{}': {}",
                        name, query_sql, e
                    )))
                }
            };

            // 6. sql_definition (present-flag + string)
            let has_sql_def = read_bool(reader)?;
            let sql_definition = if has_sql_def { Some(read_string(reader)?) } else { None };

            // Reconstruct the view definition. Prefer the verbatim
            // sql_definition; fall back to the ToSql-reconstructed SELECT.
            let view_def = match sql_definition {
                Some(def) => vibesql_catalog::ViewDefinition::new_with_sql(
                    name,
                    columns,
                    query,
                    with_check_option,
                    def,
                ),
                None => {
                    vibesql_catalog::ViewDefinition::new(name, columns, query, with_check_option)
                }
            }
            .with_schema(schema);

            db.catalog.create_view(view_def).map_err(|e| {
                StorageError::NotImplemented(format!("Failed to create view: {}", e))
            })?;
        }
    }

    // Read triggers
    let trigger_count = read_u32(reader)?;

    for _ in 0..trigger_count {
        let name = read_string(reader)?;
        let table_name = read_string(reader)?;

        // Read timing
        let timing_byte = read_u8(reader)?;
        let timing = match timing_byte {
            0 => vibesql_ast::TriggerTiming::Before,
            1 => vibesql_ast::TriggerTiming::After,
            2 => vibesql_ast::TriggerTiming::InsteadOf,
            _ => {
                return Err(StorageError::NotImplemented(format!(
                    "Invalid trigger timing: {}",
                    timing_byte
                )))
            }
        };

        // Read event
        let event_byte = read_u8(reader)?;
        let event = match event_byte {
            0 => vibesql_ast::TriggerEvent::Insert,
            1 => vibesql_ast::TriggerEvent::Update(None),
            2 => vibesql_ast::TriggerEvent::Delete,
            3 => {
                // Update with column list
                let col_count = read_u32(reader)?;
                let mut cols = Vec::new();
                for _ in 0..col_count {
                    cols.push(read_string(reader)?);
                }
                vibesql_ast::TriggerEvent::Update(Some(cols))
            }
            _ => {
                return Err(StorageError::NotImplemented(format!(
                    "Invalid trigger event: {}",
                    event_byte
                )))
            }
        };

        // Read granularity
        let granularity_byte = read_u8(reader)?;
        let granularity = match granularity_byte {
            0 => vibesql_ast::TriggerGranularity::Row,
            1 => vibesql_ast::TriggerGranularity::Statement,
            _ => {
                return Err(StorageError::NotImplemented(format!(
                    "Invalid trigger granularity: {}",
                    granularity_byte
                )))
            }
        };

        // Read when_condition
        let has_when = read_bool(reader)?;
        let when_condition = if has_when {
            Some(Box::new(super::expression::read_expression(reader)?))
        } else {
            None
        };

        // Read triggered_action
        let action_type = read_u8(reader)?;
        let triggered_action = match action_type {
            0 => {
                let sql = read_string(reader)?;
                vibesql_ast::TriggerAction::RawSql(sql)
            }
            _ => {
                return Err(StorageError::NotImplemented(format!(
                    "Invalid trigger action type: {}",
                    action_type
                )))
            }
        };

        // Read the trigger's schema (v14+, issue #5940). v13-and-earlier files
        // do not include this field; absence is treated as `None` (main schema),
        // matching prior behavior where every reloaded trigger was a main-schema
        // trigger. Written as present-flag bool + string.
        let schema = if version >= 14 {
            let has_schema = read_bool(reader)?;
            if has_schema {
                Some(read_string(reader)?)
            } else {
                None
            }
        } else {
            None
        };

        // Read the trigger's verbatim `CREATE TRIGGER` text (v16+, issue #6174).
        // v15-and-earlier files do not include this field; absence is treated as
        // `None`, so the renderer falls back to AST reconstruction (prior
        // behavior). Written as present-flag bool + optional string.
        let sql_definition = if version >= 16 {
            let has_sql_def = read_bool(reader)?;
            if has_sql_def {
                Some(read_string(reader)?)
            } else {
                None
            }
        } else {
            None
        };

        // Create trigger definition
        let trigger = vibesql_catalog::TriggerDefinition::new(
            name,
            timing,
            event,
            table_name,
            granularity,
            when_condition,
            triggered_action,
        )
        .with_schema(schema)
        .with_sql_definition(sql_definition);

        // Add to catalog
        db.catalog.create_trigger(trigger).map_err(|e| {
            StorageError::NotImplemented(format!("Failed to create trigger: {}", e))
        })?;
    }

    // Read the schema-object creation-order section (v17+, issue #6175). Each
    // object was just re-registered above and given a load-order ordinal; restore
    // the persisted ordinals so `sqlite_master` reproduces SQLite's creation
    // order. Older files have no section — the read is gated on `version >= 17`
    // and their objects keep the prior "tables first, then indexes" fallback.
    if version >= 17 {
        let creation_seq_count = read_u32(reader)?;
        for _ in 0..creation_seq_count {
            let key = read_string(reader)?;
            let seq = read_u64(reader)?;
            db.catalog.restore_creation_seq(key, seq);
        }
    }

    Ok(db)
}

/// Legacy read_catalog function for backward compatibility (defaults to v1 format)
pub fn read_catalog<R: Read>(reader: &mut R) -> Result<Database, StorageError> {
    // Default to v1 for backward compatibility with tests that don't pass version
    read_catalog_v(reader, 1)
}

/// Convert AST `IndexColumn`s (the format used by the storage-side index
/// manager) into catalog `IndexedColumn`s (the format used by
/// `vibesql_catalog::IndexMetadata`).
///
/// This mirrors the conversion in `vibesql-executor`'s
/// `btree_index::create_btree_index`. We duplicate it here because the
/// storage crate cannot depend on the executor crate, and the binary-load
/// path needs to repopulate the catalog without re-running the executor.
fn convert_ast_columns_to_catalog(
    columns: &[vibesql_ast::IndexColumn],
) -> Vec<vibesql_catalog::IndexedColumn> {
    columns
        .iter()
        .map(|col| {
            let order = match col.direction() {
                vibesql_ast::OrderDirection::Asc => vibesql_catalog::SortOrder::Ascending,
                vibesql_ast::OrderDirection::Desc => vibesql_catalog::SortOrder::Descending,
            };

            match col {
                vibesql_ast::IndexColumn::Expression { expr, .. } => {
                    vibesql_catalog::IndexedColumn::new_expression((**expr).clone(), order)
                }
                vibesql_ast::IndexColumn::Column {
                    column_name,
                    prefix_length,
                    collation,
                    is_quoted,
                    ..
                } => {
                    if let Some(prefix) = prefix_length {
                        vibesql_catalog::IndexedColumn::new_column_with_prefix(
                            column_name.clone(),
                            order,
                            *prefix,
                        )
                        .with_collation(collation.clone())
                        .with_quoted(*is_quoted)
                    } else {
                        vibesql_catalog::IndexedColumn::new_column(column_name.clone(), order)
                            .with_collation(collation.clone())
                            .with_quoted(*is_quoted)
                    }
                }
            }
        })
        .collect()
}

/// Parse data type string back to DataType enum
pub(super) fn parse_data_type(type_str: &str) -> Result<vibesql_types::DataType, StorageError> {
    use vibesql_types::DataType;

    let upper = type_str.to_uppercase();

    match upper.as_str() {
        "INTEGER" => Ok(DataType::Integer),
        "SMALLINT" => Ok(DataType::Smallint),
        "BIGINT" => Ok(DataType::Bigint),
        "BIGINT UNSIGNED" => Ok(DataType::Unsigned),
        "REAL" => Ok(DataType::Real),
        "DOUBLE PRECISION" => Ok(DataType::DoublePrecision),
        "BOOLEAN" => Ok(DataType::Boolean),
        "DATE" => Ok(DataType::Date),
        "TIME" => Ok(DataType::Time { with_timezone: false }),
        "TIMESTAMP" | "DATETIME" => Ok(DataType::Timestamp { with_timezone: false }),
        "TIMESTAMP WITH TIME ZONE" | "DATETIME WITH TIME ZONE" => {
            Ok(DataType::Timestamp { with_timezone: true })
        }
        s if s.starts_with("VARCHAR(") => {
            let len_str = s.trim_start_matches("VARCHAR(").trim_end_matches(')');
            let max_length = len_str.parse().ok();
            Ok(DataType::Varchar { max_length })
        }
        s if s.starts_with("VARCHAR") => Ok(DataType::Varchar { max_length: None }),
        s if s.starts_with("CHAR(") => {
            let len_str = s.trim_start_matches("CHAR(").trim_end_matches(')');
            let length = len_str.parse().unwrap_or(1);
            Ok(DataType::Character { length })
        }
        s if s.starts_with("FLOAT(") => {
            let prec_str = s.trim_start_matches("FLOAT(").trim_end_matches(')');
            let precision = prec_str.parse().unwrap_or(53);
            Ok(DataType::Float { precision })
        }
        s if s.starts_with("NUMERIC(") => {
            let params = s.trim_start_matches("NUMERIC(").trim_end_matches(')');
            let parts: Vec<&str> = params.split(',').map(|p| p.trim()).collect();
            let precision = parts.first().and_then(|p| p.parse().ok()).unwrap_or(38);
            let scale = parts.get(1).and_then(|p| p.parse().ok()).unwrap_or(0);
            Ok(DataType::Numeric { precision, scale })
        }
        s if s.starts_with("DECIMAL(") => {
            let params = s.trim_start_matches("DECIMAL(").trim_end_matches(')');
            let parts: Vec<&str> = params.split(',').map(|p| p.trim()).collect();
            let precision = parts.first().and_then(|p| p.parse().ok()).unwrap_or(38);
            let scale = parts.get(1).and_then(|p| p.parse().ok()).unwrap_or(0);
            Ok(DataType::Decimal { precision, scale })
        }
        // Binary/Character large objects
        "BLOB" => Ok(DataType::BinaryLargeObject),
        "CLOB" => Ok(DataType::CharacterLargeObject),
        // Bit types
        s if s.starts_with("BIT(") => {
            let len_str = s.trim_start_matches("BIT(").trim_end_matches(')');
            let length = len_str.parse().ok();
            Ok(DataType::Bit { length })
        }
        "BIT" => Ok(DataType::Bit { length: None }),
        // Vector type
        s if s.starts_with("VECTOR(") => {
            let dim_str = s.trim_start_matches("VECTOR(").trim_end_matches(')');
            let dimensions = dim_str.parse().unwrap_or(1);
            Ok(DataType::Vector { dimensions })
        }
        // Null type
        "NULL" => Ok(DataType::Null),
        // Any other name is a SQLite-style user-defined type. The save side
        // (`data_type_to_sql`) writes `UserDefined` type names verbatim, so an
        // unknown name here is the round-trip of a type like
        // `CREATE TABLE t(x banana)` or a fallback keyword used as a type name
        // (`CREATE TABLE attach(attach attach)`, keyword1.test). Storage is
        // governed by affinity only, exactly as at first parse. Erroring here
        // used to make such tables silently vanish on reopen (issue #5816;
        // the error-swallow itself is tracked in #5855).
        _ => Ok(DataType::UserDefined { type_name: type_str.to_string() }),
    }
}

#[cfg(test)]
mod tests {
    use super::{super::format::VERSION, read_catalog_v, write_catalog};
    use crate::Database;

    /// Issue #5619: the verbatim original `CREATE TABLE` source text
    /// (`TableSchema::sql_source`) must survive a binary catalog round-trip.
    ///
    /// This is the cross-process reload guarantee: the TCL shim spawns a fresh
    /// CLI process per batch against a shared `.vbsql` file, so a CREATE in one
    /// process is read back via `SELECT sql FROM sqlite_master` in another only
    /// if the binary format persists `sql_source`. Before v9 the catalog
    /// reconstructed each table via `TableSchema::new`, silently dropping the
    /// field.
    #[test]
    fn test_binary_catalog_preserves_verbatim_sql_source() {
        let mut db = Database::new();

        // A deliberately multi-line, original-formatting CREATE TABLE.
        let original_sql = "CREATE TABLE t1(\n  a INTEGER,\n  b TEXT\n)";

        let mut schema = vibesql_catalog::TableSchema::new(
            "t1".to_string(),
            vec![
                vibesql_catalog::ColumnSchema {
                    name: "a".to_string(),
                    data_type: vibesql_types::DataType::Integer,
                    nullable: true,
                    default_value: None,
                    generated_expr: None,
                    collation: None,
                    is_exact_integer_type: true,
                },
                vibesql_catalog::ColumnSchema {
                    name: "b".to_string(),
                    data_type: vibesql_types::DataType::Varchar { max_length: None },
                    nullable: true,
                    default_value: None,
                    generated_expr: None,
                    collation: None,
                    is_exact_integer_type: false,
                },
            ],
        );
        schema.set_sql_source(original_sql);
        let identifier = vibesql_catalog::TableIdentifier::new("t1", false);
        db.create_table_with_identifier(schema, identifier).unwrap();

        // Round-trip the catalog through the binary encoder/decoder at the
        // current version.
        let mut buf = Vec::new();
        write_catalog(&mut buf, &db).unwrap();
        let reloaded = read_catalog_v(&mut &buf[..], VERSION).unwrap();

        let table = reloaded.get_table("t1").expect("t1 must survive the round-trip");
        assert_eq!(
            table.schema.sql_source.as_deref(),
            Some(original_sql),
            "verbatim multi-line CREATE TABLE source must survive binary persistence (issue #5619)"
        );
    }

    /// Issue #5816: `UserDefined` column types (any SQLite-style type name,
    /// including fallback keywords like `attach` used as a type) must survive
    /// a binary catalog round-trip. The save side writes the type name
    /// verbatim; before this fix the read side had no `UserDefined` fallback,
    /// so `CREATE TABLE t(x banana)` reloaded as "Unsupported data type" and
    /// the table silently vanished on reopen (swallow tracked in #5855).
    #[test]
    fn test_binary_catalog_round_trips_user_defined_type() {
        let mut db = Database::new();

        let schema = vibesql_catalog::TableSchema::new(
            "t3".to_string(),
            vec![
                vibesql_catalog::ColumnSchema {
                    name: "x".to_string(),
                    data_type: vibesql_types::DataType::UserDefined {
                        type_name: "banana".to_string(),
                    },
                    nullable: true,
                    default_value: None,
                    generated_expr: None,
                    collation: None,
                    is_exact_integer_type: false,
                },
                vibesql_catalog::ColumnSchema {
                    name: "attach".to_string(),
                    data_type: vibesql_types::DataType::UserDefined {
                        type_name: "attach".to_string(),
                    },
                    nullable: true,
                    default_value: None,
                    generated_expr: None,
                    collation: None,
                    is_exact_integer_type: false,
                },
            ],
        );
        let identifier = vibesql_catalog::TableIdentifier::new("t3", false);
        db.create_table_with_identifier(schema, identifier).unwrap();

        let mut buf = Vec::new();
        write_catalog(&mut buf, &db).unwrap();
        let reloaded = read_catalog_v(&mut &buf[..], VERSION).unwrap();

        let table = reloaded.get_table("t3").expect("t3 must survive the round-trip");
        assert_eq!(
            table.schema.columns[0].data_type,
            vibesql_types::DataType::UserDefined { type_name: "banana".to_string() },
            "UserDefined type name must survive binary persistence (issue #5816)"
        );
        assert_eq!(
            table.schema.columns[1].data_type,
            vibesql_types::DataType::UserDefined { type_name: "attach".to_string() },
            "fallback-keyword type name must survive binary persistence (issue #5816)"
        );
    }

    /// A table with no captured source must round-trip as `None` (the
    /// reconstructed-SQL fallback path), not as an empty string or an error.
    #[test]
    fn test_binary_catalog_no_sql_source_round_trips_as_none() {
        let mut db = Database::new();
        let schema = vibesql_catalog::TableSchema::new(
            "t2".to_string(),
            vec![vibesql_catalog::ColumnSchema {
                name: "x".to_string(),
                data_type: vibesql_types::DataType::Integer,
                nullable: true,
                default_value: None,
                generated_expr: None,
                collation: None,
                is_exact_integer_type: true,
            }],
        );
        let identifier = vibesql_catalog::TableIdentifier::new("t2", false);
        db.create_table_with_identifier(schema, identifier).unwrap();

        let mut buf = Vec::new();
        write_catalog(&mut buf, &db).unwrap();
        let reloaded = read_catalog_v(&mut &buf[..], VERSION).unwrap();

        let table = reloaded.get_table("t2").expect("t2 must survive the round-trip");
        assert_eq!(table.schema.sql_source, None);
    }

    /// Issue #5796 (v12): the WITHOUT ROWID flag must survive a binary catalog
    /// round-trip. Before v12 the flag was not serialized, so a reloaded
    /// WITHOUT ROWID table forgot it was rowid-less and `sqlite_master`
    /// wrongly listed its implicit PRIMARY KEY autoindex across processes
    /// (alterdropcol 7.2).
    #[test]
    fn test_binary_catalog_without_rowid_round_trip() {
        let mut db = Database::new();
        let make_col = |name: &str| vibesql_catalog::ColumnSchema {
            name: name.to_string(),
            data_type: vibesql_types::DataType::Integer,
            nullable: true,
            default_value: None,
            generated_expr: None,
            collation: None,
            is_exact_integer_type: true,
        };

        let mut wr_schema = vibesql_catalog::TableSchema::with_primary_key(
            "t_wr".to_string(),
            vec![make_col("a"), make_col("b")],
            vec!["a".to_string()],
        );
        wr_schema.without_rowid = true;
        db.create_table_with_identifier(
            wr_schema,
            vibesql_catalog::TableIdentifier::new("t_wr", false),
        )
        .unwrap();

        let rowid_schema = vibesql_catalog::TableSchema::with_primary_key(
            "t_rowid".to_string(),
            vec![make_col("a"), make_col("b")],
            vec!["a".to_string()],
        );
        db.create_table_with_identifier(
            rowid_schema,
            vibesql_catalog::TableIdentifier::new("t_rowid", false),
        )
        .unwrap();

        let mut buf = Vec::new();
        write_catalog(&mut buf, &db).unwrap();
        let reloaded = read_catalog_v(&mut &buf[..], VERSION).unwrap();

        assert!(
            reloaded.get_table("t_wr").expect("t_wr must survive").schema.without_rowid,
            "WITHOUT ROWID flag must survive binary persistence (issue #5796)"
        );
        assert!(
            !reloaded.get_table("t_rowid").expect("t_rowid must survive").schema.without_rowid,
            "rowid table must stay rowid after reload"
        );
    }

    /// Issue #5837: a STRICT table's `strict` flag and per-column strict types
    /// must survive a binary catalog round-trip. Neither is serialized as a
    /// dedicated field — both are rederived from the persisted `sql_source` on
    /// load — so no binary format bump was required. ANY must remain
    /// distinguishable from BLOB after reload.
    #[test]
    fn test_binary_catalog_strict_round_trip() {
        let mut db = Database::new();
        let make_col = |name: &str, dt: vibesql_types::DataType| {
            let is_exact = matches!(dt, vibesql_types::DataType::Integer);
            vibesql_catalog::ColumnSchema {
                name: name.to_string(),
                data_type: dt,
                nullable: true,
                default_value: None,
                generated_expr: None,
                collation: None,
                is_exact_integer_type: is_exact,
            }
        };

        let mut strict_schema = vibesql_catalog::TableSchema::new(
            "t_strict".to_string(),
            vec![
                make_col("a", vibesql_types::DataType::Integer),
                make_col("b", vibesql_types::DataType::BinaryLargeObject), // BLOB
                make_col("c", vibesql_types::DataType::BinaryLargeObject), // ANY (same DataType!)
            ],
        );
        strict_schema.strict = true;
        strict_schema.strict_types = vec![
            vibesql_catalog::StrictType::Int,
            vibesql_catalog::StrictType::Blob,
            vibesql_catalog::StrictType::Any,
        ];
        // The strict flag/types are rederived from sql_source on load.
        strict_schema
            .set_sql_source("CREATE TABLE t_strict(a INT, b BLOB, c ANY) STRICT".to_string());
        db.create_table_with_identifier(
            strict_schema,
            vibesql_catalog::TableIdentifier::new("t_strict", false),
        )
        .unwrap();

        let mut buf = Vec::new();
        write_catalog(&mut buf, &db).unwrap();
        let reloaded = read_catalog_v(&mut &buf[..], VERSION).unwrap();

        let schema = &reloaded.get_table("t_strict").expect("t_strict must survive").schema;
        assert!(schema.strict, "STRICT flag must survive binary persistence (issue #5837)");
        assert_eq!(
            schema.strict_types,
            vec![
                vibesql_catalog::StrictType::Int,
                vibesql_catalog::StrictType::Blob,
                vibesql_catalog::StrictType::Any,
            ],
            "per-column strict types (incl. ANY vs BLOB) must survive reload"
        );
    }

    /// Issue #5771: views must survive a binary catalog round-trip. Before v10
    /// the serializer had no views section at all, so every `CREATE VIEW` was
    /// silently dropped when a file-backed DB reopened from a checkpoint under
    /// the default `wal = true` config.
    ///
    /// Covers: a plain view, a view with an explicit column list, a view with
    /// `WITH CHECK OPTION`, and a temp view — asserting name/schema/columns/
    /// with_check_option/query SQL all survive.
    #[test]
    fn test_binary_catalog_round_trips_views() {
        use vibesql_ast::pretty_print::ToSql;

        let mut db = Database::new();

        // Backing table so the defining SELECTs reference something real.
        let schema = vibesql_catalog::TableSchema::new(
            "t1".to_string(),
            vec![vibesql_catalog::ColumnSchema {
                name: "a".to_string(),
                data_type: vibesql_types::DataType::Integer,
                nullable: true,
                default_value: None,
                generated_expr: None,
                collation: None,
                is_exact_integer_type: true,
            }],
        );
        db.create_table_with_identifier(schema, vibesql_catalog::TableIdentifier::new("t1", false))
            .unwrap();

        let parse = |sql: &str| vibesql_parser::arena_parser::parse_select_to_owned(sql).unwrap();

        // 1. Plain view, with a verbatim sql_definition.
        let plain = vibesql_catalog::ViewDefinition::new_with_sql(
            "v_plain".to_string(),
            None,
            parse("SELECT a FROM t1"),
            false,
            "CREATE VIEW v_plain AS SELECT a FROM t1".to_string(),
        );
        db.catalog.create_view(plain).unwrap();

        // 2. View with an explicit column list (the wherelimit shape: tv(r,a)).
        let with_cols = vibesql_catalog::ViewDefinition::new(
            "v_cols".to_string(),
            Some(vec!["r".to_string(), "a".to_string()]),
            parse("SELECT rowid, a FROM t1"),
            false,
        );
        db.catalog.create_view(with_cols).unwrap();

        // 3. View WITH CHECK OPTION.
        let with_check = vibesql_catalog::ViewDefinition::new(
            "v_check".to_string(),
            None,
            parse("SELECT a FROM t1 WHERE a > 0"),
            true,
        );
        db.catalog.create_view(with_check).unwrap();

        // 4. Temp view (schema = Some("temp")), no sql_definition.
        let temp = vibesql_catalog::ViewDefinition::new(
            "v_temp".to_string(),
            None,
            parse("SELECT a FROM t1"),
            false,
        )
        .with_schema(Some("temp".to_string()));
        db.catalog.create_view(temp).unwrap();

        // 5. Compound view: UNION with a trailing ORDER BY (issue #5798, window9.test §8). The
        //    ORDER BY applies to the whole compound, so ToSql must render it AFTER the set
        //    operation — otherwise the persisted SELECT text is invalid SQL and the view is dropped
        //    on reload (checkpoint recovery falls back to an older/empty state).
        let compound = vibesql_catalog::ViewDefinition::new(
            "v_compound".to_string(),
            None,
            parse("SELECT 0 AS x UNION SELECT count() OVER() FROM (SELECT 0) ORDER BY 1"),
            false,
        );
        db.catalog.create_view(compound).unwrap();

        // Round-trip the catalog at the current version.
        let mut buf = Vec::new();
        write_catalog(&mut buf, &db).unwrap();
        let reloaded = read_catalog_v(&mut &buf[..], VERSION).unwrap();

        // Plain view.
        let v = reloaded.catalog.get_view("v_plain").expect("v_plain must survive");
        assert_eq!(v.name, "v_plain");
        assert_eq!(v.schema, None);
        assert_eq!(v.columns, None);
        assert!(!v.with_check_option);
        assert_eq!(v.sql_definition.as_deref(), Some("CREATE VIEW v_plain AS SELECT a FROM t1"));
        assert_eq!(v.query.to_sql(), parse("SELECT a FROM t1").to_sql());

        // Explicit column list.
        let v = reloaded.catalog.get_view("v_cols").expect("v_cols must survive");
        assert_eq!(v.columns.as_deref(), Some(&["r".to_string(), "a".to_string()][..]));
        assert_eq!(v.query.to_sql(), parse("SELECT rowid, a FROM t1").to_sql());

        // WITH CHECK OPTION.
        let v = reloaded.catalog.get_view("v_check").expect("v_check must survive");
        assert!(v.with_check_option);
        assert_eq!(v.query.to_sql(), parse("SELECT a FROM t1 WHERE a > 0").to_sql());

        // Temp view (issue #5940, Cluster A): a `CREATE TEMP VIEW` is
        // session-scoped and MUST NOT survive a checkpoint round-trip. It is
        // filtered out of `write_catalog`, so it is absent from the reloaded
        // catalog even though the other views round-trip.
        assert!(
            reloaded.catalog.get_view("v_temp").is_none(),
            "temp view must NOT survive a checkpoint round-trip"
        );

        // Compound view (UNION + ORDER BY, issue #5798): must survive the
        // round-trip with the ORDER BY still applying to the whole compound.
        // The parser assigns synthetic subquery aliases from a global counter
        // (`(subquery-N)`), so normalize those before comparing.
        let normalize = |sql: &str| -> String {
            let mut out = String::with_capacity(sql.len());
            let mut rest = sql;
            while let Some(start) = rest.find("(subquery-") {
                let after = &rest[start + "(subquery-".len()..];
                let end = after.find(')').map(|i| start + "(subquery-".len() + i + 1);
                match end {
                    Some(end) => {
                        out.push_str(&rest[..start]);
                        out.push_str("(subquery-N)");
                        rest = &rest[end..];
                    }
                    None => break,
                }
            }
            out.push_str(rest);
            out
        };
        let v = reloaded.catalog.get_view("v_compound").expect("v_compound must survive");
        assert_eq!(
            normalize(&v.query.to_sql()),
            normalize(
                &parse("SELECT 0 AS x UNION SELECT count() OVER() FROM (SELECT 0) ORDER BY 1")
                    .to_sql()
            )
        );
        assert!(
            v.query.to_sql().find("UNION").unwrap() < v.query.to_sql().find("ORDER BY").unwrap(),
            "ORDER BY must render after the set operation, got: {}",
            v.query.to_sql()
        );
    }

    /// Issue #5833: the reload path must re-parse everything the main parser
    /// accepted at CREATE VIEW time. The reader used the arena parser's
    /// `parse_select_to_owned`, whose grammar is a strict subset — it rejects
    /// bare `VALUES(...)` bodies and `COLLATE` anywhere in an expression — so
    /// such views poisoned the checkpoint: under fail-closed recovery every
    /// subsequent open failed (join7/join9: `CREATE VIEW dual(dummy) AS
    /// VALUES('x')`; tkt-a7debbe0: COLLATE in the select list).
    ///
    /// Covers: a VALUES-body view, a COLLATE select-list view, a CTE view, a
    /// RECURSIVE CTE view (RECURSIVE keyword must survive the ToSql render),
    /// and an EXCEPT/INTERSECT compound view.
    #[test]
    fn test_binary_catalog_round_trips_full_select_grammar_views() {
        use vibesql_ast::pretty_print::ToSql;

        let mut db = Database::new();

        // Backing table so the defining SELECTs reference something real.
        let schema = vibesql_catalog::TableSchema::new(
            "t1".to_string(),
            vec![vibesql_catalog::ColumnSchema {
                name: "a".to_string(),
                data_type: vibesql_types::DataType::Integer,
                nullable: true,
                default_value: None,
                generated_expr: None,
                collation: None,
                is_exact_integer_type: true,
            }],
        );
        db.create_table_with_identifier(schema, vibesql_catalog::TableIdentifier::new("t1", false))
            .unwrap();

        // Parse through the MAIN parser — the same grammar CREATE VIEW uses at
        // runtime — so the test exercises exactly what the writer serializes.
        let parse = |sql: &str| match vibesql_parser::Parser::parse_sql(sql).unwrap() {
            vibesql_ast::Statement::Select(s) => *s,
            other => panic!("expected SELECT statement for '{}', got {:?}", sql, other),
        };

        // 1. Bare VALUES body (join7/join9's `dual` view).
        db.catalog
            .create_view(vibesql_catalog::ViewDefinition::new_with_sql(
                "dual".to_string(),
                Some(vec!["dummy".to_string()]),
                parse("VALUES('x')"),
                false,
                "CREATE VIEW dual(dummy) AS VALUES('x')".to_string(),
            ))
            .unwrap();

        // 2. COLLATE in the select list (tkt-a7debbe0's v2).
        db.catalog
            .create_view(vibesql_catalog::ViewDefinition::new(
                "v_collate".to_string(),
                Some(vec!["a".to_string(), "B".to_string()]),
                parse("SELECT 'a', 'B' COLLATE NOCASE FROM t1"),
                false,
            ))
            .unwrap();

        // 3. CTE view.
        db.catalog
            .create_view(vibesql_catalog::ViewDefinition::new(
                "v_cte".to_string(),
                None,
                parse("WITH c AS (SELECT a FROM t1) SELECT * FROM c"),
                false,
            ))
            .unwrap();

        // 4. RECURSIVE CTE view — ToSql must render the RECURSIVE keyword so the flag survives the
        //    text round-trip.
        db.catalog
            .create_view(vibesql_catalog::ViewDefinition::new(
                "v_rec".to_string(),
                None,
                parse(
                    "WITH RECURSIVE c(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM c WHERE x<5) \
                     SELECT x FROM c",
                ),
                false,
            ))
            .unwrap();

        // 5. EXCEPT/INTERSECT compound view.
        db.catalog
            .create_view(vibesql_catalog::ViewDefinition::new(
                "v_compound2".to_string(),
                None,
                parse("SELECT a FROM t1 EXCEPT SELECT a FROM t1 INTERSECT SELECT a FROM t1"),
                false,
            ))
            .unwrap();

        // Round-trip the catalog at the current version.
        let mut buf = Vec::new();
        write_catalog(&mut buf, &db).unwrap();
        let reloaded = read_catalog_v(&mut &buf[..], VERSION).unwrap();

        // VALUES body.
        let v = reloaded.catalog.get_view("dual").expect("VALUES-body view must survive");
        assert_eq!(v.columns.as_deref(), Some(&["dummy".to_string()][..]));
        assert_eq!(v.query.to_sql(), parse("VALUES('x')").to_sql());
        assert!(v.query.values.is_some(), "reloaded view must still be a VALUES statement");

        // COLLATE select list.
        let v = reloaded.catalog.get_view("v_collate").expect("COLLATE view must survive");
        assert_eq!(v.query.to_sql(), parse("SELECT 'a', 'B' COLLATE NOCASE FROM t1").to_sql());
        assert!(
            v.query.to_sql().contains("COLLATE NOCASE"),
            "COLLATE must survive the round-trip, got: {}",
            v.query.to_sql()
        );

        // CTE view.
        let v = reloaded.catalog.get_view("v_cte").expect("CTE view must survive");
        assert_eq!(
            v.query.to_sql(),
            parse("WITH c AS (SELECT a FROM t1) SELECT * FROM c").to_sql()
        );

        // RECURSIVE CTE view: the recursive flag must survive.
        let v = reloaded.catalog.get_view("v_rec").expect("recursive CTE view must survive");
        let ctes = v.query.with_clause.as_ref().expect("WITH clause must survive");
        assert!(
            ctes.iter().all(|c| c.recursive),
            "RECURSIVE flag must survive the round-trip, got: {}",
            v.query.to_sql()
        );

        // EXCEPT/INTERSECT compound view.
        let v = reloaded.catalog.get_view("v_compound2").expect("compound view must survive");
        assert_eq!(
            v.query.to_sql(),
            parse("SELECT a FROM t1 EXCEPT SELECT a FROM t1 INTERSECT SELECT a FROM t1").to_sql()
        );
    }

    /// Issue #5833 (expression sites): expression indexes and partial-index
    /// WHERE clauses are also persisted as ToSql text and re-parsed on load
    /// via the arena expression parser, which rejects `COLLATE`. They must go
    /// through the main parser's expression grammar instead.
    #[test]
    fn test_binary_catalog_round_trips_collate_in_index_expressions() {
        use vibesql_ast::pretty_print::ToSql;

        let mut db = Database::new();
        let schema = vibesql_catalog::TableSchema::new(
            "t1".to_string(),
            vec![vibesql_catalog::ColumnSchema {
                name: "x".to_string(),
                data_type: vibesql_types::DataType::Varchar { max_length: None },
                nullable: true,
                default_value: None,
                generated_expr: None,
                collation: None,
                is_exact_integer_type: false,
            }],
        );
        db.create_table_with_identifier(schema, vibesql_catalog::TableIdentifier::new("t1", false))
            .unwrap();

        // Expression index whose expression contains COLLATE.
        let expr = vibesql_parser::Parser::parse_expression_sql("x COLLATE NOCASE").unwrap();
        let columns = vec![vibesql_ast::IndexColumn::Expression {
            expr: Box::new(expr),
            direction: vibesql_ast::OrderDirection::Asc,
        }];
        db.create_index("i_collate".to_string(), "t1".to_string(), false, columns).unwrap();

        // Partial index whose WHERE predicate contains COLLATE.
        let where_expr =
            vibesql_parser::Parser::parse_expression_sql("x COLLATE NOCASE = 'a'").unwrap();
        let cols = vec![vibesql_ast::IndexColumn::new_column(
            "x".to_string(),
            vibesql_ast::OrderDirection::Asc,
        )];
        db.create_index("i_partial".to_string(), "t1".to_string(), false, cols).unwrap();
        let catalog_meta = vibesql_catalog::IndexMetadata::new(
            "i_partial".to_string(),
            "t1".to_string(),
            vibesql_catalog::IndexType::BTree,
            vec![vibesql_catalog::IndexedColumn::new_column(
                "x".to_string(),
                vibesql_catalog::SortOrder::Ascending,
            )],
            false,
        );
        db.catalog.add_index(catalog_meta).unwrap();
        assert!(db.catalog.set_index_where_clause("i_partial", Some(where_expr)));

        let mut buf = Vec::new();
        write_catalog(&mut buf, &db).unwrap();
        let reloaded = read_catalog_v(&mut &buf[..], VERSION).unwrap();

        let meta = reloaded.get_index("i_collate").expect("COLLATE expression index must survive");
        match &meta.columns[0] {
            vibesql_ast::IndexColumn::Expression { expr, .. } => {
                assert!(
                    expr.to_sql().contains("COLLATE NOCASE"),
                    "COLLATE must survive in the index expression, got: {}",
                    expr.to_sql()
                );
            }
            other => panic!("expected expression index column, got {:?}", other),
        }

        let meta = reloaded
            .catalog
            .find_index_by_name("i_partial")
            .expect("partial index metadata must survive");
        let where_clause = meta.where_clause.as_ref().expect("partial-index WHERE must survive");
        assert!(
            where_clause.to_sql().contains("COLLATE NOCASE"),
            "COLLATE must survive in the partial-index WHERE, got: {}",
            where_clause.to_sql()
        );
    }

    /// Issue #5771 backward compat: a v9 (pre-views) file must load cleanly
    /// under the new (v10) reader, yielding zero views rather than an error.
    ///
    /// The only on-disk difference between v9 and v10 is the views section
    /// inserted between the indexes and triggers sections. For a catalog with
    /// no views, the v10 views section is just a single `u32` count of `0`, so
    /// the v10 serialization of a zero-view catalog is byte-identical to a
    /// genuine v9 file except for that extra trailing `[0]` count (which a v9
    /// reader, stopping after triggers, never reaches). Reading such a buffer
    /// with an explicit `version = 9` exercises exactly the path a real v9 file
    /// takes through the v10 binary: the `version >= 10` gate is skipped, no
    /// view bytes are consumed, and the catalog loads as zero views.
    #[test]
    fn test_v9_reader_treats_absent_views_as_zero() {
        let mut db = Database::new();
        let schema = vibesql_catalog::TableSchema::new(
            "t1".to_string(),
            vec![vibesql_catalog::ColumnSchema {
                name: "a".to_string(),
                data_type: vibesql_types::DataType::Integer,
                nullable: true,
                default_value: None,
                generated_expr: None,
                collation: None,
                is_exact_integer_type: true,
            }],
        );
        db.create_table_with_identifier(schema, vibesql_catalog::TableIdentifier::new("t1", false))
            .unwrap();
        // No views and no triggers: the trailing v10 view-count(0) and
        // trigger-count(0) leave the v9 read path perfectly aligned.

        let mut buf = Vec::new();
        write_catalog(&mut buf, &db).unwrap();

        // v9 reader: the views section is gated off, treated as zero views.
        let reloaded = read_catalog_v(&mut &buf[..], 9).unwrap();
        assert!(
            reloaded.catalog.list_views().is_empty(),
            "a v9 reader must treat the absent views section as zero views"
        );
        assert!(
            reloaded.get_table("t1").is_some(),
            "the table must still load under the v9 reader"
        );

        // v10 reader of the same zero-view catalog: also zero views, no error.
        let reloaded_v10 = read_catalog_v(&mut &buf[..], VERSION).unwrap();
        assert!(reloaded_v10.catalog.list_views().is_empty());
        assert!(reloaded_v10.get_table("t1").is_some());
    }

    /// Issue #5794: a generated-column expression (`c AS (a+b)`) must survive
    /// a binary catalog round-trip.
    ///
    /// Generated values are materialized at INSERT time, so rows written
    /// before a save always reload correctly — the failure mode is that a
    /// reloaded schema without `generated_expr` computes NULL for every
    /// *subsequent* INSERT. This is the cross-process reload guarantee the
    /// TCL shim relies on (each `do_execsql_test` batch runs in a fresh CLI
    /// process against a shared `.vbsql` file), and why `alterdropcol.test`
    /// section 4 failed before v11.
    #[test]
    fn test_binary_catalog_round_trips_generated_column_expr() {
        let gen_expr = vibesql_parser::arena_parser::parse_expression_to_owned("a + b").unwrap();

        let mut db = Database::new();
        let schema = vibesql_catalog::TableSchema::new(
            "mt".to_string(),
            vec![
                vibesql_catalog::ColumnSchema {
                    name: "a".to_string(),
                    data_type: vibesql_types::DataType::Integer,
                    nullable: true,
                    default_value: None,
                    generated_expr: None,
                    collation: None,
                    is_exact_integer_type: true,
                },
                vibesql_catalog::ColumnSchema {
                    name: "b".to_string(),
                    data_type: vibesql_types::DataType::Integer,
                    nullable: true,
                    default_value: None,
                    generated_expr: None,
                    collation: None,
                    is_exact_integer_type: true,
                },
                vibesql_catalog::ColumnSchema {
                    name: "c".to_string(),
                    data_type: vibesql_types::DataType::Integer,
                    nullable: true,
                    default_value: None,
                    generated_expr: Some(gen_expr.clone()),
                    collation: None,
                    is_exact_integer_type: true,
                },
                vibesql_catalog::ColumnSchema {
                    name: "d".to_string(),
                    data_type: vibesql_types::DataType::Varchar { max_length: None },
                    nullable: true,
                    default_value: None,
                    generated_expr: None,
                    collation: None,
                    is_exact_integer_type: false,
                },
            ],
        );
        db.create_table_with_identifier(schema, vibesql_catalog::TableIdentifier::new("mt", false))
            .unwrap();

        let mut buf = Vec::new();
        write_catalog(&mut buf, &db).unwrap();
        let reloaded = read_catalog_v(&mut &buf[..], VERSION).unwrap();

        let table = reloaded.get_table("mt").expect("mt must survive the round-trip");
        assert_eq!(
            table.schema.columns[2].generated_expr,
            Some(gen_expr),
            "generated-column expression must survive binary persistence (issue #5794)"
        );
        // Non-generated columns must stay non-generated.
        assert_eq!(table.schema.columns[0].generated_expr, None);
        assert_eq!(table.schema.columns[1].generated_expr, None);
        assert_eq!(table.schema.columns[3].generated_expr, None);
    }

    /// Issue #5794 backward compat: a v10 (pre-generated-expr) file must load
    /// cleanly under the v11 reader, yielding `generated_expr: None` rather
    /// than an error.
    ///
    /// Unlike the v9→v10 views case (where the new section trails the old
    /// layout), the generated-expr field sits *inside* each per-column record,
    /// so a v11-written buffer is NOT byte-compatible with a v10 read. This
    /// test therefore hand-encodes a genuine v10 catalog byte stream — column
    /// records ending at the collation flag — and reads it with
    /// `version = 10`, exercising exactly the path a real v10 file takes
    /// through the v11 binary: the `version >= 11` gate is skipped, no
    /// generated-expr bytes are consumed, and every column loads with
    /// `generated_expr: None`.
    #[test]
    fn test_v10_file_loads_generated_expr_as_none() {
        use super::super::io::{write_bool, write_string, write_u32};

        // Hand-encode a v10 catalog: one table `t1` with one column `a`.
        let mut buf: Vec<u8> = Vec::new();
        write_u32(&mut buf, 0).unwrap(); // schemas: none
        write_u32(&mut buf, 0).unwrap(); // roles: none
        write_u32(&mut buf, 0).unwrap(); // sequences: none (v2+)
        write_u32(&mut buf, 1).unwrap(); // tables: one
        write_string(&mut buf, "t1").unwrap();
        write_u32(&mut buf, 1).unwrap(); // columns: one
        write_string(&mut buf, "a").unwrap(); // column name
        write_string(&mut buf, "INTEGER").unwrap(); // data type
        write_bool(&mut buf, true).unwrap(); // nullable
        write_bool(&mut buf, false).unwrap(); // no default_value (v2+)
        write_bool(&mut buf, false).unwrap(); // no collation (v5+)
                                              // (v10 column records END here: no generated-expr field)
        write_bool(&mut buf, false).unwrap(); // no primary key (v3+)
        write_bool(&mut buf, false).unwrap(); // unquoted identifier (v4+)
        write_bool(&mut buf, false).unwrap(); // no sql_source (v9+)
        write_u32(&mut buf, 0).unwrap(); // indexes: none
        write_u32(&mut buf, 0).unwrap(); // views: none (v10+)
        write_u32(&mut buf, 0).unwrap(); // triggers: none

        let reloaded = read_catalog_v(&mut &buf[..], 10).unwrap();
        let table =
            reloaded.get_table("t1").expect("a v10 table must still load under the v11 binary");
        assert_eq!(table.schema.columns.len(), 1);
        assert_eq!(
            table.schema.columns[0].generated_expr, None,
            "a v10 file has no generated-expr field; the v11 reader must default it to None"
        );
        assert_eq!(table.schema.columns[0].collation, None);
        assert!(table.schema.columns[0].nullable);
    }

    /// Issue #5940, Cluster A: temp triggers must NOT survive a checkpoint,
    /// and a non-temp trigger's explicit `schema` field must round-trip (v14).
    ///
    /// Before the fix, `write_catalog` serialized every trigger with no
    /// `is_temp()` filter and never wrote the `schema` field, so (a) a
    /// `CREATE TEMP TRIGGER` persisted into the next session and (b) every
    /// reloaded trigger lost its schema tag and became a main-schema trigger.
    #[test]
    fn test_binary_catalog_temp_trigger_dropped_and_schema_round_trips() {
        let mut db = Database::new();

        // A plain (main-schema) trigger: `schema = None`. Must survive.
        let plain = vibesql_catalog::TriggerDefinition::new(
            "tr_main".to_string(),
            vibesql_ast::TriggerTiming::After,
            vibesql_ast::TriggerEvent::Insert,
            "t".to_string(),
            vibesql_ast::TriggerGranularity::Row,
            None,
            vibesql_ast::TriggerAction::RawSql("SELECT 1".to_string()),
        );
        db.catalog.create_trigger(plain).unwrap();

        // A trigger with an explicit non-temp schema. Must survive AND keep its
        // schema across the round-trip (this is the v14 schema-persistence fix).
        let explicit = vibesql_catalog::TriggerDefinition::new(
            "tr_explicit".to_string(),
            vibesql_ast::TriggerTiming::Before,
            vibesql_ast::TriggerEvent::Delete,
            "t".to_string(),
            vibesql_ast::TriggerGranularity::Row,
            None,
            vibesql_ast::TriggerAction::RawSql("SELECT 2".to_string()),
        )
        .with_schema(Some("main".to_string()));
        db.catalog.create_trigger(explicit).unwrap();

        // A temp trigger (`schema = Some("temp")`). Must NOT survive.
        let temp = vibesql_catalog::TriggerDefinition::new(
            "tr_temp".to_string(),
            vibesql_ast::TriggerTiming::After,
            vibesql_ast::TriggerEvent::Insert,
            "t".to_string(),
            vibesql_ast::TriggerGranularity::Row,
            None,
            vibesql_ast::TriggerAction::RawSql("SELECT 3".to_string()),
        )
        .with_schema(Some("temp".to_string()));
        db.catalog.create_trigger(temp).unwrap();

        let mut buf = Vec::new();
        write_catalog(&mut buf, &db).unwrap();
        let reloaded = read_catalog_v(&mut &buf[..], VERSION).unwrap();

        // Main-schema trigger survived.
        let tr = reloaded.catalog.get_trigger("tr_main").expect("tr_main must survive");
        assert_eq!(tr.schema, None);
        assert!(!tr.is_temp());

        // Explicit-schema trigger survived AND kept its schema (v14 fix).
        let tr = reloaded.catalog.get_trigger("tr_explicit").expect("tr_explicit must survive");
        assert_eq!(tr.schema.as_deref(), Some("main"));
        assert!(!tr.is_temp());

        // Temp trigger did NOT survive the checkpoint round-trip.
        assert!(
            reloaded.catalog.get_trigger("tr_temp").is_none(),
            "temp trigger must NOT survive a checkpoint round-trip"
        );
    }

    /// Issue #6174: a trigger's verbatim `CREATE TRIGGER` text
    /// (`sql_definition`) must round-trip through the binary catalog (v16). Before
    /// the fix, `write_catalog` never wrote the field, so every reloaded trigger
    /// had `sql_definition = None` and `sqlite_master.sql` rendered an
    /// AST-reconstructed form (injected `BEFORE`/`FOR EACH ROW`, normalized
    /// spacing) after a checkpoint (altercol.test 9.x).
    #[test]
    fn test_binary_catalog_trigger_sql_definition_round_trips() {
        let mut db = Database::new();

        // A trigger whose verbatim text differs from any AST reconstruction
        // (multi-line body, an odd unnamed-looking header, no `FOR EACH ROW`).
        let verbatim =
            "CREATE TRIGGER AFTER INSERT ON t1 BEGIN\n        SELECT _x_ FROM t1;\n      END";
        let with_sql = vibesql_catalog::TriggerDefinition::new(
            "AFTER".to_string(),
            vibesql_ast::TriggerTiming::Before,
            vibesql_ast::TriggerEvent::Insert,
            "t1".to_string(),
            vibesql_ast::TriggerGranularity::Row,
            None,
            vibesql_ast::TriggerAction::RawSql("BEGIN SELECT _x_ FROM t1; END".to_string()),
        )
        .with_sql_definition(Some(verbatim.to_string()));
        db.catalog.create_trigger(with_sql).unwrap();

        // A trigger with no preserved text: must round-trip as `None`.
        let no_sql = vibesql_catalog::TriggerDefinition::new(
            "tr_plain".to_string(),
            vibesql_ast::TriggerTiming::After,
            vibesql_ast::TriggerEvent::Insert,
            "t1".to_string(),
            vibesql_ast::TriggerGranularity::Row,
            None,
            vibesql_ast::TriggerAction::RawSql("SELECT 1".to_string()),
        );
        db.catalog.create_trigger(no_sql).unwrap();

        let mut buf = Vec::new();
        write_catalog(&mut buf, &db).unwrap();
        let reloaded = read_catalog_v(&mut &buf[..], VERSION).unwrap();

        let tr = reloaded.catalog.get_trigger("AFTER").expect("trigger must survive");
        assert_eq!(
            tr.sql_definition.as_deref(),
            Some(verbatim),
            "verbatim CREATE TRIGGER text must round-trip byte-for-byte (v16)"
        );

        let tr = reloaded.catalog.get_trigger("tr_plain").expect("trigger must survive");
        assert_eq!(
            tr.sql_definition, None,
            "a trigger with no preserved text must round-trip as None"
        );
    }

    /// Issue #6174: a v15 file has no per-trigger `sql_definition` field, so the
    /// v16 reader must default it to `None` rather than mis-parsing the following
    /// bytes. A v15 record ends right after the schema field, so reading it back
    /// at version 15 must succeed and leave `sql_definition = None`.
    #[test]
    fn test_v15_trigger_loads_sql_definition_as_none() {
        let mut db = Database::new();
        let tr = vibesql_catalog::TriggerDefinition::new(
            "tr".to_string(),
            vibesql_ast::TriggerTiming::After,
            vibesql_ast::TriggerEvent::Insert,
            "t".to_string(),
            vibesql_ast::TriggerGranularity::Row,
            None,
            vibesql_ast::TriggerAction::RawSql("SELECT 1".to_string()),
        )
        .with_sql_definition(Some("CREATE TRIGGER tr AFTER INSERT ON t ...".to_string()));
        db.catalog.create_trigger(tr).unwrap();

        let mut buf = Vec::new();
        write_catalog(&mut buf, &db).unwrap();

        // Reading the same bytes at version 15 must stop before the trailing
        // sql_definition present-flag and default the field to None.
        let reloaded = read_catalog_v(&mut &buf[..], 15).unwrap();
        let tr = reloaded.catalog.get_trigger("tr").expect("trigger must survive");
        assert_eq!(tr.sql_definition, None, "a v15 reader must default sql_definition to None");
    }

    /// Issue #5940, Cluster A: a v13 file has no per-trigger schema field, so the
    /// v14 reader must default the trigger schema to `None` (a main-schema
    /// trigger) rather than mis-parsing the following bytes. Exercised by writing
    /// a schema-less trigger record by hand and reading it back at version 13.
    #[test]
    fn test_v13_trigger_loads_schema_as_none() {
        // Build a catalog with a single main-schema trigger, serialize it at the
        // current version, then read it back at version 13. Because the write
        // path for `tr.schema = None` emits a single `false` present-flag byte at
        // the end of the record, a v13 reader (which stops before that byte)
        // still parses the rest of the record correctly and defaults schema to
        // None — identical to what a real pre-v14 file would contain.
        let mut db = Database::new();
        let tr = vibesql_catalog::TriggerDefinition::new(
            "tr".to_string(),
            vibesql_ast::TriggerTiming::After,
            vibesql_ast::TriggerEvent::Insert,
            "t".to_string(),
            vibesql_ast::TriggerGranularity::Row,
            None,
            vibesql_ast::TriggerAction::RawSql("SELECT 1".to_string()),
        );
        db.catalog.create_trigger(tr).unwrap();

        let mut buf = Vec::new();
        write_catalog(&mut buf, &db).unwrap();

        // Read at version 13: the schema field is skipped, defaults to None.
        let reloaded = read_catalog_v(&mut &buf[..], 13).unwrap();
        let tr = reloaded.catalog.get_trigger("tr").expect("tr must load under v13 reader");
        assert_eq!(tr.schema, None, "a v13 file has no trigger-schema field; must default to None");
        assert!(!tr.is_temp());
    }
}
