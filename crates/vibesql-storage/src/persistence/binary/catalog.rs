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
            s != vibesql_catalog::DEFAULT_SCHEMA && !vibesql_catalog::Catalog::is_temp_schema(s)
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

    for table_name in &table_names {
        if let Some(table) = db.get_table(table_name) {
            write_string(writer, table_name)?;

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
        }
    }

    // Write indexes
    let index_names = db.list_indexes();
    write_u32(writer, index_names.len() as u32)?;

    for index_name in index_names {
        if let Some(metadata) = db.get_index(&index_name) {
            write_string(writer, &index_name)?;
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
    let view_names = db.catalog.list_views();
    write_u32(writer, view_names.len() as u32)?;

    for view_name in view_names {
        if let Some(view) = db.catalog.get_view(&view_name) {
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
    }

    // Write triggers
    let trigger_names = db.catalog.list_triggers();
    write_u32(writer, trigger_names.len() as u32)?;

    for trigger_name in trigger_names {
        if let Some(trigger) = db.catalog.get_trigger(&trigger_name) {
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
        }
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
        // Skip built-in schemas (main and all temp schemas) - they are already created by Database::new()
        // This handles backward compatibility with databases saved before the write-side fix
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

            columns.push(vibesql_catalog::ColumnSchema {
                name: col_name,
                data_type,
                nullable,
                default_value,
                generated_expr: None,
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

        table_schemas.push((table_name, columns, primary_key, quoted, sql_source));
    }

    // Create tables
    for (table_name, columns, primary_key, quoted, sql_source) in table_schemas {
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
                        // Column reference
                        vibesql_ast::IndexColumn::Column {
                            column_name: content,
                            direction,
                            prefix_length: None,
                        }
                    }
                    1 => {
                        // Expression index - parse the SQL expression
                        let expr =
                            vibesql_parser::arena_parser::parse_expression_to_owned(&content)
                                .map_err(|e| {
                                    StorageError::NotImplemented(format!(
                                        "Failed to parse expression index '{}': {}",
                                        content, e
                                    ))
                                })?;
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
                vibesql_ast::IndexColumn::Column { column_name, direction, prefix_length: None }
            };

            columns.push(index_column);
        }

        // v8+: read optional partial-index WHERE clause. v1-v7 files do not
        // include this field, so we treat the absence as "full index".
        let where_clause: Option<vibesql_ast::Expression> = if version >= 8 {
            let has_where = read_bool(reader)?;
            if has_where {
                let sql = read_string(reader)?;
                let parsed = vibesql_parser::arena_parser::parse_expression_to_owned(&sql)
                    .map_err(|e| {
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

            // 5. defining SELECT as SQL text — re-parse into a SelectStmt
            let query_sql = read_string(reader)?;
            let query =
                vibesql_parser::arena_parser::parse_select_to_owned(&query_sql).map_err(|e| {
                    StorageError::NotImplemented(format!(
                        "Failed to parse view '{}' SELECT '{}': {}",
                        name, query_sql, e
                    ))
                })?;

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

        // Create trigger definition
        let trigger = vibesql_catalog::TriggerDefinition::new(
            name,
            timing,
            event,
            table_name,
            granularity,
            when_condition,
            triggered_action,
        );

        // Add to catalog
        db.catalog.create_trigger(trigger).map_err(|e| {
            StorageError::NotImplemented(format!("Failed to create trigger: {}", e))
        })?;
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
                vibesql_ast::IndexColumn::Column { column_name, prefix_length, .. } => {
                    if let Some(prefix) = prefix_length {
                        vibesql_catalog::IndexedColumn::new_column_with_prefix(
                            column_name.clone(),
                            order,
                            *prefix,
                        )
                    } else {
                        vibesql_catalog::IndexedColumn::new_column(column_name.clone(), order)
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
        _ => Err(StorageError::NotImplemented(format!("Unsupported data type: {}", type_str))),
    }
}

#[cfg(test)]
mod tests {
    use super::super::format::VERSION;
    use super::{read_catalog_v, write_catalog};
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

        // Temp view.
        let v = reloaded.catalog.get_view("v_temp").expect("v_temp must survive");
        assert_eq!(v.schema.as_deref(), Some("temp"));
        assert!(v.is_temp());
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
}
