// ============================================================================
// SQL Dump Generation (Save Operations)
// ============================================================================
//
// Generates SQL statements that recreate the database state including:
// - Schemas
// - Tables with column definitions
// - Indexes
// - Data (INSERT statements)
// - Roles and privileges
//
// IMPORTANT: This module uses atomic writes to prevent database corruption.
// All writes go to a temporary file first, then are atomically renamed to the
// target path. This ensures that even if the process crashes mid-write, the
// original database file remains intact.

use std::{
    fs::{self, File},
    io::{BufWriter, Write},
    path::Path,
};

use crate::{Database, StorageError};

impl Database {
    /// Save database state as SQL dump (human-readable, portable)
    ///
    /// Generates SQL statements that recreate the database state including:
    /// - Schemas
    /// - Tables with column definitions
    /// - Indexes
    /// - Data (INSERT statements)
    /// - Roles and privileges
    ///
    /// # Atomicity
    ///
    /// This function uses atomic writes to prevent corruption:
    /// 1. Writes to a temporary file in the same directory
    /// 2. Flushes and syncs the buffer to ensure all data is on disk
    /// 3. Atomically renames the temp file to the target path
    ///
    /// This ensures the database file is never in a partial/corrupt state,
    /// even if the process crashes or is interrupted mid-write.
    ///
    /// # Example
    /// ```no_run
    /// # use vibesql_storage::Database;
    /// let db = Database::new();
    /// db.save_sql_dump("database.sql").unwrap();
    /// ```
    pub fn save_sql_dump<P: AsRef<Path>>(&self, path: P) -> Result<(), StorageError> {
        let path_ref = path.as_ref();

        // Create temp file in the same directory to ensure atomic rename works
        // (rename across filesystems would fail)
        let temp_path = {
            let parent = path_ref.parent().unwrap_or(Path::new("."));
            let file_name = path_ref.file_name().map(|s| s.to_string_lossy().to_string());
            let temp_name = format!(
                ".{}.tmp.{}",
                file_name.unwrap_or_else(|| "database".to_string()),
                std::process::id()
            );
            parent.join(temp_name)
        };

        // Write to temp file - clean up on error
        let result = self.write_sql_dump_to_file(&temp_path);
        if let Err(e) = &result {
            // Clean up temp file on error
            let _ = fs::remove_file(&temp_path);
            return Err(e.clone());
        }

        // Atomically rename temp file to target path
        fs::rename(&temp_path, path_ref).map_err(|e| {
            // Clean up temp file on rename failure
            let _ = fs::remove_file(&temp_path);
            StorageError::NotImplemented(format!("Failed to rename temp file to target: {}", e))
        })?;

        Ok(())
    }

    /// Internal helper to write the SQL dump to a file
    fn write_sql_dump_to_file(&self, path: &Path) -> Result<(), StorageError> {
        let file = File::create(path).map_err(|e| {
            StorageError::NotImplemented(format!("Failed to create temp file: {}", e))
        })?;

        let mut writer = BufWriter::new(file);

        // Header
        writeln!(writer, "-- VibeSQL Database Dump")
            .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
        writeln!(writer, "-- Generated: {}", chrono::Utc::now())
            .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
        writeln!(writer, "--")
            .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
        writeln!(writer)
            .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;

        // Export schemas (except built-in schemas which always exist)
        writeln!(writer, "-- Schemas")
            .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
        for schema_name in &self.catalog.list_schemas() {
            // Skip built-in schemas - they are recreated automatically on load
            // Skip default schema and all temp schemas (temp_1, temp_2, etc.)
            if schema_name != vibesql_catalog::DEFAULT_SCHEMA
                && !vibesql_catalog::Catalog::is_temp_schema(schema_name)
                // Attached schemas are session-scoped (#6310) and never persisted.
                && !self.catalog.is_attached_schema(schema_name)
            {
                writeln!(writer, "CREATE SCHEMA {};", schema_name)
                    .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            }
        }
        writeln!(writer)
            .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;

        // Export roles
        writeln!(writer, "-- Roles")
            .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
        for role_name in &self.catalog.list_roles() {
            writeln!(writer, "CREATE ROLE {};", role_name)
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
        }
        writeln!(writer)
            .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;

        // Export tables and data
        writeln!(writer, "-- Tables and Data")
            .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;

        // Iterate through all schemas except temp schemas
        // We must NOT use get_table() which follows shadowing rules - temp tables would
        // incorrectly override main schema tables in the dump.
        for schema_name in &self.catalog.list_schemas() {
            // Skip all temp schemas (temp_1, temp_2, etc.) and attached
            // schemas (#6310) - they are session-scoped
            if vibesql_catalog::Catalog::is_temp_schema(schema_name)
                || self.catalog.is_attached_schema(schema_name)
            {
                continue;
            }

            // Get tables directly from this schema
            let schema_tables = if let Some(schema) = self.catalog.get_schema(schema_name) {
                schema.list_tables()
            } else {
                continue;
            };

            for table_name in &schema_tables {
                // Use fully qualified name to bypass temp table shadowing
                let qualified_name = format!("{}.{}", schema_name, table_name);
                let Some(table) = self.tables.get(&qualified_name) else {
                    continue;
                };

                // Echo the table's *original* declared spelling, not the
                // lowercase canonical catalog key. SQLite preserves the original
                // case (including quoted reserved words like `"create"`) in
                // sqlite_master, and the dump must round-trip that spelling so a
                // reload reproduces the same `sqlite_master.name` (issue #5618).
                // `table.schema.name` retains the verbatim name; `table_name`
                // (from `schema.list_tables()`) is the case-folded key.
                let original_name = &table.schema.name;
                // For default schema, use unqualified name in output for cleaner SQL
                let output_name = if schema_name == vibesql_catalog::DEFAULT_SCHEMA {
                    original_name.clone()
                } else {
                    format!("{}.{}", schema_name, original_name)
                };
                // CREATE TABLE statement
                let schema = &table.schema;
                let quoted_output_name = quote_identifier(&output_name);

                // Emit the verbatim original CREATE TABLE text when we captured
                // it (issue #5619), so sqlite_master.sql keeps the user's exact
                // formatting across a .vbsql save/reload round-trip. The verbatim
                // text uses the unqualified, originally-spelled table name, so it
                // is only valid for the default schema; tables in other schemas
                // fall through to the qualified reconstruction below. The reload
                // path re-stamps sql_source from this emitted text.
                if schema_name == vibesql_catalog::DEFAULT_SCHEMA {
                    if let Some(src) = schema.sql_source.as_deref() {
                        writeln!(writer, "{};", src.trim_end_matches(';').trim_end()).map_err(
                            |e| StorageError::NotImplemented(format!("Write error: {}", e)),
                        )?;
                        write_table_data(&mut writer, table, schema, &quoted_output_name)?;
                        writeln!(writer).map_err(|e| {
                            StorageError::NotImplemented(format!("Write error: {}", e))
                        })?;
                        continue;
                    }
                }

                write_create_table_ddl(&mut writer, &quoted_output_name, schema)?;

                // INSERT statements for data (only live/non-deleted rows).
                // Shared with the verbatim-source path via write_table_data so
                // both emit identical data (scan_live skips deleted rows and
                // generated columns are excluded).
                write_table_data(&mut writer, table, schema, &quoted_output_name)?;

                writeln!(writer)
                    .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            }
        }

        // Export indexes (skip auto-generated indexes which are recreated by constraints)
        writeln!(writer, "-- Indexes")
            .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
        for index_name in self.list_indexes() {
            // Skip auto-generated indexes - these are automatically created by constraints:
            // - "pk_<table_name>" indexes are created by PRIMARY KEY constraints
            // - "sqlite_autoindex_<table>_<n>" indexes are created by PRIMARY KEY/UNIQUE
            //   constraints (follows SQLite naming convention for implicit indexes)
            // - the WITHOUT ROWID PK internal index (issue #5882) is regenerated from the CREATE
            //   TABLE DDL on reload, so it must not be dumped as a CREATE INDEX
            let lower_name = index_name.to_lowercase();
            if lower_name.starts_with("pk_")
                || lower_name.starts_with("sqlite_autoindex_")
                || lower_name.starts_with(vibesql_catalog::WITHOUT_ROWID_PK_INDEX_PREFIX)
            {
                continue;
            }
            let metadata = self.get_index(&index_name).unwrap();
            // Skip indexes on tables in attached schemas - session-scoped (#6310).
            // Filter on `metadata.schema` (the owning schema resolved at CREATE
            // INDEX time), not a qualifier embedded in `table_name`: an
            // unqualified `CREATE INDEX i1 ON t(z)` that resolves to an attached
            // table stores the bare `"t"` as table_name, so a name-prefix check
            // would leak the index into the dump.
            if self.catalog.is_attached_schema(&metadata.schema) {
                continue;
            }
            write!(writer, "CREATE")
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            if metadata.unique {
                write!(writer, " UNIQUE")
                    .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            }
            // Emit the index name with its original case (#5579). `index_name`
            // here is the IndexManager's *storage key*, which is normalized to
            // lowercase for case-insensitive lookup (see `make_index_key`).
            // `metadata.index_name` preserves the name exactly as the user wrote
            // it in `CREATE INDEX`, matching sqlite3 — whose `.dump` and
            // `sqlite_master.name`/`.sql` retain the original spelling while
            // still resolving names case-insensitively.
            //
            // Identifiers (index name, table name, column names) must be quoted so
            // that names with embedded special characters (spaces, quotes, parens)
            // round-trip correctly when the dump is re-lexed on reload. Without
            // quoting, a table named `t1'x1` produces an unterminated string
            // literal (`... ON t1'x1 (...`) and the reload aborts.
            write!(
                writer,
                " INDEX {} ON {} (",
                quote_identifier(&metadata.index_name),
                quote_identifier(&metadata.table_name)
            )
            .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;

            for (i, col) in metadata.columns.iter().enumerate() {
                if i > 0 {
                    write!(writer, ", ")
                        .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
                }
                // Handle both column references and expression indexes
                use vibesql_ast::IndexColumn;
                match col {
                    IndexColumn::Column { column_name, .. } => {
                        write!(writer, "{}", quote_identifier(column_name)).map_err(|e| {
                            StorageError::NotImplemented(format!("Write error: {}", e))
                        })?;
                    }
                    IndexColumn::Expression { expr, .. } => {
                        use vibesql_ast::pretty_print::ToSql;
                        write!(writer, "{}", expr.to_sql()).map_err(|e| {
                            StorageError::NotImplemented(format!("Write error: {}", e))
                        })?;
                    }
                }
                // Emit a valid SQL sort keyword. Only DESC needs to be written;
                // ASC is the default. The `{:?}` Debug form (`Asc`/`Desc`) is not
                // valid SQL and would fail to re-lex.
                use vibesql_ast::OrderDirection;
                if col.direction() == OrderDirection::Desc {
                    write!(writer, " DESC")
                        .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
                }
            }

            write!(writer, ")")
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;

            // Emit WHERE clause for partial indexes so they round-trip through
            // the SQL-dump persistence path. The catalog-side IndexMetadata
            // carries the predicate; the storage-side metadata does not, so
            // we look it up by name.
            if let Some(catalog_meta) = self.catalog.find_index_by_name(&index_name) {
                if let Some(where_expr) = catalog_meta.where_clause.as_deref() {
                    use vibesql_ast::pretty_print::ToSql;
                    write!(writer, " WHERE {}", where_expr.to_sql())
                        .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
                }
            }

            writeln!(writer, ";")
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
        }
        writeln!(writer)
            .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;

        // Export views
        writeln!(writer, "-- Views")
            .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
        // Iterate view definitions directly rather than via `list_views()` +
        // `get_view()`: views are keyed per schema (#6490), so a name-only
        // `get_view` always resolves to the same (temp-then-main-then-attached
        // priority) entry — iterating by name could write the same main-schema
        // view's SQL twice whenever an attached schema holds a same-named view.
        for view_def in self.catalog.iter_views() {
            // Skip temp views (`CREATE TEMP VIEW`): they are session-scoped
            // and must not survive into the next session via the SQL dump
            // (issue #5940, Cluster A). Views in attached schemas are
            // likewise session-scoped (#6310).
            if view_def.is_temp()
                || view_def.schema.as_deref().is_some_and(|s| self.catalog.is_attached_schema(s))
                // The schema may also be embedded in the stored name (a
                // legacy pre-#6490 snapshot stored the qualified name).
                || view_def.name.split_once('.').is_some_and(|(s, _)| self.catalog.is_attached_schema(s))
            {
                continue;
            }
            // Use stored SQL definition if available, otherwise create a minimal definition
            let sql = view_def.sql_definition.as_ref().map_or_else(
                || {
                    // Fallback: create a representation from the stored query
                    // This is a minimal representation and may not be fully accurate
                    format!("CREATE VIEW {} AS {:?}", view_def.name, view_def.query)
                },
                |s| s.clone(),
            );
            // Strip trailing semicolons before adding one
            let sql = sql.trim_end_matches(';').trim();
            writeln!(writer, "{};", sql)
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
        }
        writeln!(writer)
            .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;

        // Export triggers
        //
        // Triggers must be emitted *after* views so that INSTEAD OF triggers (which
        // require their target view to already exist) can be reconstructed when the
        // dump is reloaded. We can only round-trip triggers whose original SQL text
        // was preserved on creation; without that text we have no way to reproduce
        // the BEGIN ... END action body, so we skip with a comment instead of writing
        // garbage that would fail to parse on reload.
        writeln!(writer, "-- Triggers")
            .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
        // Iterate trigger definitions directly. Triggers are keyed per schema, so
        // a name-only `get_trigger` could return the temp namesake of a main
        // trigger; iterating definitions keeps every non-temp trigger visible.
        for trigger_def in self.catalog.iter_triggers() {
            // Skip temp triggers (`CREATE TEMP TRIGGER`): they are
            // session-scoped and must not survive into the next session via
            // the SQL dump (issue #5940, Cluster A). Triggers in attached
            // schemas are likewise session-scoped (#6310).
            if trigger_def.is_temp()
                || trigger_def.schema.as_deref().is_some_and(|s| self.catalog.is_attached_schema(s))
            {
                continue;
            }
            match trigger_def.sql_definition.as_ref() {
                Some(sql) => {
                    let sql = sql.trim_end_matches(';').trim();
                    writeln!(writer, "{};", sql)
                        .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
                }
                None => {
                    writeln!(
                        writer,
                        "-- Skipped trigger '{}' (no preserved SQL text)",
                        trigger_def.name
                    )
                    .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
                }
            }
        }
        writeln!(writer)
            .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;

        writeln!(writer, "-- End of dump")
            .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;

        // Flush the buffer and sync to disk to ensure data durability
        // This is critical for atomic writes - we need all data on disk before rename
        writer
            .flush()
            .map_err(|e| StorageError::NotImplemented(format!("Failed to flush buffer: {}", e)))?;

        // Get the underlying file to sync it to disk
        let file = writer
            .into_inner()
            .map_err(|e| StorageError::NotImplemented(format!("Failed to get file: {}", e)))?;
        file.sync_all()
            .map_err(|e| StorageError::NotImplemented(format!("Failed to sync file: {}", e)))?;

        Ok(())
    }
}

/// Emit a reconstructed `CREATE TABLE <quoted_output_name> (...);` statement
/// from a `TableSchema`'s columns and constraints.
///
/// Extracted from [`Database::write_sql_dump_to_file`]'s per-table loop so
/// [`Database::save_attached_schema_sql_dump`] (#6362) can reuse the exact
/// same column/constraint/FK formatting without duplicating it. Unlike the
/// whole-database dump, this reconstruction path is always used for an
/// attached schema's own file (never the `sql_source`-verbatim shortcut,
/// which — for a schema-qualified table like `aux.t` — would still contain
/// the `aux.` qualifier baked into the original CREATE TABLE text and fail to
/// reload as a standalone database).
///
/// For each column, determine whether it had NO declared type at all in the
/// original `CREATE TABLE` text (e.g. `CREATE TABLE t(a)` — as opposed to an
/// explicit `CREATE TABLE t(a BLOB)`, which also parses to
/// `DataType::BinaryLargeObject` and must NOT be treated as typeless).
/// Returns `Some(vec)` aligned 1:1 with `schema.columns` when
/// the check could be performed, `None` when it could not (see below) — in
/// either case, `write_create_table_ddl` treats an unknown/absent entry as
/// "type declared", which reproduces the exact behavior this function is
/// replacing.
///
/// `TableSchema::sql_source` is the right thing to re-parse here — NOT the
/// same "verbatim shortcut" this function's own doc comment says
/// `write_create_table_ddl` deliberately avoids: that avoidance is about
/// output-side table-name qualification (`aux.` baked into the emitted
/// `CREATE TABLE` line and, separately, `sql_source` going stale after an
/// `ALTER TABLE ADD/DROP/RENAME COLUMN`), not about the *type text* of an
/// individual column that still exists. `sql_source` is kept in lockstep
/// with the live column set by every ALTER TABLE path
/// (`update_sql_source_after_alter` et al. in
/// `vibesql-executor/src/alter/mod.rs`): it is rewritten in place when the
/// edit can be applied to it, or invalidated to `None` when it can't. So a
/// present `sql_source` is exactly as trustworthy here as it already is for
/// the STRICT/rowid-alias/AUTOINCREMENT rehydration in
/// `vibesql-storage/src/persistence/binary/constraints.rs`, which relies on
/// the identical invariant. When `sql_source` is `None` (never captured, or
/// invalidated by an ALTER this function can't safely reconstruct from) or
/// fails to re-parse as a bare `CREATE TABLE`, every column conservatively
/// falls back to "type declared" — the original (typeless-losing) behavior —
/// rather than guessing.
fn typeless_columns_from_sql_source(schema: &vibesql_catalog::TableSchema) -> Option<Vec<bool>> {
    let src = schema.sql_source.as_deref()?;
    let stmt = vibesql_parser::Parser::parse_sql(src).ok()?;
    let create = match stmt {
        vibesql_ast::Statement::CreateTable(create) => create,
        _ => return None,
    };
    // CREATE TABLE ... AS SELECT carries no column type declarations at all;
    // every column type there is inferred, never "typeless" in this sense.
    if create.as_query.is_some() {
        return None;
    }
    Some(
        schema
            .columns
            .iter()
            .map(|col| {
                create
                    .columns
                    .iter()
                    .find(|c| c.name.eq_ignore_ascii_case(&col.name))
                    .map(|c| c.type_source.is_none())
                    .unwrap_or(false)
            })
            .collect(),
    )
}

fn write_create_table_ddl<W: Write>(
    writer: &mut W,
    quoted_output_name: &str,
    schema: &vibesql_catalog::TableSchema,
) -> Result<(), StorageError> {
    // Which columns had NO declared type at all in the original `CREATE
    // TABLE` (e.g. `CREATE TABLE t(a)`), so their type token can be omitted
    // below instead of defaulting to a literal "BLOB" (issue #6481). `None`
    // when this can't be determined (no captured `sql_source`, or it fails
    // to re-parse) — every column then falls back to always emitting a type,
    // the pre-#6481 behavior.
    let typeless_columns = typeless_columns_from_sql_source(schema);

    write!(writer, "CREATE TABLE {} (", quoted_output_name)
        .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;

    for (i, col) in schema.columns.iter().enumerate() {
        if i > 0 {
            write!(writer, ", ")
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
        }
        let is_typeless =
            typeless_columns.as_ref().and_then(|v| v.get(i)).copied().unwrap_or(false);
        if is_typeless {
            // No declared type: emit just the column name, matching SQLite's
            // (and this engine's own same-session/main-schema) rendering of
            // a typeless column, e.g. `CREATE TABLE t2 (d, e, f)`.
            write!(writer, "{}", quote_identifier(&col.name))
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
        } else {
            // Format column type, preserving INT vs INTEGER distinction for rowid alias behavior
            let type_str = format_column_type(&col.data_type, col.is_exact_integer_type);
            write!(writer, "{} {}", quote_identifier(&col.name), type_str)
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
        }

        // Handle generated columns (AS expression syntax)
        if let Some(ref generated_expr) = col.generated_expr {
            use vibesql_ast::pretty_print::ToSql;
            write!(writer, " AS ({})", generated_expr.to_sql())
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
        } else {
            // Only non-generated columns can have DEFAULT, COLLATE, NOT NULL
            // Add DEFAULT clause if present
            if let Some(ref default_expr) = col.default_value {
                use vibesql_ast::pretty_print::ToSql;
                write!(writer, " DEFAULT {}", default_expr.to_sql())
                    .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            }
            // Add COLLATE clause if present
            if let Some(ref collation) = col.collation {
                write!(writer, " COLLATE {}", collation)
                    .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            }
            if !col.nullable {
                write!(writer, " NOT NULL")
                    .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            }
        }
    }

    // Add PRIMARY KEY constraint if present
    if let Some(pk_cols) = &schema.primary_key {
        let quoted_pk: Vec<String> = pk_cols.iter().map(|c| quote_identifier(c)).collect();
        write!(writer, ", PRIMARY KEY ({})", quoted_pk.join(", "))
            .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
    }

    // Add UNIQUE constraints
    for unique_cols in &schema.unique_constraints {
        let quoted_uniq: Vec<String> = unique_cols.iter().map(|c| quote_identifier(c)).collect();
        write!(writer, ", UNIQUE ({})", quoted_uniq.join(", "))
            .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
    }

    // Add CHECK constraints
    // We always output CONSTRAINT <name> CHECK (<expr>) to preserve user-provided names.
    // For unnamed constraints, the name equals the expression text, which will be
    // re-derived when the table is reloaded.
    for (constraint_name, check_expr) in &schema.check_constraints {
        use vibesql_ast::pretty_print::ToSql;
        let expr_text = check_expr.to_sql();
        // An unnamed CHECK's stored "name" is the verbatim source
        // text of its expression (whitespace preserved), so it
        // differs from `to_sql()` only by operator spacing (e.g.
        // `d > 0` vs `d>0`). Compare with all whitespace removed to
        // recognize that case; otherwise a spaced source form would
        // be misread as a user-provided name and emitted as a bogus
        // `CONSTRAINT d > 0` identifier.
        let strip_ws = |s: &str| s.split_whitespace().collect::<String>();
        if strip_ws(constraint_name) != strip_ws(&expr_text) {
            // User-provided name: emit it with the re-rendered expr.
            write!(writer, ", CONSTRAINT {} CHECK ({})", constraint_name, expr_text)
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
        } else {
            // Unnamed: emit the verbatim source text so the reloaded
            // constraint's violation message round-trips byte-exact.
            write!(writer, ", CHECK ({})", constraint_name)
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
        }
    }

    // Add FOREIGN KEY constraints
    for fk in &schema.foreign_keys {
        let fk_cols: Vec<String> = fk.column_names.iter().map(|c| quote_identifier(c)).collect();

        // Filter out empty parent column names (unresolved references)
        let non_empty_parent_cols: Vec<String> = fk
            .parent_column_names
            .iter()
            .filter(|c| !c.is_empty())
            .map(|c| quote_identifier(c))
            .collect();

        if non_empty_parent_cols.is_empty() {
            // No resolved parent columns - omit column list (defaults to PK)
            write!(
                writer,
                ", FOREIGN KEY ({}) REFERENCES {}",
                fk_cols.join(", "),
                quote_identifier(&fk.parent_table)
            )
            .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
        } else {
            write!(
                writer,
                ", FOREIGN KEY ({}) REFERENCES {} ({})",
                fk_cols.join(", "),
                quote_identifier(&fk.parent_table),
                non_empty_parent_cols.join(", ")
            )
            .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
        }

        // Add ON DELETE clause if not NO ACTION
        match &fk.on_delete {
            vibesql_catalog::ReferentialAction::NoAction => {}
            vibesql_catalog::ReferentialAction::Cascade => {
                write!(writer, " ON DELETE CASCADE")
                    .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            }
            vibesql_catalog::ReferentialAction::SetNull => {
                write!(writer, " ON DELETE SET NULL")
                    .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            }
            vibesql_catalog::ReferentialAction::SetDefault => {
                write!(writer, " ON DELETE SET DEFAULT")
                    .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            }
            vibesql_catalog::ReferentialAction::Restrict => {
                write!(writer, " ON DELETE RESTRICT")
                    .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            }
        }

        // Add ON UPDATE clause if not NO ACTION
        match &fk.on_update {
            vibesql_catalog::ReferentialAction::NoAction => {}
            vibesql_catalog::ReferentialAction::Cascade => {
                write!(writer, " ON UPDATE CASCADE")
                    .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            }
            vibesql_catalog::ReferentialAction::SetNull => {
                write!(writer, " ON UPDATE SET NULL")
                    .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            }
            vibesql_catalog::ReferentialAction::SetDefault => {
                write!(writer, " ON UPDATE SET DEFAULT")
                    .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            }
            vibesql_catalog::ReferentialAction::Restrict => {
                write!(writer, " ON UPDATE RESTRICT")
                    .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            }
        }

        // Emit DEFERRABLE clause so deferred-FK semantics survive
        // a persistence round-trip. Without this, `.vbsql` dump
        // and reload would lose `INITIALLY DEFERRED` and the TCL
        // shim's batched-process model would degrade fkey6 tests
        // back to immediate enforcement.
        if fk.is_deferrable {
            if fk.initially_deferred {
                write!(writer, " DEFERRABLE INITIALLY DEFERRED")
                    .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            } else {
                write!(writer, " DEFERRABLE INITIALLY IMMEDIATE")
                    .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            }
        }
    }

    // Close the column definitions
    write!(writer, ")").map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;

    // Add WITHOUT ROWID / STRICT clauses for SQLite compatibility
    // (Issue #4803, #5837). SQLite accepts both together, comma-
    // separated: `) WITHOUT ROWID, STRICT`.
    if schema.without_rowid {
        write!(writer, " WITHOUT ROWID")
            .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
    }
    if schema.strict {
        let sep = if schema.without_rowid { "," } else { "" };
        write!(writer, "{} STRICT", sep)
            .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
    }

    writeln!(writer, ";")
        .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;

    Ok(())
}

impl Database {
    /// Persist an attached schema's tables (definitions + live row data),
    /// indexes, views, and triggers to its own file, in the
    /// reconstructed-DDL SQL-dump format (#6362 Phase 2, #6407).
    ///
    /// Attached-database persistence is deliberately snapshot-only regardless
    /// of whether the *main* database is WAL-active (see the ATTACH DATABASE
    /// issue's Scope note): each attachment gets its own independent
    /// `.vbsql`-style dump, written and reloaded through the same SQL-dump
    /// machinery as a normal snapshot-only database. The written file is a
    /// standalone, self-contained database — when re-loaded (e.g. by a later
    /// `ATTACH` of the same path) its tables/indexes/views/triggers land in
    /// the loader's own default schema, exactly like any other `.vbsql`
    /// file; the caller is responsible for re-homing them into the
    /// attachment's schema name in the live session.
    ///
    /// Views and triggers defined inside this schema embed the attachment's
    /// schema qualifier in their captured SQL text (e.g. a `CREATE VIEW
    /// aux.v1 AS SELECT x FROM aux.t` captured while `aux` was the
    /// attachment's name). Since the on-disk file is standalone (reloaded
    /// into an ordinary default schema), that qualifier is stripped via
    /// [`strip_schema_qualifier`] before being written, so the dump is
    /// schema-relative and reloads cleanly regardless of what alias a future
    /// session attaches this same file under (issue #6407). Only the exact
    /// unquoted `schema_name.` qualifier is stripped — everything else in the
    /// captured text (including references to *other* schemas) is left
    /// untouched.
    ///
    /// Index metadata (including partial `WHERE` predicates and expression
    /// columns) is likewise persisted; table names inside `CREATE INDEX` are
    /// reconstructed from structured metadata (not verbatim text) and are
    /// already schema-relative, mirroring [`write_create_table_ddl`].
    pub fn save_attached_schema_sql_dump<P: AsRef<Path>>(
        &self,
        schema_name: &str,
        path: P,
    ) -> Result<(), StorageError> {
        let path_ref = path.as_ref();

        let temp_path = {
            let parent = path_ref.parent().unwrap_or(Path::new("."));
            let file_name = path_ref.file_name().map(|s| s.to_string_lossy().to_string());
            let temp_name = format!(
                ".{}.tmp.{}",
                file_name.unwrap_or_else(|| "database".to_string()),
                std::process::id()
            );
            parent.join(temp_name)
        };

        let result = self.write_attached_schema_dump_to_file(schema_name, &temp_path);
        if let Err(e) = &result {
            let _ = fs::remove_file(&temp_path);
            return Err(e.clone());
        }

        fs::rename(&temp_path, path_ref).map_err(|e| {
            let _ = fs::remove_file(&temp_path);
            StorageError::NotImplemented(format!("Failed to rename temp file to target: {}", e))
        })?;

        Ok(())
    }

    fn write_attached_schema_dump_to_file(
        &self,
        schema_name: &str,
        path: &Path,
    ) -> Result<(), StorageError> {
        let file = File::create(path).map_err(|e| {
            StorageError::NotImplemented(format!("Failed to create temp file: {}", e))
        })?;
        let mut writer = BufWriter::new(file);

        writeln!(writer, "-- VibeSQL Attached Database Dump")
            .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
        writeln!(writer, "-- Generated: {}", chrono::Utc::now())
            .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
        writeln!(writer)
            .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;

        let table_names = self
            .catalog
            .get_schema(schema_name)
            .map(|schema| schema.list_tables())
            .unwrap_or_default();

        for table_name in &table_names {
            let Some(table_schema) =
                self.catalog.get_schema(schema_name).and_then(|s| s.get_table(table_name, true))
            else {
                continue;
            };
            let qualified_name = format!("{}.{}", schema_name, table_name);
            let Some(table) = self.tables.get(&qualified_name) else {
                continue;
            };

            // The output file is standalone (reloaded as an ordinary
            // database), so table names are always unqualified — the
            // attachment schema name is not part of the on-disk DDL.
            let original_name = &table_schema.name;
            let quoted_output_name = quote_identifier(original_name);

            write_create_table_ddl(&mut writer, &quoted_output_name, table_schema)?;
            write_table_data(&mut writer, table, table_schema, &quoted_output_name)?;
            writeln!(writer)
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
        }

        // Indexes owned by this attached schema (#6407). Mirrors the main
        // dump's index-emission loop (`write_sql_dump_to_file`), but instead
        // of *skipping* attached-schema indexes, this is the attached-schema
        // dump itself, so we emit exactly the indexes whose owning schema
        // matches `schema_name`. Table names are reconstructed from
        // structured metadata (`metadata.table_name`, always bare for an
        // index — see `CreateIndexExecutor`), so they are already
        // schema-relative and need no qualifier stripping.
        writeln!(writer, "-- Indexes")
            .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
        for index_key in self.list_indexes() {
            // Resolve the metadata FIRST and filter on `metadata.index_name`,
            // never on the iterated value. `IndexManager::list_indexes()`
            // yields storage *map keys*, and `make_index_key` prefixes every
            // non-`main` schema onto the key — an attached schema's
            // auto-generated index is keyed `aux.sqlite_autoindex_t_1`, so a
            // `starts_with("sqlite_autoindex_")` test against the key is
            // always false for exactly the indexes this filter exists to
            // exclude. The main dump's loop (`write_sql_dump_to_file`) gets
            // away with testing the key because main-schema keys are bare;
            // that stops holding the moment the same loop is pointed at a
            // non-`main` schema. Emitting an auto-index here is not cosmetic:
            // `CREATE TABLE` already recreates it on reload, and the reserved
            // `sqlite_autoindex_*` / WITHOUT ROWID PK name makes the replayed
            // statement a hard error ("object name reserved for internal
            // use"), so the attachment persists fine and can then never be
            // re-ATTACHed.
            let Some(metadata) = self.get_index(&index_key) else { continue };
            if !metadata.schema.eq_ignore_ascii_case(schema_name) {
                continue;
            }
            // Skip auto-generated indexes - these are automatically created by constraints:
            // - "pk_<table_name>" indexes are created by PRIMARY KEY constraints
            // - "sqlite_autoindex_<table>_<n>" indexes are created by PRIMARY KEY/UNIQUE
            //   constraints (follows SQLite naming convention for implicit indexes)
            // - the WITHOUT ROWID PK internal index (issue #5882) is regenerated from the CREATE
            //   TABLE DDL on reload, so it must not be dumped as a CREATE INDEX
            let lower_name = metadata.index_name.to_lowercase();
            if lower_name.starts_with("pk_")
                || lower_name.starts_with("sqlite_autoindex_")
                || lower_name.starts_with(vibesql_catalog::WITHOUT_ROWID_PK_INDEX_PREFIX)
            {
                continue;
            }

            write!(writer, "CREATE")
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            if metadata.unique {
                write!(writer, " UNIQUE")
                    .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            }
            write!(
                writer,
                " INDEX {} ON {} (",
                quote_identifier(&metadata.index_name),
                quote_identifier(&metadata.table_name)
            )
            .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;

            for (i, col) in metadata.columns.iter().enumerate() {
                if i > 0 {
                    write!(writer, ", ")
                        .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
                }
                use vibesql_ast::IndexColumn;
                match col {
                    IndexColumn::Column { column_name, .. } => {
                        write!(writer, "{}", quote_identifier(column_name)).map_err(|e| {
                            StorageError::NotImplemented(format!("Write error: {}", e))
                        })?;
                    }
                    IndexColumn::Expression { expr, .. } => {
                        use vibesql_ast::pretty_print::ToSql;
                        let expr_sql = strip_schema_qualifier(&expr.to_sql(), schema_name);
                        write!(writer, "{}", expr_sql).map_err(|e| {
                            StorageError::NotImplemented(format!("Write error: {}", e))
                        })?;
                    }
                }
                use vibesql_ast::OrderDirection;
                if col.direction() == OrderDirection::Desc {
                    write!(writer, " DESC")
                        .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
                }
            }

            write!(writer, ")")
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;

            // Look up the catalog entry via the schema-qualified table name
            // (`Catalog::get_index`, exact `schema.table.index` targeting)
            // rather than `find_index_by_name` (bare-name-only, first match
            // across every schema): an attached schema could otherwise
            // collide with a same-named index elsewhere and silently pull
            // the wrong WHERE predicate. This also sidesteps the storage
            // `IndexManager`'s schema-prefixed map key (`make_index_key`),
            // which never matches the catalog's bare `index.name`.
            let qualified_table = format!("{}.{}", schema_name, metadata.table_name);
            if let Some(catalog_meta) =
                self.catalog.get_index(&qualified_table, &metadata.index_name)
            {
                if let Some(where_expr) = catalog_meta.where_clause.as_deref() {
                    use vibesql_ast::pretty_print::ToSql;
                    let where_sql = strip_schema_qualifier(&where_expr.to_sql(), schema_name);
                    write!(writer, " WHERE {}", where_sql)
                        .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
                }
            }

            writeln!(writer, ";")
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
        }
        writeln!(writer)
            .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;

        // Views owned by this attached schema (#6407). A view created as
        // `CREATE VIEW aux.v1 AS SELECT x FROM aux.t` is homed in the `aux`
        // schema (`ViewDefinition::schema`, since #6490) with a bare
        // (unqualified) `ViewDefinition::name`; its captured `sql_definition`
        // text still embeds the qualifier throughout (name and any table
        // references), so that is stripped below to make the emitted
        // statement schema-relative and standalone-loadable. Iterate view
        // definitions directly rather than via `list_views()` + `get_view()`:
        // a name-only `get_view` always resolves to the same
        // (temp-then-main-then-attached priority) entry, so iterating by name
        // would silently skip every non-`main` schema's same-named view
        // (issue #6296's trigger-listing bug, applied to views).
        writeln!(writer, "-- Views")
            .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
        for view_def in self.catalog.iter_views() {
            if view_def.is_temp() {
                continue;
            }
            let owned_by_schema = view_def.schema.as_deref().is_some_and(|s| s.eq_ignore_ascii_case(schema_name))
                    // A legacy pre-#6490 snapshot may still carry the schema
                    // embedded in the stored name.
                    || view_def
                        .name
                        .split_once('.')
                        .is_some_and(|(s, _)| s.eq_ignore_ascii_case(schema_name));
            if !owned_by_schema {
                continue;
            }

            let sql = view_def.sql_definition.as_ref().map_or_else(
                || {
                    let bare_name =
                        view_def.name.split_once('.').map(|(_, n)| n).unwrap_or(&view_def.name);
                    format!("CREATE VIEW {} AS {:?}", bare_name, view_def.query)
                },
                |s| s.clone(),
            );
            let sql = strip_schema_qualifier(sql.trim_end_matches(';').trim(), schema_name);
            writeln!(writer, "{};", sql)
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
        }
        writeln!(writer)
            .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;

        // Triggers owned by this attached schema (#6407). Only triggers with
        // preserved SQL text can round-trip (same limitation as the main
        // dump); the trigger's `BEGIN ... END` action body is stored as raw
        // SQL text (`TriggerAction::RawSql`), so the same qualifier-stripping
        // pass covers both the header (`ON aux.t`) and the body
        // (`INSERT INTO aux.t2 ...`).
        writeln!(writer, "-- Triggers")
            .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
        for trigger_def in self.catalog.iter_triggers() {
            if trigger_def.is_temp() {
                continue;
            }
            let owned_by_schema =
                trigger_def.schema.as_deref().is_some_and(|s| s.eq_ignore_ascii_case(schema_name));
            if !owned_by_schema {
                continue;
            }

            match trigger_def.sql_definition.as_ref() {
                Some(sql) => {
                    let sql = strip_schema_qualifier(sql.trim_end_matches(';').trim(), schema_name);
                    writeln!(writer, "{};", sql)
                        .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
                }
                None => {
                    writeln!(
                        writer,
                        "-- Skipped trigger '{}' (no preserved SQL text)",
                        trigger_def.name
                    )
                    .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
                }
            }
        }
        writeln!(writer)
            .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;

        writer
            .flush()
            .map_err(|e| StorageError::NotImplemented(format!("Failed to flush buffer: {}", e)))?;
        let file = writer
            .into_inner()
            .map_err(|e| StorageError::NotImplemented(format!("Failed to get file: {}", e)))?;
        file.sync_all()
            .map_err(|e| StorageError::NotImplemented(format!("Failed to sync file: {}", e)))?;

        Ok(())
    }
}

/// Emit `INSERT INTO ...` statements for a table's live (non-deleted) rows.
///
/// Shared by the verbatim and reconstructed CREATE TABLE paths so that a table
/// whose original source text is preserved (issue #5619) still dumps its data
/// identically. Generated columns are skipped — they cannot be inserted
/// directly — and only live rows (via `scan_live`) are written.
fn write_table_data<W: Write>(
    writer: &mut W,
    table: &crate::Table,
    schema: &vibesql_catalog::TableSchema,
    quoted_output_name: &str,
) -> Result<(), StorageError> {
    if table.row_count() == 0 {
        return Ok(());
    }

    writeln!(writer).map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;

    // Find indices of non-generated columns.
    let non_generated_indices: Vec<usize> = schema
        .columns
        .iter()
        .enumerate()
        .filter(|(_, col)| col.generated_expr.is_none())
        .map(|(i, _)| i)
        .collect();

    // Build column list for INSERT only when generated columns are present.
    let has_generated_columns = non_generated_indices.len() < schema.columns.len();
    let column_list: String = if has_generated_columns {
        let col_names: Vec<&str> =
            non_generated_indices.iter().map(|&i| schema.columns[i].name.as_str()).collect();
        format!(" ({})", col_names.join(", "))
    } else {
        String::new()
    };

    for (_idx, row) in table.scan_live() {
        write!(writer, "INSERT INTO {}{} VALUES (", quoted_output_name, column_list)
            .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;

        let mut first = true;
        for &col_idx in &non_generated_indices {
            if !first {
                write!(writer, ", ")
                    .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            }
            first = false;
            write!(writer, "{}", sql_value_to_literal(&row.values[col_idx]))
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
        }
        writeln!(writer, ");")
            .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
    }

    Ok(())
}

/// Strip an unquoted `<schema_name>.` qualifier from every position it
/// appears as a whole-identifier prefix in `sql`, leaving everything else
/// untouched (issue #6407).
///
/// Used when persisting an attached schema's views/triggers/index
/// expressions to their own standalone file: captured SQL text for an object
/// created as `CREATE VIEW aux.v1 AS SELECT x FROM aux.t` embeds the
/// attachment's schema name (`aux`) throughout, which cannot be replayed
/// into a fresh default-schema reload target without stripping it first. The
/// caller re-adds the (possibly different) schema name the file is attached
/// under in the *new* session, so only the exact qualifier used at save time
/// needs removing here — everything else in the text (including references
/// to *other* schemas) round-trips byte-for-byte.
///
/// This is a lexical, quote-aware scan rather than a full parse: it tracks
/// single-quoted string literals (`'...'`, with `''`-doubling), double-quoted
/// identifiers (`"..."`, with `""`-doubling), backtick-quoted identifiers,
/// and bracket-quoted identifiers (`[...]`) so a qualifier-shaped substring
/// inside a literal or a quoted identifier is never touched. A match must be
/// a whole identifier (anchored on a word boundary) immediately followed by
/// `.`; comparison is ASCII case-insensitive, matching SQL identifier
/// semantics.
/// Known limitations, none of which affect any path this function is used
/// from (all three need SQL text this engine's own DDL reconstruction never
/// produces):
///   * A schema name written pre-quoted (e.g. `"aux".v1`) is not recognized — attachment schema
///     names are ordinary identifiers and are never written quoted here.
///   * `--` and `/* */` comments are not treated as spans, so a qualifier mentioned *inside a
///     comment* in the captured SQL is rewritten like any other occurrence. Cosmetic only: the
///     comment text changes, the statement's meaning does not.
///   * A **table alias** that happens to equal the schema name is stripped along with real
///     qualifiers: `SELECT aux.x FROM t AS aux` becomes `SELECT x FROM t AS aux`. Harmless for a
///     single-table query (the column still resolves), but it can make a join's column reference
///     ambiguous. Pinned by `strip_schema_qualifier_also_strips_an_alias_matching_the_schema_name`
///     so the behavior is at least known rather than accidental.
fn strip_schema_qualifier(sql: &str, schema_name: &str) -> String {
    // Byte-oriented scan, but every byte that is *emitted* is emitted as part
    // of a `&str` slice of the original input — never as `byte as char`, which
    // would decode UTF-8 continuation bytes as Latin-1 and mojibake any
    // non-ASCII text in the captured SQL (a string literal, a Unicode
    // identifier, a comment). This is safe because no ASCII byte ever appears
    // inside a multi-byte UTF-8 sequence, so an ASCII delimiter match can
    // never land mid-character.
    let bytes = sql.as_bytes();
    let mut out = String::with_capacity(sql.len());
    let mut i = 0usize;

    while i < bytes.len() {
        let b = bytes[i];

        // Copy quoted spans verbatim (never treat their contents as an
        // identifier to match against).
        if matches!(b, b'\'' | b'"' | b'`' | b'[') {
            let (close, doubled) = match b {
                b'\'' => (b'\'', true),
                b'"' => (b'"', true),
                b'`' => (b'`', false),
                _ => (b']', false),
            };
            let start = i;
            i += 1;
            while i < bytes.len() {
                if bytes[i] == close {
                    // A doubled closing delimiter (`''` / `""`) is an escaped
                    // literal delimiter, not the end of the span.
                    if doubled && i + 1 < bytes.len() && bytes[i + 1] == close {
                        i += 2;
                        continue;
                    }
                    i += 1;
                    break;
                }
                i += 1;
            }
            out.push_str(&sql[start..i]);
            continue;
        }

        if b.is_ascii_alphabetic() || b == b'_' {
            let start = i;
            let mut j = i;
            while j < bytes.len() && (bytes[j].is_ascii_alphanumeric() || bytes[j] == b'_') {
                j += 1;
            }
            let ident = &sql[start..j];
            // The preceding byte must be an ASCII non-word byte. Requiring
            // ASCII (rather than merely "not an ASCII word byte") keeps a
            // non-ASCII identifier character — e.g. the `é` in `caféaux.t` —
            // from being mistaken for a word boundary.
            let boundary_ok = start == 0 || {
                let prev = bytes[start - 1];
                prev.is_ascii() && !(prev.is_ascii_alphanumeric() || prev == b'_')
            };
            if boundary_ok
                && j < bytes.len()
                && bytes[j] == b'.'
                && ident.eq_ignore_ascii_case(schema_name)
            {
                // Drop the identifier and the following '.' — the qualifier
                // is elided entirely.
                i = j + 1;
                continue;
            }
            out.push_str(ident);
            i = j;
            continue;
        }

        // Any other byte: copy the whole UTF-8 character it starts, verbatim.
        let mut end = i + 1;
        while end < bytes.len() && (bytes[end] & 0xC0) == 0x80 {
            end += 1;
        }
        out.push_str(&sql[i..end]);
        i = end;
    }

    out
}

/// Quote an identifier if it contains special characters or starts with a digit.
/// Uses double-quote style quoting (SQL standard / SQLite).
fn quote_identifier(name: &str) -> String {
    // Check if the identifier needs quoting:
    // - starts with a digit
    // - contains non-alphanumeric characters (except underscore)
    // - is empty
    let needs_quoting = name.is_empty()
        || name.starts_with(|c: char| c.is_ascii_digit())
        || !name.chars().all(|c| c.is_alphanumeric() || c == '_')
        // A keyword-named identifier (e.g. `create`, `select`) must be quoted so
        // a reload re-lexes it as an identifier rather than the keyword token,
        // preserving the original spelling/case in sqlite_master (issue #5618).
        || vibesql_parser::is_keyword(name);

    if needs_quoting {
        // Escape any embedded double-quotes by doubling them
        format!("\"{}\"", name.replace('"', "\"\""))
    } else {
        name.to_string()
    }
}

/// Format a column type, preserving the INT vs INTEGER distinction for rowid alias behavior.
/// In SQLite, only `INTEGER PRIMARY KEY` is a rowid alias, not `INT PRIMARY KEY`.
fn format_column_type(data_type: &vibesql_types::DataType, is_exact_integer_type: bool) -> String {
    use vibesql_types::DataType;

    match data_type {
        DataType::Integer => {
            if is_exact_integer_type {
                "INTEGER".to_string()
            } else {
                "INT".to_string()
            }
        }
        _ => format_data_type(data_type),
    }
}

pub(super) fn format_data_type(data_type: &vibesql_types::DataType) -> String {
    use vibesql_types::DataType;

    match data_type {
        DataType::Integer => "INTEGER".to_string(),
        DataType::Smallint => "SMALLINT".to_string(),
        DataType::Bigint => "BIGINT".to_string(),
        DataType::Unsigned => "BIGINT UNSIGNED".to_string(),
        DataType::Float { precision } => format!("FLOAT({})", precision),
        DataType::Real => "REAL".to_string(),
        DataType::DoublePrecision => "DOUBLE PRECISION".to_string(),
        DataType::Varchar { max_length } => {
            if let Some(len) = max_length {
                format!("VARCHAR({})", len)
            } else {
                "VARCHAR".to_string()
            }
        }
        DataType::Character { length } => format!("CHAR({})", length),
        DataType::Boolean => "BOOLEAN".to_string(),
        DataType::Date => "DATE".to_string(),
        DataType::Time { .. } => "TIME".to_string(),
        DataType::Timestamp { with_timezone } => {
            if *with_timezone {
                "TIMESTAMP WITH TIME ZONE".to_string()
            } else {
                "TIMESTAMP".to_string()
            }
        }
        DataType::Interval { start_field, end_field: _ } => {
            // Simplified interval representation for now
            format!("INTERVAL {:?}", start_field)
        }
        DataType::Numeric { precision, scale } => {
            format!("NUMERIC({}, {})", precision, scale)
        }
        DataType::Decimal { precision, scale } => {
            format!("DECIMAL({}, {})", precision, scale)
        }
        DataType::CharacterLargeObject => "CLOB".to_string(),
        DataType::Name => "VARCHAR(128)".to_string(),
        DataType::BinaryLargeObject => "BLOB".to_string(),
        DataType::Bit { length } => {
            if let Some(len) = length {
                format!("BIT({})", len)
            } else {
                "BIT".to_string()
            }
        }
        DataType::Vector { dimensions } => format!("VECTOR({})", dimensions),
        DataType::UserDefined { type_name } => type_name.clone(),
        DataType::Null => "NULL".to_string(),
    }
}

/// Convert a SqlValue to its SQL literal representation
pub(super) fn sql_value_to_literal(value: &vibesql_types::SqlValue) -> String {
    use vibesql_types::SqlValue;

    match value {
        SqlValue::Null => "NULL".to_string(),
        SqlValue::Integer(n) => n.to_string(),
        SqlValue::Smallint(n) => n.to_string(),
        SqlValue::Bigint(n) => n.to_string(),
        SqlValue::Unsigned(n) => n.to_string(),
        SqlValue::Numeric(f) => {
            // Handle special float values that would be parsed as identifiers
            if f.is_nan() {
                "'NaN'".to_string()
            } else if f.is_infinite() {
                if f.is_sign_positive() {
                    "'Infinity'".to_string()
                } else {
                    "'-Infinity'".to_string()
                }
            } else {
                format_f64_for_sql(*f)
            }
        }
        SqlValue::Float(f) => {
            if f.is_nan() {
                "'NaN'".to_string()
            } else if f.is_infinite() {
                if f.is_sign_positive() {
                    "'Infinity'".to_string()
                } else {
                    "'-Infinity'".to_string()
                }
            } else {
                format_f32_for_sql(*f)
            }
        }
        SqlValue::Real(f) => {
            // Real is now f64 (SQLite REAL is 8-byte IEEE float)
            if f.is_nan() {
                "'NaN'".to_string()
            } else if f.is_infinite() {
                if f.is_sign_positive() {
                    "'Infinity'".to_string()
                } else {
                    "'-Infinity'".to_string()
                }
            } else {
                format_f64_for_sql(*f)
            }
        }
        SqlValue::Double(f) => {
            if f.is_nan() {
                "'NaN'".to_string()
            } else if f.is_infinite() {
                if f.is_sign_positive() {
                    "'Infinity'".to_string()
                } else {
                    "'-Infinity'".to_string()
                }
            } else {
                format_f64_for_sql(*f)
            }
        }
        SqlValue::Character(s) | SqlValue::Varchar(s) => format!("'{}'", s.replace('\'', "''")),
        SqlValue::Boolean(b) => if *b { "TRUE" } else { "FALSE" }.to_string(),
        SqlValue::Date(d) => format!("DATE '{}'", d),
        SqlValue::Time(t) => format!("TIME '{}'", t),
        SqlValue::Timestamp(ts) => format!("TIMESTAMP '{}'", ts),
        SqlValue::Interval(i) => format!("INTERVAL '{}'", i),
        SqlValue::Vector(v) => {
            // Format vector as space-separated values: [v1, v2, ...]
            let formatted: Vec<String> = v.iter().map(|f| f.to_string()).collect();
            format!("'{}'", formatted.join(","))
        }
        SqlValue::Blob(b) => {
            // Format blob as hex literal
            let hex: String = b.iter().map(|byte| format!("{:02X}", byte)).collect();
            format!("x'{}'", hex)
        }
    }
}

/// Format f64 for SQL literal with proper type preservation.
/// Ensures whole numbers like 5200000.0 include the ".0" suffix
/// so they're parsed as REAL, not INTEGER, when the dump is reloaded.
fn format_f64_for_sql(n: f64) -> String {
    // Use ryu for shortest round-trip representation
    let mut buffer = ryu::Buffer::new();
    let s = buffer.format(n);
    s.to_string()
}

/// Format f32 for SQL literal with proper type preservation.
/// Ensures whole numbers like 5200000.0 include the ".0" suffix
/// so they're parsed as REAL, not INTEGER, when the dump is reloaded.
fn format_f32_for_sql(n: f32) -> String {
    // Use ryu for shortest round-trip representation at f32 precision
    let mut buffer = ryu::Buffer::new();
    let s = buffer.format(n);
    s.to_string()
}

#[cfg(test)]
mod tests {
    use crate::Database;

    /// Issue #5940, Cluster A: the SQL dump must exclude temp views and temp
    /// triggers. They are session-scoped, so re-emitting their `CREATE TEMP …`
    /// text would re-materialize them in the next session — a persistence leak.
    /// Non-temp views/triggers must still appear in the dump.
    #[test]
    fn test_sql_dump_excludes_temp_views_and_triggers() {
        let parse = |sql: &str| vibesql_parser::arena_parser::parse_select_to_owned(sql).unwrap();

        let mut db = Database::new();

        // Backing table so the defining SELECTs reference something real.
        let schema = vibesql_catalog::TableSchema::new(
            "t".to_string(),
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
        db.create_table_with_identifier(schema, vibesql_catalog::TableIdentifier::new("t", false))
            .unwrap();

        // Persistent view — must appear in the dump.
        db.catalog
            .create_view(vibesql_catalog::ViewDefinition::new_with_sql(
                "v_main".to_string(),
                None,
                parse("SELECT a FROM t"),
                false,
                "CREATE VIEW v_main AS SELECT a FROM t".to_string(),
            ))
            .unwrap();

        // Temp view — must NOT appear in the dump.
        db.catalog
            .create_view(
                vibesql_catalog::ViewDefinition::new_with_sql(
                    "v_temp".to_string(),
                    None,
                    parse("SELECT a FROM t"),
                    false,
                    "CREATE TEMP VIEW v_temp AS SELECT a FROM t".to_string(),
                )
                .with_schema(Some("temp".to_string())),
            )
            .unwrap();

        // Persistent trigger — must appear in the dump.
        db.catalog
            .create_trigger(vibesql_catalog::TriggerDefinition::new_with_sql(
                "tr_main".to_string(),
                vibesql_ast::TriggerTiming::After,
                vibesql_ast::TriggerEvent::Insert,
                "t".to_string(),
                vibesql_ast::TriggerGranularity::Row,
                None,
                vibesql_ast::TriggerAction::RawSql("SELECT 1".to_string()),
                "CREATE TRIGGER tr_main AFTER INSERT ON t BEGIN SELECT 1; END".to_string(),
            ))
            .unwrap();

        // Temp trigger — must NOT appear in the dump.
        db.catalog
            .create_trigger(
                vibesql_catalog::TriggerDefinition::new_with_sql(
                    "tr_temp".to_string(),
                    vibesql_ast::TriggerTiming::After,
                    vibesql_ast::TriggerEvent::Insert,
                    "t".to_string(),
                    vibesql_ast::TriggerGranularity::Row,
                    None,
                    vibesql_ast::TriggerAction::RawSql("SELECT 2".to_string()),
                    "CREATE TEMP TRIGGER tr_temp AFTER INSERT ON t BEGIN SELECT 2; END".to_string(),
                )
                .with_schema(Some("temp".to_string())),
            )
            .unwrap();

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("dump.sql");
        db.save_sql_dump(&path).unwrap();
        let dump = std::fs::read_to_string(&path).unwrap();

        assert!(dump.contains("v_main"), "persistent view must appear in dump");
        assert!(dump.contains("tr_main"), "persistent trigger must appear in dump");
        assert!(
            !dump.contains("v_temp"),
            "temp view must NOT appear in the SQL dump, got:\n{dump}"
        );
        assert!(
            !dump.contains("tr_temp"),
            "temp trigger must NOT appear in the SQL dump, got:\n{dump}"
        );
    }

    // ========================================================================
    // write_create_table_ddl typeless-column reconstruction (#6481)
    //
    // Regenerated attached-schema DDL (`write_create_table_ddl`, used by
    // `save_attached_schema_sql_dump`) must preserve the "no declared type"
    // distinction for a column that had none in the original `CREATE TABLE`,
    // instead of defaulting every undeclared-type column to a literal
    // "BLOB" — the process-boundary bug from issue #6481.
    // ========================================================================

    #[test]
    fn test_attached_schema_dump_omits_type_for_typeless_column() {
        let mut db = Database::new();
        db.catalog.attach_database("aux", ":memory:").unwrap();

        let columns = vec![
            vibesql_catalog::ColumnSchema {
                name: "d".to_string(),
                data_type: vibesql_types::DataType::BinaryLargeObject,
                nullable: true,
                default_value: None,
                generated_expr: None,
                collation: None,
                is_exact_integer_type: false,
            },
            vibesql_catalog::ColumnSchema {
                name: "e".to_string(),
                data_type: vibesql_types::DataType::Varchar { max_length: None },
                nullable: true,
                default_value: None,
                generated_expr: None,
                collation: None,
                is_exact_integer_type: false,
            },
        ];
        let mut schema = vibesql_catalog::TableSchema::new("t2".to_string(), columns);
        // Mirrors the stripped (unqualified) form `create_table.rs` stores
        // for an attached-schema table (the `aux.` qualifier is never kept
        // in `sql_source`).
        schema.set_sql_source("CREATE TABLE t2(d, e TEXT)");
        db.create_table_with_identifier(
            schema,
            vibesql_catalog::TableIdentifier::qualified("aux", false, "t2", false),
        )
        .unwrap();

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("aux.vbsql");
        db.save_attached_schema_sql_dump("aux", &path).unwrap();
        let dump = std::fs::read_to_string(&path).unwrap();

        // Column `e`'s declared type is reconstructed from `data_type`
        // (`Varchar { max_length: None }`) via `format_column_type`, which
        // always renders the canonical "VARCHAR" spelling rather than
        // preserving the original "TEXT" text from `sql_source` — a
        // pre-existing, unrelated normalization of this reconstruction path
        // (see `typeless_columns_from_sql_source`'s doc comment). This
        // assertion only pins that a *typed* column still gets a concrete,
        // non-empty type token, alongside the typeless column `d` getting
        // none at all.
        assert!(
            dump.contains("CREATE TABLE t2 (d, e VARCHAR)"),
            "typeless column must be emitted with no type token, got:\n{dump}"
        );
        assert!(
            !dump.to_uppercase().contains("BLOB"),
            "typeless column must never be reconstructed as BLOB, got:\n{dump}"
        );
    }

    #[test]
    fn test_attached_schema_dump_keeps_explicit_blob_type() {
        // An explicit `BLOB` declaration must still round-trip as a
        // concrete type, not regress to typeless/empty.
        let mut db = Database::new();
        db.catalog.attach_database("aux", ":memory:").unwrap();

        let columns = vec![vibesql_catalog::ColumnSchema {
            name: "a".to_string(),
            data_type: vibesql_types::DataType::BinaryLargeObject,
            nullable: true,
            default_value: None,
            generated_expr: None,
            collation: None,
            is_exact_integer_type: false,
        }];
        let mut schema = vibesql_catalog::TableSchema::new("t3".to_string(), columns);
        schema.set_sql_source("CREATE TABLE t3(a BLOB)");
        db.create_table_with_identifier(
            schema,
            vibesql_catalog::TableIdentifier::qualified("aux", false, "t3", false),
        )
        .unwrap();

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("aux.vbsql");
        db.save_attached_schema_sql_dump("aux", &path).unwrap();
        let dump = std::fs::read_to_string(&path).unwrap();

        assert!(
            dump.contains("CREATE TABLE t3 (a BLOB)"),
            "explicit BLOB declaration must round-trip as BLOB, got:\n{dump}"
        );
    }

    #[test]
    fn test_attached_schema_dump_falls_back_to_typed_without_sql_source() {
        // No `sql_source` (e.g. a programmatically-built schema) must fall
        // back to the pre-#6481 behavior of always emitting a type, rather
        // than guessing a column is typeless.
        let mut db = Database::new();
        db.catalog.attach_database("aux", ":memory:").unwrap();

        let columns = vec![vibesql_catalog::ColumnSchema {
            name: "a".to_string(),
            data_type: vibesql_types::DataType::BinaryLargeObject,
            nullable: true,
            default_value: None,
            generated_expr: None,
            collation: None,
            is_exact_integer_type: false,
        }];
        let schema = vibesql_catalog::TableSchema::new("t4".to_string(), columns);
        db.create_table_with_identifier(
            schema,
            vibesql_catalog::TableIdentifier::qualified("aux", false, "t4", false),
        )
        .unwrap();

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("aux.vbsql");
        db.save_attached_schema_sql_dump("aux", &path).unwrap();
        let dump = std::fs::read_to_string(&path).unwrap();

        assert!(
            dump.contains("CREATE TABLE t4 (a BLOB)"),
            "no sql_source must fall back to always emitting a type, got:\n{dump}"
        );
    }

    // ========================================================================
    // strip_schema_qualifier (#6407)
    //
    // The attached-schema dump writer relies on this lexical pass to turn
    // captured, schema-qualified view/trigger SQL text into the
    // schema-relative text a standalone reload target can replay. It is the
    // one place in the round-trip that rewrites user SQL, so its
    // never-touch-a-literal guarantees are asserted directly rather than only
    // through the end-to-end ATTACH tests.
    // ========================================================================

    use super::strip_schema_qualifier as strip;

    #[test]
    fn test_strip_schema_qualifier_removes_every_occurrence() {
        assert_eq!(
            strip("CREATE VIEW aux.v1 AS SELECT aux.t.x FROM aux.t", "aux"),
            "CREATE VIEW v1 AS SELECT t.x FROM t"
        );
    }

    #[test]
    fn test_strip_schema_qualifier_is_case_insensitive() {
        assert_eq!(strip("SELECT * FROM AuX.t", "aux"), "SELECT * FROM t");
        assert_eq!(strip("SELECT * FROM aux.t", "AUX"), "SELECT * FROM t");
    }

    #[test]
    fn test_strip_schema_qualifier_requires_whole_identifier_match() {
        // Only a whole identifier immediately followed by `.` is a qualifier.
        assert_eq!(strip("SELECT * FROM myaux.t", "aux"), "SELECT * FROM myaux.t");
        assert_eq!(strip("SELECT * FROM aux_2.t", "aux"), "SELECT * FROM aux_2.t");
        assert_eq!(strip("SELECT * FROM auxiliary.t", "aux"), "SELECT * FROM auxiliary.t");
        // A bare mention with no following `.` is not a qualifier either.
        assert_eq!(strip("SELECT aux FROM t", "aux"), "SELECT aux FROM t");
    }

    #[test]
    fn test_strip_schema_qualifier_leaves_other_schemas_alone() {
        assert_eq!(
            strip("SELECT * FROM aux.t JOIN other.u ON 1=1", "aux"),
            "SELECT * FROM t JOIN other.u ON 1=1"
        );
    }

    #[test]
    fn test_strip_schema_qualifier_never_edits_string_literals() {
        // A qualifier-shaped substring inside a string literal is data, not
        // SQL — rewriting it would silently corrupt the user's values.
        assert_eq!(
            strip("SELECT * FROM aux.t WHERE label = 'aux.t is the source'", "aux"),
            "SELECT * FROM t WHERE label = 'aux.t is the source'"
        );
        // `''`-doubling inside the literal must not be read as the end of the
        // span (which would leave the tail unprotected).
        assert_eq!(strip("SELECT 'it''s aux.t' FROM aux.t", "aux"), "SELECT 'it''s aux.t' FROM t");
    }

    #[test]
    fn test_strip_schema_qualifier_never_edits_quoted_identifiers() {
        // Documented limitation, asserted so a future change is deliberate: a
        // pre-quoted schema name is left as-is (this engine's own DDL
        // reconstruction never emits one).
        assert_eq!(strip(r#"SELECT * FROM "aux".t"#, "aux"), r#"SELECT * FROM "aux".t"#);
        assert_eq!(strip("SELECT * FROM `aux`.t", "aux"), "SELECT * FROM `aux`.t");
        assert_eq!(strip("SELECT * FROM [aux].t", "aux"), "SELECT * FROM [aux].t");
        // A column literally named `aux.x` via quoting is untouched, while an
        // unquoted qualifier in the same statement still goes.
        assert_eq!(strip(r#"SELECT "aux.x" FROM aux.t"#, "aux"), r#"SELECT "aux.x" FROM t"#);
    }

    #[test]
    fn test_strip_schema_qualifier_preserves_non_ascii_text() {
        // Regression guard: a byte-wise `byte as char` copy decodes UTF-8
        // continuation bytes as Latin-1 and mojibakes any non-ASCII text.
        let sql = "SELECT * FROM aux.t WHERE name = 'café ☕ Ünïcödé'";
        assert_eq!(strip(sql, "aux"), "SELECT * FROM t WHERE name = 'café ☕ Ünïcödé'");
        // …including outside quotes, where the identifier scanner runs.
        assert_eq!(strip("-- café note\nSELECT 1", "aux"), "-- café note\nSELECT 1");
    }

    #[test]
    fn test_strip_schema_qualifier_no_op_when_schema_absent() {
        let sql = "CREATE VIEW v1 AS SELECT x FROM t WHERE x > 0";
        assert_eq!(strip(sql, "aux"), sql);
    }

    #[test]
    fn strip_schema_qualifier_also_strips_an_alias_matching_the_schema_name() {
        // Documented limitation, pinned so it is known rather than accidental
        // (#6476 review note 1): the scan is lexical, so a *table alias* that
        // happens to equal the schema name is indistinguishable from a real
        // schema qualifier and is stripped too.
        assert_eq!(strip("SELECT aux.x FROM t AS aux", "aux"), "SELECT x FROM t AS aux");
        // The alias in the FROM clause itself is left intact — only the
        // `alias.` *prefix* on a column reference is removed.
        assert_eq!(
            strip("SELECT aux.x, b.y FROM t AS aux JOIN u AS b ON aux.x = b.x", "aux"),
            "SELECT x, b.y FROM t AS aux JOIN u AS b ON x = b.x"
        );
    }

    #[test]
    fn strip_schema_qualifier_rewrites_inside_comments() {
        // Documented limitation, pinned (#6476 review note 2): `--` and
        // `/* */` are not treated as spans, so a qualifier inside a comment
        // is rewritten like any other occurrence. Cosmetic — the comment text
        // changes, the statement's meaning does not.
        assert_eq!(
            strip("-- reads aux.t\nSELECT x FROM aux.t", "aux"),
            "-- reads t\nSELECT x FROM t"
        );
        assert_eq!(
            strip("SELECT x /* from aux.t */ FROM aux.t", "aux"),
            "SELECT x /* from t */ FROM t"
        );
    }
}
