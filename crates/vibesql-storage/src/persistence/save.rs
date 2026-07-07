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
            // Skip all temp schemas (temp_1, temp_2, etc.) - they are session-scoped
            if vibesql_catalog::Catalog::is_temp_schema(schema_name) {
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

                write!(writer, "CREATE TABLE {} (", &quoted_output_name)
                    .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;

                for (i, col) in schema.columns.iter().enumerate() {
                    if i > 0 {
                        write!(writer, ", ").map_err(|e| {
                            StorageError::NotImplemented(format!("Write error: {}", e))
                        })?;
                    }
                    // Format column type, preserving INT vs INTEGER distinction for rowid alias behavior
                    let type_str = format_column_type(&col.data_type, col.is_exact_integer_type);
                    write!(writer, "{} {}", quote_identifier(&col.name), type_str)
                        .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;

                    // Handle generated columns (AS expression syntax)
                    if let Some(ref generated_expr) = col.generated_expr {
                        use vibesql_ast::pretty_print::ToSql;
                        write!(writer, " AS ({})", generated_expr.to_sql()).map_err(|e| {
                            StorageError::NotImplemented(format!("Write error: {}", e))
                        })?;
                    } else {
                        // Only non-generated columns can have DEFAULT, COLLATE, NOT NULL
                        // Add DEFAULT clause if present
                        if let Some(ref default_expr) = col.default_value {
                            use vibesql_ast::pretty_print::ToSql;
                            write!(writer, " DEFAULT {}", default_expr.to_sql()).map_err(|e| {
                                StorageError::NotImplemented(format!("Write error: {}", e))
                            })?;
                        }
                        // Add COLLATE clause if present
                        if let Some(ref collation) = col.collation {
                            write!(writer, " COLLATE {}", collation).map_err(|e| {
                                StorageError::NotImplemented(format!("Write error: {}", e))
                            })?;
                        }
                        if !col.nullable {
                            write!(writer, " NOT NULL").map_err(|e| {
                                StorageError::NotImplemented(format!("Write error: {}", e))
                            })?;
                        }
                    }
                }

                // Add PRIMARY KEY constraint if present
                if let Some(pk_cols) = &schema.primary_key {
                    let quoted_pk: Vec<String> =
                        pk_cols.iter().map(|c| quote_identifier(c)).collect();
                    write!(writer, ", PRIMARY KEY ({})", quoted_pk.join(", "))
                        .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
                }

                // Add UNIQUE constraints
                for unique_cols in &schema.unique_constraints {
                    let quoted_uniq: Vec<String> =
                        unique_cols.iter().map(|c| quote_identifier(c)).collect();
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
                            .map_err(|e| {
                                StorageError::NotImplemented(format!("Write error: {}", e))
                            })?;
                    } else {
                        // Unnamed: emit the verbatim source text so the reloaded
                        // constraint's violation message round-trips byte-exact.
                        write!(writer, ", CHECK ({})", constraint_name).map_err(|e| {
                            StorageError::NotImplemented(format!("Write error: {}", e))
                        })?;
                    }
                }

                // Add FOREIGN KEY constraints
                for fk in &schema.foreign_keys {
                    let fk_cols: Vec<String> =
                        fk.column_names.iter().map(|c| quote_identifier(c)).collect();

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
                            write!(writer, " ON DELETE CASCADE").map_err(|e| {
                                StorageError::NotImplemented(format!("Write error: {}", e))
                            })?;
                        }
                        vibesql_catalog::ReferentialAction::SetNull => {
                            write!(writer, " ON DELETE SET NULL").map_err(|e| {
                                StorageError::NotImplemented(format!("Write error: {}", e))
                            })?;
                        }
                        vibesql_catalog::ReferentialAction::SetDefault => {
                            write!(writer, " ON DELETE SET DEFAULT").map_err(|e| {
                                StorageError::NotImplemented(format!("Write error: {}", e))
                            })?;
                        }
                        vibesql_catalog::ReferentialAction::Restrict => {
                            write!(writer, " ON DELETE RESTRICT").map_err(|e| {
                                StorageError::NotImplemented(format!("Write error: {}", e))
                            })?;
                        }
                    }

                    // Add ON UPDATE clause if not NO ACTION
                    match &fk.on_update {
                        vibesql_catalog::ReferentialAction::NoAction => {}
                        vibesql_catalog::ReferentialAction::Cascade => {
                            write!(writer, " ON UPDATE CASCADE").map_err(|e| {
                                StorageError::NotImplemented(format!("Write error: {}", e))
                            })?;
                        }
                        vibesql_catalog::ReferentialAction::SetNull => {
                            write!(writer, " ON UPDATE SET NULL").map_err(|e| {
                                StorageError::NotImplemented(format!("Write error: {}", e))
                            })?;
                        }
                        vibesql_catalog::ReferentialAction::SetDefault => {
                            write!(writer, " ON UPDATE SET DEFAULT").map_err(|e| {
                                StorageError::NotImplemented(format!("Write error: {}", e))
                            })?;
                        }
                        vibesql_catalog::ReferentialAction::Restrict => {
                            write!(writer, " ON UPDATE RESTRICT").map_err(|e| {
                                StorageError::NotImplemented(format!("Write error: {}", e))
                            })?;
                        }
                    }

                    // Emit DEFERRABLE clause so deferred-FK semantics survive
                    // a persistence round-trip. Without this, `.vbsql` dump
                    // and reload would lose `INITIALLY DEFERRED` and the TCL
                    // shim's batched-process model would degrade fkey6 tests
                    // back to immediate enforcement.
                    if fk.is_deferrable {
                        if fk.initially_deferred {
                            write!(writer, " DEFERRABLE INITIALLY DEFERRED").map_err(|e| {
                                StorageError::NotImplemented(format!("Write error: {}", e))
                            })?;
                        } else {
                            write!(writer, " DEFERRABLE INITIALLY IMMEDIATE").map_err(|e| {
                                StorageError::NotImplemented(format!("Write error: {}", e))
                            })?;
                        }
                    }
                }

                // Close the column definitions
                write!(writer, ")")
                    .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;

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
            // - "sqlite_autoindex_<table>_<n>" indexes are created by PRIMARY KEY/UNIQUE constraints
            //   (follows SQLite naming convention for implicit indexes)
            // - the WITHOUT ROWID PK internal index (issue #5882) is regenerated from the
            //   CREATE TABLE DDL on reload, so it must not be dumped as a CREATE INDEX
            let lower_name = index_name.to_lowercase();
            if lower_name.starts_with("pk_")
                || lower_name.starts_with("sqlite_autoindex_")
                || lower_name.starts_with(vibesql_catalog::WITHOUT_ROWID_PK_INDEX_PREFIX)
            {
                continue;
            }
            let metadata = self.get_index(&index_name).unwrap();
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
        for view_name in self.catalog.list_views() {
            if let Some(view_def) = self.catalog.get_view(&view_name) {
                // Skip temp views (`CREATE TEMP VIEW`): they are session-scoped
                // and must not survive into the next session via the SQL dump
                // (issue #5940, Cluster A).
                if view_def.is_temp() {
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
        for trigger_name in self.catalog.list_triggers() {
            if let Some(trigger_def) = self.catalog.get_trigger(&trigger_name) {
                // Skip temp triggers (`CREATE TEMP TRIGGER`): they are
                // session-scoped and must not survive into the next session via
                // the SQL dump (issue #5940, Cluster A).
                if trigger_def.is_temp() {
                    continue;
                }
                match trigger_def.sql_definition.as_ref() {
                    Some(sql) => {
                        let sql = sql.trim_end_matches(';').trim();
                        writeln!(writer, "{};", sql).map_err(|e| {
                            StorageError::NotImplemented(format!("Write error: {}", e))
                        })?;
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
}
