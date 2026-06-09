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

                // For default schema, use unqualified name in output for cleaner SQL
                let output_name = if schema_name == vibesql_catalog::DEFAULT_SCHEMA {
                    table_name.clone()
                } else {
                    qualified_name.clone()
                };
                // CREATE TABLE statement
                let schema = &table.schema;
                let quoted_output_name = quote_identifier(&output_name);
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
                    // Only output CONSTRAINT keyword if the name is different from expression
                    // (i.e., it's a user-provided name)
                    if constraint_name != &expr_text {
                        write!(writer, ", CONSTRAINT {} CHECK ({})", constraint_name, expr_text)
                            .map_err(|e| {
                                StorageError::NotImplemented(format!("Write error: {}", e))
                            })?;
                    } else {
                        write!(writer, ", CHECK ({})", expr_text).map_err(|e| {
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

                // Add WITHOUT ROWID clause for SQLite compatibility (Issue #4803)
                if schema.without_rowid {
                    write!(writer, " WITHOUT ROWID")
                        .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
                }

                writeln!(writer, ";")
                    .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;

                // INSERT statements for data (only live/non-deleted rows)
                // Using scan_live() to skip rows marked as deleted in the deletion bitmap.
                // Note: We must skip generated columns - they cannot be inserted directly.
                if table.row_count() > 0 {
                    writeln!(writer)
                        .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;

                    // Find indices of non-generated columns
                    let non_generated_indices: Vec<usize> = schema
                        .columns
                        .iter()
                        .enumerate()
                        .filter(|(_, col)| col.generated_expr.is_none())
                        .map(|(i, _)| i)
                        .collect();

                    // Build column list for INSERT if we have generated columns
                    let has_generated_columns = non_generated_indices.len() < schema.columns.len();
                    let column_list: String = if has_generated_columns {
                        let col_names: Vec<&str> = non_generated_indices
                            .iter()
                            .map(|&i| schema.columns[i].name.as_str())
                            .collect();
                        format!(" ({})", col_names.join(", "))
                    } else {
                        String::new()
                    };

                    for (_idx, row) in table.scan_live() {
                        write!(writer, "INSERT INTO {}{} VALUES (", &quoted_output_name, column_list)
                            .map_err(|e| {
                                StorageError::NotImplemented(format!("Write error: {}", e))
                            })?;

                        // Only include values for non-generated columns
                        let mut first = true;
                        for &col_idx in &non_generated_indices {
                            if !first {
                                write!(writer, ", ").map_err(|e| {
                                    StorageError::NotImplemented(format!("Write error: {}", e))
                                })?;
                            }
                            first = false;
                            write!(writer, "{}", sql_value_to_literal(&row.values[col_idx]))
                                .map_err(|e| {
                                    StorageError::NotImplemented(format!("Write error: {}", e))
                                })?;
                        }
                        writeln!(writer, ");").map_err(|e| {
                            StorageError::NotImplemented(format!("Write error: {}", e))
                        })?;
                    }
                }

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
            let lower_name = index_name.to_lowercase();
            if lower_name.starts_with("pk_") || lower_name.starts_with("sqlite_autoindex_") {
                continue;
            }
            let metadata = self.get_index(&index_name).unwrap();
            write!(writer, "CREATE")
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            if metadata.unique {
                write!(writer, " UNIQUE")
                    .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            }
            write!(writer, " INDEX {} ON {} (", index_name, metadata.table_name)
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
                        write!(writer, "{}", column_name).map_err(|e| {
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
                write!(writer, " {:?}", col.direction())
                    .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            }

            writeln!(writer, ");")
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
        }
        writeln!(writer)
            .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;

        // Export views
        writeln!(writer, "-- Views")
            .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
        for view_name in self.catalog.list_views() {
            if let Some(view_def) = self.catalog.get_view(&view_name) {
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
                        .map_err(|e| {
                            StorageError::NotImplemented(format!("Write error: {}", e))
                        })?;
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

/// Quote an identifier if it contains special characters or starts with a digit.
/// Uses double-quote style quoting (SQL standard / SQLite).
fn quote_identifier(name: &str) -> String {
    // Check if the identifier needs quoting:
    // - starts with a digit
    // - contains non-alphanumeric characters (except underscore)
    // - is empty
    let needs_quoting = name.is_empty()
        || name.starts_with(|c: char| c.is_ascii_digit())
        || !name.chars().all(|c| c.is_alphanumeric() || c == '_');

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
