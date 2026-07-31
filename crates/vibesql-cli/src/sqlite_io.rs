//! SQLite import/export support
//!
//! Provides functions to import from SQLite `.db` files into VibeSQL databases
//! and export VibeSQL databases to SQLite `.db` files using rusqlite.

use std::path::Path;

use rusqlite::{types::Value as SqliteValue, Connection, OpenFlags};
use vibesql_storage::{Database, Row};
use vibesql_types::SqlValue;

/// Result of a SQLite import operation
pub struct ImportResult {
    pub database: Database,
    pub warnings: Vec<String>,
    pub tables_imported: usize,
    pub tables_skipped: usize,
    pub rows_imported: usize,
}

/// Import a SQLite database file into a VibeSQL Database
pub fn import_sqlite<P: AsRef<Path>>(path: P) -> anyhow::Result<ImportResult> {
    let path = path.as_ref();
    let conn = Connection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_ONLY)?;

    let mut db = Database::new();
    let mut warnings = Vec::new();
    let mut tables_imported = 0;
    let mut tables_skipped = 0;
    let mut rows_imported = 0;

    // 1. Read all table schemas from sqlite_master
    let mut stmt = conn.prepare(
        "SELECT name, sql FROM sqlite_master WHERE type='table' AND sql IS NOT NULL ORDER BY rowid",
    )?;
    let tables: Vec<(String, String)> = stmt
        .query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)))?
        .filter_map(|r| r.ok())
        .collect();

    // 2. Create tables
    for (table_name, create_sql) in &tables {
        // Skip SQLite internal tables
        if table_name.starts_with("sqlite_") {
            continue;
        }

        // Try to parse the CREATE TABLE with VibeSQL's parser
        match vibesql_parser::Parser::parse_sql(create_sql) {
            Ok(vibesql_ast::Statement::CreateTable(create_stmt)) => {
                if let Err(e) = vibesql_executor::CreateTableExecutor::execute(&create_stmt, &mut db)
                {
                    warnings.push(format!(
                        "Warning: Skipping table '{}': {}",
                        table_name, e
                    ));
                    tables_skipped += 1;
                    continue;
                }
            }
            _ => {
                // Fallback: reconstruct from PRAGMA table_info
                match create_table_from_pragma(&conn, table_name, &mut db) {
                    Ok(()) => {}
                    Err(e) => {
                        warnings.push(format!(
                            "Warning: Skipping table '{}': could not parse schema: {}",
                            table_name, e
                        ));
                        tables_skipped += 1;
                        continue;
                    }
                }
            }
        }

        // 3. Import data
        match import_table_data(&conn, table_name, &mut db) {
            Ok(count) => {
                rows_imported += count;
                tables_imported += 1;
            }
            Err(e) => {
                warnings.push(format!(
                    "Warning: Failed to import data for '{}': {}",
                    table_name, e
                ));
                // Table was created but data import failed — still count as imported
                tables_imported += 1;
            }
        }
    }

    // 4. Import indexes (skip autoindex and primary key indexes)
    let mut idx_stmt = conn.prepare(
        "SELECT name, sql FROM sqlite_master WHERE type='index' AND sql IS NOT NULL ORDER BY rowid",
    )?;
    let indexes: Vec<(String, String)> = idx_stmt
        .query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)))?
        .filter_map(|r| r.ok())
        .collect();

    for (index_name, index_sql) in &indexes {
        if index_name.starts_with("sqlite_autoindex_") {
            continue;
        }
        match vibesql_parser::Parser::parse_sql(index_sql) {
            Ok(vibesql_ast::Statement::CreateIndex(index_stmt)) => {
                if let Err(e) = vibesql_executor::CreateIndexExecutor::execute(&index_stmt, &mut db)
                {
                    warnings.push(format!(
                        "Warning: Skipping index '{}': {}",
                        index_name, e
                    ));
                }
            }
            _ => {
                warnings.push(format!(
                    "Warning: Skipping index '{}': could not parse",
                    index_name
                ));
            }
        }
    }

    // 5. Import views (best-effort)
    let mut view_stmt = conn.prepare(
        "SELECT name, sql FROM sqlite_master WHERE type='view' AND sql IS NOT NULL ORDER BY rowid",
    )?;
    let views: Vec<(String, String)> = view_stmt
        .query_map([], |row| Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?)))?
        .filter_map(|r| r.ok())
        .collect();

    for (view_name, view_sql) in &views {
        match vibesql_parser::Parser::parse_sql(view_sql) {
            Ok(vibesql_ast::Statement::CreateView(mut create_view)) => {
                create_view.sql_definition = Some(view_sql.clone());
                if let Err(e) = vibesql_executor::ViewExecutor::execute_create_view(&create_view, &mut db) {
                    warnings.push(format!(
                        "Warning: Skipping view '{}': {}",
                        view_name, e
                    ));
                }
            }
            _ => {
                warnings.push(format!(
                    "Warning: Skipping view '{}': could not parse",
                    view_name
                ));
            }
        }
    }

    // 6. Warn about triggers
    let trigger_count: i64 =
        conn.query_row("SELECT COUNT(*) FROM sqlite_master WHERE type='trigger'", [], |row| {
            row.get(0)
        })?;
    if trigger_count > 0 {
        warnings.push(format!(
            "Warning: {} trigger(s) skipped (triggers not supported)",
            trigger_count
        ));
    }

    Ok(ImportResult {
        database: db,
        warnings,
        tables_imported,
        tables_skipped,
        rows_imported,
    })
}

/// Import all rows from a SQLite table into the VibeSQL database
fn import_table_data(
    conn: &Connection,
    table_name: &str,
    db: &mut Database,
) -> anyhow::Result<usize> {
    // Get column count from VibeSQL table
    let vibe_table_name = table_name.to_uppercase();
    let col_count = db
        .get_table(&vibe_table_name)
        .map(|t| t.schema.columns.len())
        .unwrap_or(0);

    if col_count == 0 {
        return Ok(0);
    }

    // Query all rows from SQLite
    let sql = format!("SELECT * FROM \"{}\"", table_name.replace('"', "\"\""));
    let mut stmt = conn.prepare(&sql)?;
    let column_count = stmt.column_count();

    let mut rows = Vec::new();
    let mut result_rows = stmt.query([])?;

    while let Some(row) = result_rows.next()? {
        let mut values = Vec::with_capacity(column_count);
        for i in 0..column_count {
            let value = row.get_ref(i)?;
            values.push(sqlite_value_to_sql_value(value));
        }
        rows.push(Row::new(values));
    }

    let count = rows.len();
    if !rows.is_empty() {
        db.insert_rows_batch(&vibe_table_name, rows)
            .map_err(|e| anyhow::anyhow!("Insert failed for '{}': {}", table_name, e))?;
    }

    Ok(count)
}

/// Fallback: create a VibeSQL table from PRAGMA table_info when CREATE TABLE SQL can't be parsed
fn create_table_from_pragma(
    conn: &Connection,
    table_name: &str,
    db: &mut Database,
) -> anyhow::Result<()> {
    let mut stmt = conn.prepare(&format!(
        "PRAGMA table_info(\"{}\")",
        table_name.replace('"', "\"\"")
    ))?;

    let mut columns = Vec::new();
    let mut pk_columns = Vec::new();

    let rows = stmt.query_map([], |row| {
        Ok((
            row.get::<_, i64>(0)?,    // cid
            row.get::<_, String>(1)?,  // name
            row.get::<_, String>(2)?,  // type
            row.get::<_, bool>(3)?,    // notnull
            row.get::<_, i64>(5)?,     // pk
        ))
    })?;

    for row in rows {
        let (_, col_name, col_type, notnull, pk) = row?;

        let data_type = sqlite_type_to_vibesql(&col_type);
        let is_exact = col_type.eq_ignore_ascii_case("INTEGER");

        columns.push(vibesql_catalog::ColumnSchema {
            name: col_name.clone(),
            data_type,
            nullable: !notnull,
            default_value: None,
            is_exact_integer_type: is_exact,
            collation: None,
            generated_expr: None,
        });

        if pk > 0 {
            pk_columns.push(col_name);
        }
    }

    if columns.is_empty() {
        return Err(anyhow::anyhow!("Table '{}' has no columns", table_name));
    }

    let mut schema = vibesql_catalog::TableSchema::new(table_name.to_string(), columns);
    schema.primary_key = if pk_columns.is_empty() { None } else { Some(pk_columns) };

    db.create_table(schema)
        .map_err(|e| anyhow::anyhow!("{}", e))?;

    Ok(())
}

/// Convert a SQLite column type string to a VibeSQL DataType
fn sqlite_type_to_vibesql(type_str: &str) -> vibesql_types::DataType {
    let upper = type_str.to_uppercase();
    let upper = upper.trim();

    match upper {
        "INTEGER" | "INT" => vibesql_types::DataType::Integer,
        "SMALLINT" | "TINYINT" | "INT2" => vibesql_types::DataType::Smallint,
        "BIGINT" | "INT8" => vibesql_types::DataType::Bigint,
        "REAL" | "DOUBLE" | "DOUBLE PRECISION" | "FLOAT" => vibesql_types::DataType::Real,
        "BOOLEAN" | "BOOL" => vibesql_types::DataType::Boolean,
        "DATE" => vibesql_types::DataType::Date,
        "DATETIME" | "TIMESTAMP" => {
            vibesql_types::DataType::Timestamp { with_timezone: false }
        }
        "BLOB" => vibesql_types::DataType::BinaryLargeObject,
        "TEXT" => vibesql_types::DataType::Varchar { max_length: None },
        "" => vibesql_types::DataType::Varchar { max_length: None },
        _ => {
            // Try to parse VARCHAR(n) / CHAR(n) patterns
            if let Some(len_str) = upper.strip_prefix("VARCHAR(").and_then(|s| s.strip_suffix(')')) {
                if let Ok(len) = len_str.trim().parse::<usize>() {
                    return vibesql_types::DataType::Varchar { max_length: Some(len) };
                }
            }
            if let Some(len_str) = upper.strip_prefix("CHAR(").and_then(|s| s.strip_suffix(')')) {
                if let Ok(len) = len_str.trim().parse::<usize>() {
                    return vibesql_types::DataType::Character { length: len };
                }
            }
            if upper.contains("INT") {
                vibesql_types::DataType::Integer
            } else if upper.contains("CHAR") || upper.contains("TEXT") || upper.contains("CLOB") {
                vibesql_types::DataType::Varchar { max_length: None }
            } else if upper.contains("REAL") || upper.contains("FLOA") || upper.contains("DOUB") {
                vibesql_types::DataType::Real
            } else if upper.contains("BLOB") {
                vibesql_types::DataType::BinaryLargeObject
            } else {
                // Default: NUMERIC affinity maps to VARCHAR for flexibility
                vibesql_types::DataType::Varchar { max_length: None }
            }
        }
    }
}

/// Convert a rusqlite ValueRef to a VibeSQL SqlValue
fn sqlite_value_to_sql_value(value: rusqlite::types::ValueRef<'_>) -> SqlValue {
    match value {
        rusqlite::types::ValueRef::Null => SqlValue::Null,
        rusqlite::types::ValueRef::Integer(i) => SqlValue::Integer(i),
        rusqlite::types::ValueRef::Real(f) => SqlValue::Real(f),
        rusqlite::types::ValueRef::Text(bytes) => {
            let s = String::from_utf8_lossy(bytes);
            SqlValue::Varchar(s.as_ref().into())
        }
        rusqlite::types::ValueRef::Blob(bytes) => SqlValue::Blob(bytes.to_vec()),
    }
}

// ============================================================================
// Export: VibeSQL -> SQLite
// ============================================================================

/// Export a VibeSQL database to a SQLite `.db` file
pub fn export_sqlite<P: AsRef<Path>>(db: &Database, path: P) -> anyhow::Result<ExportResult> {
    let path = path.as_ref();

    // Remove existing file if present
    if path.exists() {
        std::fs::remove_file(path)?;
    }

    let conn = Connection::open(path)?;
    conn.execute_batch("PRAGMA journal_mode=DELETE;")?;

    let mut warnings = Vec::new();
    let mut tables_exported = 0;
    let mut rows_exported = 0;

    // Disable FK enforcement during export so table creation order doesn't matter
    conn.execute_batch("PRAGMA foreign_keys=OFF;")?;
    // Begin transaction for atomicity
    conn.execute_batch("BEGIN TRANSACTION;")?;

    // Iterate all tables in the database
    for schema_name in &db.catalog.list_schemas() {
        if vibesql_catalog::Catalog::is_temp_schema(schema_name)
            // Attached schemas are session-scoped (#6310) and not exported.
            || db.catalog.is_attached_schema(schema_name)
        {
            continue;
        }

        let schema_tables = if let Some(schema) = db.catalog.get_schema(schema_name) {
            schema.list_tables()
        } else {
            continue;
        };

        for table_name in &schema_tables {
            let qualified_name = format!("{}.{}", schema_name, table_name);
            let Some(table) = db.tables.get(&qualified_name) else {
                continue;
            };

            // Generate and execute CREATE TABLE
            let create_sql = generate_create_table_sql(table_name, &table.schema);
            if let Err(e) = conn.execute_batch(&create_sql) {
                warnings.push(format!("Warning: Skipping table '{}': {}", table_name, e));
                continue;
            }

            // Insert data
            let col_count = table.schema.columns.len();
            let placeholders: Vec<&str> = (0..col_count).map(|_| "?").collect();
            let insert_sql = format!(
                "INSERT INTO \"{}\" VALUES ({})",
                table_name.replace('"', "\"\""),
                placeholders.join(", ")
            );

            let mut insert_stmt = conn.prepare(&insert_sql)?;

            for (_row_idx, row) in table.scan_live() {
                let params: Vec<SqliteValue> =
                    row.values.iter().map(sql_value_to_sqlite).collect();
                let param_refs: Vec<&dyn rusqlite::types::ToSql> =
                    params.iter().map(|v| v as &dyn rusqlite::types::ToSql).collect();

                if let Err(e) = insert_stmt.execute(param_refs.as_slice()) {
                    warnings.push(format!(
                        "Warning: Skipping row in '{}': {}",
                        table_name, e
                    ));
                    continue;
                }
                rows_exported += 1;
            }

            tables_exported += 1;
        }
    }

    // Export indexes
    export_indexes(db, &conn, &mut warnings);

    // Export views
    export_views(db, &conn, &mut warnings);

    conn.execute_batch("COMMIT;")?;

    Ok(ExportResult {
        warnings,
        tables_exported,
        rows_exported,
    })
}

/// Result of a SQLite export operation
pub struct ExportResult {
    pub warnings: Vec<String>,
    pub tables_exported: usize,
    pub rows_exported: usize,
}

/// Generate a SQLite-compatible CREATE TABLE statement from a VibeSQL TableSchema
fn generate_create_table_sql(table_name: &str, schema: &vibesql_catalog::TableSchema) -> String {
    let mut sql = format!(
        "CREATE TABLE \"{}\" (",
        table_name.replace('"', "\"\"")
    );

    for (i, col) in schema.columns.iter().enumerate() {
        if i > 0 {
            sql.push_str(", ");
        }
        sql.push_str(&format!(
            "\"{}\" {}",
            col.name.replace('"', "\"\""),
            vibesql_type_to_sqlite(&col.data_type, col.is_exact_integer_type)
        ));

        if !col.nullable {
            sql.push_str(" NOT NULL");
        }
        if let Some(ref default_expr) = col.default_value {
            use vibesql_ast::pretty_print::ToSql;
            sql.push_str(&format!(" DEFAULT {}", default_expr.to_sql()));
        }
    }

    // Primary key
    if let Some(ref pk_cols) = schema.primary_key {
        let pk_str: Vec<String> = pk_cols
            .iter()
            .map(|c| format!("\"{}\"", c.replace('"', "\"\"")))
            .collect();
        sql.push_str(&format!(", PRIMARY KEY ({})", pk_str.join(", ")));
    }

    // Unique constraints
    for unique_cols in &schema.unique_constraints {
        let u_str: Vec<String> = unique_cols
            .iter()
            .map(|c| format!("\"{}\"", c.replace('"', "\"\"")))
            .collect();
        sql.push_str(&format!(", UNIQUE ({})", u_str.join(", ")));
    }

    // Foreign keys
    for fk in &schema.foreign_keys {
        let fk_cols: Vec<String> = fk
            .column_names
            .iter()
            .map(|c| format!("\"{}\"", c.replace('"', "\"\"")))
            .collect();
        let parent_cols: Vec<String> = fk
            .parent_column_names
            .iter()
            .filter(|c| !c.is_empty())
            .map(|c| format!("\"{}\"", c.replace('"', "\"\"")))
            .collect();

        sql.push_str(&format!(
            ", FOREIGN KEY ({}) REFERENCES \"{}\"",
            fk_cols.join(", "),
            fk.parent_table.replace('"', "\"\"")
        ));
        if !parent_cols.is_empty() {
            sql.push_str(&format!("({})", parent_cols.join(", ")));
        }
    }

    sql.push(')');

    if schema.without_rowid {
        sql.push_str(" WITHOUT ROWID");
    }

    sql.push(';');
    sql
}

/// Convert a VibeSQL DataType to a SQLite type string
fn vibesql_type_to_sqlite(
    data_type: &vibesql_types::DataType,
    is_exact_integer_type: bool,
) -> String {
    use vibesql_types::DataType;
    match data_type {
        DataType::Integer => {
            if is_exact_integer_type { "INTEGER" } else { "INT" }.to_string()
        }
        DataType::Smallint => "INTEGER".to_string(),
        DataType::Bigint | DataType::Unsigned => "INTEGER".to_string(),
        DataType::Float { .. } | DataType::Real | DataType::DoublePrecision => "REAL".to_string(),
        DataType::Varchar { max_length } => {
            if let Some(len) = max_length {
                format!("VARCHAR({})", len)
            } else {
                "TEXT".to_string()
            }
        }
        DataType::Character { length } => format!("CHAR({})", length),
        DataType::Boolean => "INTEGER".to_string(), // SQLite stores booleans as integers
        DataType::Date => "TEXT".to_string(),
        DataType::Time { .. } => "TEXT".to_string(),
        DataType::Timestamp { .. } => "TEXT".to_string(),
        DataType::BinaryLargeObject => "BLOB".to_string(),
        DataType::Numeric { precision, scale } => format!("NUMERIC({},{})", precision, scale),
        DataType::Decimal { precision, scale } => format!("NUMERIC({},{})", precision, scale),
        _ => "TEXT".to_string(),
    }
}

/// Convert a VibeSQL SqlValue to a rusqlite Value
fn sql_value_to_sqlite(value: &SqlValue) -> SqliteValue {
    match value {
        SqlValue::Null => SqliteValue::Null,
        SqlValue::Integer(i) => SqliteValue::Integer(*i),
        SqlValue::Smallint(i) => SqliteValue::Integer(*i as i64),
        SqlValue::Bigint(i) => SqliteValue::Integer(*i),
        SqlValue::Real(f) => SqliteValue::Real(*f),
        SqlValue::Float(f) => SqliteValue::Real(*f as f64),
        SqlValue::Boolean(b) => SqliteValue::Integer(if *b { 1 } else { 0 }),
        SqlValue::Varchar(s) | SqlValue::Character(s) => {
            SqliteValue::Text(s.to_string())
        }
        SqlValue::Date(d) => SqliteValue::Text(d.to_string()),
        SqlValue::Time(t) => SqliteValue::Text(t.to_string()),
        SqlValue::Timestamp(ts) => SqliteValue::Text(ts.to_string()),
        SqlValue::Blob(b) => SqliteValue::Blob(b.clone()),
        SqlValue::Numeric(n) => SqliteValue::Text(n.to_string()),
        _ => SqliteValue::Text(format!("{}", value)),
    }
}

/// Export indexes from VibeSQL to SQLite
fn export_indexes(db: &Database, conn: &Connection, warnings: &mut Vec<String>) {
    for index_name in db.list_indexes() {
        let lower_name = index_name.to_lowercase();
        if lower_name.starts_with("pk_") || lower_name.starts_with("sqlite_autoindex_") {
            continue;
        }
        let Some(metadata) = db.get_index(&index_name) else { continue };
        // Indexes on tables in ATTACHed schemas are session-scoped (#6310) and
        // not exported. Filter on `metadata.schema` (the owning schema resolved
        // at CREATE INDEX time) — an unqualified `CREATE INDEX i1 ON t(z)` that
        // resolves to an attached table stores the bare `"t"` as table_name.
        if db.catalog.is_attached_schema(&metadata.schema) {
            continue;
        }
        let unique = if metadata.unique { "UNIQUE " } else { "" };
        let cols: Vec<String> = metadata
            .columns
            .iter()
            .filter_map(|c| {
                use vibesql_ast::IndexColumn;
                match c {
                    IndexColumn::Column { column_name, .. } => {
                        Some(format!("\"{}\"", column_name.replace('"', "\"\"")))
                    }
                    IndexColumn::Expression { expr, .. } => {
                        use vibesql_ast::pretty_print::ToSql;
                        Some(expr.to_sql())
                    }
                }
            })
            .collect();
        let sql = format!(
            "CREATE {}INDEX \"{}\" ON \"{}\" ({});",
            unique,
            index_name.replace('"', "\"\""),
            metadata.table_name.replace('"', "\"\""),
            cols.join(", ")
        );
        if let Err(e) = conn.execute_batch(&sql) {
            warnings.push(format!("Warning: Skipping index '{}': {}", index_name, e));
        }
    }
}

/// Export views from VibeSQL to SQLite
fn export_views(db: &Database, conn: &Connection, warnings: &mut Vec<String>) {
    for view_name in db.catalog.list_views() {
        if let Some(view) = db.catalog.get_view(&view_name) {
            if let Some(ref sql_def) = view.sql_definition {
                if let Err(e) = conn.execute_batch(sql_def) {
                    warnings.push(format!("Warning: Skipping view '{}': {}", view_name, e));
                }
            }
        }
    }
}
