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

use std::{
    fs::File,
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
    /// # Example
    /// ```no_run
    /// # use vibesql_storage::Database;
    /// let db = Database::new();
    /// db.save_sql_dump("database.sql").unwrap();
    /// ```
    pub fn save_sql_dump<P: AsRef<Path>>(&self, path: P) -> Result<(), StorageError> {
        let file = File::create(path)
            .map_err(|e| StorageError::NotImplemented(format!("Failed to create file: {}", e)))?;

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

        // Export schemas (except default 'public' which always exists)
        writeln!(writer, "-- Schemas")
            .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
        for schema_name in &self.catalog.list_schemas() {
            if schema_name != "public" {
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

        // Get list of table names
        let table_names = self.catalog.list_tables();

        for table_name in &table_names {
            if let Some(table) = self.get_table(table_name) {
                // CREATE TABLE statement
                let schema = &table.schema;
                write!(writer, "CREATE TABLE {} (", &table_name)
                    .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;

                for (i, col) in schema.columns.iter().enumerate() {
                    if i > 0 {
                        write!(writer, ", ").map_err(|e| {
                            StorageError::NotImplemented(format!("Write error: {}", e))
                        })?;
                    }
                    write!(writer, "{} {}", col.name, format_data_type(&col.data_type))
                        .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
                    if !col.nullable {
                        write!(writer, " NOT NULL").map_err(|e| {
                            StorageError::NotImplemented(format!("Write error: {}", e))
                        })?;
                    }
                }

                writeln!(writer, ");")
                    .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;

                // INSERT statements for data
                if table.row_count() > 0 {
                    writeln!(writer)
                        .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
                    for row in table.scan() {
                        write!(writer, "INSERT INTO {} VALUES (", &table_name).map_err(|e| {
                            StorageError::NotImplemented(format!("Write error: {}", e))
                        })?;
                        for (i, value) in row.values.iter().enumerate() {
                            if i > 0 {
                                write!(writer, ", ").map_err(|e| {
                                    StorageError::NotImplemented(format!("Write error: {}", e))
                                })?;
                            }
                            write!(writer, "{}", sql_value_to_literal(value)).map_err(|e| {
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

        // Export indexes
        writeln!(writer, "-- Indexes")
            .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
        for index_name in self.list_indexes() {
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
                write!(writer, "{}", col.column_name)
                    .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
                write!(writer, " {:?}", col.direction)
                    .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
            }

            writeln!(writer, ");")
                .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;
        }
        writeln!(writer)
            .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;

        writeln!(writer, "-- End of dump")
            .map_err(|e| StorageError::NotImplemented(format!("Write error: {}", e)))?;

        Ok(())
    }
}

/// Format a DataType for SQL CREATE TABLE statement
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
        SqlValue::Numeric(f) => f.to_string(),
        SqlValue::Float(f) | SqlValue::Real(f) => {
            if f.is_nan() {
                "'NaN'".to_string()
            } else if f.is_infinite() {
                if f.is_sign_positive() {
                    "'Infinity'".to_string()
                } else {
                    "'-Infinity'".to_string()
                }
            } else {
                f.to_string()
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
                f.to_string()
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
    }
}
